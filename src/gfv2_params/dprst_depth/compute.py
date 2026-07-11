"""Per-tile compute of per-polygon `dprst_depth_avg` (issue #173 Task 4).

This is the compute core the SLURM array (Task 9) fans out over: read each
elevation tile ONCE and run the depth math for every dprst polygon whose
window falls in it, rather than opening a raster once per polygon (see
`tiling.py`'s module docstring for the compute-budget rationale).

Three layers, ordered by how much I/O they touch:

- `_polygon_depth_from_dem` — pure numpy-in/dict-out core (unit-tested
  offline, no S3). Given a DEM window + the polygon's interior mask,
  decides hydro-flattened ("flat") vs a genuine, measurable depression and
  returns the depth stats either way.
- `compute_polygon` — the single-polygon, always-correct path: wraps
  `topo.read_window` (which resolves + mosaics whatever 1 m/10 m source(s)
  cover the polygon) and `topo._interior_mask`, then calls
  `_polygon_depth_from_dem`. Used directly for a lone polygon (e.g. a
  multi-tile straggler, or a live smoke test) and by `run_batch` as the
  fallback path below.
- `run_batch` — the batch driver: opens each of this batch's tile keys
  ONCE as a `WarpedVRT` and windows every SINGLE-tile polygon assigned to
  it against that one open VRT (no re-open per polygon); polygons whose
  buffered window spans more than one of this batch's tiles fall back to
  `compute_polygon` (which mosaics correctly via `read_window`), computed
  once each — see `_read_tile_window`/`_open_tile_vrt` below for why this
  needs its own thin windowed-read glue instead of calling `read_window`
  per polygon (that would defeat the whole point: a fresh
  `rasterio.open` + fresh HTTP range reads for every polygon sharing a
  tile).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.vrt import WarpedVRT
from rasterio.windows import from_bounds

from .tiling import group_by_tile
from .topo import (
    _interior_mask,
    _native_resolution,
    _normalize_nodata,
    depth_to_spill,
    is_hydroflattened,
    lake_max_depth,
    read_window,
    volume_mean_depth,
)

__all__ = ["_polygon_depth_from_dem", "compute_polygon", "run_batch"]

# GDAL/rasterio env for anonymous public-bucket HTTPS reads — identical to
# `read_window`'s (see topo.py's module notes on /vsicurl/ vs /vsis3/).
_ENV_OPTS = {"AWS_NO_SIGN_REQUEST": "YES", "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR"}

# Output columns of `run_batch`'s parquet, fixed so an empty batch (a
# SLURM array task with 0 assigned tiles) still writes a well-formed,
# concat-able parquet rather than a columnless one.
_OUTPUT_COLUMNS = [
    "COMID",
    "dprst_depth_m",
    "measured_max_m",
    "hollister_max_m",
    "flat",
    "resolution",
    "method",
]


def _polygon_depth_from_dem(
    dem: np.ndarray, interior_mask: np.ndarray, transform, nodata: float = -9999.0
) -> dict:
    """Pure core: DEM window + interior mask -> depth stats for one polygon.

    Hydro-flattened water surfaces are published as an exactly-constant
    breakline elevation (USGS Lidar Base Spec): the Phase 0 spike validated
    that the flatness verdict must be read off the POLYGON INTERIOR alone
    (`interior_mask`-selected cells) — that's what produced the trustworthy
    SwampMarsh 21.7% / LakePond 11% flattened fractions (a hydro-flattened
    lake's interior reads EXACTLY 0.000 m range). Running the gate over the
    rim-inclusive window instead is wrong: a real hydro-flattened lake
    sitting in terrain with any surrounding relief would have a
    window-range > tol and be misclassified non-flat, and we'd then
    "measure" a depth off its flat water surface rather than its bed. A
    perfectly-constant interior IS what hydro-flattening looks like — that
    correctly reads flat=True; a genuine, non-flattened depression's floor
    carries real (>1 cm) interior relief and correctly reads flat=False.

    `hollister_max_m` (Task 6's terrain-slope max-depth predictor) is
    ALWAYS computed, flat or not: Task 5 uses it both as calibration fit
    data (non-flat rows: compare predicted vs `measured_max_m`) and as the
    actual filled value for flat rows (`method="flat_pending"` today,
    resolved to `"calibrated_hollister"` et al. in Task 5).

    Returns ``{"dprst_depth_m", "measured_max_m", "hollister_max_m",
    "flat"}``. Flat (or degenerate — an all-nodata/empty-interior window):
    ``dprst_depth_m``/``measured_max_m`` are ``nan`` (Task 5 fills them);
    otherwise ``dprst_depth_m`` is the V/A mean depth and ``measured_max_m``
    the max cell depth, both over `interior_mask`, both metres.
    """
    dem = np.asarray(dem, dtype=np.float64)
    interior_mask = np.asarray(interior_mask, dtype=bool)

    hollister_max_m = float(lake_max_depth(dem, interior_mask, transform))

    interior_valid = dem[interior_mask & (dem != nodata)]
    flat = interior_valid.size == 0 or bool(is_hydroflattened(interior_valid)["flat"])

    if flat:
        return {
            "dprst_depth_m": float("nan"),
            "measured_max_m": float("nan"),
            "hollister_max_m": hollister_max_m,
            "flat": True,
        }

    depth = depth_to_spill(dem, nodata=nodata)
    cell_area_m2 = abs(transform.a * transform.e)
    _, _, mean_d = volume_mean_depth(depth, interior_mask, cell_area_m2)
    measured_max_m = float(depth[interior_mask].max())
    return {
        "dprst_depth_m": mean_d,
        "measured_max_m": measured_max_m,
        "hollister_max_m": hollister_max_m,
        "flat": False,
    }


def compute_polygon(geom, best_topo: str, wesm_row=None) -> dict:
    """Full single-polygon path: `read_window` + interior mask + the core.

    Always correct regardless of how many tiles `geom`'s buffered window
    touches (`read_window` mosaics 2-4 1 m tiles via `gdal.BuildVRT` when
    needed) — the price is a fresh `rasterio.open` per call, which is fine
    for a single polygon but is exactly what `run_batch`'s tile cache
    exists to avoid at CONUS scale.

    Returns `_polygon_depth_from_dem`'s dict plus `resolution` (the
    source actually read — may be `"10m"` on a documented 1m->10m
    fallback, see `read_window`) and `method` (`"measured"` if not flat,
    else `"flat_pending"` — Task 5 fills the real fill method in).
    """
    dem, transform, _crs, source = read_window(geom, best_topo, wesm_row)
    interior_mask = _interior_mask(dem, transform, geom)
    result = _polygon_depth_from_dem(dem, interior_mask, transform)
    result["resolution"] = source["resolution"]
    result["method"] = "flat_pending" if result["flat"] else "measured"
    return result


@contextmanager
def _open_tile_vrt(tile_key: str):
    """Open one elevation tile ONCE as a `WarpedVRT`, for many windowed reads.

    Mirrors `read_window`'s per-source `WarpedVRT` setup exactly (native
    GSD via `_native_resolution`, nearest resampling so a hydro-flattened
    breakline's exact constancy survives the read — see `read_window`'s
    docstring) but yields the open VRT to the caller instead of reading a
    single window and closing: `run_batch` issues one windowed read per
    polygon assigned to this tile against the SAME open VRT, so GDAL's
    per-dataset block cache stays warm across them and the tile's
    COG header/IFD is only fetched once.
    """
    with rasterio.open(tile_key) as src:
        resolution = _native_resolution(src, "EPSG:5070")
        with WarpedVRT(
            src, crs="EPSG:5070", resampling=Resampling.nearest, resolution=resolution
        ) as vrt:
            yield vrt


def _read_tile_window(vrt, geom, rim_buffer_m: float = 200.0) -> tuple[np.ndarray, object]:
    """Windowed RAW-DEM read of `geom`'s buffered bbox against an ALREADY-OPEN VRT.

    The single-source counterpart of `read_window`'s inner read block
    (`from_bounds` -> `vrt.read` -> `window_transform` -> nodata
    normalization via `_normalize_nodata`) — deliberately NOT calling
    `read_window` itself, which always opens its source fresh (that
    per-call open is exactly what `run_batch`'s tile cache avoids for
    polygons that don't straddle a tile boundary). `geom` must be in the
    VRT's CRS (EPSG:5070, matching `dprst_gdf`/`read_window`'s
    convention), so `rim_buffer_m` (metres) adds directly to `geom.bounds`
    with no reprojection, exactly as in `read_window`.
    """
    minx, miny, maxx, maxy = geom.bounds
    minx -= rim_buffer_m
    miny -= rim_buffer_m
    maxx += rim_buffer_m
    maxy += rim_buffer_m
    window = from_bounds(minx, miny, maxx, maxy, transform=vrt.transform)
    dem = vrt.read(1, window=window).astype(np.float32)
    transform = vrt.window_transform(window)
    dem = _normalize_nodata(dem, vrt.nodata)
    return dem, transform


def _resolution_from_tile_key(tile_key: str) -> str:
    """`"1m"`/`"10m"` from a tile key's filename convention (Task 3's `tiling.py`)."""
    return "1m" if "USGS_1M_" in tile_key else "10m"


def _project_lookup(dprst_gdf: gpd.GeoDataFrame, wesm_gdf: gpd.GeoDataFrame) -> pd.Series:
    """`dprst_gdf` index -> covering WESM `project` string (centroid-in-footprint).

    Same sjoin idiom as `topo.resolution_class` (a single vectorized join,
    not one per polygon). Only needed for the multi-tile fallback path in
    `run_batch`: those polygons call `compute_polygon` -> `read_window`
    directly, which for `best_topo == "1m"` requires a `wesm_row` with a
    `project` to resolve its covering 3DEP 1 m tile(s) (single-tile
    polygons never need this — their tile key, already a resolved
    `/vsicurl/` path, is opened directly by `_open_tile_vrt`). Returns an
    empty Series if `wesm_gdf` carries no usable `project` index.
    """
    if wesm_gdf is None or len(wesm_gdf) == 0 or "project" not in wesm_gdf.columns:
        return pd.Series(dtype=object)
    pts = dprst_gdf.set_geometry(dprst_gdf.geometry.centroid)
    wesm = wesm_gdf.to_crs(dprst_gdf.crs)[["project", "geometry"]]
    hit = gpd.sjoin(pts, wesm, how="left", predicate="within")
    return hit.groupby(level=0)["project"].first()


def _empty_batch_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_OUTPUT_COLUMNS)


def run_batch(
    dprst_gdf: gpd.GeoDataFrame,
    tile_keys: list[str],
    wesm_gdf: gpd.GeoDataFrame,
    out_parquet: str | Path,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Compute `_polygon_depth_from_dem` for every polygon covered by `tile_keys`.

    `dprst_gdf` is the FULL dprst polygon set (already tagged with
    `best_topo` by `topo.resolution_class`, indexed as `tiling.group_by_tile`
    expects); `tile_keys` is ONE SLURM array task's slice of
    `tiling.tile_batches`'s output. `group_by_tile` is recomputed here (pure
    geometry, no I/O — see `tiling.py`'s module docstring) and restricted to
    `tile_keys` so `run_batch` only needs the batch's tile-key list, not the
    full CONUS tile->polygon dict, as its SLURM-array argument.

    Each tile in `tile_keys` is opened ONCE (`_open_tile_vrt`) and every
    polygon assigned ONLY to that tile within this batch is windowed
    against the same open VRT (`_read_tile_window`) — the whole point of
    the tile-grouped work-list (Task 3). A polygon whose buffered window
    spans MORE THAN ONE of this batch's tiles cannot be correctly read
    from a single tile's VRT (it may need a multi-tile mosaic), so it is
    computed exactly once via `compute_polygon` (full `read_window`) after
    the tile loop — this is the "dedup polygons that span multiple tiles"
    requirement: each polygon index is computed at most once PER BATCH.
    (A polygon whose covering tiles are split ACROSS batches by
    `tiling.tile_batches`'s bin-packing is out of scope here — `run_batch`
    only sees one batch's tile keys; CONUS-wide de-duplication across
    batch parquets, if any, is a Task 9 concatenation concern.)

    Writes `out_parquet` (one row per computed polygon: `COMID`,
    `dprst_depth_m`, `measured_max_m`, `hollister_max_m`, `flat`,
    `resolution`, `method`) and returns the same DataFrame. A read failure
    for one polygon or tile is logged and skipped, never aborts the batch.
    """
    if "best_topo" not in dprst_gdf.columns:
        raise KeyError(
            "dprst_gdf must be tagged by topo.resolution_class() first (missing 'best_topo')"
        )
    id_col = "COMID" if "COMID" in dprst_gdf.columns else None

    if not tile_keys:
        logger.info("run_batch: 0 tile keys assigned — writing empty parquet %s", out_parquet)
        empty = _empty_batch_frame()
        empty.to_parquet(out_parquet, index=False)
        return empty

    groups = group_by_tile(dprst_gdf, wesm_gdf)
    batch_tile_set = set(tile_keys)
    batch_groups = {k: v for k, v in groups.items() if k in batch_tile_set}

    tiles_per_polygon: dict[int, list[str]] = defaultdict(list)
    for tk, idxs in batch_groups.items():
        for idx in idxs:
            tiles_per_polygon[idx].append(tk)
    n_polygons = len(tiles_per_polygon)

    rows: list[dict] = []
    done: set = set()
    n_tile_reads = 0
    n_fallback = 0

    def _emit(idx, result: dict) -> None:
        if id_col is not None:
            result[id_col] = dprst_gdf.loc[idx, id_col]
        rows.append(result)
        done.add(idx)

    with rasterio.Env(**_ENV_OPTS):
        for tile_key in tile_keys:
            idxs = batch_groups.get(tile_key, [])
            single_tile_idxs = [
                idx for idx in idxs if idx not in done and len(tiles_per_polygon[idx]) == 1
            ]
            if not single_tile_idxs:
                continue
            try:
                with _open_tile_vrt(tile_key) as vrt:
                    n_tile_reads += 1
                    resolution = _resolution_from_tile_key(tile_key)
                    for idx in single_tile_idxs:
                        geom = dprst_gdf.geometry.loc[idx]
                        try:
                            dem, transform = _read_tile_window(vrt, geom)
                            interior_mask = _interior_mask(dem, transform, geom)
                            result = _polygon_depth_from_dem(dem, interior_mask, transform)
                        except Exception as exc:  # noqa: BLE001 - log and skip, never abort the batch
                            logger.warning(
                                "  tile=%s idx=%s: per-polygon read/compute failed (%s) — skipped",
                                tile_key, idx, exc,
                            )
                            continue
                        result["resolution"] = resolution
                        result["method"] = "flat_pending" if result["flat"] else "measured"
                        _emit(idx, result)
            except RasterioIOError as exc:
                logger.warning(
                    "  tile=%s: failed to open (%s) — its %d single-tile polygon(s) skipped "
                    "this batch",
                    tile_key, exc, len(single_tile_idxs),
                )
                continue
            if n_tile_reads % 25 == 0:
                logger.info(
                    "  [%d polygons / %d tiles read] tile=%s (%d polygons)",
                    len(rows), n_tile_reads, tile_key, len(single_tile_idxs),
                )

        remaining = [idx for idx in tiles_per_polygon if idx not in done]
        project_lookup = _project_lookup(dprst_gdf, wesm_gdf) if remaining else pd.Series(dtype=object)
        for idx in remaining:
            row = dprst_gdf.loc[idx]
            project = project_lookup.get(idx)
            wesm_row = {"project": project} if pd.notna(project) else None
            try:
                result = compute_polygon(row.geometry, row["best_topo"], wesm_row=wesm_row)
            except Exception as exc:  # noqa: BLE001 - log and skip, never abort the batch
                logger.warning(
                    "  idx=%s: multi-tile fallback compute_polygon failed (%s) — skipped",
                    idx, exc,
                )
                continue
            n_fallback += 1
            _emit(idx, result)

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        out_df = _empty_batch_frame()
    out_df.to_parquet(out_parquet, index=False)
    logger.info(
        "run_batch: %d/%d polygons written (%d tile reads, %d multi-tile fallback) -> %s",
        len(out_df), n_polygons, n_tile_reads, n_fallback, out_parquet,
    )
    return out_df
