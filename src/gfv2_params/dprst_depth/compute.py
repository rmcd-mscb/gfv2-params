"""Per-tile-SET compute of per-polygon `dprst_depth_avg` (issue #173 Task 4;
tile-set batching + threading, issue #223 part 2).

This is the compute core the SLURM array fans out over: open each of this
batch's real 3DEP tile SETS (`sources.TileSet` -- one project, one UTM zone,
`sources.tag_and_assign`'s ranked candidate list) ONCE and run the depth math
for every polygon whose PRIMARY set that is, rather than opening a fresh
source per polygon. Measured on the gfv2r2 CONUS run: 51.6% of polygons
(202,680 of 392,672) took a per-polygon fallback that opened a fresh remote
source for each one -- 73% of that time was `vrt.read` (network I/O + warp),
only 24% arithmetic, and tasks ran at ~55% of one core. Opening each set once
and running sets CONCURRENTLY on threads is the fix: the work is I/O-bound
(waiting on remote reads), so Python's GIL release during `vrt.read` lets
`n_threads` windows actually overlap.

Four layers, ordered by how much I/O they touch:

- `_polygon_depth_from_dem` — pure numpy-in/dict-out core (unit-tested
  offline, no S3). Given a DEM window + the polygon's interior mask,
  decides hydro-flattened ("flat") vs a genuine, measurable depression and
  returns the depth stats either way.
- `compute_polygon` — the single-polygon, always-correct path: wraps
  `topo.read_window` (which resolves + mosaics whatever 1 m/10 m source(s)
  cover the polygon) and `topo._interior_mask`, then calls
  `_polygon_depth_from_dem`. Kept for a lone polygon (e.g. a live smoke
  test) — `run_batch` no longer calls it (a tile SET's `open_tile_set`
  already yields a single mosaicked/warped source, so there is no separate
  "spans more than one source" case left to fall back for).
- `open_tile_set`/`_compute_one` — the per-set primitives `run_batch` calls:
  `open_tile_set` opens ONE `TileSet` (a single tile, or an in-memory
  `gdal.BuildVRT` mosaic of one project's same-zone tiles) as a warped
  EPSG:5070 VRT; `_compute_one` windows one polygon against an already-open
  set and returns `None` (never raises) if the interior came back with no
  valid cells there, so the caller can try the polygon's next ranked
  candidate instead of shipping a false measurement.
- `run_batch` — the batch driver: groups this batch's polygons by their
  PRIMARY tile set (`source_tiles`), opens each set once, and runs sets
  CONCURRENTLY on `n_threads` threads. A polygon whose primary set yields no
  usable interior (or fails to open/read) walks its remaining ranked
  `candidates` in a second pass, ending at the always-present 10 m seamless
  tile — see `run_batch`'s own docstring for the counter/logging contract
  this candidate-recovery path preserves from the pre-#223 per-tile design.
"""
from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.features import geometry_mask
from rasterio.vrt import WarpedVRT

from .sources import decode
from .topo import (
    GDAL_HTTP_ENV,
    _interior_mask,
    _native_resolution,
    depth_to_spill,
    is_hydroflattened,
    lake_max_depth,
    read_padded,
    read_window,
    volume_mean_depth,
)

__all__ = ["_polygon_depth_from_dem", "compute_polygon", "open_tile_set", "run_batch"]

# GDAL/rasterio env for anonymous public-bucket HTTPS reads — a COPY of
# `topo.GDAL_HTTP_ENV` (see topo.py's module notes on /vsicurl/ vs /vsis3/
# and its HTTP timeout rationale). Copied, not aliased: `rasterio.Env(**...)`
# never mutates its kwargs, but sharing one mutable dict between two modules
# would make an in-place edit to either module's "own" copy silently change
# the other's env too.
_ENV_OPTS = dict(GDAL_HTTP_ENV)

# Output columns of `run_batch`'s parquet, fixed so an empty batch (a
# SLURM array task with 0 assigned tile sets) still writes a well-formed,
# concat-able parquet rather than a columnless one. `source` is the winning
# tile set's `project` (or `"10m"` on the seamless fallback); `interior_coverage`
# is the fraction of the polygon's TRUE interior footprint whose cells were
# real, not `read_padded` sentinel padding -- a diagnostic, never a gate at
# write time (toolkit review round 4, finding 6a/7, issue #223): a partially-
# covered polygon still ships as `method="measured"`, and this column is what
# lets a later donor-filter (next task) exclude it from regional calibration.
_OUTPUT_COLUMNS = [
    "COMID",
    "dprst_depth_m",
    "measured_max_m",
    "hollister_max_m",
    "flat",
    "resolution",
    "method",
    "source",
    "interior_coverage",
]


def _polygon_depth_from_dem(
    dem: np.ndarray, interior_mask: np.ndarray, transform, nodata: float = -9999.0
) -> dict:
    """Pure core: DEM window + interior mask -> depth stats for one polygon.

    Hydro-flattened water surfaces are published as an exactly-constant
    breakline elevation (USGS Lidar Base Spec): the Phase 0 spike validated
    that the flatness verdict must be read off the POLYGON INTERIOR alone
    (`interior_mask`-selected cells) — that's what produced the trustworthy
    SwampMarsh 21.7% / LakePond 11% flattened fractions (pre-#223
    measurement — the probe's reads predate `read_padded` and were subject
    to the same misregistration/empty-window bug it fixes, so re-measure
    rather than re-cite per this repo's rule) (a hydro-flattened lake's
    interior reads EXACTLY 0.000 m range). Running the gate over the
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
def open_tile_set(ts):
    """ONE open per tile set: a single tile, or an in-memory mosaic of one
    project's same-zone tiles (BuildVRT cannot mix CRSs, which is why a set
    is single-zone -- see sources.rank_candidates). Warped to EPSG:5070 at
    native GSD with nearest resampling, exactly as the per-tile path always
    did, so in-bounds single-tile reads stay bit-identical."""
    vsimem = None
    path = ts.keys[0]
    if len(ts.keys) > 1:
        vsimem = f"/vsimem/dprst_depth_set_{uuid.uuid4().hex}.vrt"
        gdal.BuildVRT(vsimem, list(ts.keys))
        path = vsimem
    try:
        with rasterio.open(path) as src:
            resolution = _native_resolution(src, "EPSG:5070")
            with WarpedVRT(
                src, crs="EPSG:5070", resampling=Resampling.nearest, resolution=resolution
            ) as vrt:
                yield vrt
    finally:
        if vsimem is not None:
            gdal.Unlink(vsimem)


def _read_tile_window(vrt, geom, rim_buffer_m: float = 200.0) -> tuple[np.ndarray, object]:
    """Windowed RAW-DEM read of `geom`'s buffered bbox against an ALREADY-OPEN VRT.

    The single-source counterpart of `read_window`'s inner read block —
    deliberately NOT calling `read_window` itself, which always opens its
    source fresh (that per-call open is exactly what `run_batch`'s tile
    cache avoids for polygons that don't straddle a tile boundary). `geom`
    must be in the VRT's CRS (EPSG:5070, matching `dprst_gdf`/
    `read_window`'s convention), so `rim_buffer_m` (metres) adds directly
    to `geom.bounds` with no reprojection, exactly as in `read_window`.
    Delegates to `topo.read_padded` (#223) so a buffered window that
    overhangs this tile's edge comes back correctly clipped-and-padded
    instead of silently misregistered or empty.
    """
    minx, miny, maxx, maxy = geom.bounds
    return read_padded(
        vrt, (minx - rim_buffer_m, miny - rim_buffer_m, maxx + rim_buffer_m, maxy + rim_buffer_m)
    )


def _compute_one(vrt, geom) -> dict | None:
    """Depth stats for one polygon against an open set, or `None` if its
    interior has no valid cells there (so the caller tries the next
    candidate).

    Also reports `interior_coverage`: the fraction of the polygon's TRUE
    interior footprint (before the `dem != sentinel` exclusion `_interior_mask`
    applies inline) whose cells were real, not `read_padded` sentinel
    padding. `_interior_mask` returns only the post-exclusion mask and
    `topo.py` is out of scope for this task, so the footprint is rasterized a
    SECOND time here rather than splitting `_interior_mask` into two return
    values (toolkit review round 4, findings 6a/7, issue #223) -- `geom` is
    guaranteed non-degenerate here (`interior.any()` already returned True),
    so `footprint.sum()` is always >= 1. A low-but-nonzero coverage still
    ships as `method="measured"` with no gate at write time -- this figure is
    what lets a later donor-filter (next task) exclude it from regional
    calibration; see docs/dprst_depth_avg_reference.md's "coverage loss"
    paragraph.
    """
    dem, transform = _read_tile_window(vrt, geom)
    interior = _interior_mask(dem, transform, geom)
    if not interior.any():
        return None
    footprint = geometry_mask([geom], out_shape=dem.shape, transform=transform, invert=True)
    result = _polygon_depth_from_dem(dem, interior, transform)
    result["resolution"] = "1m" if abs(transform.a) < 5.0 else "10m"
    result["interior_coverage"] = float(interior.sum()) / max(int(footprint.sum()), 1)
    return result


def _empty_batch_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_OUTPUT_COLUMNS)


def run_batch(
    dprst_gdf: gpd.GeoDataFrame,
    tile_sets: list[str],
    out_parquet: str | Path,
    logger: logging.Logger,
    n_threads: int = 1,
) -> pd.DataFrame:
    """Compute every polygon whose PRIMARY tile set is in `tile_sets`.

    Each set is opened once (`open_tile_set`) and all its member polygons are
    windowed against it; sets run concurrently on `n_threads` threads (the work
    is remote-read bound: 73% of time in `vrt.read` on gfv2r2). A polygon whose
    primary yields no valid interior (or fails to read) walks its remaining
    ranked `candidates`, ending at the 10 m seamless tile. Counters stay split
    (#173 PR#177 FIX 2): read failures (expected, WARNING) vs compute errors
    (bugs, ERROR) vs polygons with no usable source at all.
    """
    for col in ("source_tiles", "candidates", "COMID"):
        if col not in dprst_gdf.columns:
            raise KeyError(f"run_batch needs '{col}' (plan with sources.tag_and_assign first)")
    wanted = set(tile_sets)
    members = {s: list(g.index) for s, g in dprst_gdf.groupby("source_tiles") if s in wanted}
    counts = {"n_read_failure": 0, "n_compute_error": 0, "n_recovered": 0, "n_no_source": 0}
    lock = threading.Lock()

    def _bump(key):
        with lock:
            counts[key] += 1

    def _attempt(ts_str, idxs):
        """Run `idxs` against one set. Returns ({idx: result}, [unresolved idx])."""
        ts = decode(ts_str)
        done, pending = {}, []
        try:
            with rasterio.Env(**_ENV_OPTS), open_tile_set(ts) as vrt:
                for idx in idxs:
                    try:
                        r = _compute_one(vrt, dprst_gdf.geometry.loc[idx])
                    except RasterioIOError as exc:
                        _bump("n_read_failure")
                        logger.warning("  set=%s idx=%s: read failure (%s)", ts.project, idx, exc)
                        r = None
                    except Exception as exc:  # noqa: BLE001 - loud, isolated, never aborts the batch
                        _bump("n_compute_error")
                        logger.error("  set=%s idx=%s: UNEXPECTED compute error (%s: %s)",
                                     ts.project, idx, type(exc).__name__, exc)
                        r = None
                    else:
                        if r is None:
                            # `_compute_one` returned None WITHOUT raising: an empty
                            # interior on this set, not an exception. This is the
                            # read-failure COUNT + WARNING from round-4's run_batch
                            # fix (#223) -- carry it forward here too, or the
                            # candidate-recovery rewrite silently drops the only
                            # operator-visible signal that a set's window missed.
                            _bump("n_read_failure")
                            logger.warning(
                                "  set=%s idx=%s: window has no valid interior on "
                                "this set — trying next candidate", ts.project, idx,
                            )
                    if r is None:
                        pending.append(idx)
                    else:
                        r["source"] = ts.project
                        done[idx] = r
        except RasterioIOError as exc:
            _bump("n_read_failure")
            logger.warning("  set=%s: open failed (%s) — %d polygon(s) to recovery", ts.project, exc, len(idxs))
            pending = list(idxs)
        return done, pending

    results, to_recover = {}, []
    with ThreadPoolExecutor(max(1, n_threads)) as ex:
        for i, (done, pending) in enumerate(ex.map(lambda kv: _attempt(*kv), sorted(members.items())), 1):
            results.update(done)
            to_recover += pending
            if i % 25 == 0:
                logger.info("  [%d/%d tile sets] %d polygons done", i, len(members), len(results))

    def _recover(idx):
        for ts_str in dprst_gdf.at[idx, "candidates"][1:]:
            done, _ = _attempt(ts_str, [idx])
            if idx in done:
                return idx, done[idx]
        return idx, None

    with ThreadPoolExecutor(max(1, n_threads)) as ex:
        for idx, r in ex.map(_recover, sorted(to_recover)):
            if r is None:
                counts["n_no_source"] += 1
            else:
                counts["n_recovered"] += 1
                results[idx] = r

    rows = []
    for idx, r in results.items():
        r["COMID"] = dprst_gdf.at[idx, "COMID"]
        r["method"] = "flat_pending" if r["flat"] else "measured"
        rows.append(r)
    out = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
    out = out.sort_values("COMID").reset_index(drop=True) if len(out) else _empty_batch_frame()
    out.to_parquet(out_parquet, index=False)
    logger.info("run_batch: %d/%d polygons written (%d tile sets, %s) -> %s",
                len(out), sum(len(v) for v in members.values()), len(members),
                ", ".join(f"{k}={v}" for k, v in counts.items()), out_parquet)
    return out
