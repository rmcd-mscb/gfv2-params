"""Diagnostic probe for Issue #173 Phase 0 spike: dprst_depth_avg from
best-available topography. Analysis-only; not a builder. See
docs/superpowers/specs/2026-07-10-dprst-depth-phase0-spike-design.md.
"""
from __future__ import annotations

import argparse
import gc
import subprocess
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import rasterio
import richdem as rd
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_geom
from rasterio.windows import from_bounds

from gfv2_params.depstor import load_connected_comids, select_connected_waterbodies
from gfv2_params.nhd_ftypes import EXCLUDE_WATERBODY_FTYPES, FORCE_DPRST_FTYPES


def dprst_polygons(wb_gdf: gpd.GeoDataFrame, connected: set[int]) -> gpd.GeoDataFrame:
    """Reconstruct the shipped `dprst` polygon set at the polygon level.

    Mirrors wbody_connectivity -> dprst: drop genuinely on-stream waterbodies,
    force Playa to dprst, exclude Ice Mass entirely.
    """
    if "FTYPE" not in wb_gdf.columns:
        raise KeyError("waterbody layer has no FTYPE column; cannot classify dprst")
    wb = wb_gdf[~wb_gdf["FTYPE"].isin(EXCLUDE_WATERBODY_FTYPES)].copy()
    onstream = select_connected_waterbodies(wb, connected)
    onstream = onstream[~onstream["FTYPE"].isin(FORCE_DPRST_FTYPES)]
    onstream_idx = set(onstream.index)
    return wb[~wb.index.isin(onstream_idx)].copy()


def resolution_class(
    dprst_gdf: gpd.GeoDataFrame, wesm_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Tag each dprst polygon with its best available topo source.

    "1m" if the polygon centroid lies inside any WESM workunit footprint,
    else "10m" (seamless 1/3 arc-second floor). Centroid test keeps it a
    single fast spatial join at CONUS scale.
    """
    out = dprst_gdf.copy()
    pts = out.set_geometry(out.geometry.centroid)
    wesm = wesm_gdf.to_crs(out.crs)[["geometry"]]
    hit = gpd.sjoin(pts, wesm, how="left", predicate="within")
    out["best_topo"] = hit.groupby(level=0)["index_right"].first().notna().map(
        {True: "1m", False: "10m"}
    )
    return out


# --- Windowed best-available-topo reader + depth-to-spill (Task 3) ---------

# 1/3 arc-second (10 m) seamless national elevation tiles, 1x1 deg, named by
# NW corner. Anonymous public bucket; see the `read_window` docstring.
TILE13_S3_TEMPLATE = (
    "/vsis3/prd-tnm/StagedProducts/Elevation/13/TIFF/current/{tile}/USGS_13_{tile}.tif"
)


def depth_to_spill(dem: np.ndarray, nodata: float | None = None) -> np.ndarray:
    """filled - raw over a RAW dem. float64 fill per the DEM-derivatives gotcha.

    Never route through WhiteboxTools here (LZW+predictor=2 corruption gotcha
    is a non-issue for richdem, which works on in-memory arrays, but the
    convention of "float64 fill, richdem not WBT" is kept for consistency
    with compute_dem_derivatives.py). Returned depth is float32, clipped to
    be non-negative, and zeroed at nodata cells.
    """
    a = np.asarray(dem, dtype=np.float64)
    rda = rd.rdarray(a, no_data=(nodata if nodata is not None else -9999.0))
    filled = np.asarray(rd.FillDepressions(rda, in_place=False), dtype=np.float64)
    depth = filled - a
    depth[depth < 0] = 0.0
    if nodata is not None:
        depth[a == nodata] = 0.0
    return depth.astype(np.float32)


def volume_mean_depth(depth: np.ndarray, mask: np.ndarray, cell_area_m2: float):
    """V = sum(depth*area), A = sum(area) over masked cells; mean = V/A (metres)."""
    sel = depth[mask]
    a = float(mask.sum()) * cell_area_m2
    v = float(sel.sum()) * cell_area_m2
    mean_d = v / a if a > 0 else 0.0
    return v, a, mean_d


def _tile13_name(lon: float, lat: float) -> str:
    """1x1 deg 1/3 arc-second tile name from a point, e.g. n48w101 for a
    point at lat 47.3, lon -100.6 (NW corner: north=ceil(lat), west=ceil(-lon))."""
    north = int(np.ceil(lat))
    west = int(np.ceil(-lon))
    return f"n{north:02d}w{west:03d}"


def read_window(geom, best_topo: str, wesm_row=None, rim_buffer_m: float = 200.0):
    """Windowed RAW-DEM read of geom bbox + rim from the best-available source.

    best_topo == "1m": read the covering WESM project COG (path from wesm_row).
    best_topo == "10m": read the seamless 1/3 arc-second tile from
      /vsis3/prd-tnm/StagedProducts/Elevation/13/TIFF/current/<tile>/USGS_13_<tile>.tif
    Returns (dem float32, transform, crs). Reproject-on-read to EPSG:5070 (equal
    area) so cell_area is uniform; never materialise beyond the window.

    `geom` is expected in EPSG:5070 — the CRS of the shipped dprst polygon set
    (`conus_waterbodies.gpkg`), which is also this function's `dst_crs`, so
    `rim_buffer_m` (metres) adds directly to `geom.bounds` with no reprojection.
    The 10 m tile grid is named by lon/lat, so only the centroid is reprojected
    to EPSG:4326 (via `transform_geom`) to resolve the tile name.

    NOTE (2026-07-10 smoke test): unlike the 10 m floor, a WESM 1 m *project*
    is not published as a single COG — S3 listing shows each project's `TIFF/`
    directory holds many small per-tile GeoTIFFs (e.g.
    `USGS_one_meter_x41y517_ND_KidderCO_2014.tif`, ~1 km tiles keyed by a
    UTM-derived x/y index, not by polygon). `wesm_row` here is treated as
    already carrying a single resolved raster path (e.g. a project VRT the
    caller built via `gdalbuildvrt`); resolving the covering per-tile 1 m COG
    (or building/caching a per-project VRT) from a raw WESM row is unimplemented
    — flagged as a Phase-1 follow-up, not solved by this spike task.
    """
    if best_topo == "1m":
        if wesm_row is None:
            raise ValueError("best_topo='1m' requires wesm_row with a source COG path")
        src_path = wesm_row["s3_path"] if hasattr(wesm_row, "__getitem__") else wesm_row
    elif best_topo == "10m":
        centroid_5070 = {"type": "Point", "coordinates": (geom.centroid.x, geom.centroid.y)}
        centroid_4326 = transform_geom("EPSG:5070", "EPSG:4326", centroid_5070)
        lon, lat = centroid_4326["coordinates"]
        tile = _tile13_name(lon, lat)
        src_path = TILE13_S3_TEMPLATE.format(tile=tile)
    else:
        raise ValueError(f"unknown best_topo {best_topo!r}; expected '1m' or '10m'")

    minx, miny, maxx, maxy = geom.bounds
    minx -= rim_buffer_m
    miny -= rim_buffer_m
    maxx += rim_buffer_m
    maxy += rim_buffer_m

    env_opts = {
        "AWS_NO_SIGN_REQUEST": "YES",
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    }
    with rasterio.Env(**env_opts):
        with rasterio.open(src_path) as src:
            with WarpedVRT(src, crs="EPSG:5070", resampling=rasterio.enums.Resampling.bilinear) as vrt:
                window = from_bounds(minx, miny, maxx, maxy, transform=vrt.transform)
                dem = vrt.read(1, window=window)
                transform = vrt.window_transform(window)
                crs = vrt.crs
    return dem.astype(np.float32), transform, crs


# --- CONUS coverage audit (--audit) -----------------------------------------
#
# WESM (Work Extent Spatial Metadata) is the authoritative 3DEP workunit
# footprint index (~3,258 workunits, ~3.6 GB GeoPackage). The task brief
# points at the S3 object via `/vsis3/prd-tnm/...`, but two GDAL access paths
# were tried and rejected on this cluster before falling back to a local
# download:
#   - `/vsis3/...` (anonymous request): GDAL's GeoPackage driver issues a
#     metadata-table probe query on open (`SELECT COUNT(*) FROM sqlite_master
#     WHERE name IN ('gpkg_metadata', ...)`) that raises "attempt to write a
#     readonly database", reproduced with both fiona and
#     `gdal.OpenEx(..., GA_ReadOnly)`.
#   - `/vsicurl/https://prd-tnm.s3.amazonaws.com/...` (plain HTTPS, sidesteps
#     the /vsis3/ driver bug above and opens fine): but a full-layer read
#     over this path raised "database disk image is malformed" partway
#     through — reproduced on both a full feature scan and a pushed-down SQL
#     GROUP BY. GeoPackage's SQLite b-tree access pattern issues many
#     scattered small-range HTTP requests across a 3.6 GB file, which is not
#     reliable on this network.
# A one-time plain HTTPS download to local disk (`ensure_wesm_local`) reads
# the same authoritative S3 object as an ordinary local SQLite file once
# fetched, avoiding both failure modes.
WESM_HTTPS_URL = (
    "https://prd-tnm.s3.amazonaws.com/"
    "StagedProducts/Elevation/metadata/WESM.gpkg"
)


def ensure_wesm_local(cache_dir: Path, logger, url: str = WESM_HTTPS_URL) -> Path:
    """Download (or reuse a cached copy of) WESM.gpkg and return its local path.

    See the module-level note above for why this reads over plain HTTPS to
    local disk rather than a GDAL `/vsis3/` or `/vsicurl/` virtual path.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = cache_dir / "WESM.gpkg"
    if local_path.exists():
        logger.info("Using cached WESM index: %s", local_path)
        return local_path
    tmp_path = cache_dir / "WESM.gpkg.part"
    logger.info("Downloading WESM workunit index (~3.6 GB) to %s ...", local_path)
    subprocess.run(["curl", "-fsS", "-o", str(tmp_path), url], check=True)
    tmp_path.rename(local_path)
    logger.info("  download complete: %s", local_path)
    return local_path


def load_wesm_1m_footprints(logger, path: Path, batch_size: int = 100) -> gpd.GeoDataFrame:
    """Read the WESM workunit index and keep only 1 m/QL1/QL2-qualifying footprints.

    WESM covers every 3DEP workunit regardless of quality (legacy 3-30 m
    LiDAR projects included). Its own `onemeter_category` field flags whether
    a workunit meets the 3DEP 1 m DEM spec; observed values on this download
    are {"Meets", "Meets with variance", "Does not meet", "Pending
    publication"}. "Meets"/"Meets with variance" are both effectively
    published 1 m/QL1/QL2 product footprints (variance = minor spec
    deviation, still 1 m data); "Does not meet" is legacy coarser LiDAR and
    "Pending publication" is not yet downloadable — both are excluded from
    the "1m" tier `resolution_class` keys on.

    Reads in two passes: a geometry-free attribute scan of all ~3,258
    workunits (cheap — logs the full category breakdown), then a batched
    geometry read filtered by a pushed-down SQL WHERE to *only* the
    qualifying ~1,790 rows. Each batch's geometries are immediately
    collapsed to their convex hull before the next batch is read.

    Two earlier, simpler versions both OOM-killed on the HPC login node's
    session memory cgroup (~11 GB observed ceiling): reading every
    workunit's full-precision geometry (including the ~1,468
    legacy/pending ones this function discards) cost ~9.5 GB; even after
    WHERE-pushdown to just the 1,790 qualifying rows it still cost ~7.6 GB.
    Some WESM workunit footprints are the un-dissolved union of every
    constituent LAS-tile rectangle (thousands of polygon parts each
    carrying GEOS/Shapely per-part object overhead) rather than a single
    simplified outline — `.simplify()` does not reduce part count, only
    per-ring vertex count, so it would not have helped. The convex hull
    collapses each workunit to one simple polygon. For a Phase-0 coverage
    *audit* (not a per-pixel data-availability claim), this is an
    acceptable, one-directional approximation: it can only ever tag a
    dprst polygon "1m" that the true (non-convex) footprint would have
    left "10m", never the reverse.
    """
    logger.info("Reading WESM workunit footprint index: %s", path)
    attrs = pyogrio.read_dataframe(
        str(path), columns=["onemeter_category"], read_geometry=False,
    )
    logger.info("  %d total WESM workunits", len(attrs))
    breakdown = attrs["onemeter_category"].value_counts().to_dict()
    logger.info("  onemeter_category breakdown: %s", breakdown)
    del attrs

    qualifying = ("Meets", "Meets with variance")
    where = "onemeter_category IN ({})".format(", ".join(f"'{v}'" for v in qualifying))
    parts = []
    offset = 0
    while True:
        batch = gpd.read_file(
            path, columns=["workunit", "onemeter_category"], where=where,
            skip_features=offset, max_features=batch_size,
        )
        if len(batch) == 0:
            break
        batch["geometry"] = batch.geometry.convex_hull
        parts.append(batch)
        offset += batch_size
    if not parts:
        raise ValueError(
            "0 WESM workunits meet the 1 m spec — onemeter_category values may "
            "have changed; refusing to silently report 0% 1m coverage."
        )
    onem = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=parts[0].crs)
    del parts
    logger.info(
        "  %d workunits qualify as 1 m/QL1/QL2 (onemeter_category in %s); "
        "footprints simplified to convex hulls",
        len(onem), qualifying,
    )
    return onem


def _read_vector(path, layer, columns, logger):
    """geopandas.read_file with the pyarrow-backed pyogrio engine, falling
    back to fiona if pyarrow is unavailable (mirrors wbody_connectivity.py)."""
    try:
        return gpd.read_file(path, layer=layer, columns=columns, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer, columns=columns)


def assign_vpu(
    dprst_gdf: gpd.GeoDataFrame, hru_gpkg, hru_layer: str, logger,
    batch_size: int = 50_000,
) -> pd.Series:
    """Tag each dprst polygon with the VPU of the HRU its centroid falls in.

    Pure-vector centroid-in-polygon join against the fabric's `vpu` attribute
    (no vpu_id raster — the fabric has no vector VPU-boundary layer of its
    own, so the HRU polygons stand in for one). Reads the 361k-polygon HRU
    fabric in sequential batches rather than one whole-fabric GeoDataFrame: a
    single full read OOM-killed on the HPC login node's per-session memory
    cgroup (~11 GB observed ceiling) once its geometries sat alongside the
    dprst frame and WESM index already in memory. Each batch's sjoin result
    is folded in and the batch discarded before the next read.
    """
    pts = dprst_gdf.set_geometry(dprst_gdf.geometry.centroid)
    assigned = pd.Series(pd.NA, index=dprst_gdf.index, dtype="object")
    crs = None
    offset = 0
    n_hru = 0
    while True:
        chunk = gpd.read_file(
            hru_gpkg, layer=hru_layer, columns=["vpu"],
            skip_features=offset, max_features=batch_size, use_arrow=True,
        )
        if len(chunk) == 0:
            break
        n_hru += len(chunk)
        if crs is None:
            crs = chunk.crs
            if crs != pts.crs:
                logger.info("  Reprojecting HRU batches from %s to %s", crs, pts.crs)
        if chunk.crs != pts.crs:
            chunk = chunk.to_crs(pts.crs)
        hit = gpd.sjoin(pts, chunk[["vpu", "geometry"]], how="inner", predicate="within")
        assigned.loc[hit.index] = hit["vpu"].to_numpy()
        offset += batch_size
        del chunk, hit
    logger.info("  %d HRU polygons scanned in batches of %d", n_hru, batch_size)
    return assigned


def run_audit(base: dict, logger, wesm_cache_dir: Path) -> pd.DataFrame:
    """Reconstruct the CONUS dprst polygon set, tag best-available topo
    resolution and VPU, log the national split, and return the per-VPU table.
    """
    from gfv2_params.config import require_config_key

    waterbody_gpkg = require_config_key(base, "waterbody_gpkg", "dprst_depth_probe")
    waterbody_layer = require_config_key(base, "waterbody_layer", "dprst_depth_probe")
    connected_table = Path(require_config_key(base, "connected_comids_table", "dprst_depth_probe"))
    flowthrough_table = base.get("flowthrough_comids_table")
    hru_gpkg = require_config_key(base, "hru_gpkg", "dprst_depth_probe")
    hru_layer = require_config_key(base, "hru_layer", "dprst_depth_probe")

    connected = load_connected_comids(connected_table)
    n_wbareacomi = len(connected)
    n_flowthrough = 0
    if flowthrough_table is not None:
        flowthrough_table = Path(flowthrough_table)
        flowthrough = load_connected_comids(flowthrough_table)
        n_flowthrough = len(flowthrough - connected)
        connected = connected | flowthrough
    logger.info(
        "connected COMIDs: %d WBAREACOMI + %d new flow-through = %d total",
        n_wbareacomi, n_flowthrough, len(connected),
    )

    logger.info("Reading waterbodies: %s (layer=%s)", waterbody_gpkg, waterbody_layer)
    wb_gdf = _read_vector(
        waterbody_gpkg, waterbody_layer,
        ["COMID", "FTYPE", "member_comid"],
        logger,
    )
    logger.info("  %d waterbody polygons", len(wb_gdf))

    dprst = dprst_polygons(wb_gdf, connected)
    del wb_gdf  # 448k full-CONUS waterbody polygons no longer needed
    gc.collect()
    dprst["area_km2"] = dprst.geometry.area / 1e6
    total_polys = len(dprst)
    total_km2 = float(dprst["area_km2"].sum())
    logger.info(
        "dprst polygons (reconstructed shipped set): %d, total area: %.1f km^2",
        total_polys, total_km2,
    )
    # Sanity check against the issue's measured figure (~285,998 polygons /
    # ~53,159 km^2). Never silently cap or truncate — just log loud if this
    # run is wildly off (more than 3x either direction), since that would
    # signal an upstream input drift, not something to paper over here.
    ref_polys, ref_km2 = 285_998, 53_159
    if not (ref_polys / 3 <= total_polys <= ref_polys * 3):
        logger.warning(
            "dprst polygon count %d is >3x off the issue's reference figure "
            "%d — check inputs before trusting this audit.", total_polys, ref_polys,
        )
    if not (ref_km2 / 3 <= total_km2 <= ref_km2 * 3):
        logger.warning(
            "dprst total area %.1f km^2 is >3x off the issue's reference figure "
            "%d km^2 — check inputs before trusting this audit.", total_km2, ref_km2,
        )

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    wesm = load_wesm_1m_footprints(logger, wesm_path)
    dprst = resolution_class(dprst, wesm)
    del wesm
    gc.collect()

    logger.info("Assigning VPU via HRU fabric (batched): %s (layer=%s)", hru_gpkg, hru_layer)
    dprst["vpu"] = assign_vpu(dprst, hru_gpkg, hru_layer, logger).to_numpy()
    n_unassigned = int(dprst["vpu"].isna().sum())
    if n_unassigned:
        logger.warning(
            "%d/%d dprst polygon centroids fell outside every HRU (no VPU "
            "assigned) — likely coastal/edge slivers; kept as 'unassigned'.",
            n_unassigned, total_polys,
        )
        dprst["vpu"] = dprst["vpu"].fillna("unassigned")

    # National split.
    national = dprst.groupby("best_topo").agg(n=("COMID", "size"), area_km2=("area_km2", "sum"))
    national["pct_n"] = 100.0 * national["n"] / total_polys
    national["pct_area"] = 100.0 * national["area_km2"] / total_km2
    logger.warning(
        "1m%% figures below are a convex-hull UPPER BOUND: WESM multi-part "
        "workunit footprints are collapsed to their convex hull before the "
        "spatial join (see load_wesm_1m_footprints docstring), which can "
        "only OVERSTATE 1m coverage, never understate it. True 1m coverage "
        "may be lower.",
    )
    for topo in ("1m", "10m"):
        if topo in national.index:
            row = national.loc[topo]
            logger.info(
                "  national best_topo=%s: %d polys (%.1f%%), %.1f km^2 (%.1f%%)",
                topo, int(row["n"]), row["pct_n"], row["area_km2"], row["pct_area"],
            )

    # Per-VPU table: count/area at 1m vs 10m, plus the % split within VPU.
    per_vpu = (
        dprst.groupby(["vpu", "best_topo"])
        .agg(n=("COMID", "size"), area_km2=("area_km2", "sum"))
        .reset_index()
    )
    vpu_totals = per_vpu.groupby("vpu").agg(n_total=("n", "sum"), area_km2_total=("area_km2", "sum"))
    per_vpu = per_vpu.join(vpu_totals, on="vpu")
    per_vpu["pct_n"] = 100.0 * per_vpu["n"] / per_vpu["n_total"]
    per_vpu["pct_area"] = 100.0 * per_vpu["area_km2"] / per_vpu["area_km2_total"]
    per_vpu = per_vpu.sort_values(["vpu", "best_topo"]).reset_index(drop=True)
    return per_vpu


def main() -> None:
    from gfv2_params.config import load_base_config
    from gfv2_params.log import configure_logging

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", action="store_true", required=True,
                         help="Run the CONUS coverage audit (Task 2, issue #173).")
    parser.add_argument("--fabric", default=None,
                         help="Fabric name (overrides FABRIC env / default_fabric).")
    parser.add_argument(
        "--out-dir", type=Path, required=True,
        help="Directory to write coverage_audit.csv into. REQUIRED — also "
             "doubles as the WESM.gpkg download cache dir (multi-GB "
             "one-time download), so pick a path with enough free space; "
             "there is no default.",
    )
    args = parser.parse_args()

    logger = configure_logging("dprst_depth_probe")
    base = load_base_config(fabric=args.fabric)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    per_vpu = run_audit(base, logger, wesm_cache_dir=args.out_dir)

    out_csv = args.out_dir / "coverage_audit.csv"
    caveat = (
        "# CAVEAT: 1m%/1m-count figures are a convex-hull UPPER BOUND — WESM "
        "multi-part workunit footprints are collapsed to their convex hull "
        "before the best_topo spatial join, which can only OVERSTATE 1m "
        "coverage, never understate it. True 1m coverage may be lower. See "
        "load_wesm_1m_footprints() in dprst_depth_probe.py.\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        per_vpu.to_csv(f, index=False)
    logger.info("Wrote per-VPU coverage audit: %s (%d rows)", out_csv, len(per_vpu))


if __name__ == "__main__":
    main()
