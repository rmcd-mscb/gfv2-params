"""Topography helpers for the dprst_depth_avg spike/builder (issue #173).

Promoted verbatim from the Phase 0 diagnostic probe
(`scripts/diagnose/dprst_depth_probe.py`) so both the diagnostic and the
Phase 1 builder import a single validated copy — no duplicated logic. See
docs/superpowers/specs/2026-07-10-dprst-depth-phase0-spike-design.md for the
design context these functions were validated against.
"""
from __future__ import annotations

import math
import uuid

import geopandas as gpd
import numpy as np
import rasterio
import richdem as rd
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.features import geometry_mask
from rasterio.vrt import WarpedVRT
from rasterio.warp import calculate_default_transform, transform_bounds, transform_geom
from rasterio.windows import from_bounds
from scipy import ndimage

from ..depstor import load_connected_comids, select_connected_waterbodies
from ..nhd_ftypes import EXCLUDE_WATERBODY_FTYPES, FORCE_DPRST_FTYPES

gdal.UseExceptions()


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


def _read_vector_arrow(path, layer, columns, logger):
    """`gpd.read_file` with the pyarrow-backed pyogrio engine, fiona fallback.

    Mirrors `depstor_builders/dprst_depth.py`'s `_load_vector` and the plan
    hook's own read idiom — kept here so the shared
    `load_fabric_dprst_polygons` below (called by BOTH the builder and the
    plan hook) reads vectors identically for both callers.
    """
    try:
        return gpd.read_file(path, layer=layer, columns=columns, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer, columns=columns)


def _clip_dprst_to_fabric(dprst, hru_gpkg, hru_layer, logger):
    """Keep only dprst polygons that intersect the fabric's HRU geometry.

    The dprst polygon set is reconstructed from the SHARED CONUS
    `conus_waterbodies.gpkg` (every fabric profile points `waterbody_gpkg`
    there), so without this clip a regional fabric (e.g. oregon) would
    reconstruct and process the ENTIRE CONUS dprst set (~321k polygons),
    not its own — defeating the "prove it on a small fabric first" workflow
    and making a regional run cost the same as CONUS. Bbox-prefilter against
    the HRU `total_bounds` (fast, drops the vast majority for a regional
    fabric), then refine with a spatial-indexed `sjoin(predicate="intersects")`
    against the HRU polygons themselves so only genuinely in-fabric polygons
    survive. For the CONUS `gfv2` fabric the HRU bbox IS CONUS, so essentially
    all polygons are (correctly) kept.
    """
    hru = _read_vector_arrow(hru_gpkg, hru_layer, None, logger)
    if hru.crs is not None and dprst.crs is not None and hru.crs != dprst.crs:
        hru = hru.to_crs(dprst.crs)

    minx, miny, maxx, maxy = hru.total_bounds
    pre = dprst.cx[minx:maxx, miny:maxy]
    if len(pre) == 0:
        logger.warning(
            "  fabric clip: NO dprst polygons fall in the HRU bbox — check the "
            "fabric/waterbody CRS alignment"
        )
        return pre.copy()

    joined = gpd.sjoin(pre, hru[["geometry"]], how="inner", predicate="intersects")
    kept_idx = joined.index.unique()
    return dprst.loc[kept_idx].copy()


def load_fabric_dprst_polygons(
    waterbody_gpkg,
    waterbody_layer,
    connected_comids_table,
    flowthrough_comids_table,
    hru_gpkg,
    hru_layer,
    logger,
) -> gpd.GeoDataFrame:
    """Reconstruct the dprst polygon set and CLIP it to the fabric extent.

    The single shared entry point used by BOTH the `dprst_depth` builder
    (`depstor_builders/dprst_depth.py::_load_dprst_polygons`) and the plan
    hook (`tiling.py::_load_and_tag_for_plan`), so the reconstruction + the
    fabric clip can never diverge between the SLURM plan/array path and the
    in-process builder path. Steps:

      1. Union the connected(WBAREACOMI) COMID set with the optional
         flow-through COMID set.
      2. Load `conus_waterbodies.gpkg` and reconstruct the dprst polygon set
         (`dprst_polygons`: drop on-stream, force-Playa-dprst, exclude Ice Mass).
      3. Clip to the fabric's HRU geometry (`_clip_dprst_to_fabric`) — the
         fix for the CONUS-scope bug (a regional fabric would otherwise
         process the whole CONUS set).

    Callers own their own presence/existence validation of the paths before
    calling this (the builder raises fabric-profile-specific KeyErrors; the
    plan hook raises its own) — this function assumes the paths are valid.
    """
    connected = load_connected_comids(connected_comids_table)
    n_wbareacomi = len(connected)
    n_flowthrough = 0
    if flowthrough_comids_table is not None:
        flowthrough = load_connected_comids(flowthrough_comids_table)
        n_flowthrough = len(flowthrough - connected)
        connected = connected | flowthrough
    logger.info(
        "  connected COMIDs: %d WBAREACOMI + %d new flow-through = %d total",
        n_wbareacomi, n_flowthrough, len(connected),
    )

    wb_gdf = _read_vector_arrow(
        waterbody_gpkg, waterbody_layer, ["COMID", "FTYPE", "member_comid"], logger,
    )
    logger.info("  %d waterbody polygons loaded", len(wb_gdf))

    dprst = dprst_polygons(wb_gdf, connected)
    logger.info("  reconstructed CONUS dprst set: %d polygons", len(dprst))

    clipped = _clip_dprst_to_fabric(dprst, hru_gpkg, hru_layer, logger)
    logger.info(
        "  clipped to fabric: %d polygons (from %d CONUS)", len(clipped), len(dprst),
    )
    return clipped


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

# Both tile templates read over plain HTTPS (`/vsicurl/https://...`), NOT
# `/vsis3/...`, despite `prd-tnm` being a public anonymous bucket. Verified
# empirically 2026-07-10 (a second, distinct instance of the same class of
# gotcha already hit for WESM.gpkg — see the module-level note near
# WESM_HTTPS_URL below): on this HPC's network, opening a genuinely
# *nonexistent* `/vsis3/...` key hangs indefinitely (30s+ timeout, no error)
# instead of returning a 404 — reproduced directly with
# `rasterio.open("/vsis3/prd-tnm/.../nonexistent.tif")`. A plain
# `/vsicurl/https://prd-tnm.s3.amazonaws.com/...` GET/HEAD against the same
# missing key returns a clean 404 in ~5s (`curl -I` returns in <1s; GDAL's
# vsicurl layer is slower but still bounded). Both existing and missing keys
# were verified to work correctly over `/vsicurl/`. This matters because 1 m
# tile *existence* must be probed (`_existing_paths`, below) — a project's
# footprint isn't rectangular, so some 10 km-grid candidates in a bbox are
# expected to be genuinely missing, and `/vsis3/` would hang on exactly
# those. `/vsis3/` is fine for a key already known to exist (used elsewhere
# in this module's --audit path where existence isn't in question).

# 1/3 arc-second (10 m) seamless national elevation tiles, 1x1 deg, named by
# NW corner. Anonymous public bucket; see the `read_window` docstring.
TILE13_HTTPS_TEMPLATE = (
    "/vsicurl/https://prd-tnm.s3.amazonaws.com/"
    "StagedProducts/Elevation/13/TIFF/current/{tile}/USGS_13_{tile}.tif"
)

# 3DEP 1 m project tiles (verified empirically 2026-07-10 against a real ND
# project — see `read_window` docstring). Each WESM 1 m *project* publishes
# many ~10 km UTM tiles under its own `TIFF/` prefix, named
# USGS_1M_<zone>_x<E/10000>y<N/10000>_<project>.tif.
TILE1M_HTTPS_TEMPLATE = (
    "/vsicurl/https://prd-tnm.s3.amazonaws.com/"
    "StagedProducts/Elevation/1m/Projects/{project}/TIFF/"
    "USGS_1M_{zone:02d}_x{x}y{y}_{project}.tif"
)


def _normalize_nodata(
    arr: np.ndarray, src_nodata: float | None, sentinel: float = -9999.0
) -> np.ndarray:
    """Return a float32 copy of `arr` with void cells mapped to `sentinel`.

    A cell is void if it is NaN, or (when `src_nodata` is not None and not
    itself NaN) if it equals `src_nodata`. All other cells are unchanged.
    Pulled out of `read_window` (issue #173 review fix) so the realistic
    nodata-normalization path — a real numeric source nodata (e.g. 3DEP's
    -999999) alongside a NaN cell — is unit-testable without a live S3 read.
    """
    out = np.asarray(arr, dtype=np.float32).copy()
    void = np.isnan(out)
    if src_nodata is not None and not math.isnan(src_nodata):
        void |= out == np.float32(src_nodata)
    out[void] = sentinel
    return out


def depth_to_spill(dem: np.ndarray, nodata: float | None = None) -> np.ndarray:
    """filled - raw over a RAW dem. float64 fill per the DEM-derivatives gotcha.

    Never route through WhiteboxTools here (LZW+predictor=2 corruption gotcha
    is a non-issue for richdem, which works on in-memory arrays, but the
    convention of "float64 fill, richdem not WBT" is kept for consistency
    with compute_dem_derivatives.py). Returned depth is float32, clipped to
    be non-negative, and zeroed at nodata cells.

    ``nodata`` defaults to the ``-9999.0`` sentinel that `read_window` now
    normalizes every source's real nodata to (issue #173 review fix — see
    `read_window` docstring). The effective sentinel (explicit ``nodata`` if
    given, else -9999.0) is used BOTH to tell richdem which cells to exclude
    from the fill AND to zero those cells in the returned depth — previously
    the zeroing only ran when a caller passed `nodata` explicitly, so the
    realistic `read_window` -> `depth_to_spill(dem)` call (no explicit
    `nodata` arg) left the zeroing dead code: a raw nodata void (e.g. a
    source's real -999999) fed into richdem *without* being flagged as
    no_data gets treated as an extremely low real elevation and filled up to
    the surrounding rim, producing a huge spurious depth at every void cell.
    """
    a = np.asarray(dem, dtype=np.float64)
    nd = -9999.0 if nodata is None else float(nodata)
    rda = rd.rdarray(a, no_data=nd)
    filled = np.asarray(rd.FillDepressions(rda, in_place=False), dtype=np.float64)
    depth = filled - a
    depth[depth < 0] = 0.0
    depth[a == nd] = 0.0
    return depth.astype(np.float32)


def is_hydroflattened(dem_in_polygon: np.ndarray, tol_m: float = 0.01) -> dict:
    """A hydro-flattened water surface is breakline-enforced -> exactly constant.
    Test interior range, not just variance."""
    v = np.asarray(dem_in_polygon, dtype=np.float64)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return {"range": float("nan"), "std": float("nan"), "n_unique": 0, "flat": False}
    rng = float(v.max() - v.min())
    return {
        "range": rng,
        "std": float(v.std()),
        "n_unique": int(np.unique(np.round(v, 3)).size),
        "flat": rng < tol_m,
    }


def volume_mean_depth(depth: np.ndarray, mask: np.ndarray, cell_area_m2: float):
    """V = sum(depth*area), A = sum(area) over masked cells; mean = V/A (metres)."""
    sel = depth[mask]
    a = float(mask.sum()) * cell_area_m2
    v = float(sel.sum()) * cell_area_m2
    mean_d = v / a if a > 0 else 0.0
    return v, a, mean_d


def lake_max_depth(dem: np.ndarray, polygon_mask: np.ndarray, transform) -> float:
    """Hollister/lakeMorpho-style: project the mean shoreline slope inward to the
    lake's point of maximum distance-to-shore. Predicts MAX depth.

    A ring cell (or a `np.gradient` neighbour of one) that sits on a
    `read_window`-style `-9999.0` nodata void (or a non-finite value) must
    never enter the slope calculation: a real elevation jumping to -9999
    across one pixel produces a wildly inflated gradient and an absurd
    `max_depth` (issue #173 T6 finding). Void cells are neutralized to NaN
    before the gradient is taken (so a void neighbour poisons only its own
    `np.gradient` output, via `np.nanmean`, not the whole ring), and the
    ring itself is also stripped of any surviving void cells before
    averaging.
    """
    sentinel = -9999.0
    cell = abs(transform.a) if transform.a else 1.0
    # mean terrain slope in a shoreline ring just outside the lake
    ring = ndimage.binary_dilation(polygon_mask, iterations=2) & ~polygon_mask
    dem_arr = np.asarray(dem, float)
    void = (dem_arr == sentinel) | ~np.isfinite(dem_arr)
    ring = ring & ~void
    dem_clean = np.where(void, np.nan, dem_arr)
    gy, gx = np.gradient(dem_clean, cell)
    slope = np.hypot(gx, gy)
    ring_slope = slope[ring]
    if not ring.any() or not np.isfinite(ring_slope).any():
        return 0.0
    mean_slope = float(np.nanmean(ring_slope))
    if not np.isfinite(mean_slope):
        return 0.0
    # max distance from any lake cell to the shore
    dist = ndimage.distance_transform_edt(polygon_mask) * cell
    return mean_slope * float(dist.max())


def _interior_mask(dem: np.ndarray, transform, geom, sentinel: float = -9999.0) -> np.ndarray:
    """Boolean mask of `dem` cells whose centre lies inside `geom` (the raw,
    unbuffered dprst polygon) and are not the nodata sentinel.

    `read_window`'s DEM window covers `geom.bounds` padded by `rim_buffer_m`
    on every side; rasterizing the *unbuffered* polygon geometry onto that
    same transform is exactly "exclude the rim buffer, keep only the
    polygon interior" — no separate erosion needed, the rim buffer only
    exists outside `geom` in the first place.

    Ported verbatim from the Phase 0 diagnostic probe
    (`scripts/diagnose/dprst_depth_probe.py`, issue #173 Task 4) so the
    probe and the Phase 1 builder (`compute.py`) share one validated
    definition of "polygon interior" instead of two drifting copies.
    """
    if dem.size == 0:
        return np.zeros(dem.shape, dtype=bool)
    mask = geometry_mask([geom], out_shape=dem.shape, transform=transform, invert=True)
    mask &= dem != sentinel
    return mask


def max_to_mean(max_depth: float, shape: str = "cone") -> float:
    """dprst_depth_avg is MEAN (V/A). Conical basin: mean = max/3."""
    factors = {"cone": 1.0 / 3.0, "paraboloid": 1.0 / 2.0, "cylinder": 1.0}
    return max_depth * factors[shape]


def _tile13_name(lon: float, lat: float) -> str:
    """1x1 deg 1/3 arc-second tile name from a point, e.g. n48w101 for a
    point at lat 47.3, lon -100.6 (NW corner: north=ceil(lat), west=ceil(-lon)).

    Verified empirically 2026-07-10: gdalinfo on
    /vsis3/prd-tnm/StagedProducts/Elevation/13/TIFF/current/n48w104/USGS_13_n48w104.tif
    has its NW corner at (-104.00056, 48.00056), i.e. tile n48w104 covers
    lon in [-104, -103), lat in [47, 48) — matches this convention exactly.
    """
    north = int(np.ceil(lat))
    west = int(np.ceil(-lon))
    return f"n{north:02d}w{west:03d}"


def _utm_zone_epsg(lon: float) -> tuple[int, str]:
    """NAD83 UTM zone + EPSG code for a CONUS (northern-hemisphere) longitude.

    3DEP 1 m tiles are keyed to the natural UTM zone of their project area —
    verified empirically: USGS_1M_13_x56y532_ND_3DEPProcessing_D22.tif (near
    104W) opens as EPSG:26913 (NAD83 UTM zone 13N), matching
    zone = floor((lon+180)/6)+1 = 13 and EPSG 26900+zone.
    """
    zone = int(math.floor((lon + 180.0) / 6.0)) + 1
    return zone, f"EPSG:269{zone:02d}"


def _1m_candidate_tiles(
    project: str, bounds_utm: tuple[float, float, float, float], zone: int
) -> list[str]:
    """Enumerate /vsis3/ paths for the 3DEP 1 m tiles covering a UTM bbox.

    Tiles sit on a 10 km x 10 km UTM grid (plus a small overlap buffer baked
    into each tile's actual raster extent — see the module-level
    TILE1M_HTTPS_TEMPLATE note). Verified empirically:
    USGS_1M_13_x56y532_ND_3DEPProcessing_D22.tif has raster origin
    (559994.0, 5320006.0) against a nominal 560000/5320000 UTM corner, i.e.
    x = floor(easting / 10000) (the tile's west edge / 10000) and
    y = ceil(northing / 10000) (the tile's north edge / 10000).
    """
    minx, miny, maxx, maxy = bounds_utm
    x_lo, x_hi = int(math.floor(minx / 10_000)), int(math.floor(maxx / 10_000))
    y_lo, y_hi = int(math.ceil(miny / 10_000)), int(math.ceil(maxy / 10_000))
    return [
        TILE1M_HTTPS_TEMPLATE.format(project=project, zone=zone, x=x, y=y)
        for y in range(y_lo, y_hi + 1)
        for x in range(x_lo, x_hi + 1)
    ]


def _existing_paths(candidates: list[str]) -> list[str]:
    """Filter candidate `/vsicurl/` tile paths to those that actually open.

    The 10 km UTM grid is a superset of the tiles a project actually
    publishes (project footprints aren't rectangular), so each candidate is
    probed with a real (cheap, COG-header-only) open; non-existent keys
    raise `RasterioIOError` (a clean, bounded ~5 s 404 over `/vsicurl/`) and
    are dropped rather than aborting the read. Candidates MUST be
    `/vsicurl/https://...` paths, not `/vsis3/...` — see the module-level
    note above TILE13_HTTPS_TEMPLATE for why a nonexistent `/vsis3/` key
    hangs instead of erroring on this HPC's network.
    """
    found = []
    for path in candidates:
        try:
            with rasterio.open(path):
                found.append(path)
        except RasterioIOError:
            continue
    return found


def _native_resolution(src, dst_crs: str) -> tuple[float, float]:
    """Source GSD reprojected into `dst_crs` units, for `WarpedVRT(resolution=...)`.

    Without an explicit `resolution=`, GDAL's default-transform heuristic can
    coarsen the output pixel size on reprojection; passing the true native
    GSD (recomputed via `calculate_default_transform`, which handles both a
    projected-metres 1 m UTM source and a geographic-degrees 10 m source
    uniformly) keeps a 1 m tile ~1 m and a 10 m tile ~10 m once warped into
    EPSG:5070 — see the "why nearest" note in `read_window`.
    """
    transform, _, _ = calculate_default_transform(
        src.crs, dst_crs, src.width, src.height, *src.bounds
    )
    return abs(transform.a), abs(transform.e)


def _resolve_1m_paths(geom, wesm_row, bounds_5070: tuple[float, float, float, float]) -> list[str]:
    """Resolve the /vsis3/ 1 m source path(s) covering `geom` for `best_topo == "1m"`.

    `wesm_row` accepts three shapes:
      - a bare string: an already-resolved single raster path/VRT (caller override).
      - a mapping with "s3_path": ditto, explicit single-source override.
      - a mapping with "project" (the WESM `project` field, e.g.
        "ND_3DEPProcessing_D22"): resolves the actual covering per-tile 1 m
        COGs from S3 (the real per-project case — see module docstring note).
    Returns an empty list if no covering tile exists (caller falls back to 10 m).
    Must be called inside an active `rasterio.Env`.
    """
    if isinstance(wesm_row, str):
        return [wesm_row]
    if hasattr(wesm_row, "__getitem__"):
        try:
            explicit = wesm_row["s3_path"]
        except (KeyError, IndexError):
            explicit = None
        if explicit:
            return [explicit]
        try:
            project = wesm_row["project"]
        except (KeyError, IndexError):
            project = None
        if project:
            centroid_5070 = {"type": "Point", "coordinates": (geom.centroid.x, geom.centroid.y)}
            centroid_4326 = transform_geom("EPSG:5070", "EPSG:4326", centroid_5070)
            lon, lat = centroid_4326["coordinates"]
            zone, utm_crs = _utm_zone_epsg(lon)
            bounds_utm = transform_bounds("EPSG:5070", utm_crs, *bounds_5070, densify_pts=21)
            candidates = _1m_candidate_tiles(project, bounds_utm, zone)
            return _existing_paths(candidates)
    raise ValueError(
        "best_topo='1m' requires wesm_row to be a source path, or a mapping "
        "with 's3_path' or 'project'"
    )


def read_window(geom, best_topo: str, wesm_row=None, rim_buffer_m: float = 200.0):
    """Windowed RAW-DEM read of geom bbox + rim from the best-available source.

    best_topo == "1m": resolve and read the covering 3DEP 1 m project tile(s)
      (see `_resolve_1m_paths`); mosaics 2-4 tiles in-memory via `gdal.BuildVRT`
      when the buffered window straddles a tile boundary. Falls back to the
      10 m path if no covering tile exists.
    best_topo == "10m": read the seamless 1/3 arc-second tile from
      /vsis3/prd-tnm/StagedProducts/Elevation/13/TIFF/current/<tile>/USGS_13_<tile>.tif
    Returns (dem float32, transform, crs, source) — reprojected on read to
    EPSG:5070 (equal area) so cell_area is uniform; never materialises beyond
    the window. `source` is `{"requested": best_topo, "resolution": "1m"|"10m",
    "paths": [...]}` — the resolution actually used (may differ from
    `requested` on a 1m->10m fallback) and the source path(s) read, so the
    caller has provenance for Phase 1 bucketing.

    `geom` is expected in EPSG:5070 — the CRS of the shipped dprst polygon set
    (`conus_waterbodies.gpkg`), which is also this function's `dst_crs`, so
    `rim_buffer_m` (metres) adds directly to `geom.bounds` with no reprojection.
    The 10 m tile grid is named by lon/lat, so only the centroid is reprojected
    to EPSG:4326 (via `transform_geom`) to resolve the tile name; the 1 m grid
    is named by UTM easting/northing, so the buffered bbox is reprojected to
    the project's natural UTM zone (`_utm_zone_epsg`) to resolve tile indices.

    Nearest-neighbour resampling (not bilinear): a hydro-flattened water
    surface is exactly constant per USGS Lidar Base Specification breakline
    enforcement, and Task 4's flatness detector depends on that constancy
    surviving the read. Bilinear blends rim/bottom elevations across the
    shoreline breakline and injects a false gradient into an exactly-constant
    surface — defeating the detector before it runs. Nearest preserves the
    raw per-pixel DEM value (a constant surface resampled nearest stays
    constant) and also avoids blending real elevations with nodata at a tile
    edge. Native GSD (`_native_resolution`) is preserved in the WarpedVRT
    rather than falling back to GDAL's coarser auto-computed default.

    Real-source nodata is normalized to the -9999.0 sentinel before return
    (mirrors `compute_dem_derivatives._fix_dem_nodata`'s convention). Verified
    empirically 2026-07-10: both the 1 m and 10 m 3DEP sources declare
    `NoData Value=-999999`, not -9999 — feeding that raw sentinel into
    `depth_to_spill` unnormalized (its no_data default is -9999.0) would
    leave a real nodata void looking like an extremely low but "valid"
    elevation, which richdem would then fill up to the surrounding rim,
    producing a huge spurious depth at every void cell (tile edge / data gap
    inside the window). Normalizing here makes `depth_to_spill`'s default
    correct for the realistic `read_window` -> `depth_to_spill` call path.
    """
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
        resolution_used = best_topo
        paths: list[str] = []
        if best_topo == "1m":
            if wesm_row is None:
                raise ValueError("best_topo='1m' requires wesm_row with a source project/path")
            paths = _resolve_1m_paths(geom, wesm_row, (minx, miny, maxx, maxy))
            if not paths:
                resolution_used = "10m"  # no covering 1 m tile found; document the fallback
        if best_topo == "10m" or (best_topo == "1m" and not paths):
            centroid_5070 = {"type": "Point", "coordinates": (geom.centroid.x, geom.centroid.y)}
            centroid_4326 = transform_geom("EPSG:5070", "EPSG:4326", centroid_5070)
            lon, lat = centroid_4326["coordinates"]
            tile = _tile13_name(lon, lat)
            paths = [TILE13_HTTPS_TEMPLATE.format(tile=tile)]
        elif best_topo not in ("1m", "10m"):
            raise ValueError(f"unknown best_topo {best_topo!r}; expected '1m' or '10m'")

        vsimem_vrt = None
        try:
            if len(paths) > 1:
                vsimem_vrt = f"/vsimem/dprst_depth_probe_{uuid.uuid4().hex}.vrt"
                gdal.BuildVRT(vsimem_vrt, paths)
                open_path = vsimem_vrt
            else:
                open_path = paths[0]

            with rasterio.open(open_path) as src:
                resolution = _native_resolution(src, "EPSG:5070")
                with WarpedVRT(
                    src, crs="EPSG:5070", resampling=Resampling.nearest, resolution=resolution,
                ) as vrt:
                    window = from_bounds(minx, miny, maxx, maxy, transform=vrt.transform)
                    dem = vrt.read(1, window=window).astype(np.float32)
                    transform = vrt.window_transform(window)
                    crs = vrt.crs
                    nodata = vrt.nodata
        finally:
            if vsimem_vrt is not None:
                gdal.Unlink(vsimem_vrt)

    dem = _normalize_nodata(dem, nodata)

    source = {"requested": best_topo, "resolution": resolution_used, "paths": paths}
    return dem, transform, crs, source
