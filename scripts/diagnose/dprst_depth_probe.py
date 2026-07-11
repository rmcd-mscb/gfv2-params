"""Diagnostic probe for Issue #173 Phase 0 spike: dprst_depth_avg from
best-available topography. Analysis-only; not a builder. See
docs/superpowers/specs/2026-07-10-dprst-depth-phase0-spike-design.md.
"""
from __future__ import annotations

import argparse
import gc
import math
import subprocess
import uuid
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import rasterio
import richdem as rd
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.vrt import WarpedVRT
from rasterio.warp import calculate_default_transform, transform_bounds, transform_geom
from rasterio.windows import from_bounds
from scipy import ndimage

from gfv2_params.depstor import load_connected_comids, select_connected_waterbodies
from gfv2_params.nhd_ftypes import EXCLUDE_WATERBODY_FTYPES, FORCE_DPRST_FTYPES

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
    lake's point of maximum distance-to-shore. Predicts MAX depth."""
    cell = abs(transform.a) if transform.a else 1.0
    # mean terrain slope in a shoreline ring just outside the lake
    ring = ndimage.binary_dilation(polygon_mask, iterations=2) & ~polygon_mask
    gy, gx = np.gradient(np.asarray(dem, float), cell)
    slope = np.hypot(gx, gy)
    mean_slope = float(slope[ring].mean()) if ring.any() else 0.0
    # max distance from any lake cell to the shore
    dist = ndimage.distance_transform_edt(polygon_mask) * cell
    return mean_slope * float(dist.max())


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


def load_conus_dprst(base: dict, logger) -> gpd.GeoDataFrame:
    """Reconstruct the shipped CONUS dprst polygon set (`wbody_connectivity`
    -> `dprst`) with an `area_km2` column.

    Shared by `run_audit` (Task 2) and `run_flatness` (Task 4) so both
    measure the exact same shipped classification — pulled out of `run_audit`
    verbatim (issue #173 Task 4), no behavioural change to the audit path.
    """
    from gfv2_params.config import require_config_key

    waterbody_gpkg = require_config_key(base, "waterbody_gpkg", "dprst_depth_probe")
    waterbody_layer = require_config_key(base, "waterbody_layer", "dprst_depth_probe")
    connected_table = Path(require_config_key(base, "connected_comids_table", "dprst_depth_probe"))
    flowthrough_table = base.get("flowthrough_comids_table")

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
    return dprst


def run_audit(base: dict, logger, wesm_cache_dir: Path) -> pd.DataFrame:
    """Reconstruct the CONUS dprst polygon set, tag best-available topo
    resolution and VPU, log the national split, and return the per-VPU table.
    """
    from gfv2_params.config import require_config_key

    hru_gpkg = require_config_key(base, "hru_gpkg", "dprst_depth_probe")
    hru_layer = require_config_key(base, "hru_layer", "dprst_depth_probe")

    dprst = load_conus_dprst(base, logger)
    total_polys = len(dprst)
    total_km2 = float(dprst["area_km2"].sum())

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


# --- Flatness detector: ND validation + SwampMarsh verdict (--flatness, Task 4) --

# North Dakota Prairie Pothole study area (issue #173 design doc: "Prairie
# Pothole Region, North Dakota ... also Hay and others 2018 PRMS
# depression-storage calibration site"). A generous state-level bbox in
# EPSG:4326 (minx, miny, maxx, maxy) — used only to restrict the CONUS dprst
# set and the WESM 1 m footprint index before picking a project, not as a
# precise study-area boundary.
ND_BBOX_4326 = (-104.05, 45.93, -96.55, 49.0)

# The four FTYPEs the spike must settle a per-FTYPE flatness verdict for
# (issue #173 design doc decision table). Order matters only for logging.
FLATNESS_FTYPES = ("SwampMarsh", "LakePond", "Playa", "Reservoir")


def select_nd_project(
    dprst_gdf: gpd.GeoDataFrame, wesm_path: Path, logger, top_n_log: int = 5,
) -> tuple[str, gpd.GeoDataFrame]:
    """Pick the North Dakota Prairie Pothole 1 m WESM project with the
    densest gfv2-dprst polygon overlap — NOT hardcoded (issue #173 design:
    "chosen programmatically ... not hardcoded").

    Restricts both the CONUS dprst polygon set and the WESM 1 m footprint
    index to `ND_BBOX_4326`, spatial-joins dprst polygon centroids against
    project footprints (real per-workunit geometry, not the audit path's
    convex-hull simplification — this bbox-restricted read is cheap enough
    to skip that optimization), and returns the project name with the most
    contained centroids plus the dprst subset that falls in it. Logs the top
    `top_n_log` candidates by overlap count so the choice is auditable, not
    just "first project matching a name filter" (the shortcut Task 3's smoke
    test used).
    """
    bbox_5070 = transform_bounds("EPSG:4326", "EPSG:5070", *ND_BBOX_4326, densify_pts=21)
    nd_dprst = dprst_gdf.cx[bbox_5070[0]:bbox_5070[2], bbox_5070[1]:bbox_5070[3]]
    logger.info(
        "  %d/%d dprst polygons fall in the ND Prairie Pothole bbox",
        len(nd_dprst), len(dprst_gdf),
    )

    bbox_4269 = transform_bounds("EPSG:4326", "EPSG:4269", *ND_BBOX_4326, densify_pts=21)
    wesm_nd = gpd.read_file(
        wesm_path, columns=["project", "onemeter_category"],
        where="onemeter_category IN ('Meets', 'Meets with variance')",
        bbox=bbox_4269,
    )
    logger.info("  %d 1m WESM workunit footprints intersect the ND bbox", len(wesm_nd))
    wesm_nd = wesm_nd.to_crs(nd_dprst.crs)

    pts = nd_dprst.set_geometry(nd_dprst.geometry.centroid)
    hit = gpd.sjoin(pts, wesm_nd[["project", "geometry"]], how="inner", predicate="within")
    hit = hit[~hit.index.duplicated(keep="first")]  # a centroid may land in >1 workunit footprint
    counts = hit.groupby("project").size().sort_values(ascending=False)
    if counts.empty:
        raise RuntimeError(
            "no 1m WESM project overlaps any dprst polygon centroid in the ND bbox "
            f"{ND_BBOX_4326} — cannot pick a study project."
        )
    logger.info(
        "  top ND 1m projects by dprst-polygon overlap:\n%s",
        counts.head(top_n_log).to_string(),
    )
    chosen = str(counts.index[0])
    logger.info(
        "  chosen project (densest dprst overlap): %s (%d dprst polygons)",
        chosen, int(counts.iloc[0]),
    )
    project_idx = hit.index[hit["project"] == chosen]
    return chosen, nd_dprst.loc[project_idx].copy()


def sample_per_ftype(
    gdf: gpd.GeoDataFrame, n_per_ftype: int, logger,
    ftypes: tuple[str, ...] = FLATNESS_FTYPES, seed: int = 173,
) -> dict[str, gpd.GeoDataFrame]:
    """Sample up to `n_per_ftype` dprst polygons per FTYPE, logging the exact
    per-FTYPE sample size actually used. NEVER silently caps below what's
    available — if a FTYPE has fewer than `n_per_ftype` polygons, every
    available polygon is used and that is logged explicitly.
    """
    rng = np.random.default_rng(seed)
    samples: dict[str, gpd.GeoDataFrame] = {}
    for ftype in ftypes:
        sub = gdf[gdf["FTYPE"] == ftype]
        n_avail = len(sub)
        if n_avail == 0:
            logger.warning(
                "  FTYPE=%s: 0 polygons available in the chosen project — skipped entirely",
                ftype,
            )
            samples[ftype] = sub.iloc[0:0]
            continue
        n = min(n_per_ftype, n_avail)
        if n_avail < n_per_ftype:
            logger.info(
                "  FTYPE=%s: only %d available (< target %d) — sampling all of them",
                ftype, n_avail, n_per_ftype,
            )
        else:
            logger.info("  FTYPE=%s: sampling %d of %d available", ftype, n, n_avail)
        idx = rng.choice(sub.index.to_numpy(), size=n, replace=False)
        samples[ftype] = sub.loc[idx]
    return samples


def _interior_mask(dem: np.ndarray, transform, geom, sentinel: float = -9999.0) -> np.ndarray:
    """Boolean mask of `dem` cells whose centre lies inside `geom` (the raw,
    unbuffered dprst polygon) and are not the nodata sentinel.

    `read_window`'s DEM window covers `geom.bounds` padded by `rim_buffer_m`
    on every side; rasterizing the *unbuffered* polygon geometry onto that
    same transform is exactly "exclude the rim buffer, keep only the
    polygon interior" — no separate erosion needed, the rim buffer only
    exists outside `geom` in the first place. Pulled out of `_interior_values`
    (issue #173 Task 5) so `run_freeboard` can reuse the identical interior
    definition Task 4's flatness detector uses, rather than re-deriving it —
    `_interior_values` (below) is now a thin wrapper over this mask.
    """
    from rasterio.features import geometry_mask

    if dem.size == 0:
        return np.zeros(dem.shape, dtype=bool)
    mask = geometry_mask([geom], out_shape=dem.shape, transform=transform, invert=True)
    mask &= dem != sentinel
    return mask


def _interior_values(dem: np.ndarray, transform, geom, sentinel: float = -9999.0) -> np.ndarray:
    """Interior (non-rim-buffer, non-nodata) `dem` values as a flat float64 array.

    See `_interior_mask` for the mask definition this wraps.
    """
    if dem.size == 0:
        return np.empty(0, dtype=np.float64)
    mask = _interior_mask(dem, transform, geom, sentinel)
    return dem[mask].astype(np.float64)


def analyze_flatness_sample(
    samples: dict[str, gpd.GeoDataFrame], project: str, logger, min_cells: int = 4,
) -> pd.DataFrame:
    """Run `read_window` -> interior mask -> `is_hydroflattened` over every
    sampled polygon and return one row per polygon (per-FTYPE aggregation is
    done by the caller from this table).
    """
    rows = []
    n_total = sum(len(v) for v in samples.values())
    n_done = 0
    for ftype, sub in samples.items():
        for _, row in sub.iterrows():
            geom = row.geometry
            comid = row["COMID"] if "COMID" in sub.columns else row.name
            n_done += 1
            try:
                dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
            except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
                logger.warning("  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                                n_done, n_total, ftype, comid, exc)
                continue
            vals = _interior_values(dem, transform, geom)
            if vals.size < min_cells:
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: only %d interior cells (< %d) — skipped",
                    n_done, n_total, ftype, comid, vals.size, min_cells,
                )
                continue
            stats = is_hydroflattened(vals)
            stats.update({
                "COMID": comid, "FTYPE": ftype, "n_cells": int(vals.size),
                "resolution": source["resolution"],
            })
            rows.append(stats)
            if n_done % 25 == 0 or n_done == n_total:
                logger.info("  [%d/%d] sampled so far (last: FTYPE=%s COMID=%s flat=%s range=%.4f)",
                            n_done, n_total, ftype, comid, stats["flat"], stats["range"])
    return pd.DataFrame(rows)


def summarize_by_ftype(per_polygon: pd.DataFrame, logger) -> pd.DataFrame:
    """Per-FTYPE flattened fraction + range/std/n_unique distributions —
    the decision table input for the SwampMarsh verdict."""
    if per_polygon.empty:
        raise RuntimeError("flatness sample produced 0 usable polygons — cannot summarize")
    agg = per_polygon.groupby("FTYPE").agg(
        n_sampled=("COMID", "size"),
        n_flat=("flat", "sum"),
        pct_flat=("flat", "mean"),
        range_median=("range", "median"),
        range_p90=("range", lambda s: float(np.percentile(s, 90))),
        std_median=("std", "median"),
        n_unique_median=("n_unique", "median"),
        pct_1m=("resolution", lambda s: float((s == "1m").mean())),
    ).reset_index()
    agg["pct_flat"] = 100.0 * agg["pct_flat"]
    agg["pct_1m"] = 100.0 * agg["pct_1m"]
    agg = agg.sort_values("FTYPE").reset_index(drop=True)
    for _, r in agg.iterrows():
        logger.info(
            "  FTYPE=%-10s n=%4d flat=%5.1f%% (range median=%.4f m, p90=%.4f m) 1m-read=%.1f%%",
            r["FTYPE"], int(r["n_sampled"]), r["pct_flat"], r["range_median"], r["range_p90"], r["pct_1m"],
        )
    return agg


def _write_separability_histogram(per_polygon: pd.DataFrame, out_png: Path, logger) -> None:
    """Log-scale histogram of interior elevation range, split flat vs
    natural — the visual evidence that the detector cleanly separates
    hydro-flattened (range ~ 0) from bare-earth (range >> tol_m) surfaces."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    flat_ranges = per_polygon.loc[per_polygon["flat"], "range"].clip(lower=1e-4)
    natural_ranges = per_polygon.loc[~per_polygon["flat"], "range"].clip(lower=1e-4)
    bins = np.logspace(-4, np.log10(max(per_polygon["range"].max(), 1.0) + 1e-6), 60)
    ax.hist(flat_ranges, bins=bins, alpha=0.7, label=f"flat (n={len(flat_ranges)})", color="#1f77b4")
    ax.hist(natural_ranges, bins=bins, alpha=0.7, label=f"natural (n={len(natural_ranges)})", color="#d62728")
    ax.axvline(0.01, color="black", linestyle="--", linewidth=1, label="tol_m = 0.01 m")
    ax.set_xscale("log")
    ax.set_xlabel("interior elevation range (m, log scale)")
    ax.set_ylabel("polygon count")
    ax.set_title("Hydro-flattening detector separability (ND dprst sample)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote separability histogram: %s", out_png)


def run_flatness(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path, n_per_ftype: int = 300,
) -> pd.DataFrame:
    """Issue #173 tasks 2+3: validate the flatness detector on a real ND
    sample and settle the SwampMarsh hydro-flattening question.

    Reconstructs the CONUS dprst set, restricts to the ND Prairie Pothole
    bbox, programmatically picks the 1 m project with the densest overlap
    (`select_nd_project`), samples up to `n_per_ftype` polygons per FTYPE
    (`sample_per_ftype`), reads each at 1 m and tests interior flatness
    (`analyze_flatness_sample`), and writes `flatness_by_ftype.csv` +
    `flatness_separability.png` to `out_dir`. Returns the per-FTYPE summary.
    """
    dprst = load_conus_dprst(base, logger)

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info("Selecting ND 1m project with densest dprst overlap ...")
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    logger.info("Sampling up to %d dprst polygons per FTYPE from project=%s ...", n_per_ftype, project)
    samples = sample_per_ftype(nd_dprst, n_per_ftype, logger)
    n_total = sum(len(v) for v in samples.values())
    logger.info("Total sample size across all FTYPEs: %d", n_total)

    logger.info("Reading each sampled polygon at 1m + testing interior flatness ...")
    per_polygon = analyze_flatness_sample(samples, project, logger)
    logger.info(
        "Flatness reads completed: %d/%d polygons produced a usable interior sample",
        len(per_polygon), n_total,
    )

    summary = summarize_by_ftype(per_polygon, logger)

    if "SwampMarsh" in summary["FTYPE"].to_numpy():
        sm = summary.loc[summary["FTYPE"] == "SwampMarsh"].iloc[0]
        logger.info(
            "*** SwampMarsh verdict: %.1f%% of sampled polygons are hydro-flattened "
            "(n=%d) — decides whether at-risk dprst area is ~89%% or ~38.5%% ***",
            sm["pct_flat"], int(sm["n_sampled"]),
        )
    else:
        logger.warning(
            "*** SwampMarsh verdict: UNDETERMINED — 0 SwampMarsh polygons in the "
            "chosen project's sample; cannot settle the headline question from this "
            "ND project alone ***",
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "flatness_by_ftype.csv"
    caveat = (
        f"# ND Prairie Pothole flatness validation (issue #173 Task 4). "
        f"Project={project}. Per-polygon sample: "
        + ", ".join(f"{k}={len(v)}" for k, v in samples.items())
        + ". flat = interior elevation range < 0.01 m (is_hydroflattened tol_m).\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        summary.to_csv(f, index=False)
    logger.info("Wrote per-FTYPE flatness summary: %s", out_csv)

    per_polygon_csv = out_dir / "flatness_per_polygon.csv"
    per_polygon.to_csv(per_polygon_csv, index=False)
    logger.info("Wrote per-polygon flatness detail: %s", per_polygon_csv)

    _write_separability_histogram(per_polygon, out_dir / "flatness_separability.png", logger)

    return summary


# --- Freeboard quantification over Task 4's detected-flat sample (--freeboard, Task 5) --


def analyze_freeboard_sample(
    flat_df: pd.DataFrame, dprst_lookup: gpd.GeoDataFrame, project: str, logger,
) -> pd.DataFrame:
    """Run `read_window` -> `depth_to_spill` -> interior mask -> `volume_mean_depth`
    over every Task-4-detected-flat polygon and return one row per polygon.

    `flat_df` is the `flat == True` slice of Task 4's `flatness_per_polygon.csv`
    (COMID + FTYPE only — the cached CSV has no geometry); `dprst_lookup` is the
    Task 4 ND dprst subset (`select_nd_project`'s return), indexed by COMID, used
    only to recover each flat polygon's geometry. Mean freeboard = filled - raw
    averaged over the polygon interior (`_interior_mask`, Task 4's helper) — the
    above-water spill storage a hydro-flattened breakline surface hides above its
    constant water-surface elevation.
    """
    rows = []
    n_total = len(flat_df)
    n_done = 0
    for row in flat_df.itertuples(index=False):
        comid = row.COMID
        ftype = row.FTYPE
        n_done += 1
        if comid not in dprst_lookup.index:
            logger.warning(
                "  [%d/%d] COMID=%s: not found in the ND dprst subset — skipped",
                n_done, n_total, comid,
            )
            continue
        geom = dprst_lookup.loc[comid, "geometry"]
        try:
            dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
        except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
            logger.warning(
                "  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                n_done, n_total, ftype, comid, exc,
            )
            continue
        mask = _interior_mask(dem, transform, geom)
        n_interior = int(mask.sum())
        if n_interior == 0:
            logger.warning(
                "  [%d/%d] FTYPE=%s COMID=%s: 0 interior cells after masking — skipped",
                n_done, n_total, ftype, comid,
            )
            continue
        depth = depth_to_spill(dem)
        cell_area_m2 = abs(transform.a * transform.e)
        v, a, mean_d = volume_mean_depth(depth, mask, cell_area_m2)
        frac_wet = float((depth[mask] > 0).mean())
        rows.append({
            "COMID": comid,
            "FTYPE": ftype,
            "area_m2": a,
            "mean_freeboard_m": mean_d,
            "mean_freeboard_in": mean_d * 39.3701,
            "frac_interior_wet": frac_wet,
            "n_interior_cells": n_interior,
            "resolution": source["resolution"],
        })
        if n_done % 25 == 0 or n_done == n_total:
            logger.info(
                "  [%d/%d] FTYPE=%s COMID=%s freeboard=%.3f in (frac_wet=%.2f)",
                n_done, n_total, ftype, comid, mean_d * 39.3701, frac_wet,
            )
    return pd.DataFrame(rows)


def _write_freeboard_cdf(result: pd.DataFrame, out_png: Path, logger) -> None:
    """CDF of mean freeboard (inches) across detected-flat polygons — the
    visual evidence for the Task 5 finding (routinely non-trivial vs ~0)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    vals = np.sort(result["mean_freeboard_in"].to_numpy())
    cdf = np.arange(1, len(vals) + 1) / len(vals)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(vals, cdf, drawstyle="steps-post", color="#1f77b4")
    ax.axvline(0.0, color="black", linestyle="--", linewidth=1, label="0 in (no freeboard)")
    ax.set_xlabel("mean freeboard (in)")
    ax.set_ylabel("cumulative fraction of detected-flat polygons")
    ax.set_title(f"Freeboard CDF over hydro-flattened dprst polygons (n={len(vals)})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote freeboard CDF: %s", out_png)


def run_freeboard(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path, limit: int | None = None,
) -> pd.DataFrame:
    """Issue #173 Task 5: quantify freeboard (filled - raw) over Task 4's
    detected-flat (hydro-flattened) dprst polygon sample.

    Reads the cached `flatness_per_polygon.csv` Task 4 wrote to `out_dir`
    rather than recomputing the flatness sample from scratch — FAILS LOUD
    if it is absent (that sample costs ~731 live /vsicurl/ reads, ~20 min;
    `--freeboard` is a downstream analysis over its cached result, not a
    standalone rerun). Filters to `flat == True` (~105/731 on the ND
    project, issue #173 Task 4), reconstructs the identical ND dprst subset
    + project (`select_nd_project`, same as Task 4) purely to recover each
    flat COMID's geometry (the per-polygon CSV is COMID/FTYPE/stats only —
    CSVs don't carry geometry), then runs `analyze_freeboard_sample` and
    writes `freeboard_dist.csv` + `freeboard_cdf.png`.

    `limit`, if given, caps the number of flat polygons analyzed (head of
    the flat slice) — for smoke-testing the read/compute path cheaply
    without paying for the full ~105-polygon read.
    """
    flat_csv = out_dir / "flatness_per_polygon.csv"
    if not flat_csv.exists():
        raise RuntimeError(
            f"{flat_csv} not found — --freeboard reads Task 4's flatness sample "
            "and does NOT recompute it (that sample costs ~731 live /vsicurl/ "
            f"reads, ~20 min). Run `--flatness --out-dir {out_dir}` first, then "
            "re-run --freeboard."
        )
    per_polygon = pd.read_csv(flat_csv)
    if "flat" not in per_polygon.columns or "COMID" not in per_polygon.columns:
        raise KeyError(
            f"{flat_csv} is missing 'flat'/'COMID' columns — not a Task 4 "
            "flatness_per_polygon.csv?"
        )
    flat_df = per_polygon[per_polygon["flat"].astype(bool)].copy()
    if limit is not None:
        flat_df = flat_df.head(limit)
    n_flat = len(flat_df)
    logger.info(
        "Loaded %d detected-flat polygons from %s (of %d total sampled)%s",
        n_flat, flat_csv, len(per_polygon),
        f" [--limit {limit}]" if limit is not None else "",
    )
    if n_flat == 0:
        raise RuntimeError(f"0 flat=True polygons in {flat_csv} — nothing to analyze")

    dprst = load_conus_dprst(base, logger)
    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info(
        "Reselecting the ND 1m project + dprst subset (Task 4's select_nd_project) "
        "to recover flat-polygon geometries ..."
    )
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    if "COMID" not in nd_dprst.columns:
        raise KeyError("ND dprst subset has no COMID column; cannot match Task 4's flat-polygon list")
    lookup = nd_dprst.drop_duplicates(subset="COMID").set_index("COMID")

    logger.info("Reading each detected-flat polygon at 1m + computing freeboard ...")
    result = analyze_freeboard_sample(flat_df, lookup, project, logger)
    n_ok = len(result)
    logger.info("Freeboard reads completed: %d/%d flat polygons produced a usable result", n_ok, n_flat)
    if result.empty:
        raise RuntimeError("freeboard analysis produced 0 usable polygons")

    med = float(result["mean_freeboard_in"].median())
    p10 = float(np.percentile(result["mean_freeboard_in"], 10))
    p90 = float(np.percentile(result["mean_freeboard_in"], 90))
    logger.info(
        "Freeboard distribution (inches) over detected-flat polygons: median=%.2f, "
        "p10=%.2f, p90=%.2f (n=%d)",
        med, p10, p90, n_ok,
    )
    if med < 1.0:
        logger.info(
            "*** Freeboard finding: median ~%.2f in is essentially ZERO — flattened "
            "ponds are outlet-controlled; baseline depth_to_spill on the RAW rim does "
            "NOT capture their storage, the terrain model must carry the submerged "
            "volume ***",
            med,
        )
    else:
        logger.info(
            "*** Freeboard finding: median ~%.2f in is NON-TRIVIAL — baseline "
            "depth_to_spill already captures meaningful above-water spill storage for "
            "flattened ponds ***",
            med,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "freeboard_dist.csv"
    caveat = (
        f"# Freeboard (filled - raw) over Task 4 detected-flat dprst polygons "
        f"(issue #173 Task 5). Project={project}. n_flat_input={n_flat}"
        + (f" (--limit {limit})" if limit is not None else "")
        + f", n_usable={n_ok}. mean_freeboard = volume_mean_depth(depth_to_spill(dem)) "
        "over the polygon interior (rim_buffer_m terrain and nodata excluded via "
        "_interior_mask).\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        result.to_csv(f, index=False)
    logger.info("Wrote per-polygon freeboard distribution: %s", out_csv)

    _write_freeboard_cdf(result, out_dir / "freeboard_cdf.png", logger)

    return result


# --- Hollister terrain-slope max-depth prototype (--hollister, Task 6) -----
#
# `lake_max_depth`/`max_to_mean` are geometry, not raster I/O — this section
# is the plumbing that runs them over a real ND sample using the same
# `select_nd_project`/`sample_per_ftype`/`read_window`/`_interior_mask`
# helpers Tasks 4-5 already validated, rather than re-deriving sampling or
# masking logic. Restricted to FTYPE=LakePond (issue #173 Task 6: "a
# LakePond sample from the ND project") — SwampMarsh/Playa/Reservoir are out
# of scope here.
HOLLISTER_FTYPES = ("LakePond",)


def analyze_hollister_sample(
    samples: dict[str, gpd.GeoDataFrame], project: str, logger,
) -> pd.DataFrame:
    """Run `read_window` -> interior polygon mask -> `lake_max_depth` ->
    `max_to_mean` over every sampled polygon and return one row per polygon.

    `max_depth_m` is the Hollister/lakeMorpho terrain-slope-extension
    estimate (mean shoreline-ring slope x max in-polygon distance-to-shore);
    `mean_depth_m` is the conical V/A conversion (`max_to_mean(..., "cone")`,
    factor 1/3) documented in the module docstring above `lake_max_depth`.
    Polygon area is taken from the sampled GeoDataFrame's own `area_km2`
    column (computed once, CONUS-wide, in `load_conus_dprst`) rather than
    re-derived from the raster window, so it matches the shipped dprst area
    exactly regardless of read/mask resolution.
    """
    rows = []
    n_total = sum(len(v) for v in samples.values())
    n_done = 0
    for ftype, sub in samples.items():
        for _, row in sub.iterrows():
            geom = row.geometry
            comid = row["COMID"] if "COMID" in sub.columns else row.name
            area_km2 = float(row["area_km2"]) if "area_km2" in sub.columns else geom.area / 1e6
            n_done += 1
            try:
                dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
            except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                    n_done, n_total, ftype, comid, exc,
                )
                continue
            mask = _interior_mask(dem, transform, geom)
            n_interior = int(mask.sum())
            if n_interior == 0:
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: 0 interior cells after masking — skipped",
                    n_done, n_total, ftype, comid,
                )
                continue
            max_depth = lake_max_depth(dem, mask, transform)
            mean_depth = max_to_mean(max_depth, shape="cone")
            rows.append({
                "COMID": comid,
                "FTYPE": ftype,
                "area_km2": area_km2,
                "max_depth_m": max_depth,
                "mean_depth_m": mean_depth,
                "mean_depth_in": mean_depth * 39.3701,
                "n_interior_cells": n_interior,
                "resolution": source["resolution"],
            })
            if n_done % 5 == 0 or n_done == n_total:
                logger.info(
                    "  [%d/%d] FTYPE=%s COMID=%s max_depth=%.2f m mean_depth=%.1f in",
                    n_done, n_total, ftype, comid, max_depth, mean_depth * 39.3701,
                )
    return pd.DataFrame(rows)


def _write_hollister_scatter(result: pd.DataFrame, out_png: Path, logger) -> None:
    """Scatter of max-depth (m) vs polygon area (km^2, log-x) — the visual
    check that Hollister depth scales with lake size in a physically
    plausible way (not e.g. clustered at one degenerate value)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(result["area_km2"], result["max_depth_m"], alpha=0.6, color="#1f77b4", s=20)
    ax.set_xscale("log")
    ax.set_xlabel("polygon area (km^2, log scale)")
    ax.set_ylabel("Hollister max depth (m)")
    ax.set_title(f"Hollister max depth vs polygon area (ND LakePond sample, n={len(result)})")
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote max-depth-vs-area scatter: %s", out_png)


def run_hollister(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path,
    n_per_ftype: int = 300, limit: int | None = None,
) -> pd.DataFrame:
    """Issue #173 Task 6: Hollister/lakeMorpho terrain-slope max-depth
    prototype over an ND LakePond sample, plus the max->mean conversion.

    Reconstructs the CONUS dprst set, picks the same ND 1m project Tasks
    4-5 use (`select_nd_project`), samples up to `n_per_ftype` LakePond
    polygons (`sample_per_ftype`, `HOLLISTER_FTYPES`), reads each at 1 m
    and computes `lake_max_depth` -> `max_to_mean` over the polygon
    interior, and writes `hollister_sample.csv` +
    `hollister_maxdepth_vs_area.png` to `out_dir`.

    max->mean conversion: this spike has no field bathymetry survey for
    these ND potholes to calibrate against, so the conical V/A relation
    (mean = max/3, `max_to_mean(..., "cone")`) is used as the documented,
    best-available assumption — the same simplifying geometry lakeMorpho
    itself uses when no bathymetry is supplied (a cone is a reasonable
    first-order shape for a glacially-scoured prairie pothole: steep sides,
    a deep central low point, no flat bottom shelf). Both the max and the
    derived mean distributions are reported (in inches) so a future task
    with real bathymetry can recalibrate the factor without rerunning this
    read.

    `limit`, if given, caps the number of sampled LakePond polygons actually
    read (head of the per-FTYPE sample) — mirrors `run_freeboard`'s `--limit`,
    for cheap smoke-testing of the read/compute path.
    """
    dprst = load_conus_dprst(base, logger)

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info("Selecting ND 1m project with densest dprst overlap ...")
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    logger.info(
        "Sampling up to %d LakePond dprst polygons from project=%s ...", n_per_ftype, project,
    )
    samples = sample_per_ftype(nd_dprst, n_per_ftype, logger, ftypes=HOLLISTER_FTYPES)
    if limit is not None:
        samples = {ftype: sub.head(limit) for ftype, sub in samples.items()}
    n_total = sum(len(v) for v in samples.values())
    logger.info(
        "Total LakePond sample size: %d%s", n_total,
        f" [--limit {limit}]" if limit is not None else "",
    )
    if n_total == 0:
        raise RuntimeError("0 LakePond polygons in the chosen ND project — nothing to analyze")

    logger.info("Reading each sampled LakePond polygon at 1m + computing Hollister max depth ...")
    result = analyze_hollister_sample(samples, project, logger)
    n_ok = len(result)
    logger.info("Hollister reads completed: %d/%d polygons produced a usable result", n_ok, n_total)
    if result.empty:
        raise RuntimeError("Hollister analysis produced 0 usable polygons")

    max_med = float(result["max_depth_m"].median())
    max_p10 = float(np.percentile(result["max_depth_m"], 10))
    max_p90 = float(np.percentile(result["max_depth_m"], 90))
    mean_med_in = float(result["mean_depth_in"].median())
    logger.info(
        "Hollister max-depth distribution (m) over LakePond sample: median=%.2f, "
        "p10=%.2f, p90=%.2f (n=%d)",
        max_med, max_p10, max_p90, n_ok,
    )
    logger.info(
        "Derived mean-depth (max/3 cone conversion) median: %.1f in (max-depth median "
        "%.2f m = %.1f in)",
        mean_med_in, max_med, max_med * 39.3701,
    )
    plausible = 0.1 <= max_med <= 10.0 and (result["max_depth_m"] >= 0).all()
    if plausible:
        logger.info(
            "*** Hollister finding: max-depth magnitudes are PLAUSIBLE (a few metres, "
            "non-negative) — consistent with prairie-pothole/lake depths, not degenerate ***",
        )
    else:
        logger.warning(
            "*** Hollister finding: max-depth magnitudes look IMPLAUSIBLE (median %.2f m, "
            "or a negative value present) — inspect before trusting this factor ***",
            max_med,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "hollister_sample.csv"
    caveat = (
        f"# Hollister/lakeMorpho terrain-slope max-depth prototype over an ND LakePond "
        f"sample (issue #173 Task 6). Project={project}. n_sampled={n_total}"
        + (f" (--limit {limit})" if limit is not None else "")
        + f", n_usable={n_ok}. max_depth_m = lake_max_depth() (mean shoreline-ring slope "
        "x max in-polygon distance-to-shore, over the polygon interior via _interior_mask). "
        "mean_depth_m = max_to_mean(max_depth_m, shape='cone') = max_depth_m / 3 -- "
        "documented assumption (V/A of a cone), NOT field-calibrated in this spike; no "
        "bathymetry survey was available to fit the factor empirically. Cross-check: NHM's "
        "calibrated dprst_depth_avg median is ~49 in (docs/superpowers/... nhm_dprst_params_"
        "are_calibrated) -- compare against mean_depth_in below, but this spike does not "
        "block on matching it.\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        result.to_csv(f, index=False)
    logger.info("Wrote per-polygon Hollister sample: %s", out_csv)

    _write_hollister_scatter(result, out_dir / "hollister_maxdepth_vs_area.png", logger)

    return result


def main() -> None:
    from gfv2_params.config import load_base_config
    from gfv2_params.log import configure_logging

    parser = argparse.ArgumentParser(description=__doc__)
    # Mutually-exclusive MODE group: exactly one investigation task per run.
    # Task 7 (issue #173) adds --regression here as another
    # mutually_exclusive_group() member — do not go back to a single
    # `required=True` flag (that breaks the moment a second mode is added;
    # see Task 2 review carry-forward note).
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--audit", action="store_true",
                       help="Run the CONUS coverage audit (Task 2, issue #173).")
    mode.add_argument("--flatness", action="store_true",
                       help="Run the ND flatness-detector validation + SwampMarsh "
                            "verdict (Task 4, issue #173).")
    mode.add_argument("--freeboard", action="store_true",
                       help="Quantify freeboard (filled - raw) over Task 4's "
                            "detected-flat dprst polygon sample (Task 5, issue #173). "
                            "Requires flatness_per_polygon.csv already in --out-dir "
                            "(run --flatness first).")
    mode.add_argument("--hollister", action="store_true",
                       help="Hollister/lakeMorpho terrain-slope max-depth prototype + "
                            "max->mean conversion over an ND LakePond sample (Task 6, "
                            "issue #173).")
    parser.add_argument("--fabric", default=None,
                         help="Fabric name (overrides FABRIC env / default_fabric).")
    parser.add_argument(
        "--out-dir", type=Path, required=True,
        help="Output directory (coverage_audit.csv for --audit; "
             "flatness_by_ftype.csv/flatness_separability.png for --flatness; "
             "hollister_sample.csv/hollister_maxdepth_vs_area.png for --hollister). "
             "Also doubles as the WESM.gpkg download cache dir (multi-GB "
             "one-time download, shared by both modes), so pick a path with "
             "enough free space; there is no default.",
    )
    parser.add_argument(
        "--n-per-ftype", type=int, default=300,
        help="--flatness/--hollister only: target sample size per FTYPE (default "
             "300, per issue #173 Task 4; --hollister samples LakePond only). "
             "Uses fewer if a FTYPE has fewer polygons in the chosen project — "
             "never silently capped, always logged.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="--freeboard/--hollister only: cap the number of polygons analyzed "
             "(head of the sample) — for cheap smoke-testing; default: analyze "
             "the full sample (every flat=True polygon for --freeboard, every "
             "sampled LakePond polygon for --hollister).",
    )
    args = parser.parse_args()

    logger = configure_logging("dprst_depth_probe")
    base = load_base_config(fabric=args.fabric)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.audit:
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
    elif args.flatness:
        run_flatness(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            n_per_ftype=args.n_per_ftype,
        )
    elif args.freeboard:
        run_freeboard(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            limit=args.limit,
        )
    elif args.hollister:
        run_hollister(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            n_per_ftype=args.n_per_ftype, limit=args.limit,
        )


if __name__ == "__main__":
    main()
