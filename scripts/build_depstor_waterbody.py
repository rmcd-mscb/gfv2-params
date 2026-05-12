"""Build the water-body binary raster and its connected-component region labels.

Reads the staged water-body polygon layer, filters by minimum area, rasterizes
to the elevation-VRT template grid, then runs a connected-component label step
(8-connectivity) to give each contiguous water body a unique region ID.

Outputs:
- {fabric}/depstor_rasters/wbody_binary.tif  (uint8 0/1, 255 nodata)
- {fabric}/depstor_rasters/wbody_regions.tif (int32 region IDs, 0 nodata)

Logic source: depstor/scripts/DepStor.py:580-663 — `wbe.clump(diag=True)`
replaced by `scipy.ndimage.label` with 3x3 structure.
"""

import argparse
import time
from pathlib import Path

import geopandas as gpd
from rasterio.warp import transform_bounds

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import (
    RasterInfo,
    clump_regions,
    rasterize_binary,
    write_int32_regions,
    write_uint8_binary,
)
from gfv2_params.log import configure_logging

# Defensive buffer (m) added to the template bounds when filtering the
# waterbody load. OGR's bbox spatial filter uses each polygon's envelope, so
# polygons crossing the template boundary will already be included — the
# buffer is insurance against floating-point rounding at the boundary and
# small envelope/extent mismatches from CRS reprojection at file scope.
# 1 km ≈ 33 cells at 30 m: generous against fp slop, negligible vs the load
# time saved by spatial-index push-down.
WATERBODY_LOAD_BBOX_BUFFER_M = 1000.0


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _load_waterbodies(path: Path, layer: str | None, info: RasterInfo, logger):
    """Load waterbody polygons whose envelope intersects the template extent.

    Pushes the bbox filter down to OGR's spatial index (R-tree in the
    geopackage) so only polygons near the template are read. For the
    448k-feature CONUS waterbodies file on a VPU-scale template this drops
    load time from ~30 s to <1 s; on a full-CONUS template it's a no-op
    (bbox encloses the full extent).

    The bbox must be in the file's CRS. We peek at the file CRS via a
    one-row read, then project the buffered template bounds with
    `transform_bounds` (no-op when CRSes match — the production case today).
    """
    buf = WATERBODY_LOAD_BBOX_BUFFER_M
    b = info.bounds
    buffered = (b.left - buf, b.bottom - buf, b.right + buf, b.top + buf)

    file_crs = gpd.read_file(path, layer=layer, rows=1).crs
    bbox = transform_bounds(info.crs, file_crs, *buffered, densify_pts=21)
    logger.info(
        "  Waterbody load bbox (file CRS %s, buffer %.0f m): %s",
        file_crs, buf, tuple(round(v, 1) for v in bbox),
    )

    try:
        return gpd.read_file(path, layer=layer, bbox=bbox, use_arrow=True)
    except Exception:
        logger.warning(
            "PyArrow unavailable for vector load; falling back to fiona."
        )
        return gpd.read_file(path, layer=layer, bbox=bbox)


def main():
    parser = argparse.ArgumentParser(description="Build depstor wbody_binary.tif and wbody_regions.tif.")
    parser.add_argument("--config", required=True, help="Path to depstor_waterbody_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_waterbody")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    template_path = Path(require_config_key(config, "template_raster", "build_depstor_waterbody"))
    waterbody_gpkg = Path(require_config_key(config, "waterbody_gpkg", "build_depstor_waterbody"))
    waterbody_layer = require_config_key(config, "waterbody_layer", "build_depstor_waterbody")
    binary_path = Path(config["wbody_binary_raster"])
    regions_path = Path(config["wbody_regions_raster"])
    min_area = float(config.get("min_area_threshold", 900.0))

    if not template_path.exists():
        raise FileNotFoundError(f"Template raster not found: {template_path}")
    if not waterbody_gpkg.exists():
        raise FileNotFoundError(f"Waterbody gpkg not found: {waterbody_gpkg}")

    logger.info("=== build_depstor_waterbody ===")
    logger.info("Template     : %s", template_path)
    logger.info("Waterbody gpkg: %s (layer=%s)", waterbody_gpkg, waterbody_layer)
    logger.info("Binary out   : %s", binary_path)
    logger.info("Regions out  : %s", regions_path)
    logger.info("Min area     : %.1f m^2", min_area)

    if binary_path.exists() and regions_path.exists() and not args.force:
        logger.info("Both outputs already exist — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    logger.info("Template grid: %dx%d, CRS=%s", info.width, info.height, info.crs)

    logger.info("--- Step 1/4: Load and filter water bodies ---")
    t1 = time.time()
    wb_gdf = _load_waterbodies(waterbody_gpkg, waterbody_layer, info, logger)
    if wb_gdf.crs != info.crs:
        logger.info("  Reprojecting wbodies from %s to %s", wb_gdf.crs, info.crs)
        wb_gdf = wb_gdf.to_crs(info.crs)
    wb_gdf = wb_gdf[wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty]
    n_before = len(wb_gdf)
    wb_gdf = wb_gdf[wb_gdf.geometry.area >= min_area].copy()
    logger.info(
        "  Loaded %d wbodies, kept %d after >= %.1f m^2 filter, in %s",
        n_before, len(wb_gdf), min_area, _elapsed(t1),
    )

    logger.info("--- Step 2/4: Rasterize wbody polygons (binary) ---")
    t2 = time.time()
    binary = rasterize_binary(wb_gdf, info, all_touched=False)
    n_in = int((binary == 1).sum())
    logger.info("  Rasterized in %s | %d wbody cells", _elapsed(t2), n_in)

    logger.info("--- Step 3/4: Write binary raster ---")
    t3 = time.time()
    write_uint8_binary(binary, info, binary_path)
    logger.info("  Binary written in %s: %s", _elapsed(t3), binary_path)

    logger.info("--- Step 4/4: Connected-component label (scipy.ndimage.label, 8-connectivity) ---")
    t4 = time.time()
    regions = clump_regions(binary)
    n_regions = int(regions.max())
    logger.info("  Labeled %d connected components in %s", n_regions, _elapsed(t4))
    write_int32_regions(regions, info, regions_path)
    logger.info("  Regions written: %s", regions_path)

    logger.info("=== build_depstor_waterbody complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
