"""Build the land-mask raster for the depression-storage pipeline.

Rasterizes the HRU polygon fabric (the authoritative modeling domain) onto the
template grid, producing land_mask.tif — a uint8 binary raster where 1 = inside
the HRU fabric (land), 255 = outside (ocean / off-domain).

Every other depstor builder masks its output against this raster. It replaces
the earlier template-DEM-nodata mask, which was unreliable: the hydro-
conditioned DEM carries valid (often garbage) elevations over coastal ocean, so
its nodata footprint bulged into the sea and those bulges leaked into the dense
outputs (perv_binary, carea_map). The HRU fabric follows the real coastline and
is exactly the domain the depstor params are aggregated to downstream.

Output: {fabric}/depstor_rasters/land_mask.tif
"""

import argparse
import time
from pathlib import Path

import geopandas as gpd

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import RasterInfo, rasterize_binary, write_uint8_binary
from gfv2_params.log import configure_logging


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _load_hru(path: Path, layer: str, logger):
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except Exception:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer)


def main():
    parser = argparse.ArgumentParser(description="Build depstor land_mask.tif from the HRU fabric.")
    parser.add_argument("--config", required=True, help="Path to depstor_landmask_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_landmask")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    template_path = Path(require_config_key(config, "template_raster", "build_depstor_landmask"))
    hru_gpkg = Path(require_config_key(config, "hru_gpkg", "build_depstor_landmask"))
    hru_layer = require_config_key(config, "hru_layer", "build_depstor_landmask")
    output_path = Path(config["output_raster"])

    if not template_path.exists():
        raise FileNotFoundError(f"Template raster not found: {template_path}")
    if not hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {hru_gpkg}")

    logger.info("=== build_depstor_landmask ===")
    logger.info("Template  : %s", template_path)
    logger.info("HRU fabric: %s (layer=%s)", hru_gpkg, hru_layer)
    logger.info("Output    : %s", output_path)

    if output_path.exists() and not args.force:
        logger.info("Output already exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    logger.info("Template grid: %dx%d, CRS=%s", info.width, info.height, info.crs)

    logger.info("--- Step 1/2: Load HRU fabric ---")
    t1 = time.time()
    hru_gdf = _load_hru(hru_gpkg, hru_layer, logger)
    hru_gdf = hru_gdf[hru_gdf.geometry.notna() & ~hru_gdf.geometry.is_empty]
    logger.info("  Loaded %d HRU polygons in %s", len(hru_gdf), _elapsed(t1))

    logger.info("--- Step 2/2: Rasterize HRU fabric to land mask ---")
    t2 = time.time()
    # all_touched=True: be inclusive at the outer coastline so thin edge HRUs
    # are not clipped. create_zonal_params is the precise arbiter downstream.
    binary = rasterize_binary(hru_gdf, info, all_touched=True)
    write_uint8_binary(binary, info, output_path)
    n_land = int((binary == 1).sum())
    logger.info(
        "  Rasterize + write done in %s | %d land cells (%.2f%% of grid)",
        _elapsed(t2), n_land, 100 * n_land / binary.size,
    )

    logger.info("=== build_depstor_landmask complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
