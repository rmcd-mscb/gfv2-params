"""Create zonal parameters (elevation, slope, aspect) from rasters by HRU polygon."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Create zonal parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", default=None, help="VPU code (e.g., 01, 03N). Omit for custom fabrics.")
    args = parser.parse_args()

    logger = configure_logging("create_zonal_params")

    config = load_config(Path(args.config), vpu=args.vpu)
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]

    # Resolve paths
    raster_path = Path(config["source_raster"])
    gpkg_path = Path(config["target_gpkg"])
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")

    logger.info("Raster: %s", raster_path)
    logger.info("GPKG: %s", gpkg_path)

    # Load target polygons
    nhru_gdf = gpd.read_file(gpkg_path, layer=target_layer)
    logger.info("Loaded %s layer: %d features", target_layer, len(nhru_gdf))

    # Load raster
    ned_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", ned_da.shape, ned_da.rio.crs)

    # Build file prefix for output
    vpu_label = args.vpu if args.vpu else "custom"
    file_prefix = f"base_nhm_{source_type}_{vpu_label}_param"

    # Create zonal stats
    data = UserTiffData(
        var=source_type,
        ds=ned_da,
        proj_ds=ned_da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics complete. Shape: %s", stats.shape)


if __name__ == "__main__":
    main()
