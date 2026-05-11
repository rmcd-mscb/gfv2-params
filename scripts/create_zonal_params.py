"""Create zonal parameters (elevation, slope, aspect) from rasters by HRU polygon."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen
from osgeo import gdal, osr

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging

# Opt into the GDAL 4.0 default behaviour of raising Python exceptions
# instead of returning C-style error codes. Silences the FutureWarning
# emitted by osgeo when neither UseExceptions/DontUseExceptions is set.
# NB: GDAL state is process-global — importing this module from a notebook
# or test harness will flip exception handling on for the whole process.
# That is the desired behaviour (GDAL 4.0's default) and what the slurm
# batches expect, but worth knowing if anyone embeds this script elsewhere.
gdal.UseExceptions()
osr.UseExceptions()


def main():
    parser = argparse.ArgumentParser(description="Create zonal parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID (from SLURM_ARRAY_TASK_ID)")
    args = parser.parse_args()

    logger = configure_logging("create_zonal_params")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    # Resolve paths
    raster_path = Path(config["source_raster"])
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{args.batch_id:04d}.gpkg"
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")

    logger.info("Raster: %s", raster_path)
    logger.info("Batch GPKG: %s", batch_gpkg)

    # Load batch polygons
    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), args.batch_id)

    # Load raster
    ned_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", ned_da.shape, ned_da.rio.crs)

    # Build file prefix for output
    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{args.batch_id:04d}_param"

    # Create zonal stats (gdptools 0.3.13+ keyword names)
    data = UserTiffData(
        source_var=source_type,
        source_ds=ned_da,
        source_crs=ned_da.rio.crs,
        source_x_coord="x",
        source_y_coord="y",
        band=1,
        bname="band",
        target_gdf=nhru_gdf,
        target_id=id_feature,
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
