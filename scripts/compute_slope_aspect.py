"""Compute slope and aspect rasters from a DEM using richdem."""

import argparse
from pathlib import Path

import richdem as rd
import rioxarray

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Compute slope and aspect rasters from DEM.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    args = parser.parse_args()

    logger = configure_logging("compute_slope_aspect")

    config = load_config(Path(args.config), vpu=args.vpu)
    input_dir = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    dem_path = input_dir / args.vpu / f"NEDSnapshot_merged_{args.vpu}.tif"
    dem_fixed_path = input_dir / args.vpu / f"NEDSnapshot_merged_fixed_{args.vpu}.tif"
    if not dem_path.exists():
        raise FileNotFoundError(f"DEM not found: {dem_path}")

    logger.info("Creating fixed nodata NEDSnapshot")
    da = rioxarray.open_rasterio(dem_path, masked=True).squeeze()
    da_fixed = da.fillna(-9999)
    da_fixed.rio.write_nodata(-9999, inplace=True)
    da_fixed.rio.to_raster(dem_fixed_path)

    logger.info("Loading DEM: %s", dem_path)
    dem = rd.LoadGDAL(str(dem_path), no_data=-9999)

    logger.info("Computing slope (degrees)...")
    slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
    slope_out = output_dir / args.vpu / f"NEDSnapshot_merged_slope_{args.vpu}.tif"
    slope_out.parent.mkdir(parents=True, exist_ok=True)
    rd.SaveGDAL(str(slope_out), slope)
    logger.info("Slope raster saved to: %s", slope_out)

    logger.info("Computing aspect...")
    aspect = rd.TerrainAttribute(dem, attrib="aspect")
    aspect_out = output_dir / args.vpu / f"NEDSnapshot_merged_aspect_{args.vpu}.tif"
    rd.SaveGDAL(str(aspect_out), aspect)
    logger.info("Aspect raster saved to: %s", aspect_out)


if __name__ == "__main__":
    main()
