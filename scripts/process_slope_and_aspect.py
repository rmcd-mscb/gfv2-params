"""
Script to compute slope and aspect rasters from a DEM using richdem.
Reads input_dir and output_dir from a YAML config file.
"""

import argparse
from pathlib import Path
import yaml
import richdem as rd
import rioxarray

def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description="Compute slope and aspect rasters from a DEM using richdem.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    args = parser.parse_args()

    config = load_config(args.config)
    input_dir = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    # Assume DEM filename pattern based on VPU code
    dem_path = input_dir / args.vpu / f"NEDSnapshot_merged_{args.vpu}.tif"
    dem_fixed_path = input_dir / args.vpu / f"NEDSnapshot_merged_fixed_{args.vpu}.tif"
    if not dem_path.exists():
        raise FileNotFoundError(f"DEM not found: {dem_path}")
    # Need to fix nodata attribute of existing NEDSnapshot_merged_{vpu}.tif for slope and aspect processing.
    print("Creating fixed nodata NEDSnapshot")
    da = rioxarray.open_rasterio(dem_path, masked=True).squeeze()
    da_fixed = da.fillna(-9999)
    da_fixed.rio.write_nodata(-9999, inplace=True)
    da_fixed.rio.to_raster(dem_fixed_path)

    print(f"Loading DEM: {dem_path}")
    dem = rd.LoadGDAL(str(dem_path), no_data=-9999)

    # Compute slope (degrees)
    print("Computing slope (degrees)...")
    slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
    slope_out = output_dir / args.vpu / f"NEDSnapshot_merged_slope_{args.vpu}.tif"
    slope_out.parent.mkdir(parents=True, exist_ok=True)
    rd.SaveGDAL(str(slope_out), slope)
    print(f"Slope raster saved to: {slope_out}")

    # Compute aspect
    print("Computing aspect...")
    aspect = rd.TerrainAttribute(dem, attrib="aspect")
    aspect_out = output_dir / args.vpu / f"NEDSnapshot_merged_aspect_{args.vpu}.tif"
    rd.SaveGDAL(str(aspect_out), aspect)
    print(f"Aspect raster saved to: {aspect_out}")

if __name__ == "__main__":
    main()