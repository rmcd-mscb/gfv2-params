"""
Script to process NEDSnapshot_merged_fixed_{vpu}.tif files by VPU.
Handles special cases for 03N, 03S, 03W.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
import yaml
from gdptools import UserTiffData, ZonalGen


def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description="Process NEDSnapshot_merged_fixed_{vpu}.tif files by VPU.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01, 03N, 03S, 03W")
    args = parser.parse_args()

    config = load_config(args.config)
    base_source_dir = Path(config["base_source_dir"])
    target_source_dir = Path(config["target_source_dir"])
    output_dir = Path(config["output_dir"])

    source_type = config.get("source_type")
    categorical = config.get("categorical", False)
    output_path = output_dir / source_type
    output_path.mkdir(parents=True, exist_ok=True)

    # Handle special cases for 03N, 03S, 03W, 10U, 10L
    if args.vpu in {"03N", "03S", "03W"}:
        raster_vpu = "03"
        gpkg_vpu = args.vpu
    elif args.vpu in {"10U", "10L"}:
        raster_vpu = "10"
        gpkg_vpu = args.vpu
    else:
        raster_vpu = args.vpu
        gpkg_vpu = args.vpu

    # Input raster path
    if source_type == "slope":
        ned_path = base_source_dir / raster_vpu / f"NEDSnapshot_merged_slope_{raster_vpu}.tif"
    elif source_type == "aspect":
        ned_path = base_source_dir / raster_vpu / f"NEDSnapshot_merged_aspect_{raster_vpu}.tif"
    elif source_type == "elevation":
        ned_path = base_source_dir / raster_vpu / f"NEDSnapshot_merged_fixed_{raster_vpu}.tif"
    else:
        raise ValueError(f"Unknown source type: {source_type}")
    if not ned_path.exists():
        raise FileNotFoundError(f"Input raster not found: {ned_path}")

    # Target geopackage path
    gpkg_path = target_source_dir / f"NHM_{gpkg_vpu}_draft.gpkg"
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")

    print(f"Raster: {ned_path}")
    print(f"GPKG:   {gpkg_path}")

    # Open the 'nhru' layer from the geopackage with geopandas
    nhru_gdf = gpd.read_file(gpkg_path, layer="nhru")
    print(f"Loaded nhru layer: {len(nhru_gdf)} features")

    # Open the raster with rioxarray
    ned_da = rioxarray.open_rasterio(ned_path, masked=True)
    print(f"Loaded raster: shape={ned_da.shape}, crs={ned_da.rio.crs}")

    # Prepare to create UserTiffData object
    tx_name = 'x'
    ty_name = 'y'
    band = 1
    bname = 'band'
    crs = ned_da.rio.crs.to_epsg()  # or use the EPSG code you expect, e.g., 5070
    varname = "elev"  # not currently used
    id_feature = "hru_id"  # or your HRU ID field

    # Create UserTiffData object
    data = UserTiffData(
        var=varname,
        ds=ned_da,
        proj_ds=crs,
        x_coord=tx_name,
        y_coord=ty_name,
        band=band,
        bname=bname,
        f_feature=nhru_gdf,
        id_feature=id_feature
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="parallel",
        zonal_writer="csv",
        out_path=output_path,
        file_prefix=f"base_nhm_{source_type}_{args.vpu}_param",
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    print(stats)
    # Placeholder for further processing logic

if __name__ == "__main__":
    main()
