"""
Script to process soils parameters from a source raster using a geopackage.
Processes two source types:
  - "soils" uses a fixed raster (e.g. TEXT_PRMS.tif)
  - "soil_moist_max" performs extra steps to generate a soil moisture max raster.
calculates zonal statistics via gdptools. Results are written out as CSV files.
"""

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

# Add the src directory to the Python path for helper function imports
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(src_path))
from helpers import load_config, mult_rasters, resample  # noqa: E402


def process_soils(
    source_da, nhru_gdf, output_path, source_type, vpu, categorical, id_feature="hru_id"
):
    """
    Process soils data (source_type "soils").
    Opens a fixed raster, creates a UserTiffData object, calculates zonal statistics,
    extracts the dominant category, and writes the result as a CSV.
    """
    print(f"Loaded raster: shape={source_da.shape}, crs={source_da.rio.crs}")

    # Prepare metadata for the zonal statistics object
    tx_name = "x"
    ty_name = "y"
    band = 1
    bname = "band"
    crs = source_da.rio.crs  # use the geometric CRS object as is
    varname = "soils"
    file_prefix = f"base_nhm_{source_type}_{vpu}_param_temp"

    # Create the UserTiffData object for zonal stats calculation
    data = UserTiffData(
        var=varname,
        ds=source_da,
        proj_ds=crs,
        x_coord=tx_name,
        y_coord=ty_name,
        band=band,
        bname=bname,
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="parallel",
        zonal_writer="csv",
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    print("Zonal statistics computed:")
    print(stats)

    # Remove temporary file created by ZonalGen (if it exists)
    zg_file = output_path / f"{file_prefix}.csv"
    if zg_file.exists():
        zg_file.unlink()

    # For categorical soils data, compute the dominant category for each feature
    category_cols = [col for col in stats.columns if str(col) not in ("count")]
    top_stats = stats.copy()
    top_stats["max_category"] = top_stats[category_cols].idxmax(axis=1)
    result = top_stats[["max_category"]].rename(columns={"max_category": "soils"})
    result.sort_index(inplace=True)

    result_csv = output_path / f"base_nhm_{source_type}_{vpu}_param.csv"
    result.to_csv(result_csv)
    print(f"Final soils parameters saved to: {result_csv}")


def process_soil_moist_max(
    base_source_dir,
    nhru_gdf,
    output_path,
    source_type,
    vpu,
    categorical,
    id_feature="hru_id",
):
    """
    Process soil_moist_max data (source_type "soil_moist_max").
    Resamples and combines input rasters to create the soil_moist_max raster,
    calculates zonal statistics, and writes out the mean value per feature.
    """
    # Define input raster paths
    soil_moist_max_rast = base_source_dir / "soils_litho/soil_moist_max.tif"
    rd_rast = base_source_dir / "lulc_veg/RootDepth.tif"
    awc_rast = base_source_dir / "soils_litho/AWC.tif"
    temp_rast = base_source_dir / "lulc_veg/rd_250_raw.tif"
    final_rast = (
        base_source_dir / "lulc_veg/rd_250_raw.tif"
    )  # assuming this is the intended final raster

    # Validate input datasets
    if not rd_rast.exists():
        raise FileNotFoundError(f"Root Depth raster not found: {rd_rast}")
    if not awc_rast.exists():
        raise FileNotFoundError(f"AWC raster not found: {awc_rast}")

    # Create soil_moist_max raster if not already generated
    if not final_rast.exists():
        resample(rd_rast, awc_rast, temp_rast, final_rast)
    if not soil_moist_max_rast.exists():
        mult_rasters(final_rast, awc_rast, soil_moist_max_rast)

    # Open the generated soil_moist_max raster
    source_da = rioxarray.open_rasterio(soil_moist_max_rast)
    if not soil_moist_max_rast.exists():
        raise FileNotFoundError(
            f"Soil_moist_max raster not found: {soil_moist_max_rast}"
        )

    print(
        f"Loaded soil_moist_max raster: shape={source_da.shape}, crs={source_da.rio.crs}"
    )

    # Prepare metadata for zonal stats
    tx_name = "x"
    ty_name = "y"
    band = 1
    bname = "band"
    crs = source_da.rio.crs
    varname = source_type
    file_prefix = f"base_nhm_{source_type}_{vpu}_param_temp"

    data = UserTiffData(
        var=varname,
        ds=source_da,
        proj_ds=crs,
        x_coord=tx_name,
        y_coord=ty_name,
        band=band,
        bname=bname,
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="parallel",
        zonal_writer="csv",
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    print("Zonal statistics computed for soil_moist_max:")
    print(stats)

    # Clean up temporary file
    zg_file = output_path / f"{file_prefix}.csv"
    if zg_file.exists():
        zg_file.unlink()

    # For continuous data, use the 'mean' statistic
    mean_stats = stats[["mean"]].rename(columns={"mean": "soil_moist_max"})
    result_csv = output_path / f"base_nhm_{source_type}_{vpu}_param.csv"
    mean_stats.to_csv(result_csv)
    print(f"Final soil_moist_max parameters saved to: {result_csv}")


def main():
    parser = argparse.ArgumentParser(
        description="Create soils parameters from source raster data."
    )
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument(
        "--vpu", required=True, help="VPU code, e.g., 01, 03N, 03S, 03W, 10U, 10L"
    )
    args = parser.parse_args()

    # Load the configuration and set up directories
    config = load_config(args.config)
    base_source_dir = Path(config["base_source_dir"])
    target_source_dir = Path(config["target_source_dir"])
    output_dir = Path(config["output_dir"])

    # Get processing parameters from the config
    source_type = config.get("source_type")
    categorical = config.get("categorical", False)

    # Setup output subdirectory for this source type
    output_path = output_dir / source_type
    output_path.mkdir(parents=True, exist_ok=True)

    # Define target geopackage path and load the 'nhru' layer with geopandas
    gpkg_path = target_source_dir / f"NHM_{args.vpu}_draft.gpkg"
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")
    nhru_gdf = gpd.read_file(gpkg_path, layer="nhru")
    print(f"Loaded nhru layer from {gpkg_path}: {len(nhru_gdf)} features")

    # Process based on the source type specified in the config
    if source_type == "soils":
        raster_path = base_source_dir / "soils_litho/TEXT_PRMS.tif"
        if not raster_path.exists():
            raise FileNotFoundError(f"Input raster not found: {raster_path}")

        print(f"Processing soils data using raster: {raster_path}")
        # Open the raster with rioxarray
        source_da = rioxarray.open_rasterio(raster_path)
        process_soils(
            source_da, nhru_gdf, output_path, source_type, args.vpu, categorical
        )

    elif source_type == "soil_moist_max":
        print("Processing soil_moist_max data...")
        process_soil_moist_max(
            base_source_dir, nhru_gdf, output_path, source_type, args.vpu, categorical
        )
    else:
        raise ValueError(f"Unknown source_type: {source_type}")


if __name__ == "__main__":
    main()
