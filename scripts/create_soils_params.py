"""Create soils and soil_moist_max parameters from raster data."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import mult_rasters, resample


def process_soils(source_da, nhru_gdf, output_path, source_type, vpu_label, categorical, id_feature, logger):
    """Process categorical soils data: zonal stats -> dominant category -> CSV."""
    logger.info("Loaded raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    file_prefix = f"base_nhm_{source_type}_{vpu_label}_param_temp"

    data = UserTiffData(
        var="soils",
        ds=source_da,
        proj_ds=source_da.rio.crs,
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
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed")

    # Remove temp file
    zg_file = output_path / f"{file_prefix}.csv"
    if zg_file.exists():
        zg_file.unlink()

    # Dominant category per feature
    category_cols = [col for col in stats.columns if str(col) not in ("count",)]
    top_stats = stats.copy()
    top_stats["max_category"] = top_stats[category_cols].idxmax(axis=1)
    result = top_stats[["max_category"]].rename(columns={"max_category": "soils"})
    result.sort_index(inplace=True)

    result_csv = output_path / f"base_nhm_{source_type}_{vpu_label}_param.csv"
    result.to_csv(result_csv)
    logger.info("Final soils parameters saved to: %s", result_csv)


def process_soil_moist_max(source_dir, nhru_gdf, output_path, source_type, vpu_label, categorical, id_feature, logger):
    """Process soil_moist_max: resample root depth, multiply by AWC, zonal mean."""
    soil_moist_max_rast = source_dir / "soils_litho/soil_moist_max.tif"
    rd_rast = source_dir / "lulc_veg/RootDepth.tif"
    awc_rast = source_dir / "soils_litho/AWC.tif"
    temp_rast = source_dir / "lulc_veg/rd_250_intermediate.tif"
    final_rast = source_dir / "lulc_veg/rd_250_raw.tif"

    if not rd_rast.exists():
        raise FileNotFoundError(f"Root Depth raster not found: {rd_rast}")
    if not awc_rast.exists():
        raise FileNotFoundError(f"AWC raster not found: {awc_rast}")

    if not final_rast.exists():
        resample(str(rd_rast), str(awc_rast), str(temp_rast), str(final_rast))
    if not soil_moist_max_rast.exists():
        mult_rasters(str(final_rast), str(awc_rast), str(soil_moist_max_rast))

    source_da = rioxarray.open_rasterio(soil_moist_max_rast)
    logger.info("Loaded soil_moist_max raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    file_prefix = f"base_nhm_{source_type}_{vpu_label}_param_temp"

    data = UserTiffData(
        var=source_type,
        ds=source_da,
        proj_ds=source_da.rio.crs,
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
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed for soil_moist_max")

    mean_stats = stats[["mean"]].rename(columns={"mean": "soil_moist_max"})
    result_csv = output_path / f"base_nhm_{source_type}_{vpu_label}_param.csv"
    mean_stats.to_csv(result_csv)
    logger.info("Final soil_moist_max parameters saved to: %s", result_csv)


def main():
    parser = argparse.ArgumentParser(description="Create soils parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", default=None, help="VPU code (e.g., 01, 03N). Omit for custom fabrics.")
    args = parser.parse_args()

    logger = configure_logging("create_soils_params")

    config = load_config(Path(args.config), vpu=args.vpu)
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]

    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    gpkg_path = Path(config["target_gpkg"])
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")
    nhru_gdf = gpd.read_file(gpkg_path, layer=target_layer)
    logger.info("Loaded %s layer from %s: %d features", target_layer, gpkg_path, len(nhru_gdf))

    vpu_label = args.vpu if args.vpu else "custom"

    if source_type == "soils":
        raster_path = Path(config["source_raster"])
        if not raster_path.exists():
            raise FileNotFoundError(f"Input raster not found: {raster_path}")
        logger.info("Processing soils data using raster: %s", raster_path)
        source_da = rioxarray.open_rasterio(raster_path)
        process_soils(source_da, nhru_gdf, output_dir, source_type, vpu_label, categorical, id_feature, logger)

    elif source_type == "soil_moist_max":
        source_dir = Path(config["source_dir"])
        logger.info("Processing soil_moist_max data from: %s", source_dir)
        process_soil_moist_max(source_dir, nhru_gdf, output_dir, source_type, vpu_label, categorical, id_feature, logger)

    else:
        raise ValueError(f"Unknown source_type: {source_type}")


if __name__ == "__main__":
    main()
