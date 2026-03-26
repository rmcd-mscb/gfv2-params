"""Create soils and soil_moist_max parameters from raster data."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def process_soils(source_da, nhru_gdf, output_path, source_type, file_prefix, categorical, id_feature, logger):
    """Process categorical soils data: zonal stats -> dominant category -> CSV."""
    data = UserTiffData(
        var="soils", ds=source_da, proj_ds=source_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    zonal_gen = ZonalGen(
        user_data=data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_path, file_prefix=f"{file_prefix}_temp", jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed")

    # Remove temp file
    zg_file = output_path / f"{file_prefix}_temp.csv"
    if zg_file.exists():
        zg_file.unlink()

    # Dominant category per feature
    category_cols = [col for col in stats.columns if str(col) not in ("count",)]
    top_stats = stats.copy()
    top_stats["max_category"] = top_stats[category_cols].idxmax(axis=1)
    result = top_stats[["max_category"]].rename(columns={"max_category": "soils"})
    result.sort_index(inplace=True)

    result_csv = output_path / f"{file_prefix}.csv"
    result.to_csv(result_csv)
    logger.info("Soils parameters saved to: %s", result_csv)


def process_soil_moist_max(source_da, nhru_gdf, output_path, source_type, file_prefix, categorical, id_feature, logger):
    """Process soil_moist_max: zonal mean from pre-built raster."""
    data = UserTiffData(
        var=source_type, ds=source_da, proj_ds=source_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    zonal_gen = ZonalGen(
        user_data=data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_path, file_prefix=f"{file_prefix}_temp", jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed for soil_moist_max")

    mean_stats = stats[["mean"]].rename(columns={"mean": "soil_moist_max"})
    result_csv = output_path / f"{file_prefix}.csv"
    mean_stats.to_csv(result_csv)
    logger.info("soil_moist_max parameters saved to: %s", result_csv)


def main():
    parser = argparse.ArgumentParser(description="Create soils parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID")
    args = parser.parse_args()

    logger = configure_logging("create_soils_params")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load batch polygons
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{args.batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), args.batch_id)

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{args.batch_id:04d}_param"

    # Load source raster (works for both soils and soil_moist_max)
    raster_path = Path(config["source_raster"])
    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    source_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    if source_type == "soils":
        process_soils(source_da, nhru_gdf, output_dir, source_type, file_prefix, categorical, id_feature, logger)
    elif source_type == "soil_moist_max":
        process_soil_moist_max(source_da, nhru_gdf, output_dir, source_type, file_prefix, categorical, id_feature, logger)
    else:
        raise ValueError(f"Unknown source_type: {source_type}")


if __name__ == "__main__":
    main()
