"""Create LULC parameters from raster data via crosswalk.

Computes per-HRU: cov_type, srain_intcp, wrain_intcp, snow_intcp,
covden_sum, covden_win from a categorical LULC raster and a continuous
canopy density raster, using a crosswalk CSV to map LULC classes to
NHM parameters.

When a ``keep_raster`` is configured (FORE-SCE), a raster-derived
retention mean is computed via zonal stats and included in the output.
When no ``keep_raster`` is present (NLCD, NALCMS), per-HRU retention
is synthesised from the crosswalk's ``evergreen_retention`` column as
a weighted average across LULC classes.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.lulc import (
    assign_cov_type,
    class_percentages_from_histogram,
    compute_covden,
    compute_interception,
    compute_retention,
    load_crosswalk,
)


def main():
    parser = argparse.ArgumentParser(description="Create LULC parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to LULC step config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID")
    args = parser.parse_args()

    logger = configure_logging("create_lulc_params")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    source_type = config["source_type"]
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

    # Load crosswalk
    # crosswalk_file may be relative to repo root or absolute
    crosswalk_path = Path(config["crosswalk_file"])
    if not crosswalk_path.is_absolute():
        crosswalk_path = Path(__file__).resolve().parent.parent / crosswalk_path
    crosswalk = load_crosswalk(crosswalk_path)
    logger.info("Loaded crosswalk: %d classes", len(crosswalk))

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{args.batch_id:04d}_param"

    # --- Step 1: Categorical zonal stats on LULC raster ---
    lulc_path = Path(config["source_raster"])
    if not lulc_path.exists():
        raise FileNotFoundError(f"LULC raster not found: {lulc_path}")
    lulc_da = rioxarray.open_rasterio(lulc_path, masked=True)
    logger.info("Loaded LULC raster: shape=%s", lulc_da.shape)

    lulc_data = UserTiffData(
        var="lulc", ds=lulc_da, proj_ds=lulc_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    lulc_zonal = ZonalGen(
        user_data=lulc_data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_dir, file_prefix=f"{file_prefix}_lulc_temp", jobs=4,
    )
    histogram = lulc_zonal.calculate_zonal(categorical=True)
    logger.info("LULC categorical zonal stats computed")

    # Clean temp file
    temp_csv = output_dir / f"{file_prefix}_lulc_temp.csv"
    if temp_csv.exists():
        temp_csv.unlink()

    # Convert histogram to class percentages
    class_perc = class_percentages_from_histogram(histogram)
    # Rename the id column to match id_feature
    id_name = histogram.index.name or "id"
    if id_name != id_feature:
        class_perc = class_perc.rename(columns={id_name: id_feature})
    logger.info("Class percentages computed for %d HRUs", class_perc[id_feature].nunique())

    # --- Step 2: Continuous zonal stats on canopy raster ---
    cnpy_path = Path(config["canopy_raster"])
    if not cnpy_path.exists():
        raise FileNotFoundError(f"Canopy raster not found: {cnpy_path}")
    cnpy_da = rioxarray.open_rasterio(cnpy_path, masked=True)
    logger.info("Loaded canopy raster: shape=%s", cnpy_da.shape)

    cnpy_data = UserTiffData(
        var="canopy", ds=cnpy_da, proj_ds=cnpy_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    cnpy_zonal = ZonalGen(
        user_data=cnpy_data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_dir, file_prefix=f"{file_prefix}_cnpy_temp", jobs=4,
    )
    cnpy_stats = cnpy_zonal.calculate_zonal(categorical=False)
    logger.info("Canopy continuous zonal stats computed")

    temp_csv = output_dir / f"{file_prefix}_cnpy_temp.csv"
    if temp_csv.exists():
        temp_csv.unlink()

    canopy_mean_df = cnpy_stats[["mean"]].rename(columns={"mean": "canopy_mean"})
    canopy_mean_df.index.name = id_feature
    canopy_mean_df = canopy_mean_df.reset_index()

    # --- Step 3: Retention (raster-based or crosswalk-based) ---
    keep_raster_str = config.get("keep_raster")
    if keep_raster_str:
        # FORE-SCE path: compute retention from keep raster via zonal mean
        keep_path = Path(keep_raster_str)
        if not keep_path.exists():
            raise FileNotFoundError(f"Keep raster not found: {keep_path}")
        keep_da = rioxarray.open_rasterio(keep_path, masked=True)
        logger.info("Loaded keep raster: shape=%s", keep_da.shape)

        keep_data = UserTiffData(
            var="keep", ds=keep_da, proj_ds=keep_da.rio.crs,
            x_coord="x", y_coord="y", band=1, bname="band",
            f_feature=nhru_gdf, id_feature=id_feature,
        )
        keep_zonal = ZonalGen(
            user_data=keep_data, zonal_engine="exactextract", zonal_writer="csv",
            out_path=output_dir, file_prefix=f"{file_prefix}_keep_temp", jobs=4,
        )
        keep_stats = keep_zonal.calculate_zonal(categorical=False)
        logger.info("Keep raster zonal stats computed")

        temp_csv = output_dir / f"{file_prefix}_keep_temp.csv"
        if temp_csv.exists():
            temp_csv.unlink()

        # Keep raster values are 0-100; normalise to 0-1
        retention_df = keep_stats[["mean"]].rename(columns={"mean": "retention"})
        retention_df["retention"] = retention_df["retention"] * 0.01
        retention_df.index.name = id_feature
        retention_df = retention_df.reset_index()
        logger.info("Retention computed from keep raster (raster-based)")
    else:
        # NLCD / NALCMS path: synthesise retention from crosswalk column
        retention_df = compute_retention(class_perc, crosswalk, id_col=id_feature)
        logger.info("Retention computed from crosswalk evergreen_retention (crosswalk-based)")

    # --- Step 4: Compute parameters ---
    cov_type_df = assign_cov_type(class_perc, crosswalk, id_col=id_feature)
    logger.info("Cover types assigned")

    intcp_df = compute_interception(class_perc, crosswalk, id_col=id_feature)
    logger.info("Interception parameters computed")

    covden_df = compute_covden(class_perc, crosswalk, canopy_mean_df, id_col=id_feature)
    logger.info("Cover density parameters computed")

    # --- Step 5: Merge and write ---
    result = (
        cov_type_df
        .merge(intcp_df, on=id_feature)
        .merge(covden_df, on=id_feature)
        .merge(retention_df, on=id_feature)
    )
    result = result.sort_values(id_feature).set_index(id_feature)

    result_csv = output_dir / f"{file_prefix}.csv"
    result.to_csv(result_csv)
    logger.info("LULC parameters saved to: %s (%d HRUs)", result_csv, len(result))


if __name__ == "__main__":
    main()
