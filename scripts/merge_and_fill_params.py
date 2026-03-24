"""Merge VPU geopackages and fill missing parameter values using KNN interpolation."""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from gfv2_params.config import VPUS_DETAILED, load_base_config
from gfv2_params.log import configure_logging


def merge_vpu_geopackages(targets_dir, vpus, output_file, simplify_tolerance, logger):
    logger.info("Merging VPU geopackages...")
    merged_gdfs = []

    for vpu in tqdm(vpus, desc="Merging VPU geopackages"):
        gpkg_file = targets_dir / f"NHM_{vpu}_draft.gpkg"
        if not gpkg_file.exists():
            logger.warning("%s not found, skipping...", gpkg_file)
            continue

        gdf = gpd.read_file(gpkg_file, layer="nhru")

        if simplify_tolerance > 0:
            gdf["geometry"] = gdf.apply(
                lambda row: row["geometry"].simplify(tolerance=simplify_tolerance, preserve_topology=True)
                if row["geometry"].area > simplify_tolerance * 10
                else row["geometry"],
                axis=1,
            )

        merged_gdfs.append(gdf)
        logger.debug("Added %d features from VPU %s", len(gdf), vpu)

    merged_gdf = pd.concat(merged_gdfs, ignore_index=True)
    merged_gdf = merged_gdf.sort_values("nat_hru_id").reset_index(drop=True)

    logger.info("Saving merged geopackage with %d features to: %s", len(merged_gdf), output_file)
    merged_gdf.to_file(output_file, driver="GPKG", layer="nhru")
    return merged_gdf


def find_missing_ids(param_file, expected_max, logger):
    logger.info("Finding missing nat_hru_id values...")
    param_df = pd.read_csv(param_file)
    existing_ids = set(param_df["nat_hru_id"])
    expected_ids = set(range(1, expected_max + 1))
    missing_ids = sorted(expected_ids - existing_ids)
    logger.info("Found %d missing nat_hru_id values out of %d", len(missing_ids), expected_max)
    return param_df, missing_ids


def fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_column, k, logger):
    logger.info("Filling missing values using KNN interpolation (k=%d)...", k)

    if not missing_ids:
        logger.info("No missing values to fill!")
        return param_df

    merged_gdf["centroid"] = merged_gdf["geometry"].centroid
    merged_gdf["x"] = merged_gdf["centroid"].x
    merged_gdf["y"] = merged_gdf["centroid"].y

    existing_df = param_df.merge(merged_gdf[["nat_hru_id", "x", "y"]], on="nat_hru_id", how="left")
    missing_df = merged_gdf[merged_gdf["nat_hru_id"].isin(missing_ids)][["nat_hru_id", "x", "y", "vpu"]]

    existing_coords = existing_df[["x", "y"]].values
    missing_coords = missing_df[["x", "y"]].values
    existing_values = existing_df[param_column].values

    knn = NearestNeighbors(n_neighbors=k)
    knn.fit(existing_coords)
    distances, indices = knn.kneighbors(missing_coords)

    interpolated_values = []
    for neighbor_indices in tqdm(indices, desc="Filling missing HRUs"):
        neighbor_values = existing_values[neighbor_indices]
        interpolated_values.append(np.mean(neighbor_values))

    missing_filled = pd.DataFrame({
        "nat_hru_id": missing_df["nat_hru_id"].values,
        param_column: interpolated_values,
        "vpu": missing_df["vpu"].values,
    })
    missing_filled["hru_id"] = missing_filled["nat_hru_id"]

    complete_df = pd.concat([param_df, missing_filled], ignore_index=True)
    complete_df = complete_df.sort_values("nat_hru_id").reset_index(drop=True)
    logger.info("Filled %d missing values", len(missing_ids))
    return complete_df


def main():
    parser = argparse.ArgumentParser(description="Merge VPU geopackages and fill missing parameter values.")
    parser.add_argument("--targets_dir", default=None)
    parser.add_argument("--param_file", default=None)
    parser.add_argument("--output_dir", default=None)
    parser.add_argument("--simplify_tolerance", type=float, default=100)
    parser.add_argument("--k_neighbors", type=int, default=1)
    parser.add_argument("--force_rebuild", action="store_true")
    args = parser.parse_args()

    logger = configure_logging("merge_and_fill_params")

    # Load base config for defaults after argparse (so --help works without config)
    base = load_base_config()
    data_root = base["data_root"]
    expected_max = base["expected_max_hru_id"]

    if args.targets_dir is None:
        args.targets_dir = f"{data_root}/targets"
    if args.param_file is None:
        args.param_file = f"{data_root}/nhm_params/nhm_params_merged/nhm_ssflux_params.csv"
    if args.output_dir is None:
        args.output_dir = f"{data_root}/nhm_params/nhm_params_merged"

    targets_dir = Path(args.targets_dir)
    param_file = Path(args.param_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    merged_gpkg = targets_dir / "gfv2_merged_simplified.gpkg"
    filled_param_file = output_dir / f"filled_{param_file.name}"

    if merged_gpkg.exists() and not args.force_rebuild:
        logger.info("Loading existing merged geopackage: %s", merged_gpkg)
        merged_gdf = gpd.read_file(merged_gpkg, layer="nhru")
        logger.info("Loaded %d features", len(merged_gdf))
    else:
        merged_gdf = merge_vpu_geopackages(targets_dir, VPUS_DETAILED, merged_gpkg, args.simplify_tolerance, logger)

    param_df, missing_ids = find_missing_ids(param_file, expected_max, logger)

    if missing_ids:
        param_columns = [col for col in param_df.columns if col not in ["hru_id", "nat_hru_id", "vpu"]]
        if not param_columns:
            logger.error("No parameter column found in the data")
            return

        param_column = param_columns[0]
        logger.info("Using parameter column: %s", param_column)

        complete_df = fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_column, args.k_neighbors, logger)
        complete_df.to_csv(filled_param_file, index=False)
        logger.info("Filled parameter file saved to: %s", filled_param_file)

        final_ids = set(complete_df["nat_hru_id"])
        expected_ids = set(range(1, expected_max + 1))
        still_missing = expected_ids - final_ids

        if still_missing:
            logger.warning("%d IDs are still missing", len(still_missing))
        else:
            logger.info("All missing values have been filled successfully!")
    else:
        logger.info("No missing values found in the parameter file")


if __name__ == "__main__":
    main()
