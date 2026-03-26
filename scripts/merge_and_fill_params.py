"""Fill missing parameter values using KNN interpolation against a merged nhru geopackage.

The merged geopackage is produced by the notebooks/merge_vpu_targets.py notebook
and serves as input to prepare_fabric.py for batching.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


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
    missing_df = merged_gdf[merged_gdf["nat_hru_id"].isin(missing_ids)][["nat_hru_id", "x", "y"]]

    existing_coords = existing_df[["x", "y"]].values
    missing_coords = missing_df[["x", "y"]].values
    existing_values = existing_df[param_column].values

    # Filter NaN coordinates from existing data (null/invalid geometries)
    nan_mask = np.isnan(existing_coords).any(axis=1)
    if nan_mask.any():
        logger.warning(
            "%d features have NaN coordinates (null/invalid geometry). "
            "Excluding from KNN fitting.", nan_mask.sum()
        )
        existing_coords = existing_coords[~nan_mask]
        existing_values = existing_values[~nan_mask]

    nan_missing = np.isnan(missing_coords).any(axis=1)
    if nan_missing.any():
        raise ValueError(
            f"{nan_missing.sum()} missing features have null geometry "
            "and cannot be filled via KNN interpolation."
        )

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
    })
    missing_filled["hru_id"] = missing_filled["nat_hru_id"]

    complete_df = pd.concat([param_df, missing_filled], ignore_index=True)
    complete_df = complete_df.sort_values("nat_hru_id").reset_index(drop=True)
    logger.info("Filled %d missing values", len(missing_ids))
    return complete_df


def main():
    parser = argparse.ArgumentParser(description="Fill missing parameter values using KNN interpolation.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--merged_gpkg", default=None, help="Path to merged nhru geopackage")
    parser.add_argument("--param_file", default=None, help="Path to merged parameter CSV to fill")
    parser.add_argument("--output_dir", default=None, help="Output directory for filled file")
    parser.add_argument("--k_neighbors", type=int, default=1)
    args = parser.parse_args()

    logger = configure_logging("merge_and_fill_params")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = base["data_root"]
    fabric = base["fabric"]
    expected_max = base["expected_max_hru_id"]

    # Resolve defaults from fabric namespace
    if args.merged_gpkg is None:
        args.merged_gpkg = f"{data_root}/{fabric}/fabric/{fabric}_nhru_merged.gpkg"
    if args.param_file is None:
        args.param_file = f"{data_root}/{fabric}/params/merged/nhm_ssflux_params.csv"
    if args.output_dir is None:
        args.output_dir = f"{data_root}/{fabric}/params/merged"

    merged_gpkg = Path(args.merged_gpkg)
    param_file = Path(args.param_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not param_file.exists():
        raise FileNotFoundError(
            f"Parameter file not found: {param_file}\n"
            "Run scripts/merge_params.py for this parameter type first."
        )

    if not merged_gpkg.exists():
        raise FileNotFoundError(
            f"Merged geopackage not found: {merged_gpkg}\n"
            "Run notebooks/merge_vpu_targets.py or scripts/prepare_fabric.py first."
        )

    logger.info("Loading merged geopackage: %s", merged_gpkg)
    try:
        merged_gdf = gpd.read_file(merged_gpkg, layer="nhru")
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read merged geopackage: {merged_gpkg}\n"
            "The file may be corrupt."
        ) from exc
    logger.info("Loaded %d features", len(merged_gdf))

    filled_param_file = output_dir / f"filled_{param_file.name}"

    param_df, missing_ids = find_missing_ids(param_file, expected_max, logger)

    if missing_ids:
        param_columns = [col for col in param_df.columns if col not in ["hru_id", "nat_hru_id", "vpu"]]
        if not param_columns:
            raise ValueError("No parameter columns found in the data")

        logger.info("Filling parameter columns: %s", param_columns)

        complete_df = param_df
        for param_column in param_columns:
            logger.info("Filling parameter column: %s", param_column)
            complete_df = fill_missing_values_knn(complete_df, missing_ids, merged_gdf, param_column, args.k_neighbors, logger)
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
