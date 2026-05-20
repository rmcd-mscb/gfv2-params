"""Fill missing parameter values using KNN interpolation against the fabric geopackage.

The fabric geopackage is read from the active base_config.yml profile's
hru_gpkg/hru_layer (the same gpkg prepare_fabric.py batched) — the single
source of truth, not a {fabric}_nhru_merged.gpkg naming convention. For
VPU-based fabrics that gpkg is produced by notebooks/merge_vpu_targets.py; for
single-file fabrics (e.g. oregon) it is a pre-existing gpkg declared in the
profile.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging


def find_missing_ids(param_file, expected_max, id_feature, logger):
    logger.info("Finding missing %s values...", id_feature)
    param_df = pd.read_csv(param_file)
    existing_ids = set(param_df[id_feature])
    expected_ids = set(range(1, expected_max + 1))
    missing_ids = sorted(expected_ids - existing_ids)
    logger.info("Found %d missing %s values out of %d", len(missing_ids), id_feature, expected_max)
    return param_df, missing_ids


def fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_columns, k, id_feature, logger):
    """KNN-interpolate every missing HRU across all `param_columns` at once.

    Builds a single block of filled rows (one row per missing id, every column
    populated) and appends it to `param_df` exactly once. Filling per column
    and re-appending would duplicate each missing id once per column, leaving
    most cells NaN — see the multi-column regression test.
    """
    logger.info("Filling missing values using KNN interpolation (k=%d)...", k)

    # Normalise to a list so a single column name still works.
    if isinstance(param_columns, str):
        param_columns = [param_columns]

    if not missing_ids:
        logger.info("No missing values to fill!")
        return param_df

    merged_gdf["centroid"] = merged_gdf["geometry"].centroid
    merged_gdf["x"] = merged_gdf["centroid"].x
    merged_gdf["y"] = merged_gdf["centroid"].y

    existing_df = param_df.merge(merged_gdf[[id_feature, "x", "y"]], on=id_feature, how="left")
    missing_df = merged_gdf[merged_gdf[id_feature].isin(missing_ids)][[id_feature, "x", "y"]]

    existing_coords = existing_df[["x", "y"]].values
    missing_coords = missing_df[["x", "y"]].values

    # Coordinates are column-independent, so the NaN-coordinate mask and the
    # null-geometry guard are computed once for the whole block.
    nan_mask = np.isnan(existing_coords).any(axis=1)
    if nan_mask.any():
        logger.warning(
            "%d features have NaN coordinates (null/invalid geometry). "
            "Excluding from KNN fitting.", nan_mask.sum()
        )

    nan_missing = np.isnan(missing_coords).any(axis=1)
    if nan_missing.any():
        raise ValueError(
            f"{nan_missing.sum()} missing features have null geometry "
            "and cannot be filled via KNN interpolation."
        )

    fit_coords = existing_coords[~nan_mask]
    knn = NearestNeighbors(n_neighbors=k)
    knn.fit(fit_coords)
    distances, indices = knn.kneighbors(missing_coords)

    # One filled row per missing id, with every parameter column populated.
    missing_filled = pd.DataFrame({id_feature: missing_df[id_feature].values})
    for param_column in tqdm(param_columns, desc="Filling param columns"):
        existing_values = existing_df[param_column].values[~nan_mask]
        missing_filled[param_column] = [
            np.mean(existing_values[neighbor_indices]) for neighbor_indices in indices
        ]
    # gfv2's merged CSVs carry a secondary local `hru_id` alongside the
    # national nat_hru_id key; populate it for filled rows. When id_feature
    # is already `hru_id` (e.g. oregon) this is a harmless self-assignment.
    missing_filled["hru_id"] = missing_filled[id_feature]

    complete_df = pd.concat([param_df, missing_filled], ignore_index=True)
    complete_df = complete_df.sort_values(id_feature).reset_index(drop=True)
    logger.info("Filled %d missing values across %d column(s)", len(missing_ids), len(param_columns))
    return complete_df


def main():
    parser = argparse.ArgumentParser(description="Fill missing parameter values using KNN interpolation.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--merged_gpkg", default=None, help="Path to merged nhru geopackage")
    parser.add_argument("--param_file", default=None, help="Path to merged parameter CSV to fill")
    parser.add_argument("--output_dir", default=None, help="Output directory for filled file")
    parser.add_argument("--k_neighbors", type=int, default=1)
    args = parser.parse_args()

    logger = configure_logging("merge_and_fill_params")

    base = load_base_config(
        Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    data_root = base["data_root"]
    fabric = base["fabric"]
    expected_max = base["expected_max_hru_id"]
    id_feature = require_config_key(base, "id_feature", "merge_and_fill_params")

    # The merged fabric gpkg is authoritative in the active base_config.yml
    # profile (hru_gpkg/hru_layer) — read it from there, not a
    # {fabric}_nhru_merged.gpkg naming convention. --merged_gpkg is an override.
    hru_layer = base.get("hru_layer", "nhru")
    if args.merged_gpkg is None:
        args.merged_gpkg = require_config_key(base, "hru_gpkg", "merge_and_fill_params")
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
            "Run scripts/derive_zonal_params.py --mode merge --param <name> for this parameter type first."
        )

    if not merged_gpkg.exists():
        raise FileNotFoundError(
            f"Fabric geopackage not found: {merged_gpkg}\n"
            "Check the active fabric profile's hru_gpkg in configs/base_config.yml. "
            "For VPU-based fabrics, run notebooks/merge_vpu_targets.py to produce it; "
            "for single-file fabrics, place the gpkg at the hru_gpkg path."
        )

    logger.info("Loading merged geopackage: %s (layer=%s)", merged_gpkg, hru_layer)
    try:
        merged_gdf = gpd.read_file(merged_gpkg, layer=hru_layer)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read merged geopackage: {merged_gpkg}\n"
            "The file may be corrupt."
        ) from exc
    logger.info("Loaded %d features", len(merged_gdf))

    filled_param_file = output_dir / f"filled_{param_file.name}"

    param_df, missing_ids = find_missing_ids(param_file, expected_max, id_feature, logger)

    if missing_ids:
        param_columns = [col for col in param_df.columns if col not in {id_feature, "hru_id", "nat_hru_id", "vpu"}]
        if not param_columns:
            raise ValueError("No parameter columns found in the data")

        logger.info("Filling parameter columns: %s", param_columns)

        complete_df = fill_missing_values_knn(
            param_df, missing_ids, merged_gdf, param_columns, args.k_neighbors, id_feature, logger
        )
        complete_df.to_csv(filled_param_file, index=False)
        logger.info("Filled parameter file saved to: %s", filled_param_file)

        final_ids = set(complete_df[id_feature])
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
