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
    """KNN-interpolate absent HRU rows AND present-but-NaN parameter cells.

    For absent ids: appends a new all-NaN row so every expected id exists in
    the frame before per-column filling begins.

    For NaN cells: per column, the fit set is rows where that column has a
    valid (non-NaN) value AND valid coordinates; the fill set is rows where
    that column is NaN AND coords are valid. NaN-valued rows are excluded from
    the fit set so they cannot be chosen as a fill source.

    Exactly one row per id is guaranteed in the output; no duplicate ids.
    """
    logger.info("Filling missing values using KNN interpolation (k=%d)...", k)

    # Normalise to a list so a single column name still works.
    if isinstance(param_columns, str):
        param_columns = [param_columns]

    # Check whether there is anything to do before loading centroids.
    has_nan_cells = param_df[param_columns].isna().to_numpy().any() if param_columns else False
    if not missing_ids and not has_nan_cells:
        logger.info("No missing values to fill!")
        return param_df

    # Compute centroids once (avoid mutating caller's GDF).
    merged_gdf = merged_gdf.copy()
    merged_gdf["centroid"] = merged_gdf["geometry"].centroid
    merged_gdf["x"] = merged_gdf["centroid"].x
    merged_gdf["y"] = merged_gdf["centroid"].y
    coords_df = merged_gdf[[id_feature, "x", "y"]].copy()

    # Step 1: append all-NaN rows for absent ids so every expected id is present.
    if missing_ids:
        absent_rows = pd.DataFrame({id_feature: missing_ids})
        # gfv2 CSVs carry a secondary local hru_id; populate it for appended rows.
        # When id_feature is already hru_id (e.g. oregon) this is a harmless self-assignment.
        absent_rows["hru_id"] = absent_rows[id_feature]
        param_df = pd.concat([param_df, absent_rows], ignore_index=True)

    # Step 2: attach centroid x,y to every row (left join preserves all ids).
    full_df = param_df.merge(coords_df, on=id_feature, how="left")

    # Step 3: per-column KNN fill.
    for param_column in tqdm(param_columns, desc="Filling param columns"):
        col_vals = full_df[param_column].values
        x_vals = full_df["x"].values
        y_vals = full_df["y"].values

        has_valid_coord = ~(np.isnan(x_vals) | np.isnan(y_vals))
        has_valid_value = ~np.isnan(col_vals)

        fill_mask = (~has_valid_value) & has_valid_coord
        fit_mask = has_valid_value & has_valid_coord

        # Null-geometry guard: any fill row with NaN coords cannot be KNN-filled.
        null_geom_fill = (~has_valid_value) & (~has_valid_coord)
        if null_geom_fill.any():
            raise ValueError(
                f"{null_geom_fill.sum()} features needing fill for '{param_column}' "
                "have null geometry and cannot be filled via KNN interpolation."
            )

        if not fill_mask.any():
            continue

        fit_coords = np.column_stack([x_vals[fit_mask], y_vals[fit_mask]])
        fit_values = col_vals[fit_mask]
        fill_coords = np.column_stack([x_vals[fill_mask], y_vals[fill_mask]])

        knn = NearestNeighbors(n_neighbors=k)
        knn.fit(fit_coords)
        _, indices = knn.kneighbors(fill_coords)

        filled_values = np.array([np.mean(fit_values[neighbor_idx]) for neighbor_idx in indices])
        full_df.loc[fill_mask, param_column] = filled_values

    # Step 4: drop helper columns, sort, reset index.
    full_df = full_df.drop(columns=["x", "y", "centroid"], errors="ignore")
    full_df = full_df.sort_values(id_feature).reset_index(drop=True)

    n_absent = len(missing_ids)
    total_nan_cells = param_df[param_columns].isna().to_numpy().sum() if has_nan_cells else 0
    logger.info(
        "Filled %d absent id(s) and %d NaN cell(s) across %d column(s)",
        n_absent, total_nan_cells, len(param_columns),
    )
    return full_df


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

    param_columns = [col for col in param_df.columns if col not in {id_feature, "hru_id", "nat_hru_id", "vpu"}]
    if not param_columns:
        raise ValueError("No parameter columns found in the data")

    needs_fill = bool(missing_ids) or param_df[param_columns].isna().to_numpy().any()

    if needs_fill:
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
