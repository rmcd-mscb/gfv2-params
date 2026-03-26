"""Create subsurface flux parameters using litho-weighted approach.

Requires pre-computed CONUS-wide weights (from build_weights.py)
and merged slope parameters (from merge_params.py).
"""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import deg_to_fraction


def main():
    parser = argparse.ArgumentParser(description="Create ssflux parameters.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID")
    args = parser.parse_args()

    logger = configure_logging("create_ssflux_params")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    output_dir = Path(config["output_dir"])
    weight_dir = Path(config["weight_dir"])
    fabric = config["fabric"]

    # Load batch polygons
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{args.batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    target_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    batch_ids = set(target_gdf[id_feature].values)
    logger.info("Loaded %d features (batch %d)", len(target_gdf), args.batch_id)

    # Load pre-computed CONUS weights and filter to this batch
    weight_file = weight_dir / f"lith_weights_{fabric}.csv"
    if not weight_file.exists():
        raise FileNotFoundError(
            f"Weight file not found: {weight_file}\n"
            "Run scripts/build_weights.py first."
        )
    all_weights = pd.read_csv(weight_file)
    weights = all_weights[all_weights[id_feature].isin(batch_ids)].copy()
    logger.info("Loaded weights: %d rows (from %d total)", len(weights), len(all_weights))

    # Load merged slope CSV
    merged_slope_file = Path(config["merged_slope_file"])
    if not merged_slope_file.exists():
        raise FileNotFoundError(
            f"Merged slope file not found: {merged_slope_file}\n"
            "Run merge_params.py for slope first."
        )
    all_slope = pd.read_csv(merged_slope_file)
    slope_df = all_slope[all_slope[id_feature].isin(batch_ids)].copy()
    slope_df["mean_slope_fraction"] = slope_df["mean"].astype(float).apply(deg_to_fraction)
    logger.info("Loaded slope for %d features", len(slope_df))

    # Load source lithology for k_perm lookup
    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))

    # Merge weights with source attributes
    weights["flux_id"] = weights["flux_id"].astype(str)
    source_gdf["flux_id"] = source_gdf["flux_id"].astype(str)

    w = weights.merge(source_gdf[["flux_id", "k_perm"]], on="flux_id")

    k_perm_min = config["k_perm_min"]
    w["k_perm"] = w["k_perm"].replace(0, k_perm_min)
    w["k_perm_actual"] = 10 ** w["k_perm"]

    w["k_perm_wtd_sum"] = w["k_perm_actual"] * (w["area_weight"] / w["flux_id_area"])

    extensive_agg = (
        w.groupby(id_feature)
        .agg(k_perm_wtd=("k_perm_wtd_sum", "sum"))
        .reset_index()
    )
    extensive_agg[id_feature] = extensive_agg[id_feature].astype(int)
    extensive_sorted = extensive_agg.sort_values(by=id_feature).reset_index(drop=True)

    # Merge with slope and area
    slope_merge = slope_df[[id_feature, "mean_slope_fraction"]].copy()
    try:
        slope_merge[id_feature] = slope_merge[id_feature].astype("int64")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Non-numeric {id_feature} values in slope data") from exc

    target_gdf["hru_area"] = target_gdf.geometry.area
    area_df = target_gdf[[id_feature, "hru_area"]].copy()
    try:
        area_df[id_feature] = area_df[id_feature].astype("int64")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Non-numeric {id_feature} values in target fabric") from exc

    df = extensive_sorted.merge(slope_merge, on=id_feature, how="left").copy()
    df = df.merge(area_df, on=id_feature, how="left")

    # Validate no features were lost in merges
    null_slope = df["mean_slope_fraction"].isna().sum()
    null_area = df["hru_area"].isna().sum()
    if null_slope > 0 or null_area > 0:
        raise ValueError(
            f"Merge produced missing values: {null_slope} features missing slope, "
            f"{null_area} features missing area. Check that slope and batch data "
            f"use consistent {id_feature} values."
        )

    # Compute raw PRMS fluxes
    df["r_soil2gw_max"] = df["k_perm_wtd"] ** 3
    df["r_ssr2gw_rate"] = df["k_perm_wtd"] * (1 - df["mean_slope_fraction"])
    df["r_slowcoef_lin"] = (df["k_perm_wtd"] * df["mean_slope_fraction"]) / df["hru_area"]
    df["r_fastcoef_lin"] = 2 * df["r_slowcoef_lin"]
    df["r_gwflow_coef"] = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"] = df["r_fastcoef_lin"]

    # Normalize using config-driven bounds.
    # Note: normalization is per-batch (not CONUS-wide), matching the prior per-VPU
    # behavior. The same raw value may map to slightly different normalized values
    # in different batches because per-batch min/max ranges differ.
    flux_params = config["flux_params"]
    param_names = [fp["name"] for fp in flux_params]
    param_maxes = [fp["max"] for fp in flux_params]
    param_mins = [fp["min"] for fp in flux_params]

    df_r = df[[f"r_{p}" for p in param_names]].agg(["min", "max"])
    df_r.loc["range"] = df_r.loc["max"] - df_r.loc["min"]

    for i, p in enumerate(param_names):
        rcol = f"r_{p}"
        min_in, rng_in = df_r.at["min", rcol], df_r.at["range", rcol]
        min_out, max_out = param_mins[i], param_maxes[i]
        rng_out = max_out - min_out
        if rng_in == 0:
            logger.warning("Range is zero for %s; using midpoint of output range", p)
            df[p] = (min_out + max_out) / 2.0
        else:
            norm = (df[rcol] - min_in) / rng_in
            df[p] = norm * rng_out + min_out

    df.drop(columns=[f"r_{p}" for p in param_names], inplace=True)

    ssflux_dir = output_dir / "ssflux"
    ssflux_dir.mkdir(parents=True, exist_ok=True)
    file_prefix = f"base_nhm_ssflux_{fabric}_batch_{args.batch_id:04d}_param"
    df.to_csv(ssflux_dir / f"{file_prefix}.csv", index=False)
    logger.info("SSFlux parameters saved (batch %d)", args.batch_id)


if __name__ == "__main__":
    main()
