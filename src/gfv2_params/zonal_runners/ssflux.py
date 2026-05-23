"""Per-batch ssflux parameter derivation.

Uses the CONUS-wide P2P weight matrix produced by ``run_build_weights`` (in
``weights.py``); chains in the merged slope CSV (from a prior ``merge`` of the
``slope`` zonal output) to compute ssflux family params (soil2gw_max,
ssr2gw_rate, fastcoef_lin, slowcoef_lin, gwflow_coef, dprst_seep_rate_open,
dprst_flow_coef).
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from ..raster_ops import deg_to_fraction


def run_ssflux_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of subsurface flux parameter derivation.

    Originally extracted from the now-retired scripts/create_ssflux_params.py
    (see PR #85). Requires pre-computed
    CONUS weights (from run_build_weights) and merged slope CSV (from
    run_merge applied to the slope param). Output writes to
    {output_dir}/ssflux/ (subdir name is hardcoded to 'ssflux' to match
    today's create_ssflux_params.py behaviour).
    """
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    output_dir = Path(config["output_dir"])
    weight_dir = Path(config["weight_dir"])
    fabric = config["fabric"]

    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    target_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    batch_ids = set(target_gdf[id_feature].values)
    logger.info("Loaded %d features (batch %d)", len(target_gdf), batch_id)

    weight_file = weight_dir / f"lith_weights_{fabric}.csv"
    if not weight_file.exists():
        raise FileNotFoundError(
            f"Weight file not found: {weight_file}\n"
            "Run --mode build_weights first."
        )
    all_weights = pd.read_csv(weight_file)
    weights = all_weights[all_weights[id_feature].isin(batch_ids)].copy()
    logger.info("Loaded weights: %d rows (from %d total)", len(weights), len(all_weights))

    merged_slope_file = Path(config["merged_slope_file"])
    if not merged_slope_file.exists():
        raise FileNotFoundError(
            f"Merged slope file not found: {merged_slope_file}\n"
            "Run merge for the slope param first."
        )
    all_slope = pd.read_csv(merged_slope_file)
    slope_df = all_slope[all_slope[id_feature].isin(batch_ids)].copy()
    slope_df["mean_slope_fraction"] = slope_df["mean"].astype(float).apply(deg_to_fraction)
    logger.info("Loaded slope for %d features", len(slope_df))

    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))

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

    null_slope = df["mean_slope_fraction"].isna().sum()
    null_area = df["hru_area"].isna().sum()
    if null_slope > 0 or null_area > 0:
        raise ValueError(
            f"Merge produced missing values: {null_slope} features missing slope, "
            f"{null_area} features missing area. Check that slope and batch data "
            f"use consistent {id_feature} values."
        )

    df["r_soil2gw_max"] = df["k_perm_wtd"] ** 3
    df["r_ssr2gw_rate"] = df["k_perm_wtd"] * (1 - df["mean_slope_fraction"])
    df["r_slowcoef_lin"] = (df["k_perm_wtd"] * df["mean_slope_fraction"]) / df["hru_area"]
    df["r_fastcoef_lin"] = 2 * df["r_slowcoef_lin"]
    df["r_gwflow_coef"] = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"] = df["r_fastcoef_lin"]

    # Normalisation is per-batch (not CONUS-wide), matching prior per-VPU
    # behaviour. The same raw value may map to slightly different normalised
    # values across batches because per-batch min/max ranges differ.
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
    file_prefix = f"base_nhm_ssflux_{fabric}_batch_{batch_id:04d}_param"
    df.to_csv(ssflux_dir / f"{file_prefix}.csv", index=False)
    logger.info("SSFlux parameters saved (batch %d)", batch_id)
