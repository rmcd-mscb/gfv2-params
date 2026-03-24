"""Create subsurface flux parameters using litho-weighted approach."""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from gdptools import WeightGenP2P

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import deg_to_fraction


def main():
    parser = argparse.ArgumentParser(description="Create ssflux parameters.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", default=None, help="VPU code (e.g., 01, 03N). Omit for custom fabrics.")
    args = parser.parse_args()

    logger = configure_logging("create_ssflux_params")

    config = load_config(Path(args.config), vpu=args.vpu)
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    output_dir = Path(config["output_dir"])
    weight_dir = Path(config["weight_dir"])
    weight_dir.mkdir(parents=True, exist_ok=True)

    vpu_label = args.vpu if args.vpu else "custom"

    # Load target geopackage
    target_gdf_path = Path(config["target_gpkg"])
    if not target_gdf_path.exists():
        raise FileNotFoundError(f"GPKG not found: {target_gdf_path}")
    target_gdf = gpd.read_file(target_gdf_path, layer=target_layer)
    logger.info("Loaded %s layer from %s: %d features", target_layer, target_gdf_path, len(target_gdf))

    # Load source lithology
    source_gdf = gpd.read_file(Path(config["source_shapefile"]))

    # Load slope params
    slope_source_gdf = pd.read_csv(output_dir / f"slope/base_nhm_slope_{vpu_label}_param.csv")
    slope_source_gdf["mean_slope_fraction"] = slope_source_gdf["mean"].astype(float).apply(deg_to_fraction)
    source_gdf["flux_id"] = np.arange(len(source_gdf))

    # Calculate or load weights
    weight_file = weight_dir / f"lith_weights_vpu_{vpu_label}.csv"
    if not weight_file.exists():
        weight_gen = WeightGenP2P(
            target_poly=target_gdf,
            target_poly_idx=id_feature,
            source_poly=source_gdf,
            source_poly_idx="flux_id",
            method="serial",
            weight_gen_crs="5070",
            output_file=weight_file,
        )
        weights = weight_gen.calculate_weights()
    else:
        weights = pd.read_csv(weight_file)
    logger.info("Weights loaded/calculated")

    # Merge weights with source attributes
    weights["flux_id"] = weights["flux_id"].astype(str)
    source_gdf["flux_id"] = source_gdf["flux_id"].astype(str)

    w = weights.merge(source_gdf[["flux_id", "k_perm"]], on="flux_id")
    logger.info("Zeros in k_perm: %d", (w["k_perm"] == 0).sum())

    # Replace zeros with config-driven minimum
    k_perm_min = config["k_perm_min"]
    w["k_perm"] = w["k_perm"].replace(0, k_perm_min)
    w["k_perm_actual"] = 10 ** w["k_perm"]

    # Extensive variable aggregation
    w["k_perm_wtd_sum"] = w["k_perm_actual"] * (w["area_weight"] / w["flux_id_area"])

    extensive_agg = (
        w.groupby(id_feature)
        .agg(k_perm_wtd=("k_perm_wtd_sum", "sum"))
        .reset_index()
    )
    extensive_agg[id_feature] = extensive_agg[id_feature].astype(int)
    extensive_sorted = extensive_agg.sort_values(by=id_feature, ascending=True).reset_index(drop=True)

    # Merge with slope and area
    slope_df = slope_source_gdf[[id_feature, "mean_slope_fraction"]].copy()
    slope_df[id_feature] = pd.to_numeric(slope_df[id_feature], errors="coerce").astype("int64")

    target_gdf["hru_area"] = target_gdf.geometry.area
    area_df = target_gdf[[id_feature, "hru_area"]].copy()
    area_df[id_feature] = pd.to_numeric(area_df[id_feature], errors="coerce").astype("int64")

    df = extensive_sorted.merge(slope_df, on=id_feature, how="left").copy()
    df = df.merge(area_df, on=id_feature, how="left")

    # Compute raw PRMS fluxes
    df["r_soil2gw_max"] = df["k_perm_wtd"] ** 3
    df["r_ssr2gw_rate"] = df["k_perm_wtd"] * (1 - df["mean_slope_fraction"])
    df["r_slowcoef_lin"] = (df["k_perm_wtd"] * df["mean_slope_fraction"]) / df["hru_area"]
    df["r_fastcoef_lin"] = 2 * df["r_slowcoef_lin"]
    df["r_gwflow_coef"] = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"] = df["r_fastcoef_lin"]

    # Normalize using config-driven bounds
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

    # Drop intermediate columns
    df.drop(columns=[f"r_{p}" for p in param_names], inplace=True)

    ssflux_dir = output_dir / "ssflux"
    ssflux_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(ssflux_dir / f"base_nhm_{source_type}_{vpu_label}_param.csv", index=False)
    logger.info("SSFlux parameters saved")


if __name__ == "__main__":
    main()
