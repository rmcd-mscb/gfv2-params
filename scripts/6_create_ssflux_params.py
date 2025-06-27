"""
Script to process ssflux params
"""
import argparse
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from gdptools import WeightGenP2P

# Add the src directory to the Python path for helper function imports
print(f"__file__: {__file__}")
src_path = Path(__file__).resolve().parent.parent / "src"
print(f"src_path: {src_path}")
sys.path.append(str(src_path))

from helpers import deg_to_fraction, load_config  # noqa: E402


def main():
    parser = argparse.ArgumentParser(
        description="Create ssflux parameters from source raster data."
    )
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument(
        "--vpu", required=True, help="VPU code, e.g., 01, 03N, 03S, 03W, 10U, 10L"
    )
    args = parser.parse_args()

    # Load the configuration and set up directories
    config = load_config(args.config)
    base_source_dir = Path(config["base_source_dir"])
    target_source_dir = Path(config["target_source_dir"])
    output_dir = Path(config["output_dir"])
    weight_dir = Path(config["weight_dir"])
    if not weight_dir.exists():
        weight_dir.mkdir(parents=True, exist_ok=True)

    # Get processing parameters from the config
    source_type = config.get("source_type")

    # Define target geopackage path and load the 'nhru' layer with geopandas
    target_gdf_path = target_source_dir / f"NHM_{args.vpu}_draft.gpkg"
    if not target_gdf_path.exists():
        raise FileNotFoundError(f"GPKG not found: {target_gdf_path}")
    target_gdf = gpd.read_file(target_gdf_path, layer="nhru")
    print(f"Loaded nhru layer from {target_gdf_path}: {len(target_gdf)} features")

    source_gdf = gpd.read_file(
        base_source_dir / "data_layers/soils_litho/Lithology_exp_Konly_Project.shp"
    )
    slope_source_gdf = gpd.read_file(
        output_dir / f"slope/base_nhm_slope_{args.vpu}_param.csv"
    )

    slope_source_gdf["mean_slope_fraction"] = slope_source_gdf["mean"].astype(float).apply(deg_to_fraction)
    source_gdf['flux_id'] = np.arange(len(source_gdf))


    weight_file = weight_dir / f"lith_weights_vpu_{args.vpu}.csv"
    if not weight_file.exists():
        weight_gen = WeightGenP2P(
            target_poly=target_gdf,
            target_poly_idx="hru_id",
            source_poly=source_gdf,
            source_poly_idx="flux_id",
            method="serial",
            weight_gen_crs="5070",
            output_file=weight_dir / f"lith_weights_vpu_{args.vpu}.csv"
        )

        weights = weight_gen.calculate_weights()
    else:
        weights = pd.read_csv(weight_file)
    print("Calculated weights:")

    # To complete merge we need to cast `hru_id` as str in both datasets
    weights['flux_id'] = weights['flux_id'].astype(str)
    source_gdf['flux_id'] = source_gdf['flux_id'].astype(str)

    # Merge the calculated weights with the corresponding source attributes
    w = weights.merge(
        source_gdf[['flux_id', 'k_perm']],
        on='flux_id'
    )
    print("Weights merged with source attributes:")
    # no more zeros?
    print((w['k_perm'] == 0).sum(), "zeros remain")

    k_permMin = -16.48
    w["k_perm"] = w["k_perm"].replace(0, k_permMin)
    w["k_perm_actual"] = 10 ** w["k_perm"]
    print((w['k_perm'] == 0).sum(), "zeros remain")
    print("Minimum k_perm:", w['k_perm'].min())

    # Extensive Variable Aggregation (Prorated Sum)
    # For k_perm (m^2), prorate the source's k_perm by the fraction of its area in the target.
    # The column "flux_id_area" should represent the total area of the source polygon.
    w["k_perm_wtd_sum"] = w["k_perm_actual"] * (w["area_weight"] / w["flux_id_area"])

    # Sum the prorated k_perm for each target polygon.
    extensive_agg = (
        w.groupby("hru_id")
        .agg(k_perm_wtd = ("k_perm_wtd_sum", "sum"))
        .reset_index()
    )
    extensive_agg["hru_id"] = extensive_agg["hru_id"].astype(int)
    extensive_sorted = extensive_agg.sort_values(
        by="hru_id",
        ascending=True
    ).reset_index(drop=True)

    # no more zeros?
    print((extensive_sorted['k_perm_wtd'] == 0).sum(), "zeros remain")

    slope_df = slope_source_gdf[["hru_id", "mean_slope_fraction"]].copy()
    slope_df["hru_id"] = pd.to_numeric(slope_df["hru_id"], errors="coerce").astype("int64")

    target_gdf["hru_area"] = target_gdf.geometry.area
    area_df = target_gdf[["hru_id", "hru_area"]].copy()
    area_df["hru_id"] = pd.to_numeric(area_df["hru_id"], errors="coerce").astype("int64")

    df = extensive_sorted.merge(slope_df, on="hru_id", how="left").copy()
    df = df.merge(area_df, on="hru_id", how="left")

    # compute PRMS fluxes
    df["r_soil2gw_max"]          = df["k_perm_wtd"]**3
    df["r_ssr2gw_rate"]          = df["k_perm_wtd"]*(1-df["mean_slope_fraction"])
    df["r_slowcoef_lin"]         = (df["k_perm_wtd"]*df["mean_slope_fraction"])/df["hru_area"]
    df["r_fastcoef_lin"]         = 2*df["r_slowcoef_lin"]
    df["r_gwflow_coef"]          = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"]      = df["r_fastcoef_lin"]

    # stats + normalize
    fluxParams    = ["soil2gw_max","ssr2gw_rate","fastcoef_lin",
                    "slowcoef_lin","gwflow_coef","dprst_seep_rate_open","dprst_flow_coef"]
    fluxParamsMax = [0.3,0.7,0.6,0.3,0.3,0.2,0.5]
    fluxParamsMin = [0.1,0.3,0.01,0.005,0.005,0.005,0.005]

    df_r = df[[f"r_{p}" for p in fluxParams]].agg(['min','max'])
    df_r.loc['range'] = df_r.loc['max'] - df_r.loc['min']

    for i,p in enumerate(fluxParams):
        rcol   = f"r_{p}"
        minIn, rngIn = df_r.at['min',rcol], df_r.at['range',rcol]
        minOut, maxOut = fluxParamsMin[i], fluxParamsMax[i]
        rngOut = maxOut - minOut
        norm   = (df[rcol]-minIn)/rngIn
        df[p] = norm * rngOut + minOut
    # drop intermediates
    df.drop(columns=[f"r_{p}" for p in fluxParams], inplace=True)
    if not (output_dir / "ssflux").exists():
        (output_dir / "ssflux").mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / f"ssflux/base_nhm_{source_type}_{args.vpu}_param.csv", index=False)

if __name__ == "__main__":
    main()
