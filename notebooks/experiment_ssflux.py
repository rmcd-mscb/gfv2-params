import marimo

__generated_with = "0.14.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import os.path

    import geopandas as gpd
    import numpy as np
    import pandas as pd

    import marimo as mo
    print(f"Current working directory: {os.getcwd()}")
    return gpd, mo, np, pd


@app.cell
def _():
    from pathlib import Path
    base_source_dir = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/source_data/")
    target_source_dir = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/targets")
    weight_dir = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/wieghts")
    if not weight_dir.exists():
        weight_dir.mkdir(exist_ok=True, parents=True)
    # output_dir = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/nhm_params")
    return Path, base_source_dir, target_source_dir, weight_dir


@app.cell
def _(target_source_dir):
    gpkg_vpu = "01"
    # Target geopackage path
    gpkg_path = target_source_dir / f"NHM_{gpkg_vpu}_draft.gpkg"
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")

    print(f"GPKG:   {gpkg_path}")
    return gpkg_path, gpkg_vpu


@app.cell
def _(gpd, gpkg_path):
    target_gdf = gpd.read_file(gpkg_path, layer="nhru")
    target_gdf
    return (target_gdf,)


@app.cell
def _(Path, base_source_dir, gpd, gpkg_vpu):
    source_gdf = gpd.read_file(base_source_dir / "data_layers/soils_litho/Lithology_exp_Konly_Project.shp")
    slope_source_gdf = gpd.read_file(Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/nhm_params/slope") / f"base_nhm_slope_{gpkg_vpu}_param.csv")
    return slope_source_gdf, source_gdf


@app.cell
def _(slope_source_gdf):
    slope_source_gdf
    return


@app.cell
def _(np):
    def deg_to_fraction(slope_deg: float) -> float:
        """
        Convert slope from degrees to percent.

        Parameters
        ----------
        slope_deg : float
            Slope angle in degrees.

        Returns
        -------
        float
            Percent slope.
        """
        return np.tan(np.deg2rad(slope_deg))
    return (deg_to_fraction,)


@app.cell
def _(deg_to_fraction, slope_source_gdf):
    slope_source_gdf["mean_slope_fraction"] = slope_source_gdf["mean"].astype(float).apply(deg_to_fraction)
    slope_source_gdf
    return


@app.cell
def _(np, source_gdf):
    source_gdf['flux_id'] = np.arange(len(source_gdf))
    source_gdf.head
    return


@app.cell
def _(source_gdf):
    source_gdf.crs

    return


@app.cell
def _(gpkg_vpu, pd, source_gdf, target_gdf, weight_dir):
    from gdptools import WeightGenP2P
    weight_file = weight_dir / f"lith_weights_vpu_{gpkg_vpu}.csv"
    if not weight_file.exists():
        weight_gen = WeightGenP2P(
            target_poly=target_gdf,
            target_poly_idx="hru_id",
            source_poly=source_gdf,
            source_poly_idx="flux_id",
            method="serial",
            weight_gen_crs="5070",
            output_file=weight_dir / f"lith_weights_vpu_{gpkg_vpu}.csv"
        )

        weights = weight_gen.calculate_weights()
    else:
        weights = pd.read_csv(weight_file)
    print("Calculated weights:")
    weights
    return (weights,)


@app.cell
def _(mo):
    mo.md(r"""4. Merge Source Attributes with Weights""")
    return


@app.cell
def _(source_gdf, weights):
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
    w
    return (w,)


@app.cell
def _(w):
    k_permMin = -16.48
    w["k_perm"] = w["k_perm"].replace(0, k_permMin)
    w["k_perm_actual"] = 10 ** w["k_perm"]
    print((w['k_perm'] == 0).sum(), "zeros remain")
    print("Minimum k_perm:", w['k_perm'].min())
    return


@app.cell
def _(w):
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

    extensive_agg
    return (extensive_agg,)


@app.cell
def _(extensive_agg):
    extensive_agg["k_perm_wtd"].values
    return


@app.cell
def _(extensive_agg):
    extensive_agg["hru_id"] = extensive_agg["hru_id"].astype(int)
    extensive_sorted = extensive_agg.sort_values(
        by="hru_id",
        ascending=True
    ).reset_index(drop=True)

    # no more zeros?
    print((extensive_sorted['k_perm_wtd'] == 0).sum(), "zeros remain")
    extensive_sorted
    return (extensive_sorted,)


@app.cell
def _(pd, slope_source_gdf):
    slope_df = slope_source_gdf[["hru_id", "mean_slope_fraction"]].copy()
    slope_df["hru_id"] = pd.to_numeric(slope_df["hru_id"], errors="coerce").astype("int64")
    slope_df
    return (slope_df,)


@app.cell
def _(pd, target_gdf):
    target_gdf["hru_area"] = target_gdf.geometry.area
    area_df = target_gdf[["hru_id", "hru_area"]].copy()
    area_df["hru_id"] = pd.to_numeric(area_df["hru_id"], errors="coerce").astype("int64")
    area_df
    return (area_df,)


@app.cell
def _(extensive_sorted):
    extensive_sorted
    return


@app.cell
def _(area_df, extensive_sorted, slope_df):
    df = extensive_sorted.merge(slope_df, on="hru_id", how="left").copy()
    df = df.merge(area_df, on="hru_id", how="left")
    df
    return (df,)


@app.cell
def _(df):
    df["k_perm_wtd"].values[:10]
    return


@app.cell
def _(df):
    # compute PRMS fluxes
    df["r_soil2gw_max"]          = df["k_perm_wtd"]**3
    df["r_ssr2gw_rate"]          = df["k_perm_wtd"]*(1-df["mean_slope_fraction"])
    df["r_slowcoef_lin"]         = (df["k_perm_wtd"]*df["mean_slope_fraction"])/df["hru_area"]
    df["r_fastcoef_lin"]         = 2*df["r_slowcoef_lin"]
    df["r_gwflow_coef"]          = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"]      = df["r_fastcoef_lin"]
    df
    return


@app.cell
def _(df):
    # stats + normalize
    fluxParams    = ["soil2gw_max","ssr2gw_rate","fastcoef_lin",
                     "slowcoef_lin","gwflow_coef","dprst_seep_rate_open","dprst_flow_coef"]
    fluxParamsMax = [0.3,0.7,0.6,0.3,0.3,0.2,0.5]
    fluxParamsMin = [0.1,0.3,0.01,0.005,0.005,0.005,0.005]

    df_r = df[[f"r_{p}" for p in fluxParams]].agg(['min','max'])
    df_r.loc['range'] = df_r.loc['max'] - df_r.loc['min']
    df_r
    return df_r, fluxParams, fluxParamsMax, fluxParamsMin


@app.cell
def _(df, df_r, fluxParams, fluxParamsMax, fluxParamsMin):
    for i,p in enumerate(fluxParams):
        rcol   = f"r_{p}"
        minIn, rngIn = df_r.at['min',rcol], df_r.at['range',rcol]
        minOut, maxOut = fluxParamsMin[i], fluxParamsMax[i]
        rngOut = maxOut - minOut
        norm   = (df[rcol]-minIn)/rngIn
        df[p] = norm * rngOut + minOut
    # drop intermediates
    # df.drop(columns=[f"r_{p}" for p in fluxParams]+["k_perm_wtd_sum"], inplace=True)
    df.drop(columns=[f"r_{p}" for p in fluxParams], inplace=True)
    return


@app.cell
def _(df):
    df
    return


if __name__ == "__main__":
    app.run()
