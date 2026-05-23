import marimo

__generated_with = "0.14.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import warnings
    from pathlib import Path

    import geopandas as gpd
    import pandas as pd
    from gdptools import ClimRCatData, WeightGen


    warnings.filterwarnings("ignore")
    return ClimRCatData, Path, WeightGen, gpd, pd


@app.cell
def _(Path, gpd):
    target_dir = Path('./targets')
    target_dir.exists()
    gdf = gpd.read_file(target_dir / 'OR_NHM_seamless.gpkg', layer='hru')
    gdf.plot()
    return (gdf,)


@app.cell
def _(gdf):
    gdf.head()
    return


@app.cell
def _(pd):
    climater_cat = "https://github.com/mikejohnson51/climateR-catalogs/releases/download/June-2024/catalog.parquet"
    cat = pd.read_parquet(climater_cat)
    cat.head()
    return (cat,)


@app.cell
def _(cat):
    # Create a dictionary of parameter dataframes for each variable
    tvars = ["ppt", "tmax", "tmin"]
    cat_params = [cat.query("id == 'prism_daily' & variable == @var").to_dict(orient="records")[0] for var in tvars]

    cat_dict = dict(zip(tvars, cat_params))

    # Output an example of the cat_param.json entry for "aet".
    cat_dict.get("ppt")
    return (cat_dict,)


@app.cell
def _(ClimRCatData, WeightGen, cat_dict, gdf):
    user_data = ClimRCatData(
        cat_dict=cat_dict,
        f_feature=gdf,
        id_feature='or_hru_id',
        period=["2000-01-01", "2000-12-31"]
    )

    wght_gen = WeightGen(
        user_data=user_data,
        method="serial",
        output_file="prism_daily_wghts.csv",
        weight_gen_crs=6931
    )

    wght_gen.calculate_weights(intersections=False)
    return


if __name__ == "__main__":
    app.run()
