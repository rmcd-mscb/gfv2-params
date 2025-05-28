import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import rioxarray
    import numpy as np
    import geopandas as gpd
    import matplotlib.pyplot as plt
    import pandas as pd
    import sys
    from gdptools import UserTiffData, ZonalGen
    from pathlib import Path
    import yaml
    from rasterstats import zonal_stats
    # Add the src directory to the Python path
    src_path = Path(__file__).resolve().parent.parent / "src"
    sys.path.append(str(src_path))

    # Now you can import helpers
    from helpers import load_config, resample, mult_rasters
    return (
        Path,
        UserTiffData,
        ZonalGen,
        gpd,
        load_config,
        mult_rasters,
        resample,
        rioxarray,
    )


@app.cell
def _():
    vpu = "01"
    return (vpu,)


@app.cell
def _(Path, load_config):
    config_file = "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/gfv2-params/configs/05_soilmoistmax_param_config.yml"
    config = load_config(config_file)
    base_source_dir = Path(config["base_source_dir"])
    target_source_dir = Path(config["target_source_dir"])
    output_dir = Path(config["output_dir"])

    source_type = config.get("source_type")
    categorical = config.get("categorical", False)
    output_path = output_dir / source_type
    output_path.mkdir(parents=True, exist_ok=True)
    return (
        base_source_dir,
        categorical,
        output_path,
        source_type,
        target_source_dir,
    )


@app.cell
def _(gpd, target_source_dir, vpu):
    # Target geopackage path
    gpkg_path = target_source_dir / f"NHM_{vpu}_draft.gpkg"
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")

    print(f"GPKG:   {gpkg_path}")

    # Open the 'nhru' layer from the geopackage with geopandas
    nhru_gdf = gpd.read_file(gpkg_path, layer="nhru")
    id_feature = "hru_id"
    print(f"Loaded nhru layer: {len(nhru_gdf)} features")
    return id_feature, nhru_gdf


@app.cell
def _(base_source_dir, source_type):
    if source_type == "soil_moist_max":
        soil_moist_max_rast = base_source_dir / "soils_litho/soil_moist_max.tif"
        rd_rast = base_source_dir / "lulc_veg/RootDepth.tif"
        awc_rast = base_source_dir / "soils_litho/AWC.tif"
        temp_rast = base_source_dir / "lulc_veg/rd_250_raw.tif"
        final_rast = base_source_dir / "lulc_veg/rd_250.tif"

        if not rd_rast.exists():
            raise FileNotFoundError(f"Root Depth raster not found: {rd_rast}")
        elif not awc_rast.exists():
            raise FileNotFoundError(f"AWC raster not found: {awc_rast}")
    return awc_rast, final_rast, rd_rast, soil_moist_max_rast, temp_rast


@app.cell
def _(awc_rast, final_rast, rd_rast, resample, temp_rast):
    if not final_rast.exists():
        resample(rd_rast, awc_rast, temp_rast, final_rast)
    return


@app.cell
def _(awc_rast, final_rast, mult_rasters, soil_moist_max_rast):
    if not soil_moist_max_rast.exists():
        mult_rasters(final_rast, awc_rast, soil_moist_max_rast)
    return


@app.cell
def _(rioxarray, soil_moist_max_rast):
    source_da = rioxarray.open_rasterio(soil_moist_max_rast)
    print(f"Loaded raster: shape={source_da.shape}, crs={source_da.rio.crs}")
    return (source_da,)


@app.cell
def _(source_da):
    source_da
    return


@app.cell
def _(
    UserTiffData,
    ZonalGen,
    categorical,
    id_feature,
    nhru_gdf,
    output_path,
    source_da,
    source_type,
    vpu,
):
    # Prepare to create UserTiffData object
    tx_name = 'x'
    ty_name = 'y'
    band = 1
    bname = 'band'
    crs = source_da.rio.crs  # or use the EPSG code you expect, e.g., 5070
    varname = source_type  # not currently used
    # id_feature = "hru_id"  # or your HRU ID field
    file_prefix=f"base_nhm_{source_type}_{vpu}_param_temp"

    # Create UserTiffData object
    data = UserTiffData(
        var=varname,
        ds=source_da,
        proj_ds=crs,
        x_coord=tx_name,
        y_coord=ty_name,
        band=band,
        bname=bname,
        f_feature=nhru_gdf,
        id_feature=id_feature
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="parallel",
        zonal_writer="csv",
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    print(stats)
    return file_prefix, stats


@app.cell
def _(file_prefix, output_path):
    zg_file = output_path / f"{file_prefix}.csv"
    if zg_file.exists():
        zg_file.unlink()
    return


@app.cell
def _(stats):
    stats
    return


@app.cell
def _(output_path, source_type, stats, vpu):
    mean_stats = stats[["mean"]].copy()
    mean_stats = mean_stats.rename(columns={"mean": "soil_moist_max"})
    mean_stats.to_csv(output_path / f"base_nhm_{source_type}_{vpu}_param.csv")
    return


if __name__ == "__main__":
    app.run()
