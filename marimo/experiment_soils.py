import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    from pathlib import Path

    import geopandas as gpd
    import rioxarray
    from gdptools import UserTiffData, ZonalGen

    # Add the src directory to the Python path
    src_path = Path(__file__).resolve().parent.parent / "src"
    sys.path.append(str(src_path))

    from helpers import load_config
    return Path, UserTiffData, ZonalGen, gpd, load_config, rioxarray


@app.cell
def _():
    # Hardwire vpu for testing
    vpu = "01"
    return (vpu,)


@app.cell
def _(Path, load_config):
    config_file = "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/gfv2-params/configs/04_soils_param_config.yml"
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
def _(base_source_dir, gpd, rioxarray, source_type, target_source_dir, vpu):
    if source_type == "soils":
        raster_path = base_source_dir / "soils_litho/TEXT_PRMS.tif"
    else:
        raise ValueError(f"Unknown source type: {source_type}")
    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")

    # Target geopackage path
    gpkg_path = target_source_dir / f"NHM_{vpu}_draft.gpkg"
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")

    print(f"Raster: {raster_path}")
    print(f"GPKG:   {gpkg_path}")

    # Open the 'nhru' layer from the geopackage with geopandas
    nhru_gdf = gpd.read_file(gpkg_path, layer="nhru")
    id_feature = "hru_id"
    print(f"Loaded nhru layer: {len(nhru_gdf)} features")

    # Open the raster with rioxarray
    source_da = rioxarray.open_rasterio(raster_path, masked=False)
    print(f"Loaded raster: shape={source_da.shape}, crs={source_da.rio.crs}")
    return id_feature, nhru_gdf, source_da


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
    # These params are used to fill out the gdptools TiffAttributes class
    tx_name= 'x'
    ty_name = 'y'
    band = 1
    bname = 'band'
    crs = source_da.rio.crs #
    varname = "soils" # not currently used
    file_prefix=f"base_nhm_{source_type}_{vpu}_param_temp"
    user_data = UserTiffData(
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
        user_data=user_data,
        zonal_engine="serial",
        zonal_writer="csv",
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    return file_prefix, stats


@app.cell
def _(file_prefix, output_path):
    zg_file = output_path / f"{file_prefix}.csv"
    if zg_file.exists():
        zg_file.unlink()

    return


@app.cell
def _(stats):
    # List the columns that are categories
    category_cols = [col for col in stats.columns if str(col) not in ("count")]
    category_cols
    return (category_cols,)


@app.cell
def _(category_cols, stats):
    top_stats = stats.copy()
    # Find the column with the maximum value in each row
    top_stats['max_category'] = top_stats[category_cols].idxmax(axis=1)
    top_stats
    return (top_stats,)


@app.cell
def _(top_stats):
    result = top_stats[['max_category']]
    result = result.rename(columns={"max_category": "soils"})
    result
    return (result,)


@app.cell
def _(output_path, result, source_type, vpu):
    result.to_csv(output_path / f"base_nhm_{source_type}_{vpu}_param.csv")

    return


if __name__ == "__main__":
    app.run()
