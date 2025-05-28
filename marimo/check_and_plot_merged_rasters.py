import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    # GeoTIFF Inspection Notebook

    This notebook allows you to:

    - Select a GeoTIFF file
    - Print all available statistics and metadata
    - Plot the raster with a colorbar

    ---
    **Instructions:**

    1. Set the path to your GeoTIFF file below.
    2. Run the notebook cells to inspect your data.
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    import rioxarray
    import numpy as np
    import matplotlib.pyplot as plt
    from pathlib import Path
    return Path, mo, plt, rioxarray


@app.cell
def _(mo):
    mo.md(r"""## 1. Set the path to your GeoTIFF file""")
    return


@app.cell
def _(Path):
    base_path = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/source_data/NHDPlus_Merged_Rasters")
    file_path = base_path / "01/NEDSnapshot_merged_slope_01.tif"
    if not file_path.exists():
        print(base_path.exists(), base_path)
        print(file_path.exists(), file_path)
        print("File path does not exist")

    return (file_path,)


@app.cell
def _():
    ## 2. Open and Inspect the GeoTIFF
    return


@app.cell
def _(file_path, rioxarray):
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    da = rioxarray.open_rasterio(file_path)
    return (da,)


@app.cell
def _():
     ## 3. Plot the Raster
    return


@app.cell
def _(da, file_path, plt):
    fill_value = da.attrs.get('_FillValue', None)
    if fill_value is None:
        fill_value = da.encoding.get('_FillValue', None)
    if fill_value is None:
        fill_value = da.attrs.get('missing_value', None)

    # Now plot, making NaN transparent
    cmap = plt.get_cmap("viridis").copy()
    cmap.set_bad(color='none')  # This makes np.nan transparent

    # Block-average downsampling (e.g., 10x10 blocks)
    da_coarse = da.coarsen(x=10, y=10, boundary="trim").mean()
    # Replace _FillValue with np.nan
    if fill_value is not None:
        da_coarse = da_coarse.where(da_coarse != fill_value)

    # Now plot, making NaN transparent
    cmap = plt.get_cmap("viridis").copy()
    cmap.set_bad(color='none')  # This makes np.nan transparent

    da_coarse.plot(figsize=(8, 6), cmap=cmap)
    plt.title(f"{file_path} (block-averaged)")
    plt.show()
    return (fill_value,)


@app.cell
def _(fill_value):
    fill_value
    return


if __name__ == "__main__":
    app.run()
