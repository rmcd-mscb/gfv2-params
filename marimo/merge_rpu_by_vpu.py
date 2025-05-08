import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    # Merge RPU NedSnapshot for VPU 14
    This is a test for reading the raw NHDPlus files and merging the NedSnapshot files into a single .tiff for later processing workflows.
    """
    )
    return


@app.cell
def _():
    import yaml
    import rioxarray as rxr
    from pathlib import Path
    from rioxarray.merge import merge_arrays
    import matplotlib.pyplot as plt
    import sys
    import marimo as mo
    import pandas as pd
    import numpy as np
    # Add the src directory to the Python path
    src_path = Path(__file__).resolve().parent.parent / "src"
    sys.path.append(str(src_path))

    # Now you can import helpers
    from helpers import load_config

    return Path, load_config, merge_arrays, mo, np, rxr


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Input/output config files

    We use config files to specify the input/output and other parameters as necessary
    """
    )
    return


@app.cell
def _(Path, load_config):
    # cell: load a config file using your helper
    config = load_config(Path("configs/config_merge_rpu_by_vpu.yml"))
    config
    return (config,)


@app.cell
def _(Path, config):
    input_dirs = [Path(p) for p in config["inputs"]["rpus"]]
    output_file = Path(config["output"]["file"]) 
    return input_dirs, output_file


@app.cell
def _(mo):
    mo.md(r"""## Open and append the datasets using rioxarray""")
    return


@app.cell
def _(Path, input_dirs, rxr):
    datasets = []
    for d in input_dirs:
        d = Path(d)
        if not d.exists():
            raise FileNotFoundError(f"Input raster folder not found: {d}")

        # Check for .adf to confirm it's a raster dataset
        if not (d / "hdr.adf").exists():
            raise ValueError(f"Folder {d} does not appear to be a valid ESRI Grid raster")

        print(f"Reading raster from: {d}")
        ds = rxr.open_rasterio(str(d), masked=True).squeeze()
        datasets.append(ds)

    return (datasets,)


@app.cell
def _(mo):
    mo.md(r"""## Merge the datasets""")
    return


@app.cell
def _(datasets, merge_arrays):
    merged = merge_arrays(datasets)
    merged.rio.write_crs(datasets[0].rio.crs, inplace=True)
    return (merged,)


@app.cell
def _(mo):
    mo.md(r"""## Output the merged .tiff""")
    return


@app.cell
def _(merged, output_file):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    merged.rio.to_raster(output_file)

    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Open the merged .tiff and plot for verfication

    The merfed tif is large so we use plotly, and datashader to create a plot.  Otherwise it would be much too large to render in the notebook.
    """
    )
    return


@app.cell
def _(Path, rxr):
    merged_path = Path("output/NHDPlusNED_14_merged.tif")

    # Read using rioxarray
    tif_file = rxr.open_rasterio(merged_path, masked=True).squeeze()

    # Confirm it's loaded
    print(f"Shape: {tif_file.shape}, dtype: {tif_file.dtype}, CRS: {tif_file.rio.crs}")
    return


@app.cell
def _(merged):
    import datashader as pds
    import datashader.transfer_functions as tf
    from datashader.colors import Elevation

    # Prepare raster
    raster = merged.squeeze()
    raster.name = "elevation"

    # Define bounds from xarray coords
    x_range = (float(raster.x.min()), float(raster.x.max()))
    y_range = (float(raster.y.min()), float(raster.y.max()))

    # Create canvas and render
    canvas = pds.Canvas(plot_width=1000, plot_height=800,
                       x_range=x_range, y_range=y_range)
    agg = canvas.raster(raster)

    img = tf.shade(agg, cmap=Elevation, how='linear')
    img_pil = img.to_pil()

    return (img_pil,)


@app.cell
def _(img_pil, np):
    import plotly.graph_objs as go

    # Convert image to numpy array (RGB)
    # Flip vertically to match geographic orientation
    img_np = np.array(img_pil)[::-1, :, :]

    # Create plotly image trace
    trace = go.Image(z=img_np)

    layout = go.Layout(
        title="Datashader-rendered Raster with Plotly (Aspect Corrected)",
        xaxis=dict(
            title="Longitude",
            showgrid=False,
            scaleanchor="y",  # Tie x scale to y
            scaleratio=1,     # 1:1 scaling
        ),
        yaxis=dict(
            title="Latitude",
            showgrid=False,
            scaleanchor="x",  # Tie y scale to x
            scaleratio=1,     # Optional, same effect here
        ),
        autosize=False,
        width=800,
        height=650,
    )


    fig = go.Figure(data=[trace], layout=layout)


    return (fig,)


@app.cell
def _(fig, mo):
    plot = mo.ui.plotly(fig)
    plot
    return


if __name__ == "__main__":
    app.run()
