import marimo

__generated_with = "0.13.11"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
        # VRT Quick-Look: elevation · slope · aspect

        Decimated overview read of the three CONUS VRTs produced by `build_vrt.py`.
        Uses rasterio's `out_shape` to read a thumbnail (~2000 px on the longest axis)
        so the full raster is never loaded into memory.
        """
    )
    return


@app.cell
def _():
    from pathlib import Path

    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling

    NHD_MERGED = Path(
        "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/work/nhd_merged"
    )
    TARGET_PX = 500

    LAYERS = [
        ("elevation", "terrain",  "m",       2, 98),
        ("slope",     "YlOrRd",   "degrees", 2, 98),
        ("aspect",    "hsv",      "degrees", 0, 100),
    ]

    return NHD_MERGED, LAYERS, TARGET_PX, Path, plt, np, rasterio, Resampling


@app.cell
def _(NHD_MERGED, LAYERS, TARGET_PX, plt, np, rasterio, Resampling, mo):
    def _read_overview(path, target_px=TARGET_PX):
        """Return a masked 2-D float32 thumbnail of *path*."""
        with rasterio.open(path) as src:
            factor = max(1, max(src.width, src.height) // target_px)
            out_h = max(1, src.height // factor)
            out_w = max(1, src.width  // factor)
            data = src.read(
                1,
                out_shape=(out_h, out_w),
                resampling=Resampling.nearest,
            ).astype(np.float32)
            nd = src.nodata
        mask = ~np.isfinite(data)
        if nd is not None:
            mask |= data == nd
        return np.ma.array(data, mask=mask)

    missing = [
        name for name, *_ in LAYERS
        if not (NHD_MERGED / f"{name}.vrt").exists()
    ]
    if missing:
        _out = mo.callout(
            mo.md(f"**Missing VRTs:** {', '.join(missing)}\n\nRun `build_vrt.py` first."),
            kind="warn",
        )
    else:
        fig, axes = plt.subplots(1, 3, figsize=(22, 7))
        for ax, (name, cmap, unit, lo, hi) in zip(axes, LAYERS):
            arr = _read_overview(NHD_MERGED / f"{name}.vrt")
            valid = arr.compressed()
            vmin, vmax = np.percentile(valid, [lo, hi])
            im = ax.imshow(
                np.clip(arr, vmin, vmax),
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
                interpolation="nearest",
                rasterized=True,
            )
            ax.set_title(
                f"{name.title()}\n"
                f"min={valid.min():.3g}  mean={valid.mean():.3g}  max={valid.max():.3g}  ({unit})",
                fontsize=10,
            )
            ax.axis("off")
            fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label=unit)

        fig.suptitle(
            f"CONUS VRTs — {NHD_MERGED}",
            fontsize=11,
            y=1.01,
        )
        plt.tight_layout()
        _out = fig

    _out
