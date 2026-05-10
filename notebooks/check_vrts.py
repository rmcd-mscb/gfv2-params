import marimo

__generated_with = "0.23.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # VRT Quick-Look: elevation · slope · aspect · fdr · twi

    Decimated overview read of the CONUS VRTs produced by `build_vrt.py`.
    Uses rasterio's `out_shape` to read a thumbnail (~500 px on the longest axis)
    so the full raster is never loaded into memory.
    """)
    return


@app.cell
def _():
    from pathlib import Path

    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling

    from gfv2_params.config import load_base_config

    NHD_MERGED = Path(load_base_config()["data_root"]) / "work" / "nhd_merged"
    TARGET_PX = 500

    # NED-based and NHDPlus rasters (existing, unchanged) plus the open-source
    # per-VPU products from compute_dem_derivatives.py (issue #52). The "twi"
    # VRT is now sourced from Twi_hydrodem_*.tif (richdem-fill + WBT D8 + numpy
    # TWI) — the per-RPU ArcPy `Twi_merged_*.tif` reference is kept on disk for
    # diff-vs-ground-truth in notebooks/diff_twi_hydrodem_vs_merged.py.
    LAYERS = [
        ("elevation",          "terrain",       "m",        2, 98),
        ("slope",              "YlOrRd",        "degrees",  2, 98),
        ("aspect",             "hsv",           "degrees",  0, 100),
        ("fdr",                "nipy_spectral", "D8 code",  0, 100),
        ("twi",                "viridis",       "unitless", 2, 98),
        ("slope_hydrodem",     "YlOrRd",        "degrees",  2, 98),
        ("slope_pct_hydrodem", "YlOrRd",        "% rise",   2, 98),
        ("aspect_hydrodem",    "hsv",           "degrees",  0, 100),
        ("fdr_hydrodem",       "nipy_spectral", "D8 code",  0, 100),
        ("fac_hydrodem",       "magma",         "cells",    2, 99),
    ]
    return LAYERS, NHD_MERGED, Resampling, TARGET_PX, np, plt, rasterio


@app.cell
def _(LAYERS, NHD_MERGED, Resampling, TARGET_PX, mo, np, plt, rasterio):
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
        n = len(LAYERS)
        ncols = min(n, 3)
        nrows = (n + ncols - 1) // ncols
        fig, axes = plt.subplots(
            nrows, ncols,
            figsize=(7.3 * ncols, 7 * nrows),
            squeeze=False,
        )
        flat_axes = axes.ravel()
        for ax, (name, cmap, unit, lo, hi) in zip(flat_axes, LAYERS):
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

        # Hide any unused panels in the trailing row.
        for ax in flat_axes[n:]:
            ax.axis("off")

        fig.suptitle(
            f"CONUS VRTs — {NHD_MERGED}",
            fontsize=11,
            y=1.01,
        )
        plt.tight_layout()
        _out = fig

    _out
    return


if __name__ == "__main__":
    app.run()
