import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
        # Derived Rasters — Visual QA

        Inspect pre-computed rasters in `work/derived_rasters/`.
        All rasters are CONUS-scale (~124k × 167k px at 30 m).

        **Best practice:** uses rasterio's `out_shape` decimated read — only a
        thumbnail-resolution array is decompressed; if internal overviews exist,
        GDAL selects the best one automatically.
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

    import marimo as mo

    DATA_ROOT = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2")
    DERIVED_DIR = DATA_ROOT / "work" / "derived_rasters"
    DISPLAY_PX = 2000

    return DATA_ROOT, DERIVED_DIR, DISPLAY_PX, Path, mo, np, plt, rasterio, Resampling


@app.cell
def _(mo):
    mo.md("## Available rasters")
    return


@app.cell
def _(DERIVED_DIR, rasterio):
    SKIP = {"intermediate"}
    rows = []
    for _p in sorted(DERIVED_DIR.glob("*.tif")):
        if any(s in _p.stem for s in SKIP):
            continue
        _ok = False
        try:
            with rasterio.open(_p):
                _ok = True
        except Exception:
            pass
        _status = "✓" if _ok else "✗ (corrupt/partial)"
        rows.append(f"  {_status}  {round(_p.stat().st_size / 1024**2):>6} MB  {_p.name}")
    print("\n".join(rows))
    return (rows,)


@app.cell
def _(mo):
    mo.md("## Helper functions")
    return


@app.cell
def _(DISPLAY_PX, np, rasterio, Resampling):
    def raster_meta(path):
        with rasterio.open(path) as src:
            return {
                "shape": src.shape,
                "dtype": src.dtypes[0],
                "crs": src.crs,
                "bounds": src.bounds,
                "res_m": abs(src.transform.a),
                "nodata": src.nodata,
                "overviews": src.overviews(1),
                "size_MB": round(path.stat().st_size / 1024**2),
            }

    def decimated_read(path, target_px=DISPLAY_PX, resampling=Resampling.average):
        """
        Read a decimated thumbnail from a large raster.

        Uses rasterio out_shape → GDAL decimated read.
        Only the required pixels are decompressed; internal overviews used automatically.
        Returns (masked_array_2d, scaled_transform, meta_dict).
        """
        with rasterio.open(path) as src:
            factor = max(1, max(src.width, src.height) // target_px)
            out_h = max(1, src.height // factor)
            out_w = max(1, src.width // factor)
            data = src.read(
                1,
                out_shape=(out_h, out_w),
                resampling=resampling,
            ).astype(np.float64)
            nodata = src.nodata
            t = src.transform
            scaled_transform = t * t.scale(src.width / out_w, src.height / out_h)
            meta = raster_meta(path)
        mask = ~np.isfinite(data)
        if nodata is not None:
            mask |= data == nodata
        return np.ma.array(data, mask=mask), scaled_transform, meta

    def percentile_stretch(arr, lo=2, hi=98):
        valid = arr.compressed() if isinstance(arr, np.ma.MaskedArray) else arr[np.isfinite(arr)]
        if valid.size == 0:
            return arr, 0.0, 1.0
        vmin, vmax = np.percentile(valid, [lo, hi])
        return np.clip(arr, vmin, vmax), vmin, vmax

    return decimated_read, percentile_stretch, raster_meta


@app.cell
def _(decimated_read, percentile_stretch, plt, np, DISPLAY_PX):
    def plot_raster(path, title, cmap="viridis", units=""):
        """Display a large raster as a decimated overview with metadata and histogram."""
        if not path.exists():
            print(f"  Skipping {path.name} — not found yet")
            return None
        try:
            data, _, meta = decimated_read(path)
        except Exception as e:
            print(f"  Cannot read {path.name}: {e}")
            return None

        stretched, vmin, vmax = percentile_stretch(data)
        valid = data.compressed()

        fig, (ax_img, ax_hist) = plt.subplots(
            1, 2, figsize=(16, 6), gridspec_kw={"width_ratios": [3, 1]}
        )
        im = ax_img.imshow(stretched, cmap=cmap, vmin=vmin, vmax=vmax, interpolation="nearest")
        ax_img.set_title(f"{title}\n{path.name}  ({meta['size_MB']} MB on disk)", fontsize=11)
        ax_img.axis("off")
        plt.colorbar(im, ax=ax_img, fraction=0.03, pad=0.02, label=units)

        ax_hist.hist(valid, bins=100, color="steelblue", edgecolor="none", density=True)
        ax_hist.axvline(vmin, color="orange", linestyle="--", linewidth=1, label=f"p2={vmin:.3g}")
        ax_hist.axvline(vmax, color="red",    linestyle="--", linewidth=1, label=f"p98={vmax:.3g}")
        ax_hist.set_title("Value distribution (decimated)")
        ax_hist.set_xlabel(units or "value")
        ax_hist.set_ylabel("density")
        ax_hist.legend(fontsize=8)

        h, w = meta["shape"]
        info = (
            f"Full grid : {h:,} × {w:,} px\n"
            f"Pixel size: {meta['res_m']:.1f} m\n"
            f"Dtype     : {meta['dtype']}\n"
            f"NoData    : {meta['nodata']}\n"
            f"Overviews : {meta['overviews'] or 'none'}\n"
            f"Display   : {data.shape[1]} × {data.shape[0]} px\n"
            f"          (1/{max(1,max(w,h)//DISPLAY_PX)}× decimated)\n"
            f"\nValid px  : {valid.size:,}\n"
            f"Min       : {valid.min():.4g}\n"
            f"Mean      : {valid.mean():.4g}\n"
            f"Max       : {valid.max():.4g}\n"
            f"Std       : {valid.std():.4g}"
        )
        ax_img.text(
            1.01, 0.5, info, transform=ax_img.transAxes, fontsize=8,
            verticalalignment="center", family="monospace",
            bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8),
        )
        plt.tight_layout()
        return fig

    return (plot_raster,)


@app.cell
def _(mo):
    mo.md("## Resampled Root Depth (`rd_250_raw.tif`)\nResampled from 250 m to AWC grid. Units: cm.")
    return


@app.cell
def _(DERIVED_DIR, plot_raster):
    plot_raster(DERIVED_DIR / "rd_250_raw.tif", "Resampled Root Depth", cmap="YlOrBr", units="cm")


@app.cell
def _(mo):
    mo.md("## Soil Moisture Max (`soil_moist_max.tif`)\nRootDepth × AWC. Units: cm.")
    return


@app.cell
def _(DERIVED_DIR, plot_raster):
    plot_raster(DERIVED_DIR / "soil_moist_max.tif", "Soil Moisture Max (RootDepth × AWC)", cmap="Blues", units="cm")


@app.cell
def _(mo):
    mo.md("## Resampled Canopy Cover (`cnpy_resampled_lulc.tif`)\nCNPY resampled to match the LULC grid. Units: % cover (0–100).")
    return


@app.cell
def _(DERIVED_DIR, plot_raster):
    plot_raster(DERIVED_DIR / "cnpy_resampled_lulc.tif", "Canopy Cover (resampled to LULC grid)", cmap="Greens", units="%")


@app.cell
def _(mo):
    mo.md("## Resampled Winter Retention (`keep_resampled_lulc.tif`)\nPer-pixel winter retention resampled to LULC grid. Units: % (0–100).")
    return


@app.cell
def _(DERIVED_DIR, plot_raster):
    plot_raster(DERIVED_DIR / "keep_resampled_lulc.tif", "Winter Retention (resampled to LULC grid)", cmap="PuBuGn", units="%")


@app.cell
def _(mo):
    mo.md("## Radiation Transmission (`radtrn_nhm_v11.tif`)\n`cnpy × keep / 100` where `lulc ≥ 3` (tree classes), else 0. Dimensionless (0–1).")
    return


@app.cell
def _(DERIVED_DIR, plot_raster):
    plot_raster(DERIVED_DIR / "radtrn_nhm_v11.tif", "Radiation Transmission (NHM v1.1)", cmap="plasma", units="fraction (0–1)")


@app.cell
def _(mo):
    mo.md("## Side-by-side: canopy, keep, radtrn")
    return


@app.cell
def _(DERIVED_DIR, decimated_read, percentile_stretch, plt):
    _targets = [
        ("cnpy_resampled_lulc.tif",  "Canopy (%)",           "Greens"),
        ("keep_resampled_lulc.tif",  "Winter Retention (%)", "PuBuGn"),
        ("radtrn_nhm_v11.tif",       "Rad. Transmission",    "plasma"),
    ]
    _available = [(fn, lbl, cm) for fn, lbl, cm in _targets if (DERIVED_DIR / fn).exists()]
    if not _available:
        print("None of the comparison rasters are ready yet.")
    else:
        _fig, _axes = plt.subplots(1, len(_available), figsize=(7 * len(_available), 6))
        if len(_available) == 1:
            _axes = [_axes]
        for _ax, (_fn, _label, _cmap) in zip(_axes, _available):
            try:
                _data, _, _ = decimated_read(DERIVED_DIR / _fn)
                _stretched, _vmin, _vmax = percentile_stretch(_data)
                _im = _ax.imshow(_stretched, cmap=_cmap, vmin=_vmin, vmax=_vmax, interpolation="nearest")
                _ax.set_title(_label)
                _ax.axis("off")
                plt.colorbar(_im, ax=_ax, fraction=0.03, pad=0.02)
            except Exception as _e:
                _ax.set_title(f"{_label}\n(error: {_e})")
                _ax.axis("off")
        plt.suptitle("NHM v1.1 LULC-derived rasters — decimated overview", fontsize=13)
        plt.tight_layout()
        _fig


if __name__ == "__main__":
    app.run()
