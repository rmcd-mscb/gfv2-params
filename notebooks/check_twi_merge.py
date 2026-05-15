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
    # TWI per-VPU QA/QC (open-source pipeline)

    Sanity checks for the per-VPU `Twi_merged_<vpu>.tif` outputs produced by
    `slurm_batch/compute_dem_derivatives.batch` (issue #52: open-source
    reproduction of the ArcPy TWI recipe via all-richdem fill + D8
    FlowAccumulation + numpy log formula, then clipped to the per-VPU HRU
    mask `work/nhd_merged/<vpu>/land_mask_<vpu>.tif`). Confirms presence,
    metadata, value ranges, and renders decimated thumbnails so flow-routing
    artifacts (e.g., endorheic-basin spillways, cross-RPU drainage corridors)
    are visible.

    Expected after a complete run: 18 outputs under
    `{data_root}/work/nhd_merged/<vpu>/Twi_merged_<vpu>.tif`, each Float32 with
    nodata=-9999 and valid values roughly in [-2, 26]. Some negative TWI is
    expected for very steep low-fac cells; cells with slope ≥ 89° are masked
    in compute_dem_derivatives.py to suppress fill artifacts in deep closed
    basins.

    For pixel-wise comparison against the ArcPy `Twi_merged_<vpu>.tif`
    reference, see `notebooks/diff_twi_hydrodem_vs_merged.py`.
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
    VPUS = [f"{i:02d}" for i in range(1, 19)]
    TARGET_PX = 500
    return NHD_MERGED, Resampling, TARGET_PX, VPUS, np, plt, rasterio


@app.cell
def _(NHD_MERGED, VPUS, mo, rasterio):
    rows = []
    for _vpu in VPUS:
        _path = NHD_MERGED / _vpu / f"Twi_merged_{_vpu}.tif"
        if not _path.exists():
            rows.append({"vpu": _vpu, "exists": False, "size_mb": None,
                         "dtype": None, "nodata": None, "shape": None})
            continue
        with rasterio.open(_path) as _src:
            rows.append({
                "vpu": _vpu,
                "exists": True,
                "size_mb": round(_path.stat().st_size / 1e6, 1),
                "dtype": _src.dtypes[0],
                "nodata": _src.nodata,
                "shape": f"{_src.width} x {_src.height}",
            })

    _missing = [r["vpu"] for r in rows if not r["exists"]]
    _present = [r["vpu"] for r in rows if r["exists"]]
    mo.md(
        f"**Presence:** {len(_present)} of 18 VPUs present"
        + (f"; **missing:** {', '.join(_missing)}" if _missing else " — all good ✓")
    )
    return (rows,)


@app.cell
def _(mo, rows):
    import pandas as pd

    _df = pd.DataFrame(rows)
    mo.ui.table(_df, page_size=18, selection=None)
    return


@app.cell
def _(NHD_MERGED, VPUS, mo, np, rasterio):
    # Compute valid-pixel statistics per VPU. Read at full resolution but
    # decimated via overview level 4 (~16x downsample) to keep memory bounded.
    stats_rows = []
    for _vpu in VPUS:
        _path = NHD_MERGED / _vpu / f"Twi_merged_{_vpu}.tif"
        if not _path.exists():
            continue
        with rasterio.open(_path) as _src:
            _factor = max(1, max(_src.width, _src.height) // 1000)
            _data = _src.read(
                1,
                out_shape=(max(1, _src.height // _factor),
                           max(1, _src.width // _factor)),
            ).astype(np.float32)
            _nd = _src.nodata
        _valid = _data[(_data != _nd) & np.isfinite(_data)]
        if _valid.size == 0:
            stats_rows.append({"vpu": _vpu, "n_valid": 0,
                               "min": None, "max": None,
                               "mean": None, "std": None,
                               "p1": None, "p99": None})
            continue
        stats_rows.append({
            "vpu": _vpu,
            "n_valid": int(_valid.size),
            "min": float(_valid.min()),
            "max": float(_valid.max()),
            "mean": float(_valid.mean()),
            "std": float(_valid.std()),
            "p1": float(np.percentile(_valid, 1)),
            "p99": float(np.percentile(_valid, 99)),
        })

    import pandas as pd
    _df = pd.DataFrame(stats_rows)
    if len(_df) > 0:
        for _c in ("min", "max", "mean", "std", "p1", "p99"):
            _df[_c] = _df[_c].round(3)
        # Flag VPUs outside the expected ~[-2, 28] range. Some negative TWI is
        # expected for very steep low-fac cells; values < -10 or > 30 suggest
        # fill artifacts that escaped the slope < 89° filter.
        _suspicious = _df[(_df["min"] < -10) | (_df["max"] > 30)]
        _hdr = mo.md(
            f"**Value-range stats** ({len(_df)} VPUs sampled at ~1000-px resolution).  "
            + (
                f"⚠️ **{len(_suspicious)} VPU(s) outside the expected ~[-2, 28] range**: {list(_suspicious['vpu'])}"
                if len(_suspicious) > 0 else
                "All VPUs within the expected ~[-2, 28] range ✓"
            )
        )
        _out = mo.vstack([_hdr, mo.ui.table(_df, page_size=18, selection=None)])
    else:
        _out = mo.md("No outputs available yet — run the merge first.")
    _out
    return


@app.cell
def _(NHD_MERGED, Resampling, TARGET_PX, VPUS, np, plt, rasterio):
    def _read_thumb(path, target_px=TARGET_PX):
        with rasterio.open(path) as src:
            factor = max(1, max(src.width, src.height) // target_px)
            data = src.read(
                1,
                out_shape=(max(1, src.height // factor),
                           max(1, src.width // factor)),
                resampling=Resampling.average,
            ).astype(np.float32)
            nd = src.nodata
        mask = (data == nd) | ~np.isfinite(data)
        return np.ma.array(data, mask=mask)

    _available = [(v, NHD_MERGED / v / f"Twi_merged_{v}.tif") for v in VPUS]
    _available = [(v, p) for v, p in _available if p.exists()]

    if not _available:
        _out = "No outputs available yet — run the merge first."
    else:
        _ncols = 6
        _nrows = (len(_available) + _ncols - 1) // _ncols
        _fig, _axes = plt.subplots(_nrows, _ncols,
                                   figsize=(3 * _ncols, 3 * _nrows),
                                   squeeze=False)
        # Find a global stretch from the union of valid pixels (use 2/98).
        _all_valid = []
        _thumbs = []
        for v, p in _available:
            t = _read_thumb(p)
            _thumbs.append((v, t))
            if t.count() > 0:
                _all_valid.append(t.compressed())
        _all_valid = np.concatenate(_all_valid) if _all_valid else np.array([0, 1])
        _vmin, _vmax = np.percentile(_all_valid, [2, 98])

        for _i, (v, t) in enumerate(_thumbs):
            _ax = _axes[_i // _ncols, _i % _ncols]
            _im = _ax.imshow(t, cmap="viridis", vmin=_vmin, vmax=_vmax,
                             interpolation="nearest", rasterized=True)
            _ax.set_title(f"VPU {v}", fontsize=10)
            _ax.axis("off")

        # Hide unused panels
        for _j in range(len(_thumbs), _nrows * _ncols):
            _axes[_j // _ncols, _j % _ncols].axis("off")

        _fig.suptitle(
            f"TWI per-VPU thumbnails (global stretch p2-p98 = {_vmin:.2f} to {_vmax:.2f})",
            fontsize=12,
        )
        _fig.colorbar(_im, ax=_axes, fraction=0.015, pad=0.02, label="TWI")
        _out = _fig
    _out
    return


@app.cell
def _(NHD_MERGED, VPUS, np, plt, rasterio):
    # CONUS-wide histogram of valid pixels across all available VPUs.
    _samples = []
    for v in VPUS:
        p = NHD_MERGED / v / f"Twi_merged_{v}.tif"
        if not p.exists():
            continue
        with rasterio.open(p) as src:
            _factor = max(1, max(src.width, src.height) // 600)
            d = src.read(
                1,
                out_shape=(max(1, src.height // _factor),
                           max(1, src.width // _factor)),
            ).astype(np.float32)
            _nd = src.nodata
        valid = d[(d != _nd) & np.isfinite(d)]
        _samples.append(valid)

    if _samples:
        all_valid = np.concatenate(_samples)
        _fig, _ax = plt.subplots(figsize=(9, 4))
        _ax.hist(all_valid, bins=120, color="steelblue", edgecolor="white")
        _ax.set_xlabel("TWI")
        _ax.set_ylabel("pixel count (decimated)")
        _ax.set_title(
            f"CONUS-wide TWI distribution from {len(_samples)} VPUs "
            f"(decimated; n={len(all_valid):,})"
        )
        _ax.axvline(-10, color="red", linestyle="--", alpha=0.5,
                    label="-10 (suspect below)")
        _ax.axvline(30, color="red", linestyle="--", alpha=0.5,
                    label="30 (suspect above)")
        _ax.legend()
        _out = _fig
    else:
        _out = "No outputs available yet — run the merge first."
    _out
    return


if __name__ == "__main__":
    app.run()
