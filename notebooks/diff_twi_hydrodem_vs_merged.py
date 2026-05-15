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
    # TWI: open-source (Hydrodem-derived) vs ArcPy (`Twi_merged`) — characterization

    Pixel-wise comparison between the open-source `Twi_hydrodem_<vpu>.tif`
    (produced by `scripts/compute_dem_derivatives.py`: hybrid pipeline —
    richdem `FillDepressions+epsilon` → WhiteboxTools `D8Pointer --esri_pntr`
    → `D8FlowAccumulation` → richdem slope/aspect on the fixed DEM, then
    masked to the per-VPU HRU mask `work/nhd_merged/<vpu>/land_mask_<vpu>.tif`)
    and the ArcPy reference `Twi_merged_<vpu>.tif` (per-RPU TWI tiles merged
    per-VPU by PR #50, masked to the same per-VPU HRU mask).

    **The ArcPy `Twi_merged` remains the canonical TWI** for downstream PRMS
    parameter extraction. The downstream consumer (`carea_max`, `smidx_coef`
    in `0b_TB_depr_stor.py`) thresholds TWI at calibrated values (8.0 and 15.6)
    that depend on the original ArcPy distribution shape; swapping the TWI
    source would invalidate those thresholds. This notebook characterizes
    *where and how* the two products differ — useful background for any
    future recalibration or for downstream consumers that don't depend on
    the calibrated thresholds.

    Known sources of divergence in the open-source product:
    - **Flat-basin / endorheic artifact**: richdem epsilon imprints a synthetic
      gradient across flats; D8 routes all flat-region drainage into a single
      spillover. Visible as a wide bright "rind" in VPU 09 (Souris-Red-Rainy
      prairie) and as artificial channels in VPU 18 (Mojave, Death Valley).
      ArcPy avoided this by computing flow per-RPU.
    - **Slope-algorithm differences**: richdem Horn vs Esri Horn produce
      slightly different slope distributions at edges and on filled cells.
    - **Per-RPU `slope.tif`** distributed by Esri reports max ≈ 151 (impossible
      for true degrees) — it's unclear precisely what slope encoding ArcPy
      used internally.
    """)
    return


@app.cell
def _():
    from pathlib import Path

    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.windows import from_bounds as window_from_bounds

    from gfv2_params.config import load_base_config

    NHD_MERGED = Path(load_base_config()["data_root"]) / "work" / "nhd_merged"
    VPUS = [f"{i:02d}" for i in range(1, 19)]
    TARGET_PX = 500

    def _intersect_bounds(path_a, path_b):
        """Return the spatial intersection of two rasters' bounds (assumes same CRS)."""
        with rasterio.open(path_a) as a, rasterio.open(path_b) as b:
            assert a.crs == b.crs, f"CRS mismatch: {a.crs} vs {b.crs}"
            return (
                max(a.bounds.left, b.bounds.left),
                max(a.bounds.bottom, b.bounds.bottom),
                min(a.bounds.right, b.bounds.right),
                min(a.bounds.top, b.bounds.top),
            )

    def _read_window_decimated(path, bounds, target_px, resampling=Resampling.nearest):
        """Read `path` clipped to `bounds` at a target longest-axis pixel count."""
        with rasterio.open(path) as src:
            win = window_from_bounds(*bounds, transform=src.transform)
            win = win.round_offsets().round_lengths()
            factor = max(1, max(int(win.height), int(win.width)) // target_px)
            h = max(1, int(win.height) // factor)
            w = max(1, int(win.width) // factor)
            data = src.read(
                1, window=win, out_shape=(h, w), resampling=resampling,
            ).astype(np.float32)
            nd = src.nodata
        mask = ((data == nd) if nd is not None else False) | ~np.isfinite(data)
        return np.ma.array(data, mask=mask)

    return (
        NHD_MERGED, TARGET_PX, VPUS,
        _intersect_bounds, _read_window_decimated,
        np, plt, rasterio,
    )


@app.cell
def _(NHD_MERGED, VPUS, mo):
    rows = []
    for _vpu in VPUS:
        _ref = NHD_MERGED / _vpu / f"Twi_merged_{_vpu}.tif"
        _new = NHD_MERGED / _vpu / f"Twi_hydrodem_{_vpu}.tif"
        rows.append({
            "vpu": _vpu,
            "Twi_merged": _ref.exists(),
            "Twi_hydrodem": _new.exists(),
            "both": _ref.exists() and _new.exists(),
        })
    _both = [r for r in rows if r["both"]]
    _only_ref = [r["vpu"] for r in rows if r["Twi_merged"] and not r["Twi_hydrodem"]]
    _only_new = [r["vpu"] for r in rows if r["Twi_hydrodem"] and not r["Twi_merged"]]
    mo.md(
        f"**Pairs available:** {len(_both)} of 18 VPUs"
        + (f"; **only ArcPy:** {', '.join(_only_ref)}" if _only_ref else "")
        + (f"; **only open-source:** {', '.join(_only_new)}" if _only_new else "")
        + (" — all paired ✓" if not _only_ref and not _only_new else "")
    )
    return (rows,)


@app.cell
def _(NHD_MERGED, VPUS, _intersect_bounds, _read_window_decimated, np):
    # Per-VPU diff stats. Twi_merged and Twi_hydrodem can have different bounds
    # (Twi_merged is per-RPU TWI tiles merged; Twi_hydrodem covers the wider
    # Hydrodem footprint), so we intersect spatially before differencing.
    stats_rows = []
    diff_samples = {}  # vpu -> 1D array of decimated diff values
    for _vpu in VPUS:
        _ref = NHD_MERGED / _vpu / f"Twi_merged_{_vpu}.tif"
        _new = NHD_MERGED / _vpu / f"Twi_hydrodem_{_vpu}.tif"
        if not (_ref.exists() and _new.exists()):
            continue
        _bounds = _intersect_bounds(_ref, _new)
        _r = _read_window_decimated(_ref, _bounds, target_px=1000)
        _n = _read_window_decimated(_new, _bounds, target_px=1000)
        _h = min(_r.shape[0], _n.shape[0])
        _w = min(_r.shape[1], _n.shape[1])
        _r = _r[:_h, :_w]
        _n = _n[:_h, :_w]
        _common_mask = _r.mask | _n.mask
        _diff = _n - _r
        _diff_valid = np.asarray(_diff[~_common_mask])
        if _diff_valid.size == 0:
            continue
        diff_samples[_vpu] = _diff_valid
        _abs = np.abs(_diff_valid)
        stats_rows.append({
            "vpu": _vpu,
            "n_valid": int(_diff_valid.size),
            "mean": float(_diff_valid.mean()),
            "mean_abs": float(_abs.mean()),
            "std": float(_diff_valid.std()),
            "p1": float(np.percentile(_diff_valid, 1)),
            "p50": float(np.percentile(_diff_valid, 50)),
            "p95": float(np.percentile(_diff_valid, 95)),
            "p99": float(np.percentile(_diff_valid, 99)),
            "frac_within_0.1": float((_abs <= 0.1).mean()),
        })
    return diff_samples, stats_rows


@app.cell
def _(mo, stats_rows):
    import pandas as pd

    if not stats_rows:
        _out = mo.md("No paired VPUs available yet — run `compute_dem_derivatives` first.")
    else:
        _df = pd.DataFrame(stats_rows)
        for _c in ("mean", "mean_abs", "std", "p1", "p50", "p95", "p99",
                   "frac_within_0.1"):
            _df[_c] = _df[_c].round(4)
        _hdr = mo.md(
            "**Per-VPU diff stats** (decimated ≈1000 px) — `Twi_hydrodem - Twi_merged`. "
            "`mean_abs` is mean(|diff|); `frac_within_0.1` is for context only — pixel-level "
            "parity isn't the goal."
        )
        _out = mo.vstack([_hdr, mo.ui.table(_df, page_size=18, selection=None)])
    _out
    return


@app.cell
def _(NHD_MERGED, TARGET_PX, VPUS, _intersect_bounds, _read_window_decimated, np, plt):
    _paired = []
    for v in VPUS:
        _ref = NHD_MERGED / v / f"Twi_merged_{v}.tif"
        _new = NHD_MERGED / v / f"Twi_hydrodem_{v}.tif"
        if _ref.exists() and _new.exists():
            _paired.append((v, _ref, _new))

    if not _paired:
        _out = "No paired outputs yet."
    else:
        _ncols = 3  # ArcPy / open-source / diff
        _nrows = len(_paired)
        _fig, _axes = plt.subplots(_nrows, _ncols,
                                   figsize=(4 * _ncols, 3 * _nrows),
                                   squeeze=False)

        # Pre-compute aligned thumbnails per VPU, then global TWI stretch from union
        _thumbs = []  # list of (vpu, r, n, diff)
        _all_twi = []
        for v, _ref_p, _new_p in _paired:
            _bounds = _intersect_bounds(_ref_p, _new_p)
            _r = _read_window_decimated(_ref_p, _bounds, target_px=TARGET_PX)
            _n = _read_window_decimated(_new_p, _bounds, target_px=TARGET_PX)
            _h = min(_r.shape[0], _n.shape[0])
            _w = min(_r.shape[1], _n.shape[1])
            _r = _r[:_h, :_w]
            _n = _n[:_h, :_w]
            _diff = _n - _r
            _thumbs.append((v, _r, _n, _diff))
            if _r.count() > 0:
                _all_twi.append(_r.compressed())
            if _n.count() > 0:
                _all_twi.append(_n.compressed())
        _all_twi = np.concatenate(_all_twi) if _all_twi else np.array([0, 1])
        _vmin, _vmax = np.percentile(_all_twi, [2, 98])

        for _i, (v, _r, _n, _diff) in enumerate(_thumbs):

            _ax = _axes[_i, 0]
            _ax.imshow(_r, cmap="viridis", vmin=_vmin, vmax=_vmax,
                       interpolation="nearest", rasterized=True)
            _ax.set_title(f"VPU {v}: ArcPy", fontsize=10)
            _ax.axis("off")

            _ax = _axes[_i, 1]
            _ax.imshow(_n, cmap="viridis", vmin=_vmin, vmax=_vmax,
                       interpolation="nearest", rasterized=True)
            _ax.set_title(f"VPU {v}: open-source", fontsize=10)
            _ax.axis("off")

            _ax = _axes[_i, 2]
            _im = _ax.imshow(_diff, cmap="RdBu", vmin=-2, vmax=2,
                             interpolation="nearest", rasterized=True)
            _ax.set_title(f"VPU {v}: diff (open-source - ArcPy)", fontsize=10)
            _ax.axis("off")

        _fig.suptitle(
            f"TWI per-VPU comparison (TWI stretch p2-p98 = {_vmin:.2f}–{_vmax:.2f}; diff stretch ±2)",
            fontsize=12,
        )
        _fig.colorbar(_im, ax=_axes[:, 2], fraction=0.04, pad=0.02, label="diff (TWI)")
        _out = _fig
    _out
    return


@app.cell
def _(diff_samples, np, plt):
    if not diff_samples:
        _out = "No paired outputs yet."
    else:
        _all = np.concatenate(list(diff_samples.values()))
        _abs = np.abs(_all)
        _frac_01 = float((_abs <= 0.1).mean())
        _fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(14, 4))

        _ax1.hist(_all, bins=200, range=(-3, 3), color="steelblue", edgecolor="white")
        _ax1.axvline(-0.1, color="red", linestyle="--", alpha=0.5, label="±0.1 threshold")
        _ax1.axvline(0.1, color="red", linestyle="--", alpha=0.5)
        _ax1.set_xlabel("TWI diff (open-source - ArcPy)")
        _ax1.set_ylabel("pixel count (decimated)")
        _ax1.set_title(
            f"CONUS-wide diff distribution from {len(diff_samples)} VPUs "
            f"(n={len(_all):,}; {_frac_01*100:.1f}% within ±0.1)"
        )
        _ax1.legend()

        _bins = np.logspace(-3, 1, 60)
        _ax2.hist(_abs, bins=_bins, color="darkorange", edgecolor="white")
        _ax2.set_xscale("log")
        _ax2.axvline(0.1, color="red", linestyle="--", alpha=0.5, label="0.1 threshold")
        _ax2.set_xlabel("|TWI diff|  (log scale)")
        _ax2.set_ylabel("pixel count")
        _ax2.set_title("|diff| distribution (log scale)")
        _ax2.legend()
        _out = _fig
    _out
    return


if __name__ == "__main__":
    app.run()
