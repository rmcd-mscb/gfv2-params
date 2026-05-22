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
    # carea_max / smidx_coef — TWI threshold sweep

    Tune the TWI threshold for the two PRMS depression-storage parameters and
    see the resulting per-HRU distributions **instantly** — no cluster reruns.

    `carea_max` and `smidx_coef` are the same per-HRU function evaluated at two
    thresholds:

    `f_hru(t) = clip( (n_perv_onstream + #(pervious non-onstream cells, TWI > t)) / n_perv , 0, 1 )`

    This notebook loads the pre-extracted artifact
    (`{data_root}/{fabric}/params/carea_twi_artifact.npz`, built by
    `scripts/build_carea_twi_artifact.py --fabric <f>`) and evaluates any
    candidate threshold against it.

    **Workflow:** set the two thresholds (absolute TWI **or** percentile), read the
    distribution / map / diffs, sweep a range, then paste the printed config
    snippet into the production config and run the pipeline.
    """)
    return


@app.cell
def _():
    from pathlib import Path

    import geopandas as gpd
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    from gfv2_params.config import load_base_config
    from gfv2_params.threshold_sweep import (
        CareaTwiArtifact,
        evaluate_threshold,
        sweep,
        value_to_percentile,
    )

    FABRIC = "oregon"
    _cfg = load_base_config(fabric=FABRIC)
    DATA_ROOT = Path(_cfg["data_root"])
    ARTIFACT_PATH = DATA_ROOT / FABRIC / "params" / "carea_twi_artifact.npz"
    HRU_GPKG = Path(_cfg["hru_gpkg"])
    HRU_LAYER = _cfg["hru_layer"]
    ID_FEATURE = _cfg["id_feature"]

    art = CareaTwiArtifact.load(ARTIFACT_PATH)
    return (
        ARTIFACT_PATH,
        FABRIC,
        HRU_GPKG,
        HRU_LAYER,
        ID_FEATURE,
        art,
        evaluate_threshold,
        gpd,
        np,
        pd,
        plt,
        sweep,
        value_to_percentile,
    )


@app.cell
def _(ARTIFACT_PATH, FABRIC, art, mo):
    mo.md(f"""
    **Loaded:** `{ARTIFACT_PATH}`
    fabric = **{FABRIC}**, twi_source = **{art.twi_source}**, HRUs = **{len(art.ids):,}**,
    bins = {len(art.bin_edges) - 1} over [{art.bin_edges[0]:.1f}, {art.bin_edges[-1]:.1f}] TWI.
    """)
    return


@app.cell
def _(mo):
    # Candidate thresholds (absolute TWI). Defaults = oregon D1 percentile thresholds.
    t_carea = mo.ui.number(value=8.94, start=0.0, stop=30.0, step=0.05, label="carea_max threshold (TWI)")
    t_smidx = mo.ui.number(value=15.15, start=0.0, stop=30.0, step=0.05, label="smidx_coef threshold (TWI)")
    mo.vstack([mo.md("### Candidate thresholds"), t_carea, t_smidx])
    return t_carea, t_smidx


@app.cell
def _(art, mo, t_carea, t_smidx, value_to_percentile):
    # Two-way readout: absolute <-> percentile of the valid-land reference distribution.
    _pc = value_to_percentile(art, t_carea.value)
    _ps = value_to_percentile(art, t_smidx.value)
    mo.md(
        f"""
        | param | threshold (TWI) | percentile of valid-land TWI |
        |---|---|---|
        | carea_max | {t_carea.value:.3f} | **P{_pc:.2f}** |
        | smidx_coef | {t_smidx.value:.3f} | **P{_ps:.2f}** |

        *(Legacy eyeballed values were 8.0 / 15.6.)*
        """
    )
    return


@app.cell
def _(art, evaluate_threshold, t_carea, t_smidx):
    carea = evaluate_threshold(art, t_carea.value)
    smidx = evaluate_threshold(art, t_smidx.value)
    return carea, smidx


@app.cell
def _(mo, np):
    # NOTE: marimo treats names starting with "_" as cell-local; a shared helper
    # must use a plain name and be returned so other cells can take it as an arg.
    def param_stats(name, p):
        return {
            "param": name,
            "mean": float(p.mean()),
            "median": float(np.median(p)),
            "frac_zero": float((p == 0.0).mean()),
            "frac_one": float((p >= 1.0).mean()),
        }

    mo.md("## View 1 — per-HRU distribution")
    return (param_stats,)


@app.cell
def _(carea, mo, param_stats, pd, smidx):
    mo.ui.table(
        pd.DataFrame([param_stats("carea_max", carea), param_stats("smidx_coef", smidx)]),
        selection=None,
    )
    return


@app.cell
def _(carea, plt, smidx):
    _fig, _ax = plt.subplots(1, 2, figsize=(11, 3.5))
    _ax[0].hist(carea, bins=40, color="tab:blue")
    _ax[0].set_title("carea_max")
    _ax[0].set_xlabel("per-HRU value")
    _ax[1].hist(smidx, bins=40, color="tab:green")
    _ax[1].set_title("smidx_coef")
    _ax[1].set_xlabel("per-HRU value")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## View 2 — spatial map
    """)
    return


@app.cell
def _(HRU_GPKG, HRU_LAYER, ID_FEATURE, art, carea, gpd, pd, plt, smidx):
    _gdf = gpd.read_file(HRU_GPKG, layer=HRU_LAYER)
    _vals = pd.DataFrame({ID_FEATURE: art.ids, "carea_max": carea, "smidx_coef": smidx})
    _gdf = _gdf.merge(_vals, on=ID_FEATURE, how="left")
    _fig, _ax = plt.subplots(1, 2, figsize=(12, 6))
    _gdf.plot(column="carea_max", ax=_ax[0], legend=True, vmin=0, vmax=1, cmap="viridis")
    _ax[0].set_title("carea_max")
    _ax[0].set_axis_off()
    _gdf.plot(column="smidx_coef", ax=_ax[1], legend=True, vmin=0, vmax=1, cmap="viridis")
    _ax[1].set_title("smidx_coef")
    _ax[1].set_axis_off()
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## View 3 — diff vs legacy 8.0 / 15.6 output

    Set `LEGACY_CSV` to a CSV with columns (`<id_feature>`, `carea_max`,
    `smidx_coef`) from a legacy absolute-threshold run. Inactive for oregon
    (no valid legacy output here); activates on VPU 01 / gfv2.
    """)
    return


@app.cell
def _(ID_FEATURE, art, carea, mo, pd, plt, smidx):
    from pathlib import Path as _Path

    LEGACY_CSV = ""  # e.g. "/path/to/nhm_carea_smidx_legacy.csv"
    if LEGACY_CSV and _Path(LEGACY_CSV).exists():
        _leg = pd.read_csv(LEGACY_CSV)
        _cur = pd.DataFrame({ID_FEATURE: art.ids, "carea_cur": carea, "smidx_cur": smidx})
        _m = _leg.merge(_cur, on=ID_FEATURE, how="inner")
        _fig, _ax = plt.subplots(1, 2, figsize=(10, 4))
        _ax[0].scatter(_m["carea_max"], _m["carea_cur"], s=4, alpha=0.3)
        _ax[0].plot([0, 1], [0, 1], "k--", lw=1)
        _ax[0].set(xlabel="legacy carea_max", ylabel="candidate", title="carea_max")
        _ax[1].scatter(_m["smidx_coef"], _m["smidx_cur"], s=4, alpha=0.3)
        _ax[1].plot([0, 1], [0, 1], "k--", lw=1)
        _ax[1].set(xlabel="legacy smidx_coef", ylabel="candidate", title="smidx_coef")
        _fig.tight_layout()
        _out = _fig
    else:
        _out = mo.md("_Legacy comparison N/A — set `LEGACY_CSV` to a valid path._")
    _out
    return


@app.cell
def _(mo):
    mo.md("""
    ## View 4 — diff vs existing NHM / gauge-tuned values

    Set `GAUGE_CSV` to a CSV with (`<id_feature>`, `carea_max`, `smidx_coef`)
    from a prior calibration to compare the candidate against the real target.
    """)
    return


@app.cell
def _(ID_FEATURE, art, carea, mo, pd, smidx):
    from pathlib import Path as _Path2

    GAUGE_CSV = ""  # e.g. "/path/to/nhm_calibrated_params.csv"
    if GAUGE_CSV and _Path2(GAUGE_CSV).exists():
        _g = pd.read_csv(GAUGE_CSV)
        _cur = pd.DataFrame({ID_FEATURE: art.ids, "carea_cur": carea, "smidx_cur": smidx})
        _m = _g.merge(_cur, on=ID_FEATURE, how="inner")
        _d_carea = (_m["carea_cur"] - _m["carea_max"]).abs().mean()
        _d_smidx = (_m["smidx_cur"] - _m["smidx_coef"]).abs().mean()
        _out = mo.md(
            f"Mean |Δ| vs gauge-tuned — carea_max: **{_d_carea:.4f}**, "
            f"smidx_coef: **{_d_smidx:.4f}** (n={len(_m):,})"
        )
    else:
        _out = mo.md("_Gauge comparison N/A — set `GAUGE_CSV` to a valid path._")
    _out
    return


@app.cell
def _(mo):
    mo.md("""
    ## Sweep curve — sensitivity of mean parameter to threshold
    """)
    return


@app.cell
def _(art, np, plt, sweep, t_carea, t_smidx):
    _grid = np.arange(4.0, 20.0, 0.25)
    _df = sweep(art, _grid)
    _fig, _ax = plt.subplots(figsize=(8, 4))
    _ax.plot(_df["threshold"], _df["mean"], label="mean param")
    _ax.plot(_df["threshold"], _df["frac_one"], label="frac == 1", ls="--")
    _ax.axvline(t_carea.value, color="tab:blue", lw=1, label=f"carea t={t_carea.value:.2f}")
    _ax.axvline(t_smidx.value, color="tab:green", lw=1, label=f"smidx t={t_smidx.value:.2f}")
    _ax.set(xlabel="TWI threshold", ylabel="value")
    _ax.legend()
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(art, mo, t_carea, t_smidx, value_to_percentile):
    # Persist: print the config snippets for the chosen value (both paths).
    _pc = value_to_percentile(art, t_carea.value)
    _ps = value_to_percentile(art, t_smidx.value)
    _abs_note = (
        f"The absolute values above were measured on the **{art.twi_source}** TWI distribution "
        f"and must be paired with that same source's VRT."
        + (
            "\n\n        > **Caution:** legacy absolute mode (8.0/15.6) is calibrated to ArcPy TWI. "
            "Because this artifact was built from a **hydrodem** source, prefer the percentile path "
            "above to avoid a source-mismatch bias."
            if art.twi_source != "arcpy" else ""
        )
    )
    mo.md(
        f"""
        ## Persist the chosen value

        **Percentile path** — `configs/shared_rasters/shared_rasters.yml`, `twi_reference` step:
        ```yaml
            percentiles:
              carea_max: {_pc:.2f}
              smidx: {_ps:.2f}
        ```
        then rerun `twi_reference` + the depstor `carea_map`.

        **Eyeball-absolute path** — `configs/depstor/depstor_rasters.yml`, `carea_map` step:
        ```yaml
            threshold_mode: absolute
            thresholds:
              carea_max: {t_carea.value:.2f}
              smidx: {t_smidx.value:.2f}
        ```
        {_abs_note}
        """
    )
    return


if __name__ == "__main__":
    app.run()
