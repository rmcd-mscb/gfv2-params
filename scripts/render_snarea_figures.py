"""Generate figures for the SNODAS snow-depletion-curve presentation deck.

Headless matplotlib. Reads the snarea pipeline's outputs for a fabric and
writes PNGs to docs/figures/snarea/<fabric>/. Re-run to refresh after the
pipeline regenerates outputs. Data-free schematics need no fabric.

Run:
    pixi run -e notebooks python scripts/render_snarea_figures.py \
        --fabric oregon --output-dir docs/figures/snarea/oregon
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import xarray as xr  # noqa: E402

from gfv2_params.config import load_config, require_config_key  # noqa: E402
from gfv2_params.snarea import representative as rep  # noqa: E402
from gfv2_params.snarea import season  # noqa: E402
from gfv2_params.snarea.library import CURVE_COLS  # noqa: E402


def schematic_concept(out_path: Path) -> None:
    """Slide-2 concept: a generic snow depletion curve, plain-English axes."""
    x = np.linspace(0, 1, 200)
    y = np.clip(1 - (1 - x) ** 1.8, 0, 1)  # illustrative gradual depletion
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.plot(x, y, lw=3, color="#1f6fb4")
    ax.fill_between(x, y, color="#1f6fb4", alpha=0.12)
    ax.set_xlabel("Fraction of peak snow remaining  (melting →, right to left)")
    ax.set_ylabel("Fraction of the HRU still snow-covered")
    ax.set_title("A snow depletion curve")
    ax.set_xlim(1, 0)  # peak on the left, snow-free on the right
    ax.set_ylim(0, 1.02)
    ax.annotate("full snow cover", xy=(0.95, 0.98), fontsize=9, color="#444")
    ax.annotate("patchy, then bare", xy=(0.08, 0.06), fontsize=9, color="#444")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def schematic_pipeline(out_path: Path) -> None:
    """Slide-6 pipeline DAG: SNODAS → Stage1 → Stage2 → Stage3 → PRMS params."""
    boxes = [
        ("SNODAS\ndaily SWE (~1 km)", "#dbe9f6"),
        ("Stage 1\naggregate to HRUs\n(swe, snow-cover, sub-grid CV)", "#cfe6d4"),
        ("Stage 2\nempirical curves\n(Driscoll 2017)", "#cfe6d4"),
        ("Stage 3\nCV/lognormal library\n(Sexstone 2020)", "#cfe6d4"),
        ("PRMS / pyWatershed\nsnarea_curve, hru_deplcrv,\nsnarea_thresh", "#f6e7cf"),
    ]
    fig, ax = plt.subplots(figsize=(12, 2.6))
    ax.axis("off")
    n = len(boxes)
    slot = 1 / n
    pad = 0.012  # horizontal padding inside each slot
    box_w = slot - 2 * pad
    for i, (label, color) in enumerate(boxes):
        left = i * slot + pad
        ax.add_patch(
            plt.Rectangle(
                (left, 0.28),
                box_w,
                0.44,
                facecolor=color,
                edgecolor="#555",
                lw=1.2,
            )
        )
        ax.text(left + box_w / 2, 0.5, label, ha="center", va="center", fontsize=9)
        if i < n - 1:
            # Arrow spans the gap from this box's right edge to the next box's left edge.
            ax.annotate(
                "",
                xy=((i + 1) * slot + pad, 0.5),  # next box left edge
                xytext=(left + box_w, 0.5),  # this box right edge
                arrowprops=dict(arrowstyle="-|>", color="#555", lw=1.8),
            )
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------
# Data-driven figures (read the snarea pipeline's outputs for one fabric)
# --------------------------------------------------------------------------

#: Config paths (relative to repo root — run the script from there).
_SNAREA_STEP_CONFIG = "configs/snarea/snarea_curve.yml"
_BASE_CONFIG = "configs/base_config.yml"


def resolve_paths(fabric: str) -> dict:
    """Resolve fabric-specific input paths via the base_config profile."""
    cfg = load_config(Path(_SNAREA_STEP_CONFIG), base_config_path=Path(_BASE_CONFIG), fabric=fabric)
    id_feature = require_config_key(cfg, "id_feature", "render_snarea_figures")
    data_root = require_config_key(cfg, "data_root", "render_snarea_figures")
    hru_gpkg = require_config_key(cfg, "hru_gpkg", "render_snarea_figures")
    hru_layer = require_config_key(cfg, "hru_layer", "render_snarea_figures")
    merged = Path(data_root) / fabric / "params" / "merged"
    return {
        "id_feature": id_feature,
        "snodas_dir": Path(data_root) / fabric / "snodas",
        "derived_csv": merged / "_intermediates" / "nhm_snarea_curve_derived.csv",
        "params_csv": merged / "nhm_snarea_curve_params.csv",
        "library_csv": merged / "nhm_snarea_curve_library.csv",
        "hru_gpkg": hru_gpkg,
        "hru_layer": hru_layer,
    }


def read_hru_daily(snodas_dir: Path, id_feature: str, hru_id: int) -> pd.DataFrame:
    """Daily swe/scov series for ONE HRU, concatenated across per-year NetCDFs.

    Selects the single HRU lazily before materializing, so this stays cheap even
    at CONUS scale (the bulk `read_daily_by_hru` would realize billions of rows).
    """
    files = sorted(Path(snodas_dir).glob("*_agg_*.nc"))
    if not files:
        raise FileNotFoundError(f"No aggregated NetCDFs in {snodas_dir}")
    ds = xr.open_mfdataset(files, combine="by_coords", data_vars="minimal")
    sub = ds[["swe", "scov"]].sel({id_feature: hru_id}).to_dataframe().reset_index()
    return sub.set_index("time")[["swe", "scov"]].sort_index()


def _water_year_slice(daily: pd.DataFrame, wy_end: int) -> pd.DataFrame:
    """Rows in water year `wy_end` (Oct 1 of wy_end-1 through Sep 30 of wy_end)."""
    return daily[(daily.index >= f"{wy_end - 1}-10-01") & (daily.index <= f"{wy_end}-09-30")]


def _best_water_year(daily: pd.DataFrame) -> int:
    """Pick the water year with a valid melt season and the highest peak SWE
    (clearest teaching example)."""
    best_wy, best_peak = None, -np.inf
    years = range(int(daily.index.year.min()) + 1, int(daily.index.year.max()) + 1)
    for wy in years:
        wy_df = _water_year_slice(daily, wy)
        if wy_df.empty:
            continue
        if season.annual_sdc(wy_df["swe"], wy_df["scov"]) is None:
            continue
        peak = float(wy_df["swe"].max())
        if peak > best_peak:
            best_wy, best_peak = wy, peak
    if best_wy is None:
        raise ValueError("no water year with a valid melt season for this HRU")
    return best_wy


def _pick_representative_hru(paths: dict) -> int:
    """A clean 'derived' HRU with several seasons and mid-range CV."""
    df = pd.read_csv(paths["derived_csv"])
    ok = df[(df["sdc_status"] == "derived") & (df["n_seasons"] >= 5)]
    ok = ok.sort_values("cv_subgrid")
    return int(ok.iloc[len(ok) // 2][paths["id_feature"]])


def fig_swe_sca_timeseries(paths: dict, hru_id: int, water_year: int, out_path: Path) -> None:
    """One HRU, one water year: daily mean SWE and snow-covered fraction."""
    daily = read_hru_daily(paths["snodas_dir"], paths["id_feature"], hru_id)
    wy = _water_year_slice(daily, water_year)
    fig, ax1 = plt.subplots(figsize=(9, 4.2))
    ax1.plot(wy.index, wy["swe"], color="#1f6fb4", lw=2, label="mean SWE")
    ax1.set_ylabel("Mean SWE (mm)", color="#1f6fb4")
    ax1.tick_params(axis="y", labelcolor="#1f6fb4")
    ax2 = ax1.twinx()
    ax2.plot(wy.index, wy["scov"], color="#c8562b", lw=1.5, alpha=0.85)
    ax2.set_ylabel("Snow-covered fraction", color="#c8562b")
    ax2.tick_params(axis="y", labelcolor="#c8562b")
    ax2.set_ylim(0, 1.02)
    ax1.set_title(f"Stage 1 — daily SWE & snow-covered fraction  ({paths['id_feature']} {hru_id}, WY{water_year})")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def fig_melt_extraction(paths: dict, hru_id: int, water_year: int, out_path: Path) -> None:
    """Peak→snow-free melt limb isolation + reversal removal for one HRU-year."""
    daily = read_hru_daily(paths["snodas_dir"], paths["id_feature"], hru_id)
    wy = _water_year_slice(daily, water_year)
    swe, sca = wy["swe"], wy["scov"]
    ms = season.melt_season(swe, sca)
    if ms is None:
        raise ValueError(f"HRU {hru_id} WY{water_year} has no valid melt season")
    melt_swe, melt_sca = ms
    clean_swe, _ = season.remove_reversals(melt_swe, melt_sca)
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.plot(swe.index, swe, color="#bbb", lw=1.2, label="full-year SWE")
    ax.plot(melt_swe.index, melt_swe, color="#c8562b", lw=1.6, label="melt limb (peak→bare)")
    ax.plot(clean_swe.index, clean_swe, color="#1f6fb4", lw=2.6, label="snowfall reversals removed")
    ax.scatter([swe.idxmax()], [swe.max()], color="k", zorder=5, label="peak SWE")
    ax.set_ylabel("Mean SWE (mm)")
    ax.set_title(f"Stage 2 — melt-season extraction (Driscoll 2017), WY{water_year}")
    ax.legend(fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def fig_multiyear_median(paths: dict, hru_id: int, out_path: Path) -> None:
    """Each melt season's SDC + the elementwise-median representative curve."""
    daily = read_hru_daily(paths["snodas_dir"], paths["id_feature"], hru_id)
    annual = []
    years = range(int(daily.index.year.min()) + 1, int(daily.index.year.max()) + 1)
    for wy in years:
        wy_df = _water_year_slice(daily, wy)
        if wy_df.empty:
            continue
        curve = season.annual_sdc(wy_df["swe"], wy_df["scov"])
        if curve is not None and np.isfinite(curve).all():
            annual.append(curve)
    annual = np.vstack(annual)
    med = rep.median_sdc(annual)
    sim = rep.similarity(annual, med)
    fig, ax = plt.subplots(figsize=(7, 4.6))
    for c in annual:
        ax.plot(season.SWE_LEVELS, c, color="#9ab", lw=1, alpha=0.6)
    ax.plot(season.SWE_LEVELS, med, color="#c8562b", lw=3, label="median (representative)")
    ax.set_xlabel("Fraction of peak SWE remaining")
    ax.set_ylabel("Snow-covered fraction")
    ax.set_xlim(1, 0)
    ax.set_ylim(0, 1.02)
    ax.set_title(f"Stage 2 — {annual.shape[0]} melt seasons + median  (similarity={sim:.3f})")
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def fig_cv_family(paths: dict, out_path: Path) -> None:
    """The library's curves colored by CV — higher CV melts out more gradually."""
    lib = pd.read_csv(paths["library_csv"]).sort_values("deplcrv_id")
    bins = lib[lib["curve_kind"] == "cv_bin"]
    cmap = plt.get_cmap("viridis")
    cvmin, cvmax = bins["cv"].min(), bins["cv"].max()
    fig, ax = plt.subplots(figsize=(7, 4.6))
    for _, r in lib.iterrows():
        curve = r[CURVE_COLS].to_numpy(float)
        if r["curve_kind"] == "default":
            ax.plot(season.SWE_LEVELS, curve, "k--", lw=2, label="reserved default")
        else:
            color = cmap((r["cv"] - cvmin) / (cvmax - cvmin + 1e-9))
            ax.plot(season.SWE_LEVELS, curve, color=color, lw=2, label=f"CV={r['cv']:.2f}")
    ax.set_xlabel("Fraction of peak SWE remaining")
    ax.set_ylabel("Snow-covered fraction")
    ax.set_xlim(1, 0)
    ax.set_ylim(0, 1.02)
    ax.set_title("Stage 3 — CV/lognormal curve library (Sexstone 2020)")
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def fig_empirical_vs_library(paths: dict, out_path: Path, n_samples: int = 4) -> None:
    """Sample HRUs: each HRU's empirical SDC vs the library curve it was assigned.

    The assigned curve is the lognormal library curve after Stage 3 calibrates
    sub-grid CV to the empirical CV, so this is the honest reconstruction check —
    how faithfully the compact ~9-curve library reproduces the per-HRU empirical
    curves (and what pyWatershed actually uses for that HRU). Samples span the
    library's CV-bin range (the raw derived tail has degenerate outliers).
    """
    derived = pd.read_csv(paths["derived_csv"])
    params = pd.read_csv(paths["params_csv"])
    lib = pd.read_csv(paths["library_csv"])
    idc = paths["id_feature"]
    lo, hi = lib.loc[lib["curve_kind"] == "cv_bin", "cv"].agg(["min", "max"])
    ok = derived[
        (derived["sdc_status"] == "derived") & derived["cv_subgrid"].notna() & derived["cv_subgrid"].between(lo, hi)
    ].sort_values("cv_subgrid")
    sample_ids = ok.iloc[np.linspace(0, len(ok) - 1, n_samples).astype(int)][idc]
    assigned = params.set_index(idc)
    fig, axes = plt.subplots(1, n_samples, figsize=(3.2 * n_samples, 3.4), sharey=True)
    axes = np.atleast_1d(axes)
    for ax, hid in zip(axes, sample_ids):
        row = ok.loc[ok[idc] == hid]
        emp = row[CURVE_COLS].to_numpy(float)[0]
        asg = assigned.loc[hid, CURVE_COLS].to_numpy(float)
        cv = float(row["cv_subgrid"].iloc[0])
        ax.plot(season.SWE_LEVELS, emp, "o-", color="#1f6fb4", ms=3, label="empirical")
        ax.plot(season.SWE_LEVELS, asg, "-", color="#c8562b", lw=2, label="assigned library curve")
        ax.set_xlim(1, 0)
        ax.set_ylim(0, 1.02)
        ax.set_title(f"sub-grid CV={cv:.2f}", fontsize=9)
        ax.set_xlabel("frac. peak SWE")
    axes[0].set_ylabel("Snow-covered fraction")
    axes[0].legend(fontsize=8)
    fig.suptitle("Stage 3 — empirical vs. assigned library curve (reconstruction check)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def fig_coverage(paths: dict, out_path: Path) -> None:
    """How many HRUs got a derived curve vs. a fallback, by sdc_status."""
    df = pd.read_csv(paths["derived_csv"])
    counts = df["sdc_status"].value_counts()
    total = int(counts.sum())
    fig, ax = plt.subplots(figsize=(7, 3.8))
    counts.plot.bar(ax=ax, color="#4a90c2")
    ax.set_ylabel("HRUs")
    ax.set_title(f"Stage 2 curve coverage — {total:,} HRUs")
    ax.set_xticklabels(counts.index, rotation=25, ha="right", fontsize=8)
    for i, v in enumerate(counts):
        ax.text(i, v, f"{v:,}\n({v / total:.0%})", ha="center", va="bottom", fontsize=8)
    ax.margins(y=0.15)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def fig_deplcrv_map(paths: dict, out_path: Path) -> None:
    """Map each HRU colored by its assigned library-curve index (hru_deplcrv)."""
    import geopandas as gpd

    idc = paths["id_feature"]
    gdf = gpd.read_file(paths["hru_gpkg"], layer=paths["hru_layer"])
    params = pd.read_csv(paths["params_csv"])[[idc, "hru_deplcrv"]]
    merged = gdf.merge(params, on=idc, how="left")
    fig, ax = plt.subplots(figsize=(11, 7))
    merged.plot(
        column="hru_deplcrv",
        categorical=True,
        legend=True,
        cmap="tab10",
        linewidth=0,
        ax=ax,
        legend_kwds={"title": "curve #", "fontsize": 8, "loc": "lower left"},
        missing_kwds={"color": "#eee", "label": "no curve"},
    )
    ax.set_axis_off()
    ax.set_title("Assigned snow-depletion curve per HRU (hru_deplcrv)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# Data-free schematics (name -> callable(out_path)).
SCHEMATICS = {
    "concept": schematic_concept,
    "pipeline": schematic_pipeline,
}

# Data figures needing only resolved `paths` (CSV reads, plus a gpkg for the map).
PATHS_FIGURES = ("coverage", "cv_family", "empirical_vs_library", "deplcrv_map")

# Data figures needing a picked HRU + water year (read the SNODAS NetCDFs).
HRU_FIGURES = ("swe_sca_timeseries", "melt_extraction", "multiyear_median")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fabric", choices=["oregon", "gfv2"], default="oregon")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument(
        "--figures",
        nargs="*",
        default=None,
        help="Subset of figure names; default = all applicable.",
    )
    args = p.parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_names = list(SCHEMATICS) + list(PATHS_FIGURES) + list(HRU_FIGURES)
    names = args.figures or all_names

    builders: dict = dict(SCHEMATICS)

    # Resolve paths only if a data figure is requested (schematics need no data).
    if any(n in PATHS_FIGURES or n in HRU_FIGURES for n in names):
        paths = resolve_paths(args.fabric)
        builders.update(
            {
                "coverage": lambda o: fig_coverage(paths, o),
                "cv_family": lambda o: fig_cv_family(paths, o),
                "empirical_vs_library": lambda o: fig_empirical_vs_library(paths, o),
                "deplcrv_map": lambda o: fig_deplcrv_map(paths, o),
            }
        )
        # Reading the SNODAS NetCDFs + picking an HRU is the expensive path;
        # only do it when an HRU-based figure is actually requested.
        if any(n in HRU_FIGURES for n in names):
            hru = _pick_representative_hru(paths)
            daily = read_hru_daily(paths["snodas_dir"], paths["id_feature"], hru)
            wy = _best_water_year(daily)
            print(f"representative HRU={hru}, water year={wy}")
            builders.update(
                {
                    "swe_sca_timeseries": lambda o: fig_swe_sca_timeseries(paths, hru, wy, o),
                    "melt_extraction": lambda o: fig_melt_extraction(paths, hru, wy, o),
                    "multiyear_median": lambda o: fig_multiyear_median(paths, hru, o),
                }
            )

    for name in names:
        builder = builders.get(name)
        if builder is None:
            print(f"skip unknown/unavailable figure: {name}")
            continue
        out = args.output_dir / f"{name}.png"
        builder(out)
        print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
