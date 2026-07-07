# SNODAS Snow-Depletion-Curve Presentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Marp slide deck in `docs/` that explains the SNODAS → `snarea_curve` workflow for USGS modelers, illustrated with real-data figures, and port the Marp render tooling from the sibling `nhf-spatial-targets` repo.

**Architecture:** Three strands — (1) port Marp tooling (pixi `marp` feature/env/tasks + `scripts/render_deck.py` + presentations README) into this repo; (2) a headless matplotlib figure-generation script (`scripts/render_snarea_figures.py`) that reads pipeline outputs and reuses the `snarea` package's own extraction/curve functions; (3) the deck markdown + doc pointers. Figures are prototyped against existing Jul-6 outputs and refreshed when the live pipeline (jobs 332599–332602) writes final outputs.

**Tech Stack:** Marp (marp-cli via npx, chrome-headless-shell via puppeteer), pixi environments, matplotlib, xarray, pandas, geopandas, the in-repo `gfv2_params.snarea` package.

## Global Constraints

- **Platforms:** `linux-64` only (`[tool.pixi.workspace] platforms`). Marp linux system deps (`libgbm`, `alsa-lib`) and the `LD_LIBRARY_PATH` activation env are required.
- **No head-node pytest** (CLAUDE.md). CI (`.github/workflows/ci.yml`) is the test gate. `py_compile`/import checks and single-process render scripts are fine on the head node.
- **Paths from the fabric profile, never hardcoded** (CLAUDE.md): read `id_feature`, `hru_gpkg`, and output dirs via `gfv2_params.config.require_config_key` against `configs/base_config.yml`. Use `{data_root}`/`{fabric}` placeholders.
- **Atomic commits** (CLAUDE.md): tooling port, figure script, and deck+doc-pointers are separate commits. Lead the PR body with a scope-expansion note if source changes exceed this plan.
- **Docs check on every code change** (CLAUDE.md): update `docs/presentations/README.md`, and add a pointer from `slurm_batch/RUNME.md` Step 8 + `docs/ARCHITECTURE.md` Part 2c to the deck.
- **Figure env:** run the figure script with `pixi run -e notebooks` (has `matplotlib-base` + the default `xarray`/`geopandas`/`gfv2_params`). The `marp` env is nodejs-only — used only for `render-deck`/`marp-setup`.
- **Fabric facts:** `id_feature` is `nat_hru_id` (gfv2) / `hru_id` (oregon). Stage-1 NetCDFs (`{data_root}/{fabric}/snodas/snodas_agg_<year>.nc`) have dims `time` × `<id_feature>` and vars `swe`, `scov`, `swe_std`. Outputs live under `{data_root}/{fabric}/params/merged/` (`nhm_snarea_curve_params.csv`, `_library.csv`, `nhm_snarea_curve.nc`) and `.../merged/_intermediates/` (`nhm_snarea_curve_derived.csv`). `data_root = /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`.

## File Structure

```
pyproject.toml                                     # + [tool.pixi.feature.marp.*] + marp env + tasks
scripts/render_deck.py                             # ported ~verbatim from nhf-spatial-targets
scripts/render_snarea_figures.py                   # new headless figure generator (CLI: --fabric, --output-dir)
tests/test_render_snarea_figures.py                # smoke test for the two data-free schematic fns
docs/presentations/README.md                       # ported/adapted render guide
docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md   # the deck
docs/figures/snarea/oregon/*.png                   # committed figures
docs/figures/snarea/gfv2/*.png
docs/ARCHITECTURE.md                               # + one-line pointer to the deck (Part 2c)
slurm_batch/RUNME.md                               # + one-line pointer to the deck (Step 8)
```

**Reused `snarea` APIs** (do not reimplement):
- `gfv2_params.snarea.season`: `SWE_LEVELS` (11 values 1.0→0.0), `melt_season(swe, sca)`, `remove_reversals(swe, sca)`, `normalize_curve(swe, sca)`, `annual_sdc(swe, sca)`.
- `gfv2_params.snarea.representative`: `median_sdc(annual)`, `similarity(annual, median)`, `select_representative(annual, median)`.
- `gfv2_params.snarea.library`: `sdc_from_cv(cv)` (lognormal 11-point curve), `CURVE_COLS` (`snarea_curve_0..10`).
- `scripts/derive_snarea_curve.py`: `read_daily_by_hru(nc_dir, id_feature, logger=None)` → `dict[int hru_id → DataFrame]` with `swe`/`scov` columns indexed by date.
- Output schemas: `_derived.csv` has `<id_feature>, snarea_curve_0..10, cv_subgrid, cv_empirical, sdc_status, n_seasons, similarity, peak_swe_mm`; `_params.csv` has `<id_feature>, hru_deplcrv, snarea_thresh, cv_subgrid, cv_empirical, sdc_status, sca_class, similarity, n_seasons, snarea_curve_0..10`; `_library.csv` has `deplcrv_id, curve_kind` (`default`/`cv_bin`), `cv, snarea_curve_0..10`.

---

### Task 1: Port Marp tooling into this repo

**Files:**
- Modify: `pyproject.toml` (add feature blocks after line 118, env line in `[tool.pixi.environments]` at ~line 172)
- Create: `scripts/render_deck.py`
- Create: `docs/presentations/README.md`

**Interfaces:**
- Produces: `pixi run -e marp render-deck <deck.slides.md> --pdf|--html|--server` and `pixi run -e marp marp-setup`.

- [ ] **Step 1: Add the `marp` feature blocks to `pyproject.toml`**

Insert after the `[tool.pixi.dependencies]` block (after line 118, before `[tool.pixi.pypi-dependencies]`):

```toml
[tool.pixi.feature.marp.dependencies]
# Marp slide-deck rendering toolchain (nodejs + marp-cli via npx). Kept in its
# own feature (not dev/notebooks) so pipeline operators don't pay the nodejs +
# chrome download (~250 MB) unless they render presentation decks. Two-step:
#   pixi install -e marp          # nodejs (+ linux chrome system deps below)
#   pixi run -e marp marp-setup   # downloads chrome-headless-shell via puppeteer
# Chromium isn't on conda-forge, so we use puppeteer's chrome-headless-shell
# (smaller, fewer system deps than full chrome — works on bare HPC).
nodejs = ">=20"

[tool.pixi.feature.marp.target.linux-64.dependencies]
# chrome-headless-shell on bare HPC needs libgbm (off-screen buffer alloc, used
# even headless) and alsa-lib (chrome touches the audio subsystem on startup).
libgbm = ">=1.0"
alsa-lib = ">=1.2"

[tool.pixi.feature.marp.target.linux-64.activation.env]
# Chrome is a non-conda binary downloaded by marp-setup with no rpath into the
# pixi env; prepend $CONDA_PREFIX/lib so it finds libgbm + alsa-lib. The
# ${VAR:+:$VAR} guard avoids a trailing ':' (== "search cwd") when unset.
LD_LIBRARY_PATH = "$CONDA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

[tool.pixi.feature.marp.tasks]
marp-setup = { cmd = "npx --yes puppeteer browsers install chrome-headless-shell", description = "One-shot: download chrome-headless-shell via puppeteer (works on bare HPC)" }
render-deck = { cmd = "python scripts/render_deck.py", description = "Render a Marp .slides.md deck (PDF/HTML/server) via marp-cli" }
```

- [ ] **Step 2: Register the `marp` environment**

In `[tool.pixi.environments]` (after the `reference` line), add:

```toml
marp = { features = ["marp"], solve-group = "marp" }
```

(Its own solve-group: nodejs-only, no need to share the default pin matrix.)

- [ ] **Step 3: Create `scripts/render_deck.py`**

Copy the file verbatim from `/caldera/hovenweep/projects/usgs/water/impd/nhgf/nhf-spatial-targets/scripts/render_deck.py` (243 lines; chrome-resolving wrapper — resolves puppeteer cache, sets `CHROME_PATH`/`MARP_USER=root`, appends `--allow-local-files`, passes args through to `npx @marp-team/marp-cli`). No edits needed — it references no repo-specific paths.

- [ ] **Step 4: Create `docs/presentations/README.md`**

Adapt from `nhf-spatial-targets/docs/presentations/README.md`. Keep: the Marp rationale, the two-step `pixi install -e marp` + `pixi run -e marp marp-setup` install, and the `render-deck` PDF/HTML/server commands. Replace the file list with a single entry:

```markdown
- `2026-07-snodas-snow-depletion-curves.slides.md` — collaborator overview of
  the SNODAS → `snarea_curve` workflow (Driscoll 2017 empirical method + Sexstone
  2020 lognormal/CV library). Figures under `../figures/snarea/{oregon,gfv2}/`.
```

Set the naming-convention note to `<YYYY>-<topic>.slides.md` (no project suffix — this deck spans both fabrics).

- [ ] **Step 5: Verify the pixi manifest parses and the marp env resolves**

Run: `cd <worktree> && pixi info -e marp 2>&1 | head -20`
Expected: prints env info for `marp` with no TOML/solve error. (Do NOT run `marp-setup` here — the ~150 MB chrome download happens at deck-render time in Task 6.)

Also verify the wrapper imports:
Run: `python -c "import ast; ast.parse(open('scripts/render_deck.py').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml scripts/render_deck.py docs/presentations/README.md pixi.lock
git commit -m "build(marp): port Marp deck-rendering tooling into this repo

pixi 'marp' feature/env + marp-setup/render-deck tasks + the
chrome-resolving render_deck.py wrapper + presentations README,
adapted from nhf-spatial-targets. nodejs-only feature; chrome-headless-shell
pulled post-install by marp-setup.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

(If `pixi info` regenerated `pixi.lock`, include it; if not, drop it from the `git add`.)

---

### Task 2: Figure-script scaffold + data-free schematics

**Files:**
- Create: `scripts/render_snarea_figures.py`
- Create: `tests/test_render_snarea_figures.py`

**Interfaces:**
- Produces: `schematic_concept(out_path)` and `schematic_pipeline(out_path)` (write PNGs, no data deps); a `main()` CLI `--fabric {oregon,gfv2} --output-dir <dir> [--figures ...]`. Later tasks add data-driven figure functions to the same module.

- [ ] **Step 1: Write the failing smoke test**

`tests/test_render_snarea_figures.py`:

```python
from pathlib import Path

from scripts.render_snarea_figures import schematic_concept, schematic_pipeline


def test_schematic_concept_writes_png(tmp_path):
    out = tmp_path / "concept.png"
    schematic_concept(out)
    assert out.exists() and out.stat().st_size > 5_000


def test_schematic_pipeline_writes_png(tmp_path):
    out = tmp_path / "pipeline.png"
    schematic_pipeline(out)
    assert out.exists() and out.stat().st_size > 5_000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e all python -m pytest tests/test_render_snarea_figures.py -q` (CI; on head node use `python -c "import scripts.render_snarea_figures"` which will `ModuleNotFoundError` until Step 3).
Expected: FAIL / import error — module not yet created.

- [ ] **Step 3: Create `scripts/render_snarea_figures.py` with the scaffold + two schematics**

```python
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
import matplotlib.pyplot as plt
import numpy as np


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
    for i, (label, color) in enumerate(boxes):
        x = i / n
        ax.add_patch(plt.Rectangle((x + 0.008, 0.25), 1 / n - 0.03, 0.5,
                                   facecolor=color, edgecolor="#555", lw=1.2))
        ax.text(x + (1 / n) / 2 - 0.008, 0.5, label, ha="center", va="center", fontsize=9)
        if i < n - 1:
            ax.annotate("", xy=(x + 1 / n - 0.02, 0.5), xytext=(x + 1 / n - 0.03, 0.5),
                        arrowprops=dict(arrowstyle="-|>", color="#555", lw=1.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# Registry: name -> (callable, needs_data). Later tasks extend this.
SCHEMATICS = {
    "concept": schematic_concept,
    "pipeline": schematic_pipeline,
}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fabric", choices=["oregon", "gfv2"], default="oregon")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--figures", nargs="*", default=None,
                   help="Subset of figure names; default = all applicable.")
    args = p.parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    names = args.figures or list(SCHEMATICS)
    for name in names:
        if name in SCHEMATICS:
            out = args.output_dir / f"{name}.png"
            SCHEMATICS[name](out)
            print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the schematics and the smoke test**

Run: `cd <worktree> && pixi run -e notebooks python scripts/render_snarea_figures.py --output-dir docs/figures/snarea/_schematics --figures concept pipeline`
Expected: writes `concept.png` and `pipeline.png`; open them and confirm they read clearly.

Run (CI-style; skip on head node): `pixi run -e all python -m pytest tests/test_render_snarea_figures.py -q`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/render_snarea_figures.py tests/test_render_snarea_figures.py docs/figures/snarea/_schematics
git commit -m "feat(figures): snarea figure-script scaffold + concept/pipeline schematics

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Stage-1 and Stage-2 real figures

**Files:**
- Modify: `scripts/render_snarea_figures.py` (add three data-driven functions + registry entries + config-driven path resolution)

**Interfaces:**
- Consumes: `read_daily_by_hru`, `season.*`, `representative.*` (see File Structure), `gfv2_params.config.load_config`/`require_config_key`.
- Produces: `fig_swe_sca_timeseries(...)`, `fig_melt_extraction(...)`, `fig_multiyear_median(...)`; a helper `resolve_paths(fabric)` → dict of resolved input paths for the fabric.

- [ ] **Step 1: Add config-driven path resolution + a representative-HRU picker**

Add near the top of the module (after imports):

```python
import pandas as pd
import xarray as xr

from gfv2_params.config import load_config, require_config_key
from gfv2_params.snarea import representative as rep
from gfv2_params.snarea import season
from gfv2_params.snarea.library import CURVE_COLS, sdc_from_cv

DATA_ROOT = "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2"


def resolve_paths(fabric: str) -> dict:
    """Resolve fabric-specific input paths via the base_config profile."""
    cfg = load_config("configs/base_config.yml", fabric=fabric)
    id_feature = require_config_key(cfg, "id_feature", "render_snarea_figures")
    root = Path(DATA_ROOT) / fabric
    merged = root / "params" / "merged"
    return {
        "id_feature": id_feature,
        "snodas_dir": root / "snodas",
        "derived_csv": merged / "_intermediates" / "nhm_snarea_curve_derived.csv",
        "params_csv": merged / "nhm_snarea_curve_params.csv",
        "library_csv": merged / "nhm_snarea_curve_library.csv",
        "hru_gpkg": require_config_key(cfg, "hru_gpkg", "render_snarea_figures"),
    }


def _pick_representative_hru(paths: dict) -> int:
    """A clean 'derived' HRU with several seasons and mid-range CV — good teaching example."""
    df = pd.read_csv(paths["derived_csv"])
    ok = df[df["sdc_status"] == "derived"].copy()
    ok = ok[ok["n_seasons"] >= 5]
    ok = ok.sort_values("cv_subgrid")
    return int(ok.iloc[len(ok) // 2][paths["id_feature"]])
```

(If `load_config`'s signature differs — verify against `scripts/derive_snarea_curve.py` — match it exactly. `sdc_status` values and the `derived` label come from `snarea/selection.py`; confirm the exact string.)

- [ ] **Step 2: Add the Stage-1 SWE + snow-cover time-series figure**

```python
def fig_swe_sca_timeseries(paths: dict, hru_id: int, water_year: int, out_path: Path) -> None:
    """One HRU, one water year: daily mean SWE and snow-covered fraction."""
    daily = read_daily_by_hru(str(paths["snodas_dir"]), paths["id_feature"])[hru_id]
    wy = daily[(daily.index >= f"{water_year-1}-10-01") & (daily.index <= f"{water_year}-09-30")]
    fig, ax1 = plt.subplots(figsize=(9, 4.2))
    ax1.plot(wy.index, wy["swe"], color="#1f6fb4", lw=2, label="mean SWE")
    ax1.set_ylabel("Mean SWE (mm)", color="#1f6fb4")
    ax2 = ax1.twinx()
    ax2.plot(wy.index, wy["scov"], color="#c8562b", lw=1.5, alpha=0.8, label="snow-covered fraction")
    ax2.set_ylabel("Snow-covered fraction", color="#c8562b")
    ax2.set_ylim(0, 1.02)
    ax1.set_title(f"{paths['id_feature']} {hru_id} — water year {water_year}")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
```

Import `read_daily_by_hru` at top: `from scripts.derive_snarea_curve import read_daily_by_hru` (or refactor: if importing from `scripts/` is awkward, the function is small — confirm it's importable; `scripts/derive_snarea_curve.py` defines it at module level and exports it in `__all__`).

- [ ] **Step 3: Add the Stage-2 melt-extraction figure (Driscoll)**

```python
def fig_melt_extraction(paths: dict, hru_id: int, water_year: int, out_path: Path) -> None:
    """Show peak→snow-free melt limb isolation + reversal removal for one HRU-year."""
    daily = read_daily_by_hru(str(paths["snodas_dir"]), paths["id_feature"])[hru_id]
    wy = daily[(daily.index >= f"{water_year-1}-10-01") & (daily.index <= f"{water_year}-09-30")]
    swe, sca = wy["swe"], wy["scov"]
    melt_swe, melt_sca = season.melt_season(swe, sca)          # peak → 0
    clean_swe, clean_sca = season.remove_reversals(melt_swe, melt_sca)
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.plot(swe.index, swe, color="#bbb", lw=1.2, label="full-year SWE")
    ax.plot(melt_swe.index, melt_swe, color="#c8562b", lw=1.5, label="melt limb (peak→bare)")
    ax.plot(clean_swe.index, clean_swe, color="#1f6fb4", lw=2.4, label="reversals removed")
    ax.scatter([swe.idxmax()], [swe.max()], color="k", zorder=5, label="peak SWE")
    ax.set_ylabel("Mean SWE (mm)")
    ax.set_title(f"Stage 2 — melt-season extraction (Driscoll 2017), WY{water_year}")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
```

(Verify `melt_season`/`remove_reversals` return `(swe, sca)` tuples — confirmed from `season.py`.)

- [ ] **Step 4: Add the Stage-2 multi-year + median representative-curve figure**

```python
def fig_multiyear_median(paths: dict, hru_id: int, out_path: Path) -> None:
    """Each melt season's SDC + the elementwise-median representative curve."""
    daily = read_daily_by_hru(str(paths["snodas_dir"]), paths["id_feature"])[hru_id]
    annual = []  # one 11-point curve per water year that yields a valid melt season
    for wy_end in range(daily.index.year.min() + 1, daily.index.year.max() + 1):
        wy = daily[(daily.index >= f"{wy_end-1}-10-01") & (daily.index <= f"{wy_end}-09-30")]
        if wy.empty:
            continue
        curve = season.annual_sdc(wy["swe"], wy["scov"])
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
    ax.set_title(f"Stage 2 — {annual.shape[0]} seasons + median  (similarity={sim:.3f})")
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
```

(Confirm `annual_sdc` returns an 11-vector or `None`; adjust the guard to its real contract.)

- [ ] **Step 5: Register the new figures + wire fabric-aware dispatch**

Extend `main()` so data figures resolve paths and pick a representative HRU/year:

```python
    paths = resolve_paths(args.fabric)
    hru = _pick_representative_hru(paths)
    wy = 2015  # a broadly snowy water year present in both fabrics; override per fabric if needed
    data_figs = {
        "swe_sca_timeseries": lambda o: fig_swe_sca_timeseries(paths, hru, wy, o),
        "melt_extraction":    lambda o: fig_melt_extraction(paths, hru, wy, o),
        "multiyear_median":   lambda o: fig_multiyear_median(paths, hru, o),
    }
```

Merge `SCHEMATICS` (data-free) and `data_figs` into the render loop; when `--figures` is omitted, render all.

- [ ] **Step 6: Generate the figures for Oregon and eyeball them**

Run:
```bash
pixi run -e notebooks python scripts/render_snarea_figures.py --fabric oregon \
  --output-dir docs/figures/snarea/oregon \
  --figures swe_sca_timeseries melt_extraction multiyear_median
```
Expected: three PNGs written; open each and confirm the melt limb, reversal removal, and median curve read correctly. Adjust `hru`/`wy` if the chosen HRU is degenerate.

- [ ] **Step 7: Commit**

```bash
git add scripts/render_snarea_figures.py docs/figures/snarea/oregon
git commit -m "feat(figures): Stage-1/2 real figures (SWE+SCA, melt extraction, representative curve)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Stage-3 real figures (CV library + validation)

**Files:**
- Modify: `scripts/render_snarea_figures.py` (add two functions + registry)

**Interfaces:**
- Consumes: `_library.csv`, `_derived.csv`, `sdc_from_cv`, `CURVE_COLS`.
- Produces: `fig_cv_family(...)`, `fig_empirical_vs_lognormal(...)`, `fig_coverage(...)`.

- [ ] **Step 1: Add the CV curve-family figure (Sexstone)**

```python
def fig_cv_family(paths: dict, out_path: Path) -> None:
    """The library's curves colored by CV — higher CV melts out more gradually."""
    lib = pd.read_csv(paths["library_csv"]).sort_values("deplcrv_id")
    cmap = plt.get_cmap("viridis")
    cvmin, cvmax = lib["cv"].min(skipna=True), lib["cv"].max(skipna=True)
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
    ax.set_title("Stage 3 — CV/lognormal curve library (Sexstone 2020)")
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
```

- [ ] **Step 2: Add the empirical-vs-lognormal overlay (calibration check)**

Reconstructs the comparison from a sample HRU's empirical `_derived.csv` curve vs `sdc_from_cv(cv_subgrid)` (the `_validation.csv` is a single-row summary, not per-HRU overlay data — so build the overlay directly):

```python
def fig_empirical_vs_lognormal(paths: dict, out_path: Path, n_samples: int = 4) -> None:
    """Sample HRUs: empirical SDC vs the analytic lognormal curve from its sub-grid CV."""
    df = pd.read_csv(paths["derived_csv"])
    ok = df[(df["sdc_status"] == "derived") & df["cv_subgrid"].notna()]
    sample = ok.sort_values("cv_subgrid").iloc[np.linspace(0, len(ok) - 1, n_samples).astype(int)]
    fig, axes = plt.subplots(1, n_samples, figsize=(3.2 * n_samples, 3.4), sharey=True)
    for ax, (_, r) in zip(np.atleast_1d(axes), sample.iterrows()):
        emp = r[CURVE_COLS].to_numpy(float)
        logn = sdc_from_cv(float(r["cv_subgrid"]))
        ax.plot(season.SWE_LEVELS, emp, "o-", color="#1f6fb4", ms=3, label="empirical")
        ax.plot(season.SWE_LEVELS, logn, "-", color="#c8562b", lw=2, label="lognormal(CV)")
        ax.set_xlim(1, 0)
        ax.set_title(f"CV={r['cv_subgrid']:.2f}", fontsize=9)
    axes[0].set_ylabel("Snow-covered fraction")
    axes[0].legend(fontsize=8)
    fig.suptitle("Stage 3 — empirical vs. lognormal curve (calibration check)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
```

- [ ] **Step 3: Add the derived-vs-default coverage bar**

```python
def fig_coverage(paths: dict, out_path: Path) -> None:
    """How many HRUs got a derived curve vs. a fallback, by sdc_status."""
    df = pd.read_csv(paths["derived_csv"])
    counts = df["sdc_status"].value_counts()
    fig, ax = plt.subplots(figsize=(6, 3.6))
    counts.plot.bar(ax=ax, color="#4a90c2")
    ax.set_ylabel("HRUs")
    ax.set_title(f"Stage 2 curve coverage — {counts.sum():,} HRUs")
    for i, v in enumerate(counts):
        ax.text(i, v, f"{v:,}\n({v/counts.sum():.0%})", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
```

- [ ] **Step 4: Register the three functions in the data-figure dispatch (they need only `paths`).**

- [ ] **Step 5: Generate for Oregon and eyeball**

Run:
```bash
pixi run -e notebooks python scripts/render_snarea_figures.py --fabric oregon \
  --output-dir docs/figures/snarea/oregon \
  --figures cv_family empirical_vs_lognormal coverage
```
Expected: three PNGs; confirm the CV family fans out as expected and empirical≈lognormal.

- [ ] **Step 6: Commit**

```bash
git add scripts/render_snarea_figures.py docs/figures/snarea/oregon
git commit -m "feat(figures): Stage-3 CV library, empirical-vs-lognormal, coverage figures

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: CONUS `hru_deplcrv` map (gfv2)

**Files:**
- Modify: `scripts/render_snarea_figures.py` (add `fig_deplcrv_map`)

**Interfaces:**
- Consumes: `_params.csv`, `hru_gpkg` (from `resolve_paths`), geopandas.
- Produces: `fig_deplcrv_map(paths, out_path)`.

- [ ] **Step 1: Add the choropleth**

```python
def fig_deplcrv_map(paths: dict, out_path: Path) -> None:
    """Map each HRU colored by its assigned library curve index (hru_deplcrv)."""
    import geopandas as gpd
    gdf = gpd.read_file(paths["hru_gpkg"])
    params = pd.read_csv(paths["params_csv"])[[paths["id_feature"], "hru_deplcrv"]]
    merged = gdf.merge(params, on=paths["id_feature"], how="left")
    fig, ax = plt.subplots(figsize=(11, 7))
    merged.plot(column="hru_deplcrv", categorical=True, legend=True, cmap="tab10",
                linewidth=0, ax=ax, missing_kwds={"color": "#eee", "label": "no curve"})
    ax.set_axis_off()
    ax.set_title("Assigned snow-depletion curve per HRU (hru_deplcrv)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
```

(Confirm the gpkg's HRU-id column name matches `id_feature`; if the layer uses a different column, read `hru_layer`/rename before merge. The gpkg may need a specific layer — pass `layer=` if `read_file` warns about multiple layers.)

- [ ] **Step 2: Register `deplcrv_map` (gfv2-only — guard or just run for gfv2).**

- [ ] **Step 3: Generate for gfv2 and eyeball**

Run:
```bash
pixi run -e notebooks python scripts/render_snarea_figures.py --fabric gfv2 \
  --output-dir docs/figures/snarea/gfv2 --figures deplcrv_map
```
Expected: a CONUS map PNG with a spatially coherent curve-index pattern (mountains vs. plains differ). This reads the full CONUS gpkg — run once, it may take a few minutes; keep it single-process (no head-node import storm risk).

- [ ] **Step 4: Also generate the gfv2 versions of the shared figures**

Run the Stage-1/2/3 figure subset for gfv2 too (so the Results slide can show CONUS):
```bash
pixi run -e notebooks python scripts/render_snarea_figures.py --fabric gfv2 \
  --output-dir docs/figures/snarea/gfv2 \
  --figures coverage cv_family empirical_vs_lognormal
```

- [ ] **Step 5: Commit**

```bash
git add scripts/render_snarea_figures.py docs/figures/snarea/gfv2
git commit -m "feat(figures): CONUS hru_deplcrv map + gfv2 stage figures

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Write the deck + doc pointers + render

**Files:**
- Create: `docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md`
- Modify: `docs/ARCHITECTURE.md` (Part 2c section — add deck pointer)
- Modify: `slurm_batch/RUNME.md` (Step 8 — add deck pointer)

**Interfaces:**
- Consumes: all committed PNGs under `docs/figures/snarea/{oregon,gfv2}/`.

- [ ] **Step 1: Write the deck front-matter + all slides**

Front-matter (adapt the header/footer; keep the style block):

```markdown
---
marp: true
theme: default
paginate: true
size: 16:9
header: '**gfv2-params** · SNODAS snow-depletion curves'
footer: '2026 · NHM/PRMS parameter generation'
style: |
  section { font-size: 22px; padding-bottom: 60px; }
  section h1 { font-size: 38px; }
  section h2 { font-size: 32px; }
  table { font-size: 18px; }
  pre { font-size: 16px; }
  img { max-height: 460px; }
  .footnote { font-size: 14px; color: #555; }
  .caption { font-size: 14px; color: #444; margin-top: 2px; }
  .callout { background: #f0f4ff; border-left: 4px solid #4477cc; padding: 8px 14px; font-size: 20px; margin-top: 8px; }
---
```

Then write the 18 slides from the spec outline (`docs/superpowers/specs/2026-07-07-snodas-snarea-presentation-design.md`), each separated by `---` on its own line. Content guidance per slide (keep prose tight, define terms in plain English, one figure per figure-slide referenced by relative path `![](../figures/snarea/<fabric>/<name>.png)`):

  1. Title — deck title, one-line purpose, presenter/date, footnote citing Driscoll 2017 + Sexstone 2020.
  2. What a depletion curve is — 2–3 plain sentences + `concept.png`.
  3. Why PRMS needs it — SCA→albedo/energy→melt timing→streamflow; note two HRUs w/ same mean SWE melt differently → CV motivation.
  4. The two papers — Driscoll 2017 (empirical from SNODAS) / Sexstone 2020 (lognormal-CV library); one line each + what each shaped (Stage 1–2 / Stage 3).
  5. The data — SNODAS: NOAA daily ~1 km modeled SWE over CONUS; define SWE.
  6. Pipeline at a glance — `pipeline.png` + one sentence per stage.
  7. Stage 1 — `swe_sca_timeseries.png`; define snow-covered fraction + sub-grid CV (std/mean of SWE across the 1 km cells inside an HRU).
  8. Stage 2 extraction — `melt_extraction.png`; melt limb, reversal removal, normalize, sample SCA at 11 fixed levels.
  9. Stage 2 representative — `multiyear_median.png`; one curve/season → elementwise median; similarity = inter-annual agreement.
  10. Stage 2 trust — small table of the 5 selection criteria (min_cells, max_water_frac, min_seasonal_sca, max_constant_frac, max_similarity) in plain English; fall back to default otherwise. Cite `coverage.png` briefly or defer to results.
  11. Stage 3 CV idea — `cv_family.png`; sub-grid variability sets shape; higher CV = more gradual.
  12. Stage 3 calibration — `empirical_vs_lognormal.png`; lognormal SWE pdf → analytic curve; calibrate sub-grid CV to empirical.
  13. From many curves to ~9 — parsimony; 8 equal-population CV bins + 1 default; each HRU indexed by `hru_deplcrv`.
  14. Results Oregon — `coverage.png` (oregon) + one representative curve; Oregon = validation fabric.
  15. Results CONUS — `deplcrv_map.png`; spatially coherent pattern.
  16. Products (pyWatershed) — the 3 inputs (`snarea_curve` library `ndepl×11`, `hru_deplcrv` nhru, `snarea_thresh` nhru) in `nhm_snarea_curve.nc`; per-HRU empirical SDC is diagnostic, compressed into the library (the Sexstone win). Use a small table.
  17. How it runs — `./submit_snarea_pipeline.sh <fabric>` → 4 chained SLURM jobs (aggregate array → merge → Stage 2 → Stage 3); one line, `afterok` chaining.
  18. Summary + references — 3 bullet takeaways; references to Driscoll 2017 + Sexstone 2020 + repo docs (`docs/ARCHITECTURE.md` Part 2c, `slurm_batch/RUNME.md` Step 8).

- [ ] **Step 2: Render to HTML (no chrome needed) to validate structure**

Run: `pixi run -e marp render-deck docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md --html`
Expected: writes the `.html` next to the deck with no marp parse errors. Open it; confirm every figure resolves (no broken-image icons) and slide count ≈ 18.

- [ ] **Step 3: Render to PDF (pulls chrome on first run)**

Run:
```bash
pixi install -e marp
pixi run -e marp marp-setup    # first time only; ~150 MB chrome-headless-shell
pixi run -e marp render-deck docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md --pdf
```
Expected: a `.pdf` is produced. (Do NOT commit the generated `.html`/`.pdf` — they are build artifacts; add them to `.gitignore` if not already ignored, or just leave them untracked.)

- [ ] **Step 4: Add doc pointers**

- `docs/ARCHITECTURE.md`: in the Part 2c snow-depletion section, add one line: `See docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md for a narrative/visual overview.`
- `slurm_batch/RUNME.md`: in Step 8, add the same one-line pointer.

- [ ] **Step 5: Verify committed tree has no stray build artifacts**

Run: `git status --porcelain` — expected: only the deck `.slides.md`, the two modified docs, staged. Confirm no `.html`/`.pdf`/`__pycache__` staged.

- [ ] **Step 6: Commit**

```bash
git add docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md docs/ARCHITECTURE.md slurm_batch/RUNME.md
git commit -m "docs(presentations): SNODAS snow-depletion-curve overview deck

18-slide Marp deck for USGS modelers: what a depletion curve is, the
Driscoll 2017 (empirical) + Sexstone 2020 (lognormal/CV) methods,
per-stage real-data figures, and pyWatershed products. Pointers added
from ARCHITECTURE.md Part 2c and RUNME.md Step 8.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Refresh figures from final outputs + open PR

**Files:** figures under `docs/figures/snarea/{oregon,gfv2}/` (regenerate); no code change expected.

- [ ] **Step 1: Confirm the live pipeline finished**

Run: `squeue -u $USER -o '%.10i %.20j %.8T' | grep -i snarea || echo "no snarea jobs queued/running"`
Also check output freshness: the final `_params.csv`/`_library.csv`/`nhm_snarea_curve.nc` mtimes should postdate the run. Confirm `_library.csv` exists (older on-disk trees only had `_params.csv`).

- [ ] **Step 2: Regenerate all figures for both fabrics**

```bash
pixi run -e notebooks python scripts/render_snarea_figures.py --fabric oregon --output-dir docs/figures/snarea/oregon
pixi run -e notebooks python scripts/render_snarea_figures.py --fabric gfv2   --output-dir docs/figures/snarea/gfv2
```
Eyeball the refreshed PNGs; re-render the deck HTML to confirm nothing broke.

- [ ] **Step 3: Commit any figure changes**

```bash
git add docs/figures/snarea
git commit -m "docs(figures): refresh snarea figures from final pipeline outputs" || echo "no changes"
```

- [ ] **Step 4: Open the PR**

Push the branch and open a PR (per memory, `gh` is blocked on this HPC — use the `gh auth token` + `curl` REST recipe). PR body: summary of the deck + tooling port + figure script; note the Marp tooling port as a scope item; link the spec. Let CI run the smoke test.

## Self-Review

**Spec coverage:** deck (Task 6) ✓; Marp tooling port — pyproject feature/env/tasks + render_deck.py + presentations README (Task 1) ✓; figure script (Tasks 2–5) ✓; all 7 real figures + 2 schematics ✓; products/pyWatershed slide (Task 6 slide 16) ✓; both fabrics (Tasks 3–5) ✓; doc pointers (Task 6) ✓; figure-refresh-on-final-outputs (Task 7) ✓; both papers framed (slides 4/8/11–12) ✓.

**Placeholder scan:** figure functions carry full code; slide content is itemized per slide (prose is intentionally authored at execution with figures in hand — structure and figure refs are fully specified). "Confirm/verify" notes flag real APIs to check against source, not missing content.

**Type consistency:** `resolve_paths` dict keys (`id_feature`, `snodas_dir`, `derived_csv`, `params_csv`, `library_csv`, `hru_gpkg`) are used consistently across Tasks 3–5. `season.SWE_LEVELS`, `CURVE_COLS`, `sdc_from_cv`, `read_daily_by_hru`, `median_sdc`/`similarity` names match the surveyed source. Figure registry names (`swe_sca_timeseries`, `melt_extraction`, `multiyear_median`, `cv_family`, `empirical_vs_lognormal`, `coverage`, `deplcrv_map`, `concept`, `pipeline`) match their `--figures`/commit references.
