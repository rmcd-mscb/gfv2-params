# CV/lognormal snarea_curve Library — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the one-curve-per-HRU `snarea_curve` output with a compact CV/lognormal curve **library** (`ndepl` curves + per-HRU `hru_deplcrv` index + per-HRU `snarea_thresh`), emitted as CSVs + a pyWatershed/PRMS NetCDF param file.

**Architecture:** Three stages in `gfv2-params`, all additive to the merged Driscoll pipeline (PR #165). Stage 1 (`aggregate/`) gains an area-weighted `swe_std` sidecar; Stage 2 (`snarea/` derive) adds a representative sub-grid CV + peak SWE per HRU and writes to `_intermediates/`; Stage 3 (new `snarea/library.py`) is a pure-tabular builder that fits the lognormal CV library, assigns curves, computes `snarea_thresh`, and serializes. The curve **shape** is one physical parameter (sub-grid SWE CV via a lognormal pdf); the per-HRU **scale** is `snarea_thresh`.

**Tech Stack:** Python 3.12, pandas/numpy/scipy (all present), xarray + netCDF4, gdptools (Stage 1), pixi env, pytest, SLURM.

**Design spec:** [`docs/superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md`](../specs/2026-07-06-snodas-snarea-curve-library-design.md) — read it first; this plan implements it section-by-section.

## Global Constraints

- **Fabric-agnostic configs.** Every per-fabric path resolves from the active profile in `configs/base_config.yml` via `require_config_key` with `{data_root}`/`{fabric}` placeholders. No literal paths, no naming conventions in code. (CLAUDE.md)
- **`load_config` resolves only top-level string placeholders** — nested config blocks pass through untouched (mirror `derive_aggregate.py`'s `_resolve` if nested resolution is needed).
- **Curve order:** repo/CSV convention is **descending** `SWE_LEVELS = 1.0 … 0.0` (index 0 = SWE/thresh 1.0 → SCA≈1; index 10 = 0.0 → SCA=0), matching `src/gfv2_params/snarea/season.py`. The PRMS NetCDF is **ascending** frac_swe — the flip lives in exactly one helper `_to_prms_order`.
- **`ndepl` = 1 reserved default curve (index 1) + `ndepl_cv` CV-bin curves (indices 2..ndepl).** `ndepl_cv` config-default **8** ⇒ `ndepl`=9.
- **Tests are the gate; CI runs `pytest tests/` on push/PR.** Never run full `pytest` on the HPC head node (concurrent geo-import storms hang). A single test file via `pixi run -e dev pytest tests/test_x.py -v` is fine; `py_compile` is fine.
- **Run `pixi run -e dev pre-commit run --all-files` before every push.** isort + ruff, line-length 120.
- **Commit style:** conventional prefixes (`feat`/`fix`/`test`/`docs`/`chore`); end messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **`gh` is firewall-blocked here** ([[gh_cli_blocked_use_curl_rest]]); open the PR via curl+REST with `--data-binary` when done.
- **data_root** = `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`. On-disk derived tables already exist for `oregon` and `gfv2` at `{data_root}/{fabric}/params/merged/nhm_snarea_curve_params.csv` (usable to smoke-test Stage 3 immediately).

---

## File Structure

**New source files**
- `src/gfv2_params/snarea/library.py` — Stage 3 core: `sdc_from_cv`, `fit_cv`, `snarea_thresh_inches`, `_to_prms_order`, `build_library`, `assign_deplcrv`, `validate_and_calibrate`, `write_library_csv` / `write_params_csv` / `write_validation_csv` / `write_prms_netcdf`.
- `src/gfv2_params/snarea/subgrid.py` — `representative_peak_stats(daily)` (Stage 2 sub-grid CV + peak SWE).
- `scripts/derive_snarea_library.py` — Stage 3 CLI driver.
- `configs/snarea/snarea_library.yml` — Stage 3 config.
- `slurm_batch/derive_snarea_library.batch` — Stage 3 SLURM batch.
- `tests/test_snarea_library.py`, `tests/test_snarea_subgrid.py` — new tests.

**Modified source files**
- `src/gfv2_params/aggregate/adapter.py` — add `std_variables` field + validation.
- `src/gfv2_params/aggregate/driver.py` — `masked_std` sidecar pass emitting `{var}_std`.
- `src/gfv2_params/aggregate/snodas.py` — `std_variables=("swe",)`.
- `scripts/derive_snarea_curve.py` — load `swe_std`; write to `_intermediates/`.
- `src/gfv2_params/snarea/build.py` — add `cv_subgrid`/`peak_swe_mm`/`n_peak_years` columns.
- `configs/aggregate/aggregate_sources.yml` — declare `swe_std`.
- `configs/snarea/snarea_curve.yml` — repoint Stage 2 output to `_intermediates/nhm_snarea_curve_derived.csv`.
- `pyproject.toml` — add `pywatershed` to a `reference` pixi feature.
- Docs: `README.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`, `docs/ARCHITECTURE.md`; memory update.

**Build order:** Phase A (Tasks 1–8) is the pure Stage-3 library core — fully unit-testable now, no HPC. Phase B (Tasks 9–13) is the Stage 1/2 sub-grid infra (needs an HPC re-run to produce real `swe_std`). Phase C (Tasks 14–16) wires the Stage-3 driver, ops, and docs.

---

## Phase A — Stage 3 library core (pure functions; no HPC)

### Task 1: `sdc_from_cv` + curve conventions

**Files:**
- Create: `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Produces: `SWE_LEVELS: np.ndarray` (11 descending 1.0→0.0); `sdc_from_cv(cv: float, mu: float = 1.0, n: int = 4000) -> np.ndarray` (11-pt descending curve).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_snarea_library.py
import numpy as np
import pytest

from gfv2_params.snarea.library import SWE_LEVELS, sdc_from_cv


def test_swe_levels_descending_11pt():
    assert SWE_LEVELS.shape == (11,)
    assert SWE_LEVELS[0] == 1.0 and SWE_LEVELS[-1] == 0.0
    assert np.all(np.diff(SWE_LEVELS) < 0)


def test_sdc_from_cv_shape_and_endpoints():
    c = sdc_from_cv(0.5)
    assert c.shape == (11,)
    assert c[0] == pytest.approx(1.0)
    assert c[-1] == pytest.approx(0.0)


def test_sdc_from_cv_monotone_nonincreasing():
    c = sdc_from_cv(0.7)
    assert np.all(np.diff(c) <= 1e-9)


def test_sdc_from_cv_higher_cv_steeper():
    # larger CV -> lower SCA at mid SWE (index 5 = SWE 0.5)
    assert sdc_from_cv(1.5)[5] < sdc_from_cv(0.5)[5] < sdc_from_cv(0.1)[5]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: FAIL — `ModuleNotFoundError` / `cannot import name 'sdc_from_cv'`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/snarea/library.py
"""Stage 3: CV/lognormal snarea_curve library builder.

The curve SHAPE is a single physical parameter — the sub-grid SWE coefficient of
variation (CV) — via a lognormal SWE pdf (Sexstone et al. 2020, eqs 3-5; Liston
2004). The dimensionless snow-depletion curve depends only on CV. Repo curve
order is DESCENDING (SWE/thresh 1.0 -> 0.0); the PRMS NetCDF is ascending (see
_to_prms_order).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import norm

# Descending, matching snarea/season.py SWE_LEVELS.
SWE_LEVELS = np.round(np.arange(1.0, -1e-4, -0.1), 1)  # 1.0 .. 0.0, 11 values


def sdc_from_cv(cv: float, mu: float = 1.0, n: int = 4000) -> np.ndarray:
    """11-point dimensionless SDC for a lognormal SWE pdf with coeff-of-var ``cv``.

    Sexstone eqs 1-5 under uniform melt: SCA(M)=P(S>M); SWE(M)=E[(S-M)^+]. The
    dimensionless curve (SCA vs SWE/peak) depends only on cv. Returns SCA at
    SWE_LEVELS (descending), clipped to [0,1], anchored (1.0 @ SWE=1, 0.0 @ SWE=0).
    """
    z = np.sqrt(np.log(1 + cv * cv))          # ζ² = ln(1+CV²)
    lam = np.log(mu) - 0.5 * z * z            # λ  = ln(μ) − ζ²/2
    M = np.concatenate([[0.0], np.exp(np.linspace(np.log(mu) - 6 * z, np.log(mu) + 6 * z, n))])
    lnM = np.log(np.where(M > 0, M, 1e-300))
    sca = norm.cdf((lam - lnM) / z)           # SCA(M) = Φ((λ−lnM)/ζ)
    swe = mu * norm.cdf((lam + z * z - lnM) / z) - M * sca
    sca[0], swe[0] = 1.0, mu
    o = np.argsort(swe / swe[0])
    return np.clip(np.interp(SWE_LEVELS, (swe / swe[0])[o], sca[o], left=1.0, right=0.0), 0, 1)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): sdc_from_cv lognormal curve + conventions

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `fit_cv` — project an empirical curve onto the lognormal family

**Files:**
- Modify: `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Consumes: `sdc_from_cv`, `SWE_LEVELS`.
- Produces: `CV_GRID: np.ndarray`; `fit_cv(curve: np.ndarray, cv_grid: np.ndarray | None = None) -> float` — best-fit CV minimising L2 over interior points 1..9 (endpoints fixed for all CVs, so carry no info).

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_snarea_library.py
from gfv2_params.snarea.library import CV_GRID, fit_cv


def test_fit_cv_recovers_known_cv():
    for true_cv in (0.3, 0.7, 1.2):
        curve = sdc_from_cv(true_cv)
        # fit is on the CV grid; recovered value within one grid step
        assert abs(fit_cv(curve) - true_cv) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9


def test_fit_cv_uses_interior_only():
    # a curve whose endpoints are perturbed but interior matches cv=0.5 still fits ~0.5
    curve = sdc_from_cv(0.5).copy()
    curve[0] = 1.0  # endpoints already fixed; assert fit ignores them
    assert fit_cv(curve) == pytest.approx(0.5, abs=CV_GRID[1] - CV_GRID[0])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_fit_cv_recovers_known_cv -v`
Expected: FAIL — `cannot import name 'fit_cv'`.

- [ ] **Step 3: Write minimal implementation**

```python
# add to src/gfv2_params/snarea/library.py
# CV search grid: 0.05..3.0 step 0.05 covers the validated range (median ~0.45,
# up to ~1.2 CONUS) with headroom.
CV_GRID = np.round(np.arange(0.05, 3.0001, 0.05), 2)
_INTERIOR = slice(1, 10)  # endpoints (0, 10) are fixed 1.0/0.0 for every cv


def _library_matrix(cv_grid: np.ndarray) -> np.ndarray:
    """(len(cv_grid), 11) matrix of analytic curves — built once, reused."""
    return np.vstack([sdc_from_cv(c) for c in cv_grid])


def fit_cv(curve: np.ndarray, cv_grid: np.ndarray | None = None) -> float:
    """Best-fit lognormal CV for an empirical 11-pt curve (min L2 over interior)."""
    grid = CV_GRID if cv_grid is None else cv_grid
    lib = _library_matrix(grid)
    d = np.linalg.norm(lib[:, _INTERIOR] - np.asarray(curve)[_INTERIOR], axis=1)
    return float(grid[int(d.argmin())])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): fit_cv projects empirical curve onto lognormal family

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `snarea_thresh_inches` + `_to_prms_order`

**Files:**
- Modify: `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Produces: `snarea_thresh_inches(peak_swe_mm: float) -> float` (mm→in, /25.4; non-finite/≤0 → 0.0); `_to_prms_order(curve: np.ndarray) -> np.ndarray` (reverse descending→ascending).

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_snarea_library.py
from gfv2_params.snarea.library import _to_prms_order, snarea_thresh_inches


def test_snarea_thresh_mm_to_inches():
    assert snarea_thresh_inches(254.0) == pytest.approx(10.0)
    assert snarea_thresh_inches(0.0) == 0.0
    assert snarea_thresh_inches(float("nan")) == 0.0
    assert snarea_thresh_inches(-5.0) == 0.0


def test_to_prms_order_is_reverse_and_involutive():
    c = sdc_from_cv(0.6)
    p = _to_prms_order(c)
    assert p[0] == pytest.approx(c[-1])   # ascending: SCA@frac0 first
    assert p[-1] == pytest.approx(c[0])
    assert np.allclose(_to_prms_order(p), c)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_snarea_thresh_mm_to_inches -v`
Expected: FAIL — import error.

- [ ] **Step 3: Write minimal implementation**

```python
# add to src/gfv2_params/snarea/library.py
_MM_PER_INCH = 25.4


def snarea_thresh_inches(peak_swe_mm: float) -> float:
    """Per-HRU SWE scale in inches. 0.0 for no-snow / undefined (curve never
    exercised there since pkwater_equiv is 0)."""
    v = float(peak_swe_mm)
    if not np.isfinite(v) or v <= 0.0:
        return 0.0
    return v / _MM_PER_INCH


def _to_prms_order(curve: np.ndarray) -> np.ndarray:
    """Repo descending (SWE 1.0->0.0) -> PRMS ascending frac_swe (0.0->1.0)."""
    return np.asarray(curve)[::-1]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): snarea_thresh mm->in + PRMS curve-order helper

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `build_library` — reserved default + equal-population CV bins

**Files:**
- Modify: `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Consumes: `sdc_from_cv`, `SWE_LEVELS`.
- Produces: `CURVE_COLS: list[str]` (`snarea_curve_0..10`); `build_library(cv_values: np.ndarray, ndepl_cv: int, default_curve: np.ndarray) -> pd.DataFrame` with columns `deplcrv_id` (1..ndepl), `curve_kind` (`default`|`cv_bin`), `cv` (bin-median; NaN for default), `snarea_curve_0..10` (descending). Row 1 = default; rows 2..ndepl = CV bins by ascending median CV.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_snarea_library.py
import pandas as pd
from gfv2_params.snarea.library import CURVE_COLS, build_library


def test_build_library_default_plus_bins():
    rng = np.linspace(0.2, 1.4, 500)   # spread of CVs
    default = np.linspace(1.0, 0.0, 11)
    lib = build_library(rng, ndepl_cv=8, default_curve=default)
    assert len(lib) == 9                              # 1 default + 8 bins
    assert list(lib["deplcrv_id"]) == list(range(1, 10))
    assert lib.iloc[0]["curve_kind"] == "default"
    assert np.isnan(lib.iloc[0]["cv"])
    np.testing.assert_allclose(lib.iloc[0][CURVE_COLS].to_numpy(float), default)
    assert (lib.iloc[1:]["curve_kind"] == "cv_bin").all()
    # bin median CVs are increasing
    assert np.all(np.diff(lib.iloc[1:]["cv"].to_numpy()) > 0)
    # each cv_bin curve equals sdc_from_cv(its median cv)
    from gfv2_params.snarea.library import sdc_from_cv
    row = lib.iloc[3]
    np.testing.assert_allclose(row[CURVE_COLS].to_numpy(float), sdc_from_cv(row["cv"]), atol=1e-9)


def test_build_library_equal_population_bins():
    cv = np.arange(1000) / 1000.0 + 0.1   # uniform
    lib = build_library(cv, ndepl_cv=5, default_curve=np.linspace(1, 0, 11))
    assert len(lib) == 6
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_build_library_default_plus_bins -v`
Expected: FAIL — import error.

- [ ] **Step 3: Write minimal implementation**

```python
# add to src/gfv2_params/snarea/library.py  (add `import pandas as pd` at top)
CURVE_COLS = [f"snarea_curve_{i}" for i in range(11)]


def build_library(cv_values: np.ndarray, ndepl_cv: int, default_curve: np.ndarray) -> pd.DataFrame:
    """Row 1 = reserved default curve; rows 2..(1+ndepl_cv) = equal-population CV
    bins, each curve = sdc_from_cv(bin median CV). Curves are descending."""
    cv = np.asarray(cv_values, dtype=float)
    cv = cv[np.isfinite(cv)]
    if cv.size == 0:
        raise ValueError("build_library: no finite CV values to bin")
    default_curve = np.asarray(default_curve, dtype=float)
    if default_curve.shape != (11,):
        raise ValueError(f"default_curve must be shape (11,), got {default_curve.shape}")

    rows = [{"deplcrv_id": 1, "curve_kind": "default", "cv": np.nan,
             **{c: float(default_curve[i]) for i, c in enumerate(CURVE_COLS)}}]

    # equal-population bins via quantile edges; label each point 0..ndepl_cv-1
    edges = np.quantile(cv, np.linspace(0, 1, ndepl_cv + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    labels = np.clip(np.digitize(cv, edges[1:-1]), 0, ndepl_cv - 1)
    medians = sorted(float(np.median(cv[labels == b])) for b in range(ndepl_cv) if (labels == b).any())
    for k, m in enumerate(medians, start=2):
        curve = sdc_from_cv(m)
        rows.append({"deplcrv_id": k, "curve_kind": "cv_bin", "cv": m,
                     **{c: float(curve[i]) for i, c in enumerate(CURVE_COLS)}})
    return pd.DataFrame(rows)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): build_library — reserved default + equal-pop CV bins

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `assign_deplcrv` — nearest-CV bin, default fallback

**Files:**
- Modify: `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Consumes: `build_library` output.
- Produces: `assign_deplcrv(cv_assign: np.ndarray, library: pd.DataFrame) -> np.ndarray` (int `deplcrv_id` per HRU). Finite CV → nearest **cv_bin** median (the default row is excluded from the search); non-finite CV → default index 1.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_snarea_library.py
from gfv2_params.snarea.library import assign_deplcrv


def test_assign_deplcrv_nearest_bin_and_default():
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    bin_cvs = lib.iloc[1:]["cv"].to_numpy()
    cv_assign = np.array([bin_cvs[0], bin_cvs[-1], np.nan, float(bin_cvs.mean())])
    out = assign_deplcrv(cv_assign, lib)
    assert out[0] == 2                     # nearest first bin
    assert out[1] == 9                     # nearest last bin
    assert out[2] == 1                     # NaN -> reserved default
    assert 2 <= out[3] <= 9                # some cv_bin, never the default
    assert out.dtype.kind in ("i", "u")


def test_assign_deplcrv_never_returns_default_for_finite_cv():
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    out = assign_deplcrv(np.full(50, 0.45), lib)
    assert (out >= 2).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_assign_deplcrv_nearest_bin_and_default -v`
Expected: FAIL — import error.

- [ ] **Step 3: Write minimal implementation**

```python
# add to src/gfv2_params/snarea/library.py
def assign_deplcrv(cv_assign: np.ndarray, library: pd.DataFrame) -> np.ndarray:
    """Nearest cv_bin curve (by CV) for finite CV; reserved default (id 1) for
    non-finite. The default row is never a nearest-CV candidate."""
    bins = library[library["curve_kind"] == "cv_bin"]
    bin_ids = bins["deplcrv_id"].to_numpy()
    bin_cvs = bins["cv"].to_numpy(float)
    default_id = int(library[library["curve_kind"] == "default"]["deplcrv_id"].iloc[0])
    cv = np.asarray(cv_assign, dtype=float)
    out = np.full(cv.shape, default_id, dtype=np.int32)
    finite = np.isfinite(cv)
    if finite.any():
        nearest = np.abs(cv[finite][:, None] - bin_cvs[None, :]).argmin(axis=1)
        out[finite] = bin_ids[nearest]
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): assign_deplcrv — nearest CV bin + default fallback

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: `validate_and_calibrate` — gate + monotone quantile map

**Files:**
- Modify: `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Consumes: `sdc_from_cv`.
- Produces: `validate_and_calibrate(cv_subgrid: np.ndarray, cv_empirical: np.ndarray, emp_curves: np.ndarray, mode: str = "auto", bias_tol: float = 0.1) -> tuple[np.ndarray, dict]`. Returns `(cv_calibrated_for_all_input, report)`. `cv_subgrid`/`cv_empirical` align by index; `cv_empirical` is NaN where not `derived`; `emp_curves` is (n,11) with NaN rows where not derived. On `auto`, if `|median(cv_subgrid_derived) − median(cv_empirical_derived)| > bias_tol`, monotone quantile-map `cv_subgrid → cv_empirical` (trained on the derived overlap) and apply to all finite `cv_subgrid`; else identity. `report` carries distribution stats, reconstruction error before/after, and `calibrated: bool`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_snarea_library.py
from gfv2_params.snarea.library import validate_and_calibrate


def test_calibration_removes_synthetic_bias():
    rng = np.linspace(0.3, 1.2, 400)
    emp_curves = np.vstack([sdc_from_cv(c) for c in rng])
    cv_empirical = rng.copy()
    cv_subgrid = rng * 0.6           # biased low by construction
    cal, report = validate_and_calibrate(cv_subgrid, cv_empirical, emp_curves, mode="auto", bias_tol=0.05)
    assert report["calibrated"] is True
    # after calibration the median bias vs empirical is much smaller
    assert abs(np.median(cal) - np.median(cv_empirical)) < abs(np.median(cv_subgrid) - np.median(cv_empirical))


def test_no_calibration_when_unbiased():
    rng = np.linspace(0.3, 1.2, 400)
    emp_curves = np.vstack([sdc_from_cv(c) for c in rng])
    cal, report = validate_and_calibrate(rng.copy(), rng.copy(), emp_curves, mode="auto", bias_tol=0.1)
    assert report["calibrated"] is False
    np.testing.assert_allclose(cal, rng)


def test_calibration_off_is_identity():
    rng = np.linspace(0.3, 1.2, 50)
    emp = np.vstack([sdc_from_cv(c) for c in rng])
    cal, report = validate_and_calibrate(rng * 0.5, rng, emp, mode="off")
    np.testing.assert_allclose(cal, rng * 0.5)
    assert report["calibrated"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_calibration_removes_synthetic_bias -v`
Expected: FAIL — import error.

- [ ] **Step 3: Write minimal implementation**

```python
# add to src/gfv2_params/snarea/library.py
def _recon_error(cv: np.ndarray, emp_curves: np.ndarray) -> tuple[float, float]:
    """mean and p95 abs-SCA error of sdc_from_cv(cv) vs emp_curves (rows aligned)."""
    ok = np.isfinite(cv) & np.isfinite(emp_curves).all(axis=1)
    if not ok.any():
        return float("nan"), float("nan")
    approx = np.vstack([sdc_from_cv(c) for c in cv[ok]])
    err = np.abs(approx - emp_curves[ok])
    return float(err.mean()), float(np.percentile(err.max(axis=1), 95))


def validate_and_calibrate(cv_subgrid, cv_empirical, emp_curves, mode="auto", bias_tol=0.1):
    cv_subgrid = np.asarray(cv_subgrid, dtype=float)
    cv_empirical = np.asarray(cv_empirical, dtype=float)
    emp_curves = np.asarray(emp_curves, dtype=float)
    derived = np.isfinite(cv_subgrid) & np.isfinite(cv_empirical)
    sub_d, emp_d = cv_subgrid[derived], cv_empirical[derived]

    report = {
        "n_derived_overlap": int(derived.sum()),
        "cv_subgrid_median": float(np.median(sub_d)) if derived.any() else float("nan"),
        "cv_empirical_median": float(np.median(emp_d)) if derived.any() else float("nan"),
        "calibrated": False,
    }
    report["recon_mean_before"], report["recon_p95_before"] = _recon_error(cv_subgrid, emp_curves)

    bias = abs(report["cv_subgrid_median"] - report["cv_empirical_median"]) if derived.any() else 0.0
    report["cv_median_bias"] = float(bias)

    cal = cv_subgrid.copy()
    if mode == "on" or (mode == "auto" and derived.sum() >= 2 and bias > bias_tol):
        # monotone quantile map trained on the derived overlap
        qs = np.linspace(0, 1, 101)
        x = np.quantile(sub_d, qs)
        y = np.quantile(emp_d, qs)
        x, idx = np.unique(x, return_index=True)   # strictly increasing x for interp
        y = y[idx]
        finite = np.isfinite(cal)
        cal[finite] = np.interp(cal[finite], x, y)
        report["calibrated"] = True
    elif mode not in ("auto", "on", "off"):
        raise ValueError(f"calibrate mode must be auto|on|off, got {mode!r}")

    report["recon_mean_after"], report["recon_p95_after"] = _recon_error(cal, emp_curves)
    return cal, report
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): validate_and_calibrate — gate + monotone quantile map

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: CSV serializers (library / params / validation)

**Files:**
- Modify: `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Consumes: `build_library`, `assign_deplcrv`, `snarea_thresh_inches`.
- Produces: `assemble_params(derived: pd.DataFrame, id_feature: str, cv_assign: np.ndarray, cv_source: np.ndarray, deplcrv: np.ndarray, library: pd.DataFrame) -> pd.DataFrame`; `write_library_csv(library, path)`, `write_params_csv(params, path)`, `write_validation_csv(report, path)`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_snarea_library.py
from gfv2_params.snarea.library import assemble_params, write_library_csv, write_params_csv


def test_assemble_params_columns_and_assigned_curve(tmp_path):
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    derived = pd.DataFrame({
        "nat_hru_id": [10, 11, 12],
        "sdc_status": ["derived", "derived", "default_no_snow"],
        "sca_class": ["high", "mid", "high"],
        "similarity": [0.05, 0.09, np.nan],
        "n_seasons": [12, 8, 0],
        "cv_subgrid": [0.4, 0.9, np.nan],
        "cv_empirical": [0.42, 0.88, np.nan],
        "peak_swe_mm": [254.0, 508.0, 0.0],
        "n_peak_years": [12, 8, 0],
    })
    cv_assign = np.array([0.4, 0.9, np.nan])
    cv_source = np.array(["subgrid", "subgrid", "default_no_snow"])
    deplcrv = assign_deplcrv(cv_assign, lib)
    params = assemble_params(derived, "nat_hru_id", cv_assign, cv_source, deplcrv, lib)
    assert list(params["nat_hru_id"]) == [10, 11, 12]
    for col in ("hru_deplcrv", "snarea_thresh", "cv_assign", "cv_source", *CURVE_COLS):
        assert col in params.columns
    assert params.loc[params.nat_hru_id == 12, "hru_deplcrv"].iloc[0] == 1     # no-snow -> default
    assert params.loc[params.nat_hru_id == 12, "snarea_thresh"].iloc[0] == 0.0
    # assigned curve row matches the library curve for that hru's deplcrv
    r = params.iloc[0]
    librow = lib[lib.deplcrv_id == r["hru_deplcrv"]].iloc[0]
    np.testing.assert_allclose(r[CURVE_COLS].to_numpy(float), librow[CURVE_COLS].to_numpy(float))


def test_write_csvs_roundtrip(tmp_path):
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    p = tmp_path / "lib.csv"
    write_library_csv(lib, p)
    assert pd.read_csv(p).shape[0] == 9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_assemble_params_columns_and_assigned_curve -v`
Expected: FAIL — import error.

- [ ] **Step 3: Write minimal implementation**

```python
# add to src/gfv2_params/snarea/library.py  (add `from pathlib import Path` at top)
_PARAM_DIAG_COLS = ["sdc_status", "sca_class", "similarity", "n_seasons", "n_peak_years", "peak_swe_mm"]


def assemble_params(derived, id_feature, cv_assign, cv_source, deplcrv, library):
    """One row per HRU: index + snarea_thresh + CVs + diagnostics + the ASSIGNED
    library curve (descending, for QA / per-HRU detail — no separate 1:1 mode)."""
    curve_by_id = {int(r.deplcrv_id): r[CURVE_COLS].to_numpy(float) for _, r in library.iterrows()}
    assigned = np.vstack([curve_by_id[int(d)] for d in deplcrv])
    out = pd.DataFrame({
        id_feature: derived[id_feature].to_numpy(),
        "hru_deplcrv": np.asarray(deplcrv, dtype=np.int32),
        "snarea_thresh": [snarea_thresh_inches(v) for v in derived["peak_swe_mm"].to_numpy()],
        "cv_assign": np.asarray(cv_assign, dtype=float),
        "cv_subgrid": derived["cv_subgrid"].to_numpy(float),
        "cv_empirical": derived["cv_empirical"].to_numpy(float),
        "cv_source": np.asarray(cv_source, dtype=object),
    })
    for c in _PARAM_DIAG_COLS:
        out[c] = derived[c].to_numpy()
    for i, c in enumerate(CURVE_COLS):
        out[c] = assigned[:, i]
    return out


def write_library_csv(library: pd.DataFrame, path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    library.to_csv(path, index=False)


def write_params_csv(params: pd.DataFrame, path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    params.to_csv(path, index=False)


def write_validation_csv(report: dict, path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([report]).to_csv(path, index=False)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): CSV serializers for library/params/validation

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: pyWatershed NetCDF param writer (verify order first)

**Files:**
- Modify: `pyproject.toml` (add `reference` pixi feature with `pywatershed`), `src/gfv2_params/snarea/library.py`
- Test: `tests/test_snarea_library.py`

**Interfaces:**
- Consumes: `_to_prms_order`, `build_library`.
- Produces: `write_prms_netcdf(library, params, id_feature, path)` — writes `snarea_curve(ndeplval)` flat **ascending**, `hru_deplcrv(nhru)` int32, `snarea_thresh(nhru)` float, `<id_feature>(nhru)` coord, CF attrs.

- [ ] **Step 1: Add `pywatershed` reference feature + verify the convention**

Edit `pyproject.toml` — add after the `[tool.pixi.feature.docs...]` blocks:

```toml
[tool.pixi.feature.reference.dependencies]
pywatershed = "*"
```

And add `reference` to `[tool.pixi.environments]`:

```toml
reference = { features = ["reference"], solve-group = "default" }
```

Then:

```bash
pixi install -e reference
pixi run -e reference python -c "
import inspect, pywatershed as pws
from pywatershed.hydrology.prms_snow import PRMSSnow
src = inspect.getsource(PRMSSnow)
i = src.find('snarea_curve')
print(src[i-200:i+400])
"
```
Expected: confirms `snarea_curve` is reshaped `(ndepl, 11)` and indexed by **ascending** `frac_swe` (frac 0.0 at index 0). If the source shows a different order/shape, adjust `_to_prms_order` and the writer below to match — **the model reads curves backwards if this is wrong.** Record the confirmed convention in a comment.

- [ ] **Step 2: Write the failing test**

```python
# add to tests/test_snarea_library.py
import xarray as xr
from gfv2_params.snarea.library import write_prms_netcdf


def test_write_prms_netcdf_structure_and_ascending_order(tmp_path):
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    params = pd.DataFrame({
        "nat_hru_id": [10, 11, 12],
        "hru_deplcrv": np.array([2, 5, 1], dtype=np.int32),
        "snarea_thresh": [10.0, 20.0, 0.0],
    })
    p = tmp_path / "snarea.nc"
    write_prms_netcdf(lib, params, "nat_hru_id", p)
    ds = xr.open_dataset(p)
    ndepl = len(lib)
    assert ds.sizes["ndeplval"] == 11 * ndepl
    assert ds.sizes["nhru"] == 3
    assert ds["hru_deplcrv"].dtype == np.int32
    # first curve (default, deplcrv_id 1) ascending: index 0 == descending's last (0.0)
    flat = ds["snarea_curve"].values
    first_curve_ascending = flat[:11]
    desc = lib[lib.deplcrv_id == 1][CURVE_COLS].to_numpy(float).ravel()
    np.testing.assert_allclose(first_curve_ascending, desc[::-1])
    assert first_curve_ascending[0] <= first_curve_ascending[-1]   # ascending SCA
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_write_prms_netcdf_structure_and_ascending_order -v`
Expected: FAIL — import error.

- [ ] **Step 4: Write minimal implementation**

```python
# add to src/gfv2_params/snarea/library.py
def write_prms_netcdf(library, params, id_feature, path) -> None:
    """PRMS/pyWatershed param file: snarea_curve flat ASCENDING (ndeplval=11*ndepl),
    hru_deplcrv, snarea_thresh, all on nhru. Verified against prms_snow (Task 8 step 1)."""
    import numpy as np
    import xarray as xr

    lib_sorted = library.sort_values("deplcrv_id")
    ndepl = len(lib_sorted)
    flat = np.concatenate([_to_prms_order(r[CURVE_COLS].to_numpy(float))
                           for _, r in lib_sorted.iterrows()])
    ds = xr.Dataset(
        data_vars={
            "snarea_curve": ("ndeplval", flat.astype("float64"),
                             {"long_name": "snow area depletion curve values", "units": "fraction"}),
            "hru_deplcrv": ("nhru", params["hru_deplcrv"].to_numpy(np.int32),
                            {"long_name": "index of snarea_curve for each HRU"}),
            "snarea_thresh": ("nhru", params["snarea_thresh"].to_numpy("float64"),
                              {"long_name": "SWE above which HRU is 100% snow covered", "units": "inches"}),
        },
        coords={id_feature: ("nhru", params[id_feature].to_numpy())},
        attrs={"ndepl": ndepl, "Description": "SNODAS-derived CV/lognormal snarea_curve library"},
    )
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path)
```

- [ ] **Step 5: Run test + cf-netcdf-review**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v`
Expected: PASS.
Then invoke the `cf-netcdf-review` skill on `write_prms_netcdf` and apply any CF-compliance fixes it recommends (units/standard_name/grid-mapping). Re-run the test after fixes.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml pixi.lock src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): pyWatershed NetCDF param writer (ascending, verified)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase B — Stage 1/2 sub-grid CV infrastructure

### Task 9: `SourceAdapter.std_variables`

**Files:**
- Modify: `src/gfv2_params/aggregate/adapter.py`
- Test: `tests/test_aggregate_adapter.py`

**Interfaces:**
- Produces: `SourceAdapter.std_variables: tuple[str, ...] = ()`, normalized to a tuple, each name must be in `variables`, `masked_std` implied.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_aggregate_adapter.py
import pytest
from gfv2_params.aggregate import SourceAdapter


def test_std_variables_defaults_empty_and_validates():
    a = SourceAdapter(source_key="s", variables=("swe", "scov"), files_glob="*.nc")
    assert a.std_variables == ()
    a2 = SourceAdapter(source_key="s", variables=("swe",), files_glob="*.nc", std_variables=("swe",))
    assert a2.std_variables == ("swe",)
    with pytest.raises(ValueError, match="std_variables"):
        SourceAdapter(source_key="s", variables=("swe",), files_glob="*.nc", std_variables=("missing",))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_aggregate_adapter.py::test_std_variables_defaults_empty_and_validates -v`
Expected: FAIL — unexpected keyword `std_variables`.

- [ ] **Step 3: Write minimal implementation**

In `src/gfv2_params/aggregate/adapter.py`, add the field after `grid_variable`:

```python
    std_variables: tuple[str, ...] = field(default=())
```

And in `__post_init__` (after the existing `grid_variable` checks):

```python
        object.__setattr__(self, "std_variables", tuple(self.std_variables))
        missing = [v for v in self.std_variables if v not in self.variables]
        if missing:
            raise ValueError(
                f"std_variables {missing} must all be in variables {self.variables}"
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_aggregate_adapter.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/aggregate/adapter.py tests/test_aggregate_adapter.py
git commit -m "feat(aggregate): SourceAdapter.std_variables field + validation

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: `masked_std` sidecar pass in the driver

**Files:**
- Modify: `src/gfv2_params/aggregate/driver.py`
- Test: `tests/test_aggregate_driver.py`

**Interfaces:**
- Consumes: `adapter.std_variables`, cached `weights`.
- Produces: `aggregate_variables` also emits `{var}_std` for each `var` in `adapter.std_variables`, via a second `AggGen(stat_method="masked_std", weights=<same>)`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_aggregate_driver.py — reuse _synthetic_grid/_two_polys
def test_std_sidecar_emits_var_std(tmp_path):
    _synthetic_grid(tmp_path)
    gdf = _two_polys()
    adapter = SourceAdapter(
        source_key="demo", variables=("swe",), files_glob="demo_daily_*.nc",
        source_crs="EPSG:5070", x_coord="x", y_coord="y", time_coord="time",
        stat_method="mean", std_variables=("swe",),
    )
    out = aggregate_source(adapter, gdf, "hru_id", input_dir=tmp_path,
                           output_dir=tmp_path / "out", weight_file=tmp_path / "w.csv",
                           output_prefix="demo")
    res = xr.open_dataset(out[0])
    assert "swe" in res and "swe_std" in res
    # day 0 left poly: cells all 1.0 -> std 0; (see _synthetic_grid values)
    assert float(res["swe_std"].sel(hru_id=1).values[0]) == pytest.approx(0.0, abs=1e-6)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_aggregate_driver.py::test_std_sidecar_emits_var_std -v`
Expected: FAIL — `swe_std` not in result.

- [ ] **Step 3: Write minimal implementation**

In `src/gfv2_params/aggregate/driver.py`, at the end of `aggregate_variables`, before `return ds`, add:

```python
    if adapter.std_variables:
        std_user = UserCatData(
            source_ds=source_ds, source_crs=adapter.source_crs,
            source_x_coord=adapter.x_coord, source_y_coord=adapter.y_coord,
            source_t_coord=adapter.time_coord,
            source_var=list(adapter.std_variables),
            target_gdf=fabric_gdf, target_crs=WEIGHT_GEN_CRS, target_id=id_col,
            source_time_period=[period[0], period[1]],
        )
        std_agg = AggGen(user_data=std_user, stat_method="masked_std",
                         agg_engine="serial", agg_writer="none", weights=weights)
        _g, std_ds = std_agg.calculate_agg()
        for v in adapter.std_variables:
            if v not in std_ds.data_vars:
                raise RuntimeError(f"masked_std pass produced no {v!r} (got {list(std_ds.data_vars)})")
            ds[f"{v}_std"] = std_ds[v]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_aggregate_driver.py -v`
Expected: PASS (existing tests unaffected — `std_variables` defaults empty).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/aggregate/driver.py tests/test_aggregate_driver.py
git commit -m "feat(aggregate): masked_std sidecar pass emitting {var}_std

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: SNODAS adapter `std_variables=("swe",)`

**Files:**
- Modify: `src/gfv2_params/aggregate/snodas.py`, `configs/aggregate/aggregate_sources.yml`
- Test: `tests/test_aggregate_snodas.py`

**Interfaces:**
- Produces: `SNODAS_ADAPTER.std_variables == ("swe",)`; Stage-1 SNODAS output carries `swe_std`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_aggregate_snodas.py
def test_snodas_adapter_declares_swe_std():
    assert SNODAS_ADAPTER.std_variables == ("swe",)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_aggregate_snodas.py::test_snodas_adapter_declares_swe_std -v`
Expected: FAIL — `std_variables == ()`.

- [ ] **Step 3: Write minimal implementation**

In `src/gfv2_params/aggregate/snodas.py`, add `std_variables=("swe",)` to the `SNODAS_ADAPTER` constructor. In `configs/aggregate/aggregate_sources.yml`, add a comment under the `snodas` source noting `swe_std` is emitted (no config key needed — it's adapter-driven).

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_aggregate_snodas.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/aggregate/snodas.py configs/aggregate/aggregate_sources.yml tests/test_aggregate_snodas.py
git commit -m "feat(aggregate): SNODAS adapter emits swe_std sidecar

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: `subgrid.py` — `representative_peak_stats`

**Files:**
- Create: `src/gfv2_params/snarea/subgrid.py`
- Test: `tests/test_snarea_subgrid.py`

**Interfaces:**
- Produces: `representative_peak_stats(daily: pd.DataFrame) -> dict` with keys `cv_subgrid`, `peak_swe_mm`, `n_peak_years`. `daily` has a DatetimeIndex and columns `swe` (mean), `swe_std`. Groups by water year (Oct 1–Sep 30, reusing `build.py`'s framing), finds each year's peak-mean-SWE day, records `cv_year = swe_std[peak]/swe[peak]` and `peak_swe = swe[peak]`; skips years with `swe[peak] <= 0` or non-finite. Returns medians + count (NaN/0 if none).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_snarea_subgrid.py
import numpy as np
import pandas as pd

from gfv2_params.snarea.subgrid import representative_peak_stats


def _daily(dates, swe, std):
    return pd.DataFrame({"swe": swe, "swe_std": std}, index=pd.to_datetime(dates))


def test_peak_stats_single_water_year():
    # one melt season peaking Apr 1 2011: peak swe 200, std 100 -> cv 0.5
    dates = pd.date_range("2010-10-01", "2011-09-30")
    swe = np.concatenate([np.linspace(0, 200, 183), np.linspace(200, 0, len(dates) - 183)])
    std = swe * 0.5
    out = representative_peak_stats(_daily(dates, swe, std))
    assert out["n_peak_years"] == 1
    assert out["cv_subgrid"] == pytest.approx(0.5, abs=1e-6)
    assert out["peak_swe_mm"] == pytest.approx(200.0, abs=1e-6)


def test_peak_stats_no_snow_returns_nan():
    dates = pd.date_range("2010-10-01", "2011-09-30")
    out = representative_peak_stats(_daily(dates, np.zeros(len(dates)), np.zeros(len(dates))))
    assert out["n_peak_years"] == 0
    assert np.isnan(out["cv_subgrid"])


def test_peak_stats_median_across_years():
    d1 = pd.date_range("2010-10-01", "2011-09-30")
    d2 = pd.date_range("2011-10-01", "2012-09-30")
    def season(dates, peak, cvf):
        h = len(dates) // 2
        swe = np.concatenate([np.linspace(0, peak, h), np.linspace(peak, 0, len(dates) - h)])
        return swe, swe * cvf
    s1, t1 = season(d1, 100, 0.4)
    s2, t2 = season(d2, 300, 0.8)
    df = pd.concat([_daily(d1, s1, t1), _daily(d2, s2, t2)])
    out = representative_peak_stats(df)
    assert out["n_peak_years"] == 2
    assert out["cv_subgrid"] == pytest.approx(0.6, abs=1e-6)   # median(0.4, 0.8)
    assert out["peak_swe_mm"] == pytest.approx(200.0, abs=1e-6)  # median(100, 300)
```
(Add `import pytest` at top.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_subgrid.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/snarea/subgrid.py
"""Representative sub-grid SWE CV + peak SWE per HRU, for the CV/lognormal library.

CV = area-weighted std/mean of the SNODAS SWE pdf within an HRU, taken at each
water-year's peak-mean-SWE day (CV is most stable where mean SWE is largest), then
median across years. Water-year framing matches snarea/build.py:_seasons so a
late-December accumulation is not mis-picked as the peak.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def representative_peak_stats(daily: pd.DataFrame) -> dict:
    if len(daily) == 0:
        return {"cv_subgrid": float("nan"), "peak_swe_mm": float("nan"), "n_peak_years": 0}
    water_year = daily.index.year + (daily.index.month >= 10).astype(int)
    cvs, peaks = [], []
    for _wy, grp in daily.groupby(water_year):
        swe = grp["swe"].to_numpy(float)
        std = grp["swe_std"].to_numpy(float)
        if not np.isfinite(swe).any() or np.nanmax(swe) <= 0:
            continue
        i = int(np.nanargmax(swe))
        peak, s = swe[i], std[i]
        if not (np.isfinite(peak) and peak > 0 and np.isfinite(s)):
            continue
        cvs.append(s / peak)
        peaks.append(peak)
    if not cvs:
        return {"cv_subgrid": float("nan"), "peak_swe_mm": float("nan"), "n_peak_years": 0}
    return {"cv_subgrid": float(np.median(cvs)), "peak_swe_mm": float(np.median(peaks)),
            "n_peak_years": len(cvs)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_subgrid.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/subgrid.py tests/test_snarea_subgrid.py
git commit -m "feat(snarea): representative_peak_stats — sub-grid CV + peak SWE

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: Wire sub-grid stats into Stage 2 + repoint output

**Files:**
- Modify: `scripts/derive_snarea_curve.py`, `src/gfv2_params/snarea/build.py`, `configs/snarea/snarea_curve.yml`
- Test: `tests/test_derive_snarea_curve.py`, `tests/test_snarea_build.py`

**Interfaces:**
- Consumes: `representative_peak_stats`.
- Produces: `read_daily_by_hru` frames include `swe_std`; `build_hru_record`/`build_snarea_curve` add `cv_subgrid`, `peak_swe_mm`, `n_peak_years`; Stage 2 writes `_intermediates/nhm_snarea_curve_derived.csv`.

- [ ] **Step 1: Write the failing tests**

```python
# add to tests/test_derive_snarea_curve.py
def test_read_daily_by_hru_includes_swe_std(tmp_path):
    import numpy as np, pandas as pd, xarray as xr
    from gfv2_params.snarea import ...  # existing imports
    t = pd.date_range("2011-01-01", periods=3)
    ds = xr.Dataset(
        {"swe": (("time", "nat_hru_id"), np.array([[1.0], [2.0], [0.0]])),
         "scov": (("time", "nat_hru_id"), np.array([[1.0], [1.0], [0.0]])),
         "swe_std": (("time", "nat_hru_id"), np.array([[0.5], [1.0], [0.0]]))},
        coords={"time": t, "nat_hru_id": [7]},
    )
    ds.to_netcdf(tmp_path / "snodas_agg_2011.nc")
    from scripts.derive_snarea_curve import read_daily_by_hru  # or module import per repo
    daily = read_daily_by_hru(tmp_path, "nat_hru_id")
    assert "swe_std" in daily[7].columns
```

```python
# add to tests/test_snarea_build.py — build_hru_record carries the new columns
def test_build_hru_record_has_subgrid_columns():
    import numpy as np, pandas as pd
    from gfv2_params.snarea.build import build_hru_record
    from gfv2_params.snarea.selection import SelectionParams
    dates = pd.date_range("2010-10-01", "2011-09-30")
    h = len(dates) // 2
    swe = np.concatenate([np.linspace(0, 200, h), np.linspace(200, 0, len(dates) - h)])
    daily = pd.DataFrame({"swe": swe, "sca": (swe > 0).astype(float), "swe_std": swe * 0.5},
                         index=dates)
    rec = build_hru_record(7, daily, n_cells=50, water_frac=0.0,
                           params=SelectionParams(), default_curve=np.linspace(1, 0, 11))
    for c in ("cv_subgrid", "peak_swe_mm", "n_peak_years"):
        assert c in rec
    assert rec["cv_subgrid"] == pytest.approx(0.5, abs=1e-6)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_snarea_build.py::test_build_hru_record_has_subgrid_columns tests/test_derive_snarea_curve.py -v`
Expected: FAIL — no `swe_std`/`cv_subgrid`.

- [ ] **Step 3: Write minimal implementation**

- In `scripts/derive_snarea_curve.py::read_daily_by_hru`, change the `to_dataframe()` selection and the per-HRU frame to include `swe_std`:
  ```python
  df = ds[["swe", "scov", "swe_std"]].to_dataframe().reset_index()
  df = df.rename(columns={"scov": "sca"})
  ...
  s = grp.set_index("time")[["swe", "sca", "swe_std"]].sort_index()
  ```
  (Keep the load-phase logging; `swe_std` rides the same frame.)
- In `src/gfv2_params/snarea/build.py::build_hru_record`, after computing the empirical record, add:
  ```python
  from .subgrid import representative_peak_stats
  ...
  stats = representative_peak_stats(daily) if "swe_std" in daily.columns else {
      "cv_subgrid": float("nan"), "peak_swe_mm": float("nan"), "n_peak_years": 0}
  record.update(stats)
  ```
- In `configs/snarea/snarea_curve.yml`, change:
  ```yaml
  output_dir: "{data_root}/{fabric}/params/merged/_intermediates"
  merged_file: nhm_snarea_curve_derived.csv
  ```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_snarea_build.py tests/test_derive_snarea_curve.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/derive_snarea_curve.py src/gfv2_params/snarea/build.py configs/snarea/snarea_curve.yml tests/test_snarea_build.py tests/test_derive_snarea_curve.py
git commit -m "feat(snarea): Stage 2 emits sub-grid CV/peak SWE to _intermediates

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase C — Stage 3 driver, ops, docs

### Task 14: Stage 3 driver + config

**Files:**
- Create: `scripts/derive_snarea_library.py`, `configs/snarea/snarea_library.yml`
- Test: `tests/test_snarea_library.py` (add an end-to-end driver test on a tiny synthetic derived CSV)

**Interfaces:**
- Consumes: all `library.py` functions; the Stage-2 derived CSV.
- Produces: `build_from_derived(derived: pd.DataFrame, id_feature: str, ndepl_cv: int, default_curve: np.ndarray, calibrate: str, bias_tol: float) -> tuple[library_df, params_df, report]` (the orchestration function, so it's unit-testable without file IO); `main()` CLI.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_snarea_library.py
from gfv2_params.snarea.library import build_from_derived


def test_build_from_derived_end_to_end():
    rng = np.random.default_rng(0)
    n = 300
    cv = np.clip(rng.normal(0.5, 0.2, n), 0.1, 1.5)
    derived = pd.DataFrame({
        "nat_hru_id": np.arange(1, n + 1),
        "sdc_status": ["derived"] * n,
        "sca_class": ["high"] * n,
        "similarity": 0.05, "n_seasons": 10,
        "cv_subgrid": cv,
        "cv_empirical": cv,   # unbiased -> no calibration
        "peak_swe_mm": 254.0, "n_peak_years": 10,
        **{c: np.tile(sdc_from_cv(0.5)[i], n) for i, c in enumerate(CURVE_COLS)},
    })
    lib, params, report = build_from_derived(derived, "nat_hru_id", ndepl_cv=8,
                                             default_curve=np.linspace(1, 0, 11),
                                             calibrate="auto", bias_tol=0.1)
    assert len(lib) == 9
    assert len(params) == n
    assert set(params["hru_deplcrv"].unique()) <= set(range(2, 10))   # all estimable -> cv bins
    assert report["calibrated"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_library.py::test_build_from_derived_end_to_end -v`
Expected: FAIL — `build_from_derived` not found.

- [ ] **Step 3: Write the orchestrator + driver**

Add `build_from_derived` to `library.py`:

```python
def build_from_derived(derived, id_feature, ndepl_cv, default_curve, calibrate="auto", bias_tol=0.1):
    """Estimable = finite cv_subgrid (>=2 snow cells). Calibrate vs cv_empirical on
    the derived overlap, bin, assign, assemble."""
    derived = derived.reset_index(drop=True)
    n = len(derived)
    cv_sub = derived["cv_subgrid"].to_numpy(float)
    # cv_empirical only for derived HRUs whose empirical curve is present
    cv_emp = derived.get("cv_empirical", pd.Series(np.full(n, np.nan))).to_numpy(float)
    emp_curves = (derived[CURVE_COLS].to_numpy(float)
                  if all(c in derived.columns for c in CURVE_COLS)
                  else np.full((n, 11), np.nan))
    cal, report = validate_and_calibrate(cv_sub, cv_emp, emp_curves, mode=calibrate, bias_tol=bias_tol)

    # cv_assign: calibrated subgrid if finite; else cv_empirical if finite; else default
    cv_assign = np.where(np.isfinite(cal), cal, cv_emp)
    cv_source = np.where(np.isfinite(cal),
                         "subgrid_calibrated" if report["calibrated"] else "subgrid",
                         np.where(np.isfinite(cv_emp), "empirical", "default_no_snow"))
    library = build_library(cv_assign[np.isfinite(cv_assign)], ndepl_cv, default_curve)
    deplcrv = assign_deplcrv(cv_assign, library)
    params = assemble_params(derived, id_feature, cv_assign, cv_source, deplcrv, library)
    report["n_hru"] = n
    report["n_estimable"] = int(np.isfinite(cv_assign).sum())
    return library, params, report
```

Create `scripts/derive_snarea_library.py`:

```python
"""Stage 3 driver: build the CV/lognormal snarea_curve library from the Stage 2
derived CSV. Pure tabular — cheap, re-runnable at any ndepl_cv without reloading
the daily SWE. Fabric-agnostic (paths from the profile via require_config_key)."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.snarea.build import DEFAULT_SNAREA_CURVE
from gfv2_params.snarea.library import (
    build_from_derived, write_library_csv, write_params_csv, write_validation_csv,
    write_prms_netcdf,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--config", default="configs/snarea/snarea_library.yml")
    ap.add_argument("--base_config", default="configs/base_config.yml")
    args = ap.parse_args()
    logger = configure_logging("derive_snarea_library")

    cfg = load_config(Path(args.config), base_config_path=Path(args.base_config), fabric=args.fabric)
    id_feature = require_config_key(cfg, "id_feature", "derive_snarea_library")
    derived_csv = Path(cfg["derived_csv"])
    out_dir = Path(cfg["output_dir"])
    ndepl_cv = int(cfg.get("ndepl_cv", 8))
    calibrate = cfg.get("calibrate", "auto")
    bias_tol = float(cfg.get("calibrate_bias_tol", 0.1))
    default_curve = np.asarray(cfg.get("default_curve", DEFAULT_SNAREA_CURVE), dtype=float)

    logger.info("Reading Stage 2 derived table: %s", derived_csv)
    derived = pd.read_csv(derived_csv)
    logger.info("Building CV/lognormal library (ndepl_cv=%d, calibrate=%s) for %d HRUs ...",
                ndepl_cv, calibrate, len(derived))
    library, params, report = build_from_derived(
        derived, id_feature, ndepl_cv, default_curve, calibrate, bias_tol)

    write_library_csv(library, out_dir / cfg["library_file"])
    write_params_csv(params, out_dir / cfg["params_file"])
    write_validation_csv(report, out_dir / cfg["validation_file"])
    write_prms_netcdf(library, params, id_feature, out_dir / cfg["netcdf_file"])
    logger.info("ndepl=%d | estimable %d/%d | calibrated=%s | recon mean %.3f",
                len(library), report["n_estimable"], report["n_hru"],
                report["calibrated"], report.get("recon_mean_after", float("nan")))


if __name__ == "__main__":
    main()
```

Create `configs/snarea/snarea_library.yml`:

```yaml
# Stage 3: CV/lognormal snarea_curve library from the Stage 2 derived CSV.
# Top-level placeholders resolve {data_root}/{fabric}; nested blocks pass through.
derived_csv: "{data_root}/{fabric}/params/merged/_intermediates/nhm_snarea_curve_derived.csv"
output_dir: "{data_root}/{fabric}/params/merged"
library_file: nhm_snarea_curve_library.csv
params_file: nhm_snarea_curve_params.csv
validation_file: nhm_snarea_curve_validation.csv
netcdf_file: nhm_snarea_curve.nc

ndepl_cv: 8              # 8 CV bins + 1 reserved default = ndepl 9 (grouping memory: elbow 5-8)
calibrate: auto          # auto | on | off
calibrate_bias_tol: 0.1  # |median(cv_subgrid) - median(cv_empirical)| trigger
default_curve: [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]
```

- [ ] **Step 4: Run tests + a real smoke test**

Run: `pixi run -e dev pytest tests/test_snarea_library.py -v` → PASS.
Then smoke-test on the existing Oregon derived table (the current file lacks `cv_subgrid`; it will exercise the `empirical`/`default` fallback path — expected until Stage 1/2 re-run):
```bash
pixi run -e dev python -c "
import pandas as pd, numpy as np
from gfv2_params.snarea.library import build_from_derived, CURVE_COLS
d = pd.read_csv('/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/oregon/params/merged/nhm_snarea_curve_params.csv')
d = d.rename(columns={'hru_id':'nat_hru_id'}) if 'hru_id' in d.columns else d
d['cv_subgrid'] = np.nan  # no subgrid yet
from gfv2_params.snarea.library import fit_cv
d['cv_empirical'] = [fit_cv(r[CURVE_COLS].to_numpy(float)) if s=='derived' else np.nan
                     for s,(_,r) in zip(d['sdc_status'], d.iterrows())]
d['peak_swe_mm']=np.nan; d['n_peak_years']=0
lib,params,rep = build_from_derived(d,'nat_hru_id',8,np.linspace(1,0,11),'auto',0.1)
print('ndepl',len(lib),'params',len(params),'estimable',rep['n_estimable'])
"
```
Expected: runs; estimable count reflects derived HRUs via the empirical fallback (sanity only).

- [ ] **Step 5: Commit**

```bash
git add scripts/derive_snarea_library.py configs/snarea/snarea_library.yml src/gfv2_params/snarea/library.py tests/test_snarea_library.py
git commit -m "feat(snarea): Stage 3 library driver + config (build_from_derived)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 15: SLURM batch + `__init__` exports

**Files:**
- Create: `slurm_batch/derive_snarea_library.batch`
- Modify: `src/gfv2_params/snarea/__init__.py`
- Test: `py_compile` + import check

**Interfaces:**
- Produces: `slurm_batch/derive_snarea_library.batch`; `gfv2_params.snarea` re-exports the library entry points.

- [ ] **Step 1: Create the batch**

```bash
# slurm_batch/derive_snarea_library.batch
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=snarea_library
#SBATCH --output=logs/job_%j.out
#SBATCH --error=logs/job_%j.err
#SBATCH --time=00:30:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#
# Stage 3: build the CV/lognormal snarea_curve library from the Stage 2 derived
# CSV. Pure tabular — cheap. Re-runnable at any ndepl_cv without a daily reload.
#   FABRIC=gfv2 sbatch slurm_batch/derive_snarea_library.batch
cd "$SLURM_SUBMIT_DIR"
BASE_CONFIG=${BASE_CONFIG:-configs/base_config.yml}
FABRIC=${FABRIC:-oregon}
pixi run --as-is python scripts/derive_snarea_library.py \
    --fabric "$FABRIC" --config configs/snarea/snarea_library.yml --base_config "$BASE_CONFIG"
```

- [ ] **Step 2: Add exports**

In `src/gfv2_params/snarea/__init__.py`, extend `__all__` and imports:

```python
from .library import build_from_derived, build_library, sdc_from_cv
__all__ = ["build_snarea_curve", "DEFAULT_SNAREA_CURVE",
           "build_from_derived", "build_library", "sdc_from_cv"]
```

- [ ] **Step 3: Verify**

Run:
```bash
python -m py_compile scripts/derive_snarea_library.py slurm_batch/../src/gfv2_params/snarea/library.py
pixi run -e dev python -c "from gfv2_params.snarea import build_from_derived, sdc_from_cv; print('ok')"
```
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add slurm_batch/derive_snarea_library.batch src/gfv2_params/snarea/__init__.py
git commit -m "feat(snarea): Stage 3 SLURM batch + package exports

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 16: Docs + memory + PR

**Files:**
- Modify: `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`, `README.md`, `docs/ARCHITECTURE.md`, `docs/superpowers/specs/2026-07-06-...-library-design.md` (citation fix)
- Memory: `sdc_grouping_decision`, `snodas_snarea_curve_pipeline`

- [ ] **Step 1: Docs edits**
  - `RUNME.md` Step 8: after Stage 2, add Stage 3 (`derive_snarea_library.batch`), and note Stage 1 must be re-run once to add `swe_std` (weights cached).
  - `HPC_REFERENCE.md` Stage 10: document Stage 3 (inputs = derived CSV; outputs = library/params/validation CSVs + `.nc`; cheap, 16G/30min); note `swe_std` sidecar in Stage 1 and the sub-grid CV in Stage 2.
  - `README.md` snarea section: outputs are now a curve library + per-HRU params + a pyWatershed `.nc`.
  - `docs/ARCHITECTURE.md`: if it enumerates Part 2c stages, add Stage 3.
  - Fix the spec's method-source citation to *Sexstone, Driscoll, Hay, Hammond & Barnhart (2020), "Runoff sensitivity to snow depletion curve representation within a continental scale hydrologic model," Hydrological Processes 34:2365–2380.*

- [ ] **Step 2: Update memory**
  - `sdc_grouping_decision.md`: mark IMPLEMENTED (Stage 3 library, sub-grid-CV primary, calibration gate); link the plan.
  - `snodas_snarea_curve_pipeline.md`: add Stage 3 + the `swe_std`/sub-grid additions.

- [ ] **Step 3: Full pre-commit + single-file test sanity**

```bash
pixi run -e dev pre-commit run --all-files
pixi run -e dev pytest tests/test_snarea_library.py tests/test_snarea_subgrid.py tests/test_aggregate_snodas.py -v
```
Expected: hooks pass; targeted tests pass. (CI runs the full suite on push.)

- [ ] **Step 4: Commit docs**

```bash
git add slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md README.md docs/ARCHITECTURE.md docs/superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md
git commit -m "docs(snarea): Stage 3 library runbook + architecture + citation fix

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 5: Push + open PR (curl+REST; gh blocked)**

Push the branch, then POST to the pulls API with `--data-binary` (see [[gh_cli_blocked_use_curl_rest]]). PR body: summarize the 3 stages, the sub-grid-CV-primary + calibration-gate design, the validation results (fill in after the Oregon/CONUS runs), and link the spec + plan.

---

## HPC rollout (after the PR is green) — not code tasks

1. **Oregon** (`FABRIC=oregon`): re-run Stage 1 (`derive_snodas_aggregate.batch`, adds `swe_std`; weights cached → cheap) → merge → Stage 2 (`derive_snarea_curve.batch`, adds sub-grid stats) → Stage 3 (`derive_snarea_library.batch`). Inspect `nhm_snarea_curve_validation.csv`: `cv_subgrid` vs `cv_empirical`, reconstruction (~0.03 target), whether calibration fired, coverage vs the near-linear default, Cascades-high-CV / desert-low-CV sanity.
2. **CONUS gfv2** (`FABRIC=gfv2`): same, Stage 2 at `--mem=384G`. Confirm `ndepl`=9 reconstructs the derived population to ~0.03 mean SCA and coverage improves markedly over the 58%-default baseline.

---

## Self-review

- **Spec coverage:** §3 swe_std → Tasks 9–11; §4 sub-grid CV/peak → Tasks 12–13; §5.1 sdc_from_cv → Task 1; §5.2 fit_cv/library/assign/calibrate/thresh → Tasks 2–6; §6 serialization (4 artifacts + order flip) → Tasks 7–8; §7 module/config/test/deps → Tasks 8,14,15; §8 rollout → HPC section; §9 judgment calls encoded in config defaults (Task 14). Covered.
- **Placeholder scan:** every code step carries complete code; no TBD/TODO.
- **Type consistency:** `sdc_from_cv`/`fit_cv`/`build_library`/`assign_deplcrv`/`validate_and_calibrate`/`assemble_params`/`build_from_derived`/`representative_peak_stats` signatures are defined once and reused verbatim across tasks; `CURVE_COLS`/`SWE_LEVELS`/`CV_GRID` are module constants. Curve order (descending in-repo, ascending in NetCDF via `_to_prms_order`) is consistent.
- **Open verification (flagged in Task 8):** the pyWatershed ascending-order convention is confirmed by inspecting `prms_snow` before implementing the writer — do not skip Task 8 Step 1.
