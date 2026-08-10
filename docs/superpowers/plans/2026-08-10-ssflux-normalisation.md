# ssflux Normalisation Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the seven `ssflux` parameters, currently ~90% pinned at their range minimum, by aggregating permeability as an intensive variable in log10 space and moving normalisation out of the per-batch task into a reduce step.

**Architecture:** Pure numerical helpers move to a new `ssflux_math.py` so the maths is testable without geo fixtures. `run_ssflux_batch` becomes a map phase emitting raw log-space `L_*` columns; a new `MERGE_REDUCERS` dispatch (mirroring the existing `BATCH_RUNNERS`) lets `run_merge` call an ssflux reducer that computes min/max over the whole fabric (or per VPU) and interpolates linearly.

**Tech Stack:** Python 3.12, pandas, numpy, gdptools (weights only), pixi, pytest.

**Spec:** [`../specs/2026-08-10-ssflux-normalisation-design.md`](../specs/2026-08-10-ssflux-normalisation-design.md)
**Issue:** [#175](https://github.com/rmcd-mscb/gfv2-params/issues/175)

## Global Constraints

- **Do not run `pytest` on the HPC login node.** CI (`.github/workflows/ci.yml`) is the test gate. Quick `py_compile`/import checks on the head node are fine.
- **`pre-commit run --all-files` must run under `srun` with `--mem=64G`** (the prettier hook OOMs). Targeted `--files a b c` runs are fine locally.
- Every declared config entry needs a `prms:` block; every emitted column goes in exactly one of `columns:` / `defects:` / `provenance:`. `processes:` is **per column**.
- After editing any `prms:` block run `python scripts/build_parameter_index.py`; CI fails if `docs/parameter_index.md` is stale.
- Paths and fabric inputs come from the profile via `require_config_key`, never hardcoded.
- **No exact `==` on computed floats.** The one deliberate exception is `k_perm == 0`, a *stored* no-data flag — comment why.
- **Rebuild on `gfv2_dev` first**, never the canonical `gfv2` product.
- Atomic commits; split combined fixes before pushing.

## File Structure

| File | Responsibility |
|---|---|
| `src/gfv2_params/zonal_runners/ssflux_math.py` | **new** — pure functions: intensive aggregation, log-additive derivation, linear interpolation. No I/O, no geo imports. |
| `src/gfv2_params/zonal_runners/ssflux.py` | modify — map phase; emits `k_perm_log_wtd`, `fflux`, `vpu`, `L_*`. No normalisation. |
| `src/gfv2_params/zonal_runners/merge.py` | modify — accept an optional `reducer` callable, call it after concat, before write. |
| `src/gfv2_params/zonal_runners/__init__.py` | modify — add `MERGE_REDUCERS`, re-export `run_ssflux_reduce`. |
| `scripts/derive_zonal_params.py` | modify — resolve `reducer:` tag → callable, pass to `run_merge`. |
| `configs/zonal/zonal_params.yml` | modify — `reducer:`, `norm_scope:`, `dprst_flow_coef` cap, `prms:` block. |
| `tests/test_ssflux_math.py` | **new** — unit tests for the pure functions. |
| `tests/test_ssflux.py` | **new** — map/reduce behaviour, batch invariance, CSV round-trip. |

---

### Task 1: Pure numerical helpers (`ssflux_math.py`)

Delivers tested pure functions with **no behaviour change** — nothing calls them yet.

**Files:**
- Create: `src/gfv2_params/zonal_runners/ssflux_math.py`
- Test: `tests/test_ssflux_math.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `aggregate_k_perm_log(weights: pd.DataFrame, id_feature: str, *, weight_col: str = "normalized_area_weight", k_col: str = "k_perm") -> pd.DataFrame` — returns columns `[id_feature, "k_perm_log_wtd", "fflux"]`.
  - `derive_log_params(k_perm_log_wtd: np.ndarray, slope_fraction: np.ndarray, hru_area: np.ndarray) -> dict[str, np.ndarray]` — keys are the seven parameter names.
  - `interpolate_to_range(values: np.ndarray, lo: float, hi: float) -> np.ndarray`
  - `validate_weight_coverage(weights: pd.DataFrame, id_feature: str, logger, *, weight_col: str = "normalized_area_weight", tol: float = 0.01, max_bad_fraction: float = 0.05) -> None`
  - Constants `K_PERM_NODATA = 0.0`, `FFLUX_NO_OVERLAP = -1.0`, `SLOPE_FLOOR = 1e-4`, `SLOPE_CEIL = 1.0 - 1e-4`, `PARAM_NAMES` (tuple of 7).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_ssflux_math.py`:

```python
"""Unit tests for the pure ssflux maths (no geo fixtures, no data root)."""

import numpy as np
import pandas as pd
import pytest

from gfv2_params.zonal_runners.ssflux_math import (
    FFLUX_NO_OVERLAP,
    PARAM_NAMES,
    aggregate_k_perm_log,
    derive_log_params,
    interpolate_to_range,
    validate_weight_coverage,
)

ID = "nat_hru_id"


def _weights(rows):
    return pd.DataFrame(rows, columns=[ID, "k_perm", "normalized_area_weight"])


def test_intensive_aggregation_is_area_weighted_mean():
    """(sum v*a)/(sum a) -- gdptools INTENSIVE form, not the extensive sum."""
    w = _weights([(1, -10.0, 0.25), (1, -14.0, 0.75)])
    out = aggregate_k_perm_log(w, ID)
    assert out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0] == pytest.approx(-13.0)


def test_weights_are_renormalised_when_coverage_is_partial():
    """A gap (weights summing to 0.5) must not halve the mean."""
    w = _weights([(1, -10.0, 0.25), (1, -14.0, 0.25)])
    out = aggregate_k_perm_log(w, ID)
    assert out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0] == pytest.approx(-12.0)
    assert out.loc[out[ID] == 1, "fflux"].iloc[0] == pytest.approx(0.5)


def test_nodata_k_perm_is_excluded_not_floored():
    """k_perm == 0 is a no-data flag; it must not enter the mean."""
    w = _weights([(1, -10.0, 0.5), (1, 0.0, 0.5)])
    out = aggregate_k_perm_log(w, ID)
    assert out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0] == pytest.approx(-10.0)
    assert out.loc[out[ID] == 1, "fflux"].iloc[0] == pytest.approx(0.5)


def test_hru_with_no_valid_lithology_is_nan_with_sentinel_fflux():
    w = _weights([(1, 0.0, 1.0)])
    out = aggregate_k_perm_log(w, ID)
    assert np.isnan(out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0])
    assert out.loc[out[ID] == 1, "fflux"].iloc[0] == FFLUX_NO_OVERLAP


def test_aggregation_rejects_missing_weight_column():
    w = _weights([(1, -10.0, 0.5)]).rename(columns={"normalized_area_weight": "wght"})
    with pytest.raises(ValueError, match="normalized_area_weight"):
        aggregate_k_perm_log(w, ID)


def test_derive_log_params_matches_closed_forms():
    k = np.array([-12.0])
    slope = np.array([0.25])
    area = np.array([100.0])
    out = derive_log_params(k, slope, area)
    assert out["soil2gw_max"][0] == pytest.approx(-36.0)              # 3*k
    assert out["ssr2gw_rate"][0] == pytest.approx(-12.0 + np.log10(0.75))
    expected_slow = -12.0 + np.log10(0.25) - np.log10(100.0)
    assert out["slowcoef_lin"][0] == pytest.approx(expected_slow)
    assert out["fastcoef_lin"][0] == pytest.approx(expected_slow + np.log10(2))
    assert out["gwflow_coef"][0] == pytest.approx(expected_slow)
    assert out["dprst_seep_rate_open"][0] == pytest.approx(out["ssr2gw_rate"][0])
    assert out["dprst_flow_coef"][0] == pytest.approx(out["fastcoef_lin"][0])
    assert set(out) == set(PARAM_NAMES)


def test_slope_guards_produce_finite_output():
    """slope == 0 and slope >= 1 must not yield -inf or NaN."""
    k = np.array([-12.0, -12.0, -12.0])
    slope = np.array([0.0, 1.0, 2.34])
    area = np.array([100.0, 100.0, 100.0])
    out = derive_log_params(k, slope, area)
    for name in PARAM_NAMES:
        assert np.all(np.isfinite(out[name])), name


def test_nan_k_perm_propagates_as_nan_not_an_error():
    out = derive_log_params(np.array([np.nan]), np.array([0.5]), np.array([100.0]))
    assert np.isnan(out["soil2gw_max"][0])


def test_derive_rejects_non_positive_area():
    with pytest.raises(ValueError, match="hru_area"):
        derive_log_params(np.array([-12.0]), np.array([0.5]), np.array([0.0]))


def test_interpolate_maps_endpoints_and_is_affine_invariant():
    v = np.array([-3.0, -2.0, -1.0])
    out = interpolate_to_range(v, 0.1, 0.3)
    assert out[0] == pytest.approx(0.1)
    assert out[-1] == pytest.approx(0.3)
    # the cube is a scale on the exponent -> absorbed by min-max normalisation
    assert interpolate_to_range(3 * v, 0.1, 0.3) == pytest.approx(out, abs=1e-12)


def test_interpolate_ignores_nan_but_preserves_position():
    v = np.array([-3.0, np.nan, -1.0])
    out = interpolate_to_range(v, 0.0, 1.0)
    assert out[0] == pytest.approx(0.0)
    assert np.isnan(out[1])
    assert out[2] == pytest.approx(1.0)


def test_interpolate_raises_on_degenerate_range():
    with pytest.raises(ValueError, match="degenerate"):
        interpolate_to_range(np.array([2.0, 2.0, 2.0]), 0.0, 1.0)


def test_extensive_form_is_not_silently_accepted():
    """A frame carrying ONLY the extensive columns must raise, not guess.

    Guards the #175 root cause: dividing by the source-polygon area is gdptools'
    EXTENSIVE form and must never be reachable again by accident.
    """
    w = pd.DataFrame(
        {ID: [1, 1], "k_perm": [-10.0, -14.0],
         "area_weight": [100.0, 300.0], "flux_id_area": [400.0, 1200.0]}
    )
    with pytest.raises(ValueError, match="extensive"):
        aggregate_k_perm_log(w, ID)


class _CapturingLogger:
    def __init__(self): self.warnings = []
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def warning(self, msg, *a): self.warnings.append(msg % a if a else msg)


def test_weight_coverage_passes_when_weights_sum_to_one():
    w = _weights([(1, -10.0, 0.5), (1, -12.0, 0.5), (2, -11.0, 1.0)])
    log = _CapturingLogger()
    validate_weight_coverage(w, ID, log)
    assert log.warnings == []


def test_weight_coverage_warns_on_gaps_and_overlaps():
    w = _weights([(1, -10.0, 0.4), (2, -11.0, 2.0), (3, -12.0, 1.0)])
    log = _CapturingLogger()
    validate_weight_coverage(w, ID, log, max_bad_fraction=0.9)
    assert any("outside" in m for m in log.warnings)


def test_weight_coverage_raises_when_too_many_are_bad():
    """A wholesale deviation from 1.0 means the wrong weight column."""
    w = _weights([(1, -10.0, 0.02), (2, -11.0, 0.03), (3, -12.0, 0.01)])
    with pytest.raises(ValueError, match="normalized_area_weight"):
        validate_weight_coverage(w, ID, _CapturingLogger())
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pixi run -e dev pytest tests/test_ssflux_math.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'gfv2_params.zonal_runners.ssflux_math'`.

- [ ] **Step 3: Write the implementation**

Create `src/gfv2_params/zonal_runners/ssflux_math.py`:

```python
"""Pure numerical helpers for the ssflux derivation.

Kept separate from ``ssflux.py`` so the maths is unit-testable without geo
fixtures, a data root, or SLURM. See
``docs/superpowers/specs/2026-08-10-ssflux-normalisation-design.md``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Gleeson k_perm is log10 permeability, so 0 would mean 1 m^2 -- physically
# impossible. It is the dataset's no-data flag (9,409 of 202,108 CONUS
# polygons). This is a STORED flag, not a computed value, so exact equality is
# correct here; a tolerance would risk swallowing a genuine measurement. Note
# -16.48 is NOT a sentinel -- it is the real least-permeable lithology class
# (26,441 polygons), which is why no-data must be excluded rather than floored.
K_PERM_NODATA = 0.0

# fflux value for an HRU with no valid lithology overlap at all, matching the
# reference implementation (Viger 2014, doi:10.5066/F7CN71XR).
FFLUX_NO_OVERLAP = -1.0

# mean_slope_fraction is tan(slope), so it is unbounded above; 317 CONUS HRUs
# are exactly 0 and 3 exceed 1 (up to 66.85 deg), where log10(slope) and
# log10(1 - slope) are undefined. Clamp rather than drop: these are real HRUs
# that still need a parameter value.
SLOPE_FLOOR = 1e-4
SLOPE_CEIL = 1.0 - 1e-4

# Below this width the input range carries no information and min-max
# normalisation would divide by ~0.
RANGE_ATOL = 1e-12

PARAM_NAMES = (
    "soil2gw_max",
    "ssr2gw_rate",
    "fastcoef_lin",
    "slowcoef_lin",
    "gwflow_coef",
    "dprst_seep_rate_open",
    "dprst_flow_coef",
)


def validate_weight_coverage(
    weights: pd.DataFrame,
    id_feature: str,
    logger,
    *,
    weight_col: str = "normalized_area_weight",
    tol: float = 0.01,
    max_bad_fraction: float = 0.05,
) -> None:
    """Assert the weights behave like gdptools' intensive ``wght``.

    ``wght`` sums to ~1.0 per target for a spatially continuous source layer.
    Real CONUS data is close but not exact: 1,828 HRUs have coverage gaps and 98
    have overlapping source polygons, so a handful outside ``[1-tol, 1+tol]`` is
    expected and only warned about.

    A *wholesale* deviation means the wrong column is being summed -- the
    extensive construction that caused #175 gave per-HRU sums spanning 3.6e13x
    with a median of 0.022. That raises. This is the guard that stops the
    extensive/intensive confusion from silently returning.
    """
    if weight_col not in weights.columns:
        raise ValueError(
            f"Weight frame is missing '{weight_col}', gdptools' intensive weight. "
            f"Columns: {sorted(weights.columns)}."
        )
    sums = weights.groupby(id_feature)[weight_col].sum()
    bad = ~np.isclose(sums.to_numpy(), 1.0, rtol=0.0, atol=tol)
    bad_fraction = float(bad.mean()) if len(sums) else 0.0
    if bad_fraction > max_bad_fraction:
        raise ValueError(
            f"{bad_fraction:.1%} of HRUs have sum({weight_col}) outside "
            f"1.0 +/- {tol} (median {float(sums.median()):.4f}, "
            f"min {float(sums.min()):.4g}, max {float(sums.max()):.4g}). "
            f"'{weight_col}' must be gdptools' `wght`, which sums to ~1 per "
            "target. A median far from 1 means an EXTENSIVE construction such as "
            "area_weight / <source>_area is being used -- that is issue #175."
        )
    if bad.any():
        logger.warning(
            "%d of %d HRUs have sum(%s) outside 1.0 +/- %s (min %.4g, max %.4g) -- "
            "lithology coverage gaps and overlapping source polygons; the "
            "aggregation renormalises by the per-HRU sum, so these are handled.",
            int(bad.sum()), len(sums), weight_col, tol,
            float(sums.min()), float(sums.max()),
        )


def aggregate_k_perm_log(
    weights: pd.DataFrame,
    id_feature: str,
    *,
    weight_col: str = "normalized_area_weight",
    k_col: str = "k_perm",
) -> pd.DataFrame:
    """Area-weighted mean of log10 permeability -- gdptools' INTENSIVE form.

    Permeability is intensive: it does not add up when regions combine, so the
    aggregation is ``(sum v_i a_i) / (sum a_i)``, NOT the extensive
    ``sum V_i (a_i / A_i)``. gdptools labels the source-polygon area column
    "for extensive variables"; using it here was the root cause of #175.

    ``weight_col`` is gdptools' ``wght`` (shipped as ``normalized_area_weight``),
    the proportional area of the source polygon within the target.

    Dividing by ``den`` is load-bearing beyond no-data exclusion: the lithology
    layer is not perfectly continuous (1,828 CONUS HRUs have coverage gaps, 98
    have overlapping source polygons).

    Returns one row per HRU with ``k_perm_log_wtd`` (NaN where no valid
    lithology) and ``fflux`` (valid-coverage fraction, ``FFLUX_NO_OVERLAP``
    where none).
    """
    for col in (id_feature, k_col, weight_col):
        if col not in weights.columns:
            raise ValueError(
                f"Weight frame is missing '{col}'. Columns: {sorted(weights.columns)}. "
                f"'{weight_col}' is gdptools' intensive weight and is required -- do "
                f"not substitute area_weight / <source>_area, which is the extensive form."
            )

    w = weights[[id_feature, k_col, weight_col]].copy()
    w[k_col] = w[k_col].astype(float)
    w[weight_col] = w[weight_col].astype(float)

    # Exact comparison is deliberate: a stored no-data flag, not a computed value.
    valid = w[k_col].notna() & (w[k_col] != K_PERM_NODATA)

    all_ids = pd.Index(w[id_feature].unique(), name=id_feature).sort_values()
    v = w[valid]
    num = (v[k_col] * v[weight_col]).groupby(v[id_feature]).sum().reindex(all_ids)
    den = v[weight_col].groupby(v[id_feature]).sum().reindex(all_ids)

    den_arr = den.to_numpy()
    covered = np.isfinite(den_arr) & (den_arr > 0.0)

    k_log = np.full(len(all_ids), np.nan)
    np.divide(num.to_numpy(), den_arr, out=k_log, where=covered)

    fflux = np.where(covered, den_arr, FFLUX_NO_OVERLAP)

    return pd.DataFrame(
        {id_feature: all_ids.to_numpy(), "k_perm_log_wtd": k_log, "fflux": fflux}
    )


def derive_log_params(
    k_perm_log_wtd: np.ndarray,
    slope_fraction: np.ndarray,
    hru_area: np.ndarray,
) -> dict[str, np.ndarray]:
    """TM 6-B9's structure, expressed additively in log10 space.

    Because ``k_perm_log_wtd`` is already log10, a product becomes a sum and the
    cube becomes a factor of 3 on the exponent::

        L(soil2gw_max)  = 3*k
        L(ssr2gw_rate)  =   k + log10(1 - slope)
        L(slowcoef_lin) =   k + log10(slope) - log10(hru_area)

    The cube is on ``soil2gw_max`` only, matching the reference implementation's
    FGDC attribute definitions (Viger 2014) and today's code -- TM 6-B9 line 790
    states a cube for the other two, but the 2014 source metadata does not. It
    makes no difference to output anyway: for ``soil2gw_max`` the x3 is a pure
    scale on the exponent and is fully absorbed by the later min-max
    interpolation.

    NaN in ``k_perm_log_wtd`` propagates to NaN, which is the signal for the
    downstream KNN gap-fill.
    """
    k = np.asarray(k_perm_log_wtd, dtype=float)
    slope = np.asarray(slope_fraction, dtype=float)
    area = np.asarray(hru_area, dtype=float)

    if np.any(~np.isfinite(area) | (area <= 0.0)):
        bad = int(np.sum(~np.isfinite(area) | (area <= 0.0)))
        raise ValueError(
            f"hru_area must be finite and > 0 for log10; {bad} row(s) violate this. "
            "A non-positive area means the fabric geometry is degenerate."
        )

    slope_c = np.clip(slope, SLOPE_FLOOR, SLOPE_CEIL)

    out: dict[str, np.ndarray] = {}
    out["soil2gw_max"] = 3.0 * k
    out["ssr2gw_rate"] = k + np.log10(1.0 - slope_c)
    out["slowcoef_lin"] = k + np.log10(slope_c) - np.log10(area)
    out["fastcoef_lin"] = out["slowcoef_lin"] + np.log10(2.0)
    out["gwflow_coef"] = out["slowcoef_lin"]
    out["dprst_seep_rate_open"] = out["ssr2gw_rate"]
    out["dprst_flow_coef"] = out["fastcoef_lin"]
    return out


def interpolate_to_range(values: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """Linearly scale ``values`` onto ``[lo, hi]`` -- TM 6-B9's interpolation.

    NaN is preserved in place (it marks an HRU for the KNN gap-fill) and is
    excluded from the min/max. ``min``/``max`` are exact and order-independent,
    which is what makes the merged output invariant to batch partitioning.
    """
    v = np.asarray(values, dtype=float)
    finite = np.isfinite(v)
    if not finite.any():
        raise ValueError("No finite values to interpolate; cannot derive a range.")

    lo_in = float(np.min(v[finite]))
    hi_in = float(np.max(v[finite]))
    span = hi_in - lo_in
    if span <= RANGE_ATOL:
        raise ValueError(
            f"Input range is degenerate (min={lo_in!r}, max={hi_in!r}, "
            f"span={span!r} <= {RANGE_ATOL}). Refusing to emit a constant field; "
            "this means every HRU shares one value, which is the #175 failure mode."
        )

    out = np.full(v.shape, np.nan)
    np.divide(v - lo_in, span, out=out, where=finite)
    return out * (hi - lo) + lo
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pixi run -e dev pytest tests/test_ssflux_math.py -v
```

Expected: 16 passed.

- [ ] **Step 5: Lint and commit**

```bash
pixi run -e dev pre-commit run --files \
  src/gfv2_params/zonal_runners/ssflux_math.py tests/test_ssflux_math.py
git add src/gfv2_params/zonal_runners/ssflux_math.py tests/test_ssflux_math.py
git commit -m "feat(ssflux): pure intensive-aggregation and log-space helpers (#175)"
```

---

### Task 2: `MERGE_REDUCERS` dispatch

Adds the reduce hook with **no ssflux behaviour change** — params without a `reducer:` key are untouched.

**Files:**
- Modify: `src/gfv2_params/zonal_runners/merge.py:53-118`
- Modify: `src/gfv2_params/zonal_runners/__init__.py:74-105`
- Modify: `scripts/derive_zonal_params.py:140-146`
- Test: `tests/test_merge_reducers.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces:
  - `run_merge(config: dict, logger, *, reducer: Callable[[pd.DataFrame, dict, object], pd.DataFrame] | None = None) -> None`
  - `MERGE_REDUCERS: dict[str, Callable]` in `zonal_runners/__init__.py` (empty at end of this task; Task 3 registers `"ssflux"`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_merge_reducers.py`:

```python
"""The optional reduce hook on run_merge."""

import pandas as pd
import pytest

from gfv2_params.zonal_runners import MERGE_REDUCERS, run_merge


class _Logger:
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def warning(self, *a, **k): pass


def _write_batches(tmp_path, n_batches=2):
    d = tmp_path / "ssflux"
    d.mkdir(parents=True)
    rows = [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)]
    per = len(rows) // n_batches
    for i in range(n_batches):
        chunk = rows[i * per:(i + 1) * per]
        pd.DataFrame(chunk, columns=["nat_hru_id", "L_x"]).to_csv(
            d / f"base_nhm_ssflux_testfab_batch_{i:04d}_param.csv", index=False
        )
    return {
        "source_type": "ssflux",
        "id_feature": "nat_hru_id",
        "merged_file": "out.csv",
        "fabric": "testfab",
        "output_dir": str(tmp_path),
    }


def test_merge_without_reducer_is_unchanged(tmp_path):
    cfg = _write_batches(tmp_path)
    run_merge(cfg, _Logger())
    out = pd.read_csv(tmp_path / "merged" / "out.csv")
    assert list(out.columns) == ["nat_hru_id", "L_x"]
    assert out["L_x"].tolist() == [10.0, 20.0, 30.0, 40.0]


def test_reducer_receives_full_frame_and_its_result_is_written(tmp_path):
    cfg = _write_batches(tmp_path)
    seen = {}

    def reducer(df, config, logger):
        seen["n"] = len(df)
        return df.assign(scaled=df["L_x"] / df["L_x"].max()).drop(columns=["L_x"])

    run_merge(cfg, _Logger(), reducer=reducer)
    out = pd.read_csv(tmp_path / "merged" / "out.csv")
    assert seen["n"] == 4, "reducer must see all batches, not one"
    assert "L_x" not in out.columns
    assert out["scaled"].max() == pytest.approx(1.0)


def test_reducer_must_return_a_dataframe(tmp_path):
    cfg = _write_batches(tmp_path)
    with pytest.raises(TypeError, match="DataFrame"):
        run_merge(cfg, _Logger(), reducer=lambda df, config, logger: None)


def test_merge_reducers_is_a_dict():
    assert isinstance(MERGE_REDUCERS, dict)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pixi run -e dev pytest tests/test_merge_reducers.py -v
```

Expected: `ImportError: cannot import name 'MERGE_REDUCERS'`.

- [ ] **Step 3: Add the `reducer` hook to `run_merge`**

In `src/gfv2_params/zonal_runners/merge.py`, change the signature and add the call. Replace the `def run_merge(config: dict, logger) -> None:` line and its docstring opening with:

```python
def run_merge(config: dict, logger, *, reducer=None) -> None:
    """Concat per-batch CSVs for one param into the merged output CSV.

    Originally extracted from the now-retired scripts/merge_params.py:process_files()
    (see PR #85). Validates no
    duplicates, warns on gaps (if expected_max_hru_id is set in config).

    ``reducer`` is an optional ``(df, config, logger) -> df`` callable applied
    once to the CONCATENATED frame, before the file is written. It exists for
    params whose final values need a statistic over the whole population and so
    cannot be computed per batch -- ssflux's min/max interpolation. The
    orchestrator resolves it from the config's ``reducer:`` tag via
    MERGE_REDUCERS; params without that tag keep today's behaviour exactly.
    """
```

Then insert the reducer call immediately **after** the `derived` block and **before** `output_path` (currently lines 111-116):

```python
    if reducer is not None:
        merged_df = reducer(merged_df, config, logger)
        if not isinstance(merged_df, pd.DataFrame):
            raise TypeError(
                f"reducer must return a pandas DataFrame, got "
                f"{type(merged_df).__name__}. It is applied to the concatenated "
                "frame and its return value is what gets written."
            )
        logger.info("Applied merge reducer: %s", config.get("reducer"))

    output_path = final_output_dir / merged_file
```

- [ ] **Step 4: Add `MERGE_REDUCERS` to the package**

In `src/gfv2_params/zonal_runners/__init__.py`, add `"MERGE_REDUCERS"` to `__all__` (keep it alphabetically first, beside `BATCH_RUNNERS`), then append after the `BATCH_RUNNERS` block:

```python
# Dispatch table: `reducer:` tag in configs/zonal/zonal_params.yml -> a
# (df, config, logger) -> df callable applied once to the CONCATENATED merged
# frame. Mirrors BATCH_RUNNERS above. Params with no `reducer:` tag are
# unaffected. Used by scripts/derive_zonal_params.py.
MERGE_REDUCERS = {}
```

- [ ] **Step 5: Wire the orchestrator**

In `scripts/derive_zonal_params.py`, change the import on line 32 to include `MERGE_REDUCERS`:

```python
from gfv2_params.zonal_runners import BATCH_RUNNERS, MERGE_REDUCERS, run_build_weights, run_merge
```

Then replace the `run_merge(param_cfg, logger)` call in `run_merge_mode` (line 146) with:

```python
    reducer_tag = param_cfg.get("reducer")
    reducer = None
    if reducer_tag is not None:
        if reducer_tag not in MERGE_REDUCERS:
            raise ValueError(
                f"Unknown reducer '{reducer_tag}' for param '{args.param}'. "
                f"Available: {sorted(MERGE_REDUCERS)}"
            )
        reducer = MERGE_REDUCERS[reducer_tag]
    run_merge(param_cfg, logger, reducer=reducer)
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
pixi run -e dev pytest tests/test_merge_reducers.py tests/test_zonal_runners_package.py -v
```

Expected: all pass.

- [ ] **Step 7: Lint and commit**

```bash
pixi run -e dev pre-commit run --files \
  src/gfv2_params/zonal_runners/merge.py \
  src/gfv2_params/zonal_runners/__init__.py \
  scripts/derive_zonal_params.py tests/test_merge_reducers.py
git add src/gfv2_params/zonal_runners/merge.py \
        src/gfv2_params/zonal_runners/__init__.py \
        scripts/derive_zonal_params.py tests/test_merge_reducers.py
git commit -m "feat(zonal): add MERGE_REDUCERS reduce hook to run_merge (#175)"
```

---

### Task 3: Cut ssflux over to map/reduce

The behaviour change. Map and reduce land **together** — the map phase stops writing final parameters, so the reducer must exist in the same commit or the pipeline is broken.

**Files:**
- Modify: `src/gfv2_params/zonal_runners/ssflux.py:66-148`
- Modify: `src/gfv2_params/zonal_runners/__init__.py` (register `"ssflux"`, re-export `run_ssflux_reduce`)
- Test: `tests/test_ssflux.py`

**Interfaces:**
- Consumes: `aggregate_k_perm_log`, `derive_log_params`, `interpolate_to_range`, `PARAM_NAMES` from Task 1; the `reducer=` kwarg and `MERGE_REDUCERS` from Task 2.
- Produces: `run_ssflux_reduce(df: pd.DataFrame, config: dict, logger) -> pd.DataFrame`; batch CSVs gain `k_perm_log_wtd`, `fflux`, `vpu`, `L_<param>` ×7 and lose `k_perm_wtd`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_ssflux.py`:

```python
"""Map/reduce behaviour for ssflux (no geo fixtures -- the reducer is pure)."""

import numpy as np
import pandas as pd
import pytest

from gfv2_params.zonal_runners import MERGE_REDUCERS, run_ssflux_reduce
from gfv2_params.zonal_runners.ssflux_math import PARAM_NAMES

ID = "nat_hru_id"

FLUX_PARAMS = [
    {"name": "soil2gw_max", "min": 0.1, "max": 0.3},
    {"name": "ssr2gw_rate", "min": 0.3, "max": 0.7},
    {"name": "fastcoef_lin", "min": 0.01, "max": 0.6},
    {"name": "slowcoef_lin", "min": 0.005, "max": 0.3},
    {"name": "gwflow_coef", "min": 0.005, "max": 0.3},
    {"name": "dprst_seep_rate_open", "min": 0.005, "max": 0.2},
    {"name": "dprst_flow_coef", "min": 0.005, "max": 0.1},
]


class _Logger:
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def warning(self, *a, **k): pass


def _frame(n=50, with_vpu=False):
    rng = np.random.default_rng(0)
    df = pd.DataFrame({ID: np.arange(1, n + 1)})
    for p in PARAM_NAMES:
        df[f"L_{p}"] = rng.normal(-12.0, 2.0, n)
    if with_vpu:
        df["vpu"] = np.where(df[ID] <= n // 2, "01", "02")
    return df


def _cfg(scope="fabric"):
    return {"id_feature": ID, "flux_params": FLUX_PARAMS, "norm_scope": scope}


def test_reduce_maps_into_configured_ranges_and_drops_L_columns():
    out = run_ssflux_reduce(_frame(), _cfg(), _Logger())
    for p in FLUX_PARAMS:
        col = out[p["name"]]
        assert col.min() == pytest.approx(p["min"])
        assert col.max() == pytest.approx(p["max"])
    assert not [c for c in out.columns if c.startswith("L_")]


def test_reduce_is_registered():
    assert MERGE_REDUCERS["ssflux"] is run_ssflux_reduce


def test_batch_invariance_is_bit_exact():
    """The issue's acceptance criterion: identical output across batch splits."""
    df = _frame(40)
    whole = run_ssflux_reduce(df.copy(), _cfg(), _Logger())
    # same rows, different partition + arrival order
    shuffled = pd.concat([df.iloc[20:], df.iloc[:20]], ignore_index=True)
    split = run_ssflux_reduce(shuffled, _cfg(), _Logger()).sort_values(ID).reset_index(drop=True)
    pd.testing.assert_frame_equal(whole, split)


def test_nan_rows_survive_for_gap_fill():
    df = _frame(20)
    df.loc[df[ID] == 5, [f"L_{p}" for p in PARAM_NAMES]] = np.nan
    out = run_ssflux_reduce(df, _cfg(), _Logger())
    assert out.loc[out[ID] == 5, "soil2gw_max"].isna().all()
    assert out["soil2gw_max"].notna().sum() == 19


def test_vpu_scope_normalises_within_each_region():
    out = run_ssflux_reduce(_frame(40, with_vpu=True), _cfg("vpu"), _Logger())
    for vpu in ("01", "02"):
        sub = out[out["vpu"] == vpu]["soil2gw_max"]
        assert sub.min() == pytest.approx(0.1)
        assert sub.max() == pytest.approx(0.3)


def test_vpu_scope_requires_the_column():
    with pytest.raises(ValueError, match="vpu"):
        run_ssflux_reduce(_frame(10), _cfg("vpu"), _Logger())


def test_unknown_norm_scope_raises():
    with pytest.raises(ValueError, match="norm_scope"):
        run_ssflux_reduce(_frame(10), _cfg("galaxy"), _Logger())


def test_missing_L_column_raises():
    df = _frame(10).drop(columns=["L_gwflow_coef"])
    with pytest.raises(ValueError, match="L_gwflow_coef"):
        run_ssflux_reduce(df, _cfg(), _Logger())


def test_csv_round_trip_is_lossless(tmp_path):
    """The map/reduce split writes L_* to CSV and reads them back."""
    df = _frame(30)
    p = tmp_path / "batch.csv"
    df.to_csv(p, index=False)
    back = pd.read_csv(p)
    pd.testing.assert_frame_equal(df, back)


def test_output_is_not_degenerate():
    """#175's acceptance criteria, on a realistic log-normal-ish input.

    The pre-fix product had >=89.5% of HRUs within 1% of the range minimum and
    an IQR spanning 0.1% of the range. Both thresholds here would have failed it.
    """
    out = run_ssflux_reduce(_frame(5000), _cfg(), _Logger())
    for p in FLUX_PARAMS:
        col = out[p["name"]].to_numpy()
        rng = p["max"] - p["min"]
        at_min = np.mean(col <= p["min"] + 0.01 * rng)
        iqr = np.subtract(*np.percentile(col, [75, 25])) / rng
        assert at_min <= 0.20, f"{p['name']}: {at_min:.1%} pinned at range minimum"
        assert iqr >= 0.10, f"{p['name']}: IQR spans only {iqr:.3f} of the range"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pixi run -e dev pytest tests/test_ssflux.py -v
```

Expected: `ImportError: cannot import name 'run_ssflux_reduce'`.

- [ ] **Step 3: Rewrite the ssflux map phase**

In `src/gfv2_params/zonal_runners/ssflux.py`, add to the imports:

```python
from .ssflux_math import (
    aggregate_k_perm_log,
    derive_log_params,
    interpolate_to_range,
    validate_weight_coverage,
)
```

(Do **not** import `PARAM_NAMES` here — the map phase iterates
`derive_log_params(...)`'s return value and the reducer iterates `flux_params`
from config, so an unused import would trip ruff's F401.)

Replace lines 66-84 (the source read through `extensive_sorted`) with:

```python
    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))

    weights["flux_id"] = weights["flux_id"].astype(str)
    source_gdf["flux_id"] = source_gdf["flux_id"].astype(str)
    w = weights.merge(source_gdf[["flux_id", "k_perm"]], on="flux_id")

    # Permeability is INTENSIVE: area-weighted mean, not an area-prorated sum.
    # k_perm == 0 is Gleeson's no-data flag and is excluded, not floored to
    # k_perm_min -- see ssflux_math.aggregate_k_perm_log and issue #175.
    validate_weight_coverage(w, id_feature, logger)
    agg = aggregate_k_perm_log(w, id_feature)
    agg[id_feature] = agg[id_feature].astype(int)
    k_perm_agg = agg.sort_values(by=id_feature).reset_index(drop=True)
```

The local names `extensive_agg` / `extensive_sorted` are **deleted**, not kept —
they assert the exact modelling error #175 fixes. Update their single downstream
use, line 99, from `extensive_sorted.merge(...)` to `k_perm_agg.merge(...)`.

Replace lines 111-142 (the `r_*` derivation through the `df.drop`) with:

```python
    # Raw values stay in log10 space; normalisation happens in the reduce step
    # (run_ssflux_reduce) because min/max must be taken over the whole fabric,
    # not over an arbitrary SLURM batch.
    log_params = derive_log_params(
        df["k_perm_log_wtd"].to_numpy(),
        df["mean_slope_fraction"].to_numpy(),
        df["hru_area"].to_numpy(),
    )
    for name, values in log_params.items():
        df[f"L_{name}"] = values
```

Add `vpu` to the batch frame. After the `area_df` block (line 93), add:

```python
    # Carried so the reducer can honour `norm_scope: vpu`. gfv2 is multi-VPU and
    # keys this off the per-HRU `vpu` attribute; single-VPU fabrics declare a
    # `vpu` scalar in their base_config profile instead.
    if "vpu" in target_gdf.columns:
        vpu_df = target_gdf[[id_feature, "vpu"]].copy()
    else:
        vpu_df = pd.DataFrame(
            {id_feature: target_gdf[id_feature], "vpu": config.get("vpu", pd.NA)}
        )
    vpu_df[id_feature] = vpu_df[id_feature].astype("int64")
```

and merge it alongside the others (after line 100's `area_df` merge):

```python
    df = df.merge(vpu_df, on=id_feature, how="left")
```

- [ ] **Step 4: Add the reducer**

Append to `src/gfv2_params/zonal_runners/ssflux.py`:

```python
def run_ssflux_reduce(df, config: dict, logger):
    """Normalise the log-space L_* columns onto each parameter's target range.

    Runs once on the CONCATENATED frame (see run_merge's `reducer` hook), never
    per batch: TM 6-B9 interpolates over "all HRUs in a GF region", and doing it
    per SLURM batch made two HRUs with identical geology and slope receive
    different values depending on how the fabric was chunked (#175 item 3).

    `norm_scope: fabric` (default) takes one min/max over every HRU, which makes
    the output invariant to batch partitioning by construction. `norm_scope: vpu`
    reproduces TM 6-B9's per-region wording, at the cost of discontinuities at
    VPU boundaries.
    """
    id_feature = config["id_feature"]
    flux_params = config["flux_params"]
    scope = config.get("norm_scope", "fabric")
    if scope not in ("fabric", "vpu"):
        raise ValueError(f"Unknown norm_scope '{scope}'; expected 'fabric' or 'vpu'.")

    missing = [f"L_{fp['name']}" for fp in flux_params if f"L_{fp['name']}" not in df.columns]
    if missing:
        raise ValueError(
            f"Merged frame is missing {missing}. The ssflux batch runner must emit "
            "L_* columns; a frame without them predates the #175 map/reduce split, "
            "so re-run --mode zonal before merging."
        )

    if scope == "vpu":
        if "vpu" not in df.columns or df["vpu"].isna().all():
            raise ValueError(
                "norm_scope: vpu needs a populated 'vpu' column in the merged frame. "
                "Multi-VPU fabrics carry it per HRU; single-VPU fabrics must declare "
                "a `vpu` scalar in their base_config profile."
            )
        groups = list(df.groupby("vpu").groups.items())
    else:
        groups = [("__fabric__", df.index)]

    out = df.copy()
    for fp in flux_params:
        name, lo, hi = fp["name"], float(fp["min"]), float(fp["max"])
        col = np.full(len(out), np.nan)
        for label, idx in groups:
            pos = out.index.get_indexer(idx)
            try:
                col[pos] = interpolate_to_range(out.loc[idx, f"L_{name}"].to_numpy(), lo, hi)
            except ValueError as exc:
                raise ValueError(f"{name} (scope={scope}, group={label}): {exc}") from exc
        out[name] = col

    out = out.drop(columns=[f"L_{fp['name']}" for fp in flux_params])
    logger.info(
        "Normalised %d ssflux params over scope=%s (%d group(s))",
        len(flux_params), scope, len(groups),
    )
    return out.sort_values(id_feature).reset_index(drop=True)
```

- [ ] **Step 5: Register the reducer**

In `src/gfv2_params/zonal_runners/__init__.py`, extend the ssflux import, add `"run_ssflux_reduce"` to `__all__`, and populate the table:

```python
from .ssflux import run_ssflux_batch, run_ssflux_reduce
```

```python
MERGE_REDUCERS = {
    "ssflux": run_ssflux_reduce,
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
pixi run -e dev pytest tests/test_ssflux.py tests/test_ssflux_math.py \
  tests/test_merge_reducers.py tests/test_zonal_runners_package.py -v
```

Expected: all pass.

- [ ] **Step 7: Lint and commit**

```bash
pixi run -e dev pre-commit run --files \
  src/gfv2_params/zonal_runners/ssflux.py \
  src/gfv2_params/zonal_runners/__init__.py tests/test_ssflux.py
git add src/gfv2_params/zonal_runners/ssflux.py \
        src/gfv2_params/zonal_runners/__init__.py tests/test_ssflux.py
git commit -m "fix(ssflux): intensive log-space aggregation + reduce-step normalisation (#175)"
```

---

### Task 4: Config, `prms:` block, and docs

**Files:**
- Modify: `configs/zonal/zonal_params.yml` (ssflux entry, starts line 451)
- Modify: `docs/parameter_index.md` (regenerated, do not hand-edit)
- Modify: `CLAUDE.md`, `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: the `reducer:` tag resolved in Task 2, the columns emitted in Task 3.
- Produces: no new code interfaces.

- [ ] **Step 1: Update the ssflux config entry**

In `configs/zonal/zonal_params.yml`, in the `- name: ssflux` entry:

Add below `depends_on: build_weights`:

```yaml
    # Normalisation runs once over the whole merged frame, not per batch --
    # min/max over an arbitrary SLURM chunk made identical HRUs disagree (#175).
    reducer: ssflux
    # `fabric` (one min/max over every HRU) is batch-invariant by construction.
    # `vpu` reproduces TM 6-B9's per-GF-region wording; it needs the per-HRU
    # `vpu` attribute and introduces discontinuities at region boundaries.
    norm_scope: fabric
```

Change the `dprst_flow_coef` entry in `flux_params` from `max: 0.5` to:

```yaml
      - name: dprst_flow_coef
        min: 0.005
        # 0.1, not 0.5: Driscoll 2020 Table 1 gives the NHM's calibrated range as
        # 0.0001-0.1 (median 0.048), and pywatershed's default 0.05 sits on that
        # median. The old 0.5 was 5x the calibrated maximum.
        max: 0.1
```

Replace the `provenance:` block with:

```yaml
      provenance:
        k_perm_log_wtd:
          area-weighted mean of log10 permeability (geometric mean, gdptools
          INTENSIVE form) -- flux-normalisation input, replaces the pre-#175
          k_perm_wtd which used the EXTENSIVE form and was not an average at all
        fflux:
          fraction of HRU area underlain by valid (non-no-data) lithology; -1
          where there is no overlap at all. Matches the reference
          implementation's fflux attribute (Viger 2014, doi:10.5066/F7CN71XR).
          A measured coverage fact, deliberately NOT in fill_columns
        vpu: per-HRU VPU, carried so the reducer can honour norm_scope: vpu
        mean_slope_fraction: tan(radians(slope mean)), flux-normalisation input
        hru_area: fabric geometry.area in m2 -- NOT PRMS hru_area, which is acres
```

- [ ] **Step 2: Regenerate the parameter index**

```bash
pixi run --as-is python scripts/build_parameter_index.py
git diff --stat docs/parameter_index.md
```

Expected: `docs/parameter_index.md` changes (the `k_perm_wtd` provenance row is replaced by `k_perm_log_wtd`, `fflux`, `vpu`).

- [ ] **Step 3: Verify the config parses and Guard 1 passes**

```bash
pixi run -e dev pytest tests/test_params_index.py -v
```

Expected: PASS. (Guard 2, `tests/test_params_index_ondisk.py`, is data-root-gated and SKIPS in CI — record its result by SLURM job id after the rebuild, never infer it from a green badge.)

- [ ] **Step 4: Update the architecture docs**

In `CLAUDE.md`, add to the "Non-obvious conventions & gotchas" list:

```markdown
- **`k_perm` is INTENSIVE — aggregate it as an area-weighted mean, never an
  area-prorated sum.** `ssflux.py` originally used gdptools' *extensive* form
  (`Σ Vᵢ·aᵢ/Aᵢ`, dividing by the SOURCE polygon area, the column gdptools labels
  "for extensive variables"), which is correct for population or volume but not
  for a property of the medium. Per-HRU weight sums spanned 3.6e13× instead of
  1.0, inflating spread from a physical 5.6 to an artifactual 15.4 orders of
  magnitude and pinning ~90% of HRUs at the range minimum (#175). Use
  `normalized_area_weight` (gdptools' `wght`) and renormalise by its per-HRU sum
  — that also corrects the 1,828 coverage-gap and 98 overlapping-source HRUs.
  Related traps: `k_perm == 0` is a **no-data flag**, not a measurement, and must
  be excluded rather than floored to `k_perm_min` (-16.48 is simultaneously the
  genuine least-permeable lithology class, 26,441 polygons); `k_perm` has only
  **8 distinct values**, so ~5% of HRUs sitting at the range minimum is real
  geology, not a defect. Normalisation is a **reduce step** — never per batch;
  and never fix the per-batch scope before fixing the log-space aggregation, or
  degeneracy goes from ~90% to 97-100%.
```

In `docs/ARCHITECTURE.md`, document the `reducer:` config key beside the existing `derived_columns:` description: a `(df, config, logger) -> df` callable applied once to the concatenated merged frame, dispatched from `MERGE_REDUCERS`, for params whose values need a whole-population statistic.

- [ ] **Step 5: Full lint under srun, then commit**

```bash
srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=4 --mem=64G \
  pixi run -e dev pre-commit run --all-files
git add configs/zonal/zonal_params.yml docs/parameter_index.md CLAUDE.md docs/ARCHITECTURE.md
git commit -m "feat(ssflux): reduce-step config, fflux provenance, cap dprst_flow_coef at 0.1 (#175)"
```

---

## Validation (after merge, on `gfv2_dev`)

Not part of the plan's tasks — code lands first, CI is the gate. Then:

1. Rebuild ssflux on **`gfv2_dev`**: `--mode build_weights` is unaffected (the weights CSV already carries `normalized_area_weight`), so only `--mode zonal` + `--mode merge` re-run.
2. Assert the acceptance criteria on the merged CSV — expected, measured from the current inputs:

   | param | % ≤1% of min | IQR/range |
   |---|---|---|
   | `soil2gw_max` | 5.0% | 0.383 |
   | `ssr2gw_rate`, `dprst_seep_rate_open` | 0.0% | 0.261 |
   | the four `/hru_area` params | 0.0% | 0.139 |

   `598` HRUs are expected to land NaN (no valid lithology) for the KNN gap-fill.
3. `viz.py` maps of `ssr2gw_rate` and `dprst_seep_rate_open` should show a coherent pattern tracking lithology and slope, not a uniform field.
4. Optional, settles the cube question empirically: download a region from
   `GeospatialFabricAttributes-PRMS_Gleeson_{01..21}.zip`
   ([doi:10.5066/F7CN71XR](https://doi.org/10.5066/F7CN71XR)) and compare
   **distributionally** — gfv2 has 361,471 HRUs vs the NHM's 109,951, so there is
   no 1:1 id join.
5. Promote to `gfv2` only after the gate passes. `oregon` and `tjc` need the same
   rebuild, no code change.
