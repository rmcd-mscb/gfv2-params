# Design: CV/lognormal snow-depletion-curve **library** for `snarea_curve`

**Date:** 2026-07-06
**Status:** Approved design (pre-implementation)
**Supersedes/extends:** the deferred "grouping" work parked in §9 of
[`2026-07-04-snodas-snarea-curve-design.md`](2026-07-04-snodas-snarea-curve-design.md)
("Curve clustering / a reduced curve library"). That pipeline is MERGED (PR #165)
and its per-HRU derived tables are on disk; this spec builds on them.
**Method source:** Sexstone, Driscoll, Hay, Hammond & Barnhart (2020), *Runoff
sensitivity to snow depletion curve representation within a continental scale
hydrologic model*, Hydrological Processes 34:2365–2380, DOI 10.1002/hyp.13735.
Local copy: [`docs/hyp.13735.pdf`](../../hyp.13735.pdf).
Empirical derivation method (Stage 2, unchanged): Driscoll, Hay & Bock (2017),
[`docs/Snow_Depletion_Curves.md`](../../Snow_Depletion_Curves.md).

---

## 1. Goal

Replace the current one-curve-per-HRU `snarea_curve` output with a compact,
**physically-based curve library**, and emit the two per-HRU parameters the model
needs but the current pipeline omits:

- **`snarea_curve`** — `ndepl` shared depletion curves (11 points each), where the
  curve **shape** is parameterised by a single physical quantity: the sub-grid SWE
  **coefficient of variation (CV)** via a lognormal SWE pdf (Sexstone eqs 3–5).
- **`hru_deplcrv`** — per-HRU integer index (1..`ndepl`) into the library.
- **`snarea_thresh`** — per-HRU SWE **scale** (inches) at which SCA reaches 1.0.
- **`ndepl`** — number of library curves.

The model **decouples groupable shape (`snarea_curve`) from per-HRU scale
(`snarea_thresh`)**: PRMS indexes the curve by `frac_swe = pkwater_equiv /
snarea_thresh` (Sexstone: `snarea_thresh` ≡ SWE₁₀₀, "the maximum SWE amount for a
given HRU below which the SDC is applied"). This spec supplies both.

Validate on **Oregon** (VPU 17), then **CONUS gfv2**.

### 1.1 Locked decisions (see also the `sdc_grouping_decision` memory)

| Decision | Choice |
|---|---|
| Curve family | 1-parameter **lognormal CV** basis (Sexstone/Liston), analytic closed form |
| CV source (primary, all snow HRUs) | **Sub-grid-direct CV** = area-weighted `std/mean` of the SNODAS SWE pdf within the HRU, at peak accumulation |
| Role of the empirical Driscoll curves | **Validation oracle** + calibration reference (no longer the parameter source) |
| `ndepl` | **1 reserved default curve + `ndepl_cv` CV-binned curves** (`ndepl_cv` config-default **8** ⇒ `ndepl`=9) |
| `snarea_thresh` estimator | **Median across water-years of the annual peak mean-SWE**, mm → inches |
| Failure defaults | Sub-grid CV gives near-**universal coverage**; only genuine no-snow / un-estimable HRUs use the reserved default curve |
| Output | **Dual CSV** (library + per-HRU) **+ pyWatershed NetCDF** param file + a validation report |
| Validation fabric order | Oregon, then CONUS gfv2 |

### 1.2 Why unified sub-grid CV (not empirical-fit primary)

Computing sub-grid CV requires the **same** new Stage 1/2 infrastructure whether
we use it everywhere or only for derivation failures, so using it as the single
CV definition is *less* Stage 3 code (no per-HRU curve fit) and removes the
derived/failure "seam." It is the physical quantity Sexstone/Liston/NHM use, and
it is estimable for **every** snow-bearing HRU — solving the ~50 %-default problem
(CONUS: 58 % of HRUs currently fall on the near-linear placeholder), which the
grouping memory flags as **a bigger lever than the grouping itself**.

**The one honest risk:** SNODAS is 1 km, so `std/mean` across an HRU's ~74 cells
captures 1-km-and-coarser SWE variability but misses sub-1-km drift/aspect/veg
variability. True sub-HRU CV is therefore ≥ the SNODAS-measured CV, so parametric
curves could deplete too fast. Mitigations, in order: (a) the empirical Driscoll
SCA is *also* built from the same 1-km field (`scov = SWE>0` at 1 km), so the two
should agree far better than a 1-km-vs-truth comparison implies; (b) a
**validation gate** measures the disagreement on the `derived` HRUs before we lock
the parameter source; (c) if biased, a **calibration** step maps sub-grid CV onto
the empirical-fit CV scale; (d) if hopeless, a config switch reverts to
empirical-primary — the infra is identical, so this is a config change, not a
redesign.

---

## 2. Architecture

Three stages in `gfv2-params`. Stage 3 is the new deliverable; Stages 1–2 get
strictly-additive changes and keep their existing behaviour.

```
Stage 1  aggregate/  ── ADD area-weighted swe_std (sub-grid SWE std)
  raw SNODAS daily SWE (EPSG:5070)
     ─► per-HRU daily { swe (mean), scov (SCA), swe_std }   (re-run; weights cached ⇒ cheap)
        │
Stage 2  snarea/ (derive) ── ADD per-HRU sub-grid stats; KEEP empirical curve + sdc_status
     ─► {data_root}/{fabric}/params/merged/_intermediates/nhm_snarea_curve_derived.csv
        (empirical curve, sdc_status, sca_class, similarity, n_seasons = validation oracle
         + cv_subgrid, peak_swe_mm, n_peak_years                       = new)
        │
Stage 3  snarea/library.py  (NEW; pure pandas/numpy/scipy; no daily reload; re-runnable)
     ─► nhm_snarea_curve_library.csv     (ndepl curves)
        nhm_snarea_curve_params.csv      (per-HRU: hru_deplcrv, snarea_thresh, CVs, diagnostics)
        nhm_snarea_curve.nc              (pyWatershed/PRMS param file)
        nhm_snarea_curve_validation.csv  (validation-gate report)
```

**Fabric independence (non-negotiable, per repo rule):** process configs are
fabric-agnostic; every per-fabric input/output resolves from the active profile in
`configs/base_config.yml` via `require_config_key` with `{data_root}`/`{fabric}`
placeholders. No literal paths, no naming conventions in code.

**Why a separate Stage 3 (not folded into Stage 2):** re-running the library at a
different `ndepl_cv` must not re-pay Stage 2's ~344 GB / ~31 min CONUS daily load.
Stage 3 reads only the small per-HRU CSV, so `ndepl` is tunable in seconds.

---

## 3. Stage 1 change — area-weighted sub-grid `swe_std`

The aggregate harness applies **one** `stat_method` per `AggGen` pass
([`aggregate/driver.py:aggregate_variables`](../../../src/gfv2_params/aggregate/driver.py)),
so `swe` and `scov` both use `masked_mean`. Sub-grid std needs a second
`masked_std` pass over the SWE variable, sharing the already-cached weights.

- **`SourceAdapter`** ([`aggregate/adapter.py`](../../../src/gfv2_params/aggregate/adapter.py))
  gains an optional field `std_variables: tuple[str, ...] = ()` — variables to
  *also* aggregate with `masked_std`, emitted as `{var}_std`. Validated against the
  existing `_ALLOWED_STAT_METHODS` set (which already lists `masked_std`).
- **Driver** ([`aggregate/driver.py`](../../../src/gfv2_params/aggregate/driver.py)):
  after the primary pass, if `adapter.std_variables` is non-empty, run one more
  `AggGen(stat_method="masked_std", weights=<same cached weights>)` over those
  variables and merge each result into the year's Dataset as `{var}_std`. Weights
  are geometry-only, so this reuses `compute_or_load_weights`'s cache — no
  re-computation.
- **SNODAS adapter** ([`aggregate/snodas.py`](../../../src/gfv2_params/aggregate/snodas.py)):
  set `std_variables=("swe",)`. The `_snodas_hook` fill-masking is unchanged;
  `masked_std` inherits the same NaN mask, so `swe_std` is the area-weighted std of
  **finite** (snow-or-bare land) pixels, consistent with `swe`'s `masked_mean`.

Output per-year NetCDF now carries `swe`, `scov`, `swe_std` on dims
`(time, <id_feature>)`. Stage 1 must be re-run once; the expensive `WeightGen`
step is cached, so this is a cheap extra `AggGen` per year.

---

## 4. Stage 2 change — representative sub-grid CV + peak SWE

Additive to the existing empirical derivation; the empirical curve, `sdc_status`,
`sca_class`, `similarity`, `n_seasons` are **retained unchanged** as the
validation oracle.

- **`read_daily_by_hru`** ([`scripts/derive_snarea_curve.py`](../../../scripts/derive_snarea_curve.py))
  also loads `swe_std` into each per-HRU daily frame.
- **New `src/gfv2_params/snarea/subgrid.py`** — `representative_peak_stats(daily)`:
  1. Group the daily frame by **water year** (Oct 1–Sep 30), reusing the same
     framing as [`snarea/build.py:_seasons`](../../../src/gfv2_params/snarea/build.py)
     (a late-December accumulation must not be mis-picked as the peak).
  2. For each water year, find the day of **peak mean SWE**; record
     `peak_swe = swe[peak]` and `cv_year = swe_std[peak] / swe[peak]` (CV is most
     stable where mean SWE is largest — the peak day). Skip years with no snow /
     non-finite / `swe[peak] <= 0`.
  3. Return `cv_subgrid = median(cv_year)`, `peak_swe_mm = median(peak_swe)`,
     `n_peak_years = count`. (Config option, deferred: average CV over a near-peak
     window `swe >= f·peak` if the single-day estimate proves noisy.)
- **`build_hru_record`** ([`snarea/build.py`](../../../src/gfv2_params/snarea/build.py))
  adds columns `cv_subgrid`, `peak_swe_mm`, `n_peak_years`.
- **Output location:** Stage 2 writes to
  `{data_root}/{fabric}/params/merged/_intermediates/nhm_snarea_curve_derived.csv`
  (mirrors the depstor `merged/` vs `merged/_intermediates/` convention). The
  terminal `nhm_snarea_curve_params.csv` name is reused by **Stage 3**.

---

## 5. Stage 3 algorithm — the library builder (`snarea/library.py`)

Pure `pandas`/`numpy`/`scipy` over the Stage 2 derived CSV.

### 5.1 The closed form (`sdc_from_cv`)

Sexstone eqs 1–5 (lognormal SWE pdf under uniform melt); the dimensionless SDC
depends **only** on CV. Index 0 = SWE/thresh = 1.0 → index 10 = 0.0 (the repo's
existing descending convention, matching
[`snarea/season.py:SWE_LEVELS`](../../../src/gfv2_params/snarea/season.py)).

```python
import numpy as np
from scipy.stats import norm

SWE_LEVELS = np.round(np.arange(1.0, -1e-4, -0.1), 1)  # 1.0 .. 0.0

def sdc_from_cv(cv: float, mu: float = 1.0, n: int = 4000) -> np.ndarray:
    z = np.sqrt(np.log(1 + cv * cv))          # ζ² = ln(1+CV²)
    lam = np.log(mu) - 0.5 * z * z            # λ  = ln(μ) − ζ²/2
    M = np.concatenate([[0.0], np.exp(np.linspace(np.log(mu) - 6*z, np.log(mu) + 6*z, n))])
    lnM = np.log(np.where(M > 0, M, 1e-300))
    sca = norm.cdf((lam - lnM) / z)           # SCA(M) = Φ((λ−lnM)/ζ)
    swe = mu * norm.cdf((lam + z*z - lnM) / z) - M * sca
    sca[0], swe[0] = 1.0, mu
    o = np.argsort(swe / swe[0])
    return np.clip(np.interp(SWE_LEVELS, (swe / swe[0])[o], sca[o], left=1.0, right=0.0), 0, 1)
```

Properties (asserted in tests): monotonic non-increasing; `curve[0]==1.0`,
`curve[-1]==0.0`; larger CV ⇒ steeper (lower SCA at mid SWE).

### 5.2 Steps

1. **Load** the Stage 2 derived CSV. Define the **estimable** set: HRUs with a
   finite `cv_subgrid` (`peak_swe_mm > 0`, `n_peak_years >= 1`, sub-grid std
   defined ⇒ ≥ 2 snow cells).
2. **`cv_empirical` (diagnostic + reference)** — for `derived` HRUs, project the
   empirical 11-pt curve onto the lognormal family: grid-search CV minimising L2
   over **interior** points 1–9 against `sdc_from_cv(cv)` (endpoints are fixed at
   1.0/0.0 for all CVs, so they carry no fit information). Satisfies task
   deliverable 1(a).
3. **Validation gate + optional calibration** — on `derived` HRUs (both CVs and the
   empirical curve available):
   - Report `cv_subgrid` vs `cv_empirical` distribution stats and the
     **reconstruction error** of `sdc_from_cv(cv_subgrid)` vs the empirical curves
     (mean and p95 abs SCA; target ≈ 0.03 mean — the validated tolerance).
   - `calibrate: auto|on|off` (config). On `auto`, if the median bias
     `|median(cv_subgrid) − median(cv_empirical)|` exceeds a configured tolerance,
     **quantile-map** `cv_subgrid` → `cv_empirical` (monotone, using the `derived`
     HRUs as the training set) and apply to **all** HRUs → `cv_assign`. Otherwise
     `cv_assign = cv_subgrid`. Record the mapping in the validation report.
4. **`cv_assign` resolution (per HRU):** calibrated `cv_subgrid` if finite; else
   `cv_empirical` if finite (rare edge: derived HRU with undefined sub-grid std);
   else **none** → reserved default. `cv_source` ∈
   {`subgrid`, `subgrid_calibrated`, `empirical`, `default_no_snow`}.
5. **Library** (`ndepl` = 1 + `ndepl_cv` curves):
   - **Index 1 — reserved default:** a configurable non-increasing curve
     (`default_curve`, currently the near-linear placeholder inherited from
     [`snarea/build.py:DEFAULT_SNAREA_CURVE`](../../../src/gfv2_params/snarea/build.py);
     swap for the real NHM default when staged). `cv` = sentinel (NaN).
   - **Indices 2..`ndepl` — CV bins:** partition `cv_assign` over the estimable
     HRUs into `ndepl_cv` **equal-population** bins (quantiles); each bin's
     **median CV** → `sdc_from_cv` → the library curve. Binning on the same metric
     used for assignment keeps bin medians self-consistent.
6. **Assign `hru_deplcrv`:** for estimable HRUs, the **CV-bin** library curve
   (indices 2..`ndepl`) whose CV is nearest `cv_assign` — the reserved default
   (index 1, `cv`=NaN) is **not** a candidate in this nearest-CV search.
   Non-estimable HRUs → index 1. Every HRU maps to a valid curve. (Bins are
   equal-population, so nearest-median assignment ≈ bin membership, differing only
   for HRUs near a bin edge, where the nearer curve is the better match.)
7. **`snarea_thresh` = `peak_swe_mm / 25.4`** (inches); 0.0 for no-snow /
   un-estimable HRUs (the curve is never exercised there — `pkwater_equiv` = 0).

---

## 6. Serialization

Four artifacts under `{data_root}/{fabric}/params/merged/`. The descending
(CSV) ↔ ascending (PRMS NetCDF) curve-order flip lives in one tested helper
`_to_prms_order(curve) -> curve[::-1]`.

1. **`nhm_snarea_curve_library.csv`** — `ndepl` rows: `deplcrv_id` (1..`ndepl`),
   `curve_kind` (`default`|`cv_bin`), `cv` (bin-median CV; sentinel for default),
   `n_hru` (HRUs assigned to it), `snarea_curve_0..10` (descending).
2. **`nhm_snarea_curve_params.csv`** — one row per HRU (terminal artifact, name
   preserved for `viz.py`/docs consumers): `<id_feature>`, `hru_deplcrv`
   (1..`ndepl`), `snarea_thresh` (in), `cv_assign`, `cv_subgrid`, `cv_empirical`
   (NaN if not `derived`), `cv_source`, `sdc_status`, `sca_class`, `similarity`,
   `n_seasons`, `n_peak_years`, `peak_swe_mm`, `snarea_curve_0..10` (the *assigned*
   library curve, descending — QA + backward-compat; preserves per-HRU detail so no
   separate "1:1 mode" is needed).
3. **`nhm_snarea_curve.nc`** — pyWatershed/PRMS param file (verified against
   `pywatershed.hydrology.prms_snow`, which does `reshape(snarea_curve,(ndepl,11))`
   and indexes ascending frac_swe):
   - dims: `ndepl` (=`ndepl`), `ndeplval` (=11·`ndepl`), `nhru`
   - `snarea_curve(ndeplval)` float — **flat, ASCENDING** (index 0 = frac_swe 0.0 →
     SCA≈0), curves concatenated in `deplcrv_id` order (= reversed repo curve)
   - `hru_deplcrv(nhru)` int32 (1..`ndepl`)
   - `snarea_thresh(nhru)` float (inches)
   - `<id_feature>(nhru)` coord; CF attrs (`units`, `long_name`). The
     **cf-netcdf-review** skill is run when this writer is implemented.
4. **`nhm_snarea_curve_validation.csv`** — gate report: `cv_subgrid`/`cv_empirical`
   distribution stats, reconstruction error before/after calibration, calibration
   mapping summary, per-`sdc_status` and per-`cv_source` counts.

---

## 7. Module / config / test layout + deps

**Source (new / changed):**

| File | Change |
|---|---|
| `src/gfv2_params/snarea/library.py` | **NEW** — `sdc_from_cv`, `fit_cv`, `build_library`, `assign_deplcrv`, `snarea_thresh_inches`, validation/calibration, CSV+NetCDF serializers, `_to_prms_order` |
| `src/gfv2_params/snarea/subgrid.py` | **NEW** — `representative_peak_stats` |
| `src/gfv2_params/snarea/build.py` | add `cv_subgrid`/`peak_swe_mm`/`n_peak_years`; write to `_intermediates/` |
| `src/gfv2_params/aggregate/adapter.py` | add `std_variables` field + validation |
| `src/gfv2_params/aggregate/snodas.py` | `std_variables=("swe",)` |
| `src/gfv2_params/aggregate/driver.py` | `masked_std` sidecar pass, merged as `{var}_std` |
| `scripts/derive_snarea_curve.py` | load `swe_std`; wire subgrid stats |
| `scripts/derive_snarea_library.py` | **NEW** — Stage 3 driver (`load_config`+`require_config_key`+placeholders) |

**Config:**

- `configs/aggregate/aggregate_sources.yml` — declare `swe_std` for the SNODAS source.
- `configs/snarea/snarea_library.yml` — **NEW** Stage 3: input derived CSV, output
  paths, `ndepl_cv: 8`, `calibrate: auto`, calibration-bias tolerance, reserved
  `default_curve`. Top-level placeholdered keys (`load_config` only resolves
  top-level `{data_root}`/`{fabric}`; nested blocks pass through untouched).
- `configs/snarea/snarea_curve.yml` — point Stage 2 `output_dir`/`merged_file` at
  `_intermediates/nhm_snarea_curve_derived.csv`.

**Tests (CI-gated; never full pytest on the HPC head node):**

- `tests/test_snarea_library.py` **(NEW)** — `sdc_from_cv` properties; `fit_cv`
  recovers a curve generated from a known CV; equal-population binning + reserved
  default; assignment incl. no-snow→default and the `cv_assign` fallback order;
  `snarea_thresh` mm→in; calibration removes a synthetic CV bias; NetCDF dims +
  ASCENDING order + `_to_prms_order` round-trip.
- `tests/test_snarea_subgrid.py` **(NEW)** — peak-day CV + peak SWE + water-year
  framing on hand-built series with known answers.
- extend `tests/test_aggregate_snodas.py` / `tests/test_aggregate_driver.py` — the
  `masked_std` sidecar emits `swe_std` on a tiny synthetic grid.
- extend `tests/test_derive_snarea_curve.py` — `read_daily_by_hru` reads `swe_std`.

**Deps:** add `pywatershed` (conda-forge) to a **`reference`** (or dev) pixi
feature in `pyproject.toml` — queryable for param conventions, **not** required by
CI so the test env stays light/fast. `scipy` already present. The NetCDF test
validates structure directly (no `pywatershed` import).

---

## 8. Validation & rollout

**Incremental (no HPC needed for the empirical side):** `cv_empirical`, library
binning, and reconstruction (≈0.03 target) can be checked immediately against the
existing on-disk Oregon/CONUS derived tables
(`{data_root}/{oregon,gfv2}/params/merged/nhm_snarea_curve_params.csv`, present as
of this design). Only `cv_subgrid` requires the Stage 1/2 re-run.

**Rollout (Oregon → CONUS):**

1. **Oregon** (`FABRIC=oregon`, fast): re-run Stage 1 (adds `swe_std`; weights
   cached), Stage 2 (adds subgrid stats), Stage 3 (library). Confirm:
   - validation gate — `cv_subgrid` vs `cv_empirical`; reconstruction; calibrate if
     biased;
   - **near-universal coverage** — the near-linear placeholder no longer dominates;
   - physical sanity — Cascades HRUs get higher CV / steeper curves, low-desert
     HRUs low CV or the no-snow default; spot-check a few curves.
2. **CONUS gfv2** (`FABRIC=gfv2`): same at scale. Stage 2 at `--mem=384G` (the
   ~344 GB `to_dataframe` load); Stage 3 is cheap (tabular).

**SLURM:** `slurm_batch/derive_snarea_library.batch` **(NEW)** — small mem/time.
Stage 1/2 batches unchanged (Stage 2 already defaults to 384 G).

**Data-root:** `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`.

**Docs (same branch, per the doc-audit rule):** this spec; `README.md` §snarea
(output now library + params + `.nc`); `slurm_batch/HPC_REFERENCE.md` (Stage 3);
`slurm_batch/RUNME.md` (runbook); `docs/ARCHITECTURE.md` if it enumerates stages;
memory update (`sdc_grouping_decision`, `snodas_snarea_curve_pipeline`).

---

## 9. Judgment calls (defaulted; revisit if desired)

1. **CV timing = peak day** of each water year, median across years — CV=std/mean is
   most stable at max mean SWE. Near-peak-window averaging is a config-tunable
   fallback if noisy.
2. **`snarea_thresh` = median annual peak mean-SWE**, self-consistent with the
   peak-SWE normalisation the empirical curves already use, and equal to Sexstone's
   SWE₁₀₀ scale.
3. **Reserved default curve** kept for no-snow / un-estimable HRUs so "no-snow →
   flat default" is explicit and every HRU maps to a valid curve; it is the same
   configurable `default_curve` placeholder until the real NHM default is staged.
4. **Per-HRU detail retained** (`cv_empirical`, assigned curve, all diagnostics in
   `nhm_snarea_curve_params.csv`) — so a "1:1 curve per HRU" QA view exists without
   a separate output mode.
5. **`ndepl_cv` = 8** (grouping memory: elbow 5–8, scale/climate-independent;
   config 5–8).

---

## 10. Out of scope (YAGNI)

- Regional / elevation-banded CV stratification, and a topo/climate CV regression
  (global calibration only for v1; CV median is stable ≈0.45 across Oregon+CONUS).
- Real NHM default-curve staging (placeholder stays until the file is provided).
- Sub-1-km CV downscaling to recover unresolved sub-grid variability.
- Reproducing the paper's published figures or its fixed CV=0.1–2.0 experiment set.
- Retiring the empirical Driscoll derivation (kept as the validation oracle).

---

## 11. References

- Sexstone, Driscoll, Hay, Hammond & Barnhart (2020), "Runoff sensitivity to
  snow depletion curve representation within a continental scale hydrologic
  model," *Hydrological Processes* 34:2365–2380, DOI 10.1002/hyp.13735 —
  [`docs/hyp.13735.pdf`](../../hyp.13735.pdf). Lognormal SDC(CV) closed form
  (eqs 1–5); `snarea_thresh` ≡ SWE₁₀₀; runoff sensitivity to SDC representation.
- Driscoll, Hay & Bock (2017) — [`docs/Snow_Depletion_Curves.md`](../../Snow_Depletion_Curves.md).
  Empirical per-HRU SDC derivation (Stage 2, retained).
- Liston (2004); Luce, Tarboton & Cooley (1999) — lognormal sub-grid SWE framework.
- pyWatershed `hydrology/prms_snow.py` — `snarea_curve`/`hru_deplcrv`/`snarea_thresh`
  param conventions (flat `ndeplval`, ascending frac_swe).
- Prior spec: [`2026-07-04-snodas-snarea-curve-design.md`](2026-07-04-snodas-snarea-curve-design.md).
