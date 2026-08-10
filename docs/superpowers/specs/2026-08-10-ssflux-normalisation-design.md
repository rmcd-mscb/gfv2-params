# ssflux normalisation redesign

**Issue:** [#175](https://github.com/rmcd-mscb/gfv2-params/issues/175) — ssflux
parameter fields are degenerate (~90% pinned at range minimum)
**Status:** design approved, not yet implemented
**Date:** 2026-08-10

## Problem

All seven `ssflux` parameters are effectively spatially constant. Measured on
`gfv2/params/merged/nhm_ssflux_params.csv` (n = 361,394 rows — the gfv2 fabric
has 361,471 HRUs; the 77-row shortfall is the known gap already documented in
the `ssflux` config's `fabric_columns` comment):

| param | range | % of HRUs within 1% of range MIN | IQR / range |
|---|---|---|---|
| `soil2gw_max` | 0.1 – 0.3 | 99.3% | 0.0000 |
| `ssr2gw_rate` | 0.3 – 0.7 | 89.5% | 0.0010 |
| `fastcoef_lin` | 0.01 – 0.6 | 90.5% | 0.0010 |
| `slowcoef_lin` | 0.005 – 0.3 | 90.5% | 0.0010 |
| `gwflow_coef` | 0.005 – 0.3 | 90.5% | 0.0010 |
| `dprst_seep_rate_open` | 0.005 – 0.2 | 89.5% | 0.0010 |
| `dprst_flow_coef` | 0.005 – 0.5 | 90.5% | 0.0010 |

TM 6-B9 (line 782) states the whole point of these parameters is "to create a
realistic spatial pattern of variation in values ... **in place of the
assumption of spatially constant values**". We ship the thing the derivation
exists to avoid.

This propagates into pywatershed's `PRMSRunoff` via `dprst_seep_rate_open` and
`dprst_flow_coef`.

## Root causes

Four defects compound. The issue body identified #1 and #4; #2 and #3 were
found during triage.

### 1. Permeability is aggregated as an EXTENSIVE variable

`zonal_runners/ssflux.py:76`:

```python
w["k_perm_wtd_sum"] = w["k_perm_actual"] * (w["area_weight"] / w["flux_id_area"])
```

This is gdptools' documented **extensive** aggregation, `Σ Vᵢ × (aᵢ / Aᵢ)`,
where `Aᵢ` is the *source* polygon area. `WeightGenP2P.calculate_weights`
labels that column explicitly:

> `source_id_area`: Total area of the source polygon **(for extensive variables)**.

Permeability is **intensive** — it does not add up when regions combine. The
correct gdptools form is the area-weighted mean `(Σ vᵢ aᵢ) / (Σ aᵢ)`, available
directly as the `normalized_area_weight` column (`= wght`, "proportional area
of the source polygon within the target (0.0–1.0)").

The original intent is explicit in the first commit (`d0a9fbb`, 2025-06-27),
and the variable names still say it in current code (`extensive_agg`,
`extensive_sorted`, `ssflux.py:78-99`):

```python
# For k_perm (m^2), prorate the source's k_perm by the fraction of its area in the target.
```

The likely trap: permeability is *dimensioned* as an area (m²) but is a
property of the medium, not an amount of anything.

**Consequence.** Per-HRU weight sums under the extensive construction span
**3.6e13×** (median 0.0222, max 1116.76) instead of 1.0. The result scales with
how many lithology polygons an HRU happens to touch and with the HRU-to-polygon
size ratio — neither related to permeability. This inflates the spread of the
HRU-level value from a physical **5.6 orders of magnitude** to an artifactual
**15.4**.

`normalized_area_weight` has **zero consumers** anywhere in `src/`, `scripts/`
or `configs/`. The correct weight has been in column 6 of every weights CSV the
whole time.

### 2. Linear min–max normalisation of a log-distributed variable

`ssflux.py:139` maps `[min, max] → [lo, hi]` linearly. Applied to a variable
spanning 15.4 orders, essentially every HRU lands on the floor. Corroboration:
`soil2gw_max` is the only parameter that cubes the value (`ssflux.py:111`),
widening the span to 45 orders, and it is the most degenerate at 99.3%.

### 3. `k_perm == 0` is a no-data flag treated as real data

`ssflux.py:74` maps `0 → k_perm_min = -16.48`. Gleeson `k_perm` is log10
permeability, so `0` would mean 1 m² — physically impossible; it is a no-data
flag on **9,409 of 202,108** polygons (4.7%). `-16.48` is simultaneously the
**genuine least-permeable lithology class** (26,441 polygons, 13%), so the
current mapping inflates that real class by 36% and asserts "no data == the most
impermeable rock observed".

### 4. Normalisation is per-batch, not per-region

`ssflux.py:119-121` computes min/max within each SLURM batch. Confirmed: exactly
**64 HRUs** sit at each configured maximum, one per batch. Two HRUs with
identical geology and slope get different values depending on how the fabric was
split, and the product is not reproducible under a change of batch count.

**Sequencing trap.** Fixing #4 alone makes the product strictly *worse* —
per-batch normalisation is currently *masking* #2. Measured degeneracy under
CONUS-wide linear normalisation is **97.2 – 100%** versus 89.5 – 90.5% per-batch.
#4 must never land before #1 and #2.

## Key facts established during triage

- **`k_perm` is categorical, not continuous** — only **8 distinct non-zero
  values** (−10.87, −11.79, −12.47, −12.50, −12.78, −14.05, −15.05, −16.48),
  assigned per lithology class. The achievable spatial pattern is 8 classes,
  area-blended at HRU level and modulated by slope and area. A residual ~5% of
  HRUs at the range minimum is **genuine geology** (HRUs lying entirely in the
  least-permeable class), not a defect.
- **Only 3 of the 7 parameters are independent.** `gwflow_coef` is bit-identical
  to `slowcoef_lin` on disk; `fastcoef_lin`, `dprst_flow_coef` and
  `dprst_seep_rate_open` are affine remaps. Min–max normalisation is
  affine-invariant, so `2 × slowcoef_lin` carries identical normalised
  information. Tests should target the three independent fields.
- **The cube is a no-op in log space.** `norm(log10(k³)) == norm(log10(k))`
  (`allclose`, max diff 3.05e-16) because ×3 on the exponent is absorbed by the
  normalisation. Issue item 2 ("missing cube") largely dissolves once the space
  is right.
- **Sign trap.** `k_perm` is negative (−16.48 … −10.87). Substituting it directly
  into TM 6-B9's multiplicative forms inverts the intended response: with `k<0`,
  `k³(1−slope)` *increases* with slope, and `slope>1` flips the sign again.
- **Source layer is not perfectly continuous.** 1,828 HRUs have coverage gaps
  (Σ`wght` < 0.99) and 98 have overlapping source polygons (Σ up to 2.0).

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Aggregate `k_perm` with gdptools' **intensive** form, applied in log10 space | Fixes root cause #1 using the library's documented column; an arithmetic mean of log10 k *is* the geometric mean of permeability, the correct average for heterogeneous media |
| D2 | Derive parameters **additively in log10 space**, then interpolate linearly | Fixes #2 while preserving TM 6-B9's structure and its literal "linearly interpolating" wording |
| D3 | `k_perm == 0` → NaN; renormalise over valid lithology; HRUs with no valid coverage → NaN for the existing KNN gap-fill | Fixes #3; avoids conflating no-data with the real −16.48 class |
| D4 | Normalisation moves to a **reduce step**; `norm_scope: fabric` (default) or `vpu` | Fixes #4; fabric-wide is batch-invariant by construction, `vpu` retained for NhmParamDb comparison |
| D5 | Cap `dprst_flow_coef` at **0.1** | Issue item 5; Driscoll 2020 Table 1 calibrated max is 0.1, ours was 5× that |
| D6 | Plain min–max, **no** percentile anchors | Measured unnecessary — plain linear passes every criterion; keeps maximum fidelity to TM 6-B9 |

## Design

### Aggregation (replaces `ssflux.py:73-84`)

```python
# k_perm == 0 is a no-data flag, not a measurement.
w["k_perm"] = w["k_perm"].replace(0, np.nan)

# gdptools INTENSIVE aggregation, applied to log10 permeability.
#   (Σ vᵢ aᵢ) / (Σ aᵢ)  ==  Σ(v · wght) / Σ(wght)
# Renormalising by `den` is load-bearing: it corrects no-data exclusion,
# the 1,828 coverage-gap HRUs and the 98 overlapping-source HRUs alike.
valid = w["k_perm"].notna()
num = (w["k_perm"] * w["normalized_area_weight"])[valid].groupby(w[id_feature]).sum()
den = w["normalized_area_weight"][valid].groupby(w[id_feature]).sum()
k_perm_log_wtd = num / den          # geometric mean of permeability, log10 units
```

`k_perm_log_wtd` replaces `k_perm_wtd` in the output. Range on gfv2:
−16.48 … −10.87.

### Derivation (replaces `ssflux.py:111-117`)

Everything stays additive in log10 space, which is what "geometric mean +
linear interpolation" implies:

```
L_soil2gw_max        = 3·k_perm_log_wtd
L_ssr2gw_rate        = 3·k_perm_log_wtd + log10(1 − slope)
L_slowcoef_lin       = 3·k_perm_log_wtd + log10(slope) − log10(hru_area)
L_fastcoef_lin       = L_slowcoef_lin + log10(2)
L_gwflow_coef        = L_slowcoef_lin
L_dprst_seep_rate_open = L_ssr2gw_rate
L_dprst_flow_coef    = L_fastcoef_lin
```

The `×3` is retained for structural fidelity to TM 6-B9 even though it is
provably absorbed by the normalisation for `soil2gw_max`; it is *not* absorbed
for the other two, where it re-weights permeability against the slope/area term.

### Map/reduce split

Normalisation cannot stay in the per-batch task. The existing chained
`zonal → merge (afterok)` orchestration already provides the reduce point.

- **Map** (`run_ssflux_batch`, per batch): emit `k_perm_log_wtd`,
  `mean_slope_fraction`, `hru_area` and the seven `L_*` columns. No
  normalisation.
- **Reduce** (new, dispatched from `run_merge`): concatenate, compute
  `min`/`max` of each `L_*` over the population selected by `norm_scope`,
  interpolate linearly to each parameter's configured range, drop the `L_*`
  columns, write the merged CSV.

`run_merge` is currently generic (concat + duplicate check). Add a
`MERGE_REDUCERS` dispatch table in `zonal_runners/__init__.py` mirroring the
existing `BATCH_RUNNERS`, keyed off a new optional `reducer:` tag in the param
config. Params without a `reducer:` keep today's behaviour exactly.

### Config changes (`configs/zonal/zonal_params.yml`, `ssflux` entry)

- `reducer: ssflux`
- `norm_scope: fabric` (default; `vpu` alternative)
- `flux_params[dprst_flow_coef].max: 0.5 → 0.1` (D5)
- `prms:` block updated for `k_perm_wtd → k_perm_log_wtd` and the new `L_*`
  columns (all `provenance:` — they are inputs to the interpolation, not PRMS
  parameters). Regenerate `docs/parameter_index.md` via
  `scripts/build_parameter_index.py`.

`norm_scope: vpu` requires a VPU column in the ssflux flow; source it from the
fabric profile rather than a naming convention.

## Numerical robustness

Floating point is load-bearing here: one acceptance criterion is a **bit-equality**
claim ("identical merged output across batch counts"), and the investigation was
itself derailed once by float dust (normalising `k` to a `[0,1]` index put the
floor HRUs at ~1e-16 instead of 0, and cubing amplified that into a 45-order
tail that swamped the real signal).

Rules for the implementation:

1. **No exact `==` on computed floats.** Comparisons against aggregate results
   (weight sums, degenerate ranges, floor detection) use `math.isclose` /
   `np.isclose` with a stated tolerance. The one deliberate exception is
   `k_perm == 0`: that is a *stored* no-data flag, not a computed value, and a
   tolerance there would risk swallowing a genuine measurement — keep exact and
   comment why.
2. **Guard every `log10` domain.** `slope` is clipped to `[1e-4, 1 − 1e-4]`
   (317 HRUs at `slope == 0`; 3 at `slope ≥ 1`, up to 66.85°, where `1 − slope`
   is meaningless). `hru_area` must be `> 0`. A non-positive argument must raise
   or produce an explicit NaN — never a silent `-inf` that then propagates
   through min/max and destroys the whole range.
3. **Guard the zero denominator.** `num / den` where `den == 0` (no valid
   lithology) must yield NaN by explicit mask, not by 0/0 with a
   `RuntimeWarning`.
4. **Guard the degenerate range.** The reducer's `hi_in − lo_in` check uses a
   tolerance, not `== 0`, and raises rather than silently emitting the midpoint
   for a whole fabric.
5. **Weight-sum validation.** Log HRUs whose `Σ normalized_area_weight` falls
   outside `[0.99, 1.01]` and fail if the fraction exceeds a configured
   threshold. This is the guard that stops the extensive/intensive confusion
   from silently returning.
6. **Batch-invariance is achievable bit-exactly, and the test asserts it.**
   Per-HRU aggregation sums the same weight rows in the same file order
   regardless of batch partitioning, and `min`/`max` in the reducer are exact
   and order-independent (no accumulation). So the merged output must be
   *identical*, not merely close. If a future change introduces an
   order-dependent accumulation (mean, std, variance), this guarantee breaks and
   the test is the thing that will catch it.
7. **CSV round-trip must be lossless.** The map/reduce split writes `L_*` to CSV
   and reads it back before normalising. `pandas.to_csv` defaults to `repr`,
   which is round-trippable for float64 — assert this in a test rather than
   assuming it, since a stray `float_format` would silently quantise every
   parameter.
8. **Do not assert bit-equality between algebraically equivalent forms.**
   `norm(3·k)` and `norm(k)` agree to ~3e-16, not exactly; tests comparing them
   use a tolerance.

## Output schema change

`k_perm_wtd` (linear, extensive) → `k_perm_log_wtd` (log10, intensive). This
changes the on-disk header, so:

- update the `prms:` block (`provenance:` bucket) for the ssflux entry;
- regenerate `docs/parameter_index.md`;
- Guard 2 (`tests/test_params_index_ondisk.py`) is data-root-gated and **skips
  in CI** — record its result by SLURM job id, never infer it from a green
  badge.

`fill_columns` is unchanged (the same seven parameters). `fabric_columns`
`hru_area` handling is unchanged.

## Test plan

New `tests/test_ssflux.py` (none exists today):

| test | asserts |
|---|---|
| intensive weighting | `Σ normalized_area_weight ≈ 1` on a fixture; aggregation equals `(Σ v·a)/(Σ a)` |
| extensive regression | the old `/ flux_id_area` form is *not* produced (guards the root cause) |
| no-data handling | `k_perm == 0` excluded; weights renormalised; zero-coverage HRU → NaN |
| log-additive derivation | `L_*` match the closed forms above |
| affine no-op | `norm(3·k)` ≈ `norm(k)` within tolerance |
| affine family | `gwflow_coef` ≡ `slowcoef_lin`; `fastcoef_lin` is the affine remap |
| slope guards | `slope == 0` and `slope ≥ 1` produce finite output, no `-inf` |
| degenerate range | reducer raises on a zero-width input range |
| **batch invariance** | two different batch counts → **identical** merged CSV |
| CSV round-trip | `L_*` survive write/read bit-exactly |
| distribution | `% ≤1% of min ≤ 20` and `IQR/range ≥ 0.10` on a fixture |

## Expected outcome (measured on gfv2 inputs)

Fabric-wide linear interpolation, 598 HRUs → NaN for gap-fill:

| param | range | % ≤1% min | IQR/range | |
|---|---|---|---|---|
| `soil2gw_max` | 0.1 – 0.3 | 5.0% | 0.383 | PASS |
| `ssr2gw_rate` | 0.3 – 0.7 | 0.0% | 0.371 | PASS |
| `fastcoef_lin` | 0.01 – 0.6 | 0.0% | 0.249 | PASS |
| `slowcoef_lin` | 0.005 – 0.3 | 0.0% | 0.249 | PASS |
| `gwflow_coef` | 0.005 – 0.3 | 0.0% | 0.249 | PASS |
| `dprst_seep_rate_open` | 0.005 – 0.2 | 0.0% | 0.371 | PASS |
| `dprst_flow_coef` | 0.005 – **0.1** | 0.0% | 0.249 | PASS |

Every acceptance criterion in #175 is met. The residual 5.0% on `soil2gw_max`
is the genuine least-permeable lithology class.

## Rollout

1. Land code + tests; CI is the test gate (not the head node).
2. Rebuild on **`gfv2_dev`** first, never the canonical `gfv2` product.
3. Verify: `viz.py` maps of `ssr2gw_rate` and `dprst_seep_rate_open` show a
   coherent pattern tracking lithology and slope; distributions compared to
   NhmParamDb **distributionally**, not per-HRU (gfv2 has 361,471 HRUs vs the
   NHM's 109,951 — no 1:1 id join).
4. Promote to `gfv2` only after the gate passes.
5. Blast radius is contained: `ssflux.py` is the only consumer of
   `lith_weights_*.csv`. `oregon` and `tjc` need the same rebuild but no code
   change.

## Non-goals / rejected designs

- **Percentile-anchored normalisation** — measured unnecessary; plain linear
  passes every criterion, and clipping would depart further from TM 6-B9.
- **Special `hru_area` outlier conditioning** — the apparent `/hru_area`
  degeneracy was an artifact of an early candidate that normalised `k` to a
  `[0,1]` index before cubing. Under the log-additive construction it does not
  arise.
- **Changing `gwflow_coef`'s method** (issue item 4) — TM 6-B9 derives it from a
  multiple-linear regression on CONUS GIS data (range 0.004 – 0.055) whereas we
  set it to `slowcoef_lin`. Recorded, not fixed here; needs its own issue.
- **Imputing no-data `k_perm` from neighbouring lithology** — considered;
  defers to the existing KNN gap-fill instead of adding a spatial step to the
  lithology pipeline.

## References

- `docs/NHM_description_Regan_2018_TM6B9.md` — lines 782, 790 (soil zone
  parameters, "GF region" interpolation)
- `docs/Surface_depression_storage_Driscoll_2020.md` — Table 1 (calibrated
  ranges; `dprst_flow_coef` 0.0001 – 0.1)
- [gdptools: Extensive vs. intensive variables](https://gdptools.readthedocs.io/en/develop/Examples/PolyToPoly/Extensive_vs_intensive_variables.html)
- `gdptools/weight_gen_p2p.py:208-230` — `calculate_weights` column semantics
- Gleeson and others, 2011 — `k_perm` (log10 permeability) source dataset
- Viger, R.J., 2014; Viger and Leavesley, 2007 — cited by TM 6-B9 for
  derivation detail (not yet consulted; would settle the cube's intended space)
