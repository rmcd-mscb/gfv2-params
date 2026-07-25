# Parameter gap-fill convention — design

**Date:** 2026-07-25
**Status:** design, awaiting review
**Issue:** file on approval

Makes `{fabric}/params/merged/nhm_*_params.csv` the single canonical per-HRU parameter
set — always gap-filled — and retires the `filled_` prefix. Declares which columns are
fillable per param, so provenance columns are never overwritten.

## Problem

Gap-filling today is a per-file manual step, and the result is a directory a consumer
cannot read without prior knowledge.

`scripts/merge_and_fill_params.py` fills **one file per invocation** and hardcodes
`nhm_ssflux_params.csv` as its default (line 162); `slurm_batch/merge_and_fill_params.batch`
passes only `--fabric`. So a param gets filled if and only if someone remembered to pass
`--param_file` for it. The observable result on 2026-07-25:

| fabric | filled files present |
| --- | --- |
| `gfv2` | `ssflux`, `soil_moist_max` |
| `oregon` | `ssflux`, `soil_moist_max`, `lulc_nalcms`, `lulc_nhm_v11` |

**The canonical set differs per fabric, and nothing records which file is authoritative.**
A consumer cannot learn the rule from one fabric and apply it to another. The only code
that knows the convention is `src/gfv2_params/viz.py`, which hardcodes
`filled_nhm_ssflux_params.csv` for 7 parameters — a convention encoded in exactly one
consumer and nowhere else.

Measured gaps on `oregon` (16,814 HRUs), which is what prompted this:

| param | gap | was it filled? |
| --- | --- | --- |
| `nhm_ssflux` | 10 absent HRU rows | yes |
| `nhm_soil_moist_max` | 65 NaN cells | only after being asked for |
| `nhm_lulc_nalcms` | 1 absent HRU row | only after being asked for |
| `nhm_lulc_nhm_v11` | 1 absent HRU row | only after being asked for |

RUNME §5 documents the whole step in three lines ("KNN-fills any missing per-HRU
parameter values") and mentions neither the one-file-per-run behaviour nor the `filled_`
output.

### The trap that shapes the design

`nhm_snarea_curve_params.csv` reports 7,891 of 16,814 rows containing NaN — but every one
is in a **provenance** column, not a parameter:

* `cv_empirical` (7,891 NaN) — the empirical CV is derivable for only ~42% of HRUs by
  design; `cv_subgrid` exists precisely to rescue the rest. NaN is the *result*.
* `cv_assign`, `cv_subgrid`, `similarity`, `peak_swe_mm` — 20 NaN each.
* Every actual PRMS parameter — `hru_deplcrv`, `snarea_thresh`, `snarea_curve_0..10` —
  is **complete**.

A naive "fill everything with a gap" would overwrite `cv_empirical` with interpolated
values and destroy the record of how each curve was derived. So the fill cannot be
driven by "is there a NaN"; it must be told what a parameter is.

## The convention

**Consumer contract, one line:** read `{fabric}/params/merged/nhm_*_params.csv`. Always
canonical, always gap-filled. The `filled_` prefix ceases to exist.

The pre-fill copy moves to `{fabric}/params/merged/_unfilled/`, mirroring the repo's
existing `merged/` (canonical) vs `merged/_intermediates/` (working) split.

## What gets filled

Each param entry in `configs/zonal/zonal_params.yml` and `configs/depstor/depstor_params.yml`
declares its fillable columns:

```yaml
  - name: snarea_curve
    fill_columns:
      - hru_deplcrv
      - snarea_thresh
      - snarea_curve_0
      # ... snarea_curve_1 .. snarea_curve_10
    # cv_empirical / cv_assign / cv_subgrid / cv_source / sdc_status / similarity /
    # peak_swe_mm / n_seasons / n_peak_years / sca_class are DELIBERATELY absent:
    # NaN in those is "not derivable", a result rather than a gap.
```

Params needing the key: `ssflux` (7 columns), `snarea_curve` (13), `lulc_nalcms` and
`lulc_nhm_v11` (7 each), `soil_moist_max`, `elevation`, `slope`, `aspect`, `soils`, and
the single-column depstor params (`dprst_frac`, `carea_max`, `smidx_coef`,
`sro_to_dprst_perv`, `sro_to_dprst_imperv`, `hru_percent_imperv`, `dprst_depth_avg`).

## The guard

An undeclared column is never touched — the safe direction, since it cannot destroy
provenance. But "don't fill" must never be silent, or this design just trades a
consumer-side ambiguity for a producer-side one. So the step audits **every** column
regardless of what it fills:

| finding | response |
| --- | --- |
| declared column, gap present | fill (KNN, `k=1`) |
| **undeclared column with NaN cells** | **WARNING** naming column + count |
| **undeclared param with an absent HRU row** | **RAISE** |

The asymmetry is deliberate. A NaN cell is ambiguous — it may be a legitimate "not
derivable", as `cv_empirical` is. An **absent row** is not ambiguous: the HRU is either
in the file or it isn't, and no provenance reading makes a missing HRU correct. So a
missing row in a param nobody declared fillable is a configuration error and fails loud.

This is the same failure mode as the three defects found during the 2026-07-25 oregon
rebuild — a step that reports success while quietly doing nothing. The design makes
"I found a gap I was not told to fill" impossible to miss.

## Components

### `scripts/merge_and_fill_params.py`

* **Default mode becomes all-params**: iterate every param declared in the zonal +
  depstor configs for the active fabric. `--param_file` is retained for single-param runs.
* **Write in place** at `merged/<name>.csv`; move the pre-fill copy to `merged/_unfilled/<name>.csv`.
* **Restore the original column dtype after filling.** The 2026-07-25 oregon fill turned
  `cov_type` from `int64` into `float64` (values `0.0/1.0/2.0/3.0`). With `k=1` no
  averaging occurs so the classes stayed integral, but the dtype change is visible to any
  consumer that does not cast. A declared categorical must come back as the integer class
  it was.

**Idempotency rule, load-bearing:** `_unfilled/<name>.csv` is written **only if it does
not already exist**. Without that, a second run moves the *already-filled* file into
`_unfilled/` and the true pre-fill version is destroyed — a one-way data loss on a shared
filesystem with no version control. This gets a dedicated test; it is precisely the
re-run-shaped hazard that produced the stale-`dprst_depth` bug the same day.

### `src/gfv2_params/viz.py`

Its 7 hardcoded `filled_nhm_ssflux_params.csv` references become `nhm_ssflux_params.csv`.
It is the only consumer that encodes the old convention.

### Migration

One-time, touching real products on both fabrics (`gfv2`: `ssflux`, `soil_moist_max`;
`oregon`: those plus `lulc_nalcms`, `lulc_nhm_v11`). A script with `--dry-run` first —
not hand-run `mv`s. Per existing `filled_X.csv`:

1. `X.csv` → `_unfilled/X.csv` (skip if `_unfilled/X.csv` exists)
2. `filled_X.csv` → `X.csv`

Idempotent and reversible.

## Testing

Synthetic, CI-safe (no data root):

* declared columns filled; undeclared columns byte-identical afterwards
* undeclared column with NaN → warning naming the column
* undeclared param with an absent HRU row → raises
* in-place write lands at `merged/<name>.csv`, pre-fill copy at `merged/_unfilled/<name>.csv`
* **running twice does not clobber `_unfilled/`** — the second run's `_unfilled/` still
  holds the original pre-fill content
* a declared categorical column keeps its integer dtype after filling
* a param with no gaps at all is a no-op (no `_unfilled/` entry created)

## Docs

* **`slurm_batch/RUNME.md` §5** — currently three lines. Must state: the step now covers
  every declared param rather than one; `merged/` is canonical and gap-filled; the raw
  pre-fill copies live in `merged/_unfilled/`; and how to see what was filled (the run log
  reports per-param filled counts, plus the warnings for undeclared gaps).
* **`docs/ARCHITECTURE.md`** — add `fill_columns` to the per-key required-field table and
  document the `merged/` vs `merged/_unfilled/` split alongside the existing
  `merged/_intermediates/` description.
* **`slurm_batch/HPC_REFERENCE.md`** — the param-file inventory should name
  `merged/*.csv` as canonical.

## Out of scope

* Changing *what* KNN fill does (still `k=1` nearest-neighbour). Only what it is run
  against, where it writes, and what it refuses to touch.
* `nhm_snarea_curve_library.csv` (9 rows) and `nhm_snarea_curve_validation.csv` (1 row) —
  not per-HRU files; they are not params and are not filled.
* Whether `lulc_nalcms` or `lulc_nhm_v11` should be preferred downstream. They are
  different products from different source rasters, not two versions of one thing; the
  crosswalk provenance favours `nhm_v11`, but selecting between them is a modelling
  decision, not a fill-convention one.
