# Investigation: near-zero derived snarea_curve on Oregon (2026-07-06)

**Status:** in progress. **Context:** first full Oregon run of the SNODAS→snarea_curve
pipeline (#164/#165 merged; #166 follow-up branch `feat/snodas-lazy-batched-aggregation`)
produced only **3 of 16,814 HRUs** with a `derived` curve — the rest fell back to the
NHM default. This note captures why.

## Symptom (first Oregon run, sdc_status)

```
default_dissimilar     8476   # passed cells+snow+SCA, but similarity > 0.15
default_too_few_cells  8182   # < 25 SNODAS cells
default_constant_sca    133
default_no_snow          20
derived                   3   # <-- essentially nothing
```

Expected (paper): representative SDCs for ~49% of *all* CONUS HRUs, and the large
majority of snow-dominated ones. 0.02% is wrong.

## Finding 1 — similarity metric scales with n_seasons (BUG, dominant cause)

`representative.similarity = Σ|annual − median| / points` sums over **every season ×
every point** but divides only by `points` (11) → similarity ≈ `n_seasons ×
(mean per-point deviation)`. The paper (Eq. 1) does the same but used a **fixed 9
seasons** for every HRU, so it was comparable across HRUs. We use a **variable 4–21
seasons per HRU**, so the raw metric mostly measures *how many seasons an HRU has*.

Evidence (whole-fabric, from the run CSV):
- `corr(similarity, n_seasons) = 0.466`  → strong.
- `corr(similarity/n_seasons, n_seasons) = 0.027` → ≈0. Normalizing by `n_seasons`
  removes the dependence entirely.
- The 3 `derived` HRUs all have the **minimum** `n_seasons = 4`.
- Scale-free metric `norm_sim = sim/n_seasons` (= mean per-point deviation) is tight
  and well-behaved: median **0.080**, 10th–90th pct **0.048–0.112**, max 0.166.

**Fix:** normalize similarity by `n_seasons` (divide by `points × n_seasons`, i.e.
`annual.size`) so it is the mean per-point-per-season deviation — comparable across
HRUs regardless of season count. Then recalibrate `max_similarity` on the new scale.

## Finding 2 — `too_few_cells` is a fabric mismatch, not a bug

The Oregon fabric adds **elevation bounds** to the original NHM HRUs, so HRUs are much
smaller than the paper's ~74 km² CONUS HRUs (especially narrow high-elevation bands).
SNODAS cell counts per HRU: median **25**, mean 31, and **48.8% have < 25 cells**
(< 15: 29%, < 10: 17%). The paper's 25-cell criterion drops half of this fabric.
This is expected for an elevation-banded fabric — recalibrate `min_cells`, don't treat
it as an error. (`min_cells` is a `SelectionParams` field, already config-exposed.)

## Finding 3 — curves are not garbage

Reconstructed annual SDCs for derived (n_seasons=4) vs dissimilar (n_seasons=16–17)
HRUs: median SDCs are smooth and monotonic in both; dissimilar HRUs have ~2.5× higher
scale-free per-point deviation (0.09 vs 0.035), driven by a **few outlier years**
(dev ~0.2–0.28) among many consistent ones. Consistent with narrow elevation bands
melting sharply/near-uniformly and being under-resolved by 1-km SNODAS. *(Noise-source
characterization — peak-DOY / duration / calendar-year-framing — pending; see below.)*

## Threshold sweep (with the fixed, normalized metric)

Derivable fraction vs (`min_cells`, `norm_sim ≤ τ`):

| min_cells | τ=0.06 | τ=0.07 | τ=0.08 | τ=0.10 |
|---|---|---|---|---|
| 25 | 7.8% | 15.8% | 25.6% | 42.2% |
| 15 | 12.0% | 22.5% | 35.2% | **57.6%** |
| 10 | 15.1% | 26.9% | 41.0% | 66.5% |

e.g. `min_cells=15, norm_sim≤0.10` → **58% derived**, comparable to the paper's ~49%
CONUS-wide (vs. the current 0.02%).

## Finding 4 — calendar-year framing corrupts seasons (BUG, biggest real-noise driver)

We window by **calendar year** (`build._seasons` groups on `daily.index.year`); the
paper uses **water years (Oct 1 – Sep 30)**. Calendar-year framing puts the *end* of a
season's accumulation (Oct–Dec) in the same window as the *next* season's melt
(Jan–Jul), so `argmax` over the window can pick a **late-December snowfall event** as
the annual "peak" → a garbage 2–4 day "melt season" unrelated to the real spring melt.

Evidence (60 HRUs, n_seasons≥10, 1,238 HRU-years):
- peak day-of-year p10/50/90 = **11 / 60 / 341** — mass piled at *both* year ends.
- **26.5%** of peaks in January (doy≤31), **14.6%** in Nov–Dec (doy≥305) → ~41% at the
  calendar boundary (a spring-peaking snowpack should have ~0% there).
- **17% "never_zero"** (melt runs past Dec 31 → dropped); many 2–4 day durations.
- Per-HRU: the high-dev outlier years are exactly the late-December-peak / 2–4 day ones
  (e.g. HRU 2: 2006 doy=357 dur=3d dev=0.114; 2019 doy=340 dur=7d dev=0.144). High-
  elevation HRUs (HRU 4: peaks doy 33–95, dur 40–134 d) are far cleaner — this hits
  **low/mid-elevation** HRUs where December snow competes with the real melt.

**Fix:** window Stage 2 by **water year**. Stage 1's per-calendar-year NCs are read as
one continuous daily series (`open_mfdataset`) and just re-windowed, so the change is
localized to the grouping key in `build._seasons`
(`wy = index.year + (index.month >= 10)`), plus dropping the partial water years at the
record ends. Stage 1 (aggregation) is unchanged.

## Root cause = three compounding issues

The near-zero derived count is fully explained by:
1. **Calendar-year framing** (Finding 4) — genuine curve noise from mis-picked peaks.
2. **Similarity scales with n_seasons** (Finding 1) — the un-normalized metric then
   selects for poorly-sampled HRUs.
3. **`min_cells=25` too strict** for the elevation-banded fabric (Finding 2) — drops 49%
   before the similarity gate even applies.

## Proposed fix sequence

1. **Water-year framing** in `build._seasons` (Finding 4). *(methodological, matches paper)*
2. **Normalize `representative.similarity` by n_seasons** (Finding 1). *(bug)*
3. **Re-run Stage 2 only** (fast ~4 min; Stage 1 NCs unchanged) → recompute the
   `norm_sim` and cell-count distributions on the corrected curves.
4. **Recalibrate `SelectionParams`** (`min_cells`, `max_similarity`) from the corrected
   distributions — per-fabric config values (domain sign-off).

Do 1–2 first, because water-year framing will lower the real per-point noise, so the
`max_similarity` threshold should be set on the *corrected* distribution, not the
current calendar-year one (median norm_sim 0.080).

## RESOLUTION (implemented, branch `feat/snodas-lazy-batched-aggregation`)

Fixes 1 (water-year framing, `795a266`) + 2 (scale-free similarity, `795a266`) + 3
(threshold recalibration, `44a4201`) implemented with tests. Stage-2 re-run on the
unchanged aggregated NCs:

| sdc_status | before | after fixes 1+2 (old thresholds) |
|---|---|---|
| derived | 3 | **8,469 (50%)** |
| default_dissimilar | 8,476 | **10** |
| default_too_few_cells | 8,182 | 8,182 |
| default_constant_sca / no_snow | 133 / 20 | 133 / 20 |

- `n_seasons` median **16 → 21** (water-year framing recovers ~5 usable seasons/HRU).
- New scale-free similarity: p10/50/90/95 = **0.055 / 0.085 / 0.117 / 0.126**.
- **Chosen thresholds (domain sign-off): `min_cells=15`, `max_similarity=0.10`** →
  ~54% derived (paper ~49% CONUS-wide). Both are config values, per-fabric overridable.
  CONUS gfv2 (coarser HRUs) is unlikely to need different values but should be
  re-checked at its own run.

