# Cross-check: TM 6-B9 + Driscoll 2020 vs. our depstor workflow and pyWatershed

**Date:** 2026-07-08
**Sources added this round:**

- [`NHM_description_Regan_2018_TM6B9.md`](NHM_description_Regan_2018_TM6B9.md) — full conversion of USGS TM 6-B9 (Regan and others, 2018). **This is the authoritative NHM parameter-derivation reference.** We did not have it in-repo before.
- [`Surface_depression_storage_Driscoll_2020.md`](Surface_depression_storage_Driscoll_2020.md) — technical extract of Driscoll and others (2020), JAWRA 56(1):16–29.

**Checked against:** [`pywatershed_depression_storage_requirements.md`](pywatershed_depression_storage_requirements.md)
(pyWatershed 2.0.4 `PRMSRunoff`), `configs/depstor/depstor_params.yml`,
`configs/zonal/zonal_params.yml`, `src/gfv2_params/depstor_builders/`,
`src/gfv2_params/zonal_runners/ssflux.py`, and the on-disk CONUS product at
`gfv2_param_v2/gfv2/params/merged/`.

---

## The one-paragraph version

TM 6-B9's appendix 1 says plainly that **five DPRST parameters were calibrated
per-HRU** — `dprst_depth_avg`, `dprst_et_coef`, `dprst_flow_coef`,
`op_flow_thres`, `dprst_seep_rate_open` — and Driscoll 2020 is the paper that
documents that calibration and publishes their distributions. Our gap doc treats
all five as constants that "match the pyWatershed default." They are not
constants in the NHM, and for two of them (`dprst_depth_avg`, `op_flow_thres`)
the constant we carry is materially off the NHM central value. Separately, TM 6-B9
gives an explicit **derivation equation for `smidx_exp`**, which our gap doc
declares has "no spatial basis"; we already produce every input to it but one.
And two parameters the gap doc lists as unproduced gaps — `dprst_flow_coef`,
`dprst_seep_rate_open` — **we have been emitting per-HRU all along** via the
`ssflux` runner. Nothing in either paper argues for retrograding the classifier
work; our on-stream logic is a deliberate, documented improvement on theirs.

---

## Findings that change something

### 1. `dprst_depth_avg` is calibrated per-HRU in the NHM, not 132 inches

*(the parameter you called out by name)*

| Source | Value |
|---|---|
| PRMS / pyWatershed 2.0.4 code default | 132.0 in |
| TM 6-B9 Table 1–1 ("constant-value parameters") | 132.0 in |
| TM 6-B9 appendix 1 narrative | *"A calibration procedure by HRU was used to determine spatially distributed values for the average DPRST depth within [an] HRU (`dprst_depth_avg`)…"* |
| **Driscoll 2020 Table 1 (the actual NHM calibrated values)** | **range 10–300 in; median 49; mean 80.4; sd 79** |

TM 6-B9 contradicts itself: Table 1–1 lists 132.0 as a constant, and then the
appendix says it was calibrated. Driscoll 2020 resolves it — 132 is the code
default; the NHM production values are distributed, and **the median is 49 in,
about 2.7× smaller than 132.**

Why it matters: max depression volume = `dprst_depth_avg × hru_area × dprst_frac`.
This term alone sets storage capacity, and capacity is what decides whether spill
ever happens. A uniform 132 in gives every HRU a depression ~11 ft deep, near the
upper end of the calibrated distribution. Depressions will underfill, spill will
be suppressed, and simulated depression storage will run high.

**Options, best first:**

1. **Harvest the calibrated values from NhmParamDb.** All five calibrated params
   are published per-HRU (Driscoll and others, 2017, ScienceBase; TM 6-B9's
   "Parameter Visualization" section notes the full NhmParamDb was released as
   CSVs). If gfv2 HRU ids can be joined to NHM v1.0 `nhm_id`, this is a direct
   transfer and needs no new science. **Verify the id join first** — NHM v1.0 is
   109,951 HRUs on the GFv1 fabric; gfv2 is a different fabric.
2. Adopt the NHM **median (49 in)** as the constant, not 132. Defensible, honest,
   one-line change.
3. Keep 132 and document that we knowingly use the code default. Weakest option.

There is no CONUS depression-depth raster, so a zonal-stats derivation is not
available. This is a transfer-or-choose-a-constant decision, not a builder.

### 2. `op_flow_thres` = 1.0 suppresses depression interflow entirely

Gap doc says 1.0 "matches" the pyWatershed default. It does. But the NHM
calibrated distribution is **0.75–1.0, median 0.83, mean 0.85**.

`op_flow_thres` is the fraction of open-depression capacity above which interflow
occurs. At exactly 1.0, interflow only begins when storage is 100% full — which
is also where spill begins. Setting it to 1.0 effectively turns off the DPRST
interflow pathway and routes everything through spill. The NHM's median 0.83
opens interflow at 83% full. This is a real behavioral difference, not a rounding
choice.

Same remedy options as (1). Note `dprst_flow_coef` is the *rate* on this same
pathway, so (2) and (4) interact.

### 3. `smidx_exp` HAS a derivation — and we already have all but one input

Gap doc: *"adopt the pyWatershed default (`smidx_exp=0.3`); no spatial basis
exists for any of them, so there is nothing further to derive."*

TM 6-B9 appendix 1 (eqs. 1–1, 1–2) derives it per-HRU:

```
smidx_exp = log10( carea_max / smidx_coef ) / smidx_max
smidx_max = soil_moist_max + (0.5 × ppt_max)
```

`ppt_max` = maximum daily precipitation per HRU, from Daymet, 1980–2014.

> ⚠️ **The report typesets eq. 1–1 as `(log10(carea_max/smidx_coef))^(1/smidx_max)`
> — a fractional power, not a division. That is a typo.** Inverting PRMS-IV
> eq. 1-98 (`carea = smidx_coef × 10^(smidx_exp × smidx)`, capped at `carea_max`)
> at `smidx = smidx_max` gives the division. The units settle it: TM 6-B9 Table 1–2
> gives `smidx_exp` units of `1.0/inch` and `smidx_max` is in inches, so
> `log10(ratio)/inches → 1/inch`. The printed power form is dimensionless.
> I verified the typeset equation against a 200-dpi page render before concluding
> this. Implement the division.

We already emit `carea_max`, `smidx_coef`, and `soil_moist_max` per-HRU. **The only
missing input is `ppt_max`** — a per-HRU max-daily-precip zonal statistic over
Daymet. This is a tractable new builder (`smidx_exp`), and it is the highest-value
*derivable* item in this whole cross-check.

Guards it will need: `smidx_coef == 0` (division by zero), and
`carea_max == smidx_coef` (log10 → 0 → `smidx_exp` = 0, which would disable the
nonlinear term). Both occur — `smidx_coef` is 0 wherever no cell exceeds TWI 15.6.

### 4. `dprst_flow_coef` and `dprst_seep_rate_open` are not gaps — we already emit them

The gap doc's "Bucket 3" says these have "no spatial basis" and recommends adopting
scalar defaults. Both are wrong:

- `src/gfv2_params/zonal_runners/ssflux.py:116-117` computes
  `dprst_seep_rate_open := ssr2gw_rate` and `dprst_flow_coef := fastcoef_lin`.
- They are on disk, per-HRU, for CONUS:
  `gfv2/params/merged/filled_nhm_ssflux_params.csv` (verified — columns present,
  non-degenerate).
- `src/gfv2_params/viz.py:530-531` already maps them for plotting.

So Bucket 3 has exactly **one** true member: `smidx_exp` — and per (3), it is
derivable rather than adoptable.

**But our normalization ranges should be checked against the NHM envelope:**

| Param | our `flux_params` range | NHM calibrated range (Driscoll T1) | NHM median | pyWS default |
|---|---|---|---|---|
| `dprst_seep_rate_open` | 0.005 – **0.2** | 0.00001 – **0.2** | 0.033 | 0.02 |
| `dprst_flow_coef` | 0.005 – **0.5** | 0.0001 – **0.1** | 0.048 | 0.05 |

`dprst_seep_rate_open`'s upper bound matches the NHM exactly. **`dprst_flow_coef`'s
upper bound is 5× the NHM's calibrated maximum.** Recommend capping at 0.1.
(Reassuringly, pyWatershed's 0.05 default ≈ the NHM median 0.048.)

### 5. `dprst_seep_rate_clos` — the flagged "needs modeler sign-off" question is answered

Gap doc: legacy 0.2 vs. pyWatershed 0.02, a 10× discrepancy, "treat this as needing
a modeler's sign-off."

TM 6-B9, appendix 1, last line of the DPRST section:

> "Parameter `dprst_seep_rate_clos` was set to values of `dprst_seep_rate_open`."

So in the NHM it is not a constant at all — it is a **copy of the per-HRU
`dprst_seep_rate_open` array.** Neither 0.2 nor 0.02.

It is also *inert* in any NHM-faithful run: `dprst_frac_open = 1.0` everywhere, so
there is no closed storage for it to act on. Emit `clos := open` and move on. No
sign-off needed. (The legacy 0.2 was almost certainly the top of the seep range,
not a considered value.)

### 6. `dprst_et_coef` — calibrated, but 1.0 is a fine central value

NHM: 0.75–1.25, median 1.04, mean 1.02. Our 1.0 is defensible. Low priority.
Driscoll 2020 notes this parameter is entangled with the rectangular-basin
geometry assumption (`va_*_exp` = 0.001) — a better bottom geometry would change
the exposed evaporative area and hence the meaningful value of `dprst_et_coef`.
Worth knowing, not worth acting on.

---

## Verified correct — do not "fix" these

Everything here matches TM 6-B9 exactly. Recording it so a future reader doesn't
"correct" us toward the paper.

- **`carea_max`** = fraction of HRU **pervious** area where `TWI > 8.0` **or**
  on-stream storage exists. Our `compute_carea_map_binary` is
  `land ∧ perv ∧ (TWI > thr ∨ onstream)`, with `denominator: perv_frac` in
  `depstor_params.yml`. Exact match, including the pervious-area denominator.
- **`smidx_coef`** — same formulation at `TWI > 15.6`. TM 6-B9 adds "with the
  condition that values must be less than or equal to `carea_max`."
  **We need no clamp:** `{TWI > 15.6} ⊂ {TWI > 8.0}`, both branches share the same
  `perv` gate and the same `∨ onstream` term, so the `smidx` numerator is a subset
  of the `carea` numerator over an identical denominator. The constraint holds
  structurally. (Also true in `percentile` mode, since p_smidx > p_carea.)
- **`hru_percent_imperv`** = NLCD cells with imperviousness > 50 percent. Match.
- **`sro_to_dprst_perv` / `sro_to_dprst_imperv`** — TM 6-B9's worked example
  (fig. 1–3: `perv = 30/130 = 0.2301`, `imperv = 5/15 = 0.333`) confirms the
  denominators are *total* pervious and *total* impervious area in the HRU, where
  total pervious = `hru_area − imperv_area − dprst_area`. Our `perv` builder is
  `land ∧ ¬imperv ∧ ¬dprst` — match. TM also requires that "these contributing
  areas exclude the surface depressions themselves"; ours do, automatically,
  because `perv` and `imperv` are each disjoint from `dprst` under the per-cell
  carve.
- **`dprst_frac_open` = 1.0 / `va_open_exp` = `va_clos_exp` = 0.001.** Driscoll
  2020: all depressions specified open; bottom shape approximates a rectangular
  basin. TM 6-B9 Table 1–1 annotates 0.001 as "an approximate rectangle." Both
  agree with what we carry.
- **`snarea_thresh`** = per-HRU median of the yearly maximum SWE from SNODAS.
  That is exactly what `snarea/library.py::snarea_thresh_inches` computes.
- **`imperv_stor_max` = 0.05 in, `dprst_frac_init` = 0.5.** Match.

### The on-stream classifier: ours is better, keep it

TM 6-B9 and Driscoll 2020 both define on-stream as *NHDPlus waterbodies within or
intersecting a **60-m buffer** of the GF stream segments.* That is the legacy
`streambuffer` test this repo retired in PR #139, and replaced with an NHD-topology
classifier (WBAREACOMI artificial-path topology ∪ geometric flow-through topology,
both gated on Network-Flowline membership; Playa force-dprst; Ice Mass excluded).

**This is a deliberate improvement and should not be retrograded.** The papers
predate it. Two consequences to keep in mind:

- Because `carea_max`/`smidx_coef` take `∨ onstream` in their numerators, our
  improved on-stream mask propagates into `carea_max` too. Our `carea_max`
  intentionally differs from NHM v1.0's. That's correct, and it is a second reason
  transferred NhmParamDb values (option 1 in finding 1) must be treated as
  *depression-physics* parameters only — do not transfer `carea_max`/`smidx_coef`.
- `dprst_frac` in the NHM covers 53,007 of 109,951 HRUs (48.2%). A useful sanity
  benchmark for our own dprst coverage on a comparable fabric.

---

## Adjacent finding: `ssflux` deviates from TM 6-B9 (affects 2 dprst params)

Not depression-storage per se, but `dprst_seep_rate_open` and `dprst_flow_coef` are
both derived from `ssflux`, so it's in scope.

> **Update 2026-07-09 — the headline is worse than the deviations below.** Measured on
> the production CONUS product (n = 361,471 HRUs), **every `ssflux` parameter is
> effectively spatially constant**: ~90% of HRUs sit within 1% of their configured
> range *minimum* (99.3% for `soil2gw_max`). Root cause: `k_perm` is log10
> permeability, `ssflux.py:75` exponentiates it to linear space, and `ssflux.py:139`
> then applies a **linear** min–max normalisation to a variable spanning ~15 orders
> of magnitude (`k_perm_wtd`: 2.6e-25 → 6.5e-10, max/median = 3.4e5). Everything
> collapses onto the floor. `soil2gw_max` is the only parameter that cubes
> `k_perm_wtd`, and it is the most degenerate — strong evidence the interpolation
> was meant to happen on the **log** scale. This defeats TM 6-B9's stated purpose
> ("in place of the assumption of spatially constant values") and it degrades
> `dprst_seep_rate_open` and `dprst_flow_coef`, both of which pyWatershed consumes.
> Tracked in **issue #175**; resolve the normalisation space *before* the cube in (a).

**(a) Missing cube.** TM 6-B9, appendix 1, "Soil Zone Parameters":

| Param | TM 6-B9 | `ssflux.py` | |
|---|---|---|---|
| `soil2gw_max` | `k_perm³` | `k_perm_wtd ** 3` | ✅ |
| `ssr2gw_rate` | `k_perm³ × (1 − hru_slope)` | `k_perm_wtd * (1 - slope)` | ❌ no cube |
| `slowcoef_lin` | `k_perm³ × hru_slope / hru_area` | `k_perm_wtd * slope / hru_area` | ❌ no cube |
| `fastcoef_lin` | `slowcoef_lin × 2` | `2 * r_slowcoef_lin` | ✅ |

Min–max normalization afterward is monotonic, so ranks survive — but cubing is
strongly nonlinear and `k_perm_wtd` spans many orders of magnitude, so the
*normalized values* differ substantially. `ssr2gw_rate` feeds
`dprst_seep_rate_open` directly.

**(b) Per-batch normalization.** TM 6-B9 says values are interpolated to the
acceptable range across "all HRUs in a **GF region**." `ssflux.py:119-121`
normalizes per **batch**, and its own comment concedes: *"The same raw value may
map to slightly different normalised values across batches."* Batches are arbitrary
HRU chunks, not regions. Identical geology + slope should not yield different
parameters because of how HRUs were split across SLURM array tasks. This is a
defect on its own terms, independent of the papers.

**(c) `gwflow_coef`.** TM 6-B9 derives it from a best-fit multiple-linear
regression on geology, drainage density, aquifer type, vegetation type, and
base-flow index, giving values in **0.004–0.055**. We set
`gwflow_coef := slowcoef_lin` normalized to **0.005–0.3** — a different method with
a top end ~5.5× the NHM's. Out of scope for depstor; flagging for the record.

---

## Recommended actions

Nothing here is committed. Ordered by value:

1. **Correct `pywatershed_depression_storage_requirements.md`.** Bucket 2 is
   mislabeled ("matches" is true of the *code default*, not of the NHM), Bucket 3
   has two false members, and the `dprst_seep_rate_clos` open question is answered
   by TM 6-B9. This doc is currently the thing a modeler would read and trust.
2. **Decide `dprst_depth_avg` and `op_flow_thres`.** Check whether gfv2 HRU ids
   join to NHM v1.0 `nhm_id`; if so, transfer all five calibrated params from
   NhmParamDb. If not, adopt the NHM medians (49 in, 0.83) over the code defaults.
   *This is a modeler's call and the one place I'd want your sign-off.*
3. **Build `smidx_exp`** (new builder + test, per repo convention). Needs a
   `ppt_max` zonal stat over Daymet. Implement the division form of eq. 1–1.
4. **Cap `dprst_flow_coef` at 0.1** in `configs/zonal/zonal_params.yml` to stay
   inside the NHM's calibrated envelope.
5. **Emit `dprst_seep_rate_clos := dprst_seep_rate_open`** rather than a constant.
6. **File an issue for the `ssflux` deviations** (missing cube; per-batch vs.
   per-region normalization; `gwflow_coef` method). Do not fold into a depstor PR.

## Open questions I could not resolve from the papers

- ~~Does the gfv2 fabric's `nat_hru_id` join to NHM v1.0 `nhm_id`?~~ **Answered
  2026-07-09: no 1:1 join.** gfv2 has **361,471 HRUs**; NHM v1.0 has 109,951. So
  option 1 in finding (1) — transferring calibrated values from NhmParamDb — is not
  a direct join, and any comparison to NHM values must be **distributional**, not
  per-HRU. This is what motivated deriving `dprst_depth_avg` from topography
  instead (issue #173).
- TM 6-B9 says `soil_type` and `soil_moist_max` are "based on a reclassification of
  NLCD2001" in one sentence, having just said two sentences earlier that STATSGO
  supplies them. The STATSGO reading is obviously correct; noting the report's
  wording is unreliable here.
- We do not emit `soil_rechr_max_frac` (= `soil_rechr_max / soil_moist_max`).
  It's a soilzone parameter, outside `PRMSRunoff`, so it did not surface in the
  pyWatershed gap analysis — but PRMS needs it. Worth a separate check of the
  soilzone parameter set against TM 6-B9 Table 1–2, which we now have.
