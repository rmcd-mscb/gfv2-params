# Design: distribution-invariant `carea_max` / `smidx_coef` (unify #94 + #55 Stage 1)

**Date:** 2026-05-21
**Status:** design — pending implementation plan
**Issues folded in:** #94 (TWI staging gap), #55 Stage 1 (decouple from absolute TWI thresholds)
**Deferred:** #55 Stage 2 (NWI/SSURGO/NLCD observational decoupling)

---

## 1. Problem & motivation

Two PRMS depression-storage parameters, both per-HRU floats in [0, 1]:

- **`carea_max`** — the *storm-event ceiling*: the maximum fraction of an HRU's
  pervious area that can produce surface runoff at full saturation (upper bound
  of the variable source area).
- **`smidx_coef`** — the *soil-moisture-index coefficient*: scales how fast the
  contributing area grows with wetting in PRMS's nonlinear runoff term
  `ca_fraction = smidx_coef × smidx^smidx_exp`.

They are physically distinct (a ceiling vs. a reactivity rate), but the legacy
method derives **both from a single proxy — TWI — at two hardcoded thresholds.**

Two open issues meet here:

- **#94** — the canonical ArcPy TWI (`Twi_merged_<vpu>.tif` → `shared/conus/vrt/twi.vrt`)
  is only populated for **VPU 01**. Everywhere else `twi.vrt` is nodata, so any
  `carea_map` run outside VPU 01 is degenerate (`carea_map_t8 ≡ carea_map_t156`,
  byte-identical). The populated alternative — open-source `Twi_hydrodem_<vpu>.tif`,
  present and valid for all 18 VPUs — *cannot* be adopted because…
- **#55** — …the `8.0`/`15.6` thresholds are calibrated to the *shape* of the
  ArcPy TWI distribution. Any TWI pipeline change silently invalidates them
  ([build_vrt.py](../../../src/gfv2_params/shared_rasters/build_vrt.py) literally
  says "**DO NOT SWAP**" to `Twi_hydrodem`).

**The link that unifies them:** if the cutoff is derived from the TWI data
itself (a percentile) instead of being hardcoded, it *self-recalibrates* when the
source changes — so adopting `Twi_hydrodem` becomes safe, which closes #94's
coverage gap. #55 Stage 1 is the enabler for #94.

---

## 2. Background: the previous method (ArcPy) and how this repo reproduces it

### 2.1 The legacy computation

`getCarea(threshold)` in [docs/0b_TB_depr_stor.py:264-312](../../0b_TB_depr_stor.py#L264)
does exactly:

```
careaMap = pervious AND ( TWI > threshold  OR  onStreamStorage )   # 0/1 mask
carea    = count(careaMap cells per HRU) / count(pervious cells per HRU)
carea    = min(carea, 1.0)
```

with two hardcoded thresholds:

- `carea_max`  → **TWI > 8.0**  ([:244-261](../../0b_TB_depr_stor.py#L244))
- `smidx_coef` → **TWI > 15.6** ([:219-239](../../0b_TB_depr_stor.py#L219))

Each parameter is *"the fraction of this HRU's pervious cells whose TWI clears a
fixed cutoff (or that sit on a stream)."* Because 15.6 > 8.0, `smidx_coef ≤ carea_max`
by construction.

### 2.2 Faithful reproduction in this repo

The same math, split across the orchestrator/builder pattern:

- [carea_map.py](../../../src/gfv2_params/depstor_builders/carea_map.py) +
  `compute_carea_map_binary` build the two 0/1 rasters
  (`carea_map_t8_binary.tif`, `carea_map_t156_binary.tif`) using the literal
  `8.0`/`15.6` from
  [depstor_rasters.yml](../../../configs/depstor/depstor_rasters.yml#L62-L68).
- [depstor_params.yml](../../../configs/depstor/depstor_params.yml) zonal-counts
  each binary per HRU (`carea_t8_frac`, `carea_t156_frac`), counts pervious cells
  (`perv_frac`), and the `carea_max` / `smidx_coef` ratios divide-and-clamp —
  byte-for-byte the legacy `careaCount / perviousCount` capped at 1.0.

### 2.3 Provenance of 8.0 / 15.6 — *undocumented*

Searched every reference doc. `8.0`/`15.6` appear as **bare constants with no
derivation**: the legacy docstrings, [depstor_workflow.md:344,354](../../depstor_workflow.md#L344),
and [README.md:294](../../../README.md#L294) all present them as given.
README frames them as *"calibration thresholds [that] reference the canonical
ArcPy-derived TWI"* — i.e. empirically calibrated to that specific TWI product,
not analytically derived. **We do not know a priori what percentile they occupy.**

### 2.4 The three structural problems (from #55)

1. **One proxy, two thresholds, conflating two physics.** TWI is a steady-state
   geomorphic wetness proxy; using it at 8.0 (storm ceiling) and 15.6 (reactivity)
   entangles two quantities that should move independently.
2. **Absolute cutoffs welded to one distribution.** Any TWI pipeline change shifts
   the distribution and silently invalidates 8.0/15.6 — the root cause of #94's
   deadlock.
3. **Hard step at the threshold** — TWI 7.99 → 0, 8.01 → 1; no graduated response.

---

## 3. Proposed change (Stage 1)

**Replace the hardcoded cutoff with a cutoff derived from the TWI data itself —
a percentile `T_P`** — applied otherwise identically:

```
T_P      = the P-th percentile of TWI over a reference population
careaMap = pervious AND ( TWI > T_P  OR  onStreamStorage )
carea    = count(careaMap) / count(pervious)   ,  capped at 1.0
```

Changing the TWI source shifts the whole distribution, but `T_P` is recomputed
from that same distribution, so it shifts with it: a cell's *rank* (is it in the
wettest quartile?) is preserved. The parameters become **invariant to the TWI
source** → `Twi_hydrodem` is safe → #94 closes. This fixes **problem 2 only**.

### 3.1 What Stage 1 deliberately does NOT do

It keeps the single-proxy / two-threshold / step-function structure. **Problems 1
and 3** (decoupling the two parameters via observational data — stream buffer +
NLCD wetlands for `carea_max`; NWI + hydric soils, *no TWI*, for `smidx_coef` —
and a graduated response) are **#55 Stage 2**, deferred to a follow-up issue.

### 3.2 Reference population (decided: valid-land TWI)

A percentile needs a population. Per-HRU self-referential cutoffs are **degenerate**
(taking each HRU's own top quartile drives `carea_max ≈ 1−P` everywhere, destroying
cross-HRU contrast). A **reference population** yields one `T_P` applied across
HRUs — structurally like today's 8.0, but auto-derived — and preserves cross-HRU
dynamic range.

**Decision:** the reference population is **valid-land TWI** (all land cells, via
`land_mask`), *not* pervious-only. Rationale: the percentile's job is only to
define "what TWI is high" for a given raster product — a clean, **source-intrinsic,
fabric-independent** property computed once per TWI source and cached in `shared/`.
The pervious restriction still governs the numerator/denominator of the actual
parameter; the two concerns are separable.

### 3.3 Reference scope (decided: compute both, switchable)

Compute `T_P` at **two scopes** for **both TWI sources**, and select via config:

- **CONUS-global** — one `T_P` nationwide; closest analog to today's fixed scalar,
  auto-recalibrating per source.
- **Per-VPU** — one `T_P` per VPU; preserves regional climate/terrain differences.

### 3.4 Setting the percentile defaults (principled, not guessed)

Rather than guessing P75/P95, **derive the defaults by inverting the legacy
thresholds through the VPU 01 ArcPy-TWI CDF**: measure what percentile `8.0` and
`15.6` occupy in VPU 01's valid-land TWI distribution. Those measured percentiles
(`P_carea`, `P_smidx`) become the defaults — so percentile-mode reproduces the
legacy parameters on VPU 01 *by construction*, then ports invariantly to other
sources/regions. P75/P95 (#55's draft) is retained only as a sanity reference.
Percentiles remain config-overridable.

---

## 4. Architecture

### 4.1 Data staging (shared / Part-1)

- **`twi_hydrodem.vrt`** — add a VRT entry in
  [build_vrt.py](../../../src/gfv2_params/shared_rasters/build_vrt.py) sourced
  from `Twi_hydrodem_*.tif` (all 18 present, multi-GB, valid). Apply
  `-a_srs EPSG:5070` so the VRT carries a *named* CRS — the tiles report
  `"unnamed"` Albers/GRS80 and carea_map does a strict `src.crs != template.crs`
  check. Verified: `Twi_hydrodem_17` is whole-cell aligned with the fdr-clip
  template (Δx/30 = 4297.0, Δy/30 = 0.0).
- **Finish `twi.vrt`** — a SLURM step running `merge_rpu_by_vpu` (TWI manifest,
  [merge_rpu_by_vpu_twi.yml](../../../configs/shared_rasters/merge_rpu_by_vpu_twi.yml))
  for VPUs 02–18, then rebuilding the VRT. All inputs are present
  (`input/twi/<rpu>/twi.tif`, 59 RPUs; per-VPU land masks for all 18 VPUs);
  02–18 were simply never merged. Pure on-cluster — **no ArcPy needed.** This
  enables the `twi.vrt` vs `twi_hydrodem.vrt` A/B.

Both VRTs become selectable `twi_raster` sources in the fabric profiles.

### 4.2 Reference-percentile pre-step (new)

A new step computes the `T_P` table over valid-land TWI:

- Output: a small cached artifact (CSV/YAML) keyed by
  `(twi_source, scope, vpu?)` → `{p_carea: T_Pa, p_smidx: T_Ps}`, for both scopes
  and both sources. Lives in `shared/`.
- Includes the §3.4 inversion: report the percentile that 8.0 / 15.6 occupy in the
  VPU 01 ArcPy distribution, used to seed `P_carea` / `P_smidx`.
- Cheap (decimated/streamed percentile over land-masked TWI per VPU + CONUS).

### 4.3 `carea_max` / `smidx_coef` refactor (Approach A — zonal stage)

A `threshold_mode` switch:

- **`absolute`** (default for now) — 8.0 / 15.6 via the **existing binary-raster
  builder + count/ratio**, untouched. This is the calibrated A/B baseline.
- **`percentile`** with `reference_scope: vpu | conus` and
  `twi_source: arcpy | hydrodem` — a **new zonal runner** resolves each HRU's
  threshold (`mode × scope × source`; per-VPU keyed by the HRU's VPU — a profile
  scalar for single-VPU fabrics like oregon, an HRU attribute for multi-VPU gfv2)
  and computes the parameter directly per HRU: same numpy classify as
  `compute_carea_map_binary`, aggregated per HRU instead of rasterized.

**Decision:** keep `absolute` on the existing builder (zero regression risk to the
trusted baseline, exact byte-for-byte A/B reference) rather than unifying both modes
into one runner. Accepts two code paths short-term; can collapse to one once
percentile mode is validated.

The parameter contract is **unchanged**: same pervious denominator, same onstream
inclusion, same clamp to 1.0, same per-HRU [0, 1] float into the same PRMS slots.
Only the source of the cutoff number changes.

### 4.4 Mode ↔ source pairing

`threshold_mode` and `twi_source` are **independent config axes**, but only one
combination of each is a valid *production* output. The defaults wire up the
correct pairing; the cross combinations exist solely for validation.

**Production pairing (the "diagonal" — these are the only shippable outputs):**

| `threshold_mode` | `twi_source` | Meaning |
|---|---|---|
| `absolute` (8.0 / 15.6) | `twi.vrt` (ArcPy `Twi_merged`) | The only source those constants were calibrated against. Valid only where ArcPy TWI exists (VPU 01, or anywhere once §4.1 finishes `twi.vrt`). |
| `percentile` (data-derived) | `twi_hydrodem.vrt` | CONUS-complete open-source source; the path forward for every fabric outside VPU 01. |

**Off-diagonal (validation-only, never shipped):**

- `absolute` × `twi_hydrodem.vrt` — the *broken* case (absolute thresholds on a
  distribution they were not calibrated to). Run **only** in the §5 invariance
  proof as the counterexample that demonstrates absolute-mode fragility.
- `percentile` × `twi.vrt` — used in the §3.4 CDF inversion (measure where
  8.0/15.6 land in VPU 01's ArcPy distribution) and as the percentile-mode arm of
  the VPU 01 calibration A/B.

**Guard:** the config loader emits a warning (not a hard error — validation needs
the off-diagonal) when `threshold_mode: absolute` is paired with any non-ArcPy
`twi_source`, so a broken combination cannot be shipped by accident.

### 4.5 Per-VPU threshold application (multi-VPU fabrics, e.g. gfv2)

`reference_scope: vpu` resolves a different `T_P` per VPU, so a fabric spanning
multiple VPUs (gfv2 = all 18) needs an HRU→VPU mapping. Because the percentile
runner processes HRUs one at a time (Approach A, §4.3), this is a per-HRU lookup,
not a spatial operation.

**HRU→VPU resolution precedence:**

1. **Profile `vpu:` scalar** — single-VPU fabrics (oregon → `vpu: "17"`) declare
   their VPU in the profile; every HRU uses that `T_P`.
2. **Fabric `vpu` attribute** — multi-VPU fabrics use the per-HRU column. Verified
   present on gfv2 (`gfv2_nhru_merged.gpkg` layer `nhru`: fields include `vpu`,
   `source_vpu`, `vpu_agg_id`; 361,471 HRUs). The runner loads an
   `id_feature → vpu` map once from the fabric and looks up each HRU by id within
   the batch (no dependency on batch files carrying the attribute).
3. **Neither present** — `percentile` + `vpu` scope raises with a clear message
   (require a profile `vpu:` or a fabric `vpu` column).

**Border HRUs:** an HRU is assigned to its single home `vpu` (the attribute value),
so it gets exactly one threshold even if its cells drape slightly across a VPU
boundary — well-defined, no per-cell ambiguity. This matches how the parameter is
already a single per-HRU value.

**Reference-percentile coverage:** the §4.2 table must contain a per-VPU `T_P` for
every VPU present in the fabric. The pre-step computes all 18 regardless, so gfv2
is covered; the runner validates that each HRU's `vpu` has an entry and fails
loudly otherwise.

---

## 5. Validation & success criteria

- **Non-degeneracy:** on **oregon** (VPU 17), percentile-mode yields
  `carea_max ≠ smidx_coef` (the t8 ≡ t156 byte-identical bug is gone).
- **Calibration A/B on `gfv2_vpu01`** (the one fabric with valid ArcPy TWI):
  compare `percentile`-mode vs `absolute`-mode parameters per HRU; with the §3.4
  inverted defaults, percentile-mode should reproduce the absolute outputs closely.
  Report per-HRU distribution stats / scatter.
- **Source A/B:** `twi.vrt` (ArcPy) vs `twi_hydrodem.vrt` once both exist —
  quantify how much the parameters move (should be small under percentile-mode,
  large under absolute-mode — demonstrating the invariance claim).

---

## 6. Tests & docs

- **Unit tests** (builder + test together, repo convention): percentile
  computation over a masked array; threshold resolution (`mode × scope × source`,
  per-VPU lookup); per-HRU classify + aggregate equivalence with
  `compute_carea_map_binary` on a synthetic grid; CDF-inversion helper.
- **Docs:** README, `slurm_batch/RUNME.md` (new Stage: TWI merge completion +
  reference-percentile + percentile-mode params), update the #94 caveat in
  [base_config.yml](../../../configs/base_config.yml), refresh the
  `twi_canonical_source` memory and the "DO NOT SWAP" note in `build_vrt.py`.
- **Issue housekeeping:** open one combined umbrella issue; mark #94 + #55-Stage-1
  folded in; leave #55-Stage-2 as the remaining scope.

---

## 7. Open questions / follow-ups

- **`smidx_exp`** is still not produced by this pipeline (NHM default); unchanged
  by this work, noted for completeness.
- **Stage 2** (observational decoupling) — separate spec.

(The multi-VPU per-VPU application is resolved in §4.5: gfv2 carries a per-HRU
`vpu` attribute, so it's a direct lookup, not a spatial join.)
