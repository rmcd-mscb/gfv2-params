# Depression-respecting FDR for `drains_to_dprst` contributing area (#147)

**Date:** 2026-06-29
**Issue:** [#147](https://github.com/rmcd-mscb/gfv2-params/issues/147) — Investigate: depression-respecting FDR (vs fully-filled HydroDEM) for `drains_to_dprst` contributing area
**Branch:** `feat/depression-respecting-fdr`
**Status:** design approved, pending spec review

## Problem

`drains_to_dprst` (the contributing area routed to depression storage) is traced
with the in-process ESRI-D8 kernel over **`fdr.vrt`**, the NHDPlus V2 `FdrFac`
component: an NHDPlus HydroDEM that is **stream-burned + walled + fully
depression-filled** (drainage-enforced). On a fully filled DEM, real sinks are
removed, so flow is forced *through* what should be terminal depressions, and
adjacent catchments merge across former divides. Both effects inflate and
mis-attribute each depression's contributing area.

This is a separate axis from #145 (which fixed *which* waterbodies are
pour-points). #147 is about *how far upslope each pour-point reaches*. It is **not**
a cap or tuning knob — the goal is to choose the physically appropriate flow
field so large contributing areas fall out of correct hydrology, not a threshold.

Provenance is pinned in the issue comments: both the legacy ArcPy lineage
(SRTM → ArcGIS Fill-all → D8) and the current gfv2 lineage (NHDPlus HydroDEM →
official NHDPlus `FdrFac`) **fully fill depressions**, so the over-connection is
baked into both. A "burn waterbody cells to FDR=0" shortcut was analysed and
rejected (no-op for already-terminal dprst cells; regression for #145
flow-through cells; edits the sink not the upslope paths; can't distinguish a
terminal sink from a link in an endorheic spill chain). The correct fix is to
**not fully fill depressions when deriving the FDR** — breach instead of fill —
yielding natural termini at real depressions while preserving spill connectivity.

## Goal of this work

A **full empirical investigation** (issue's proposed scope): build a
depression-respecting FDR as an **additional derived raster** (never swapping
`fdr.vrt` or `Fdr_hydrodem`), then A/B `drains_to_dprst` against it on two test
VPUs and write the findings up on #147. Decide whether breach is worth a CONUS
scale-up (a *follow-up*, not this branch).

## Conditioning method

**Breaching** via WhiteboxTools `BreachDepressionsLeastCost` — least-cost outlet
carving that preserves real closed depressions while keeping spill connectivity
between chained depressions. Already in the stack (WBT is used by
`compute_dem_derivatives.py`). The issue's leading recommendation.

Depth/area-thresholded fill and depression-hierarchy classification are
alternatives if breach-alone proves insufficient — out of scope here.

## Test regions

| VPU | Region | Role |
|-----|--------|------|
| 09  | Souris-Red-Rainy (Prairie Pothole) | Real closed potholes; must **retain** high dprst, where over-fill most inflates contributing area. Red River valley also gives a humid through-flow corridor. |
| 16  | Great Basin (endorheic) | Closed-basin spill chains down to terminal playas; must **not collapse** under breach (over-fragmentation guardrail). |

Both VPUs already have the full open-source derivative set staged in
`shared/per_vpu/<vpu>/`: `Hydrodem_merged_fixed_<vpu>.tif` (breach input),
`Fdr_hydrodem_<vpu>.tif` (the fill-control), and `land_mask_<vpu>.tif`. Only the
breach branch is new compute.

## Architecture

Three pieces, isolated and independently testable.

### Piece 1 — `compute_breached_fdr` shared-raster builder (new artifact)

A new **opt-in** builder module
`src/gfv2_params/shared_rasters/compute_breached_fdr.py`, registered in the
`BUILDERS` dict in `shared_rasters/__init__.py` but **NOT** added to the default
`steps:` list of `configs/shared_rasters/shared_rasters.yml` — same opt-in
posture as `compute_dem_derivatives`.

Single purpose: produce a depression-respecting FDR. Per VPU:

```
Hydrodem_merged_fixed_<vpu>.tif   (reuse if present; else _fix_dem_nodata from the source Hydrodem)
  → WBT BreachDepressionsLeastCost  → Hydrodem_breached_<vpu>.tif
  → WBT D8Pointer (--esri_pntr)      → Fdr_breached_<vpu>.tif
```

- Reuses `_fix_dem_nodata` and `_run_wbt` from `compute_dem_derivatives.py`
  (import, do not duplicate). Re-creates the fixed DEM only if absent.
- **No** FAC / slope / aspect / TWI — those are TWI-side derivatives, irrelevant
  to depression routing. Keeping this builder FDR-only is what makes it a clean,
  single-purpose unit.
- Outputs land in `shared/per_vpu/<vpu>/`, parallel to `Fdr_hydrodem_<vpu>.tif`.
  Nothing existing is overwritten.
- **WBT predictor=2 gotcha:** the fixed DEM is written LZW *without* `predictor=2`;
  breach and D8 outputs must stay predictor-free so any downstream WBT step reads
  them correctly.

`BreachDepressionsLeastCost` parameters (`--dist` max breach search radius in
cells; `--max_cost`; `--fill` to fill any pit it cannot breach within `--dist`):
start with a first manual pass on VPU 09, inspect, then fix the chosen values as
documented module constants with rationale (the way `SLOPE_CAP_DEG` is documented
in `compute_dem_derivatives.py`). Too small a `--dist` re-introduces over-connection
via fallback fill; too large over-carves real depressions. No automated tuning
beyond this one pass.

An optional `build_vrt`-style mosaic of the per-VPU tiles into `fdr_breached.vrt`
is *convenience only* — the A/B runs per-VPU and does not require it.

**Test:** `tests/test_compute_breached_fdr.py` (builder-level, matches nearest
existing shared-raster test style). On a small synthetic DEM containing a real
closed depression that a fill would remove: assert the breach output exists, the
FDR contains only valid ESRI-D8 codes `{1,2,4,8,16,32,64,128}` plus nodata, and
the real depression survives (is not fully drained-through) while an artifact
1-cell pit is removed.

### Piece 2 — A/B harness `scripts/ab_drains_to_dprst.py` (analysis tool)

Mirrors the existing `scripts/diagnose_drains_to_dprst.py` precedent: an analysis
script, not a pipeline builder (so no DAG registration / config block — this is
investigation tooling, like `diagnose_*`).

The per-VPU candidate FDRs (`Fdr_hydrodem`, `Fdr_breached`) live on the per-VPU
**Hydrodem grid**; dprst pour-points and `vpu_id` live on the CONUS
**dprst/template grid** (the `fdr.vrt` clip). To compare all three FDRs against
one set of pour-points, each candidate FDR is warped onto the dprst grid using
routing.py's existing streaming `gdal.Warp` alignment (`_align_fdr_to_dprst_grid`,
factored for reuse), then the **existing** `d8_routing.drains_to_dprst_kernel` is
run masked to the single test VPU. The only variable across runs is the FDR;
grid, pour-points, and kernel are identical.

CLI: `--vpu <code> --fdr {production|fill|breach}` plus dprst raster + pour-point /
`vpu_id` paths from the active fabric profile. Writes
`drains_to_dprst_<vpu>_<fdr>.tif` to a scratch/analysis output dir.

Three FDR inputs per test VPU:

| Run | FDR source | Isolates |
|-----|-----------|----------|
| production | `fdr.vrt` (NHDPlus `FdrFac`, stream-burned + filled) | the status quo |
| fill-control | `Fdr_hydrodem_<vpu>.tif` (richdem fill-all on Hydrodem) | DEM/stream-burn vs `FdrFac` |
| breach | `Fdr_breached_<vpu>.tif` (new) | **fill vs breach** — the hypothesis |

The fill-control is what lets the A/B attribute any change to the conditioning
(fill→breach) rather than to the DEM-source / stream-burn difference between
`FdrFac` and the Hydrodem-derived candidates. It is nearly free: already staged.

### Piece 3 — Diagnostics & decision

For each of the 6 runs (2 VPUs × 3 FDRs):

1. **Coverage** — `drains_to_dprst` land-fraction per VPU via the existing
   `diagnose_drains_to_dprst.py`. Expectation: VPU 09 drops fill→breach as over-fill
   connectivity is removed; VPU 16 stays ~flat (endorheic chains survive).
2. **Per-depression contributing area** — distribution of upslope-cell counts per
   pour-point label, computed from `drains` × the labeled dprst regions (already
   produced by the `dprst` builder). Breach should shrink genuine-depression
   catchments toward *local* sizes without zeroing the endorheic terminal playas.
3. **Reference check** — qualitative comparison of the resulting dprst-storage
   spatial pattern against **Driscoll et al. (2020)**, *Spatiotemporal Variability
   of Modeled Watershed-Scale Surface-Depression Storage and Runoff for the CONUS*,
   JAWRA: glaciated plains / PPR retain high dprst, major-river corridors low.

**Decision (written up on #147):** adopt breach for `drains_to_dprst` only if
VPU 09 contributing areas shrink to plausible local catchments **and** VPU 16
endorheic spill chains do not collapse. If both hold, propose a CONUS scale-up as
a follow-up issue/branch.

## Out of scope (YAGNI)

- No CONUS rebuild and no swap of production `fdr.vrt` / fabric profile
  `fdr_raster` — the new FDR is strictly additional.
- No hybrid stream-burn-channels + breach-local FDR (issue option 4) unless
  breach-alone fails a channel-network sanity check.
- No `BreachDepressionsLeastCost` auto-tuning beyond one manual VPU-09 pass.
- No depth/area-thresholded fill or depression-hierarchy variant.

## Risks / watch-for

- **Over-fragmentation:** breach must not collapse pothole-in-a-chain spill
  connectivity or endorheic spill chains (VPU 16 is the guardrail run).
- **Over-connection regression:** too small a breach `--dist` falls back to fill,
  re-introducing the #145 problem; inspect the VPU-09 result before trusting it.
- **Grid alignment:** the warp-to-dprst-grid step is a near-identity NN resample
  (the FDRs already share the projection); verify pour-point registration on the
  warped grid before reading per-label areas.
- **Hydro-flattening is distinct** from depression conditioning (flat lake
  surfaces) — keep them separate; this work touches only the fill→breach axis.

## Files

New:
- `src/gfv2_params/shared_rasters/compute_breached_fdr.py`
- `tests/test_compute_breached_fdr.py`
- `scripts/ab_drains_to_dprst.py`
- (per-VPU outputs) `shared/per_vpu/<vpu>/Hydrodem_breached_<vpu>.tif`,
  `Fdr_breached_<vpu>.tif`

Modified:
- `src/gfv2_params/shared_rasters/__init__.py` — register `compute_breached_fdr`
  in `BUILDERS`.
- `src/gfv2_params/depstor_builders/routing.py` — factor `_align_fdr_to_dprst_grid`
  for reuse by the A/B script (no behavior change to the routing builder).
- Docs: `docs/ARCHITECTURE.md` (note the additional FDR artifact + opt-in step),
  and a short A/B usage note where `diagnose_drains_to_dprst.py` is referenced
  (per the repo's "every code change needs a docs check" rule).
