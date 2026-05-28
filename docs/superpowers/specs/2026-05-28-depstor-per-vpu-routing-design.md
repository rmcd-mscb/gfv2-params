# Per-VPU tiled WBT routing for the depstor pipeline

**Status:** Design — 2026-05-28

## Problem

The depstor `routing` step runs WhiteboxTools (WBT) `Watershed` on the full-CONUS
D8 flow-direction raster (FDR) plus the depression-storage pour-points to produce
`drains_to_dprst.tif` — the upslope contributing-area mask that feeds
`sro_to_dprst_perv` and `sro_to_dprst_imperv` (2 of the 6 PRMS depstor
parameters).

WBT loads every raster fully into RAM as f64 regardless of on-disk dtype. At CONUS
scale the template is 153830×109901 ≈ 16.9 B cells, so the d8 pointer + pour-points
+ output ≈ 3 × 135 GB ≈ **405 GB** plus the Watershed working set. It OOM-killed at
`--mem=384G`, and that is above the cluster's largest node (`RealMemory` 515246 MB
≈ 503 GB usable; every partition — `cpu`/`gpu`/`viz` — is the same spec, no
bigmem node). So whole-CONUS WBT routing **cannot run on this hardware**.

Two earlier CONUS OOMs in this same step were already fixed and are out of scope
here: the in-memory `rioxarray.reproject_match` FDR alignment (now streaming
`gdal.Warp`) and the redundant int32 copy in `clump_regions` (now `copy=False`).
This spec addresses the remaining WBT-`Watershed` OOM.

## Goal

Make `routing` complete at CONUS scale within roughly the default `--mem` (≤~192 G)
by partitioning the WBT `Watershed` computation per VPU, with **no change to the
`drains_to_dprst.tif` semantics** for any fabric that already fits.

## Key decision — independent per-VPU routing (no halo)

Route each VPU's cells in isolation; contributing areas truncate at VPU
boundaries. This is hydrologically correct because NHDPlus VPU boundaries follow
**drainage divides**: cells across a VPU boundary drain *away* over the divide and
so do not contribute to this side's depressions — truncating there drops exactly
the cells that legitimately don't contribute. Confirmed with the hydrologist
(2026-05-28). It also matches how the pipeline already treats VPUs as independent
(per-VPU TWI masks, independent per-VPU segment networks).

## Partition source — `vpu_id.tif`

`vpu_id.tif` (from the `vpu_id` step) labels every template cell with its HRU's
home VPU code (1..18; 0 = nodata) on the *exact* dprst/template grid. It is the
partition, and the behaviour adapts to how the fabric declares VPUs:

- **Per-HRU `vpu` attribute** (e.g. `gfv2`): each cell → its home VPU; routing
  tiles per VPU. Correct **and** memory-bounded.
- **Profile `vpu:` scalar** (e.g. `oregon` = `"17"`): `vpu_id` is a constant fill
  → one tile → the whole fabric routes as a single unit (no truncation, no
  tiling).

Because cell→VPU is a partition (each cell belongs to exactly one VPU), the
per-VPU outputs mosaic with no overlap reconciliation.

`vpu_id` **reads** the per-HRU `vpu` attribute that is already on the fabric — it
does **not** derive or add VPU membership. Verified on `gfv2_nhru_merged.gpkg`: a
`vpu` String column with the 21 detailed labels (`01`..`18`, incl. `03N/03S/03W`,
`10L/10U`), inherited from the `NHM_<vpu>_draft.gpkg` sources and carried through
the merge. `vpu_to_code` then collapses the detailed labels to **raster VPU codes
1..18** (`03N/03S/03W → 3`, `10L/10U → 10`), so routing produces **≤18 tiles** and
a VPU's sub-regions route together as one drainage unit (no false truncation
between sub-regions).

### Reusing an existing attribute (not a new requirement)

Routing's partition comes from the same `vpu` source the pipeline **already**
depends on: `carea_map`'s percentile-`vpu` mode burns the per-HRU `vpu` to key its
TWI thresholds, so multi-VPU fabrics already supply it. Both current fabrics carry
a `vpu` column (`gfv2_nhru_merged.gpkg`, and Oregon's `model_layers 9.gpkg`), and
single-VPU fabrics declare a `vpu:` scalar (Oregon also sets `"17"`). So this
refactor adds **no new data requirement** — it reuses what's already there.

The only failure mode is a fabric that is multi-VPU yet carries *neither* a `vpu`
column nor a profile `vpu:` scalar — `resolve_vpu_source` already raises loudly in
that case. (A large multi-VPU fabric mis-declared with a single scalar would route
whole and risk OOM — a memory failure, not a wrong answer.)

Computing the VPU assignment geometrically (dissolve the per-VPU draft footprints
→ spatial-join) is possible but **deferred**: it adds a new artifact + step and
could diverge from the authoritative NHM home-VPU assignment that `carea_map` also
relies on. It belongs as a future enhancement to the `vpu_id` step (which would
benefit carea_map too), not bolted onto this routing refactor.

## Changes

1. **DAG reorder** (`STEP_ORDER` in `src/gfv2_params/depstor_builders/__init__.py`):
   move `vpu_id` before `routing`. Safe — `vpu_id` depends only on the template +
   HRU fabric, nothing from `routing` or later. Reorder the step list in
   `configs/depstor/depstor_rasters.yml` to match (cosmetic; `STEP_ORDER` is
   authoritative).
2. **`routing.build` refactor** (`src/gfv2_params/depstor_builders/routing.py`):
   - Keep the streaming `gdal.Warp` → `fdr_aligned.tif` (CONUS, low memory) —
     unchanged.
   - `ctx.require("vpu_id")`; enumerate the VPU codes present in `vpu_id.tif`
     (exclude nodata 0).
   - For each code: compute the bbox of `vpu_id == code`; within that window write
     a masked FDR (`fdr_aligned` where `vpu_id == code`, else nodata 255) and
     pour-points (`dprst == 1 & vpu_id == code` → 1, else 0); run WBT `Watershed`
     on the window; take labeled → 1; write into the CONUS `drains_to_dprst.tif`
     **only for `vpu_id == code` cells**.
   - Land-mask the final output (unchanged behaviour).
   - Clean per-VPU temp tiles in a `finally` block (as the current code does for
     its intermediates).
3. **New pure helper** in `src/gfv2_params/depstor.py` for the tile bbox / mask /
   per-VPU mosaic arithmetic (no WBT subprocess) so the partition+mosaic logic is
   unit-testable in isolation.
4. `_watershed_to_binary`: unchanged logic; applied per-window during the loop.

## Components & data flow

```
fdr_aligned.tif (gdal.Warp, CONUS)         vpu_id.tif (per-cell VPU code)
dprst_binary.tif (pour candidates)                 │
        │                                          │
        └──────────────┬───────────────────────────┘
                       ▼   for each VPU code c:
        window = bbox(vpu_id == c)
        fdr_c  = mask(fdr_aligned[window], vpu_id[window]==c, nodata=255)
        pour_c = (dprst[window]==1) & (vpu_id[window]==c)
        WBT Watershed(fdr_c, pour_c) -> labels_c
        drains[window][vpu_id==c] = (labels_c is labeled)
                       ▼
        land-mask -> drains_to_dprst.tif  (CONUS, identical schema to today)
```

Isolation: the pure helper owns bbox/mask/mosaic and is unit-tested; `routing.build`
owns the loop + WBT subprocess per tile; the downstream contract
(`drains_to_dprst.tif`) is unchanged, so `drains_perv`/`drains_imperv` and the
params need no changes.

## Memory

Per-VPU window holds f64 arrays sized to one VPU's bbox — the largest is ~2–4 B
cells ≈ **50–100 GB** WBT working set (vs 405 GB CONUS). Fits comfortably under
192 G; right-size the batch `--mem` default after one clean run.

## Testing

- **Unit test** (pure helper, no WBT): synthetic small grid with two VPU codes.
  Assert (a) per-VPU isolation — a cell that would drain across the internal VPU
  boundary is NOT marked, and (b) clean mosaic — each cell assigned to exactly one
  VPU, no double-count or cross-VPU overwrite.
- **Regression**: run the refactored routing on `oregon` (single VPU) and diff
  `drains_to_dprst.tif` against its existing output — must be identical (one tile
  == the non-tiled path). This is the concrete correctness anchor.
- Multi-tile mosaic has no full-scale non-tiled baseline (CONUS cannot run
  non-tiled), so it is covered by the unit test plus the divide-based correctness
  argument above.

## Non-goals

- Cross-VPU halos / exact cross-boundary contributing areas (accepted truncation).
- SLURM-array parallelism across VPUs — a sequential loop inside the single
  `routing` job is the scope; revisit only if wall time becomes a problem.
- Any change to other depstor steps or to downstream parameters.

## Risks

- **vpu_id boundary accuracy:** edge cells depend on the HRU `vpu` attribute; the
  effect is bounded and is the same edge approximation already accepted.
- **Wall time:** per-VPU WBT runs are sequential, so total time is the sum over
  VPUs (bounded by the largest VPU). Acceptable; parallelism deferred.
- **Misdeclared fabric:** a large multi-VPU fabric declared with a single `vpu:`
  scalar routes whole and can OOM — surfaced by the loud `resolve_vpu_source`
  error / reuse note above; fix is to supply the per-HRU `vpu` attribute (which
  carea_map needs anyway).
