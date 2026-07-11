# Oregon validation — `dprst_depth` builder (Issue #173 Phase 1)

**Date:** 2026-07-11
**Run:** SLURM DAG (jobs 1657949–1657953) on the `oregon` fabric, ~17 min wall-clock.
**Output:** `oregon/params/merged/nhm_dprst_depth_avg_params.csv` (16,814 HRUs).

## Verdict: **GO for CONUS, after one physical cap** (details below)

The full pipeline ran end-to-end on real 3DEP data — plan → per-tile compute (8
batches, 8–14 min each, concurrent) → fill → burn → per-HRU exactextract mean →
finalize — and produced a valid, physically-plausible parameter set. The method
behaves as the spike predicted. One outlier-tail issue must be capped before CONUS.

## What worked

- **End-to-end, within budget.** Oregon (3,141 dprst polygons, clipped from the
  321,589 CONUS set) finished in ~17 min. The ≤5 hr CONUS target holds
  (150 tile-batches → 1.7–3.3 hr projected).
- **Valid output.** 16,814 HRUs, **0 NaN, 0 ≤ 0**. HRUs with `dprst_frac == 0`
  (15,224; 90.5%) correctly receive the NHM 49 in floor (`no_dprst_cells`).
- **Method mix as designed** (per-polygon, n = 3,141):

  | method | polygons | note |
  |---|---|---|
  | measured (raw-DEM V/A) | 2,815 (89.6%) | the non-flat majority |
  | regional_fill (per-ecoregion median) | 227 (7.2%) | hydro-flat minority |
  | constant_floor | 98 (3.1%) | uncomputable → floored (see Risk 2) |
  | **calibrated_hollister** | 1 | **won its ecoregion×FTYPE group on CV skill** |

  The single calibrated-Hollister win confirms the "keep it only where it beats
  the median null" design earns its place — the candidate is neither forced on nor
  categorically excluded; the data decided, per ecoregion.
- **Physically plausible centre.** Among dprst-bearing HRUs (n = 1,526):
  **median 14.5 in**, p90 83.5 in. Measured polygons: **median 14.2 in**. Same
  order of magnitude as the NHM calibrated median (49 in) — shallower, as expected
  for Oregon's many small ponds vs. a CONUS-wide calibrated distribution.
- **1 m reads work.** Pre-flight smoke: ~92% of predicted 1 m tiles return HTTP
  200 (tile naming correct); the ~8% 404s are the known convex-hull
  over-inclusion and fall back to 10 m cleanly.

## Risk 1 (must fix before CONUS) — unphysical outlier tail

Measured depths have a heavy right tail:

| threshold | polygons | % of measured |
|---|---|---|
| > 120 in (10 ft) | 170 | 6.0% |
| > 240 in (20 ft) | 53 | 1.9% |
| > 600 in (50 ft) | 4 | 0.14% |
| > 1200 in (100 ft) | 1 | 0.04% |

Percentiles (in): p50 14.2 · p95 131 · p99 307 · p99.9 684 · **max 4,225 (352 ft)**.

These are `depth_to_spill` (filled − raw) artifacts on polygons where the DEM fill
runs to a **high pour point** — reservoirs behind dams, depressions abutting steep
terrain, or valley-spanning waterbodies — not physical surface-ponding depths.
352 ft of depression storage is not a valid PRMS `dprst_depth_avg`.

**Fix:** cap measured `dprst_depth_avg` at the **NHM calibrated maximum, 300 in**
(the issue's own acceptance range is "10–300 in"). Apply the cap in the fill/finalize
stage, record capped polygons in provenance (`measured_capped`), and report the
capped share. This is a documented, reference-anchored bound, not a tuning knob.

## Risk 2 (investigate) — 98 polygons floored that "shouldn't" be

The build logged: *`fill_flat: 98 rows still NaN/non-positive after the fallback
ladder — forcing to the 1.2446 m floor (this should not happen; investigate
upstream)`*. These 98 (3.1%) are non-flat polygons whose `depth_to_spill` returned
NaN — almost certainly **read failures** (both 1 m 404 *and* 10 m unavailable/nodata)
or degenerate windows (`measured_max ≤ 0`) — and the per-ecoregion/FTYPE median
ladder did not fill them (likely because their group had no measured donors).

The output is safe (floored, never NaN), but the ladder should catch these before
the defensive net. **Action:** confirm the read-failure rate, and make the fallback
ladder fill a NaN-measured non-flat polygon via its ecoregion/FTYPE median (not jump
to the defensive floor). 4 polygons also fell outside every ecoregion (`unassigned`).

## Risk 3 (diagnostics) — provenance parquet too thin

`dprst_depth_polygons.parquet` persists only `[COMID, method, dprst_depth_m]`, so
the 1 m/10 m resolution split, FTYPE, ecoregion, and `measured_max` could not be
analysed at the HRU level. **Action:** persist `resolution`, `FTYPE`, `ecoregion`,
`measured_max_m`, `hollister_max_m` in the companion parquet for CONUS diagnostics
(and richer provenance).

## Recommendation

1. **Add the 300 in physical cap** (Risk 1) — required before CONUS; unphysical
   depths would otherwise propagate to the CONUS product.
2. Investigate the 98-polygon floor + enrich the provenance parquet (Risks 2, 3) —
   lower priority; the run is valid without them, but both improve trust at CONUS scale.
3. Then run CONUS (`submit_dprst_depth.sh <gfv2/batches> gfv2 … 150`) and compare
   the CONUS distribution to the NHM calibrated one (median ~49 in, range 10–300).

The calibrated-Hollister evaluation across Oregon's ecoregions is inconclusive on
its own (1 win in a small fabric); CONUS's many ecoregions are where it gets a real
test — this run confirms the *mechanism* fires correctly.
