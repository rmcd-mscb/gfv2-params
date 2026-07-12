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

## Post-fix re-validation (2026-07-11, commit 949fc41)

The three fixes were applied and Oregon re-run (fill→burn→aggregate on the same
compute parquet):

| metric | before | after fix |
|---|---|---|
| max `dprst_depth_avg` | 4,224 in (352 ft) | **300.0 in** (NHM cap) |
| `constant_floor` polygons | 98 | **0** (now regional-filled) |
| provenance parquet columns | 3 | 9 (adds resolution/ftype/ecoregion/measured_max/hollister_max) |
| NaN / ≤ 0 | 0 / 0 | 0 / 0 |

Resolution split (per polygon): **74% 1 m / 26% 10 m**. Post-fix method mix:
measured 2,784 · regional_fill 255 · **calibrated_hollister 71** · measured_capped 31.

Two minor residuals (both handled; output valid, max = 300, no NaN):
- The finalize backstop still clamped **11 HRUs** > 300 in — the per-polygon cap
  did not fully propagate to the burned raster for a few HRUs. Output is correct
  (clamped); the "should be impossible" path warrants a quick look before/at CONUS.
- `calibrated_hollister` wins rose from ~1 to 71 polygons after the cap changed
  the donor depths feeding the per-ecoregion fits — a positive for the calibration
  idea, but confirm on CONUS the wins are sensible fits, not shallow-value overfits.

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

## CONUS run (2026-07-12, contract-correct, all PR#177 fixes)

Full CONUS `gfv2` run completed (3 pothole-belt long-pole batches took 5–8 h at the
bumped 12 h wall limit; no OOM — the giant-window guard held; 147/150 batches ran
in the first pass). Output: `gfv2/params/merged/nhm_dprst_depth_avg_params.csv`.

- **361,471 HRUs; 0 NaN, 0 ≤ 0, max = 300.0 in (cap held).** dprst-bearing HRUs
  (n = 78,620): **median 10.7 in**, p90 53.7 in. Shallower than the NHM *calibrated*
  median (49 in) but within the 10–300 range and same order of magnitude — expected,
  since ours is *measured* topographic depth (shallow potholes/wetlands) and theirs is
  fitted to a runoff proxy.
- **Method mix (per polygon, n = 279,391):** measured 249,278 (89.2%), regional_fill
  19,975, **calibrated_hollister 9,328 (3.3%)**, measured_capped 810. Calibrated-Hollister
  won its per-ecoregion CV comparison in ~1,943 HRUs across CONUS — the calibration idea
  earns its place where the data supports it, at national scale.
- **PR #177 fixes confirmed at scale:** `dprst_depth ⊆ dprst_binary` (burn masked); the
  300 in cap fired on 810 polygons; the giant-window guard retagged 30; the narrowed
  `except` produced **zero `n_compute_error`**; the parquet dedup dropped 83 overlapping
  COMIDs; 1,611 unassigned-ecoregion polygons logged.

### Follow-up surfaced by the new achieved-resolution gate
The resolution-logging fix (PR #177) fired: **achieved 1 m coverage 58.6% (163,665/279,391)
vs tagged-1 m 98.7%.** This is *not* a regression — the WESM 1 m footprint index is
convex-hull-simplified (a documented upper bound; see the spike's coverage caveat), so
many "1 m-tagged" polygons legitimately fall back to 10 m at read time where no actual
1 m tile exists. 58.6% is closer to the *true* 1 m coverage. Two cheap follow-ups: (a)
relax the gate's 20 pp threshold or compare against a realistic (non-upper-bound)
expectation so it doesn't false-alarm; (b) optionally tighten the WESM footprint (drop the
convex-hull) so `best_topo` tagging matches achievable coverage. The 10 m fallback depths
are valid; this only means ~40% of polygons are coarser-resolved than the optimistic tag implied.

### Oregon re-run on the final builder (2026-07-12, all PR#177 fixes)
Re-ran Oregon fully on the fixed builder (guard + `burn ⊆ dprst_binary`) so the validation
artifact matches the shipped code. Result: **max 300 in, 0 NaN**; per-polygon method mix and
1 m/10 m split are **identical** to the post-fix run (the giant-window guard is a **no-op for
Oregon** — no polygon exceeds the ~14 km / 200 M-cell threshold). The only change is from the
new `dprst_binary` burn-mask: the per-HRU mean is now taken over exactly the dprst cells,
nudging the **dprst-bearing median 14.5 → 13.9 in** (p90 83.5 → 80.3) — small and in the
expected direction. Oregon is now consistent with the CONUS product.
