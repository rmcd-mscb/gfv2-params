# Design — Phase 1 `dprst_depth` builder (Issue #173)

**Date:** 2026-07-11
**Issue:** #173 — derive `dprst_depth_avg` from 3DEP topography; set `op_flow_thres = 1.0`
**Depends on:** the Phase 0 spike (branch `feat/dprst-depth-phase0-spike`, PR #176) — its
diagnostic module `scripts/diagnose/dprst_depth_probe.py` holds the validated per-polygon
machinery this builder productionizes. Phase 1 is gated on that spike's **GO** outcome.

## What the spike settled (inputs to this design)

- **98.7%** of dprst area has 3DEP 1 m coverage; best-available ladder (1 m → 10 m → constant).
- Hydro-flattening is a **minority** (SwampMarsh 21.7%, LakePond 11%, Reservoir 5.4% in the ND PPR),
  detected per-polygon (interior elevation range < 0.01 m). The issue's feared ~89% does not hold.
- **Non-flat polygons** (~85%): raw-DEM `depth_to_spill` V/A = real measured bed depth. ✓
- **Flat polygons** (freeboard ≈ 0): bed unknowable. **Hollister terrain-slope is too weak to use raw**
  (validated: max-depth R²=0.17, end-to-end R²=0.06, systematic +3.2 m over-prediction; depth–area
  regression R²≈0). Empirical max→mean factor median 0.53 (cone 1/3 within IQR).

## Method (per dprst polygon)

1. **Detect flatness** on the polygon's own best-available DEM (interior range < 0.01 m).
   Per-polygon detection makes the method **generalization-safe** — no assumed national flattened rate.
2. **Non-flat** → `dprst_depth = depth_to_spill(dem)` V/A over the polygon interior (measured bed depth).
3. **Flat / degenerate (measured_max ≤ 0)** → **per-ecoregion regional fill**: the median measured V/A
   depth of *non-flat* polygons of the same **FTYPE × EPA ecoregion** (the measurable majority are the
   donors); floor at the NHM constant **49 in**.
   - **Calibrated-Hollister candidate (first-class, built):** for each ecoregion, fit a calibrated
     Hollister (slope→depth coefficient `k` + shape factor) on that ecoregion's *measured non-flat*
     polygons and score it against the regional-median null by **cross-validated skill**. Where it beats
     the null, it replaces the median for that ecoregion's flat polygons; where it doesn't, the median
     stands. The fixed nodata-ring bug lifted Hollister to a real r≈0.42 correlation with a calibratable
     +3.2 m bias, so there is genuine signal to exploit. Untestable on single-ecoregion ND spike data —
     **the per-ecoregion keep/drop is decided empirically on the Oregon run** (multiple ecoregions). The
     median is the safe null it must beat, per ecoregion; a global on/off flag is a fallback only if the
     per-ecoregion machinery is deferred.
4. **Provenance** per polygon: `resolution` (1 m / 10 m) × `method` (measured / regional_fill /
   calibrated_hollister / constant_floor).

Per-HRU `dprst_depth_avg` = volume-weighted mean over the HRU's dprst cells = `Σ V_p / Σ A_p`.

## Architecture

Reuses the depstor pipeline pattern (builder → `{fabric}/depstor_rasters/*.tif` → gdptools
area-weighted zonal → `{fabric}/params/merged/*.csv`). **Only the per-polygon 1 m compute is new**;
everything downstream is existing infrastructure.

### 0. Ecoregion layer — staged once as a shared, reusable input
EPA Level III/IV Ecoregions of the conterminous US. **Not fabric-specific and reusable by other
parameterizations**, so it is staged like the other shared inputs: a new `src/gfv2_params/download/epa_ecoregions.py`
module (matching the `download/` pattern — `nhd_*`, `mrlc_impervious`, `copernicus_dem`) that pulls the
layer and writes it to a shared location under `{data_root}/input/ecoregions/` (e.g. `us_eco_l3l4.gpkg`,
EPSG:5070), path recorded in the shared config / base profile (not per-fabric). The dprst polygon →
ecoregion assignment is a centroid spatial join done once during the fill stage.
- **Source-reachability caveat:** only AWS S3 over HTTPS is confirmed reachable from this HPC (PROJ CDN,
  `gh`, and `/vsis3/` GeoPackage all had issues). The EPA download host must be tested for reachability
  the way S3 was; if the EPA server is DPI-blocked, resolve a reachable mirror (ScienceBase / an S3 copy)
  or stage manually and point the config at it. The staging step must fail loud, not silently skip.

### 1. Config block — `configs/depstor/depstor_rasters.yml` (+ `depstor_params.yml` entry)
3DEP paths (`{data_root}` placeholders / `/vsicurl/` S3 templates), WESM index path, rim buffer,
flatness tol, shared ecoregion layer path + join field, regional-fill floor (49 in), calibrated-Hollister
controls. Read via `require_config_key` against the active fabric profile. No hardcoded paths.

### 2. ⭐ Per-tile compute (SLURM array) — the new heavy piece
Target: **≤ 5 hr wall-clock** for CONUS (~286k polygons, ~250–500 core-hours raw).
- **Fan-out unit = the 1 m processing tile**, not the polygon. Group dprst polygons by covering 1 m
  tile; each array task reads its tiles **once** and processes all polygons in them (potholes cluster
  → collapses the redundant-read cost the diagnostic pays per-polygon today).
- **Resolve covering tiles from the WESM index up front** — eliminate the per-polygon `/vsicurl` 404
  existence-probe (~5 s/miss) the spike flagged.
- Optionally stage the batch's tiles to node-local scratch once per task.
- Output: per-batch parquet `{COMID, dprst_depth_m, measured_max_m, flat, ecoregion, ftype, resolution, method}`.
- Array sized to ~100–200 tasks → 250–500 core-hr / ~100 concurrent ≈ 2.5–5 hr.
- Follows the CONUS streaming rules in `CLAUDE.md`: windowed reads only, never a 1 m lattice.

### 3. Regional-fill + burn
Barrier after the array: pool measured non-flat depths by FTYPE × ecoregion → fill flat/degenerate
polygons (optional calibrated-Hollister where it earns it) → burn each polygon's final V/A depth into a
30 m `{fabric}/depstor_rasters/dprst_depth.tif` (dprst-masked, aligned to the template lattice). Burning
per-polygon V/A means the downstream **area-weighted** zonal mean reproduces the exact per-HRU `ΣV/ΣA`.

### 4. Per-HRU aggregation — reuse existing gdptools zonal
Add a `depstor_params.yml` entry pointing at `dprst_depth.tif` (mean aggregation, not the binary
count). Output `{fabric}/params/merged/nhm_dprst_depth_avg_params.csv` — schema
`{id_feature}, dprst_depth_avg` (inches) + a provenance column (source × method counts per HRU).
HRUs with `dprst_frac == 0` get the fill default; never NaN.

### 5. Constant params
Emit `op_flow_thres = 1.0` wherever the constant dprst param set is written. Record the dependency:
if the constant-fallback share balloons under CONUS generalization, revisit.

### 6. Convention deliverables
Ecoregion staging module (§0) + builder module + DAG registration + config block +
**`tests/test_dprst_depth.py`** (synthetic bowl → known V/A; flat polygon → regional-median fill;
no-coverage → 10 m → constant ladder; provenance counts; a calibrated-Hollister-beats-null unit case).

## Sequencing

1. **Build + prove on Oregon** — humid (Willamette/coast) + arid (high-desert closed basins) = multiple
   ecoregions in one already-processed fabric; exercises the 1 m → 10 m best-available ladder; and is the
   test bed for whether calibrated-Hollister beats the regional-median null per ecoregion.
2. Compare Oregon `dprst_depth_avg` distribution to the prior on-disk param set + the NHM calibrated
   distribution (median 49 in, order-of-magnitude check).
3. **CONUS run** (SLURM array, ≤ 5 hr) once Oregon validates.

## Open items carried from the spike

- `lake_max_depth` nodata-ring guard — **FIXED** (commit 9cf0fe3) before the calibrated-Hollister evaluation.
- Ecoregion layer must be staged (new spatial input, not currently in the pipeline).
- The regional-fill donor pool needs enough non-flat polygons per FTYPE × ecoregion; where sparse, fall
  back to a coarser grouping (ecoregion-only, then FTYPE-only, then the constant floor) — log the fallback.

## Acceptance criteria

- CONUS `dprst_depth_avg` physically plausible, same order of magnitude as NHM (10–300 in, median ~49).
- No HRU with `dprst_frac > 0` has `dprst_depth_avg ≤ 0`; HRUs with `dprst_frac == 0` are never NaN.
- Provenance reports source × method counts; the constant-floor share is reported, not silently absorbed.
- CONUS run completes in **≤ 5 hr** wall-clock.
- Tests cover: bowl → V/A; flat → regional fill; no-coverage → fallback ladder.

## Non-goals

- `dprst_flow_coef` / `dprst_seep_rate_open` (already emitted by `ssflux.py`) and `smidx_exp` (separate issues).
- Changing the dprst classifier (`wbody_connectivity` → `dprst`) — consumed unchanged.
