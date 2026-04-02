# Multi-Source LULC Parameterization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add crosswalk-mediated LULC parameterization supporting FORE-SCE, NLCD, and NALCMS raster sources. Compute 7 per-HRU parameters (cov_type, srain/wrain/snow_intcp, covden_sum, covden_win, retention) via the existing spatial batching pipeline.

**Architecture:** A new `lulc.py` library module handles crosswalk loading, class-percentage conversion, cover-type decision tree, and parameter computation. A `create_lulc_params.py` batch script runs categorical + continuous zonal stats, calls the library, and writes per-batch CSVs. Retention is raster-derived (FORE-SCE) or crosswalk-derived (NLCD/NALCMS) depending on whether `keep_raster` is configured.

**Tech Stack:** Python 3.12, pandas, numpy, rasterio, rioxarray, gdptools (ZonalGen, UserTiffData, exactextract), pyyaml, pytest

**Spec:** `docs/superpowers/specs/2026-04-02-lulc-parameterization-design.md`

---

## File Map

### New files
| File | Responsibility |
|---|---|
| `src/gfv2_params/lulc.py` | Core library: `load_crosswalk()`, `class_percentages_from_histogram()`, `assign_cov_type()`, `compute_interception()`, `compute_covden()`, `compute_retention()` |
| `tests/test_lulc.py` | 21 unit tests for lulc module |
| `scripts/create_lulc_params.py` | Batch script: categorical + continuous zonal stats → crosswalk params → CSV |
| `scripts/build_lulc_rasters.py` | Pre-computation: resample CNPY/keep to LULC grid, compute radtrn |
| `configs/lulc_foresce_param.yml` | FORE-SCE step config (with keep_raster, scenario, year) |
| `configs/lulc_nlcd_param.yml` | NLCD step config (no keep_raster) |
| `configs/lulc_nalcms_param.yml` | NALCMS step config (no keep_raster) |
| `crosswalks/foresce_nhm.csv` | FORE-SCE crosswalk (21 classes) |
| `crosswalks/nlcd_nhm.csv` | NLCD crosswalk (20 classes) |
| `crosswalks/nalcms_nhm.csv` | NALCMS crosswalk (19 classes) |
| `slurm_batch/create_lulc_params.batch` | SLURM template — FORE-SCE |
| `slurm_batch/create_lulc_nlcd_params.batch` | SLURM template — NLCD |
| `slurm_batch/create_lulc_nalcms_params.batch` | SLURM template — NALCMS |

### Modified files
| File | What changes |
|---|---|
| `src/gfv2_params/config.py` | Add self-referencing placeholder resolution from step config scalars |
| `src/gfv2_params/raster_ops.py` | Add `compute_radtrn()` block-processed function |
| `tests/test_config.py` | Add `test_load_config_step_self_reference` |
| `slurm_batch/merge_output_params.batch` | Add LULC merge step |
| `slurm_batch/RUNME.md` | Add Stage 2c (LULC rasters), LULC to Stage 4, NLCD/NALCMS alternatives, mapping table |

---

## Implementation Tasks

### Phase 1: Foundation (testable locally, no rasters needed)

- [x] **Task 1: Create FORE-SCE crosswalk CSV**
  - File: `crosswalks/foresce_nhm.csv`
  - 21 FORE-SCE classes with NHM cover type, interception coefficients, nhm_covden_win, evergreen_retention=-1
  - Values from NHM documentation and notebook's LU_DIC mapping

- [x] **Task 2: Implement lulc.py module**
  - File: `src/gfv2_params/lulc.py`
  - Functions: `load_crosswalk()`, `class_percentages_from_histogram()`, `assign_cov_type()`, `compute_interception()`, `compute_covden()`, `compute_retention()`
  - `assign_cov_type` implements decision tree with thresholds: bare>=90%, tree>=20%, shrub>=20%, shrub+tree>=35%, grass>=50%, else max
  - `compute_retention` synthesises per-HRU retention from crosswalk when no keep raster

- [x] **Task 3: Write unit tests**
  - File: `tests/test_lulc.py`
  - 21 tests: 3 crosswalk, 2 histogram, 8 cov_type decision tree, 2 interception, 2 covden, 4 retention
  - All use synthetic data (no rasters needed)

### Phase 2: Config + Pre-computation

- [x] **Task 4: Enhance config.py placeholder resolution**
  - File: `src/gfv2_params/config.py`
  - Step config scalar values (str, int, float not containing `{`) become available as `{placeholders}`
  - Enables `{lulc_source}`, `{scenario}`, `{year}` in LULC configs
  - File: `tests/test_config.py` — add `test_load_config_step_self_reference`

- [x] **Task 5: Create FORE-SCE step config**
  - File: `configs/lulc_foresce_param.yml`
  - Keys: source_type, lulc_source, scenario, year, source_raster, canopy_raster, keep_raster, radtrn_raster, crosswalk_file, batch_dir, target_layer, id_feature, output_dir, merged_file, categorical

- [x] **Task 6: Add compute_radtrn to raster_ops**
  - File: `src/gfv2_params/raster_ops.py`
  - Block-processed (2048x2048): `radtrn = cnpy * keep / 100 where lulc >= tree_threshold, else 0`

- [x] **Task 7: Create build_lulc_rasters.py**
  - File: `scripts/build_lulc_rasters.py`
  - Resample CNPY and keep to LULC grid, compute radtrn
  - Skips radtrn when no keep_raster configured (NLCD/NALCMS)

### Phase 3: Batch Script + SLURM

- [x] **Task 8: Create create_lulc_params.py**
  - File: `scripts/create_lulc_params.py`
  - Two zonal stats passes: categorical (LULC histogram) + continuous (canopy mean)
  - Retention: raster-based when keep_raster configured, crosswalk-based otherwise
  - Output: 7-column CSV per batch

- [x] **Task 9: Create SLURM batch and update docs**
  - Files: `slurm_batch/create_lulc_params.batch`, `slurm_batch/merge_output_params.batch`, `slurm_batch/RUNME.md`

### Phase 4: NLCD + NALCMS Sources

- [x] **Task 10: Create NLCD crosswalk and config**
  - Files: `crosswalks/nlcd_nhm.csv` (20 classes), `configs/lulc_nlcd_param.yml`
  - No keep_raster — uses crosswalk evergreen_retention
  - NLCD classes 11-95 mapped to NHM cov_types with retention values

- [x] **Task 11: Create NALCMS crosswalk and config**
  - Files: `crosswalks/nalcms_nhm.csv` (19 classes), `configs/lulc_nalcms_param.yml`
  - No keep_raster — uses crosswalk evergreen_retention
  - NALCMS classes 1-19 mapped to NHM cov_types with retention values

- [x] **Task 12: SLURM batch files for NLCD/NALCMS**
  - Files: `slurm_batch/create_lulc_nlcd_params.batch`, `slurm_batch/create_lulc_nalcms_params.batch`
  - Update RUNME.md mapping table

### Phase 5: Retention implementation

- [x] **Task 13: Implement compute_retention() in lulc.py**
  - Weighted average: `retention = sum(perc/100 * evergreen_retention)` for non-bare classes
  - Bare/developed classes contribute zero regardless of crosswalk value

- [x] **Task 14: Update create_lulc_params.py for retention**
  - When keep_raster configured: zonal mean of keep raster → retention (normalised 0-1)
  - When no keep_raster: call compute_retention() with crosswalk values
  - Retention column included in all output CSVs regardless of source

- [x] **Task 15: Retention unit tests**
  - 4 tests: mixed classes, all evergreen, bare zeroing, mixed forest composition

---

## Verification

1. `pytest tests/test_lulc.py tests/test_config.py` — 22 tests pass (21 lulc + 1 config)
2. `pytest tests/` — full suite 82 tests pass
3. Local single-batch test: `python scripts/create_lulc_params.py --config configs/lulc_foresce_param.yml --base_config configs/base_config.yml --batch_id 0`
4. Verify output CSV has 8 columns: nat_hru_id, cov_type, srain_intcp, wrain_intcp, snow_intcp, covden_sum, covden_win, retention
5. Merge test: `python scripts/merge_params.py --config configs/lulc_foresce_param.yml --base_config configs/base_config.yml`

## Open Items

- [ ] Verify FORE-SCE crosswalk coefficient values against original `FORSE_CW.csv` (not available locally)
- [ ] Validate NLCD/NALCMS interception coefficients against published literature
- [ ] Determine canopy raster source for NALCMS (NLCD Tree Canopy, MODIS VCF, or other)
- [ ] HPC integration testing with real raster data on Hovenweep
- [ ] Assess whether 32G SLURM memory is sufficient for two zonal stats passes per batch
