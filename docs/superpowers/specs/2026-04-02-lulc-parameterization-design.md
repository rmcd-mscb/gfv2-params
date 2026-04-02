# Multi-Source LULC Parameterization Design

## Goal

Add land use / land cover (LULC) parameterization to the gfv2-params pipeline, producing seven per-HRU parameters from any of three LULC raster sources (FORE-SCE, NLCD, NALCMS) via a crosswalk-mediated, source-agnostic architecture.

## Motivation

The existing LULC notebook (`/home/rmcd/projects/usgs/LULC`) computes PRMS/NHM cover-type and interception parameters from FORE-SCE rasters but is hard-coded to a single source, runs outside the batch pipeline, and requires pre-computed per-HRU class percentages. Integrating this into gfv2-params gives us:

- **Multi-source support**: Swap the crosswalk CSV + config to switch between FORE-SCE (with future projections), NLCD (observational snapshots), or NALCMS (North American coverage).
- **On-the-fly class percentages**: Compute per-HRU LULC class histograms from the raster at batch time, eliminating the need for pre-staged CSVs.
- **Batch-scale processing**: Same KD-tree spatial batching and SLURM array pattern as elevation/soils/ssflux.
- **Scenario and year support**: Config fields allow processing multiple time periods or climate scenarios.

## Output Parameters

Each batch produces a CSV with one row per HRU and these columns:

| Column | Type | Description |
|--------|------|-------------|
| `cov_type` | int | Dominant NHM cover type: 0=bare/developed, 1=grass, 2=shrub, 3=forest |
| `srain_intcp` | float | Summer rain interception (weighted sum across LULC classes) |
| `wrain_intcp` | float | Winter rain interception |
| `snow_intcp` | float | Snow interception |
| `covden_sum` | float | Summer canopy density (weighted canopy % across non-bare classes) |
| `covden_win` | float | Winter canopy density (covden_sum adjusted by deciduous fraction) |
| `retention` | float | Evergreen canopy retention fraction (0=fully deciduous, 1=fully evergreen) |

## Architecture

### Crosswalk-Mediated Design

The central abstraction is a **crosswalk CSV** that maps each LULC raster class code to NHM hydrologic parameters. The processing logic is source-agnostic — it reads class codes from the raster histogram and looks up coefficients from the crosswalk. Switching between FORE-SCE, NLCD, and NALCMS requires only a different crosswalk file and config.

### Universal Crosswalk Schema

All three LULC sources use the same CSV schema:

```
lu_code,lu_desc,nhm_cov_type,srain_intcp,wrain_intcp,snow_intcp,nhm_covden_win,evergreen_retention
```

| Column | Type | Description |
|--------|------|-------------|
| `lu_code` | int | Raster class code (0-20 for FORE-SCE, 11-95 for NLCD, 1-19 for NALCMS) |
| `lu_desc` | str | Human-readable class name |
| `nhm_cov_type` | int | NHM cover type: 0=bare, 1=grass, 2=shrub, 3=forest |
| `srain_intcp` | float | Summer rain interception coefficient |
| `wrain_intcp` | float | Winter rain interception coefficient |
| `snow_intcp` | float | Snow interception coefficient |
| `nhm_covden_win` | float | Winter cover density reduction factor (0-1). Represents deciduous fraction: 0.0 = fully evergreen (no winter canopy loss), 0.5 = deciduous (50% loss), 1.0 = fully deciduous (complete loss). Used as: `covden_win = covden_sum * (1 - nhm_covden_win)` |
| `evergreen_retention` | float | Per-class evergreen retention fraction (0-1). Used to synthesise a per-HRU retention metric when no `keep` raster is available. Set to -1 for FORE-SCE (which uses the raster instead). |

### Retention: Two Code Paths

The `retention` output represents how much canopy persists through winter. It is computed differently depending on whether a `keep` raster is configured:

**Path A — Raster-based (FORE-SCE):**
- Config has `keep_raster` pointing to the FORE-SCE evergreen retention raster (0-100 scale).
- Zonal mean of the keep raster is computed per HRU, then normalised to 0-1.
- The crosswalk's `evergreen_retention` column is ignored (set to -1).

**Path B — Crosswalk-based (NLCD, NALCMS):**
- Config has no `keep_raster` key.
- Per-HRU retention is synthesised as a weighted average of the crosswalk's `evergreen_retention` column across all non-bare LULC classes:
  ```
  retention = sum(perc / 100 * evergreen_retention)
  ```
  where the sum runs over classes with `nhm_cov_type != 0`.
- Example NLCD values: Deciduous Forest = 0.0, Evergreen Forest = 1.0, Mixed Forest = 0.5, Shrub = 0.3.

### Cover Type Decision Tree

The `assign_cov_type` function applies a priority-ordered decision tree to each HRU, using the aggregated percentage of each NHM cover type (summed from the LULC class percentages via the crosswalk):

| Priority | Condition | Result |
|----------|-----------|--------|
| 1 | bare (nhm_cov_type=0) >= 90% | cov_type = 0 |
| 2 | tree (nhm_cov_type=3) >= 20% | cov_type = 3 |
| 3 | shrub (nhm_cov_type=2) >= 20% | cov_type = 2 |
| 4 | shrub + tree combined >= 35% | cov_type = whichever is greater |
| 5 | grass (nhm_cov_type=1) >= 50% | cov_type = 1 |
| 6 | none of the above | cov_type = highest percentage class |

These thresholds match the NHM documentation and are hardcoded (not configurable), as they reflect scientifically motivated classification rules, not tuning parameters.

### Parameter Formulas

**Interception** (per HRU, summed across LULC classes k with nhm_cov_type != 0):
```
srain_intcp = sum_k(perc_k / 100 * srain_intcp_k)
wrain_intcp = sum_k(perc_k / 100 * wrain_intcp_k)
snow_intcp  = sum_k(perc_k / 100 * snow_intcp_k)
```

**Cover density** (per HRU, using per-HRU canopy mean from zonal stats):
```
covden_sum = sum_k(perc_k / 100 * canopy_mean / 100)    for nhm_cov_type_k != 0
covden_win = sum_k(perc_k / 100 * canopy_mean / 100 * (1 - nhm_covden_win_k))
```

**Retention** (see two code paths above).

### Radiation Transmission Raster (Pre-computed)

When a `keep` raster is available (FORE-SCE), a derived radiation transmission raster is pre-computed:

```
radtrn = (cnpy * keep / 100)   where lulc >= tree_threshold (default 3)
radtrn = 0                     where lulc < tree_threshold
```

This is computed block-by-block (2048x2048) to handle CONUS-scale rasters. The `build_lulc_rasters.py` script orchestrates resampling CNPY and keep rasters to the LULC grid, then calls `raster_ops.compute_radtrn()`.

When no `keep` raster is configured (NLCD, NALCMS), the script skips radtrn computation entirely.

## Config System Enhancement

Step config scalar values (e.g., `lulc_source: foresce`, `scenario: bau_rcp45`, `year: 2070`) are available as `{placeholders}` in other step config values. This enables patterns like:

```yaml
source_raster: "{data_root}/input/lulc_veg/{lulc_source}/LULC_{scenario}_{year}.tif"
```

Implementation: after building the base replacements dict in `config.py:load_config()`, iterate over step config keys and add any scalar value (str, int, float) that does not itself contain `{` as an additional replacement. This is backward-compatible.

## Per-Source Configuration

### FORE-SCE (`configs/lulc_foresce_param.yml`)
- Has `keep_raster` and `radtrn_raster` paths
- Supports future scenarios via `scenario` and `year` fields
- Uses `crosswalks/foresce_nhm.csv` (21 classes)

### NLCD (`configs/lulc_nlcd_param.yml`)
- No `keep_raster` — retention from crosswalk
- Uses NLCD Tree Canopy Cover as `canopy_raster`
- Uses `crosswalks/nlcd_nhm.csv` (20 classes)

### NALCMS (`configs/lulc_nalcms_param.yml`)
- No `keep_raster` — retention from crosswalk
- Needs a canopy raster source (to be determined; could use MODIS VCF or NLCD Tree Canopy)
- Uses `crosswalks/nalcms_nhm.csv` (19 classes)

## Processing Flow (Per Batch)

```
1. Load batch GPKG (~500 HRUs)
2. Load crosswalk CSV
3. CATEGORICAL zonal histogram on LULC raster → per-HRU class pixel counts
4. Convert histogram to class percentages (normalised to 0-100%)
5. CONTINUOUS zonal mean on canopy raster → per-HRU canopy_mean
6. If keep_raster configured:
     CONTINUOUS zonal mean on keep raster → retention (normalised 0-1)
   Else:
     Synthesise retention from crosswalk evergreen_retention column
7. assign_cov_type() → decision tree → cov_type per HRU
8. compute_interception() → weighted srain/wrain/snow per HRU
9. compute_covden() → covden_sum/covden_win per HRU
10. Merge all + write CSV
```

## File Layout

### New Files
| File | Purpose |
|------|---------|
| `src/gfv2_params/lulc.py` | Core library: crosswalk loading, class percentages, decision tree, interception, covden, retention |
| `scripts/create_lulc_params.py` | Batch processing script |
| `scripts/build_lulc_rasters.py` | Pre-compute radtrn raster |
| `configs/lulc_foresce_param.yml` | FORE-SCE step config |
| `configs/lulc_nlcd_param.yml` | NLCD step config |
| `configs/lulc_nalcms_param.yml` | NALCMS step config |
| `crosswalks/foresce_nhm.csv` | FORE-SCE crosswalk (21 classes) |
| `crosswalks/nlcd_nhm.csv` | NLCD crosswalk (20 classes) |
| `crosswalks/nalcms_nhm.csv` | NALCMS crosswalk (19 classes) |
| `slurm_batch/create_lulc_params.batch` | SLURM template — FORE-SCE |
| `slurm_batch/create_lulc_nlcd_params.batch` | SLURM template — NLCD |
| `slurm_batch/create_lulc_nalcms_params.batch` | SLURM template — NALCMS |
| `tests/test_lulc.py` | 21 unit tests |

### Modified Files
| File | Change |
|------|--------|
| `src/gfv2_params/config.py` | Self-referencing placeholder resolution |
| `src/gfv2_params/raster_ops.py` | `compute_radtrn()` function |
| `slurm_batch/merge_output_params.batch` | Add LULC merge step |
| `slurm_batch/RUNME.md` | Add LULC stages and multi-source info |

## Test Coverage

21 unit tests in `tests/test_lulc.py` covering:

- **load_crosswalk**: valid, missing columns, duplicate lu_code (3 tests)
- **class_percentages_from_histogram**: normal, zero pixels (2 tests)
- **assign_cov_type**: bare dominant, tree 20%, shrub 20%, shrub+tree combined (both directions), grass 50%, fallback max, multiple HRUs (8 tests)
- **compute_interception**: weighted sum, bare zero contribution (2 tests)
- **compute_covden**: mixed forest, bare zero (2 tests)
- **compute_retention**: mixed classes, all evergreen, bare zero, mixed forest (4 tests)

Plus 1 config test for self-referencing placeholders.

## Data Directory Extensions

```
input/lulc_veg/
├── foresce/
│   ├── LULC_{scenario}_{year}.tif
│   ├── CNPY.tif
│   └── keep.tif
├── nlcd/
│   ├── nlcd_{year}_land_cover.tif
│   └── nlcd_{year}_tree_canopy.tif
└── nalcms/
    ├── nalcms_{year}_land_cover.tif
    └── nalcms_{year}_tree_canopy.tif

work/derived_rasters/
└── radtrn_{lulc_source}_{scenario}_{year}.tif

{fabric}/params/
├── lulc/
│   └── base_nhm_lulc_{fabric}_batch_XXXX_param.csv
└── merged/
    └── nhm_lulc_params.csv
```
