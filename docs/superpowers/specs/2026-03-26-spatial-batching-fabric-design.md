# Spatial Batching & Fabric-Aware Pipeline Design

## Goal

Replace VPU-based chunking with spatial batching (KD-tree recursive bisection) for parameter generation, add fabric namespacing to isolate outputs per watershed fabric, and reorganize the data directory to separate inputs, intermediates, and outputs by provenance.

## Motivation

The current pipeline processes one VPU at a time (21 SLURM array jobs). This works but has limitations:

- **Memory waste**: each job loads an entire VPU geopackage (variable size — some VPUs are much larger than others), leading to uneven resource utilization and high memory reservations (256G).
- **No fabric isolation**: outputs from different fabrics (gfv2 CONUS vs. custom Oregon) collide in the same directories.
- **Mixed provenance**: external inputs, reproducible intermediates, and final outputs are interleaved in `source_data/`, making it unclear what can be safely deleted or regenerated (issue #22).

## Architecture

The pipeline gains three new stages that run before parameter generation:

1. **Build VRTs** — combine per-VPU rasters into CONUS-wide virtual rasters (one-time)
2. **Prepare fabric** — spatially batch the merged fabric into per-batch geopackages (one-time per fabric)
3. **Submit jobs** — wrapper script reads batch manifest for SLURM array range

Processing scripts switch from `--vpu` to `--batch_id`. Each SLURM job loads only its small batch geopackage (~500 HRUs). gdptools spatially subsets the CONUS-wide VRT to the batch footprint automatically.

## Data Directory Layout

The `data_root` directory (configured in `base_config.yml`) is reorganized into three top-level zones plus per-fabric namespaces:

```
gfv2_param/
├── input/                                  # External data — manually staged or downloaded
│   ├── fabrics/
│   │   ├── NHM_<VPU>_draft.gpkg            # Per-VPU watershed fabrics
│   │   └── oregon_fabric.gpkg              # Custom fabrics
│   ├── nhd_downloads/                      # Raw NHDPlus zips (download/rpu_rasters.py)
│   ├── mrlc_impervious/                    # NLCD frac impervious (download/mrlc_impervious.py)
│   ├── soils_litho/
│   │   ├── AWC.tif
│   │   ├── TEXT_PRMS.tif
│   │   └── Lithology_exp_Konly_Project.*
│   ├── lulc_veg/
│   │   ├── RootDepth.tif
│   │   ├── CNPY.tif
│   │   └── Imperv.tif
│   ├── nalcms_lulc/                        # NA land cover 2020
│   └── nhm_defaults/                       # NHM default parameter files
│
├── work/                                   # Reproducible intermediates — safe to delete & regenerate
│   ├── nhd_extracted/                      # Unzipped per-RPU rasters
│   ├── nhd_merged/
│   │   ├── <VPU>/                          # Per-VPU merged GeoTIFFs
│   │   ├── elevation.vrt                   # CONUS-wide VRTs (built by build_vrt.py)
│   │   ├── slope.vrt
│   │   └── aspect.vrt
│   ├── derived_rasters/                    # Pipeline-generated rasters
│   │   ├── soil_moist_max.tif
│   │   └── rd_250_raw.tif
│   └── weights/                            # P2P polygon weights (ssflux)
│
├── gfv2/                                   # Fabric namespace: gfv2 CONUS
│   ├── fabric/
│   │   └── gfv2_nhru_merged.gpkg           # Merged fabric (notebook output)
│   ├── batches/
│   │   ├── manifest.yml                    # Batch metadata (count, fabric, batch_size)
│   │   ├── batch_0000.gpkg
│   │   ├── batch_0001.gpkg
│   │   └── ...
│   └── params/
│       ├── elevation/
│       │   └── base_nhm_elevation_gfv2_batch_NNN_param.csv
│       ├── slope/
│       ├── aspect/
│       ├── soils/
│       ├── soil_moist_max/
│       ├── ssflux/
│       ├── merged/                         # Merged + validated per-parameter CSVs
│       │   ├── nhm_elevation_params.csv
│       │   └── filled_nhm_ssflux_params.csv
│       └── defaults_merged/                # NHM defaults rekeyed to nat_hru_id
│
└── oregon/                                 # Fabric namespace: custom Oregon
    ├── fabric/
    ├── batches/
    └── params/
```

### Provenance rules

- **`input/`**: never written to by pipeline scripts (except download scripts). Manually staged or downloaded. Never delete without replacement.
- **`work/`**: all contents reproducible from `input/` by pipeline scripts. Safe to delete and regenerate.
- **`{fabric}/`**: all contents specific to one watershed fabric. Batches and per-batch params are intermediates (regenerable from fabric + work). Merged params are final outputs.

## Config System

### base_config.yml

Gains a `fabric` field:

```yaml
data_root: /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param
fabric: gfv2
expected_max_hru_id: 361471
```

For custom fabrics, use a separate base config:

```yaml
# base_config_oregon.yml
data_root: /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param
fabric: oregon
expected_max_hru_id: 12345
```

### Per-step configs

Template placeholders gain `{fabric}`. Old VPU-specific paths are replaced. The config key `source_raster` is kept (VRTs are valid raster paths for rioxarray/GDAL), avoiding unnecessary code changes in scripts that read `config["source_raster"]`.

```yaml
# elev_param.yml
source_type: elevation
source_raster: "{data_root}/work/nhd_merged/elevation.vrt"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_elevation_params.csv
categorical: false
```

```yaml
# slope_param.yml
source_type: slope
source_raster: "{data_root}/work/nhd_merged/slope.vrt"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_slope_params.csv
categorical: false
```

```yaml
# aspect_param.yml
source_type: aspect
source_raster: "{data_root}/work/nhd_merged/aspect.vrt"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_aspect_params.csv
categorical: false
```

```yaml
# soils_param.yml
source_type: soils
source_raster: "{data_root}/input/soils_litho/TEXT_PRMS.tif"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_soils_params.csv
categorical: true
```

```yaml
# soilmoistmax_param.yml
source_type: soil_moist_max
source_raster: "{data_root}/work/derived_rasters/soil_moist_max.tif"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_soil_moist_max_params.csv
categorical: false
```

Note: `soilmoistmax_param.yml` previously used `source_dir` to point at the parent directory and the script derived multiple raster paths from it. The new config points directly at the pre-built `soil_moist_max.tif` in `work/derived_rasters/`. The raster derivation (resample RootDepth, multiply by AWC) moves to a new pre-processing step (see "Stage 2b: Derive soil_moist_max raster" below).

```yaml
# ssflux_param.yml
source_type: ssflux
source_shapefile: "{data_root}/input/soils_litho/Lithology_exp_Konly_Project.shp"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
weight_dir: "{data_root}/work/weights"
merged_slope_file: "{data_root}/{fabric}/params/merged/nhm_slope_params.csv"
merged_file: nhm_ssflux_params.csv
categorical: false

k_perm_min: -16.48
flux_params:
  - name: soil2gw_max
    min: 0.1
    max: 0.3
  # ... (same as current)
```

The `{vpu}` and `{raster_vpu}` placeholders remain available for raster prep scripts that are still VPU-based.

### Placeholder resolution

`config.py`'s `_resolve_placeholders()` adds `fabric` to the replacement map:

```python
replacements = {"data_root": data_root, "fabric": fabric}
```

The `load_config()` function reads `fabric` from the base config and resolves it automatically.

## New Module: `src/gfv2_params/batching.py`

Ported from [hydro-param/batching.py](https://github.com/rmcd-mscb/hydro-param/blob/main/src/hydro_param/batching.py). Two public functions:

### `spatial_batch(gdf, batch_size=500) -> GeoDataFrame`

- Computes centroids, runs KD-tree recursive bisection
- Returns copy of input with `batch_id` column (int, 0-indexed)
- Handles edge cases: empty GeoDataFrame, single-batch (all features fit)
- O(n log n) performance

### `write_batches(gdf, batch_dir, fabric, id_feature, target_layer="nhru") -> dict`

- Takes a GeoDataFrame with `batch_id` column
- Writes per-batch geopackages: `{batch_dir}/batch_NNNN.gpkg` (4-digit zero-padded to support up to 9999 batches)
- Writes manifest: `{batch_dir}/manifest.yml`
- Returns the manifest dict

### `_recursive_bisect(centroids, indices, depth, max_depth, min_batch_size) -> list[np.ndarray]`

Internal function. KD-tree recursive bisection along alternating x/y axes. Splits at median. Stops at max_depth or min_batch_size.

### Manifest format

```yaml
fabric: gfv2
batch_size: 500
n_batches: 48
n_features: 361471
id_feature: nat_hru_id
target_layer: nhru
created: "2026-03-26T14:30:00"
```

## New Script: `scripts/prepare_fabric.py`

CLI entry point for fabric preparation:

```bash
python scripts/prepare_fabric.py \
    --fabric_gpkg /path/to/gfv2_nhru_merged.gpkg \
    --base_config configs/base_config.yml \
    --batch_size 500 \
    --layer nhru
```

Steps:
1. Load base config to get `data_root` and `fabric`
2. Read the fabric geopackage
3. Call `spatial_batch()` to assign batch IDs
4. Call `write_batches()` to write per-batch gpkgs + manifest
5. Log summary (batch count, size range, output directory)

## New Script: `scripts/build_vrt.py`

Creates CONUS-wide VRT files from per-VPU rasters:

```bash
python scripts/build_vrt.py --base_config configs/base_config.yml
```

Steps:
1. Read `data_root` from base config
2. For each raster type (elevation, slope, aspect): glob per-VPU GeoTIFFs from `work/nhd_merged/*/`
3. Call `gdal.BuildVRT()` to create `work/nhd_merged/{type}.vrt`
4. Log which VRTs were created and how many source rasters each references

Soils and ssflux source data are already CONUS-wide — no VRTs needed.

## New Script: `scripts/build_derived_rasters.py`

Pre-computes derived rasters that the current `create_soils_params.py` builds at runtime. This eliminates the race condition where multiple concurrent SLURM batch jobs would try to create the same derived raster files simultaneously.

```bash
python scripts/build_derived_rasters.py --base_config configs/base_config.yml
```

Steps:
1. Read `data_root` from base config
2. Resample `input/lulc_veg/RootDepth.tif` to match `input/soils_litho/AWC.tif` grid → `work/derived_rasters/rd_250_raw.tif`
3. Multiply `rd_250_raw.tif` × `AWC.tif` → `work/derived_rasters/soil_moist_max.tif`
4. Skip if output files already exist (with `--force` flag to override)

Run once before batch parameter generation. The `soilmoistmax_param.yml` config then points directly at the pre-built `work/derived_rasters/soil_moist_max.tif`.

## Modified Processing Scripts

### Interface change

All parameter generation scripts replace `--vpu` with `--batch_id`:

```bash
# Old
python scripts/create_zonal_params.py --config configs/elev_param.yml --vpu 03N

# New
python scripts/create_zonal_params.py \
    --config configs/elev_param.yml \
    --base_config configs/base_config.yml \
    --batch_id 7
```

### `create_zonal_params.py`

1. Load config (resolves `{fabric}`, `{data_root}`)
2. Read `{batch_dir}/batch_{batch_id:04d}.gpkg` — small, just this batch's HRUs
3. Open the VRT via rioxarray (gdptools spatially subsets to batch footprint)
4. Run zonal stats
5. Write `{output_dir}/elevation/base_nhm_elevation_{fabric}_batch_{batch_id:04d}_param.csv`

### `create_soils_params.py`

Same pattern. Source raster is already CONUS-wide (`input/soils_litho/TEXT_PRMS.tif` for soils, derived raster for soil_moist_max). Reads batch gpkg instead of VPU gpkg.

### `create_soils_params.py`

For `soils`: same pattern as zonal — reads CONUS-wide `TEXT_PRMS.tif` from `input/soils_litho/`, reads batch gpkg.

For `soil_moist_max`: the raster derivation (resample + multiply) is moved to the `build_derived_rasters.py` pre-processing step. This script now reads the pre-built `work/derived_rasters/soil_moist_max.tif` directly and runs zonal stats against the batch gpkg. The `source_dir` config key is replaced by `source_raster` pointing at the derived raster.

### `create_ssflux_params.py`

Uses CONUS-wide lithology shapefile from `input/soils_litho/`. Reads batch gpkg for target polygons.

**Weight computation strategy**: Polygon-to-polygon weights (`WeightGenP2P`) are pre-computed once per fabric as a CONUS-wide weight table, then each batch job subsets the weight table by its HRU IDs. This avoids 48+ redundant spatial intersections against the full lithology shapefile.

The weight pre-computation is a new step added to `prepare_fabric.py` (or a separate `build_weights.py` script — see Pipeline Stages). It writes the full weight table to `work/weights/lith_weights_{fabric}.csv`. Each batch job then:
1. Reads the CONUS weight table
2. Filters to rows matching its batch's `nat_hru_id` values
3. Reads the merged slope CSV from `config["merged_slope_file"]` (`{fabric}/params/merged/nhm_slope_params.csv`)
4. Computes flux parameters for its batch's HRUs
5. Writes per-batch output CSV

The `ssflux_param.yml` config gains `merged_slope_file` to specify the merged slope dependency explicitly.

### Scripts that remain VPU-based (no change to interface)

- `merge_rpu_by_vpu.py` — raster prep, inherently per-VPU
- `compute_slope_aspect.py` — derived rasters, inherently per-VPU

These scripts continue to use `--vpu` and write to `work/nhd_merged/<VPU>/`.

## Modified Merge Scripts

### `merge_params.py`

**New CLI interface:**

```bash
python scripts/merge_params.py \
    --config configs/elev_param.yml \
    --base_config configs/base_config.yml
```

Adds `--base_config` argument. The script reads `fabric` and `expected_max_hru_id` from the base config.

**Changes from current implementation:**
- Glob pattern changes from `base_nhm_{source_type}_*_param.csv` to `base_nhm_{source_type}_{fabric}_batch_*_param.csv`
- VPU column extraction (`file.stem.split("_")[3]`) is removed — batch-based output does not have a VPU label
- Output directory changes from hardcoded `nhm_params_merged/` to `{output_dir}/merged/` (resolved via config)
- **New validation logic** (not present in current code):
  - Checks for duplicate `id_feature` values (raises — indicates overlapping batches)
  - Checks for gaps against `expected_max_hru_id` (logs warning — gaps may be expected for custom fabrics with non-contiguous IDs)
  - Gap validation is optional: skipped when `expected_max_hru_id` is not set in base config, allowing custom fabrics with non-contiguous ID ranges

**Steps:**
1. Load config (resolves `{fabric}`, `{data_root}`)
2. Glob `{output_dir}/{source_type}/base_nhm_{source_type}_{fabric}_batch_*_param.csv`
3. Concatenate all batch CSVs
4. Sort by `id_feature` (`nat_hru_id`)
5. Validate: raise on duplicates, warn on gaps
6. Write `{output_dir}/merged/nhm_{source_type}_params.csv`

### `merge_and_fill_params.py`

**New CLI interface:**

```bash
python scripts/merge_and_fill_params.py \
    --base_config configs/base_config.yml \
    --param_file <path>       # optional, defaults to {fabric}/params/merged/nhm_ssflux_params.csv
    --output_dir <path>       # optional, defaults to {fabric}/params/merged/
    --k_neighbors 1           # optional
```

**Changes from current implementation:**
- `--targets_dir` removed — no longer needed
- `--merged_gpkg` default changes to `{data_root}/{fabric}/fabric/<fabric>_nhru_merged.gpkg`
- `--param_file` default changes to `{data_root}/{fabric}/params/merged/nhm_ssflux_params.csv`
- `--output_dir` default changes to `{data_root}/{fabric}/params/merged/`
- `--force_rebuild` and `--simplify_tolerance` removed — the merged gpkg is produced by the `prepare_fabric.py` step; there is no legacy VPU-merge fallback path
- `VPUS_DETAILED` import removed — no longer used
- The `vpu` column is no longer expected or propagated in the data; batch-based output does not include it
- All default paths are derived from base config (`data_root` + `fabric`)

### `merge_default_params.py`

**New CLI interface:**

```bash
python scripts/merge_default_params.py \
    --base_config configs/base_config.yml \
    --dict <path>             # optional, defaults to {data_root}/input/nhm_defaults/param_dict.csv
```

**Changes from current implementation:**
- `--base_dir` default changes from `{data_root}/nhm_params/default` to `{data_root}/input/nhm_defaults/`
  - The NHM default parameter files (with per-VPU subdirectories `r01/`, `r02/`, etc.) move to `input/nhm_defaults/`
  - The parameter dictionary CSV also moves to `input/nhm_defaults/param_dict.csv`
- `--output_dir` default changes to `{data_root}/{fabric}/params/defaults_merged/`
- `--dict` gains a default path so it can be omitted when using the standard layout

## SLURM Integration

### Batch files

Batch files simplify — no hardcoded VPU arrays. The `--array` range is supplied at submission time:

```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=elev_zonal
#SBATCH --output=logs/job_%A_%a.out
#SBATCH --error=logs/job_%A_%a.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G

module load miniforge/latest
conda activate geoenv

python scripts/create_zonal_params.py \
    --config configs/elev_param.yml \
    --base_config configs/base_config.yml \
    --batch_id $SLURM_ARRAY_TASK_ID
```

Memory drops from 256G to ~32G since each job processes ~500 HRUs instead of an entire VPU. GDAL only reads the VRT tiles that intersect the batch footprint. Tune up if OOM occurs; tune down for better scheduler throughput.

### Submission wrapper: `slurm_batch/submit_jobs.sh`

```bash
#!/bin/bash
# Usage: ./submit_jobs.sh /path/to/{fabric}/batches <batch_script.batch>
FABRIC_DIR=$1
BATCH_SCRIPT=$2

N_BATCHES=$(grep n_batches "$FABRIC_DIR/manifest.yml" | awk '{print $2}')
LAST_IDX=$((N_BATCHES - 1))

sbatch --array=0-$LAST_IDX "$BATCH_SCRIPT"
```

### Raster prep batch files

`merge_rpu_by_vpu.batch` and `compute_slope_aspect.batch` remain VPU-based with hardcoded arrays — no change.

## Pipeline Stages (step-by-step)

### Stage 1: Raster preparation (VPU-based, unchanged)

```bash
sbatch merge_rpu_by_vpu.batch
```

Merges per-RPU rasters into per-VPU GeoTIFFs in `work/nhd_merged/<VPU>/`.

Then derive slope and aspect from DEM:

```bash
sbatch compute_slope_aspect.batch
```

Note: `compute_slope_aspect.batch` produces both slope and aspect GeoTIFFs per VPU. If you use this, you do **not** need separate slope/aspect zonal jobs in Stage 4 — those read the VRTs built from these outputs.

### Stage 2a: Build VRTs (one-time, interactive)

```bash
python scripts/build_vrt.py --base_config configs/base_config.yml
```

Creates `work/nhd_merged/{elevation,slope,aspect}.vrt`.

### Stage 2b: Derive soil_moist_max raster (one-time, interactive)

```bash
python scripts/build_derived_rasters.py --base_config configs/base_config.yml
```

Resamples `input/lulc_veg/RootDepth.tif`, multiplies by `input/soils_litho/AWC.tif`, writes `work/derived_rasters/soil_moist_max.tif`. Must run before soil_moist_max batch jobs.

### Stage 3: Prepare fabric (one-time per fabric, interactive)

```bash
python scripts/prepare_fabric.py \
    --fabric_gpkg gfv2/fabric/gfv2_nhru_merged.gpkg \
    --base_config configs/base_config.yml \
    --batch_size 500 \
    --layer nhru
```

Writes per-batch gpkgs + manifest to `gfv2/batches/`.

### Stage 4: Generate parameters (SLURM array jobs)

```bash
BATCHES=gfv2_param/gfv2/batches
./submit_jobs.sh $BATCHES slurm_batch/create_zonal_elev_params.batch
./submit_jobs.sh $BATCHES slurm_batch/create_zonal_slope_params.batch
./submit_jobs.sh $BATCHES slurm_batch/create_zonal_aspect_params.batch
./submit_jobs.sh $BATCHES slurm_batch/create_soils_params.batch
./submit_jobs.sh $BATCHES slurm_batch/create_soilmoistmax_params.batch
```

**Partial reruns**: To rerun a single failed batch, use `sbatch --array=N` (e.g., `sbatch --array=37 create_zonal_elev_params.batch`) rather than resubmitting the full array.

### Stage 5: Merge & validate

```bash
python scripts/merge_params.py --config configs/elev_param.yml --base_config configs/base_config.yml
python scripts/merge_params.py --config configs/slope_param.yml --base_config configs/base_config.yml
python scripts/merge_params.py --config configs/aspect_param.yml --base_config configs/base_config.yml
python scripts/merge_params.py --config configs/soils_param.yml --base_config configs/base_config.yml
python scripts/merge_params.py --config configs/soilmoistmax_param.yml --base_config configs/base_config.yml
```

### Stage 6: SSFlux (depends on merged slope from Stage 5)

Pre-compute CONUS-wide polygon-to-polygon weights (one-time per fabric):

```bash
python scripts/build_weights.py \
    --config configs/ssflux_param.yml \
    --base_config configs/base_config.yml
```

Writes `work/weights/lith_weights_gfv2.csv`. Then run batch jobs:

```bash
./submit_jobs.sh $BATCHES slurm_batch/create_ssflux_params.batch
python scripts/merge_params.py --config configs/ssflux_param.yml --base_config configs/base_config.yml
```

### Stage 7: KNN gap-fill (ssflux)

```bash
python scripts/merge_and_fill_params.py --base_config configs/base_config.yml
```

### Stage 8: Merge NHM defaults (optional)

```bash
python scripts/merge_default_params.py --base_config configs/base_config.yml
```

## Documentation Updates

### `slurm_batch/RUNME.md`

Rewrite to reflect the new pipeline stages above. Include:
- Prerequisites (conda env, pip install, data staging)
- Stage-by-stage instructions with exact commands
- Custom fabric example (Oregon)
- Monitoring and troubleshooting

### `README.md`

Update:
- Project structure section to reflect new scripts
- Output directory structure to show `input/`, `work/`, `{fabric}/` layout
- Usage section with new CLI examples
- Custom fabric workflow

## Testing

### `tests/test_batching.py`

- `spatial_batch()`: empty GeoDataFrame, single-batch short-circuit, multi-batch partitioning, batch_id coverage (all features assigned exactly once)
- `write_batches()`: correct file count, manifest content, gpkg readability
- `_recursive_bisect()`: alternating axes, min_batch_size stopping, equal-coordinate edge case

### Existing tests

- `test_config.py`: add tests for `{fabric}` placeholder resolution
- `test_merge_and_fill_params.py`: update paths to reflect new directory structure

## Migration

This is a breaking change to the data directory layout. Migration approach:

1. Create `input/`, `work/` directories and move/symlink existing data
2. Run `prepare_fabric.py` to generate batches
3. Run `build_vrt.py` to create VRTs
4. Update configs to new paths
5. Old per-VPU output CSVs can be archived or deleted after verification

The repo README and RUNME.md guide users through the new layout. Since the user plans to start fresh on HPC (move existing repo aside), no in-place migration is needed — the new layout is built from scratch.
