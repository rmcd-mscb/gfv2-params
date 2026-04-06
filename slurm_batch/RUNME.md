# GFv2 Pipeline: HPC Workflow

## Prerequisites

```bash
module load miniforge/latest
conda activate geoenv
pip install -e .
```

## Data Directory Layout

All data lives under `data_root` (set in `configs/base_config.yml`):

```
gfv2_param/
├── input/          # External data (manually staged or downloaded)
│   ├── fabrics/    # Per-VPU and custom watershed fabric gpkgs
│   ├── nhd_downloads/
│   ├── mrlc_impervious/
│   ├── soils_litho/
│   ├── lulc_veg/
│   │   └── nhm_v11/    # NHM v1.1 pre-derived LULC rasters (downloadable)
│   └── nhm_defaults/
├── work/           # Reproducible intermediates (safe to delete)
│   ├── nhd_extracted/
│   ├── nhd_merged/     # Per-VPU GeoTIFFs + CONUS VRTs
│   ├── derived_rasters/
│   └── weights/
└── {fabric}/       # Per-fabric outputs (e.g., gfv2/, oregon/)
    ├── fabric/     # Merged fabric gpkg
    ├── batches/    # Per-batch gpkgs + manifest
    └── params/     # Parameter outputs + merged + filled
```

## Pipeline Stages

All commands below assume the repo root as your working directory, e.g.:
```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
```

---

## Part 1: Fabric-Independent Tasks

These stages do not require a watershed fabric and can be run while fabric preparation proceeds in parallel. Complete all Part 1 stages before moving to Part 2.

### Stage 0: Initialize data root and stage inputs

Scaffold the full directory tree under your `data_root`:

```bash
python scripts/init_data_root.py
```

Verify that staged inputs are present:

```bash
python scripts/init_data_root.py --check
```

The following externally-provided files must be placed in the scaffolded directories before running `--check`:

| Destination | Required files |
|---|---|
| `input/fabric/` | `NHM_<VPU>_draft.gpkg` for each of the 21 VPUs: `01 02 03N 03S 03W 04 05 06 07 08 09 10L 10U 11 12 13 14 15 16 17 18` |
| `input/soils_litho/` | `TEXT_PRMS.tif`, `AWC.tif`, `Lithology_exp_Konly_Project.shp` (+ sidecar files: `.dbf`, `.prj`, `.shx`) |
| `input/lulc_veg/` | `RootDepth.tif`, `CNPY.tif`, `Imperv.tif` |
| `input/nhm_default/` | NHM default parameter files (input to final merge step) |

The NALCMS 2020 land cover raster can be downloaded automatically (see below).

**Download NHDPlus RPU rasters** from S3 (network-bound, ~112 GB, submit as a SLURM job):

```bash
mkdir -p logs
sbatch slurm_batch/download_rpu_rasters.batch
```

**Download NALCMS 2020 land cover raster** from CEC (~2 GB zip, submit as a SLURM job):

```bash
sbatch slurm_batch/download_nalcms.batch
```

**Download NHM v1.1 LULC rasters** from ScienceBase item `5ebb182b82ce25b5136181cf`
(`LULC.zip`, `keep.zip`, `CNPY.zip` — network-bound; submit as a SLURM job):

```bash
sbatch slurm_batch/download_nhm_v11.batch
```

All download scripts are idempotent — already-downloaded files are skipped on resubmission.

Note: `--check` only validates manually-staged inputs (soils, litho, lulc_veg). Verify downloads completed successfully by checking the job logs before proceeding.

### Stage 1: Raster preparation (VPU-based)

Download and merge per-RPU NHDPlus rasters, then derive slope/aspect:

```bash
sbatch slurm_batch/merge_rpu_by_vpu.batch
sbatch slurm_batch/compute_slope_aspect.batch
```

### Stage 2a: Build VRTs (one-time)

Combine per-VPU rasters into CONUS-wide virtual rasters:

```bash
python scripts/build_vrt.py --base_config configs/base_config.yml
```

### Stage 2b: Build derived rasters (one-time)

Pre-compute `rd_250_raw.tif` and `soil_moist_max.tif`:

```bash
sbatch slurm_batch/build_derived_rasters.batch
```

To use a different base config, override `BASE_CONFIG`:

```bash
BASE_CONFIG=configs/base_config_oregon.yml sbatch slurm_batch/build_derived_rasters.batch
```

### Stage 2c: Build LULC derived rasters (one-time)

Pre-compute radiation transmission raster from LULC + canopy + keep:

```bash
sbatch slurm_batch/build_lulc_rasters.batch
```

To use a different LULC source, override `LULC_CONFIG`:

```bash
LULC_CONFIG=configs/lulc_nalcms_param.yml sbatch slurm_batch/build_lulc_rasters.batch
```

---

## Part 2: Fabric-Dependent Tasks

These stages require the merged fabric geopackage and the per-batch gpkgs produced by `prepare_fabric.py`. Complete Part 1 before proceeding.

### Stage 3: Prepare fabric (one-time per fabric)

Merge per-VPU fabric geopackages into a single CONUS fabric:

```bash
marimo run notebooks/merge_vpu_targets.py
```

Then spatially batch the merged fabric into per-batch geopackages (`batch_size` is read from `base_config.yml`):

```bash
python scripts/prepare_fabric.py \
    --fabric_gpkg {data_root}/gfv2/fabric/gfv2_nhru_merged.gpkg \
    --base_config configs/base_config.yml
```

### Stage 4: Generate parameters (SLURM array jobs)

Submit batch jobs using the wrapper script:

Pass the corresponding param config as the 4th argument to auto-submit a merge job
that runs immediately after each array job completes (`afterok` dependency):

```bash
BATCHES=/path/to/gfv2/batches
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_elev_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_slope_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_aspect_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_soils_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_soilmoistmax_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_nhm_v11_params.batch
```

To use an alternative LULC source (NLCD or NALCMS) instead of NHM v1.1:
```bash
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_nlcd_params.batch
# or
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_nalcms_params.batch
```

The `create_lulc_params.batch` job produces per-HRU fractional land cover percentages for each
NALCMS 2020 class (19 classes). Output: `{fabric}/params/nalcms_2020/` per batch, merged to
`{fabric}/params/merged/nhm_nalcms_2020_lulc_params.csv`.

### Stage 5: Merge and validate

If all Stage 4 jobs were submitted with the 4th merge-config argument (recommended), merges
run automatically as chained SLURM jobs. To re-run all merges manually at once:

```bash
sbatch slurm_batch/merge_output_params.batch
```

Or individually:
```bash
python scripts/merge_params.py --config configs/elev_param.yml --base_config configs/base_config.yml
```

### Stage 6: SSFlux (depends on merged slope)

Pre-compute weights, then run batch jobs:

```bash
python scripts/build_weights.py --config configs/ssflux_param.yml --base_config configs/base_config.yml
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_ssflux_params.batch
python scripts/merge_params.py --config configs/ssflux_param.yml --base_config configs/base_config.yml
```

### Stage 7: KNN gap-fill

```bash
python scripts/merge_and_fill_params.py --base_config configs/base_config.yml
```

### Stage 8: Merge NHM defaults (optional)

```bash
python scripts/merge_default_params.py --base_config configs/base_config.yml
```

## Custom Fabric (e.g., Oregon)

Two cases depending on whether the fabric is already merged or comes as per-VPU gpkgs.

**Case A: Pre-merged fabric** (single gpkg covering the full domain — e.g., Oregon)

1. Create `configs/base_config_oregon.yml` with `fabric: oregon`, `expected_max_hru_id`, and `batch_size`
2. Scaffold the fabric's output directories:
   ```bash
   python scripts/init_data_root.py --base_config configs/base_config_oregon.yml
   ```
3. Place the fabric gpkg directly in `{data_root}/oregon/fabric/` (NOT in `input/fabric/`)
4. Prepare batches:
   ```bash
   python scripts/prepare_fabric.py \
       --fabric_gpkg {data_root}/oregon/fabric/NHM_OR_draft.gpkg \
       --base_config configs/base_config_oregon.yml
   ```
5. Submit parameter jobs, passing the fabric config as the third argument:
   ```bash
   BATCHES={data_root}/oregon/batches
   slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_params.batch configs/base_config_oregon.yml configs/nalcms_param.yml
   ```

**Case B: VPU-based fabric** (per-VPU gpkgs that need merging — e.g., gfv2)

1. Create `configs/base_config_<fabric>.yml` with `fabric: <name>`, `expected_max_hru_id`, and `batch_size`
2. Place per-VPU gpkgs in `input/fabric/`
3. Scaffold and merge:
   ```bash
   python scripts/init_data_root.py --base_config configs/base_config_<fabric>.yml
   marimo run notebooks/merge_vpu_targets.py
   ```
4. Continue from Stage 3 above, passing `--base_config configs/base_config_<fabric>.yml`

## Partial Reruns

To rerun a single failed batch (the batch file reads `$SLURM_ARRAY_TASK_ID` regardless of how the array was specified):
```bash
sbatch --array=37 slurm_batch/create_zonal_elev_params.batch
```

## Monitoring

```bash
squeue -u "$USER"
tail -n 200 logs/job_*.out
sacct -j <JOBID> -o JobID,State,Elapsed,MaxRSS
```

## Script -> Config -> Entry Point Mapping

| Batch file | Config | Script |
|---|---|---|
| create_zonal_elev_params.batch | elev_param.yml | create_zonal_params.py |
| create_zonal_slope_params.batch | slope_param.yml | create_zonal_params.py |
| create_zonal_aspect_params.batch | aspect_param.yml | create_zonal_params.py |
| create_soils_params.batch | soils_param.yml | create_soils_params.py |
| create_soilmoistmax_params.batch | soilmoistmax_param.yml | create_soils_params.py |
| create_lulc_params.batch | lulc_foresce_param.yml | create_lulc_params.py |
| create_lulc_nlcd_params.batch | lulc_nlcd_param.yml | create_lulc_params.py |
| create_lulc_nalcms_params.batch | lulc_nalcms_param.yml | create_lulc_params.py |
| create_ssflux_params.batch | ssflux_param.yml | create_ssflux_params.py |
| merge_output_params.batch | all param configs | merge_params.py |
| merge_rpu_by_vpu.batch | merge_rpu_by_vpu.yml | merge_rpu_by_vpu.py |
| compute_slope_aspect.batch | slope_aspect.yml | compute_slope_aspect.py |
| download_rpu_rasters.batch | base_config.yml | gfv2_params.download.rpu_rasters |
| download_nalcms.batch | base_config.yml | gfv2_params.download.nalcms_lulc |
| create_lulc_params.batch | nalcms_param.yml | create_zonal_params.py |
