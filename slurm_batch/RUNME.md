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

Both download scripts are idempotent — already-downloaded files are skipped on resubmission.

Once all downloads complete, re-run `--check` to confirm all required inputs are present before proceeding.

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

Pre-compute soil_moist_max raster:

```bash
python scripts/build_derived_rasters.py --base_config configs/base_config.yml
```

### Stage 3: Prepare fabric (one-time per fabric)

Spatially batch the merged fabric into per-batch geopackages:

```bash
python scripts/prepare_fabric.py \
    --fabric_gpkg /path/to/gfv2_nhru_merged.gpkg \
    --base_config configs/base_config.yml \
    --batch_size 500
```

### Stage 4: Generate parameters (SLURM array jobs)

Submit batch jobs using the wrapper script:

```bash
BATCHES=/path/to/gfv2/batches
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_elev_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_slope_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_aspect_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_soils_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_soilmoistmax_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_params.batch
```

The `create_lulc_params.batch` job produces per-HRU fractional land cover percentages for each
NALCMS 2020 class (19 classes). Output: `{fabric}/params/nalcms_2020/` per batch, merged to
`{fabric}/params/merged/nhm_nalcms_2020_lulc_params.csv`.

### Stage 5: Merge and validate

```bash
sbatch slurm_batch/merge_output_params.batch
```

Note: `merge_output_params.batch` merges all parameter types except ssflux (which is produced in Stage 6). Run individually if needed:
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

1. Create `configs/base_config_oregon.yml` with `fabric: oregon` and appropriate `expected_max_hru_id`
2. Place fabric gpkg in `input/fabrics/`
3. Run prepare_fabric with `--base_config configs/base_config_oregon.yml`
4. Run all stages with `--base_config configs/base_config_oregon.yml`

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
| create_ssflux_params.batch | ssflux_param.yml | create_ssflux_params.py |
| merge_output_params.batch | all param configs | merge_params.py |
| merge_rpu_by_vpu.batch | merge_rpu_by_vpu.yml | merge_rpu_by_vpu.py |
| compute_slope_aspect.batch | slope_aspect.yml | compute_slope_aspect.py |
| download_rpu_rasters.batch | base_config.yml | gfv2_params.download.rpu_rasters |
| download_nalcms.batch | base_config.yml | gfv2_params.download.nalcms_lulc |
| create_lulc_params.batch | nalcms_param.yml | create_zonal_params.py |
