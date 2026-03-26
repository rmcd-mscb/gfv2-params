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

### Stage 1: Raster preparation (VPU-based)

Download and merge per-RPU NHDPlus rasters, then derive slope/aspect:

```bash
sbatch merge_rpu_by_vpu.batch
sbatch compute_slope_aspect.batch
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
./submit_jobs.sh $BATCHES create_zonal_elev_params.batch
./submit_jobs.sh $BATCHES create_zonal_slope_params.batch
./submit_jobs.sh $BATCHES create_zonal_aspect_params.batch
./submit_jobs.sh $BATCHES create_soils_params.batch
./submit_jobs.sh $BATCHES create_soilmoistmax_params.batch
```

### Stage 5: Merge and validate

```bash
sbatch merge_output_params.batch
```

Or run individually:
```bash
python scripts/merge_params.py --config configs/elev_param.yml --base_config configs/base_config.yml
```

### Stage 6: SSFlux (depends on merged slope)

Pre-compute weights, then run batch jobs:

```bash
python scripts/build_weights.py --config configs/ssflux_param.yml --base_config configs/base_config.yml
./submit_jobs.sh $BATCHES create_ssflux_params.batch
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

To rerun a single failed batch:
```bash
sbatch --array=37 create_zonal_elev_params.batch
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
