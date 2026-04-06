# gfv2-params

PRMS/NHM parameter generation from watershed fabric polygons.

Given a watershed fabric of polygons (HRUs), this pipeline computes parameters for the PRMS/National Hydrologic Model by performing zonal statistics against source rasters (DEM, soils, lithology, etc.).

## Setup

### 1. Create conda environment (compiled GIS dependencies)

```bash
conda env create -f environment.yml
conda activate geoenv
```

### 2. Install the package

```bash
pip install -e .
# Or with notebook dependencies:
pip install -e ".[notebooks]"
```

## Project Structure

```
gfv2-params/
├── src/gfv2_params/          # Installable Python package
│   ├── config.py             # Config loading, VPU definitions
│   ├── raster_ops.py         # Raster utilities (resample, multiply, slope conversion)
│   ├── log.py                # Logging setup
│   └── download/             # Data download utilities
├── scripts/                  # CLI processing scripts
│   ├── create_zonal_params.py      # Elevation, slope, aspect parameters
│   ├── create_soils_params.py      # Soils and soil moisture max
│   ├── create_ssflux_params.py     # Subsurface flux parameters
│   ├── merge_rpu_by_vpu.py         # Merge RPU rasters by VPU
│   ├── compute_slope_aspect.py     # Derive slope/aspect from DEM
│   ├── prepare_fabric.py           # Spatially batch fabric into per-batch gpkgs
│   ├── build_vrt.py                # Build CONUS-wide virtual rasters
│   ├── build_derived_rasters.py    # Pre-compute derived rasters (e.g., soil_moist_max)
│   ├── build_weights.py            # Pre-compute polygon-to-polygon weights
│   ├── merge_params.py             # Merge per-batch CSVs
│   ├── merge_default_params.py     # Merge NHM default params
│   ├── merge_and_fill_params.py    # KNN gap-filling
│   └── find_missing_hru_ids.py     # Identify missing HRU IDs
├── configs/                  # YAML configuration files
│   ├── base_config.yml       # Data root and shared settings
│   └── *.yml                 # Per-step configs with template placeholders
├── slurm_batch/              # HPC SLURM batch scripts
│   ├── submit_jobs.sh        # SLURM array job submission wrapper
│   └── RUNME.md              # HPC workflow documentation
├── notebooks/                # Marimo interactive notebooks
├── tests/                    # Unit tests
├── pyproject.toml            # Package configuration
└── environment.yml           # Conda environment (compiled deps only)
```

## Output Directory Structure

The data root (`data_root`) is set in `configs/base_config.yml`. All source data and outputs live under this root:

```
gfv2_param/
├── input/                          # External data (manually staged or downloaded)
│   ├── fabric/                     # Per-VPU watershed fabric gpkgs
│   ├── soils_litho/                # TEXT_PRMS.tif, AWC.tif, Lithology_exp_Konly_Project.*
│   ├── lulc_veg/                   # RootDepth.tif, CNPY.tif, Imperv.tif
│   │   └── nhm_v11/               # NHM v1.1 pre-derived LULC (downloadable)
│   ├── lulc/
│   │   ├── nlcd_annual_imperv/     # NLCD fractional imperviousness (downloadable)
│   │   └── nalcms_2020/            # NALCMS 2020 land cover (downloadable)
│   ├── nhm_default/                # NHM default parameter files
│   └── nhd_downloads/              # Raw NHDPlus zip archives (downloadable)
├── work/                           # Reproducible intermediates (safe to delete)
│   ├── nhd_extracted/              # Unzipped per-RPU rasters
│   ├── nhd_merged/                 # Per-VPU GeoTIFFs + CONUS VRTs
│   ├── derived_rasters/            # soil_moist_max.tif, radtrn, resampled CNPY/keep
│   └── weights/                    # P2P polygon weights for ssflux
└── {fabric}/                       # Per-fabric outputs (e.g., gfv2/, oregon/)
    ├── fabric/                     # Merged fabric gpkg
    ├── batches/                    # Per-batch gpkgs + manifest
    └── params/                     # Parameter outputs + merged/ + filled
```

## Usage

### 1. Initialize the data root

Scaffold the directory tree and verify staged inputs:

```bash
python scripts/init_data_root.py
python scripts/init_data_root.py --check
```

### 2. Stage external inputs

The following externally-provided files must be placed in the scaffolded directories:

| Destination | Required files |
|---|---|
| `input/fabric/` | `NHM_<VPU>_draft.gpkg` for each of the 21 VPUs: `01 02 03N 03S 03W 04 05 06 07 08 09 10L 10U 11 12 13 14 15 16 17 18` |
| `input/soils_litho/` | `TEXT_PRMS.tif`, `AWC.tif`, `Lithology_exp_Konly_Project.shp` (+ sidecar files: `.dbf`, `.prj`, `.shx`) |
| `input/lulc_veg/` | `RootDepth.tif`, `CNPY.tif`, `Imperv.tif` |
| `input/nhm_default/` | NHM default parameter files (input to final merge step) |

### 3. Run fabric-independent tasks

These stages do not require a watershed fabric and can run while fabric preparation proceeds in parallel. This includes downloading and merging NHD rasters, building VRTs, and computing derived rasters. See `slurm_batch/RUNME.md` **Part 1** for the full sequence.

**Download NHDPlus RPU rasters** from S3 (~112 GB):

```bash
mkdir -p logs
sbatch slurm_batch/download_rpu_rasters.batch
```

**Download NALCMS 2020 land cover** from CEC (~2 GB):

```bash
sbatch slurm_batch/download_nalcms.batch
```

Both scripts are idempotent — already-downloaded files are skipped on resubmission.

### 4. Run fabric-dependent tasks

Once raster prep is complete and the merged fabric is available, prepare the fabric batches and run parameter generation. See `slurm_batch/RUNME.md` **Part 2** for the full sequence.

### Single-batch run
```bash
python scripts/create_zonal_params.py --config configs/elev_param.yml --batch_id 0042
```

## Custom Fabric

To run the pipeline against a non-default fabric (e.g., a regional subset), there are two cases:

**Pre-merged fabric** (single gpkg covering the full domain — e.g., Oregon):

1. Create `configs/base_config_oregon.yml` with `fabric: oregon`, `expected_max_hru_id`, and `batch_size`
2. Scaffold the fabric output dirs: `python scripts/init_data_root.py --base_config configs/base_config_oregon.yml`
3. Place the fabric gpkg in `{data_root}/oregon/fabric/` (not in `input/fabric/` — that is for raw per-VPU inputs only)
4. Run `prepare_fabric.py` and all parameter jobs with `--base_config configs/base_config_oregon.yml`
5. Pass the config to `submit_jobs.sh` as the third argument: `slurm_batch/submit_jobs.sh $BATCHES <script>.batch configs/base_config_oregon.yml`

**VPU-based fabric** (per-VPU gpkgs that need merging — e.g., gfv2):

1. Create `configs/base_config_<fabric>.yml` and place per-VPU gpkgs in `input/fabric/`
2. Scaffold, merge with `marimo run notebooks/merge_vpu_targets.py`, then run `prepare_fabric.py` and all stages

See `slurm_batch/RUNME.md` for the full step-by-step workflow.

## Configuration

`configs/base_config.yml` defines the data root path and active fabric name. Per-step configs use `{data_root}` and `{fabric}` template placeholders that are resolved at runtime. The `{vpu}` and `{raster_vpu}` placeholders remain available for VPU-based raster prep scripts.

## Logging

All scripts use Python's `logging` module. Control verbosity via the `LOG_LEVEL` environment variable:
```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO (default), WARNING, ERROR
```

## License

CC0 1.0 Universal
