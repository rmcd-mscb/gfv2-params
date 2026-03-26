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
├── submit_jobs.sh            # SLURM array job submission wrapper
├── configs/                  # YAML configuration files
│   ├── base_config.yml       # Data root and shared settings
│   └── *.yml                 # Per-step configs with template placeholders
├── slurm_batch/              # HPC SLURM batch scripts
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

## Usage

### Batch-based CONUS processing
```bash
python scripts/create_zonal_params.py --config configs/elev_param.yml --batch_id 0042
```

### HPC (SLURM)
See `slurm_batch/RUNME.md` for the full HPC workflow.

## Custom Fabric

To run the pipeline against a non-default fabric (e.g., a regional subset):

1. Create `configs/base_config_oregon.yml` with `fabric: oregon` and the appropriate `expected_max_hru_id`.
2. Place the fabric gpkg in `input/fabrics/`.
3. Run `prepare_fabric.py` with `--base_config configs/base_config_oregon.yml`.
4. Run all pipeline stages passing `--base_config configs/base_config_oregon.yml`.

## Configuration

`configs/base_config.yml` defines the data root path and active fabric name. Per-step configs use `{data_root}`, `{fabric}`, and `{batch_id}` template placeholders that are resolved at runtime.

## Logging

All scripts use Python's `logging` module. Control verbosity via the `LOG_LEVEL` environment variable:
```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO (default), WARNING, ERROR
```

## License

CC0 1.0 Universal
