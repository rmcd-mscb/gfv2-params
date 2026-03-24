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
│   ├── merge_params.py             # Merge per-VPU CSVs
│   ├── merge_default_params.py     # Merge NHM default params
│   ├── merge_and_fill_params.py    # KNN gap-filling
│   └── find_missing_hru_ids.py     # Identify missing HRU IDs
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

## Usage

### VPU-based CONUS processing
```bash
python scripts/create_zonal_params.py --config configs/elev_param.yml --vpu 03N
```

### Custom fabric processing
Create a config with explicit paths (no `{vpu}` placeholders):
```bash
python scripts/create_zonal_params.py --config configs/my_custom_elev.yml
```

### HPC (SLURM)
See `slurm_batch/RUNME.md` for the full HPC workflow.

## Configuration

`configs/base_config.yml` defines the data root path. Per-step configs use `{data_root}`, `{vpu}`, and `{raster_vpu}` template placeholders that are resolved at runtime.

## Logging

All scripts use Python's `logging` module. Control verbosity via the `LOG_LEVEL` environment variable:
```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO (default), WARNING, ERROR
```

## License

CC0 1.0 Universal
