# gfv2-params Repository Restructure Design

**Date:** 2026-03-23
**Status:** Draft
**Scope:** Full restructure of the gfv2-params repository for maintainability, extensibility, and support for custom fabric processing

## Context

gfv2-params is a parameter generation pipeline for PRMS/NHM. Given a watershed fabric of polygons, it computes parameters (elevation, slope, aspect, soils, soil moisture capacity, subsurface flux) via zonal statistics against source rasters. It runs on USGS HPC (SLURM) and processes CONUS through 18 NHDPlus VPUs (361,471 HRUs).

### Problems with the current layout

1. **`load_config` defined in multiple places** — `helpers.py`, `7_add_nat_hru_id.py`, `process_NHD_by_vpu.py`, `process_slope_and_aspect.py`, plus marimo notebooks
2. **`sys.path` hacking** — every script appends `src/` to `sys.path` instead of importing from an installed package
3. **No `[build-system]`** — `pyproject.toml` exists but the package isn't installable
4. **Hardcoded `/caldera/...` path** in `process_NHD_by_vpu.py` line 19
5. **VPU mapping scattered** across batch scripts, Python scripts, and configs
6. **Commented-out dead code** throughout (old cumulative offset logic, temp file cleanup)
7. **Debug prints left in production** (`__file__`, `src_path` in ssflux)
8. **Magic numbers** — ssflux flux parameter bounds hardcoded in script
9. **Inconsistent naming** — scripts use `1_`, `4_`, `6_` (gaps), batches use `01_`-`08_`
10. **Duplicate dependency** — `dask` listed twice in pyproject.toml
11. **Pipeline tightly coupled to VPU scheme** — cannot process custom fabrics without modifying scripts
12. **Bare `print()` statements** instead of structured logging
13. **Stale zonal engine** — scripts use `zonal_engine="parallel"` instead of the newer `exactextract` engine now supported by gdptools

## Design

### 1. Package Structure

```
gfv2-params/
├── src/gfv2_params/              # installable Python package
│   ├── __init__.py               # version string
│   ├── config.py                 # load_config(), VPU definitions, path resolution
│   ├── raster_ops.py             # resample(), mult_rasters(), deg_to_fraction()
│   ├── log.py                    # configure_logging() helper
│   └── download/                 # data download utilities
│       ├── __init__.py
│       ├── rpu_rasters.py        # from current src/download_rpu_rasters.py
│       └── mrlc_impervious.py    # from current src/download_mrlc_fract_impervious_rasters.py
├── scripts/                      # standalone CLI scripts, import from gfv2_params
│   ├── create_zonal_params.py    # elevation, slope, aspect (was 1_create_dem_params.py)
│   ├── create_soils_params.py    # soils + soil_moist_max (was 4_create_soils_params.py)
│   ├── create_ssflux_params.py   # subsurface flux (was 6_create_ssflux_params.py)
│   ├── merge_rpu_by_vpu.py       # was process_NHD_by_vpu.py
│   ├── compute_slope_aspect.py   # was process_slope_and_aspect.py
│   ├── merge_params.py           # was 7_add_nat_hru_id.py
│   ├── merge_default_params.py   # was 8_add_nat_hru_id_default_nhru.py
│   ├── merge_and_fill_params.py  # was merge_vpu_and_fill_params.py
│   └── find_missing_hru_ids.py   # kept
├── configs/
│   ├── base_config.yml           # NEW: data root, shared settings
│   ├── elev_param.yml            # was 01_elev_param_config.yml
│   ├── slope_param.yml           # was 02_slope_param_config.yml
│   ├── aspect_param.yml          # was 03_aspect_param_config.yml
│   ├── soils_param.yml           # was 04_soils_param_config.yml
│   ├── soilmoistmax_param.yml    # was 05_soilmoistmax_param_config.yml
│   ├── ssflux_param.yml          # was 06_ssflux_param_config.yml (absorbs magic numbers)
│   ├── merge_rpu_by_vpu.yml      # was config_merge_rpu_by_vpu.yml
│   └── slope_aspect.yml          # was config_slope_aspect.yml
├── slurm_batch/                  # updated to match new script/config names
│   ├── RUNME.md
│   └── *.batch                   # renamed, no number prefixes
├── notebooks/                    # renamed from marimo/
├── pyproject.toml                # with [build-system], dependency groups
├── environment.yml               # conda deps (binary/compiled only)
├── README.md
├── LICENSE
├── .pre-commit-config.yaml
└── .gitignore
```

### 2. Config System

#### `base_config.yml` — single source of truth for shared settings

```yaml
data_root: /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param
targets_dir: targets
output_dir: nhm_params
expected_max_hru_id: 361471
```

#### Per-step configs — support both VPU-based and custom fabric processing

**VPU-based (template placeholders):**
```yaml
source_type: elevation
source_raster: "{data_root}/NHDPlus_Merged_Rasters/{raster_vpu}/NEDSnapshot_merged_fixed_{raster_vpu}.tif"
target_gpkg: "{data_root}/targets/NHM_{vpu}_draft.gpkg"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/nhm_params"
categorical: false
```

**Custom fabric (explicit paths):**
```yaml
source_type: elevation
source_raster: /caldera/some/project/my_dem.tif
target_gpkg: /caldera/some/project/my_watershed.gpkg
target_layer: catchments
id_feature: catch_id
output_dir: /caldera/some/project/output
categorical: false
```

**Config key mapping (old -> new):**

| Old key | New key | Notes |
|---------|---------|-------|
| `base_source_dir` | `source_raster` (full path with filename) | Template or explicit |
| `target_source_dir` | `target_gpkg` (full path with filename) | Template or explicit |
| `output_dir` | `output_dir` | Same, supports `{data_root}` |
| (hardcoded in script) | `target_layer` | Was hardcoded as `"nhru"` |
| (hardcoded in script) | `id_feature` | Was hardcoded as `"nat_hru_id"` |
| `weight_dir` (ssflux only) | `weight_dir` | Kept, supports `{data_root}` |
| `input_dir` (slope_aspect) | `input_dir` | Kept, supports `{data_root}` |

**Configs that keep their own structure:**
- `merge_rpu_by_vpu.yml`: Nested VPU->dataset->RPU list structure. Only change is replacing hardcoded absolute RPU paths with `{data_root}`-relative paths.
- `ssflux_param.yml`: Adds `k_perm_min`, `flux_params_max`, `flux_params_min` (moved from hardcoded values in script).

#### `config.py` — loading and resolution

```python
def load_config(step_config_path: Path, vpu: str | None = None) -> dict:
    """
    Load base_config.yml + step config, resolve placeholders.

    When vpu is provided, resolves {data_root}, {vpu}, {raster_vpu} placeholders.
    When vpu is None, resolves {data_root} only; all paths must be explicit.
    """
```

#### VPU definitions — canonical, single location

```python
VPUS_DETAILED = [
    "01", "02", "03N", "03S", "03W", "04", "05", "06", "07", "08",
    "09", "10L", "10U", "11", "12", "13", "14", "15", "16", "17", "18",
]

VPUS_SIMPLE = [f"{i:02d}" for i in range(1, 19)]

VPU_RASTER_MAP = {
    "03N": "03", "03S": "03", "03W": "03",
    "10U": "10", "10L": "10",
    "OR": "17",
}

def resolve_vpu(vpu: str) -> tuple[str, str]:
    """Return (raster_vpu, gpkg_vpu) for a given VPU code."""
    raster_vpu = VPU_RASTER_MAP.get(vpu, vpu)
    gpkg_vpu = vpu
    return raster_vpu, gpkg_vpu
```

### 3. Script Argument Patterns

Most scripts follow a uniform pattern of `--config` (required) and `--vpu` (optional):

```bash
# VPU-based CONUS processing
python scripts/create_zonal_params.py --config configs/elev_param.yml --vpu 03N

# Custom fabric processing
python scripts/create_zonal_params.py --config configs/custom_fabric_elev.yml
```

When `--vpu` is omitted, the config must contain fully resolved paths. When provided, template placeholders are resolved.

**Scripts that follow this pattern:** `create_zonal_params.py`, `create_soils_params.py`, `create_ssflux_params.py`, `compute_slope_aspect.py`

**Scripts with different interfaces:**

- **`merge_rpu_by_vpu.py`**: Takes `--config` (required, pointing to `merge_rpu_by_vpu.yml` which has a unique nested VPU->dataset->RPU structure) and `--vpu` (required, selects which VPU entry to process). This config structure is fundamentally different from the per-step configs and is kept as-is — only the hardcoded `base_path` on line 19 is replaced with `{data_root}` resolution from `base_config.yml`.

- **`merge_params.py`**: Takes `--config` pointing to a per-step config (uses `output_dir`, `source_type`, `merged_file` keys). No `--vpu` argument — it globs all VPU outputs and merges them. Adopts `load_config` from `gfv2_params.config` but keeps its own config key structure.

- **`merge_and_fill_params.py`**: Takes explicit CLI arguments (`--targets_dir`, `--param_file`, `--output_dir`, `--simplify_tolerance`, `--k_neighbors`, `--force_rebuild`). This script does not use a YAML config. Defaults are updated to use paths derived from `base_config.yml` loaded in the script, but the CLI args remain for override flexibility.

- **`merge_default_params.py`**: Takes `--dict`, `--base_dir`, `--output_dir`. Similar to `merge_and_fill_params.py`, no YAML config. Defaults derived from `base_config.yml`.

- **`find_missing_hru_ids.py`**: Standalone analysis script. Uses `base_config.yml` for `expected_max_hru_id` instead of hardcoding 361471.

### 4. Logging

#### `gfv2_params/log.py`

```python
import logging
import os

def configure_logging(name: str) -> logging.Logger:
    """
    Configure and return a logger.

    Reads LOG_LEVEL from environment (default: INFO).
    Format includes timestamp, level, and logger name for SLURM log files.

    Uses explicit handler setup rather than basicConfig to avoid conflicts
    when multiple modules configure logging in the same process.
    """
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(getattr(logging, level))
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False

    return logger
```

#### Usage in scripts

```python
from gfv2_params.log import configure_logging

def main():
    logger = configure_logging("create_zonal_params")
    logger.info("Raster: %s", ned_path)
    logger.info("Loaded nhru layer: %d features", len(nhru_gdf))
    logger.debug("CRS: %s", ned_da.rio.crs)
```

#### SLURM control

- Default: INFO-level with timestamps in log files
- Debug a failed job: add `export LOG_LEVEL=DEBUG` to the batch script, resubmit
- No code changes needed to adjust verbosity

### 5. Progress Bars (tqdm)

- Add `tqdm` to core dependencies
- Wrap loops with known iteration counts that take significant time:
  - VPU geopackage merging
  - KNN gap-filling iteration
  - Weight calculation loops
- tqdm auto-detects non-interactive environments (SLURM) and falls back to simple line-based output
- NOT applied to gdptools internal calls (`ZonalGen.calculate_zonal()`) since those are library-internal; log before/after instead

### 6. Zonal Engine Update

All scripts using gdptools `ZonalGen` switch from `zonal_engine="parallel"` to `zonal_engine="exactextract"`. The `exactextract` package is added to `environment.yml` as a conda dependency (compiled C library).

### 7. pyproject.toml

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "gfv2-params"
version = "0.1.0"
description = "PRMS/NHM parameter generation from watershed fabric polygons"
requires-python = ">=3.12"
dependencies = [
    "gdptools==0.3.11",
    "pandas",
    "numpy",
    "geopandas",
    "xarray",
    "rioxarray",
    "rasterstats",
    "spatialpandas",
    "pooch",
    "py7zr",
    "scikit-learn",
    "pyyaml",
    "tqdm",
]

[project.optional-dependencies]
notebooks = [
    "marimo[recommended]",
    "plotly",
    "matplotlib",
    "hvplot",
    "geoviews",
    "datashader",
    "dask",
    "distributed",
    "intake",
    "intake-xarray",
    "intake-parquet",
    "s3fs",
    "ipykernel",
]
dev = [
    "pytest",
    "pre-commit",
    "ruff",
]

[tool.isort]
profile = "black"
line_length = 120

[tool.ruff]
line-length = 120

[tool.ruff.lint]
extend-ignore = ["F722"]
```

### 8. environment.yml

Conda handles binary/compiled dependencies only:

Conda handles only compiled C/C++/Fortran libraries that are difficult to pip-install on HPC. Everything else is installed via `pip install -e .` from pyproject.toml.

```yaml
name: geoenv
channels:
  - conda-forge
dependencies:
  - python=3.12
  - gdal
  - rasterio
  - geopandas
  - pyproj
  - numba
  - llvmlite
  - richdem
  - exactextract
  - pip
```

Setup: `conda activate geoenv && pip install -e .` (or `pip install -e ".[notebooks]"` for notebook dependencies)

### 9. SLURM Batch Scripts

- **Rename to match new script names** — drop number prefixes, use descriptive names
- **Delete stale files**: `01_create_elev_params copy.batch`, `01_OR_*`, `04_OR_*`, `06_OR_*`
- **Update script and config paths** in all batch files
- **Update RUNME.md** with new names, setup prerequisites (`pip install -e .`), and custom fabric instructions
- **VPU array definitions** in batch files now reference `gfv2_params.config.VPUS_DETAILED` conceptually but remain as bash arrays (batch scripts can't import Python constants)

### 10. Cleanup Checklist

- [ ] Delete commented-out cumulative offset code in `7_add_nat_hru_id.py` and `merge_vpu_and_fill_params.py`
- [ ] Delete commented-out temp file cleanup in `4_create_soils_params.py`
- [ ] Delete `01_create_elev_params copy.batch`
- [ ] Delete OR-specific batch files (3 files: `01_OR_*`, `04_OR_*`, `06_OR_*`)
- [ ] Delete OR-specific config (`01_OR_elev_param_config.yml`)
- [ ] Remove debug prints (`__file__`, `src_path`) from ssflux script
- [ ] Remove all `sys.path` manipulation from scripts
- [ ] Remove duplicate `load_config` definitions from: `7_add_nat_hru_id.py`, `process_NHD_by_vpu.py`, `process_slope_and_aspect.py`, and marimo notebooks (keep only in `gfv2_params/config.py`)
- [ ] Move ssflux magic numbers (`k_permMin`, `fluxParamsMax`, `fluxParamsMin`) to `ssflux_param.yml`
- [ ] Fix pyproject.toml author placeholder
- [ ] Remove duplicate `dask` from dependencies
- [ ] Remove `simple_parsing` from dependencies (unused — all scripts use `argparse`)
- [ ] Replace all `print()` with `logger.*()` calls
- [ ] Replace `zonal_engine="parallel"` with `zonal_engine="exactextract"`
- [ ] Rename `marimo/` to `notebooks/`
- [ ] Update README with new structure and setup instructions
- [ ] Clean up any existing `*.egg-info` directories, add to `.gitignore`
- [ ] Rename/keep `*_update.batch` single-VPU rerun scripts (update names to match new conventions)
- [ ] Fix SLURM array range bug: batch files using detailed VPU scheme (21 entries) should use `--array=0-20`, not `--array=0-17`

**Note on dependencies:** `gdal`, `rasterio`, `richdem`, and other compiled libraries are installed via conda (`environment.yml`) and intentionally omitted from `pyproject.toml` to avoid pip build failures. They are available at runtime because `pip install -e .` runs inside the conda environment.

## File Mapping (old -> new)

| Old | New |
|-----|-----|
| `src/helpers.py` | `src/gfv2_params/raster_ops.py` + `src/gfv2_params/config.py` |
| `src/download_rpu_rasters.py` | `src/gfv2_params/download/rpu_rasters.py` |
| `src/download_mrlc_fract_impervious_rasters.py` | `src/gfv2_params/download/mrlc_impervious.py` |
| `scripts/1_create_dem_params.py` | `scripts/create_zonal_params.py` |
| `scripts/4_create_soils_params.py` | `scripts/create_soils_params.py` |
| `scripts/6_create_ssflux_params.py` | `scripts/create_ssflux_params.py` |
| `scripts/process_NHD_by_vpu.py` | `scripts/merge_rpu_by_vpu.py` |
| `scripts/process_slope_and_aspect.py` | `scripts/compute_slope_aspect.py` |
| `scripts/7_add_nat_hru_id.py` | `scripts/merge_params.py` |
| `scripts/8_add_nat_hru_id_default_nhru.py` | `scripts/merge_default_params.py` |
| `scripts/merge_vpu_and_fill_params.py` | `scripts/merge_and_fill_params.py` |
| `scripts/find_missing_hru_ids.py` | `scripts/find_missing_hru_ids.py` |
| `configs/01_elev_param_config.yml` | `configs/elev_param.yml` |
| `configs/02_slope_param_config.yml` | `configs/slope_param.yml` |
| `configs/03_aspect_param_config.yml` | `configs/aspect_param.yml` |
| `configs/04_soils_param_config.yml` | `configs/soils_param.yml` |
| `configs/05_soilmoistmax_param_config.yml` | `configs/soilmoistmax_param.yml` |
| `configs/06_ssflux_param_config.yml` | `configs/ssflux_param.yml` |
| `configs/config_merge_rpu_by_vpu.yml` | `configs/merge_rpu_by_vpu.yml` |
| `configs/config_slope_aspect.yml` | `configs/slope_aspect.yml` |
| `marimo/` | `notebooks/` |

## Non-Goals

- No master CLI / click/typer framework — scripts stay as standalone `python scripts/foo.py` invocations
- No refactoring of the processing logic itself (zonal stats, weighting, KNN fill) — only structural/organizational changes
- No changes to SLURM resource allocations (memory, CPUs, time limits)
- No notebook cleanup beyond renaming the directory and updating `from helpers import ...` to `from gfv2_params.config import ...` (since the old import path will no longer exist)
