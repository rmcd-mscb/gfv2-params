# gfv2-params Repository Restructure Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure gfv2-params into an installable Python package with config-driven processing, proper logging, and support for custom fabric polygons alongside VPU-based CONUS processing.

**Architecture:** Convert `src/` into an installable `gfv2_params` package with modules for config loading/VPU resolution, raster operations, and logging. Scripts become thin CLI wrappers that import from the package. A `base_config.yml` provides the data root path, and per-step configs use `{data_root}` template placeholders resolved at load time. When `--vpu` is passed, additional `{vpu}` and `{raster_vpu}` placeholders are resolved. When omitted, configs must contain explicit paths for custom fabric processing.

**Tech Stack:** Python 3.12, hatchling (build), gdptools, GDAL/rasterio (conda), exactextract, logging, tqdm, SLURM

**Spec:** `docs/superpowers/specs/2026-03-23-repo-restructure-design.md`

---

## File Structure

### New files to create
- `src/gfv2_params/__init__.py` — package marker with version
- `src/gfv2_params/config.py` — `load_config()`, VPU constants, `resolve_vpu()`
- `src/gfv2_params/raster_ops.py` — `resample()`, `mult_rasters()`, `deg_to_fraction()`
- `src/gfv2_params/log.py` — `configure_logging()`
- `src/gfv2_params/download/__init__.py` — subpackage marker
- `src/gfv2_params/download/rpu_rasters.py` — from `src/download_rpu_rasters.py`
- `src/gfv2_params/download/mrlc_impervious.py` — from `src/download_mrlc_fract_impervious_rasters.py`
- `configs/base_config.yml` — data root and shared settings
- `tests/test_config.py` — tests for config loading and VPU resolution
- `tests/test_raster_ops.py` — tests for raster utility functions
- `tests/test_log.py` — tests for logging setup

### Files to rewrite (new name, updated content)
- `scripts/create_zonal_params.py` — from `scripts/1_create_dem_params.py`
- `scripts/create_soils_params.py` — from `scripts/4_create_soils_params.py`
- `scripts/create_ssflux_params.py` — from `scripts/6_create_ssflux_params.py`
- `scripts/merge_rpu_by_vpu.py` — from `scripts/process_NHD_by_vpu.py`
- `scripts/compute_slope_aspect.py` — from `scripts/process_slope_and_aspect.py`
- `scripts/merge_params.py` — from `scripts/7_add_nat_hru_id.py`
- `scripts/merge_default_params.py` — from `scripts/8_add_nat_hru_id_default_nhru.py`
- `scripts/merge_and_fill_params.py` — from `scripts/merge_vpu_and_fill_params.py`
- `scripts/find_missing_hru_ids.py` — updated to use base_config

### Config files to rewrite (renamed + template placeholders)
- `configs/elev_param.yml` — from `configs/01_elev_param_config.yml`
- `configs/slope_param.yml` — from `configs/02_slope_param_config.yml`
- `configs/aspect_param.yml` — from `configs/03_aspect_param_config.yml`
- `configs/soils_param.yml` — from `configs/04_soils_param_config.yml`
- `configs/soilmoistmax_param.yml` — from `configs/05_soilmoistmax_param_config.yml`
- `configs/ssflux_param.yml` — from `configs/06_ssflux_param_config.yml` (adds magic numbers)
- `configs/merge_rpu_by_vpu.yml` — from `configs/config_merge_rpu_by_vpu.yml`
- `configs/slope_aspect.yml` — from `configs/config_slope_aspect.yml`

### SLURM batch files to rewrite (renamed + updated paths)
- All batch files in `slurm_batch/` renamed and updated
- `slurm_batch/RUNME.md` rewritten

### Files to modify
- `pyproject.toml` — add `[build-system]`, fix deps, update metadata
- `environment.yml` — trim to compiled-only deps
- `.gitignore` — add `*.egg-info`
- `README.md` — new structure and setup instructions

### Files to delete
- `src/helpers.py`
- `src/__init__.py`
- `src/download_rpu_rasters.py`
- `src/download_mrlc_fract_impervious_rasters.py`
- `scripts/1_create_dem_params.py`
- `scripts/4_create_soils_params.py`
- `scripts/6_create_ssflux_params.py`
- `scripts/process_NHD_by_vpu.py`
- `scripts/process_slope_and_aspect.py`
- `scripts/7_add_nat_hru_id.py`
- `scripts/8_add_nat_hru_id_default_nhru.py`
- `scripts/merge_vpu_and_fill_params.py`
- `configs/01_elev_param_config.yml`
- `configs/01_OR_elev_param_config.yml`
- `configs/02_slope_param_config.yml`
- `configs/03_aspect_param_config.yml`
- `configs/04_soils_param_config.yml`
- `configs/05_soilmoistmax_param_config.yml`
- `configs/06_ssflux_param_config.yml`
- `configs/config_merge_rpu_by_vpu.yml`
- `configs/config_slope_aspect.yml`
- `slurm_batch/01_create_elev_params copy.batch`
- `slurm_batch/01_OR_create_elev_params.batch`
- `slurm_batch/04_OR_create_soils_params.batch`
- `slurm_batch/06_OR_create_ssflux_params.batch`
- All old-named batch files (replaced by new-named versions)

### Directory renames
- `marimo/` -> `notebooks/`

---

## Task 1: Create the installable package skeleton

**Files:**
- Create: `src/gfv2_params/__init__.py`
- Create: `src/gfv2_params/log.py`
- Create: `tests/test_log.py`
- Modify: `pyproject.toml`
- Modify: `environment.yml`
- Modify: `.gitignore`

This task establishes the package structure and build system so all subsequent tasks can import from `gfv2_params`.

- [ ] **Step 1: Create package directory structure**

```bash
mkdir -p src/gfv2_params/download
mkdir -p tests
```

- [ ] **Step 2: Create `src/gfv2_params/__init__.py`**

```python
__version__ = "0.1.0"
```

- [ ] **Step 3: Create `src/gfv2_params/download/__init__.py`**

Empty file.

- [ ] **Step 4: Create `src/gfv2_params/log.py`**

```python
import logging
import os


def configure_logging(name: str) -> logging.Logger:
    """Configure and return a logger.

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

- [ ] **Step 5: Write test for logging**

Create `tests/test_log.py`:

```python
import logging
import os

from gfv2_params.log import configure_logging


def test_configure_logging_returns_logger():
    logger = configure_logging("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    assert logger.propagate is False


def test_configure_logging_respects_env(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    logger = configure_logging("test_debug_logger")
    assert logger.level == logging.DEBUG


def test_configure_logging_no_duplicate_handlers():
    logger = configure_logging("test_dup_logger")
    configure_logging("test_dup_logger")
    assert len(logger.handlers) == 1
```

- [ ] **Step 6: Rewrite `pyproject.toml`**

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

- [ ] **Step 7: Rewrite `environment.yml`**

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

- [ ] **Step 8: Update `.gitignore`**

Add these lines to `.gitignore`:

```
*.egg-info/
```

- [ ] **Step 9: Run tests to verify package installs and logging works**

```bash
pip install -e ".[dev]"
pytest tests/test_log.py -v
```

Expected: all 3 tests PASS.

- [ ] **Step 10: Commit**

```bash
git add src/gfv2_params/__init__.py src/gfv2_params/download/__init__.py src/gfv2_params/log.py tests/test_log.py pyproject.toml environment.yml .gitignore
git commit -m "feat: create installable gfv2_params package with logging module"
```

---

## Task 2: Create config module with VPU resolution and template loading

**Files:**
- Create: `src/gfv2_params/config.py`
- Create: `configs/base_config.yml`
- Create: `tests/test_config.py`

- [ ] **Step 1: Create `configs/base_config.yml`**

```yaml
data_root: /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param
targets_dir: targets
output_dir: nhm_params
expected_max_hru_id: 361471
```

- [ ] **Step 2: Write tests for config module**

Create `tests/test_config.py`:

```python
import tempfile
from pathlib import Path

import yaml

from gfv2_params.config import (
    VPUS_DETAILED,
    VPUS_SIMPLE,
    VPU_RASTER_MAP,
    load_base_config,
    load_config,
    resolve_vpu,
)


def test_vpus_detailed_has_21_entries():
    assert len(VPUS_DETAILED) == 21


def test_vpus_simple_has_18_entries():
    assert len(VPUS_SIMPLE) == 18
    assert VPUS_SIMPLE[0] == "01"
    assert VPUS_SIMPLE[-1] == "18"


def test_resolve_vpu_standard():
    raster_vpu, gpkg_vpu = resolve_vpu("14")
    assert raster_vpu == "14"
    assert gpkg_vpu == "14"


def test_resolve_vpu_03N():
    raster_vpu, gpkg_vpu = resolve_vpu("03N")
    assert raster_vpu == "03"
    assert gpkg_vpu == "03N"


def test_resolve_vpu_10L():
    raster_vpu, gpkg_vpu = resolve_vpu("10L")
    assert raster_vpu == "10"
    assert gpkg_vpu == "10L"


def test_resolve_vpu_OR():
    raster_vpu, gpkg_vpu = resolve_vpu("OR")
    assert raster_vpu == "17"
    assert gpkg_vpu == "OR"


def test_load_config_without_vpu():
    """Config with explicit paths should work without --vpu."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "targets_dir": "targets",
            "output_dir": "nhm_params",
            "expected_max_hru_id": 100,
        }))

        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_type": "elevation",
            "source_raster": "/explicit/path/dem.tif",
            "target_gpkg": "/explicit/path/fabric.gpkg",
            "target_layer": "catchments",
            "id_feature": "catch_id",
            "output_dir": "/explicit/path/output",
            "categorical": False,
        }))

        config = load_config(step_config, vpu=None, base_config_path=base_config)
        assert config["source_raster"] == "/explicit/path/dem.tif"
        assert config["target_layer"] == "catchments"
        assert config["data_root"] == "/fake/root"


def test_load_config_with_vpu_resolves_placeholders():
    """Config with {data_root}, {vpu}, {raster_vpu} placeholders should resolve."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "targets_dir": "targets",
            "output_dir": "nhm_params",
            "expected_max_hru_id": 100,
        }))

        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_type": "elevation",
            "source_raster": "{data_root}/rasters/{raster_vpu}/dem_{raster_vpu}.tif",
            "target_gpkg": "{data_root}/targets/NHM_{vpu}_draft.gpkg",
            "target_layer": "nhru",
            "id_feature": "nat_hru_id",
            "output_dir": "{data_root}/nhm_params",
            "categorical": False,
        }))

        config = load_config(step_config, vpu="03N", base_config_path=base_config)
        assert config["source_raster"] == "/fake/root/rasters/03/dem_03.tif"
        assert config["target_gpkg"] == "/fake/root/targets/NHM_03N_draft.gpkg"
        assert config["output_dir"] == "/fake/root/nhm_params"


def test_load_base_config():
    """load_base_config returns only base config keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "targets_dir": "targets",
            "output_dir": "nhm_params",
            "expected_max_hru_id": 100,
        }))

        config = load_base_config(base_config)
        assert config["data_root"] == "/fake/root"
        assert config["expected_max_hru_id"] == 100
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest tests/test_config.py -v
```

Expected: FAIL — `gfv2_params.config` does not exist yet.

- [ ] **Step 4: Create `src/gfv2_params/config.py`**

```python
from pathlib import Path

import yaml

# Canonical VPU definitions
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

# Default base config location (relative to this file -> repo root)
_DEFAULT_BASE_CONFIG = Path(__file__).resolve().parent.parent.parent / "configs" / "base_config.yml"


def resolve_vpu(vpu: str) -> tuple[str, str]:
    """Return (raster_vpu, gpkg_vpu) for a given VPU code.

    For VPUs with sub-regions (03N/S/W, 10L/U), the raster VPU is the
    parent region while the geopackage VPU retains the sub-region suffix.
    """
    raster_vpu = VPU_RASTER_MAP.get(vpu, vpu)
    gpkg_vpu = vpu
    return raster_vpu, gpkg_vpu


def load_base_config(base_config_path: Path | None = None) -> dict:
    """Load only the base config (data_root, targets_dir, etc.).

    Use this when a script needs base paths but does not use a per-step
    YAML config (e.g., merge_and_fill_params, find_missing_hru_ids).
    """
    if base_config_path is None:
        base_config_path = _DEFAULT_BASE_CONFIG
    return _load_yaml(base_config_path)


def _load_yaml(path: Path) -> dict:
    """Load a YAML file and return its contents as a dict."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _resolve_placeholders(config: dict, replacements: dict) -> dict:
    """Resolve {placeholder} strings in config values."""
    resolved = {}
    for key, value in config.items():
        if isinstance(value, str):
            for placeholder, replacement in replacements.items():
                value = value.replace(f"{{{placeholder}}}", replacement)
        resolved[key] = value
    return resolved


def load_config(
    step_config_path: Path,
    vpu: str | None = None,
    base_config_path: Path | None = None,
) -> dict:
    """Load base config + step config, resolve placeholders.

    Parameters
    ----------
    step_config_path : Path
        Path to the per-step YAML config file.
    vpu : str, optional
        VPU code (e.g., "03N", "14"). When provided, resolves {data_root},
        {vpu}, and {raster_vpu} placeholders. When None, only {data_root}
        is resolved and all paths must be explicit in the config.
    base_config_path : Path, optional
        Path to base_config.yml. Defaults to configs/base_config.yml
        relative to the package installation.

    Returns
    -------
    dict
        Merged and resolved configuration dictionary. Contains all keys
        from both base and step configs, with base config values available
        as top-level keys (data_root, targets_dir, output_dir, etc.).
    """
    if base_config_path is None:
        base_config_path = _DEFAULT_BASE_CONFIG

    base = _load_yaml(base_config_path)
    step = _load_yaml(step_config_path)

    data_root = base["data_root"]

    # Build replacement map
    replacements = {"data_root": data_root}
    if vpu is not None:
        raster_vpu, gpkg_vpu = resolve_vpu(vpu)
        replacements["vpu"] = gpkg_vpu
        replacements["raster_vpu"] = raster_vpu

    # Resolve placeholders in step config
    resolved_step = _resolve_placeholders(step, replacements)

    # Merge: base config provides defaults, step config overrides
    merged = {**base, **resolved_step}

    # Add vpu to config if provided
    if vpu is not None:
        merged["vpu"] = vpu

    return merged
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_config.py -v
```

Expected: all 9 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/config.py configs/base_config.yml tests/test_config.py
git commit -m "feat: add config module with VPU resolution and template loading"
```

---

## Task 3: Create raster_ops module

**Files:**
- Create: `src/gfv2_params/raster_ops.py`
- Create: `tests/test_raster_ops.py`

- [ ] **Step 1: Write test for `deg_to_fraction`**

Create `tests/test_raster_ops.py`:

```python
import math

from gfv2_params.raster_ops import deg_to_fraction


def test_deg_to_fraction_zero():
    assert deg_to_fraction(0.0) == 0.0


def test_deg_to_fraction_45():
    result = deg_to_fraction(45.0)
    assert math.isclose(result, 1.0, rel_tol=1e-9)


def test_deg_to_fraction_30():
    result = deg_to_fraction(30.0)
    expected = math.tan(math.radians(30.0))
    assert math.isclose(result, expected, rel_tol=1e-9)
```

Note: `resample()` and `mult_rasters()` depend on GDAL/rasterio with real raster files. These are integration-level functions tested via the pipeline on HPC, not via unit tests.

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_raster_ops.py -v
```

Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Create `src/gfv2_params/raster_ops.py`**

Copy from `src/helpers.py` — the three functions `resample()`, `mult_rasters()`, `deg_to_fraction()` — with no changes to logic:

```python
from pathlib import Path

import numpy as np
import rasterio
from osgeo import gdal, gdalconst


def resample(
    src_path: str,
    template_path: str,
    intermediate_path: str,
    output_path: str,
    mask_values=(128, 0),
    mask_negative=True,
) -> None:
    """Reproject and resample src_path raster to match template_path's spatial reference.

    Writes the result to intermediate_path, applies NoData masking,
    and saves the final raster to output_path.
    """
    src = gdal.Open(src_path, gdalconst.GA_ReadOnly)
    if src is None:
        raise FileNotFoundError(f"Source raster not found: {src_path}")
    src_proj = src.GetProjection()

    tmpl = gdal.Open(template_path, gdalconst.GA_ReadOnly)
    if tmpl is None:
        raise FileNotFoundError(f"Template raster not found: {template_path}")
    tmpl_proj = tmpl.GetProjection()
    tmpl_geotrans = tmpl.GetGeoTransform()
    width = tmpl.RasterXSize
    height = tmpl.RasterYSize

    driver = gdal.GetDriverByName("GTiff")
    dst = driver.Create(intermediate_path, width, height, 1, gdalconst.GDT_Float32)
    dst.SetGeoTransform(tmpl_geotrans)
    dst.SetProjection(tmpl_proj)

    gdal.ReprojectImage(src, dst, src_proj, tmpl_proj, gdalconst.GRA_NearestNeighbour)
    del dst

    with rasterio.open(intermediate_path) as src_rio:
        data = src_rio.read(1)
        profile = src_rio.profile
        profile.update(dtype=rasterio.float64, count=1, compress="lzw")

        for val in mask_values:
            data[data == val] = np.nan
        if mask_negative:
            data[data < 0] = np.nan

        with rasterio.open(output_path, "w", **profile) as dst_rio:
            dst_rio.write(data.astype(rasterio.float64), 1)


def mult_rasters(
    rast1_path: str,
    rast2_path: str,
    out_path: str,
    nodata_value: float = None,
) -> None:
    """Multiply two single-band rasters and write the result.

    Handles NoData values. Assumes input rasters are aligned.
    """
    with rasterio.open(rast1_path) as src1, rasterio.open(rast2_path) as src2:
        if src1.shape != src2.shape:
            raise ValueError("Input rasters do not have the same shape.")
        if src1.transform != src2.transform:
            raise ValueError("Input rasters do not have the same geotransform.")
        if src1.crs != src2.crs:
            raise ValueError("Input rasters do not have the same CRS.")

        arr1 = src1.read(1).astype(np.float64)
        arr2 = src2.read(1).astype(np.float64)

        nodata1 = src1.nodata
        nodata2 = src2.nodata

        mask = np.full(arr1.shape, False, dtype=bool)
        if nodata1 is not None:
            mask |= arr1 == nodata1
        if nodata2 is not None:
            mask |= arr2 == nodata2

        result = np.where(~mask, arr1 * arr2, np.nan)

        profile = src1.profile.copy()
        profile.update(
            dtype=rasterio.float64,
            count=1,
            compress="lzw",
            nodata=nodata_value if nodata_value is not None else np.nan,
        )

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(result, 1)


def deg_to_fraction(slope_deg: float) -> float:
    """Convert slope from degrees to fractional slope (rise/run)."""
    return np.tan(np.deg2rad(slope_deg))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_raster_ops.py -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/raster_ops.py tests/test_raster_ops.py
git commit -m "feat: add raster_ops module with resample, mult_rasters, deg_to_fraction"
```

---

## Task 4: Rewrite per-step config files with template placeholders

**Files:**
- Create: `configs/elev_param.yml`
- Create: `configs/slope_param.yml`
- Create: `configs/aspect_param.yml`
- Create: `configs/soils_param.yml`
- Create: `configs/soilmoistmax_param.yml`
- Create: `configs/ssflux_param.yml`
- Create: `configs/slope_aspect.yml`
- Rename: `configs/config_merge_rpu_by_vpu.yml` -> `configs/merge_rpu_by_vpu.yml`
- Delete: all old `configs/0*` files, `configs/config_*` files, `configs/01_OR_*`

- [ ] **Step 1: Create `configs/elev_param.yml`**

```yaml
source_type: elevation
source_raster: "{data_root}/source_data/NHDPlus_Merged_Rasters/{raster_vpu}/NEDSnapshot_merged_fixed_{raster_vpu}.tif"
target_gpkg: "{data_root}/targets/NHM_{vpu}_draft.gpkg"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/nhm_params"
merged_file: nhm_elevation_params.csv
categorical: false
```

- [ ] **Step 2: Create `configs/slope_param.yml`**

```yaml
source_type: slope
source_raster: "{data_root}/source_data/NHDPlus_Merged_Rasters/{raster_vpu}/NEDSnapshot_merged_slope_{raster_vpu}.tif"
target_gpkg: "{data_root}/targets/NHM_{vpu}_draft.gpkg"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/nhm_params"
merged_file: nhm_slope_params.csv
categorical: false
```

- [ ] **Step 3: Create `configs/aspect_param.yml`**

```yaml
source_type: aspect
source_raster: "{data_root}/source_data/NHDPlus_Merged_Rasters/{raster_vpu}/NEDSnapshot_merged_aspect_{raster_vpu}.tif"
target_gpkg: "{data_root}/targets/NHM_{vpu}_draft.gpkg"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/nhm_params"
merged_file: nhm_aspect_params.csv
categorical: false
```

- [ ] **Step 4: Create `configs/soils_param.yml`**

```yaml
source_type: soils
source_raster: "{data_root}/source_data/data_layers/soils_litho/TEXT_PRMS.tif"
target_gpkg: "{data_root}/targets/NHM_{vpu}_draft.gpkg"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/nhm_params"
merged_file: nhm_soils_params.csv
categorical: true
```

- [ ] **Step 5: Create `configs/soilmoistmax_param.yml`**

```yaml
source_type: soil_moist_max
source_dir: "{data_root}/source_data/data_layers"
target_gpkg: "{data_root}/targets/NHM_{vpu}_draft.gpkg"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/nhm_params"
merged_file: nhm_soil_moist_max_params.csv
categorical: false
```

- [ ] **Step 6: Create `configs/ssflux_param.yml`**

This config absorbs the magic numbers that were previously hardcoded in the script:

```yaml
source_type: ssflux
source_shapefile: "{data_root}/source_data/data_layers/soils_litho/Lithology_exp_Konly_Project.shp"
target_gpkg: "{data_root}/targets/NHM_{vpu}_draft.gpkg"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/nhm_params"
weight_dir: "{data_root}/weights"
merged_file: nhm_ssflux_params.csv
categorical: false

# Lithology permeability constants (previously hardcoded in script)
k_perm_min: -16.48

# PRMS flux parameter normalization bounds
# Order: soil2gw_max, ssr2gw_rate, fastcoef_lin, slowcoef_lin,
#         gwflow_coef, dprst_seep_rate_open, dprst_flow_coef
flux_params:
  - name: soil2gw_max
    min: 0.1
    max: 0.3
  - name: ssr2gw_rate
    min: 0.3
    max: 0.7
  - name: fastcoef_lin
    min: 0.01
    max: 0.6
  - name: slowcoef_lin
    min: 0.005
    max: 0.3
  - name: gwflow_coef
    min: 0.005
    max: 0.3
  - name: dprst_seep_rate_open
    min: 0.005
    max: 0.2
  - name: dprst_flow_coef
    min: 0.005
    max: 0.5
```

- [ ] **Step 7: Create `configs/slope_aspect.yml`**

```yaml
input_dir: "{data_root}/source_data/NHDPlus_Merged_Rasters"
output_dir: "{data_root}/source_data/NHDPlus_Merged_Rasters"
```

- [ ] **Step 8: Rename `configs/config_merge_rpu_by_vpu.yml` to `configs/merge_rpu_by_vpu.yml`**

```bash
git mv configs/config_merge_rpu_by_vpu.yml configs/merge_rpu_by_vpu.yml
```

No content changes needed — this config's nested structure is kept as-is.

- [ ] **Step 9: Delete old config files**

```bash
git rm configs/01_elev_param_config.yml
git rm configs/01_OR_elev_param_config.yml
git rm configs/02_slope_param_config.yml
git rm configs/03_aspect_param_config.yml
git rm configs/04_soils_param_config.yml
git rm configs/05_soilmoistmax_param_config.yml
git rm configs/06_ssflux_param_config.yml
git rm configs/config_slope_aspect.yml
```

- [ ] **Step 10: Commit**

```bash
git add configs/
git commit -m "feat: rewrite configs with template placeholders, move ssflux magic numbers to config"
```

---

## Task 5: Rewrite processing scripts

**Files:**
- Create: `scripts/create_zonal_params.py` (from `scripts/1_create_dem_params.py`)
- Create: `scripts/create_soils_params.py` (from `scripts/4_create_soils_params.py`)
- Create: `scripts/create_ssflux_params.py` (from `scripts/6_create_ssflux_params.py`)
- Create: `scripts/merge_rpu_by_vpu.py` (from `scripts/process_NHD_by_vpu.py`)
- Create: `scripts/compute_slope_aspect.py` (from `scripts/process_slope_and_aspect.py`)
- Delete: old script files

Each script is rewritten to:
1. Import from `gfv2_params` (no `sys.path` hacking)
2. Use `load_config(config_path, vpu)` for config loading
3. Use `configure_logging()` instead of `print()`
4. Use `zonal_engine="exactextract"` instead of `"parallel"`
5. Read `target_layer` and `id_feature` from config instead of hardcoding

- [ ] **Step 1: Create `scripts/create_zonal_params.py`**

This replaces `scripts/1_create_dem_params.py`. Handles elevation, slope, and aspect via config:

```python
"""Create zonal parameters (elevation, slope, aspect) from rasters by HRU polygon."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Create zonal parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", default=None, help="VPU code (e.g., 01, 03N). Omit for custom fabrics.")
    args = parser.parse_args()

    logger = configure_logging("create_zonal_params")

    config = load_config(Path(args.config), vpu=args.vpu)
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]

    # Resolve paths
    raster_path = Path(config["source_raster"])
    gpkg_path = Path(config["target_gpkg"])
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")

    logger.info("Raster: %s", raster_path)
    logger.info("GPKG: %s", gpkg_path)

    # Load target polygons
    nhru_gdf = gpd.read_file(gpkg_path, layer=target_layer)
    logger.info("Loaded %s layer: %d features", target_layer, len(nhru_gdf))

    # Load raster
    ned_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", ned_da.shape, ned_da.rio.crs)

    # Build file prefix for output
    vpu_label = args.vpu if args.vpu else "custom"
    file_prefix = f"base_nhm_{source_type}_{vpu_label}_param"

    # Create zonal stats
    data = UserTiffData(
        var=source_type,
        ds=ned_da,
        proj_ds=ned_da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics complete. Shape: %s", stats.shape)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create `scripts/create_soils_params.py`**

Rewrite of `scripts/4_create_soils_params.py`. Key changes: imports from `gfv2_params`, uses config for paths/layer/id_feature, logging, exactextract. Processing logic unchanged.

```python
"""Create soils and soil_moist_max parameters from raster data."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import mult_rasters, resample


def process_soils(source_da, nhru_gdf, output_path, source_type, vpu_label, categorical, id_feature, logger):
    """Process categorical soils data: zonal stats -> dominant category -> CSV."""
    logger.info("Loaded raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    file_prefix = f"base_nhm_{source_type}_{vpu_label}_param_temp"

    data = UserTiffData(
        var="soils",
        ds=source_da,
        proj_ds=source_da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed")

    # Remove temp file
    zg_file = output_path / f"{file_prefix}.csv"
    if zg_file.exists():
        zg_file.unlink()

    # Dominant category per feature
    category_cols = [col for col in stats.columns if str(col) not in ("count",)]
    top_stats = stats.copy()
    top_stats["max_category"] = top_stats[category_cols].idxmax(axis=1)
    result = top_stats[["max_category"]].rename(columns={"max_category": "soils"})
    result.sort_index(inplace=True)

    result_csv = output_path / f"base_nhm_{source_type}_{vpu_label}_param.csv"
    result.to_csv(result_csv)
    logger.info("Final soils parameters saved to: %s", result_csv)


def process_soil_moist_max(source_dir, nhru_gdf, output_path, source_type, vpu_label, categorical, id_feature, logger):
    """Process soil_moist_max: resample root depth, multiply by AWC, zonal mean."""
    soil_moist_max_rast = source_dir / "soils_litho/soil_moist_max.tif"
    rd_rast = source_dir / "lulc_veg/RootDepth.tif"
    awc_rast = source_dir / "soils_litho/AWC.tif"
    temp_rast = source_dir / "lulc_veg/rd_250_raw.tif"
    final_rast = source_dir / "lulc_veg/rd_250_raw.tif"

    if not rd_rast.exists():
        raise FileNotFoundError(f"Root Depth raster not found: {rd_rast}")
    if not awc_rast.exists():
        raise FileNotFoundError(f"AWC raster not found: {awc_rast}")

    if not final_rast.exists():
        resample(str(rd_rast), str(awc_rast), str(temp_rast), str(final_rast))
    if not soil_moist_max_rast.exists():
        mult_rasters(str(final_rast), str(awc_rast), str(soil_moist_max_rast))

    source_da = rioxarray.open_rasterio(soil_moist_max_rast)
    logger.info("Loaded soil_moist_max raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    file_prefix = f"base_nhm_{source_type}_{vpu_label}_param_temp"

    data = UserTiffData(
        var=source_type,
        ds=source_da,
        proj_ds=source_da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_path,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed for soil_moist_max")

    mean_stats = stats[["mean"]].rename(columns={"mean": "soil_moist_max"})
    result_csv = output_path / f"base_nhm_{source_type}_{vpu_label}_param.csv"
    mean_stats.to_csv(result_csv)
    logger.info("Final soil_moist_max parameters saved to: %s", result_csv)


def main():
    parser = argparse.ArgumentParser(description="Create soils parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", default=None, help="VPU code (e.g., 01, 03N). Omit for custom fabrics.")
    args = parser.parse_args()

    logger = configure_logging("create_soils_params")

    config = load_config(Path(args.config), vpu=args.vpu)
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]

    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    gpkg_path = Path(config["target_gpkg"])
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")
    nhru_gdf = gpd.read_file(gpkg_path, layer=target_layer)
    logger.info("Loaded %s layer from %s: %d features", target_layer, gpkg_path, len(nhru_gdf))

    vpu_label = args.vpu if args.vpu else "custom"

    if source_type == "soils":
        raster_path = Path(config["source_raster"])
        if not raster_path.exists():
            raise FileNotFoundError(f"Input raster not found: {raster_path}")
        logger.info("Processing soils data using raster: %s", raster_path)
        source_da = rioxarray.open_rasterio(raster_path)
        process_soils(source_da, nhru_gdf, output_dir, source_type, vpu_label, categorical, id_feature, logger)

    elif source_type == "soil_moist_max":
        source_dir = Path(config["source_dir"])
        logger.info("Processing soil_moist_max data from: %s", source_dir)
        process_soil_moist_max(source_dir, nhru_gdf, output_dir, source_type, vpu_label, categorical, id_feature, logger)

    else:
        raise ValueError(f"Unknown source_type: {source_type}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Create `scripts/create_ssflux_params.py`**

Rewrite of `scripts/6_create_ssflux_params.py`. Key changes: magic numbers from config, imports from `gfv2_params`, logging. Processing logic unchanged.

```python
"""Create subsurface flux parameters using litho-weighted approach."""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from gdptools import WeightGenP2P

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import deg_to_fraction


def main():
    parser = argparse.ArgumentParser(description="Create ssflux parameters.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", default=None, help="VPU code (e.g., 01, 03N). Omit for custom fabrics.")
    args = parser.parse_args()

    logger = configure_logging("create_ssflux_params")

    config = load_config(Path(args.config), vpu=args.vpu)
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    output_dir = Path(config["output_dir"])
    weight_dir = Path(config["weight_dir"])
    weight_dir.mkdir(parents=True, exist_ok=True)

    vpu_label = args.vpu if args.vpu else "custom"

    # Load target geopackage
    target_gdf_path = Path(config["target_gpkg"])
    if not target_gdf_path.exists():
        raise FileNotFoundError(f"GPKG not found: {target_gdf_path}")
    target_gdf = gpd.read_file(target_gdf_path, layer=target_layer)
    logger.info("Loaded %s layer from %s: %d features", target_layer, target_gdf_path, len(target_gdf))

    # Load source lithology
    source_gdf = gpd.read_file(Path(config["source_shapefile"]))

    # Load slope params
    slope_source_gdf = gpd.read_file(output_dir / f"slope/base_nhm_slope_{vpu_label}_param.csv")
    slope_source_gdf["mean_slope_fraction"] = slope_source_gdf["mean"].astype(float).apply(deg_to_fraction)
    source_gdf["flux_id"] = np.arange(len(source_gdf))

    # Calculate or load weights
    weight_file = weight_dir / f"lith_weights_vpu_{vpu_label}.csv"
    if not weight_file.exists():
        weight_gen = WeightGenP2P(
            target_poly=target_gdf,
            target_poly_idx=id_feature,
            source_poly=source_gdf,
            source_poly_idx="flux_id",
            method="serial",
            weight_gen_crs="5070",
            output_file=weight_file,
        )
        weights = weight_gen.calculate_weights()
    else:
        weights = pd.read_csv(weight_file)
    logger.info("Weights loaded/calculated")

    # Merge weights with source attributes
    weights["flux_id"] = weights["flux_id"].astype(str)
    source_gdf["flux_id"] = source_gdf["flux_id"].astype(str)

    w = weights.merge(source_gdf[["flux_id", "k_perm"]], on="flux_id")
    logger.info("Zeros in k_perm: %d", (w["k_perm"] == 0).sum())

    # Replace zeros with config-driven minimum
    k_perm_min = config["k_perm_min"]
    w["k_perm"] = w["k_perm"].replace(0, k_perm_min)
    w["k_perm_actual"] = 10 ** w["k_perm"]

    # Extensive variable aggregation
    w["k_perm_wtd_sum"] = w["k_perm_actual"] * (w["area_weight"] / w["flux_id_area"])

    extensive_agg = (
        w.groupby(id_feature)
        .agg(k_perm_wtd=("k_perm_wtd_sum", "sum"))
        .reset_index()
    )
    extensive_agg[id_feature] = extensive_agg[id_feature].astype(int)
    extensive_sorted = extensive_agg.sort_values(by=id_feature, ascending=True).reset_index(drop=True)

    # Merge with slope and area
    slope_df = slope_source_gdf[[id_feature, "mean_slope_fraction"]].copy()
    slope_df[id_feature] = pd.to_numeric(slope_df[id_feature], errors="coerce").astype("int64")

    target_gdf["hru_area"] = target_gdf.geometry.area
    area_df = target_gdf[[id_feature, "hru_area"]].copy()
    area_df[id_feature] = pd.to_numeric(area_df[id_feature], errors="coerce").astype("int64")

    df = extensive_sorted.merge(slope_df, on=id_feature, how="left").copy()
    df = df.merge(area_df, on=id_feature, how="left")

    # Compute raw PRMS fluxes
    df["r_soil2gw_max"] = df["k_perm_wtd"] ** 3
    df["r_ssr2gw_rate"] = df["k_perm_wtd"] * (1 - df["mean_slope_fraction"])
    df["r_slowcoef_lin"] = (df["k_perm_wtd"] * df["mean_slope_fraction"]) / df["hru_area"]
    df["r_fastcoef_lin"] = 2 * df["r_slowcoef_lin"]
    df["r_gwflow_coef"] = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"] = df["r_fastcoef_lin"]

    # Normalize using config-driven bounds
    flux_params = config["flux_params"]
    param_names = [fp["name"] for fp in flux_params]
    param_maxes = [fp["max"] for fp in flux_params]
    param_mins = [fp["min"] for fp in flux_params]

    df_r = df[[f"r_{p}" for p in param_names]].agg(["min", "max"])
    df_r.loc["range"] = df_r.loc["max"] - df_r.loc["min"]

    for i, p in enumerate(param_names):
        rcol = f"r_{p}"
        min_in, rng_in = df_r.at["min", rcol], df_r.at["range", rcol]
        min_out, max_out = param_mins[i], param_maxes[i]
        rng_out = max_out - min_out
        norm = (df[rcol] - min_in) / rng_in
        df[p] = norm * rng_out + min_out

    # Drop intermediate columns
    df.drop(columns=[f"r_{p}" for p in param_names], inplace=True)

    ssflux_dir = output_dir / "ssflux"
    ssflux_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(ssflux_dir / f"base_nhm_{source_type}_{vpu_label}_param.csv", index=False)
    logger.info("SSFlux parameters saved")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Create `scripts/merge_rpu_by_vpu.py`**

Rewrite of `scripts/process_NHD_by_vpu.py`. Key changes: removes hardcoded `base_path`, uses `load_config` for `data_root`, logging.

```python
"""Merge Regional Processing Unit (RPU) rasters by VPU and dataset type."""

import argparse
from pathlib import Path

import rioxarray as rxr
import yaml
from rioxarray.merge import merge_arrays

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Merge NHD rasters by VPU and dataset type.")
    parser.add_argument("--config", required=True, help="Path to merge_rpu_by_vpu.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    args = parser.parse_args()

    logger = configure_logging("merge_rpu_by_vpu")

    # Load base config for data_root (this config has a unique nested structure,
    # so we load it separately via yaml rather than through load_config)
    base = load_base_config()
    base_path = Path(base["data_root"])

    # Load the RPU merge config (VPU-keyed nested structure)
    with open(args.config, "r") as f:
        rpu_config = yaml.safe_load(f)

    vpu_config = rpu_config.get(args.vpu)
    if vpu_config is None:
        raise ValueError(f"VPU {args.vpu} not found in config.")

    for dataset_name, values in vpu_config.items():
        logger.info("Dataset: %s", dataset_name)
        rpus = values.get("rpus", [])
        output_file = values.get("output")
        output = base_path / output_file.lstrip("/")

        if output.exists():
            logger.info("Output already exists, skipping: %s", output)
            continue

        datasets = []
        for d in rpus:
            d = base_path / d.lstrip("/")
            logger.info("Reading raster from: %s", d)
            if not d.exists():
                raise FileNotFoundError(f"Input raster folder not found: {d}")
            if not (d / "hdr.adf").exists():
                raise ValueError(f"Folder {d} does not appear to be a valid ESRI Grid raster")
            ds = rxr.open_rasterio(str(d), masked=True).squeeze()
            datasets.append(ds)

        logger.info("Merging %d datasets", len(datasets))
        if len(datasets) == 1:
            merged = datasets[0]
        else:
            if dataset_name in ("NEDSnapshot", "Hydrodem"):
                merged = merge_arrays(datasets, method="min")
            else:
                merged = merge_arrays(datasets, method="first")

        crs_set = {ds.rio.crs.to_string() for ds in datasets}
        if len(crs_set) > 1:
            raise ValueError(f"Inconsistent CRS among inputs: {crs_set}")

        match dataset_name:
            case "NEDSnapshot":
                nodata_val = -9999
                merged = merged.astype("float32")
                merged = merged.where(~merged.isnull(), nodata_val)
                merged = merged / 100.0
                merged.rio.write_nodata(nodata_val, inplace=True)
                logger.info("Converted NEDSnapshot from centimeters to meters.")

            case "Hydrodem":
                nodata_val = -9999
                merged = merged.astype("float32")
                merged = merged.where(~merged.isnull(), nodata_val)
                merged = merged / 100.0
                logger.info("Converted Hydrodem from centimeters to meters.")

            case "FdrFac_Fdr":
                nodata_val = 255
                merged = merged.fillna(nodata_val).astype("uint8")

            case "FdrFac_Fac":
                nodata_val = -9999
                merged = merged.fillna(nodata_val).astype("int32")

            case _:
                raise ValueError(f"Unknown dataset_name: {dataset_name}")

        logger.info("Writing raster: %s", output)
        output.parent.mkdir(parents=True, exist_ok=True)

        merged.rio.write_crs(datasets[0].rio.crs, inplace=True)
        merged.rio.write_nodata(nodata_val, inplace=True)

        match dataset_name:
            case "NEDSnapshot" | "Hydrodem":
                merged.rio.to_raster(output, compress="lzw", predictor=2, tiled=True, blockxsize=512, blockysize=512)
            case "FdrFac_Fdr" | "FdrFac_Fac":
                merged.rio.to_raster(output, compress="lzw", tiled=True, blockxsize=512, blockysize=512)

        logger.info("Wrote raster: %s", output)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Create `scripts/compute_slope_aspect.py`**

Rewrite of `scripts/process_slope_and_aspect.py`:

```python
"""Compute slope and aspect rasters from a DEM using richdem."""

import argparse
from pathlib import Path

import richdem as rd
import rioxarray

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Compute slope and aspect rasters from DEM.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    args = parser.parse_args()

    logger = configure_logging("compute_slope_aspect")

    config = load_config(Path(args.config), vpu=args.vpu)
    input_dir = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    dem_path = input_dir / args.vpu / f"NEDSnapshot_merged_{args.vpu}.tif"
    dem_fixed_path = input_dir / args.vpu / f"NEDSnapshot_merged_fixed_{args.vpu}.tif"
    if not dem_path.exists():
        raise FileNotFoundError(f"DEM not found: {dem_path}")

    logger.info("Creating fixed nodata NEDSnapshot")
    da = rioxarray.open_rasterio(dem_path, masked=True).squeeze()
    da_fixed = da.fillna(-9999)
    da_fixed.rio.write_nodata(-9999, inplace=True)
    da_fixed.rio.to_raster(dem_fixed_path)

    logger.info("Loading DEM: %s", dem_path)
    dem = rd.LoadGDAL(str(dem_path), no_data=-9999)

    logger.info("Computing slope (degrees)...")
    slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
    slope_out = output_dir / args.vpu / f"NEDSnapshot_merged_slope_{args.vpu}.tif"
    slope_out.parent.mkdir(parents=True, exist_ok=True)
    rd.SaveGDAL(str(slope_out), slope)
    logger.info("Slope raster saved to: %s", slope_out)

    logger.info("Computing aspect...")
    aspect = rd.TerrainAttribute(dem, attrib="aspect")
    aspect_out = output_dir / args.vpu / f"NEDSnapshot_merged_aspect_{args.vpu}.tif"
    rd.SaveGDAL(str(aspect_out), aspect)
    logger.info("Aspect raster saved to: %s", aspect_out)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Delete old script files**

```bash
git rm scripts/1_create_dem_params.py
git rm scripts/4_create_soils_params.py
git rm scripts/6_create_ssflux_params.py
git rm scripts/process_NHD_by_vpu.py
git rm scripts/process_slope_and_aspect.py
```

- [ ] **Step 7: Commit**

```bash
git add scripts/create_zonal_params.py scripts/create_soils_params.py scripts/create_ssflux_params.py scripts/merge_rpu_by_vpu.py scripts/compute_slope_aspect.py
git commit -m "feat: rewrite processing scripts to use gfv2_params package, config templates, logging, exactextract"
```

---

## Task 6: Rewrite merge/fill scripts

**Files:**
- Create: `scripts/merge_params.py` (from `scripts/7_add_nat_hru_id.py`)
- Create: `scripts/merge_default_params.py` (from `scripts/8_add_nat_hru_id_default_nhru.py`)
- Create: `scripts/merge_and_fill_params.py` (from `scripts/merge_vpu_and_fill_params.py`)
- Modify: `scripts/find_missing_hru_ids.py`
- Delete: old script files

- [ ] **Step 1: Create `scripts/merge_params.py`**

Rewrite of `scripts/7_add_nat_hru_id.py`. Removes local `load_config`, uses package import, logging. Removes commented-out code.

```python
"""Merge per-VPU parameter CSVs into a single file, sorted by nat_hru_id."""

import argparse
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def process_files(config, logger):
    input_dir = Path(config["output_dir"]) / config["source_type"]
    source_type = config["source_type"]
    merged_file = Path(config["merged_file"])
    final_output_dir = Path(config["output_dir"]) / "nhm_params_merged"
    final_output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    file_pattern = f"base_nhm_{source_type}_*_param.csv"
    files = sorted(input_dir.glob(file_pattern), key=lambda f: f.stem.split("_")[3])

    merged_df = pd.DataFrame()

    for file in files:
        logger.info("Processing file: %s", file)
        df = pd.read_csv(file)

        if "nat_hru_id" not in df.columns:
            raise ValueError(f"'nat_hru_id' column not found in file: {file}")

        df = df.sort_values("nat_hru_id")
        vpu = file.stem.split("_")[3]
        df["vpu"] = vpu

        logger.info("vpu: %s, num_hru: %d", vpu, len(df))
        merged_df = pd.concat([merged_df, df], ignore_index=True)
        merged_df = merged_df.sort_values("nat_hru_id")

    merged_df.to_csv(final_output_dir / merged_file, index=False)
    logger.info("Merged file saved to: %s", final_output_dir / merged_file)


def main():
    parser = argparse.ArgumentParser(description="Merge per-VPU parameter CSVs.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    logger = configure_logging("merge_params")
    config = load_config(Path(args.config))
    process_files(config, logger)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create `scripts/merge_default_params.py`**

Rewrite of `scripts/8_add_nat_hru_id_default_nhru.py`. Uses `load_config` for base path defaults, logging. Logic unchanged.

```python
"""Merge NHM default parameter tables to nat_hru_id."""

import argparse
import re
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def load_param_dict(dict_file, logger):
    param_df = pd.read_csv(dict_file)
    nhru_params = param_df[param_df["Dimensions"] == "nhru"]["Parameter"].tolist()
    logger.info("Found %d parameters with 'nhru' dimension", len(nhru_params))
    return nhru_params


def find_param_files(param_name, base_dir):
    all_dirs = [d for d in base_dir.glob("*") if d.is_dir() and d.name != "rOR"]
    matching_files = []
    for directory in all_dirs:
        matches = list(directory.glob(f"{param_name}*.csv"))
        matching_files.extend(matches)

    def vpu_sort_key(filepath):
        folder = filepath.parent.name
        match = re.match(r"r(\d+)([a-zA-Z]*)", folder)
        if match:
            num = int(match.group(1))
            suffix = match.group(2).lower() if match.group(2) else ""
            return (num, suffix)
        return folder

    return sorted(matching_files, key=vpu_sort_key)


def merge_param_files(param_name, files, output_dir, logger):
    if not files:
        logger.warning("No files found for parameter: %s", param_name)
        return None

    logger.info("Merging %d files for parameter: %s", len(files), param_name)

    cumulative_offset = 0
    merged_df = pd.DataFrame()

    for file in files:
        logger.debug("Processing file: %s", file)
        df = pd.read_csv(file)

        if "hru_id" not in df.columns and "$id" in df.columns:
            df = df.rename(columns={"$id": "hru_id"})

        if "hru_id" not in df.columns:
            logger.warning("'hru_id' column not found in file: %s", file)
            continue

        folder = file.parent.name
        vpu_match = re.match(r"r(\d+)([a-zA-Z]*)", folder)
        if vpu_match:
            num = vpu_match.group(1)
            suffix = vpu_match.group(2).lower() if vpu_match.group(2) else ""
            vpu = f"{num}{suffix}"
        else:
            vpu = "unknown"

        df["nat_hru_id"] = df["hru_id"] + cumulative_offset
        df["vpu"] = vpu
        cumulative_offset += len(df["hru_id"])

        merged_df = pd.concat([merged_df, df], ignore_index=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    merged_file = output_dir / f"{param_name}_merged.csv"
    merged_df.to_csv(merged_file, index=False)
    logger.info("Merged file saved to: %s", merged_file)
    return merged_file


def main():
    parser = argparse.ArgumentParser(description="Merge default parameter files by nat_hru_id.")
    parser.add_argument("--dict", required=True, help="Path to parameter dictionary CSV file")
    parser.add_argument("--base_dir", default=None, help="Base directory containing parameter files")
    parser.add_argument("--output_dir", default=None, help="Output directory for merged files")
    args = parser.parse_args()

    logger = configure_logging("merge_default_params")

    # Load base config after argparse so --help works without config file
    base = load_base_config()
    data_root = base["data_root"]

    if args.base_dir is None:
        args.base_dir = f"{data_root}/nhm_params/default"
    if args.output_dir is None:
        args.output_dir = f"{data_root}/nhm_params/merged"

    dict_file = Path(args.dict)
    base_dir = Path(args.base_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    nhru_params = load_param_dict(dict_file, logger)

    for param_name in nhru_params:
        logger.info("Processing parameter: %s", param_name)
        param_files = find_param_files(param_name, base_dir)

        if param_files:
            logger.info("Found %d files for %s", len(param_files), param_name)
            merge_param_files(param_name, param_files, output_dir, logger)
        else:
            logger.warning("No files found for parameter: %s", param_name)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Create `scripts/merge_and_fill_params.py`**

Rewrite of `scripts/merge_vpu_and_fill_params.py`. Uses `load_config` for default paths, logging, tqdm. Removes commented-out code.

```python
"""Merge VPU geopackages and fill missing parameter values using KNN interpolation."""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from gfv2_params.config import VPUS_DETAILED, load_base_config
from gfv2_params.log import configure_logging


def merge_vpu_geopackages(targets_dir, vpus, output_file, simplify_tolerance, logger):
    logger.info("Merging VPU geopackages...")
    merged_gdfs = []

    for vpu in tqdm(vpus, desc="Merging VPU geopackages"):
        gpkg_file = targets_dir / f"NHM_{vpu}_draft.gpkg"
        if not gpkg_file.exists():
            logger.warning("%s not found, skipping...", gpkg_file)
            continue

        gdf = gpd.read_file(gpkg_file, layer="nhru")

        if simplify_tolerance > 0:
            gdf["geometry"] = gdf.apply(
                lambda row: row["geometry"].simplify(tolerance=simplify_tolerance, preserve_topology=True)
                if row["geometry"].area > simplify_tolerance * 10
                else row["geometry"],
                axis=1,
            )

        merged_gdfs.append(gdf)
        logger.debug("Added %d features from VPU %s", len(gdf), vpu)

    merged_gdf = pd.concat(merged_gdfs, ignore_index=True)
    merged_gdf = merged_gdf.sort_values("nat_hru_id").reset_index(drop=True)

    logger.info("Saving merged geopackage with %d features to: %s", len(merged_gdf), output_file)
    merged_gdf.to_file(output_file, driver="GPKG", layer="nhru")
    return merged_gdf


def find_missing_ids(param_file, expected_max, logger):
    logger.info("Finding missing nat_hru_id values...")
    param_df = pd.read_csv(param_file)
    existing_ids = set(param_df["nat_hru_id"])
    expected_ids = set(range(1, expected_max + 1))
    missing_ids = sorted(expected_ids - existing_ids)
    logger.info("Found %d missing nat_hru_id values out of %d", len(missing_ids), expected_max)
    return param_df, missing_ids


def fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_column, k, logger):
    logger.info("Filling missing values using KNN interpolation (k=%d)...", k)

    if not missing_ids:
        logger.info("No missing values to fill!")
        return param_df

    merged_gdf["centroid"] = merged_gdf["geometry"].centroid
    merged_gdf["x"] = merged_gdf["centroid"].x
    merged_gdf["y"] = merged_gdf["centroid"].y

    existing_df = param_df.merge(merged_gdf[["nat_hru_id", "x", "y"]], on="nat_hru_id", how="left")
    missing_df = merged_gdf[merged_gdf["nat_hru_id"].isin(missing_ids)][["nat_hru_id", "x", "y", "vpu"]]

    existing_coords = existing_df[["x", "y"]].values
    missing_coords = missing_df[["x", "y"]].values
    existing_values = existing_df[param_column].values

    knn = NearestNeighbors(n_neighbors=k)
    knn.fit(existing_coords)
    distances, indices = knn.kneighbors(missing_coords)

    interpolated_values = []
    for neighbor_indices in tqdm(indices, desc="Filling missing HRUs"):
        neighbor_values = existing_values[neighbor_indices]
        interpolated_values.append(np.mean(neighbor_values))

    missing_filled = pd.DataFrame({
        "nat_hru_id": missing_df["nat_hru_id"].values,
        param_column: interpolated_values,
        "vpu": missing_df["vpu"].values,
    })
    missing_filled["hru_id"] = missing_filled["nat_hru_id"]

    complete_df = pd.concat([param_df, missing_filled], ignore_index=True)
    complete_df = complete_df.sort_values("nat_hru_id").reset_index(drop=True)
    logger.info("Filled %d missing values", len(missing_ids))
    return complete_df


def main():
    parser = argparse.ArgumentParser(description="Merge VPU geopackages and fill missing parameter values.")
    parser.add_argument("--targets_dir", default=None)
    parser.add_argument("--param_file", default=None)
    parser.add_argument("--output_dir", default=None)
    parser.add_argument("--simplify_tolerance", type=float, default=100)
    parser.add_argument("--k_neighbors", type=int, default=1)
    parser.add_argument("--force_rebuild", action="store_true")
    args = parser.parse_args()

    logger = configure_logging("merge_and_fill_params")

    # Load base config for defaults after argparse (so --help works without config)
    base = load_base_config()
    data_root = base["data_root"]
    expected_max = base["expected_max_hru_id"]

    if args.targets_dir is None:
        args.targets_dir = f"{data_root}/targets"
    if args.param_file is None:
        args.param_file = f"{data_root}/nhm_params/nhm_params_merged/nhm_ssflux_params.csv"
    if args.output_dir is None:
        args.output_dir = f"{data_root}/nhm_params/nhm_params_merged"

    targets_dir = Path(args.targets_dir)
    param_file = Path(args.param_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    merged_gpkg = targets_dir / "gfv2_merged_simplified.gpkg"
    filled_param_file = output_dir / f"filled_{param_file.name}"

    if merged_gpkg.exists() and not args.force_rebuild:
        logger.info("Loading existing merged geopackage: %s", merged_gpkg)
        merged_gdf = gpd.read_file(merged_gpkg, layer="nhru")
        logger.info("Loaded %d features", len(merged_gdf))
    else:
        merged_gdf = merge_vpu_geopackages(targets_dir, VPUS_DETAILED, merged_gpkg, args.simplify_tolerance, logger)

    param_df, missing_ids = find_missing_ids(param_file, expected_max, logger)

    if missing_ids:
        param_columns = [col for col in param_df.columns if col not in ["hru_id", "nat_hru_id", "vpu"]]
        if not param_columns:
            logger.error("No parameter column found in the data")
            return

        param_column = param_columns[0]
        logger.info("Using parameter column: %s", param_column)

        complete_df = fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_column, args.k_neighbors, logger)
        complete_df.to_csv(filled_param_file, index=False)
        logger.info("Filled parameter file saved to: %s", filled_param_file)

        final_ids = set(complete_df["nat_hru_id"])
        expected_ids = set(range(1, expected_max + 1))
        still_missing = expected_ids - final_ids

        if still_missing:
            logger.warning("%d IDs are still missing", len(still_missing))
        else:
            logger.info("All missing values have been filled successfully!")
    else:
        logger.info("No missing values found in the parameter file")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Update `scripts/find_missing_hru_ids.py`**

Replace hardcoded `expected_max = 361471` with config-driven value, add logging:

```python
"""Find missing nat_hru_id values in a parameter CSV file."""

import argparse
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def find_missing_nat_hru_ids(csv_file, expected_max, output_file, logger):
    df = pd.read_csv(csv_file)
    nat_hru_ids = df["nat_hru_id"].sort_values()

    expected_range = set(range(1, expected_max + 1))
    actual_values = set(nat_hru_ids)
    missing_values = sorted(expected_range - actual_values)

    output_lines = []
    output_lines.append(f"Analysis of missing nat_hru_id values in: {csv_file}")
    output_lines.append("=" * 60)
    output_lines.append(f"Total expected values: {len(expected_range)}")
    output_lines.append(f"Total actual values: {len(actual_values)}")
    output_lines.append(f"Missing values count: {len(missing_values)}")
    output_lines.append(f"Minimum nat_hru_id: {nat_hru_ids.min()}")
    output_lines.append(f"Maximum nat_hru_id: {nat_hru_ids.max()}")
    output_lines.append("")

    if missing_values:
        output_lines.append(f"First 20 missing values: {missing_values[:20]}")
        output_lines.append(f"Last 20 missing values: {missing_values[-20:]}")
        output_lines.append("")

        gaps = []
        gap_start = missing_values[0]
        gap_end = missing_values[0]

        for i in range(1, len(missing_values)):
            if missing_values[i] == missing_values[i - 1] + 1:
                gap_end = missing_values[i]
            else:
                if gap_end > gap_start:
                    gaps.append((gap_start, gap_end))
                else:
                    gaps.append((gap_start,))
                gap_start = missing_values[i]
                gap_end = missing_values[i]

        if gap_end > gap_start:
            gaps.append((gap_start, gap_end))
        else:
            gaps.append((gap_start,))

        output_lines.append(f"Number of gaps: {len(gaps)}")
        if gaps:
            output_lines.append("First 10 gaps:")
            for i, gap in enumerate(gaps[:10]):
                if len(gap) == 2:
                    output_lines.append(f"  Gap {i+1}: {gap[0]} to {gap[1]} (size: {gap[1] - gap[0] + 1})")
                else:
                    output_lines.append(f"  Gap {i+1}: {gap[0]} (single missing value)")

        output_lines.append("")
        output_lines.append("ALL MISSING nat_hru_id VALUES:")
        output_lines.append("-" * 40)

        for i in range(0, len(missing_values), 10):
            chunk = missing_values[i : i + 10]
            output_lines.append(", ".join(map(str, chunk)))
    else:
        output_lines.append("No missing values found!")

    for line in output_lines:
        logger.info(line)

    if output_file is None:
        csv_path = Path(csv_file)
        output_file = csv_path.parent / f"missing_hru_ids_{csv_path.stem}.txt"

    with open(output_file, "w") as f:
        for line in output_lines:
            f.write(line + "\n")

    logger.info("Results saved to: %s", output_file)


def main():
    parser = argparse.ArgumentParser(description="Find missing nat_hru_id values in CSV file")
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("--output", "-o", help="Path to output text file (optional)")
    args = parser.parse_args()

    logger = configure_logging("find_missing_hru_ids")

    base = load_base_config()
    expected_max = base["expected_max_hru_id"]

    find_missing_nat_hru_ids(args.csv_file, expected_max, args.output, logger)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Delete old script files**

```bash
git rm scripts/7_add_nat_hru_id.py
git rm scripts/8_add_nat_hru_id_default_nhru.py
git rm scripts/merge_vpu_and_fill_params.py
```

- [ ] **Step 6: Commit**

```bash
git add scripts/merge_params.py scripts/merge_default_params.py scripts/merge_and_fill_params.py scripts/find_missing_hru_ids.py
git commit -m "feat: rewrite merge/fill scripts to use gfv2_params package, logging, tqdm, config-driven defaults"
```

---

## Task 7: Move download utilities into package

**Files:**
- Create: `src/gfv2_params/download/rpu_rasters.py`
- Create: `src/gfv2_params/download/mrlc_impervious.py`
- Delete: `src/helpers.py`, `src/__init__.py`, `src/download_rpu_rasters.py`, `src/download_mrlc_fract_impervious_rasters.py`

- [ ] **Step 1: Create `src/gfv2_params/download/rpu_rasters.py`**

Copy `src/download_rpu_rasters.py` content, replace `print()` with logging, replace hardcoded paths with config-driven defaults:

The content is the same as the current `src/download_rpu_rasters.py` but with:
- `from gfv2_params.log import configure_logging` and `logger = configure_logging("download_rpu_rasters")`
- All `print()` -> `logger.info()` / `logger.error()`
- `download_dir` and `extract_dir` derived from `base_config.yml` `data_root`

- [ ] **Step 2: Create `src/gfv2_params/download/mrlc_impervious.py`**

Same pattern: copy content, add logging, derive paths from config.

- [ ] **Step 3: Delete old source files**

```bash
git rm src/helpers.py
git rm src/__init__.py
git rm src/download_rpu_rasters.py
git rm src/download_mrlc_fract_impervious_rasters.py
```

- [ ] **Step 4: Commit**

```bash
git add src/gfv2_params/download/
git commit -m "feat: move download utilities into gfv2_params.download subpackage"
```

---

## Task 8: Update SLURM batch scripts

**Files:**
- Rewrite all batch files in `slurm_batch/`
- Delete stale batch files
- Rewrite `slurm_batch/RUNME.md`

- [ ] **Step 1: Delete stale batch files**

```bash
git rm "slurm_batch/01_create_elev_params copy.batch"
git rm slurm_batch/01_OR_create_elev_params.batch
git rm slurm_batch/04_OR_create_soils_params.batch
git rm slurm_batch/06_OR_create_ssflux_params.batch
```

- [ ] **Step 2: Rename and update all batch files**

For each batch file, rename to match new script names and update:
1. Script paths to new names
2. Config paths to new names
3. Fix VPU array range: `--array=0-20` for detailed scheme (21 VPUs), `--array=0-17` for simple scheme (18 VPUs)
4. Add `pip install -e .` after conda activate (or document it as one-time setup)

Example — `slurm_batch/create_zonal_elev_params.batch` (replaces `01_create_elev_params.batch`):

```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=zonal_elev
#SBATCH --output=logs/job_%A_%a.out
#SBATCH --error=logs/job_%A_%a.err
#SBATCH --array=0-20
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=256G

module load miniforge/latest
conda activate geoenv

vpus=("01" "02" "03N" "03S" "03W" "04" "05" "06" "07" "08" "09" "10L" "10U" "11" "12" "13" "14" "15" "16" "17" "18")
vpu=${vpus[$SLURM_ARRAY_TASK_ID]}

python /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/gfv2-params/scripts/create_zonal_params.py \
    --config /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/gfv2-params/configs/elev_param.yml \
    --vpu $vpu
```

Repeat this pattern for all batch files:
- `create_zonal_elev_params.batch` (was `01_create_elev_params.batch`)
- `create_zonal_slope_params.batch` (was `02_create_slope_params.batch`)
- `create_zonal_aspect_params.batch` (was `03_create_aspect_params.batch`)
- `create_soils_params.batch` (was `04_create_soils_params.batch`)
- `create_soilmoistmax_params.batch` (was `05_create_soilmoistmax_params.batch`)
- `create_ssflux_params.batch` (was `06_create_ssflux_params.batch`)
- `merge_output_params.batch` (was `07_merge_output_params.batch`)
- `merge_default_output_params.batch` (was `08_merge_default_output_params.batch`)
- `merge_rpu_by_vpu.batch` (was `a_process_NHD_by_vpu.batch`)
- `compute_slope_aspect.batch` (was `b_process_slope_aspect.batch`)
- Rename `*_update.batch` files similarly (e.g., `create_zonal_elev_params_update.batch`)

- [ ] **Step 3: Delete old batch files**

```bash
git rm slurm_batch/01_create_elev_params.batch
git rm slurm_batch/01a_create_elev_params_update.batch
git rm slurm_batch/02_create_slope_params.batch
git rm slurm_batch/02a_create_slope_params_update.batch
git rm slurm_batch/03_create_aspect_params.batch
git rm slurm_batch/03a_create_aspect_params_update.batch
git rm slurm_batch/04_create_soils_params.batch
git rm slurm_batch/04a_create_soils_params_update.batch
git rm slurm_batch/05_create_soilmoistmax_params.batch
git rm slurm_batch/05a_create_soilmoistmax_params_update.batch
git rm slurm_batch/06_create_ssflux_params.batch
git rm slurm_batch/06a_create_ssflux_params_update.batch
git rm slurm_batch/07_merge_output_params.batch
git rm slurm_batch/08_merge_default_output_params.batch
git rm slurm_batch/a_process_NHD_by_vpu.batch
git rm slurm_batch/b_process_slope_aspect.batch
```

- [ ] **Step 4: Rewrite `slurm_batch/RUNME.md`**

Update with new script/config names, `pip install -e .` setup, custom fabric instructions, corrected VPU array mapping.

- [ ] **Step 5: Commit**

```bash
git add slurm_batch/
git commit -m "feat: rename and update SLURM batch scripts to match new structure, fix array range bug"
```

---

## Task 9: Rename notebooks directory, update README, final cleanup

**Files:**
- Rename: `marimo/` -> `notebooks/`
- Modify: `README.md`
- Update notebook imports

- [ ] **Step 1: Rename marimo directory**

```bash
git mv marimo notebooks
```

- [ ] **Step 2: Update notebook imports**

In each `.py` file under `notebooks/`, replace any `from helpers import ...` or `sys.path` hacking with clean imports from `gfv2_params`:

```python
# Old:
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(src_path))
from helpers import load_config

# New:
from gfv2_params.config import load_config
```

- [ ] **Step 3: Rewrite `README.md`**

Update with:
- New project structure diagram
- Setup instructions: `conda env create -f environment.yml && conda activate geoenv && pip install -e .`
- Brief description of each script
- Link to `slurm_batch/RUNME.md` for HPC workflow
- Custom fabric usage example

- [ ] **Step 4: Clean up any egg-info directories**

```bash
rm -rf *.egg-info src/*.egg-info
```

- [ ] **Step 5: Commit**

```bash
git add notebooks/ README.md .gitignore
git commit -m "chore: rename marimo to notebooks, update README, final cleanup"
```

---

## Task 10: Verify the complete restructure

- [ ] **Step 1: Verify package installs cleanly**

```bash
pip install -e ".[dev]"
```

- [ ] **Step 2: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 3: Verify imports work from scripts**

```bash
python -c "from gfv2_params.config import load_config, VPUS_DETAILED, resolve_vpu; print('config OK')"
python -c "from gfv2_params.raster_ops import resample, mult_rasters, deg_to_fraction; print('raster_ops OK')"
python -c "from gfv2_params.log import configure_logging; print('log OK')"
```

- [ ] **Step 4: Verify script --help works for all scripts**

```bash
python scripts/create_zonal_params.py --help
python scripts/create_soils_params.py --help
python scripts/create_ssflux_params.py --help
python scripts/merge_rpu_by_vpu.py --help
python scripts/compute_slope_aspect.py --help
python scripts/merge_params.py --help
python scripts/merge_default_params.py --help
python scripts/merge_and_fill_params.py --help
python scripts/find_missing_hru_ids.py --help
```

- [ ] **Step 5: Verify no old files remain**

```bash
# Should find nothing:
ls src/helpers.py src/download_*.py scripts/1_* scripts/4_* scripts/6_* scripts/7_* scripts/8_* scripts/process_* configs/0* configs/config_* 2>/dev/null && echo "OLD FILES STILL EXIST" || echo "All clean"
```

- [ ] **Step 6: Final commit if any cleanup needed**

```bash
git status
# If clean, no commit needed
```
