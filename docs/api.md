# API reference

Auto-generated from docstrings in [`src/gfv2_params/`](https://github.com/rmcd-mscb/gfv2-params/tree/main/src/gfv2_params).
Coverage tracks what's documented; modules without docstrings show only
signatures.

## Core

### `gfv2_params.config`

::: gfv2_params.config

### `gfv2_params.depstor`

::: gfv2_params.depstor

### `gfv2_params.depstor_ratios`

::: gfv2_params.depstor_ratios

### `gfv2_params.lulc`

::: gfv2_params.lulc

### `gfv2_params.raster_ops`

::: gfv2_params.raster_ops

### `gfv2_params.batching`

::: gfv2_params.batching

### `gfv2_params.threshold_sweep`

::: gfv2_params.threshold_sweep

### `gfv2_params.wbt`

::: gfv2_params.wbt

### `gfv2_params.log`

::: gfv2_params.log

### `gfv2_params.viz`

::: gfv2_params.viz

## Builder packages

The orchestrator packages each ship a dispatch table mapping step name
strings to builder/runner functions, plus a default step order:

- `gfv2_params.depstor_builders.BUILDERS` + `STEP_ORDER`
- `gfv2_params.shared_rasters.BUILDERS` + `STEP_ORDER`
- `gfv2_params.zonal_runners.BATCH_RUNNERS` (per-`script:` tag dispatch)

See [Architecture / Canonical](ARCHITECTURE.md) for the pattern.

### `gfv2_params.depstor_builders`

::: gfv2_params.depstor_builders

### `gfv2_params.shared_rasters`

::: gfv2_params.shared_rasters

### `gfv2_params.zonal_runners`

::: gfv2_params.zonal_runners

## Data acquisition

Per-source raster + LULC download helpers. Each module wraps fetching one
upstream dataset onto the data root.

### `gfv2_params.download`

::: gfv2_params.download

### `gfv2_params.download.copernicus_dem`

::: gfv2_params.download.copernicus_dem

### `gfv2_params.download.mrlc_impervious`

::: gfv2_params.download.mrlc_impervious

### `gfv2_params.download.nalcms_lulc`

::: gfv2_params.download.nalcms_lulc

### `gfv2_params.download.nhm_v11_lulc`

::: gfv2_params.download.nhm_v11_lulc

### `gfv2_params.download.rpu_rasters`

::: gfv2_params.download.rpu_rasters
