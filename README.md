# gfv2-params

PRMS/NHM parameter generation from watershed fabric polygons.

Given a watershed fabric of polygons (HRUs), this pipeline computes parameters for the PRMS/National Hydrologic Model by performing zonal statistics against source rasters (DEM, soils, lithology, etc.).

## Setup

This project uses [pixi](https://pixi.sh) for environment management. Install pixi
once per user (see https://pixi.sh/latest/installation/) and ensure `~/.pixi/bin`
is on your `PATH`. From the repo root:

```bash
pixi install
bash scripts/refresh_pixi_activation.sh
```

`pixi install` materialises `.pixi/envs/default/` from `pixi.lock` (config lives
in `pyproject.toml` under `[tool.pixi.*]`). The activation refresh pre-bakes
`.pixi-activate.sh` so SLURM batches can `source` it without racing on
`pixi shell-hook` under array submission. Re-run the refresh script any time
`pyproject.toml` or `pixi.lock` change.

For interactive use:

```bash
pixi shell                       # default env
pixi shell -e notebooks          # default + marimo, plotly, hvplot, ...
pixi shell -e dev                # default + pytest, ruff, pre-commit
```

The legacy `environment.yml` / `geoenv` conda environment is retained as a
deprecated fallback only — new work should use pixi.

## Project Structure

```
gfv2-params/
├── src/gfv2_params/          # Installable Python package
│   ├── config.py             # Config loading, fabric profile resolution
│   ├── raster_ops.py         # Raster utilities (resample, multiply, slope conversion)
│   ├── batching.py           # Spatial batching helpers
│   ├── lulc.py               # LULC reclassification helpers
│   ├── depstor.py            # Depression-storage raster helpers (binarize, intersect, write)
│   ├── depstor_builders/     # Per-step depstor raster builders (used by build_depstor_rasters.py)
│   ├── depstor_ratios.py     # PRMS Level-5 ratio arithmetic (compute_ratio)
│   ├── log.py                # Logging setup
│   └── download/             # Data download utilities
├── scripts/                  # CLI processing scripts
│   ├── init_data_root.py           # Scaffold data-root tree; verify staged inputs
│   ├── stage_twi.sh                # Stage per-RPU TWI rasters from shared FS
│   ├── prepare_fabric.py           # Spatially batch fabric into per-batch gpkgs
│   ├── build_vrt.py                # Build CONUS-wide virtual rasters
│   ├── build_border_dem.py         # Copernicus GLO-30 border-fill elevation
│   ├── build_derived_rasters.py    # Pre-compute derived rasters (e.g., soil_moist_max)
│   ├── build_lulc_rasters.py       # Pre-compute LULC-derived rasters
│   ├── build_weights.py            # Pre-compute polygon-to-polygon weights
│   ├── compute_slope_aspect.py     # Derive slope/aspect from DEM
│   ├── compute_dem_derivatives.py  # Open-source TWI/FDR/FAC/slope/aspect from Hydrodem (richdem fill + WBT D8)
│   ├── merge_rpu_by_vpu.py         # Merge RPU rasters by VPU
│   ├── create_zonal_params.py      # Elevation, slope, aspect, depstor-fraction parameters
│   ├── create_soils_params.py      # Soils and soil moisture max
│   ├── create_lulc_params.py       # LULC fractional parameters
│   ├── create_ssflux_params.py     # Subsurface flux parameters
│   ├── build_depstor_rasters.py       # Build the full depstor raster stack (10 steps)
│   ├── derive_depstor_params.py       # Depstor zonal stats + Level-5 ratios (zonal/merge/ratios)
│   ├── merge_params.py             # Merge per-batch CSVs
│   ├── merge_default_params.py     # Merge NHM default params
│   ├── merge_and_fill_params.py    # KNN gap-filling
│   └── find_missing_hru_ids.py     # Identify missing HRU IDs
├── configs/                  # YAML configuration files
│   ├── base_config.yml       # Data root + fabric profiles (single source of truth)
│   └── *.yml                 # Fabric-agnostic per-step configs with template placeholders
├── slurm_batch/              # HPC SLURM batch scripts
│   ├── submit_jobs.sh        # SLURM array job submission wrapper
│   └── RUNME.md              # HPC workflow documentation (authoritative)
├── docs/                     # Pipeline reference docs (depstor workflow, validation, port summary)
├── notebooks/                # Marimo interactive notebooks (incl. VPU01 QA/QC)
├── tests/                    # Unit tests
├── pyproject.toml            # Package + pixi config
├── pixi.lock                 # Pinned pixi environment
└── environment.yml           # Legacy conda environment (deprecated fallback)
```

## Output Directory Structure

The data root (`data_root`) is set in `configs/base_config.yml`. All source data and outputs live under this root:

```
gfv2_param/
├── input/                          # External data (manually staged or downloaded)
│   ├── fabric/                     # Per-VPU watershed fabric gpkgs
│   ├── soils_litho/                # TEXT_PRMS.tif, AWC.tif, Lithology_exp_Konly_Project.*
│   ├── lulc_veg/                   # RootDepth.tif, CNPY.tif, Imperv.tif
│   │   └── nhm_v11/                # NHM v1.1 pre-derived LULC (downloadable)
│   ├── lulc/
│   │   ├── nlcd_annual_imperv/     # NLCD fractional imperviousness (downloadable)
│   │   └── nalcms_2020/            # NALCMS 2020 land cover (downloadable)
│   ├── depstor/                    # Per-fabric depression-storage inputs
│   │   └── <fabric>_segments_wbodies.gpkg   # nsegment + v2_wb layers
│   │                               # (FDR comes from shared work/nhd_merged/fdr.vrt)
│   ├── twi/<rpu>/                  # Per-RPU TWI (twi.tif + sidecars; staged via stage_twi.sh)
│   ├── nhm_default/                # NHM default parameter files
│   └── nhd_downloads/              # Raw NHDPlus zip archives (downloadable)
├── work/                           # Reproducible intermediates (safe to delete)
│   ├── nhd_extracted/              # Unzipped per-RPU rasters
│   ├── nhd_merged/                 # Per-VPU GeoTIFFs + CONUS VRTs (incl. twi.vrt)
│   │   └── copernicus_fill/        # Border-DEM fill (Canada/Mexico) for elevation VRT
│   ├── derived_rasters/            # soil_moist_max.tif, radtrn, resampled CNPY/keep
│   └── weights/                    # P2P polygon weights for ssflux
└── {fabric}/                       # Per-fabric outputs (e.g., gfv2/, gfv2_vpu01/, oregon/)
    ├── fabric/                     # Merged fabric gpkg
    ├── batches/                    # Per-batch gpkgs + manifest
    ├── depstor_rasters/            # Depression-storage intermediate rasters (per fabric)
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
| `input/depstor/` | Per-fabric: `<fabric>_segments_wbodies.gpkg` (layers `nsegment`, `v2_wb`). The D8 flow-direction raster is no longer expected here — the gfv2 profile now consumes `work/nhd_merged/fdr.vrt` produced by the shared raster pipeline. |
| `input/twi/<rpu>/` | Per-RPU `twi.tif` (+ `.tfw`, `.aux.xml`, `.ovr`, `.xml` sidecars). Stage with `bash scripts/stage_twi.sh` (or `sbatch slurm_batch/stage_twi.batch` for an unattended run). |

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

**Build the CONUS shared raster store (recommended):** once the downloads
above are complete, the entire raster preparation DAG runs from one
orchestrator over [configs/shared_rasters.yml](configs/shared_rasters.yml):

```bash
pixi run python scripts/build_shared_rasters.py --config configs/shared_rasters.yml
```

This walks 7 production steps in dependency order (merge_rpu_by_vpu →
compute_slope_aspect → build_border_dem → build_vpu_landmask →
merge_rpu_by_vpu_twi → build_vrt → build_derived_rasters → build_lulc_rasters)
and writes everything into the shared `work/` store consumed by every
fabric. Add `--step <name>` to run one step or `--from <name>` to resume.
`--vpus 01,02` scopes per-VPU steps to a subset; `--force` rebuilds
existing outputs. See [Shared rasters pipeline](#shared-rasters-pipeline)
below for the design notes.

The per-script entry points (`merge_rpu_by_vpu.py`, `compute_slope_aspect.py`,
etc.) and their sbatch wrappers (`slurm_batch/merge_rpu_by_vpu.batch`, etc.)
are preserved as thin shells around the same library builders, so existing
job submissions keep working unchanged.

### 4. Run fabric-dependent tasks

Once raster prep is complete and the merged fabric is available, prepare the fabric batches and run parameter generation. See `slurm_batch/RUNME.md` **Part 2** for the full sequence.

### Single-batch run
```bash
python scripts/create_zonal_params.py --config configs/elev_param.yml --batch_id 0042
```

## Custom Fabric

Fabrics are defined as profiles inside `configs/base_config.yml` under the
`fabrics:` mapping — one file edit, no new YAMLs. The active profile is selected
via (highest precedence first):

1. `--fabric <name>` CLI flag on any script
2. `FABRIC` env var passed through `sbatch`
3. `default_fabric` in `configs/base_config.yml`

**Pre-merged fabric** (single gpkg covering the full domain — e.g., Oregon):

1. Add a profile under `fabrics:` in `configs/base_config.yml`. Required keys
   are `expected_max_hru_id` and `batch_size`. If the depstor pipeline will be
   run, also set `template_raster`, `fdr_raster`, `twi_raster`, `segments_gpkg`,
   `waterbody_gpkg`, and `waterbody_layer`.
2. Scaffold output dirs: `python scripts/init_data_root.py --fabric oregon`
3. Place the fabric gpkg directly in `{data_root}/oregon/fabric/` (NOT in `input/fabric/`)
4. Run `prepare_fabric.py --fabric oregon` and submit jobs via
   `slurm_batch/submit_jobs.sh $BATCHES <script>.batch <base_config> <merge_config> oregon`
   (fabric is the 5th positional arg to `submit_jobs.sh`).

**VPU-based fabric** (per-VPU gpkgs that need merging — e.g., gfv2):

1. Add a profile under `fabrics:` and place per-VPU gpkgs in `input/fabric/`
2. Scaffold, merge with `marimo run notebooks/merge_vpu_targets.py`, then run
   `prepare_fabric.py` and all stages with `--fabric <name>` (or `FABRIC=<name>`).

See `slurm_batch/RUNME.md` for the full step-by-step workflow.

## Shared rasters pipeline

The CONUS shared-raster preparation (Part 1 of the workflow) is driven by
one orchestrator and one unified config:

- [scripts/build_shared_rasters.py](scripts/build_shared_rasters.py) reads
  [configs/shared_rasters.yml](configs/shared_rasters.yml) and walks the
  step DAG via the builder modules under
  [src/gfv2_params/shared_rasters/](src/gfv2_params/shared_rasters/).
- The DAG covers per-VPU NHDPlus prep (`merge_rpu_by_vpu`,
  `compute_slope_aspect`), border-DEM fill (`build_border_dem`), per-VPU
  HRU landmask (`build_vpu_landmask`), the masked TWI merge
  (`merge_rpu_by_vpu_twi`), CONUS VRT assembly (`build_vrt`), and CONUS
  derived rasters (`build_derived_rasters`, `build_lulc_rasters`).
- `compute_dem_derivatives` is registered as an opt-in step (parallel
  open-source TWI pipeline; not in the default `steps:` list because PRMS
  calibration thresholds reference the canonical ArcPy-derived TWI — see
  the [module docstring](src/gfv2_params/shared_rasters/compute_dem_derivatives.py)).

These outputs live under `{data_root}/work/` and are **fabric-independent**:
every fabric reuses the same CONUS rasters. Per-VPU iteration happens
inside the builders, not in per-VPU sbatch launches, so the orchestrator
runs as a single job. The per-script entrypoints and sbatch wrappers are
preserved as thin shells around the same builders.

## Depression-storage pipeline

The depstor pipeline (Levels 2-5) is driven by two orchestrators and two
unified configs:

- [scripts/build_depstor_rasters.py](scripts/build_depstor_rasters.py) reads
  [configs/depstor_rasters.yml](configs/depstor_rasters.yml) and walks the
  10-step DAG (landmask → imperv/streambuffer/waterbody → dprst → perv/routing
  → drains_perv/drains_imperv → carea_map) via the builder modules under
  [src/gfv2_params/depstor_builders/](src/gfv2_params/depstor_builders/).
- [scripts/derive_depstor_params.py](scripts/derive_depstor_params.py) reads
  [configs/depstor_params.yml](configs/depstor_params.yml) and dispatches the
  9 fractions (`perv_frac`, `imperv_frac`, `dprst_frac`,
  `drains_perv_frac`, `drains_imperv_frac`, `onstream_storage_frac`,
  `drains_to_dprst_frac`, `carea_t8_frac`, `carea_t156_frac`) plus the 4 PRMS
  Level-5 ratios (`sro_to_dprst_perv`, `sro_to_dprst_imperv`, `carea_max`,
  `smidx_coef`). The slurm wrapper
  [slurm_batch/submit_depstor_params.sh](slurm_batch/submit_depstor_params.sh)
  chains 9 zonal arrays → 9 merges → 1 ratios job via afterok.

See [docs/depstor_workflow.md](docs/depstor_workflow.md) for the design notes
and [docs/depstor_port_summary.md](docs/depstor_port_summary.md) for the
ArcPy-to-open-source port summary. Stage 2d in `slurm_batch/RUNME.md` lists the
build order and dependencies.

## Configuration

`configs/base_config.yml` is the single source of truth for the data root and
fabric profiles. Per-step configs are fabric-agnostic — they use `{data_root}`,
`{fabric}`, `{vpu}`, and `{raster_vpu}` template placeholders that are resolved
at runtime against the active profile.

## Logging

All scripts use Python's `logging` module. Control verbosity via the `LOG_LEVEL` environment variable:
```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO (default), WARNING, ERROR
```

## License

CC0 1.0 Universal
