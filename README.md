# gfv2-params

PRMS/NHM parameter generation from watershed fabric polygons.

Given a watershed fabric of polygons (HRUs), this pipeline computes parameters for the PRMS/National Hydrologic Model by performing zonal statistics against source rasters (DEM, soils, lithology, etc.).

## Setup

This project uses [pixi](https://pixi.sh) for environment management. Install pixi
once per user (see https://pixi.sh/latest/installation/) and ensure `~/.pixi/bin`
is on your `PATH`. From the repo root:

```bash
pixi install
```

`pixi install` materialises `.pixi/envs/default/` from `pixi.lock` (config lives
in `pyproject.toml` under `[tool.pixi.*]`). SLURM batches invoke the env with
`pixi run --as-is` (= `--no-install --frozen`): the already-installed env is used
verbatim with no lock check or env mutation, so concurrent array tasks don't race
on `.pixi/envs/.../conda-meta`. Re-run `pixi install` after `pyproject.toml` or
`pixi.lock` change.

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
│   ├── init_data_root.py             # Scaffold data-root tree; verify staged inputs
│   ├── stage_twi.sh                  # Stage per-RPU TWI rasters from shared FS
│   ├── prepare_fabric.py             # Spatially batch fabric into per-batch gpkgs
│   ├── migrate_to_shared_layout.py   # One-shot: legacy work/ → shared/ on-disk migration
│   ├── build_shared_rasters.py       # Part 1 orchestrator (CONUS shared raster prep)
│   ├── clip_shared_to_fabric.py      # Stage a fabric-bounds FDR clip → depstor template/fdr
│   ├── build_depstor_rasters.py      # Part 2a depstor raster stack (10 steps)
│   ├── derive_depstor_params.py      # Part 2a depstor params (zonal/merge/ratios)
│   ├── derive_zonal_params.py        # Part 2b zonal-pass orchestrator (zonal/merge/build_weights)
│   ├── merge_default_params.py       # Stage 8: merge NHM default params
│   ├── merge_and_fill_params.py      # KNN gap-filling
│   └── find_missing_hru_ids.py       # Identify missing HRU IDs
├── configs/                  # YAML configuration files
│   ├── base_config.yml       # Data root + fabric profiles (single source of truth)
│   ├── shared_rasters/       # Part 1 raster-prep configs (orchestrator + per-step + lulc/)
│   ├── depstor/              # Part 2 depstor configs (rasters + params)
│   └── zonal/                # Part 2 zonal-pass configs (orchestrator + per-script fallbacks)
├── slurm_batch/              # HPC SLURM batch scripts
│   ├── submit_jobs.sh        # SLURM array job submission wrapper
│   └── RUNME.md              # HPC workflow documentation (authoritative)
├── docs/                     # Pipeline reference docs (depstor workflow, validation, port summary)
├── notebooks/                # Interactive notebooks (marimo + Jupyter QA/QC)
│   └── fabric_results/        # Fabric results viewers (01 inputs, 02 depstor, 03 params)
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
│   │                               # (FDR comes from shared/conus/vrt/fdr.vrt)
│   ├── twi/<rpu>/                  # Per-RPU TWI (twi.tif + sidecars; staged via stage_twi.sh)
│   ├── nhm_default/                # NHM default parameter files
│   └── nhd_downloads/              # Raw NHDPlus zip archives (downloadable)
├── shared/                         # Fabric-independent intermediates (reused by every fabric)
│   ├── source/                     # Unzipped per-RPU NHDPlus rasters
│   ├── per_vpu/<vpu>/              # Per-VPU merged GeoTIFFs (NED/Hydrodem/Fdr/Fac/Twi/slope/aspect/landmask)
│   └── conus/
│       ├── vrt/                    # CONUS-wide GDAL virtual rasters (elevation/slope/aspect/fdr/twi)
│       ├── derived/                # soil_moist_max.tif, radtrn, resampled CNPY/keep
│       ├── borders/                # Copernicus border-DEM fill (Canada/Mexico)
│       └── weights/                # P2P polygon weights for ssflux
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
pixi run init-data-root
pixi run init-data-root --check
```

### 2. Stage external inputs

The following externally-provided files must be placed in the scaffolded directories:

| Destination | Required files |
|---|---|
| `input/fabric/` | `NHM_<VPU>_draft.gpkg` for each of the 21 VPUs: `01 02 03N 03S 03W 04 05 06 07 08 09 10L 10U 11 12 13 14 15 16 17 18` |
| `input/soils_litho/` | `TEXT_PRMS.tif`, `AWC.tif`, `Lithology_exp_Konly_Project.shp` (+ sidecar files: `.dbf`, `.prj`, `.shx`) |
| `input/lulc_veg/` | `RootDepth.tif`, `CNPY.tif`, `Imperv.tif` |
| `input/nhm_default/` | NHM default parameter files (input to final merge step) |
| `input/depstor/` | Per-fabric: `<fabric>_segments_wbodies.gpkg` (layers `nsegment`, `v2_wb`). The D8 flow-direction raster is no longer expected here — the gfv2 profile now consumes `shared/conus/vrt/fdr.vrt` produced by the shared raster pipeline. |
| `input/twi/<rpu>/` | Per-RPU `twi.tif` (+ `.tfw`, `.aux.xml`, `.ovr`, `.xml` sidecars). Stage with `bash scripts/stage_twi.sh` (or `sbatch slurm_batch/stage_twi.batch` for an unattended run). |

> **Upgrading an existing `data_root` from the legacy `work/` layout?** Run
> `pixi run python scripts/migrate_to_shared_layout.py --data-root <path> --dry-run`
> to preview the 27 directory renames, then `--execute` to apply them.
> Atomic `os.rename` on the same filesystem (metadata-only, near-instant);
> regenerates CONUS VRTs at the end since they encode absolute source paths.
> Idempotent — re-running after success is a no-op.

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
orchestrator over [configs/shared_rasters/shared_rasters.yml](configs/shared_rasters/shared_rasters.yml):

```bash
pixi run python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml
```

This walks 7 production steps in dependency order (merge_rpu_by_vpu →
compute_slope_aspect → build_border_dem → build_vpu_landmask →
merge_rpu_by_vpu_twi → build_vrt → build_derived_rasters → build_lulc_rasters)
and writes everything into the shared `work/` store consumed by every
fabric. Add `--step <name>` to run one step or `--from <name>` to resume.
`--vpus 01,02` scopes per-VPU steps to a subset; `--force` rebuilds
existing outputs. See [Shared rasters pipeline](#shared-rasters-pipeline)
below for the design notes.

The orchestrator dispatches into per-step library builders under
[src/gfv2_params/shared_rasters/](src/gfv2_params/shared_rasters/). The
older per-script CLI wrappers were retired in favour of `--step <name>`
on the orchestrator.

### 4. Run fabric-dependent tasks

Once raster prep is complete and the merged fabric is available, prepare
the fabric batches and run parameter generation. The unified Part 2
dispatcher walks every zonal param + chained merges + the ssflux weights
prereq in one invocation:

```bash
bash slurm_batch/submit_zonal_params.sh \
    {data_root}/{fabric}/batches {fabric} configs/base_config.yml
```

See [Zonal-pass parameter pipeline](#zonal-pass-parameter-pipeline) below
for the design notes, and `slurm_batch/RUNME.md` **Part 2** for the full
sequence including the depstor pipeline and gap-fill.

### Single-batch run (debugging one param + batch)

```bash
pixi run python scripts/derive_zonal_params.py --mode zonal --param elevation --batch_id 42 \
    --config configs/zonal/zonal_params.yml --base_config configs/base_config.yml
```

`--mode merge --param <name>` runs just the merge step;
`--mode build_weights` builds the CONUS-once ssflux prereq.

## Custom Fabric

Fabrics are defined as profiles inside `configs/base_config.yml` under the
`fabrics:` mapping — one file edit, no new YAMLs. The active profile is selected
via (highest precedence first):

1. `--fabric <name>` CLI flag on any script
2. `FABRIC` env var passed through `sbatch`
3. `default_fabric` in `configs/base_config.yml`

**Pre-merged fabric** (single gpkg covering the full domain — e.g., Oregon):

1. Register the fabric and scaffold its output dirs in one step:
   `pixi run init-data-root --add-fabric oregon` appends a profile stub under
   `fabrics:` in `configs/base_config.yml` (or hand-edit it). Then fill the
   stub's TODO placeholders. **All shared, required fabric inputs live in the
   profile** — no required path on a CLI arg or inferred from a naming
   convention. Every fabric needs `expected_max_hru_id`, `batch_size`,
   `id_feature` (the HRU id column present in the fabric — e.g. `nat_hru_id`
   for gfv2, `hru_id` for oregon — which flows through to the merged parameter
   CSVs), and `hru_gpkg`/`hru_layer` (the fabric geopackage + layer,
   authoritative for `prepare_fabric`, the ssflux `build_weights` step, and
   gap-fill). If the depstor pipeline will be run, also set `template_raster`,
   `fdr_raster`, `twi_raster`, `segments_gpkg`/`segments_layer`, and
   `waterbody_gpkg`/`waterbody_layer` (waterbody is **required** for depstor —
   the step raises if it is unset). For `template_raster`/`fdr_raster`, stage a
   fabric-bounds clip of the CONUS FDR with
   `pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric <name>`
   (writes `{data_root}/<name>/shared/<name>_fdr.vrt`) and point both keys at it.
   Every depstor builder sizes its arrays to the `template_raster` grid, so the
   clip scopes compute to the fabric extent while staying VPU-agnostic (works for
   fabrics that straddle VPU boundaries). The clip comes from `fdr.vrt` — the
   hydrology lattice `carea_map` requires the template to share with `twi.vrt`
   (`elevation.vrt` is on the offset DEM lattice and is rejected). `twi_raster`
   uses the CONUS `twi.vrt` (warp-windowed onto the template). For a single-file
   fabric, `segments_gpkg` can point at the same gpkg as `hru_gpkg` with
   `segments_layer: nsegment`. The `oregon` profile has these keys active
   (issue #90), using the CONUS NHDPlusV2 waterbodies at
   `input/nhd/conus_waterbodies.gpkg` (layer `waterbodies`). To obtain valid
   `carea_max`/`smidx_coef` for non-VPU-01 fabrics, use
   `threshold_mode: percentile` with `twi_raster` pointing at
   `twi_hydrodem.vrt` (the CONUS-complete open-source source built by
   `build_vrt`) and build the `twi_reference` percentile table first (Stage
   2a' in `slurm_batch/RUNME.md`); see
   `docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md`.
2. Place the fabric gpkg at the `hru_gpkg` path you set, under
   `{data_root}/oregon/fabric/` (NOT in `input/fabric/`)
3. Run `prepare_fabric.py --fabric oregon` (reads `hru_gpkg` from the profile —
   no `--fabric_gpkg` needed), then submit Part 2 jobs via
   `slurm_batch/submit_zonal_params.sh $BATCHES oregon configs/base_config.yml`
   (loops every entry in `configs/zonal/zonal_params.yml` and chains array
   + merge per param). For Part 1 raster prep, `sbatch slurm_batch/build_shared_rasters.batch`.
   The Part 2 zonal pass (and depstor) read the CONUS shared rasters from Part 1,
   so scope Part 1 to the VPUs your fabric overlaps — Oregon HRUs fall in VPU 17
   (incidental), so `VPUS=17 sbatch slurm_batch/build_shared_rasters.batch`
   avoids rebuilding all of CONUS for a regional test. Stage 2d depstor is
   **active** for `oregon` (issue #90); after staging the FDR clip (step 1),
   run `FABRIC=oregon sbatch slurm_batch/build_depstor_rasters.batch`. For valid
   `carea_max`/`smidx_coef`, use `threshold_mode: percentile` with
   `twi_hydrodem.vrt` after completing Stage 2a' (`twi_reference`).

**VPU-based fabric** (per-VPU gpkgs that need merging — e.g., gfv2):

1. Register the fabric + scaffold dirs: `pixi run init-data-root --add-fabric <name>`
   (or hand-edit `fabrics:`), then fill the stub's TODO placeholders. Place
   per-VPU gpkgs in `input/fabric/`.
2. Merge with `pixi run -e notebooks marimo run notebooks/merge_vpu_targets.py`, then run
   `prepare_fabric.py` and all stages with `--fabric <name>` (or `FABRIC=<name>`).

See `slurm_batch/RUNME.md` for the full step-by-step workflow.

## Shared rasters pipeline

The CONUS shared-raster preparation (Part 1 of the workflow) is driven by
one orchestrator and one unified config:

- [scripts/build_shared_rasters.py](scripts/build_shared_rasters.py) reads
  [configs/shared_rasters/shared_rasters.yml](configs/shared_rasters/shared_rasters.yml) and walks the
  step DAG via the builder modules under
  [src/gfv2_params/shared_rasters/](src/gfv2_params/shared_rasters/).
- The DAG covers per-VPU NHDPlus prep (`merge_rpu_by_vpu`,
  `compute_slope_aspect`), border-DEM fill (`build_border_dem`), per-VPU
  HRU landmask (`build_vpu_landmask`), the masked TWI merge
  (`merge_rpu_by_vpu_twi`), CONUS VRT assembly (`build_vrt`), and CONUS
  derived rasters (`build_derived_rasters`, `build_lulc_rasters`).
- `compute_dem_derivatives` is registered as an opt-in step (parallel
  open-source TWI pipeline). The absolute calibration thresholds (8.0/15.6)
  used by `carea_max`/`smidx_coef` still require the ArcPy-derived
  `twi.vrt` when `threshold_mode: absolute`. However, `carea_map` now also
  supports `threshold_mode: percentile` (configured in
  `configs/depstor/depstor_rasters.yml`): it derives the TWI cutoff from the
  data via the per-VPU reference table produced by the `twi_reference`
  shared-raster step, making the open-source `twi_hydrodem.vrt` a first-class
  source that is safe to use. See
  `docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md`
  for the full design rationale.

These outputs live under `{data_root}/shared/` and are **fabric-independent**:
every fabric reuses the same CONUS rasters. Per-VPU iteration happens
inside the builders, not in per-VPU sbatch launches, so the orchestrator
runs as a single job. The per-script entrypoints and sbatch wrappers are
preserved as thin shells around the same builders.

## Depression-storage pipeline

The depstor pipeline (Levels 2-5) is driven by two orchestrators and two
unified configs:

- [scripts/build_depstor_rasters.py](scripts/build_depstor_rasters.py) reads
  [configs/depstor/depstor_rasters.yml](configs/depstor/depstor_rasters.yml) and walks the
  10-step DAG (landmask → imperv/streambuffer/waterbody → dprst → perv/routing
  → drains_perv/drains_imperv → carea_map) via the builder modules under
  [src/gfv2_params/depstor_builders/](src/gfv2_params/depstor_builders/).
- [scripts/derive_depstor_params.py](scripts/derive_depstor_params.py) reads
  [configs/depstor/depstor_params.yml](configs/depstor/depstor_params.yml) and dispatches the
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

## Zonal-pass parameter pipeline

The Part 2 per-fabric zonal-pass parameter pipeline is driven by one
orchestrator over one unified config:

- [scripts/derive_zonal_params.py](scripts/derive_zonal_params.py) reads
  [configs/zonal/zonal_params.yml](configs/zonal/zonal_params.yml) and dispatches every
  Part 2 param type (`elevation`, `slope`, `aspect`, `soils`,
  `soil_moist_max`, `lulc_nhm_v11`, `lulc_nalcms`, `lulc_nlcd`,
  `lulc_foresce`, `ssflux`) into the matching per-script work function
  under [src/gfv2_params/zonal_runners.py](src/gfv2_params/zonal_runners.py).
  Three modes: `--mode zonal --param <name> --batch_id <N>`,
  `--mode merge --param <name>`, `--mode build_weights` (CONUS-once ssflux
  prereq).
- The slurm wrapper
  [slurm_batch/submit_zonal_params.sh](slurm_batch/submit_zonal_params.sh)
  loops every entry in `params:` and submits per-param array + merge jobs
  in one go (chained via `afterok`). When an entry carries
  `depends_on: build_weights` (typically `ssflux`), the wrapper submits
  `build_zonal_weights.batch` first and chains the ssflux array + merge on
  its `afterok`. ssflux also chains on the merged slope CSV.

```bash
bash slurm_batch/submit_zonal_params.sh \
    {data_root}/gfv2_vpu01/batches gfv2_vpu01 configs/base_config.yml
```

(The fabric is the 2nd positional argument; `FABRIC=` env can also drive
non-default fabric resolution for the per-job library calls but is
redundant when the positional is given.)

The orchestrator's per-step library functions live under
[src/gfv2_params/zonal_runners.py](src/gfv2_params/zonal_runners.py).
For per-step debugging, invoke the orchestrator directly with
`--mode zonal --param <name> --batch_id <N>` (see "Single-batch run" above).

## Viewing fabric results

Once a fabric is processed, the three Jupyter notebooks in
[notebooks/fabric_results/](notebooks/fabric_results/) give a complete picture of
a fabric's parameterization — the inputs that fed it and the per-HRU results:

| Notebook | Shows |
|---|---|
| `01_input_rasters.ipynb` | Every shared/zonal source raster clipped to the fabric bounds, HRU outline overlaid. |
| `02_depstor_rasters.ipynb` | The 14 per-fabric depression-storage binary/label rasters + coverage stats. |
| `03_param_results.ipynb` | Choropleth + distribution of all 16 merged per-HRU params, plus a depstor-ratio summary. |

All three are **parameterized by the `FABRIC` env var** (default `oregon`) and read
the active profile via `load_base_config`; they share the tested helpers in
[src/gfv2_params/viz.py](src/gfv2_params/viz.py). Run them **inside JupyterHub on a
compute node with enough `--mem`** — a full CONUS `gfv2` render loads ~361k HRU
polygons and is too large for the login node. Per-fabric launch notes live in
`notebooks/<fabric>/README.md` (e.g. [notebooks/oregon/README.md](notebooks/oregon/README.md)).

**Saving figures for a report.** Set `SAVE_FIGURES=1` (or `viz.SAVE_FIGURES = True`
in the first cell) to write each plot to `docs/figures/<fabric>/` with a
`{section}_{item}.png` name. To regenerate the whole set headlessly:

```bash
pixi run -e notebooks python scripts/render_figures.py --fabric oregon
```

The PNGs under `docs/figures/<fabric>/` are committed; the executed notebook copies
in `docs/figures/.cache/` are gitignored.

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
