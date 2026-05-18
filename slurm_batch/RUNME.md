# GFv2 Pipeline: HPC Workflow

## Prerequisites

This pipeline uses [pixi](https://pixi.sh) for environment management. Install
pixi once per user (see https://pixi.sh/latest/installation/) and ensure
`~/.pixi/bin` is on your `PATH`. From the repo root:

```bash
pixi install
bash scripts/refresh_pixi_activation.sh
```

The first command materialises `.pixi/envs/default/` from `pixi.lock` (config
lives in `pyproject.toml` under `[tool.pixi.*]`). The second pre-bakes a
static activation script (`.pixi-activate.sh`) that the slurm batches
`source` instead of invoking pixi at task start. Why: concurrent
`pixi shell-hook` calls under array submission race on
`.pixi/envs/default/conda-meta/` reads ("File was modified during parsing",
etc.) and a fraction of tasks fail before reaching python. Sourcing a
pre-baked script is pure shell — no concurrency surface.

**Re-run `bash scripts/refresh_pixi_activation.sh`** any time `pyproject.toml`
or `pixi.lock` change.

For interactive use:

```bash
pixi shell                       # default env
pixi shell -e notebooks          # default + marimo, plotly, hvplot, ...
pixi shell -e dev                # default + pytest, ruff, pre-commit
```

Run a one-off command in the env without an interactive shell:

```bash
pixi run python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml --step build_vrt
```

> **Migrating from `geoenv`?** The legacy `environment.yml` is retained as a
> deprecated fallback only. New work should use pixi.

## Data Directory Layout

All data lives under `data_root` (set in `configs/base_config.yml`):

```
gfv2_param/
├── input/                  # External data (manually staged or downloaded)
│   ├── fabrics/            # Per-VPU and custom watershed fabric gpkgs
│   ├── nhd_downloads/
│   ├── mrlc_impervious/
│   ├── soils_litho/
│   ├── lulc_veg/
│   │   └── nhm_v11/        # NHM v1.1 pre-derived LULC rasters (downloadable)
│   └── nhm_defaults/
├── shared/                 # Fabric-independent intermediates (reused by every fabric)
│   ├── source/             # Unzipped per-RPU NHDPlus rasters
│   ├── per_vpu/<vpu>/      # Per-VPU merged GeoTIFFs (NED/Hydrodem/Fdr/Fac/Twi/slope/aspect/landmask)
│   └── conus/
│       ├── vrt/            # CONUS-wide GDAL virtual rasters (elevation/slope/aspect/fdr/twi)
│       ├── derived/        # soil_moist_max.tif, radtrn, resampled CNPY/keep
│       ├── borders/        # Copernicus border-DEM fill (Canada/Mexico)
│       └── weights/        # P2P polygon weights for ssflux
└── {fabric}/               # Per-fabric outputs (e.g., gfv2/, oregon/)
    ├── fabric/             # Merged fabric gpkg
    ├── batches/            # Per-batch gpkgs + manifest
    └── params/             # Parameter outputs + merged + filled
```

> **Upgrading an existing `data_root` from the legacy `work/` layout?** Run
> `pixi run python scripts/migrate_to_shared_layout.py --data-root <path> --dry-run`
> to preview the 27 directory renames, then `--execute` to apply them.
> Atomic `os.rename` on the same filesystem (metadata-only, near-instant);
> regenerates CONUS VRTs at the end since they encode absolute source paths.
> Idempotent — re-running after success is a no-op.

## Selecting a fabric

Fabric identities and their per-fabric inputs (`template_raster`, `fdr_raster`,
`waterbody_gpkg`/layer, `expected_max_hru_id`, `batch_size`) live as profiles
in a single `configs/base_config.yml` under a `fabrics:` mapping. The active
profile is selected via:

1. `--fabric <name>` CLI flag on any script, OR
2. `FABRIC` env var passed through sbatch, OR
3. `default_fabric` in `configs/base_config.yml` (currently `gfv2`).

Slurm batches default to `gfv2`. To run the same batch against a different
fabric, set `FABRIC` and (optionally) override resource asks at submission:

```bash
# CONUS gfv2 — default
sbatch slurm_batch/build_depstor_rasters.batch

# VPU01 validation overlay — smaller; override resources
FABRIC=gfv2_vpu01 sbatch --time=02:00:00 --mem=48G slurm_batch/build_depstor_rasters.batch
```

`submit_jobs.sh` accepts fabric as its 5th positional argument and forwards it
via `--export=ALL,FABRIC=...` to the array job (and the chained merge job).

It also accepts an optional 6th argument (or `SUBMIT_JOBS_MAX_CONCURRENT` env
var) capping how many array tasks run at once — defaults to 4. The cap exists
because concurrent geo-library imports (rasterio / GDAL / PROJ / pyogrio) can
deadlock under shared-FS metadata contention when many tasks start
simultaneously; one of eight VPU01 array tasks hung indefinitely during the
issue-#61 smoke test. Set to `0` (or `off`) to disable the cap. For CONUS the
default of 4 trades ~1 wave of wall-clock time for reliability.

## Pipeline Stages

All commands below assume the repo root as your working directory, e.g.:
```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
```

---

## Part 1: Fabric-Independent Tasks

These stages do not require a watershed fabric and can be run while fabric preparation proceeds in parallel. Complete all Part 1 stages before moving to Part 2.

### Recommended: run Part 1 via the unified shared-rasters orchestrator

After Stage 0 completes and the downloads in Stages 1/1b have finished, every
remaining raster prep step can be driven from one sbatch:

```bash
sbatch slurm_batch/build_shared_rasters.batch
```

This walks the full DAG (merge_rpu_by_vpu → compute_slope_aspect →
build_border_dem → build_vpu_landmask → merge_rpu_by_vpu_twi → build_vrt →
build_derived_rasters → build_lulc_rasters) in dependency order, replacing
the per-stage sbatch invocations below. Per-VPU steps iterate the `vpus`
list inside `configs/shared_rasters/shared_rasters.yml` rather than launching one sbatch
per VPU. Env knobs the batch honours:

- `FORCE=1` — pass `--force` to rebuild outputs that already exist
- `VPUS=01,02` — pass `--vpus 01,02` to restrict per-VPU steps to a subset

For interactive use (or finer-grained flags like `--step <name>` or
`--from <name>`), invoke the orchestrator directly:

```bash
pixi run python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml
```

The individual `slurm_batch/*.batch` files and per-script CLIs documented in
Stages 1 through 2c below are preserved as thin shells around the same
library builders. Use them when you want per-step granularity or per-VPU
parallelism via SLURM arrays; use the orchestrator batch when you want one
job that walks the whole DAG.

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
| `input/depstor/` | Per-fabric: `<fabric>_segments_wbodies.gpkg` (layers `nsegment`, `v2_wb`). The D8 flow-direction raster is sourced from the shared `shared/conus/vrt/fdr.vrt` produced by Part 1 — no fabric-specific FDR is required here. |
| `input/twi/<rpu>/` | Per-RPU TWI raster `twi.tif` (+ `.tfw`, `.aux.xml`, `.ovr`, `.xml` sidecars). Stage with `bash scripts/stage_twi.sh` (see below). |

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

**Download NHM v1.1 LULC rasters** from ScienceBase item `5ebb182b82ce25b5136181cf`
(`LULC.zip`, `keep.zip`, `CNPY.zip` — network-bound; submit as a SLURM job):

```bash
sbatch slurm_batch/download_nhm_v11.batch
```

All download scripts are idempotent — already-downloaded files are skipped on resubmission.

**Stage per-RPU TWI rasters** — provenance is USGS ScienceBase item
`5f5154ba82ce4c3d12386a02`
(<https://www.sciencebase.gov/catalog/item/5f5154ba82ce4c3d12386a02> — **not a
public link**; access is gated). For impd-group users, an operational mirror
lives on the shared cluster filesystem; the staging script reads from it by
default and copies into `input/twi/<rpu>/twi.tif`:

```bash
bash scripts/stage_twi.sh
# or pass an alternate source:
bash scripts/stage_twi.sh /alt/path/to/data_bins
```

This is a ~30 GB single-threaded `cp` against the shared filesystem. Running it
on a login node is borderline (busy login nodes don't love sustained I/O) — the
recommended path for an unattended run is the slurm wrapper:

```bash
sbatch slurm_batch/stage_twi.batch
# or with an alternate source:
SRC=/alt/path/to/data_bins sbatch slurm_batch/stage_twi.batch
```

The script handles HRU06a's uppercase `TWI.*` source filenames by normalizing
to lowercase in the destination so the merge config can reference all 18 VPUs
without per-RPU casing exceptions. Idempotent — re-running skips files already
present and newer than the source.

Note: `--check` only validates manually-staged inputs (soils, litho, lulc_veg, twi). Verify downloads completed successfully by checking the job logs before proceeding.

### Stages 1, 1b, 1c1, 1c2, 2a, 2b, 2c — Shared raster prep (one orchestrator)

**All of these are now driven by `sbatch slurm_batch/build_shared_rasters.batch`** (or the
`pixi run python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml`
interactive equivalent). The orchestrator walks the canonical 8-step DAG in
dependency order — there is no longer a per-step batch surface; all the
per-step CLIs/sbatches were retired now that the orchestrator covers them.

For a single-step rebuild, pass `--step <name>` to the orchestrator (e.g.
`--step build_border_dem` or `--step build_vrt`). Use `--from <name>` to
resume mid-DAG. Step names match the keys in
[configs/shared_rasters/shared_rasters.yml](../configs/shared_rasters/shared_rasters.yml).

The narrative below describes what each step does and what it depends on;
the bash invocation is always the same orchestrator.

**Stage 1 — `merge_rpu_by_vpu` + `compute_slope_aspect`:** Merge per-RPU
NHDPlus rasters into per-VPU GeoTIFFs (NED, Hydrodem, FDR, FAC), then derive
slope/aspect on the fixed-nodata NEDSnapshot.

**Stage 1b — `build_border_dem`:** Download Copernicus GLO-30 tiles and
build elevation/slope/aspect fill rasters for HRUs that extend into Canada or
Mexico beyond NHDPlus coverage. Creates fill rasters in `shared/conus/borders/`;
the subsequent `build_vrt` step composites these behind NHDPlus so NHDPlus
takes priority where it has valid data and Copernicus fills the border gaps.
Depends on Stage 1 — needs the NHDPlus `_fixed_` elevation tiles to build a
seamless composite for slope/aspect computation.

**Stage 1c1 — `build_vpu_landmask`:** Build the per-VPU HRU-fabric land mask
consumed by both TWI pipelines. Produces `shared/per_vpu/<vpu>/land_mask_<vpu>.tif`
— a uint8 1/255 raster where 1 = inside an HRU whose `vpu` attribute matches
this VPU, 255 = outside. The mask is rasterised onto the per-VPU
`Hydrodem_merged_<vpu>.tif` grid, so TWI products downstream get a strict match
to their VPU's HRU coverage rather than the CONUS-wide depstor `land_mask.tif`
(which leaves cells unmasked wherever adjacent-VPU HRUs drape into a VPU's
Hydrodem footprint). Depends only on Stage 1; independent of Stage 2d.

**Stage 1c2 — `merge_rpu_by_vpu_twi`:** Merge the per-RPU TWI rasters staged
in Stage 0 into per-VPU GeoTIFFs (`Twi_merged_<vpu>.tif`). The merge clips its
output to the per-VPU HRU mask from Stage 1c1 so the per-RPU TWI bulges
(coast on the east, adjacent-VPU/border drape on the west/north) never reach
downstream zonal aggregation. Depends on Stage 1c1.

**Stage 2a — `build_vrt`:** Combine per-VPU rasters and optional Copernicus
fill into CONUS-wide GDAL virtual rasters (elevation/slope/aspect/fdr/twi).

**Stage 2b — `build_derived_rasters`:** Pre-compute `rd_250_raw.tif` and
`soil_moist_max.tif`.

**Stage 2c — `build_lulc_rasters`:** Pre-compute canopy-resampled + keep-resampled
+ radiation-transmission rasters for every LULC source listed in
[configs/shared_rasters/shared_rasters.yml](../configs/shared_rasters/shared_rasters.yml)'s
`sources:` block (currently 4 sources: nhm_v11, nalcms, nlcd, foresce).

**Per-fabric overrides:** All of the above are fabric-independent (CONUS).
For depstor-only fabric overrides (Stage 2d), use `FABRIC=...` env in the
sbatch command.

### Stage 2d: Build depstor rasters (per fabric)

Build the full depression-storage raster stack on the elevation-VRT template
grid. Outputs go to `{fabric}/depstor_rasters/` and feed the Stage 4 depstor
zonal-stats orchestrator below.

Inputs (manually staged per fabric):
- `input/depstor/<fabric>_segments_wbodies.gpkg` (layers `nsegment`, `v2_wb`)
- Per-fabric `hru_gpkg` / `twi_raster` (from `base_config.yml`).
- Shared `fdr_raster` from `shared/conus/vrt/fdr.vrt` (Part 1 output; no per-fabric FDR needed).
- The NLCD 2015 fractional-impervious raster (path set in
  `configs/depstor/depstor_rasters.yml` under `imperv_source`) and
  `shared/conus/vrt/elevation.vrt`.

One sbatch builds the entire stack in dependency order
(landmask → imperv/streambuffer/waterbody → dprst → perv/routing →
drains_perv/drains_imperv → carea_map). The 10-step DAG is encoded in
`src/gfv2_params/depstor_builders/__init__.py`; selective re-runs are supported
via `--step <name>` or `--from <name>` passed through to the python script.

```bash
sbatch slurm_batch/build_depstor_rasters.batch

# VPU01 validation overlay — smaller; override resources:
FABRIC=gfv2_vpu01 sbatch --time=02:00:00 --mem=48G \
    slurm_batch/build_depstor_rasters.batch

# Resume from a specific step (e.g. after a routing crash):
sbatch slurm_batch/build_depstor_rasters.batch --from routing --force
```

Default resources size the job for the long pole (`routing` — WhiteboxTools
Watershed on CONUS). For VPU01 / smaller fabrics, override `--time` and
`--mem` at submission as shown above.

Note: Stage 2d depends on Stage 2a (the elevation VRT exists) but is otherwise
fabric-independent of the rest of Part 1. It can run in parallel with Part 2's
fabric prep.

---

## Part 2: Fabric-Dependent Tasks

These stages require the merged fabric geopackage and the per-batch gpkgs produced by `prepare_fabric.py`. Complete Part 1 before proceeding.

### Stage 3: Prepare fabric (one-time per fabric)

Merge per-VPU fabric geopackages into a single CONUS fabric:

```bash
marimo run notebooks/merge_vpu_targets.py
```

Then spatially batch the merged fabric into per-batch geopackages (`batch_size` is read from `base_config.yml`):

```bash
python scripts/prepare_fabric.py \
    --fabric_gpkg {data_root}/gfv2/fabric/gfv2_nhru_merged.gpkg \
    --base_config configs/base_config.yml
```

### Stage 4: Generate parameters (SLURM array jobs)

#### Recommended: run Part 2 via the unified zonal-params dispatcher

After Stage 3 completes (fabric merged + batched), every Part 2 param can
be driven from one shell invocation:

```bash
BATCHES=/path/to/gfv2/batches
slurm_batch/submit_zonal_params.sh $BATCHES gfv2 configs/base_config.yml
```

This loops every entry in `configs/zonal/zonal_params.yml` (10 params today:
elevation, slope, aspect, soils, soil_moist_max, 4× LULC, ssflux) and
submits per-param array + merge jobs chained via `afterok`. ssflux's
`depends_on: build_weights` prereq is detected automatically: the wrapper
submits `build_zonal_weights.batch` first and chains the ssflux array on
its `afterok` (the weight matrix is CONUS-once per fabric — idempotent).
ssflux also chains on the merged slope CSV.

Env knobs:

- `FABRIC=gfv2_vpu01` — switch to a non-default fabric
- `SUBMIT_JOBS_MAX_CONCURRENT=4` — concurrency cap per array job (default 4)
- `FORCE=1` — passed to build_zonal_weights to overwrite the existing matrix

The per-param Python CLIs and per-param sbatch wrappers were retired now
that the orchestrator covers them. For single-param debugging, invoke the
orchestrator directly:

```bash
python scripts/derive_zonal_params.py --mode zonal --param elevation --batch_id 42 \
    --config configs/zonal/zonal_params.yml --base_config configs/base_config.yml
```

`--mode merge --param <name>` runs just the merge for one param;
`--mode build_weights` builds the CONUS-once ssflux prereq.

Depression-storage zonal stats + Level-5 ratios (require Stage 2d outputs):

```bash
slurm_batch/submit_depstor_params.sh $BATCHES
# or for a non-default fabric:
slurm_batch/submit_depstor_params.sh $BATCHES gfv2_vpu01
```

A single call submits 10 zonal-stats array jobs (one per fraction), chains 10
merge jobs via `afterok`, and finally chains one ratios job that depends on
every merge. Outputs land in two subdirectories under `{fabric}/params/merged/`:

- `{fabric}/params/merged/` — **6 final PRMS-ready ratio CSVs**, all
  dimensionless and bounded in [0, 1]:
  `sro_to_dprst_perv`, `sro_to_dprst_imperv`, `carea_max`, `smidx_coef`,
  `hru_percent_imperv`, `dprst_frac`.
- `{fabric}/params/merged/_intermediates/` — **10 per-fraction count CSVs**
  (`nhm_<name>_frac_params.csv` and `nhm_hru_total_count_params.csv`).
  Each row's `count` column is the partial-pixel-weighted sum of `1`-valued
  cells per HRU — **NOT** a [0, 1] fraction. Inputs to the ratio derivation;
  not direct PRMS parameters. To get a true area fraction divide by the HRU
  pixel count (e.g. `areasqkm * 1e6 / 900` for the 30 m template grid; the
  `hru_total` fraction aggregates `land_mask.tif` to give exactly that
  denominator).

### Stage 5: Merge and validate

`submit_zonal_params.sh` chains a merge job after every per-param array via
`afterok`, so Stage 5 is automatic — there is no separate manual-merge
step. To re-run a single param's merge (e.g., after manually fixing a
batch CSV):

```bash
python scripts/derive_zonal_params.py --mode merge --param elevation \
    --config configs/zonal/zonal_params.yml --base_config configs/base_config.yml
```

### Stage 6: SSFlux (depends on merged slope)

Handled automatically by `submit_zonal_params.sh` — the dispatcher sees
`depends_on: build_weights` on the ssflux entry in
`configs/zonal/zonal_params.yml`, submits `build_zonal_weights.batch`
first, and chains the ssflux array + merge on its `afterok`. ssflux also
chains on the merged slope CSV.

To build the CONUS-once weight matrix on its own (idempotent — skips if
the matrix already exists, pass `FORCE=1` to overwrite):

```bash
sbatch slurm_batch/build_zonal_weights.batch
```

### Stage 7: KNN gap-fill

```bash
python scripts/merge_and_fill_params.py --base_config configs/base_config.yml
```

### Stage 8: Merge NHM defaults (optional)

```bash
python scripts/merge_default_params.py --base_config configs/base_config.yml
```

## Adding a new fabric (e.g., Oregon)

A new fabric is added by appending a profile to `configs/base_config.yml` —
one file edit, no new YAMLs. Two cases depending on whether the fabric is
already merged or comes as per-VPU gpkgs.

**Case A: Pre-merged fabric** (single gpkg covering the full domain — e.g., Oregon)

1. Add a profile under `fabrics:` in `configs/base_config.yml`. Required keys
   are `expected_max_hru_id` and `batch_size`. If the depstor pipeline will be
   run for this fabric, also set `template_raster`, `fdr_raster`,
   `segments_gpkg`, `waterbody_gpkg`, and `waterbody_layer`. The `oregon`
   profile shows the minimum (no depstor inputs yet).
2. Scaffold the fabric's output directories:
   ```bash
   python scripts/init_data_root.py --fabric oregon
   ```
3. Place the fabric gpkg directly in `{data_root}/oregon/fabric/` (NOT in `input/fabric/`)
4. Prepare batches:
   ```bash
   python scripts/prepare_fabric.py \
       --fabric_gpkg {data_root}/oregon/fabric/NHM_OR_draft.gpkg \
       --fabric oregon
   ```
5. Submit parameter jobs. Easiest is the unified Part 2 dispatcher (one
   invocation walks every param + chained merges + ssflux's weights prereq):
   ```bash
   BATCHES={data_root}/oregon/batches
   slurm_batch/submit_zonal_params.sh $BATCHES oregon configs/base_config.yml
   ```
   For per-param granularity, invoke the orchestrator directly with
   `--mode zonal --param <name> --batch_id <N>` (see "Single-batch run"
   in README.md or Stage 4 above).

**Case B: VPU-based fabric** (per-VPU gpkgs that need merging — e.g., gfv2)

1. Add a profile under `fabrics:` in `configs/base_config.yml`
2. Place per-VPU gpkgs in `input/fabric/`
3. Scaffold and merge:
   ```bash
   python scripts/init_data_root.py --fabric <name>
   marimo run notebooks/merge_vpu_targets.py
   ```
4. Continue from Stage 3 above, passing `--fabric <name>` (or `FABRIC=<name>` env)

## Partial Reruns

To rerun a single failed batch within one of the orchestrator's array
jobs, submit a one-task array against the generic per-batch worker with
`$PARAM` set:

```bash
sbatch --array=37 --export=ALL,PARAM=elevation,FABRIC=gfv2,BASE_CONFIG=configs/base_config.yml \
    slurm_batch/derive_zonal_params.batch
```

(For Part 1 raster prep, re-run the single step via the orchestrator's
`--step` flag: `python scripts/build_shared_rasters.py
--config configs/shared_rasters/shared_rasters.yml --step <name>`.)

## Monitoring

```bash
squeue -u "$USER"
tail -n 200 logs/job_*.out
sacct -j <JOBID> -o JobID,State,Elapsed,MaxRSS
```

## Script -> Config -> Entry Point Mapping

Every production workflow is now driven by an orchestrator. The per-step
Python CLIs and per-step sbatch wrappers were retired now that the
orchestrators cover them — the library code they delegated to lives under
`src/gfv2_params/`.

### Orchestrators (the primary surface)

| Batch / shell | Config | Script |
|---|---|---|
| `build_shared_rasters.batch` | `shared_rasters/shared_rasters.yml` | `build_shared_rasters.py` |
| `build_depstor_rasters.batch` | `depstor/depstor_rasters.yml` | `build_depstor_rasters.py` |
| `submit_depstor_params.sh` | `depstor/depstor_params.yml` | `derive_depstor_params.py` (dispatches 10 fractions × zonal+merge, then ratios via afterok) |
| `submit_zonal_params.sh` | `zonal/zonal_params.yml` | `derive_zonal_params.py` (dispatches 10 params × zonal+merge, with ssflux's `build_weights` prereq chained automatically) |

### Workers invoked by the orchestrators (don't sbatch these directly)

| Batch | Used by | Config | Script |
|---|---|---|---|
| `derive_zonal_params.batch` | `submit_zonal_params.sh` | `zonal/zonal_params.yml` | `derive_zonal_params.py --mode zonal --param $PARAM` |
| `merge_zonal_param.batch` | `submit_zonal_params.sh` | `zonal/zonal_params.yml` | `derive_zonal_params.py --mode merge --param $PARAM` |
| `build_zonal_weights.batch` | `submit_zonal_params.sh` (for ssflux prereq) | `zonal/zonal_params.yml` | `derive_zonal_params.py --mode build_weights` |
| `create_depstor_zonal.batch` | `submit_depstor_params.sh` | `depstor/depstor_params.yml` | `derive_depstor_params.py --mode zonal --fraction $FRACTION` |
| `merge_depstor_fraction.batch` | `submit_depstor_params.sh` | `depstor/depstor_params.yml` | `derive_depstor_params.py --mode merge --fraction $FRACTION` |
| `derive_depstor_ratios.batch` | `submit_depstor_params.sh` | `depstor/depstor_params.yml` | `derive_depstor_params.py --mode ratios` |

### Standalone (one-off / setup / post-processing)

| Batch / shell | Config | Script |
|---|---|---|
| `stage_twi.batch` | `base_config.yml` (indirectly) | `scripts/stage_twi.sh` |
| `download_rpu_rasters.batch` | `base_config.yml` | `gfv2_params.download.rpu_rasters` |
| `download_nalcms.batch` | `base_config.yml` | `gfv2_params.download.nalcms_lulc` |
| `download_nhm_v11.batch` | `base_config.yml` | `gfv2_params.download.nhm_v11_lulc` |
| `diagnose_slope_aspect.batch` | per-config | diagnostic tool |
| `merge_default_output_params.batch` (Stage 8) | `base_config.yml` | `merge_default_params.py` |
| `submit_jobs.sh` | (caller-provided) | generic per-VPU array dispatcher |
