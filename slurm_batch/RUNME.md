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
pixi run python scripts/build_vrt.py --base_config configs/base_config.yml
```

> **Migrating from `geoenv`?** The legacy `environment.yml` is retained as a
> deprecated fallback only. New work should use pixi.

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
│   │   └── nhm_v11/    # NHM v1.1 pre-derived LULC rasters (downloadable)
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
sbatch slurm_batch/build_depstor_imperv.batch

# VPU01 validation overlay — smaller; override resources
FABRIC=gfv2_vpu01 sbatch --time=01:00:00 --mem=16G slurm_batch/build_depstor_imperv.batch
```

`submit_jobs.sh` accepts fabric as its 5th positional argument and forwards it
via `--export=ALL,FABRIC=...` to the array job (and the chained merge job).

## Pipeline Stages

All commands below assume the repo root as your working directory, e.g.:
```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
```

---

## Part 1: Fabric-Independent Tasks

These stages do not require a watershed fabric and can be run while fabric preparation proceeds in parallel. Complete all Part 1 stages before moving to Part 2.

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
| `input/depstor/` | Per-fabric: `<fabric>_segments_wbodies.gpkg` (layers `nsegment`, `v2_wb`); `<fabric>_fdr.tif` (D8 flow direction, Esri pointer encoding) |
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

### Stage 1: Raster preparation (VPU-based)

Download and merge per-RPU NHDPlus rasters, then derive slope/aspect:

```bash
sbatch slurm_batch/merge_rpu_by_vpu.batch
sbatch slurm_batch/compute_slope_aspect.batch
```

### Stage 1b: Build border DEM fill (one-time)

Download Copernicus GLO-30 tiles and build elevation/slope/aspect fill rasters
for HRUs that extend into Canada or Mexico beyond NHDPlus coverage:

```bash
sbatch slurm_batch/build_border_dem.batch
```

This creates fill rasters in `work/nhd_merged/copernicus_fill/`. The subsequent
`build_vrt.py` step composites these behind the NHDPlus tiles, so NHDPlus takes
priority where it has valid data and Copernicus fills the border gaps.

**Dependency:** Must run AFTER Stage 1 completes, because it needs the
NHDPlus `_fixed_` elevation tiles produced by `compute_slope_aspect.py` to
build a seamless composite elevation surface for slope/aspect computation.

### Stage 1c: Merge TWI by VPU

Merge the per-RPU TWI rasters staged in Stage 0 into per-VPU GeoTIFFs:

```bash
sbatch slurm_batch/merge_rpu_by_vpu_twi.batch
```

Produces `work/nhd_merged/<vpu>/Twi_merged_<vpu>.tif` for each of the 18 VPUs.
Independent of Stage 1 / 1b — can run as soon as the TWI staging in Stage 0 is
complete.

### Stage 2a: Build VRTs (one-time)

Combine per-VPU rasters and optional Copernicus fill into virtual rasters:

```bash
python scripts/build_vrt.py --base_config configs/base_config.yml
```

### Stage 2b: Build derived rasters (one-time)

Pre-compute `rd_250_raw.tif` and `soil_moist_max.tif`:

```bash
sbatch slurm_batch/build_derived_rasters.batch
```

To use a different fabric, override `FABRIC`:

```bash
FABRIC=oregon sbatch slurm_batch/build_derived_rasters.batch
```

### Stage 2c: Build LULC derived rasters (one-time)

Pre-compute radiation transmission raster from LULC + canopy + keep:

```bash
sbatch slurm_batch/build_lulc_rasters.batch
```

To use a different LULC source, override `LULC_CONFIG`:

```bash
LULC_CONFIG=configs/lulc_nalcms_param.yml sbatch slurm_batch/build_lulc_rasters.batch
```

### Stage 2d: Build depstor rasters (per fabric)

Build the depression-storage intermediate rasters on the elevation-VRT template
grid. Outputs go to `{fabric}/depstor_rasters/` and feed the new Stage 4
zonal-stats jobs (`dprst_frac`, `imperv_frac`, `onstream_storage_frac`,
`drains_to_dprst_frac`).

Inputs (manually staged per fabric):
- `input/depstor/<fabric>_segments_wbodies.gpkg` (layers `nsegment`, `v2_wb`)
- `input/depstor/<fabric>_fdr.tif` (D8 flow direction, Esri pointer)
- Reuses existing `input/lulc_veg/Imperv.tif` and `work/nhd_merged/elevation.vrt`.

Run in order — each step writes intermediates the next consumes:

```bash
sbatch slurm_batch/build_depstor_imperv.batch
sbatch slurm_batch/build_depstor_streambuffer.batch
sbatch slurm_batch/build_depstor_waterbody.batch
sbatch slurm_batch/build_depstor_dprst.batch        # depends on the three above
sbatch slurm_batch/build_depstor_perv.batch         # depends on imperv + dprst
sbatch slurm_batch/build_depstor_routing.batch      # depends on dprst + staged FDR
```

Steps 1-3 are independent of each other and can run concurrently. Step 4
combines them and must wait. Steps 5 and 6 each depend on outputs from earlier
steps and can run in parallel with one another. Step 6 (`build_depstor_routing`)
runs WhiteboxTools `Watershed` against the staged FDR + the dprst output and is
the most memory- and time-intensive.

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

Submit batch jobs using the wrapper script:

Pass the corresponding param config as the 4th argument to auto-submit a merge job
that runs immediately after each array job completes (`afterok` dependency):

```bash
BATCHES=/path/to/gfv2/batches
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_elev_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_slope_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_zonal_aspect_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_soils_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_soilmoistmax_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_nhm_v11_params.batch
```

To use an alternative LULC source (NLCD or NALCMS) instead of NHM v1.1:
```bash
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_nlcd_params.batch
# or
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_nalcms_params.batch
```

The `create_lulc_params.batch` job produces per-HRU fractional land cover percentages for each
NALCMS 2020 class (19 classes). Output: `{fabric}/params/nalcms_2020/` per batch, merged to
`{fabric}/params/merged/nhm_nalcms_2020_lulc_params.csv`.

Depression-storage zonal stats (require Stage 2d outputs):

```bash
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_dprst_frac_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_imperv_frac_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_onstream_storage_frac_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_drains_to_dprst_frac_params.batch
slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_perv_frac_params.batch
```

Each produces a per-HRU fraction in [0, 1]. With `categorical: false` on a uint8
binary raster, the gdptools exactextract mean equals the fraction of HRU area
covered by 1-valued cells.

### Stage 5: Merge and validate

If all Stage 4 jobs were submitted with the 4th merge-config argument (recommended), merges
run automatically as chained SLURM jobs. To re-run all merges manually at once:

```bash
sbatch slurm_batch/merge_output_params.batch
```

Or individually:
```bash
python scripts/merge_params.py --config configs/elev_param.yml --base_config configs/base_config.yml
```

### Stage 6: SSFlux (depends on merged slope)

A single batch job handles the full ssflux workflow: pre-compute weights, submit the
ssflux array job, and automatically chain a merge job via `afterok` dependency:

```bash
BATCHES={data_root}/gfv2/batches \
  sbatch slurm_batch/build_weights.batch
```

The job sequence is:
1. `build_weights.py` — computes CONUS-wide P2P lithology weights
2. `submit_jobs.sh` — submits the ssflux array; merge job is chained automatically

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
5. Submit parameter jobs, passing the fabric as the 5th positional arg to
   submit_jobs.sh (or via `FABRIC` env on direct sbatch calls):
   ```bash
   BATCHES={data_root}/oregon/batches
   slurm_batch/submit_jobs.sh $BATCHES slurm_batch/create_lulc_params.batch \
       configs/base_config.yml configs/lulc_nalcms_param.yml oregon
   ```

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

To rerun a single failed batch (the batch file reads `$SLURM_ARRAY_TASK_ID` regardless of how the array was specified):
```bash
sbatch --array=37 slurm_batch/create_zonal_elev_params.batch
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
| merge_rpu_by_vpu.batch | merge_rpu_by_vpu.yml | merge_rpu_by_vpu.py |
| merge_rpu_by_vpu_twi.batch | merge_rpu_by_vpu_twi.yml | merge_rpu_by_vpu.py |
| stage_twi.batch | (uses base_config.yml indirectly) | scripts/stage_twi.sh |
| compute_slope_aspect.batch | slope_aspect.yml | compute_slope_aspect.py |
| build_border_dem.batch | base_config.yml | build_border_dem.py |
| build_derived_rasters.batch | base_config.yml | build_derived_rasters.py |
| build_lulc_rasters.batch | lulc_nhm_v11_param.yml | build_lulc_rasters.py |
| create_zonal_elev_params.batch | elev_param.yml | create_zonal_params.py |
| create_zonal_slope_params.batch | slope_param.yml | create_zonal_params.py |
| create_zonal_aspect_params.batch | aspect_param.yml | create_zonal_params.py |
| create_soils_params.batch | soils_param.yml | create_soils_params.py |
| create_soilmoistmax_params.batch | soilmoistmax_param.yml | create_soils_params.py |
| create_lulc_nhm_v11_params.batch | lulc_nhm_v11_param.yml | create_lulc_params.py |
| create_lulc_params.batch | lulc_foresce_param.yml | create_lulc_params.py |
| create_lulc_nlcd_params.batch | lulc_nlcd_param.yml | create_lulc_params.py |
| create_lulc_nalcms_params.batch | lulc_nalcms_param.yml | create_lulc_params.py |
| build_weights.batch | ssflux_param.yml | build_weights.py → create_ssflux_params.py → merge_params.py |
| create_ssflux_params.batch | ssflux_param.yml | create_ssflux_params.py |
| merge_output_params.batch | all param configs | merge_params.py |
| merge_params.batch | (via MERGE_CONFIG env) | merge_params.py |
| merge_default_output_params.batch | base_config.yml | merge_default_params.py |
| download_rpu_rasters.batch | base_config.yml | gfv2_params.download.rpu_rasters |
| download_nalcms.batch | base_config.yml | gfv2_params.download.nalcms_lulc |
| download_nhm_v11.batch | base_config.yml | gfv2_params.download.nhm_v11_lulc |
| build_depstor_imperv.batch | depstor_imperv_raster.yml | build_depstor_imperv.py |
| build_depstor_streambuffer.batch | depstor_streambuffer_raster.yml | build_depstor_streambuffer.py |
| build_depstor_waterbody.batch | depstor_waterbody_raster.yml | build_depstor_waterbody.py |
| build_depstor_dprst.batch | depstor_dprst_raster.yml | build_depstor_dprst.py |
| build_depstor_perv.batch | depstor_perv_raster.yml | build_depstor_perv.py |
| build_depstor_routing.batch | depstor_routing_raster.yml | build_depstor_routing.py |
| create_dprst_frac_params.batch | dprst_frac_param.yml | create_zonal_params.py |
| create_imperv_frac_params.batch | imperv_frac_param.yml | create_zonal_params.py |
| create_onstream_storage_frac_params.batch | onstream_storage_frac_param.yml | create_zonal_params.py |
| create_drains_to_dprst_frac_params.batch | drains_to_dprst_frac_param.yml | create_zonal_params.py |
| create_perv_frac_params.batch | perv_frac_param.yml | create_zonal_params.py |
