# gfv2-params

PRMS/NHM parameter generation from watershed fabric polygons.

Given a watershed fabric of polygons (HRUs), this pipeline computes parameters for the PRMS/National Hydrologic Model by performing zonal statistics against source rasters (DEM, soils, lithology, etc.).

For a navigable local docs site (Setup + HPC workflow + Architecture + Adding a parameter + Python patterns + auto-API ref), run `pixi run -e docs docs-serve` and open <http://localhost:8000>.

## Setup

Environment is managed by [pixi](https://pixi.sh). From the repo root:

```bash
pixi install                                          # materialise the env from pixi.lock
pixi shell -e dev                                     # interactive shell (default + pytest, ruff, pre-commit)
pixi run -e dev pytest tests/test_wbt.py -v           # example: run a small test
```

Install pixi once per user (see https://pixi.sh/latest/installation/) and ensure
`~/.pixi/bin` is on your `PATH`. For the full HPC workflow (downloads → shared
rasters → fabric → zonal + depstor params), see
[`slurm_batch/RUNME.md`](slurm_batch/RUNME.md) (runbook) and
[`slurm_batch/HPC_REFERENCE.md`](slurm_batch/HPC_REFERENCE.md) (per-stage detail).

<details>
<summary>Why <code>pixi run --as-is</code> and other SLURM gotchas</summary>

`pixi install` materialises `.pixi/envs/default/` from `pixi.lock` (config lives
in `pyproject.toml` under `[tool.pixi.*]`). SLURM batches invoke the env with
`pixi run --as-is` (= `--no-install --frozen`): the already-installed env is used
verbatim with no lock check or env mutation, so concurrent array tasks don't race
on `.pixi/envs/.../conda-meta`. Re-run `pixi install` after `pyproject.toml` or
`pixi.lock` change. Always `sbatch` from a shell where `~/.pixi/bin` is on
`PATH` — SLURM inherits the submitting shell's environment.

Other interactive shells are available for non-default workflows:

```bash
pixi shell                       # default env
pixi shell -e notebooks          # default + marimo, plotly, hvplot, ...
pixi shell -e dev                # default + pytest, ruff, pre-commit
```

The legacy `environment.yml` / `geoenv` conda environment is retained as a
deprecated fallback only — new work should use pixi.

</details>

## Project Structure

```
gfv2-params/
├── src/gfv2_params/          # Installable Python package
│   ├── config.py             # Config loading, fabric profile resolution
│   ├── raster_ops.py         # Raster utilities
│   ├── batching.py           # Spatial batching
│   ├── lulc.py               # LULC reclassification helpers
│   ├── depstor.py            # Depression-storage helpers
│   ├── depstor_builders/     # Per-step depstor raster builders
│   ├── depstor_ratios.py     # PRMS Level-5 ratio arithmetic
│   ├── shared_rasters/       # Part 1 CONUS raster builders
│   ├── zonal_runners/        # Part 2 zonal-pass runners
│   ├── log.py                # Logging setup
│   └── download/             # Data download utilities
├── scripts/                  # CLI orchestrators + standalone helpers
├── configs/                  # Per-stage YAML configs (base + shared_rasters/ + depstor/ + zonal/)
├── slurm_batch/              # HPC SLURM batch scripts (RUNME.md = runbook; HPC_REFERENCE.md = detail)
├── docs/                     # ARCHITECTURE.md, depstor docs, superpowers/ design tree
├── notebooks/                # Interactive notebooks (fabric_results/, oregon/, _archive/)
├── tests/                    # Unit tests
├── pyproject.toml            # Package + pixi config
├── pixi.lock                 # Pinned pixi environment
└── environment.yml           # Legacy conda environment (deprecated fallback)
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the orchestrator +
builder + unified-config pattern that shapes `src/gfv2_params/`,
`scripts/`, and `configs/`.

## Output Directory Structure

All source data and outputs live under `data_root` (set in
`configs/base_config.yml`) in the `input/` → `shared/` → `{fabric}/`
layout. See [`docs/ARCHITECTURE.md#data-root-layout-the-key-invariant`](docs/ARCHITECTURE.md#data-root-layout-the-key-invariant)
for the canonical tree.

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
| `input/nhd/` | `conus_waterbodies.gpkg` (layer `waterbodies`) — shared CONUS NHDPlusV2 depression-storage polygons, used by every depstor fabric. Stream segments are not staged here: a VPU-based fabric (gfv2) merges them from the per-VPU `nsegment` layers via `scripts/merge_vpu_segments.py`; the D8 flow-direction raster comes from the shared `shared/conus/vrt/fdr.vrt`. |
| `input/twi/<rpu>/` | Per-RPU `twi.tif` (+ `.tfw`, `.aux.xml`, `.ovr`, `.xml` sidecars). Stage with `bash scripts/stage_twi.sh` (or `sbatch slurm_batch/stage_twi.batch` for an unattended run). |

> **Upgrading an existing `data_root` from the legacy `work/` layout?** Run
> `pixi run python scripts/migrate_to_shared_layout.py --data-root <path> --dry-run`
> to preview the 27 directory renames, then `--execute` to apply them.
> Atomic `os.rename` on the same filesystem (metadata-only, near-instant);
> regenerates CONUS VRTs at the end since they encode absolute source paths.
> Idempotent — re-running after success is a no-op.

### 3. Run fabric-independent tasks

These stages do not require a watershed fabric and can run while fabric preparation proceeds in parallel. This includes downloading and merging NHD rasters, building VRTs, and computing derived rasters. See `slurm_batch/HPC_REFERENCE.md` **Part 1 stage detail** for the full sequence.

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

This walks 9 production steps in dependency order (merge_rpu_by_vpu →
compute_slope_aspect → build_border_dem → build_vpu_landmask →
merge_rpu_by_vpu_twi → build_vrt → twi_reference → build_derived_rasters →
build_lulc_rasters) and writes everything into the shared `shared/` store
consumed by every fabric. Add `--step <name>` to run one step or `--from <name>` to resume.
`--vpus 01,02` scopes per-VPU steps to a subset; `--force` rebuilds
existing outputs. See [Shared rasters pipeline](#shared-rasters-pipeline)
below for the design notes.

The orchestrator dispatches into per-step library builders under
[src/gfv2_params/shared_rasters/](src/gfv2_params/shared_rasters/). The
older per-script CLI wrappers were retired in favour of `--step <name>`
on the orchestrator.

### 4. Run fabric-dependent tasks

Once raster prep is complete and the merged fabric is available, prepare
the fabric batches and run parameter generation. There are two equivalent
ways to run Part 2 (they produce identical outputs):

- **Run by parameter** — submit each param/fraction as a small array + merge
  unit, in sequence, so you can follow the workflow and inspect one parameter
  at a time. See `slurm_batch/HPC_REFERENCE.md` **Stage 4A** for the per-parameter
  commands and order.
- **Run wholesale** — one command per stage; each wrapper just loops the
  by-parameter steps and chains them via `afterok`:

  ```bash
  BATCHES={data_root}/{fabric}/batches
  slurm_batch/submit_zonal_params.sh   $BATCHES {fabric} configs/base_config.yml
  slurm_batch/submit_depstor_params.sh $BATCHES {fabric} configs/base_config.yml
  ```

See [Zonal-pass parameter pipeline](#zonal-pass-parameter-pipeline) below
for the design notes, and `slurm_batch/HPC_REFERENCE.md` **Stage 4A/4B** for the
full sequence (both paths) including the depstor pipeline and gap-fill.

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

1. Register the fabric and scaffold its output dirs:
   `pixi run init-data-root --add-fabric oregon` appends a profile stub under
   `fabrics:` in `configs/base_config.yml`. Then fill the stub's TODO
   placeholders. **All shared, required fabric inputs live in the profile**
   — see [`docs/ARCHITECTURE.md#required-profile-keys`](docs/ARCHITECTURE.md#required-profile-keys)
   for the per-key required-field table (which keys are always required, which
   are depstor-only, and how to stage the fabric-bounds clip for
   `template_raster`/`fdr_raster` via
   `scripts/clip_shared_to_fabric.py`).

   For non-VPU-01 fabrics with depstor, use `threshold_mode: percentile` in
   `configs/depstor/depstor_rasters.yml` with `twi_raster` pointing at
   `twi_hydrodem.vrt`, and run the `twi_reference` step first (Stage 2a' in
   `slurm_batch/HPC_REFERENCE.md`).
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

See `slurm_batch/RUNME.md` for the runbook; `slurm_batch/HPC_REFERENCE.md` for per-stage detail.

## Shared rasters pipeline

Part 1 (fabric-independent CONUS raster prep) is driven by
[`scripts/build_shared_rasters.py`](scripts/build_shared_rasters.py) over
[`configs/shared_rasters/shared_rasters.yml`](configs/shared_rasters/shared_rasters.yml),
walking the step DAG via builder modules in
[`src/gfv2_params/shared_rasters/`](src/gfv2_params/shared_rasters/).
Outputs land under `{data_root}/shared/` and are reused by every fabric.

The DAG covers per-VPU NHDPlus prep, border-DEM fill, per-VPU HRU landmask,
masked TWI merge, CONUS VRT assembly, the TWI percentile reference, and
CONUS derived rasters. `compute_dem_derivatives` is an opt-in parallel
open-source TWI pipeline.

See [`docs/ARCHITECTURE.md#orchestrator-builder-unified-config-pattern`](docs/ARCHITECTURE.md#orchestrator-builder-unified-config-pattern)
for the pattern, and the package's
[`__init__.py`](src/gfv2_params/shared_rasters/__init__.py) for per-step
detail.

## Depression-storage pipeline

Part 2a (per-fabric depstor) is driven by two orchestrators over two
unified configs:

- [`scripts/build_depstor_rasters.py`](scripts/build_depstor_rasters.py)
  + [`configs/depstor/depstor_rasters.yml`](configs/depstor/depstor_rasters.yml)
  → 10-step raster DAG via
  [`src/gfv2_params/depstor_builders/`](src/gfv2_params/depstor_builders/).
- [`scripts/derive_depstor_params.py`](scripts/derive_depstor_params.py)
  + [`configs/depstor/depstor_params.yml`](configs/depstor/depstor_params.yml)
  → 10 fractions + 6 PRMS Level-5 ratios. The slurm wrapper
  [`slurm_batch/submit_depstor_params.sh`](slurm_batch/submit_depstor_params.sh)
  chains 10 zonal arrays → 10 merges → 1 ratios job via `afterok`.

See [`docs/depstor_workflow.md`](docs/depstor_workflow.md) and
[`docs/depstor_port_summary.md`](docs/depstor_port_summary.md) for the
historical port reference. Stage 2d in `slurm_batch/HPC_REFERENCE.md` lists the
build order.

## Zonal-pass parameter pipeline

Part 2b (per-fabric zonal-pass params) is driven by
[`scripts/derive_zonal_params.py`](scripts/derive_zonal_params.py) over
[`configs/zonal/zonal_params.yml`](configs/zonal/zonal_params.yml),
dispatching every Part-2 param type (`elevation`, `slope`, `aspect`,
`soils`, `soil_moist_max`, `lulc_{nhm_v11,nalcms,nlcd,foresce}`, `ssflux`)
into the matching `run_*_batch` function in
[`src/gfv2_params/zonal_runners/`](src/gfv2_params/zonal_runners/) via the
package's `BATCH_RUNNERS` dispatch table.

Three modes: `--mode zonal --param <name> --batch_id <N>`,
`--mode merge --param <name>`, `--mode build_weights` (CONUS-once ssflux
prereq).

The slurm wrapper
[`slurm_batch/submit_zonal_params.sh`](slurm_batch/submit_zonal_params.sh)
loops every entry in `params:` and chains per-param array + merge jobs via
`afterok`. When an entry carries `depends_on: build_weights` (typically
`ssflux`), the wrapper first submits `build_zonal_weights.batch` and
chains ssflux on its `afterok`.

```bash
bash slurm_batch/submit_zonal_params.sh \
    {data_root}/gfv2_vpu01/batches gfv2_vpu01 configs/base_config.yml
```

See [`docs/ARCHITECTURE.md#orchestrator-builder-unified-config-pattern`](docs/ARCHITECTURE.md#orchestrator-builder-unified-config-pattern)
for the pattern.

## Snow depletion curves (SNODAS → snarea_curve)

Derives the PRMS `snarea_curve` parameter (an 11-point areal snow-depletion
curve) plus `hru_deplcrv` from daily SNODAS SWE, following the Driscoll, Hay &
Bock (2017) method. Two fabric-independent stages. Stage 1 runs as a SLURM
array over the fabric's spatial batches (same pattern as the depstor/zonal
parameter jobs), then a merge job; Stage 2 remains a plain `pixi run`:

```bash
# Stage 1 — aggregate daily SNODAS SWE to the HRU fabric, per spatial batch
N=$(grep '^n_batches:' "{data_root}/oregon/batches/manifest.yml" | awk '{print $2}')
AID=$(sbatch --parsable --array=0-$((N-1)) --export=ALL,FABRIC=oregon \
    slurm_batch/derive_snodas_aggregate.batch)
sbatch --dependency=afterok:$AID --export=ALL,FABRIC=oregon \
    slurm_batch/merge_snodas_aggregate.batch

# Stage 2 — derive per-HRU snarea_curve from the aggregated SWE/SCA
pixi run python scripts/derive_snarea_curve.py --fabric oregon
```

- **Inputs:** raw daily SNODAS SWE NetCDFs from the shared datastore (default
  `{data_root}/../nhf-datastore/snodas/daily` in
  [`configs/aggregate/aggregate_sources.yml`](configs/aggregate/aggregate_sources.yml);
  overridable per-fabric via the profile's `snodas_dir` key), plus the fabric's
  spatial batch geopackages (`{data_root}/{fabric}/batches/batch_<N>.gpkg`,
  produced by `prepare_fabric`/Step 2).
- **Stage 1 output:** one array task per batch writes
  `{data_root}/{fabric}/snodas/_batches/snodas_batch<NNNN>_agg_<year>.nc`
  (source grid clipped to that batch's extent before aggregating — see
  `aggregate_source`/`subset_to_gdf_bounds` in
  `src/gfv2_params/aggregate/driver.py`), plus a per-batch cached gdptools
  weight CSV under `{data_root}/{fabric}/weights_agg/`. The merge job then
  concatenates all batches per year into
  `{data_root}/{fabric}/snodas/snodas_agg_<year>.nc` (dims `time`/`<id_feature>`;
  vars `swe` area-weighted mean, `scov` snow-covered-area fraction) — the file
  Stage 2 reads.
- **Stage 2 output:**
  `{data_root}/{fabric}/params/merged/nhm_snarea_curve_params.csv` — one row per
  HRU with `snarea_curve_0..10`, `hru_deplcrv`, and `sdc_status`/`sca_class`
  diagnostics; HRUs failing the six Driscoll selection criteria fall back to a
  configured default curve, flagged in `sdc_status`.

`--fabric` is resolved the same way as every other stage (CLI flag → `FABRIC`
env var → `default_fabric`); swap in `--fabric gfv2` for the CONUS fabric.
Design spec: [`docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md`](docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md);
converted method paper: [`docs/Snow_Depletion_Curves.md`](docs/Snow_Depletion_Curves.md).

## Viewing fabric results

Once a fabric is processed, the Jupyter notebooks in
[notebooks/fabric_results/](notebooks/fabric_results/) give a complete picture of
a fabric's parameterization — the inputs that fed it and the per-HRU results:

| Notebook | Shows |
|---|---|
| `01_input_rasters.ipynb` | Every shared/zonal source raster clipped to the fabric bounds, HRU outline overlaid. |
| `02_depstor_rasters.ipynb` | The 14 per-fabric depression-storage binary/label rasters + coverage stats. |
| `03_param_results.ipynb` | Choropleth + distribution of all 25 merged per-HRU params (DEM stats, soils, LULC incl. rain/snow interception, depstor ratios, ssflux), plus a depstor-ratio summary. |
| `04_depstor_overlay.ipynb` | Interactive folium map: the 7 depstor binaries that directly feed a PRMS ratio (`carea_max`, `smidx_coef`, `sro_to_dprst_perv/imperv`, `hru_percent_imperv`, `dprst_frac`), each as a toggleable color overlay on OpenStreetMap / CartoDB / Esri imagery basemaps. |

All are **parameterized by the `FABRIC` env var** (default `oregon`) and read
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
