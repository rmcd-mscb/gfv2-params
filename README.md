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
`~/.pixi/bin` is on your `PATH`.

The reference PDFs under [`docs/`](docs/) are stored with
[git-lfs](https://git-lfs.com). Install it once per user so plain `git` can
resolve them — without it you get small pointer files instead of real PDFs:

```bash
pixi global install git-lfs   # lands in ~/.pixi/bin, already on your PATH
git lfs install               # once per clone
git lfs pull                  # fetch the PDFs for an existing clone
```

(`git-lfs` is also in the `dev` feature, so `pixi shell -e dev` works too — but
the global install is preferable because git's LFS filters run outside the pixi
environment.)

For the full HPC workflow (downloads → shared
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
| `input/nhd/` | `nhd_waterbodies.gpkg` (layer `waterbodies`) — shared CONUS NHDPlusV2 depression-storage polygons, used by every depstor fabric (`gfv2`, `gfv2_dev`, `oregon`, `tjc`). Source-derived and reproducible, staged by `python -m gfv2_params.download.nhd_waterbodies` and converted from its verified `nhd_waterbodies.parquet` (see `slurm_batch/HPC_REFERENCE.md`); verified against the retired hand-made `conus_waterbodies.gpkg` via `scripts/diagnose/verify_nhd_waterbodies.py`, which stays on disk only for that A/B reference. Stream segments are not staged here: a VPU-based fabric (gfv2) merges them from the per-VPU `nsegment` layers via `scripts/merge_vpu_segments.py` — this merged layer is also the on-stream classifier's primary input (`segment_wbody`: a waterbody is on-stream iff a segment intersects it with positive length); the D8 flow-direction raster comes from the shared `shared/conus/vrt/fdr.vrt`. |
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

**Download EPA Level III Ecoregions** (~28 MB) — a shared, reusable input used by
the `dprst_depth` builder (per-ecoregion depth fill) and available to other
parameterizations. Source:
[us_eco_l3.zip](https://dmap-prod-oms-edc.s3.us-east-1.amazonaws.com/ORD/Ecoregions/us/us_eco_l3.zip)
(US EPA; on AWS S3, reachable from the HPC). Level III (77 regions) is the default — larger, more robust
per-region donor pools. A finer
[Level IV alternative](https://dmap-prod-oms-edc.s3.us-east-1.amazonaws.com/ORD/Ecoregions/us/us_eco_l4.zip)
is available for sensitivity testing (finer regions → sparser donors). Staged to `{data_root}/input/ecoregions/`:

```bash
pixi run python -m gfv2_params.download.epa_ecoregions --dest "$DATA_ROOT/input/ecoregions"
```

**Stage the real 3DEP 1 m tile inventory** (~30–35 min; shared, fabric-independent) —
the `dprst_depth` builder (issue #223 part 2) picks each dprst polygon's
elevation source from every published USGS 3DEP 1 m tile's own real
(often-cropped) extent, not a convex-hull WESM footprint (a hull invents
coverage a project never flew). Lists the S3 project directories, reads each
tile's own COG header for its true bounds, and reads WESM geometry-free for
quality/date attributes only. Writes
`{data_root}/input/3dep/dem_1m_tile_inventory.parquet` and
`{data_root}/input/wesm/wesm_project_attrs.parquet`:

```bash
sbatch slurm_batch/stage_dem_1m_inventory.batch
```

The inventory is a **snapshot** of what USGS publishes on the day it's staged —
re-staging (`FORCE=1 sbatch slurm_batch/stage_dem_1m_inventory.batch`) obliges
a `dprst_depth` re-run for every fabric, the same obligation a `shared_rasters`
rebuild carries.

All download scripts are idempotent — already-downloaded files are skipped on resubmission.

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
  slurm_batch/submit_dprst_depth.sh    $BATCHES {fabric} configs/base_config.yml
  ```

  `submit_dprst_depth.sh` (issue #173) must be run **before** an unfiltered
  `build_depstor_rasters.batch`, since `dprst_depth`'s in-process fallback is
  a ~250-500 core-hour CONUS compute — see `slurm_batch/RUNME.md` Step 3 and
  `slurm_batch/HPC_REFERENCE.md` "Stage 2d'". Since #221 the builder enforces
  this: above 25,000 polygons it refuses the in-process path and names
  `submit_dprst_depth.sh`, and `submit_fabric_rerun.sh` runs the tiled stage
  between two halves of the depstor stack for you.

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

**Pre-merged fabric** (single gpkg with `nhru` + `nsegment` layers — e.g.
`tjc`, `flaming_gorge`): follow
[`docs/adding-a-fabric.md`](docs/adding-a-fabric.md). It is the complete,
plain-language procedure — inspect the gpkg with `ogrinfo`, register the
profile with `pixi run --as-is init-data-root --add-fabric <name>` (the stub is
a complete active profile with four TODO values), clip the FDR, validate with
`scripts/check_fabric_profile.py`, prepare batches, then run the whole chain
with `slurm_batch/submit_fabric_rerun.sh`. Every profile key is explained in
[`docs/ARCHITECTURE.md#required-profile-keys`](docs/ARCHITECTURE.md#required-profile-keys).

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
  → 14-step raster DAG via
  [`src/gfv2_params/depstor_builders/`](src/gfv2_params/depstor_builders/).
  One step, `dprst_depth` (issue #173), is a CONUS-scale compute outlier —
  its cost scales with the ~286k dprst **polygons**, not the grid, so it has
  its own SLURM array, run separately from (and before) this orchestrator's
  unfiltered walk reaches it — see below.
- [`scripts/derive_depstor_params.py`](scripts/derive_depstor_params.py)
  + [`configs/depstor/depstor_params.yml`](configs/depstor/depstor_params.yml)
  → 10 fractions + 6 PRMS Level-5 ratios, plus the `dprst_depth_avg` mean.
  The slurm wrapper
  [`slurm_batch/submit_depstor_params.sh`](slurm_batch/submit_depstor_params.sh)
  chains 10 zonal arrays → 10 merges → 1 ratios job via `afterok`;
  [`slurm_batch/submit_dprst_depth.sh`](slurm_batch/submit_dprst_depth.sh)
  drives `dprst_depth` end-to-end on its own (tile-batch array → raster
  fill+burn → per-HRU mean → `nhm_dprst_depth_avg_params.csv`) — see
  `slurm_batch/HPC_REFERENCE.md` "Stage 2d'" for the full DAG and sizing.

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
does **not** read the YAML — it carries a hardcoded `PARAMS` bash array mirroring
the `params:` list, and chains per-param array + merge jobs via `afterok`. Adding a
param to the YAML without adding it to that array silently skips it;
`tests/test_submit_wrapper_param_lists.py` guards the pair. When an entry carries `depends_on: build_weights` (typically
`ssflux`), the wrapper first submits `build_zonal_weights.batch` and
chains ssflux on its `afterok`.

```bash
bash slurm_batch/submit_zonal_params.sh \
    {data_root}/gfv2_vpu01/batches gfv2_vpu01 configs/base_config.yml
```

See [`docs/ARCHITECTURE.md#orchestrator-builder-unified-config-pattern`](docs/ARCHITECTURE.md#orchestrator-builder-unified-config-pattern)
for the pattern.

## Snow depletion curves (SNODAS → snarea_curve)

Derives a PRMS `snarea_curve` library (CV/lognormal depletion curves) plus
per-HRU `hru_deplcrv`/`snarea_thresh` from daily SNODAS SWE. Three
fabric-independent stages: Stage 1/2 follow the Driscoll, Hay & Bock (2017)
empirical method; Stage 3 fits each HRU to a physically-based lognormal-CV
curve (Sexstone, Driscoll, Hay, Hammond & Barnhart 2020), calibrated against
the Stage 2 empirical curves. Stage 1 runs as a SLURM array over the fabric's
spatial batches (same pattern as the depstor/zonal parameter jobs), then a
merge job; Stage 2 remains a plain `pixi run`; Stage 3 is a cheap `sbatch` job:

```bash
# Stage 1 — aggregate daily SNODAS SWE (+ swe_std) to the HRU fabric, per spatial batch
N=$(grep '^n_batches:' "{data_root}/oregon/batches/manifest.yml" | awk '{print $2}')
AID=$(sbatch --parsable --array=0-$((N-1)) --export=ALL,FABRIC=oregon \
    slurm_batch/derive_snodas_aggregate.batch)
sbatch --dependency=afterok:$AID --export=ALL,FABRIC=oregon \
    slurm_batch/merge_snodas_aggregate.batch

# Stage 2 — derive per-HRU empirical snarea_curve + sub-grid CV from the aggregated SWE/SCA
pixi run python scripts/derive_snarea_curve.py --fabric oregon

# Stage 3 — build the CV/lognormal curve library + per-HRU params from the Stage 2 output
FABRIC=oregon sbatch slurm_batch/derive_snarea_library.batch
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
  vars `swe` area-weighted mean, `scov` snow-covered-area fraction, `swe_std`
  per-cell SWE std dev sidecar) — the file Stage 2 reads.
- **Stage 2 output:** the intermediate
  `{data_root}/{fabric}/params/merged/_intermediates/nhm_snarea_curve_derived.csv`
  — one row per HRU with the empirical `snarea_curve_0..10`, `sdc_status`/
  `sca_class` diagnostics, and `cv_subgrid` (from `swe_std`); HRUs failing the
  six Driscoll selection criteria fall back to a configured default curve,
  flagged in `sdc_status`. Not the terminal params — that's Stage 3.
- **Stage 3 output**, all under `{data_root}/{fabric}/params/merged/`:
  `nhm_snarea_curve_library.csv` (one row per `deplcrv_id` — the reserved
  default curve plus `ndepl_cv` equal-population CV-bin curves),
  `nhm_snarea_curve_params.csv` (one row per HRU: `hru_deplcrv`,
  `snarea_thresh`, CV diagnostics, the assigned curve),
  `nhm_snarea_curve_validation.csv` (calibration/reconstruction-error report),
  and `nhm_snarea_curve.nc` — a pyWatershed-ready parameter file
  (`snarea_curve`, `hru_deplcrv`, `snarea_thresh`).

`--fabric` is resolved the same way as every other stage (CLI flag → `FABRIC`
env var → `default_fabric`); swap in `--fabric gfv2` for the CONUS fabric.
Design specs:
[`docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md`](docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md)
(Stages 1/2) and
[`docs/superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md`](docs/superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md)
(Stage 3 library); converted method paper:
[`docs/Snow_Depletion_Curves.md`](docs/Snow_Depletion_Curves.md).

## PRMS parameters covered

`params/merged/` holds one file per pipeline step (`nhm_soils_params.csv`,
`nhm_ssflux_params.csv`, ...), and several of those mix PRMS parameters with raw zonal
statistics and provenance columns. The **export** step splits them into one file per PRMS
parameter, named exactly as in the PRMS/pywatershed data dictionary
([`docs/HI_paramDB_DataDictionary.csv`](docs/HI_paramDB_DataDictionary.csv)):

```bash
FABRIC=<fabric> sbatch slurm_batch/export_prms_params.batch    # run AFTER the fill step
```

This writes `{data_root}/{fabric}/params/prms/<parameter>.csv` — for example `hru_elev.csv`,
`soil_type.csv`, `rad_trncf.csv`. Each per-HRU file has two columns: the fabric's HRU id
column and the parameter. Values are copied from the **gap-filled** `merged/` files exactly
as stored; there is no unit conversion.

Things to know before using the files:

- **`snarea_curve.csv` is not per-HRU.** It is the snow-depletion curve array
  (`ndeplval` = 11 values per curve, ascending, curves in `hru_deplcrv` order) with columns
  `ndeplval_index, snarea_curve`. `hru_deplcrv.csv` is the per-HRU index into it. Do not
  join it on the HRU id.
- **"Covered" means declared and exported, not built on every fabric.** The list below comes
  from the parameter declarations in `configs/`. Which files exist for a given fabric is
  whatever is in its `params/prms/` directory.
- **Units are the dictionary's.** `hru_elev` is the zonal *mean* elevation in the units of
  the source DEM (the dictionary describes it as the median); `hru_slope` is a fraction, not
  degrees. The pipeline's `hru_area` is in m², PRMS wants acres, so it is not exported.
- **One source per parameter.** Land cover comes from the `nhm_v11` source only. The
  `nalcms`, `nlcd` and `foresce` alternatives stay in `merged/` and are not exported.
- Any parameter that is not covered here has to come from elsewhere (NHM defaults, the
  fabric itself, or a separate derivation); the list below says which kind.

For which pipeline step produces each parameter and which PRMS process uses it, see
[`docs/parameter_index.md`](docs/parameter_index.md).

<!-- BEGIN GENERATED: prms-summary -->
**30 of the 116 parameters in the data dictionary are covered**; 86 are not.
<!-- END GENERATED: prms-summary -->

### Covered

<!-- BEGIN GENERATED: prms-covered -->
| Parameter | Type | Units | Dimension | Description | Written from |
|---|---|---|---|---|---|
| `carea_max` | F | decimal fraction | nhru | Maximum possible area contributing to surface runoff expressed as a portion of the hydrologic response unit area | `nhm_carea_max_params.csv` (`carea_max`) |
| `cov_type` | I | none | nhru | Vegetation cover type for each hydrologic response unit (0=bare soil; 1=grasses; 2=shrubs; 3=trees; 4=coniferous) | `nhm_lulc_nhm_v11_params.csv` (`cov_type`) |
| `covden_sum` | F | decimal fraction | nhru | Summer vegetation cover density for the major vegetation type in each hydrologic response unit | `nhm_lulc_nhm_v11_params.csv` (`covden_sum`) |
| `covden_win` | F | decimal fraction | nhru | Winter vegetation cover density for the major vegetation type in each hydrologic response unit | `nhm_lulc_nhm_v11_params.csv` (`covden_win`) |
| `dprst_depth_avg` | F | inches | nhru | Average depth of surface depressions at maximum storage capacity | `nhm_dprst_depth_avg_params.csv` (`dprst_depth_avg`) |
| `dprst_flow_coef` | F | fraction/day | nhru | Coefficient in linear flow routing equation for open surface depressions for each hydrologic response unit | `nhm_ssflux_params.csv` (`dprst_flow_coef`) |
| `dprst_frac` | F | decimal fraction | nhru | Fraction of each hydrologic response unit area that has surface depressions | `nhm_dprst_frac_params.csv` (`dprst_frac`) |
| `dprst_seep_rate_open` | F | fraction/day | nhru | Coefficient used in linear seepage flow equation for open surface depressions for each hydrologic response unit | `nhm_ssflux_params.csv` (`dprst_seep_rate_open`) |
| `fastcoef_lin` | F | fraction/day | nhru | Linear coefficient in equation to route preferential-flow storage downslope for each hydrologic response unit | `nhm_ssflux_params.csv` (`fastcoef_lin`) |
| `gwflow_coef` | F | fraction/day | nhru | Linear coefficient in the equation to compute groundwater discharge for each groundwater reservoir | `nhm_ssflux_params.csv` (`gwflow_coef`) |
| `hru_aspect` | F | angular degrees | nhru | Aspect of each hydrologic response unit | `nhm_aspect_params.csv` (`hru_aspect`) |
| `hru_deplcrv` | I | none | nhru | Index number for the snowpack areal depletion curve associated with each hydrologic response unit | `nhm_snarea_curve_params.csv` (`hru_deplcrv`) |
| `hru_elev` | F | elev_units | nhru | Median elevation for each hydrologic response unit | `nhm_elevation_params.csv` (`mean`) |
| `hru_percent_imperv` | F | decimal fraction | nhru | Fraction of each hydrologic response unit area that is impervious | `nhm_hru_percent_imperv_params.csv` (`hru_percent_imperv`) |
| `hru_slope` | F | decimal fraction | nhru | Slope of each hydrologic response unit, specified as change in vertical length divided by change in horizontal length | `nhm_slope_params.csv` (`hru_slope`) |
| `op_flow_thres` | F | decimal fraction | nhru | Fraction of open depression storage above which surface runoff occurs; any water above maximum open storage capacity spills as surface runoff | `nhm_op_flow_thres_params.csv` (`op_flow_thres`) |
| `rad_trncf` | F | decimal fraction | nhru | Transmission coefficient for short-wave radiation through the winter vegetation canopy | `nhm_lulc_nhm_v11_params.csv` (`retention` / `rad_trncf`) |
| `slowcoef_lin` | F | fraction/day | nhru | Linear coefficient in equation to route gravity-reservoir storage downslope for each hydrologic response unit | `nhm_ssflux_params.csv` (`slowcoef_lin`) |
| `smidx_coef` | F | decimal fraction | nhru | Coefficient in non-linear contributing area algorithm for each hydrologic response unit | `nhm_smidx_coef_params.csv` (`smidx_coef`) |
| `snarea_curve` | F | decimal fraction | ndeplval | Snow area depletion curve values, 11 values for each curve (0.0 to 1.0 in 0.1 increments) | snarea curve library (flat; not per-HRU) |
| `snarea_thresh` | F | inches | nhru | Maximum threshold snowpack water equivalent below which the snow-covered-area curve is applied | `nhm_snarea_curve_params.csv` (`snarea_thresh`) |
| `snow_intcp` | F | inches | nhru | Snow interception storage capacity for the major vegetation type in each hydrologic response unit | `nhm_lulc_nhm_v11_params.csv` (`snow_intcp`) |
| `soil2gw_max` | F | inches | nhru | Maximum amount of the capillary reservoir excess that is routed directly to the GWR for each HRU | `nhm_ssflux_params.csv` (`soil2gw_max`) |
| `soil_moist_max` | F | inches | nhru | Maximum value of water for soil zone | `nhm_soil_moist_max_params.csv` (`soil_moist_max`) |
| `soil_type` | I | none | nhru | Soil type of each hydrologic response unit (1=sand; 2=loam; 3=clay) | `nhm_soils_params.csv` (`soils`) |
| `srain_intcp` | F | inches | nhru | Summer rain interception storage capacity for the major vegetation type in each hydrologic response unit | `nhm_lulc_nhm_v11_params.csv` (`srain_intcp`) |
| `sro_to_dprst_imperv` | F | decimal fraction | nhru | Fraction of impervious surface runoff that flows into surface-depression storage; the remainder flows to a stream network for each hydrologic response unit | `nhm_sro_to_dprst_imperv_params.csv` (`sro_to_dprst_imperv`) |
| `sro_to_dprst_perv` | F | decimal fraction | nhru | Fraction of pervious surface runoff that flows into surface-depression storage; the remainder flows to a stream network for each hydrologic response unit | `nhm_sro_to_dprst_perv_params.csv` (`sro_to_dprst_perv`) |
| `ssr2gw_rate` | F | fraction/day | nhru | Linear coefficient in equation used to route water from the gravity reservoir to the groundwater reservoir for each hydrologic response unit | `nhm_ssflux_params.csv` (`ssr2gw_rate`) |
| `wrain_intcp` | F | inches | nhru | Winter rain interception storage capacity for the major vegetation type in each hydrologic response unit | `nhm_lulc_nhm_v11_params.csv` (`wrain_intcp`) |
<!-- END GENERATED: prms-covered -->

### Not covered

<!-- BEGIN GENERATED: prms-not-covered -->
**Per-HRU constants (NHM defaults)** (32)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `dprst_et_coef` | decimal fraction | nhru | Fraction of unsatisfied potential evapotranspiration to apply to surface-depression storage |
| `dprst_frac_init` | decimal fraction | nhru | Fraction of maximum surface-depression storage that contains water at the start of a simulation |
| `dprst_frac_open` | decimal fraction | nhru | Fraction of open surface-depression storage area within an hydrologic response unit that can generate surface runoff as a function of storage volume |
| `emis_noppt` | decimal fraction | nhru | Average emissivity of air on days without precipitation for each hydrologic response unit |
| `fastcoef_sq` | none | nhru | Non-linear coefficient in equation used to route preferential-flow storage downslope for each hydrologic response unit |
| `freeh2o_cap` | decimal fraction | nhru | Free-water holding capacity of snowpack for each hydrologic response unit, expressed as a decimal fraction of the frozen water content of the snowpack (pk_ice) |
| `gwsink_coef` | fraction/day | nhru | Linear coefficient in the equation to compute outflow to the groundwater sink for each groundwater reservoir |
| `gwstor_init` | inches | nhru | Storage in each groundwater reservoir at the beginning of a simulation |
| `gwstor_min` | inches | nhru | Minimum storage in each groundwater reservoir to ensure storage is greater than specified value to account for inflow from deep aquifers or injection wells with the water source outside the basin |
| `hru_type` | none | nhru | Type of each hydrologic response unit (0=inactive; 1=land; 2=lake; 3=swale) |
| `imperv_stor_max` | inches | nhru | Maximum impervious area retention storage for each hydrologic response unit |
| `melt_force` | Julian day | nhru | Julian date to force snowpack to spring snowmelt stage for each hydrologic response unit; varies with region depending on length of time that permanent snowpack exists |
| `melt_look` | Julian day | nhru | Julian date to start looking for spring snowmelt stage for each hydrologic response unit; varies with region depending on length of time that permanent snowpack exists |
| `potet_sublim` | decimal fraction | nhru | Fraction of potential evapotranspiration that is sublimated from snow in the canopy and snowpack for each hydrologic response unit |
| `pref_flow_den` | decimal fraction | nhru | Fraction of the soil zone in which preferential flow occurs for each hydrologic response unit |
| `radj_sppt` | decimal fraction | nhru | Adjustment factor for computed solar radiation for summer day with greater than ppt_rad_adj inches of precipitation for each hydrologic response unit |
| `radj_wppt` | decimal fraction | nhru | Adjustment factor for computed solar radiation for winter day with greater than ppt_rad_adj inches of precipitation for each hydrologic response unit |
| `sat_threshold` | inches | nhru | Water holding capacity of the gravity and preferential-flow reservoirs; difference between field capacity and total soil saturation for each hydrologic response unit |
| `slowcoef_sq` | none | nhru | Non-linear coefficient in equation to route gravity-reservoir storage downslope for each hydrologic response unit |
| `smidx_exp` | 1.0/inch | nhru | Exponent in non-linear contributing area algorithm for each hydrologic response unit |
| `snowinfil_max` | inches/day | nhru | Maximum snow infiltration per day for each hydrologic response unit |
| `snowpack_init` | inches | nhru | Storage of snowpack in each hydrologic response unit at the beginning of a simulation |
| `soil_moist_init_frac` | decimal fraction | nhru | Initial fraction of available water in the capillary reservoir (fraction of soil_moist_max for each hydrologic response unit) |
| `soil_rechr_init_frac` | decimal fraction | nhru | Initial fraction of available water in the soil recharge zone within the capillary reservoir |
| `soil_rechr_max_frac` | decimal fraction | nhru | Fraction of capillary reservoir where losses occur as both evaporation and transpiration (soil recharge zone) |
| `ssr2gw_exp` | none | nhru | Non-linear coefficient in equation used to route water from the gravity reservoir to the groundwater reservoir for each hydrologic response unit |
| `ssstor_init_frac` | decimal fraction | nhru | Initial fraction of available water in the gravity plus preferential-flow reservoirs (fraction of sat_threshold) for each hydrologic response unit |
| `transp_beg` | month | nhru | Month to begin summing the maximum air temperature for each hydrologic response unit; when sum is greater than or equal to transp_tmax, transpiration begins |
| `transp_end` | month | nhru | Month to stop transpiration computations; transpiration is computed thru end of previous month |
| `transp_tmax` | temp_units | nhru | Temperature index to determine the specific date of the start of the transpiration period; the maximum air temperature for each hydrologic response unit is summed starting with the first day of month transp_beg; when the sum exceeds this index, transpiration begins |
| `va_clos_exp` | none | nhru | Coefficient in the exponential equation relating maximum surface area to the fraction that closed depressions are full to compute current surface area for each hydrologic response unit; 0.001 is an approximate rectangle; 1.0 is a triangle |
| `va_open_exp` | none | nhru | Coefficient in the exponential equation relating maximum surface area to the fraction that open depressions are full to compute current surface area for each hydrologic response unit; 0.001 is an approximate rectangle; 1.0 is a triangle |

**Monthly parameters (nhru × nmonths)** (18)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `adjmix_rain` | decimal fraction | nhru, nmonths | Monthly (January to December) factor to adjust rain proportion in a mixed rain/snow event |
| `cecn_coef` | calories per degree Celsius above 0 | nhru, nmonths | Monthly (January to December) convection condensation energy coefficient for each hydrologic response unit |
| `dday_intcp` | dday | nhru, nmonths | Monthly (January to December) intercept in degree-day equation for each hydrologic response unit |
| `dday_slope` | dday/temp_units | nhru, nmonths | Monthly (January to December) slope in degree-day equation for each hydrologic response unit |
| `epan_coef` | decimal fraction | nhru, nmonths | Monthly (January to December) evaporation pan coefficient for each hydrologic response unit |
| `jh_coef` | per degrees Fahrenheit | nhru, nmonths | Monthly (January to December) air temperature coefficient used in Jensen-Haise potential evapotranspiration computations for each hydrologic response unit |
| `ppt_rad_adj` | inches | nhru, nmonths | Monthly minimum precipitation, if hydrologic response unit precipitation exceeds this value, radiation is multiplied by radj_sppt or radj_wppt adjustment factor |
| `radadj_intcp` | dday | nhru, nmonths | Monthly (January to December) intercept in air temperature range adjustment to degree-day equation for each hydrologic response unit |
| `radadj_slope` | dday/temp_units | nhru, nmonths | Monthly (January to December) slope in air temperature range adjustment to degree-day equation for each hydrologic response unit |
| `radmax` | decimal fraction | nhru, nmonths | Monthly (January to December) maximum fraction of the potential solar radiation that may reach the ground due to haze, dust, smog, and so forth, for each hydrologic response unit |
| `rain_cbh_adj` | decimal fraction | nhru, nmonths | Monthly (January to December) adjustment factor to measured precipitation determined to be rain on each hydrologic response unit to account for differences in elevation, and so forth |
| `snow_cbh_adj` | decimal fraction | nhru, nmonths | Monthly (January to December) adjustment factor to measured precipitation determined to be snow on each hydrologic response unit to account for differences in elevation, and so forth |
| `tmax_allrain_offset` | temp_units | nhru, nmonths | Monthly (January to December) maximum air temperature when precipitation is assumed to be rain; if hydrologic response unit air temperature is greater than or equal to tmax_allsnow plus this value, precipitation is rain |
| `tmax_allsnow` | temp_units | nhru, nmonths | Maximum air temperature when precipitation is assumed to be snow; if hydrologic response unit air temperature is less than or equal to this value, precipitation is snow |
| `tmax_cbh_adj` | temp_units | nhru, nmonths | Monthly (January to December) adjustment factor to maximum air temperature for each hydrologic response unit, estimated on the basis of slope and aspect |
| `tmax_index` | temp_units | nhru, nmonths | Monthly (January to December) index temperature used to determine precipitation adjustments to solar radiation for each hydrologic response unit |
| `tmin_cbh_adj` | temp_units | nhru, nmonths | Monthly (January to December) adjustment factor to minimum air temperature for each hydrologic response unit, estimated on the basis of slope and aspect |
| `tstorm_mo` | none | nhru, nmonths | Monthly flag (January to December) for prevalent storm type for each hydrologic response unit (0=frontal storms; 1=convective storms) |

**Derived from spatial layers, not yet produced here** (2)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `dprst_seep_rate_close` | fraction/day | nhru | Coefficient used in linear seepage flow equation for closed surface depressions for each hydrologic response unit |
| `jh_coef_hru` | degrees Fahrenheit | nhru | Air temperature coefficient used in Jensen-Haise potential evapotranspiration computations for each hydrologic response unit |

**Fabric attributes (geometry, topology, identity)** (5)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `hru_area` | acres | nhru | Area of each hydrologic response unit |
| `hru_lat` | degrees North | nhru | Latitude of each hydrologic response unit |
| `hru_lon` | degrees East | nhru | Longitude of each hydrologic response unit |
| `hru_segment_nhm` | none | nhru | NHM Segment index to which an hydrologic response unit contributes lateral flows (surface runoff, interflow, and groundwater discharge) |
| `nhm_id` | none | nhru | National Hydrologic Model hydrologic response unit ID |

**Stream-segment network** (12)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `K_coef` | hours | nsegment | Travel time of flood wave from one segment to the next downstream segment, called the Muskingum storage coefficient; enter 1.0 for reservoirs, diversions, and segment(s) flowing out of the basin |
| `mann_n` | dimensionless | nsegment | Mannings roughness coefficient for each hydrologic response unit |
| `nhm_seg` | none | nsegment | National Model ID |
| `obsin_segment` | none | nsegment | Index of measured streamflow station that replaces inflow to a segment |
| `obsout_segment` | none | nsegment | Index of measured streamflow station that replaces outflow from a segment |
| `seg_depth` | meter | nsegment | Depth of bankfull water in segment |
| `seg_length` | meters | nsegment | Length of each segment |
| `seg_slope` | decimal fraction | nsegment | Surface slope of each segment |
| `segment_flow_init` | cfs | nsegment | Initial flow in each stream segment |
| `segment_type` | none | nsegment | Segment type (0=segment; 1=headwater; 2=lake; 3=replace inflow; 4=inbound to NHM; 5=outbound from NHM; 6=inbound to region; 7=outbound from region; 8=drains to ocean; 9=sink; 10=inbound from Great Lakes; 11=outbound to Great Lakes) |
| `to_segment` | none | nsegment | Index of downstream segment to which the segment streamflow flows, for segments that do not flow to another segment enter 0 |
| `x_coef` | decimal fraction | nsegment | The amount of attenuation of the flow wave, called the Muskingum routing weighting factor; enter 0.0 for reservoirs, diversions, and segment(s) flowing out of the basin |

**Gages / points of interest** (3)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `poi_gage_id` | none | npoigages | USGS stream gage for this point of interest |
| `poi_gage_segment` | none | npoigages | Segment index for gage point of interest |
| `poi_type` | none | npoigages | Type code for each point of interest gage |

**Scalar constants** (7)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `albset_rna` | decimal fraction | one | Fraction of rain in a mixed precipitation event above which the snow albedo is not reset; applied during the snowpack accumulation stage |
| `albset_rnm` | decimal fraction | one | Fraction of rain in a mixed precipitation event above which the snow albedo is not reset; applied during the snowpack melt stage |
| `albset_sna` | inches | one | Minimum snowfall, in water equivalent, needed to reset snow albedo during the snowpack accumulation stage |
| `albset_snm` | inches | one | Minimum snowfall, in water equivalent, needed to reset snow albedo during the snowpack melt stage |
| `den_init` | gram/cubic centimeter | one | Initial density of new-fallen snow |
| `den_max` | gram/cubic centimeter | one | Average maximum snowpack density |
| `settle_const` | decimal fraction | one | Snowpack settlement time constant |

**Run controls and unit flags** (7)

| Parameter | Units | Dimension | Description |
|---|---|---|---|
| `elev_units` | none | one | Flag to indicate the units of the elevation values (0=feet; 1=meters) |
| `outlet_sta` | none | one | Index of measured streamflow station corresponding to the basin outlet |
| `precip_units` | none | one | Units for measured precipitation (0=inches; 1=millimeters) |
| `print_freq` | none | one | Flag to select the output frequency; for combinations, add index numbers, e.g., daily plus yearly = 10; yearly plus total = 3 (0=none; 1=run totals; 2=yearly; 4=monthly; 8=daily; or additive combinations) |
| `print_type` | none | one | Flag to select the type of results written to the output file (0=measured and simulated flow only; 1=water balance table; 2=detailed output) |
| `runoff_units` | none | one | Measured streamflow units (0=cubic feet per second; 1=cubic meters per second) |
| `temp_units` | none | one | Flag to indicate the units of measured air-temperature values (0=Fahrenheit; 1=Celsius) |
<!-- END GENERATED: prms-not-covered -->

> The three tables above are generated from `configs/` and the data dictionary by
> `python scripts/build_prms_coverage_doc.py`; CI fails if they are stale. Do not edit them
> by hand.

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
