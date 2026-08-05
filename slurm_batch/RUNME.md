# GFv2 Pipeline — Runbook (CONUS `gfv2`)

The commands to take a fresh data root to finished parameters, in order.
Running a different fabric, re-running one piece, internals, and recovery are
in [HPC_REFERENCE.md](HPC_REFERENCE.md).

---

## Before you start

- Run `pixi install` once from the repo root; ensure `~/.pixi/bin` is on `PATH`.
- Always run `sbatch` / `submit_*.sh` from a shell where `~/.pixi/bin` is on
  `PATH` (SLURM inherits it — a missing PATH causes immediate `pixi: command not found`).
- Run everything from the repo root (`cd <repo>`).

---

## Quick Start for Scientists (CONUS `gfv2`)

If you are running the standard CONUS `gfv2` pipeline from scratch, these are
the commands in order. Each block says what to wait for before moving on.
For anything non-standard (different fabric, re-running one step, recovery),
see the full steps below or [HPC_REFERENCE.md](HPC_REFERENCE.md).

```bash
# 0 · Initialize and download inputs (~112 GB total; runs overnight)
pixi run init-data-root
sbatch slurm_batch/download_rpu_rasters.batch
sbatch slurm_batch/download_nalcms.batch
sbatch slurm_batch/download_nhm_v11.batch
sbatch slurm_batch/stage_twi.batch
```
> Wait for all four jobs `COMPLETED`. Then stage the manual inputs listed in
> `README.md` (soils, LULC, NHM defaults), then verify:
```bash
pixi run init-data-root --check

# 1 · Build shared CONUS rasters (runs once; reused by every fabric)
sbatch slurm_batch/build_shared_rasters.batch
```
> Wait for `COMPLETED`.
```bash

# 2 · Prepare the fabric
#     NOTE: run the first command on a COMPUTE NODE (JupyterHub or salloc), not the login node.
pixi run -e notebooks marimo run notebooks/merge_vpu_targets.py
sbatch slurm_batch/merge_vpu_segments.batch
sbatch slurm_batch/prepare_fabric.batch
```
> Wait for both batch jobs `COMPLETED`.
```bash

# 3 · Build depression-storage rasters
#     One-time downloads (skip if already staged in this data_root):
srun -p cpu -A impd --time=02:00:00 --ntasks=1 --cpus-per-task=4 --mem=48G \
  pixi run --as-is python -m gfv2_params.download.nhd_waterbodies
pixi run --as-is python -m gfv2_params.download.nhd_burn_components
pixi run --as-is python -m gfv2_params.download.wbd_huc12
pixi run --as-is python -m gfv2_params.download.wesm
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric gfv2
#     Depstor raster stack — run in this order, waiting for each to COMPLETE:
sbatch slurm_batch/build_depstor_rasters.batch --step landmask
sbatch slurm_batch/build_depstor_rasters.batch --step segment_wbody
sbatch slurm_batch/build_depstor_rasters.batch --step endorheic
sbatch slurm_batch/build_depstor_rasters.batch --step imperv
sbatch slurm_batch/build_depstor_rasters.batch --step waterbody
sbatch slurm_batch/build_depstor_rasters.batch --step wbody_connectivity
sbatch slurm_batch/build_depstor_rasters.batch --step dprst
BATCHES=$(pixi run data-root)/gfv2/batches
slurm_batch/submit_dprst_depth.sh "$BATCHES" gfv2 configs/base_config.yml 150
```
> Wait for `submit_dprst_depth.sh`'s final job (`mean_finalize`) `COMPLETED`, then:
```bash
sbatch slurm_batch/build_depstor_rasters.batch
```
> Wait for `COMPLETED`.
```bash

# 4 · Generate parameters (submits and chains all jobs automatically)
slurm_batch/submit_zonal_params.sh   "$BATCHES" gfv2 configs/base_config.yml
slurm_batch/submit_depstor_params.sh "$BATCHES" gfv2 configs/base_config.yml
```
> Wait for all submitted jobs `COMPLETED` — monitor with `squeue -u "$USER"`.
```bash

# 5 · Gap-fill missing values
sbatch slurm_batch/merge_and_fill_params.batch
```
> Wait for `COMPLETED`.
```bash

# 6 · (optional) Merge NHM default parameter tables
sbatch slurm_batch/merge_default_output_params.batch

# 7 · Render results figures
sbatch slurm_batch/render_figures.batch
```

Outputs land in `{data_root}/gfv2/params/merged/` (parameter CSVs) and
`docs/figures/gfv2/` (figures). See [Where outputs land](#where-outputs-land) below.

---

## Pipeline at a glance

1. **Step 0** — Initialize data root, download public rasters, stage TWI.
2. **Step 1** — Build shared (CONUS) rasters (one orchestrator batch).
3. **Step 2** — Prepare the fabric: merge nhru/nsegment, spatial batching.
4. **Step 3** — Clip fabric-bounds FDR template; build depstor raster stack.
5. **Step 4** — Fan out zonal + depstor parameter array jobs.
6. **Step 5** — KNN gap-fill missing parameter values.
7. **Step 6** — (optional) Merge NHM default parameter tables.
8. **Step 7** — Render results figures headlessly.
9. **Step 8** — (optional, fabric-independent) Derive snow depletion curves
   (SNODAS → `snarea_curve` curve library, 3 stages).

---

## Run it (CONUS gfv2)

### 0 · Initialize + stage inputs

```bash
pixi run init-data-root
sbatch slurm_batch/download_rpu_rasters.batch
sbatch slurm_batch/download_nalcms.batch
sbatch slurm_batch/download_nhm_v11.batch
sbatch slurm_batch/stage_twi.batch
pixi run init-data-root --check     # after downloads + manual inputs are in place
```

**What it does:** scaffolds `data_root`, downloads the public rasters (~112 GB
NHDPlus RPU, ~2 GB NALCMS, NHM v1.1 LULC), stages per-RPU TWI; `--check`
verifies manually-staged inputs. (Manual-input table + provenance →
HPC_REFERENCE "Stage 0".)

**Wait for:** the four download/stage jobs `COMPLETED` in `squeue`, and
`init-data-root --check` reporting all inputs present.

---

### 1 · Build shared (CONUS) rasters

```bash
sbatch slurm_batch/build_shared_rasters.batch
```

**What it does:** walks the whole shared-raster DAG (per-VPU merge →
slope/aspect → border DEM → land mask → TWI merge → VRTs → derived + LULC
rasters).

**Wait for:** the job `COMPLETED`; `shared/conus/vrt/` holds the VRTs (esp.
`fdr.vrt`, `twi.vrt`, `twi_hydrodem.vrt`).

---

### 2 · Prepare the fabric

```bash
# merge_vpu_targets is an interactive marimo notebook — run it on a COMPUTE
# node (JupyterHub or `salloc`), never the login node. See HPC_REFERENCE.
pixi run -e notebooks marimo run notebooks/merge_vpu_targets.py   # nhru merge (compute node)
sbatch slurm_batch/merge_vpu_segments.batch                        # nsegment merge (VPU-based fabrics)
sbatch slurm_batch/prepare_fabric.batch                            # spatial batching + manifest
```

**What it does:** merges per-VPU `nhru`/`nsegment` into the CONUS fabric, then
batches it into per-batch geopackages.

**Wait for:** `{fabric}/fabric/` has the merged nhru + nsegment gpkgs and
`{fabric}/batches/manifest.yml` exists.

---

### 3 · Build depstor rasters

```bash
# The waterbody source (required): every CONUS fabric's waterbody_gpkg points at
# this source-derived layer, not the retired hand-made conus_waterbodies.gpkg.
srun -p cpu -A impd --time=02:00:00 --ntasks=1 --cpus-per-task=4 --mem=48G \
  pixi run --as-is python -m gfv2_params.download.nhd_waterbodies
# Endorheic classifier inputs (run once; CONUS-shared, fabric-independent):
pixi run --as-is python -m gfv2_params.download.nhd_burn_components   # Sink.shp + BurnAddWaterbody
pixi run --as-is python -m gfv2_params.download.wbd_huc12             # full WBD (type-C closed basins)
# Stage WESM 1m footprints (one-time, CONUS; dprst_depth's best-available-topo tagging):
pixi run --as-is python -m gfv2_params.download.wesm
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric gfv2   # tiny VRT (login OK)
```

> **Optional — NHD segment classifier A/B comparison only.** Not needed for a
> normal depstor run. On-stream classification comes from `segment_wbody` (the
> model's own `nsegment` network). These three staging steps are only needed if
> you set `connected_comids_table`/`flowthrough_comids_table` in a fabric profile
> to A/B against NHD flowline topology (see `CLAUDE.md` "MODEL's own segment
> network"):
>
> ```bash
> pixi run --as-is python -m gfv2_params.download.nhd_topology       # must run first
> sbatch slurm_batch/download_nhd_flowlines.batch                    # WBAREACOMI-connected COMIDs
> sbatch slurm_batch/stage_nhd_flowthrough.batch                     # flow-through COMIDs
> ```

```bash
# 3a. landmask, segment_wbody, and endorheic FIRST, standalone -- each needs
# only profile-level inputs (segments_gpkg/waterbody_gpkg/template_raster for
# segment_wbody; waterbody_gpkg/fdr_raster for endorheic), not any other
# step's output, so all three can run ahead of the rest of the stack (3d):
#   - landmask: dprst_depth (3c, below) needs land_mask.tif on disk before it
#     can fill+burn, but must itself run BEFORE the rest of the stack (3d)
#     reaches the dprst_depth step (issue #173 -- its in-process fallback is a
#     ~250-500 CORE-HOUR CONUS compute, i.e. unbounded wall-clock on one core;
#     see HPC_REFERENCE.md "Stage 2d'").
#   - segment_wbody / endorheic: 3c's `tiling.py --plan` reconstructs the
#     dprst polygon set against the REAL on-stream classifier (segment_wbody
#     minus endorheic, not NHD), so it needs `segment_waterbody_comids.parquet`
#     and `endorheic_waterbody_comids.parquet` already on disk -- both are
#     STEP_ORDER outputs (positions 3 and 5) that 3d would otherwise only
#     produce later, after 3c has already tried and failed to read them.
sbatch slurm_batch/build_depstor_rasters.batch --step landmask
sbatch slurm_batch/build_depstor_rasters.batch --step segment_wbody
sbatch slurm_batch/build_depstor_rasters.batch --step endorheic

# 3b. the rest of the stack UP THROUGH dprst -- imperv, waterbody,
# wbody_connectivity, dprst (STEP_ORDER positions 1, 3, 5, 6; segment_wbody
# and endorheic already ran in 3a). This MUST complete, and `dprst_binary.tif`
# MUST be freshly written, before 3c: 3c's burn (`dprst_depth/burn.py`) masks
# the newly-computed per-polygon depths to whatever `dprst_binary.tif` is ON
# DISK at the time it runs (`dprst_depth.py` -- `ctx.require("dprst")`). On an
# EXISTING data_root (the normal case for a classifier change, not a
# from-scratch build), skipping straight to 3c would burn the new depths
# against the PREVIOUS run's dprst mask -- built by whatever classifier was in
# place before -- and exit 0 with `dprst_depth_avg` silently inconsistent with
# the new `dprst_frac`. `dprst` itself needs `imperv`/`waterbody`
# (`wbody_binary`/`wbody_regions`)/`wbody_connectivity`
# (`connected_wbody`/`endorheic_wbody`), so those run here too -- `waterbody`
# and `dprst` are the ~384G full-grid steps (`--mem=384G`, already the batch
# default):
sbatch slurm_batch/build_depstor_rasters.batch --step imperv
sbatch slurm_batch/build_depstor_rasters.batch --step waterbody
sbatch slurm_batch/build_depstor_rasters.batch --step wbody_connectivity
sbatch slurm_batch/build_depstor_rasters.batch --step dprst

# 3c. dprst_depth's own SLURM array (plan -> array -> build -> mean_zonal ->
# mean_finalize) -- now burns against the FRESH `dprst_binary.tif` from 3b.
# Wait for this to COMPLETE before 3d:
BATCHES=$(pixi run data-root)/gfv2/batches
slurm_batch/submit_dprst_depth.sh "$BATCHES" gfv2 configs/base_config.yml 150

# 3d. the rest of the depstor raster stack (landmask + imperv + segment_wbody
# + waterbody + endorheic + wbody_connectivity + dprst + dprst_depth all
# already exist -> skipped fast; perv/hru_id/vpu_id/routing/routing_hru/
# drains_*/carea_map run normally):
sbatch slurm_batch/build_depstor_rasters.batch
```

**What it does:** clips the fabric-bounds FDR template, then builds the full
depression-storage raster stack (`landmask → imperv → segment_wbody →
waterbody → endorheic → wbody_connectivity → dprst → perv → hru_id →
dprst_depth → vpu_id → routing → routing_hru → drains_perv → drains_imperv →
carea_map`). `segment_wbody` is the on-stream classifier's PRIMARY source: a
waterbody is on-stream iff a model `nsegment` (the fabric's `segments_gpkg`)
intersects it with positive length — see CLAUDE.md's "MODEL's own segment
network" bullet. It's cheap (42 s / 2.0 GB at CONUS), so it doesn't affect this
job's `--mem` sizing. `nhd_waterbodies` and the WESM stage above are the only
one-time CONUS staging runs a normal build needs; the NHD topology/flowlines/
flowthrough steps are opt-in comparison only (see the block above) and are
NOT needed here.

`nhd_burn_components` and `wbd_huc12` stage the (optional) inputs to the
`endorheic` depstor step — Signal A (FDR terminus-inside-itself) needs no
staging and always runs; Signal B (majority-inside a closed WBD HUC12) and the
BurnAddWaterbody union into `waterbody` need these two. Never substitute the
pre-made `input/nhd/NHD_sink_points.gpkg` or `input/nhd/closed_huc12.gpkg` —
both are incomplete extracts (see `HPC_REFERENCE.md`'s "Endorheic classifier
inputs"). `wbody_connectivity` subtracts the `endorheic` output from the
on-stream set — a strict subtraction, never additive — so changing the
`segments_gpkg`/classifier logic alone re-runs `segment_wbody → waterbody →
endorheic → wbody_connectivity → dprst → dprst_depth → routing →
drains_perv/drains_imperv` (`--from segment_wbody --force`); changing the
waterbody layer as well (e.g. a new BurnAddWaterbody union, or the layer swap
this branch made) starts one step later at `waterbody` (`--from waterbody
--force`) since `segment_wbody` itself is unaffected. Either way
`waterbody`/`dprst` are the ~384G full-grid steps (`--mem=384G`), `routing` is
`96G`. **`dprst_depth` is in this cascade too** — it reconstructs the dprst
polygon set from the on-stream classifier directly (not just the burn mask),
so a classifier change invalidates `dprst_depth_batches/`, not just
`dprst_binary.tif`: wipe that directory and re-run the full `submit_dprst_depth.sh`
DAG (3c) after `dprst` (3b) completes, rather than trusting stale per-tile-batch
parquets left over from the previous classifier's polygon set (see the `--plan`
consumer's `min_onstream_comids`/`min_endorheic_comids` floors, which catch a
collapsed reconstruction but not a merely STALE one).

`wbody_connectivity` also writes a second raster, `endorheic_wbody.tif` (the
full endorheic-classified set, regardless of on-stream status). `dprst`
consumes it to exempt an endorheic waterbody's own cells from the
region-level on-stream exclusion when `clump_regions`' 8-connected labelling
has merged it with a genuinely on-stream neighbour — e.g. the Great Salt Lake
is 8-connected to a 49.1 km² inflow SwampMarsh, and without the exemption that
one marsh vetoed the whole 4,369 km² lake out of depression storage. Optional:
a fabric that hasn't run `endorheic` (no `endorheic_wbody` on disk) gets no
exemption, a pure no-op.

`dprst_depth` (3c) is split out of the single whole-stack job (3d) because its
compute cost scales with the ~286k CONUS dprst **polygons** (one windowed DEM
read each), not the CONUS grid — see `docs/ARCHITECTURE.md`'s "CONUS-scale
COMPUTE" gotcha and `HPC_REFERENCE.md`'s "Stage 2d'" for the full DAG,
sizing arithmetic, and recovery. `submit_dprst_depth.sh`'s stages produce
`dprst_depth.tif`/`op_flow_thres_params.csv` (`{fabric}/depstor_rasters/`)
*and* `nhm_dprst_depth_avg_params.csv` (`{fabric}/params/merged/`) — the
latter does not go through Step 4's depstor-fractions loop below.

**Wait for:** all three step 3a jobs (`landmask`, `segment_wbody`, `endorheic`)
`COMPLETED`; all four step 3b jobs (`imperv`, `waterbody`,
`wbody_connectivity`, `dprst`) `COMPLETED`; step 3c's final job
(`mean_finalize`) `COMPLETED`; then step 3d `COMPLETED`. `{fabric}/depstor_rasters/`
holds the full stack (through `carea_map_t8/t156_binary.tif`).

---

### 4 · Generate parameters

```bash
BATCHES=$(pixi run data-root)/gfv2/batches
slurm_batch/submit_zonal_params.sh   "$BATCHES" gfv2 configs/base_config.yml
slurm_batch/submit_depstor_params.sh "$BATCHES" gfv2 configs/base_config.yml
```

**What it does:** `submit_zonal_params.sh` chains every zonal parameter (array
+ merge per param, `slope`→`ssflux` dependency and weights prereq handled
automatically); `submit_depstor_params.sh` chains all 10 depstor fractions +
the 6 PRMS ratios job.

**Wait for:** all submitted jobs `COMPLETED` — monitor with `squeue -u "$USER"`.

<details>
<summary>Run one parameter at a time (debugging)</summary>

Each parameter is **two batch jobs**: an array job over every HRU batch, then a
merge that runs after it (`afterok`). Submit them **in order, waiting for each
merge before the next** — `slope` must merge before `ssflux`. First set the
shared variables:

```bash
BATCHES=$(pixi run data-root)/gfv2/batches
FABRIC=gfv2
BASE_CONFIG=configs/base_config.yml
N=$(grep '^n_batches:' "$BATCHES/manifest.yml" | awk '{print $2}')   # array size
THROTTLE=4                                                            # concurrent array tasks
```

**Zonal parameters** — run this pair for each `P`, in order: `elevation`,
`slope`, `aspect`, `soils`, `soil_moist_max`, `lulc_nhm_v11`, `lulc_nalcms`,
`lulc_nlcd`, `lulc_foresce`, and `ssflux` (`ssflux` last — it has an extra
prereq, see below).

> **Note:** `lulc_nlcd` and `lulc_foresce` have no staged CONUS inputs
> (`input/lulc_veg/{nlcd,foresce}/` are absent), so they fail on a gfv2 run
> while the other eight merge normally. Skip them with
> `export ZONAL_PARAMS="elevation slope aspect soils soil_moist_max lulc_nhm_v11 lulc_nalcms ssflux"`.

```bash
P=elevation     # change P and re-run for each parameter above, in order
AID=$(sbatch --parsable --array=0-$((N-1))%$THROTTLE \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,PARAM=$P \
      slurm_batch/derive_zonal_params.batch)
sbatch --dependency=afterok:$AID \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,PARAM=$P \
      slurm_batch/merge_zonal_param.batch
```

**ssflux** needs the CONUS P2P weight matrix and the merged `slope` CSV first.
Build the weights, then run the pair above with `P=ssflux`:

```bash
sbatch --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC slurm_batch/build_zonal_weights.batch
# after weights + slope merge finish, submit ssflux's array + merge with P=ssflux
```

**Depstor fractions** — same pair per `F` (any order): `perv_frac`,
`imperv_frac`, `dprst_frac`, `drains_perv_frac`, `drains_imperv_frac`,
`onstream_storage_frac`, `drains_to_dprst_frac`, `carea_t8_frac`,
`carea_t156_frac`, `hru_total`:

```bash
F=perv_frac     # change F and re-run for each fraction above
AID=$(sbatch --parsable --array=0-$((N-1))%$THROTTLE \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,FRACTION=$F \
      slurm_batch/create_depstor_zonal.batch)
sbatch --dependency=afterok:$AID \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,FRACTION=$F \
      slurm_batch/merge_depstor_fraction.batch
```

**Depstor ratios** — after **all 10** fraction merges have `COMPLETED`, derive
the 6 PRMS ratios:

```bash
sbatch --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC slurm_batch/derive_depstor_ratios.batch
```

</details>

### 5 · Gap-fill missing values

```bash
sbatch slurm_batch/merge_and_fill_params.batch
```

**What it does:** default (all-params) mode — loops **every** param that
declares `fill_columns` in `configs/zonal/zonal_params.yml`,
`configs/depstor/depstor_params.yml`, or `configs/snarea/snarea_library.yml`
and whose `merged/` CSV already exists for this fabric, and KNN-fills it
against the fabric's HRU centroids. This replaced an earlier version that
filled exactly one hardcoded file (`nhm_ssflux_params.csv`) per invocation —
which is why the canonical set silently differed per fabric (2 files on
`gfv2`, 4 on `oregon`) and why four `oregon` params went unfilled until
someone audited them by hand. Pass `--param_file <path>` to fill one file
instead of the default sweep.

**`merged/<name>.csv` is the single canonical, always-gap-filled per-HRU file
— read `merged/*.csv`.** The `filled_` prefix is **retired**: there is no
longer a separate `filled_nhm_*.csv` to look for (an older checkout's docs or
memory may still mention it). The step writes the filled result **in place**
over `merged/<name>.csv`; the pre-fill (raw) copy is preserved once at
`merged/_unfilled/<name>.csv` (never overwritten on a re-run, since the
on-disk file is by then already filled).

The run logs, per param, how many absent HRU rows and NaN cells it filled.
Two asymmetric guards fire per param:

- A column **not** declared in that param's `fill_columns` but carrying NaN
  cells only **warns** (names the column and the NaN count) — a NaN cell can
  be a legitimate "not derivable" result (e.g. `cv_empirical`, derivable for
  only ~42% of HRUs by design; `cv_subgrid` exists to rescue the rest), so it
  is left untouched rather than silently filled.
- A param that is missing an HRU **row** entirely but declares no
  `fill_columns` (or `fabric_columns`) **raises** instead of passing silently
  — an absent row admits no "not derivable" reading, unlike a NaN cell.

A param may also declare **`fabric_columns`** for values that are exact facts
already on disk in the fabric gpkg rather than things to interpolate. These
are copied verbatim from the fabric into synthesized rows only — never
KNN-filled, never applied to rows that already exist. Today only `ssflux`
declares one (`hru_area: {source: geometry, scale: 1.0}`, i.e.
`geometry.area` in the fabric CRS's units). Unlike the NaN-cell warning
above, every `fabric_columns` failure **raises**: a malformed spec, a
`source` absent from the fabric gpkg, an id the fabric cannot serve, or a
value still NaN after the copy. See `docs/ARCHITECTURE.md`.

Migrating an existing product off the old `filled_` layout is a one-time,
separate step — see `scripts/migrate_filled_params.py` (dry-run by default,
`--apply` to execute).

> **Run the migration BEFORE the first Step 5 run on an existing product.**
> If a fabric's `merged/` directory still has any `filled_*.csv` files from
> before this convention existed, migrate it first. Running Step 5 (which
> writes the new, correct fill in place) and only migrating afterward would
> have the migration move the stale `filled_` file over top of today's
> correct fill — refused loudly, not silently, as of the Finding-1 fix, but
> the ordering above avoids hitting the refusal at all.

**Wait for:** the job `COMPLETED`.

---

### 6 · Merge NHM defaults

> **Optional:** only needed if you want to merge NHM default parameter tables
> into the per-HRU outputs.

```bash
sbatch slurm_batch/merge_default_output_params.batch
```

**What it does:** merges the NHM default parameter tables into the per-HRU
outputs.

**Wait for:** the job `COMPLETED`.

---

### 7 · View results

```bash
sbatch slurm_batch/render_figures.batch     # PNGs -> docs/figures/gfv2/
```

**What it does:** renders the fabric_results figure set headlessly.
(Interactive viewing via JupyterHub → HPC_REFERENCE "Stage 9".)

**Wait for:** the job `COMPLETED`; PNGs in `docs/figures/gfv2/`.

---

### 8 · Snow depletion curves (SNODAS → snarea_curve)

> **Optional:** fabric-independent; run after Step 5 if `snarea_curve`,
> `hru_deplcrv`, and `snarea_thresh` are needed.

```bash
# One-command recipe: submits all 4 jobs (Stage 1 array -> merge -> Stage 2
# derive -> Stage 3 library) chained --dependency=afterok, and prints the IDs.
# Sizes the Stage-1 array from the fabric manifest and picks a Stage-2 --mem by
# fabric (64G for oregon, 384G CONUS default). Dry-run first with DRYRUN=1.
DRYRUN=1 ./slurm_batch/submit_snarea_pipeline.sh gfv2   # inspect the chain
./slurm_batch/submit_snarea_pipeline.sh gfv2            # submit it
# oregon (small) validation run — Stage 2 auto-drops to --mem=64G:
./slurm_batch/submit_snarea_pipeline.sh oregon
```

**Re-run Stage 1 (do not skip it):** Stage 1 now emits the per-HRU `swe_std`
sidecar feeding Stage 3's CV. Aggregated NetCDFs written before `swe_std` was
added lack it, so Stage 2 raises `ValueError("...missing swe_std... Re-run
Stage 1...")` until Stage 1 is re-run. The recipe always runs Stage 1, so
just launch it; the gdptools weights are cached (`{fabric}/weights_agg/`) and
reused, making the re-run a cheap extra `masked_std` pass, not a weight
recompute. Export `CLEAR_BATCHES=1` to wipe `{fabric}/snodas/_batches/` first
if you want to be extra safe. The manual per-stage commands are still available
(see HPC_REFERENCE.md "Stage 10") when you want to inspect between stages.

**What it does:** Stage 1 aggregates daily SNODAS SWE to the HRU fabric as a
SLURM array over the fabric's spatial batches (`derive_snodas_aggregate.batch`,
one array task per batch, source grid clipped to each batch's extent), then
`merge_snodas_aggregate.batch` concatenates the per-batch per-year NetCDFs
into one final `snodas_agg_<year>.nc` per calendar year (area-weighted mean
SWE + snow-covered-area fraction, now also the per-cell SWE std dev `swe_std`
sidecar used by Stage 2's sub-grid CV, via the gdptools-backed `aggregate`
harness); Stage 2 derives per-HRU empirical depletion curves and sub-grid CV
from those daily series (Driscoll, Hay & Bock 2017 selection method) and
writes the intermediate `_intermediates/nhm_snarea_curve_derived.csv` (not yet
the terminal params); Stage 3 (`derive_snarea_library.py`) builds the
CV/lognormal curve library from that derived CSV — cheap, pure-tabular, no
daily-SWE reload — and writes the terminal `nhm_snarea_curve_library.csv`,
`nhm_snarea_curve_params.csv`, `nhm_snarea_curve_validation.csv`, and the
pyWatershed `nhm_snarea_curve.nc`. Fabric-independent — no code change to run
against `gfv2`, `gfv2_vpu01`, or `oregon`. `submit_snarea_pipeline.sh` submits
all four jobs (including Stage 2) as an afterok chain; run any stage directly
with `pixi run python ...` / `sbatch` when you want to inspect between stages.

**Wait for:** the merge job `COMPLETED`, printing one `snodas_agg_<year>.nc`
per year written; Stage 2 prints the `sdc_status` breakdown and writes the
derived CSV; the Stage 3 job `COMPLETED` (`--mem=16G --time=00:30:00`),
printing the `ndepl`/estimable/calibrated/reconstruction-error summary. See
HPC_REFERENCE.md "Stage 10" for per-stage detail. For a plain-English,
figure-driven overview of the whole workflow (Driscoll/Sexstone methods and the
pyWatershed products), see the Marp deck
`docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md`.

---

## Monitoring

```bash
squeue -u "$USER"
sacct -j <JOBID> -o JobID,State,Elapsed,MaxRSS
tail -n 200 logs/job_<JOBID>.err
```

---

## Where outputs land

- `{data_root}/gfv2/params/merged/` — the canonical, always-gap-filled
  per-HRU parameter CSVs (Step 5 fills every param declaring `fill_columns`
  in place; consumers read `merged/*.csv`, never a `filled_` prefix — see
  Step 5). Includes the 6 depstor ratios (`sro_to_dprst_perv`,
  `sro_to_dprst_imperv`, `carea_max`, `smidx_coef`, `hru_percent_imperv`,
  `dprst_frac`) and
  `nhm_dprst_depth_avg_params.csv` (issue #173 — derived, NOT the pyWatershed
  132 in default; see `docs/pywatershed_depression_storage_requirements.md`).
- `{data_root}/gfv2/params/merged/_intermediates/` — 10 per-fraction count
  CSVs (inputs to ratio derivation; `count` is NOT a [0, 1] fraction).
- `{data_root}/gfv2/params/merged/_unfilled/` — the pre-fill (raw) copy of
  each `merged/<name>.csv` that Step 5 has gap-filled, preserved once and
  never overwritten on a re-run.
- `{data_root}/gfv2/depstor_rasters/dprst_depth.tif`,
  `op_flow_thres_params.csv` — Step 3's `dprst_depth` step output (per-cell
  V/A mean depth raster; `op_flow_thres_params.csv` is the constant-1.0
  per-HRU CSV, not a `merged/` CSV — see Step 3).
- `docs/figures/gfv2/` — rendered PNG figures.
- `{data_root}/gfv2/snodas/` — per-year aggregated SNODAS SWE/SCA/`swe_std`
  NetCDFs (Stage 1 of the snow depletion curve pipeline, optional Step 8).
- `{data_root}/gfv2/params/merged/_intermediates/nhm_snarea_curve_derived.csv`
  — per-HRU empirical curve + sub-grid CV (Stage 2, optional Step 8).
- `{data_root}/gfv2/params/merged/nhm_snarea_curve_library.csv`,
  `nhm_snarea_curve_params.csv` (`snarea_curve`/`hru_deplcrv`/`snarea_thresh`),
  `nhm_snarea_curve_validation.csv`, and `nhm_snarea_curve.nc` (pyWatershed
  parameter file) — Stage 3, optional Step 8.

---

## Need more?

See [HPC_REFERENCE.md](HPC_REFERENCE.md) for:

- Running other fabrics (VPU01 validation, Oregon, new fabric registration).
- Running one parameter at a time (Stage 4A incremental path).
- Single-step raster rebuilds (`--step <name>`, `--from <name>`).
- Recovery / partial reruns (single-batch array resubmit, VPU source refill).
- Environment internals and array concurrency throttle.
- The script → config → entry-point map.
