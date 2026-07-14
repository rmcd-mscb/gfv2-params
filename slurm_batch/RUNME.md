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
# Stage NHDPlus flowline topology FIRST (one-time, CONUS; required by both NHD
# COMID steps below for the Network-Flowline gate):
pixi run --as-is python -m gfv2_params.download.nhd_topology
# Stage NHD-connected waterbody COMIDs (one-time, CONUS):
sbatch slurm_batch/download_nhd_flowlines.batch
# Stage flow-through waterbody COMIDs (one-time, CONUS):
sbatch slurm_batch/stage_nhd_flowthrough.batch
# Endorheic classifier inputs (run once; CONUS-shared, fabric-independent):
pixi run --as-is python -m gfv2_params.download.nhd_burn_components   # Sink.shp + BurnAddWaterbody
pixi run --as-is python -m gfv2_params.download.wbd_huc12             # full WBD (type-C closed basins)
# Stage WESM 1m footprints (one-time, CONUS; dprst_depth's best-available-topo tagging):
pixi run --as-is python -m gfv2_params.download.wesm
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric gfv2   # tiny VRT (login OK)

# 3a. landmask FIRST, standalone — dprst_depth (3b, below) needs land_mask.tif
# on disk before it can fill+burn, but must itself run BEFORE the rest of the
# stack (3c) reaches the dprst_depth step (issue #173 — its in-process
# fallback is a ~250-500 CORE-HOUR CONUS compute, i.e. unbounded wall-clock
# on one core; see HPC_REFERENCE.md "Stage 2d'"):
sbatch slurm_batch/build_depstor_rasters.batch --step landmask

# 3b. dprst_depth's own SLURM array (plan -> array -> build -> mean_zonal ->
# mean_finalize) -- wait for this to COMPLETE before 3c:
BATCHES=$(pixi run --as-is python -c \
  "import yaml;print(yaml.safe_load(open('configs/base_config.yml'))['data_root'])")/gfv2/batches
slurm_batch/submit_dprst_depth.sh "$BATCHES" gfv2 configs/base_config.yml 150

# 3c. the rest of the depstor raster stack (landmask + dprst_depth both
# already exist -> skipped fast; imperv/waterbody/endorheic/wbody_connectivity/
# dprst/perv/hru_id/vpu_id/routing/routing_hru/drains_*/carea_map run normally):
sbatch slurm_batch/build_depstor_rasters.batch
```

**What it does:** clips the fabric-bounds FDR template, then builds the full
depression-storage raster stack. The NHD/WBD staging steps + the WESM stage
are one-time CONUS runs. `nhd_topology` stages the NHDPlus PlusFlowlineVAA
network (`flowline_topology.parquet`) and **must run first**: both COMID
steps gate on-stream promotion on Network-Flowline membership, so a waterbody
NHD tagged only via Non-Network flowlines (closed-basin lakes) stays
depression storage (issue #161). `nhd_flowlines` then stages
WBAREACOMI-connected COMIDs and `nhd_flowthrough` adds flow-through COMIDs
(both fail loud if the topology parquet is missing) — the two COMID sets are
unioned by the `wbody_connectivity` builder. If you update either NHD staging
COMID output after an initial build, rerun the depstor stack from
`wbody_connectivity`
(`sbatch slurm_batch/build_depstor_rasters.batch --from wbody_connectivity --force`).

`nhd_burn_components` and `wbd_huc12` stage the (optional) inputs to the
`endorheic` depstor step — Signal A (FDR terminus-inside-itself) needs no
staging and always runs; Signal B (majority-inside a closed WBD HUC12) and the
BurnAddWaterbody union into `waterbody` need these two. Never substitute the
pre-made `input/nhd/NHD_sink_points.gpkg` or `input/nhd/closed_huc12.gpkg` —
both are incomplete extracts (see `HPC_REFERENCE.md`'s "Endorheic classifier
inputs"). `wbody_connectivity` subtracts the `endorheic` output from the
on-stream set — a strict subtraction, never additive — so changing the
waterbody layer or the on-stream COMID set re-runs `waterbody → endorheic →
wbody_connectivity → dprst → routing → drains_perv/drains_imperv`
(`--mem=384G` for `waterbody`/`dprst`, `96G` for `routing`).

`wbody_connectivity` also writes a second raster, `endorheic_wbody.tif` (the
full endorheic-classified set, regardless of on-stream status). `dprst`
consumes it to exempt an endorheic waterbody's own cells from the
region-level on-stream exclusion when `clump_regions`' 8-connected labelling
has merged it with a genuinely on-stream neighbour — e.g. the Great Salt Lake
is 8-connected to a 49.1 km² inflow SwampMarsh, and without the exemption that
one marsh vetoed the whole 4,369 km² lake out of depression storage. Optional:
a fabric that hasn't run `endorheic` (no `endorheic_wbody` on disk) gets no
exemption, a pure no-op.

`dprst_depth` (3b) is split out of the single whole-stack job (3c) because its
compute cost scales with the ~286k CONUS dprst **polygons** (one windowed DEM
read each), not the CONUS grid — see `docs/ARCHITECTURE.md`'s "CONUS-scale
COMPUTE" gotcha and `HPC_REFERENCE.md`'s "Stage 2d'" for the full DAG,
sizing arithmetic, and recovery. `submit_dprst_depth.sh`'s stages produce
`dprst_depth.tif`/`op_flow_thres_params.csv` (`{fabric}/depstor_rasters/`)
*and* `nhm_dprst_depth_avg_params.csv` (`{fabric}/params/merged/`) — the
latter does not go through Step 4's depstor-fractions loop below.

**Wait for:** step 3a `COMPLETED`; step 3b's final job (`mean_finalize`)
`COMPLETED`; then step 3c `COMPLETED`. `{fabric}/depstor_rasters/` holds the
full stack (through `carea_map_t8/t156_binary.tif`).

---

### 4 · Generate parameters

Each parameter is **two batch jobs**: an array job over every HRU batch, then a
merge that runs after it (`afterok`). Submit them **in order, waiting for each
merge before the next** — `slope` must merge before `ssflux`. First set the
shared variables:

```bash
BATCHES=$(pixi run --as-is python -c \
  "import yaml;print(yaml.safe_load(open('configs/base_config.yml'))['data_root'])")/gfv2/batches
FABRIC=gfv2
BASE_CONFIG=configs/base_config.yml
N=$(grep '^n_batches:' "$BATCHES/manifest.yml" | awk '{print $2}')   # array size
THROTTLE=4                                                            # concurrent array tasks
```

**Zonal parameters** — run this pair for each `P`, in order: `elevation`,
`slope`, `aspect`, `soils`, `soil_moist_max`, `lulc_nhm_v11`, `lulc_nalcms`,
`lulc_nlcd`, `lulc_foresce`, and `ssflux` (`ssflux` last — it has an extra
prereq, see below):

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

**What it does:** computes every per-HRU zonal parameter and the 6 depstor ratios.

**Wait for:** all array + merge jobs `COMPLETED` (`squeue -u "$USER"`), then the
ratios job `COMPLETED`.

> **Convenience — run wholesale.** The two wrappers below submit exactly the
> batch jobs above for you, chained with `afterok` (and they handle the
> `slope`→`ssflux` dependency and the weights prereq automatically):
>
> ```bash
> slurm_batch/submit_zonal_params.sh   "$BATCHES" gfv2 configs/base_config.yml
> slurm_batch/submit_depstor_params.sh "$BATCHES" gfv2 configs/base_config.yml
> ```

---

### 5 · Gap-fill missing values

```bash
sbatch slurm_batch/merge_and_fill_params.batch
```

**What it does:** KNN-fills any missing per-HRU parameter values.

**Wait for:** the job `COMPLETED`.

---

### 6 · (optional) Merge NHM defaults

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

### 8 · (optional) Snow depletion curves (SNODAS → snarea_curve)

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

- `{data_root}/gfv2/params/merged/` — final parameter CSVs, including the 6
  depstor ratios (`sro_to_dprst_perv`, `sro_to_dprst_imperv`, `carea_max`,
  `smidx_coef`, `hru_percent_imperv`, `dprst_frac`) and
  `nhm_dprst_depth_avg_params.csv` (issue #173 — derived, NOT the pyWatershed
  132 in default; see `docs/pywatershed_depression_storage_requirements.md`).
- `{data_root}/gfv2/params/merged/_intermediates/` — 10 per-fraction count
  CSVs (inputs to ratio derivation; `count` is NOT a [0, 1] fraction).
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
