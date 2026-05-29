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
sbatch slurm_batch/merge_vpu_segments.batch                        # nsegment merge (depstor)
sbatch slurm_batch/prepare_fabric.batch                            # spatial batching + manifest
```

**What it does:** merges per-VPU `nhru`/`nsegment` into the CONUS fabric, then
batches it into per-batch geopackages.

**Wait for:** `{fabric}/fabric/` has the merged nhru + nsegment gpkgs and
`{fabric}/batches/manifest.yml` exists.

---

### 3 · Build depstor rasters

```bash
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric gfv2   # tiny VRT (login OK)
sbatch slurm_batch/build_depstor_rasters.batch
```

**What it does:** clips the fabric-bounds FDR template, then builds the full
depression-storage raster stack.

**Wait for:** the job `COMPLETED`; `{fabric}/depstor_rasters/` holds the full
stack (through `carea_map_t8/t156_binary.tif`).

---

### 4 · Generate parameters

```bash
BATCHES=$(pixi run --as-is python -c \
  "import yaml;print(yaml.safe_load(open('configs/base_config.yml'))['data_root'])")/gfv2/batches
slurm_batch/submit_zonal_params.sh   "$BATCHES" gfv2 configs/base_config.yml
slurm_batch/submit_depstor_params.sh "$BATCHES" gfv2 configs/base_config.yml
```

**What it does:** fans out the zonal + depstor param array jobs and chains
their merges (+ ratios / ssflux weights) via `afterok`.

**Wait for:** all array + merge (+ ratios) jobs `COMPLETED` in `squeue`.

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
  `smidx_coef`, `hru_percent_imperv`, `dprst_frac`).
- `{data_root}/gfv2/params/merged/_intermediates/` — 10 per-fraction count
  CSVs (inputs to ratio derivation; `count` is NOT a [0, 1] fraction).
- `docs/figures/gfv2/` — rendered PNG figures.

---

## Need more?

See [HPC_REFERENCE.md](HPC_REFERENCE.md) for:

- Running other fabrics (VPU01 validation, Oregon, new fabric registration).
- Running one parameter at a time (Stage 4A incremental path).
- Single-step raster rebuilds (`--step <name>`, `--from <name>`).
- Recovery / partial reruns (single-batch array resubmit, VPU source refill).
- Environment internals and array concurrency throttle.
- The script → config → entry-point map.
