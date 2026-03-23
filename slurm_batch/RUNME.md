# GFv2 SLURM batch: how to run

This folder contains SLURM scripts to run the parameter-generation pipeline on HPC.
All scripts assume:
- `module load miniforge/latest`
- `conda activate geoenv`
- Python entry points under `gfv2-params/scripts/`
- Config files under `gfv2-params/configs/`
- Logs in `slurm_batch/logs/`

## Typical sequencing
1. NHD preprocessing per VPU (optional, if not already prepared)
   - `sbatch a_process_NHD_by_vpu.batch`
2. Generate parameters per VPU
   - Elevation: `sbatch 01_create_elev_params.batch`
   - Slope: `sbatch 02_create_slope_params.batch`
   - Aspect: `sbatch 03_create_aspect_params.batch`
   - Soils: `sbatch 04_create_soils_params.batch`
   - Soil Moist Max: `sbatch 05_create_soilmoistmax_params.batch`
   - SSFlux: `sbatch 06_create_ssflux_params.batch`
3. Merge results and fill gaps
   - `sbatch 07_merge_output_params.batch`
4. Merge default NHM parameter tables by nat_hru_id (optional)
   - `sbatch 08_merge_default_output_params.batch`

## Common options
- Most scripts are array jobs over VPUs. To run a single VPU, use the corresponding `*a_*_update.batch` file or edit the `#SBATCH --array` line.
- Two VPU naming schemes exist:
  - Detailed: `01 02 03N 03S 03W 04 05 06 07 08 09 10L 10U 11 12 13 14 15 16 17 18`
  - Simple: `01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18`
  Match the scheme to the script you're using.

## Examples
Run elevation for all VPUs:
```bash
sbatch 01_create_elev_params.batch
```

Rerun SSFlux just for VPU 14 (long runtime):
```bash
sbatch 06a_create_ssflux_params_update.batch
```

Merge all outputs and fill missing nat_hru_id:
```bash
sbatch 07_merge_output_params.batch
```

Merge NHM default params to nat_hru_id:
```bash
sbatch 08_merge_default_output_params.batch
```

## Tips
- Memory is large (128–256G). If OOM occurs, raise `--mem` or reduce dataset scope.
- Keep an eye on `logs/job_<JOBID>_<ARRAY_INDEX>.{out,err}` for failures.
- Elevation, slope, and aspect use the same driver (`scripts/1_create_dem_params.py`) with different configs. Don’t duplicate logic—pass the right `--config`.
- `07_merge_output_params.batch` runs multiple `srun --exclusive` jobs in parallel; ensure `--ntasks` matches the number of backgrounded `srun` lines.

## Run these first (prep)
These prepare datasets used by the other steps.

- NHD preprocess by VPU (merges/normalizes inputs):
   ```bash
   sbatch a_process_NHD_by_vpu.batch
   ```
- Combined slope+aspect (alternative to separate 02/03 jobs):
   ```bash
   sbatch b_process_slope_aspect.batch
   ```
   If you run this, you typically do not need `02_create_slope_params.batch` and `03_create_aspect_params.batch`.

## Script → Config → Entry point
- 01 elevation → `configs/01_elev_param_config.yml` → `scripts/1_create_dem_params.py`
- 02 slope → `configs/02_slope_param_config.yml` → `scripts/1_create_dem_params.py`
- 03 aspect → `configs/03_aspect_param_config.yml` → `scripts/1_create_dem_params.py`
- 04 soils → `configs/04_soils_param_config.yml` → `scripts/4_create_soils_params.py`
- 05 soilmoistmax → `configs/05_soilmoistmax_param_config.yml` → `scripts/4_create_soils_params.py`
- 06 ssflux → `configs/06_ssflux_param_config.yml` → `scripts/6_create_ssflux_params.py`
- 07 merge+fill → multiple `7_add_nat_hru_id.py` calls then `merge_vpu_and_fill_params.py`
- 08 merge defaults → `8_add_nat_hru_id_default_nhru.py` with param DB dictionary
- a_process_NHD_by_vpu → `configs/config_merge_rpu_by_vpu.yml` → `scripts/process_NHD_by_vpu.py`
- b_process_slope_aspect → `configs/config_slope_aspect.yml` → `scripts/process_slope_and_aspect.py`

## VPU selection and reruns
- Most scripts define a `vpus=(...)` array and use `#SBATCH --array=...` to fan out.
- To target one VPU, prefer the matching `*a_*_update.batch` script, or temporarily narrow the array (e.g., `#SBATCH --array=0-0`) and set `vpus=("14")`.
- Two naming schemes exist; match the scheme to the script:
   - Detailed: `01 02 03N 03S 03W 04 05 06 07 08 09 10L 10U 11 12 13 14 15 16 17 18`
   - Simple: `01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18`

## Monitoring & troubleshooting
- Check queue and running jobs:
   ```bash
   squeue -u "$USER"
   ```
- Inspect recent logs:
   ```bash
   tail -n 200 logs/job_*.out
   tail -n 200 logs/job_*.err
   ```
- See job/accounting info (if enabled):
   ```bash
   sacct -j <JOBID> -o JobID,State,Elapsed,MaxRSS,ReqMem,AllocCPUS
   ```
- Cancel or requeue:
   ```bash
   scancel <JOBID>
   scontrol requeue <JOBID>
   ```

## Expected outputs (by stage)
- 01 elevation: per-VPU elevation-derived parameter files (see configs for exact output paths); consumed later by 07 merge.
- 02 slope: per-VPU slope parameter outputs; or use combined step below.
- 03 aspect: per-VPU aspect outputs; may be produced by the combined slope/aspect step instead.
- b_process_slope_aspect: produces both slope and aspect per VPU via `process_slope_and_aspect.py` and `config_slope_aspect.yml`.
- 04 soils: per-VPU soils parameter outputs.
- 05 soilmoistmax: per-VPU soil moisture capacity outputs.
- 06 ssflux: per-VPU subsurface flux parameter outputs.
- 07 merge+fill: merged cross-VPU parameter tables with `nat_hru_id`, with nearest-neighbor fill for missing ssflux IDs.
- 08 merge defaults: NHM default parameter tables rekeyed to `nat_hru_id` into `nhm_params/nhm_default_params_merged/`.

Actual filenames and directories are controlled by the YAML configs under `gfv2-params/configs/`.

## VPU array index → code map
Many scripts select the VPU from a bash array using `SLURM_ARRAY_TASK_ID`. Here’s the common ordering used:

- Detailed scheme (used by 01/02/03/06):
   0: 01, 1: 02, 2: 03N, 3: 03S, 4: 03W, 5: 04, 6: 05, 7: 06, 8: 07, 9: 08,
   10: 09, 11: 10L, 12: 10U, 13: 11, 14: 12, 15: 13, 16: 14, 17: 15, 18: 16, 19: 17, 20: 18

- Simple scheme (used by a_process_NHD_by_vpu and b_process_slope_aspect):
   0: 01, 1: 02, 2: 03, 3: 04, 4: 05, 5: 06, 6: 07, 7: 08, 8: 09, 9: 10,
   10: 11, 11: 12, 12: 13, 13: 14, 14: 15, 15: 16, 16: 17, 17: 18

Confirm the array and VPU list inside the specific `.batch` file before launching.
