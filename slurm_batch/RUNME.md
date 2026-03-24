# GFv2 SLURM batch: how to run

This folder contains SLURM scripts to run the parameter-generation pipeline on HPC.

## Setup

```bash
module load miniforge/latest
conda activate geoenv
pip install -e .
```

All scripts assume:
- Python entry points under `gfv2-params/scripts/`
- Config files under `gfv2-params/configs/`
- Logs in `slurm_batch/logs/`

## Typical sequencing

1. NHD preprocessing per VPU (optional, if not already prepared)
   - `sbatch merge_rpu_by_vpu.batch`
2. Generate parameters per VPU
   - Elevation: `sbatch create_zonal_elev_params.batch`
   - Slope: `sbatch create_zonal_slope_params.batch`
   - Aspect: `sbatch create_zonal_aspect_params.batch`
   - Soils: `sbatch create_soils_params.batch`
   - Soil Moist Max: `sbatch create_soilmoistmax_params.batch`
   - SSFlux: `sbatch create_ssflux_params.batch`
3. Merge results and fill gaps
   - `sbatch merge_output_params.batch`
4. Merge default NHM parameter tables by nat_hru_id (optional)
   - `sbatch merge_default_output_params.batch`

## Common options

- Most scripts are array jobs over VPUs. To run a single VPU, use the corresponding `*_update.batch` file or edit the `#SBATCH --array` line.
- Two VPU naming schemes exist:
  - Detailed: `01 02 03N 03S 03W 04 05 06 07 08 09 10L 10U 11 12 13 14 15 16 17 18` (21 VPUs, `--array=0-20`)
  - Simple: `01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18` (18 VPUs, `--array=0-17`)
  Match the scheme to the script you're using.

## VPU array mapping

### Detailed scheme (used by create_zonal_*, create_soils_*, create_soilmoistmax_*, create_ssflux_*)
Array range: `--array=0-20` (21 entries)

| Index | VPU | Index | VPU |
|-------|-----|-------|-----|
| 0     | 01  | 11    | 10L |
| 1     | 02  | 12    | 10U |
| 2     | 03N | 13    | 11  |
| 3     | 03S | 14    | 12  |
| 4     | 03W | 15    | 13  |
| 5     | 04  | 16    | 14  |
| 6     | 05  | 17    | 15  |
| 7     | 06  | 18    | 16  |
| 8     | 07  | 19    | 17  |
| 9     | 08  | 20    | 18  |
| 10    | 09  |       |     |

### Simple scheme (used by merge_rpu_by_vpu, compute_slope_aspect)
Array range: `--array=0-17` (18 entries)

| Index | VPU | Index | VPU |
|-------|-----|-------|-----|
| 0     | 01  | 9     | 10  |
| 1     | 02  | 10    | 11  |
| 2     | 03  | 11    | 12  |
| 3     | 04  | 12    | 13  |
| 4     | 05  | 13    | 14  |
| 5     | 06  | 14    | 15  |
| 6     | 07  | 15    | 16  |
| 7     | 08  | 16    | 17  |
| 8     | 09  | 17    | 18  |

## Custom fabric (non-standard VPU list)

Write a config with explicit input/output paths, then run the script without `--vpu`:

```bash
python scripts/create_zonal_params.py --config configs/my_custom_fabric.yml
```

Omitting `--vpu` causes the script to use whatever paths are set directly in the config.

## Script -> Config -> Entry point mapping

| Batch file | Config | Script |
|---|---|---|
| `create_zonal_elev_params.batch` | `configs/elev_param.yml` | `scripts/create_zonal_params.py` |
| `create_zonal_slope_params.batch` | `configs/slope_param.yml` | `scripts/create_zonal_params.py` |
| `create_zonal_aspect_params.batch` | `configs/aspect_param.yml` | `scripts/create_zonal_params.py` |
| `create_soils_params.batch` | `configs/soils_param.yml` | `scripts/create_soils_params.py` |
| `create_soilmoistmax_params.batch` | `configs/soilmoistmax_param.yml` | `scripts/create_soils_params.py` |
| `create_ssflux_params.batch` | `configs/ssflux_param.yml` | `scripts/create_ssflux_params.py` |
| `merge_output_params.batch` | all param configs | `scripts/merge_params.py` + `scripts/merge_and_fill_params.py` |
| `merge_default_output_params.batch` | (CLI args only) | `scripts/merge_default_params.py` |
| `merge_rpu_by_vpu.batch` | `configs/merge_rpu_by_vpu.yml` | `scripts/merge_rpu_by_vpu.py` |
| `compute_slope_aspect.batch` | `configs/slope_aspect.yml` | `scripts/compute_slope_aspect.py` |

## Examples

Run elevation for all VPUs:
```bash
sbatch create_zonal_elev_params.batch
```

Rerun SSFlux just for VPU 14 (long runtime):
```bash
sbatch create_ssflux_params_update.batch
```

Merge all outputs and fill missing nat_hru_id:
```bash
sbatch merge_output_params.batch
```

Merge NHM default params to nat_hru_id:
```bash
sbatch merge_default_output_params.batch
```

Combined slope+aspect (alternative to separate slope/aspect jobs):
```bash
sbatch compute_slope_aspect.batch
```

If you run `compute_slope_aspect.batch`, you typically do not need `create_zonal_slope_params.batch` and `create_zonal_aspect_params.batch`.

## Tips

- Memory is large (128-256G). If OOM occurs, raise `--mem` or reduce dataset scope.
- Keep an eye on `logs/job_<JOBID>_<ARRAY_INDEX>.{out,err}` for failures.
- Elevation, slope, and aspect use the same driver (`scripts/create_zonal_params.py`) with different configs. Pass the right `--config`.
- `merge_output_params.batch` runs multiple `srun --exclusive` jobs in parallel; ensure `--ntasks` matches the number of backgrounded `srun` lines.

## Monitoring & troubleshooting

Check queue and running jobs:
```bash
squeue -u "$USER"
```

Inspect recent logs:
```bash
tail -n 200 logs/job_*.out
tail -n 200 logs/job_*.err
```

See job/accounting info (if enabled):
```bash
sacct -j <JOBID> -o JobID,State,Elapsed,MaxRSS,ReqMem,AllocCPUS
```

Cancel or requeue:
```bash
scancel <JOBID>
scontrol requeue <JOBID>
```

## Expected outputs (by stage)

- elevation: per-VPU elevation-derived parameter files (see `configs/elev_param.yml` for exact output paths); consumed later by merge step.
- slope: per-VPU slope parameter outputs; or use combined step below.
- aspect: per-VPU aspect outputs; may be produced by the combined slope/aspect step instead.
- `compute_slope_aspect.batch`: produces both slope and aspect per VPU via `compute_slope_aspect.py` and `configs/slope_aspect.yml`.
- soils: per-VPU soils parameter outputs.
- soilmoistmax: per-VPU soil moisture capacity outputs.
- ssflux: per-VPU subsurface flux parameter outputs.
- merge+fill: merged cross-VPU parameter tables with `nat_hru_id`, with nearest-neighbor fill for missing ssflux IDs.
- merge defaults: NHM default parameter tables rekeyed to `nat_hru_id` into `nhm_params/nhm_default_params_merged/`.

Actual filenames and directories are controlled by the YAML configs under `gfv2-params/configs/`.
