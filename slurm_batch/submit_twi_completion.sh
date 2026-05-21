#!/usr/bin/env bash
# Finish twi.vrt (ArcPy Twi_merged for VPUs 02-18 were never merged) and build
# both TWI VRTs (twi.vrt + twi_hydrodem.vrt). Inputs are all staged: per-RPU
# TWI at input/twi/<rpu>/twi.tif (59 RPUs) and per-VPU land masks for all 18.
# Pure on-cluster; no ArcPy. Run from a shell with ~/.pixi/bin on PATH.
#
#   bash slurm_batch/submit_twi_completion.sh
set -euo pipefail
cd "$(dirname "$0")/.."
CFG=configs/shared_rasters/shared_rasters.yml

# 1) merge ArcPy per-RPU TWI -> per-VPU Twi_merged for every VPU (idempotent;
#    --force re-merges 01 too so all 18 are consistent).
merge=$(sbatch --parsable --job-name=twi_merge \
  --wrap="pixi run --as-is python scripts/build_shared_rasters.py \
          --config $CFG --step merge_rpu_by_vpu_twi --force")
echo "merge_rpu_by_vpu_twi: $merge"

# 2) (re)build CONUS VRTs after the merge — builds twi.vrt AND twi_hydrodem.vrt.
vrt=$(sbatch --parsable --dependency=afterok:$merge --job-name=twi_vrt \
  --wrap="pixi run --as-is python scripts/build_shared_rasters.py \
          --config $CFG --step build_vrt --force")
echo "build_vrt: $vrt"
echo "Submitted. When done, verify with: scripts/build_shared_rasters.py logs + gdalinfo on both VRTs."
