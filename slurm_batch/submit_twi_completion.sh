#!/usr/bin/env bash
# Finish twi.vrt (ArcPy Twi_merged for VPUs 02-18 were never merged), build both
# TWI VRTs (twi.vrt + twi_hydrodem.vrt), and compute the TWI reference percentiles
# — the three shared-raster steps the percentile carea_map pipeline depends on.
# Inputs are all staged: per-RPU TWI at input/twi/<rpu>/twi.tif (59 RPUs) and
# per-VPU land masks for all 18. Pure on-cluster; no ArcPy.
#
# Submits three afterok-chained jobs (merge -> build_vrt -> twi_reference) with
# the cluster's required directives (-p cpu -A impd + time/mem), mirroring
# slurm_batch/build_shared_rasters.batch. Run from a shell with ~/.pixi/bin on
# PATH (sbatch inherits it):
#
#   bash slurm_batch/submit_twi_completion.sh
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs
CFG=configs/shared_rasters/shared_rasters.yml
# The TWI manifest (merge_rpu_by_vpu_twi.yml) is keyed by RASTER VPUs (01..18),
# but shared_rasters.yml's `vpus:` is the DETAILED list (03N/03S/03W, 10L/10U).
# Iterating the detailed list makes the merge skip VPU 03 and 10 ("not in
# manifest"), leaving twi.vrt incomplete. Drive the merge with the raster VPUs.
RASTER_VPUS=01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18
# Common directives every job needs (partition/account/logs); the QOS rejects
# jobs that omit partition/account/time/mem.
SB=(sbatch --parsable -p cpu -A impd --ntasks=1
    --output=logs/job_%j.out --error=logs/job_%j.err)

# 1) merge ArcPy per-RPU TWI -> per-VPU Twi_merged for every raster VPU in the
#    manifest. --force is REQUIRED here: the builder's skip-if-output-exists is
#    idempotent on file existence, but VPUs 02..18 already exist as empty ~stubs
#    from the #94 gap, so a non-forced run would skip them and never fill the
#    gap. --force overwrites the stubs. (To resume after a partial failure, scope
#    it: --vpus 10,11,...,18 --force leaves the already-good 01..09 untouched.)
#    384G: VPU 10 (largest, 9 RPUs) holds several full-extent float32 copies in
#    the merge+mask+write path and OOMs at 96G; the cpu partition has 503G nodes.
merge=$("${SB[@]}" --job-name=twi_merge --time=12:00:00 --cpus-per-task=8 --mem=384G \
  --wrap="pixi run --as-is python scripts/build_shared_rasters.py \
          --config $CFG --step merge_rpu_by_vpu_twi --vpus $RASTER_VPUS --force")
echo "merge_rpu_by_vpu_twi: $merge"

# 2) (re)build CONUS VRTs after the merge — builds twi.vrt AND twi_hydrodem.vrt.
vrt=$("${SB[@]}" --dependency=afterok:"$merge" --job-name=twi_vrt \
  --time=02:00:00 --cpus-per-task=8 --mem=48G \
  --wrap="pixi run --as-is python scripts/build_shared_rasters.py \
          --config $CFG --step build_vrt --force")
echo "build_vrt: $vrt"

# 3) valid-land TWI percentile cutoffs per VPU + CONUS, both sources.
ref=$("${SB[@]}" --dependency=afterok:"$vrt" --job-name=twi_ref \
  --time=04:00:00 --cpus-per-task=8 --mem=64G \
  --wrap="pixi run --as-is python scripts/build_shared_rasters.py \
          --config $CFG --step twi_reference --force")
echo "twi_reference: $ref"

echo "Chained merge($merge) -> vrt($vrt) -> twi_reference($ref). Watch: squeue -u \$USER"
