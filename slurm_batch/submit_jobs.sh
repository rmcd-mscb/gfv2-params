#!/bin/bash
# Usage: ./submit_jobs.sh <batches_dir> <batch_script> [base_config] [fabric] [max_concurrent]
#
# Reads the batch count from manifest.yml and submits the batch script
# as a SLURM array job with the appropriate range.
# The optional third argument sets BASE_CONFIG (default: configs/base_config.yml).
# The optional fourth argument sets FABRIC (default: gfv2). FABRIC propagates
# to the batch via --export=ALL and selects the active fabric profile.
# The optional fifth argument caps simultaneously-running tasks via SLURM's
# `--array=0-N%K` throttle, or env var SUBMIT_JOBS_MAX_CONCURRENT (default: 4).
# This guards against a pixi/GDAL/PROJ import-storm hang when many tasks slam
# the shared FS at startup — observed on VPU01 issue-#61 run, one of eight
# tasks deadlocked in library init with zero open data files. Set to 0 or
# "off" to disable the cap and run fully concurrent.
#
# For the chained-merge workflows (zonal + depstor params), use the
# dedicated wrappers instead:
#   slurm_batch/submit_zonal_params.sh   (loops every zonal param, chains
#                                         array -> merge per param)
#   slurm_batch/submit_depstor_params.sh (loops every depstor fraction,
#                                         chains array -> merge -> ratios)

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <batches_dir> <batch_script> [base_config.yml] [fabric] [max_concurrent]"
    echo "  batches_dir:    path to {fabric}/batches/ (contains manifest.yml)"
    echo "  batch_script:   SLURM batch file to submit"
    echo "  base_config:    optional path to base_config.yml (default: configs/base_config.yml)"
    echo "  fabric:         optional fabric name (default: gfv2)"
    echo "  max_concurrent: optional concurrency cap (default: 4; 0/off disables)"
    exit 1
fi

FABRIC_DIR="$1"
BATCH_SCRIPT="$2"
BASE_CONFIG="${3:-configs/base_config.yml}"
FABRIC="${4:-gfv2}"
MAX_CONCURRENT="${5:-${SUBMIT_JOBS_MAX_CONCURRENT:-4}}"
MANIFEST="$FABRIC_DIR/manifest.yml"

if [ ! -f "$MANIFEST" ]; then
    echo "Error: manifest not found: $MANIFEST"
    echo "Run scripts/prepare_fabric.py first."
    exit 1
fi

N_BATCHES=$(grep '^n_batches:' "$MANIFEST" | awk '{print $2}')
if [ -z "$N_BATCHES" ] || [ "$N_BATCHES" -le 0 ] 2>/dev/null; then
    echo "Error: could not parse n_batches from $MANIFEST (got: '$N_BATCHES')"
    echo "Expected format: 'n_batches: <positive_integer>'"
    exit 1
fi
LAST_IDX=$((N_BATCHES - 1))

# Build the array spec — append %K throttle unless explicitly disabled.
case "$MAX_CONCURRENT" in
    0|off|OFF|none|NONE|"")
        ARRAY_SPEC="0-$LAST_IDX"
        THROTTLE_NOTE="no concurrency cap"
        ;;
    *)
        ARRAY_SPEC="0-$LAST_IDX%$MAX_CONCURRENT"
        THROTTLE_NOTE="max $MAX_CONCURRENT concurrent"
        ;;
esac

echo "Submitting $BATCH_SCRIPT with --array=$ARRAY_SPEC ($N_BATCHES batches, $THROTTLE_NOTE), FABRIC=$FABRIC"
ARRAY_JOB_ID=$(sbatch --array="$ARRAY_SPEC" \
                     --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC" \
                     "$BATCH_SCRIPT" | awk '{print $NF}')
echo "Array job ID: $ARRAY_JOB_ID"
echo ""
echo "To chain a merge after the array completes, submit the matching merge"
echo "worker. Note the env-var name differs between zonal and depstor:"
echo "  # zonal param:"
echo "  sbatch --dependency=afterok:$ARRAY_JOB_ID --export=ALL,FABRIC=$FABRIC,PARAM=<name> \\"
echo "         slurm_batch/merge_zonal_param.batch"
echo "  # depstor fraction:"
echo "  sbatch --dependency=afterok:$ARRAY_JOB_ID --export=ALL,FABRIC=$FABRIC,FRACTION=<name> \\"
echo "         slurm_batch/merge_depstor_fraction.batch"
echo "Or use slurm_batch/submit_zonal_params.sh / submit_depstor_params.sh"
echo "for the chained per-parameter workflow. See RUNME.md Stage 4A."
