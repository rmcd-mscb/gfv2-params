#!/bin/bash
# Usage: ./submit_depstor_params.sh <batches_dir> [fabric] [base_config] [max_concurrent]
#
# For each of the 9 depstor fractions, submits:
#   1. an array zonal job over every HRU batch (max_concurrent throttled), and
#   2. a chained merge job (afterok on the array).
#
# After all 9 merges, a single ratios job runs (afterok on every merge) to
# derive the 4 PRMS Level-5 params (sro_to_dprst_perv, sro_to_dprst_imperv,
# carea_max, smidx_coef).
#
# The 9 fractions are the canonical list from configs/depstor_params.yml; if
# you add or remove fractions there, update FRACTIONS below.

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <batches_dir> [fabric] [base_config] [max_concurrent]"
    echo "  batches_dir:    path to {fabric}/batches/ (contains manifest.yml)"
    echo "  fabric:         optional fabric name (default: gfv2)"
    echo "  base_config:    optional path to base_config.yml (default: configs/base_config.yml)"
    echo "  max_concurrent: optional concurrency cap (default: 4; 0/off disables)"
    exit 1
fi

FABRIC_DIR="$1"
FABRIC="${2:-gfv2}"
BASE_CONFIG="${3:-configs/base_config.yml}"
MAX_CONCURRENT="${4:-${SUBMIT_JOBS_MAX_CONCURRENT:-4}}"
MANIFEST="$FABRIC_DIR/manifest.yml"

if [ ! -f "$MANIFEST" ]; then
    echo "Error: manifest not found: $MANIFEST"
    echo "Run scripts/prepare_fabric.py first."
    exit 1
fi

N_BATCHES=$(grep '^n_batches:' "$MANIFEST" | awk '{print $2}')
if [ -z "$N_BATCHES" ] || [ "$N_BATCHES" -le 0 ] 2>/dev/null; then
    echo "Error: could not parse n_batches from $MANIFEST (got: '$N_BATCHES')"
    exit 1
fi
LAST_IDX=$((N_BATCHES - 1))

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

FRACTIONS=(
    perv_frac
    imperv_frac
    dprst_frac
    drains_perv_frac
    drains_imperv_frac
    onstream_storage_frac
    drains_to_dprst_frac
    carea_t8_frac
    carea_t156_frac
)

echo "Submitting ${#FRACTIONS[@]} depstor fractions x $N_BATCHES batches each ($THROTTLE_NOTE), FABRIC=$FABRIC"

MERGE_JOB_IDS=()
for FRACTION in "${FRACTIONS[@]}"; do
    echo "--- $FRACTION ---"

    ARRAY_JOB_ID=$(sbatch --array="$ARRAY_SPEC" \
                         --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC",FRACTION="$FRACTION" \
                         slurm_batch/create_depstor_zonal.batch | awk '{print $NF}')
    echo "  zonal  array: $ARRAY_JOB_ID"

    MERGE_JOB_ID=$(sbatch --dependency=afterok:"$ARRAY_JOB_ID" \
                         --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC",FRACTION="$FRACTION" \
                         slurm_batch/merge_depstor_fraction.batch | awk '{print $NF}')
    echo "  merge afterok:$ARRAY_JOB_ID -> $MERGE_JOB_ID"
    MERGE_JOB_IDS+=("$MERGE_JOB_ID")
done

DEPENDS=$(IFS=:; echo "${MERGE_JOB_IDS[*]}")
echo "Submitting ratios job (afterok:$DEPENDS)"
RATIOS_JOB_ID=$(sbatch --dependency=afterok:"$DEPENDS" \
                     --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC" \
                     slurm_batch/derive_depstor_ratios.batch | awk '{print $NF}')
echo "  ratios afterok:$DEPENDS -> $RATIOS_JOB_ID"

echo "Done. Final ratios job ID: $RATIOS_JOB_ID"
