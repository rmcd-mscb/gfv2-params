#!/bin/bash
# Usage: ./submit_zonal_params.sh <batches_dir> [fabric] [base_config] [max_concurrent]
#
# For each Part 2 param in configs/zonal_params.yml, submits:
#   1. an array zonal job over every HRU batch (max_concurrent throttled), and
#   2. a chained merge job (afterok on the array) writing to
#      {fabric}/params/merged/.
#
# When an entry in configs/zonal_params.yml carries `depends_on: build_weights`
# (typically `ssflux`), a build_zonal_weights.batch job is submitted FIRST and
# both the array zonal AND the merge for that entry are chained --dependency=
# afterok on the weights job. The CONUS-wide weight matrix is built once per
# fabric (idempotent), so a second invocation skips re-computation unless
# FORCE=1 is exported.
#
# The 10 params are listed in configs/zonal_params.yml — if you add or remove
# entries there, also update PARAMS below.

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

# Param list — must match the `name:` entries in configs/zonal_params.yml.
# Entries marked with build_weights_dep get the ssflux-style prereq chain.
# Keep in dependency order: slope must merge before ssflux can run, because
# ssflux reads the merged slope CSV at zonal time.
PARAMS=(
    elevation
    slope
    aspect
    soils
    soil_moist_max
    lulc_nhm_v11
    lulc_nalcms
    lulc_nlcd
    lulc_foresce
    ssflux                  # depends_on: build_weights (resolved per-entry below)
)

# Entries that need the CONUS weight matrix as a prereq. The submit loop
# special-cases these to submit build_zonal_weights.batch first + chain
# the array zonal + merge on its afterok.
declare -A NEEDS_WEIGHTS=(
    [ssflux]=1
)

# Entries that need a specific upstream merge to land first (because the
# per-batch zonal step reads a merged CSV). ssflux reads the merged slope
# CSV at zonal time.
declare -A NEEDS_MERGE_OF=(
    [ssflux]=slope
)

echo "Submitting ${#PARAMS[@]} Part 2 params x $N_BATCHES batches each ($THROTTLE_NOTE), FABRIC=$FABRIC"

WEIGHTS_JOB_ID=""

# Map of PARAM -> merge job ID, so downstream params (like ssflux) can chain
# on the right merge_id.
declare -A MERGE_JOB_BY_PARAM

for PARAM in "${PARAMS[@]}"; do
    echo "--- $PARAM ---"

    EXTRA_DEPS=()

    # Step A: build_weights prereq (one-shot per submit run).
    if [ -n "${NEEDS_WEIGHTS[$PARAM]:-}" ]; then
        if [ -z "$WEIGHTS_JOB_ID" ]; then
            WEIGHTS_JOB_ID=$(sbatch \
                --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC" \
                slurm_batch/build_zonal_weights.batch | awk '{print $NF}')
            echo "  weights: $WEIGHTS_JOB_ID"
        else
            echo "  weights: $WEIGHTS_JOB_ID (reused)"
        fi
        EXTRA_DEPS+=("afterok:$WEIGHTS_JOB_ID")
    fi

    # Step B: upstream-merge prereq (e.g., ssflux needs merged slope).
    if [ -n "${NEEDS_MERGE_OF[$PARAM]:-}" ]; then
        UP="${NEEDS_MERGE_OF[$PARAM]}"
        UP_MERGE_ID="${MERGE_JOB_BY_PARAM[$UP]:-}"
        if [ -z "$UP_MERGE_ID" ]; then
            echo "ERROR: $PARAM needs merged $UP but no merge job submitted for $UP yet." >&2
            echo "       Ensure $UP appears before $PARAM in PARAMS." >&2
            exit 1
        fi
        EXTRA_DEPS+=("afterok:$UP_MERGE_ID")
    fi

    # Combine deps into a single --dependency arg (or empty).
    DEP_ARG=""
    if [ "${#EXTRA_DEPS[@]}" -gt 0 ]; then
        DEP_STR=$(IFS=,; echo "${EXTRA_DEPS[*]}")
        DEP_ARG="--dependency=$DEP_STR"
    fi

    # Step C: array zonal job.
    ARRAY_JOB_ID=$(sbatch --array="$ARRAY_SPEC" \
                         $DEP_ARG \
                         --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC",PARAM="$PARAM" \
                         slurm_batch/derive_zonal_params.batch | awk '{print $NF}')
    echo "  zonal  array: $ARRAY_JOB_ID${DEP_ARG:+ ($DEP_ARG)}"

    # Step D: merge job, afterok the array.
    MERGE_JOB_ID=$(sbatch --dependency=afterok:"$ARRAY_JOB_ID" \
                         --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC",PARAM="$PARAM" \
                         slurm_batch/merge_zonal_param.batch | awk '{print $NF}')
    echo "  merge afterok:$ARRAY_JOB_ID -> $MERGE_JOB_ID"
    MERGE_JOB_BY_PARAM[$PARAM]="$MERGE_JOB_ID"
done

echo "Done. Submitted ${#PARAMS[@]} params; last merge job ID: ${MERGE_JOB_ID}"
