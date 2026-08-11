#!/bin/bash
# Usage: ./submit_zonal_params.sh <batches_dir> [fabric] [base_config] [max_concurrent]
#
# For each Part 2 param in configs/zonal/zonal_params.yml, submits:
#   1. an array zonal job over every HRU batch (max_concurrent throttled), and
#   2. a chained merge job (afterok on the array) writing to
#      {fabric}/params/merged/.
#
# When an entry in configs/zonal/zonal_params.yml carries `depends_on: build_weights`
# (typically `ssflux`), a build_zonal_weights.batch job is submitted FIRST and
# both the array zonal AND the merge for that entry are chained --dependency=
# afterok on the weights job. The CONUS-wide weight matrix is built once per
# fabric (idempotent), so a second invocation skips re-computation unless
# FORCE=1 is exported.
#
# The 10 params are listed in configs/zonal/zonal_params.yml — if you add or remove
# entries there, also update PARAMS below.
#
# This is the "run wholesale" path. To run the same work one parameter at a
# time (submit + inspect each in turn), see slurm_batch/RUNME.md Stage 4A —
# this wrapper just loops those per-parameter array + merge steps.

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
# {data_root}/{fabric} -- the batches dir is documented as {fabric}/batches/, and
# dirname strips any trailing slash. Used by the Step B on-disk prereq check.
FABRIC_ROOT="$(dirname "$FABRIC_DIR")"

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

# Param list — must match the `name:` entries in configs/zonal/zonal_params.yml.
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

# Optional override: export ZONAL_PARAMS="elevation slope ..." to run only a
# subset of the params above (space-separated, must be `name:` entries in
# configs/zonal/zonal_params.yml). Use when some sources are unstaged — e.g.
# lulc_nlcd/lulc_foresce on fabrics without those CONUS rasters. Keep slope
# before ssflux in the list, since ssflux reads the merged slope CSV.
if [ -n "${ZONAL_PARAMS:-}" ]; then
    read -ra PARAMS <<< "$ZONAL_PARAMS"
    echo "ZONAL_PARAMS override: running ${#PARAMS[@]} params -> ${PARAMS[*]}"
fi

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
    #
    # Two ways the prereq can be satisfied, and BOTH are legitimate:
    #   1. $UP is in THIS submission -> chain on its merge job (the common case);
    #   2. $UP is not in this run, but its merged CSV is already on disk and is
    #      not older than the per-batch CSVs it was built from.
    #
    # Case 2 used to be rejected outright, which meant that re-running ssflux
    # alone after a completed slope rebuild required re-running all 64 slope
    # batches purely to re-satisfy a job-ordering check. The guard's job is to
    # stop $PARAM reading a MISSING or STALE merge, not to insist the merge
    # happen inside this particular invocation.
    if [ -n "${NEEDS_MERGE_OF[$PARAM]:-}" ]; then
        UP="${NEEDS_MERGE_OF[$PARAM]}"
        UP_MERGE_ID="${MERGE_JOB_BY_PARAM[$UP]:-}"
        if [ -n "$UP_MERGE_ID" ]; then
            EXTRA_DEPS+=("afterok:$UP_MERGE_ID")
        else
            UP_MERGED="$FABRIC_ROOT/params/merged/nhm_${UP}_params.csv"
            UP_BATCH_DIR="$FABRIC_ROOT/params/$UP"
            if [ ! -f "$UP_MERGED" ]; then
                echo "ERROR: $PARAM needs merged $UP, which is not in this run and" >&2
                echo "       does not exist on disk: $UP_MERGED" >&2
                echo "       Either add $UP before $PARAM in PARAMS/ZONAL_PARAMS, or" >&2
                echo "       build it first." >&2
                exit 1
            fi
            # A per-batch CSV newer than the merged file means the merge predates
            # the zonal pass that produced its inputs -- $PARAM would read a stale
            # merge, silently. Cheaper and more honest than comparing timestamps
            # by hand: ask find whether any input is newer than the output.
            if [ -d "$UP_BATCH_DIR" ] && \
               [ -n "$(find "$UP_BATCH_DIR" -maxdepth 1 -name '*.csv' -newer "$UP_MERGED" -print -quit 2>/dev/null)" ]; then
                echo "ERROR: $PARAM needs merged $UP, which is STALE: at least one" >&2
                echo "       per-batch CSV in $UP_BATCH_DIR is newer than" >&2
                echo "       $UP_MERGED" >&2
                echo "       Re-merge it first:" >&2
                echo "         pixi run --as-is python scripts/derive_zonal_params.py \\" >&2
                echo "           --mode merge --param $UP --fabric $FABRIC" >&2
                exit 1
            fi
            echo "  $UP: using merged CSV already on disk (not in this run)"
        fi
    fi

    # Combine deps into a single --dependency arg (or empty).
    DEP_ARG=""
    if [ "${#EXTRA_DEPS[@]}" -gt 0 ]; then
        DEP_STR=$(IFS=,; echo "${EXTRA_DEPS[*]}")
        DEP_ARG="--dependency=$DEP_STR"
    fi

    # Step C: array zonal job.
    # shellcheck disable=SC2086  # $DEP_ARG is deliberately unquoted: it is either
    # empty or a whole `--dependency=...` argument, and quoting it would pass an
    # EMPTY string as an argument to sbatch whenever this param has no upstream
    # dependency. The word-splitting is the mechanism, not an oversight.
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
