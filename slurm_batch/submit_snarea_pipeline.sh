#!/bin/bash
# Usage: ./submit_snarea_pipeline.sh <fabric> [base_config] [extra sbatch opts...]
#
# One-command recipe for the full 3-stage SNODAS -> snarea_curve pipeline.
# Submits four SLURM jobs chained --dependency=afterok, so each waits for the
# previous to succeed, and prints the job IDs:
#
#   1. Stage 1 aggregate  (derive_snodas_aggregate.batch, ARRAY over spatial
#      batches sized from the fabric manifest: oregon N=2, gfv2 N=64)
#   2. merge              (merge_snodas_aggregate.batch, --mode merge)
#   3. Stage 2 derive     (derive_snarea_curve.batch; --mem parameterized —
#      384G CONUS default, smaller for oregon)
#   4. Stage 3 library    (derive_snarea_library.batch, CV/lognormal library)
#
# Because every job is afterok on the prior one, a failed Stage-1 array task
# aborts the whole chain (merge/derive/library stay PENDING then cancel) — a
# partial Stage-1 run never silently mixes old/new per-batch NetCDFs into the
# merge. Monitor with `squeue -u $USER` / `sacct -j <id>`.
#
# WHY STAGE 1 MUST RE-RUN (do not skip it): Stage 1 now emits a per-HRU
# `swe_std` sidecar (sub-grid SWE std feeding Stage 3's CV). The on-disk
# aggregated NetCDFs for both fabrics predate it, so Stage 2 raises
# ValueError("...missing swe_std... Re-run Stage 1...") until Stage 1 is
# re-run. The gdptools weights are cached under {data_root}/{fabric}/
# weights_agg/ and reused (geometry unchanged), so the re-run is a cheap extra
# masked_std AggGen pass — it does NOT recompute weights. The aggregate driver
# overwrites per-batch NetCDFs, so a re-run is clean; export CLEAR_BATCHES=1 to
# wipe {data_root}/{fabric}/snodas/_batches/ first if you want to be extra safe.
#
# Env overrides:
#   STAGE2_MEM   Stage 2 --mem   (default: 64G for oregon, 384G otherwise)
#   STAGE2_TIME  Stage 2 --time  (default: 02:00:00 for oregon, batch default otherwise)
#   MAX_CONCURRENT  Stage-1 array %K throttle (default: 8; 0/off disables) —
#                   guards the pixi/GDAL/PROJ import storm on the shared FS.
#   CLEAR_BATCHES=1  rm the {fabric}/snodas/_batches/ dir before submitting.
#   DRYRUN=1     echo the sbatch commands instead of submitting.
#
# Any trailing args after <fabric> [base_config] are forwarded verbatim to
# EVERY sbatch (e.g. a global --time or --qos). They apply after the script's
# own flags, so they override them.

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <fabric> [base_config] [extra sbatch opts...]"
    echo "  fabric:      fabric name (e.g. oregon, gfv2)"
    echo "  base_config: optional path to base_config.yml (default: configs/base_config.yml)"
    exit 1
fi

FABRIC="$1"; shift
BASE_CONFIG="configs/base_config.yml"
if [ $# -gt 0 ] && [[ "$1" != -* ]]; then
    BASE_CONFIG="$1"; shift
fi
EXTRA_OPTS=("$@")   # forwarded to every sbatch

# Resolve data_root the same grep-parse way the batches/manifest is parsed.
DATA_ROOT=$(grep '^data_root:' "$BASE_CONFIG" | awk '{print $2}')
if [ -z "$DATA_ROOT" ]; then
    echo "Error: could not parse data_root from $BASE_CONFIG"
    exit 1
fi
MANIFEST="$DATA_ROOT/$FABRIC/batches/manifest.yml"
if [ ! -f "$MANIFEST" ]; then
    echo "Error: manifest not found: $MANIFEST"
    echo "Run scripts/prepare_fabric.py for '$FABRIC' first."
    exit 1
fi

N_BATCHES=$(grep '^n_batches:' "$MANIFEST" | awk '{print $2}')
if [ -z "$N_BATCHES" ] || [ "$N_BATCHES" -le 0 ] 2>/dev/null; then
    echo "Error: could not parse n_batches from $MANIFEST (got: '$N_BATCHES')"
    exit 1
fi
LAST_IDX=$((N_BATCHES - 1))

# Stage-1 array throttle (see submit_jobs.sh for the import-storm rationale).
MAX_CONCURRENT="${MAX_CONCURRENT:-8}"
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

# Stage-2 memory/time: sized for CONUS by default, overridable down for small
# fabrics. Oregon (~17k HRUs) fits in far less than the 384G CONUS default.
if [ "$FABRIC" = "oregon" ]; then
    STAGE2_MEM="${STAGE2_MEM:-64G}"
    STAGE2_TIME="${STAGE2_TIME:-02:00:00}"
else
    STAGE2_MEM="${STAGE2_MEM:-384G}"
    STAGE2_TIME="${STAGE2_TIME:-}"   # empty => use the batch's #SBATCH --time
fi
STAGE2_TIME_ARG=()
[ -n "$STAGE2_TIME" ] && STAGE2_TIME_ARG=(--time="$STAGE2_TIME")

EXPORT="ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC"

echo "SNODAS -> snarea_curve pipeline for FABRIC=$FABRIC"
echo "  data_root:   $DATA_ROOT"
echo "  n_batches:   $N_BATCHES  (Stage-1 array 0-$LAST_IDX, $THROTTLE_NOTE)"
echo "  Stage 2 mem: $STAGE2_MEM  time: ${STAGE2_TIME:-<batch default>}"
[ "${#EXTRA_OPTS[@]}" -gt 0 ] && echo "  extra sbatch: ${EXTRA_OPTS[*]}"

# submit <flag...> -- <batch-script> : wraps sbatch --parsable, honouring DRYRUN.
submit() {
    local args=() batch
    while [ "$1" != "--" ]; do args+=("$1"); shift; done
    shift
    batch="$1"
    if [ -n "${DRYRUN:-}" ]; then
        echo "  DRYRUN sbatch --parsable ${args[*]} ${EXTRA_OPTS[*]} $batch" >&2
        echo "DRYRUN"   # stand-in job id
        return 0
    fi
    sbatch --parsable "${args[@]}" "${EXTRA_OPTS[@]}" "$batch"
}

if [ -n "${CLEAR_BATCHES:-}" ] && [ -z "${DRYRUN:-}" ]; then
    echo "CLEAR_BATCHES=1: removing $DATA_ROOT/$FABRIC/snodas/_batches/"
    rm -rf "$DATA_ROOT/$FABRIC/snodas/_batches"
fi

echo "--- Stage 1: aggregate (array) ---"
AID=$(submit --array="$ARRAY_SPEC" --export="$EXPORT" -- slurm_batch/derive_snodas_aggregate.batch)
echo "  aggregate array: $AID"

echo "--- merge ---"
MID=$(submit --dependency=afterok:"$AID" --export="$EXPORT" -- slurm_batch/merge_snodas_aggregate.batch)
echo "  merge afterok:$AID -> $MID"

echo "--- Stage 2: derive snarea_curve ---"
S2=$(submit --dependency=afterok:"$MID" --mem="$STAGE2_MEM" "${STAGE2_TIME_ARG[@]}" --export="$EXPORT" \
     -- slurm_batch/derive_snarea_curve.batch)
echo "  derive afterok:$MID -> $S2"

echo "--- Stage 3: snarea_curve library ---"
S3=$(submit --dependency=afterok:"$S2" --export="$EXPORT" -- slurm_batch/derive_snarea_library.batch)
echo "  library afterok:$S2 -> $S3"

echo ""
echo "Done. Chain: $AID (agg) -> $MID (merge) -> $S2 (derive) -> $S3 (library)"
echo "Monitor: squeue -u \$USER   |   sacct -j $AID,$MID,$S2,$S3"
