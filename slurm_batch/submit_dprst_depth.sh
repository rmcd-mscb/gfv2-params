#!/bin/bash
# Usage: ./submit_dprst_depth.sh <batches_dir> [fabric] [base_config] [n_tile_batches] [hru_max_concurrent]
#
# Drives the full dprst_depth_avg pipeline (issue #173) as a single afterok
# DAG, 4 stages:
#   1. PLAN         (1 task)                        tiling.group_by_tile / component_tile_batches
#   2. ARRAY        (0..n_tile_batches-1, afterok:1) compute.run_batch per tile-batch
#   3. BUILD        (1 task, afterok:2)              depstor_builders.dprst_depth fill+burn+op_flow_thres
#   4a. MEAN_ZONAL  (0..n_hru_batches-1, afterok:3)  per-HRU exactextract mean of dprst_depth.tif
#   4b. MEAN_FINALIZE (1 task, afterok:4a)           m->in + floor + provenance -> nhm_dprst_depth_avg_params.csv
#
# Stage 4a/4b (`--mode mean_zonal`/`--mode mean_finalize`, Task 8) were left
# UNCHAINED when Task 8 landed; this script is what actually wires them into
# a runnable job, in the dprst_depth-specific submit script rather than
# submit_depstor_params.sh -- a `means:` aggregation is a structurally
# different shape from a `fractions:`/`ratios:` one (no numerator/denominator,
# no {fabric}/batches/*.gpkg re-derivation), and its whole source-raster ->
# final-CSV chain (stages 1-4b here) already lives together in this one
# script, so keeping the mean_zonal/mean_finalize chain here too avoids
# splitting one param's pipeline across two submit scripts.
#
# --- Compute-budget sizing (issue #173's <=5 hr target, stages 1-3) --------
# CONUS has ~286k dprst polygons; reading a windowed DEM per polygon serially
# costs ~250-500 core-hours (Task 3/9 design doc). The fan-out unit is the
# elevation TILE, not the polygon: tiling.component_tile_batches bins the
# tile -> polygon work-list into N_TILE_BATCHES roughly-equal-polygon-load
# SLURM array tasks (greedy LPT bin-packing, connected-component-safe so a
# polygon spanning >1 tile is never split across batches). With
# N_TILE_BATCHES array tasks running CONCURRENTLY:
#
#     wall-clock (stage 2) ~= (250-500 core-hours) / N_TILE_BATCHES
#
#       N_TILE_BATCHES=100 -> 2.5 - 5.0 hr
#       N_TILE_BATCHES=150 -> 1.7 - 3.3 hr   (default; margin under 5 hr even
#                                              with imperfect LPT balance or
#                                              read-time variance)
#       N_TILE_BATCHES=200 -> 1.25 - 2.5 hr
#
# This ONLY holds if stage 2 actually runs with high concurrency -- it is
# deliberately NOT throttled with a max_concurrent cap the way stages 4a/HRU
# batches (and submit_depstor_params.sh / submit_zonal_params.sh's arrays)
# are; that cap exists for exactextract/memory reasons specific to the
# HRU-batch arrays, not for this tile-batch one. Before submitting, dry-run
# the exact batching for the target N_TILE_BATCHES (pure geometry, no live
# S3, safe to run on the head node for a small fabric):
#
#   pixi run python -m gfv2_params.dprst_depth.tiling --plan \
#       --fabric <fabric> --n-batches <N_TILE_BATCHES>
#
# It prints the per-batch polygon-load balance and the same core-hour ->
# wall-clock projection as above, flagging if the projection exceeds 5 hr.
# -----------------------------------------------------------------------------

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <batches_dir> [fabric] [base_config] [n_tile_batches] [hru_max_concurrent]"
    echo "  batches_dir:         path to {fabric}/batches/ (HRU batch manifest; drives stage 4a's mean_zonal array)"
    echo "  fabric:              optional fabric name (default: gfv2)"
    echo "  base_config:         optional path to base_config.yml (default: configs/base_config.yml)"
    echo "  n_tile_batches:      optional stage 1/2 SLURM array size (default: 150; see sizing note above)"
    echo "  hru_max_concurrent:  optional concurrency cap for stage 4a's mean_zonal array (default: 4; 0/off disables)"
    exit 1
fi

FABRIC_DIR="$1"
FABRIC="${2:-gfv2}"
BASE_CONFIG="${3:-configs/base_config.yml}"
N_TILE_BATCHES="${4:-150}"
HRU_MAX_CONCURRENT="${5:-${SUBMIT_JOBS_MAX_CONCURRENT:-4}}"
MANIFEST="$FABRIC_DIR/manifest.yml"

if [ ! -f "$MANIFEST" ]; then
    echo "Error: manifest not found: $MANIFEST"
    echo "Run scripts/prepare_fabric.py first."
    exit 1
fi

N_HRU_BATCHES=$(grep '^n_batches:' "$MANIFEST" | awk '{print $2}')
if [ -z "$N_HRU_BATCHES" ] || [ "$N_HRU_BATCHES" -le 0 ] 2>/dev/null; then
    echo "Error: could not parse n_batches from $MANIFEST (got: '$N_HRU_BATCHES')"
    exit 1
fi
if [ "$N_TILE_BATCHES" -le 0 ] 2>/dev/null; then
    echo "Error: n_tile_batches must be positive (got: '$N_TILE_BATCHES')"
    exit 1
fi
LAST_HRU_IDX=$((N_HRU_BATCHES - 1))
LAST_TILE_IDX=$((N_TILE_BATCHES - 1))

case "$HRU_MAX_CONCURRENT" in
    0|off|OFF|none|NONE|"")
        HRU_ARRAY_SPEC="0-$LAST_HRU_IDX"
        HRU_THROTTLE_NOTE="no concurrency cap"
        ;;
    *)
        HRU_ARRAY_SPEC="0-$LAST_HRU_IDX%$HRU_MAX_CONCURRENT"
        HRU_THROTTLE_NOTE="max $HRU_MAX_CONCURRENT concurrent"
        ;;
esac

echo "dprst_depth pipeline: FABRIC=$FABRIC"
echo "  stage 1-2: $N_TILE_BATCHES tile batches (uncapped concurrency -- see sizing note)"
echo "  stage 4a : $N_HRU_BATCHES HRU batches ($HRU_THROTTLE_NOTE)"

echo "--- stage 1: plan (tile work-list) ---"
PLAN_JOB_ID=$(sbatch \
    --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC",N_TILE_BATCHES="$N_TILE_BATCHES" \
    slurm_batch/plan_dprst_depth_batches.batch | awk '{print $NF}')
echo "  plan: $PLAN_JOB_ID"

echo "--- stage 2: tile-batch compute array (afterok:$PLAN_JOB_ID) ---"
ARRAY_JOB_ID=$(sbatch --array="0-$LAST_TILE_IDX" \
    --dependency=afterok:"$PLAN_JOB_ID" \
    --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC" \
    slurm_batch/run_dprst_depth_batch.batch | awk '{print $NF}')
echo "  array: $ARRAY_JOB_ID"

echo "--- stage 3: fill+burn+op_flow_thres (afterok:$ARRAY_JOB_ID) ---"
# Overrides build_depstor_rasters.batch's #SBATCH defaults (384G/18h, sized
# for the OTHER depstor steps' full-grid ops) -- dprst_depth's own compute is
# vector-scale (the tagged polygon set) + a streamed row-strip burn
# (burn_depth's STRIP_ROWS pattern), not a full-CONUS-grid materialization.
BUILD_JOB_ID=$(sbatch --dependency=afterok:"$ARRAY_JOB_ID" \
    --mem=64G --time=02:00:00 \
    --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC" \
    slurm_batch/build_depstor_rasters.batch --step dprst_depth | awk '{print $NF}')
echo "  build: $BUILD_JOB_ID"

echo "--- stage 4a: mean_zonal array (afterok:$BUILD_JOB_ID) ---"
MEAN_ARRAY_JOB_ID=$(sbatch --array="$HRU_ARRAY_SPEC" \
    --dependency=afterok:"$BUILD_JOB_ID" \
    --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC" \
    slurm_batch/mean_zonal_dprst_depth.batch | awk '{print $NF}')
echo "  mean_zonal array: $MEAN_ARRAY_JOB_ID"

echo "--- stage 4b: mean_finalize (afterok:$MEAN_ARRAY_JOB_ID) ---"
MEAN_FINALIZE_JOB_ID=$(sbatch --dependency=afterok:"$MEAN_ARRAY_JOB_ID" \
    --export=ALL,BASE_CONFIG="$BASE_CONFIG",FABRIC="$FABRIC" \
    slurm_batch/mean_finalize_dprst_depth.batch | awk '{print $NF}')
echo "  mean_finalize: $MEAN_FINALIZE_JOB_ID"

echo "Done. Final job ID: $MEAN_FINALIZE_JOB_ID (writes {output_dir}/merged/nhm_dprst_depth_avg_params.csv)"
