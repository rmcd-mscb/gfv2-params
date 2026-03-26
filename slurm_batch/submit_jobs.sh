#!/bin/bash
# Usage: ./submit_jobs.sh /path/to/{fabric}/batches <batch_script.batch> [base_config.yml]
#
# Reads the batch count from manifest.yml and submits the batch script
# as a SLURM array job with the appropriate range.
# The optional third argument sets BASE_CONFIG (default: configs/base_config.yml).

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <batches_dir> <batch_script> [base_config.yml]"
    echo "  batches_dir:   path to {fabric}/batches/ (contains manifest.yml)"
    echo "  batch_script:  SLURM batch file to submit"
    echo "  base_config:   optional path to base_config.yml (default: configs/base_config.yml)"
    exit 1
fi

FABRIC_DIR="$1"
BATCH_SCRIPT="$2"
BASE_CONFIG="${3:-configs/base_config.yml}"
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

echo "Submitting $BATCH_SCRIPT with --array=0-$LAST_IDX ($N_BATCHES batches)"
sbatch --array=0-"$LAST_IDX" --export=ALL,BASE_CONFIG="$BASE_CONFIG" "$BATCH_SCRIPT"
