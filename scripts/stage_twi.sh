#!/usr/bin/env bash
# Stage per-RPU TWI rasters into data_root for the per-VPU merge step.
#
# Provenance:
#   USGS ScienceBase item 5f5154ba82ce4c3d12386a02
#   https://www.sciencebase.gov/catalog/item/5f5154ba82ce4c3d12386a02
#   (Note: this is NOT a public link; access is gated.)
#
# Operational mirror on the shared cluster filesystem (default $SRC):
#   /caldera/hovenweep/projects/usgs/water/impd/nhgf/data_creation/nhm_data_bins/data_bins/HRU<rpu>/topo/twi.*
#
# HRU06a's source files are named TWI.* (uppercase); all others are twi.*.
# This script normalizes to lowercase twi.* in the destination so the YAML
# merge config can reference all 18 VPUs without per-RPU casing exceptions.
#
# Idempotent: skips files already present and newer than the source.
#
# Usage:
#   bash scripts/stage_twi.sh
#   bash scripts/stage_twi.sh /alt/path/to/source/data_bins

set -euo pipefail

SRC=${1:-/caldera/hovenweep/projects/usgs/water/impd/nhgf/data_creation/nhm_data_bins/data_bins}
DEST_ROOT=$(pixi run python -c "import yaml; print(yaml.safe_load(open('configs/base_config.yml'))['data_root'])")
DEST="$DEST_ROOT/input/twi"

if [ ! -d "$SRC" ]; then
    echo "Error: source directory not found: $SRC" >&2
    exit 1
fi

mkdir -p "$DEST"
echo "source: $SRC"
echo "dest  : $DEST"

n_copied=0
n_up_to_date=0
n_no_source=0
shopt -s nocaseglob

for hru_dir in "$SRC"/HRU*; do
    rpu=$(basename "$hru_dir" | sed 's/^HRU//')
    out_dir="$DEST/$rpu"

    # Glob case-insensitively for twi.* files (catches both twi.* and TWI.*).
    src_files=("$hru_dir"/topo/twi.*)
    if [ ! -e "${src_files[0]}" ]; then
        n_no_source=$((n_no_source + 1))
        continue
    fi

    n_files_for_rpu=0
    for src_path in "${src_files[@]}"; do
        # Normalize destination filename to lowercase
        out_name=$(basename "$src_path" | tr '[:upper:]' '[:lower:]')
        out_file="$out_dir/$out_name"
        if [ -f "$out_file" ] && [ "$out_file" -nt "$src_path" ]; then
            continue
        fi
        mkdir -p "$out_dir"
        cp -p "$src_path" "$out_file"
        n_files_for_rpu=$((n_files_for_rpu + 1))
    done

    if [ "$n_files_for_rpu" -gt 0 ]; then
        n_copied=$((n_copied + 1))
        echo "COPIED  $rpu  ($n_files_for_rpu files)"
    else
        n_up_to_date=$((n_up_to_date + 1))
    fi
done

shopt -u nocaseglob

echo "done: $n_copied RPUs copied, $n_up_to_date up-to-date, $n_no_source HRU dirs without TWI source"
