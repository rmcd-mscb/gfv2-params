#!/usr/bin/env bash
# Pre-bake the pixi activation script so slurm batches can `source` it
# without invoking pixi at task start.
#
# Why: `pixi shell-hook --locked` reads/writes metadata under
# .pixi/envs/<env>/conda-meta/ on every invocation. When 18 array tasks
# all run that simultaneously, they race ("File was modified during
# parsing", "Failed to update PyPI packages", etc.) and a fraction of
# tasks fail before they ever reach python. Sourcing a pre-baked script
# is pure shell — no concurrency surface.
#
# Run this:
#   - once after `pixi install`
#   - again any time pyproject.toml or pixi.lock change

set -euo pipefail
cd "$(dirname "$0")/.."

pixi shell-hook --locked --shell bash > .pixi-activate.sh
echo "wrote .pixi-activate.sh ($(wc -l < .pixi-activate.sh) lines)"

# The notebooks env is optional — only refresh its activation if it's
# been installed.
if [ -d .pixi/envs/notebooks ]; then
    pixi shell-hook --locked --shell bash -e notebooks > .pixi-activate-notebooks.sh
    echo "wrote .pixi-activate-notebooks.sh ($(wc -l < .pixi-activate-notebooks.sh) lines)"
fi
