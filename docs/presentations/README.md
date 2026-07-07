# Presentations

Marp slide decks (`*.slides.md`). Rendered with the `marp` pixi environment.

## Decks

- `2026-07-depression-storage-workflow.slides.md` — the PRMS/NHM depression-storage
  parameter workflow: the legacy ArcPy pipeline (`docs/0b_TB_depr_stor.py`) vs. the
  current open-source pipeline. Method/workflow-focused; spans fabrics.

## Rendering

One-time chrome download (bare HPC / fresh checkout):

    pixi install -e marp
    pixi run -e marp marp-setup

Render:

    pixi run -e marp render-deck docs/presentations/<deck>.slides.md --html
    pixi run -e marp render-deck docs/presentations/<deck>.slides.md --pdf
    pixi run -e marp render-deck docs/presentations/ --server   # live preview
