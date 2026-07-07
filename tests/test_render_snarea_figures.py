"""Smoke tests for the data-free schematic figures in render_snarea_figures.

The data-driven figures read pipeline outputs and are verified by visual
inspection of the rendered PNGs, not unit tests. These two schematics take no
data, so they are cheap to smoke-test in CI.
"""
from pathlib import Path

from scripts.render_snarea_figures import schematic_concept, schematic_pipeline


def test_schematic_concept_writes_png(tmp_path: Path) -> None:
    out = tmp_path / "concept.png"
    schematic_concept(out)
    assert out.exists() and out.stat().st_size > 5_000


def test_schematic_pipeline_writes_png(tmp_path: Path) -> None:
    out = tmp_path / "pipeline.png"
    schematic_pipeline(out)
    assert out.exists() and out.stat().st_size > 5_000
