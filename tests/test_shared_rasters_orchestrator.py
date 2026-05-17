"""Smoke tests for the build_shared_rasters orchestrator scaffolding.

Steps land incrementally; until builders are wired up the orchestrator
must still parse its config, walk zero steps, and exit cleanly.
"""

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

from gfv2_params.shared_rasters import (
    BUILDERS,
    PLANNED_STEPS,
    STEP_ORDER,
    SharedRastersContext,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
ORCHESTRATOR = REPO_ROOT / "scripts" / "build_shared_rasters.py"


def test_planned_and_registered_steps_are_disjoint_until_migrated():
    overlap = set(STEP_ORDER) & (set(PLANNED_STEPS) - set(STEP_ORDER))
    assert overlap == set()


def test_every_registered_step_has_a_builder():
    assert set(STEP_ORDER) == set(BUILDERS.keys())


def test_planned_steps_cover_the_eight_legacy_scripts():
    expected = {
        "merge_rpu_by_vpu",
        "compute_slope_aspect",
        "compute_dem_derivatives",
        "build_border_dem",
        "build_vpu_landmask",
        "build_vrt",
        "build_derived_rasters",
        "build_lulc_rasters",
    }
    assert set(PLANNED_STEPS) >= expected


def test_context_require_raises_for_missing_key(tmp_path):
    ctx = SharedRastersContext(data_root=tmp_path, vpus=["01"], output_dir=tmp_path)
    with pytest.raises(FileNotFoundError, match="upstream output 'foo'"):
        ctx.require("foo")


def test_context_require_returns_path_when_present(tmp_path):
    target = tmp_path / "x.tif"
    target.write_text("")
    ctx = SharedRastersContext(data_root=tmp_path, vpus=[], output_dir=tmp_path)
    ctx.paths["x"] = target
    assert ctx.require("x") == target


def test_orchestrator_smoke_with_empty_steps(tmp_path):
    """The empty-DAG case should print 'no steps to run' and exit 0."""
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    base_config = tmp_path / "base_config.yml"
    base_config.write_text(yaml.safe_dump({
        "data_root": str(data_root),
        "default_fabric": "stub",
        "fabrics": {"stub": {}},
    }))
    shared_config = tmp_path / "shared_rasters.yml"
    shared_config.write_text(yaml.safe_dump({
        "vpus": ["01"],
        "output_dir": "{data_root}/work",
        "steps": [],
    }))
    result = subprocess.run(
        [
            sys.executable,
            str(ORCHESTRATOR),
            "--config", str(shared_config),
            "--base_config", str(base_config),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "No steps to run" in (result.stdout + result.stderr)
    assert (data_root / "work").exists()


def test_orchestrator_rejects_unknown_step(tmp_path):
    """A step name not in STEP_ORDER (and not yet migrated) should fail clearly."""
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    base_config = tmp_path / "base_config.yml"
    base_config.write_text(yaml.safe_dump({
        "data_root": str(data_root),
        "default_fabric": "stub",
        "fabrics": {"stub": {}},
    }))
    shared_config = tmp_path / "shared_rasters.yml"
    shared_config.write_text(yaml.safe_dump({
        "vpus": ["01"],
        "output_dir": "{data_root}/work",
        "steps": [{"name": "merge_rpu_by_vpu"}],
    }))
    result = subprocess.run(
        [
            sys.executable,
            str(ORCHESTRATOR),
            "--config", str(shared_config),
            "--base_config", str(base_config),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "not registered" in (result.stdout + result.stderr) or "merge_rpu_by_vpu" in result.stderr
