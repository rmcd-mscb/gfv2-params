"""Smoke tests for the build_shared_rasters orchestrator scaffolding.

Steps land incrementally; until builders are wired up the orchestrator
must still parse its config, walk zero steps, and exit cleanly.
"""

import subprocess
import sys
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


def test_every_registered_step_has_a_builder():
    assert set(STEP_ORDER) == set(BUILDERS.keys())


def test_default_vpus_scope_matches_merge_manifests():
    """The per-VPU loop scope (`vpus:` in shared_rasters.yml) must match the
    consolidated VPU keys of the merge manifests (01-18), NOT the RPU-split
    units (03N/03S/03W, 10L/10U).

    If they drift, a per-VPU `--step` run with no `--vpus` override walks the
    21-unit list and raises FileNotFoundError on the missing
    NEDSnapshot_merged_03N.tif (the merged tiles are named `03`/`10`). This
    test pins the two together so the config can't go stale again.
    """
    cfg_dir = REPO_ROOT / "configs" / "shared_rasters"
    vpus = set(yaml.safe_load((cfg_dir / "shared_rasters.yml").read_text())["vpus"])
    std = set(yaml.safe_load((cfg_dir / "merge_rpu_by_vpu.yml").read_text()))
    twi = set(yaml.safe_load((cfg_dir / "merge_rpu_by_vpu_twi.yml").read_text()))
    assert vpus == std, f"vpus scope != merge_rpu_by_vpu manifest: {vpus ^ std}"
    assert vpus == twi, f"vpus scope != merge_rpu_by_vpu_twi manifest: {vpus ^ twi}"


def test_every_registered_step_is_in_planned_or_documented_aliases():
    """Every STEP_ORDER entry must either be in PLANNED_STEPS or be a documented
    secondary invocation of an already-planned builder (e.g.,
    merge_rpu_by_vpu_twi reuses the merge_rpu_by_vpu builder for the
    post-landmask TWI pass)."""
    for step in STEP_ORDER:
        assert step in PLANNED_STEPS, (
            f"Registered step '{step}' is not in PLANNED_STEPS. Update "
            f"PLANNED_STEPS roadmap when adding a new step."
        )


def test_planned_steps_cover_the_production_pipeline():
    expected = {
        "merge_rpu_by_vpu",
        "compute_slope_aspect",
        "build_border_dem",
        "build_vpu_landmask",
        "merge_rpu_by_vpu_twi",
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
        "output_dir": "{data_root}/shared",
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
    assert (data_root / "shared").exists()


def test_orchestrator_rejects_unknown_step(tmp_path):
    """A step name not in STEP_ORDER (typo or step not yet migrated) should
    fail clearly with a message pointing at PLANNED_STEPS."""
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
        "output_dir": "{data_root}/shared",
        "steps": [{"name": "this_step_does_not_exist"}],
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
    combined = result.stdout + result.stderr
    assert "this_step_does_not_exist" in combined
    assert "not registered" in combined or "Registered" in combined
