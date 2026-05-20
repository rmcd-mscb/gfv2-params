"""The fabric gpkg is config-driven via the active profile's hru_gpkg/hru_layer.

Issue #88: prepare_fabric, the ssflux build_weights step, and gap-fill
(merge_and_fill_params) must all resolve the fabric geopackage from
configs/base_config.yml's `hru_gpkg` (read with `hru_layer`) — never from a
required --fabric_gpkg CLI arg or a {fabric}_nhru_merged.gpkg naming
convention. These are subprocess invariants (mirroring
tests/test_zonal_orchestrator.py) so CI runs them without the heavy geo libs
actually loading a real fabric.
"""

import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PREPARE_FABRIC = REPO_ROOT / "scripts" / "prepare_fabric.py"
MERGE_AND_FILL = REPO_ROOT / "scripts" / "merge_and_fill_params.py"
ZONAL_ORCHESTRATOR = REPO_ROOT / "scripts" / "derive_zonal_params.py"
ZONAL_PARAMS_CONFIG = REPO_ROOT / "configs" / "zonal" / "zonal_params.yml"


def _write_base_config(tmp_path, profile: dict, *, data_root=None) -> Path:
    """Write a minimal base_config.yml with a single `stub` fabric profile."""
    base_config = tmp_path / "base_config.yml"
    base_config.write_text(yaml.safe_dump({
        "data_root": str(data_root or (tmp_path / "fake_root")),
        "default_fabric": "stub",
        "fabrics": {"stub": profile},
    }))
    return base_config


def _run(script: Path, *args) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(script), *args],
        capture_output=True,
        text=True,
    )


# ---------------------------------------------------------------------------
# prepare_fabric: --fabric_gpkg is now an optional override of hru_gpkg
# ---------------------------------------------------------------------------

def test_prepare_fabric_gpkg_arg_is_optional():
    """--fabric_gpkg must no longer be a required CLI arg (it defaults to the
    profile's hru_gpkg)."""
    result = _run(PREPARE_FABRIC, "--help")
    assert result.returncode == 0
    # argparse marks required args in usage with no surrounding brackets;
    # an optional arg appears as [--fabric_gpkg ...].
    assert "[--fabric_gpkg" in result.stdout


def test_prepare_fabric_requires_hru_gpkg_when_no_override(tmp_path):
    """No --fabric_gpkg and no profile hru_gpkg -> clear, profile-pointing error."""
    base_config = _write_base_config(tmp_path, {"id_feature": "hru_id"})
    result = _run(PREPARE_FABRIC, "--base_config", str(base_config), "--fabric", "stub")
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "hru_gpkg" in combined
    assert "base_config" in combined


def test_prepare_fabric_reads_hru_gpkg_from_profile(tmp_path):
    """With hru_gpkg set (but the file absent), prepare_fabric resolves it from
    the profile — proven by the missing-file error naming that exact path."""
    base_config = _write_base_config(
        tmp_path,
        {"id_feature": "hru_id", "hru_gpkg": "{data_root}/from_profile.gpkg", "hru_layer": "nhru"},
    )
    result = _run(PREPARE_FABRIC, "--base_config", str(base_config), "--fabric", "stub")
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Fabric geopackage not found" in combined
    assert "from_profile.gpkg" in combined


# ---------------------------------------------------------------------------
# merge_and_fill_params: --merged_gpkg defaults to hru_gpkg
# ---------------------------------------------------------------------------

def test_merge_and_fill_requires_hru_gpkg_when_no_override(tmp_path):
    """No --merged_gpkg and no profile hru_gpkg -> clear, profile-pointing error
    (not a {fabric}_nhru_merged.gpkg guess)."""
    base_config = _write_base_config(
        tmp_path, {"id_feature": "hru_id", "expected_max_hru_id": 10}
    )
    result = _run(MERGE_AND_FILL, "--base_config", str(base_config), "--fabric", "stub")
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "hru_gpkg" in combined
    assert "base_config" in combined


# ---------------------------------------------------------------------------
# derive_zonal_params build_weights: reads hru_gpkg threaded via _build_param_cfg
# ---------------------------------------------------------------------------

def test_build_weights_requires_hru_gpkg(tmp_path):
    """build_weights flows through _build_param_cfg, which now requires hru_gpkg
    (same pattern as id_feature). A profile missing it fails loudly."""
    base_config = _write_base_config(tmp_path, {"id_feature": "hru_id"})
    result = _run(
        ZONAL_ORCHESTRATOR,
        "--config", str(ZONAL_PARAMS_CONFIG),
        "--base_config", str(base_config),
        "--mode", "build_weights",
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "hru_gpkg" in combined
    assert "base_config" in combined
