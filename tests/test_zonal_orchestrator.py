"""Tests for scripts/derive_zonal_params.py + configs/zonal_params.yml.

Mirrors tests/test_shared_rasters_orchestrator.py. Validates the unified
zonal-pass config invariants without actually invoking the heavy geo
libraries (so CI can run these in seconds).
"""

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
ORCHESTRATOR = REPO_ROOT / "scripts" / "derive_zonal_params.py"
ZONAL_PARAMS_CONFIG = REPO_ROOT / "configs" / "zonal_params.yml"


# Dispatch tags the orchestrator recognises. Must match _BATCH_RUNNERS in
# scripts/derive_zonal_params.py.
_KNOWN_SCRIPT_TAGS = {"zonal", "soils", "lulc", "ssflux"}

# Params expected to be present in the production zonal_params.yml. If you
# add a new param entry to the config, also add it here so the test catches
# accidental removal.
_EXPECTED_PARAMS = {
    "elevation",
    "slope",
    "aspect",
    "soils",
    "soil_moist_max",
    "lulc_nhm_v11",
    "lulc_nalcms",
    "lulc_nlcd",
    "lulc_foresce",
    "ssflux",
}


def _load_config_raw() -> dict:
    """Read configs/zonal_params.yml without resolving any placeholders.

    The orchestrator's full resolution path requires a base_config.yml +
    fabric profile, which is more than these invariant checks need.
    """
    with open(ZONAL_PARAMS_CONFIG) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Config-shape invariants
# ---------------------------------------------------------------------------

def test_zonal_params_config_parses():
    """The committed configs/zonal_params.yml must be valid YAML."""
    config = _load_config_raw()
    assert "defaults" in config
    assert "params" in config
    assert isinstance(config["params"], list)
    assert len(config["params"]) > 0


def test_every_param_has_a_known_script_tag():
    """Every entry's `script:` must dispatch to a recognised run_*_batch."""
    config = _load_config_raw()
    for entry in config["params"]:
        assert "name" in entry, f"Param entry missing `name`: {entry}"
        assert "script" in entry, f"Param entry '{entry['name']}' missing `script:`"
        assert entry["script"] in _KNOWN_SCRIPT_TAGS, (
            f"Param '{entry['name']}' has unknown script tag '{entry['script']}'. "
            f"Known: {sorted(_KNOWN_SCRIPT_TAGS)}"
        )


def test_every_param_has_a_merged_file():
    """Every entry needs a merged_file so run_merge knows where to write."""
    config = _load_config_raw()
    for entry in config["params"]:
        assert "merged_file" in entry, (
            f"Param '{entry['name']}' missing `merged_file`"
        )


def test_param_names_are_unique():
    """Duplicate `name:` would let the wrong entry win in _find_param."""
    config = _load_config_raw()
    names = [entry["name"] for entry in config["params"]]
    duplicates = [n for n in names if names.count(n) > 1]
    assert not duplicates, f"Duplicate param names: {sorted(set(duplicates))}"


def test_production_pipeline_params_present():
    """The 10 known param types must all appear in zonal_params.yml.

    Catches accidental removal during edits; new entries are fine.
    """
    config = _load_config_raw()
    names = {entry["name"] for entry in config["params"]}
    missing = _EXPECTED_PARAMS - names
    assert not missing, f"Missing expected params: {sorted(missing)}"


def test_ssflux_declares_weights_dependency():
    """ssflux can't run without the build_weights prereq + merged slope."""
    config = _load_config_raw()
    ssflux = next((e for e in config["params"] if e["name"] == "ssflux"), None)
    assert ssflux is not None, "ssflux entry missing"
    assert ssflux.get("depends_on") == "build_weights", (
        "ssflux must carry `depends_on: build_weights` for submit_zonal_params.sh "
        "to wire up the prereq job"
    )
    # The submit script also chains ssflux on the slope merge — confirm the
    # entry references a merged slope path.
    assert "merged_slope_file" in ssflux, "ssflux entry missing merged_slope_file"
    assert "nhm_slope_params.csv" in ssflux["merged_slope_file"], (
        "ssflux merged_slope_file must point at the slope merge output "
        "(nhm_slope_params.csv) so the dispatch chain is correct"
    )


def test_lulc_entries_use_per_source_names():
    """Each LULC source must have its own `name` (lulc_<source>) to avoid
    per-batch output collisions when multiple LULC sources run in parallel."""
    config = _load_config_raw()
    lulc_names = {e["name"] for e in config["params"] if e.get("script") == "lulc"}
    assert lulc_names >= {"lulc_nhm_v11", "lulc_nalcms", "lulc_nlcd", "lulc_foresce"}
    # No bare "lulc" entry (would collide with the legacy CLI's source_type).
    assert "lulc" not in lulc_names


# ---------------------------------------------------------------------------
# Orchestrator CLI invariants
# ---------------------------------------------------------------------------

def _run_orchestrator(*args) -> subprocess.CompletedProcess:
    """Invoke the orchestrator under the current Python with given args."""
    return subprocess.run(
        [sys.executable, str(ORCHESTRATOR), *args],
        capture_output=True,
        text=True,
    )


def test_orchestrator_requires_mode():
    """--mode is required; no default."""
    result = _run_orchestrator("--config", str(ZONAL_PARAMS_CONFIG))
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "--mode" in combined


def test_orchestrator_rejects_unknown_mode():
    """argparse choices keep --mode constrained to zonal/merge/build_weights."""
    result = _run_orchestrator(
        "--config", str(ZONAL_PARAMS_CONFIG),
        "--mode", "this_mode_does_not_exist",
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "invalid choice" in combined.lower() or "this_mode_does_not_exist" in combined


def test_orchestrator_zonal_mode_requires_param_and_batch_id():
    """--mode zonal needs --param + --batch_id; missing either should fail."""
    result = _run_orchestrator(
        "--config", str(ZONAL_PARAMS_CONFIG),
        "--mode", "zonal",
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "--param" in combined

    # With --param but no --batch_id
    result = _run_orchestrator(
        "--config", str(ZONAL_PARAMS_CONFIG),
        "--mode", "zonal",
        "--param", "elevation",
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "--batch_id" in combined


def test_orchestrator_merge_mode_requires_param():
    """--mode merge needs --param."""
    result = _run_orchestrator(
        "--config", str(ZONAL_PARAMS_CONFIG),
        "--mode", "merge",
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "--param" in combined


def test_orchestrator_rejects_unknown_param(tmp_path):
    """An unknown --param value should fail with a clear error pointing at
    the available list."""
    # Build a minimal base_config so the orchestrator can load_config without
    # depending on the real production data_root layout.
    base_config = tmp_path / "base_config.yml"
    base_config.write_text(yaml.safe_dump({
        "data_root": str(tmp_path / "fake_root"),
        "default_fabric": "stub",
        "fabrics": {"stub": {}},
    }))

    result = _run_orchestrator(
        "--config", str(ZONAL_PARAMS_CONFIG),
        "--base_config", str(base_config),
        "--mode", "merge",
        "--param", "this_param_does_not_exist",
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "this_param_does_not_exist" in combined
    assert "available" in combined.lower() or "Available" in combined
