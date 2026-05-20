"""Tests for the --add-fabric profile-stub insertion in scripts/init_data_root.py"""

import importlib.util
import logging
from pathlib import Path

import pytest
import yaml

_spec = importlib.util.spec_from_file_location(
    "init_data_root",
    Path(__file__).resolve().parent.parent / "scripts" / "init_data_root.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

add_fabric_profile = _mod.add_fabric_profile

_logger = logging.getLogger("test")

# A minimal base_config with comments, gfv2 the only fabric, fabrics last.
_BASE = """\
# A leading comment that must survive the edit.
data_root: /fake/root
default_fabric: gfv2

fabrics:
  gfv2:
    expected_max_hru_id: 100
    batch_size: 10
    id_feature: nat_hru_id
"""


def _write_base(tmp_path) -> Path:
    p = tmp_path / "base_config.yml"
    p.write_text(_BASE)
    return p


def test_appends_parseable_profile_with_required_keys(tmp_path):
    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)

    cfg = yaml.safe_load(p.read_text())
    assert "oregon" in cfg["fabrics"]
    oregon = cfg["fabrics"]["oregon"]
    assert oregon["batch_size"] == 10000
    # required keys present as stubs (id_feature defaults to nat_hru_id placeholder)
    assert "expected_max_hru_id" in oregon
    assert oregon["id_feature"] == "nat_hru_id"
    # hru_gpkg/hru_layer are required for EVERY fabric (prepare_fabric,
    # build_weights, gap-fill) — they must be active keys in the stub, not
    # buried in the commented depstor block, or the first pipeline step
    # (prepare_fabric) fails with a KeyError.
    assert "hru_gpkg" in oregon
    assert oregon["hru_layer"] == "nhru"
    # depstor keys stay commented out (absent until that pipeline is staged)
    assert "template_raster" not in oregon
    assert "waterbody_gpkg" not in oregon
    # existing fabric untouched
    assert cfg["fabrics"]["gfv2"]["id_feature"] == "nat_hru_id"


def test_preserves_existing_comments(tmp_path):
    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)
    assert "# A leading comment that must survive the edit." in p.read_text()


def test_raises_when_fabric_exists(tmp_path):
    p = _write_base(tmp_path)
    with pytest.raises(ValueError, match="already exists"):
        add_fabric_profile(p, "gfv2", _logger)


def test_rejects_invalid_name(tmp_path):
    p = _write_base(tmp_path)
    with pytest.raises(ValueError, match="Invalid fabric name"):
        add_fabric_profile(p, "bad name", _logger)


def test_raises_without_fabrics_mapping(tmp_path):
    p = tmp_path / "base_config.yml"
    p.write_text("data_root: /fake/root\ndefault_fabric: gfv2\n")
    with pytest.raises(ValueError, match="No top-level `fabrics:`"):
        add_fabric_profile(p, "oregon", _logger)
