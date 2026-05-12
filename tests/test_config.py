import tempfile
from pathlib import Path

import pytest
import yaml

from gfv2_params.config import (
    VPUS_DETAILED,
    VPUS_SIMPLE,
    VPU_RASTER_MAP,
    _load_yaml,
    _resolve_placeholders,
    load_base_config,
    load_config,
    require_profile_key,
    resolve_vpu,
)


def test_vpus_detailed_has_21_entries():
    assert len(VPUS_DETAILED) == 21


def test_vpus_simple_has_18_entries():
    assert len(VPUS_SIMPLE) == 18
    assert VPUS_SIMPLE[0] == "01"
    assert VPUS_SIMPLE[-1] == "18"


def test_resolve_vpu_standard():
    raster_vpu, gpkg_vpu = resolve_vpu("14")
    assert raster_vpu == "14"
    assert gpkg_vpu == "14"


def test_resolve_vpu_03N():
    raster_vpu, gpkg_vpu = resolve_vpu("03N")
    assert raster_vpu == "03"
    assert gpkg_vpu == "03N"


def test_resolve_vpu_10L():
    raster_vpu, gpkg_vpu = resolve_vpu("10L")
    assert raster_vpu == "10"
    assert gpkg_vpu == "10L"


def test_resolve_vpu_OR():
    raster_vpu, gpkg_vpu = resolve_vpu("OR")
    assert raster_vpu == "17"
    assert gpkg_vpu == "OR"


def test_load_config_without_vpu():
    """Config with explicit paths should work without --vpu."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "targets_dir": "targets",
            "output_dir": "nhm_params",
            "default_fabric": "test",
            "fabrics": {"test": {"expected_max_hru_id": 100}},
        }))

        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_type": "elevation",
            "source_raster": "/explicit/path/dem.tif",
            "target_gpkg": "/explicit/path/fabric.gpkg",
            "target_layer": "catchments",
            "id_feature": "catch_id",
            "output_dir": "/explicit/path/output",
            "categorical": False,
        }))

        config = load_config(step_config, vpu=None, base_config_path=base_config)
        assert config["source_raster"] == "/explicit/path/dem.tif"
        assert config["target_layer"] == "catchments"
        assert config["data_root"] == "/fake/root"


def test_load_config_with_vpu_resolves_placeholders():
    """Config with {data_root}, {vpu}, {raster_vpu} placeholders should resolve."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "targets_dir": "targets",
            "output_dir": "nhm_params",
            "default_fabric": "test",
            "fabrics": {"test": {"expected_max_hru_id": 100}},
        }))

        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_type": "elevation",
            "source_raster": "{data_root}/rasters/{raster_vpu}/dem_{raster_vpu}.tif",
            "target_gpkg": "{data_root}/targets/NHM_{vpu}_draft.gpkg",
            "target_layer": "nhru",
            "id_feature": "nat_hru_id",
            "output_dir": "{data_root}/nhm_params",
            "categorical": False,
        }))

        config = load_config(step_config, vpu="03N", base_config_path=base_config)
        assert config["source_raster"] == "/fake/root/rasters/03/dem_03.tif"
        assert config["target_gpkg"] == "/fake/root/targets/NHM_03N_draft.gpkg"
        assert config["output_dir"] == "/fake/root/nhm_params"


def test_load_base_config():
    """load_base_config returns base config keys + active fabric profile."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "targets_dir": "targets",
            "output_dir": "nhm_params",
            "default_fabric": "test",
            "fabrics": {"test": {"expected_max_hru_id": 100}},
        }))

        config = load_base_config(base_config)
        assert config["data_root"] == "/fake/root"
        assert config["expected_max_hru_id"] == 100
        assert config["fabric"] == "test"


def test_resolve_vpu_lowercase():
    """Lowercase VPU not in map returns itself for both values."""
    raster_vpu, gpkg_vpu = resolve_vpu("03n")
    assert raster_vpu == "03n"
    assert gpkg_vpu == "03n"


def test_resolve_vpu_nonexistent():
    """Non-existent VPU returns itself for both values."""
    raster_vpu, gpkg_vpu = resolve_vpu("99")
    assert raster_vpu == "99"
    assert gpkg_vpu == "99"


def test_load_yaml_empty_file():
    """Empty YAML file raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        empty_file = Path(tmpdir) / "empty.yml"
        empty_file.write_text("")
        with pytest.raises(ValueError, match="empty or contains no YAML data"):
            _load_yaml(empty_file)


def test_load_yaml_non_dict():
    """YAML file with a list raises TypeError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        list_file = Path(tmpdir) / "list.yml"
        list_file.write_text("- item1\n- item2\n")
        with pytest.raises(TypeError, match="must contain a YAML mapping"):
            _load_yaml(list_file)


def test_resolve_placeholders_unresolved():
    """Unresolved placeholders raise ValueError."""
    config = {"key": "{data_root}/path/{vpu}/file.tif"}
    replacements = {"data_root": "/root"}
    with pytest.raises(ValueError, match="Unresolved placeholder"):
        _resolve_placeholders(config, replacements)


def test_load_config_resolves_fabric_placeholder():
    """Config with {fabric} placeholder should resolve from active profile."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "default_fabric": "gfv2",
            "fabrics": {"gfv2": {"expected_max_hru_id": 100}},
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_type": "elevation",
            "source_raster": "{data_root}/work/nhd_merged/elevation.vrt",
            "batch_dir": "{data_root}/{fabric}/batches",
            "output_dir": "{data_root}/{fabric}/params",
            "target_layer": "nhru",
            "id_feature": "nat_hru_id",
            "categorical": False,
        }))
        config = load_config(step_config, base_config_path=base_config)
        assert config["batch_dir"] == "/fake/root/gfv2/batches"
        assert config["output_dir"] == "/fake/root/gfv2/params"
        assert config["fabric"] == "gfv2"


def test_load_config_fabric_with_vpu():
    """Both {fabric} and {vpu} should resolve when vpu is provided."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "default_fabric": "gfv2",
            "fabrics": {"gfv2": {"expected_max_hru_id": 100}},
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "target_gpkg": "{data_root}/input/fabrics/NHM_{vpu}_draft.gpkg",
            "output_dir": "{data_root}/{fabric}/work",
        }))
        config = load_config(step_config, vpu="03N", base_config_path=base_config)
        assert config["target_gpkg"] == "/fake/root/input/fabrics/NHM_03N_draft.gpkg"
        assert config["output_dir"] == "/fake/root/gfv2/work"


def test_load_config_step_self_reference():
    """Step config scalar values resolve as placeholders in other step values."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "default_fabric": "gfv2",
            "fabrics": {"gfv2": {"expected_max_hru_id": 100}},
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_type": "lulc",
            "lulc_source": "foresce",
            "scenario": "bau_rcp45",
            "year": 2070,
            "source_raster": "{data_root}/input/{lulc_source}/LULC_{scenario}_{year}.tif",
            "batch_dir": "{data_root}/{fabric}/batches",
        }))
        config = load_config(step_config, base_config_path=base_config)
        assert config["source_raster"] == "/fake/root/input/foresce/LULC_bau_rcp45_2070.tif"
        assert config["batch_dir"] == "/fake/root/gfv2/batches"


def test_load_config_legacy_base_without_fabrics_raises():
    """Base config without `fabrics:` mapping raises a clear error."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "fabric": "legacy",
            "expected_max_hru_id": 100,
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_raster": "{data_root}/rasters/dem.tif",
        }))
        with pytest.raises(ValueError, match="no `fabrics:` mapping"):
            load_config(step_config, base_config_path=base_config)


# ---------------------------------------------------------------------------
# Fabric profile tests
# ---------------------------------------------------------------------------

def _profiles_base_config(tmpdir: str) -> Path:
    """Write a fabrics-profile-style base_config and return its path."""
    base_config = Path(tmpdir) / "base_config.yml"
    base_config.write_text(yaml.dump({
        "data_root": "/fake/root",
        "default_fabric": "gfv2",
        "fabrics": {
            "gfv2": {
                "expected_max_hru_id": 361471,
                "batch_size": 10000,
                "template_raster": "{data_root}/work/elevation.vrt",
                "waterbody_layer": "v2_wb",
            },
            "gfv2_vpu01": {
                "expected_max_hru_id": 11278,
                "batch_size": 2000,
                "template_raster": "{data_root}/work/01/Hydrodem_merged_01.tif",
                "waterbody_layer": "wbs",
            },
        },
    }))
    return base_config


def test_load_config_selects_fabric_profile():
    """Explicit fabric kwarg picks the right profile."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = _profiles_base_config(tmpdir)
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "output_dir": "{data_root}/{fabric}/params",
        }))
        config = load_config(step_config, base_config_path=base_config, fabric="gfv2_vpu01")
        assert config["fabric"] == "gfv2_vpu01"
        assert config["expected_max_hru_id"] == 11278
        assert config["batch_size"] == 2000
        assert config["waterbody_layer"] == "wbs"
        assert config["template_raster"] == "/fake/root/work/01/Hydrodem_merged_01.tif"
        assert config["output_dir"] == "/fake/root/gfv2_vpu01/params"


def test_load_config_uses_default_fabric():
    """Without explicit fabric or env, default_fabric in base config is used."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = _profiles_base_config(tmpdir)
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "output_dir": "{data_root}/{fabric}/params",
        }))
        config = load_config(step_config, base_config_path=base_config)
        assert config["fabric"] == "gfv2"
        assert config["expected_max_hru_id"] == 361471


def test_load_config_fabric_from_env(monkeypatch):
    """FABRIC env var selects profile when no kwarg is passed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = _profiles_base_config(tmpdir)
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "output_dir": "{data_root}/{fabric}/params",
        }))
        monkeypatch.setenv("FABRIC", "gfv2_vpu01")
        config = load_config(step_config, base_config_path=base_config)
        assert config["fabric"] == "gfv2_vpu01"


def test_load_config_explicit_fabric_overrides_env(monkeypatch):
    """Explicit kwarg wins over FABRIC env var."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = _profiles_base_config(tmpdir)
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "output_dir": "{data_root}/{fabric}/params",
        }))
        monkeypatch.setenv("FABRIC", "gfv2_vpu01")
        config = load_config(step_config, base_config_path=base_config, fabric="gfv2")
        assert config["fabric"] == "gfv2"


def test_load_config_unknown_fabric_raises():
    """Unknown fabric name raises ValueError listing valid choices."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = _profiles_base_config(tmpdir)
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({"output_dir": "{data_root}"}))
        with pytest.raises(ValueError, match="Fabric 'atlantis' not in"):
            load_config(step_config, base_config_path=base_config, fabric="atlantis")


def test_load_config_no_fabric_anywhere_raises(monkeypatch):
    """If base has no default_fabric, no env, no kwarg -> ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "fabrics": {
                "gfv2": {"expected_max_hru_id": 100},
            },
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({"output_dir": "{data_root}"}))
        monkeypatch.delenv("FABRIC", raising=False)
        with pytest.raises(ValueError, match="No fabric resolved"):
            load_config(step_config, base_config_path=base_config)


def test_require_profile_key_missing_raises():
    """require_profile_key raises a clear error when profile omits a key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "default_fabric": "oregon",
            "fabrics": {
                "oregon": {"expected_max_hru_id": 16814},
            },
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({"output_dir": "{data_root}"}))
        config = load_config(step_config, base_config_path=base_config)
        with pytest.raises(KeyError, match="Fabric profile 'oregon' does not define 'template_raster'"):
            require_profile_key(config, "template_raster", "build_depstor_imperv")


def test_require_profile_key_present_returns_value():
    """require_profile_key returns the value when present."""
    config = {"fabric": "gfv2", "template_raster": "/path/to/dem.vrt"}
    assert require_profile_key(config, "template_raster", "build_x") == "/path/to/dem.vrt"


def test_load_base_config_with_fabric_profile():
    """load_base_config flattens the active fabric profile."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = _profiles_base_config(tmpdir)
        config = load_base_config(base_config, fabric="gfv2_vpu01")
        assert config["fabric"] == "gfv2_vpu01"
        assert config["expected_max_hru_id"] == 11278
        assert config["batch_size"] == 2000
        # Template raster from profile is not placeholder-resolved here
        # (load_base_config doesn't run placeholder resolution); it stays raw.
        assert "{data_root}" in config["template_raster"]
