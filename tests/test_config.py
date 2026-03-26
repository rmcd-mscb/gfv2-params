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
            "expected_max_hru_id": 100,
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
            "expected_max_hru_id": 100,
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
    """load_base_config returns only base config keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "targets_dir": "targets",
            "output_dir": "nhm_params",
            "expected_max_hru_id": 100,
        }))

        config = load_base_config(base_config)
        assert config["data_root"] == "/fake/root"
        assert config["expected_max_hru_id"] == 100


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
    """Config with {fabric} placeholder should resolve from base config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "fabric": "gfv2",
            "expected_max_hru_id": 100,
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
            "fabric": "gfv2",
            "expected_max_hru_id": 100,
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "target_gpkg": "{data_root}/input/fabrics/NHM_{vpu}_draft.gpkg",
            "output_dir": "{data_root}/{fabric}/work",
        }))
        config = load_config(step_config, vpu="03N", base_config_path=base_config)
        assert config["target_gpkg"] == "/fake/root/input/fabrics/NHM_03N_draft.gpkg"
        assert config["output_dir"] == "/fake/root/gfv2/work"


def test_load_config_without_fabric_still_works():
    """Existing configs without {fabric} placeholder should still work."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "expected_max_hru_id": 100,
        }))
        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_raster": "{data_root}/rasters/dem.tif",
        }))
        config = load_config(step_config, base_config_path=base_config)
        assert config["source_raster"] == "/fake/root/rasters/dem.tif"
