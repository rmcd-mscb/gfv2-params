import tempfile
from pathlib import Path

import yaml

from gfv2_params.config import (
    VPUS_DETAILED,
    VPUS_SIMPLE,
    VPU_RASTER_MAP,
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
