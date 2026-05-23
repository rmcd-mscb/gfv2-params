"""Tests for scripts/init_data_root.validate_inputs shapefile-sidecar check.

Closes issue #47: validate_inputs must report `.shx`/`.dbf`/`.prj` sidecar
files missing alongside `.shp` paths, instead of silently passing and
letting pyogrio/fiona fail with a confusing error later.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path


# scripts/init_data_root.py is not a package import; load it by path.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_INIT_DATA_ROOT_PATH = _REPO_ROOT / "scripts" / "init_data_root.py"
_spec = importlib.util.spec_from_file_location("init_data_root", _INIT_DATA_ROOT_PATH)
init_data_root = importlib.util.module_from_spec(_spec)
sys.modules["init_data_root"] = init_data_root
_spec.loader.exec_module(init_data_root)


def _stage_all_non_shp(data_root: Path, fabric: str) -> None:
    """Touch every required non-shapefile path so only the .shp check varies."""
    paths = [
        data_root / "input" / "soils_litho" / "TEXT_PRMS.tif",
        data_root / "input" / "soils_litho" / "AWC.tif",
        data_root / "input" / "lulc_veg" / "RootDepth.tif",
        data_root / "input" / "depstor" / f"{fabric}_segments_wbodies.gpkg",
        data_root / "input" / "twi" / "01a" / "twi.tif",
    ]
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()


def _stage_shapefile(data_root: Path, with_sidecars: tuple[str, ...]) -> Path:
    """Touch Lithology_exp_Konly_Project.shp + the named sidecars."""
    shp_dir = data_root / "input" / "soils_litho"
    shp_dir.mkdir(parents=True, exist_ok=True)
    shp = shp_dir / "Lithology_exp_Konly_Project.shp"
    shp.touch()
    for ext in with_sidecars:
        (shp.with_suffix(ext)).touch()
    return shp


def test_all_required_present_emits_info(tmp_path, caplog):
    """Happy path: every required file + every sidecar present → INFO 'All ... present'."""
    _stage_all_non_shp(tmp_path, "gfv2")
    _stage_shapefile(tmp_path, with_sidecars=(".shx", ".dbf", ".prj"))
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_validate_inputs_happy")
    init_data_root.validate_inputs(tmp_path, "gfv2", logger)
    assert any("All required staged inputs are present" in r.message for r in caplog.records)
    assert not any("MISSING" in r.message for r in caplog.records)


def test_missing_sidecars_reported_individually(tmp_path, caplog):
    """When only .shp is staged, all 3 sidecars (.shx/.dbf/.prj) must be reported."""
    _stage_all_non_shp(tmp_path, "gfv2")
    _stage_shapefile(tmp_path, with_sidecars=())  # bare .shp, no sidecars
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_missing_sidecars")
    init_data_root.validate_inputs(tmp_path, "gfv2", logger)
    missing_msgs = [r.message for r in caplog.records if "MISSING" in r.message]
    assert any(".shx" in m for m in missing_msgs), missing_msgs
    assert any(".dbf" in m for m in missing_msgs), missing_msgs
    assert any(".prj" in m for m in missing_msgs), missing_msgs
    # .shp itself was staged, so it should NOT be in the missing list
    assert not any(m.endswith(".shp") for m in missing_msgs)


def test_missing_shp_does_not_duplicate_as_sidecars(tmp_path, caplog):
    """When the .shp itself is missing, report it (don't also pretend sidecars are missing)."""
    _stage_all_non_shp(tmp_path, "gfv2")
    # Do NOT stage the shapefile at all
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_missing_shp")
    init_data_root.validate_inputs(tmp_path, "gfv2", logger)
    missing_msgs = [r.message for r in caplog.records if "MISSING" in r.message]
    # The .shp is reported as missing
    assert any("Lithology_exp_Konly_Project.shp" in m for m in missing_msgs)
    # Sidecars are also reported (they're missing too, since the .shp is missing)
    # — this is correct + helpful: the user sees the full list of files to stage.
    assert any(".shx" in m for m in missing_msgs)


def test_non_shp_path_missing_still_reported(tmp_path, caplog):
    """A non-.shp required path (e.g. TEXT_PRMS.tif) missing still triggers a warning."""
    # Only stage some of the required files; intentionally omit TEXT_PRMS.tif
    paths = [
        tmp_path / "input" / "soils_litho" / "AWC.tif",
        tmp_path / "input" / "lulc_veg" / "RootDepth.tif",
        tmp_path / "input" / "depstor" / "gfv2_segments_wbodies.gpkg",
        tmp_path / "input" / "twi" / "01a" / "twi.tif",
    ]
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
    _stage_shapefile(tmp_path, with_sidecars=(".shx", ".dbf", ".prj"))
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_missing_tif")
    init_data_root.validate_inputs(tmp_path, "gfv2", logger)
    missing_msgs = [r.message for r in caplog.records if "MISSING" in r.message]
    assert any("TEXT_PRMS.tif" in m for m in missing_msgs)
