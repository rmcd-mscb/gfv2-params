"""Tests for scripts/init_data_root.validate_inputs (the `--check` flag).

Two contracts:

1. (issue #47) `.shx`/`.dbf`/`.prj` sidecars are reported individually
   alongside a `.shp`, instead of silently passing and letting pyogrio/fiona
   fail with a confusing error later.
2. The shared depstor inputs are read from the ACTIVE PROFILE's own path
   keys, not from a hand-maintained literal list. The old list still named
   `input/depstor/<fabric>_segments_wbodies.gpkg`, a layout retired when the
   on-stream classifier moved to the fabric's own `nsegment` layer, so
   `--check` kept reporting a file no profile reads while saying nothing
   about the six shared layers every profile does read.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path

import pytest

# scripts/init_data_root.py is not a package import; load it by path.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_INIT_DATA_ROOT_PATH = _REPO_ROOT / "scripts" / "init_data_root.py"
_spec = importlib.util.spec_from_file_location("init_data_root", _INIT_DATA_ROOT_PATH)
init_data_root = importlib.util.module_from_spec(_spec)
sys.modules["init_data_root"] = init_data_root
_spec.loader.exec_module(init_data_root)


# The fixed, non-profile paths: staged inputs with no profile key, plus the two
# shared products every depstor fabric reads by convention (fdr.vrt is
# clip_shared_to_fabric's default source; the percentile table is what
# carea_map's percentile mode looks up).
_FIXED = [
    "input/soils_litho/TEXT_PRMS.tif",
    "input/soils_litho/AWC.tif",
    "input/lulc_veg/RootDepth.tif",
    "input/twi/01a/twi.tif",
    "shared/conus/vrt/fdr.vrt",
    "shared/conus/twi_reference_percentiles.hydrodem.csv",
]

# Profile-keyed shared inputs, as a real resolved profile would carry them.
_PROFILE_KEYS = {
    "waterbody_gpkg": "input/nhd/nhd_waterbodies.gpkg",
    "wbd_huc12_table": "input/wbd/wbd_huc12.parquet",
    "burn_add_waterbody_table": "input/nhd/burn_add_waterbodies.parquet",
    "sink_points_table": "input/nhd/sink_points.parquet",
    "ecoregions_gpkg": "input/ecoregions/us_eco_l3.gpkg",
    "dem_1m_inventory": "input/3dep/dem_1m_tile_inventory.parquet",
    "wesm_project_attrs": "input/wesm/wesm_project_attrs.parquet",
    "twi_raster": "shared/conus/vrt/twi_hydrodem.vrt",
}


def _touch(data_root: Path, rel: str) -> Path:
    p = data_root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()
    return p


def _profile(data_root: Path, omit: tuple[str, ...] = ()) -> dict:
    """A resolved profile dict (what load_base_config returns), minus `omit`."""
    cfg = {"data_root": str(data_root), "fabric": "gfv2"}
    for k, rel in _PROFILE_KEYS.items():
        if k not in omit:
            cfg[k] = str(data_root / rel)
    return cfg


def _stage_everything(data_root: Path, sidecars: tuple[str, ...] = (".shx", ".dbf", ".prj")) -> None:
    for rel in _FIXED:
        _touch(data_root, rel)
    for rel in _PROFILE_KEYS.values():
        _touch(data_root, rel)
    _stage_shapefile(data_root, with_sidecars=sidecars)


def _stage_shapefile(data_root: Path, with_sidecars: tuple[str, ...]) -> Path:
    """Touch Lithology_exp_Konly_Project.shp + the named sidecars."""
    shp = _touch(data_root, "input/soils_litho/Lithology_exp_Konly_Project.shp")
    for ext in with_sidecars:
        (shp.with_suffix(ext)).touch()
    return shp


def _missing(caplog) -> list[str]:
    return [r.message for r in caplog.records if "MISSING" in r.message]


def test_all_required_present_emits_info(tmp_path, caplog):
    """Happy path: every fixed path, every profile path, every sidecar present."""
    _stage_everything(tmp_path)
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_validate_inputs_happy")
    init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logger)
    assert any("All required staged inputs are present" in r.message for r in caplog.records)
    assert not _missing(caplog)


def test_missing_sidecars_reported_individually(tmp_path, caplog):
    """When only .shp is staged, all 3 sidecars (.shx/.dbf/.prj) must be reported."""
    _stage_everything(tmp_path, sidecars=())  # bare .shp, no sidecars
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_missing_sidecars")
    init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logger)
    missing_msgs = _missing(caplog)
    assert any(".shx" in m for m in missing_msgs), missing_msgs
    assert any(".dbf" in m for m in missing_msgs), missing_msgs
    assert any(".prj" in m for m in missing_msgs), missing_msgs
    # .shp itself was staged, so it should NOT be in the missing list
    assert not any(m.endswith(".shp") for m in missing_msgs)


def test_missing_shp_reports_shp_and_all_sidecars(tmp_path, caplog):
    """When the .shp itself is absent, all 4 paths (.shp + .shx/.dbf/.prj) are reported.

    The user needs the full list of files to stage; reporting only the .shp
    would leave them guessing about the sidecars they still need to provide.
    """
    for rel in _FIXED:
        _touch(tmp_path, rel)
    for rel in _PROFILE_KEYS.values():
        _touch(tmp_path, rel)
    # Do NOT stage the shapefile at all
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_missing_shp")
    init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logger)
    missing_msgs = _missing(caplog)
    assert any("Lithology_exp_Konly_Project.shp" in m for m in missing_msgs)
    assert any(".shx" in m for m in missing_msgs)
    assert any(".dbf" in m for m in missing_msgs)
    assert any(".prj" in m for m in missing_msgs)


def test_fixed_path_missing_still_reported(tmp_path, caplog):
    """A fixed, non-profile path (e.g. TEXT_PRMS.tif) missing still triggers a warning."""
    _stage_everything(tmp_path)
    (tmp_path / "input/soils_litho/TEXT_PRMS.tif").unlink()
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_missing_tif")
    init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logger)
    assert any("TEXT_PRMS.tif" in m for m in _missing(caplog))


def test_fixture_key_list_matches_the_shared_tuple():
    """Fixture honesty: a key added to SHARED_DEPSTOR_INPUT_KEYS must be staged and
    probed here too, or the parametrized test below silently never covers it."""
    from gfv2_params.config import SHARED_DEPSTOR_INPUT_KEYS

    assert set(_PROFILE_KEYS) == set(SHARED_DEPSTOR_INPUT_KEYS)
    assert init_data_root._SHARED_INPUT_PROFILE_KEYS == SHARED_DEPSTOR_INPUT_KEYS


@pytest.mark.parametrize("key", sorted(_PROFILE_KEYS))
def test_declared_profile_path_missing_is_reported_at_its_resolved_path(tmp_path, caplog, key):
    """A shared input the profile DECLARES but that is not on disk is reported,
    by the exact path the pipeline will try to open — for every key."""
    _stage_everything(tmp_path)
    (tmp_path / _PROFILE_KEYS[key]).unlink()
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_missing_profile_path")
    missing = init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logger)
    missing_msgs = _missing(caplog)
    assert any(str(tmp_path / _PROFILE_KEYS[key]) in m for m in missing_msgs), missing_msgs
    assert missing == [tmp_path / _PROFILE_KEYS[key]]


def test_returns_an_empty_list_when_everything_is_present(tmp_path):
    """The return value is what --check exits on."""
    _stage_everything(tmp_path)
    assert init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logging.getLogger("t")) == []


def test_undeclared_profile_key_is_not_checked(tmp_path, caplog):
    """A key the active profile does not declare (tjc omits burn_add_waterbody_table)
    is simply not checked: a documented, legitimate omission is not a missing file."""
    _stage_everything(tmp_path)
    (tmp_path / _PROFILE_KEYS["burn_add_waterbody_table"]).unlink()
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_validate_inputs_undeclared")
    init_data_root.validate_inputs(
        tmp_path, _profile(tmp_path, omit=("burn_add_waterbody_table",)), logger
    )
    assert not _missing(caplog)
    assert any("All required staged inputs are present" in r.message for r in caplog.records)


def test_shared_products_every_fabric_reads_are_checked(tmp_path, caplog):
    """fdr.vrt (the clip source) and the hydrodem percentile table (carea_map's
    lookup) have no profile key, but a fabric cannot run without them."""
    _stage_everything(tmp_path)
    (tmp_path / "shared/conus/vrt/fdr.vrt").unlink()
    (tmp_path / "shared/conus/twi_reference_percentiles.hydrodem.csv").unlink()
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_shared_products")
    init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logger)
    missing_msgs = _missing(caplog)
    assert any("fdr.vrt" in m for m in missing_msgs), missing_msgs
    assert any("twi_reference_percentiles.hydrodem.csv" in m for m in missing_msgs), missing_msgs


def test_retired_per_fabric_depstor_sentinel_is_gone(tmp_path, caplog):
    """`input/depstor/<fabric>_segments_wbodies.gpkg` is a retired layout: no
    profile reads it, so --check must not demand it."""
    _stage_everything(tmp_path)
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test_validate_inputs_sentinel")
    init_data_root.validate_inputs(tmp_path, _profile(tmp_path), logger)
    assert not any("segments_wbodies" in m for m in _missing(caplog))
