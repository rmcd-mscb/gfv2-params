"""Tests for scripts/check_fabric_profile.py.

The validator is the step between "I filled in the profile" and "I submitted a
multi-hour SLURM chain against it". It checks the profile against the gpkg it
names, collects every failure rather than stopping at the first, and exits 1
on any failure. The checks it makes are exactly the mistakes that corrupt a
new fabric silently: an id column with gaps (gap-fill invents phantom HRUs), a
wrong expected_max_hru_id, a layer name that does not exist, a vpu that cannot
resolve (vpu_id raises hours in), a declared input that is not on disk.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import geopandas as gpd
import yaml
from shapely.geometry import LineString, box

_REPO_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "check_fabric_profile", _REPO_ROOT / "scripts" / "check_fabric_profile.py"
)
check_fabric_profile = importlib.util.module_from_spec(_spec)
sys.modules["check_fabric_profile"] = check_fabric_profile
_spec.loader.exec_module(check_fabric_profile)

run_checks = check_fabric_profile.run_checks


def _write_fabric(
    path: Path,
    ids: list[int],
    id_col: str = "hru_id",
    vpu_attr: list[str] | None = None,
    with_segments: bool = True,
) -> Path:
    data = {id_col: ids}
    if vpu_attr is not None:
        data["vpu"] = vpu_attr
    hru = gpd.GeoDataFrame(
        data, geometry=[box(i, 0, i + 1, 1) for i in range(len(ids))], crs="EPSG:5070"
    )
    hru.to_file(path, layer="nhru", driver="GPKG")
    if with_segments:
        seg = gpd.GeoDataFrame(
            {"seg_id": [1]}, geometry=[LineString([(0, 0.5), (len(ids), 0.5)])], crs="EPSG:5070"
        )
        seg.to_file(path, layer="nsegment", driver="GPKG", mode="a")
    return path


def _profile(tmp_path: Path, gpkg: Path, **overrides) -> dict:
    """A resolved profile (what load_base_config returns) for the gpkg."""
    cfg = {
        "data_root": str(tmp_path),
        "fabric": "demo",
        "expected_max_hru_id": 3,
        "batch_size": 10000,
        "id_feature": "hru_id",
        "hru_gpkg": str(gpkg),
        "hru_layer": "nhru",
        "segments_gpkg": str(gpkg),
        "segments_layer": "nsegment",
        "vpu": "14",
    }
    cfg.update(overrides)
    return cfg


def _failed(results) -> dict[str, str]:
    return {r.name: r.detail for r in results if not r.ok}


def test_a_correct_profile_passes_every_check(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    results = run_checks(_profile(tmp_path, gpkg))
    assert results, "no checks ran"
    assert _failed(results) == {}


def test_duplicate_ids_fail(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 2])
    failed = _failed(run_checks(_profile(tmp_path, gpkg)))
    assert "id column unique" in failed
    assert "2" in failed["id column unique"]


def test_a_gap_in_the_id_sequence_fails(tmp_path):
    """ids 1,2,4 with expected_max 4: gap-fill would synthesize HRU 3."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 4])
    failed = _failed(run_checks(_profile(tmp_path, gpkg, expected_max_hru_id=4)))
    assert "id column contiguous 1..N" in failed
    assert "3" in failed["id column contiguous 1..N"]


def test_expected_max_hru_id_mismatch_fails(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    failed = _failed(run_checks(_profile(tmp_path, gpkg, expected_max_hru_id=1745)))
    assert "expected_max_hru_id matches" in failed
    assert "1745" in failed["expected_max_hru_id matches"]
    assert "3" in failed["expected_max_hru_id matches"]


def test_placeholder_expected_max_of_zero_fails(tmp_path):
    """The stub ships expected_max_hru_id: 0; leaving it is the most likely slip."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    failed = _failed(run_checks(_profile(tmp_path, gpkg, expected_max_hru_id=0)))
    assert "expected_max_hru_id matches" in failed


def test_missing_id_column_fails_without_crashing_later_checks(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], id_col="nat_hru_id")
    results = run_checks(_profile(tmp_path, gpkg))  # profile still says hru_id
    failed = _failed(results)
    assert "id column present" in failed
    assert "hru_id" in failed["id column present"]
    # the vpu check still ran and passed; the run did not abort at the id check
    assert any(r.name == "vpu resolves" and r.ok for r in results)


def test_missing_hru_layer_fails(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    failed = _failed(run_checks(_profile(tmp_path, gpkg, hru_layer="hrus")))
    assert "hru_layer present" in failed
    assert "hrus" in failed["hru_layer present"]
    assert "nhru" in failed["hru_layer present"]  # tells the user what IS there


def test_missing_segments_layer_fails(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], with_segments=False)
    failed = _failed(run_checks(_profile(tmp_path, gpkg)))
    assert "segments_layer present" in failed


def test_missing_hru_gpkg_fails_and_names_the_path(tmp_path):
    missing = tmp_path / "nope.gpkg"
    failed = _failed(run_checks(_profile(tmp_path, missing)))
    assert "hru_gpkg exists" in failed
    assert str(missing) in failed["hru_gpkg exists"]


def test_vpu_placeholder_fails(tmp_path):
    """The stub ships vpu: "00"; it is not a VPU and vpu_id would raise hours in."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    failed = _failed(run_checks(_profile(tmp_path, gpkg, vpu="00")))
    assert "vpu resolves" in failed


def test_no_vpu_scalar_and_no_vpu_attribute_fails(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    cfg = _profile(tmp_path, gpkg)
    del cfg["vpu"]
    failed = _failed(run_checks(cfg))
    assert "vpu resolves" in failed


def test_no_vpu_scalar_but_a_vpu_attribute_passes(tmp_path):
    """The gfv2 pattern: a multi-VPU fabric carries a per-HRU vpu column."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], vpu_attr=["14", "14", "15"])
    cfg = _profile(tmp_path, gpkg)
    del cfg["vpu"]
    assert "vpu resolves" not in _failed(run_checks(cfg))


def test_declared_input_path_missing_fails_and_undeclared_is_not_checked(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    present = tmp_path / "us_eco_l3.gpkg"
    present.touch()
    cfg = _profile(
        tmp_path, gpkg,
        ecoregions_gpkg=str(present),
        wbd_huc12_table=str(tmp_path / "missing.parquet"),
    )
    results = run_checks(cfg)
    failed = _failed(results)
    assert "wbd_huc12_table exists" in failed
    assert str(tmp_path / "missing.parquet") in failed["wbd_huc12_table exists"]
    assert "ecoregions_gpkg exists" not in failed
    # burn_add_waterbody_table is not declared, so nothing checks it
    assert not any(r.name.startswith("burn_add_waterbody_table") for r in results)


def test_main_exits_nonzero_on_failure_and_zero_on_success(tmp_path, capsys):
    _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    base = tmp_path / "base_config.yml"
    base.write_text(yaml.safe_dump({
        "data_root": str(tmp_path),
        "default_fabric": "demo",
        "fabrics": {"demo": {
            "expected_max_hru_id": 99,
            "batch_size": 10000,
            "id_feature": "hru_id",
            "hru_gpkg": "{data_root}/demo.gpkg",
            "hru_layer": "nhru",
            "segments_gpkg": "{data_root}/demo.gpkg",
            "segments_layer": "nsegment",
            "vpu": "14",
        }},
    }))
    rc = check_fabric_profile.main(["--fabric", "demo", "--base_config", str(base)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "FAIL" in out and "expected_max_hru_id matches" in out
    assert "PASS" in out  # the passing checks are listed too

    cfg = yaml.safe_load(base.read_text())
    cfg["fabrics"]["demo"]["expected_max_hru_id"] = 3
    base.write_text(yaml.safe_dump(cfg))
    rc = check_fabric_profile.main(["--fabric", "demo", "--base_config", str(base)])
    assert rc == 0
    assert "FAIL" not in capsys.readouterr().out


def test_a_text_id_column_fails_instead_of_crashing(tmp_path):
    """A string id column must come back as a FAIL line, not a traceback:
    every check runs so the whole punch-list comes out of one run."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", ["a", "b", "c"])
    results = run_checks(_profile(tmp_path, gpkg))
    failed = _failed(results)
    assert "id column is integer" in failed
    assert "a" in failed["id column is integer"]
    assert any(r.name == "vpu resolves" for r in results)  # later checks still ran


def test_an_all_null_id_column_fails_instead_of_crashing(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [None, None, None])
    results = run_checks(_profile(tmp_path, gpkg))
    failed = _failed(results)
    assert "id column has no nulls" in failed
    assert "id column contiguous 1..N" in failed
    assert any(r.name == "vpu resolves" for r in results)


def test_an_empty_hru_layer_fails_instead_of_crashing(tmp_path):
    gpkg = tmp_path / "demo.gpkg"
    empty = gpd.GeoDataFrame({"hru_id": []}, geometry=gpd.GeoSeries([], crs="EPSG:5070"))
    empty.to_file(gpkg, layer="nhru", driver="GPKG")
    results = run_checks(_profile(tmp_path, gpkg, segments_gpkg=None))
    failed = _failed(results)
    assert "id column contiguous 1..N" in failed
    assert "expected_max_hru_id matches" in failed
    assert any(r.name == "vpu resolves" for r in results)


def test_a_bad_value_in_the_vpu_attribute_fails(tmp_path):
    """The multi-VPU path: vpu_id calls vpu_to_code on every row, so one bad
    value raises hours into the depstor stack. Check the distinct values now."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], vpu_attr=["14", "14", "99"])
    cfg = _profile(tmp_path, gpkg)
    del cfg["vpu"]
    failed = _failed(run_checks(cfg))
    assert "vpu resolves" in failed
    assert "99" in failed["vpu resolves"]


def test_sub_region_labels_in_the_vpu_attribute_pass(tmp_path):
    """03N / 10U map to their parent raster VPU, exactly as vpu_to_code does."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], vpu_attr=["03N", "10U", "10L"])
    cfg = _profile(tmp_path, gpkg)
    del cfg["vpu"]
    assert "vpu resolves" not in _failed(run_checks(cfg))


# --- review round 2: nothing may escape as a traceback, nothing may mask a FAIL ---


def test_a_null_in_the_vpu_attribute_fails(tmp_path):
    """vpu_id calls vpu_to_code on EVERY row, nulls included; a null raises there.
    Dropping nulls before the check would report PASS for a fabric that fails
    at step 11 of the depstor stack."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], vpu_attr=["14", None, "14"])
    cfg = _profile(tmp_path, gpkg)
    del cfg["vpu"]
    failed = _failed(run_checks(cfg))
    assert "vpu resolves" in failed
    assert "null" in failed["vpu resolves"]


def test_an_unreadable_hru_gpkg_fails_instead_of_crashing(tmp_path):
    """A 0-byte file (an interrupted cp in Step 2) is not a gpkg."""
    gpkg = tmp_path / "demo.gpkg"
    gpkg.touch()
    results = run_checks(_profile(tmp_path, gpkg, segments_gpkg=None))
    failed = _failed(results)
    assert "hru_gpkg readable" in failed
    assert any(r.name == "vpu resolves" for r in results)


def test_an_unreadable_segments_gpkg_fails_and_the_id_checks_still_run(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], with_segments=False)
    broken = tmp_path / "segments.gpkg"
    broken.write_text("not a geopackage")
    results = run_checks(_profile(tmp_path, gpkg, segments_gpkg=str(broken)))
    failed = _failed(results)
    assert "segments_gpkg readable" in failed
    assert any(r.name == "id column contiguous 1..N" and r.ok for r in results)


def test_an_empty_segments_layer_fails(tmp_path):
    """A present-but-empty nsegment layer makes every waterbody depression storage."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3], with_segments=False)
    empty = gpd.GeoDataFrame({"seg_id": []}, geometry=gpd.GeoSeries([], crs="EPSG:5070"))
    empty.to_file(gpkg, layer="nsegment", driver="GPKG", mode="a")
    failed = _failed(run_checks(_profile(tmp_path, gpkg)))
    assert "segments_layer has features" in failed


def test_missing_id_feature_key_fails_instead_of_crashing(tmp_path):
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    cfg = _profile(tmp_path, gpkg)
    del cfg["id_feature"]
    results = run_checks(cfg)
    assert "id_feature declared" in _failed(results)
    assert any(r.name == "vpu resolves" for r in results)


def test_missing_hru_gpkg_key_fails_instead_of_crashing(tmp_path):
    cfg = _profile(tmp_path, tmp_path / "demo.gpkg")
    del cfg["hru_gpkg"]
    results = run_checks(cfg)
    assert "hru_gpkg declared" in _failed(results)
    assert results


def test_missing_hru_layer_or_segments_gpkg_key_fails(tmp_path):
    """The pipeline requires both (require_config_key / segment_wbody raises);
    the validator must not default them and report PASS."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    cfg = _profile(tmp_path, gpkg)
    del cfg["hru_layer"]
    assert "hru_layer declared" in _failed(run_checks(cfg))
    cfg = _profile(tmp_path, gpkg)
    del cfg["segments_gpkg"]
    assert "segments_gpkg declared" in _failed(run_checks(cfg))


def test_a_null_path_value_fails_instead_of_crashing(tmp_path):
    """A user who blanks `twi_raster:` in the profile gets a FAIL line, not a TypeError,
    and the remaining path checks still run."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    present = tmp_path / "us_eco_l3.gpkg"
    present.touch()
    cfg = _profile(tmp_path, gpkg, twi_raster=None, ecoregions_gpkg=str(present))
    results = run_checks(cfg)
    failed = _failed(results)
    assert "twi_raster declared" in failed
    assert any(r.name == "ecoregions_gpkg exists" and r.ok for r in results)


def test_expected_max_hru_id_must_be_a_plain_integer(tmp_path):
    """1745.0 passes `==` but breaks range() at the fill stage; "1745" (quoted like vpu)
    fails the comparison with a message showing two equal numbers."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    for bad in (3.0, "3", True):
        failed = _failed(run_checks(_profile(tmp_path, gpkg, expected_max_hru_id=bad)))
        assert "expected_max_hru_id is an integer" in failed, bad


def test_every_declared_path_key_missing_on_disk_is_reported(tmp_path):
    """Including template_raster / fdr_raster: the case a user who skipped the clip step hits."""
    gpkg = _write_fabric(tmp_path / "demo.gpkg", [1, 2, 3])
    keys = check_fabric_profile._PATH_KEYS
    cfg = _profile(tmp_path, gpkg, **{k: str(tmp_path / f"{k}.missing") for k in keys})
    failed = _failed(run_checks(cfg))
    for k in keys:
        assert f"{k} exists" in failed, k


def test_main_reports_an_unknown_fabric_as_a_fail_line(tmp_path, capsys):
    base = tmp_path / "base_config.yml"
    base.write_text(yaml.safe_dump({"data_root": str(tmp_path), "fabrics": {"demo": {}}}))
    rc = check_fabric_profile.main(["--fabric", "nope", "--base_config", str(base)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "FAIL" in out and "nope" in out


def test_every_repo_profile_runs_without_raising():
    """Every fabric in the real configs/base_config.yml goes through run_checks.

    In CI the data root does not exist, so paths FAIL; the assertion is that no
    profile makes the validator raise instead of reporting.
    """
    from gfv2_params.config import load_base_config

    base = _REPO_ROOT / "configs" / "base_config.yml"
    fabrics = yaml.safe_load(base.read_text())["fabrics"]
    for fabric in fabrics:
        results = run_checks(load_base_config(base, fabric=fabric))
        assert results, fabric
