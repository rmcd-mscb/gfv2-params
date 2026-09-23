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
