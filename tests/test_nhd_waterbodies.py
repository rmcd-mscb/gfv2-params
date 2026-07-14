"""Unit tests for the NHDWaterbody staging module (synthetic frames, no network)."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import box

from gfv2_params.download.nhd_waterbodies import (
    OUTPUT_COLUMNS,
    dedupe_cross_vpu_duplicates,
    dissolve_named_parts,
    download_waterbody_snapshot,
    read_waterbody_attrs,
)

CRS = "EPSG:5070"


def _wb(rows):
    return gpd.GeoDataFrame(
        rows,
        columns=["COMID", "GNIS_ID", "GNIS_NAME", "FTYPE", "geometry"],
        crs=CRS,
    )


def test_read_waterbody_attrs_resolves_fields_case_insensitively(tmp_path):
    # NHD field-name casing varies across VPU snapshots (VPU 12 ships COMID,
    # VPU 13 ships ComID). Lower-case columns here stand in for that drift.
    gdf = gpd.GeoDataFrame(
        {
            "comid": [1],
            "gnis_id": ["123"],
            "gnis_name": ["Test Lake"],
            "ftype": ["LakePond"],
        },
        geometry=[box(0, 0, 1, 1)],
        crs=CRS,
    )
    src = tmp_path / "NHDWaterbody.gpkg"
    gdf.to_file(src, driver="GPKG")

    out = read_waterbody_attrs(src)
    assert set(out.columns) >= {"COMID", "GNIS_ID", "GNIS_NAME", "FTYPE"}
    assert out["COMID"].iloc[0] == 1
    assert out["FTYPE"].iloc[0] == "LakePond"


def test_read_waterbody_attrs_fails_loud_on_missing_field(tmp_path):
    gdf = gpd.GeoDataFrame(
        {"comid": [1], "gnis_id": ["123"], "ftype": ["LakePond"]},
        geometry=[box(0, 0, 1, 1)],
        crs=CRS,
    )
    src = tmp_path / "NHDWaterbody.gpkg"
    gdf.to_file(src, driver="GPKG")
    with pytest.raises(KeyError, match="GNIS_NAME"):
        read_waterbody_attrs(src)


def test_download_waterbody_snapshot_derives_sibling_shapefile(tmp_path, monkeypatch):
    # NHDWaterbody.shp lives beside NHDFlowline.shp in the same extracted
    # NHDSnapshot dir; the download/extract itself is nhd_flowlines' job.
    hydro_dir = tmp_path / "16" / "NHDSnapshot" / "Hydrography"
    hydro_dir.mkdir(parents=True)
    flowline = hydro_dir / "NHDFlowline.shp"
    flowline.touch()
    (hydro_dir / "NHDWaterbody.shp").touch()

    monkeypatch.setattr(
        "gfv2_params.download.nhd_waterbodies._download_flowline_snapshot",
        lambda dd, vpu, download_dir, extract_dir: flowline,
    )
    got = download_waterbody_snapshot("GB", "16", tmp_path / "dl", tmp_path / "ex")
    assert got == hydro_dir / "NHDWaterbody.shp"


def test_download_waterbody_snapshot_none_when_flowline_download_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "gfv2_params.download.nhd_waterbodies._download_flowline_snapshot",
        lambda dd, vpu, download_dir, extract_dir: None,
    )
    assert download_waterbody_snapshot("GB", "16", tmp_path / "dl", tmp_path / "ex") is None


def test_download_waterbody_snapshot_none_when_waterbody_shp_missing(tmp_path, monkeypatch):
    hydro_dir = tmp_path / "16" / "NHDSnapshot" / "Hydrography"
    hydro_dir.mkdir(parents=True)
    flowline = hydro_dir / "NHDFlowline.shp"
    flowline.touch()
    # No NHDWaterbody.shp written alongside it.

    monkeypatch.setattr(
        "gfv2_params.download.nhd_waterbodies._download_flowline_snapshot",
        lambda dd, vpu, download_dir, extract_dir: flowline,
    )
    assert download_waterbody_snapshot("GB", "16", tmp_path / "dl", tmp_path / "ex") is None


def test_dissolve_passes_through_unnamed_waterbodies_unchanged():
    g = _wb([
        [1, None, None, "LakePond", box(0, 0, 1, 1)],
        [2, "", "", "LakePond", box(10, 10, 11, 11)],
    ])
    out = dissolve_named_parts(g)
    assert list(out.columns) == OUTPUT_COLUMNS
    assert list(out["COMID"]) == [1, 2]
    assert list(out["member_comid"]) == ["1", "2"]


def test_dissolve_passes_through_singleton_named_group_unchanged():
    g = _wb([[1, "123", "Test Lake", "LakePond", box(0, 0, 1, 1)]])
    out = dissolve_named_parts(g)
    assert len(out) == 1
    assert out["COMID"].iloc[0] == 1
    assert out["member_comid"].iloc[0] == "1"


def test_dissolve_merges_touching_same_gnis_id_parts_keeping_largest_area_comid():
    # Mirrors the real Mono Lake case: two touching parts, same GNIS_ID, the
    # larger-area member's COMID and geometry-derived area_sqkm are retained.
    big = box(0, 0, 10, 10)        # area 100
    small = box(10, 0, 12, 10)     # area 20, touches `big` along x=10
    g = _wb([
        [120053921, "263749", "Mono Lake", "LakePond", big],
        [20286504, "263749", "Mono Lake", "LakePond", small],
    ])
    out = dissolve_named_parts(g)
    assert len(out) == 1
    row = out.iloc[0]
    assert row["COMID"] == 120053921  # larger-area member retained
    assert row["member_comid"] == "20286504,120053921"  # ascending numeric order
    assert row["area_sqkm"] == pytest.approx(120.0 / 1e6)
    assert row["GNIS_NAME"] == "Mono Lake"


def test_dissolve_merges_non_touching_same_gnis_id_parts_within_one_vpu():
    # Real CONUS case (Lake Conroe, VPU 12, GNIS_ID 1380953): two parts 662.8 m
    # apart (measured touches=False, intersects=False) are still merged in the
    # existing hand-made layer. GNIS_ID alone is the merge criterion WITHIN a
    # single VPU's frame -- there is no spatial-adjacency test. (Separation
    # across VPU archives, e.g. the 14 residual same-GNIS_ID pairs in the real
    # layer, comes entirely from `main()` calling this function once per VPU,
    # never from anything inside this function.)
    far_apart_small = box(0, 0, 1, 1)
    far_apart_big = box(1000, 1000, 1020, 1000.5)  # area 10, nowhere near `small`
    g = _wb([
        [1466730, "1380953", "Lake Conroe", "LakePond", far_apart_small],
        [120053033, "1380953", "Lake Conroe", "LakePond", far_apart_big],
    ])
    out = dissolve_named_parts(g)
    assert len(out) == 1
    assert out["COMID"].iloc[0] == 120053033  # larger-area member retained
    assert out["member_comid"].iloc[0] == "1466730,120053033"


def test_dissolve_merges_a_larger_group_regardless_of_layout():
    # Mirrors the real Clear Lake case: 6 parts under one GNIS_ID, all merged.
    parts = [box(i, 0, i + 1, 1) for i in range(6)]  # each touches its neighbour
    rows = [
        [100 + i, "1664234", "Clear Lake", "LakePond", parts[i]] for i in range(6)
    ]
    g = _wb(rows)
    out = dissolve_named_parts(g)
    assert len(out) == 1
    assert out["member_comid"].iloc[0] == "100,101,102,103,104,105"


def test_dissolve_resolves_mismatched_ftype_within_a_group_via_largest_area():
    # Real CONUS case: Lake Oahe (GNIS_ID 1266878) mixes two small LakePond parts
    # with a much larger Reservoir part; the existing hand-made layer resolves
    # this the same way it resolves COMID/GNIS_NAME -- largest-area member wins,
    # not a raise.
    small = box(0, 0, 1, 1)          # area 1
    big = box(1, 0, 21, 1)           # area 20 -- dominant member
    g = _wb([
        [1, "999", "Odd Lake", "LakePond", small],
        [2, "999", "Odd Lake", "Reservoir", big],
    ])
    out = dissolve_named_parts(g)
    assert len(out) == 1
    assert out["FTYPE"].iloc[0] == "Reservoir"
    assert out["COMID"].iloc[0] == 2


def test_dissolve_output_schema_matches_the_consumer_contract():
    g = _wb([[1, "123", "Test Lake", "LakePond", box(0, 0, 1, 1)]])
    out = dissolve_named_parts(g)
    assert list(out.columns) == [
        "GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm", "geometry",
    ]
    assert out.crs == g.crs


def test_dedupe_collapses_identical_cross_vpu_boundary_seam_copies():
    # NHDPlus ships a boundary-straddling waterbody into BOTH adjacent
    # drainage-area archives (measured: VPU 04/07 seam, 80 identical COMIDs).
    shape = box(0, 0, 1, 1)
    g = _wb([
        [1, None, None, "SwampMarsh", shape],
        [1, None, None, "SwampMarsh", shape],  # exact duplicate from the other VPU
        [2, None, None, "LakePond", box(10, 10, 11, 11)],
    ])
    out = dedupe_cross_vpu_duplicates(g)
    assert sorted(out["COMID"]) == [1, 2]


def test_dedupe_is_a_noop_with_no_duplicates():
    g = _wb([[1, None, None, "LakePond", box(0, 0, 1, 1)]])
    out = dedupe_cross_vpu_duplicates(g)
    assert len(out) == 1


def test_dedupe_raises_on_a_genuinely_conflicting_duplicate():
    # Same COMID, but the two copies disagree (different FTYPE) -- this is NOT
    # the benign boundary-seam case and must not be silently resolved by
    # picking one copy.
    shape = box(0, 0, 1, 1)
    g = _wb([
        [1, None, None, "LakePond", shape],
        [1, None, None, "SwampMarsh", shape],
    ])
    with pytest.raises(ValueError, match="NOT identical copies"):
        dedupe_cross_vpu_duplicates(g)


def test_dedupe_raises_on_mismatched_geometry_for_a_duplicate_comid():
    g = _wb([
        [1, None, None, "LakePond", box(0, 0, 1, 1)],
        [1, None, None, "LakePond", box(0, 0, 2, 2)],  # different extent
    ])
    with pytest.raises(ValueError, match="NOT identical copies"):
        dedupe_cross_vpu_duplicates(g)


def test_dissolve_keeps_resolving_a_benign_mixed_ftype_group_by_largest_area():
    # The real Lake Oahe case (GNIS_ID 1266878): two small LakePond parts and a
    # dominant Reservoir part. Neither FTYPE is a guardrail, so largest-area-wins is
    # the right resolution and must NOT raise.
    big = box(0, 0, 10, 10)     # Reservoir, area 100
    small = box(10, 0, 11, 10)  # LakePond, area 10
    g = _wb([
        [19251179, "1266878", "Lake Oahe", "Reservoir", big],
        [19247123, "1266878", "Lake Oahe", "LakePond", small],
    ])
    out = dissolve_named_parts(g)
    assert len(out) == 1
    assert out["FTYPE"].iloc[0] == "Reservoir"  # largest-area member wins


def test_dissolve_refuses_to_retag_a_playa_out_of_a_mixed_ftype_group():
    """A dissolve must not strip the Playa force-dprst guardrail.

    FTYPE is not cosmetic for NEVER_ONSTREAM_FTYPES: Playa is force-dprst and Ice Mass
    is excluded from the waterbody classification entirely. Largest-area-wins would
    retag a small Playa dissolved into a bigger LakePond as LakePond, silently making
    that depression area eligible for on-stream promotion via WBAREACOMI -- logged at
    INFO, inside a run emitting hundreds of thousands of lines. `ftype_for_fcode`
    already refuses to GUESS an FTYPE for this exact reason.
    """
    big = box(0, 0, 10, 10)     # LakePond, area 100
    playa = box(10, 0, 11, 10)  # Playa, area 10 -- would be retagged LakePond
    g = _wb([
        [111, "999", "Mixed Basin", "LakePond", big],
        [222, "999", "Mixed Basin", "Playa", playa],
    ])
    with pytest.raises(ValueError, match="guardrail FTYPE"):
        dissolve_named_parts(g)


def test_dissolve_refuses_to_retag_an_ice_mass_out_of_a_mixed_ftype_group():
    big = box(0, 0, 10, 10)
    ice = box(10, 0, 11, 10)
    g = _wb([
        [111, "999", "Mixed", "LakePond", big],
        [222, "999", "Mixed", "Ice Mass", ice],
    ])
    with pytest.raises(ValueError, match="guardrail FTYPE"):
        dissolve_named_parts(g)
