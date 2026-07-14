"""Unit tests for the NHDPlusBurnComponents staging module (synthetic frames)."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from gfv2_params.download.nhd_burn_components import (
    FTYPE_BY_FCODE_PREFIX,
    PURPCODE_IS_SINK,
    burn_add_to_waterbody_frame,
    normalize_purpcode,
    pick_component_key,
)

CRS = "EPSG:4269"
SQ = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def _baw(rows):
    # BurnAddWaterbody.shp columns as shipped by NHDPlus
    return gpd.GeoDataFrame(
        rows, columns=["PolyID", "PurpCode", "PurpDesc", "OnOffNet", "FCode", "geometry"],
        crs=CRS,
    )


def test_pick_component_key_takes_the_highest_version():
    keys = [
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_NHDPlusBurnComponents_01.7z",
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_NHDPlusBurnComponents_02.7z",
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_NHDSnapshot_08.7z",
    ]
    key = pick_component_key(keys, "16")
    assert key.endswith("NHDPlusBurnComponents_02.7z")


def test_pick_component_key_returns_none_when_absent():
    assert pick_component_key(["some/other/file.7z"], "16") is None


def test_burn_add_maps_purpcode_to_ftype():
    g = _baw([[-367111, "4", "BurnAddWaterbody Playa", 1, 36100, SQ],
              [-367116, "8", "BurnAddWaterbody closed lake", 1, 39001, SQ]])
    out = burn_add_to_waterbody_frame(g)
    assert list(out.FTYPE) == ["Playa", "LakePond"]
    # PolyID is the (negative) COMID, and member_comid mirrors it so
    # depstor.select_connected_waterbodies can join without a KeyError.
    assert list(out.COMID) == [-367111, -367116]
    assert list(out.member_comid) == [-367111, -367116]
    assert set(out.columns) >= {
        "GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm", "geometry",
    }


def test_burn_add_comids_are_all_negative():
    # The negative PolyID is what makes BurnAdd waterbodies structurally
    # incapable of matching a WBAREACOMI / flow-through COMID.
    g = _baw([[-367111, "4", "BurnAddWaterbody Playa", 1, 36100, SQ]])
    assert (burn_add_to_waterbody_frame(g).COMID < 0).all()


def test_burn_add_fails_loud_on_positive_polyid():
    # A positive PolyID could collide with a real WBAREACOMI/flow-through
    # COMID and get promoted on-stream, even though NHDPlus flagged every
    # BurnAddWaterbody as a sink. This is a hard constraint of the task, so the
    # guard must actually fire on non-negative input — not just pass by
    # construction on fixtures that are always negative.
    g = _baw([[367111, "4", "BurnAddWaterbody Playa", 1, 36100, SQ]])
    with pytest.raises(ValueError, match="expected to be negative"):
        burn_add_to_waterbody_frame(g)


def test_burn_add_resolves_fields_case_insensitively():
    # Sink.shp/BurnAddWaterbody.shp are the same class of raw per-VPU NHDPlus
    # shapefile as NHDFlowline/PlusFlowlineVAA, where field casing is known to
    # vary across VPUs (VPU 12 ships COMID/WBAREACOMI, VPU 13 ships
    # ComID/WBAreaComI). Lower-case columns here stand in for that drift.
    g = gpd.GeoDataFrame(
        [[-367111, 4, 36100, SQ]],
        columns=["polyid", "purpcode", "fcode", "geometry"], crs=CRS,
    )
    out = burn_add_to_waterbody_frame(g)
    assert list(out.COMID) == [-367111]
    assert list(out.FTYPE) == ["Playa"]


def test_burn_add_fails_loud_on_missing_required_field():
    # A genuinely missing field must raise a descriptive error naming the
    # field and the available columns, not a bare pandas KeyError 15 VPUs
    # into a CONUS run.
    g = gpd.GeoDataFrame(
        [[-367111, 36100, SQ]], columns=["PolyID", "FCode", "geometry"], crs=CRS
    )
    with pytest.raises(KeyError, match="PurpCode"):
        burn_add_to_waterbody_frame(g)


def test_burn_add_fails_loud_on_unknown_purpcode():
    # An unrecognised but POPULATED PurpCode must NOT default to a FTYPE: FTYPE drives
    # NEVER_ONSTREAM_FTYPES, so a mis-defaulted Playa becomes promotable on-stream.
    g = _baw([[-1, "99", "Something New", 1, 39001, SQ]])
    with pytest.raises(ValueError, match="unrecognised PurpCode"):
        burn_add_to_waterbody_frame(g)


def test_burn_add_drops_null_purpcode_rows_instead_of_raising():
    # BurnAddWaterbody is NOT a sink layer. VPU 01 ships 702 rows whose PurpCode /
    # PurpDesc are entirely NULL against ZERO sinks in its own Sink.shp; 503 of them
    # are ON-network and their FCodes include 46006 (StreamRiver) and 33600
    # (CanalDitch). A NULL PurpCode means "added to the DEM burn, not a sink" — it
    # must be DROPPED (it used to crash: `int()` on None), never merged, or canals
    # and river reaches become depression storage.
    g = _baw([[-367101, None, None, 1, 46006, SQ],     # on-network StreamRiver
              [-367102, None, None, 1, 33600, SQ]])    # on-network CanalDitch
    out = burn_add_to_waterbody_frame(g)
    assert len(out) == 0
    assert list(out.columns) == [
        "GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm",
        "geometry",
    ]


def test_burn_add_keeps_only_sink_purpose_rows_in_a_mixed_frame():
    # A frame carrying both sink-purpose rows (PurpCode 4/8) and NULL-PurpCode rows
    # keeps exactly the sink-purpose ones.
    g = _baw([[-367111, "4", "BurnAddWaterbody Playa", 1, 36100, SQ],
              [-367112, None, None, 1, 46006, SQ],
              [-367113, "8", "BurnAddWaterbody closed lake", 0, 39001, SQ],
              [-367114, None, None, 1, 39004, SQ],
              [-320005, "NT", "Canada NTDB", 1, 39004, SQ]])
    out = burn_add_to_waterbody_frame(g)
    assert list(out.COMID) == [-367111, -367113]
    assert list(out.FTYPE) == ["Playa", "LakePond"]


def test_burn_add_still_raises_on_a_populated_unknown_code_beside_nulls():
    # Dropping NULLs must not soften the guard on a populated unrecognised code.
    g = _baw([[-1, None, None, 1, 46006, SQ],
              [-2, "99", "Something New", 1, 39001, SQ]])
    with pytest.raises(ValueError, match="unrecognised PurpCode"):
        burn_add_to_waterbody_frame(g)


def test_purpcode_sink_table_matches_the_measured_conus_domain():
    # The complete PurpCode domain across all 21 CONUS VPU archives. 4/5/8 are the
    # closed-lake/playa (sink) purposes; NT is a provenance tag on Canadian NTDB fill
    # polygons (one of the three is a 26.7 km2 StreamRiver), not a sink purpose.
    assert PURPCODE_IS_SINK == {"4": True, "5": True, "8": True, "NT": False}


def test_normalize_purpcode_handles_the_mixed_domain():
    # PurpCode arrives as int, float or str across VPUs, and "NT" is a real code.
    assert normalize_purpcode(4) == "4"
    assert normalize_purpcode(4.0) == "4"
    assert normalize_purpcode(" 4 ") == "4"
    assert normalize_purpcode("nt") == "NT"
    assert normalize_purpcode(None) is None
    assert normalize_purpcode(float("nan")) is None
    assert normalize_purpcode("") is None


def test_burn_add_keeps_purpcode_5_and_takes_ftype_from_fcode():
    # PurpCode 5 ("NHD Waterbody closed lake") is a sink purpose and IS new depression
    # area: measured on the real archives, all 21 of its polygons have 0.000 area
    # overlap with conus_waterbodies.gpkg. Its FCodes span BOTH Playa (36100, x17 in
    # VPU 15) and SwampMarsh (46600, x4 in VPU 03S), so FTYPE must come from FCODE —
    # a PurpCode->FTYPE map would mislabel 17 real Rio Grande playas as lakes.
    g = _baw([[-408334, "5", "NHD Waterbody closed lake", 0, 36100, SQ],
              [-413263, "5", "NHD Waterbody closed lake", 0, 46600, SQ]])
    out = burn_add_to_waterbody_frame(g)
    assert list(out.FTYPE) == ["Playa", "SwampMarsh"]


def test_burn_add_refuses_a_sink_purpose_conveyance_row():
    # The whole point of the PurpCode gate is that a river reach / canal must never
    # become depression storage. If a future archive ever tags one with a sink
    # PurpCode, fail loud rather than merge it.
    g = _baw([[-1, "8", "BurnAddWaterbody closed lake", 1, 46006, SQ]])  # StreamRiver
    with pytest.raises(ValueError, match="conveyance FTYPE"):
        burn_add_to_waterbody_frame(g)


def test_burn_add_fails_loud_on_unknown_fcode():
    g = _baw([[-1, "8", "BurnAddWaterbody closed lake", 1, 99999, SQ]])
    with pytest.raises(ValueError, match="not in FTYPE_BY_FCODE_PREFIX"):
        burn_add_to_waterbody_frame(g)


def test_fcode_prefix_table_covers_the_measured_waterbody_ftypes():
    assert FTYPE_BY_FCODE_PREFIX[361] == "Playa"
    assert FTYPE_BY_FCODE_PREFIX[390] == "LakePond"
    assert FTYPE_BY_FCODE_PREFIX[466] == "SwampMarsh"
    assert FTYPE_BY_FCODE_PREFIX[378] == "Ice Mass"
