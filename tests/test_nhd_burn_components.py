"""Unit tests for the NHDPlusBurnComponents staging module (synthetic frames)."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from gfv2_params.download.nhd_burn_components import (
    PURPCODE_TO_FTYPE,
    burn_add_to_waterbody_frame,
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
    g = _baw([[-367111, 4, "BurnAddWaterbody Playa", 1, 36100, SQ],
              [-367116, 8, "BurnAddWaterbody closed lake", 1, 39001, SQ]])
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
    g = _baw([[-367111, 4, "BurnAddWaterbody Playa", 1, 36100, SQ]])
    assert (burn_add_to_waterbody_frame(g).COMID < 0).all()


def test_burn_add_fails_loud_on_unknown_purpcode():
    # An unrecognised PurpCode must NOT default to a FTYPE: FTYPE drives
    # NEVER_ONSTREAM_FTYPES, so a mis-defaulted Playa becomes promotable on-stream.
    g = _baw([[-1, 99, "Something New", 1, 12345, SQ]])
    with pytest.raises(ValueError, match="unrecognised PurpCode"):
        burn_add_to_waterbody_frame(g)


def test_purpcode_table_is_exactly_playa_and_lakepond():
    assert PURPCODE_TO_FTYPE == {4: "Playa", 8: "LakePond"}
