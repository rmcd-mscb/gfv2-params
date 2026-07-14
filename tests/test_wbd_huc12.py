"""Unit tests for the WBD HUC12 staging module (synthetic frames)."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from gfv2_params.download.wbd_huc12 import closed_basin_frame, pick_wbd_key

CRS = "EPSG:4269"
SQ = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def _wbd(rows):
    return gpd.GeoDataFrame(
        rows, columns=["HUC_12", "HU_12_TYPE", "NCONTRB_A", "geometry"], crs=CRS
    )


def test_pick_wbd_key_takes_the_highest_version():
    keys = [
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_WBDSnapshot_02.7z",
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_WBDSnapshot_03.7z",
    ]
    assert pick_wbd_key(keys, "16").endswith("WBDSnapshot_03.7z")


def test_pick_wbd_key_returns_none_when_absent():
    assert pick_wbd_key(["nope.7z"], "16") is None


def test_closed_basin_frame_keeps_only_type_C():
    # We filter HU_12_TYPE == 'C' ourselves rather than trusting any upstream
    # selection: the pre-made closed_huc12.gpkg carried 219 non-C rows, 212 of them
    # fully CONTRIBUTING HUC12s (NCONTRB_A == 0) that merely drain into closed ones.
    # Demoting lakes on their internal stream network would be wrong.
    g = _wbd([
        ["160203100200", "C", 100.0, SQ],   # closed
        ["160203100201", "S", 0.0, SQ],     # standard -- drains onward
        ["160203100202", "F", 0.0, SQ],     # frontal -- drains to the coast
        ["160203100203", "W", 0.0, SQ],     # water
    ])
    out = closed_basin_frame(g)
    assert list(out.HUC_12) == ["160203100200"]
    assert set(out.columns) == {"HUC_12", "HU_12_TYPE", "geometry"}


def test_closed_basin_frame_resolves_fields_case_insensitively():
    # WBD_Subwatershed.shp is the same class of raw per-VPU NHDPlus shapefile as
    # NHDFlowline/PlusFlowlineVAA/BurnAddWaterbody, where field-name casing is known
    # to vary across VPUs (VPU 12 ships COMID, VPU 13 ships ComID). Lower-case
    # columns here stand in for that drift.
    g = gpd.GeoDataFrame(
        [["160203100200", "C", SQ]], columns=["huc_12", "hu_12_type", "geometry"], crs=CRS
    )
    out = closed_basin_frame(g)
    assert list(out.HUC_12) == ["160203100200"]
    assert set(out.columns) == {"HUC_12", "HU_12_TYPE", "geometry"}


def test_closed_basin_frame_fails_loud_on_missing_type_column():
    # A WBD layer with no HU_12_TYPE column must raise, not silently stage an
    # empty table -- that would make Signal B a no-op and leave Great Salt Lake
    # (and every other closed-basin lake) on-stream.
    g = gpd.GeoDataFrame(
        [["160203100200", SQ]], columns=["HUC_12", "geometry"], crs=CRS
    )
    with pytest.raises(KeyError, match="HU_12_TYPE"):
        closed_basin_frame(g)
