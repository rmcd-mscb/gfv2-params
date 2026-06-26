"""Unit tests for the flow-through waterbody classifier (synthetic geometry)."""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import LineString, Polygon

from gfv2_params.download.nhd_flowthrough import flowthrough_comids

CRS = "EPSG:4269"


def _wb(rows):
    return gpd.GeoDataFrame(
        rows, columns=["COMID", "FTYPE", "geometry"], crs=CRS
    )


def _fl(rows):
    return gpd.GeoDataFrame(
        rows, columns=["FTYPE", "FLOWDIR", "geometry"], crs=CRS
    )


# A unit square waterbody centred near (0,0)..(2,2).
SQUARE = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])


def test_single_line_passes_through_is_onstream():
    # T1: one conveyance line crosses the boundary twice (enters west, exits east)
    wb = _wb([[101, "SwampMarsh", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == {101}


def test_split_inflow_and_outflow_is_onstream():
    # T2: line A ends inside (inflow), line B starts inside (outflow); neither
    # alone crosses twice, but together they pair to flow-through.
    wb = _wb([[102, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])],  # downstream end inside -> inflow
        ["StreamRiver", "With Digitized", LineString([(1, 1), (3, 1)])],   # upstream end inside -> outflow
    ])
    assert flowthrough_comids(wb, fl) == {102}


def test_terminal_sink_inflow_only_stays_dprst():
    # Inflow only (line ends inside, nothing leaves) -> NOT promoted.
    wb = _wb([[103, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_spilling_pothole_outflow_only_stays_dprst():
    # Outflow only (line starts inside, nothing enters) -> NOT promoted.
    wb = _wb([[104, "SwampMarsh", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_isolated_waterbody_stays_dprst():
    wb = _wb([[105, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(5, 5), (7, 5)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_playa_force_dprst_even_with_throughflow():
    # Endorheic guardrail: a Playa with a line straight through stays dprst.
    wb = _wb([[106, "Playa", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_ice_mass_force_dprst():
    wb = _wb([[107, "Ice Mass", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_non_conveyance_line_ignored():
    # A Pipeline through the waterbody is not a stream -> not flow-through.
    wb = _wb([[108, "LakePond", SQUARE]])
    fl = _fl([["Pipeline", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_uninitialized_flowdir_still_caught_by_t1():
    # FLOWDIR unreliable, but a single line crossing twice (T1) is direction-free.
    wb = _wb([[109, "SwampMarsh", SQUARE]])
    fl = _fl([["StreamRiver", "Uninitialized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == {109}


def test_uninitialized_split_pair_not_paired_by_t2():
    # Two separate Uninitialized lines (one ends inside, one starts inside):
    # T2 must NOT trust their direction, so they don't pair -> stays dprst.
    wb = _wb([[110, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "Uninitialized", LineString([(-1, 1), (1, 1)])],
        ["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])],
    ])
    assert flowthrough_comids(wb, fl) == set()


def test_nhdarea_coincidence_is_onstream():
    # T3: waterbody overlaps a StreamRiver NHDArea polygon (2-D channel).
    wb = _wb([[111, "LakePond", SQUARE]])
    fl = _fl([])  # no flowlines at all
    areas = gpd.GeoDataFrame(
        [["StreamRiver", Polygon([(1, -1), (3, -1), (3, 3), (1, 3)])]],
        columns=["FTYPE", "geometry"], crs=CRS,
    )
    assert flowthrough_comids(wb, fl, areas) == {111}
