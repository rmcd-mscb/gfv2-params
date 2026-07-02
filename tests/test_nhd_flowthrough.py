"""Unit tests for the flow-through waterbody classifier (synthetic geometry)."""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import LineString, Polygon

from gfv2_params.download.nhd_flowthrough import (
    flowthrough_comids,
    locate_layer,
    read_layer,
)

CRS = "EPSG:4269"


def _wb(rows):
    return gpd.GeoDataFrame(
        rows, columns=["COMID", "FTYPE", "geometry"], crs=CRS
    )


def _fl(rows):
    # rows are [FTYPE, FLOWDIR, geometry]; assign synthetic COMIDs 9001.. so the
    # frame carries the COMID column D1 joins against routed_comids on. FLOWDIR
    # is dead fixture data here — the classifier never reads it (direction comes
    # from topology/routed_comids); several older tests below still pass a
    # FLOWDIR literal per row purely as leftover fixture shape. See
    # test_flowthrough_ignores_missing_flowdir_column for the FLOWDIR-free contract.
    out = [[9001 + i, *r] for i, r in enumerate(rows)]
    return gpd.GeoDataFrame(
        out, columns=["COMID", "FTYPE", "FLOWDIR", "geometry"], crs=CRS
    )


# A unit square waterbody centred near (0,0)..(2,2).
SQUARE = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])


def test_flowthrough_ignores_missing_flowdir_column():
    # FLOWDIR is no longer part of the contract (T2 was replaced by topology-based
    # D1), so flowthrough_comids must work on flowlines carrying only COMID/FTYPE.
    wb = _wb([[301, "SwampMarsh", SQUARE]])
    fl = gpd.GeoDataFrame(
        [[9001, "StreamRiver", LineString([(-1, 1), (3, 1)])]],
        columns=["COMID", "FTYPE", "geometry"], crs=CRS,
    )
    assert flowthrough_comids(wb, fl) == {301}  # T1 through-flow, no FLOWDIR needed


def test_source_lake_routed_outflow_is_onstream():
    # D1: a headwater line whose UPSTREAM end is inside W, present in the routed
    # network (DnHydroseq != 0), promotes W even with no inflow (the VPU 14
    # COMID 16969532 case). The line is COMID 9001 (first _fl row).
    wb = _wb([[201, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl, routed_comids={9001}) == {201}


def test_outflow_only_non_network_stays_dprst():
    # Same geometry but the outflow line is NOT in the routed network -> a local
    # spill, not a source lake -> stays dprst.
    wb = _wb([[202, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl, routed_comids=set()) == set()


def test_single_line_passes_through_is_onstream():
    # T1: one conveyance line crosses the boundary twice (enters west, exits east)
    wb = _wb([[101, "SwampMarsh", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == {101}


def test_throughflow_running_along_boundary_is_onstream():
    # Regression (real case: VPU 15 waterbody COMID 21744935, flowline 21745077).
    # A single sinuous conveyance line passes through the waterbody but partly
    # runs ALONG the shoreline, so `line.intersection(poly.boundary)` is a
    # GeometryCollection (mixed Point + LineString), not a clean MultiPoint. The
    # old T1 crossing-counter only recognised `Multi*` types and collapsed
    # everything else to n=1, defeating T1. Both endpoints lie outside the
    # waterbody, so the endpoint-inside rule (D1) also legitimately misses it.
    wb = _wb([[112, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized",
               LineString([(-1, 1), (1, 1), (1, 0), (1.5, 0), (1.5, -1)])]])
    assert flowthrough_comids(wb, fl) == {112}


def test_split_inflow_and_outflow_is_onstream():
    # Split pass-through: line A ends inside (inflow), line B starts inside
    # (outflow). D1 promotes via B's upstream-inside end when B is routed.
    wb = _wb([[102, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])],
        ["StreamRiver", "With Digitized", LineString([(1, 1), (3, 1)])],
    ])
    assert flowthrough_comids(wb, fl, routed_comids={9002}) == {102}


def test_terminal_sink_inflow_only_stays_dprst():
    # Inflow only (downstream end inside); upstream end outside -> D1 false even
    # when routed -> stays dprst. Endorheic terminal-sink guardrail.
    wb = _wb([[103, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])]])
    assert flowthrough_comids(wb, fl, routed_comids={9001}) == set()


def test_isolated_waterbody_stays_dprst():
    wb = _wb([[105, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(5, 5), (7, 5)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_playa_force_dprst_even_with_throughflow():
    # Endorheic guardrail: a Playa with a line straight through stays dprst.
    wb = _wb([[106, "Playa", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_ice_mass_excluded_from_onstream():
    # Ice Mass is kept out of the on-stream set here (via NEVER_ONSTREAM_FTYPES);
    # it is also excluded from dprst entirely upstream at the waterbody builder
    # (not depression storage — a glacier's cells fall back to land/LULC).
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


def test_split_pair_not_in_routed_network_stays_dprst():
    # Two split lines, neither in the routed network -> no D1 promotion -> dprst.
    # (Direction/FLOWDIR is now irrelevant; network membership decides.)
    wb = _wb([[110, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "Uninitialized", LineString([(-1, 1), (1, 1)])],
        ["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])],
    ])
    assert flowthrough_comids(wb, fl, routed_comids=set()) == set()


def test_t1_throughflow_non_network_line_stays_dprst():
    # Network gate: a conveyance line that geometrically flows through the
    # waterbody but is a Non-Network Flowline (absent from network_comids) must
    # NOT promote on-stream. This is the endorheic closed-basin case (e.g. VPU 18
    # COMID 2556875, promoted by Non-Network ArtificialPath 2561885).
    wb = _wb([[120, "LakePond", SQUARE]])
    fl = _fl([["ArtificialPath", "Uninitialized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl, network_comids=set()) == set()


def test_t1_throughflow_network_line_is_onstream():
    # Same through-flow geometry, but the line IS a Network Flowline -> genuine
    # on-stream through-flow is still promoted under the gate.
    wb = _wb([[121, "LakePond", SQUARE]])
    fl = _fl([["ArtificialPath", "Uninitialized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl, network_comids={9001}) == {121}


def test_network_gate_absent_leaves_t1_ungated():
    # Backward-compat: with no network set supplied, T1 stays pure-geometry
    # (the pre-gate contract), so a through-flow line still promotes.
    wb = _wb([[122, "LakePond", SQUARE]])
    fl = _fl([["ArtificialPath", "Uninitialized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == {122}


def test_nhdarea_coincidence_is_onstream():
    # T3: waterbody overlaps a StreamRiver NHDArea polygon (2-D channel).
    wb = _wb([[111, "LakePond", SQUARE]])
    fl = _fl([])  # no flowlines at all
    areas = gpd.GeoDataFrame(
        [["StreamRiver", Polygon([(1, -1), (3, -1), (3, 3), (1, 3)])]],
        columns=["FTYPE", "geometry"], crs=CRS,
    )
    assert flowthrough_comids(wb, fl, areas) == {111}


def test_locate_layer_finds_sibling(tmp_path):
    hydro = tmp_path / "NHDPlus17" / "NHDSnapshot" / "Hydrography"
    hydro.mkdir(parents=True)
    (hydro / "NHDFlowline.shp").write_bytes(b"")
    (hydro / "NHDWaterbody.shp").write_bytes(b"")
    flowline = hydro / "NHDFlowline.shp"
    assert locate_layer(flowline, "NHDWaterbody") == hydro / "NHDWaterbody.shp"
    assert locate_layer(flowline, "NHDArea") is None


def test_read_layer_normalises_field_casing(tmp_path):
    # Mixed-case fields (VPU 13 ships ComID/FType) must normalise to upper-case.
    p = tmp_path / "wb.gpkg"
    gpd.GeoDataFrame(
        {"ComID": [9], "FType": ["SwampMarsh"],
         "geometry": [Polygon([(0, 0), (1, 0), (1, 1)])]},
        crs="EPSG:4269",
    ).to_file(p)
    out = read_layer(p, ["COMID", "FTYPE"])
    assert list(out.columns) == ["COMID", "FTYPE", "geometry"]
    assert out["FTYPE"].iloc[0] == "SwampMarsh"


# --- Regression: real NHD geometry is measured 3D (XYZM); endpoints/coords carry
# extra ordinates that shapely's Point() rejects (">2 or 3, got 4"). The classifier
# must reduce geometry to 2D. (Synthetic 2D tests above never exercised this.) ---


def test_endpoints_strips_higher_dimensions():
    # A 3D LineString's coords are (x, y, z); _endpoints must not leak z into the
    # returned points (and a 4-ordinate XYZM line would otherwise crash Point()).
    from gfv2_params.download.nhd_flowthrough import _endpoints

    line3d = LineString([(0, 0, 5), (1, 1, 7)])
    up, down = _endpoints(line3d)
    assert up.has_z is False
    assert down.has_z is False
    assert (up.x, up.y) == (0, 0)
    assert (down.x, down.y) == (1, 1)


def test_flowthrough_classifies_three_dimensional_throughflow():
    # A conveyance line passing through the waterbody, given as 3D geometry,
    # must still be detected (T1) without crashing.
    wb = _wb([[401, "SwampMarsh", Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])]])
    fl = _fl([["StreamRiver", "With Digitized",
               LineString([(-1, 1, 3), (3, 1, 3)])]])
    assert flowthrough_comids(wb, fl) == {401}


def test_read_layer_drops_z(tmp_path):
    # read_layer must return planar 2D geometry even when the source carries Z
    # (NHD ships measured 3D). Without force-2D, downstream Point() construction
    # on 4-ordinate coords raises ValueError.
    p = tmp_path / "lines3d.gpkg"
    gpd.GeoDataFrame(
        {"FTYPE": ["StreamRiver"], "FLOWDIR": ["With Digitized"],
         "geometry": [LineString([(0, 0, 9), (1, 1, 9)])]},
        crs=CRS,
    ).to_file(p)
    out = read_layer(p, ["FTYPE", "FLOWDIR"])
    assert out.geometry.iloc[0].has_z is False
