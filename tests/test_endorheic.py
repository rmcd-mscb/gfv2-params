"""Unit tests for the endorheic dprst classifier (synthetic geometry + FDR arrays)."""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import Polygon

from gfv2_params.endorheic import closed_basin_comids

CRS = "EPSG:5070"


def _box(x0, y0, x1, y1):
    return Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])


def _wb(rows):
    return gpd.GeoDataFrame(rows, columns=["COMID", "geometry"], crs=CRS)


def _closed(polys):
    return gpd.GeoDataFrame({"HUC_12": [str(i) for i in range(len(polys))]},
                            geometry=polys, crs=CRS)


def test_closed_basin_keeps_a_waterbody_fully_inside():
    wb = _wb([[101, _box(1, 1, 2, 2)]])
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == {101}


def test_closed_basin_rejects_a_boundary_graze():
    # THE regression that matters. A polygon touching the closed-HUC12 boundary with
    # ZERO interior overlap returns True from `intersects` -- this artifact produced a
    # false "Cedar Lake routes out of its closed basin" reading during design, and in
    # the real data Eagle Lake / Middle Alkali Lake graze at frac_in = 0.000.
    # Majority-area must reject them; `intersects` must never be substituted back in.
    wb = _wb([[102, _box(10, 0, 12, 2)]])          # shares only the x=10 edge
    closed = _closed([_box(0, 0, 10, 10)])
    assert wb.geometry.iloc[0].intersects(closed.geometry.iloc[0])  # the trap
    assert closed_basin_comids(wb, closed) == set()


def test_closed_basin_keeps_a_majority_overlap():
    # Great Salt Lake sits at frac_in = 0.989 -- it spills ~1% into a neighbouring
    # HUC12, so a strict `within` predicate would drop it. Majority-area keeps it.
    wb = _wb([[103, _box(8, 0, 11, 2)]])           # 2/3 inside the closed box
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == {103}


def test_closed_basin_rejects_a_minority_overlap():
    wb = _wb([[104, _box(9, 0, 12, 2)]])           # 1/3 inside
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == set()


def test_closed_basin_dissolves_adjacent_huc12s():
    # A lake straddling two ADJACENT closed HUC12s is fully inside the closed system
    # but majority-inside neither polygon on its own. Dissolve first, then measure.
    wb = _wb([[105, _box(4, 1, 6, 2)]])            # half in each of two closed boxes
    closed = _closed([_box(0, 0, 5, 10), _box(5, 0, 10, 10)])
    assert closed_basin_comids(wb, closed) == {105}


def test_closed_basin_empty_closed_set_demotes_nothing():
    wb = _wb([[106, _box(1, 1, 2, 2)]])
    empty = gpd.GeoDataFrame({"HUC_12": []}, geometry=[], crs=CRS)
    assert closed_basin_comids(wb, empty) == set()


def test_closed_basin_aggregates_multi_row_comid_by_area():
    # A single COMID split across two rows -- multi-part waterbody geometry, as
    # in the real conus_waterbodies.gpkg layer (448,124 rows, strictly fewer
    # unique COMIDs). Row A is 100% inside the closed union but is only 10% of
    # the COMID's true total area; row B is the other 90% and sits entirely
    # outside. The true combined fraction is 0.1 (must NOT be endorheic), but
    # "any row individually clears min_frac" semantics wrongly grab COMID 107
    # off row A alone (frac_A = 1.0). A waterbody is a COMID, not a row.
    wb = _wb([
        [107, _box(1, 1, 2, 2)],        # area 1, fully inside closed box -> frac 1.0
        [107, _box(20, 0, 29, 1)],      # area 9, fully outside -> frac 0.0
    ])
    closed = _closed([_box(0, 0, 10, 10)])
    assert closed_basin_comids(wb, closed) == set()
