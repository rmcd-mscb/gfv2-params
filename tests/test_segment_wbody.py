"""Tests for the segment-driven on-stream waterbody classifier (pure logic).

Fully synthetic geometry: CI runs with no data root. The five edge cases below are
DELIBERATE decisions recorded in the design spec, not incidental behaviour —
notably that a shoreline-collinear segment and a segment terminating inside a
waterbody both PROMOTE it.
"""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import shapely
from shapely.geometry import LineString, Polygon

from gfv2_params.segment_wbody import (
    check_onstream_floor,
    load_segment_comids,
    repair_invalid,
    segment_comid_frame,
    segment_waterbody_comids,
    segment_waterbody_pairs,
    write_segment_comids,
)

# A 10x10 unit square lake at the origin; all fixtures below are placed against it.
_LAKE = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])

# The five verified edge cases. `expected` is whether the rule PROMOTES.
_EDGE_CASES = [
    ("crosses_interior", LineString([(-1, 5), (11, 5)]), 10.0, True),
    ("single_point_touch", LineString([(-1, -1), (0, 0), (-1, 1)]), 0.0, False),
    ("corner_clip_through_vertex", LineString([(-1, 1), (1, -1)]), 0.0, False),
    ("collinear_with_shoreline", LineString([(2, 0), (8, 0)]), 6.0, True),
    ("endpoint_inside", LineString([(-1, 5), (5, 5)]), 5.0, True),
]


def _wb(comids, geoms=None, ftypes=None):
    geoms = geoms if geoms is not None else [_LAKE] * len(comids)
    data = {"COMID": comids}
    if ftypes is not None:
        data["FTYPE"] = ftypes
    return gpd.GeoDataFrame(data, geometry=geoms, crs="EPSG:5070")


def _seg(geoms):
    return gpd.GeoDataFrame(geometry=list(geoms), crs="EPSG:5070")


@pytest.mark.parametrize("name,line,expected_len,expected_kept", _EDGE_CASES)
def test_edge_case_overlap_and_promotion(name, line, expected_len, expected_kept):
    pairs = segment_waterbody_pairs(_seg([line]), _wb([7]))
    assert len(pairs) == 1, f"{name}: sjoin should match the lake"
    assert pairs["overlap_m"].iloc[0] == pytest.approx(expected_len), name
    assert (segment_waterbody_comids(pairs) == {7}) is expected_kept, name


def test_disjoint_segment_produces_no_pair_and_no_comid():
    pairs = segment_waterbody_pairs(_seg([LineString([(50, 50), (60, 60)])]), _wb([7]))
    assert pairs.empty
    assert segment_waterbody_comids(pairs) == set()
    assert segment_comid_frame(pairs).empty


def test_multi_segment_lake_aggregates_to_one_comid():
    # Three distinct segments crossing the same lake -> one COMID, n_segments == 3,
    # overlap_m summed. n_segments must count SEGMENTS, not pair rows.
    lines = [
        LineString([(-1, 2), (11, 2)]),
        LineString([(-1, 5), (11, 5)]),
        LineString([(-1, 8), (11, 8)]),
    ]
    frame = segment_comid_frame(segment_waterbody_pairs(_seg(lines), _wb([7])))
    assert frame["comid"].tolist() == [7]
    assert frame["n_segments"].iloc[0] == 3
    assert frame["overlap_m"].iloc[0] == pytest.approx(30.0)


def test_multi_row_comid_does_not_inflate_n_segments():
    # A multi-part waterbody is several ROWS sharing one COMID (448,124 rows /
    # 447,907 COMIDs on the real layer). One segment crossing both rows must give
    # n_segments == 1, not 2 -- this is why pairs are keyed on the row index.
    left = Polygon([(0, 0), (4, 0), (4, 10), (0, 10)])
    right = Polygon([(6, 0), (10, 0), (10, 10), (6, 10)])
    pairs = segment_waterbody_pairs(
        _seg([LineString([(-1, 5), (11, 5)])]), _wb([7, 7], geoms=[left, right])
    )
    assert len(pairs) == 2, "one segment x two rows = two pair rows"
    assert pairs["wb_index"].nunique() == 2
    frame = segment_comid_frame(pairs)
    assert frame["comid"].tolist() == [7]
    assert frame["n_segments"].iloc[0] == 1


def test_ftype_is_not_filtered_here():
    # The Playa/Ice Mass guardrail lives in wbody_connectivity so it applies to the
    # NHD comparison path too. This builder must stay FTYPE-agnostic.
    pairs = segment_waterbody_pairs(
        _seg([LineString([(-1, 5), (11, 5)])]),
        _wb([7], ftypes=["Playa"]),
    )
    assert segment_waterbody_comids(pairs) == {7}


def test_invalid_geometry_is_repaired_and_still_classified():
    bowtie = Polygon([(0, 0), (10, 10), (10, 0), (0, 10)])  # self-intersecting
    repaired, n = repair_invalid(_wb([7], geoms=[bowtie]), name="waterbodies")
    assert n == 1
    pairs = segment_waterbody_pairs(_seg([LineString([(-1, 2), (11, 2)])]), repaired)
    assert segment_waterbody_comids(pairs) == {7}


def test_repair_invalid_is_a_noop_on_valid_geometry():
    gdf, n = repair_invalid(_wb([7]), name="waterbodies")
    assert n == 0
    assert gdf.geometry.iloc[0].equals(_LAKE)


@pytest.mark.filterwarnings("ignore:invalid value encountered:RuntimeWarning")
def test_repair_invalid_raises_when_still_invalid_after_make_valid():
    # Verified empirically (not assumed): shapely.make_valid does NOT raise on this
    # NaN-vertex bowtie -- it returns a LINESTRING that still carries the NaN
    # coordinate, so is_valid stays False afterward. That is exactly the n_still
    # branch: a geometry make_valid "handles" without actually repairing. (A bare
    # NaN coordinate without the accompanying self-intersection instead makes
    # shapely.make_valid itself raise GEOSException -- a different, unguarded
    # failure mode -- so this specific self-intersecting + NaN combination is
    # load-bearing for reaching this branch.) NaN flowing through the GEOS ufuncs
    # also raises a benign numpy RuntimeWarning at construction and at make_valid
    # time; silenced above since it is not the behaviour under test.
    bowtie_nan = Polygon([(0, 0), (10, 10), (10, np.nan), (0, 10)])
    with pytest.raises(ValueError, match="make_valid"):
        repair_invalid(_wb([7], geoms=[bowtie_nan]), name="waterbodies")


def test_unmeasurable_pair_raises_rather_than_zero_length(monkeypatch):
    # This is the exact failure recorded in repair_invalid's docstring: the real
    # CONUS measurement run died with `GEOSException: TopologyException: side
    # location conflict` -- a pair that survives repair_invalid (both geometries
    # individually valid) but still fails inside the intersection call itself.
    # The per-row fallback in _overlap_lengths means shapely.intersection must
    # fail on BOTH the chunked call and the single-pair retry for a NaN to survive
    # to this check -- patching it unconditionally exercises both call sites.
    def boom(*a, **k):
        raise RuntimeError("simulated GEOS failure")

    monkeypatch.setattr(shapely, "intersection", boom)
    with pytest.raises(ValueError, match="unmeasurable"):
        segment_waterbody_pairs(_seg([LineString([(-1, 5), (11, 5)])]), _wb([7]))


def test_non_numeric_comid_rows_are_dropped():
    wb = gpd.GeoDataFrame(
        {"COMID": ["7", "not-a-comid"]},
        geometry=[_LAKE, Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])],
        crs="EPSG:5070",
    )
    seg = _seg([LineString([(-1, 5), (11, 5)]), LineString([(19, 5), (31, 5)])])
    pairs = segment_waterbody_pairs(seg, wb)
    assert segment_waterbody_comids(pairs) == {7}
    assert pairs["comid"].dtype == np.dtype("int64")


def test_missing_comid_column_raises():
    wb = gpd.GeoDataFrame({"GNIS_NAME": ["x"]}, geometry=[_LAKE], crs="EPSG:5070")
    with pytest.raises(KeyError, match="COMID"):
        segment_waterbody_pairs(_seg([LineString([(-1, 5), (11, 5)])]), wb)


def test_geographic_crs_raises():
    # The rule is a length in metres; a degree-based CRS would silently compare
    # degrees against zero and "work", so refuse it.
    wb = _wb([7]).to_crs("EPSG:4326")
    seg = _seg([LineString([(-1, 5), (11, 5)])]).to_crs("EPSG:4326")
    with pytest.raises(ValueError, match="projected"):
        segment_waterbody_pairs(seg, wb)


def test_missing_crs_raises():
    wb = gpd.GeoDataFrame({"COMID": [7]}, geometry=[_LAKE], crs=None)
    with pytest.raises(ValueError, match="CRS"):
        segment_waterbody_pairs(_seg([LineString([(-1, 5), (11, 5)])]), wb)


def test_segments_are_reprojected_to_the_waterbody_crs():
    # A segment layer in a different projected CRS must be reprojected, not rejected:
    # the oregon fabric and the CONUS waterbody layer are both 5070 today, but a
    # fabric-local CRS must not silently produce zero pairs.
    seg = _seg([LineString([(-1, 5), (11, 5)])]).to_crs("EPSG:3857")
    pairs = segment_waterbody_pairs(seg, _wb([7]))
    assert segment_waterbody_comids(pairs) == {7}


def test_parquet_round_trip(tmp_path):
    frame = segment_comid_frame(
        segment_waterbody_pairs(_seg([LineString([(-1, 5), (11, 5)])]), _wb([7]))
    )
    path = tmp_path / "segment_waterbody_comids.parquet"
    write_segment_comids(frame, path)
    assert load_segment_comids(path) == {7}


def test_empty_frame_round_trips_with_a_stable_schema(tmp_path):
    path = tmp_path / "empty.parquet"
    write_segment_comids(segment_comid_frame(pd.DataFrame(
        {"comid": pd.array([], dtype="int64"), "wb_index": pd.array([], dtype="int64"),
         "seg_index": pd.array([], dtype="int64"),
         "overlap_m": pd.array([], dtype="float64")}
    )), path)
    assert load_segment_comids(path) == set()


def test_check_onstream_floor_raises_below_floor(tmp_path):
    with pytest.raises(ValueError, match="min_onstream_comids"):
        check_onstream_floor(12, fabric="gfv2", floor=30000, source=tmp_path / "t.parquet")


def test_check_onstream_floor_is_opt_in(tmp_path):
    # `floor=None` is the tjc contract: no floor declared, so ANY count passes.
    # The assertion is that this does NOT raise — returning normally IS the result.
    check_onstream_floor(0, fabric="tjc", floor=None, source=tmp_path / "t.parquet")


def test_check_onstream_floor_passes_at_floor(tmp_path):
    # Boundary: the floor is inclusive, so n == floor must NOT raise. Guards against
    # a `<=` typo that would reject a legitimately-at-floor result.
    check_onstream_floor(30000, fabric="gfv2", floor=30000, source=tmp_path / "t.parquet")
