"""Gate the pure helpers in scripts/render_depstor_figures.py.

These are the renderer's correctness hazards: NHDFlowline field casing varies
by VPU (16 ships `ComID`, 01/08 ship `COMID`), the land/dprst/on-stream class
precedence must put dprst last, and the frac_own threshold sweep is the deck's
"0.5 is not a tuned knob" claim.

No HPC data, no I/O — CI runs these.
"""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import LineString, Polygon

import scripts.render_depstor_figures as rdf


def test_normalize_fields_uppercases_vpu16_casing():
    """VPU 16 ships ComID/WBAreaComI; VPUs 01/08 ship COMID/WBAREACOMI."""
    gdf = gpd.GeoDataFrame(
        {"ComID": [1], "WBAreaComI": [7], "FCode": [46006]},
        geometry=[LineString([(0, 0), (1, 1)])],
        crs="EPSG:4269",
    )
    out = rdf.normalize_fields(gdf)
    assert set(out.columns) == {"COMID", "WBAREACOMI", "FCODE", "geometry"}
    assert out["COMID"].iloc[0] == 1
    assert out.crs == gdf.crs


def test_normalize_fields_is_idempotent_on_uppercase_vpus():
    gdf = gpd.GeoDataFrame(
        {"COMID": [1], "WBAREACOMI": [7]},
        geometry=[LineString([(0, 0), (1, 1)])],
        crs="EPSG:4269",
    )
    out = rdf.normalize_fields(gdf)
    assert set(out.columns) == {"COMID", "WBAREACOMI", "geometry"}


def test_classification_array_dprst_wins_over_onstream():
    """A cell flagged in both masks is dprst — dprst is written last.

    This mirrors the product: endorheic cells recovered by the clump-veto
    exemption are dprst even where the region touches the on-stream mask.
    """
    dprst = np.array([[0, 1, 1]], dtype=np.uint8)
    onstream = np.array([[0, 0, 1]], dtype=np.uint8)
    cat = rdf.classification_array(dprst, 0, onstream, 0)
    assert cat.tolist() == [[0, 1, 1]]


def test_classification_array_marks_land_dprst_onstream():
    dprst = np.array([[255, 1, 255]], dtype=np.uint8)
    onstream = np.array([[255, 255, 1]], dtype=np.uint8)
    cat = rdf.classification_array(dprst, 255, onstream, 255)
    assert cat.tolist() == [[0, 1, 2]]


def test_frac_own_stats_reports_bimodality_and_sweep():
    """The deck claims frac_own is bimodal and the 0.5 threshold is inert."""
    df = pd.DataFrame(
        {
            "comid": [1, 2, 3, 4, 5, 6],
            # 4 high, 1 mid-band, 1 zero (a Signal-B-only waterbody)
            "frac_own": [1.0, 1.0, 0.99, 0.96, 0.50, 0.0],
            "by_terminus": [True, True, True, True, False, False],
            "by_closed_huc12": [False, False, False, False, False, True],
        }
    )
    stats = rdf.frac_own_stats(df)
    # Candidates = waterbodies with a computed frac_own (> 0), not all rows.
    assert stats["candidates"] == 5
    assert stats["at_or_above_95"] == 4
    assert stats["in_band_45_55"] == 1
    assert stats["sweep"] == {0.3: 5, 0.5: 4, 0.7: 4}
    assert stats["swing"] == pytest.approx(0.25)


def test_split_terminal_cells_by_polygon_partitions_inside_vs_outside():
    """This is the terminus-inside-itself test the marquee figure depends on.

    A point strictly inside the square is "evidence"; a point outside is
    "context" -- drawing both undifferentiated is exactly the bug this test
    guards against (the FDR code-0 markers scattered across the whole tile,
    not just inside the waterbody).
    """
    square = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    xs = np.array([5.0, 5.0, 50.0, -50.0])
    ys = np.array([5.0, 2.0, 50.0, -50.0])
    in_xs, in_ys, out_xs, out_ys = rdf.split_terminal_cells_by_polygon(xs, ys, square)
    assert sorted(in_xs.tolist()) == [5.0, 5.0]
    assert sorted(in_ys.tolist()) == [2.0, 5.0]
    assert sorted(out_xs.tolist()) == [-50.0, 50.0]
    assert sorted(out_ys.tolist()) == [-50.0, 50.0]


def test_split_terminal_cells_by_polygon_handles_empty_input():
    square = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    in_xs, in_ys, out_xs, out_ys = rdf.split_terminal_cells_by_polygon(
        np.array([]), np.array([]), square
    )
    assert len(in_xs) == len(in_ys) == len(out_xs) == len(out_ys) == 0
