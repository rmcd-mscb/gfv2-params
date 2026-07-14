"""Gate the pure helpers in scripts/render_depstor_figures.py.

These are the renderer's correctness hazards: NHDFlowline field casing varies
by VPU (16 ships `ComID`, 01/08 ship `COMID`), the land/dprst/on-stream class
precedence must put dprst last, and the frac_own threshold sweep is the deck's
"0.5 is not a tuned knob" claim.

No HPC data, no I/O — CI runs these.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import LineString

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_renderer():
    """Import the renderer by path (it lives in scripts/, not the package)."""
    path = REPO_ROOT / "scripts" / "render_depstor_figures.py"
    spec = importlib.util.spec_from_file_location("render_depstor_figures", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["render_depstor_figures"] = module
    spec.loader.exec_module(module)
    return module


rdf = _load_renderer()


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
