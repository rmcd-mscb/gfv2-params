"""Tests for WBAREACOMI-driven waterbody connectivity (helper + builder)."""

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from gfv2_params.depstor import load_connected_comids, select_connected_waterbodies


def _wb_gdf():
    geoms = [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])] * 4
    return gpd.GeoDataFrame(
        {
            "COMID": [10, 20, 30, 40],
            # row 3 is a multipart case: COMID 30 not connected, but its
            # member_comid 999 is.
            "member_comid": ["10", "20", "999", "40"],
        },
        geometry=geoms,
        crs="EPSG:5070",
    )


def test_select_connected_by_comid_or_member():
    out = select_connected_waterbodies(_wb_gdf(), {10, 999})
    assert sorted(out["COMID"].tolist()) == [10, 30]  # 10 by COMID, 30 by member


def test_select_connected_empty_set():
    out = select_connected_waterbodies(_wb_gdf(), set())
    assert len(out) == 0


def test_load_connected_comids(tmp_path):
    p = tmp_path / "c.parquet"
    pd.DataFrame({"comid": [5, 7, 9]}).to_parquet(p, index=False)
    assert load_connected_comids(p) == {5, 7, 9}
