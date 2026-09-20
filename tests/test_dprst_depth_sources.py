import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from gfv2_params.dprst_depth import sources as S


def _inv(rows):
    df = pd.DataFrame(rows, columns=["project", "key", "zone", "minx", "miny", "maxx", "maxy"])
    return gpd.GeoDataFrame(df, geometry=[box(*r[3:]) for r in rows], crs="EPSG:5070")


ATTRS = pd.DataFrame(
    {"ql_rank": [2, 1, 1, 1], "collect_end": pd.to_datetime(["2022-01-01", "2015-01-01", "2020-01-01", "2020-01-01"])},
    index=pd.Index(["NEW_QL2", "OLD_QL1", "MID_QL1", "AAA_QL1"], name="project"),
)


def test_encode_decode_roundtrip():
    ts = S.TileSet("P", ("/vsicurl/a.tif", "/vsicurl/b.tif"), True)
    assert S.decode(S.encode(ts)) == ts


def test_rank_prefers_full_cover_then_ql_then_newest_then_name():
    window = box(0, 0, 100, 100)
    hits = _inv([
        ("NEW_QL2", "k_new", 15, -10, -10, 200, 200),   # covers, QL2
        ("OLD_QL1", "k_old", 15, -10, -10, 200, 200),   # covers, QL1, 2015
        ("MID_QL1", "k_mid", 15, -10, -10, 200, 200),   # covers, QL1, 2020
        ("AAA_QL1", "k_aaa", 15, -10, -10, 200, 200),   # covers, QL1, 2020 -> wins on name
        ("PART", "k_part", 15, 50, -10, 200, 200),       # partial cover, no attrs
    ])
    ranked = S.rank_candidates(window, hits, ATTRS)
    assert [t.project for t in ranked] == ["AAA_QL1", "MID_QL1", "OLD_QL1", "NEW_QL2", "PART"]
    assert ranked[-1].covers is False


def test_rank_unions_a_projects_tiles_into_one_set_and_picks_one_zone():
    window = box(0, 0, 100, 100)
    hits = _inv([
        ("P", "k_left", 15, -10, -10, 50, 200),
        ("P", "k_right", 15, 50, -10, 200, 200),
        ("P", "k_other_zone", 16, 90, -10, 200, 200),   # smaller overlap, other zone -> dropped
    ])
    (ts,) = S.rank_candidates(window, hits, ATTRS)
    assert ts.keys == ("k_left", "k_right") and ts.covers is True


def test_tag_best_topo_uses_real_tile_bounds_not_hulls():
    inv = _inv([("P", "k", 15, 0, 0, 1000, 1000)])
    dprst = gpd.GeoDataFrame({"COMID": [1, 2]}, geometry=[box(10, 10, 20, 20), box(5000, 5000, 5010, 5010)],
                             crs="EPSG:5070")
    out = S.tag_best_topo(dprst, inv)
    assert out["best_topo"].tolist() == ["1m", "10m"]


def test_assign_sources_always_ends_with_the_10m_last_resort(monkeypatch):
    monkeypatch.setattr(S, "_tile13_key", lambda geom, crs: "/vsicurl/10m.tif")
    inv = _inv([("P", "k", 15, -1000, -1000, 1000, 1000)])
    dprst = gpd.GeoDataFrame({"COMID": [1, 2], "best_topo": ["1m", "10m"]},
                             geometry=[box(0, 0, 10, 10), box(0, 0, 10, 10)], crs="EPSG:5070")
    out = S.assign_sources(dprst, inv, ATTRS)
    one, ten = out.loc[0, "candidates"], out.loc[1, "candidates"]
    assert S.decode(one[0]).project == "P" and S.decode(one[-1]).project == "10m"
    assert len(ten) == 1 and S.decode(ten[0]).project == "10m"
    assert out.loc[0, "source_tiles"] == one[0]
