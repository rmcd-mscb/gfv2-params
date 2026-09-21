import logging

import geopandas as gpd
import pandas as pd
import pytest
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


# --- tag_and_assign's consuming-end inventory/attrs floor (issue #223 review round 2) ---
# See CLAUDE.md's `min_onstream_comids`/`min_endorheic_comids` floor discussion and
# `sources.DEFAULT_MIN_INVENTORY_TILES`'s module comment for the doctrine: the floor
# belongs at this shared entry point, not only in the producer that writes the table.


def _write_inventory_rows(tmp_path, n_tiles: int, n_projects: int, name: str = "inventory.parquet"):
    """`n_tiles` real, well-formed rows spread round-robin across `n_projects`
    distinct project names -- lets a test hold one axis fixed (e.g. plenty of
    tiles, too few projects) without generating anywhere near CONUS scale."""
    from gfv2_params.dprst_depth.inventory import INVENTORY_COLUMNS

    rows = [
        [f"P{i % n_projects}", f"/vsicurl/https://example/P{i % n_projects}_{i}.tif", 15,
         "EPSG:26915", 100, 100, float(i), float(i), float(i) + 1, float(i) + 1]
        for i in range(n_tiles)
    ]
    path = tmp_path / name
    pd.DataFrame(rows, columns=INVENTORY_COLUMNS).to_parquet(path)
    return path


def _write_attrs_rows(tmp_path, n_rows: int, name: str = "attrs.parquet"):
    path = tmp_path / name
    pd.DataFrame(
        {"ql_rank": [1] * n_rows, "collect_end": [pd.Timestamp("2020-01-01")] * n_rows,
         "matched_by": ["project"] * n_rows},
        index=pd.Index([f"P{i}" for i in range(n_rows)], name="project"),
    ).to_parquet(path)
    return path


def _tiny_dprst():
    return gpd.GeoDataFrame({"COMID": [1]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:5070")


def test_tag_and_assign_raises_on_empty_inventory(tmp_path):
    from gfv2_params.dprst_depth.inventory import INVENTORY_COLUMNS

    inv_path = tmp_path / "inventory.parquet"
    pd.DataFrame(columns=INVENTORY_COLUMNS).to_parquet(inv_path)
    attrs_path = _write_attrs_rows(tmp_path, 5)
    with pytest.raises(RuntimeError, match="dem_1m_inventory is empty"):
        S.tag_and_assign(_tiny_dprst(), inv_path, attrs_path, logging.getLogger("t"))


def test_tag_and_assign_raises_below_tile_floor(tmp_path):
    # 10 tiles/10 projects: both axes are under the module defaults
    # (DEFAULT_MIN_INVENTORY_TILES=100,000 / DEFAULT_MIN_INVENTORY_PROJECTS=900) --
    # the tile-count check is the first one `tag_and_assign` runs, so it fires here.
    inv_path = _write_inventory_rows(tmp_path, n_tiles=10, n_projects=10)
    attrs_path = _write_attrs_rows(tmp_path, 10)
    with pytest.raises(RuntimeError, match=r"dem_1m_inventory carries 10 tiles, below its floor of 100,000"):
        S.tag_and_assign(_tiny_dprst(), inv_path, attrs_path, logging.getLogger("t"))


def test_tag_and_assign_raises_below_project_floor(tmp_path):
    # Override the tile floor down so ONLY the distinct-project-count check can fire --
    # isolates it from the tile-count check without generating 100,000 synthetic rows.
    inv_path = _write_inventory_rows(tmp_path, n_tiles=20, n_projects=3)
    attrs_path = _write_attrs_rows(tmp_path, 3)
    with pytest.raises(RuntimeError, match=r"dem_1m_inventory spans 3 distinct projects, below its floor of 900"):
        S.tag_and_assign(
            _tiny_dprst(), inv_path, attrs_path, logging.getLogger("t"), min_inventory_tiles=5,
        )


def test_tag_and_assign_default_floors_catch_the_second_historical_incident(tmp_path):
    """(#223 review round 3) The FIRST partial inventory this branch staged during
    development (88,403 tiles/357 projects) trips the tile floor easily. The
    SECOND (121,849 tiles/875 projects) does NOT trip the tile floor
    (121,849 >= DEFAULT_MIN_INVENTORY_TILES) -- only the distinct-project floor
    catches it (875 < DEFAULT_MIN_INVENTORY_PROJECTS). Both review passes that
    flagged this used a real inventory-shaped count, not a tiny synthetic one, so
    reproduce that here: 875 distinct projects, well over the tile floor. No
    overrides -- this exercises the DEFAULT floors, which every existing floor
    test either uses tiny synthetic counts against or overrides down."""
    inv_path = _write_inventory_rows(tmp_path, n_tiles=121_849, n_projects=875)
    attrs_path = _write_attrs_rows(tmp_path, 875)
    assert 121_849 >= S.DEFAULT_MIN_INVENTORY_TILES, "tile floor must NOT fire for this incident"
    assert 875 < S.DEFAULT_MIN_INVENTORY_PROJECTS, "project floor MUST fire for this incident"
    with pytest.raises(RuntimeError, match=r"dem_1m_inventory spans 875 distinct projects, below its floor of 900"):
        S.tag_and_assign(_tiny_dprst(), inv_path, attrs_path, logging.getLogger("t"))


def test_tag_and_assign_raises_on_empty_attrs(tmp_path):
    # Satisfy both inventory floors via override so only the attrs check can fire.
    inv_path = _write_inventory_rows(tmp_path, n_tiles=20, n_projects=3)
    attrs_path = tmp_path / "attrs.parquet"
    pd.DataFrame(columns=["ql_rank", "collect_end", "matched_by"]).to_parquet(attrs_path)
    with pytest.raises(RuntimeError, match="wesm_project_attrs is empty"):
        S.tag_and_assign(
            _tiny_dprst(), inv_path, attrs_path, logging.getLogger("t"),
            min_inventory_tiles=5, min_inventory_projects=1,
        )


def test_tag_and_assign_raises_below_attrs_floor(tmp_path):
    inv_path = _write_inventory_rows(tmp_path, n_tiles=20, n_projects=3)
    attrs_path = _write_attrs_rows(tmp_path, 3)
    with pytest.raises(RuntimeError, match=r"wesm_project_attrs carries 3 rows, below its floor of 500"):
        S.tag_and_assign(
            _tiny_dprst(), inv_path, attrs_path, logging.getLogger("t"),
            min_inventory_tiles=5, min_inventory_projects=1,
        )


def test_tag_and_assign_floor_override_of_zero_disables_the_check(monkeypatch, tmp_path):
    monkeypatch.setattr(S, "_tile13_key", lambda geom, crs: "/vsicurl/10m.tif")
    inv_path = _write_inventory_rows(tmp_path, n_tiles=1, n_projects=1)
    attrs_path = _write_attrs_rows(tmp_path, 1)
    out = S.tag_and_assign(
        _tiny_dprst(), inv_path, attrs_path, logging.getLogger("t"),
        min_inventory_tiles=0, min_inventory_projects=0, min_attrs_rows=0,
    )
    assert "source_tiles" in out.columns
