import geopandas as gpd
import pytest
from shapely.geometry import box

from gfv2_params.dprst_depth.tiling import (
    component_tile_batches,
    group_by_tile,
    guard_oversized_windows,
    polygon_window_cost,
    tile_batches,
)


def test_group_by_tile_and_batching():
    # two polygons in a 10m fallback area (no 1m footprint) -> grouped by 10m tile
    dprst = gpd.GeoDataFrame(
        {"COMID": [1, 2], "best_topo": ["10m", "10m"]},
        geometry=[box(0, 0, 10, 10), box(20, 20, 30, 30)],
        crs="EPSG:5070",
    )
    wesm = gpd.GeoDataFrame({"workunit": []}, geometry=[], crs="EPSG:5070")
    groups = group_by_tile(dprst, wesm)
    assert sum(len(v) for v in groups.values()) == 2  # every polygon placed
    batches = tile_batches(groups, n_batches=2)
    assert sum(len(b) for b in batches) == len(groups)  # every tile in one batch


def test_group_by_tile_requires_best_topo_column():
    dprst = gpd.GeoDataFrame(
        {"COMID": [1]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:5070"
    )
    wesm = gpd.GeoDataFrame({"workunit": []}, geometry=[], crs="EPSG:5070")
    with pytest.raises(KeyError):
        group_by_tile(dprst, wesm)


def test_group_by_tile_1m_resolves_from_wesm_project_no_probe():
    # A single 1m-tagged polygon whose rim-buffered window falls inside a
    # WESM project footprint -> resolves candidate 3DEP 1m tile keys from
    # geometry alone (no /vsicurl existence probe, no live read).
    dprst = gpd.GeoDataFrame(
        {"COMID": [1], "best_topo": ["1m"]},
        geometry=[box(500_000, 5_300_000, 500_100, 5_300_100)],
        crs="EPSG:26913",  # NAD83 UTM 13N, matches the ND project CRS in topo.py
    )
    wesm = gpd.GeoDataFrame(
        {"project": ["ND_3DEPProcessing_D22"]},
        geometry=[box(0, 5_000_000, 900_000, 5_600_000)],
        crs="EPSG:26913",
    )
    groups = group_by_tile(dprst, wesm)
    assert sum(len(v) for v in groups.values()) >= 1
    assert all(k.startswith("/vsicurl/") for k in groups)
    assert any("USGS_1M_" in k for k in groups)


def test_group_by_tile_1m_falls_back_to_10m_without_wesm_hit():
    # 1m-tagged but no WESM footprint intersects the buffered window ->
    # every polygon must still land in at least one group (10m fallback).
    dprst = gpd.GeoDataFrame(
        {"COMID": [1], "best_topo": ["1m"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:5070",
    )
    wesm = gpd.GeoDataFrame({"workunit": []}, geometry=[], crs="EPSG:5070")
    groups = group_by_tile(dprst, wesm)
    assert sum(len(v) for v in groups.values()) == 1
    assert any("USGS_13_" in k for k in groups)


def test_tile_batches_balances_by_polygon_count():
    groups = {"a": [1, 2, 3, 4], "b": [5], "c": [6], "d": [7]}
    batches = tile_batches(groups, n_batches=2)
    assert len(batches) == 2
    assert sum(len(b) for b in batches) == len(groups)
    assert {k for b in batches for k in b} == set(groups)
    # the heaviest tile ("a", 4 polygons) should land alone in a batch so the
    # other batch (b+c+d, also 3 polygons) balances against it.
    loads = [sum(len(groups[k]) for k in b) for b in batches]
    assert max(loads) - min(loads) <= 1


def test_tile_batches_more_batches_than_tiles_yields_empty_batches():
    groups = {"a": [1], "b": [2]}
    batches = tile_batches(groups, n_batches=5)
    assert len(batches) == 5
    assert sum(len(b) for b in batches) == 2
    assert sum(1 for b in batches if b) == 2


def test_component_tile_batches_keeps_multi_tile_polygon_co_batched():
    # Polygon 1 spans tiles "a" and "b" (present in both groups); a plain
    # tile_batches (Task 3) could legally place "a" and "b" in different
    # batches, which would make compute.run_batch treat polygon 1 as
    # single-tile in EACH batch and compute it twice from an incomplete
    # window (Task 4's flagged concern). component_tile_batches must never
    # split them, regardless of n_batches or how many other tiles exist.
    groups = {
        "a": [1, 2],
        "b": [1, 3],
        "c": [4],
        "d": [5],
        "e": [6],
    }
    for n_batches in (1, 2, 3, 5, 10):
        batches = component_tile_batches(groups, n_batches=n_batches)
        assert len(batches) == n_batches
        assert sum(len(b) for b in batches) == len(groups)
        assert {k for b in batches for k in b} == set(groups)
        batch_of_a = next(i for i, b in enumerate(batches) if "a" in b)
        batch_of_b = next(i for i, b in enumerate(batches) if "b" in b)
        assert batch_of_a == batch_of_b


def test_component_tile_batches_matches_tile_batches_when_no_sharing():
    # No polygon spans two tiles -> every component is a singleton -> same
    # partition tile_batches itself would produce.
    groups = {"a": [1, 2, 3, 4], "b": [5], "c": [6], "d": [7]}
    plain = tile_batches(groups, n_batches=2)
    componentised = component_tile_batches(groups, n_batches=2)
    assert {frozenset(b) for b in plain} == {frozenset(b) for b in componentised}


# --- guard_oversized_windows (issue #173 giant-window OOM/load-balance fix) -


def test_guard_oversized_windows_retags_giant_1m_polygon():
    # ~20km x 20km bbox at "1m": (20_000 + 2*200)^2 ~= 4.16e8 cells, well over
    # the 200M-cell default budget -> must be downgraded to "10m".
    giant = box(0, 0, 20_000, 20_000)
    # 10m x 10m bbox: (10 + 400)^2 ~= 168k cells, nowhere near the budget ->
    # left alone.
    normal = box(100_000, 100_000, 100_010, 100_010)
    dprst = gpd.GeoDataFrame(
        {"COMID": [1, 2], "best_topo": ["1m", "1m"]},
        geometry=[giant, normal],
        crs="EPSG:5070",
    )
    out = guard_oversized_windows(dprst)

    giant_row = out.loc[out["COMID"] == 1].iloc[0]
    normal_row = out.loc[out["COMID"] == 2].iloc[0]
    assert giant_row["best_topo"] == "10m"
    assert bool(giant_row["oversized_1m"]) is True
    assert normal_row["best_topo"] == "1m"
    assert bool(normal_row["oversized_1m"]) is False
    # original untouched (pure function)
    assert dprst.loc[dprst["COMID"] == 1, "best_topo"].iloc[0] == "1m"


def test_guard_oversized_windows_leaves_already_10m_polygons_alone():
    # A polygon already tagged "10m" is never touched, however large its
    # bbox -- its window is already 100x smaller at the same bbox.
    giant = box(0, 0, 50_000, 50_000)
    dprst = gpd.GeoDataFrame(
        {"COMID": [1], "best_topo": ["10m"]}, geometry=[giant], crs="EPSG:5070",
    )
    out = guard_oversized_windows(dprst)
    assert out["best_topo"].iloc[0] == "10m"
    assert bool(out["oversized_1m"].iloc[0]) is False


def test_guard_oversized_windows_requires_best_topo_column():
    dprst = gpd.GeoDataFrame({"COMID": [1]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:5070")
    with pytest.raises(KeyError):
        guard_oversized_windows(dprst)


# --- cost-weighted bin-packing (issue #173) ---------------------------------


def test_tile_batches_balances_by_cost_not_count():
    # Two "heavy" tiles ("a", "e") each hold one giant-window polygon; four
    # "light" tiles hold one cheap polygon each. Every tile has polygon
    # COUNT 1, so plain count-based packing (costs=None) can't tell the
    # heavy tiles apart from the light ones and may stack both heavy tiles
    # into the same batch. Cost-weighted packing must anchor each heavy tile
    # in its own batch.
    groups = {"a": [1], "b": [2], "c": [3], "d": [4], "e": [5], "f": [6]}
    costs = {1: 1_000_000, 2: 10, 3: 10, 4: 10, 5: 1_000_000, 6: 10}

    batches = tile_batches(groups, n_batches=2, costs=costs)
    assert len(batches) == 2
    assert sum(len(b) for b in batches) == len(groups)
    assert {k for b in batches for k in b} == set(groups)

    batch_of_a = next(i for i, b in enumerate(batches) if "a" in b)
    batch_of_e = next(i for i, b in enumerate(batches) if "e" in b)
    assert batch_of_a != batch_of_e  # the two heavy tiles must not stack

    loads = [sum(costs[idx] for tk in b for idx in groups[tk]) for b in batches]
    assert abs(loads[0] - loads[1]) <= 30  # only the light tiles' cost can differ


def test_tile_batches_cost_none_falls_back_to_count():
    # costs=None must reproduce the original count-based behavior exactly.
    groups = {"a": [1, 2, 3, 4], "b": [5], "c": [6], "d": [7]}
    assert tile_batches(groups, n_batches=2) == tile_batches(groups, n_batches=2, costs=None)


def test_component_tile_batches_balances_by_cost():
    # Two components, each spanning 2 tiles via a shared heavy polygon (cost
    # 2e6), plus four cheap singleton-tile components. A count-based
    # balance (2 tiles per heavy component vs 1 tile per light component)
    # could easily stack both heavy components in one batch; cost-weighted
    # packing must separate them.
    groups = {
        "a": [1, 10], "b": [1, 11],  # component {a,b}: polygons {1,10,11}
        "c": [2, 20], "d": [2, 21],  # component {c,d}: polygons {2,20,21}
        "e": [30], "f": [31], "g": [32], "h": [33],
    }
    costs = {
        1: 2_000_000, 10: 10, 11: 10,
        2: 2_000_000, 20: 10, 21: 10,
        30: 10, 31: 10, 32: 10, 33: 10,
    }
    batches = component_tile_batches(groups, n_batches=2, costs=costs)
    assert len(batches) == 2
    assert sum(len(b) for b in batches) == len(groups)
    assert {k for b in batches for k in b} == set(groups)  # every tile placed once

    # multi-tile-component invariant still holds under cost weighting
    for pair in (("a", "b"), ("c", "d")):
        i0 = next(i for i, b in enumerate(batches) if pair[0] in b)
        i1 = next(i for i, b in enumerate(batches) if pair[1] in b)
        assert i0 == i1

    batch_ab = next(i for i, b in enumerate(batches) if "a" in b)
    batch_cd = next(i for i, b in enumerate(batches) if "c" in b)
    assert batch_ab != batch_cd  # the two heavy components must not stack


def test_polygon_window_cost_scales_1m_vs_10m_and_adds_overhead():
    # Same bbox, different best_topo -> 10m cost should be ~100x cheaper
    # than 1m cost (cell size 10x10 vs 1x1), modulo the shared fixed overhead.
    geom = box(0, 0, 1_000, 1_000)
    dprst = gpd.GeoDataFrame(
        {"COMID": [1, 2], "best_topo": ["1m", "10m"]},
        geometry=[geom, geom],
        crs="EPSG:5070",
    )
    costs = polygon_window_cost(dprst)
    idx_1m, idx_10m = dprst.index[0], dprst.index[1]
    cell_1m = 1_400 * 1_400  # (1000 + 2*200)^2 at 1m GSD
    cell_10m = cell_1m / 100.0
    assert costs[idx_1m] == pytest.approx(cell_1m + 50_000)
    assert costs[idx_10m] == pytest.approx(cell_10m + 50_000)
    assert costs[idx_1m] > costs[idx_10m]


def test_polygon_window_cost_requires_best_topo_column():
    dprst = gpd.GeoDataFrame({"COMID": [1]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:5070")
    with pytest.raises(KeyError):
        polygon_window_cost(dprst)
