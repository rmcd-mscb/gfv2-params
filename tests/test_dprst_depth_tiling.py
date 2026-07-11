import geopandas as gpd
import pytest
from shapely.geometry import box

from gfv2_params.dprst_depth.tiling import component_tile_batches, group_by_tile, tile_batches


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
