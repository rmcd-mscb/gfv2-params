import logging

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from gfv2_params.dprst_depth.inventory import INVENTORY_COLUMNS
from gfv2_params.dprst_depth.sources import TileSet, decode, encode, tag_and_assign
from gfv2_params.dprst_depth.tiling import (
    _clear_stale_batches,
    _load_and_tag_for_plan,
    guard_oversized_windows,
    polygon_window_cost,
    tile_batches,
    tile_set_groups,
)


def test_tile_set_groups_puts_each_polygon_in_exactly_one_group():
    a = encode(TileSet("P", ("k1",), True))
    b = encode(TileSet("P", ("k1", "k2"), True))
    dprst = gpd.GeoDataFrame({"source_tiles": [a, b, a]}, geometry=[box(0, 0, 1, 1)] * 3, crs="EPSG:5070")
    groups = tile_set_groups(dprst)
    assert groups == {a: [0, 2], b: [1]}
    assert sorted(i for v in groups.values() for i in v) == [0, 1, 2]


def test_tile_set_batches_never_split_a_set():
    groups = {f"s{i}": [i] for i in range(10)}
    batches = tile_batches(groups, n_batches=3)
    flat = [k for b in batches for k in b]
    assert sorted(flat) == sorted(groups) and len(flat) == len(set(flat))


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


# --- _load_and_tag_for_plan (Task 9, issue #173: segment-driven on-stream) --


def test_load_and_tag_for_plan_required_keys_exclude_nhd_tables():
    """Neither `connected_comids_table` nor `flowthrough_comids_table` (the
    retired NHD sources) may appear in the --plan `required` list any more --
    `output_dir` is required instead, since the segment/endorheic parquets are
    resolved from it (dprst_depth now reconstructs against the segment
    classifier, not NHD). `wesm_index` (the hull footprint index) is retired
    too -- the plan hook now requires `dem_1m_inventory`/`wesm_project_attrs`
    (issue #223's real tile inventory, resolved via `sources.tag_and_assign`)."""
    with pytest.raises(KeyError) as exc:
        _load_and_tag_for_plan({}, logging.getLogger("t"))
    missing = exc.value.args[0]
    assert "connected_comids_table" not in missing
    assert "flowthrough_comids_table" not in missing
    assert "wesm_index" not in missing
    assert "output_dir" in missing
    assert "dem_1m_inventory" in missing
    assert "wesm_project_attrs" in missing


def test_load_and_tag_for_plan_resolves_segment_and_endorheic_from_output_dir(tmp_path):
    """`output_dir` (not `connected_comids_table`/`flowthrough_comids_table`)
    is where the segment + endorheic parquets are resolved from. With every
    required input present and valid -- including a tiny real-inventory
    fixture covering the one fixture polygon -- `_load_and_tag_for_plan` must
    run all the way through `sources.tag_and_assign` (issue #223) and return a
    frame carrying `source_tiles`/`candidates`, not just `best_topo`."""
    waterbody_gpkg = tmp_path / "waterbodies.gpkg"
    gpd.GeoDataFrame(
        {"COMID": [1], "member_comid": ["1"], "FTYPE": ["LakePond"]},
        geometry=[box(0, 0, 1, 1)], crs="EPSG:5070",
    ).to_file(waterbody_gpkg, layer="waterbodies", driver="GPKG")

    dem_1m_inventory = tmp_path / "dem_1m_tile_inventory.parquet"
    pd.DataFrame(
        [["P22", "/vsicurl/https://example/tile1.tif", 13, "EPSG:26913", 1000, 1000, -10, -10, 10, 10]],
        columns=INVENTORY_COLUMNS,
    ).to_parquet(dem_1m_inventory)

    wesm_project_attrs = tmp_path / "wesm_project_attrs.parquet"
    pd.DataFrame(
        {"ql_rank": [1], "collect_end": [pd.Timestamp("2020-01-01")], "matched_by": ["project"]},
        index=pd.Index(["P22"], name="project"),
    ).to_parquet(wesm_project_attrs)

    ecoregions_gpkg = tmp_path / "eco.gpkg"
    gpd.GeoDataFrame(
        {"US_L3CODE": ["1"]}, geometry=[box(-100, -100, 100, 100)], crs="EPSG:5070",
    ).to_file(ecoregions_gpkg, layer="eco", driver="GPKG")

    hru_gpkg = tmp_path / "hru.gpkg"
    gpd.GeoDataFrame(
        {"nat_hru_id": [1]}, geometry=[box(-100, -100, 100, 100)], crs="EPSG:5070",
    ).to_file(hru_gpkg, layer="nhru", driver="GPKG")

    config = {
        "waterbody_gpkg": str(waterbody_gpkg), "waterbody_layer": "waterbodies",
        "output_dir": str(tmp_path),
        "dem_1m_inventory": str(dem_1m_inventory), "wesm_project_attrs": str(wesm_project_attrs),
        "ecoregions_gpkg": str(ecoregions_gpkg),
        "hru_gpkg": str(hru_gpkg), "hru_layer": "nhru",
    }

    # Neither the segment nor the endorheic parquet is staged under
    # `output_dir` yet -- must raise FileNotFoundError naming the missing
    # segment table, not silently proceed with an unresolved on-stream set.
    with pytest.raises(FileNotFoundError, match="segment_waterbody_comids.parquet"):
        _load_and_tag_for_plan(config, logging.getLogger("t"))

    # Stage empty (but well-formed) segment + endorheic tables -- no
    # min_onstream_comids/min_endorheic_comids floor is set, so an empty
    # on-stream/endorheic set is legitimate (see CLAUDE.md: an empty
    # endorheic table is a legitimate result for a domain with no closed
    # basin) -- and re-run all the way through.
    pd.DataFrame({"comid": pd.Series([], dtype="int64")}).to_parquet(
        tmp_path / "segment_waterbody_comids.parquet"
    )
    pd.DataFrame(
        {"comid": pd.Series([], dtype="int64"), "by_terminus": pd.Series([], dtype="bool"),
         "by_closed_huc12": pd.Series([], dtype="bool")}
    ).to_parquet(tmp_path / "endorheic_waterbody_comids.parquet")

    out = _load_and_tag_for_plan(config, logging.getLogger("t"))
    assert "source_tiles" in out.columns
    assert "candidates" in out.columns
    assert len(out) == 1
    assert out["candidates"].iloc[0]  # never empty -- the 10m tile is always appended


def test_tag_and_assign_runs_the_guard_between_tagging_and_assignment(tmp_path):
    """`sources.tag_and_assign`'s fixed order -- `tag_best_topo` ->
    `guard_oversized_windows` -> `assign_sources` -- is load-bearing (its own
    docstring says so: `assign_sources` reads the POST-guard `best_topo`) but
    had no test exercising the three-function sequence end to end, only the
    pieces individually (`tests/test_dprst_depth_sources.py`). A polygon
    whose centroid falls inside a real 1 m tile is tagged `"1m"` by
    `tag_best_topo`; here its bbox is also ~20 km x 20 km, so
    `guard_oversized_windows` must retag it `"10m"` BEFORE `assign_sources`
    runs. If the guard ran AFTER (or was skipped), `assign_sources` would
    build real 1 m candidates for a polygon whose window is far too big for
    the SLURM array's memory budget (`MAX_1M_WINDOW_CELLS`); this proves it
    doesn't -- `candidates` must be the 10 m last resort ONLY."""
    giant = box(0, 0, 20_000, 20_000)
    dprst = gpd.GeoDataFrame({"COMID": [1]}, geometry=[giant], crs="EPSG:5070")

    inventory_path = tmp_path / "inventory.parquet"
    pd.DataFrame(
        [["P22", "/vsicurl/https://example/tile1.tif", 15, "EPSG:26915", 1000, 1000,
          -1_000, -1_000, 21_000, 21_000]],
        columns=INVENTORY_COLUMNS,
    ).to_parquet(inventory_path)

    attrs_path = tmp_path / "attrs.parquet"
    pd.DataFrame(
        {"ql_rank": [1], "collect_end": [pd.Timestamp("2020-01-01")], "matched_by": ["project"]},
        index=pd.Index(["P22"], name="project"),
    ).to_parquet(attrs_path)

    out = tag_and_assign(dprst, inventory_path, attrs_path, logging.getLogger("t"))

    assert out["best_topo"].iloc[0] == "10m"
    assert bool(out["oversized_1m"].iloc[0]) is True
    candidates = out["candidates"].iloc[0]
    assert len(candidates) == 1
    assert decode(candidates[0]).project == "10m"
    assert out["source_tiles"].iloc[0] == candidates[0]


def test_a_new_plan_clears_the_previous_plans_batch_files(tmp_path):
    """#221: workers write batch_{id:04d}.parquet, so a new plan with FEWER batches
    used to overwrite the low indices and leave the old high ones in place, to be
    globbed in with the fresh ones. The planner now clears them first. Only
    batch_*.parquet go: the _plan/ directory and anything else in batches_dir stay."""
    batches = tmp_path / "dprst_depth_batches"
    (batches / "_plan").mkdir(parents=True)
    for i in (0, 1, 499):
        (batches / f"batch_{i:04d}.parquet").write_bytes(b"old")
    (batches / "_plan" / "batch_manifest.json").write_text("{}")
    (batches / "notes.txt").write_text("keep me")

    n = _clear_stale_batches(batches, logging.getLogger("t"))

    assert n == 3
    assert not list(batches.glob("batch_*.parquet"))
    assert (batches / "_plan" / "batch_manifest.json").exists()
    assert (batches / "notes.txt").exists()


def test_clearing_a_batches_dir_that_does_not_exist_yet_is_a_noop(tmp_path):
    """Every first build: the planner runs before any batch directory exists."""
    assert _clear_stale_batches(tmp_path / "absent", logging.getLogger("t")) == 0
