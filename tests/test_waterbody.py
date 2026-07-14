"""Tests for the `waterbody` depstor builder (clump rasterisation + FTYPE exclusion)."""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon, box

from gfv2_params.depstor_builders.waterbody import merge_burn_add


def _write_template(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="float32",
        crs="EPSG:5070", transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(np.full((n, n), 100.0, dtype=np.float32), 1)


def _write_landmask(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="uint8",
        crs="EPSG:5070", transform=transform, nodata=255,
    ) as dst:
        dst.write(np.ones((n, n), dtype=np.uint8), 1)  # all land


def test_waterbody_excludes_ice_mass_keeps_lakepond(tmp_path):
    """Ice Mass polygons must be absent from wbody_binary; LakePond must remain."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    # 2 waterbodies, both well above the default min_area (900 m^2):
    #   WB 10 — LakePond, top-left 2x2 block (3600 m^2) -> must be kept
    #   WB 20 — Ice Mass, bottom-right 2x2 block (3600 m^2) -> must be excluded
    gdf = gpd.GeoDataFrame(
        {"COMID": [10, 20], "FTYPE": ["LakePond", "Ice Mass"]},
        geometry=[
            box(0, 270, 60, 300),   # top-left 2x2: cells [0,0],[0,1],[1,0],[1,1]
            box(240, 0, 300, 30),   # bottom-right 2x2: cells [8,8],[8,9],[9,8],[9,9]
        ],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
    )
    ctx.paths["landmask"] = landmask

    produced = waterbody.build(
        {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["wbody_binary"]) as src:
        arr = src.read(1)

    assert arr[0, 0] == 1     # WB 10 (LakePond) present
    assert arr[9, 9] != 1     # WB 20 (Ice Mass) absent


def test_waterbody_missing_ftype_column_raises(tmp_path):
    """A waterbody layer without FTYPE must raise (refuse to silently miss Ice Mass)."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10]},
        geometry=[box(0, 270, 60, 300)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(KeyError):
        waterbody.build(
            {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
            ctx, logging.getLogger("test"),
        )


CRS = "EPSG:5070"
WB_COLS = ["GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm"]


def _sq(x0, y0, s=100):
    return Polygon([(x0, y0), (x0 + s, y0), (x0 + s, y0 + s), (x0, y0 + s)])


def _frame(rows):
    return gpd.GeoDataFrame(rows, columns=WB_COLS + ["geometry"], crs=CRS)


def test_merge_burn_add_appends_the_polygons():
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(500, 500)]])
    out = merge_burn_add(base, burn)
    assert len(out) == 2
    assert set(out.COMID) == {111, -367111}


def test_merge_burn_add_rejects_a_non_negative_comid():
    # A positive BurnAdd COMID could match a WBAREACOMI / flow-through COMID and be
    # promoted on-stream -- but NHDPlus flagged every BurnAddWaterbody as a SINK.
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, 222, "Playa", 222, 0.01, _sq(500, 500)]])
    with pytest.raises(ValueError, match="negative"):
        merge_burn_add(base, burn)


def test_merge_burn_add_is_a_noop_when_not_configured():
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    assert merge_burn_add(base, None) is base


def _write_burn_add_parquet(path: Path, rows: list[list]) -> None:
    gpd.GeoDataFrame(rows, columns=WB_COLS + ["geometry"], crs=CRS).to_parquet(path)


def test_build_merges_burn_add_before_ftype_exclusion(tmp_path):
    """A BurnAdd Ice Mass polygon must still be excluded from wbody_binary.

    `merge_burn_add` must run BEFORE the `EXCLUDE_WATERBODY_FTYPES` filter inside
    `waterbody.build()`. A refactor that moved the merge to AFTER that filter would
    pass every other test in this file (they all call `merge_burn_add` directly) but
    would let a BurnAdd Ice Mass polygon leak into the raster unfiltered -- this test
    drives the real `build()` entry point to catch exactly that regression.

    Note: no "Ice Mass" BurnAdd row occurs in the real staged data (the sink-purpose
    rows across all 21 CONUS VPUs are Playa/LakePond/SwampMarsh only -- see
    `nhd_burn_components.PURPCODE_IS_SINK` / `FTYPE_BY_FCODE_PREFIX`), but the staging
    path derives FTYPE from FCODE and so *could* emit one. This test exercises the
    merge-before-filter ordering invariant that keeps such a row out of the raster.
    """
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    burn_parquet = tmp_path / "burn_add.parquet"
    _write_template(template)
    _write_landmask(landmask)

    # Base layer: one ordinary LakePond, top-left 2x2 block (cells [0,0]-[1,1]).
    gpd.GeoDataFrame(
        {"COMID": [10], "FTYPE": ["LakePond"]},
        geometry=[box(0, 270, 60, 300)],
        crs=CRS,
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    # BurnAdd Ice Mass polygon: a 100x100 m (10,000 m^2) block spanning rows 2-5 /
    # cols 5-8, far from the base waterbody so it can't be clump-merged with it.
    _write_burn_add_parquet(
        burn_parquet,
        [[None, None, -900555, "Ice Mass", -900555, 0.0009, _sq(150, 120)]],
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        burn_add_waterbody_table=burn_parquet,
    )
    ctx.paths["landmask"] = landmask

    produced = waterbody.build(
        {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["wbody_binary"]) as src:
        arr = src.read(1)

    assert arr[0, 0] == 1     # base LakePond present
    assert arr[5, 5] != 1     # BurnAdd Ice Mass excluded, even though it was merged in


def test_build_raises_on_configured_but_missing_burn_add_table(tmp_path):
    """A configured `burn_add_waterbody_table` that doesn't exist must fail loud."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10], "FTYPE": ["LakePond"]},
        geometry=[box(0, 270, 60, 300)],
        crs=CRS,
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        burn_add_waterbody_table=tmp_path / "does_not_exist.parquet",
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(FileNotFoundError, match="BurnAddWaterbody table not found"):
        waterbody.build(
            {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
            ctx, logging.getLogger("test"),
        )


def test_merge_burn_add_rejects_overlap_with_onstream_neighbor():
    """A BurnAdd polygon overlapping an ON-STREAM waterbody must FAIL LOUD.

    `clump_regions` labels 8-connected components, so an overlap merges the BurnAdd
    playa and the existing waterbody into ONE region. If that waterbody is on-stream,
    `regions_touching_mask` excludes the whole clump — silently DELETING the playa's
    depression area, the exact opposite of why we staged it, with nothing in the log
    to say so. Real CONUS data measured 112 of 1,658 BurnAdd/existing overlaps, none
    on-stream — but the guard must still fire on this failure mode if it ever occurs.
    """
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(50, 50)]])  # overlaps
    with pytest.raises(ValueError, match="reaches an ON-STREAM waterbody"):
        merge_burn_add(base, burn, onstream_comids={111})


def test_merge_burn_add_does_not_raise_for_overlap_with_dprst_neighbor():
    """A BurnAdd polygon overlapping an already-DPRST (not on-stream) neighbour is
    harmless and must NOT raise. `clump_regions` still merges them into one region,
    but `regions_touching_mask` only excludes clumps touching the on-stream mask —
    a dprst-only clump simply stays dprst, and the BurnAdd depression area is
    preserved. This is the real CONUS-wide case: all 112 of 1,658 measured
    BurnAdd/existing overlaps neighbour an already-dprst waterbody, none on-stream —
    so the old unconditional guard wrongly hard-failed the whole CONUS build here.
    """
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(50, 50)]])  # overlaps
    # 111 is absent from onstream_comids -> it's a dprst neighbour, not on-stream.
    out = merge_burn_add(base, burn, onstream_comids=set())
    assert len(out) == 2
    assert set(out.COMID) == {111, -367111}


def test_merge_burn_add_falls_back_to_broad_guard_when_onstream_unknown():
    """When the on-stream COMID set can't be determined (`onstream_comids=None`, the
    default — mirroring `connected_comids_table`/`flowthrough_comids_table` not
    configured or not yet staged), the guard must fall back to the OLD broad
    behaviour: raise on ANY overlap, regardless of whether the neighbour is
    actually on-stream or dprst. A false negative here (silently letting a real
    on-stream merge through unraised) is worse than the false positive of raising
    on a dprst neighbour.
    """
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(50, 50)]])  # overlaps
    with pytest.raises(ValueError, match="overlap"):
        merge_burn_add(base, burn, onstream_comids=None)


def test_build_burn_add_overlap_falls_back_to_broad_guard_without_comid_tables(tmp_path):
    """Integration check on `waterbody.build()` itself: when `connected_comids_table`
    isn't configured on the fabric profile (the `BuildContext` default), the ctx-level
    wiring (`_load_onstream_comids` returning `None`) must still reach
    `merge_burn_add`'s broad guard and raise on a real overlap — the fallback isn't
    silently skipped just because on-stream status is unknowable.
    """
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    burn_parquet = tmp_path / "burn_add.parquet"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [111], "FTYPE": ["LakePond"], "member_comid": [111]},
        geometry=[_sq(0, 0)],
        crs=CRS,
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    _write_burn_add_parquet(
        burn_parquet,
        [[None, None, -367111, "Playa", -367111, 0.01, _sq(50, 50)]],  # overlaps
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        burn_add_waterbody_table=burn_parquet,
        # connected_comids_table intentionally left at its BuildContext default (None).
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(ValueError, match="overlap"):
        waterbody.build(
            {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
            ctx, logging.getLogger("test"),
        )


def test_merge_burn_add_rejects_a_near_miss_that_would_8_connect():
    """A BurnAdd polygon that does NOT vector-intersect an existing waterbody, but is
    close enough to become an 8-connected clump once rasterized, must still FAIL LOUD.

    `clump_regions` labels 8-connected components -- cells whose centres are up to
    `cell_size * sqrt(2)` apart (a diagonal neighbour) merge into one region. A vector
    `predicate="intersects"` test alone misses this: two polygons with a real gap
    between them (here 10 m, at cell_size=30 m => diagonal ~= 42.4 m) do not
    intersect, but their rasterized cells can still land in the same 8-connected
    clump. The guard must buffer by one cell diagonal before the join to catch this,
    or it silently misses the exact hazard `test_merge_burn_add_rejects_an_overlapping_polygon`
    is meant to cover.
    """
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0, s=100)]])
    # Gap of 10 m between base's right edge (x=100) and burn's left edge (x=110) --
    # no vector intersection, but well within one 30 m cell diagonal (~42.4 m).
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(110, 0, s=100)]])
    assert not base.geometry.iloc[0].intersects(burn.geometry.iloc[0])
    with pytest.raises(ValueError, match="overlap"):
        merge_burn_add(base, burn, cell_size=30.0)


def test_merge_burn_add_normalises_object_dtype_member_comid():
    """When wb_gdf's `member_comid` is genuine object dtype (not pandas StringDtype),
    the burn frame's int64 `member_comid` must be stringified, not silently wrapped
    as raw ints via a dead `target_dtype is object` identity check.

    `pd.Series(dtype=object).dtype is object` is always False for a pandas dtype
    instance, so the old `if target_dtype is object:` branch never actually fired --
    execution fell through to `.astype(target_dtype)` every time, which on a genuine
    object dtype does nothing to int64 values (wraps them as Python ints, not str).
    This test forces a real object-dtype column so the branch has to fire correctly.
    """
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    base["member_comid"] = base["member_comid"].astype(object)
    assert pd.api.types.is_object_dtype(base["member_comid"].dtype)

    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(500, 500)]])
    # burn_add_to_waterbody_frame emits member_comid as int64.
    burn["member_comid"] = burn["member_comid"].astype("int64")

    out = merge_burn_add(base, burn)
    merged_burn_row = out[out["COMID"] == -367111].iloc[0]
    assert isinstance(merged_burn_row["member_comid"], str)
    assert merged_burn_row["member_comid"] == "-367111"


# ---------------------------------------------------------------------------
# The BurnAdd clump guard is TRANSITIVE (clump_regions is 8-connected labelling,
# and connectivity chains through intermediate waterbodies).
# ---------------------------------------------------------------------------

# 30 m cells -> one cell diagonal is 42.4 m, so squares 20 m apart clump-merge
# without vector-intersecting.
_GAP = 20


def test_merge_burn_add_raises_on_a_clump_that_reaches_onstream_TRANSITIVELY():
    """BurnAdd -> dprst waterbody -> on-stream waterbody is ONE 8-connected region.

    The direct-neighbour guard could not see this chain: the BurnAdd polygon's only
    neighbour (COMID 111) is a perfectly ordinary dprst waterbody. But `clump_regions`
    merges all three into one region, and `regions_touching_mask` then excludes the
    whole region -- silently deleting the BurnAdd playa's depression area, which is the
    exact harm this guard exists to prevent.

    The premise the old guard rested on -- "merging into an already-dprst neighbour is
    harmless, the clump simply stays dprst" -- is what the Great Salt Lake / COMID
    10273192 marsh case disproves: being dprst by COMID does not mean your REGION
    survives the on-stream exclusion.
    """
    base = _frame([
        # Chain: burn(-500) ~ 111 (dprst) ~ 222 (ON-STREAM)
        [None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)],
        [None, None, 222, "StreamRiver", 222, 0.01, _sq(100 + _GAP, 0)],
    ])
    burn = _frame([[None, None, -500, "Playa", -500, 0.01, _sq(-100 - _GAP, 0)]])

    with pytest.raises(ValueError, match="transitively"):
        merge_burn_add(base, burn, cell_size=30.0, onstream_comids={222})


def test_merge_burn_add_allows_a_clump_that_never_reaches_onstream():
    """The same chain, but nothing in it is on-stream -> the clump stays dprst.

    This is the case the guard must NOT block: 112 of 1,658 real BurnAdd polygons
    neighbour an existing waterbody, and none of their clumps reaches an on-stream
    feature, so the guard has to stay inert on real CONUS data.
    """
    base = _frame([
        [None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)],
        [None, None, 222, "LakePond", 222, 0.01, _sq(100 + _GAP, 0)],
    ])
    burn = _frame([[None, None, -500, "Playa", -500, 0.01, _sq(-100 - _GAP, 0)]])

    out = merge_burn_add(base, burn, cell_size=30.0, onstream_comids=set())
    assert set(out.COMID) == {111, 222, -500}  # merged, nothing dropped


def test_merge_burn_add_clump_walk_does_not_chain_through_ice_mass():
    """Ice Mass is dropped from the waterbody layer entirely, so it cannot carry a clump.

    `waterbody.build()` removes EXCLUDE_WATERBODY_FTYPES before rasterizing, so an Ice
    Mass polygon never becomes a cell and cannot 8-connect a BurnAdd polygon to an
    on-stream one. Chaining through it would be a false positive that blocks the build.
    """
    base = _frame([
        [None, None, 111, "Ice Mass", 111, 0.01, _sq(0, 0)],          # never rasterized
        [None, None, 222, "StreamRiver", 222, 0.01, _sq(100 + _GAP, 0)],
    ])
    burn = _frame([[None, None, -500, "Playa", -500, 0.01, _sq(-100 - _GAP, 0)]])

    out = merge_burn_add(base, burn, cell_size=30.0, onstream_comids={222})
    assert set(out.COMID) == {111, 222, -500}


def test_build_raises_on_a_configured_but_empty_burn_add_table(tmp_path):
    """A zero-row BurnAdd table means truncated/corrupted staging, not "no playas".

    `nhd_burn_components.main()` itself refuses to write an empty table, so a
    configured, present, EMPTY one can only mean corruption. `merge_burn_add` would
    quietly return the frame unchanged and ~722 km2 of playa / closed-lake depression
    area would vanish from dprst behind an INFO line reading "merged 0 polygons".
    """
    import rasterio
    from rasterio.transform import from_origin

    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    n = 10
    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    for path, arr, dt, nd in (
        (template, np.full((n, n), 100.0, dtype=np.float32), "float32", -9999.0),
        (landmask, np.ones((n, n), dtype=np.uint8), "uint8", 255),
    ):
        with rasterio.open(
            path, "w", driver="GTiff", height=n, width=n, count=1, dtype=dt,
            crs=CRS, transform=from_origin(0, n * 30, 30, 30), nodata=nd,
        ) as dst:
            dst.write(arr, 1)

    wb_gpkg = tmp_path / "wb.gpkg"
    _frame([[None, None, 111, "LakePond", "111", 0.01, _sq(0, 0)]]).to_file(
        wb_gpkg, layer="waterbodies", driver="GPKG"
    )
    empty_burn = tmp_path / "burn.parquet"
    _frame([]).to_parquet(empty_burn)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        burn_add_waterbody_table=empty_burn,
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(ValueError, match="ZERO rows"):
        waterbody.build(
            {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
            ctx, logging.getLogger("test"),
        )
