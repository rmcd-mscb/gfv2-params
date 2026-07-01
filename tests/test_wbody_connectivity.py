"""Tests for WBAREACOMI-driven waterbody connectivity (helper + builder)."""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon

from gfv2_params.depstor import load_connected_comids, select_connected_waterbodies


def _sq(x):
    return Polygon([(x, 0), (x + 1, 0), (x + 1, 1), (x, 1)])


def test_union_promotes_flowthrough_only_waterbody():
    # member_comid is a string in the real GPKG schema. The union must flag:
    #   WB 200 — WBAREACOMI-connected (COMID branch),
    #   WB 201 — ONLY flow-through, matched by COMID (WBAREACOMI alone misses it),
    #   WB 300 — a merged waterbody whose top-level COMID (300) is in NEITHER set
    #            but whose member_comid (777) IS in the flow-through set; this is
    #            the production case where the merged COMID differs from the
    #            original NHDWaterbody COMID the flow-through set is keyed by.
    # WB 202 is in neither set and must stay unflagged.
    wb = gpd.GeoDataFrame(
        {"COMID": [200, 201, 202, 300],
         "member_comid": ["200", "201", "202", "777"],
         "geometry": [_sq(0), _sq(2), _sq(4), _sq(6)]},
        crs="EPSG:4269",
    )
    connected = {200}
    flowthrough = {201, 777}
    union = connected | flowthrough
    sel = select_connected_waterbodies(wb, union)
    assert set(sel["COMID"]) == {200, 201, 300}


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


def test_select_connected_missing_join_columns_raises():
    import pytest

    # A waterbody layer without COMID/member_comid can't be joined; the error
    # must name the missing column rather than surface a bare pandas KeyError.
    gdf = gpd.GeoDataFrame(
        {"GNIS_NAME": ["a"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:5070",
    )
    with pytest.raises(KeyError, match="member_comid"):
        select_connected_waterbodies(gdf, {1})


# ---------------------------------------------------------------------------
# Builder tests
# ---------------------------------------------------------------------------


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


def test_wbody_connectivity_rasterizes_only_connected(tmp_path):
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    table = tmp_path / "connected.parquet"
    _write_template(template)
    _write_landmask(landmask)

    # Connected polygon (COMID 10) at top-left; disconnected (COMID 20) bottom-right.
    gdf = gpd.GeoDataFrame(
        {"COMID": [10, 20], "member_comid": ["10", "20"],
         "FTYPE": ["LakePond", "LakePond"]},
        geometry=[box(0, 270, 60, 300), box(240, 0, 300, 30)],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": [10]}).to_parquet(table, index=False)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=table,
    )
    ctx.paths["landmask"] = landmask

    produced = wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("test")
    )

    out = produced["connected_wbody"]
    with rasterio.open(out) as src:
        arr = src.read(1)
        assert src.nodata == 255
    assert arr[0, 0] == 1     # connected polygon burned
    assert arr[9, 9] != 1     # disconnected polygon NOT burned
    assert int((arr == 1).sum()) > 0


def test_wbody_connectivity_requires_table(tmp_path):
    import pytest

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    _write_template(template)
    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="waterbodies",
        waterbody_gpkg=tmp_path / "x.gpkg", waterbody_layer="waterbodies",
        connected_comids_table=None,
    )
    with pytest.raises(KeyError):
        wbody_connectivity.build({"output": "connected_wbody.tif"}, ctx, logging.getLogger("test"))


def test_wbody_connectivity_zero_match_raises(tmp_path):
    """Zero waterbodies matched would misclassify everything as dprst -> fail loud."""
    import pytest
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    table = tmp_path / "connected.parquet"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10, 20], "member_comid": ["10", "20"],
         "FTYPE": ["LakePond", "LakePond"]},
        geometry=[box(0, 270, 60, 300), box(240, 0, 300, 30)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    # Connected set shares no COMID with the waterbodies -> 0 matches.
    pd.DataFrame({"comid": [999]}).to_parquet(table, index=False)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=table,
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(ValueError, match="matched 0 of"):
        wbody_connectivity.build({"output": "connected_wbody.tif"}, ctx, logging.getLogger("test"))


def test_wbody_connectivity_drops_non_land_cells(tmp_path):
    """A connected polygon over a non-land cell must still be masked to nodata."""
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    table = tmp_path / "connected.parquet"
    _write_template(template)

    # Land mask: all land EXCEPT the top-left cell [0, 0] (ocean), which the
    # connected polygon below covers. Land=1, off-land=255 (nodata).
    transform = from_origin(0, 10 * 30, 30, 30)
    mask = np.ones((10, 10), dtype=np.uint8)
    mask[0, 0] = 255
    with rasterio.open(
        landmask, "w", driver="GTiff", height=10, width=10, count=1, dtype="uint8",
        crs="EPSG:5070", transform=transform, nodata=255,
    ) as dst:
        dst.write(mask, 1)

    # Connected polygon covers the top-left 2x2 block (cells [0,0],[0,1],[1,0],[1,1]).
    gpd.GeoDataFrame(
        {"COMID": [10], "member_comid": ["10"], "FTYPE": ["LakePond"]},
        geometry=[box(0, 240, 60, 300)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": [10]}).to_parquet(table, index=False)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=table,
    )
    ctx.paths["landmask"] = landmask

    produced = wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("test")
    )
    with rasterio.open(produced["connected_wbody"]) as src:
        arr = src.read(1)
    assert arr[0, 0] == 255   # ocean cell dropped despite connected polygon
    assert arr[0, 1] == 1     # adjacent land cell under the polygon still burned


# ---------------------------------------------------------------------------
# Flow-through union and guard tests (Fix 3)
# ---------------------------------------------------------------------------


def test_wbody_connectivity_flowthrough_only_waterbody_burned(tmp_path):
    """A flow-through-only waterbody (not in WBAREACOMI set) must be burned."""
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    connected_table = tmp_path / "connected.parquet"
    flowthrough_table = tmp_path / "flowthrough.parquet"
    _write_template(template)
    _write_landmask(landmask)

    # 3 waterbodies:
    #   WB 10 (COMID 10) — top-left block, WBAREACOMI-connected only
    #   WB 20 (COMID 20) — middle block, flow-through only
    #   WB 30 (COMID 30) — bottom-right block, in neither set
    gdf = gpd.GeoDataFrame(
        {"COMID": [10, 20, 30], "member_comid": ["10", "20", "30"],
         "FTYPE": ["LakePond", "LakePond", "LakePond"]},
        geometry=[
            box(0, 270, 60, 300),    # top-left 2x2: cells [0,0],[0,1],[1,0],[1,1]
            box(120, 150, 180, 180), # middle area: cells ~[4,4],[4,5],[5,4],[5,5]
            box(240, 0, 300, 30),    # bottom-right 2x2: cells [8,8],[8,9],[9,8],[9,9]
        ],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    # WBAREACOMI set: only WB 10
    pd.DataFrame({"comid": pd.array([10], dtype="int64")}).to_parquet(
        connected_table, index=False
    )
    # Flow-through set: only WB 20
    pd.DataFrame({"comid": pd.array([20], dtype="int64")}).to_parquet(
        flowthrough_table, index=False
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
        flowthrough_comids_table=flowthrough_table,
    )
    ctx.paths["landmask"] = landmask

    produced = wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("test")
    )

    with rasterio.open(produced["connected_wbody"]) as src:
        arr = src.read(1)

    assert arr[0, 0] == 1              # WB 10 burned (WBAREACOMI)
    assert arr[4, 4:6].max() == 1     # WB 20 burned (flow-through only; box covers row 4, cols 4-5)
    assert arr[9, 9] != 1             # WB 30 NOT burned (in neither set)


def test_wbody_connectivity_flowthrough_missing_raises(tmp_path):
    """Pointing flowthrough_comids_table at a nonexistent file must raise FileNotFoundError."""
    import pytest

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    _write_template(template)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="waterbodies",
        waterbody_gpkg=tmp_path / "x.gpkg", waterbody_layer="waterbodies",
        connected_comids_table=tmp_path / "connected.parquet",
        flowthrough_comids_table=tmp_path / "does_not_exist.parquet",
    )
    with pytest.raises(FileNotFoundError):
        wbody_connectivity.build({"output": "connected_wbody.tif"}, ctx, logging.getLogger("test"))


def test_wbody_connectivity_flowthrough_empty_raises(tmp_path):
    """An empty flow-through parquet must raise ValueError with 'empty' in the message."""
    import pytest
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    connected_table = tmp_path / "connected.parquet"
    flowthrough_table = tmp_path / "empty_flowthrough.parquet"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10], "member_comid": ["10"]},
        geometry=[box(0, 270, 60, 300)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": pd.array([10], dtype="int64")}).to_parquet(
        connected_table, index=False
    )
    # Empty flow-through table (0 rows)
    pd.DataFrame({"comid": pd.array([], dtype="int64")}).to_parquet(
        flowthrough_table, index=False
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
        flowthrough_comids_table=flowthrough_table,
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(ValueError, match="empty"):
        wbody_connectivity.build({"output": "connected_wbody.tif"}, ctx, logging.getLogger("test"))


def test_wbody_connectivity_force_dprst_ftypes_excluded(tmp_path):
    """Playa/Ice Mass waterbodies promoted on-stream via WBAREACOMI must not burn.

    The guardrail (previously applied only inside nhd_flowthrough's flow-through
    classifier) must also apply at the wbody_connectivity union chokepoint,
    since WBAREACOMI promotion has no guardrail of its own. It now uses
    NEVER_ONSTREAM_FTYPES (= FORCE_DPRST_FTYPES | EXCLUDE_WATERBODY_FTYPES), so
    both Playa (force-dprst) and Ice Mass (excluded from the waterbody
    classification entirely — belt-and-suspenders, since Ice Mass is already
    dropped upstream at the waterbody builder) are kept out of the on-stream set.
    """
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    connected_table = tmp_path / "connected.parquet"
    _write_template(template)
    _write_landmask(landmask)

    # 3 waterbodies, all WBAREACOMI-connected:
    #   WB 10 (COMID 10) — LakePond, ordinary connected waterbody -> burned
    #   WB 20 (COMID 20) — Playa, force-dprst FTYPE -> must be dropped
    #   WB 30 (COMID 30) — Ice Mass, excluded FTYPE -> must be dropped
    gdf = gpd.GeoDataFrame(
        {
            "COMID": [10, 20, 30],
            "member_comid": ["10", "20", "30"],
            "FTYPE": ["LakePond", "Playa", "Ice Mass"],
        },
        geometry=[
            box(0, 270, 60, 300),    # top-left 2x2: cells [0,0],[0,1],[1,0],[1,1]
            box(240, 0, 300, 30),    # bottom-right 2x2: cells [8,8],[8,9],[9,8],[9,9]
            box(120, 150, 180, 180), # middle: cells [4,4],[4,5],[5,4],[5,5]
        ],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": pd.array([10, 20, 30], dtype="int64")}).to_parquet(
        connected_table, index=False
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
    )
    ctx.paths["landmask"] = landmask

    produced = wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("test")
    )

    with rasterio.open(produced["connected_wbody"]) as src:
        arr = src.read(1)

    assert arr[0, 0] == 1              # WB 10 (LakePond) burned
    assert arr[9, 9] != 1              # WB 20 (Playa) NOT burned despite WBAREACOMI connection
    assert arr[4, 4:6].max() != 1      # WB 30 (Ice Mass) NOT burned despite WBAREACOMI connection


def test_wbody_connectivity_missing_ftype_column_raises(tmp_path):
    """A waterbody layer without FTYPE must raise (refuse to silently skip the
    never-on-stream guardrail and promote a Playa/Ice Mass waterbody)."""
    import pytest
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    connected_table = tmp_path / "connected.parquet"
    _write_template(template)
    _write_landmask(landmask)

    # Same fixture as test_wbody_connectivity_force_dprst_ftypes_excluded, minus
    # the FTYPE column.
    gdf = gpd.GeoDataFrame(
        {
            "COMID": [10, 20, 30],
            "member_comid": ["10", "20", "30"],
        },
        geometry=[
            box(0, 270, 60, 300),    # top-left 2x2: cells [0,0],[0,1],[1,0],[1,1]
            box(240, 0, 300, 30),    # bottom-right 2x2: cells [8,8],[8,9],[9,8],[9,9]
            box(120, 150, 180, 180), # middle: cells [4,4],[4,5],[5,4],[5,5]
        ],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": pd.array([10, 20, 30], dtype="int64")}).to_parquet(
        connected_table, index=False
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(KeyError):
        wbody_connectivity.build(
            {"output": "connected_wbody.tif"}, ctx, logging.getLogger("test")
        )


def test_wbody_connectivity_flowthrough_none_is_silent_noop(tmp_path):
    """When flowthrough_comids_table is None, build() must succeed without error."""
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    connected_table = tmp_path / "connected.parquet"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10], "member_comid": ["10"], "FTYPE": ["LakePond"]},
        geometry=[box(0, 270, 60, 300)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": pd.array([10], dtype="int64")}).to_parquet(
        connected_table, index=False
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
        flowthrough_comids_table=None,
    )
    ctx.paths["landmask"] = landmask

    produced = wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("test")
    )
    assert "connected_wbody" in produced
