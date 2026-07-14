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

# step_cfg for wbody_connectivity.build() — two outputs (connected + endorheic),
# mirroring how the `dprst`/`waterbody` steps declare `outputs:`.
_STEP_CFG = {
    "outputs": {
        "connected_wbody": "connected_wbody.tif",
        "endorheic_wbody": "endorheic_wbody.tif",
    }
}


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


def _write_empty_endorheic(tmp_path: Path) -> Path:
    """A present-but-zero-row endorheic table — the no-closed-basin no-op case.

    `wbody_connectivity` now raises if `endorheic_comids` is missing from the
    build context entirely (see test_endorheic_comids_missing_from_context_raises).
    Every builder test in this module that doesn't specifically exercise that
    raise needs a *present* endorheic table wired in, so an empty one (rather
    than a populated one) keeps these fixtures' on-stream sets unchanged.
    """
    path = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        columns=["comid", "frac_own", "by_terminus", "by_closed_huc12"]
    ).to_parquet(path, index=False)
    return path


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
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)

    produced = wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("test")
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
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


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
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)

    with pytest.raises(ValueError, match="matched 0 of"):
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


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
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)

    produced = wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("test")
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
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)

    produced = wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("test")
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
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


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
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


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
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)

    produced = wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("test")
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
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)

    with pytest.raises(KeyError, match="FTYPE"):
        wbody_connectivity.build(
            _STEP_CFG, ctx, logging.getLogger("test")
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
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)

    produced = wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("test")
    )
    assert "connected_wbody" in produced


# ---------------------------------------------------------------------------
# Endorheic demotion (Task 6)
# ---------------------------------------------------------------------------


def test_endorheic_comid_is_demoted_from_the_connected_raster(tmp_path):
    """A waterbody the endorheic classifier flags must NOT reach connected_wbody.tif.

    This is the Great Salt Lake path: it IS in the WBAREACOMI/flow-through union
    (both local classifiers promote it), and the endorheic subtraction is the only
    thing that takes it back out.
    """
    import logging

    import geopandas as gpd
    import pandas as pd
    import rasterio
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    _write_template(template)
    _write_landmask(landmask)

    # Two on-stream waterbodies; COMID 2 is also flagged endorheic. Grid-aligned to
    # the 30 m _write_template/_write_landmask pixels (see the top-left/middle boxes
    # used by the other builder tests in this file) so `rasterize_binary`'s
    # pixel-center predicate actually burns them.
    wb = gpd.GeoDataFrame(
        {"COMID": [1, 2], "member_comid": [1, 2], "FTYPE": ["LakePond", "LakePond"]},
        geometry=[box(0, 270, 60, 300), box(120, 150, 180, 180)],
        crs="EPSG:5070",
    )
    wb_path = tmp_path / "wb.gpkg"
    wb.to_file(wb_path, layer="waterbodies", driver="GPKG")

    conn = tmp_path / "connected.parquet"
    pd.DataFrame({"comid": [1, 2]}).to_parquet(conn, index=False)

    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        {"comid": [2], "frac_own": [1.0], "by_terminus": [True],
         "by_closed_huc12": [False]}
    ).to_parquet(endo, index=False)

    ctx = BuildContext(
        fabric="test", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_path, hru_layer="waterbodies",
        waterbody_gpkg=wb_path, waterbody_layer="waterbodies",
        connected_comids_table=conn,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo

    wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("t")
    )
    with rasterio.open(tmp_path / "connected_wbody.tif") as src:
        arr = src.read(1)

    # COMID 1 (on-stream, not endorheic) is rasterised; COMID 2 (demoted) is not.
    assert (arr[0:1, 0:2] == 1).any(), "COMID 1 should still be on-stream"
    assert not (arr[4:6, 4:6] == 1).any(), "COMID 2 was endorheic and must be demoted"


def test_endorheic_comids_missing_from_context_raises(tmp_path):
    """A MISSING endorheic table (the `endorheic` step never ran) must fail loud.

    Distinct from an EMPTY table (tjc — a legitimate no-op, see the test below):
    this is the case where `endorheic_comids` was never produced at all, and
    silently proceeding would leave terminal lakes like the Great Salt Lake
    classified on-stream with no signal beyond a log line.
    """
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
    )
    ctx.paths["landmask"] = landmask
    # NOTE: `endorheic_comids` deliberately NOT set in ctx.paths.

    with pytest.raises(KeyError, match="endorheic"):
        wbody_connectivity.build(
            _STEP_CFG, ctx, logging.getLogger("test")
        )


def test_endorheic_comids_empty_table_is_a_legitimate_noop(tmp_path):
    """An EMPTY endorheic table (e.g. tjc, no closed basin) must NOT raise.

    Distinct from the missing-table case above: the `endorheic` step ran and
    produced a present-but-zero-row table, which is a correct no-op subtraction.
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

    gpd.GeoDataFrame(
        {"COMID": [10], "member_comid": ["10"], "FTYPE": ["LakePond"]},
        geometry=[box(0, 270, 60, 300)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": pd.array([10], dtype="int64")}).to_parquet(
        connected_table, index=False
    )

    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        columns=["comid", "frac_own", "by_terminus", "by_closed_huc12"]
    ).to_parquet(endo, index=False)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo

    produced = wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("test")
    )
    with rasterio.open(produced["connected_wbody"]) as src:
        arr = src.read(1)
    assert arr[0, 0] == 1  # on-stream waterbody untouched by an empty subtraction


def test_endorheic_subtraction_never_widens_the_onstream_set(tmp_path):
    """An endorheic COMID that is NOT on-stream must be a pure no-op.

    The safety invariant: these signals may only SUBTRACT. If this ever fails, the
    endorheic table has become capable of ADDING to the on-stream mask.
    """
    import logging

    import geopandas as gpd
    import pandas as pd
    import rasterio
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    _write_template(template)
    _write_landmask(landmask)

    # Grid-aligned to the 30 m template pixels — see the note in the previous test.
    # COMID 1 is genuinely on-stream (in the connected table). COMID 999 has a real
    # waterbody polygon (middle block, cells [4,4],[4,5],[5,4],[5,5]) but is NOT in
    # the connected table -- i.e. it was never on-stream. If the subtraction ever
    # regressed to a union (`connected | endorheic` instead of `connected -
    # endorheic`), COMID 999 would be wrongly added to the on-stream set and its
    # pixels would burn -- this fixture is what makes that regression observable;
    # with no polygon for 999, byte-identical output would hide the bug.
    wb = gpd.GeoDataFrame(
        {"COMID": [1, 999], "member_comid": [1, 999], "FTYPE": ["LakePond", "LakePond"]},
        geometry=[box(0, 270, 60, 300), box(120, 150, 180, 180)], crs="EPSG:5070",
    )
    wb_path = tmp_path / "wb.gpkg"
    wb.to_file(wb_path, layer="waterbodies", driver="GPKG")

    conn = tmp_path / "connected.parquet"
    pd.DataFrame({"comid": [1]}).to_parquet(conn, index=False)

    # COMID 999 is endorheic but was never on-stream: subtracting it changes nothing.
    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        {"comid": [999], "frac_own": [1.0], "by_terminus": [True],
         "by_closed_huc12": [False]}
    ).to_parquet(endo, index=False)

    ctx = BuildContext(
        fabric="test", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_path, hru_layer="waterbodies",
        waterbody_gpkg=wb_path, waterbody_layer="waterbodies",
        connected_comids_table=conn,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo

    wbody_connectivity.build(
        _STEP_CFG, ctx, logging.getLogger("t")
    )
    with rasterio.open(tmp_path / "connected_wbody.tif") as src:
        arr = src.read(1)
    assert (arr[0:1, 0:2] == 1).any(), "COMID 1 must remain on-stream — no widening"
    assert not (arr[4:6, 4:6] == 1).any(), (
        "COMID 999 was never on-stream; a union-regression would wrongly burn it"
    )


# ---------------------------------------------------------------------------
# endorheic_wbody.tif (the second output — GSL-clump-veto fix)
# ---------------------------------------------------------------------------


def test_wbody_connectivity_writes_endorheic_wbody_raster(tmp_path):
    """`endorheic_wbody.tif` must burn the FULL endorheic set, on-stream or not.

    COMID 2 is both on-stream (WBAREACOMI) AND endorheic — the Great Salt Lake
    case: it is demoted from `connected_wbody.tif` by the subtraction, but it
    must still appear in `endorheic_wbody.tif`, since that raster carries
    positive hydrologic evidence independent of on-stream status (this is what
    lets `dprst.py` exempt it later even though its clump also touches an
    on-stream neighbour). COMID 1 is on-stream and NOT endorheic, so it must be
    in `connected_wbody.tif` but absent from `endorheic_wbody.tif`.
    """
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    wb = gpd.GeoDataFrame(
        {"COMID": [1, 2], "member_comid": [1, 2], "FTYPE": ["LakePond", "LakePond"]},
        geometry=[box(0, 270, 60, 300), box(120, 150, 180, 180)],
        crs="EPSG:5070",
    )
    wb_path = tmp_path / "wb.gpkg"
    wb.to_file(wb_path, layer="waterbodies", driver="GPKG")

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    _write_template(template)
    _write_landmask(landmask)

    conn = tmp_path / "connected.parquet"
    pd.DataFrame({"comid": [1, 2]}).to_parquet(conn, index=False)

    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        {"comid": [2], "frac_own": [1.0], "by_terminus": [True],
         "by_closed_huc12": [False]}
    ).to_parquet(endo, index=False)

    ctx = BuildContext(
        fabric="test", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_path, hru_layer="waterbodies",
        waterbody_gpkg=wb_path, waterbody_layer="waterbodies",
        connected_comids_table=conn,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo

    produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("t"))
    assert "endorheic_wbody" in produced

    with rasterio.open(produced["connected_wbody"]) as src:
        connected_arr = src.read(1)
    with rasterio.open(produced["endorheic_wbody"]) as src:
        endorheic_arr = src.read(1)

    # COMID 1: on-stream, not endorheic.
    assert (connected_arr[0:1, 0:2] == 1).any()
    assert not (endorheic_arr[0:1, 0:2] == 1).any()
    # COMID 2: endorheic-demoted out of connected_wbody, but present in
    # endorheic_wbody regardless (positive evidence, independent of on-stream).
    assert not (connected_arr[4:6, 4:6] == 1).any()
    assert (endorheic_arr[4:6, 4:6] == 1).any()


# ---------------------------------------------------------------------------
# The `min_endorheic_comids` floor at the CONSUMING end, and the COMID-keyed
# contract between the endorheic table and the member_comid join.
# ---------------------------------------------------------------------------


def _endorheic_fixture(tmp_path, *, endorheic_rows, comids=(10, 20), member=None):
    """A two-waterbody fixture wired for wbody_connectivity, with a chosen endorheic table."""
    from shapely.geometry import box

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    connected_table = tmp_path / "connected.parquet"
    _write_template(template)
    _write_landmask(landmask)

    member = member if member is not None else [str(c) for c in comids]
    gpd.GeoDataFrame(
        {"COMID": list(comids), "member_comid": member,
         "FTYPE": ["LakePond"] * len(comids)},
        geometry=[box(0, 270, 60, 300), box(240, 0, 300, 30)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": pd.array(list(comids), dtype="int64")}).to_parquet(
        connected_table, index=False
    )

    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        endorheic_rows,
        columns=["comid", "frac_own", "by_terminus", "by_closed_huc12"],
    ).astype(
        {"comid": "int64", "frac_own": "float64",
         "by_terminus": "bool", "by_closed_huc12": "bool"}
    ).to_parquet(endo, index=False)
    return template, landmask, wb_gpkg, connected_table, endo


def _endorheic_ctx(tmp_path, *, endorheic_rows, member=None, **kw):
    from gfv2_params.depstor_builders.context import BuildContext

    template, landmask, wb_gpkg, connected_table, endo = _endorheic_fixture(
        tmp_path, endorheic_rows=endorheic_rows, member=member
    )
    ctx = BuildContext(
        fabric="gfv2", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table, **kw,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo
    return ctx


def test_endorheic_floor_is_enforced_at_the_consuming_end(tmp_path):
    """The floor must fire HERE, not only in the `endorheic` builder that wrote the table.

    `--from wbody_connectivity --force` is the documented cascade-rebuild recipe
    (slurm_batch/RUNME.md), and it leaves `endorheic` out of the run list entirely — the
    orchestrator hydrates its table straight off disk with no validation. A collapsed
    table left behind by an aborted run would then subtract the empty set, write an
    all-nodata `endorheic_wbody.tif`, and take the whole CONUS cascade green with the
    Great Salt Lake still on-stream. The producing builder's floor never executes on
    that path, so it cannot be the only guard.
    """
    import pytest

    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _endorheic_ctx(tmp_path, endorheic_rows=[], min_endorheic_comids=100)
    with pytest.raises(ValueError, match="min_endorheic_comids"):
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_endorheic_empty_table_still_a_noop_without_a_floor(tmp_path):
    """The floor is opt-in: a fabric that declares none (tjc) still tolerates empty."""
    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _endorheic_ctx(tmp_path, endorheic_rows=[])
    produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))
    with rasterio.open(produced["connected_wbody"]) as src:
        assert src.read(1)[0, 0] == 1  # untouched by an empty subtraction


def test_endorheic_demotion_cannot_be_undone_through_member_comid(tmp_path):
    """A waterbody demoted by COMID must not return on-stream via its member_comid.

    The demotion is COMID-keyed but `select_connected_waterbodies` promotes on COMID
    **or** `member_comid`. Inert on the real layer today (the keys agree on every
    numeric row) — this is what makes that a checked fact rather than a comment. Here
    COMID 10 is demoted, but its member_comid points at 20, which is still on-stream:
    without the guard it would be silently re-promoted and the demotion lost.
    """
    import pytest

    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _endorheic_ctx(
        tmp_path,
        endorheic_rows=[[10, 1.0, True, False]],
        member=["20", "20"],  # COMID 10's member_comid points at the on-stream COMID 20
    )
    with pytest.raises(ValueError, match="re-promoted through `member_comid`"):
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_endorheic_raster_cannot_gain_a_comid_the_classifier_never_flagged(tmp_path):
    """The endorheic signals may only SUBTRACT — this is where that could invert.

    `endorheic_wbody.tif` is selected with the same COMID-or-member_comid join, but
    here a member_comid match is NOT legitimate: `dprst` exempts these cells from the
    region-level on-stream exclusion, so a polygon selected on a key its own COMID
    never earned would be ADDED to depression storage on evidence the classifier never
    produced for it. COMID 20 is not endorheic, but its member_comid points at the
    endorheic COMID 10.
    """
    import pytest

    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _endorheic_ctx(
        tmp_path,
        endorheic_rows=[[10, 1.0, True, False]],
        member=["10", "10"],  # COMID 20's member_comid points at the endorheic COMID 10
    )
    with pytest.raises(ValueError, match="without their own COMID being flagged"):
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_endorheic_wbody_raster_is_land_masked(tmp_path):
    """`endorheic_wbody.tif` must be masked against `land_mask.tif` like every other
    depstor raster. The land-mask test above only inspected `connected_wbody`, so
    deleting the endorheic raster's `[not_land] = 255` left the suite green.
    """
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    n = 10
    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    connected_table = tmp_path / "connected.parquet"
    _write_template(template, n)

    # Land only in the TOP half; the bottom half is ocean.
    transform = from_origin(0, n * 30, 30, 30)
    land = np.zeros((n, n), dtype=np.uint8)
    land[0:5, :] = 1
    with rasterio.open(
        landmask, "w", driver="GTiff", height=n, width=n, count=1, dtype="uint8",
        crs="EPSG:5070", transform=transform, nodata=255,
    ) as dst:
        dst.write(land, 1)

    # COMID 10 on-stream (top-left, on land). COMID 20 endorheic, straddling the
    # land/ocean boundary: rows 3-6, so its bottom half must be masked off.
    gpd.GeoDataFrame(
        {"COMID": [10, 20], "member_comid": [10, 20],
         "FTYPE": ["LakePond", "LakePond"]},
        geometry=[box(0, 270, 60, 300), box(0, 120, 60, 210)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": pd.array([10], dtype="int64")}).to_parquet(
        connected_table, index=False
    )

    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        {"comid": pd.array([20], dtype="int64"), "frac_own": [1.0],
         "by_terminus": [True], "by_closed_huc12": [False]}
    ).to_parquet(endo, index=False)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo

    produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))
    with rasterio.open(produced["endorheic_wbody"]) as src:
        arr = src.read(1)

    assert (arr[3:5, 0:2] == 1).any(), "endorheic cells on LAND must be burned"
    assert not (arr[5:, :] == 1).any(), "endorheic cells over OCEAN must be masked off"
