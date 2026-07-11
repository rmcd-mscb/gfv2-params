"""Integration test for the dprst_depth builder (issue #173 Task 7).

`test_dprst_depth_build_end_to_end` drives `build()` on a tiny synthetic
fabric: a local 1000x1000 m / 1 m-cell DEM (in place of a real 3DEP tile),
two dprst polygons (one real, non-flat depression; one hydro-flattened —
`fill.py`'s fallback ladder must supply its depth), a tiny HRU/ecoregion/WESM
footprint set, and no per-batch parquet dir (exercises the in-process
`tiling.group_by_tile` + `compute.run_batch` path).

No live S3 read: `rasterio.open` is monkeypatched to redirect any
`/vsicurl/`-or-`/vsis3/`-prefixed path to the local synthetic DEM. This is
deliberately a broader interception point than the task brief's suggested
`topo.read_window` patch: `compute.run_batch`'s single-tile fast path
(`_open_tile_vrt`) calls `rasterio.open(tile_key)` directly and never goes
through `read_window` at all, so patching only `read_window` would miss it.
Patching `rasterio.open` (auto-reverted by `monkeypatch`) covers both the
fast tile-cache path and the multi-tile `compute_polygon` fallback path
uniformly, whichever one `group_by_tile`'s real geometry math happens to
route each polygon through.
"""
from __future__ import annotations

import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from gfv2_params.depstor_builders import BUILDERS, dprst_depth
from gfv2_params.depstor_builders.context import BuildContext

_CRS = "EPSG:5070"


def _L():
    return logging.getLogger("test_dprst_depth")


def test_dprst_depth_registered():
    # BUILDERS values are the bound `build` functions themselves (see
    # depstor_builders/__init__.py: `"dprst_depth": dprst_depth.build`), not
    # the module -- every other entry follows the same convention.
    assert "dprst_depth" in BUILDERS
    assert callable(BUILDERS["dprst_depth"])
    assert BUILDERS["dprst_depth"] is dprst_depth.build


def _write_dem(path):
    """1000x1000 m, 1 m cells. Baseline 100.0 everywhere; two 40x40 m
    features far enough apart that each one's 200 m rim-buffered read
    window stays inside the raster (avoids an out-of-bounds windowed read).

    Polygon A footprint (x,y in [280,320]): a real, non-flat depression —
    a gradient pit (95.0 -> 90.0), interior range 5 m >> the 0.01 m
    hydro-flattening tolerance -> `flat=False`, a genuine measured depth.
    Polygon B footprint (x,y in [680,720]): a hydro-flattened surface — an
    EXACTLY constant 95.0 -> interior range 0 -> `flat=True`, must go
    through fill.py's fallback ladder.
    """
    n = 1000
    dem = np.full((n, n), 100.0, dtype=np.float32)
    transform = from_origin(0, n, 1, 1)  # row = n - y ; col = x

    # A: rows 680:720 (y 280..320), cols 280:320 (x 280..320)
    pit = np.linspace(95.0, 90.0, num=40)
    dem[680:720, 280:320] = np.tile(pit, (40, 1))

    # B: rows 680:720 (y 280..320), cols 680:720 (x 680..720)
    dem[680:720, 680:720] = 95.0

    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1,
        dtype="float32", crs=_CRS, transform=transform, nodata=-999999.0,
    ) as dst:
        dst.write(dem, 1)


def _write_template_and_landmask(tmp_path):
    # Coarser 10 m grid over the same 0..1000 extent — burn_depth rasterizes
    # dynamically onto whatever template grid is given, independent of the
    # DEM tile source's resolution/extent.
    n = 100
    transform = from_origin(0, 1000, 10, 10)
    tmpl = tmp_path / "template.tif"
    with rasterio.open(
        tmpl, "w", driver="GTiff", height=n, width=n, count=1,
        dtype="float32", crs=_CRS, transform=transform, nodata=-9999.0,
    ) as d:
        d.write(np.full((n, n), 100.0, np.float32), 1)
    lm = tmp_path / "land_mask.tif"
    with rasterio.open(
        lm, "w", driver="GTiff", height=n, width=n, count=1,
        dtype="uint8", crs=_CRS, transform=transform, nodata=0,
    ) as d:
        d.write(np.ones((n, n), np.uint8), 1)
    return tmpl, lm


def _write_waterbody_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {
            "COMID": [101, 102],
            "member_comid": ["101", "102"],
            "FTYPE": ["LakePond", "LakePond"],
            "geometry": [box(280, 280, 320, 320), box(680, 280, 720, 320)],
        },
        crs=_CRS,
    )
    gdf.to_file(path, layer="waterbodies", driver="GPKG")


def _write_connected_parquet(path):
    # Empty connected set: neither polygon is on-stream -> both stay dprst.
    pd.DataFrame({"comid": pd.Series([], dtype="int64")}).to_parquet(path)


def _write_ecoregions_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {"US_L3CODE": ["17"], "geometry": [box(-1000, -1000, 2000, 2000)]}, crs=_CRS,
    )
    gdf.to_file(path, layer="ecoregions", driver="GPKG")


def _write_wesm_gpkg(path):
    # A footprint far from both dprst polygons -> resolution_class tags
    # everything "10m" (no covering 1m project), keeping the compute path
    # on the simple seamless-tile branch.
    gdf = gpd.GeoDataFrame(
        {"project": ["unused"], "geometry": [box(10_000_000, 10_000_000, 10_000_100, 10_000_100)]},
        crs=_CRS,
    )
    gdf.to_file(path, layer="wesm", driver="GPKG")


def _write_hru_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {
            "hru_id": [1, 2],
            "geometry": [box(250, 250, 350, 350), box(650, 250, 750, 350)],
        },
        crs=_CRS,
    )
    gdf.to_file(path, layer="nhru", driver="GPKG")


def test_dprst_depth_build_end_to_end(tmp_path, monkeypatch):
    dem_path = tmp_path / "local_dem.tif"
    _write_dem(dem_path)

    real_open = rasterio.open

    def _fake_open(path, *args, **kwargs):
        if isinstance(path, str) and ("/vsicurl/" in path or "/vsis3/" in path):
            return real_open(str(dem_path), *args, **kwargs)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(rasterio, "open", _fake_open)

    tmpl, lm = _write_template_and_landmask(tmp_path)

    waterbody_gpkg = tmp_path / "waterbodies.gpkg"
    _write_waterbody_gpkg(waterbody_gpkg)
    connected_table = tmp_path / "connected.parquet"
    _write_connected_parquet(connected_table)
    ecoregions_gpkg = tmp_path / "ecoregions.gpkg"
    _write_ecoregions_gpkg(ecoregions_gpkg)
    wesm_index = tmp_path / "wesm.gpkg"
    _write_wesm_gpkg(wesm_index)
    hru_gpkg = tmp_path / "hru.gpkg"
    _write_hru_gpkg(hru_gpkg)

    ctx = BuildContext(
        fabric="t", template_path=tmpl, output_dir=tmp_path,
        hru_gpkg=hru_gpkg, hru_layer="nhru", id_feature="hru_id",
        waterbody_gpkg=waterbody_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
        wesm_index=wesm_index, ecoregions_gpkg=ecoregions_gpkg,
    )
    ctx.paths["landmask"] = lm

    step_cfg = {"outputs": {"dprst_depth": "dprst_depth.tif", "op_flow_thres": "op_flow_thres_params.csv"}}
    produced = dprst_depth.build(step_cfg, ctx, _L())

    assert produced["dprst_depth"].exists()
    assert produced["op_flow_thres"].exists()

    # op_flow_thres: constant 1.0 for every HRU.
    op_flow = pd.read_csv(produced["op_flow_thres"])
    assert set(op_flow["hru_id"]) == {1, 2}
    assert (op_flow["op_flow_thres"] == 1.0).all()

    # HRU 1 (covers polygon A) has a positive burned depth via a manual
    # zonal read (Task 8 owns the real gdptools aggregation).
    with rasterio.open(produced["dprst_depth"]) as src:
        from rasterio.mask import mask as rio_mask
        hru1_geom = [box(250, 250, 350, 350)]
        out_arr, _ = rio_mask(src, hru1_geom, crop=True)
        nodata = src.nodata
        valid = out_arr[0][out_arr[0] != nodata]
        assert valid.size > 0
        assert valid.mean() > 0

        hru2_geom = [box(650, 250, 750, 350)]
        out_arr2, _ = rio_mask(src, hru2_geom, crop=True)
        valid2 = out_arr2[0][out_arr2[0] != nodata]
        assert valid2.size > 0
        assert valid2.mean() > 0


def test_dprst_depth_skips_when_outputs_exist(tmp_path, monkeypatch):
    tmpl, lm = _write_template_and_landmask(tmp_path)
    depth_out = tmp_path / "dprst_depth.tif"
    op_flow_out = tmp_path / "op_flow_thres_params.csv"
    depth_out.write_bytes(b"placeholder")
    op_flow_out.write_text("hru_id,op_flow_thres\n1,1.0\n")

    def _boom(*a, **k):
        raise AssertionError("build() should have skipped — outputs already exist")

    monkeypatch.setattr(dprst_depth, "_load_dprst_polygons", _boom)

    ctx = BuildContext(
        fabric="t", template_path=tmpl, output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru", id_feature="hru_id",
    )
    ctx.paths["landmask"] = lm

    step_cfg = {"outputs": {"dprst_depth": "dprst_depth.tif", "op_flow_thres": "op_flow_thres_params.csv"}}
    produced = dprst_depth.build(step_cfg, ctx, _L())
    assert produced == {"dprst_depth": depth_out, "op_flow_thres": op_flow_out}
