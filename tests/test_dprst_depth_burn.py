import logging

import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import box

from gfv2_params.dprst_depth.burn import STRIP_ROWS, burn_depth


def _L():
    return logging.getLogger("test_dprst_depth_burn")


def _write_template_and_landmask(tmp_path, land_arr=None):
    tr = rasterio.transform.from_origin(0, 10, 1, 1)
    tmpl = tmp_path / "tmpl.tif"
    with rasterio.open(
        tmpl, "w", driver="GTiff", height=10, width=10, count=1,
        dtype="float32", crs="EPSG:5070", transform=tr, nodata=-9999,
    ) as d:
        d.write(np.ones((10, 10), np.float32), 1)

    lm = tmp_path / "lm.tif"
    if land_arr is None:
        land_arr = np.ones((10, 10), np.uint8)
    with rasterio.open(
        lm, "w", driver="GTiff", height=10, width=10, count=1,
        dtype="uint8", crs="EPSG:5070", transform=tr, nodata=0,
    ) as d:
        d.write(land_arr, 1)
    return tmpl, lm


def test_burn_depth_writes_polygon_values(tmp_path):
    # 10x10 template, 1 m cells; one polygon depth 2.0 over a 4x4 block
    tmpl, lm = _write_template_and_landmask(tmp_path)
    g = gpd.GeoDataFrame({"dprst_depth_m": [2.0]}, geometry=[box(2, 2, 6, 6)], crs="EPSG:5070")
    out = tmp_path / "dprst_depth.tif"
    burn_depth(g, str(tmpl), str(lm), str(out), logger=_L())
    with rasterio.open(out) as d:
        a = d.read(1)
        nodata = d.nodata
    assert np.isclose(a[a != nodata].max(), 2.0)     # burned depth present


def test_burn_depth_output_aligned_to_template(tmp_path):
    tmpl, lm = _write_template_and_landmask(tmp_path)
    g = gpd.GeoDataFrame({"dprst_depth_m": [1.5]}, geometry=[box(0, 0, 3, 3)], crs="EPSG:5070")
    out = tmp_path / "dprst_depth.tif"
    burn_depth(g, str(tmpl), str(lm), str(out), logger=_L())
    with rasterio.open(tmpl) as t, rasterio.open(out) as d:
        assert d.width == t.width
        assert d.height == t.height
        assert d.transform == t.transform
        assert d.crs == t.crs
        assert d.nodata == -9999
        assert d.dtypes[0] == "float32"


def test_burn_depth_masks_to_land(tmp_path):
    # Land mask excludes the whole grid -> no burned cells should survive.
    land_arr = np.zeros((10, 10), np.uint8)
    tmpl, lm = _write_template_and_landmask(tmp_path, land_arr=land_arr)
    g = gpd.GeoDataFrame({"dprst_depth_m": [2.0]}, geometry=[box(2, 2, 6, 6)], crs="EPSG:5070")
    out = tmp_path / "dprst_depth.tif"
    burn_depth(g, str(tmpl), str(lm), str(out), logger=_L())
    with rasterio.open(out) as d:
        a = d.read(1)
        nodata = d.nodata
    assert (a == nodata).all()


def test_burn_depth_streams_by_strips_no_full_grid(tmp_path, monkeypatch):
    # Force a tiny STRIP_ROWS so a 10-row grid spans multiple strips, then
    # verify the polygon straddling two strips still burns correctly end to
    # end (regression guard against strip-boundary off-by-ones).
    import gfv2_params.dprst_depth.burn as burn_mod
    monkeypatch.setattr(burn_mod, "STRIP_ROWS", 3)

    tmpl, lm = _write_template_and_landmask(tmp_path)
    g = gpd.GeoDataFrame({"dprst_depth_m": [3.0]}, geometry=[box(1, 1, 9, 9)], crs="EPSG:5070")
    out = tmp_path / "dprst_depth.tif"
    burn_depth(g, str(tmpl), str(lm), str(out), logger=_L())
    with rasterio.open(out) as d:
        a = d.read(1)
        nodata = d.nodata
    burned = a[a != nodata]
    # Exact expected count, not just "some cells burned": all_touched=False
    # rasterize keeps a cell only if its pixel CENTER falls inside the
    # polygon. Grid: from_origin(0, 10, 1, 1) -> col c center x = c+0.5, row
    # r center y = 9.5-r. Polygon box(1,1,9,9) covers x,y in (1,9) at pixel
    # centers, i.e. c=1..8 and r=1..8 (centers 1.5..8.5) -> an 8x8 = 64-cell
    # block. This guards against strip-boundary off-by-ones that `> 0` would
    # silently pass.
    assert burned.size == 64
    assert np.allclose(burned, 3.0)


def test_burn_depth_rejects_non_positive_depth(tmp_path):
    tmpl, lm = _write_template_and_landmask(tmp_path)
    g = gpd.GeoDataFrame({"dprst_depth_m": [0.0]}, geometry=[box(2, 2, 6, 6)], crs="EPSG:5070")
    out = tmp_path / "dprst_depth.tif"
    import pytest
    with pytest.raises(ValueError):
        burn_depth(g, str(tmpl), str(lm), str(out), logger=_L())


def test_strip_rows_module_constant_is_sane():
    # STRIP_ROWS should be a positive, reasonably sized chunk (mirrors
    # carea_map's windowed-strip pattern) — not the full CONUS grid.
    assert 0 < STRIP_ROWS <= 4096
