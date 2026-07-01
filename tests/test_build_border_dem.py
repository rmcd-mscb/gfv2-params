"""Tests for build_border_dem composite elevation and fill masking."""

from pathlib import Path

import numpy as np
from osgeo import gdal, osr


def _make_tif(path: Path, data: np.ndarray, nodata: float = -9999.0) -> None:
    """Create a Float32 GeoTIFF from a 2D numpy array."""
    rows, cols = data.shape
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), cols, rows, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteRaster(
        0, 0, cols, rows,
        data.astype(np.float32).tobytes(),
    )
    ds.FlushCache()
    del ds


def _make_mask_tif(path: Path, mask: np.ndarray) -> None:
    """Create a UInt8 mask GeoTIFF (1 = fill zone) on the standard test grid."""
    rows, cols = mask.shape
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), cols, rows, 1, gdal.GDT_Byte)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    ds.GetRasterBand(1).WriteArray(mask.astype(np.uint8))
    ds.FlushCache()
    del ds


def _read(path: Path) -> np.ndarray:
    ds = gdal.Open(str(path))
    arr = ds.GetRasterBand(1).ReadAsArray()
    del ds
    return arr


class TestWriteFillMask:
    """_write_fill_mask streams a UInt8 fill-zone raster (1 where Copernicus has
    data AND NHDPlus is nodata) instead of holding full-extent arrays in memory
    (the previous _compute_fill_mask OOM'd at CONUS scale). See issue: the
    2x full-extent gdal.Warp-to-MEM needed ~560 GB > the 503 GB node."""

    def test_fill_zone_values(self, tmp_path):
        from gfv2_params.shared_rasters import build_border_dem as bbd

        nd = -9999.0
        cop = np.array([[nd, 500.0], [600.0, 700.0]], dtype=np.float32)
        nhd = np.array([[100.0, 200.0], [nd, nd]], dtype=np.float32)
        _make_tif(tmp_path / "cop.tif", cop)
        _make_tif(tmp_path / "nhd.tif", nhd)
        _make_tif(tmp_path / "ref.tif", np.zeros((2, 2), dtype=np.float32))
        mask_out = tmp_path / "mask.tif"

        bbd._write_fill_mask(
            tmp_path / "cop.tif", tmp_path / "nhd.tif", tmp_path / "ref.tif", mask_out,
        )

        # fill zone = Copernicus has data AND NHDPlus is nodata -> row 1 only
        np.testing.assert_array_equal(
            _read(mask_out), np.array([[0, 0], [1, 1]], dtype=np.uint8),
        )

    def test_streaming_multi_strip_matches_naive(self, tmp_path):
        """With STRIP_ROWS forced small, the streamed mask must equal the naive
        full-array computation across strip boundaries."""
        from gfv2_params.shared_rasters import build_border_dem as bbd

        nd = -9999.0
        cop = np.full((6, 4), 100.0, dtype=np.float32)
        cop[0, :] = nd
        cop[3, 1] = nd
        nhd = np.full((6, 4), nd, dtype=np.float32)
        nhd[4, :] = 5.0
        nhd[0, 0] = 5.0
        expected = ((cop != nd) & (nhd == nd)).astype(np.uint8)

        _make_tif(tmp_path / "cop.tif", cop)
        _make_tif(tmp_path / "nhd.tif", nhd)
        _make_tif(tmp_path / "ref.tif", np.zeros((6, 4), dtype=np.float32))
        mask_out = tmp_path / "mask.tif"

        orig = bbd.STRIP_ROWS
        try:
            bbd.STRIP_ROWS = 2  # 3 strips over 6 rows
            bbd._write_fill_mask(
                tmp_path / "cop.tif", tmp_path / "nhd.tif", tmp_path / "ref.tif", mask_out,
            )
        finally:
            bbd.STRIP_ROWS = orig

        np.testing.assert_array_equal(_read(mask_out), expected)


class TestApplyFillMaskCog:
    """_apply_fill_mask stream-applies a mask RASTER to a raw slope/aspect tile
    and writes a COG (tiled 512 + overviews + ZSTD/pred3), windowed to avoid
    loading the full-extent raw into memory."""

    def _meta(self, path):
        ds = gdal.Open(str(path))
        band = ds.GetRasterBand(1)
        s = ds.GetMetadata("IMAGE_STRUCTURE")
        m = {
            "block": band.GetBlockSize(),
            "overviews": band.GetOverviewCount(),
            "compression": s.get("COMPRESSION"),
            "predictor": s.get("PREDICTOR"),
            "overview_resampling": s.get("OVERVIEW_RESAMPLING"),
            "layout": s.get("LAYOUT"),
        }
        del ds
        return m

    def test_slope_fill_is_cog_bilinear(self, tmp_path):
        from gfv2_params.shared_rasters.build_border_dem import _apply_fill_mask

        raw = tmp_path / "slope_raw.tif"
        mask = tmp_path / "mask.tif"
        out = tmp_path / "slope.tif"
        _make_tif(raw, np.full((1024, 1024), 7.5, dtype=np.float32))
        _make_mask_tif(mask, np.ones((1024, 1024), dtype=np.uint8))

        _apply_fill_mask(raw, mask, out, overview_resampling="BILINEAR")

        m = self._meta(out)
        assert m["layout"] == "COG"
        assert m["block"] == [512, 512]
        assert m["overviews"] >= 1
        assert m["compression"] == "ZSTD"
        assert m["predictor"] == "3"
        assert m["overview_resampling"] == "BILINEAR"

    def test_aspect_fill_is_cog_nearest(self, tmp_path):
        from gfv2_params.shared_rasters.build_border_dem import _apply_fill_mask

        raw = tmp_path / "aspect_raw.tif"
        mask = tmp_path / "mask.tif"
        out = tmp_path / "aspect.tif"
        _make_tif(raw, np.full((1024, 1024), 180.0, dtype=np.float32))
        _make_mask_tif(mask, np.ones((1024, 1024), dtype=np.uint8))

        _apply_fill_mask(raw, mask, out, overview_resampling="NEAREST")

        assert self._meta(out)["overview_resampling"] == "NEAREST"

    def test_masked_values_multi_strip(self, tmp_path):
        """Windowed apply must keep raw values in the fill zone and write nodata
        elsewhere, correctly across strip boundaries."""
        from gfv2_params.shared_rasters import build_border_dem as bbd

        raw = np.arange(24, dtype=np.float32).reshape(6, 4)
        mask = np.zeros((6, 4), dtype=np.uint8)
        mask[1, 1] = 1
        mask[4, 3] = 1  # in different strips (STRIP_ROWS=2)
        _make_tif(tmp_path / "raw.tif", raw)
        _make_mask_tif(tmp_path / "mask.tif", mask)
        out = tmp_path / "out.tif"

        orig = bbd.STRIP_ROWS
        try:
            bbd.STRIP_ROWS = 2
            bbd._apply_fill_mask(
                tmp_path / "raw.tif", tmp_path / "mask.tif", out,
                overview_resampling="BILINEAR",
            )
        finally:
            bbd.STRIP_ROWS = orig

        result = _read(out)
        expected = np.where(mask.astype(bool), raw, np.float32(-9999.0))
        np.testing.assert_array_equal(result, expected)


class TestCompositeVrtOrdering:
    """Verify composite VRT lists Copernicus first, NHDPlus last."""

    def test_nhdplus_overwrites_copernicus_in_composite(self, tmp_path):
        """In the composite, NHDPlus values should win in the overlap zone."""
        cop_data = np.full((4, 4), 500.0, dtype=np.float32)
        nhd_data = np.full((4, 4), 100.0, dtype=np.float32)
        nhd_data[2:, :] = -9999.0

        cop_path = tmp_path / "copernicus.tif"
        nhd_path = tmp_path / "nhdplus.tif"
        _make_tif(cop_path, cop_data)
        _make_tif(nhd_path, nhd_data)

        vrt_path = str(tmp_path / "composite.vrt")
        vrt_options = gdal.BuildVRTOptions(resolution="highest", srcNodata="-9999")
        vrt_ds = gdal.BuildVRT(
            vrt_path,
            [str(cop_path), str(nhd_path)],
            options=vrt_options,
        )
        vrt_ds.FlushCache()
        del vrt_ds

        ds = gdal.Open(vrt_path)
        result = ds.GetRasterBand(1).ReadAsArray()
        del ds

        np.testing.assert_array_equal(result[:2, :], 100.0)
        np.testing.assert_array_equal(result[2:, :], 500.0)
