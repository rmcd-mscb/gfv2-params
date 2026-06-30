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


class TestFillMask:
    """Verify fill_mask = (copernicus != nodata) & (nhdplus == nodata)."""

    def test_fill_mask_basic(self):
        """Fill mask should be True only where Copernicus has data and NHDPlus does not."""
        nodata = -9999.0
        copernicus = np.array([[nodata, 500.0], [600.0, 700.0]])
        nhdplus = np.array([[100.0, 200.0], [nodata, nodata]])

        fill_mask = (copernicus != nodata) & (nhdplus == nodata)

        assert not fill_mask[0, 0]
        assert not fill_mask[0, 1]
        assert fill_mask[1, 0]
        assert fill_mask[1, 1]

    def test_masked_slope_retains_only_fill_zone(self):
        """After masking, slope values should only exist in the fill zone."""
        nodata = -9999.0
        copernicus = np.array([[nodata, 500.0], [600.0, 700.0]])
        nhdplus = np.array([[100.0, 200.0], [nodata, nodata]])
        raw_slope = np.array([[5.0, 10.0], [15.0, 20.0]])

        fill_mask = (copernicus != nodata) & (nhdplus == nodata)
        masked_slope = np.where(fill_mask, raw_slope, nodata)

        assert masked_slope[0, 0] == nodata
        assert masked_slope[0, 1] == nodata
        assert masked_slope[1, 0] == 15.0
        assert masked_slope[1, 1] == 20.0


class TestApplyFillMaskCog:
    """The masked slope/aspect fill tiles feed the slope/aspect VRTs and must
    be COGs (tiled 512 + overviews + ZSTD/pred3), like the per-VPU outputs."""

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
        out = tmp_path / "slope.tif"
        _make_tif(raw, np.full((1024, 1024), 7.5, dtype=np.float32))
        mask = np.ones((1024, 1024), dtype=bool)

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
        out = tmp_path / "aspect.tif"
        _make_tif(raw, np.full((1024, 1024), 180.0, dtype=np.float32))
        mask = np.ones((1024, 1024), dtype=bool)

        _apply_fill_mask(raw, mask, out, overview_resampling="NEAREST")

        assert self._meta(out)["overview_resampling"] == "NEAREST"


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
