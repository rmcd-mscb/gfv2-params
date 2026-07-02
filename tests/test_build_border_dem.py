"""Tests for build_border_dem composite elevation and fill masking."""

from pathlib import Path

import numpy as np
import pytest
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
    (the previous _compute_fill_mask OOM'd at CONUS scale — the full-extent warp
    readbacks plus their copies exceeded the 503 GB node)."""

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

    def test_streaming_partial_last_strip(self, tmp_path):
        """A raster height not divisible by STRIP_ROWS must still tile fully:
        5 rows / STRIP_ROWS=2 -> strips of 2, 2, 1 (the trailing partial strip
        is where an off-by-one in the min() clamp / row offset would hide)."""
        from gfv2_params.shared_rasters import build_border_dem as bbd

        nd = -9999.0
        cop = np.full((5, 3), 100.0, dtype=np.float32)
        cop[0, :] = nd
        nhd = np.full((5, 3), nd, dtype=np.float32)
        nhd[2, 1] = 5.0
        nhd[4, 2] = 7.0  # a toggle in the final 1-row strip
        expected = ((cop != nd) & (nhd == nd)).astype(np.uint8)

        _make_tif(tmp_path / "cop.tif", cop)
        _make_tif(tmp_path / "nhd.tif", nhd)
        _make_tif(tmp_path / "ref.tif", np.zeros((5, 3), dtype=np.float32))
        mask_out = tmp_path / "mask.tif"

        orig = bbd.STRIP_ROWS
        try:
            bbd.STRIP_ROWS = 2  # 3 strips over 5 rows: 2, 2, 1
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

    def test_masked_values_partial_last_strip(self, tmp_path):
        """Windowed apply over a height not divisible by STRIP_ROWS: 5 rows /
        STRIP_ROWS=2 -> 2, 2, 1, with a fill pixel in the trailing 1-row strip."""
        from gfv2_params.shared_rasters import build_border_dem as bbd

        raw = np.arange(15, dtype=np.float32).reshape(5, 3)
        mask = np.zeros((5, 3), dtype=np.uint8)
        mask[1, 0] = 1
        mask[4, 2] = 1  # trailing 1-row strip
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

    def test_shape_mismatch_raises(self, tmp_path):
        """A raw/mask dimension mismatch must fail loudly, not misalign cells."""
        from gfv2_params.shared_rasters.build_border_dem import _apply_fill_mask

        raw = tmp_path / "raw.tif"
        mask = tmp_path / "mask.tif"
        _make_tif(raw, np.zeros((6, 4), dtype=np.float32))
        _make_mask_tif(mask, np.ones((6, 5), dtype=np.uint8))

        with pytest.raises(RuntimeError, match="Shape mismatch"):
            _apply_fill_mask(raw, mask, tmp_path / "out.tif", overview_resampling="BILINEAR")


class TestCheckShortfall:
    """_check_shortfall classifies a tile-count shortfall against the ocean
    baseline: None when complete, a warning within OCEAN_SHORTFALL_PCT (strict),
    RuntimeError above it."""

    def test_no_shortfall_returns_none(self):
        from gfv2_params.shared_rasters.build_border_dem import _check_shortfall

        assert _check_shortfall(100, 100) is None

    def test_ocean_baseline_warns(self):
        from gfv2_params.shared_rasters.build_border_dem import _check_shortfall

        # ~20% — the deterministic open-ocean 404 baseline: warn, don't raise.
        msg = _check_shortfall(100, 80)
        assert msg is not None
        assert "shortfall" in msg

    def test_at_threshold_warns_not_raises(self):
        from gfv2_params.shared_rasters.build_border_dem import _check_shortfall

        # Exactly 30% — the guard is strict (> 30), so this warns.
        assert _check_shortfall(100, 70) is not None

    def test_above_threshold_raises(self):
        from gfv2_params.shared_rasters.build_border_dem import _check_shortfall

        # 31% — a gross shortfall implying a real coverage failure.
        with pytest.raises(RuntimeError, match="shortfall"):
            _check_shortfall(100, 69)


class _FakeResp:
    """Minimal stand-in for a streaming requests.Response context manager."""

    def __init__(self, status_code: int, body: bytes = b""):
        self.status_code = status_code
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size=65536):
        yield self._body


class TestDownloadTilesClassification:
    """download_tiles must separate expected open-ocean 404s (silently skipped)
    from real non-404 failures (returned in `failed` for the caller to abort)."""

    def test_404_vs_error_vs_ok(self, tmp_path, monkeypatch):
        from gfv2_params.download import copernicus_dem

        labels = ["TILE_OK", "TILE_OCEAN", "TILE_BROKEN"]

        def fake_get(url, stream=False, timeout=None):
            if "TILE_OK" in url:
                return _FakeResp(200, b"elevation-bytes")
            if "TILE_OCEAN" in url:
                return _FakeResp(404)
            return _FakeResp(500)  # server error -> real failure

        monkeypatch.setattr(copernicus_dem.requests, "get", fake_get)

        paths, failed = copernicus_dem.download_tiles(labels, tmp_path)

        assert [p.name for p in paths] == ["TILE_OK.tif"]
        assert failed == ["TILE_BROKEN"]  # 404 is NOT a failure
        assert (tmp_path / "TILE_OK.tif").exists()


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
