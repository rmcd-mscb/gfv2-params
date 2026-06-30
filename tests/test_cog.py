"""Tests for the shared COG-conversion helper (shared_rasters/cog.py).

The helper turns a plain GeoTIFF into a Cloud-Optimized GeoTIFF with internal
tiling + overviews, ZSTD compression, and the floating-point predictor. It is
used for the *visualization/processing* elevation-mosaic tiles (elevation,
slope, aspect) which are consumed only by GDAL/rasterio/QGIS — NEVER for
WhiteboxTools-fed rasters, whose format must stay LZW-without-predictor.
"""

import fnmatch
import struct

import pytest
from osgeo import gdal, osr

from gfv2_params.shared_rasters.cog import cog_temp, to_cog


def _make_striped_float_tif(path, *, width=1024, height=1024, value=12.5, nodata=-9999.0):
    """Create a striped (1-row-block), uncompressed Float32 GeoTIFF.

    Mirrors the current compute_slope_aspect output that the COG helper must
    fix: default GTiff creation = striped, no compression, no predictor.
    """
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), width, height, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    row = struct.pack(f"{width}f", *([value] * width))
    for r in range(height):
        band.WriteRaster(0, r, width, 1, row)
    ds.FlushCache()
    del ds


def _band_metadata(path):
    ds = gdal.Open(str(path))
    band = ds.GetRasterBand(1)
    # COMPRESSION and PREDICTOR are exposed on the dataset's IMAGE_STRUCTURE,
    # not the band's (the COG driver reports them at the dataset level).
    ds_struct = ds.GetMetadata("IMAGE_STRUCTURE")
    md = {
        "block": band.GetBlockSize(),
        "overviews": band.GetOverviewCount(),
        "nodata": band.GetNoDataValue(),
        "compression": ds_struct.get("COMPRESSION"),
        "predictor": ds_struct.get("PREDICTOR"),
        "value": band.ReadAsArray(0, 0, 1, 1)[0, 0],
    }
    del ds
    return md


class TestToCog:
    def test_output_is_tiled_512(self, tmp_path):
        src = tmp_path / "striped.tif"
        dst = tmp_path / "cog.tif"
        _make_striped_float_tif(src)

        to_cog(src, dst, overview_resampling="BILINEAR", predictor=3)

        assert _band_metadata(dst)["block"] == [512, 512]

    def test_output_has_overviews(self, tmp_path):
        src = tmp_path / "striped.tif"
        dst = tmp_path / "cog.tif"
        _make_striped_float_tif(src)  # 1024px -> at least one 512 overview

        to_cog(src, dst, overview_resampling="BILINEAR", predictor=3)

        assert _band_metadata(dst)["overviews"] >= 1

    def test_output_uses_zstd_and_float_predictor(self, tmp_path):
        src = tmp_path / "striped.tif"
        dst = tmp_path / "cog.tif"
        _make_striped_float_tif(src)

        to_cog(src, dst, overview_resampling="BILINEAR", predictor=3)

        md = _band_metadata(dst)
        assert md["compression"] == "ZSTD"
        assert md["predictor"] == "3"

    def test_nodata_and_values_preserved(self, tmp_path):
        src = tmp_path / "striped.tif"
        dst = tmp_path / "cog.tif"
        _make_striped_float_tif(src, value=42.0, nodata=-9999.0)

        to_cog(src, dst, overview_resampling="BILINEAR", predictor=3)

        md = _band_metadata(dst)
        assert md["nodata"] == -9999.0
        assert md["value"] == pytest.approx(42.0)

    def test_validates_as_cog(self, tmp_path):
        """The output must satisfy GDAL's COG layout (IFD/tile ordering)."""
        src = tmp_path / "striped.tif"
        dst = tmp_path / "cog.tif"
        _make_striped_float_tif(src)

        to_cog(src, dst, overview_resampling="BILINEAR", predictor=3)

        # GDAL ships a validate_cloud_optimized_geotiff sample; if unavailable,
        # fall back to asserting the COG-defining structural properties.
        try:
            from osgeo_utils.samples.validate_cloud_optimized_geotiff import (
                validate,
            )
        except ImportError:
            md = _band_metadata(dst)
            assert md["block"] == [512, 512] and md["overviews"] >= 1
            return
        warnings, errors, _details = validate(str(dst))
        assert errors == [], f"COG validation errors: {errors}"


# build_vrt source globs that a leaked pre-COG temp must NOT match.
_VRT_PATTERNS = [
    "NEDSnapshot_merged_fixed_*.tif",
    "NEDSnapshot_merged_slope_*.tif",
    "NEDSnapshot_merged_aspect_*.tif",
    "Twi_merged_*.tif",
    "Twi_hydrodem_*.tif",
]


class TestCogTemp:
    """cog_temp() yields a scratch path for the plain pre-COG write and
    guarantees cleanup, with a name that can never be globbed as a VRT source."""

    def test_temp_name_is_unglobbable_by_vrt_patterns(self, tmp_path):
        output = tmp_path / "NEDSnapshot_merged_fixed_01.tif"
        with cog_temp(output) as tmp:
            tmp.write_text("x")  # simulate the plain write
            # While it exists on disk, it must not match any VRT source glob.
            for pattern in _VRT_PATTERNS:
                assert not fnmatch.fnmatch(tmp.name, pattern), (
                    f"temp {tmp.name} is globbable by {pattern}"
                )
            # ...and a directory glob (as build_vrt does) must not list it.
            assert tmp not in set(tmp_path.glob("NEDSnapshot_merged_fixed_*.tif"))

    def test_temp_is_removed_on_success(self, tmp_path):
        output = tmp_path / "Twi_merged_07.tif"
        with cog_temp(output) as tmp:
            tmp.write_text("x")
            created = tmp
        assert not created.exists()

    def test_temp_is_removed_on_error(self, tmp_path):
        """A failure between the plain write and to_cog must not strand the temp."""
        output = tmp_path / "Twi_hydrodem_07.tif"
        created = None
        with pytest.raises(RuntimeError, match="boom"):
            with cog_temp(output) as tmp:
                tmp.write_text("x")
                created = tmp
                raise RuntimeError("boom")  # e.g. to_cog/gdal failure
        assert created is not None and not created.exists()
