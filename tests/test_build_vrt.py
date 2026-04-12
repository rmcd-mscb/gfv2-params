"""Tests for build_vrt.py VRT source ordering."""

import struct
from pathlib import Path

import pytest
from osgeo import gdal


def _make_tiny_tif(path: Path, value: float, nodata: float = -9999.0) -> None:
    """Create a minimal 2x2 Float32 GeoTIFF with a constant value."""
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), 2, 2, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    from osgeo import osr
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteRaster(0, 0, 2, 2, struct.pack("4f", *([value] * 4)))
    ds.FlushCache()
    del ds


class TestVrtSourceOrdering:
    """Verify that fill sources are listed BEFORE primary sources in the VRT.

    GDAL VRT compositing is last-source-wins: later sources overwrite earlier
    ones (except at nodata pixels). By listing fill first and primary last,
    NHDPlus data takes priority wherever it has valid values.
    """

    def test_fill_before_primary_in_vrt(self, tmp_path):
        """VRT XML should list fill source before primary source."""
        nhd_dir = tmp_path / "nhd_merged"
        vpu_dir = nhd_dir / "01"
        fill_dir = nhd_dir / "copernicus_fill"
        vpu_dir.mkdir(parents=True)
        fill_dir.mkdir(parents=True)

        _make_tiny_tif(vpu_dir / "NEDSnapshot_merged_fixed_01.tif", value=100.0)
        _make_tiny_tif(fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif", value=200.0)

        FILL_DIRS = {"copernicus_fill"}
        pattern = "NEDSnapshot_merged_fixed_*.tif"

        primary_files = sorted(
            f for f in nhd_dir.glob(f"*/{pattern}")
            if f.parent.name not in FILL_DIRS
        )
        fill_files = []
        for fill_dir_name in sorted(FILL_DIRS):
            fill_files.extend(sorted(nhd_dir.glob(f"{fill_dir_name}/{pattern}")))

        # Correct ordering: fill first, primary last
        source_files = fill_files + primary_files

        assert len(source_files) == 2
        assert "copernicus" in source_files[0].name, "Fill source must be listed first"
        assert "01" in source_files[1].name, "Primary source must be listed last"

    def test_primary_overwrites_fill_in_vrt(self, tmp_path):
        """When both sources have valid data at same pixel, primary (last) wins."""
        nhd_dir = tmp_path / "nhd_merged"
        vpu_dir = nhd_dir / "01"
        fill_dir = nhd_dir / "copernicus_fill"
        vpu_dir.mkdir(parents=True)
        fill_dir.mkdir(parents=True)

        _make_tiny_tif(vpu_dir / "NEDSnapshot_merged_fixed_01.tif", value=100.0)
        _make_tiny_tif(fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif", value=200.0)

        source_files = [
            str(fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif"),
            str(vpu_dir / "NEDSnapshot_merged_fixed_01.tif"),
        ]
        vrt_path = str(nhd_dir / "test.vrt")
        vrt_options = gdal.BuildVRTOptions(resolution="highest", srcNodata="-9999")
        vrt_ds = gdal.BuildVRT(vrt_path, source_files, options=vrt_options)
        vrt_ds.FlushCache()
        del vrt_ds

        ds = gdal.Open(vrt_path)
        band = ds.GetRasterBand(1)
        data = band.ReadAsArray()
        del ds

        assert data[0, 0] == pytest.approx(100.0), (
            f"Expected primary value 100.0 but got {data[0, 0]}. "
            "GDAL VRT last-source-wins: primary must be listed last."
        )
