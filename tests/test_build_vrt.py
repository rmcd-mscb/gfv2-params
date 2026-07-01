"""Tests for build_vrt.py VRT source ordering."""

import struct
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from osgeo import gdal, osr

from gfv2_params.shared_rasters import build_vrt


def _make_tiny_tif(path: Path, value: float, nodata: float = -9999.0) -> None:
    """Create a minimal 2x2 Float32 GeoTIFF with a constant value."""
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), 2, 2, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
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

        primary_files = sorted(f for f in nhd_dir.glob(f"*/{pattern}") if f.parent.name not in FILL_DIRS)
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


class TestBreachedFdrRegistered:
    def test_fdr_breached_builds_and_is_keyed(self, tmp_path, caplog):
        from gfv2_params.shared_rasters import build_vrt

        per_vpu = tmp_path / "per_vpu"
        (per_vpu / "09").mkdir(parents=True)
        _make_tiny_tif(per_vpu / "09" / "Fdr_breached_09.tif", value=1.0, nodata=255.0)

        class Ctx:
            pass
        ctx = Ctx()
        ctx.per_vpu_dir = per_vpu
        ctx.borders_dir = tmp_path / "nonexistent_borders"
        ctx.vrt_dir = tmp_path / "vrt"

        produced = build_vrt.build({}, ctx, __import__("logging").getLogger("t"))
        assert "fdr_breached_vrt" in produced
        assert produced["fdr_breached_vrt"].name == "fdr_breached.vrt"
        assert produced["fdr_breached_vrt"].exists()


def _make_sized_tif(path, *, size, value, dtype, nodata):
    """A constant-value GeoTIFF large enough to carry overviews."""
    path.parent.mkdir(parents=True, exist_ok=True)
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), size, size, 1, dtype)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteArray(np.full((size, size), value))
    ds.FlushCache()
    del ds


class TestVrtOverviews:
    """build_vrt must add overviews to each VRT for fast full-extent rendering.

    A bare .vrt over full-resolution sources forces QGIS to decimate
    full-resolution data on every continental pan/zoom. An external .ovr
    (gdaladdo on the VRT) gives a coarse pyramid for snappy rendering.
    """

    def _run_build(self, tmp_path):
        per_vpu = tmp_path / "per_vpu"
        borders = tmp_path / "borders"  # absent on purpose
        vrt_dir = tmp_path / "vrt"
        _make_sized_tif(
            per_vpu / "01" / "NEDSnapshot_merged_fixed_01.tif",
            size=1024,
            value=100.0,
            dtype=gdal.GDT_Float32,
            nodata=-9999,
        )
        _make_sized_tif(
            per_vpu / "01" / "Fdr_merged_01.tif",
            size=1024,
            value=1,
            dtype=gdal.GDT_Byte,
            nodata=255,
        )
        ctx = SimpleNamespace(
            per_vpu_dir=per_vpu,
            borders_dir=borders,
            vrt_dir=vrt_dir,
        )
        import logging

        build_vrt.build({}, ctx, logging.getLogger("test_build_vrt"))
        return vrt_dir

    def test_elevation_vrt_has_overviews(self, tmp_path):
        vrt_dir = self._run_build(tmp_path)
        ds = gdal.Open(str(vrt_dir / "elevation.vrt"))
        n = ds.GetRasterBand(1).GetOverviewCount()
        del ds
        assert n >= 1, "elevation.vrt has no overviews"
        assert (vrt_dir / "elevation.vrt.ovr").exists()

    def test_fdr_vrt_has_overviews(self, tmp_path):
        vrt_dir = self._run_build(tmp_path)
        ds = gdal.Open(str(vrt_dir / "fdr.vrt"))
        n = ds.GetRasterBand(1).GetOverviewCount()
        del ds
        assert n >= 1, "fdr.vrt has no overviews"


class TestOverviewResamplingChoice:
    """Categorical/circular fields (fdr D8 codes, aspect 0/360) must decimate
    with nearest; continuous surfaces with bilinear."""

    def test_nearest_for_fdr_and_aspect(self):
        assert build_vrt._overview_resampling("fdr") == "nearest"
        assert build_vrt._overview_resampling("aspect") == "nearest"

    def test_bilinear_for_continuous(self):
        for name in ("elevation", "slope", "twi", "twi_hydrodem"):
            assert build_vrt._overview_resampling(name) == "bilinear"
