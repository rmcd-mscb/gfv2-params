"""Tests for compute_dem_derivatives output format (open-source TWI).

``Twi_hydrodem_*.tif`` is a CONUS VRT source (``twi_hydrodem.vrt``) consumed
only by GDAL-based tools (carea_map, marimo, QGIS) — never WhiteboxTools — so
it is written as a COG (tiled 512 + overviews + ZSTD/pred3), like the other
elevation-mosaic float rasters.
"""

import logging

import numpy as np
from osgeo import gdal, osr

from gfv2_params.shared_rasters.compute_dem_derivatives import _compute_twi

LOGGER = logging.getLogger("test_compute_dem_derivatives")


def _make_fac(path, *, size=600, value=100.0, nodata=-9999.0):
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), size, size, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteArray(np.full((size, size), value, dtype=np.float32))
    ds.FlushCache()
    del ds


def _meta(path):
    ds = gdal.Open(str(path))
    band = ds.GetRasterBand(1)
    s = ds.GetMetadata("IMAGE_STRUCTURE")
    m = {
        "block": band.GetBlockSize(),
        "overviews": band.GetOverviewCount(),
        "nodata": band.GetNoDataValue(),
        "compression": s.get("COMPRESSION"),
        "predictor": s.get("PREDICTOR"),
        "layout": s.get("LAYOUT"),
    }
    del ds
    return m


class TestTwiHydrodemFormat:
    def test_twi_is_cog(self, tmp_path):
        size = 600
        fac = tmp_path / "fac.tif"
        twi_out = tmp_path / "Twi_hydrodem_99.tif"
        _make_fac(fac, size=size)
        slope_deg = np.full((size, size), 10.0, dtype=np.float64)
        land_valid = np.ones((size, size), dtype=bool)

        _compute_twi(fac, slope_deg, land_valid, twi_out, LOGGER)

        m = _meta(twi_out)
        assert m["layout"] == "COG"
        assert m["block"] == [512, 512]
        assert m["overviews"] >= 1
        assert m["compression"] == "ZSTD"
        assert m["predictor"] == "3"
        assert m["nodata"] == -9999.0
