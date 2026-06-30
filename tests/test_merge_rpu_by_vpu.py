"""Tests for merge_rpu_by_vpu output format.

The per-VPU TWI tile (``Twi_merged_<vpu>.tif``) is the ``twi.vrt`` source,
consumed only by GDAL-based tools (carea_map percentile mode, QGIS) — never
WhiteboxTools — so it is written as a COG (tiled 512 + overviews + ZSTD/pred3).
The NEDSnapshot/Hydrodem/FDR/FAC merge tiles are intermediates (or the WBT-fed
Hydrodem chain) and stay on their existing LZW write paths.
"""

import logging

import numpy as np
from osgeo import gdal, osr

from gfv2_params.shared_rasters.merge_rpu_by_vpu import _process_dataset

LOGGER = logging.getLogger("test_merge_rpu_by_vpu")


def _make_raster(path, data, *, dtype=gdal.GDT_Float32, nodata=-9999.0):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows, cols = data.shape
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), cols, rows, 1, dtype)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteArray(data)
    ds.FlushCache()
    del ds


class TestTwiMergeFormat:
    def test_twi_merge_tile_is_cog(self, tmp_path):
        vpu = "99"
        size = 600
        base = tmp_path
        # TWI source raster + all-valid per-VPU land mask on the same grid.
        _make_raster(base / "twi_in.tif", np.full((size, size), 5.0, dtype=np.float32))
        _make_raster(
            base / "shared" / "per_vpu" / vpu / f"land_mask_{vpu}.tif",
            np.ones((size, size), dtype=np.uint8),
            dtype=gdal.GDT_Byte,
            nodata=255,
        )
        values = {"rpus": ["twi_in.tif"], "output": "Twi_merged_99.tif"}

        _process_dataset("TWI", values, vpu, base, force=True, logger=LOGGER)

        ds = gdal.Open(str(base / "Twi_merged_99.tif"))
        band = ds.GetRasterBand(1)
        s = ds.GetMetadata("IMAGE_STRUCTURE")
        block = band.GetBlockSize()
        overviews = band.GetOverviewCount()
        layout = s.get("LAYOUT")
        compression = s.get("COMPRESSION")
        predictor = s.get("PREDICTOR")
        del ds

        assert layout == "COG"
        assert block == [512, 512]
        assert overviews >= 1
        assert compression == "ZSTD"
        assert predictor == "3"
