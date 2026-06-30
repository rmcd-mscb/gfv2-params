"""Tests for compute_slope_aspect output format.

The three per-VPU outputs (fixed elevation, slope, aspect) are the source
tiles for the elevation/slope/aspect CONUS VRTs and are consumed only by
GDAL/rasterio/QGIS. They must be Cloud-Optimized GeoTIFFs (tiled 512 +
overviews + ZSTD) — not the striped, uncompressed default GTiff that the
previous bare ``to_raster``/``SaveGDAL`` writes produced.
"""

import logging

import numpy as np
from osgeo import gdal, osr

from gfv2_params.shared_rasters.compute_slope_aspect import _process_vpu

LOGGER = logging.getLogger("test_compute_slope_aspect")


def _make_dem(path, *, size=600, nodata=-99.99):
    """A small valid DEM (sloping plane) with the per-VPU nodata convention."""
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), size, size, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    # A tilted plane + ripple so slope/aspect are non-degenerate.
    yy, xx = np.mgrid[0:size, 0:size].astype("float32")
    elev = 100.0 + 0.5 * xx + 0.3 * yy + 5.0 * np.sin(xx / 20.0)
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteArray(elev.astype("float32"))
    ds.FlushCache()
    del ds


def _meta(path):
    ds = gdal.Open(str(path))
    band = ds.GetRasterBand(1)
    struct = ds.GetMetadata("IMAGE_STRUCTURE")
    m = {
        "block": band.GetBlockSize(),
        "overviews": band.GetOverviewCount(),
        "nodata": band.GetNoDataValue(),
        "compression": struct.get("COMPRESSION"),
        "predictor": struct.get("PREDICTOR"),
        "overview_resampling": struct.get("OVERVIEW_RESAMPLING"),
        "layout": struct.get("LAYOUT"),
    }
    del ds
    return m


def _run(tmp_path):
    vpu = "99"
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    (in_dir / vpu).mkdir(parents=True)
    _make_dem(in_dir / vpu / f"NEDSnapshot_merged_{vpu}.tif")
    _process_vpu(vpu, in_dir, out_dir, force=True, logger=LOGGER)
    return {
        "fixed": in_dir / vpu / f"NEDSnapshot_merged_fixed_{vpu}.tif",
        "slope": out_dir / vpu / f"NEDSnapshot_merged_slope_{vpu}.tif",
        "aspect": out_dir / vpu / f"NEDSnapshot_merged_aspect_{vpu}.tif",
    }


class TestComputeSlopeAspectFormat:
    def test_fixed_elevation_is_cog(self, tmp_path):
        out = _run(tmp_path)
        m = _meta(out["fixed"])
        assert m["layout"] == "COG"
        assert m["block"] == [512, 512]
        assert m["overviews"] >= 1
        assert m["compression"] == "ZSTD"
        assert m["predictor"] == "3"
        assert m["nodata"] == -9999.0

    def test_slope_is_cog_bilinear(self, tmp_path):
        out = _run(tmp_path)
        m = _meta(out["slope"])
        assert m["layout"] == "COG"
        assert m["block"] == [512, 512]
        assert m["overviews"] >= 1
        assert m["overview_resampling"] == "BILINEAR"
        assert m["nodata"] == -9999.0

    def test_aspect_is_cog_nearest(self, tmp_path):
        """Aspect is circular (0-360); overviews must use NEAREST, not average."""
        out = _run(tmp_path)
        m = _meta(out["aspect"])
        assert m["layout"] == "COG"
        assert m["overviews"] >= 1
        assert m["overview_resampling"] == "NEAREST"

    def test_no_striped_output(self, tmp_path):
        """Regression: outputs must never be 1-row-block striped tiles."""
        out = _run(tmp_path)
        for key in ("fixed", "slope", "aspect"):
            block = _meta(out[key])["block"]
            assert block[1] != 1, f"{key} is striped: block={block}"
