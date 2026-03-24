import math
import tempfile
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds

from gfv2_params.raster_ops import deg_to_fraction, mult_rasters, resample


def test_deg_to_fraction_zero():
    assert deg_to_fraction(0.0) == 0.0


def test_deg_to_fraction_45():
    result = deg_to_fraction(45.0)
    assert math.isclose(result, 1.0, rel_tol=1e-9)


def test_deg_to_fraction_30():
    result = deg_to_fraction(30.0)
    expected = math.tan(math.radians(30.0))
    assert math.isclose(result, expected, rel_tol=1e-9)


def test_deg_to_fraction_90():
    result = deg_to_fraction(90.0)
    assert result > 1e15  # tan(90) is extremely large (approaches infinity)


def test_deg_to_fraction_negative():
    result = deg_to_fraction(-30.0)
    assert result < 0


def _write_tiff(path, data, crs="EPSG:4326", transform=None, nodata=None):
    """Helper to write a small GeoTIFF."""
    rows, cols = data.shape
    if transform is None:
        transform = from_bounds(0, 0, cols, rows, cols, rows)
    profile = {
        "driver": "GTiff",
        "dtype": data.dtype,
        "width": cols,
        "height": rows,
        "count": 1,
        "crs": CRS.from_string(crs),
        "transform": transform,
    }
    if nodata is not None:
        profile["nodata"] = nodata
    with rasterio.open(str(path), "w", **profile) as dst:
        dst.write(data, 1)


def test_resample_masks_nodata_values():
    """Values 128 and 0 should become NaN after resample."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        src_path = tmpdir / "src.tif"
        tmpl_path = tmpdir / "tmpl.tif"
        intermediate_path = tmpdir / "intermediate.tif"
        output_path = tmpdir / "output.tif"

        data = np.array([[1.0, 128.0, 5.0], [0.0, 3.0, 7.0], [2.0, 4.0, 6.0]], dtype=np.float32)
        _write_tiff(src_path, data)
        _write_tiff(tmpl_path, np.ones((3, 3), dtype=np.float32))

        resample(str(src_path), str(tmpl_path), str(intermediate_path), str(output_path))

        with rasterio.open(str(output_path)) as src:
            result = src.read(1)
            assert np.isnan(result[0, 1])  # was 128
            assert np.isnan(result[1, 0])  # was 0


def test_resample_masks_negative_values():
    """Negative values should become NaN after resample."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        src_path = tmpdir / "src.tif"
        tmpl_path = tmpdir / "tmpl.tif"
        intermediate_path = tmpdir / "intermediate.tif"
        output_path = tmpdir / "output.tif"

        data = np.array([[1.0, -5.0, 3.0], [2.0, 4.0, 6.0], [7.0, 8.0, 9.0]], dtype=np.float32)
        _write_tiff(src_path, data)
        _write_tiff(tmpl_path, np.ones((3, 3), dtype=np.float32))

        resample(str(src_path), str(tmpl_path), str(intermediate_path), str(output_path))

        with rasterio.open(str(output_path)) as src:
            result = src.read(1)
            assert np.isnan(result[0, 1])  # was -5


def test_resample_file_not_found():
    """FileNotFoundError for missing source raster."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        tmpl_path = tmpdir / "tmpl.tif"
        _write_tiff(tmpl_path, np.ones((3, 3), dtype=np.float32))

        with pytest.raises(FileNotFoundError):
            resample(
                str(tmpdir / "nonexistent.tif"),
                str(tmpl_path),
                str(tmpdir / "inter.tif"),
                str(tmpdir / "out.tif"),
            )


def test_mult_rasters_basic():
    """Element-wise multiplication of two 3x3 rasters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        r1_path = tmpdir / "r1.tif"
        r2_path = tmpdir / "r2.tif"
        out_path = tmpdir / "out.tif"

        arr1 = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]], dtype=np.float64)
        arr2 = np.array([[2.0, 2.0, 2.0], [3.0, 3.0, 3.0], [0.5, 0.5, 0.5]], dtype=np.float64)

        _write_tiff(r1_path, arr1)
        _write_tiff(r2_path, arr2)

        mult_rasters(str(r1_path), str(r2_path), str(out_path))

        with rasterio.open(str(out_path)) as src:
            result = src.read(1)
            expected = arr1 * arr2
            np.testing.assert_array_almost_equal(result, expected)


def test_mult_rasters_nodata_handling():
    """NoData pixels produce NaN in the output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        r1_path = tmpdir / "r1.tif"
        r2_path = tmpdir / "r2.tif"
        out_path = tmpdir / "out.tif"

        arr1 = np.array([[1.0, -9999.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]], dtype=np.float64)
        arr2 = np.array([[2.0, 2.0, 2.0], [3.0, 3.0, 3.0], [0.5, 0.5, 0.5]], dtype=np.float64)

        _write_tiff(r1_path, arr1, nodata=-9999.0)
        _write_tiff(r2_path, arr2)

        mult_rasters(str(r1_path), str(r2_path), str(out_path))

        with rasterio.open(str(out_path)) as src:
            result = src.read(1)
            assert np.isnan(result[0, 1])  # nodata pixel
            assert math.isclose(result[0, 0], 2.0)


def test_mult_rasters_shape_mismatch():
    """ValueError for rasters with different shapes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        r1_path = tmpdir / "r1.tif"
        r2_path = tmpdir / "r2.tif"

        _write_tiff(r1_path, np.ones((3, 3), dtype=np.float64))
        _write_tiff(r2_path, np.ones((5, 5), dtype=np.float64))

        with pytest.raises(ValueError, match="same shape"):
            mult_rasters(str(r1_path), str(r2_path), str(tmpdir / "out.tif"))


def test_mult_rasters_crs_mismatch():
    """ValueError for rasters with different CRS."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        r1_path = tmpdir / "r1.tif"
        r2_path = tmpdir / "r2.tif"

        data = np.ones((3, 3), dtype=np.float64)
        transform = from_bounds(0, 0, 3, 3, 3, 3)
        _write_tiff(r1_path, data, crs="EPSG:4326", transform=transform)
        _write_tiff(r2_path, data, crs="EPSG:32617", transform=transform)

        with pytest.raises(ValueError, match="same CRS"):
            mult_rasters(str(r1_path), str(r2_path), str(tmpdir / "out.tif"))
