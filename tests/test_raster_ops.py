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
    """Explicit mask_values=(128, 0) should rewrite both pixel values to NaN."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        src_path = tmpdir / "src.tif"
        tmpl_path = tmpdir / "tmpl.tif"
        intermediate_path = tmpdir / "intermediate.tif"
        output_path = tmpdir / "output.tif"

        data = np.array([[1.0, 128.0, 5.0], [0.0, 3.0, 7.0], [2.0, 4.0, 6.0]], dtype=np.float32)
        _write_tiff(src_path, data)
        _write_tiff(tmpl_path, np.ones((3, 3), dtype=np.float32))

        resample(str(src_path), str(tmpl_path), str(intermediate_path), str(output_path),
                 mask_values=(128, 0))

        with rasterio.open(str(output_path)) as src:
            result = src.read(1)
            assert np.isnan(result[0, 1])  # was 128
            assert np.isnan(result[1, 0])  # was 0


def test_resample_default_mask_values_is_no_op():
    """Default mask_values=() preserves all non-negative pixel values."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        src_path = tmpdir / "src.tif"
        tmpl_path = tmpdir / "tmpl.tif"
        intermediate_path = tmpdir / "intermediate.tif"
        output_path = tmpdir / "output.tif"

        # 0, 128, and other positives should all survive when mask_values is unset.
        data = np.array([[1.0, 128.0, 5.0], [0.0, 3.0, 7.0], [2.0, 4.0, 6.0]], dtype=np.float32)
        _write_tiff(src_path, data)
        _write_tiff(tmpl_path, np.ones((3, 3), dtype=np.float32))

        resample(str(src_path), str(tmpl_path), str(intermediate_path), str(output_path))

        with rasterio.open(str(output_path)) as src:
            result = src.read(1)
            # No value-mask applied — every input value reaches the output.
            assert result[0, 1] == 128.0
            assert result[1, 0] == 0.0
            assert result[2, 2] == 6.0
            # mask_negative=True is still on by default; no negatives in this
            # input, so the output should be NaN-free.
            assert not np.any(np.isnan(result))


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


def test_resample_atomic_rename_clears_stale_tmp_and_leaves_no_tmp_on_success():
    """Atomic-rename guard: stale .tmp files at the target paths are unlinked
    before a fresh run, and no .tmp companions remain after success.

    Regression test for the corruption mode in job 20515994: a previous job
    killed mid-write left a 4.75 GB partial cnpy_resampled_nalcms.tif that
    rasterio could still open, so the orchestrator's _is_valid_raster()
    skipped the resample and the next run consumed a 96%-incomplete file.
    With the .tmp + os.replace pattern, partial state lives at <path>.tmp
    where the existence check at the final <path> never sees it.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        src_path = tmpdir / "src.tif"
        tmpl_path = tmpdir / "tmpl.tif"
        intermediate_path = tmpdir / "intermediate.tif"
        output_path = tmpdir / "output.tif"

        data = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]], dtype=np.float32)
        _write_tiff(src_path, data)
        _write_tiff(tmpl_path, np.ones((3, 3), dtype=np.float32))

        # Simulate the post-kill state: stale .tmp files from a previous
        # interrupted run sitting at both expected paths. They should be
        # unlinked at the top of resample() before any work begins.
        stale_intermediate_tmp = Path(f"{intermediate_path}.tmp")
        stale_output_tmp = Path(f"{output_path}.tmp")
        stale_intermediate_tmp.write_bytes(b"garbage from prior killed run")
        stale_output_tmp.write_bytes(b"garbage from prior killed run")
        assert stale_intermediate_tmp.exists()
        assert stale_output_tmp.exists()

        resample(str(src_path), str(tmpl_path), str(intermediate_path), str(output_path))

        # Final outputs are at the canonical paths, NOT at .tmp paths.
        assert output_path.exists()
        # The intermediate is deleted at the end of a successful run.
        assert not intermediate_path.exists()
        # No .tmp companions should survive a successful run.
        assert not stale_intermediate_tmp.exists()
        assert not stale_output_tmp.exists()
        # And the output is a valid raster (sanity check — confirms the
        # rename produced a real file, not just the absence of .tmp).
        with rasterio.open(str(output_path)) as src:
            assert src.read(1).shape == (3, 3)


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
