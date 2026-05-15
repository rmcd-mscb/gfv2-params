"""Tests for the read_land_mask helper used by the depstor builders."""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from gfv2_params.depstor import read_land_mask


def _write_landmask(path, data):
    """Write a uint8 1/255 land_mask.tif (the build_depstor_landmask convention)."""
    with rasterio.open(
        path, "w", driver="GTiff", height=data.shape[0], width=data.shape[1],
        count=1, dtype="uint8", crs="EPSG:5070",
        transform=from_origin(0, data.shape[0] * 30, 30, 30), nodata=255,
    ) as dst:
        dst.write(data, 1)


def test_read_land_mask_returns_bool_in_fabric(tmp_path):
    """land_mask.tif uses the 1/255 convention; read_land_mask returns True
    only where the value is 1 (inside the HRU fabric)."""
    data = np.array([[1, 255], [255, 1]], dtype=np.uint8)
    path = tmp_path / "land_mask.tif"
    _write_landmask(path, data)
    out = read_land_mask(path)
    assert out.dtype == np.bool_
    np.testing.assert_array_equal(out, np.array([[True, False], [False, True]]))


def test_read_land_mask_all_land(tmp_path):
    path = tmp_path / "land_mask.tif"
    _write_landmask(path, np.ones((3, 3), dtype=np.uint8))
    assert read_land_mask(path).all()


def test_read_land_mask_all_ocean(tmp_path):
    path = tmp_path / "land_mask.tif"
    _write_landmask(path, np.full((3, 3), 255, dtype=np.uint8))
    assert not read_land_mask(path).any()
