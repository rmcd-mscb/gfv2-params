"""Tests for promoted depstor helpers: assert_raster_aligned + uint8_binary_profile.

Acceptance criterion for issue #64: `assert_raster_aligned` raises on shape /
CRS / transform mismatch. Plus a single positive case to confirm the happy
path. No geo I/O; uses minimal in-memory fixtures.
"""

from dataclasses import dataclass

import pytest
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
from rasterio.transform import Affine

from gfv2_params.depstor import (
    RasterInfo,
    assert_raster_aligned,
    uint8_binary_profile,
)


@dataclass
class FakeSrc:
    """Minimal stand-in for a rasterio dataset — only the 4 attrs the
    helper reads."""
    width: int
    height: int
    crs: CRS
    transform: Affine


@pytest.fixture
def template_info() -> RasterInfo:
    """Minimal template: EPSG:5070, 100x80 cells at 30m, origin (0, 0)."""
    return RasterInfo(
        crs=CRS.from_epsg(5070),
        width=100,
        height=80,
        transform=Affine(30, 0, 0, 0, -30, 0),
        nodata=255,
        bounds=BoundingBox(left=0, bottom=-2400, right=3000, top=0),
    )


@pytest.fixture
def aligned_src(template_info) -> FakeSrc:
    return FakeSrc(
        width=template_info.width,
        height=template_info.height,
        crs=template_info.crs,
        transform=template_info.transform,
    )


# --- assert_raster_aligned -------------------------------------------------


def test_assert_raster_aligned_passes_on_match(template_info, aligned_src):
    """Happy path — no exception, no return value."""
    assert assert_raster_aligned(aligned_src, template_info, "test") is None


def test_assert_raster_aligned_raises_on_shape_mismatch(template_info, aligned_src):
    aligned_src.width = template_info.width + 1
    with pytest.raises(ValueError, match=r"shape \(101x80\) != template \(100x80\)"):
        assert_raster_aligned(aligned_src, template_info, "shape_test")


def test_assert_raster_aligned_raises_on_crs_mismatch(template_info, aligned_src):
    aligned_src.crs = CRS.from_epsg(4326)
    with pytest.raises(ValueError, match=r"crs_test CRS.*!= template CRS"):
        assert_raster_aligned(aligned_src, template_info, "crs_test")


def test_assert_raster_aligned_raises_on_transform_mismatch(template_info, aligned_src):
    aligned_src.transform = Affine(60, 0, 0, 0, -60, 0)  # different cell size
    with pytest.raises(ValueError, match=r"transform_test transform mismatch"):
        assert_raster_aligned(aligned_src, template_info, "transform_test")


# --- uint8_binary_profile --------------------------------------------------


def test_uint8_binary_profile_shape(template_info):
    """Profile must reflect the template's shape, CRS, transform."""
    profile = uint8_binary_profile(template_info)
    assert profile["width"] == template_info.width
    assert profile["height"] == template_info.height
    assert profile["crs"] == template_info.crs
    assert profile["transform"] == template_info.transform


def test_uint8_binary_profile_uint8_conventions(template_info):
    """uint8 binary masks: dtype=uint8, nodata=255, LZW, tiled 256x256, BIGTIFF."""
    profile = uint8_binary_profile(template_info)
    assert profile["dtype"] == "uint8"
    assert profile["nodata"] == 255
    assert profile["count"] == 1
    assert profile["driver"] == "GTiff"
    assert profile["compress"] == "LZW"
    assert profile["tiled"] is True
    assert profile["blockxsize"] == 256
    assert profile["blockysize"] == 256
    assert profile["BIGTIFF"] == "YES"
