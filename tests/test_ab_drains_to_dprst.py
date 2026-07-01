"""Unit tests for the A/B harness helpers (no WBT, no warp — pure logic)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
from rasterio.transform import Affine

from gfv2_params.depstor import RasterInfo
from scripts.diagnose.ab_drains_to_dprst import (
    _write_window_uint8,
    per_depression_counts,
    resolve_fdr_path,
)


def test_resolve_fdr_path_selects_each_source():
    fdr_vrt = Path("/data/shared/gfv2_fdr.vrt")
    per_vpu = Path("/data/shared/per_vpu")
    assert resolve_fdr_path("production", "09", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == fdr_vrt
    assert resolve_fdr_path("fill", "09", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == \
        per_vpu / "09" / "Fdr_hydrodem_09.tif"
    assert resolve_fdr_path("breach", "16", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == \
        per_vpu / "16" / "Fdr_breached_16.tif"


def test_resolve_fdr_path_rejects_unknown():
    with pytest.raises(ValueError):
        resolve_fdr_path("bogus", "09", fdr_vrt=Path("x"), per_vpu_dir=Path("y"))


def test_per_depression_counts_drops_background_and_counts_labels():
    labeled = np.array([[0, 7, 7], [9, 9, 9]], dtype=np.int32)
    counts = per_depression_counts(labeled)
    assert counts == {7: 2, 9: 3}


def test_write_window_uint8_matches_bbox_dimensions(tmp_path):
    """_write_window_uint8 must write the passed-in array without re-slicing it.

    The bug being guarded: the old code did ``drains[r0:r1, c0:c1]`` where
    r0/c0/r1/c1 are full-grid offsets (thousands of rows/cols), silently
    yielding an empty array when the input was already VPU-window-sized.
    """
    info = RasterInfo(
        crs=CRS.from_epsg(5070),
        width=1000,
        height=1000,
        transform=Affine(30.0, 0.0, 0.0, 0.0, -30.0, 0.0),
        nodata=255,
        bounds=BoundingBox(0, -30000, 30000, 0),
    )
    # bbox with full-grid-style offsets: height = 13-10 = 3, width = 24-20 = 4
    bbox = (10, 13, 20, 24)
    arr = np.array(
        [[1, 0, 1, 0],
         [0, 1, 1, 0],
         [1, 1, 0, 1]],
        dtype=np.uint8,
    )
    out = tmp_path / "win.tif"
    _write_window_uint8(arr, info, bbox, out)
    with rasterio.open(out) as ds:
        assert (ds.height, ds.width) == (3, 4)
        np.testing.assert_array_equal(ds.read(1), arr)
