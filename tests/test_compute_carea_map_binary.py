"""Tests for compute_carea_map_binary used by build_depstor_carea_map."""

import numpy as np
import pytest

from gfv2_params.depstor import compute_carea_map_binary


# (perv_val, onstream_val, twi_val, threshold, twi_nodata, land_val, expected_out)
# Convention: perv/onstream are uint8 binary (1 = present, 255 = nodata).
# twi is float32. land_val is the boolean template-DEM land mask.
# Output is uint8: 1 = carea cell, 255 = excluded/nodata.
TRUTH_TABLE = [
    # perv=1, twi above threshold -> 1 (the "above-threshold" branch)
    (1, 255, 10.0, 8.0, -9999.0, True, 1),
    # perv=1, twi at threshold (strict >) -> 255
    (1, 255, 8.0, 8.0, -9999.0, True, 255),
    # perv=1, twi below threshold, onstream=1 -> 1 (the "onstream" branch)
    (1, 1, 5.0, 8.0, -9999.0, True, 1),
    # perv=1, twi below threshold, onstream=255 -> 255
    (1, 255, 5.0, 8.0, -9999.0, True, 255),
    # perv=255 (non-perv) -> 255 regardless of twi/onstream
    (255, 1, 100.0, 8.0, -9999.0, True, 255),
    (255, 255, 100.0, 8.0, -9999.0, True, 255),
    # perv=255 with TWI nodata sentinel must NOT cause spurious activation
    (255, 255, -9999.0, 8.0, -9999.0, True, 255),
    # perv=255 with TWI NaN must NOT cause spurious activation
    (255, 255, np.nan, 8.0, -9999.0, True, 255),
    # perv=1 with TWI nodata sentinel: above_thresh is false (nodata),
    # but onstream=1 -> still 1 (onstream rescue branch).
    (1, 1, -9999.0, 8.0, -9999.0, True, 1),
    # perv=1 with TWI NaN, onstream=255 -> 255
    (1, 255, np.nan, 8.0, -9999.0, True, 255),
    # Second threshold (smidx, 15.6): perv=1 twi=20 -> 1
    (1, 255, 20.0, 15.6, -9999.0, True, 1),
    # Second threshold: perv=1 twi=10 (above carea_max thresh but NOT smidx) -> 255
    (1, 255, 10.0, 15.6, -9999.0, True, 255),
    # Off-land cells are masked out even when every other condition passes.
    # (Both the above-threshold branch and the onstream rescue branch.)
    (1, 255, 10.0, 8.0, -9999.0, False, 255),
    (1, 1, 5.0, 8.0, -9999.0, False, 255),
]


@pytest.mark.parametrize(
    "perv_val,onstream_val,twi_val,threshold,twi_nodata,land_val,expected",
    TRUTH_TABLE,
)
def test_compute_carea_map_binary_truth_table(
    perv_val, onstream_val, twi_val, threshold, twi_nodata, land_val, expected,
):
    perv = np.full((3, 3), perv_val, dtype=np.uint8)
    onstream = np.full((3, 3), onstream_val, dtype=np.uint8)
    twi = np.full((3, 3), twi_val, dtype=np.float32)
    land_valid = np.full((3, 3), land_val, dtype=bool)
    out = compute_carea_map_binary(perv, onstream, twi, threshold, twi_nodata, land_valid)
    assert out.dtype == np.uint8
    assert (out == expected).all(), f"expected {expected}, got {out}"


def test_compute_carea_map_binary_mixed_grid():
    """Independent flags per cell — verify element-wise application."""
    perv = np.array([[1, 1, 1, 1, 255, 1]], dtype=np.uint8)
    onstream = np.array([[255, 1, 255, 1, 1, 1]], dtype=np.uint8)
    twi = np.array([[10.0, 5.0, 5.0, np.nan, 100.0, 10.0]], dtype=np.float32)
    land_valid = np.array([[True, True, True, True, True, False]], dtype=bool)
    # threshold=8.0, nodata=-9999.0
    # cell 0: perv=1, twi(10) > 8 -> 1
    # cell 1: perv=1, twi(5) <= 8, onstream=1 -> 1
    # cell 2: perv=1, twi(5) <= 8, onstream=255 -> 255
    # cell 3: perv=1, twi=NaN invalid, onstream=1 -> 1 (onstream rescue)
    # cell 4: perv=255 -> 255 (short-circuit on non-perv)
    # cell 5: every condition passes but land_valid=False -> 255 (off-land)
    out = compute_carea_map_binary(perv, onstream, twi, 8.0, -9999.0, land_valid)
    np.testing.assert_array_equal(
        out, np.array([[1, 1, 255, 1, 255, 255]], dtype=np.uint8)
    )


def test_compute_carea_map_binary_handles_nan_nodata_kwarg():
    """When the TWI raster's nodata is NaN (rather than a sentinel), NaN cells
    must still be treated as invalid TWI."""
    perv = np.array([[1, 1]], dtype=np.uint8)
    onstream = np.array([[255, 255]], dtype=np.uint8)
    twi = np.array([[10.0, np.nan]], dtype=np.float32)
    land_valid = np.array([[True, True]], dtype=bool)
    out = compute_carea_map_binary(perv, onstream, twi, 8.0, np.nan, land_valid)
    np.testing.assert_array_equal(out, np.array([[1, 255]], dtype=np.uint8))
