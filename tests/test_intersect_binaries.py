"""Tests for the intersect_binaries helper used by build_depstor_intersect."""

import numpy as np
import pytest

from gfv2_params.depstor import intersect_binaries


# (a_val, b_val, expected_out)
TRUTH_TABLE = [
    (1, 1, 1),       # both present → intersect
    (1, 255, 255),   # only A → nodata
    (255, 1, 255),   # only B → nodata
    (255, 255, 255), # neither → nodata
]


@pytest.mark.parametrize("a_val,b_val,expected", TRUTH_TABLE)
def test_intersect_binaries_truth_table(a_val, b_val, expected):
    a = np.full((3, 3), a_val, dtype=np.uint8)
    b = np.full((3, 3), b_val, dtype=np.uint8)
    out = intersect_binaries(a, b)
    assert out.dtype == np.uint8
    assert (out == expected).all()


def test_intersect_binaries_mixed_grid():
    """Independent flags per cell — verify element-wise application."""
    a = np.array([[1, 1, 255, 255]], dtype=np.uint8)
    b = np.array([[1, 255, 1, 255]], dtype=np.uint8)
    out = intersect_binaries(a, b)
    assert out.dtype == np.uint8
    np.testing.assert_array_equal(out, np.array([[1, 255, 255, 255]], dtype=np.uint8))
