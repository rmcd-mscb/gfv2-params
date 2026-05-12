"""Tests for the pervious-area binary truth table in build_depstor_perv."""

import importlib.util
from pathlib import Path

import numpy as np
import pytest


_SCRIPT = Path(__file__).parent.parent / "scripts" / "build_depstor_perv.py"
_spec = importlib.util.spec_from_file_location("build_depstor_perv", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
compute_perv_binary = _mod.compute_perv_binary


# (imperv, dprst, expected_perv)
# Convention: 1 = present, 255 = absent/nodata.
TRUTH_TABLE = [
    (1, 1, 255),   # both flags set → not pervious
    (1, 255, 255), # imperv only → not pervious
    (255, 1, 255), # dprst only → not pervious
    (255, 255, 1), # neither → pervious
]


@pytest.mark.parametrize("imperv_val,dprst_val,expected", TRUTH_TABLE)
def test_compute_perv_binary_truth_table(imperv_val, dprst_val, expected):
    imperv = np.full((3, 3), imperv_val, dtype=np.uint8)
    dprst = np.full((3, 3), dprst_val, dtype=np.uint8)
    out = compute_perv_binary(imperv, dprst)
    assert out.dtype == np.uint8
    assert (out == expected).all()


def test_compute_perv_binary_mixed_grid():
    """Independent flags per cell — verify element-wise application."""
    imperv = np.array([[1, 1, 255, 255]], dtype=np.uint8)
    dprst = np.array([[1, 255, 1, 255]], dtype=np.uint8)
    out = compute_perv_binary(imperv, dprst)
    assert out.dtype == np.uint8
    np.testing.assert_array_equal(out, np.array([[255, 255, 255, 1]], dtype=np.uint8))
