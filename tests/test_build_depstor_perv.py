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


# (imperv, dprst, land_valid, expected_perv)
# Convention: imperv/dprst are 1 = present, 255 = absent/nodata.
# land_valid is the boolean template-DEM land mask (True = on land).
TRUTH_TABLE = [
    (1, 1, True, 255),     # both flags set → not pervious
    (1, 255, True, 255),   # imperv only → not pervious
    (255, 1, True, 255),   # dprst only → not pervious
    (255, 255, True, 1),   # neither, on land → pervious
    (255, 255, False, 255),  # neither flag but OFF LAND (ocean) → not pervious
    (1, 1, False, 255),    # off land → not pervious regardless of flags
]


@pytest.mark.parametrize("imperv_val,dprst_val,land_val,expected", TRUTH_TABLE)
def test_compute_perv_binary_truth_table(imperv_val, dprst_val, land_val, expected):
    imperv = np.full((3, 3), imperv_val, dtype=np.uint8)
    dprst = np.full((3, 3), dprst_val, dtype=np.uint8)
    land_valid = np.full((3, 3), land_val, dtype=bool)
    out = compute_perv_binary(imperv, dprst, land_valid)
    assert out.dtype == np.uint8
    assert (out == expected).all()


def test_compute_perv_binary_mixed_grid():
    """Independent flags per cell — verify element-wise application."""
    imperv = np.array([[1, 1, 255, 255, 255]], dtype=np.uint8)
    dprst = np.array([[1, 255, 1, 255, 255]], dtype=np.uint8)
    land_valid = np.array([[True, True, True, True, False]], dtype=bool)
    # cell 4 has neither flag set but is off-land → masked to 255.
    out = compute_perv_binary(imperv, dprst, land_valid)
    assert out.dtype == np.uint8
    np.testing.assert_array_equal(
        out, np.array([[255, 255, 255, 1, 255]], dtype=np.uint8)
    )


def test_compute_perv_binary_land_mask_overrides_clear_cell():
    """An off-land cell that is neither impervious nor depression-storage —
    the case that previously leaked the whole ocean in as pervious."""
    imperv = np.full((2, 2), 255, dtype=np.uint8)
    dprst = np.full((2, 2), 255, dtype=np.uint8)
    land_valid = np.array([[True, False], [False, True]], dtype=bool)
    out = compute_perv_binary(imperv, dprst, land_valid)
    np.testing.assert_array_equal(out, np.array([[1, 255], [255, 1]], dtype=np.uint8))
