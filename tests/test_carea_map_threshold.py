"""compute_carea_map_binary must accept a per-cell-array threshold (percentile
mode, per-VPU) and match the scalar path cell-for-cell when the array is
constant (issue #55)."""

import numpy as np

from gfv2_params.depstor import compute_carea_map_binary


def _inputs():
    perv = np.array([[1, 1, 1], [1, 0, 1]], dtype="uint8")
    onstream = np.array([[0, 0, 1], [0, 0, 0]], dtype="uint8")
    twi = np.array([[5.0, 9.0, 5.0], [20.0, 9.0, -9999.0]], dtype="float32")
    land = np.ones((2, 3), dtype=bool)
    return perv, onstream, twi, land


def test_scalar_threshold_unchanged():
    perv, onstream, twi, land = _inputs()
    out = compute_carea_map_binary(perv, onstream, twi, 8.0, -9999.0, land)
    # keep where perv & land & (twi>8 or onstream); 255 elsewhere
    expected = np.array([[255, 1, 1], [1, 255, 255]], dtype="uint8")
    assert np.array_equal(out, expected)


def test_constant_array_threshold_matches_scalar():
    perv, onstream, twi, land = _inputs()
    scalar = compute_carea_map_binary(perv, onstream, twi, 8.0, -9999.0, land)
    arr = np.full(twi.shape, 8.0, dtype="float64")
    out = compute_carea_map_binary(perv, onstream, twi, arr, -9999.0, land)
    assert np.array_equal(out, scalar)


def test_per_cell_array_threshold_varies():
    perv, onstream, twi, land = _inputs()
    # column 1 has twi=9; threshold 10 there -> excluded; threshold 8 -> included
    arr = np.array([[8.0, 10.0, 8.0], [8.0, 10.0, 8.0]], dtype="float64")
    out = compute_carea_map_binary(perv, onstream, twi, arr, -9999.0, land)
    assert out[0, 1] == 255  # 9 !> 10
    assert out[0, 2] == 1    # onstream rescues
