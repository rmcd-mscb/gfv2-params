"""compute_carea_map_binary must accept a per-cell-array threshold (percentile
mode, per-VPU) and match the scalar path cell-for-cell when the array is
constant (issue #55)."""

import csv
from pathlib import Path

import numpy as np
import pytest

from gfv2_params.depstor import compute_carea_map_binary
from gfv2_params.depstor_builders.carea_map import (
    load_reference_table,
    resolve_scalar_thresholds,
)


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


def _write_table(tmp_path) -> Path:
    p = tmp_path / "twi_reference_percentiles.hydrodem.csv"
    with open(p, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["source", "scope", "vpu", "p_carea", "p_smidx", "t_carea", "t_smidx"])
        w.writeheader()
        w.writerow({"source": "hydrodem", "scope": "conus", "vpu": "CONUS", "p_carea": 8, "p_smidx": 16, "t_carea": 7.7, "t_smidx": 14.9})
        w.writerow({"source": "hydrodem", "scope": "vpu", "vpu": "17", "p_carea": 8, "p_smidx": 16, "t_carea": 6.2, "t_smidx": 12.1})
    return p


def test_load_reference_table_indexes_by_scope_vpu(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    assert table[("conus", "CONUS")]["t_carea"] == 7.7
    assert table[("vpu", "17")]["t_smidx"] == 12.1


def test_resolve_scalar_conus(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    tc, ts = resolve_scalar_thresholds(table, scope="conus", vpu=None)
    assert (tc, ts) == (7.7, 14.9)


def test_resolve_scalar_single_vpu(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    tc, ts = resolve_scalar_thresholds(table, scope="vpu", vpu="17")
    assert (tc, ts) == (6.2, 12.1)


def test_resolve_scalar_missing_vpu_raises(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    with pytest.raises(KeyError, match="no reference row"):
        resolve_scalar_thresholds(table, scope="vpu", vpu="09")
