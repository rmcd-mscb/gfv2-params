"""Unit tests for the carea/smidx threshold-sweep math + artifact (issue #55)."""

import numpy as np
import pytest

from gfv2_params.threshold_sweep import (
    CareaTwiArtifact,
    accumulate_strip,
    evaluate_threshold,
    percentile_to_value,
    reference_grid,
    sweep,
    value_to_percentile,
)


def _toy_artifact():
    # 3 HRUs, 4 TWI bins with edges [0,5,10,15,20] -> centers 2.5,7.5,12.5,17.5
    bin_edges = np.array([0.0, 5.0, 10.0, 15.0, 20.0])
    hist = np.array([
        [0, 2, 1, 1],   # HRU 0: 4 perv-non-onstream cells across bins
        [0, 0, 0, 0],   # HRU 1: none in hist (all onstream or no twi)
        [10, 0, 0, 0],  # HRU 2: 10 cells in lowest bin
    ], dtype="int64")
    # reference grid: percentiles 0..100 step 50 -> values 0,10,20 (linear)
    ref_pctl = np.array([0.0, 50.0, 100.0])
    ref_value = np.array([0.0, 10.0, 20.0])
    return CareaTwiArtifact(
        ids=np.array([101, 102, 103]),
        vpu=np.array(["17", "17", "17"], dtype=object),
        n_perv=np.array([5, 3, 10], dtype="int64"),         # HRU0 has 1 extra perv (nodata twi)
        n_perv_onstream=np.array([1, 3, 0], dtype="int64"),  # HRU1 fully onstream
        hist=hist, bin_edges=bin_edges,
        ref_pctl=ref_pctl, ref_value=ref_value,
        fabric="oregon", twi_source="hydrodem",
    )


def test_evaluate_threshold_below_all_bins():
    a = _toy_artifact()
    # t=0 -> all hist cells (centers>0) count; HRU0: onstream1 + 4 = 5 /5 =1.0
    p = evaluate_threshold(a, 0.0)
    assert p[0] == pytest.approx(1.0)          # (1 + 4)/5
    assert p[1] == pytest.approx(1.0)          # (3 + 0)/3 fully onstream
    assert p[2] == pytest.approx(1.0)          # (0 + 10)/10


def test_evaluate_threshold_mid():
    a = _toy_artifact()
    # t=10 -> bins with center>10 are centers 12.5,17.5 => HRU0 cols 2,3 => 1+1=2
    p = evaluate_threshold(a, 10.0)
    assert p[0] == pytest.approx((1 + 2) / 5)  # onstream1 + 2
    assert p[1] == pytest.approx(1.0)          # onstream rescue, 3/3
    assert p[2] == pytest.approx(0.0)          # 0 above + 0 onstream / 10


def test_evaluate_threshold_above_all():
    a = _toy_artifact()
    p = evaluate_threshold(a, 100.0)           # nothing above
    assert p[0] == pytest.approx(1 / 5)        # only onstream
    assert p[1] == pytest.approx(1.0)
    assert p[2] == pytest.approx(0.0)


def test_evaluate_zero_perv_is_zero():
    a = _toy_artifact()
    a.n_perv[2] = 0
    p = evaluate_threshold(a, 10.0)
    assert p[2] == 0.0


def test_value_percentile_roundtrip():
    a = _toy_artifact()
    assert value_to_percentile(a, 10.0) == pytest.approx(50.0)
    assert percentile_to_value(a, 50.0) == pytest.approx(10.0)


def test_sweep_mean_is_non_increasing():
    a = _toy_artifact()
    df = sweep(a, np.array([0.0, 5.0, 10.0, 15.0, 100.0]))
    means = df["mean"].to_numpy()
    assert np.all(np.diff(means) <= 1e-9)
    assert set(["threshold", "mean", "median", "frac_zero", "frac_one"]).issubset(df.columns)


def test_artifact_save_load_roundtrip(tmp_path):
    a = _toy_artifact()
    p = tmp_path / "art.npz"
    a.save(p)
    b = CareaTwiArtifact.load(p)
    assert np.array_equal(a.hist, b.hist)
    assert np.array_equal(a.ids, b.ids)
    assert list(a.vpu) == list(b.vpu)
    assert b.fabric == "oregon" and b.twi_source == "hydrodem"
    assert evaluate_threshold(b, 10.0)[0] == pytest.approx((1 + 2) / 5)


def test_accumulate_strip_counts():
    n_hru = 2
    bin_edges = np.array([0.0, 5.0, 10.0, 15.0])  # centers 2.5,7.5,12.5
    n_perv = np.zeros(n_hru, "int64")
    n_perv_onstream = np.zeros(n_hru, "int64")
    hist = np.zeros((n_hru, 3), "int64")
    land_twi_hist = np.zeros(3, "int64")
    # 2x3 strip. hru_idx -1 = no HRU.
    hru_idx = np.array([[0, 0, 1], [1, -1, 0]])
    perv = np.array([[1, 1, 1], [1, 1, 0]], "uint8")
    onstream = np.array([[0, 1, 0], [0, 0, 0]], "uint8")
    twi = np.array([[2.0, 8.0, 12.0], [13.0, 8.0, 1.0]], "float32")
    land = np.ones((2, 3), bool)
    accumulate_strip(hru_idx, perv, onstream, twi, land, -9999.0, bin_edges,
                     n_perv, n_perv_onstream, hist, land_twi_hist)
    # HRU0 perv cells: (0,0)twi2 perv nonos; (0,1)twi8 perv onstream; (1,2)perv=0 skip
    #   -> n_perv[0]=2, onstream[0]=1, hist[0]= bin(2.0)->0 => [1,0,0]
    assert n_perv[0] == 2 and n_perv_onstream[0] == 1
    assert list(hist[0]) == [1, 0, 0]
    # HRU1 perv cells: (0,2)twi12 nonos; (1,0)twi13 nonos -> n_perv=2, hist bins 12.5,12.5
    assert n_perv[1] == 2 and n_perv_onstream[1] == 0
    assert list(hist[1]) == [0, 0, 2]
    # land_twi_hist counts ALL land valid-twi cells (incl. onstream & non-perv):
    # twis: 2,8,12,13,8,1 -> bins: 0,1,2,2,1,0 => [2,2,2]
    assert list(land_twi_hist) == [2, 2, 2]


def test_accumulate_strip_skips_nodata_twi_in_hist():
    bin_edges = np.array([0.0, 5.0, 10.0])
    n_perv = np.zeros(1, "int64")
    n_os = np.zeros(1, "int64")
    hist = np.zeros((1, 2), "int64")
    land_twi = np.zeros(2, "int64")
    hru_idx = np.array([[0, 0]])
    perv = np.array([[1, 1]], "uint8")
    onstream = np.array([[0, 0]], "uint8")
    twi = np.array([[3.0, -9999.0]], "float32")  # 2nd cell nodata
    land = np.ones((1, 2), bool)
    accumulate_strip(hru_idx, perv, onstream, twi, land, -9999.0, bin_edges,
                     n_perv, n_os, hist, land_twi)
    assert n_perv[0] == 2          # both pervious count toward denom
    assert list(hist[0]) == [1, 0]  # only the valid-twi cell binned
    assert list(land_twi) == [1, 0]


def test_reference_grid_linear():
    # uniform hist over [0,20] -> percentile p maps ~linearly to value
    bin_edges = np.linspace(0, 20, 5)       # 4 bins, centers 2.5,7.5,12.5,17.5
    land_twi_hist = np.array([10, 10, 10, 10], "int64")
    pctl, value = reference_grid(land_twi_hist, bin_edges, np.array([0.0, 50.0, 100.0]))
    assert pctl[1] == 50.0
    assert value[0] == pytest.approx(0.0)    # p0 -> first edge
    assert value[1] == pytest.approx(10.0)   # p50 of uniform [0,20] -> 10.0 (edge-based CDF)
    assert value[2] == pytest.approx(20.0)   # p100 -> last edge
