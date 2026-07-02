"""Per-depression labeled D8 attribution (drains_to_dprst_labeled_kernel)."""

from __future__ import annotations

import numpy as np

from gfv2_params.d8_routing import drains_to_dprst_labeled_kernel

# ESRI D8: 1=E 2=SE 4=S 8=SW 16=W 32=NW 64=N 128=NE


def test_two_depressions_get_distinct_labels_and_local_areas():
    # 1x5 row: cells 0,1 flow east into depression label 7 at col 2;
    # cells 4,3 flow west into depression label 9 at... use a 1x6 row:
    # cols: [0]->E [1]->E [2]=dep7  [3]=dep9 [4]->W [5]->W
    fdr = np.array([[1, 1, 0, 0, 16, 16]], dtype=np.uint8)
    labels = np.array([[0, 0, 7, 9, 0, 0]], dtype=np.int32)
    barrier = np.zeros_like(labels, dtype=np.uint8)
    out, n_cycles = drains_to_dprst_labeled_kernel(fdr, labels, barrier, fdr_nodata=255)
    assert n_cycles == 0
    # cols 0,1,2 attributed to depression 7; cols 3,4,5 to depression 9
    assert out.tolist() == [[7, 7, 7, 9, 9, 9]]
    # per-depression contributing area (cells, incl. the pour cell)
    counts = np.bincount(out.ravel())
    assert counts[7] == 3
    assert counts[9] == 3


def test_cell_flowing_to_sink_gets_zero_label():
    # col0 -> E into col1; col1 has FDR nodata (sink, no depression)
    fdr = np.array([[1, 255]], dtype=np.uint8)
    labels = np.array([[0, 0]], dtype=np.int32)
    barrier = np.zeros_like(labels, dtype=np.uint8)
    out, n_cycles = drains_to_dprst_labeled_kernel(fdr, labels, barrier, fdr_nodata=255)
    assert n_cycles == 0
    assert out.tolist() == [[0, 0]]


def test_cycle_marked_zero_and_counted():
    # col0 -> E (col1), col1 -> W (col0): a 2-cell cycle, no depression reached
    fdr = np.array([[1, 16]], dtype=np.uint8)
    labels = np.array([[0, 0]], dtype=np.int32)
    barrier = np.zeros_like(labels, dtype=np.uint8)
    out, n_cycles = drains_to_dprst_labeled_kernel(fdr, labels, barrier, fdr_nodata=255)
    assert n_cycles == 1
    assert out.tolist() == [[0, 0]]
