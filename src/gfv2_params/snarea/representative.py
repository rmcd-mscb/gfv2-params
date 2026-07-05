"""Median SDC, inter-annual similarity (Eq. 1), and representative-curve pick."""

from __future__ import annotations

import numpy as np


def median_sdc(annual: np.ndarray) -> np.ndarray:
    """Elementwise median across the annual SDCs (rows = years, cols = levels)."""
    return np.median(annual, axis=0)


def similarity(annual: np.ndarray, median: np.ndarray) -> float:
    """Driscoll Eq. 1: Σ over years and points of |SDC − median| ÷ points.

    A per-HRU scalar; 0 = identical curves every year, larger = more inter-annual
    variability. `points` is the number of curve points (11).
    """
    points = annual.shape[1]
    return float(np.sum(np.abs(annual - median)) / points)


def select_representative(annual: np.ndarray, median: np.ndarray) -> np.ndarray:
    """The single year's SDC closest (min mean-abs distance) to the median."""
    dist = np.mean(np.abs(annual - median), axis=1)
    return annual[int(np.argmin(dist))]
