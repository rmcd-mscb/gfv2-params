"""Median SDC, inter-annual similarity (Eq. 1), and representative-curve pick."""

from __future__ import annotations

import numpy as np


def median_sdc(annual: np.ndarray) -> np.ndarray:
    """Elementwise median across the annual SDCs (rows = years, cols = levels)."""
    return np.median(annual, axis=0)


def similarity(annual: np.ndarray, median: np.ndarray) -> float:
    """Mean per-point absolute deviation of the annual SDCs from their median.

    A per-HRU scalar; 0 = identical curves every year, larger = more inter-annual
    variability. This is a **scale-free** variant of Driscoll Eq. 1: the paper
    summed the per-point distances over its FIXED nine seasons and divided only
    by the number of points, but with a VARIABLE number of usable seasons per
    HRU (as here) that sum grows with season count, so a fixed ``max_similarity``
    threshold would select for poorly-sampled HRUs. Dividing by ``annual.size``
    (points × n_seasons) — i.e. taking the mean — makes the value comparable
    across HRUs regardless of how many seasons they have (see the 2026-07-06
    Oregon investigation).
    """
    return float(np.mean(np.abs(annual - median)))


def select_representative(annual: np.ndarray, median: np.ndarray) -> np.ndarray:
    """The single year's SDC closest (min mean-abs distance) to the median."""
    dist = np.mean(np.abs(annual - median), axis=1)
    return annual[int(np.argmin(dist))]
