"""Valid-land TWI percentile cutoffs for distribution-invariant carea_max /
smidx_coef (issues #94 + #55 Stage 1).

The cutoff that classifies a cell as "wet" used to be a hardcoded TWI value
(8.0 / 15.6) calibrated to the ArcPy TWI distribution. Here we derive it from
the data instead: the P-th percentile of valid-land TWI over a reference
population (per-VPU or CONUS). Because it is recomputed from whatever TWI
source is in play, swapping the source preserves each cell's rank, so the
parameters become invariant to the source.

This module holds the pure math (percentile / CDF-inversion) plus the
`build_twi_reference` shared-raster builder that samples the staged TWI tiles.
"""

from __future__ import annotations

import numpy as np


def _valid(values: np.ndarray, nodata: float | None) -> np.ndarray:
    """Return the finite, non-nodata subset as a 1-D float64 array."""
    v = np.asarray(values, dtype="float64").ravel()
    mask = np.isfinite(v)
    if nodata is not None:
        mask &= v != nodata
    return v[mask]


def percentile_of_values(values: np.ndarray, ps, nodata: float | None = None):
    """The P-th percentile(s) of the valid values. `ps` is a list of [0,100]."""
    valid = _valid(values, nodata)
    if valid.size == 0:
        raise ValueError("percentile_of_values: no valid (finite, non-nodata) values")
    return [float(x) for x in np.percentile(valid, ps)]


def rank_of_value(values: np.ndarray, value: float, nodata: float | None = None) -> float:
    """Percentile rank (0-100) of `value` in the valid distribution: the
    fraction of valid values <= `value`, x100. Inverse of percentile_of_values;
    used to find what percentile the legacy 8.0 / 15.6 occupy."""
    valid = _valid(values, nodata)
    if valid.size == 0:
        raise ValueError("rank_of_value: no valid values")
    return float(100.0 * np.count_nonzero(valid <= value) / valid.size)
