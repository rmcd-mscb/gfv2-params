"""Per-calendar-year snow-depletion-curve extraction from daily SWE/SCA.

Implements the Driscoll et al. (2017) melt-season curve: isolate peak→SWE=0,
remove post-peak snowfall reversals (idealized monotonic melt), normalize, and
sample SCA at the 11 fixed normalized-SWE levels.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SWE_LEVELS = np.round(np.arange(1.0, -0.0001, -0.1), 1)  # 1.0 .. 0.0, 11 values


def melt_season(swe: pd.Series, sca: pd.Series):
    """Trim to the melt window: date of peak SWE → first day SWE returns to 0.

    Returns (swe_window, sca_window) or None if the HRU-year has no snow or SWE
    never returns to 0 within the series (persistent snowfield / truncated year).
    """
    if swe.max() <= 0:
        return None
    peak_pos = int(np.argmax(swe.values))
    after = swe.iloc[peak_pos:]
    zero_positions = np.where(after.values <= 0)[0]
    if len(zero_positions) == 0:
        return None                        # never melts out — flagged upstream
    end_pos = peak_pos + int(zero_positions[0])
    return swe.iloc[peak_pos:end_pos + 1], sca.iloc[peak_pos:end_pos + 1]


def remove_reversals(swe: pd.Series, sca: pd.Series):
    """Keep only the running-minimum envelope of SCA (idealized monotonic melt).

    Reproduces the paper's rule that post-peak snowfall (an SCA increase) is
    removed until SCA returns to its pre-increase value: a point survives only
    if its SCA is ≤ the smallest SCA kept so far.
    """
    keep = []
    running_min = np.inf
    for i, val in enumerate(sca.values):
        if val <= running_min:
            keep.append(i)
            running_min = val
    return swe.iloc[keep], sca.iloc[keep]


def normalize_curve(swe: pd.Series, sca: pd.Series):
    """Normalize SWE by peak SWE and SCA by its value at peak SWE."""
    peak_swe = swe.iloc[0]
    sca_at_peak = sca.iloc[0]
    swe_n = (swe.values / peak_swe) if peak_swe > 0 else np.zeros(len(swe))
    sca_n = (sca.values / sca_at_peak) if sca_at_peak > 0 else np.zeros(len(sca))
    return swe_n, sca_n


def annual_sdc(swe: pd.Series, sca: pd.Series):
    """Return the 11-point SDC (SCA at each SWE_LEVEL) or None if unusable."""
    ms = melt_season(swe, sca)
    if ms is None:
        return None
    swe_w, sca_w = remove_reversals(*ms)
    if len(swe_w) < 2:
        return None
    swe_n, sca_n = normalize_curve(swe_w, sca_w)
    # np.interp needs ascending x; swe_n descends over the melt, so sort ascending.
    order = np.argsort(swe_n)
    xs, ys = swe_n[order], sca_n[order]
    curve = np.interp(SWE_LEVELS, xs, ys, left=ys[0], right=ys[-1])
    # Enforce monotonic non-increasing across descending SWE levels (numerical guard).
    curve = np.minimum.accumulate(curve)
    curve[0] = min(curve[0], 1.0)
    return np.clip(curve, 0.0, 1.0)
