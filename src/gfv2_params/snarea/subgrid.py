"""Representative sub-grid SWE CV + peak SWE per HRU, for the CV/lognormal library.

CV = area-weighted std/mean of the SNODAS SWE pdf within an HRU, taken at each
water-year's peak-mean-SWE day (CV is most stable where mean SWE is largest), then
median across years. Water-year framing matches snarea/build.py:_seasons so a
late-December accumulation is not mis-picked as the peak.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def representative_peak_stats(daily: pd.DataFrame) -> dict:
    """Compute median CV and peak SWE across water years.

    Parameters
    ----------
    daily : pd.DataFrame
        DataFrame with DatetimeIndex and columns `swe` (mean) and `swe_std`.

    Returns
    -------
    dict
        Keys: cv_subgrid (median CV at peak day), peak_swe_mm (median peak SWE),
        n_peak_years (count of valid years). If no valid years, returns NaN/0.
    """
    if len(daily) == 0:
        return {"cv_subgrid": float("nan"), "peak_swe_mm": float("nan"), "n_peak_years": 0}

    water_year = daily.index.year + (daily.index.month >= 10).astype(int)
    cvs, peaks = [], []

    for _wy, grp in daily.groupby(water_year):
        swe = grp["swe"].to_numpy(dtype=float)
        std = grp["swe_std"].to_numpy(dtype=float)

        # Skip years with no finite values or max <= 0
        if not np.isfinite(swe).any() or np.nanmax(swe) <= 0:
            continue

        # Find peak-mean-SWE day
        i = int(np.nanargmax(swe))
        peak, s = swe[i], std[i]

        # Skip if peak or std non-finite, or peak <= 0
        if not (np.isfinite(peak) and peak > 0 and np.isfinite(s)):
            continue

        cvs.append(s / peak)
        peaks.append(peak)

    # Return medians + count
    if not cvs:
        return {"cv_subgrid": float("nan"), "peak_swe_mm": float("nan"), "n_peak_years": 0}

    return {
        "cv_subgrid": float(np.median(cvs)),
        "peak_swe_mm": float(np.median(peaks)),
        "n_peak_years": len(cvs),
    }
