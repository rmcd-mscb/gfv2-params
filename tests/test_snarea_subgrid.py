import numpy as np
import pandas as pd
import pytest

from gfv2_params.snarea.subgrid import representative_peak_stats


def _daily(dates, swe, std):
    return pd.DataFrame({"swe": swe, "swe_std": std}, index=pd.to_datetime(dates))


def test_peak_stats_single_water_year():
    # one melt season peaking Apr 1 2011: peak swe 200, std 100 -> cv 0.5
    dates = pd.date_range("2010-10-01", "2011-09-30")
    swe = np.concatenate([np.linspace(0, 200, 183), np.linspace(200, 0, len(dates) - 183)])
    std = swe * 0.5
    out = representative_peak_stats(_daily(dates, swe, std))
    assert out["n_peak_years"] == 1
    assert out["cv_subgrid"] == pytest.approx(0.5, abs=1e-6)
    assert out["peak_swe_mm"] == pytest.approx(200.0, abs=1e-6)


def test_peak_stats_no_snow_returns_nan():
    dates = pd.date_range("2010-10-01", "2011-09-30")
    out = representative_peak_stats(_daily(dates, np.zeros(len(dates)), np.zeros(len(dates))))
    assert out["n_peak_years"] == 0
    assert np.isnan(out["cv_subgrid"])


def test_peak_stats_median_across_years():
    d1 = pd.date_range("2010-10-01", "2011-09-30")
    d2 = pd.date_range("2011-10-01", "2012-09-30")

    def season(dates, peak, cvf):
        h = len(dates) // 2
        swe = np.concatenate([np.linspace(0, peak, h), np.linspace(peak, 0, len(dates) - h)])
        return swe, swe * cvf

    s1, t1 = season(d1, 100, 0.4)
    s2, t2 = season(d2, 300, 0.8)
    df = pd.concat([_daily(d1, s1, t1), _daily(d2, s2, t2)])
    out = representative_peak_stats(df)
    assert out["n_peak_years"] == 2
    assert out["cv_subgrid"] == pytest.approx(0.6, abs=1e-6)   # median(0.4, 0.8)
    assert out["peak_swe_mm"] == pytest.approx(200.0, abs=1e-6)  # median(100, 300)
