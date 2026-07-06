import pandas as pd

from gfv2_params.snarea.build import DEFAULT_SNAREA_CURVE, build_hru_record
from gfv2_params.snarea.selection import SelectionParams


def _daily_two_years():
    # two clean melt seasons in 2010 and 2011, snow present, monotonic.
    # A lower lead-in day precedes the peak so the peak isn't on day 0 of
    # the calendar-year record (melt_season excludes peak-at-day0 per spec §4).
    frames = []
    for yr in (2010, 2011):
        idx = pd.date_range(f"{yr}-01-31", periods=7, freq="D")
        frames.append(pd.DataFrame(
            {"swe": [5, 10, 8, 6, 4, 2, 0], "sca": [.5, 1.0, .9, .7, .5, .3, 0.0]}, index=idx))
    return pd.concat(frames)


def test_derived_hru_record():
    rec = build_hru_record(
        hru_id=7, daily=_daily_two_years(), n_cells=100, water_frac=0.0,
        params=SelectionParams(), default_curve=DEFAULT_SNAREA_CURVE)
    assert rec["hru_id"] == 7
    assert rec["sdc_status"] == "derived"
    assert rec["n_seasons"] == 2
    assert rec["snarea_curve_0"] == 1.0
    assert rec["snarea_curve_10"] <= 1e-6
    assert rec["sca_class"] in {"low", "mid", "high"}


def test_no_snow_falls_back_to_default():
    idx = pd.date_range("2010-02-01", periods=6, freq="D")
    dry = pd.DataFrame({"swe": [0.0] * 6, "sca": [0.0] * 6}, index=idx)
    rec = build_hru_record(
        hru_id=3, daily=dry, n_cells=100, water_frac=0.0,
        params=SelectionParams(), default_curve=DEFAULT_SNAREA_CURVE)
    assert rec["sdc_status"] == "default_no_snow"
    assert rec["snarea_curve_0"] == DEFAULT_SNAREA_CURVE[0]
    assert rec["snarea_curve_10"] == DEFAULT_SNAREA_CURVE[10]


def test_seasons_uses_water_year_grouping():
    # A real melt season (Jan-Feb 2021, peak Jan) plus a LARGER late-year
    # accumulation (Nov-Dec 2021) that never melts. Calendar-year framing would
    # argmax-pick the December spike in CY2021 and lose the real season (0
    # curves); water-year framing keeps the Jan-Feb melt as WY2021 (1 usable
    # season) and drops the Nov-Dec accumulation into WY2022.
    import numpy as np

    from gfv2_params.snarea.build import _seasons

    idx = pd.date_range("2021-01-01", "2021-12-31", freq="D")
    swe = pd.Series(0.0, index=idx)
    sca = pd.Series(0.0, index=idx)
    # Real season: accumulate Jan 1-20 (peak not on day 0), melt Jan 20 - Mar 10.
    rise = (idx >= "2021-01-01") & (idx <= "2021-01-20")
    swe[rise] = np.linspace(0.5, 5.0, int(rise.sum()))
    sca[rise] = np.linspace(0.2, 1.0, int(rise.sum()))
    melt = (idx > "2021-01-20") & (idx <= "2021-03-10")
    swe[melt] = np.linspace(5.0, 0.0, int(melt.sum()))
    sca[melt] = np.linspace(1.0, 0.0, int(melt.sum()))
    # Larger late-year accumulation (never melts in-frame) -> would be the
    # calendar-year argmax peak, but belongs to WY2022.
    accum = idx >= "2021-11-01"
    swe[accum] = np.linspace(0.5, 10.0, int(accum.sum()))
    sca[accum] = np.linspace(0.1, 1.0, int(accum.sum()))
    frame = pd.DataFrame({"swe": swe, "sca": sca})

    seasons = _seasons(frame)
    assert len(seasons) == 1          # the real Jan-Mar melt (WY2021), not the Dec spike
    assert seasons[0][0] == 1.0       # normalized SDC starts at 1.0
