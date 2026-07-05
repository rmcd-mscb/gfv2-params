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
