import numpy as np
import pandas as pd

from gfv2_params.dprst_depth.fill import fill_flat, fit_ecoregion_models


def test_fill_flat_uses_group_median_and_floor():
    df = pd.DataFrame({
        "COMID": [1, 2, 3, 4], "ftype": ["LakePond"] * 4, "ecoregion": ["17"] * 4,
        "dprst_depth_m": [1.0, 2.0, np.nan, np.nan], "measured_max_m": [1.5, 3.0, np.nan, np.nan],
        "hollister_max_m": [1.2, 2.8, 4.0, 0.1], "flat": [False, False, True, True]})
    models = fit_ecoregion_models(df[~df.flat])
    out = fill_flat(df, models, floor_in=49.0)
    filled = out[out.flat]
    assert filled["dprst_depth_m"].notna().all()          # no NaN left
    assert (filled["dprst_depth_m"] > 0).all()
    # group median of measured non-flat = median(1.0,2.0)=1.5 m unless Hollister won
    assert filled["method"].isin({"regional_fill", "calibrated_hollister", "constant_floor"}).all()


def test_fill_flat_floor_when_no_donors():
    df = pd.DataFrame({"COMID": [1], "ftype": ["Playa"], "ecoregion": ["80"],
        "dprst_depth_m": [np.nan], "measured_max_m": [np.nan], "hollister_max_m": [np.nan], "flat": [True]})
    out = fill_flat(df, fit_ecoregion_models(df[~df.flat]), floor_in=49.0)
    assert out.loc[0, "method"] == "constant_floor"
    assert np.isclose(out.loc[0, "dprst_depth_m"] * 39.3701, 49.0)  # 49 in floor


def test_fit_ecoregion_models_no_nan_across_full_pipeline():
    # A mixed group: some measured donors, a flat row with valid hollister, a
    # degenerate flat row (no hollister at all) — the full fill must still
    # leave zero NaN and all-positive depths.
    df = pd.DataFrame({
        "COMID": [1, 2, 3, 4, 5, 6],
        "ftype": ["LakePond"] * 6,
        "ecoregion": ["17"] * 6,
        "dprst_depth_m": [1.0, 1.4, 1.8, 2.2, 2.6, np.nan],
        "hollister_max_m": [1.0, 2.0, 3.0, 4.0, 5.0, np.nan],
        "flat": [False, False, False, False, False, True],
    })
    models = fit_ecoregion_models(df[~df.flat])
    out = fill_flat(df, models, floor_in=49.0)
    assert out["dprst_depth_m"].notna().all()
    assert (out["dprst_depth_m"] > 0).all()


def test_fit_ecoregion_models_calibrated_hollister_wins_when_linear():
    # Strongly linear, low-noise mean ~ hollister_max relationship: the
    # calibrated slope should generalize far better than the group median,
    # so cross-validated RMSE must pick "calibrated_hollister".
    rng = np.random.default_rng(0)
    n = 40
    x = np.linspace(1.0, 10.0, n)
    slope_true = 0.6
    y = slope_true * x + rng.normal(scale=0.02, size=n)
    df = pd.DataFrame({
        "ecoregion": ["17"] * n,
        "ftype": ["LakePond"] * n,
        "dprst_depth_m": y,
        "hollister_max_m": x,
        "flat": [False] * n,
    })
    models = fit_ecoregion_models(df, n_min=5)
    model = models[("17", "LakePond")]
    assert model.kind == "calibrated_hollister"
    assert model.cv_rmse_hollister < model.cv_rmse_median


def test_fit_ecoregion_models_median_wins_when_not_linear():
    # hollister_max is uncorrelated with the (near-constant) measured depth:
    # a through-origin linear fit should NOT generalize better than the
    # median, so cross-validated RMSE must pick "median".
    rng = np.random.default_rng(1)
    n = 40
    x = rng.uniform(1.0, 10.0, size=n)
    y = 2.0 + rng.normal(scale=0.05, size=n)
    df = pd.DataFrame({
        "ecoregion": ["21"] * n,
        "ftype": ["LakePond"] * n,
        "dprst_depth_m": y,
        "hollister_max_m": x,
        "flat": [False] * n,
    })
    models = fit_ecoregion_models(df, n_min=5)
    model = models[("21", "LakePond")]
    assert model.kind == "median"


def test_fill_flat_fallback_ladder_ecoregion_then_ftype():
    # No (eco,ftype) donors for the flat row's own group, but the SAME
    # ecoregion has donors under a different FTYPE -> eco-only median.
    df = pd.DataFrame({
        "ftype": ["SwampMarsh", "SwampMarsh", "Playa"],
        "ecoregion": ["17", "17", "17"],
        "dprst_depth_m": [1.0, 3.0, np.nan],
        "hollister_max_m": [1.0, 3.0, np.nan],
        "flat": [False, False, True],
    })
    models = fit_ecoregion_models(df[~df.flat])
    out = fill_flat(df, models, floor_in=49.0)
    row = out.iloc[2]
    assert row["method"] == "regional_fill"
    assert np.isclose(row["dprst_depth_m"], 2.0)  # median(1.0, 3.0) via ecoregion-only fallback
