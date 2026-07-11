import numpy as np
import pandas as pd

from gfv2_params.dprst_depth.fill import DEPTH_CAP_M, M_TO_IN, fill_flat, fit_ecoregion_models


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


def test_fill_flat_fallback_ladder_ftype_only_rung():
    # The flat row's own (eco, FTYPE) group has NO donors, and its ecoregion
    # ("80") has no donors under ANY FTYPE either -> rungs 1 and 2 both miss.
    # A different ecoregion ("17") does have donors under the SAME FTYPE
    # ("LakePond") -> rung 3 (FTYPE-only, all ecoregions pooled) must supply
    # the fill, exercising the ladder's 3rd rung in isolation.
    df = pd.DataFrame({
        "ftype": ["LakePond", "LakePond", "LakePond"],
        "ecoregion": ["17", "17", "80"],
        "dprst_depth_m": [1.0, 3.0, np.nan],
        "hollister_max_m": [1.0, 3.0, np.nan],
        "flat": [False, False, True],
    })
    models = fit_ecoregion_models(df[~df.flat])
    out = fill_flat(df, models, floor_in=49.0)
    row = out.iloc[2]
    assert row["method"] == "regional_fill"
    assert np.isfinite(row["dprst_depth_m"])
    assert row["dprst_depth_m"] > 0
    assert np.isclose(row["dprst_depth_m"], 2.0)  # median(1.0, 3.0) via FTYPE-only fallback


def test_fill_flat_caps_unphysical_measured_depth_at_300in():
    # #173 FIX 1 (Oregon validation Risk 1): a measured depth equivalent to
    # 500 in (a depth_to_spill artifact on a high-pour-point polygon) must be
    # clamped to the NHM calibrated max of 300 in exactly, and relabeled
    # "measured_capped" so provenance records the clamp. A normal, in-range
    # measured depth must be untouched.
    over_cap_m = 500.0 / M_TO_IN
    normal_m = 5.0 / M_TO_IN
    df = pd.DataFrame({
        "COMID": [1, 2],
        "ftype": ["LakePond"] * 2,
        "ecoregion": ["17"] * 2,
        "dprst_depth_m": [over_cap_m, normal_m],
        "hollister_max_m": [4.0, 1.0],
        "flat": [False, False],
    })
    models = fit_ecoregion_models(df[~df.flat])
    out = fill_flat(df, models, floor_in=49.0)

    capped = out.set_index("COMID").loc[1]
    assert capped["method"] == "measured_capped"
    assert np.isclose(capped["dprst_depth_m"], DEPTH_CAP_M)
    assert np.isclose(capped["dprst_depth_m"] * M_TO_IN, 300.0)

    normal = out.set_index("COMID").loc[2]
    assert normal["method"] == "measured"
    assert np.isclose(normal["dprst_depth_m"], normal_m)


def test_fill_flat_nan_measured_non_flat_uses_regional_ladder_not_floor():
    # #173 FIX 2 (Oregon validation Risk 2): a NON-flat polygon whose
    # depth_to_spill read failed (NaN dprst_depth_m) must be routed through
    # the SAME regional-median fallback ladder as a flat row when its
    # (ecoregion, FTYPE) group has measured donors -- NOT straight to the
    # defensive constant_floor.
    df = pd.DataFrame({
        "COMID": [1, 2, 3],
        "ftype": ["LakePond"] * 3,
        "ecoregion": ["17"] * 3,
        "dprst_depth_m": [1.0, 3.0, np.nan],
        "hollister_max_m": [1.0, 3.0, np.nan],
        "flat": [False, False, False],  # row 3 is NOT flat -- a read failure
    })
    donors = df[(df["flat"] == False) & df["dprst_depth_m"].notna()]  # noqa: E712
    models = fit_ecoregion_models(donors)
    out = fill_flat(df, models, floor_in=49.0)

    row = out.set_index("COMID").loc[3]
    assert row["method"] == "regional_fill"
    assert np.isclose(row["dprst_depth_m"], 2.0)  # median(1.0, 3.0)


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
