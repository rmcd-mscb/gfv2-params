import numpy as np
import pandas as pd
import pytest
import xarray as xr

from gfv2_params.snarea.library import (
    _INTERIOR,
    CURVE_COLS,
    CV_GRID,
    SWE_LEVELS,
    _to_prms_order,
    assemble_params,
    assign_deplcrv,
    build_from_derived,
    build_library,
    fit_cv,
    sdc_from_cv,
    snarea_thresh_inches,
    validate_and_calibrate,
    write_library_csv,
    write_prms_netcdf,
)


def test_swe_levels_descending_11pt():
    assert SWE_LEVELS.shape == (11,)
    assert SWE_LEVELS[0] == 1.0 and SWE_LEVELS[-1] == 0.0
    assert np.all(np.diff(SWE_LEVELS) < 0)


def test_sdc_from_cv_shape_and_endpoints():
    c = sdc_from_cv(0.5)
    assert c.shape == (11,)
    assert c[0] == pytest.approx(1.0)
    assert c[-1] == pytest.approx(0.0)


def test_sdc_from_cv_monotone_nonincreasing():
    c = sdc_from_cv(0.7)
    assert np.all(np.diff(c) <= 1e-9)


def test_sdc_from_cv_higher_cv_steeper():
    # larger CV -> lower SCA at mid SWE (index 5 = SWE 0.5)
    assert sdc_from_cv(1.5)[5] < sdc_from_cv(0.5)[5] < sdc_from_cv(0.1)[5]


def test_fit_cv_recovers_known_cv():
    for true_cv in (0.32, 0.7, 1.2):
        curve = sdc_from_cv(true_cv)
        # fit is on the CV grid; recovered value within one grid step
        assert abs(fit_cv(curve) - true_cv) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9


def test_fit_cv_is_interior_driven():
    # Assert the interior-only invariant directly
    assert _INTERIOR == slice(1, 10)

    # Assert that interior shape drives the fit: differing interiors yield different fitted CVs
    cv_low = 0.4
    cv_high = 1.0
    curve_low = sdc_from_cv(cv_low)
    curve_high = sdc_from_cv(cv_high)

    fitted_low = fit_cv(curve_low)
    fitted_high = fit_cv(curve_high)

    # Both should be recovered within one grid step
    assert abs(fitted_low - cv_low) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9
    assert abs(fitted_high - cv_high) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9

    # They should be distinctly different (not confused by endpoints)
    assert fitted_low < fitted_high


def test_snarea_thresh_mm_to_inches():
    assert snarea_thresh_inches(254.0) == pytest.approx(10.0)
    assert snarea_thresh_inches(0.0) == 0.0
    assert snarea_thresh_inches(float("nan")) == 0.0
    assert snarea_thresh_inches(-5.0) == 0.0


def test_to_prms_order_is_reverse_and_involutive():
    c = sdc_from_cv(0.6)
    p = _to_prms_order(c)
    assert p[0] == pytest.approx(c[-1])   # ascending: SCA@frac0 first
    assert p[-1] == pytest.approx(c[0])
    assert np.allclose(_to_prms_order(p), c)


def test_build_library_default_plus_bins():
    rng = np.linspace(0.2, 1.4, 500)   # spread of CVs
    default = np.linspace(1.0, 0.0, 11)
    lib = build_library(rng, ndepl_cv=8, default_curve=default)
    assert len(lib) == 9                              # 1 default + 8 bins
    assert list(lib["deplcrv_id"]) == list(range(1, 10))
    assert lib.iloc[0]["curve_kind"] == "default"
    assert np.isnan(lib.iloc[0]["cv"])
    np.testing.assert_allclose(lib.iloc[0][CURVE_COLS].to_numpy(float), default)
    assert (lib.iloc[1:]["curve_kind"] == "cv_bin").all()
    # bin median CVs are increasing
    assert np.all(np.diff(lib.iloc[1:]["cv"].to_numpy()) > 0)
    # each cv_bin curve equals sdc_from_cv(its median cv)
    row = lib.iloc[3]
    np.testing.assert_allclose(row[CURVE_COLS].to_numpy(float), sdc_from_cv(row["cv"]), atol=1e-9)


def test_build_library_equal_population_bins():
    cv = np.arange(1000) / 1000.0 + 0.1   # uniform
    lib = build_library(cv, ndepl_cv=5, default_curve=np.linspace(1, 0, 11))
    assert len(lib) == 6


def test_build_library_tied_cvs_still_produce_ndepl_cv_bins():
    # fit_cv snaps CVs to a grid, so heavy ties are realistic; must NOT collapse.
    cv = np.concatenate([np.full(1800, 0.45), np.linspace(0.2, 1.4, 200)])
    lib = build_library(cv, ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    assert len(lib) == 9                                  # 1 default + 8 cv bins, no collapse
    assert (lib.iloc[1:]["curve_kind"] == "cv_bin").all()
    assert list(lib["deplcrv_id"]) == list(range(1, 10))


def test_build_library_raises_when_fewer_cvs_than_bins():
    with pytest.raises(ValueError, match="at least ndepl_cv"):
        build_library(np.array([0.4, 0.5, 0.6]), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))


def test_assign_deplcrv_nearest_bin_and_default():
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    bin_cvs = lib.iloc[1:]["cv"].to_numpy()
    cv_assign = np.array([bin_cvs[0], bin_cvs[-1], np.nan, float(bin_cvs.mean())])
    out = assign_deplcrv(cv_assign, lib)
    assert out[0] == 2                     # nearest first bin
    assert out[1] == 9                     # nearest last bin
    assert out[2] == 1                     # NaN -> reserved default
    assert 2 <= out[3] <= 9                # some cv_bin, never the default
    assert out.dtype.kind in ("i", "u")


def test_assign_deplcrv_never_returns_default_for_finite_cv():
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    out = assign_deplcrv(np.full(50, 0.45), lib)
    assert (out >= 2).all()


def test_calibration_removes_synthetic_bias():
    rng = np.linspace(0.3, 1.2, 400)
    emp_curves = np.vstack([sdc_from_cv(c) for c in rng])
    cv_empirical = rng.copy()
    cv_subgrid = rng * 0.6  # biased low by construction
    cal, report = validate_and_calibrate(cv_subgrid, cv_empirical, emp_curves, mode="auto", bias_tol=0.05)
    assert report["calibrated"] is True
    # after calibration the median bias vs empirical is much smaller
    assert abs(np.median(cal) - np.median(cv_empirical)) < abs(
        np.median(cv_subgrid) - np.median(cv_empirical)
    )


def test_no_calibration_when_unbiased():
    rng = np.linspace(0.3, 1.2, 400)
    emp_curves = np.vstack([sdc_from_cv(c) for c in rng])
    cal, report = validate_and_calibrate(rng.copy(), rng.copy(), emp_curves, mode="auto", bias_tol=0.1)
    assert report["calibrated"] is False
    np.testing.assert_allclose(cal, rng)


def test_calibration_off_is_identity():
    rng = np.linspace(0.3, 1.2, 50)
    emp = np.vstack([sdc_from_cv(c) for c in rng])
    cal, report = validate_and_calibrate(rng * 0.5, rng, emp, mode="off")
    np.testing.assert_allclose(cal, rng * 0.5)
    assert report["calibrated"] is False


def test_calibration_mode_on_maps_even_when_unbiased():
    rng = np.linspace(0.3, 1.2, 200)
    emp = np.vstack([sdc_from_cv(c) for c in rng])
    cal, report = validate_and_calibrate(rng.copy(), rng.copy(), emp, mode="on")
    assert report["calibrated"] is True   # 'on' maps even with zero bias


def test_calibration_mode_on_graceful_identity_when_no_overlap():
    # cv_empirical all-NaN -> zero overlap; mode='on' must NOT crash, must be identity
    n = 3
    cal, report = validate_and_calibrate(
        np.array([0.3, 0.5, 0.7]), np.full(n, np.nan), np.full((n, 11), np.nan), mode="on")
    assert report["calibrated"] is False
    np.testing.assert_allclose(cal, [0.3, 0.5, 0.7])


def test_calibration_invalid_mode_raises():
    n = 3
    with pytest.raises(ValueError):
        validate_and_calibrate(np.array([0.3, 0.5, 0.7]), np.full(n, np.nan),
                               np.full((n, 11), np.nan), mode="bogus")


def test_assemble_params_columns_and_assigned_curve(tmp_path):
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    derived = pd.DataFrame({
        "nat_hru_id": [10, 11, 12],
        "sdc_status": ["derived", "derived", "default_no_snow"],
        "sca_class": ["high", "mid", "high"],
        "similarity": [0.05, 0.09, np.nan],
        "n_seasons": [12, 8, 0],
        "cv_subgrid": [0.4, 0.9, np.nan],
        "cv_empirical": [0.42, 0.88, np.nan],
        "peak_swe_mm": [254.0, 508.0, 0.0],
        "n_peak_years": [12, 8, 0],
    })
    cv_assign = np.array([0.4, 0.9, np.nan])
    cv_source = np.array(["subgrid", "subgrid", "default_no_snow"])
    deplcrv = assign_deplcrv(cv_assign, lib)
    params = assemble_params(derived, "nat_hru_id", cv_assign, cv_source, deplcrv, lib)
    assert list(params["nat_hru_id"]) == [10, 11, 12]
    for col in ("hru_deplcrv", "snarea_thresh", "cv_assign", "cv_source", *CURVE_COLS):
        assert col in params.columns
    assert params.loc[params.nat_hru_id == 12, "hru_deplcrv"].iloc[0] == 1     # no-snow -> default
    assert params.loc[params.nat_hru_id == 12, "snarea_thresh"].iloc[0] == 0.0
    # assigned curve row matches the library curve for that hru's deplcrv
    r = params.iloc[0]
    librow = lib[lib.deplcrv_id == r["hru_deplcrv"]].iloc[0]
    np.testing.assert_allclose(r[CURVE_COLS].to_numpy(float), librow[CURVE_COLS].to_numpy(float))


def test_write_csvs_roundtrip(tmp_path):
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    p = tmp_path / "lib.csv"
    write_library_csv(lib, p)
    assert pd.read_csv(p).shape[0] == 9


def test_build_from_derived_computes_cv_empirical_end_to_end():
    rng = np.random.default_rng(0)
    n = 300
    cv = np.clip(rng.normal(0.5, 0.2, n), 0.1, 1.5)
    curves = np.vstack([sdc_from_cv(c) for c in cv])   # each HRU's empirical curve
    derived = pd.DataFrame({
        "nat_hru_id": np.arange(1, n + 1),
        "sdc_status": ["derived"] * n, "sca_class": ["high"] * n,
        "similarity": 0.05, "n_seasons": 10,
        "cv_subgrid": cv, "peak_swe_mm": 254.0, "n_peak_years": 10,
        **{c: curves[:, i] for i, c in enumerate(CURVE_COLS)},
    })
    lib, params, report = build_from_derived(derived, "nat_hru_id", 8, np.linspace(1, 0, 11), "auto", 0.1)
    assert len(lib) == 9
    assert len(params) == n
    assert set(params["hru_deplcrv"].unique()) <= set(range(2, 10))    # all estimable -> cv bins
    # cv_empirical was COMPUTED (recovers each true CV within one grid step)
    assert np.nanmax(np.abs(params["cv_empirical"].to_numpy() - cv)) <= 0.05 + 1e-9
    assert report["calibrated"] is False   # cv_subgrid == implied empirical -> unbiased


def test_build_from_derived_no_snow_uses_default():
    derived = pd.DataFrame({
        "nat_hru_id": [1, 2],
        "sdc_status": ["derived", "default_no_snow"],
        "sca_class": ["high", "high"], "similarity": [0.05, np.nan], "n_seasons": [10, 0],
        "cv_subgrid": [0.5, np.nan], "peak_swe_mm": [254.0, 0.0], "n_peak_years": [10, 0],
        **{c: [sdc_from_cv(0.5)[i], np.linspace(1, 0, 11)[i]] for i, c in enumerate(CURVE_COLS)},
    })
    lib, params, report = build_from_derived(derived, "nat_hru_id", 1, np.linspace(1, 0, 11), "off", 0.1)
    assert params.loc[params.nat_hru_id == 2, "hru_deplcrv"].iloc[0] == 1     # no-snow -> default
    assert params.loc[params.nat_hru_id == 2, "snarea_thresh"].iloc[0] == 0.0


def test_write_prms_netcdf_structure_and_ascending_order(tmp_path):
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    params = pd.DataFrame({
        "nat_hru_id": [10, 11, 12],
        "hru_deplcrv": np.array([2, 5, 1], dtype=np.int32),
        "snarea_thresh": [10.0, 20.0, 0.0],
    })
    p = tmp_path / "snarea.nc"
    write_prms_netcdf(lib, params, "nat_hru_id", p)
    ds = xr.open_dataset(p)
    ndepl = len(lib)
    assert ds.sizes["ndeplval"] == 11 * ndepl
    assert ds.sizes["nhru"] == 3
    assert ds["hru_deplcrv"].dtype == np.int32
    # first curve (default, deplcrv_id 1) ascending: index 0 == descending's last (0.0)
    flat = ds["snarea_curve"].values
    first_curve_ascending = flat[:11]
    desc = lib[lib.deplcrv_id == 1][CURVE_COLS].to_numpy(float).ravel()
    np.testing.assert_allclose(first_curve_ascending, desc[::-1])
    assert first_curve_ascending[0] <= first_curve_ascending[-1]   # ascending SCA
