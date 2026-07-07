"""Stage 3: CV/lognormal snarea_curve library builder.

The curve SHAPE is a single physical parameter — the sub-grid SWE coefficient of
variation (CV) — via a lognormal SWE pdf (Sexstone et al. 2020, eqs 3-5; Liston
2004). The dimensionless snow-depletion curve depends only on CV. Repo curve
order is DESCENDING (SWE/thresh 1.0 -> 0.0); the PRMS NetCDF is ascending (see
_to_prms_order).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from scipy.stats import norm

_MM_PER_INCH = 25.4

# Descending, matching snarea/season.py SWE_LEVELS.
SWE_LEVELS = np.round(np.arange(1.0, -1e-4, -0.1), 1)  # 1.0 .. 0.0, 11 values

# Curve column names for snarea_curve_0..10 (descending).
CURVE_COLS = [f"snarea_curve_{i}" for i in range(11)]

# CV search grid: 0.05..3.0 step 0.05 covers the validated range (median ~0.45,
# up to ~1.2 CONUS) with headroom.
CV_GRID = np.round(np.arange(0.05, 3.0001, 0.05), 2)
_INTERIOR = slice(1, 10)  # endpoints (0, 10) are fixed 1.0/0.0 for every cv


def sdc_from_cv(cv: float, mu: float = 1.0, n: int = 4000) -> np.ndarray:
    """11-point dimensionless SDC for a lognormal SWE pdf with coeff-of-var ``cv``.

    Sexstone eqs 1-5 under uniform melt: SCA(M)=P(S>M); SWE(M)=E[(S-M)^+]. The
    dimensionless curve (SCA vs SWE/peak) depends only on cv. Returns SCA at
    SWE_LEVELS (descending), clipped to [0,1], anchored (1.0 @ SWE=1, 0.0 @ SWE=0).
    """
    z = np.sqrt(np.log(1 + cv * cv))          # ζ² = ln(1+CV²)
    lam = np.log(mu) - 0.5 * z * z            # λ  = ln(μ) − ζ²/2
    M = np.concatenate([[0.0], np.exp(np.linspace(np.log(mu) - 6 * z, np.log(mu) + 6 * z, n))])
    lnM = np.log(np.where(M > 0, M, 1e-300))
    sca = norm.cdf((lam - lnM) / z)           # SCA(M) = Φ((λ−lnM)/ζ)
    swe = mu * norm.cdf((lam + z * z - lnM) / z) - M * sca
    sca[0], swe[0] = 1.0, mu
    o = np.argsort(swe / swe[0])
    x = (swe / swe[0])[o]
    y = sca[o]
    # Ensure the curve spans from (0, 0) to (1, 1) for proper interpolation
    if x[0] > 0:
        x = np.concatenate([[0], x])
        y = np.concatenate([[0], y])
    if x[-1] < 1:
        x = np.concatenate([x, [1]])
        y = np.concatenate([y, [1]])
    return np.clip(np.interp(SWE_LEVELS, x, y, left=1.0, right=0.0), 0, 1)


def _library_matrix(cv_grid: np.ndarray) -> np.ndarray:
    """(len(cv_grid), 11) matrix of analytic curves — built once, reused."""
    return np.vstack([sdc_from_cv(c) for c in cv_grid])


def fit_cv(curve: np.ndarray, cv_grid: np.ndarray | None = None) -> float:
    """Best-fit lognormal CV for an empirical 11-pt curve (min L2 over interior)."""
    grid = CV_GRID if cv_grid is None else cv_grid
    lib = _library_matrix(grid)
    d = np.linalg.norm(lib[:, _INTERIOR] - np.asarray(curve)[_INTERIOR], axis=1)
    return float(grid[int(d.argmin())])


def build_library(
    cv_values: np.ndarray, ndepl_cv: int, default_curve: np.ndarray
) -> pd.DataFrame:
    """Row 1 = reserved default curve; rows 2..(1+ndepl_cv) = exactly ``ndepl_cv``
    equal-population CV bins (guaranteed non-empty via rank-based assignment, so
    ties in ``cv_values`` cannot collapse the bin count), each curve =
    sdc_from_cv(bin median CV). Curves are descending. Raises ValueError if
    fewer than ``ndepl_cv`` finite CV values are available."""
    cv = np.asarray(cv_values, dtype=float)
    cv = cv[np.isfinite(cv)]
    if cv.size == 0:
        raise ValueError("build_library: no finite CV values to bin")
    default_curve = np.asarray(default_curve, dtype=float)
    if default_curve.shape != (11,):
        raise ValueError(f"default_curve must be shape (11,), got {default_curve.shape}")

    rows = [
        {
            "deplcrv_id": 1,
            "curve_kind": "default",
            "cv": np.nan,
            **{c: float(default_curve[i]) for i, c in enumerate(CURVE_COLS)},
        }
    ]

    # Rank-based equal-population bins: tie-robust, guarantees ndepl_cv non-empty
    # groups (quantile-edges + digitize can silently collapse bins under ties).
    cv_sorted_idx = np.argsort(cv, kind="stable")  # ordinal: ties broken by position
    n = cv.size
    if n < ndepl_cv:
        raise ValueError(
            f"build_library: need at least ndepl_cv ({ndepl_cv}) finite CV values "
            f"to form equal-population bins, got {n}"
        )
    labels = np.empty(n, dtype=int)
    labels[cv_sorted_idx] = (np.arange(n) * ndepl_cv) // n  # 0..ndepl_cv-1, equal population
    medians = [float(np.median(cv[labels == b])) for b in range(ndepl_cv)]  # ascending; each non-empty
    for k, m in enumerate(medians, start=2):
        curve = sdc_from_cv(m)
        rows.append(
            {
                "deplcrv_id": k,
                "curve_kind": "cv_bin",
                "cv": m,
                **{c: float(curve[i]) for i, c in enumerate(CURVE_COLS)},
            }
        )
    return pd.DataFrame(rows)


def snarea_thresh_inches(peak_swe_mm: float) -> float:
    """Per-HRU SWE scale in inches. 0.0 for no-snow / undefined (curve never
    exercised there since pkwater_equiv is 0)."""
    v = float(peak_swe_mm)
    if not np.isfinite(v) or v <= 0.0:
        return 0.0
    return v / _MM_PER_INCH


def assign_deplcrv(cv_assign: np.ndarray, library: pd.DataFrame) -> np.ndarray:
    """Nearest cv_bin curve (by CV) for finite CV; reserved default (id 1) for non-finite.
    The default row is never a nearest-CV candidate."""
    bins = library[library["curve_kind"] == "cv_bin"]
    bin_ids = bins["deplcrv_id"].to_numpy()
    bin_cvs = bins["cv"].to_numpy(dtype=float)
    default_id = int(library[library["curve_kind"] == "default"]["deplcrv_id"].iloc[0])
    cv = np.asarray(cv_assign, dtype=float)
    out = np.full(cv.shape, default_id, dtype=np.int32)
    finite = np.isfinite(cv)
    if finite.any():
        nearest = np.abs(cv[finite][:, None] - bin_cvs[None, :]).argmin(axis=1)
        out[finite] = bin_ids[nearest]
    return out


def _to_prms_order(curve: np.ndarray) -> np.ndarray:
    """Repo descending (SWE 1.0->0.0) -> PRMS ascending frac_swe (0.0->1.0)."""
    return np.asarray(curve)[::-1]


def _recon_error(cv: np.ndarray, emp_curves: np.ndarray) -> tuple[float, float]:
    """Mean and p95 abs-SCA error of sdc_from_cv(cv) vs emp_curves (rows aligned)."""
    ok = np.isfinite(cv) & np.isfinite(emp_curves).all(axis=1)
    if not ok.any():
        return float("nan"), float("nan")
    approx = np.vstack([sdc_from_cv(c) for c in cv[ok]])
    err = np.abs(approx - emp_curves[ok])
    return float(err.mean()), float(np.percentile(err.max(axis=1), 95))


def validate_and_calibrate(
    cv_subgrid: np.ndarray,
    cv_empirical: np.ndarray,
    emp_curves: np.ndarray,
    mode: str = "auto",
    bias_tol: float = 0.1,
) -> tuple[np.ndarray, dict]:
    """Gate + monotone quantile-map calibration for sub-grid CV vs empirical CV.

    Args:
        cv_subgrid: sub-grid coefficient of variation array (may contain NaN).
        cv_empirical: empirical CV values aligned by index (may contain NaN where not derived).
        emp_curves: (n, 11) empirical SDC curves, NaN rows where not derived.
        mode: "auto" (conditional), "on" (force), or "off" (identity).
        bias_tol: threshold for median CV bias to trigger calibration in auto mode.

    Returns:
        (cv_calibrated_for_all_input, report_dict) where report_dict carries distribution
        stats, reconstruction error before/after, and calibrated flag.
    """
    cv_subgrid = np.asarray(cv_subgrid, dtype=float)
    cv_empirical = np.asarray(cv_empirical, dtype=float)
    emp_curves = np.asarray(emp_curves, dtype=float)
    derived = np.isfinite(cv_subgrid) & np.isfinite(cv_empirical)
    sub_d, emp_d = cv_subgrid[derived], cv_empirical[derived]

    report = {
        "n_derived_overlap": int(derived.sum()),
        "cv_subgrid_median": float(np.median(sub_d)) if derived.any() else float("nan"),
        "cv_empirical_median": float(np.median(emp_d)) if derived.any() else float("nan"),
        "calibrated": False,
    }
    report["recon_mean_before"], report["recon_p95_before"] = _recon_error(cv_subgrid, emp_curves)

    bias = (
        abs(report["cv_subgrid_median"] - report["cv_empirical_median"])
        if derived.any()
        else float("nan")
    )
    report["cv_median_bias"] = float(bias)

    cal = cv_subgrid.copy()
    if derived.sum() >= 2 and (mode == "on" or (mode == "auto" and bias > bias_tol)):
        # monotone quantile map trained on the derived overlap
        qs = np.linspace(0, 1, 101)
        x = np.quantile(sub_d, qs)
        y = np.quantile(emp_d, qs)
        x, idx = np.unique(x, return_index=True)  # strictly increasing x for interp
        y = y[idx]
        finite = np.isfinite(cal)
        cal[finite] = np.interp(cal[finite], x, y)
        report["calibrated"] = True
    elif mode not in ("auto", "on", "off"):
        raise ValueError(f"calibrate mode must be auto|on|off, got {mode!r}")

    report["recon_mean_after"], report["recon_p95_after"] = _recon_error(cal, emp_curves)
    return cal, report


_PARAM_DIAG_COLS = ["sdc_status", "sca_class", "similarity", "n_seasons", "n_peak_years", "peak_swe_mm"]


def assemble_params(derived, id_feature, cv_assign, cv_source, deplcrv, library):
    """One row per HRU: index + snarea_thresh + CVs + diagnostics + the ASSIGNED
    library curve (descending, for QA / per-HRU detail — no separate 1:1 mode)."""
    curve_by_id = {int(r.deplcrv_id): r[CURVE_COLS].to_numpy(float) for _, r in library.iterrows()}
    assigned = np.vstack([curve_by_id[int(d)] for d in deplcrv])
    out = pd.DataFrame({
        id_feature: derived[id_feature].to_numpy(),
        "hru_deplcrv": np.asarray(deplcrv, dtype=np.int32),
        "snarea_thresh": [snarea_thresh_inches(v) for v in derived["peak_swe_mm"].to_numpy()],
        "cv_assign": np.asarray(cv_assign, dtype=float),
        "cv_subgrid": derived["cv_subgrid"].to_numpy(float),
        "cv_empirical": derived["cv_empirical"].to_numpy(float),
        "cv_source": np.asarray(cv_source, dtype=object),
    })
    for c in _PARAM_DIAG_COLS:
        out[c] = derived[c].to_numpy()
    for i, c in enumerate(CURVE_COLS):
        out[c] = assigned[:, i]
    return out


def write_library_csv(library: pd.DataFrame, path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    library.to_csv(path, index=False)


def write_params_csv(params: pd.DataFrame, path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    params.to_csv(path, index=False)


def write_validation_csv(report: dict, path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([report]).to_csv(path, index=False)


def write_prms_netcdf(library: pd.DataFrame, params: pd.DataFrame, id_feature: str, path) -> None:
    """PRMS/pyWatershed param file: snarea_curve flat ASCENDING (ndeplval=11*ndepl),
    hru_deplcrv, snarea_thresh, all on nhru.

    Convention verified against ``pywatershed.hydrology.prms_snow.PRMSSnow`` (Task 8
    step 1): ``snarea_curve_2d = np.reshape(snarea_curve_flat, (ndepl, 11))`` (row-major),
    row selected via ``snarea_curve_2d[hru_deplcrv - 1, :]`` (hru_deplcrv is 1-indexed, so
    flat-array row order must follow ascending deplcrv_id starting at row 0 = id 1).
    Within a row, tracing ``_calc_sca_deplcrv``: frac_swe=0.0 -> curve[0], frac_swe=0.5 ->
    curve[5], frac_swe=1.0 -> curve[10] (also asserted "the maximum" at
    ``snarea_curve[11 - 1]`` on new-snow reset) -> ascending frac_swe, frac 0.0 at index 0.
    This matches ``_to_prms_order`` (repo descending -> ascending reverse).
    """
    lib_sorted = library.sort_values("deplcrv_id")
    ndepl = len(lib_sorted)
    flat = np.concatenate(
        [_to_prms_order(r[CURVE_COLS].to_numpy(float)) for _, r in lib_sorted.iterrows()]
    )
    ds = xr.Dataset(
        data_vars={
            "snarea_curve": (
                "ndeplval",
                flat.astype("float64"),
                {
                    # long_name/units match pywatershed's canonical
                    # parameters.yaml entry for snarea_curve.
                    "long_name": "Snow area depletion curve values",
                    "units": "1",
                    "description": (
                        "Flat, ascending frac_swe within each 11-point curve "
                        "(index 0 = frac_swe 0.0, index 10 = frac_swe 1.0); curves "
                        "concatenated in ascending deplcrv_id order"
                    ),
                },
            ),
            "hru_deplcrv": (
                "nhru",
                params["hru_deplcrv"].to_numpy(np.int32),
                {
                    # long_name matches pywatershed's canonical parameters.yaml
                    # entry for hru_deplcrv; units "1" per CF (dimensionless index).
                    "long_name": "Index number for snowpack areal depletion curve",
                    "units": "1",
                },
            ),
            "snarea_thresh": (
                "nhru",
                params["snarea_thresh"].to_numpy("float64"),
                {
                    # long_name/units match pywatershed's canonical
                    # parameters.yaml entry for snarea_thresh ("inches", plural).
                    "long_name": "Maximum threshold water equivalent for snow depletion",
                    "units": "inches",
                },
            ),
        },
        coords={
            id_feature: (
                "nhru",
                params[id_feature].to_numpy(),
                {"long_name": "HRU identifier"},
            )
        },
        attrs={
            "ndepl": ndepl,
            "Description": "SNODAS-derived CV/lognormal snarea_curve library",
            "Conventions": "CF-1.6",
        },
    )
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path)
