"""Pure numerical helpers for the ssflux derivation.

Kept separate from ``ssflux.py`` so the maths is unit-testable without geo
fixtures, a data root, or SLURM. See
``docs/superpowers/specs/2026-08-10-ssflux-normalisation-design.md``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Gleeson k_perm is log10 permeability, so 0 would mean 1 m^2 -- physically
# impossible. It is the dataset's no-data flag (9,409 of 202,108 CONUS
# polygons). This is a STORED flag, not a computed value, so exact equality is
# correct here; a tolerance would risk swallowing a genuine measurement. Note
# -16.48 is NOT a sentinel -- it is the real least-permeable lithology class
# (26,441 polygons), which is why no-data must be excluded rather than floored.
K_PERM_NODATA = 0.0

# fflux value for an HRU with no valid lithology overlap at all, matching the
# reference implementation (Viger 2014, doi:10.5066/F7CN71XR).
FFLUX_NO_OVERLAP = -1.0

# mean_slope_fraction is tan(slope), so it is unbounded above; 317 CONUS HRUs
# are exactly 0 and 3 exceed 1 (up to 66.85 deg), where log10(slope) and
# log10(1 - slope) are undefined. Clamp rather than drop: these are real HRUs
# that still need a parameter value.
SLOPE_FLOOR = 1e-4
SLOPE_CEIL = 1.0 - 1e-4

# Below this width the input range carries no information and min-max
# normalisation would divide by ~0.
RANGE_ATOL = 1e-12

PARAM_NAMES = (
    "soil2gw_max",
    "ssr2gw_rate",
    "fastcoef_lin",
    "slowcoef_lin",
    "gwflow_coef",
    "dprst_seep_rate_open",
    "dprst_flow_coef",
)


def validate_weight_coverage(
    weights: pd.DataFrame,
    id_feature: str,
    logger,
    *,
    weight_col: str = "normalized_area_weight",
    tol: float = 0.01,
    max_bad_fraction: float = 0.05,
) -> None:
    """Assert the weights behave like gdptools' intensive ``wght``.

    ``wght`` sums to ~1.0 per target for a spatially continuous source layer.
    Real CONUS data is close but not exact: 1,828 HRUs have coverage gaps and 98
    have overlapping source polygons, so a handful outside ``[1-tol, 1+tol]`` is
    expected and only warned about.

    A *wholesale* deviation means the wrong column is being summed -- the
    extensive construction that caused #175 gave per-HRU sums spanning 3.6e13x
    with a median of 0.022. That raises. This is the guard that stops the
    extensive/intensive confusion from silently returning.
    """
    if weight_col not in weights.columns:
        raise ValueError(
            f"Weight frame is missing '{weight_col}', gdptools' intensive weight. "
            f"Columns: {sorted(weights.columns)}."
        )
    sums = weights.groupby(id_feature)[weight_col].sum()
    bad = ~np.isclose(sums.to_numpy(), 1.0, rtol=0.0, atol=tol)
    bad_fraction = float(bad.mean()) if len(sums) else 0.0
    if bad_fraction > max_bad_fraction:
        raise ValueError(
            f"{bad_fraction:.1%} of HRUs have sum({weight_col}) outside "
            f"1.0 +/- {tol} (median {float(sums.median()):.4f}, "
            f"min {float(sums.min()):.4g}, max {float(sums.max()):.4g}). "
            f"'{weight_col}' must be gdptools' `wght`, which sums to ~1 per "
            "target. A median far from 1 means an EXTENSIVE construction such as "
            "area_weight / <source>_area is being used -- that is issue #175."
        )
    if bad.any():
        logger.warning(
            "%d of %d HRUs have sum(%s) outside 1.0 +/- %s (min %.4g, max %.4g) -- "
            "lithology coverage gaps and overlapping source polygons; the "
            "aggregation renormalises by the per-HRU sum, so these are handled.",
            int(bad.sum()), len(sums), weight_col, tol,
            float(sums.min()), float(sums.max()),
        )


def aggregate_k_perm_log(
    weights: pd.DataFrame,
    id_feature: str,
    *,
    weight_col: str = "normalized_area_weight",
    k_col: str = "k_perm",
) -> pd.DataFrame:
    """Area-weighted mean of log10 permeability -- gdptools' INTENSIVE form.

    Permeability is intensive: it does not add up when regions combine, so the
    aggregation is ``(sum v_i a_i) / (sum a_i)``, NOT the extensive
    ``sum V_i (a_i / A_i)``. gdptools labels the source-polygon area column
    "for extensive variables"; using it here was the root cause of #175.

    ``weight_col`` is gdptools' ``wght`` (shipped as ``normalized_area_weight``),
    the proportional area of the source polygon within the target.

    Dividing by ``den`` is load-bearing beyond no-data exclusion: the lithology
    layer is not perfectly continuous (1,828 CONUS HRUs have coverage gaps, 98
    have overlapping source polygons).

    Returns one row per HRU with ``k_perm_log_wtd`` (NaN where no valid
    lithology) and ``fflux`` (valid-coverage fraction, ``FFLUX_NO_OVERLAP``
    where none).
    """
    for col in (id_feature, k_col, weight_col):
        if col not in weights.columns:
            raise ValueError(
                f"Weight frame is missing '{col}'. Columns: {sorted(weights.columns)}. "
                f"'{weight_col}' is gdptools' intensive weight and is required -- do "
                f"not substitute area_weight / <source>_area, which is the extensive form."
            )

    w = weights[[id_feature, k_col, weight_col]].copy()
    w[k_col] = w[k_col].astype(float)
    w[weight_col] = w[weight_col].astype(float)

    # Exact comparison is deliberate: a stored no-data flag, not a computed value.
    valid = w[k_col].notna() & (w[k_col] != K_PERM_NODATA)

    all_ids = pd.Index(w[id_feature].unique(), name=id_feature).sort_values()
    v = w[valid]
    num = (v[k_col] * v[weight_col]).groupby(v[id_feature]).sum().reindex(all_ids)
    den = v[weight_col].groupby(v[id_feature]).sum().reindex(all_ids)

    den_arr = den.to_numpy()
    covered = np.isfinite(den_arr) & (den_arr > 0.0)

    k_log = np.full(len(all_ids), np.nan)
    np.divide(num.to_numpy(), den_arr, out=k_log, where=covered)

    fflux = np.where(covered, den_arr, FFLUX_NO_OVERLAP)

    return pd.DataFrame(
        {id_feature: all_ids.to_numpy(), "k_perm_log_wtd": k_log, "fflux": fflux}
    )


def derive_log_params(
    k_perm_log_wtd: np.ndarray,
    slope_fraction: np.ndarray,
    hru_area: np.ndarray,
) -> dict[str, np.ndarray]:
    """TM 6-B9's structure, expressed additively in log10 space.

    Because ``k_perm_log_wtd`` is already log10, a product becomes a sum and the
    cube becomes a factor of 3 on the exponent::

        L(soil2gw_max)  = 3*k
        L(ssr2gw_rate)  =   k + log10(1 - slope)
        L(slowcoef_lin) =   k + log10(slope) - log10(hru_area)

    The cube is on ``soil2gw_max`` only, matching the reference implementation's
    FGDC attribute definitions (Viger 2014) and today's code -- TM 6-B9 line 790
    states a cube for the other two, but the 2014 source metadata does not. It
    makes no difference to output anyway: for ``soil2gw_max`` the x3 is a pure
    scale on the exponent and is fully absorbed by the later min-max
    interpolation.

    NaN in ``k_perm_log_wtd`` propagates to NaN, which is the signal for the
    downstream KNN gap-fill.
    """
    k = np.asarray(k_perm_log_wtd, dtype=float)
    slope = np.asarray(slope_fraction, dtype=float)
    area = np.asarray(hru_area, dtype=float)

    if np.any(~np.isfinite(area) | (area <= 0.0)):
        bad = int(np.sum(~np.isfinite(area) | (area <= 0.0)))
        raise ValueError(
            f"hru_area must be finite and > 0 for log10; {bad} row(s) violate this. "
            "A non-positive area means the fabric geometry is degenerate."
        )

    slope_c = np.clip(slope, SLOPE_FLOOR, SLOPE_CEIL)

    out: dict[str, np.ndarray] = {}
    out["soil2gw_max"] = 3.0 * k
    out["ssr2gw_rate"] = k + np.log10(1.0 - slope_c)
    out["slowcoef_lin"] = k + np.log10(slope_c) - np.log10(area)
    out["fastcoef_lin"] = out["slowcoef_lin"] + np.log10(2.0)
    out["gwflow_coef"] = out["slowcoef_lin"]
    out["dprst_seep_rate_open"] = out["ssr2gw_rate"]
    out["dprst_flow_coef"] = out["fastcoef_lin"]
    return out


def interpolate_to_range(values: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """Linearly scale ``values`` onto ``[lo, hi]`` -- TM 6-B9's interpolation.

    NaN is preserved in place (it marks an HRU for the KNN gap-fill) and is
    excluded from the min/max. ``min``/``max`` are exact and order-independent,
    which is what makes the merged output invariant to batch partitioning.
    """
    v = np.asarray(values, dtype=float)
    finite = np.isfinite(v)
    if not finite.any():
        raise ValueError("No finite values to interpolate; cannot derive a range.")

    lo_in = float(np.min(v[finite]))
    hi_in = float(np.max(v[finite]))
    span = hi_in - lo_in
    if span <= RANGE_ATOL:
        raise ValueError(
            f"Input range is degenerate (min={lo_in!r}, max={hi_in!r}, "
            f"span={span!r} <= {RANGE_ATOL}). Refusing to emit a constant field; "
            "this means every HRU shares one value, which is the #175 failure mode."
        )

    out = np.full(v.shape, np.nan)
    np.divide(v - lo_in, span, out=out, where=finite)
    return out * (hi - lo) + lo
