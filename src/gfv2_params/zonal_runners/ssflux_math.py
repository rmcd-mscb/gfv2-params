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

# mean_slope_fraction is tan(slope), so it is unbounded above; 320 CONUS HRUs
# are exactly 0 (measured from the slope INPUT, nhm_slope_params.csv,
# 361,471 rows -- not the stale ssflux output, which undercounts at 317
# because it has only 361,394 rows; re-measure from nhm_slope_params.csv
# rather than trusting this number, per CLAUDE.md's re-measure convention)
# and 3 exceed 1 (up to 66.85 deg), where log10(slope) and log10(1 - slope)
# are undefined. Clamp rather than drop: these are real HRUs that still need
# a parameter value.
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
    median_tol: float = 0.1,
    band_lo: float = 0.5,
    band_hi: float = 2.0,
    magnitude_bad_fraction: float = 0.05,
) -> None:
    """Assert the weights behave like gdptools' intensive ``wght``.

    ``wght`` sums to ~1.0 per target for a spatially continuous source layer.
    Real CONUS data is close but not exact: 1,828 HRUs have coverage gaps and 98
    have overlapping source polygons, so a handful outside ``[1-tol, 1+tol]`` is
    only warned about -- and gfv2 batches are spatially contiguous, so those
    gaps CLUSTER: 1,828 known gap HRUs inside one ~5,650-HRU batch is 32%,
    comfortably over a tail-fraction threshold like 5%, even though every
    individual sum is a legitimate renormalised gap, not an extensive-form
    signature. A tail-COUNT threshold therefore cannot distinguish "the wrong
    column is being summed" from "this batch happens to contain a coverage-gap
    cluster" -- see #175 (median 0.022 across the extensive form vs a clustered
    per-batch tail that is a red herring).

    The **median** is the strongest signal: the extensive construction that
    caused #175 displaced the median itself (0.022 vs 1.0), because it scales
    with source-polygon size, not target coverage. Raise when the median
    strays from 1.0 by more than ``median_tol``.

    The median check alone has a blind spot, though: it only reflects the
    MAJORITY of HRUs. A frame with 60% of sums at 1.0 and 40% at 0.05 (the
    exact extensive-form signature on a minority of rows) still has a median
    of 1.0 -- up to just under 50% of HRUs could carry the bug completely
    undetected by the median alone. So a second check covers the minority
    case, keyed on MAGNITUDE rather than a bare tail count.

    An earlier version of this check used a tail-COUNT ceiling
    (``hard_bad_fraction``, default 0.35) sitting between the documented ~32%
    legitimate clustered-gap worst case and an illustrative 40% counterexample
    -- three points of margin that is not margin, and the 32% figure was not
    even reproducible from the config (gfv2 ``batch_size`` is 10000, so the
    1,828 known gap HRUs are ~18% of a batch, not 32%). Measured against the
    real CONUS weights file (``shared/conus/weights/lith_weights_gfv2.csv``,
    361,394 HRUs) against the extensive-form construction #175's bug produced::

        per-HRU weight sum      REAL (intensive)   EXTENSIVE (#175 bug)
        outside [0.5, 2.0]      0.111% (402 HRUs)   85.4%
        below 0.1               0.071% (255)        63.1%
        median                  1.0000              0.02215

    So "fraction of HRUs whose weight sum falls outside [``band_lo``,
    ``band_hi``] = [0.5, 2.0]" separates real data from the bug by ~770x,
    versus the tail-count rule's ~1.1x margin. Real CONUS data sits at
    0.111%, so the default ``magnitude_bad_fraction`` of 0.05 (5%) carries
    ~45x margin over real data -- and, unlike the count rule, it also catches
    a PARTIAL extensive-form regression affecting as little as ~6% of HRUs;
    the old count rule needed 35% of HRUs to be affected before it would even
    consider raising. Raised as a probable partial extensive-form regression,
    not merely logged.

    Between ``max_bad_fraction`` (the ``[1-tol, 1+tol]`` tail, a soft
    coverage-gap/overlap signal) and the magnitude band raise, the caller only
    gets a warning -- the median being near 1.0 does not, on its own, prove
    the extensive form is absent for a large minority of HRUs, so that warning
    must not claim it does.
    """
    if weight_col not in weights.columns:
        raise ValueError(
            f"Weight frame is missing '{weight_col}', gdptools' intensive weight. "
            f"Columns: {sorted(weights.columns)}."
        )
    sums = weights.groupby(id_feature)[weight_col].sum()
    median = float(sums.median()) if len(sums) else float("nan")
    if len(sums) and not np.isclose(median, 1.0, rtol=0.0, atol=median_tol):
        raise ValueError(
            f"median(sum({weight_col})) = {median:.4f}, more than {median_tol} "
            "away from 1.0. This is the extensive-form signature: 'wght' sums "
            "to ~1.0 per target for gdptools' intensive weight, but scales with "
            f"source-polygon size under the extensive form. '{weight_col}' must "
            "be gdptools' `wght` -- do not substitute area_weight / "
            "<source>_area, which is issue #175's root cause."
        )

    bad = ~np.isclose(sums.to_numpy(), 1.0, rtol=0.0, atol=tol)
    bad_fraction = float(bad.mean()) if len(sums) else 0.0

    # Magnitude discriminator (replaces the old tail-COUNT hard_bad_fraction
    # ceiling -- see the docstring for the measured real-vs-extensive table
    # this threshold rests on). Keyed on how far outside a plausible band the
    # sum falls, not merely how many HRUs are outside a tight tolerance, so a
    # cluster of mildly-off legitimate coverage gaps (sums like 0.6-0.95, well
    # inside [band_lo, band_hi]) cannot trip it no matter how large the
    # cluster, while a minority of HRUs at extensive-form magnitude
    # (sums ~0.02-0.05, far outside the band) trips it even at a small
    # fraction.
    out_of_band = (sums.to_numpy() < band_lo) | (sums.to_numpy() > band_hi)
    magnitude_fraction = float(out_of_band.mean()) if len(sums) else 0.0
    if magnitude_fraction > magnitude_bad_fraction:
        raise ValueError(
            f"{magnitude_fraction:.1%} of HRUs have sum({weight_col}) outside "
            f"the plausible band [{band_lo}, {band_hi}] (median {median:.4f}), "
            f"above the ceiling of {magnitude_bad_fraction:.0%}. Real CONUS "
            "data measures 0.111% of HRUs outside this band (402 of 361,394), "
            "vs. 85.4% under the #175 extensive-form bug -- a ~770x "
            "separation. A fraction this large -- even with a median near "
            "1.0, which only rules out a WHOLESALE extensive-form regression "
            "-- is treated as a probable PARTIAL extensive-form regression "
            f"affecting a minority of HRUs. '{weight_col}' must be gdptools' "
            "intensive `wght` -- do not substitute area_weight / "
            "<source>_area."
        )
    if bad_fraction > max_bad_fraction:
        logger.warning(
            "%.1f%% of HRUs have sum(%s) outside 1.0 +/- %s (median %.4f, "
            "min %.4g, max %.4g). The median is close to 1.0, which only rules "
            "out a wholesale extensive-form regression -- it does NOT prove the "
            "extensive form is absent for this minority. This is consistent "
            "with EITHER a clustered batch of legitimate lithology coverage "
            "gaps/overlaps (which the aggregation renormalises by the per-HRU "
            "sum) OR a partial extensive-form regression affecting a minority "
            "of HRUs; it warrants checking.",
            bad_fraction * 100, weight_col, tol, median,
            float(sums.min()), float(sums.max()),
        )
    elif bad.any():
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
    all_ids=None,
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

    ``all_ids`` (optional): the full target HRU id list (e.g. a batch's
    ``target_gdf[id_feature]``). When supplied, the output index is built from
    it instead of from the weight frame's own unique ids -- an HRU with ZERO
    weight rows (no lithology-polygon overlap at all) still gets a row, with
    NaN ``k_perm_log_wtd`` and ``fflux == FFLUX_NO_OVERLAP`` -- see issue #209.
    Without it (the default), behaviour is unchanged from before #209: the
    output index is only the ids that appear in ``weights``, so a
    zero-coverage HRU is silently absent from the result.

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

    if all_ids is None:
        target_ids = pd.Index(w[id_feature].unique(), name=id_feature).sort_values()
    else:
        target_ids = pd.Index(
            pd.unique(pd.Index(all_ids)), name=id_feature
        ).sort_values()
    v = w[valid]
    num = (v[k_col] * v[weight_col]).groupby(v[id_feature]).sum().reindex(target_ids)
    den = v[weight_col].groupby(v[id_feature]).sum().reindex(target_ids)

    den_arr = den.to_numpy()
    covered = np.isfinite(den_arr) & (den_arr > 0.0)

    k_log = np.full(len(target_ids), np.nan)
    np.divide(num.to_numpy(), den_arr, out=k_log, where=covered)

    fflux = np.where(covered, den_arr, FFLUX_NO_OVERLAP)

    return pd.DataFrame(
        {id_feature: target_ids.to_numpy(), "k_perm_log_wtd": k_log, "fflux": fflux}
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
    if not hi > lo:
        raise ValueError(
            f"Target range is not valid: hi={hi!r} must be strictly greater "
            f"than lo={lo!r}. A transposed min/max in the flux_params config "
            "would silently emit an inverted field otherwise."
        )

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
