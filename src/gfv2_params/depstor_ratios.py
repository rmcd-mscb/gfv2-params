"""Per-HRU ratio arithmetic for the PRMS depstor Level-5 parameters.

Pulled out of the old `scripts/derive_depstor_ratios.py` so it can be unit
tested and reused by `scripts/derive_depstor_params.py --mode ratios`.

The fraction CSVs feeding `compute_ratio` come from gdptools exactextract:
each row is one HRU with a `count` column = weighted cell count
(sum of partial-pixel coverage fractions x value). Both sides of every ratio
share the same per-HRU coverage convention, so the partial-pixel weighting
cancels and the result matches the integer cell-count ratio used in the
ArcPy reference at docs/0b_TB_depr_stor.py:36-156, 220-312.

Behavioural rules locked by tests/test_derive_depstor_ratios.py:
- denominator == 0 (or NaN -> filled to 0) -> output 0
- numerator or denominator missing for an HRU (outer-join gap) -> filled to 0
- numpy float divide-by-zero (0/0 -> NaN, x/0 -> inf) -> masked to 0
- clamp_to_one=True caps ratios at 1.0 (carea_max, smidx_coef);
  sro_to_dprst_* ratios pass clamp_to_one=False.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_ratio(
    numerator_df: pd.DataFrame,
    denominator_df: pd.DataFrame,
    id_feature: str,
    count_column: str,
    param_name: str,
    clamp_to_one: bool,
) -> tuple[pd.DataFrame, dict]:
    """Per-HRU ratio with clamp + div-by-zero/NaN -> 0.

    Returns (df, stats) where `df` has two columns (`id_feature`, `param_name`)
    and `stats` is a dict with QA counts: `n_total`, `n_zero_denom` (HRUs whose
    denominator was 0 or NaN -> collapsed to 0), and `n_clamped` (HRUs whose
    raw ratio exceeded 1.0; 0 when `clamp_to_one=False`).
    """
    cols = [id_feature, count_column]
    num = numerator_df[cols].rename(columns={count_column: "__numerator__"})
    den = denominator_df[cols].rename(columns={count_column: "__denominator__"})
    merged = num.merge(den, on=id_feature, how="outer").sort_values(id_feature).reset_index(drop=True)

    merged["__numerator__"] = merged["__numerator__"].fillna(0.0)
    merged["__denominator__"] = merged["__denominator__"].fillna(0.0)

    den_values = merged["__denominator__"].to_numpy()
    num_values = merged["__numerator__"].to_numpy()
    n_zero_denom = int((den_values == 0).sum())

    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = num_values / den_values
    ratio = np.where(np.isfinite(ratio), ratio, 0.0)

    n_clamped = 0
    if clamp_to_one:
        n_clamped = int((ratio > 1.0).sum())
        ratio = np.minimum(ratio, 1.0)

    stats = {"n_total": len(merged), "n_zero_denom": n_zero_denom, "n_clamped": n_clamped}
    df = pd.DataFrame({id_feature: merged[id_feature].to_numpy(), param_name: ratio})
    return df, stats
