"""Per-ecoregion regional fill for hydro-flattened dprst polygons (#173 Task 5).

The Phase 0 spike found NO model that predicts a hydro-flattened polygon's
bed well from first principles (Hollister max-depth R^2=0.17, end-to-end
R^2=0.06, depth-area regression R^2~0 — see `docs/dprst_depth_spike.md`
S7a). Rather than force one global model, this module fits a small,
empirically-chosen regional model PER (ecoregion, FTYPE) GROUP, using the
group's own *measurable* (non-flat) donor polygons as ground truth, and
lets the data decide between two candidates:

- **Null: the group's median measured `dprst_depth_m`** (V/A mean depth
  over non-flat polygons). Robust, always available once there is >=1
  donor.
- **Candidate: a calibrated Hollister model**, `mean = shape_factor * k *
  hollister_max_m`, fit by least squares (through the origin — Hollister's
  own physical model has mean -> 0 as max depth -> 0) on the group's
  donors, `x = hollister_max_m`, `y = measured dprst_depth_m`. `shape_factor`
  is fixed a priori (default 1/3, the same conical V/A assumption as
  `topo.max_to_mean("cone")`); `k` absorbs everything the raw Hollister
  slope-projection model gets wrong (the validated +3.2 m bias, per-region
  vegetation/substrate effects, etc.) — see `docs/dprst_depth_spike.md`
  S7a for the r~=0.42 correlation this calibration is built on.

The two are compared by K-FOLD CROSS-VALIDATED RMSE (hand-rolled with
numpy — no new dependency), fit on train folds and scored on held-out
folds, so a calibrated model only wins if it GENERALIZES, not merely
fits; with a handful of donors this will usually and CORRECTLY pick the
median (see the two "which wins" unit tests below).

`fill_flat` then applies the chosen per-group model to every flat/
degenerate row, walking a **fallback ladder** when a group has too few
donors to trust even its own median:
  (ecoregion, FTYPE) model -> ecoregion-only median -> FTYPE-only median
  -> constant floor (49 in, the NHM calibrated median — see
  `docs/superpowers/.../nhm-dprst-params-are-calibrated`).
Every fallback step is logged; nothing is silently dropped, and the
output guarantees no NaN and every depth > 0.

Units: `dprst_depth_m` stays in METRES throughout this module (matching
Task 4's compute output) — the inches conversion is Task 8's aggregation
concern. `floor_in` is the one inches-flavoured input/knob (49 in, the
NHM convention) and is converted to metres immediately on entry.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

__all__ = [
    "M_TO_IN",
    "DEFAULT_SHAPE_FACTOR",
    "N_MIN_DEFAULT",
    "Model",
    "fit_ecoregion_models",
    "fill_flat",
]

logger = logging.getLogger(__name__)

# 1 m = 39.3701 in (matches the brief's floor conversion exactly).
M_TO_IN = 39.3701

# Conical V/A assumption — same constant as `topo.max_to_mean("cone")`
# (mean = max/3). Fixed a priori so the fitted `k` has a stable, comparable
# meaning across groups: k=1 means "raw cone-Hollister already unbiased",
# k>1/k<1 says how far the group's real bathymetry departs from that.
DEFAULT_SHAPE_FACTOR = 1.0 / 3.0

# Minimum donor count for a group to get its OWN (ecoregion, FTYPE) model
# (median or calibrated-Hollister) rather than falling back one rung.
N_MIN_DEFAULT = 5

# K-fold count for the median-vs-calibrated-Hollister comparison.
N_FOLDS_DEFAULT = 5

# Sentinel key component marking a coarser, single-axis fallback group
# (e.g. `(eco, _ALL)` = "median over this ecoregion, all FTYPEs pooled").
_ALL = "__ALL__"

_REQUIRED_NON_FLAT_COLUMNS = {"ecoregion", "ftype", "dprst_depth_m", "hollister_max_m"}


@dataclass(frozen=True)
class Model:
    """One group's chosen fill model.

    `kind` is `"median"` (the null, or a group too sparse to trust a fit)
    or `"calibrated_hollister"` (won the CV comparison). `median_m` is
    ALWAYS populated (even for a calibrated-Hollister model) so `predict`
    always has a safe fallback for a row whose own `hollister_max_m` is
    missing/invalid. `k`/`shape_factor` are only set for
    `"calibrated_hollister"`. `cv_rmse_median`/`cv_rmse_hollister` record
    the comparison that decided `kind` (both `None` for a group too sparse
    to run CV at all — see `fit_ecoregion_models`' `n_min` gate).
    """

    kind: str
    median_m: float
    k: float | None = None
    shape_factor: float | None = None
    n_donors: int = 0
    cv_rmse_median: float | None = None
    cv_rmse_hollister: float | None = None

    def predict(self, hollister_max_m: float) -> tuple[float, bool]:
        """-> (depth_m, used_hollister). `used_hollister=False` means the
        median null was used — either because this model IS the median, or
        because the row's own `hollister_max_m` was missing/invalid/
        non-positive and the calibrated model fell back to its own median.
        """
        if self.kind == "calibrated_hollister" and pd.notna(hollister_max_m) and hollister_max_m > 0:
            pred = self.shape_factor * self.k * hollister_max_m
            if np.isfinite(pred) and pred > 0:
                return float(pred), True
        return self.median_m, False


def _kfold_splits(n: int, n_folds: int, rng: np.random.Generator) -> list[np.ndarray]:
    """Index permutation split into `n_folds` roughly-equal folds."""
    order = rng.permutation(n)
    return np.array_split(order, n_folds)


def _fit_slope(x: np.ndarray, y: np.ndarray) -> float:
    """Least-squares slope through the origin: argmin sum((y - slope*x)^2)."""
    denom = float(np.sum(x * x))
    if denom <= 0:
        return 0.0
    return float(np.sum(x * y) / denom)


def _cv_rmse(x: np.ndarray, y: np.ndarray, folds: list[np.ndarray], kind: str) -> float:
    """Honest K-fold CV RMSE: fit on train folds only, score on the held-out fold.

    `folds` is a precomputed list of test-index arrays (from a SINGLE
    `_kfold_splits` call in `_group_model`) — the caller must pass the SAME
    `folds` for both the "median" and "slope" candidates on a given group so
    the two are compared on identical train/test partitions (a paired
    comparison), not each on its own independently-permuted split.

    `kind="median"` fits `np.median(y_train)`; `kind="slope"` fits
    `_fit_slope(x_train, y_train)` (the raw, undecomposed slope — folding
    `shape_factor` in or out doesn't change which model wins, so it's
    applied once at the end, not inside CV).
    """
    sq_errors = []
    for i, test_idx in enumerate(folds):
        train_idx = np.concatenate([folds[j] for j in range(len(folds)) if j != i])
        if len(train_idx) == 0 or len(test_idx) == 0:
            continue
        if kind == "median":
            pred = np.full(len(test_idx), np.median(y[train_idx]))
        else:
            slope = _fit_slope(x[train_idx], y[train_idx])
            pred = slope * x[test_idx]
        sq_errors.append((y[test_idx] - pred) ** 2)
    if not sq_errors:
        return float("nan")
    return float(np.sqrt(np.mean(np.concatenate(sq_errors))))


def _group_model(
    y: np.ndarray,
    x: np.ndarray,
    n_min: int,
    n_folds: int,
    shape_factor: float,
    rng: np.random.Generator,
) -> Model:
    """Fit one (ecoregion, FTYPE) group's model: median null, and — if
    there are enough donors with a valid `hollister_max_m` — a
    CV-compared calibrated-Hollister candidate.
    """
    median_m = float(np.median(y))
    n = len(y)

    valid = np.isfinite(x) & (x > 0)
    x_valid, y_valid = x[valid], y[valid]

    if n < n_min or len(x_valid) < n_min:
        return Model(kind="median", median_m=median_m, n_donors=n)

    # Draw the fold assignment ONCE and reuse it for both candidates so the
    # median-null and calibrated-Hollister CV RMSEs are a PAIRED comparison
    # (same train/test partitions) rather than each scored on its own
    # independently-permuted split — otherwise the winner on a borderline
    # group can flip based on which candidate got the easier split.
    folds = _kfold_splits(len(y_valid), min(n_folds, len(y_valid)), rng)
    cv_rmse_med = _cv_rmse(x_valid, y_valid, folds, "median")
    cv_rmse_hol = _cv_rmse(x_valid, y_valid, folds, "slope")

    if np.isfinite(cv_rmse_hol) and cv_rmse_hol < cv_rmse_med:
        slope = _fit_slope(x_valid, y_valid)
        k = slope / shape_factor if shape_factor else slope
        return Model(
            kind="calibrated_hollister",
            median_m=median_m,
            k=k,
            shape_factor=shape_factor,
            n_donors=n,
            cv_rmse_median=cv_rmse_med,
            cv_rmse_hollister=cv_rmse_hol,
        )
    return Model(
        kind="median",
        median_m=median_m,
        n_donors=n,
        cv_rmse_median=cv_rmse_med,
        cv_rmse_hollister=cv_rmse_hol,
    )


def fit_ecoregion_models(
    non_flat_df: pd.DataFrame,
    n_min: int = N_MIN_DEFAULT,
    n_folds: int = N_FOLDS_DEFAULT,
    shape_factor: float = DEFAULT_SHAPE_FACTOR,
    random_state: int = 0,
) -> dict[tuple[str, str], Model]:
    """Fit a `Model` per (ecoregion, FTYPE) group, PLUS coarser one-axis
    fallback models the fallback ladder in `fill_flat` needs.

    `non_flat_df` is the MEASURED (non-flat) subset — donors, ground
    truth for both the median null and the Hollister calibration fit
    (`x=hollister_max_m`, `y=dprst_depth_m`). Rows with NaN `dprst_depth_m`
    are dropped (defensive — Task 4 should never emit those for non-flat
    rows).

    Returns a single dict keyed three ways so `fill_flat`'s ladder is a
    plain sequence of `dict.get` calls:
      - `(ecoregion, ftype)`       -> the group's own model (median, or
        calibrated-Hollister if it beat the median on CV RMSE).
      - `(ecoregion, "__ALL__")`   -> ecoregion-only median (all FTYPEs
        pooled) — rung 2 of the fallback ladder.
      - `("__ALL__", ftype)`       -> FTYPE-only median (all ecoregions
        pooled) — rung 3.
    The coarse rungs are ALWAYS median-only (a calibrated-Hollister fit is
    only trusted at the finer, more homogeneous (ecoregion, FTYPE) grain).

    A group with fewer than `n_min` donors (or fewer than `n_min` donors
    with a valid `hollister_max_m`) never attempts the CV comparison and
    is recorded as `kind="median"` outright — see `Model`'s docstring for
    how a missing/degenerate `hollister_max_m` on a later row still falls
    back safely even for a `kind="calibrated_hollister"` group.
    """
    models: dict[tuple[str, str], Model] = {}
    if non_flat_df is None or len(non_flat_df) == 0:
        logger.info("fit_ecoregion_models: 0 non-flat donor rows — returning no models")
        return models

    missing = _REQUIRED_NON_FLAT_COLUMNS - set(non_flat_df.columns)
    if missing:
        raise KeyError(f"fit_ecoregion_models: non_flat_df missing columns {sorted(missing)}")

    df = non_flat_df.dropna(subset=["dprst_depth_m"])
    rng = np.random.default_rng(random_state)

    n_calibrated = 0
    n_median_cv = 0
    n_median_sparse = 0

    for (eco, ftype), grp in df.groupby(["ecoregion", "ftype"]):
        y = grp["dprst_depth_m"].to_numpy(dtype=float)
        x = grp["hollister_max_m"].to_numpy(dtype=float)
        model = _group_model(y, x, n_min, n_folds, shape_factor, rng)
        models[(eco, ftype)] = model
        if model.kind == "calibrated_hollister":
            n_calibrated += 1
        elif model.cv_rmse_median is not None:
            n_median_cv += 1
        else:
            n_median_sparse += 1

    for eco, grp in df.groupby("ecoregion"):
        y = grp["dprst_depth_m"].to_numpy(dtype=float)
        models[(eco, _ALL)] = Model(kind="median", median_m=float(np.median(y)), n_donors=len(y))

    for ftype, grp in df.groupby("ftype"):
        y = grp["dprst_depth_m"].to_numpy(dtype=float)
        models[(_ALL, ftype)] = Model(kind="median", median_m=float(np.median(y)), n_donors=len(y))

    logger.info(
        "fit_ecoregion_models: %d (ecoregion,FTYPE) groups — %d calibrated_hollister "
        "(beat median on CV RMSE), %d median (CV-compared, median won), %d median "
        "(< n_min=%d donors, no CV run); %d ecoregion-only + %d FTYPE-only fallback medians",
        n_calibrated + n_median_cv + n_median_sparse,
        n_calibrated,
        n_median_cv,
        n_median_sparse,
        n_min,
        df["ecoregion"].nunique(),
        df["ftype"].nunique(),
    )
    return models


def fill_flat(
    df: pd.DataFrame,
    models: dict[tuple[str, str], Model],
    floor_in: float = 49.0,
) -> pd.DataFrame:
    """Fill every flat/degenerate row's `dprst_depth_m` via the fallback ladder.

    Non-flat rows are untouched except `method` is (re)set to `"measured"`
    (Task 4 already computed their real `dprst_depth_m`). For each flat
    row, in order:
      1. its own `(ecoregion, FTYPE)` model (`models[(eco, ftype)]`);
      2. ecoregion-only median (`models[(eco, "__ALL__")]`);
      3. FTYPE-only median (`models[("__ALL__", ftype)]`);
      4. the constant floor (`floor_in`, converted to metres via
         `M_TO_IN` — kept in metres internally per this module's unit
         convention; inches conversion is Task 8's concern downstream).
    `method` is set to `"calibrated_hollister"` only when step 1's model
    actually used the row's own `hollister_max_m` (see `Model.predict`);
    any median use — own-group, ecoregion-only, or FTYPE-only — is
    `"regional_fill"`; step 4 is `"constant_floor"`.

    GUARANTEES: no NaN in `dprst_depth_m` on return, every value > 0 (a
    final defensive pass forces any leftover NaN/non-positive value to the
    floor and logs it — this should never fire given the ladder above, but
    a silent NaN in a PRMS parameter is worse than a logged floor value).
    """
    if "dprst_depth_m" not in df.columns:
        raise KeyError("fill_flat: df missing 'dprst_depth_m'")
    if "flat" not in df.columns:
        raise KeyError("fill_flat: df missing 'flat'")

    floor_m = floor_in / M_TO_IN

    out = df.copy()
    if "method" not in out.columns:
        out["method"] = pd.Series(pd.NA, index=out.index, dtype=object)
    else:
        out["method"] = out["method"].astype(object)

    flat_mask = out["flat"].fillna(False).astype(bool)
    out.loc[~flat_mask, "method"] = "measured"

    n_regional = 0
    n_calibrated = 0
    n_floor = 0

    has_eco = "ecoregion" in out.columns
    has_ftype = "ftype" in out.columns
    has_hollister = "hollister_max_m" in out.columns

    for idx in out.index[flat_mask]:
        eco = out.at[idx, "ecoregion"] if has_eco else None
        ftype = out.at[idx, "ftype"] if has_ftype else None
        hollister = out.at[idx, "hollister_max_m"] if has_hollister else float("nan")

        model = models.get((eco, ftype))
        rung = "own"
        if model is None:
            model = models.get((eco, _ALL))
            rung = "ecoregion"
        if model is None:
            model = models.get((_ALL, ftype))
            rung = "ftype"

        if model is not None:
            depth, used_hollister = model.predict(hollister)
            if used_hollister:
                method = "calibrated_hollister"
                n_calibrated += 1
            else:
                method = "regional_fill"
                n_regional += 1
        else:
            depth, method = floor_m, "constant_floor"
            n_floor += 1
            rung = "floor"

        out.at[idx, "dprst_depth_m"] = depth
        out.at[idx, "method"] = method
        logger.debug("fill_flat: idx=%s eco=%s ftype=%s rung=%s method=%s depth_m=%.4f",
                     idx, eco, ftype, rung, method, depth)

    # Defensive final guard: never let a NaN or non-positive depth escape
    # this function, even if the ladder above somehow missed a case.
    bad = out["dprst_depth_m"].isna() | (out["dprst_depth_m"] <= 0)
    n_bad = int(bad.sum())
    if n_bad:
        logger.warning(
            "fill_flat: %d row(s) still NaN/non-positive after the fallback ladder — "
            "forcing to the %.4f m floor (this should not happen; investigate upstream)",
            n_bad, floor_m,
        )
        out.loc[bad, "dprst_depth_m"] = floor_m
        out.loc[bad, "method"] = "constant_floor"
        n_floor += n_bad

    logger.info(
        "fill_flat: %d flat rows filled (%d regional_fill, %d calibrated_hollister, "
        "%d constant_floor; floor=%.4f m = %.1f in)",
        int(flat_mask.sum()), n_regional, n_calibrated, n_floor, floor_m, floor_in,
    )
    return out
