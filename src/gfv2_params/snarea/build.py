"""Assemble per-HRU snarea_curve rows: derive representative SDC or fall back.

DEFAULT_SNAREA_CURVE is a documented placeholder (a near-linear depletion curve)
used when an HRU fails selection. Replace it with the fabric's actual NHM default
snarea_curve when that file is staged (see plan Task 9 note); it is intentionally
a single named constant so the swap is one edit + config override.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .representative import median_sdc, select_representative, similarity
from .season import SDC_LENGTH, annual_sdc
from .selection import SelectionParams, classify, passes_selection
from .subgrid import representative_peak_stats

# Placeholder: SCA declines linearly with normalized SWE (1.0 → 0.0).
DEFAULT_SNAREA_CURVE = np.round(np.linspace(1.0, 0.0, SDC_LENGTH), 4)

_CURVE_COLS = [f"snarea_curve_{i}" for i in range(SDC_LENGTH)]


def validate_default_curve(arr: np.ndarray) -> None:
    """Validate a `default_curve` override: shape, value range, non-increasing.

    Raises ValueError (not `assert`, which is stripped under `python -O`)
    naming which check failed. Shared by the Stage 2 (derive_snarea_curve.py) and
    Stage 3 (derive_snarea_library.py) drivers — lives here (not in either script)
    because `scripts/` is not an importable package at runtime (only under
    pytest's rootdir-on-sys.path), so a script-to-script import would break when
    run directly via `python scripts/derive_snarea_library.py`.
    """
    if arr.shape != (SDC_LENGTH,):
        raise ValueError(
            f"default_curve must have shape ({SDC_LENGTH},), got {arr.shape}"
        )
    if not np.all((arr >= 0.0) & (arr <= 1.0)):
        raise ValueError(f"default_curve values must all be within [0.0, 1.0], got {arr}")
    if not np.all(np.diff(arr) <= 1e-9):
        raise ValueError(f"default_curve must be non-increasing, got {arr}")


def _seasons(daily: pd.DataFrame) -> tuple[list[np.ndarray], int]:
    """Up to one annual SDC per WATER YEAR (Oct 1 – Sep 30) in the frame.

    Returns ``(curves, n_years_total)`` — the usable curves, and how many water
    years were examined to get them. The count is what makes a dropped year
    visible: an HRU where most years never melt out reports the same
    ``n_seasons`` as one with that many clean years (see #166), and the
    difference is the only signal that the record is thin. A year is dropped
    when SNODAS shows no snow, when the snow never melts out, or when the
    record is too short — including, at the domain edge, a year that is
    entirely SNODAS fill and therefore reads as snow-free.

    Water-year framing (vs calendar year) keeps each snow season's accumulation
    (Oct–Dec) and melt (Jan–Jul) in one window, so the annual peak is the spring
    maximum — not a late-December snowfall event that a calendar-year ``argmax``
    would mis-pick, producing a garbage 2–4 day "melt season" (see the 2026-07-06
    Oregon investigation). Years where ``annual_sdc`` returns None (no snow /
    never melts / too few points) are omitted, so ``len(result)`` counts usable
    seasons, not water years. The USGS water year is labelled by its ending
    calendar year, so Oct–Dec advance the label by one.
    """
    water_year = daily.index.year + (daily.index.month >= 10).astype(int)
    out = []
    n_total = 0
    for _wy, grp in daily.groupby(water_year):
        n_total += 1
        curve = annual_sdc(grp["swe"], grp["sca"])
        if curve is not None:
            out.append(curve)
    return out, n_total


def _constant_frac(daily: pd.DataFrame) -> float:
    """Fraction of snow-present days whose SCA is within 1e-9 of the single max
    SCA over the full (possibly multi-year) record — a flat/degenerate-SCA
    proxy.
    """
    snow = daily[daily["swe"] > 0]
    if len(snow) == 0:
        return 1.0
    return float((snow["sca"] >= snow["sca"].max() - 1e-9).mean())


def build_hru_record(
    hru_id: int,
    daily: pd.DataFrame,
    n_cells: int,
    water_frac: float,
    params: SelectionParams,
    default_curve: np.ndarray,
    coverage: float = float("nan"),
) -> dict:
    seasons, n_years_total = _seasons(daily)
    has_snow = daily["swe"].max() > 0
    sim = float("nan")
    rep = default_curve
    n_seasons = len(seasons)

    if seasons:
        annual = np.vstack(seasons)
        median = median_sdc(annual)
        sim = similarity(annual, median)
        rep_candidate = select_representative(annual, median)
    else:
        rep_candidate = default_curve

    ok, status = passes_selection(
        has_snow=has_snow,
        n_cells=n_cells,
        water_frac=water_frac,
        seasonal_sca_max=float(daily["sca"].max()) if len(daily) else 0.0,
        constant_frac=_constant_frac(daily),
        similarity_value=sim if not np.isnan(sim) else float("inf"),
        params=params,
    )
    if ok:
        rep = rep_candidate

    record = {
        "hru_id": hru_id,
        # 1:1 index; assumes id_feature is already the dense/1-based index
        # PRMS expects (revisit for non-contiguous ids)
        "hru_deplcrv": hru_id,
        "sdc_status": status,
        "sca_class": classify(rep),
        "similarity": sim,
        "n_seasons": n_seasons,
        "n_years_total": n_years_total,
        "n_years_dropped": n_years_total - n_seasons,
    }
    record.update({c: float(rep[i]) for i, c in enumerate(_CURVE_COLS)})

    stats = (
        representative_peak_stats(daily)
        if "swe_std" in daily.columns
        else {"cv_subgrid": float("nan"), "peak_swe_mm": float("nan"), "n_peak_years": 0}
    )
    record.update(stats)
    # Diagnostic only: the fraction of the HRU's SNODAS weight that falls on
    # valid (non-fill) cells. It never gates selection — see #166, where a
    # gate would have flipped ~13k CONUS HRUs off their empirical curve — but
    # without it a zero-coverage HRU is indistinguishable from a snow-free one,
    # since gdptools' masked_mean returns 0.0 (not NaN) for an all-fill HRU.
    record["coverage"] = float(coverage)
    return record


def build_snarea_curve(
    daily_by_hru: dict,
    cells_by_hru: dict,
    water_by_hru: dict,
    id_feature: str,
    params: SelectionParams,
    default_curve: np.ndarray,
    logger: logging.Logger | None = None,
    log_every: int = 25_000,
    coverage_by_hru: dict | None = None,
) -> pd.DataFrame:
    """Per-HRU derivation loop. Pass ``logger`` to emit progress every
    ``log_every`` HRUs — the loop is silent otherwise, which reads as a hang at
    CONUS scale (361k HRUs take minutes)."""
    items = sorted(daily_by_hru.items())
    n = len(items)
    # NaN, not 0.0: an HRU missing from the coverage table was NOT MEASURED,
    # which must not be read as "measured, and empty".
    coverage_by_hru = coverage_by_hru or {}
    rows = []
    for i, (hru_id, daily) in enumerate(items, start=1):
        rows.append(build_hru_record(
            hru_id, daily, cells_by_hru.get(hru_id, 0),
            water_by_hru.get(hru_id, 0.0), params, default_curve,
            coverage=coverage_by_hru.get(hru_id, float("nan")),
        ))
        if logger is not None and (i % log_every == 0 or i == n):
            logger.info("  derived %d/%d HRUs (%.0f%%)", i, n, 100 * i / n)
    df = pd.DataFrame(rows).rename(columns={"hru_id": id_feature})
    return df
