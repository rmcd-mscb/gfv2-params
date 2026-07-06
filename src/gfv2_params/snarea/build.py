"""Assemble per-HRU snarea_curve rows: derive representative SDC or fall back.

DEFAULT_SNAREA_CURVE is a documented placeholder (a near-linear depletion curve)
used when an HRU fails selection. Replace it with the fabric's actual NHM default
snarea_curve when that file is staged (see plan Task 9 note); it is intentionally
a single named constant so the swap is one edit + config override.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .representative import median_sdc, select_representative, similarity
from .season import annual_sdc
from .selection import SelectionParams, classify, passes_selection

# Placeholder: SCA declines linearly with normalized SWE (1.0 → 0.0).
DEFAULT_SNAREA_CURVE = np.round(np.linspace(1.0, 0.0, 11), 4)

_CURVE_COLS = [f"snarea_curve_{i}" for i in range(11)]


def _seasons(daily: pd.DataFrame) -> list[np.ndarray]:
    """Up to one annual SDC per WATER YEAR (Oct 1 – Sep 30) in the frame.

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
    for _wy, grp in daily.groupby(water_year):
        curve = annual_sdc(grp["swe"], grp["sca"])
        if curve is not None:
            out.append(curve)
    return out


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
) -> dict:
    seasons = _seasons(daily)
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
    }
    record.update({c: float(rep[i]) for i, c in enumerate(_CURVE_COLS)})
    return record


def build_snarea_curve(
    daily_by_hru: dict,
    cells_by_hru: dict,
    water_by_hru: dict,
    id_feature: str,
    params: SelectionParams,
    default_curve: np.ndarray,
) -> pd.DataFrame:
    rows = [
        build_hru_record(
            hru_id, daily, cells_by_hru.get(hru_id, 0),
            water_by_hru.get(hru_id, 0.0), params, default_curve,
        )
        for hru_id, daily in sorted(daily_by_hru.items())
    ]
    df = pd.DataFrame(rows).rename(columns={"hru_id": id_feature})
    return df
