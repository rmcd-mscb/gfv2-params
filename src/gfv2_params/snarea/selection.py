"""HRU selection criteria (Driscoll et al. Table/§Selection) and SDC classification.

Criterion 1 (full SNODAS coverage) is handled upstream by the fabric extent and
is not re-tested here. Criteria are evaluated in a fixed order; the first failure
names the fallback reason.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .season import SWE_LEVELS

_HALF_SWE_INDEX = int(np.where(np.isclose(SWE_LEVELS, 0.5))[0][0])  # = 5


@dataclass(frozen=True)
class SelectionParams:
    min_cells: int = 25
    max_water_frac: float = 0.5
    min_seasonal_sca: float = 0.5
    max_constant_frac: float = 0.8
    max_similarity: float = 0.15


def passes_selection(
    *,
    has_snow: bool,
    n_cells: int,
    water_frac: float,
    seasonal_sca_max: float,
    constant_frac: float,
    similarity_value: float,
    params: SelectionParams,
) -> tuple[bool, str]:
    if not has_snow:
        return False, "default_no_snow"
    if n_cells < params.min_cells:
        return False, "default_too_few_cells"
    if water_frac > params.max_water_frac:
        return False, "default_water_dominated"
    if seasonal_sca_max < params.min_seasonal_sca:
        return False, "default_low_sca"
    if constant_frac > params.max_constant_frac:
        return False, "default_constant_sca"
    if similarity_value > params.max_similarity:
        return False, "default_dissimilar"
    return True, "derived"


def classify(rep_sdc: np.ndarray) -> str:
    """low (<0.45) / mid (0.45–0.55) / high (>0.55) from SCA at normalized SWE=0.5."""
    sca_half = float(rep_sdc[_HALF_SWE_INDEX])
    if sca_half < 0.45:
        return "low"
    if sca_half > 0.55:
        return "high"
    return "mid"
