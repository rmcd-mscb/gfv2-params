"""Stage 3: CV/lognormal snarea_curve library builder.

The curve SHAPE is a single physical parameter — the sub-grid SWE coefficient of
variation (CV) — via a lognormal SWE pdf (Sexstone et al. 2020, eqs 3-5; Liston
2004). The dimensionless snow-depletion curve depends only on CV. Repo curve
order is DESCENDING (SWE/thresh 1.0 -> 0.0); the PRMS NetCDF is ascending (see
_to_prms_order).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import norm

# Descending, matching snarea/season.py SWE_LEVELS.
SWE_LEVELS = np.round(np.arange(1.0, -1e-4, -0.1), 1)  # 1.0 .. 0.0, 11 values


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
