"""SNODAS-derived snow-depletion-curve (snarea_curve) builder."""

from __future__ import annotations

from .build import DEFAULT_SNAREA_CURVE, build_snarea_curve
from .library import build_from_derived, build_library, sdc_from_cv

__all__ = [
    "build_snarea_curve",
    "DEFAULT_SNAREA_CURVE",
    "build_from_derived",
    "build_library",
    "sdc_from_cv",
]
