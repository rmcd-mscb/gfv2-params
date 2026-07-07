"""Declarative description of a gridded source for the aggregation driver.

Fabric-agnostic: a SourceAdapter describes a *source* (its variables, grid CRS,
coordinate names, and optional pre-aggregation transform), never a fabric. The
driver receives the fabric/id_col separately, resolved from the active profile.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import xarray as xr

# gdptools area-weighted reductions we allow (a typo should fail at construction).
_ALLOWED_STAT_METHODS = {
    "mean", "masked_mean", "median", "masked_median", "std", "masked_std",
    "min", "masked_min", "max", "masked_max", "sum", "masked_sum",
    "count", "masked_count",
}


@dataclass(frozen=True)
class SourceAdapter:
    source_key: str
    variables: tuple[str, ...]
    files_glob: str
    source_crs: str = "EPSG:4326"
    x_coord: str = "x"
    y_coord: str = "y"
    time_coord: str = "time"
    stat_method: str = "mean"
    pre_aggregate_hook: Callable[[xr.Dataset], xr.Dataset] | None = field(default=None)
    grid_variable: str | None = None
    std_variables: tuple[str, ...] = field(default=())

    def __post_init__(self) -> None:
        object.__setattr__(self, "variables", tuple(self.variables))
        if len(self.variables) == 0:
            raise ValueError("SourceAdapter.variables must be non-empty")
        if self.stat_method not in _ALLOWED_STAT_METHODS:
            raise ValueError(
                f"SourceAdapter.stat_method={self.stat_method!r} is not a gdptools "
                f"STATSMETHODS value; expected one of {sorted(_ALLOWED_STAT_METHODS)}"
            )
        if self.grid_variable is None:
            object.__setattr__(self, "grid_variable", self.variables[0])
        elif self.grid_variable not in self.variables:
            raise ValueError(
                f"grid_variable {self.grid_variable!r} must be one of {self.variables}"
            )
        object.__setattr__(self, "std_variables", tuple(self.std_variables))
        missing = [v for v in self.std_variables if v not in self.variables]
        if missing:
            raise ValueError(
                f"std_variables {missing} must all be in variables {self.variables}"
            )
