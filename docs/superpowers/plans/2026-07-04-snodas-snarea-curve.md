# SNODAS → PRMS `snarea_curve` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Derive the PRMS/NHM `snarea_curve` parameter (one per-HRU snow-depletion curve) from daily SNODAS SWE, following Driscoll, Hay & Bock (2017).

**Architecture:** Two stages, both in `gfv2-params`. Stage 1 is a new, source-agnostic gridded-time-series→HRU aggregation harness (`src/gfv2_params/aggregate/`) with a SNODAS adapter that emits per-HRU daily mean SWE and snow-covered-area fraction. Stage 2 (`src/gfv2_params/snarea/`) is pure pandas/numpy that turns those daily series into per-HRU representative depletion curves plus a curve index and status flag.

**Tech Stack:** Python 3, pixi env, gdptools (`UserCatData`/`WeightGen`/`AggGen`), xarray, geopandas, pandas, numpy, pytest. Design spec: [`docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md`](../specs/2026-07-04-snodas-snarea-curve-design.md).

## Global Constraints

- **Fabric-independent:** process configs carry only `{data_root}`/`{fabric}` placeholders and source settings — never a fabric name or literal path. All per-fabric inputs come from the active profile in `configs/base_config.yml`, read via `require_config_key(config, key, script_name)`. (CLAUDE.md "Paths and fabric inputs come from the profile".)
- **Every new pipeline step is a module + config block + `tests/test_<name>.py`.** Match the nearest existing test for style.
- **Do NOT run `pytest` on the HPC head node.** CI (`.github/workflows/ci.yml`) is the test gate. Local `py_compile`/import checks are fine. Full-fabric runs go through pixi/SLURM.
- **pixi commands:** tests via `pixi run -e dev pytest <path> -v`; one-offs via `pixi run python …`; SLURM batches use `pixi run --as-is`.
- **CRS:** SNODAS is pre-projected to EPSG:5070; the weight-gen/target CRS is `5070` (`WEIGHT_GEN_CRS = 5070`), so gdptools does not reproject the source.
- **SNODAS fill:** `_FillValue = -9999`; mask with a `> -9990` threshold (slop margin) and use `stat_method="masked_mean"`.
- **Commit after every task.** Pre-push: `pixi run -e dev pre-commit run --all-files`.
- **Study period:** all available years 2004–2024, processed per calendar year.
- **11 SWE levels:** `SWE_LEVELS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]` (curve stored in this order; SCA at `SWE_LEVELS[5]` = the 50%-SWE classification point).

---

## File Structure

**Stage 1 — aggregation harness (new package `src/gfv2_params/aggregate/`):**
- `__init__.py` — re-export `SourceAdapter`, `aggregate_source`.
- `adapter.py` — `SourceAdapter` frozen dataclass (source description; fabric-agnostic).
- `driver.py` — `compute_or_load_weights`, `aggregate_variables`, `aggregate_source` (the gdptools loop, per calendar year, weights cached once).
- `snodas.py` — `SNODAS_ADAPTER` + `_snodas_hook` (`swe` fill-mask, derive `scov`).

**Stage 2 — snarea builder (new package `src/gfv2_params/snarea/`):**
- `__init__.py` — re-export the public builder entry `build_snarea_curve`.
- `season.py` — `melt_season`, `remove_reversals`, `normalize_curve`, `annual_sdc`.
- `representative.py` — `median_sdc`, `similarity`, `select_representative`.
- `selection.py` — `SelectionParams`, `passes_selection`, `classify`.
- `build.py` — `build_snarea_curve` (reads daily NCs → per-HRU table with fallback).

**Orchestrators / config / profile:**
- `scripts/derive_aggregate.py` — CLI driver for Stage 1 (one source, one fabric, optional year-shard).
- `scripts/derive_snarea_curve.py` — CLI driver for Stage 2.
- `configs/aggregate/aggregate_sources.yml` — Stage 1 config (`defaults` + `snodas` entry, `snodas_dir` default).
- `configs/snarea/snarea_curve.yml` — Stage 2 config (thresholds, default curve, output).
- `configs/base_config.yml` — document optional per-profile `snodas_dir` override (no code change required; default lives in the process config).

**Tests:** `tests/test_aggregate_adapter.py`, `tests/test_aggregate_driver.py`, `tests/test_aggregate_snodas.py`, `tests/test_snarea_season.py`, `tests/test_snarea_representative.py`, `tests/test_snarea_selection.py`, `tests/test_snarea_build.py`.

---

## Task 1: `SourceAdapter` dataclass

**Files:**
- Create: `src/gfv2_params/aggregate/__init__.py`
- Create: `src/gfv2_params/aggregate/adapter.py`
- Test: `tests/test_aggregate_adapter.py`

**Interfaces:**
- Produces: `SourceAdapter(source_key: str, variables: tuple[str,...], files_glob: str, source_crs: str="EPSG:4326", x_coord: str="x", y_coord: str="y", time_coord: str="time", stat_method: str="mean", pre_aggregate_hook: Callable[[xr.Dataset], xr.Dataset] | None=None, grid_variable: str|None=None)`. Frozen dataclass; `grid_variable` defaults to `variables[0]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_aggregate_adapter.py
import pytest
from gfv2_params.aggregate import SourceAdapter


def test_defaults_and_grid_variable():
    a = SourceAdapter(source_key="demo", variables=["swe"], files_glob="*.nc")
    assert a.variables == ("swe",)          # list coerced to tuple
    assert a.grid_variable == "swe"         # defaults to first variable
    assert a.stat_method == "mean"
    assert a.source_crs == "EPSG:4326"


def test_rejects_bad_stat_method():
    with pytest.raises(ValueError, match="stat_method"):
        SourceAdapter(source_key="d", variables=("swe",), files_glob="*.nc",
                      stat_method="not_a_method")


def test_rejects_empty_variables():
    with pytest.raises(ValueError, match="variables"):
        SourceAdapter(source_key="d", variables=(), files_glob="*.nc")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_aggregate_adapter.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'gfv2_params.aggregate'`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/aggregate/adapter.py
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
```

```python
# src/gfv2_params/aggregate/__init__.py
"""Gridded-time-series → HRU aggregation harness (source-agnostic).

The time-series counterpart to the static-raster zonal_runners: wraps gdptools
UserCatData/WeightGen/AggGen so any gridded source (SNODAS today, climate later)
can be area-weighted to an HRU fabric via a declarative SourceAdapter.
"""

from __future__ import annotations

from .adapter import SourceAdapter
from .driver import aggregate_source

__all__ = ["SourceAdapter", "aggregate_source"]
```

Note: the `__init__.py` imports `aggregate_source` from `driver` (Task 2). Until Task 2 exists this import fails, so for Task 1 temporarily omit the `driver` import line and the `aggregate_source` entry, then restore both in Task 2 Step 3. (Alternatively implement Task 1 and 2 back-to-back.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_aggregate_adapter.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/aggregate/__init__.py src/gfv2_params/aggregate/adapter.py tests/test_aggregate_adapter.py
git commit -m "feat(aggregate): SourceAdapter dataclass for gridded-time-series aggregation"
```

---

## Task 2: aggregation driver (weights + AggGen loop)

**Files:**
- Create: `src/gfv2_params/aggregate/driver.py`
- Modify: `src/gfv2_params/aggregate/__init__.py` (restore the `driver` import from Task 1)
- Test: `tests/test_aggregate_driver.py`

**Interfaces:**
- Consumes: `SourceAdapter` (Task 1).
- Produces:
  - `WEIGHT_GEN_CRS = 5070`
  - `compute_or_load_weights(adapter, sample_ds, fabric_gdf, id_col, period, weight_file: Path) -> pandas.DataFrame`
  - `aggregate_variables(adapter, source_ds, fabric_gdf, id_col, weights, period) -> xarray.Dataset` (dims `time`, `<id_col>`; one var per `adapter.variables`)
  - `aggregate_source(adapter, fabric_gdf, id_col, input_dir: Path, output_dir: Path, weight_file: Path, output_prefix: str, years: list[int] | None = None) -> list[Path]` (writes one NetCDF per calendar year, returns written paths)

- [ ] **Step 1: Write the failing test**

Uses a tiny synthetic 4×4 EPSG:5070 grid with a 2-day time axis and two square polygons, so the area-weighted mean is analytically known.

```python
# tests/test_aggregate_driver.py
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import box

from gfv2_params.aggregate import SourceAdapter
from gfv2_params.aggregate.driver import aggregate_source


def _synthetic_grid(tmp_path: Path) -> Path:
    # 4x4 grid, 1000 m cells, EPSG:5070, origin (0,0). Cell centers at 500,1500,...
    x = np.array([500.0, 1500.0, 2500.0, 3500.0])
    y = np.array([3500.0, 2500.0, 1500.0, 500.0])  # descending (north-up)
    # day 0: all 1.0 ; day 1: left half 2.0, right half 0.0
    d0 = np.ones((4, 4), dtype="float32")
    d1 = np.array([[2, 2, 0, 0]] * 4, dtype="float32")
    swe = np.stack([d0, d1])  # (time, y, x)
    ds = xr.Dataset(
        {"swe": (("time", "y", "x"), swe)},
        coords={"time": pd.to_datetime(["2010-01-01", "2010-01-02"]), "y": y, "x": x},
    )
    ds["swe"].rio  # noqa: keep rioxarray import path warm if present
    p = tmp_path / "demo_daily_2010.nc"
    ds.to_netcdf(p)
    return p


def _two_polys() -> gpd.GeoDataFrame:
    # left poly covers x in [0,2000], right poly x in [2000,4000], full y.
    left = box(0, 0, 2000, 4000)
    right = box(2000, 0, 4000, 4000)
    return gpd.GeoDataFrame({"hru_id": [1, 2]}, geometry=[left, right], crs="EPSG:5070")


def test_aggregate_source_area_weighted_mean(tmp_path):
    _synthetic_grid(tmp_path)
    gdf = _two_polys()
    adapter = SourceAdapter(
        source_key="demo", variables=("swe",), files_glob="demo_daily_*.nc",
        source_crs="EPSG:5070", x_coord="x", y_coord="y", time_coord="time",
        stat_method="mean",
    )
    out = aggregate_source(
        adapter, gdf, "hru_id",
        input_dir=tmp_path, output_dir=tmp_path / "out",
        weight_file=tmp_path / "w.csv", output_prefix="demo",
    )
    assert len(out) == 1
    res = xr.open_dataset(out[0])
    # day 0: both polys mean 1.0 ; day 1: left mean 2.0, right mean 0.0
    swe = res["swe"].sel(hru_id=[1, 2]).values  # (time, hru)
    np.testing.assert_allclose(swe[0], [1.0, 1.0], atol=1e-6)
    np.testing.assert_allclose(swe[1], [2.0, 0.0], atol=1e-6)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_aggregate_driver.py -v`
Expected: FAIL — `ModuleNotFoundError` / `aggregate_source` not defined.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/aggregate/driver.py
"""gdptools-backed aggregation driver: weights once, AggGen per variable/year.

Ports the aggregation core of nhf-spatial-targets' aggregate/_driver.py, trimmed
to gfv2-params (no manifest/lineage/release). Weights depend only on grid∩fabric
geometry, so they are computed once (from the first year's grid) and reused for
every variable and every year.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import xarray as xr
from gdptools import AggGen, UserCatData, WeightGen

from .adapter import SourceAdapter

logger = logging.getLogger(__name__)

WEIGHT_GEN_CRS = 5070  # NAD83 / CONUS Albers (equal-area)


def _period_bounds(ds: xr.Dataset, time_coord: str) -> tuple[str, str]:
    t = pd.to_datetime(ds[time_coord].values)
    return (str(t.min().date()), str(t.max().date()))


def compute_or_load_weights(
    adapter: SourceAdapter,
    sample_ds: xr.Dataset,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    period: tuple[str, str],
    weight_file: Path,
) -> pd.DataFrame:
    """Return grid→polygon weights, computing and caching them if absent."""
    if weight_file.exists():
        logger.info("Loading cached weights: %s", weight_file)
        return pd.read_csv(weight_file)

    user_data = UserCatData(
        source_ds=sample_ds,
        source_crs=adapter.source_crs,
        source_x_coord=adapter.x_coord,
        source_y_coord=adapter.y_coord,
        source_t_coord=adapter.time_coord,
        source_var=[adapter.grid_variable],
        target_gdf=fabric_gdf,
        target_crs=WEIGHT_GEN_CRS,
        target_id=id_col,
        source_time_period=[period[0], period[1]],
    )
    wg = WeightGen(user_data=user_data, method="serial", weight_gen_crs=WEIGHT_GEN_CRS)
    weights = wg.calculate_weights()
    if weights is None or len(weights) == 0:
        raise RuntimeError(
            "WeightGen returned no weights — check grid/fabric spatial overlap."
        )
    weight_file.parent.mkdir(parents=True, exist_ok=True)
    weights.to_csv(weight_file, index=False)
    logger.info("Weights computed: %d rows -> %s", len(weights), weight_file)
    return weights


def aggregate_variables(
    adapter: SourceAdapter,
    source_ds: xr.Dataset,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    weights: pd.DataFrame,
    period: tuple[str, str],
) -> xr.Dataset:
    """Run AggGen once per declared variable; merge on the HRU id dimension."""
    per_var: list[xr.Dataset] = []
    for var in adapter.variables:
        user_data = UserCatData(
            source_ds=source_ds,
            source_crs=adapter.source_crs,
            source_x_coord=adapter.x_coord,
            source_y_coord=adapter.y_coord,
            source_t_coord=adapter.time_coord,
            source_var=[var],
            target_gdf=fabric_gdf,
            target_crs=WEIGHT_GEN_CRS,
            target_id=id_col,
            source_time_period=[period[0], period[1]],
        )
        agg = AggGen(
            user_data=user_data,
            stat_method=adapter.stat_method,
            agg_engine="serial",
            agg_writer="none",
            weights=weights,
        )
        _gdf, ds = agg.calculate_agg()
        per_var.append(ds)
    return xr.merge(per_var)


def _year_of(path: Path) -> int:
    m = re.search(r"(\d{4})", path.stem)
    if not m:
        raise ValueError(f"Cannot parse a 4-digit year from filename: {path.name}")
    return int(m.group(1))


def aggregate_source(
    adapter: SourceAdapter,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    input_dir: Path,
    output_dir: Path,
    weight_file: Path,
    output_prefix: str,
    years: list[int] | None = None,
) -> list[Path]:
    """Aggregate every per-year file matching the adapter glob to the fabric.

    Writes one NetCDF per calendar year: ``{output_prefix}_agg_{year}.nc`` with
    dims (time, <id_col>) and one data var per adapter variable.
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob(adapter.files_glob))
    if not files:
        raise FileNotFoundError(f"No files match {input_dir / adapter.files_glob}")

    written: list[Path] = []
    weights: pd.DataFrame | None = None
    for f in files:
        year = _year_of(f)
        if years is not None and year not in years:
            continue
        ds = xr.open_dataset(f)
        if adapter.pre_aggregate_hook is not None:
            ds = adapter.pre_aggregate_hook(ds)
        period = _period_bounds(ds, adapter.time_coord)
        if weights is None:
            weights = compute_or_load_weights(
                adapter, ds, fabric_gdf, id_col, period, weight_file
            )
        hru_ds = aggregate_variables(adapter, ds, fabric_gdf, id_col, weights, period)
        out = output_dir / f"{output_prefix}_agg_{year}.nc"
        hru_ds.to_netcdf(out)
        written.append(out)
        logger.info("Aggregated %s -> %s", f.name, out.name)
    return written
```

Then restore the `__init__.py` import from Task 1 (it already references `driver.aggregate_source`).

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_aggregate_driver.py -v`
Expected: PASS. (If gdptools' exact output dimension name for the id differs, adjust the `.sel(hru_id=…)` access to match `res` dims — inspect with `print(res)` once.)

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/aggregate/driver.py src/gfv2_params/aggregate/__init__.py tests/test_aggregate_driver.py
git commit -m "feat(aggregate): gdptools driver (cached weights + per-year AggGen loop)"
```

---

## Task 3: SNODAS adapter (`swe` + derived `scov`)

**Files:**
- Create: `src/gfv2_params/aggregate/snodas.py`
- Test: `tests/test_aggregate_snodas.py`

**Interfaces:**
- Consumes: `SourceAdapter` (Task 1).
- Produces: `SNODAS_ADAPTER: SourceAdapter` (variables `("swe","scov")`, `masked_mean`, EPSG:5070, glob `snodas_daily_*.nc`) and `_snodas_hook(ds) -> xr.Dataset`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_aggregate_snodas.py
import numpy as np
import xarray as xr

from gfv2_params.aggregate.snodas import SNODAS_ADAPTER, _snodas_hook


def _ds(swe_vals):
    return xr.Dataset({"swe": (("time", "y", "x"), np.array(swe_vals, dtype="float32"))})


def test_hook_derives_scov_and_masks_fill():
    # one time slice, 1x3: [fill=-9999, dry=0, snow=5]
    ds = _ds([[[-9999.0, 0.0, 5.0]]])
    out = _snodas_hook(ds)
    swe = out["swe"].values.ravel()
    scov = out["scov"].values.ravel()
    assert np.isnan(swe[0])                    # fill masked to NaN
    assert np.isnan(scov[0])                    # scov NaN where fill (not counted as 0)
    assert scov[1] == 0.0                       # dry -> 0
    assert scov[2] == 1.0                       # snow -> 1


def test_adapter_settings():
    assert SNODAS_ADAPTER.variables == ("swe", "scov")
    assert SNODAS_ADAPTER.stat_method == "masked_mean"
    assert SNODAS_ADAPTER.source_crs == "EPSG:5070"
    assert SNODAS_ADAPTER.grid_variable == "swe"
    assert SNODAS_ADAPTER.files_glob == "snodas_daily_*.nc"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_aggregate_snodas.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/aggregate/snodas.py
"""SNODAS daily SWE adapter: area-weighted mean SWE + snow-covered-area fraction.

SWE arrives pre-projected to EPSG:5070 (1000 m) with -9999 fill over ocean /
non-CONUS / Great Lakes. The hook re-asserts that fill as NaN and derives a
binary snow-cover field `scov = (swe > 0)` that *carries the NaN mask* (fill
pixels stay NaN, not 0), so under masked_mean the HRU `scov` is the
area-weighted fraction of finite pixels with snow — the Driscoll et al. SCA.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from .adapter import SourceAdapter

_SNODAS_FILL_THRESHOLD = -9990  # -9999 fill, with slop margin


def _snodas_hook(ds: xr.Dataset) -> xr.Dataset:
    swe = ds["swe"].where(ds["swe"] > _SNODAS_FILL_THRESHOLD)
    scov = xr.where(swe.notnull(), (swe > 0).astype("float32"), np.float32("nan"))
    return ds.assign(swe=swe, scov=scov)


SNODAS_ADAPTER = SourceAdapter(
    source_key="snodas",
    variables=("swe", "scov"),
    files_glob="snodas_daily_*.nc",
    source_crs="EPSG:5070",
    x_coord="x",
    y_coord="y",
    time_coord="time",
    stat_method="masked_mean",
    pre_aggregate_hook=_snodas_hook,
    grid_variable="swe",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_aggregate_snodas.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/aggregate/snodas.py tests/test_aggregate_snodas.py
git commit -m "feat(aggregate): SNODAS adapter deriving scov (SCA) alongside SWE"
```

---

## Task 4: Stage 1 config + orchestrator

**Files:**
- Create: `configs/aggregate/aggregate_sources.yml`
- Create: `scripts/derive_aggregate.py`
- Test: `tests/test_derive_aggregate.py`

**Interfaces:**
- Consumes: `SNODAS_ADAPTER` (Task 3), `aggregate_source` (Task 2), `load_config`/`require_config_key` (existing `gfv2_params.config`).
- Produces: a CLI `python scripts/derive_aggregate.py --source snodas --fabric <name> [--years 2010 2011]` that resolves the fabric profile and writes per-year NCs to `{data_root}/{fabric}/snodas/`.

Config (fabric-agnostic; `snodas_dir` default points at the shared datastore, overridable per profile):

```yaml
# configs/aggregate/aggregate_sources.yml
# Stage 1: aggregate gridded time-series sources to the HRU fabric.
# Fabric-agnostic — per-fabric fabric gpkg / id_feature come from base_config.yml.
defaults:
  output_dir:  "{data_root}/{fabric}/snodas"
  weight_dir:  "{data_root}/{fabric}/weights_agg"

sources:
  - name: snodas
    # Shared raw datastore; a profile may override `snodas_dir`.
    snodas_dir: "{data_root}/../nhf-datastore/snodas/daily"
    output_prefix: snodas
```

```python
# scripts/derive_aggregate.py
"""Stage 1 driver: aggregate a gridded source to the active fabric (per year).

Resolves the fabric geopackage + id_feature from the base_config.yml profile and
writes one per-HRU per-day NetCDF per calendar year. Fabric-agnostic.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import geopandas as gpd

from gfv2_params.aggregate import aggregate_source
from gfv2_params.aggregate.snodas import SNODAS_ADAPTER
from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging

ADAPTERS = {"snodas": SNODAS_ADAPTER}


def _resolve(value, repl: dict):
    if isinstance(value, str):
        for ph, rep in repl.items():
            value = value.replace(f"{{{ph}}}", str(rep))
        return value
    if isinstance(value, list):
        return [_resolve(v, repl) for v in value]
    if isinstance(value, dict):
        return {k: _resolve(v, repl) for k, v in value.items()}
    return value


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, choices=sorted(ADAPTERS))
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--years", nargs="*", type=int, default=None)
    ap.add_argument("--config", default="configs/aggregate/aggregate_sources.yml")
    ap.add_argument("--base_config", default="configs/base_config.yml")
    args = ap.parse_args()
    logger = configure_logging("derive_aggregate")

    cfg = load_config(Path(args.config), base_config_path=Path(args.base_config),
                      fabric=args.fabric)
    repl = {"data_root": cfg["data_root"], "fabric": cfg["fabric"]}
    cfg = {k: _resolve(v, repl) for k, v in cfg.items()}

    src = next(s for s in cfg["sources"] if s["name"] == args.source)
    # snodas_dir may be overridden in the profile; fall back to the source entry.
    snodas_dir = _resolve(cfg.get("snodas_dir", src["snodas_dir"]), repl)

    hru_gpkg = require_config_key(cfg, "hru_gpkg", "derive_aggregate")
    hru_layer = cfg.get("hru_layer", "nhru")
    id_feature = require_config_key(cfg, "id_feature", "derive_aggregate")

    fabric_gdf = gpd.read_file(hru_gpkg, layer=hru_layer)
    logger.info("Fabric %s: %d HRUs (id=%s)", args.fabric, len(fabric_gdf), id_feature)

    out = aggregate_source(
        ADAPTERS[args.source], fabric_gdf, id_feature,
        input_dir=Path(snodas_dir),
        output_dir=Path(cfg["output_dir"]),
        weight_file=Path(cfg["weight_dir"]) / f"{args.source}_weights_{args.fabric}.csv",
        output_prefix=src["output_prefix"],
        years=args.years,
    )
    logger.info("Wrote %d per-year files to %s", len(out), cfg["output_dir"])


if __name__ == "__main__":
    main()
```

- [ ] **Step 1: Write the failing test** (config resolution only — no gdptools/HPC)

```python
# tests/test_derive_aggregate.py
from pathlib import Path

from gfv2_params.config import load_config


def test_snodas_source_entry_resolves():
    cfg = load_config(
        Path("configs/aggregate/aggregate_sources.yml"),
        base_config_path=Path("configs/base_config.yml"),
        fabric="oregon",
    )
    assert cfg["fabric"] == "oregon"
    src = next(s for s in cfg["sources"] if s["name"] == "snodas")
    assert src["output_prefix"] == "snodas"
    # output_dir placeholder present pre-resolution
    assert "{fabric}" in cfg["output_dir"] or "oregon" in cfg["output_dir"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_derive_aggregate.py -v`
Expected: FAIL — config file does not exist yet.

- [ ] **Step 3: Write minimal implementation** — create the two files above.

- [ ] **Step 4: Run test + import check**

Run: `pixi run -e dev pytest tests/test_derive_aggregate.py -v`
Expected: PASS.
Run: `pixi run python -c "import ast; ast.parse(open('scripts/derive_aggregate.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add configs/aggregate/aggregate_sources.yml scripts/derive_aggregate.py tests/test_derive_aggregate.py
git commit -m "feat(aggregate): Stage 1 config + fabric-agnostic derive_aggregate driver"
```

---

## Task 5: Run Stage 1 on Oregon + validate against the SWE oracle

**Files:** none (a run + validation task; produces on-disk NCs under `{data_root}/oregon/snodas/`).

**Interfaces:** consumes `scripts/derive_aggregate.py`. Produces `snodas_agg_<year>.nc` (dims `time`, `hru_id`; vars `swe`, `scov`) — the input Stage 2 reads.

- [ ] **Step 1: Verify the HRU-id identity assumption (spec §7).** Confirm the gfv2-params Oregon `hru_id` matches the nhf-spatial-targets `nhm_id` on the shared fabric.

Run:
```bash
pixi run python - <<'PY'
import geopandas as gpd, xarray as xr
gdf = gpd.read_file("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/oregon/fabric/model_layers 9.gpkg", layer="nhru")
ora = xr.open_dataset("/caldera/hovenweep/projects/usgs/water/impd/nhgf/or-spatial-targets/data/aggregated/snodas/snodas_2010_agg.nc")
print("gfv2 hru_id: n=", len(gdf), "min/max", gdf["hru_id"].min(), gdf["hru_id"].max())
print("oracle nhm_id: n=", ora.sizes.get("nhm_id"), "min/max", int(ora["nhm_id"].min()), int(ora["nhm_id"].max()))
PY
```
Expected: both 16,814 HRUs with identical id ranges (1..16814). If ranges differ, STOP and build a crosswalk before Step 3 (record the finding in the plan/spec).

- [ ] **Step 2: Run the aggregation for two years (fast smoke)** from a shell with `~/.pixi/bin` on PATH:

```bash
pixi run python scripts/derive_aggregate.py --source snodas --fabric oregon --years 2010 2011
```
Expected: writes `snodas_agg_2010.nc`, `snodas_agg_2011.nc` under `.../oregon/snodas/`, each with `swe` and `scov`.

- [ ] **Step 3: Validate SWE against the oracle** (must match within tolerance):

```bash
pixi run python - <<'PY'
import numpy as np, xarray as xr
ours = xr.open_dataset(".../oregon/snodas/snodas_agg_2010.nc")   # fill real path
ora  = xr.open_dataset(".../or-spatial-targets/data/aggregated/snodas/snodas_2010_agg.nc")
a = ours["swe"].rename({"hru_id":"id"}).sortby("id")
b = ora["swe"].rename({"nhm_id":"id"}).sortby("id")
# align on common time + id, compare finite cells
common_t = np.intersect1d(a["time"], b["time"])
a, b = a.sel(time=common_t), b.sel(time=common_t)
diff = (a - b).values
finite = np.isfinite(diff)
print("max abs diff:", np.nanmax(np.abs(diff[finite])), "mean:", np.nanmean(np.abs(diff[finite])))
PY
```
Expected: max abs diff within a few mm (small numerical/weight differences), confirming the ported harness reproduces the proven SWE. Record the number. If large, debug CRS/coord/weights before proceeding.

- [ ] **Step 4: Sanity-check `scov`.** Confirm `scov` ∈ [0,1], is ~1 in deep winter for Cascade HRUs, and 0/NaN in summer. No commit (data artifacts only); note results in the PR description.

---

## Task 6: per-season SDC (`season.py`)

**Files:**
- Create: `src/gfv2_params/snarea/__init__.py` (empty re-export stub; populate in Task 9)
- Create: `src/gfv2_params/snarea/season.py`
- Test: `tests/test_snarea_season.py`

**Interfaces:**
- Produces:
  - `SWE_LEVELS: np.ndarray` (the 11 levels, descending)
  - `melt_season(swe: pd.Series, sca: pd.Series) -> tuple[pd.Series, pd.Series] | None` (peak→first-zero window; `None` if no snow / never returns to zero)
  - `remove_reversals(swe: pd.Series, sca: pd.Series) -> tuple[pd.Series, pd.Series]` (running-minimum envelope on SCA → idealized monotonic melt)
  - `normalize_curve(swe: pd.Series, sca: pd.Series) -> tuple[np.ndarray, np.ndarray]` (`swe_n = swe/peak`, `sca_n = sca/sca_at_peak`)
  - `annual_sdc(swe: pd.Series, sca: pd.Series) -> np.ndarray | None` (11-vector of SCA at `SWE_LEVELS`, or `None` if the season is unusable)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_snarea_season.py
import numpy as np
import pandas as pd

from gfv2_params.snarea.season import (
    SWE_LEVELS, melt_season, remove_reversals, normalize_curve, annual_sdc,
)


def _series(vals):
    idx = pd.date_range("2010-02-01", periods=len(vals), freq="D")
    return pd.Series(vals, index=idx, dtype="float64")


def test_swe_levels():
    assert list(SWE_LEVELS) == [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]


def test_melt_season_trims_to_peak_and_first_zero():
    swe = _series([1, 3, 10, 6, 2, 0, 0, 4])   # peak at idx2, zero at idx5
    sca = _series([0.2, .5, 1, .8, .4, 0, 0, .6])
    ms = melt_season(swe, sca)
    assert ms is not None
    swe_m, _ = ms
    assert swe_m.iloc[0] == 10 and swe_m.iloc[-1] == 0
    assert len(swe_m) == 4                      # idx2..idx5


def test_melt_season_none_when_no_snow():
    assert melt_season(_series([0, 0, 0]), _series([0, 0, 0])) is None


def test_remove_reversals_enforces_monotonic_sca():
    # SCA dips then rises (snowfall reversal) then falls again
    swe = _series([10, 8, 9, 5, 0])
    sca = _series([1.0, 0.6, 0.9, 0.4, 0.0])
    _, sca_r = remove_reversals(swe, sca)
    # kept SCA must be non-increasing
    assert list(sca_r.values) == sorted(sca_r.values, reverse=True)


def test_annual_sdc_shape_and_endpoints():
    swe = _series([10, 8, 6, 4, 2, 0])
    sca = _series([1.0, 0.9, 0.7, 0.5, 0.3, 0.0])
    curve = annual_sdc(swe, sca)
    assert curve is not None and curve.shape == (11,)
    assert curve[0] == 1.0                        # at swe_n=1 -> sca_n=1
    np.testing.assert_allclose(curve[-1], 0.0, atol=1e-9)  # at swe_n=0 -> ~0
    assert np.all(np.diff(curve) <= 1e-9)         # non-increasing across levels
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_season.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/snarea/season.py
"""Per-calendar-year snow-depletion-curve extraction from daily SWE/SCA.

Implements the Driscoll et al. (2017) melt-season curve: isolate peak→SWE=0,
remove post-peak snowfall reversals (idealized monotonic melt), normalize, and
sample SCA at the 11 fixed normalized-SWE levels.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SWE_LEVELS = np.round(np.arange(1.0, -0.0001, -0.1), 1)  # 1.0 .. 0.0, 11 values


def melt_season(swe: pd.Series, sca: pd.Series):
    """Trim to the melt window: date of peak SWE → first day SWE returns to 0.

    Returns (swe_window, sca_window) or None if the HRU-year has no snow or SWE
    never returns to 0 within the series (persistent snowfield / truncated year).
    """
    if swe.max() <= 0:
        return None
    peak_pos = int(np.argmax(swe.values))
    after = swe.iloc[peak_pos:]
    zero_positions = np.where(after.values <= 0)[0]
    if len(zero_positions) == 0:
        return None                        # never melts out — flagged upstream
    end_pos = peak_pos + int(zero_positions[0])
    return swe.iloc[peak_pos:end_pos + 1], sca.iloc[peak_pos:end_pos + 1]


def remove_reversals(swe: pd.Series, sca: pd.Series):
    """Keep only the running-minimum envelope of SCA (idealized monotonic melt).

    Reproduces the paper's rule that post-peak snowfall (an SCA increase) is
    removed until SCA returns to its pre-increase value: a point survives only
    if its SCA is ≤ the smallest SCA kept so far.
    """
    keep = []
    running_min = np.inf
    for i, val in enumerate(sca.values):
        if val <= running_min:
            keep.append(i)
            running_min = val
    return swe.iloc[keep], sca.iloc[keep]


def normalize_curve(swe: pd.Series, sca: pd.Series):
    """Normalize SWE by peak SWE and SCA by its value at peak SWE."""
    peak_swe = swe.iloc[0]
    sca_at_peak = sca.iloc[0]
    swe_n = (swe.values / peak_swe) if peak_swe > 0 else np.zeros(len(swe))
    sca_n = (sca.values / sca_at_peak) if sca_at_peak > 0 else np.zeros(len(sca))
    return swe_n, sca_n


def annual_sdc(swe: pd.Series, sca: pd.Series):
    """Return the 11-point SDC (SCA at each SWE_LEVEL) or None if unusable."""
    ms = melt_season(swe, sca)
    if ms is None:
        return None
    swe_w, sca_w = remove_reversals(*ms)
    if len(swe_w) < 2:
        return None
    swe_n, sca_n = normalize_curve(swe_w, sca_w)
    # np.interp needs ascending x; swe_n descends over the melt, so sort ascending.
    order = np.argsort(swe_n)
    xs, ys = swe_n[order], sca_n[order]
    curve = np.interp(SWE_LEVELS, xs, ys, left=ys[0], right=ys[-1])
    # Enforce monotonic non-increasing across descending SWE levels (numerical guard).
    curve = np.minimum.accumulate(curve)
    curve[0] = min(curve[0], 1.0)
    return np.clip(curve, 0.0, 1.0)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_season.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/__init__.py src/gfv2_params/snarea/season.py tests/test_snarea_season.py
git commit -m "feat(snarea): per-season SDC extraction (melt window, reversals, normalize, 11-pt)"
```

---

## Task 7: representative curve (`representative.py`)

**Files:**
- Create: `src/gfv2_params/snarea/representative.py`
- Test: `tests/test_snarea_representative.py`

**Interfaces:**
- Consumes: annual SDCs from Task 6 (each an 11-vector).
- Produces:
  - `median_sdc(annual: np.ndarray) -> np.ndarray` (input `(n_years, 11)` → `(11,)`)
  - `similarity(annual: np.ndarray, median: np.ndarray) -> float` (Eq. 1: sum of |curve − median| over all points, divided by number of points)
  - `select_representative(annual: np.ndarray, median: np.ndarray) -> np.ndarray` (the year-row with the smallest mean-abs distance to the median)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_snarea_representative.py
import numpy as np

from gfv2_params.snarea.representative import median_sdc, similarity, select_representative


def test_median_elementwise():
    annual = np.array([[0.0] * 11, [0.5] * 11, [1.0] * 11])
    np.testing.assert_allclose(median_sdc(annual), [0.5] * 11)


def test_similarity_zero_for_identical():
    med = np.linspace(1, 0, 11)
    annual = np.stack([med, med])
    assert similarity(annual, med) == 0.0


def test_similarity_positive_and_scaled_by_points():
    med = np.zeros(11)
    annual = np.array([np.ones(11)])       # each of 11 points off by 1
    # sum(|1-0|)=11 over 11 points/curve -> 11/11 = 1.0
    assert similarity(annual, med) == 1.0


def test_select_representative_picks_closest_year():
    med = np.linspace(1, 0, 11)
    far = np.zeros(11)
    near = med + 0.01
    annual = np.stack([far, near])
    rep = select_representative(annual, med)
    np.testing.assert_allclose(rep, near)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_representative.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/snarea/representative.py
"""Median SDC, inter-annual similarity (Eq. 1), and representative-curve pick."""

from __future__ import annotations

import numpy as np


def median_sdc(annual: np.ndarray) -> np.ndarray:
    """Elementwise median across the annual SDCs (rows = years, cols = levels)."""
    return np.median(annual, axis=0)


def similarity(annual: np.ndarray, median: np.ndarray) -> float:
    """Driscoll Eq. 1: Σ over years and points of |SDC − median| ÷ points.

    A per-HRU scalar; 0 = identical curves every year, larger = more inter-annual
    variability. `points` is the number of curve points (11).
    """
    points = annual.shape[1]
    return float(np.sum(np.abs(annual - median)) / points)


def select_representative(annual: np.ndarray, median: np.ndarray) -> np.ndarray:
    """The single year's SDC closest (min mean-abs distance) to the median."""
    dist = np.mean(np.abs(annual - median), axis=1)
    return annual[int(np.argmin(dist))]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_representative.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/representative.py tests/test_snarea_representative.py
git commit -m "feat(snarea): median/similarity/representative SDC selection (Eq. 1)"
```

---

## Task 8: selection criteria + classification (`selection.py`)

**Files:**
- Create: `src/gfv2_params/snarea/selection.py`
- Test: `tests/test_snarea_selection.py`

**Interfaces:**
- Produces:
  - `SelectionParams` dataclass: `min_cells:int=25`, `max_water_frac:float=0.5`, `min_seasonal_sca:float=0.5`, `max_constant_frac:float=0.8`, `max_similarity:float=0.15` (all overridable from config).
  - `passes_selection(*, has_snow: bool, n_cells: int, water_frac: float, seasonal_sca_max: float, constant_frac: float, similarity_value: float, params: SelectionParams) -> tuple[bool, str]` → `(True, "derived")` or `(False, "default_<reason>")`.
  - `classify(rep_sdc: np.ndarray) -> str` — `"low"` (<0.45), `"high"` (>0.55), else `"mid"`, from SCA at `SWE_LEVELS[5]` (0.5).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_snarea_selection.py
import numpy as np

from gfv2_params.snarea.selection import SelectionParams, passes_selection, classify


def _ok(**over):
    base = dict(has_snow=True, n_cells=100, water_frac=0.1,
                seasonal_sca_max=0.9, constant_frac=0.3, similarity_value=0.05,
                params=SelectionParams())
    base.update(over)
    return passes_selection(**base)


def test_passes_when_all_good():
    assert _ok() == (True, "derived")


def test_reasons():
    assert _ok(has_snow=False)[1] == "default_no_snow"
    assert _ok(n_cells=10)[1] == "default_too_few_cells"
    assert _ok(water_frac=0.8)[1] == "default_water_dominated"
    assert _ok(seasonal_sca_max=0.2)[1] == "default_low_sca"
    assert _ok(constant_frac=0.95)[1] == "default_constant_sca"
    assert _ok(similarity_value=0.5)[1] == "default_dissimilar"


def test_classify():
    lo = np.array([1, .9, .8, .7, .6, .40, .3, .2, .1, .05, 0.0])
    hi = np.array([1, .95, .9, .85, .8, .70, .6, .5, .3, .1, 0.0])
    mid = np.array([1, .9, .8, .7, .6, .50, .4, .3, .2, .1, 0.0])
    assert classify(lo) == "low"
    assert classify(hi) == "high"
    assert classify(mid) == "mid"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_selection.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/snarea/selection.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_selection.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/selection.py tests/test_snarea_selection.py
git commit -m "feat(snarea): six-criteria HRU selection + low/mid/high classification"
```

---

## Task 9: assemble per-HRU param table + default fallback (`build.py`)

**Files:**
- Create: `src/gfv2_params/snarea/build.py`
- Modify: `src/gfv2_params/snarea/__init__.py` (re-export `build_snarea_curve`)
- Test: `tests/test_snarea_build.py`

**Interfaces:**
- Consumes: Tasks 6–8 (`annual_sdc`, `median_sdc`, `similarity`, `select_representative`, `passes_selection`, `classify`, `SelectionParams`, `SWE_LEVELS`).
- Produces:
  - `DEFAULT_SNAREA_CURVE: np.ndarray` (documented placeholder default; swap when the real NHM default is staged).
  - `build_hru_record(hru_id, daily: pd.DataFrame, n_cells: int, water_frac: float, params: SelectionParams, default_curve: np.ndarray) -> dict` — one HRU's full result row.
  - `build_snarea_curve(daily_by_hru: dict[int, pd.DataFrame], cells_by_hru: dict[int,int], water_by_hru: dict[int,float], id_feature: str, params: SelectionParams, default_curve: np.ndarray) -> pandas.DataFrame` — the full per-HRU table.

Output columns: `<id_feature>, hru_deplcrv, snarea_curve_0..snarea_curve_10, sdc_status, sca_class, similarity, n_seasons`. `hru_deplcrv` is 1:1 (each HRU indexes its own curve; assigned as row order / HRU id per the fabric's PRMS export convention).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_snarea_build.py
import numpy as np
import pandas as pd

from gfv2_params.snarea.build import DEFAULT_SNAREA_CURVE, build_hru_record
from gfv2_params.snarea.selection import SelectionParams


def _daily_two_years():
    # two clean melt seasons in 2010 and 2011, snow present, monotonic
    frames = []
    for yr in (2010, 2011):
        idx = pd.date_range(f"{yr}-02-01", periods=6, freq="D")
        frames.append(pd.DataFrame(
            {"swe": [10, 8, 6, 4, 2, 0], "sca": [1.0, .9, .7, .5, .3, 0.0]}, index=idx))
    return pd.concat(frames)


def test_derived_hru_record():
    rec = build_hru_record(
        hru_id=7, daily=_daily_two_years(), n_cells=100, water_frac=0.0,
        params=SelectionParams(), default_curve=DEFAULT_SNAREA_CURVE)
    assert rec["hru_id"] == 7
    assert rec["sdc_status"] == "derived"
    assert rec["n_seasons"] == 2
    assert rec["snarea_curve_0"] == 1.0
    assert rec["snarea_curve_10"] <= 1e-6
    assert rec["sca_class"] in {"low", "mid", "high"}


def test_no_snow_falls_back_to_default():
    idx = pd.date_range("2010-02-01", periods=6, freq="D")
    dry = pd.DataFrame({"swe": [0.0] * 6, "sca": [0.0] * 6}, index=idx)
    rec = build_hru_record(
        hru_id=3, daily=dry, n_cells=100, water_frac=0.0,
        params=SelectionParams(), default_curve=DEFAULT_SNAREA_CURVE)
    assert rec["sdc_status"] == "default_no_snow"
    assert rec["snarea_curve_0"] == DEFAULT_SNAREA_CURVE[0]
    assert rec["snarea_curve_10"] == DEFAULT_SNAREA_CURVE[10]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_snarea_build.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/snarea/build.py
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
from .season import SWE_LEVELS, annual_sdc
from .selection import SelectionParams, classify, passes_selection

# Placeholder: SCA declines linearly with normalized SWE (1.0 → 0.0).
DEFAULT_SNAREA_CURVE = np.round(np.linspace(1.0, 0.0, 11), 4)

_CURVE_COLS = [f"snarea_curve_{i}" for i in range(11)]


def _seasons(daily: pd.DataFrame) -> list[np.ndarray]:
    """One annual SDC per calendar year present in the daily frame."""
    out = []
    for _year, grp in daily.groupby(daily.index.year):
        curve = annual_sdc(grp["swe"], grp["sca"])
        if curve is not None:
            out.append(curve)
    return out


def _constant_frac(daily: pd.DataFrame) -> float:
    """Fraction of snow-present days whose SCA equals the daily max (flat SCA)."""
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
        "hru_deplcrv": hru_id,          # 1:1 index (each HRU → own curve)
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
```

```python
# src/gfv2_params/snarea/__init__.py  (replace the Task 6 stub)
"""SNODAS-derived snow-depletion-curve (snarea_curve) builder."""

from __future__ import annotations

from .build import DEFAULT_SNAREA_CURVE, build_snarea_curve

__all__ = ["build_snarea_curve", "DEFAULT_SNAREA_CURVE"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_snarea_build.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/snarea/build.py src/gfv2_params/snarea/__init__.py tests/test_snarea_build.py
git commit -m "feat(snarea): assemble per-HRU snarea_curve table with default fallback"
```

---

## Task 10: Stage 2 config + orchestrator + daily-NC reader

**Files:**
- Create: `configs/snarea/snarea_curve.yml`
- Create: `scripts/derive_snarea_curve.py`
- Test: `tests/test_derive_snarea_curve.py`

**Interfaces:**
- Consumes: `build_snarea_curve`, `SelectionParams`, `DEFAULT_SNAREA_CURVE`; the per-year NCs from Task 5 (`{data_root}/{fabric}/snodas/snodas_agg_*.nc`, dims `time`,`hru_id`, vars `swe`,`scov`); the weights CSV from Task 5 (for per-HRU cell counts).
- Produces: `read_daily_by_hru(nc_dir: Path, id_dim: str) -> dict[int, pandas.DataFrame]`; `cells_from_weights(weight_file: Path, id_col: str) -> dict[int,int]`; a CLI writing `{data_root}/{fabric}/params/merged/nhm_snarea_curve_params.csv`.

Config:

```yaml
# configs/snarea/snarea_curve.yml
# Stage 2: derive per-HRU snarea_curve from the aggregated daily SWE/SCA.
defaults:
  snodas_agg_dir: "{data_root}/{fabric}/snodas"
  weight_file:    "{data_root}/{fabric}/weights_agg/snodas_weights_{fabric}.csv"
  output_dir:     "{data_root}/{fabric}/params/merged"
  merged_file:    nhm_snarea_curve_params.csv

selection:
  min_cells: 25
  max_water_frac: 0.5
  min_seasonal_sca: 0.5
  max_constant_frac: 0.8
  max_similarity: 0.15

# Placeholder near-linear default (see Task 9). Override with the real NHM default
# when staged. Must be 11 descending values in [0,1].
default_curve: [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]
```

```python
# scripts/derive_snarea_curve.py
"""Stage 2 driver: per-HRU snarea_curve from aggregated daily SWE/SCA.

Water fraction (selection criterion 4) is optional: if a per-HRU water-fraction
source is wired via --water_csv it is used; otherwise it defaults to 0 (a no-op
for the fabric, acceptable for Oregon per spec §8.3).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.snarea import DEFAULT_SNAREA_CURVE, build_snarea_curve
from gfv2_params.snarea.selection import SelectionParams


def read_daily_by_hru(nc_dir: Path, id_dim: str) -> dict:
    """Concatenate per-year NCs into one daily DataFrame per HRU (index=date)."""
    files = sorted(Path(nc_dir).glob("*_agg_*.nc"))
    if not files:
        raise FileNotFoundError(f"No aggregated NCs in {nc_dir}")
    ds = xr.open_mfdataset(files, combine="by_coords")
    df = ds[["swe", "scov"]].to_dataframe().reset_index()
    df = df.rename(columns={"scov": "sca"})
    out = {}
    for hru_id, grp in df.groupby(id_dim):
        s = grp.set_index("time")[["swe", "sca"]].sort_index()
        out[int(hru_id)] = s
    return out


def cells_from_weights(weight_file: Path, id_col: str) -> dict:
    """Per-HRU contributing SNODAS cell count from the gdptools weight table."""
    w = pd.read_csv(weight_file)
    return w.groupby(id_col).size().astype(int).to_dict()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--config", default="configs/snarea/snarea_curve.yml")
    ap.add_argument("--base_config", default="configs/base_config.yml")
    ap.add_argument("--water_csv", default=None,
                    help="Optional CSV with columns <id_feature>,water_frac")
    args = ap.parse_args()
    logger = configure_logging("derive_snarea_curve")

    cfg = load_config(Path(args.config), base_config_path=Path(args.base_config),
                      fabric=args.fabric)
    repl = {"data_root": cfg["data_root"], "fabric": cfg["fabric"]}

    def R(s: str) -> str:
        for ph, rep in repl.items():
            s = s.replace(f"{{{ph}}}", str(rep))
        return s

    id_feature = require_config_key(cfg, "id_feature", "derive_snarea_curve")
    nc_dir = Path(R(cfg["snodas_agg_dir"]))
    weight_file = Path(R(cfg["weight_file"]))
    out_dir = Path(R(cfg["output_dir"]))
    out_dir.mkdir(parents=True, exist_ok=True)

    sel = SelectionParams(**cfg["selection"])
    default_curve = np.asarray(cfg["default_curve"], dtype=float)
    assert default_curve.shape == (11,), "default_curve must have 11 values"

    daily = read_daily_by_hru(nc_dir, id_feature if id_feature in ("hru_id",) else "hru_id")
    cells = cells_from_weights(weight_file, id_feature)
    water = {}
    if args.water_csv:
        wdf = pd.read_csv(args.water_csv)
        water = dict(zip(wdf[id_feature], wdf["water_frac"]))

    table = build_snarea_curve(daily, cells, water, id_feature, sel, default_curve)
    out = out_dir / cfg["merged_file"]
    table.to_csv(out, index=False)
    logger.info("Wrote %d HRU curves -> %s", len(table), out)
    logger.info("Status counts:\n%s", table["sdc_status"].value_counts().to_string())


if __name__ == "__main__":
    main()
```

> **Note on the id dimension:** the aggregated NC's HRU dimension is named after the fabric `id_feature` used at aggregation time (Task 5, `id_feature="hru_id"` for Oregon). If a future fabric aggregates under a different id name, pass it through consistently; the `read_daily_by_hru` call above assumes the NC id-dim equals `id_feature`. Simplify by always grouping on the NC's actual id dim: replace the `id_feature if … else "hru_id"` guard with the single detected non-`time` dim.

- [ ] **Step 1: Write the failing test** (unit-test the two readers on tiny synthetic files)

```python
# tests/test_derive_snarea_curve.py
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from scripts.derive_snarea_curve import cells_from_weights, read_daily_by_hru  # noqa


def test_cells_from_weights(tmp_path):
    wf = tmp_path / "w.csv"
    pd.DataFrame({"hru_id": [1, 1, 1, 2], "wght": [.1, .2, .3, .9]}).to_csv(wf, index=False)
    assert cells_from_weights(wf, "hru_id") == {1: 3, 2: 1}


def test_read_daily_by_hru(tmp_path):
    idx = pd.date_range("2010-02-01", periods=3, freq="D")
    ds = xr.Dataset(
        {"swe": (("time", "hru_id"), np.array([[10, 5], [8, 4], [0, 0]], "float64")),
         "scov": (("time", "hru_id"), np.array([[1, 1], [.8, .5], [0, 0]], "float64"))},
        coords={"time": idx, "hru_id": [1, 2]},
    )
    ds.to_netcdf(tmp_path / "snodas_agg_2010.nc")
    out = read_daily_by_hru(tmp_path, "hru_id")
    assert set(out) == {1, 2}
    assert list(out[1]["swe"].values) == [10, 8, 0]
    assert "sca" in out[1].columns          # scov renamed to sca
```

To make `from scripts.…` importable, add an empty `scripts/__init__.py` if one is not already present (check first; the repo may already import scripts in tests — mirror the existing pattern, e.g. `tests/test_merge_params.py`).

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_derive_snarea_curve.py -v`
Expected: FAIL — config/module missing.

- [ ] **Step 3: Write minimal implementation** — create the config, the script, and (if needed) `scripts/__init__.py`.

- [ ] **Step 4: Run test + import check**

Run: `pixi run -e dev pytest tests/test_derive_snarea_curve.py -v`
Expected: PASS (2 tests).
Run: `pixi run python -c "import ast; ast.parse(open('scripts/derive_snarea_curve.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add configs/snarea/snarea_curve.yml scripts/derive_snarea_curve.py tests/test_derive_snarea_curve.py
git commit -m "feat(snarea): Stage 2 config + derive_snarea_curve driver + NC readers"
```

---

## Task 11: Oregon end-to-end run, sanity checks, and docs

**Files:**
- Modify: `README.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`, `docs/ARCHITECTURE.md` (add the SNODAS→snarea_curve stage + the new `aggregate/` harness).
- Create (optional): `slurm_batch/submit_snarea_curve.sh` if the fabric is run under SLURM (mirror `submit_zonal_params.sh`); for Oregon the two pixi commands below suffice.

**Interfaces:** consumes both drivers end-to-end.

- [ ] **Step 1: Full Oregon Stage 1** (all years):

```bash
pixi run python scripts/derive_aggregate.py --source snodas --fabric oregon
```
Expected: 21 `snodas_agg_<year>.nc` under `.../oregon/snodas/`.

- [ ] **Step 2: Full Oregon Stage 2**:

```bash
pixi run python scripts/derive_snarea_curve.py --fabric oregon
```
Expected: `nhm_snarea_curve_params.csv` with 16,814 rows; a printed `sdc_status` breakdown.

- [ ] **Step 3: Sanity checks** (record numbers in the PR description):

```bash
pixi run python - <<'PY'
import numpy as np, pandas as pd
df = pd.read_csv(".../oregon/params/merged/nhm_snarea_curve_params.csv")  # fill path
cc = [f"snarea_curve_{i}" for i in range(11)]
curves = df[cc].values
print("rows:", len(df))
print("status:\n", df["sdc_status"].value_counts())
print("class:\n", df["sca_class"].value_counts())
# every curve monotonic non-increasing, endpoints in [0,1]
assert np.all(np.diff(curves, axis=1) <= 1e-6), "non-monotonic curve present"
assert curves.min() >= -1e-9 and curves.max() <= 1 + 1e-9
print("all curves monotonic, in [0,1]  ✓")
PY
```
Expected: high-SCA dominance (paper: ~95% high); Cascade/Blue-Mountain HRUs `derived`; low-desert HRUs `default_no_snow`/`default_low_sca`; all curves monotonic in [0,1].

- [ ] **Step 4: Docs** — add a "Snow depletion curves (SNODAS → snarea_curve)" section to `README.md` and `slurm_batch/RUNME.md` (the two `derive_*` commands, inputs, outputs), note the new `src/gfv2_params/aggregate/` harness in `docs/ARCHITECTURE.md`, and record the SWE-oracle validation number from Task 5. Commit:

```bash
git add README.md slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md docs/ARCHITECTURE.md
git commit -m "docs: document SNODAS→snarea_curve pipeline + aggregation harness"
```

- [ ] **Step 5: Pre-push gate**

```bash
pixi run -e dev pre-commit run --all-files
pixi run -e dev pytest tests/ -q
```
Expected: clean. Open a PR from `feat/snodas-snarea-curve`; let CI run the full suite.

---

## Later (out of scope for this plan)

- **CONUS gfv2 run:** same two drivers with `--fabric gfv2`, under the SLURM array + afterok pattern; Stage 1 weights cached once, Stage 2 batched. Set memory per the repo's CONUS guidance. No code change (fabric-independent).
- **Real NHM default curve:** replace `DEFAULT_SNAREA_CURVE` / `default_curve` config with the fabric's actual NHM default when staged.
- **Water fraction (criterion 4):** wire `--water_csv` from the repo's depstor/waterbody per-HRU water fraction for CONUS.
- **Curve clustering** and the **Daymet climate-relation** figures (spec §9).

## Self-Review notes (spec coverage)

- Spec §3 Piece 1 → Tasks 1–2; Piece 2 → Task 3; Piece 3 → Tasks 6–9. ✓
- Spec §3.0 fabric-independence → Global Constraints + Tasks 4/10 (profile-resolved paths, `require_config_key`). ✓
- Spec §4 algorithm (peak/melt/reversals/normalize/11-pt/median/similarity/representative/criteria/classify/fallback) → Tasks 6–9. ✓
- Spec §4 step-1 edge cases (never-melts, peak at boundary) → `melt_season` returns None → default fallback. ✓
- Spec §6 validation (SWE oracle, unit tests, Oregon sanity) → Tasks 5, 11 + per-task tests. ✓
- Spec §7 id-join verify → Task 5 Step 1. ✓
- Spec §8 judgment calls (SCA normalize by peak, cells-from-weights, water no-op) → Tasks 6, 10. ✓
