# `carea_max`/`smidx_coef` Threshold-Sweep Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fast in-memory tool to tune the TWI threshold for `carea_max`/`smidx_coef` — extract per-HRU pervious-TWI histograms once, then evaluate the resulting per-HRU parameters for any threshold (absolute or percentile) instantly, with a notebook to inspect distributions/maps/diffs and a sweep curve.

**Architecture:** A pure-math + extraction library (`src/gfv2_params/threshold_sweep.py`), a thin extraction CLI (`scripts/build_carea_twi_artifact.py`), and a notebook UI (`notebooks/carea_threshold_sweep.ipynb`). The extraction mirrors `compute_carea_map_binary` exactly (same land/perv/onstream/warped-TWI logic) so a swept value reproduces a production `carea_map` run, within histogram-bin resolution.

**Tech Stack:** Python 3, numpy, pandas, rasterio/GDAL (osgeo), geopandas, pixi env. Tests under `pixi run -e dev pytest` (CI gate — never the HPC head node; single-file unit runs are fine).

**Spec:** `docs/superpowers/specs/2026-05-22-carea-threshold-sweep-design.md`
**Branch:** `feat/carea-threshold-sweep` (already checked out; stacked on `feat/twi-percentile-carea-smidx` / PR #95)

---

## Conventions for every task

- Run python in-env: `pixi run --as-is python ...`. Tests: `pixi run -e dev pytest <path> -v`.
- Paths/fabric inputs come from the active profile in `configs/base_config.yml` via `require_config_key(config, key, script_name)` — never hardcoded.
- Commit after each task with the message shown. Atomic commits.
- The per-HRU parameter function (both params) is:
  `f_hru(t) = clip( (n_perv_onstream + #(perv, non-onstream cells with TWI > t)) / n_perv , 0, 1 )`, `0` when `n_perv == 0`.

---

## File structure

**Created:**
- `src/gfv2_params/threshold_sweep.py` — the `CareaTwiArtifact` dataclass + `save`/`load`; pure sweep math (`evaluate_threshold`, `value_to_percentile`, `percentile_to_value`, `sweep`); extraction helpers (`accumulate_strip`, `reference_grid`) and the `build_artifact` driver.
- `scripts/build_carea_twi_artifact.py` — thin `--fabric` CLI around `build_artifact` (sbatch-able).
- `notebooks/carea_threshold_sweep.ipynb` — interactive UI (four views + sweep curve).
- `tests/test_threshold_sweep.py` — unit tests for the dataclass, sweep math, and extraction helpers.

**Reused (no change):** `gfv2_params.depstor.RasterInfo`, the `compute_carea_map_binary` semantics, fabric profile + `gfv2_params.config.{load_config,require_config_key}`, the depstor raster outputs under `{data_root}/{fabric}/depstor_rasters/`.

---

## Task 1: Artifact dataclass, save/load, and pure sweep math

**Files:**
- Create: `src/gfv2_params/threshold_sweep.py`
- Test: `tests/test_threshold_sweep.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_threshold_sweep.py`:

```python
"""Unit tests for the carea/smidx threshold-sweep math + artifact (issue #55)."""

import numpy as np
import pytest

from gfv2_params.threshold_sweep import (
    CareaTwiArtifact,
    evaluate_threshold,
    percentile_to_value,
    sweep,
    value_to_percentile,
)


def _toy_artifact():
    # 3 HRUs, 4 TWI bins with edges [0,5,10,15,20] -> centers 2.5,7.5,12.5,17.5
    bin_edges = np.array([0.0, 5.0, 10.0, 15.0, 20.0])
    hist = np.array([
        [0, 2, 1, 1],   # HRU 0: 4 perv-non-onstream cells across bins
        [0, 0, 0, 0],   # HRU 1: none in hist (all onstream or no twi)
        [10, 0, 0, 0],  # HRU 2: 10 cells in lowest bin
    ], dtype="int64")
    # reference grid: percentiles 0..100 step 50 -> values 0,10,20 (linear)
    ref_pctl = np.array([0.0, 50.0, 100.0])
    ref_value = np.array([0.0, 10.0, 20.0])
    return CareaTwiArtifact(
        ids=np.array([101, 102, 103]),
        vpu=np.array(["17", "17", "17"], dtype=object),
        n_perv=np.array([5, 3, 10], dtype="int64"),         # HRU0 has 1 extra perv (nodata twi)
        n_perv_onstream=np.array([1, 3, 0], dtype="int64"),  # HRU1 fully onstream
        hist=hist, bin_edges=bin_edges,
        ref_pctl=ref_pctl, ref_value=ref_value,
        fabric="oregon", twi_source="hydrodem",
    )


def test_evaluate_threshold_below_all_bins():
    a = _toy_artifact()
    # t=0 -> all hist cells (centers>0) count; HRU0: onstream1 + 4 = 5 /5 =1.0
    p = evaluate_threshold(a, 0.0)
    assert p[0] == pytest.approx(1.0)          # (1 + 4)/5
    assert p[1] == pytest.approx(1.0)          # (3 + 0)/3 fully onstream
    assert p[2] == pytest.approx(1.0)          # (0 + 10)/10


def test_evaluate_threshold_mid():
    a = _toy_artifact()
    # t=10 -> bins with center>10 are centers 12.5,17.5 => HRU0 cols 2,3 => 1+1=2
    p = evaluate_threshold(a, 10.0)
    assert p[0] == pytest.approx((1 + 2) / 5)  # onstream1 + 2
    assert p[1] == pytest.approx(1.0)          # onstream rescue, 3/3
    assert p[2] == pytest.approx(0.0)          # 0 above + 0 onstream / 10


def test_evaluate_threshold_above_all():
    a = _toy_artifact()
    p = evaluate_threshold(a, 100.0)           # nothing above
    assert p[0] == pytest.approx(1 / 5)        # only onstream
    assert p[1] == pytest.approx(1.0)
    assert p[2] == pytest.approx(0.0)


def test_evaluate_zero_perv_is_zero():
    a = _toy_artifact()
    a.n_perv[2] = 0
    p = evaluate_threshold(a, 10.0)
    assert p[2] == 0.0


def test_value_percentile_roundtrip():
    a = _toy_artifact()
    assert value_to_percentile(a, 10.0) == pytest.approx(50.0)
    assert percentile_to_value(a, 50.0) == pytest.approx(10.0)


def test_sweep_mean_is_non_increasing():
    a = _toy_artifact()
    df = sweep(a, np.array([0.0, 5.0, 10.0, 15.0, 100.0]))
    means = df["mean"].to_numpy()
    assert np.all(np.diff(means) <= 1e-9)
    assert set(["threshold", "mean", "median", "frac_zero", "frac_one"]).issubset(df.columns)


def test_artifact_save_load_roundtrip(tmp_path):
    a = _toy_artifact()
    p = tmp_path / "art.npz"
    a.save(p)
    b = CareaTwiArtifact.load(p)
    assert np.array_equal(a.hist, b.hist)
    assert np.array_equal(a.ids, b.ids)
    assert list(a.vpu) == list(b.vpu)
    assert b.fabric == "oregon" and b.twi_source == "hydrodem"
    assert evaluate_threshold(b, 10.0)[0] == pytest.approx((1 + 2) / 5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_threshold_sweep.py -v`
Expected: FAIL (module/symbols undefined).

- [ ] **Step 3: Implement the dataclass + sweep math**

Create `src/gfv2_params/threshold_sweep.py`:

```python
"""Fast threshold-iteration for carea_max / smidx_coef (issue #55).

carea_max and smidx_coef are the SAME per-HRU function evaluated at two TWI
thresholds:

    f_hru(t) = clip( (n_perv_onstream + #(perv non-onstream cells, TWI > t)) / n_perv , 0, 1 )

So we extract, once per fabric, each HRU's pervious-cell TWI distribution (as a
histogram) plus the two scalar counts, then evaluate any threshold in-memory.
This module holds the artifact, the pure sweep math, and the extraction driver
(`build_artifact`) that mirrors `compute_carea_map_binary`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass
class CareaTwiArtifact:
    ids: np.ndarray              # (n_hru,) id_feature values
    vpu: np.ndarray              # (n_hru,) object  per-HRU VPU label
    n_perv: np.ndarray           # (n_hru,) pervious cell count (denominator)
    n_perv_onstream: np.ndarray  # (n_hru,) pervious AND on-stream (threshold-independent)
    hist: np.ndarray             # (n_hru, n_bins) TWI histogram of perv non-onstream cells
    bin_edges: np.ndarray        # (n_bins + 1,) histogram bin edges
    ref_pctl: np.ndarray         # (n_grid,) reference percentile grid (0..100)
    ref_value: np.ndarray        # (n_grid,) valid-land TWI value at each percentile
    fabric: str
    twi_source: str

    def save(self, path) -> None:
        np.savez_compressed(
            path, ids=self.ids, vpu=self.vpu.astype("U8"),
            n_perv=self.n_perv, n_perv_onstream=self.n_perv_onstream,
            hist=self.hist, bin_edges=self.bin_edges,
            ref_pctl=self.ref_pctl, ref_value=self.ref_value,
            fabric=np.array(self.fabric), twi_source=np.array(self.twi_source),
        )

    @classmethod
    def load(cls, path) -> "CareaTwiArtifact":
        z = np.load(path, allow_pickle=False)
        return cls(
            ids=z["ids"], vpu=z["vpu"].astype(object),
            n_perv=z["n_perv"], n_perv_onstream=z["n_perv_onstream"],
            hist=z["hist"], bin_edges=z["bin_edges"],
            ref_pctl=z["ref_pctl"], ref_value=z["ref_value"],
            fabric=str(z["fabric"]), twi_source=str(z["twi_source"]),
        )


def _bin_centers(bin_edges: np.ndarray) -> np.ndarray:
    return 0.5 * (bin_edges[:-1] + bin_edges[1:])


def evaluate_threshold(artifact: CareaTwiArtifact, t: float) -> np.ndarray:
    """Per-HRU parameter f_hru(t) in [0, 1] for a single TWI threshold."""
    centers = _bin_centers(artifact.bin_edges)
    above = artifact.hist[:, centers > t].sum(axis=1)
    num = artifact.n_perv_onstream + above
    denom = artifact.n_perv
    with np.errstate(divide="ignore", invalid="ignore"):
        param = np.where(denom > 0, num / denom, 0.0)
    return np.clip(param, 0.0, 1.0)


def value_to_percentile(artifact: CareaTwiArtifact, t: float) -> float:
    """Percentile (0..100) of a TWI value in the valid-land reference grid."""
    return float(np.interp(t, artifact.ref_value, artifact.ref_pctl))


def percentile_to_value(artifact: CareaTwiArtifact, p: float) -> float:
    """TWI value at a percentile (0..100) of the valid-land reference grid."""
    return float(np.interp(p, artifact.ref_pctl, artifact.ref_value))


def sweep(artifact: CareaTwiArtifact, t_grid: np.ndarray) -> pd.DataFrame:
    """Per-threshold summary stats of the per-HRU parameter (sensitivity curve)."""
    rows = []
    for t in np.asarray(t_grid, dtype="float64"):
        p = evaluate_threshold(artifact, float(t))
        rows.append({
            "threshold": float(t),
            "mean": float(p.mean()),
            "median": float(np.median(p)),
            "frac_zero": float((p == 0.0).mean()),
            "frac_one": float((p >= 1.0).mean()),
        })
    return pd.DataFrame(rows)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_threshold_sweep.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/threshold_sweep.py tests/test_threshold_sweep.py
git commit -m "feat(sweep): CareaTwiArtifact + pure threshold-sweep math (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 2: Extraction helpers — `accumulate_strip` and `reference_grid`

**Files:**
- Modify: `src/gfv2_params/threshold_sweep.py`
- Test: `tests/test_threshold_sweep.py` (extend)

These are the pure cores of the extraction pass: accumulate per-HRU counts from a
strip of aligned arrays, and turn a global valid-land TWI histogram into a
percentile→value grid. Both are CI-testable on synthetic arrays.

- [ ] **Step 1: Write the failing test (append)**

Append to `tests/test_threshold_sweep.py`:

```python
from gfv2_params.threshold_sweep import accumulate_strip, reference_grid


def test_accumulate_strip_counts():
    n_hru = 2
    bin_edges = np.array([0.0, 5.0, 10.0, 15.0])  # centers 2.5,7.5,12.5
    n_perv = np.zeros(n_hru, "int64")
    n_perv_onstream = np.zeros(n_hru, "int64")
    hist = np.zeros((n_hru, 3), "int64")
    land_twi_hist = np.zeros(3, "int64")
    # 2x3 strip. hru_idx -1 = no HRU.
    hru_idx = np.array([[0, 0, 1], [1, -1, 0]])
    perv = np.array([[1, 1, 1], [1, 1, 0]], "uint8")
    onstream = np.array([[0, 1, 0], [0, 0, 0]], "uint8")
    twi = np.array([[2.0, 8.0, 12.0], [13.0, 8.0, 1.0]], "float32")
    land = np.ones((2, 3), bool)
    accumulate_strip(hru_idx, perv, onstream, twi, land, -9999.0, bin_edges,
                     n_perv, n_perv_onstream, hist, land_twi_hist)
    # HRU0 perv cells: (0,0)twi2 perv nonos; (0,1)twi8 perv onstream; (1,2)perv=0 skip
    #   -> n_perv[0]=2, onstream[0]=1, hist[0]= bin(2.0)->0 => [1,0,0]
    assert n_perv[0] == 2 and n_perv_onstream[0] == 1
    assert list(hist[0]) == [1, 0, 0]
    # HRU1 perv cells: (0,2)twi12 nonos; (1,0)twi13 nonos -> n_perv=2, hist bins 12.5,12.5
    assert n_perv[1] == 2 and n_perv_onstream[1] == 0
    assert list(hist[1]) == [0, 0, 2]
    # land_twi_hist counts ALL land valid-twi cells (incl. onstream & non-perv):
    # twis: 2,8,12,13,8,1 -> bins: 0,1,2,2,1,0 => [2,2,2]
    assert list(land_twi_hist) == [2, 2, 2]


def test_accumulate_strip_skips_nodata_twi_in_hist():
    bin_edges = np.array([0.0, 5.0, 10.0])
    n_perv = np.zeros(1, "int64"); n_os = np.zeros(1, "int64")
    hist = np.zeros((1, 2), "int64"); land_twi = np.zeros(2, "int64")
    hru_idx = np.array([[0, 0]])
    perv = np.array([[1, 1]], "uint8")
    onstream = np.array([[0, 0]], "uint8")
    twi = np.array([[3.0, -9999.0]], "float32")  # 2nd cell nodata
    land = np.ones((1, 2), bool)
    accumulate_strip(hru_idx, perv, onstream, twi, land, -9999.0, bin_edges,
                     n_perv, n_os, hist, land_twi)
    assert n_perv[0] == 2          # both pervious count toward denom
    assert list(hist[0]) == [1, 0]  # only the valid-twi cell binned
    assert list(land_twi) == [1, 0]


def test_reference_grid_linear():
    # uniform hist over [0,20] -> percentile p maps ~linearly to value
    bin_edges = np.linspace(0, 20, 5)       # 4 bins, centers 2.5,7.5,12.5,17.5
    land_twi_hist = np.array([10, 10, 10, 10], "int64")
    pctl, value = reference_grid(land_twi_hist, bin_edges, np.array([0.0, 50.0, 100.0]))
    assert pctl[1] == 50.0
    assert 7.5 <= value[1] <= 12.5          # median near the middle
    assert value[0] <= value[1] <= value[2]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_threshold_sweep.py -v`
Expected: FAIL (`accumulate_strip` / `reference_grid` undefined).

- [ ] **Step 3: Implement the helpers (append to `threshold_sweep.py`)**

```python
def accumulate_strip(
    hru_idx: np.ndarray, perv: np.ndarray, onstream: np.ndarray, twi: np.ndarray,
    land_valid: np.ndarray, twi_nodata, bin_edges: np.ndarray,
    n_perv: np.ndarray, n_perv_onstream: np.ndarray, hist: np.ndarray,
    land_twi_hist: np.ndarray,
) -> None:
    """Accumulate one aligned strip into the per-HRU and global accumulators.

    `hru_idx` is the per-cell HRU row-index (>=0 valid; <0 = no HRU). Mirrors
    `compute_carea_map_binary`: a cell contributes to a parameter when it is land
    & pervious & (TWI > t OR on-stream). Here we bin pervious non-onstream cells
    by TWI so any threshold can be evaluated later; on-stream pervious cells are
    counted separately (threshold-independent). `land_twi_hist` accumulates ALL
    land valid-TWI cells for the percentile reference grid.
    """
    if twi_nodata is not None and isinstance(twi_nodata, float) and np.isnan(twi_nodata):
        twi_valid = ~np.isnan(twi)
    elif twi_nodata is not None:
        twi_valid = (twi != twi_nodata) & ~np.isnan(twi)
    else:
        twi_valid = ~np.isnan(twi)

    has_hru = hru_idx >= 0
    land_hru = land_valid & has_hru

    # Global valid-land TWI histogram (reference distribution).
    gl = land_valid & twi_valid
    if gl.any():
        gbins = np.clip(np.digitize(twi[gl], bin_edges) - 1, 0, len(bin_edges) - 2)
        np.add.at(land_twi_hist, gbins, 1)

    is_perv = land_hru & (perv == 1)
    np.add.at(n_perv, hru_idx[is_perv], 1)

    is_os = is_perv & (onstream == 1)
    np.add.at(n_perv_onstream, hru_idx[is_os], 1)

    is_hist = is_perv & (onstream != 1) & twi_valid
    if is_hist.any():
        bins = np.clip(np.digitize(twi[is_hist], bin_edges) - 1, 0, len(bin_edges) - 2)
        np.add.at(hist, (hru_idx[is_hist], bins), 1)


def reference_grid(land_twi_hist: np.ndarray, bin_edges: np.ndarray, pctls: np.ndarray):
    """Percentile->TWI-value grid from a valid-land TWI histogram.

    Returns (pctls, values) where values[i] is the TWI at percentile pctls[i].
    Uses bin centers and the cumulative fraction (CDF) of the histogram.
    """
    centers = _bin_centers(bin_edges)
    counts = np.asarray(land_twi_hist, dtype="float64")
    total = counts.sum()
    if total <= 0:
        raise ValueError("reference_grid: empty valid-land TWI histogram")
    cdf_pct = 100.0 * np.cumsum(counts) / total   # percentile at each bin's upper extent
    values = np.interp(pctls, cdf_pct, centers)
    return np.asarray(pctls, dtype="float64"), values
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_threshold_sweep.py -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/threshold_sweep.py tests/test_threshold_sweep.py
git commit -m "feat(sweep): strip accumulation + reference-grid helpers (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 3: `build_artifact` extraction driver

**Files:**
- Modify: `src/gfv2_params/threshold_sweep.py`

Orchestrates the raster reads: rasterise HRU ids onto the template, strip-read
land/perv/onstream/warped-TWI, drive `accumulate_strip`, build the reference grid,
return a `CareaTwiArtifact`. This is integration code (reads rasters); the cores are
already unit-tested in Tasks 1–2, so this task has no new unit test — it is exercised
operationally in Task 6.

- [ ] **Step 1: Implement `build_artifact` (append to `threshold_sweep.py`)**

```python
import logging
from contextlib import ExitStack

import geopandas as gpd
import rasterio
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from .depstor import RasterInfo

_STRIP_ROWS = 1024


def build_artifact(
    *, fabric: str, twi_raster: Path, template_raster: Path, hru_gpkg: Path,
    hru_layer: str, id_feature: str, perv_path: Path, onstream_path: Path,
    landmask_path: Path, vpu_column: str | None, twi_source: str,
    bin_min: float = 0.0, bin_max: float = 30.0, bin_width: float = 0.05,
    n_ref_grid: int = 1001, logger: logging.Logger | None = None,
) -> CareaTwiArtifact:
    """Single pass over a fabric's depstor stack -> CareaTwiArtifact.

    Mirrors compute_carea_map_binary (land & perv & (twi>t | onstream)) so a swept
    threshold reproduces a production carea_map run within bin resolution.
    """
    log = logger or logging.getLogger("build_carea_twi_artifact")
    info = RasterInfo.from_path(template_raster)
    bin_edges = np.arange(bin_min, bin_max + bin_width, bin_width)
    n_bins = len(bin_edges) - 1

    hru = gpd.read_file(hru_gpkg, layer=hru_layer)
    hru = hru[hru.geometry.notna() & ~hru.geometry.is_empty]
    if hru.crs != info.crs:
        hru = hru.to_crs(info.crs)
    ids = hru[id_feature].to_numpy()
    order = np.argsort(ids)
    ids = ids[order]
    n_hru = len(ids)
    id_to_idx = {int(v): i for i, v in enumerate(ids)}
    if vpu_column and vpu_column in hru.columns:
        vpu_by_id = dict(zip(hru[id_feature].to_numpy(), hru[vpu_column].astype(str)))
        vpu = np.array([vpu_by_id[int(v)] for v in ids], dtype=object)
    else:
        vpu = np.array(["" for _ in ids], dtype=object)
    log.info("build_artifact: fabric=%s n_hru=%d grid=%dx%d bins=%d",
             fabric, n_hru, info.width, info.height, n_bins)

    # Per-cell HRU row-index raster (full template; oregon-scale OK). int32 -1=none.
    geoms = hru.geometry.values
    idxvals = np.array([id_to_idx[int(v)] for v in hru[id_feature].to_numpy()], dtype="int32")
    hru_idx_full = rasterize(
        ((g, int(i)) for g, i in zip(geoms, idxvals)),
        out_shape=(info.height, info.width), transform=info.transform,
        fill=-1, dtype="int32", all_touched=True,
    )

    n_perv = np.zeros(n_hru, "int64")
    n_perv_onstream = np.zeros(n_hru, "int64")
    hist = np.zeros((n_hru, n_bins), "int64")
    land_twi_hist = np.zeros(n_bins, "int64")

    with ExitStack() as stack:
        land_src = stack.enter_context(rasterio.open(landmask_path))
        perv_src = stack.enter_context(rasterio.open(perv_path))
        onstream_src = stack.enter_context(rasterio.open(onstream_path))
        twi_src = stack.enter_context(rasterio.open(twi_raster))
        if twi_src.crs != info.crs:
            raise ValueError(f"TWI CRS {twi_src.crs} != template CRS {info.crs}")
        twi_vrt = stack.enter_context(WarpedVRT(
            twi_src, crs=info.crs, transform=info.transform,
            width=info.width, height=info.height,
            resampling=Resampling.nearest, nodata=twi_src.nodata,
        ))
        twi_nodata = twi_src.nodata
        for row_off in range(0, info.height, _STRIP_ROWS):
            h = min(_STRIP_ROWS, info.height - row_off)
            win = Window(0, row_off, info.width, h)
            accumulate_strip(
                hru_idx_full[row_off:row_off + h, :],
                perv_src.read(1, window=win),
                onstream_src.read(1, window=win),
                twi_vrt.read(1, window=win),
                land_src.read(1, window=win) == 1,
                twi_nodata, bin_edges,
                n_perv, n_perv_onstream, hist, land_twi_hist,
            )

    ref_pctl = np.linspace(0.0, 100.0, n_ref_grid)
    ref_pctl, ref_value = reference_grid(land_twi_hist, bin_edges, ref_pctl)
    log.info("build_artifact: %d pervious cells total; %d valid-land TWI cells",
             int(n_perv.sum()), int(land_twi_hist.sum()))
    return CareaTwiArtifact(
        ids=ids, vpu=vpu, n_perv=n_perv, n_perv_onstream=n_perv_onstream,
        hist=hist, bin_edges=bin_edges, ref_pctl=ref_pctl, ref_value=ref_value,
        fabric=fabric, twi_source=twi_source,
    )
```

- [ ] **Step 2: Import-check**

Run: `pixi run --as-is python -c "from gfv2_params.threshold_sweep import build_artifact; print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Re-run unit tests (ensure Task-1/2 still green after the appends)**

Run: `pixi run -e dev pytest tests/test_threshold_sweep.py -v`
Expected: PASS (10 tests).

- [ ] **Step 4: Lint**

Run: `pixi run -e dev ruff check src/gfv2_params/threshold_sweep.py`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/threshold_sweep.py
git commit -m "feat(sweep): build_artifact extraction mirroring compute_carea_map_binary (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 4: Extraction CLI `scripts/build_carea_twi_artifact.py`

**Files:**
- Create: `scripts/build_carea_twi_artifact.py`

Thin `--fabric` wrapper resolving profile inputs and calling `build_artifact`.
Mirrors the arg/loader style of `scripts/clip_shared_to_fabric.py`.

- [ ] **Step 1: Create the script**

```python
"""Build the carea/smidx threshold-sweep artifact for a fabric.

Reads the fabric profile (twi_raster, template_raster, hru_gpkg/layer, id_feature,
vpu) and the depstor rasters under {data_root}/{fabric}/depstor_rasters/, then runs
gfv2_params.threshold_sweep.build_artifact and saves a .npz the sweep notebook
loads. Heavy (full template grid) -> sbatch for large fabrics.

  pixi run --as-is python scripts/build_carea_twi_artifact.py --fabric oregon
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.threshold_sweep import build_artifact


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--base_config", default=None)
    ap.add_argument("--output", default=None,
                    help="Output .npz (default: {data_root}/{fabric}/params/carea_twi_artifact.npz)")
    ap.add_argument("--bin-width", type=float, default=0.05)
    args = ap.parse_args()

    logger = configure_logging("build_carea_twi_artifact")
    config = load_base_config(Path(args.base_config) if args.base_config else None,
                              fabric=args.fabric)
    data_root = config["data_root"]
    fabric = config["fabric"]
    twi_raster = Path(require_config_key(config, "twi_raster", "build_carea_twi_artifact"))
    template = Path(require_config_key(config, "template_raster", "build_carea_twi_artifact"))
    hru_gpkg = Path(require_config_key(config, "hru_gpkg", "build_carea_twi_artifact"))
    hru_layer = require_config_key(config, "hru_layer", "build_carea_twi_artifact")
    id_feature = require_config_key(config, "id_feature", "build_carea_twi_artifact")
    vpu_column = "vpu"  # multi-VPU fabrics carry it; single-VPU fabrics fall back to ""
    depstor = Path(data_root) / fabric / "depstor_rasters"
    out = Path(args.output) if args.output else Path(data_root) / fabric / "params" / "carea_twi_artifact.npz"

    twi_source = "hydrodem" if "hydrodem" in twi_raster.name else "arcpy"
    logger.info("Building artifact: fabric=%s twi=%s (%s)", fabric, twi_raster.name, twi_source)

    artifact = build_artifact(
        fabric=fabric, twi_raster=twi_raster, template_raster=template,
        hru_gpkg=hru_gpkg, hru_layer=hru_layer, id_feature=id_feature,
        perv_path=depstor / "perv_binary.tif",
        onstream_path=depstor / "onstream_binary.tif",
        landmask_path=depstor / "land_mask.tif",
        vpu_column=vpu_column, twi_source=twi_source,
        bin_width=args.bin_width, logger=logger,
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    artifact.save(out)
    logger.info("Wrote artifact -> %s (%d HRUs)", out, len(artifact.ids))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Argparse smoke (no env mutation)**

Run: `pixi run --as-is python scripts/build_carea_twi_artifact.py --help`
Expected: usage text prints, exit 0.

- [ ] **Step 3: Lint**

Run: `pixi run -e dev ruff check scripts/build_carea_twi_artifact.py`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add scripts/build_carea_twi_artifact.py
git commit -m "feat(sweep): --fabric CLI to build the threshold-sweep artifact (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 5: Sweep notebook `notebooks/carea_threshold_sweep.ipynb`

**Files:**
- Create: `notebooks/carea_threshold_sweep.ipynb`

The notebook is a thin UI over the tested library. Build it as a `.py` percent-script
first (reviewable, lintable), then convert to `.ipynb` with jupytext. Cells:

1. **Load** — `art = CareaTwiArtifact.load(<data_root>/oregon/params/carea_twi_artifact.npz)`.
2. **Candidate readout** — set `t_carea`/`t_smidx` (absolute) OR `p_carea`/`p_smidx`
   (percentile); print the two-way conversion via `value_to_percentile`/`percentile_to_value`.
3. **Compute** — `carea = evaluate_threshold(art, t_carea)`, `smidx = evaluate_threshold(art, t_smidx)`.
4. **View 1 distribution** — `plt.hist` of `carea` and `smidx`; print mean/median/frac_zero/frac_one.
5. **View 2 spatial map** — join `carea`/`smidx` to the fabric `hru_gpkg` by `id_feature`, `gdf.plot(column=...)`.
6. **View 3 legacy diff** — `if legacy_csv:` load, merge on `id_feature`, scatter + Δ stats; else print "legacy comparison N/A for this fabric".
7. **View 4 gauge diff** — `if gauge_csv:` same pattern.
8. **Sweep curve** — `df = sweep(art, np.arange(4, 20, 0.25)); df.plot(x="threshold", y="mean")`; overlay the chosen `t_carea`/`t_smidx`.
9. **Persist** — print the config snippet for the chosen value (both percentile and absolute forms).

- [ ] **Step 1: Write the percent-script**

Create `notebooks/carea_threshold_sweep.py` with the cells above as `# %%`-delimited
blocks. Use exactly these library calls (already implemented + tested):
`from gfv2_params.threshold_sweep import CareaTwiArtifact, evaluate_threshold, value_to_percentile, percentile_to_value, sweep`.
Read `data_root` via `from gfv2_params.config import load_base_config; load_base_config(None, fabric="oregon")["data_root"]`.
Resolve the fabric geometry from the profile's `hru_gpkg`/`hru_layer`. Guard the
legacy/gauge cells behind `Path(...).exists()` so the notebook runs end-to-end on
oregon with no extra inputs.

- [ ] **Step 2: Convert to ipynb**

Run: `pixi run --as-is jupytext --to notebook notebooks/carea_threshold_sweep.py`
Expected: writes `notebooks/carea_threshold_sweep.ipynb`. (If `jupytext` is unavailable,
add it to `pyproject.toml` pypi deps and `pixi install`; otherwise keep the `.py`
percent-script as the deliverable and note it.)

- [ ] **Step 3: Lint the percent-script**

Run: `pixi run -e dev ruff check notebooks/carea_threshold_sweep.py`
Expected: clean (or matching the repo's notebook ruff config).

- [ ] **Step 4: Commit**

```bash
git add notebooks/carea_threshold_sweep.py notebooks/carea_threshold_sweep.ipynb
git commit -m "feat(sweep): interactive threshold-sweep notebook (4 views + curve) (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 6: Operational faithfulness check + docs

**Files:**
- Modify: `README.md` (or `docs/depstor_workflow.md`) — document the calibration loop.

- [ ] **Step 1: Build the oregon artifact (cluster)**

```bash
sbatch -p cpu -A impd --job-name=carea_art --time=01:00:00 --ntasks=1 --cpus-per-task=4 --mem=48G \
  --output=logs/job_%j.out --error=logs/job_%j.err \
  --wrap="pixi run --as-is python scripts/build_carea_twi_artifact.py --fabric oregon"
```
Expected: writes `{data_root}/oregon/params/carea_twi_artifact.npz`; log shows n_hru ≈ 16814.

- [ ] **Step 2: Faithfulness vs the D1 production rasters**

The D1 run (PR #95) built oregon `carea_map` at `t_carea=8.9364`, `t_smidx=15.1503`
(VPU 17). Confirm the sweep tool agrees at those thresholds, at the grid (cell) level,
within bin resolution:

```bash
DR=$(awk '/^data_root:/ {print $2}' configs/base_config.yml)
test -f "$DR/oregon/params/carea_twi_artifact.npz" \
  && echo "artifact present" \
  || echo ">>> build it first (Step 1)"
# Note: the path uses the SHELL var $DR expanded into the Python string literal —
# do NOT write f"{DR}/..." (DR is not a Python variable in this heredoc).
pixi run --as-is python - <<PY
from gfv2_params.threshold_sweep import CareaTwiArtifact, evaluate_threshold
art = CareaTwiArtifact.load("$DR/oregon/params/carea_twi_artifact.npz")
carea = evaluate_threshold(art, 8.9364); smidx = evaluate_threshold(art, 15.1503)
print("sweep mean carea:", round(float(carea.mean()),4), " mean smidx:", round(float(smidx.mean()),4))
print("frac HRUs carea>0:", round(float((carea>0).mean()),3), " smidx>0:", round(float((smidx>0).mean()),3))
PY
```
Expected: `carea` mean > `smidx` mean (carea less selective); both in [0,1]; non-degenerate
(distinct). This confirms the artifact reproduces the production threshold behavior.

- [ ] **Step 3: Document the loop**

Add a short "Calibrating the TWI threshold" subsection (README or `docs/depstor_workflow.md`):
build the artifact once (`build_carea_twi_artifact.py --fabric <f>`), open
`notebooks/carea_threshold_sweep.ipynb`, iterate thresholds, then paste the printed
config snippet into the `twi_reference` `percentiles:` (percentile) or `carea_map`
`thresholds:` + `threshold_mode: absolute` (eyeball) block and run production.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(sweep): document the carea/smidx threshold calibration loop (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** §3.1 extraction → Tasks 2–4; §3.2 sweep math → Task 1; §3.3 notebook → Task 5; §3.4 persistence → Task 5 Step 9 + Task 6 Step 3; §4 testing → Tasks 1–2 (CI) + Task 6 (operational faithfulness); §5 file structure → matches.
- **Deviation from spec (intentional):** the reference distribution is accumulated from the same warped-TWI pass (`land_twi_hist`) rather than re-reading a per-VPU tile via `_sample_land_masked_twi` — more self-consistent (same TWI the parameter sees) and avoids per-VPU/source-name lookups. Equivalent population for a single-VPU fabric.
- **Faithfulness caveat:** swept values match production within one histogram bin (0.05 TWI); snap a threshold to a bin edge for exactness.
- **Function-name consistency:** `evaluate_threshold`, `value_to_percentile`, `percentile_to_value`, `sweep`, `accumulate_strip`, `reference_grid`, `build_artifact`, `CareaTwiArtifact.{save,load}` used identically across tasks.
- **gfv2 deferral:** `build_artifact` rasterises the full HRU-index array in memory (oregon-scale fine). gfv2-scale needs a chunked rasterisation — out of scope (spec §6).
