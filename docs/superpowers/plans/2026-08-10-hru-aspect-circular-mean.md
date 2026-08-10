# hru_aspect Circular Mean Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `nhm_aspect_params.csv`'s arithmetic mean of aspect with TM 6-B9 §603's circular mean, `atan2(mean(sin(aspect)), mean(cos(aspect)))`, excluding flat cells.

**Architecture:** A new per-batch runner clips the aspect and slope CONUS VRTs to the batch's bounding box once, then runs three exactextract passes over that window — raw aspect (the nine legacy columns, unchanged), `sin(aspect)` and `cos(aspect)` over non-flat cells only. It writes one per-batch CSV. `hru_aspect` itself is a two-argument `derived_columns:` transform applied at merge time and re-applied after the KNN fill sweep, so it is always recomputed and never interpolated as a raw number.

**Tech Stack:** Python 3.12, pixi, gdptools (`UserTiffData` + `ZonalGen`, exactextract engine), rioxarray/rasterio, geopandas, pandas, numpy, pytest, SLURM.

**Spec:** [`docs/superpowers/specs/2026-08-10-hru-aspect-circular-mean-design.md`](../specs/2026-08-10-hru-aspect-circular-mean-design.md)
**Issue:** [#201](https://github.com/rmcd-mscb/gfv2-params/issues/201)
**Branch:** `feat/hru-aspect-circular-mean` (already created; the spec is committed on it as `a603583`)

## Global Constraints

- **Never run `pytest` on the HPC head node.** Concurrent geo-library imports trigger shared-FS metadata import storms that hang. CI (`.github/workflows/ci.yml`) is the authoritative gate; it runs `pixi run -e dev pytest tests/ -v -ra` on every PR targeting `main`. Quick `python -m py_compile` and single-module import checks on the head node are fine.
- If you must run tests, do it under `srun` with `--as-is`:
  `srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G pixi run -e dev --as-is pytest tests/ -q`
- **`pre-commit run --all-files` needs `srun` and `--mem=64G`** (the prettier hook OOMs on the login node and still exits 0, so a login-node run is not a pass). Targeted `--files a b c` runs are fine on the login node.
- **Atomic commits.** One deliverable per commit. Do not squash unrelated changes together.
- **Every code change needs a docs check** — `docs/`, `README.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`.
- **Paths come from the config, never hardcoded.** Read fabric inputs with `require_config_key(...)` against the active profile in `configs/base_config.yml`, using `{data_root}` / `{fabric}` / `{vpu}` placeholders.
- **Flat-cell constant:** RichDEM writes `270.0` for `slope == 0`. The flat test is exact equality against `0.0` on the slope raster, never a tolerance and never a test against `aspect == 270`.
- **Data root** for any on-disk work: `pixi run data-root` → `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `src/gfv2_params/raster_ops.py` | Modify (append near `deg_to_fraction`, currently the last function, line ~263) | Add `atan2_deg` — the pure circular-mean transform. |
| `src/gfv2_params/zonal_runners/merge.py` | Modify (lines 16–50) | Allow `derived_columns:` `from:` to be a list; register `atan2_deg`. |
| `src/gfv2_params/zonal_runners/aspect.py` | Create | The three-pass per-batch aspect runner. |
| `src/gfv2_params/zonal_runners/__init__.py` | Modify (re-exports ~line 76, `__all__`, `BATCH_RUNNERS` ~line 100) | Register `script: aspect`. |
| `src/gfv2_params/params_index.py` | Modify (`DeclaredParam` line 39, `_record` line 137) | Sixth field: `derived_columns`. |
| `scripts/merge_and_fill_params.py` | Modify (`run_fill_sweep` ~line 575 only) | Re-derive derived columns after the KNN fill. |
| `configs/zonal/zonal_params.yml` | Modify (aspect entry lines 132–165; slope entry comment ~line 88) | New `script:`, `slope_raster:`, `derived_columns:`, rewritten `prms:` block. |
| `slurm_batch/derive_zonal_params.batch` | Modify (line 10) | `--mem=32G` → `--mem=64G`. |
| `docs/parameter_index.md` | Modify (generated regions + "Known gaps" ~line 281) | Regenerate; rewrite the aspect gap as a resolution. |
| `CLAUDE.md` | Modify (derived_columns rule, lines 437–443) | Keep the prohibition; replace its stale silent-NaN rationale. |
| `docs/ARCHITECTURE.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md` | Modify | `aspect` is no longer a generic `zonal` entry; two rasters; 64G. |
| `tests/test_aspect_zonal.py` | Create | End-to-end runner tests on synthetic rasters. |
| `tests/test_merge_params.py` | Modify (after line 143) | Two-argument `derived_columns`. |
| `tests/test_merge_and_fill_params.py` | Modify | Post-fill re-derivation. |
| `tests/test_zonal_runners_package.py` | Modify (lines 11–46) | `BATCH_RUNNERS` gains a sixth tag. |

---

## Task 1: Two-argument derived columns

**Files:**
- Modify: `src/gfv2_params/raster_ops.py` (append after `deg_to_fraction`, the file's last function)
- Modify: `src/gfv2_params/zonal_runners/merge.py:16-50`
- Test: `tests/test_merge_params.py` (append after `test_apply_derived_columns_is_a_noop_when_undeclared`, line 143)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `gfv2_params.raster_ops.atan2_deg(sin_mean, cos_mean)` → same type as input (Series in, Series out), values in `[0, 360)`, NaN-preserving. `apply_derived_columns(df, derived_columns)` now accepts `{"out": {"from": ["a", "b"], "transform": "atan2_deg"}}` in addition to today's `{"from": "a", ...}` string form. Task 2's config and Task 3's fill re-derivation both rely on this.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_merge_params.py`:

```python
def test_atan2_deg_recovers_the_circular_mean_across_the_wrap():
    """The defect, stated as a test: 350 deg and 10 deg average to 0, not 180.

    Arithmetic mean of [350, 10] is 180 -- due SOUTH for two nearly-north-facing
    cells. That is issue #201 in two numbers.
    """
    sin_mean = pd.Series([(math.sin(math.radians(350)) + math.sin(math.radians(10))) / 2])
    cos_mean = pd.Series([(math.cos(math.radians(350)) + math.cos(math.radians(10))) / 2])
    out = apply_derived_columns(
        pd.DataFrame({"mean_sin": sin_mean, "mean_cos": cos_mean}),
        {"hru_aspect": {"from": ["mean_sin", "mean_cos"], "transform": "atan2_deg"}},
    )
    assert math.isclose(out["hru_aspect"][0], 0.0, abs_tol=1e-9)


def test_atan2_deg_normalises_into_zero_to_360():
    """numpy's arctan2 returns -180..180; PRMS hru_aspect is 0-360 (TM6B9:603)."""
    # 225 deg (south-west) -> arctan2 gives -135 without the modulo.
    df = pd.DataFrame({
        "mean_sin": [math.sin(math.radians(225)), math.sin(math.radians(90))],
        "mean_cos": [math.cos(math.radians(225)), math.cos(math.radians(90))],
    })
    out = apply_derived_columns(
        df, {"hru_aspect": {"from": ["mean_sin", "mean_cos"], "transform": "atan2_deg"}}
    )
    assert math.isclose(out["hru_aspect"][0], 225.0, abs_tol=1e-9)
    assert math.isclose(out["hru_aspect"][1], 90.0, abs_tol=1e-9)
    assert (out["hru_aspect"] >= 0).all() and (out["hru_aspect"] < 360).all()


def test_atan2_deg_preserves_nan_for_an_hru_with_no_sloped_cells():
    """An all-flat HRU has NaN means; hru_aspect must stay NaN, not become 0 (north).

    The fill sweep supplies neighbours' means and Task 3 re-derives from those.
    """
    df = pd.DataFrame({"mean_sin": [float("nan")], "mean_cos": [float("nan")]})
    out = apply_derived_columns(
        df, {"hru_aspect": {"from": ["mean_sin", "mean_cos"], "transform": "atan2_deg"}}
    )
    assert math.isnan(out["hru_aspect"][0])


def test_apply_derived_columns_rejects_a_missing_column_in_a_list_source():
    """The list form gets the same eager validation as the string form."""
    df = pd.DataFrame({"mean_sin": [0.0]})
    with pytest.raises(ValueError, match="not in the merged"):
        apply_derived_columns(
            df, {"x": {"from": ["mean_sin", "absent"], "transform": "atan2_deg"}}
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G pixi run -e dev --as-is pytest tests/test_merge_params.py -q`

Expected: the four new tests FAIL — `ValueError: 'atan2_deg' is not a known transform for derived column 'hru_aspect'. Known: ['deg_to_fraction']`. The existing tests in the file must still PASS.

- [ ] **Step 3: Add `atan2_deg` to `raster_ops.py`**

Append after `deg_to_fraction`:

```python
def atan2_deg(sin_mean, cos_mean):
    """Circular mean in degrees clockwise from north, normalised to [0, 360).

    TM 6-B9 §603: ``hru_aspect = atan2[mean(sin(aspect)), mean(cos(aspect))]``.
    The modulo is load-bearing -- ``np.arctan2`` returns -180..180 and PRMS
    specifies 0-360, so without it every westerly HRU ships as a negative bearing.

    Vectorised over pandas Series (``apply_derived_columns`` hands it whole
    columns). NaN in either argument propagates, which is what an HRU with no
    non-flat cells must produce: the fill sweep supplies interpolated means and
    ``merge_and_fill_params`` re-derives from those.
    """
    return np.degrees(np.arctan2(sin_mean, cos_mean)) % 360.0
```

- [ ] **Step 4: Generalise `apply_derived_columns` in `merge.py`**

Replace the `_TRANSFORMS` dict (lines 19–21) with:

```python
_TRANSFORMS = {
    "deg_to_fraction": raster_ops.deg_to_fraction,
    "atan2_deg": raster_ops.atan2_deg,
}
```

Replace the body of the `for` loop (lines 35–49) with:

```python
    for out_col, spec in (derived_columns or {}).items():
        src, tname = spec["from"], spec["transform"]
        if tname not in _TRANSFORMS:
            raise ValueError(
                f"`{tname}` is not a known transform for derived column '{out_col}'. "
                f"Known: {sorted(_TRANSFORMS)}."
            )
        # `from:` is one column name (deg_to_fraction) or a list of them
        # (atan2_deg needs mean_sin AND mean_cos). Normalised to a list here so
        # the missing-column check below covers both shapes with one code path --
        # a list form that skipped validation would address the wrong columns and
        # return plausible bearings.
        srcs = [src] if isinstance(src, str) else list(src)
        missing = [s for s in srcs if s not in df.columns]
        if missing:
            raise ValueError(
                f"derived column '{out_col}' reads {missing}, which are not in the merged "
                f"frame (columns: {sorted(df.columns)})."
            )
        # The transforms vectorise, so hand them whole Series rather than calling
        # them 361k times per param.
        df[out_col] = _TRANSFORMS[tname](*(df[s].astype(float) for s in srcs))
    return df
```

Also update the docstring's closing paragraph (lines 30–33) to add:

```
    `from:` accepts a single column name or a list of them; the transform's arity
    must match. `hru_slope` reads one column, `hru_aspect` reads two (`mean_sin`,
    `mean_cos`) because a circular mean is not a function of any single statistic.
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G pixi run -e dev --as-is pytest tests/test_merge_params.py tests/test_raster_ops.py -q`

Expected: PASS, including the four pre-existing `derived_columns` tests and `test_apply_derived_columns_rejects_a_missing_source_column` (whose `match="not in the merged"` still holds against the reworded message).

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/raster_ops.py src/gfv2_params/zonal_runners/merge.py tests/test_merge_params.py
git commit -m "$(cat <<'EOF'
feat(merge): two-argument derived_columns + atan2_deg

`from:` now accepts a list of source columns as well as a single name, and
raster_ops gains atan2_deg -- TM 6-B9 §603's circular mean, normalised to
[0, 360) because np.arctan2 returns -180..180 and PRMS specifies 0-360.

A circular mean is not a function of any single statistic, so hru_aspect
cannot use the one-argument form hru_slope does. Refs #201.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: The aspect runner

**Files:**
- Create: `src/gfv2_params/zonal_runners/aspect.py`
- Modify: `src/gfv2_params/zonal_runners/__init__.py` (re-export block ~line 76, `__all__` ~line 84, `BATCH_RUNNERS` ~line 100)
- Modify: `tests/test_zonal_runners_package.py:11-46`
- Test: `tests/test_aspect_zonal.py` (create)

**Interfaces:**
- Consumes: nothing from Task 1 at runtime — the runner does NOT compute `hru_aspect`. It emits the two means that Task 1's `atan2_deg` consumes at merge time.
- Produces: `gfv2_params.zonal_runners.aspect.run_aspect_batch(config: dict, batch_id: int, logger) -> None`, registered as `BATCH_RUNNERS["aspect"]`. Writes exactly one CSV, `{output_dir}/aspect/base_nhm_aspect_{fabric}_batch_{NNNN}_param.csv`, whose columns are `{id_feature}` (as the index), then `count, mean, std, min, 25%, 50%, 75%, max, sum, n_aspect_cells, mean_sin, mean_cos, flat_frac`. Task 4's config declares exactly these names.

Config keys read: `source_type`, `id_feature`, `target_layer`, `fabric`, `source_raster`, `slope_raster`, `batch_dir`, `output_dir`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_aspect_zonal.py`:

```python
"""End-to-end tests for the circular-mean aspect runner (issue #201).

These build real (tiny) GeoTIFFs and a real batch gpkg and run the real
gdptools/exactextract path, rather than mocking it. That is deliberate: the
design leans on exactextract treating NaN as nodata (so masked flat cells drop
out of `mean` and out of `count`), and a mock would assert our belief about
gdptools instead of gdptools' behaviour.
"""

import logging
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from gfv2_params.raster_ops import atan2_deg
from gfv2_params.zonal_runners.aspect import run_aspect_batch

_CELL = 30.0
_ORIGIN_X, _ORIGIN_Y = 0.0, 60.0          # a 2-row grid: y from 60 down to 0
_TRANSFORM = from_origin(_ORIGIN_X, _ORIGIN_Y, _CELL, _CELL)
_CRS = "EPSG:5070"
_LOG = logging.getLogger("test_aspect")


def _write_raster(path: Path, arr: np.ndarray, transform=_TRANSFORM) -> None:
    height, width = arr.shape
    with rasterio.open(
        path, "w", driver="GTiff", height=height, width=width, count=1,
        dtype="float32", crs=_CRS, transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(arr.astype("float32"), 1)


def _write_batch_gpkg(path: Path, id_feature="nat_hru_id", hru_ids=(1,), boxes=None):
    """One polygon per HRU. Default: a single polygon covering the whole grid."""
    if boxes is None:
        boxes = [box(_ORIGIN_X, _ORIGIN_Y - 2 * _CELL, _ORIGIN_X + 4 * _CELL, _ORIGIN_Y)]
    gdf = gpd.GeoDataFrame({id_feature: list(hru_ids)}, geometry=list(boxes), crs=_CRS)
    gdf.to_file(path, layer="nhru", driver="GPKG")


def _make_config(tmp_path, aspect_arr, slope_arr, slope_transform=_TRANSFORM):
    batch_dir = tmp_path / "batches"
    batch_dir.mkdir()
    output_dir = tmp_path / "params"
    output_dir.mkdir()

    _write_raster(tmp_path / "aspect.tif", aspect_arr)
    _write_raster(tmp_path / "slope.tif", slope_arr, transform=slope_transform)
    _write_batch_gpkg(batch_dir / "batch_0000.gpkg")

    return {
        "source_type": "aspect",
        "id_feature": "nat_hru_id",
        "target_layer": "nhru",
        "fabric": "testfab",
        "source_raster": str(tmp_path / "aspect.tif"),
        "slope_raster": str(tmp_path / "slope.tif"),
        "batch_dir": str(batch_dir),
        "output_dir": str(output_dir),
    }


def _read_output(config) -> pd.DataFrame:
    path = (Path(config["output_dir"]) / "aspect"
            / "base_nhm_aspect_testfab_batch_0000_param.csv")
    return pd.read_csv(path)


def test_circular_mean_survives_the_wrap_where_the_arithmetic_mean_does_not(tmp_path):
    """The whole of issue #201, in one HRU.

    Four cells alternate 350 deg and 10 deg -- all essentially north-facing. The
    arithmetic `mean` says 180 (due SOUTH). atan2(mean_sin, mean_cos) says 0.
    Both numbers are in the same file, which is what makes the old product
    auditable after the fix lands.
    """
    aspect = np.array([[350.0, 10.0, 350.0, 10.0],
                       [350.0, 10.0, 350.0, 10.0]])
    slope = np.full((2, 4), 5.0)
    config = _make_config(tmp_path, aspect, slope)

    run_aspect_batch(config, 0, _LOG)
    out = _read_output(config)

    assert math.isclose(out["mean"][0], 180.0, abs_tol=1e-3)          # the defect
    assert math.isclose(atan2_deg(out["mean_sin"], out["mean_cos"])[0], 0.0, abs_tol=1e-3)
    assert math.isclose(out["flat_frac"][0], 0.0, abs_tol=1e-9)


def test_flat_cells_are_excluded_and_counted(tmp_path):
    """RichDEM writes 270 for slope == 0; a flat cell has no down slope direction.

    Half the grid is flat-at-270, half is genuinely 90 (east). The circular mean
    must be 90, not the 180 that including the flats would give.
    """
    aspect = np.array([[90.0, 90.0, 270.0, 270.0],
                       [90.0, 90.0, 270.0, 270.0]])
    slope = np.array([[5.0, 5.0, 0.0, 0.0],
                      [5.0, 5.0, 0.0, 0.0]])
    config = _make_config(tmp_path, aspect, slope)

    run_aspect_batch(config, 0, _LOG)
    out = _read_output(config)

    assert math.isclose(atan2_deg(out["mean_sin"], out["mean_cos"])[0], 90.0, abs_tol=1e-3)
    assert math.isclose(out["flat_frac"][0], 0.5, abs_tol=1e-6)
    assert math.isclose(out["n_aspect_cells"][0], 4.0, abs_tol=1e-6)
    assert math.isclose(out["count"][0], 8.0, abs_tol=1e-6)
    # Pass 1 is UNMASKED: `mean` still averages all eight cells, flats included,
    # so it stays byte-comparable with the pre-fix product.
    assert math.isclose(out["mean"][0], 180.0, abs_tol=1e-3)


def test_an_all_flat_hru_reports_nan_means_not_west(tmp_path):
    """flat_frac == 1 and NaN means, rather than a confident 270 deg."""
    aspect = np.full((2, 4), 270.0)
    slope = np.zeros((2, 4))
    config = _make_config(tmp_path, aspect, slope)

    run_aspect_batch(config, 0, _LOG)
    out = _read_output(config)

    assert math.isnan(out["mean_sin"][0])
    assert math.isnan(out["mean_cos"][0])
    assert math.isclose(out["n_aspect_cells"][0], 0.0, abs_tol=1e-9)
    assert math.isclose(out["flat_frac"][0], 1.0, abs_tol=1e-9)


def test_legacy_columns_match_the_generic_zonal_runner(tmp_path):
    """Pass 1 must reproduce `run_zonal_batch` exactly, flats included.

    If it did not, the retained `mean` column would be a NEW statistic wearing
    the old name and the old-vs-new comparison at rollout would be meaningless.
    """
    from gfv2_params.zonal_runners.zonal import run_zonal_batch

    aspect = np.array([[10.0, 100.0, 200.0, 300.0],
                       [20.0, 110.0, 210.0, 310.0]])
    slope = np.array([[5.0, 5.0, 0.0, 5.0],
                      [5.0, 0.0, 5.0, 5.0]])
    config = _make_config(tmp_path, aspect, slope)
    run_aspect_batch(config, 0, _LOG)
    ours = _read_output(config)

    ref_config = dict(config, source_type="aspect_ref", categorical=False)
    (Path(config["output_dir"]) / "aspect_ref").mkdir(parents=True, exist_ok=True)
    run_zonal_batch(ref_config, 0, _LOG)
    ref = pd.read_csv(
        Path(config["output_dir"]) / "aspect_ref"
        / "base_nhm_aspect_ref_testfab_batch_0000_param.csv"
    )

    for col in ["count", "mean", "std", "min", "25%", "50%", "75%", "max", "sum"]:
        assert math.isclose(ours[col][0], ref[col][0], rel_tol=1e-9, abs_tol=1e-9), col


def test_misaligned_slope_raster_raises(tmp_path):
    """A half-cell offset would mask the flat test against the wrong aspect cells.

    Nothing downstream would ever report it, so it must raise here.
    """
    shifted = from_origin(_ORIGIN_X + _CELL / 2, _ORIGIN_Y, _CELL, _CELL)
    config = _make_config(
        tmp_path, np.full((2, 4), 90.0), np.full((2, 4), 5.0), slope_transform=shifted
    )
    with pytest.raises(ValueError, match="not co-registered"):
        run_aspect_batch(config, 0, _LOG)


def test_writes_exactly_one_csv(tmp_path):
    """gdptools writes a CSV per ZonalGen when zonal_writer == "csv".

    Three passes must not leave three files: `run_merge` globs this directory
    with `base_nhm_aspect_{fabric}_batch_*_param.csv` and would concat strays.
    """
    config = _make_config(tmp_path, np.full((2, 4), 90.0), np.full((2, 4), 5.0))
    run_aspect_batch(config, 0, _LOG)
    written = sorted((Path(config["output_dir"]) / "aspect").glob("*.csv"))
    assert [p.name for p in written] == ["base_nhm_aspect_testfab_batch_0000_param.csv"]
```

Also update `tests/test_zonal_runners_package.py`: add `run_aspect_batch` to the import block (lines 11–20, alphabetically first among the `run_*` names), add `assert callable(run_aspect_batch)` to `test_public_reexports_resolve`, change its docstring from "7 public re-exports" to "8 public re-exports", and in `test_batch_runners_keys_and_identity` change the docstring "5 script: tags" to "6 script: tags" and update the two assertions:

```python
    assert sorted(BATCH_RUNNERS) == ["aspect", "lulc", "lulc_prederived", "soils", "ssflux", "zonal"]
    assert BATCH_RUNNERS["aspect"] is run_aspect_batch
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=4 --mem=16G pixi run -e dev --as-is pytest tests/test_aspect_zonal.py tests/test_zonal_runners_package.py -q`

Expected: FAIL at collection — `ModuleNotFoundError: No module named 'gfv2_params.zonal_runners.aspect'`.

- [ ] **Step 3: Write `src/gfv2_params/zonal_runners/aspect.py`**

```python
"""Per-batch circular-mean aspect stats from the CONUS aspect + slope VRTs.

Drives the ``aspect`` entry in ``configs/zonal/zonal_params.yml``
(``script: aspect``). Separate from the generic ``zonal`` runner because
``hru_aspect`` is a CIRCULAR mean: TM 6-B9 §603 requires
``atan2(mean(sin(aspect)), mean(cos(aspect)))``, and an arithmetic mean of a
wrapped 0-360 field measures how symmetric an HRU's cells are about 180 deg, not
which way it faces. Measured on gfv2's 361,471 HRUs the old column's median was
179.4 deg with an IQR of 151.7-207.5 -- that central tendency is the artifact
(issue #201).

Three exactextract passes over ONE clipped window:

  1. raw aspect, every valid cell -> count, mean, std, min, 25/50/75%, max, sum
     (byte-comparable with the pre-fix product -- see `_STAT_COLUMNS`)
  2. sin(aspect), non-flat cells only -> mean_sin, and count as n_aspect_cells
  3. cos(aspect), non-flat cells only -> mean_cos

``hru_aspect`` itself is NOT computed here. It is a ``derived_columns:`` entry
applied by ``run_merge`` and re-applied after the KNN fill sweep, so it is always
recomputed from the two means rather than concatenated or interpolated -- KNN on a
circular quantity would reintroduce exactly the defect this module exists to fix.

No CONUS sin/cos rasters are built. gdptools already subsets the source to the
batch's bounding box (``UserTiffData.prep_agg_data``) and batches are KD-tree
spatially compact (``gfv2_params.batching``), so the derived arrays are per-batch
and transient: gfv2's largest batch bbox is 0.80e9 cells (3.2 GB float32), median
0.16e9. The alternative -- ~90 GB of new CONUS tiles plus VRTs and overviews --
would also land the two means in SEPARATE merged CSVs that ``derived_columns``
cannot join.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import rioxarray
from gdptools import UserTiffData, ZonalGen

from ..config import require_config_key

# RichDEM does not flag flat cells: rd.TerrainAttribute(dem, attrib="aspect")
# assigns them 270.0 (due west). Measured against the co-registered slope tile on
# a 3000x3000 window per VPU -- VPU 01: 2.79% of cells are slope == 0 and 99.05%
# of aspect == 270 cells are among them; VPU 07: 0.34%; VPU 12: ~0%. No cell
# carries -1 or any other sentinel. ArcGIS, the implementation TM 6-B9 §603
# cites, writes -1 for flats instead, so this is an artifact of the move off
# ArcPy. A flat cell has no down slope direction -- which is what §603 says
# hru_aspect is the mean of -- so flats are excluded rather than voting for west.
#
# The test is exact equality on the SLOPE raster, never `aspect == 270`: a
# genuinely west-facing sloped cell is also 270 and must be kept.
FLAT_SLOPE = 0.0

# Pass 1's columns, in the order gdptools' exactextract engine returns them. This
# is exactly what `run_zonal_batch` emits for a continuous raster; keeping the
# order and the population identical is what makes the retained `mean` an honest
# record of the pre-fix product rather than a new statistic wearing the old name.
_STAT_COLUMNS = ["count", "mean", "std", "min", "25%", "50%", "75%", "max", "sum"]

# Cells of slop added to the batch bounds before clipping. gdptools buffers by a
# cell before its own subset (`_get_shp_bounds_w_buffer`); an exact-bbox clip
# would drop the partially-covered edge cells exactextract weights for
# boundary-touching HRUs, biasing every HRU on a batch seam.
_BOUNDS_BUFFER_CELLS = 2


def _buffered_bounds(gdf, da):
    """Batch bounds in the raster's CRS, grown by `_BOUNDS_BUFFER_CELLS`."""
    minx, miny, maxx, maxy = gdf.total_bounds
    res_x, res_y = (abs(v) for v in da.rio.resolution())
    pad_x = _BOUNDS_BUFFER_CELLS * res_x
    pad_y = _BOUNDS_BUFFER_CELLS * res_y
    return (minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y)


def _assert_co_registered(aspect_da, slope_da, aspect_name: str, slope_name: str) -> None:
    """Both clips must land on the same grid, or the flat mask masks the wrong cells.

    aspect.vrt and slope.vrt both derive from the same per-VPU NEDSnapshot DEM, so
    this holds by construction today. It is checked anyway because the failure is
    silent: a half-cell offset would exclude the neighbours of the flat cells and
    keep the flats, and every downstream number would still look plausible.
    """
    if aspect_da.shape != slope_da.shape or aspect_da.rio.transform() != slope_da.rio.transform():
        raise ValueError(
            f"aspect and slope clips are not co-registered: {aspect_name} is "
            f"{aspect_da.shape} at {aspect_da.rio.transform()}, {slope_name} is "
            f"{slope_da.shape} at {slope_da.rio.transform()}. Both must come from "
            f"the same DEM lattice."
        )


def _zonal_means(da, nhru_gdf, id_feature: str, var_name: str, out_dir: Path):
    """One exactextract pass over an already-clipped DataArray.

    `zonal_writer=None`: gdptools writes a CSV only when it equals "csv"
    (`ZonalGen.calculate_zonal`) and returns the frame either way. Three passes
    must not leave three files in `out_dir` -- `run_merge` globs it with
    `base_nhm_aspect_{fabric}_batch_*_param.csv` and would concat any stray that
    happened to match. `tests/test_aspect_zonal.py` asserts exactly one CSV is
    written, so a gdptools change here fails loudly.
    """
    data = UserTiffData(
        source_var=var_name,
        source_ds=da,
        source_crs=da.rio.crs,
        source_x_coord="x",
        source_y_coord="y",
        band=1,
        bname="band",
        target_gdf=nhru_gdf,
        target_id=id_feature,
    )
    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer=None,
        out_path=str(out_dir),
        file_prefix=var_name,
        jobs=4,
    )
    return zonal_gen.calculate_zonal(categorical=False)


def run_aspect_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of circular-mean aspect stats.

    Writes a single CSV named to the same pattern the generic runner uses, so
    `run_merge` needs no special case.
    """
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    aspect_path = Path(config["source_raster"])
    slope_path = Path(require_config_key(config, "slope_raster", "run_aspect_batch"))
    batch_gpkg = Path(config["batch_dir"]) / f"batch_{batch_id:04d}.gpkg"
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    for path, label in ((aspect_path, "aspect"), (slope_path, "slope")):
        if not path.exists():
            raise FileNotFoundError(f"Input {label} raster not found: {path}")
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")

    logger.info("Aspect raster: %s", aspect_path)
    logger.info("Slope raster (flat mask): %s", slope_path)
    logger.info("Batch GPKG: %s", batch_gpkg)

    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)",
                target_layer, len(nhru_gdf), batch_id)

    aspect_full = rioxarray.open_rasterio(aspect_path, masked=True)
    slope_full = rioxarray.open_rasterio(slope_path, masked=True)

    bounds = _buffered_bounds(nhru_gdf.to_crs(aspect_full.rio.crs), aspect_full)
    aspect_da = aspect_full.rio.clip_box(*bounds)
    slope_da = slope_full.rio.clip_box(*bounds)
    _assert_co_registered(aspect_da, slope_da, aspect_path.name, slope_path.name)
    logger.info("Clipped both rasters to batch bounds: shape=%s", aspect_da.shape)

    # `slope == 0` is False where slope is NaN, so nodata never counts as flat.
    flat = slope_da == FLAT_SLOPE
    sloped_aspect = aspect_da.where(~flat)
    radians = np.deg2rad(sloped_aspect)
    # Arithmetic on a DataArray keeps coords but the .rio accessor needs the CRS
    # restated before UserTiffData reads it back off the array.
    sin_da = np.sin(radians).rio.write_crs(aspect_da.rio.crs)
    cos_da = np.cos(radians).rio.write_crs(aspect_da.rio.crs)

    raw = _zonal_means(aspect_da, nhru_gdf, id_feature, source_type, output_dir)
    sin_stats = _zonal_means(sin_da, nhru_gdf, id_feature, f"{source_type}_sin", output_dir)
    cos_stats = _zonal_means(cos_da, nhru_gdf, id_feature, f"{source_type}_cos", output_dir)

    # All three frames are indexed by id_feature, so these align by HRU, not by
    # row order.
    out = raw[_STAT_COLUMNS].copy()
    out["n_aspect_cells"] = sin_stats["count"]
    out["mean_sin"] = sin_stats["mean"]
    out["mean_cos"] = cos_stats["mean"]
    # `count` is exactextract's COVERAGE-WEIGHTED cell count, so this is an area
    # fraction, not a cell tally. NaN rather than inf/0 for an HRU with no covered
    # cells at all -- "no data" is not "no flats".
    out["flat_frac"] = np.where(
        out["count"] > 0, 1.0 - out["n_aspect_cells"] / out["count"], np.nan
    )

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"
    out_path = output_dir / f"{file_prefix}.csv"
    out.to_csv(out_path)
    logger.info("Aspect zonal statistics complete. Shape: %s -> %s", out.shape, out_path)
```

- [ ] **Step 4: Register the runner in `src/gfv2_params/zonal_runners/__init__.py`**

Add to the re-export block (it is alphabetical; `aspect` goes first, before `.lulc`):

```python
from .aspect import run_aspect_batch
```

Add `"run_aspect_batch"` to `__all__` (after `"MERGE_REDUCERS"`, keeping the list sorted).

Add to `BATCH_RUNNERS`, and update the comment above it from "Adding a new `script:` tag means" — that text stays correct as-is:

```python
BATCH_RUNNERS = {
    "zonal": run_zonal_batch,
    "aspect": run_aspect_batch,
    "soils": run_soils_batch,
    "lulc": run_lulc_batch,
    "lulc_prederived": run_lulc_prederived_batch,
    "ssflux": run_ssflux_batch,
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=4 --mem=16G pixi run -e dev --as-is pytest tests/test_aspect_zonal.py tests/test_zonal_runners_package.py tests/test_zonal_orchestrator.py -q`

Expected: PASS.

If `test_writes_exactly_one_csv` fails with three files present, gdptools has changed its writer contract — do NOT delete the strays as a workaround. Switch `_zonal_means` to write into a `tempfile.TemporaryDirectory()` per pass and keep the assertion.

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/zonal_runners/aspect.py src/gfv2_params/zonal_runners/__init__.py tests/test_aspect_zonal.py tests/test_zonal_runners_package.py
git commit -m "$(cat <<'EOF'
feat(aspect): circular-mean zonal runner (script: aspect)

Three exactextract passes over one clipped window: raw aspect (the nine
legacy columns, unmasked and byte-comparable with the pre-fix product),
sin(aspect) and cos(aspect) over non-flat cells only.

Flats are excluded because RichDEM assigns them 270 deg rather than a
sentinel -- 2.8% of a VPU 01 window, 99% coincident with slope == 0. ArcGIS,
which TM 6-B9 §603 cites, writes -1. A flat cell has no down slope
direction, so it must not vote for west; flat_frac reports its share.

No CONUS sin/cos rasters: gdptools already subsets to the batch bbox and
batches are KD-tree compact (largest gfv2 bbox 0.80e9 cells), so both means
land in ONE csv where derived_columns can join them. Refs #201.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Re-derive derived columns after the KNN fill

**Files:**
- Modify: `src/gfv2_params/params_index.py:39-92` (`DeclaredParam`), `:137-145` (`_record`)
- Modify: `scripts/merge_and_fill_params.py` — `resolve_fill_plan` signature and NaN census (~lines 75, 176–191), `run_fill_sweep` (~lines 524–576)
- Test: `tests/test_merge_and_fill_params.py`

**Interfaces:**
- Consumes: `apply_derived_columns` from Task 1; the `derived_columns:` config shape Task 4 declares.
- Produces: `DeclaredParam` gains a sixth field, `derived_columns: Mapping = MappingProxyType({})`. `resolve_fill_plan`'s signature is **unchanged**.

**Ruling that shaped this task (2026-08-10, from the pre-flight scan).** An earlier
draft dropped `hru_slope` from `fill_columns` and kept `hru_aspect` out of it, which
collides with [CLAUDE.md:443](../../../CLAUDE.md) — *"Do not 'fix' it by dropping the
column from `fill_columns`."* The user ruled that CLAUDE.md governs. So **both derived
columns stay declared fillable**, and the ordering is what makes them correct:

```
KNN fill (interpolates hru_slope / hru_aspect along with everything else)
        ↓
apply_derived_columns  ← OVERWRITES both from their filled sources
        ↓
write_filled_in_place
```

The interpolated bearing never reaches disk, so the circular-average defect still
cannot ship — and `resolve_fill_plan`'s raise-on-a-declared-column-the-file-lacks
survives as the loud, CI-visible tripwire that fires if a `derived_columns:` block is
ever deleted or a merge never re-run. Guard 2 is data-root-gated and SKIPs in CI, so
it is not a substitute for that tripwire ([test_params_index_ondisk.py:107](../../../tests/test_params_index_ondisk.py#L107)
says so explicitly).

**Consequence, deliberate and loud:** every fabric's aspect CSV currently predates
`hru_aspect`, so the fill sweep will RAISE until that fabric's aspect zonal pass has
been re-run. That is the documented prerequisite behaviour, not a regression. Task 7
sequences the re-run ahead of the fill.

**No NaN-census exemption.** A derived column that is declared fillable is already
skipped by the census's `col in resolved` test, so an exemption would be dead code.
Do not add one.

- [ ] **Step 1: Write the failing tests**

First extend the file's existing `_declared` helper (line 25) so fixtures can carry a
declaration — keep the docstring, it explains why hand-built tuples are banned here:

```python
def _declared(name, merged_file, fill_columns, fabric_columns=None, derived_columns=None):
    return maf.DeclaredParam(
        name, merged_file, fill_columns, fabric_columns or {},
        derived_columns=derived_columns or {},
    )
```

Then add these three tests. The first and third go inside `class TestRunFillSweep`
(line 676) — they use its `self._merged_gdf()`, whose three points are at (0,0),
(10,0) and (5,0) for hru_id 1, 2, 3. The second is module-level.

```python
    def test_derived_columns_are_rederived_after_the_knn_fill(self, tmp_path):
        """hru_aspect must equal atan2 of the FILLED means, not an interpolated bearing.

        HRU 2 is absent, so KNN synthesizes its mean_sin/mean_cos from HRUs 1 and 3
        -- which face 350 deg and 10 deg. Both are essentially north. KNN-filling
        hru_aspect directly would average the two BEARINGS to 180 deg, due south:
        issue #201 reappearing on exactly the HRUs nobody inspects.
        """
        import logging
        import math

        pf = tmp_path / "nhm_aspect_params.csv"
        pd.DataFrame({
            "hru_id": [1, 3],
            "mean_sin": [math.sin(math.radians(350)), math.sin(math.radians(10))],
            "mean_cos": [math.cos(math.radians(350)), math.cos(math.radians(10))],
            "hru_aspect": [350.0, 10.0],
        }).to_csv(pf, index=False)

        # hru_aspect IS declared fillable (CLAUDE.md:443) -- so KNN interpolates it
        # to ~180 and the re-derivation must then OVERWRITE that. If the ordering
        # were wrong, this test's final assertion is what catches it.
        declared = _declared(
            "aspect", "nhm_aspect_params.csv", ["mean_sin", "mean_cos", "hru_aspect"],
            derived_columns={
                "hru_aspect": {"from": ["mean_sin", "mean_cos"], "transform": "atan2_deg"}
            },
        )

        failed = maf.run_fill_sweep(
            [(declared, pf)], self._merged_gdf(), expected_max=3, id_feature="hru_id",
            k_neighbors=2, logger=logging.getLogger("test_rederive_aspect"),
        )
        assert failed == []

        out = pd.read_csv(pf).sort_values("hru_id").reset_index(drop=True)
        assert out["hru_id"].tolist() == [1, 2, 3]
        assert out["hru_aspect"].notna().all()
        # Every row, not just the synthesized one: the derived column is a function
        # of its declared sources by construction.
        expected = np.degrees(np.arctan2(out["mean_sin"], out["mean_cos"])) % 360.0
        np.testing.assert_allclose(out["hru_aspect"].to_numpy(), expected.to_numpy(),
                                   atol=1e-9)
        # And the synthesized HRU faces north (0 or 360), NOT the 180 an arithmetic
        # KNN over the two bearings would have produced.
        filled = float(out.loc[out["hru_id"] == 2, "hru_aspect"].iloc[0])
        assert min(filled, 360.0 - filled) < 1.0

    def test_hru_slope_is_rederived_so_it_agrees_with_its_source(self, tmp_path):
        """SCOPE EXPANSION beyond #201, deliberate: see the spec.

        hru_slope was KNN-filled independently of `mean`, so a synthesized row could
        carry hru_slope != tan(radians(mean)) -- a derived column disagreeing with
        its own declared source inside one file.
        """
        import logging

        pf = tmp_path / "nhm_slope_params.csv"
        pd.DataFrame({
            "hru_id": [1, 3],
            "mean": [4.4252, 45.0],
            "hru_slope": [0.07738825, 1.0],
        }).to_csv(pf, index=False)

        declared = _declared(
            "slope", "nhm_slope_params.csv", ["mean", "hru_slope"],
            derived_columns={"hru_slope": {"from": "mean", "transform": "deg_to_fraction"}},
        )

        failed = maf.run_fill_sweep(
            [(declared, pf)], self._merged_gdf(), expected_max=3, id_feature="hru_id",
            k_neighbors=2, logger=logging.getLogger("test_rederive_slope"),
        )
        assert failed == []

        out = pd.read_csv(pf)
        assert out["hru_slope"].notna().all()
        np.testing.assert_allclose(
            out["hru_slope"].to_numpy(),
            np.tan(np.radians(out["mean"].to_numpy())),
            rtol=1e-9,
        )
```

Module-level, next to the other `resolve_fill_plan` tests — the loud tripwire
CLAUDE.md:443 exists to preserve, stated as a test:

```python
def test_a_declared_derived_column_absent_from_the_file_still_raises():
    """The reason hru_slope/hru_aspect stay in fill_columns (CLAUDE.md:443).

    A fabric whose CSV predates the derived column must fail loudly and name the
    one command that fixes it, rather than silently shipping a merged/ product with
    a PRMS parameter missing. This is the ONLY CI-visible backstop: Guard 2
    (test_params_index_ondisk) is data-root-gated and skips in CI.
    """
    df = pd.DataFrame({"hru_id": [1, 2], "mean_sin": [0.1, 0.2], "mean_cos": [0.9, 0.8]})
    with pytest.raises(ValueError, match="not present in"):
        maf.resolve_fill_plan(
            df, ["mean_sin", "mean_cos", "hru_aspect"], [], "hru_id", "aspect"
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G pixi run -e dev --as-is pytest tests/test_merge_and_fill_params.py -q`

Expected: the two `run_fill_sweep` tests FAIL — `hru_aspect` comes back as the KNN interpolation of 350° and 10° (~180°) instead of the re-derived ~0°, and `hru_slope` disagrees with `tan(radians(mean))` on the synthesized row. `_declared` will also `TypeError` on the `derived_columns=` keyword until Step 3 widens `DeclaredParam`. `test_a_declared_derived_column_absent_from_the_file_still_raises` should PASS immediately — it pins behaviour that already exists and must survive this task.

- [ ] **Step 3: Widen `DeclaredParam` in `params_index.py`**

Add the field after `prms` (a NamedTuple's defaulted fields must come last, and `prms` already carries a default, so `derived_columns` goes after it):

```python
    # Sixth field. Same MappingProxyType-not-{} reasoning as `prms` above: a
    # NamedTuple default is one object shared by every instance.
    #
    # Read by merge_and_fill_params' fill sweep, which RE-DERIVES these columns
    # after the KNN pass instead of interpolating them. That is not a nicety for
    # hru_aspect -- it is required: KNN-averaging neighbours at 350 deg and 10 deg
    # gives 180 deg, which is issue #201 reappearing on gap-filled HRUs. The
    # linear sources (mean_sin, mean_cos) are what get interpolated.
    derived_columns: Mapping = MappingProxyType({})
```

Update the class docstring's opening line — it currently says the record "has now been widened THREE times"; make that FOUR and name `derived_columns` alongside `fill_columns`, `fabric_columns` and `prms`.

In `_record` (line 138), add:

```python
            derived_columns=dict(entry.get("derived_columns") or {}),
```

- [ ] **Step 4: Wire the re-derivation into `merge_and_fill_params.py`**

Add the import at the top of the file, next to the existing `gfv2_params` imports:

```python
from gfv2_params.zonal_runners.merge import apply_derived_columns
```

`resolve_fill_plan` is **not** modified — see the ruling above. Leave its signature,
its NaN census and the early-return guard at line 543 exactly as they are.

The only change is in `run_fill_sweep`. Immediately before `write_filled_in_place`
(line 576), add:

```python
            if declared.derived_columns:
                # AFTER the KNN pass and the fabric copy, so every derived column is
                # a function of the values actually being written. Both of today's
                # derived columns are ALSO in fill_columns, so KNN has just written
                # an interpolated value into each -- this overwrites it, and the
                # ordering is the whole safety property:
                #
                #   hru_aspect is CIRCULAR. Interpolating neighbours at 350 deg and
                #   10 deg gives 180 deg, due south, which is issue #201 reappearing
                #   on gap-filled HRUs. hru_slope is monotone so its interpolation
                #   was defensible, but it was computed independently of `mean` and
                #   could therefore disagree with its own declared source on a
                #   synthesized row.
                #
                # They stay declared fillable (CLAUDE.md's "do not fix it by dropping
                # the column from fill_columns") because resolve_fill_plan's
                # raise-on-a-declared-column-the-file-lacks is the only CI-visible
                # tripwire for a deleted derived_columns block; Guard 2 is
                # data-root-gated and skips in CI.
                complete_df = apply_derived_columns(complete_df, declared.derived_columns)
                logger.info("  Re-derived %s from filled sources",
                            sorted(declared.derived_columns))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=4 --mem=16G pixi run -e dev --as-is pytest tests/test_merge_and_fill_params.py tests/test_params_index.py tests/test_build_parameter_index.py -q`

Expected: PASS. `test_params_index.py` builds `DeclaredParam` positionally in places; the sixth field has a default, so those still construct — but any test asserting tuple EQUALITY against a 5-tuple will now fail and must be rewritten to attribute access (the class docstring already warns about this).

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/params_index.py scripts/merge_and_fill_params.py tests/test_merge_and_fill_params.py tests/test_params_index.py
git commit -m "$(cat <<'EOF'
fix(fill): re-derive derived_columns after the KNN sweep

DeclaredParam gains derived_columns, and run_fill_sweep re-applies it after
fill_missing_values_knn / apply_fabric_columns so a derived column is always
a function of the values actually written.

Required for hru_aspect: KNN-averaging neighbours at 350 deg and 10 deg gives
180 deg, so an interpolated bearing reintroduces issue #201 on exactly the
gap-filled HRUs nobody inspects. The re-derivation overwrites it.

Both derived columns stay declared in fill_columns per CLAUDE.md -- the KNN
value is overwritten, and the declaration is what keeps resolve_fill_plan's
raise-on-a-missing-declared-column as the only CI-visible tripwire for a
deleted derived_columns block (Guard 2 is data-root-gated and skips in CI).

SCOPE EXPANSION: this also changes hru_slope. It was previously KNN-filled
independently of `mean`, so a synthesized row could carry
hru_slope != tan(radians(mean)) -- a derived column disagreeing with its own
declared source inside one file. It now agrees by construction.

Refs #201.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Config — the aspect entry and the slope `fill_columns`

**Files:**
- Modify: `configs/zonal/zonal_params.yml` — aspect entry (lines 132–165), slope entry `fill_columns` (line ~110)
- Test: `tests/test_params_index.py` (Guard 1, no edit expected), `tests/test_submit_wrapper_param_lists.py` (no edit expected)

**Interfaces:**
- Consumes: `script: aspect` (Task 2), `transform: atan2_deg` (Task 1), `derived_columns` on `DeclaredParam` (Task 3).
- Produces: the declaration `scripts/build_parameter_index.py` renders in Task 5.

- [ ] **Step 1: Replace the aspect entry**

Replace `configs/zonal/zonal_params.yml` lines 132–165 in full:

```yaml
  - name: aspect
    script: aspect
    source_raster: "{data_root}/shared/conus/vrt/aspect.vrt"
    # Read ONLY to build the flat mask. RichDEM does not flag flat cells -- it
    # assigns them 270 deg (due west), 2.8% of a VPU 01 window and 99% coincident
    # with slope == 0. ArcGIS, which TM6B9:603 cites, writes -1 instead. Both VRTs
    # derive from the same per-VPU NEDSnapshot DEM, so the clips co-register by
    # construction; the runner asserts it anyway because a half-cell offset would
    # mask the wrong cells and every downstream number would still look plausible.
    slope_raster: "{data_root}/shared/conus/vrt/slope.vrt"
    categorical: false
    merged_file: nhm_aspect_params.csv
    # `mean` and the eight other raw stats are the UNMASKED pass-1 statistics,
    # unchanged from the pre-fix product and as safe to interpolate as elevation's.
    # mean_sin/mean_cos are linear and safe.
    #
    # hru_aspect is declared here even though KNN must NOT be what determines it:
    # merge_and_fill_params re-derives every derived column from its filled sources
    # AFTER the KNN pass, so the interpolated bearing is overwritten and never
    # reaches disk. The declaration is kept for the tripwire, not the fill --
    # resolve_fill_plan raises on a declared column the file does not have, which
    # is the only CI-visible signal that a fabric's aspect CSV predates this change
    # or that the derived_columns block below was deleted. Guard 2 is data-root-
    # gated and skips in CI, so it is not a substitute. Dropping the column from
    # fill_columns to express "not interpolated" is explicitly forbidden by
    # CLAUDE.md's derived_columns/fill_columns rule.
    #
    # Consequence, deliberate: a fabric whose aspect CSV predates hru_aspect will
    # RAISE on the fill sweep until `derive_zonal_params.py` has re-run the aspect
    # zonal pass and merge for it.
    fill_columns:
      [count, mean, std, min, "25%", "50%", "75%", max, sum,
       n_aspect_cells, flat_frac, mean_sin, mean_cos, hru_aspect]
    # TM6B9:603: hru_aspect = atan2[mean(sin(aspect)), mean(cos(aspect))], 0-360
    # clockwise from north. The two-argument form is not a convenience: a circular
    # mean is not a function of any single statistic, so unlike hru_slope this
    # cannot be recovered from `mean` after the fact (issue #201).
    #
    # merge_and_fill_params RE-DERIVES this after the KNN pass, so a gap-filled HRU
    # gets interpolated mean_sin/mean_cos and a recomputed bearing. That ordering is
    # load-bearing: KNN on hru_aspect itself averages 350 deg and 10 deg to 180 deg
    # -- the defect, reappearing on exactly the HRUs nobody inspects.
    derived_columns:
      hru_aspect:
        from: [mean_sin, mean_cos]
        transform: atan2_deg
    prms:
      builder: zonal_runners/aspect.py + zonal_runners/merge.py (derived_columns)
      columns:
        hru_aspect:
          prms: hru_aspect
          processes: [PRMSSolarGeometry, PRMSAtmosphere]
      provenance:
        mean: >-
          arithmetic mean of a CIRCULAR variable -- retained as the auditable record of
          the pre-#201 product, NOT hru_aspect. Median 179.4 deg across gfv2's 361,471
          HRUs, IQR 151.7-207.5: that central tendency is the artifact, not the terrain.
        mean_sin: coverage-weighted mean of sin(aspect) over non-flat cells
        mean_cos: coverage-weighted mean of cos(aspect) over non-flat cells
        n_aspect_cells: >-
          coverage-weighted non-flat cell count -- the population hru_aspect is
          derived from
        flat_frac: >-
          area fraction of the HRU with slope == 0, which RichDEM reports as 270 deg
          and which has no down slope direction; 1.0 means hru_aspect is
          neighbour-interpolated rather than measured
        count: exactextract cell count
        std: within-HRU standard deviation
        min: within-HRU minimum
        "25%": within-HRU 25th percentile
        "50%": within-HRU median
        "75%": within-HRU 75th percentile
        max: within-HRU maximum
        sum: within-HRU sum
```

Note the `defects:` block is gone — `mean` is a real statistic, just not the PRMS parameter, so it belongs in `provenance:`.

- [ ] **Step 2: Amend the slope entry's comment (its `fill_columns` list does NOT change)**

Leave `fill_columns` exactly as it is — `hru_slope` stays declared, per the ruling in
Task 3. Only the comment needs updating: the paragraph beginning "hru_slope IS declared
fillable" currently says interpolating it directly "is exactly as defensible as
interpolating `mean`", which is no longer what happens. Replace that sentence (keep the
following `NB:` re-merge-prerequisite paragraph verbatim — it is still true and is now
the stated reason the declaration is kept) with:

```yaml
    # hru_slope IS declared fillable, but the KNN value is not what ships:
    # merge_and_fill_params re-derives every derived column from its filled sources
    # after the KNN pass, so hru_slope is tan(radians(mean)) on every row including
    # synthesized ones. It was previously interpolated independently of `mean`,
    # which let a filled row carry hru_slope != tan(radians(mean)) -- a derived
    # column disagreeing with its own declared source inside one file. The
    # declaration is kept for the tripwire below, not for the fill.
```

- [ ] **Step 3: Run the config guards**

Run: `srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G pixi run -e dev --as-is pytest tests/test_params_index.py tests/test_submit_wrapper_param_lists.py tests/test_config.py tests/test_zonal_orchestrator.py -q`

Expected: PASS. Guard 1 (`test_params_index.py`) requires every emitted column to sit in exactly one of `columns:` / `defects:` / `provenance:` — if it reports an unaccounted column, the runner emits a name this entry does not declare, and the entry is what must change to match the runner.

`test_submit_wrapper_param_lists.py` must pass untouched: the entry keeps the name `aspect`, so `submit_zonal_params.sh`'s hardcoded `PARAMS` array still matches.

- [ ] **Step 4: Lint the YAML**

Run: `pixi run -e dev pre-commit run --files configs/zonal/zonal_params.yml`

Expected: `yamllint` and `prettier` Passed. (A targeted `--files` run is safe on the login node; the `--all-files` sweep is not.)

- [ ] **Step 5: Commit**

```bash
git add configs/zonal/zonal_params.yml
git commit -m "$(cat <<'EOF'
feat(config): aspect emits hru_aspect via atan2(mean_sin, mean_cos)

The aspect entry moves to `script: aspect`, gains `slope_raster:` for the flat
mask and a two-argument `derived_columns:` producing hru_aspect. `mean` moves
from `defects:` to `provenance:` -- it is a real statistic, just not the PRMS
parameter, and it stays as the auditable record of the pre-#201 product.

hru_aspect joins fill_columns and hru_slope stays there: both are re-derived
after the KNN pass, so the declaration buys the loud missing-column tripwire
without the interpolated value ever reaching disk.

Refs #201.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: SLURM resources and documentation

**Files:**
- Modify: `slurm_batch/derive_zonal_params.batch:10`
- Modify: `docs/parameter_index.md` (generated regions + "Known gaps" ~line 281, "by-builder" note ~line 232)
- Modify: `docs/ARCHITECTURE.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`

**Interfaces:**
- Consumes: the config from Task 4 (the index generator reads it).
- Produces: nothing code-facing.

- [ ] **Step 1: Raise the zonal batch memory**

In `slurm_batch/derive_zonal_params.batch`, change line 10 from `#SBATCH --mem=32G` to `#SBATCH --mem=64G`, and add above it:

```bash
# 64G, not 32G: the `aspect` runner holds four arrays over the batch window at
# once (aspect, slope, sin, cos) -- ~13 GB on gfv2's largest batch bbox (0.80e9
# cells), plus exactextract's working set. There is no per-param memory override
# in submit_zonal_params.sh and this does not add one; the bump is harmless for
# the other params on ~515 GB nodes.
```

- [ ] **Step 2: Regenerate the parameter index**

Run: `pixi run python scripts/build_parameter_index.py`

This rewrites only the three `<!-- BEGIN GENERATED: ... -->` regions. CI fails if they are stale.

- [ ] **Step 3: Rewrite the aspect "Known gaps" section**

Replace `docs/parameter_index.md`'s `### hru_aspect is DEFECTIVE — do not use nhm_aspect_params.csv:mean` section (lines ~281–299) with a resolution note. It must carry forward every fact the old section recorded — the measured 179.4°/IQR/std numbers, the raster-boundary evidence, the "not recoverable from what is on disk" point — because this is now the only surviving record of the defect. Add:

- the resolution: `hru_aspect` is emitted by `zonal_runners/aspect.py` as `atan2(mean_sin, mean_cos)`, and `mean` is retained as provenance so the old product stays auditable;
- the flat-cell finding (RichDEM 270°, ArcGIS -1, the per-VPU measurements) and `flat_frac`;
- the **role in the model**, ported from `notebooks/_archive/check_params.ipynb` — the only thing in that notebook not already recorded here: *"Controls direct-beam solar radiation incident on each HRU; south-facing slopes receive more insolation, melt snow faster, and have higher PET. Used to compute `swrad_adj` in PRMS."* This discharges the issue's ⚠️ that `docs/repo_review_issues.md` CODE-4 would otherwise delete the only record.

Retitle the section `### hru_aspect is a CIRCULAR mean (resolved, #201)`.

- [ ] **Step 4: Amend CLAUDE.md's derived_columns rule**

CLAUDE.md is a project instruction that overrides default behaviour, so it must stay
true. Its bullet at lines 437–443 (`A derived_columns: output that is also in
fill_columns makes a re-merge a prerequisite of the next fill sweep`) keeps its
prohibition — the ruling upheld it — but its *rationale* is now stale: it says the
alternative is "a silent NaN in a PRMS parameter for exactly the HRUs that were
missing", which post-fill re-derivation makes false. Rewrite the bullet to:

- keep "Do not 'fix' it by dropping the column from `fill_columns`" as the rule;
- replace the silent-NaN rationale with the real one — the declaration is the only
  **CI-visible** tripwire for a deleted `derived_columns:` block or an un-re-merged
  fabric, because Guard 2 is data-root-gated and SKIPs in CI;
- record that the KNN value is overwritten by `run_fill_sweep`'s re-derivation, so
  a derived column is a function of its declared sources on every row;
- name `hru_aspect` alongside `hru_slope` as the second instance.

Per CLAUDE.md's own AI-assistant-memory-sync rule, check whether
`.amazonq/rules/workflow.md` needs the same change. This is a deep architectural
gotcha, which that rule assigns to CLAUDE.md **only** — so the expected outcome is
no change to `.amazonq/rules/workflow.md`. Confirm rather than assume; if you do
change it, say so in the commit.

- [ ] **Step 5: Update the runbooks and architecture doc**

- `slurm_batch/RUNME.md` and `slurm_batch/HPC_REFERENCE.md`: wherever Step 4's zonal params are described, note that `aspect` reads two rasters (`aspect.vrt` + `slope.vrt`) and that the array job now requests 64G. Grep for `--mem=32G` and for `aspect` in both files and fix every hit.
- `docs/ARCHITECTURE.md`: `aspect` is no longer a generic `script: zonal` entry. Grep for `zonal_runners` and for the list of `script:` tags; add `aspect` alongside `zonal`, `soils`, `lulc`, `lulc_prederived`, `ssflux`.

- [ ] **Step 6: Verify nothing else references the old shape**

Run:

```bash
grep -rn "hru_aspect\|nhm_aspect_params" docs/ README.md slurm_batch/ configs/ src/ scripts/ | grep -v "docs/superpowers/"
```

Every hit must either describe the new behaviour or be a historical record explicitly framed as such. Fix any that still assert `mean` IS `hru_aspect`.

- [ ] **Step 7: Lint and commit**

```bash
pixi run -e dev pre-commit run --files slurm_batch/derive_zonal_params.batch docs/parameter_index.md docs/ARCHITECTURE.md slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md
git add slurm_batch/derive_zonal_params.batch docs/parameter_index.md docs/ARCHITECTURE.md slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md
git commit -m "$(cat <<'EOF'
docs(aspect): record the circular-mean fix; zonal array to 64G

Regenerates docs/parameter_index.md and rewrites its aspect "Known gaps"
entry from a defect to a resolution, carrying forward every measurement the
old entry recorded plus the flat-cell finding.

Ports the "role in model" note out of notebooks/_archive/check_params.ipynb,
which repo_review_issues.md CODE-4 proposes deleting -- it was the only
record of the limitation outside this file.

The zonal array job goes to 64G: the aspect runner holds four arrays over the
batch window at once (~13 GB on gfv2's largest bbox).

Refs #201.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Full test sweep and PR

**Files:** none modified.

- [ ] **Step 1: Full suite**

Run:

```bash
srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G \
  pixi run -e dev --as-is pytest tests/ -q
```

Expected: PASS. Guards 2 and 3 (`test_params_index_ondisk.py`, `test_merged_products_ondisk.py`) will FAIL or SKIP here depending on data-root visibility — they check the on-disk product, which is still the pre-fix one until Task 7. Record which, and do not treat a Guard 2 failure at this point as a code defect.

- [ ] **Step 2: Full pre-commit sweep (under srun, 64G)**

```bash
srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=4 --mem=64G \
  pixi run -e dev --as-is pre-commit run --all-files
```

Do not run this on the login node — the prettier hook is SIGKILLed there and the shell still exits 0, so a login-node run reads as a pass when it is not.

- [ ] **Step 3: Push and open the PR**

```bash
git push -u origin feat/hru-aspect-circular-mean
```

`gh` is blocked on this HPC (Go TLS ClientHello dropped by a DPI middlebox). Create the PR with `gh auth token` + `curl --data-binary @payload.json` against the REST API — note `-d @file` returns 400 malformed, `--data-binary` is required.

The PR description must **lead** with the scope-expansion callout for the `hru_slope` re-derivation (Task 3), then cover the two triage findings the issue body does not contain (RichDEM's 270° flats; why no CONUS sin/cos rasters), then the rollout plan.

CI runs only on PRs targeting `main`, not on branch pushes — opening the PR is what starts the gate.

---

## Task 7: Rollout

**Files:** none. This is operational work on the data root, run BEFORE the PR merges — a green suite has previously failed to imply a correct product on this pipeline.

- [ ] **Step 1: Prove it on oregon (3 batches)**

```bash
cd slurm_batch
ZONAL_PARAMS="aspect" FABRIC=oregon ./submit_zonal_params.sh
```

Wait for the array + chained merge to complete.

- [ ] **Step 2: Inspect the oregon product**

```bash
pixi run python - <<'PY'
import pandas as pd, numpy as np
p = "<data_root>/oregon/params/merged/nhm_aspect_params.csv"
df = pd.read_csv(p)
print(df[["mean", "hru_aspect", "flat_frac", "n_aspect_cells", "count"]].describe())
print("hru_aspect NaN:", df["hru_aspect"].isna().sum(), "of", len(df))
print("flat_frac == 1:", (df["flat_frac"] >= 1.0 - 1e-9).sum())
# The tell: the old column's tight south-clustered IQR should open out.
for col in ["mean", "hru_aspect"]:
    q = df[col].quantile([0.25, 0.5, 0.75])
    print(f"{col}: median {q[0.5]:.1f}  IQR {q[0.25]:.1f}-{q[0.75]:.1f}")
PY
```

Expected: `hru_aspect`'s IQR is markedly wider than `mean`'s and not centred on ~180°; `flat_frac` is small and non-zero; `n_aspect_cells <= count` on every row. If `hru_aspect` reproduces `mean`'s tight south-clustered IQR, STOP — the flat mask or the two means are wrong, and a CONUS run would burn hours producing the same defect under a new name.

- [ ] **Step 3: Back up the gfv2 product, then re-run it**

```bash
cp <data_root>/gfv2/params/merged/nhm_aspect_params.csv \
   <data_root>/gfv2/params/merged/nhm_aspect_params.csv.pre201.bak
cd slurm_batch && ZONAL_PARAMS="aspect" FABRIC=gfv2 ./submit_zonal_params.sh
```

66 array tasks at 64G plus the chained merge.

- [ ] **Step 4: Compare CONUS old vs new**

Join the backup against the new file on `nat_hru_id` and confirm the `mean` column is unchanged (pass 1 is unmasked, so it must be), while `hru_aspect` is not a function of it. Report the new median and IQR against the recorded 179.4° / 151.7°–207.5°.

- [ ] **Step 5: tjc (2 batches)**

```bash
cd slurm_batch && ZONAL_PARAMS="aspect" FABRIC=tjc ./submit_zonal_params.sh
```

- [ ] **Step 6: Fill sweep and record the on-disk guards**

Run the fill sweep for each fabric, then run Guards 2 and 3 against the data root and **record their SLURM job ids** in the PR. They are data-root-gated and SKIP in CI, so a green CI badge is not evidence they passed.

- [ ] **Step 7: Post the rollout results to the PR and to issue #201**

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: §Design.1 (runner) → Task 2; §Design.2 (two-arg transform) → Task 1; §Design.3 (post-fill re-derive, including the hru_slope scope expansion) → Task 3 + Task 4 Step 2; §Design.4 (config + declaration) → Task 4; §Design.5 (documentation, incl. the notebook port) → Task 5; §Testing → Tasks 1–3 Step 1 and Task 6; §Rollout → Task 7. The spec's "rejected alternatives" need no task.

**Deviation from the spec, deliberate.** The spec said the `hru_slope` change would land as "its own commit". It cannot: the re-derivation is one mechanism that applies to every param with `derived_columns`, so there is no separable code change. It lands in Task 3, whose commit message names the `hru_slope` consequence explicitly, and the PR-description callout the spec requires is Task 6 Step 3.

**Ruling applied after the pre-flight scan (2026-08-10).** The plan originally dropped `hru_slope` from `fill_columns` and kept `hru_aspect` out of it. That collides verbatim with [CLAUDE.md:443](../../../CLAUDE.md) — "Do not 'fix' it by dropping the column from `fill_columns`" — and, per [test_params_index_ondisk.py:107](../../../tests/test_params_index_ondisk.py#L107), would have discarded the only CI-visible tripwire for a deleted `derived_columns:` block, since Guard 2 skips in CI. The user ruled CLAUDE.md governs. Both columns are now declared fillable and the re-derivation overwrites the interpolated value, so the safety property and the fix both hold. Tasks 3, 4 and 5 were rewritten before any dispatch; the spec's §Design.3 text ("`hru_aspect` is **not** declared fillable") is superseded on that one point and everything else in it stands.

**Type consistency.** `atan2_deg(sin_mean, cos_mean)` is the same name and argument order in Task 1's implementation, Task 1's tests, Task 2's test imports, and Task 4's config. `run_aspect_batch(config, batch_id, logger)` matches the `BATCH_RUNNERS[tag](param_cfg, args.batch_id, logger)` call in `scripts/derive_zonal_params.py:144`. `DeclaredParam.derived_columns` is the same name in `params_index.py`, `_record`, and both `merge_and_fill_params.py` call sites. The emitted column names — `count, mean, std, min, 25%, 50%, 75%, max, sum, n_aspect_cells, mean_sin, mean_cos, flat_frac` — are identical in Task 2's `_STAT_COLUMNS` plus assignments, Task 2's tests, and Task 4's `fill_columns` and `provenance:` blocks.

**Placeholder scan.** One found and fixed: Task 3's test bodies were first written as prose contracts with the fixtures left to the implementer. They are now concrete, built on `tests/test_merge_and_fill_params.py`'s real `_declared` helper (line 25) and `TestRunFillSweep._merged_gdf` (line 677), whose three points at (0,0)/(10,0)/(5,0) make HRU 2's two nearest neighbours HRUs 3 and 1 — which is what lets the 350°/10° fixture prove the KNN-averaging point.

**One behaviour worth confirming during Task 3, not assumed here.** `write_filled_in_place` (line 303) restores pre-fill dtypes by iterating the captured `dtypes` dict and skipping any name not in `out.columns`. A derived column that did not exist in the file before the sweep is therefore left as float64 rather than raising — correct, but check it holds if that function changes.
