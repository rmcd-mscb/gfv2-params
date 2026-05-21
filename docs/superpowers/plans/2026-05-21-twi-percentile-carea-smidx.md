# Distribution-Invariant `carea_max` / `smidx_coef` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PRMS `carea_max`/`smidx_coef` invariant to the TWI source by deriving the TWI cutoff from the data (a percentile) instead of the hardcoded 8.0/15.6, and adopt the CONUS-complete open-source `Twi_hydrodem` — fixing the #94 coverage gap (degenerate carea outside VPU 01) and #55 Stage 1 in one branch.

**Architecture:** Reuse the existing `carea_map` binary builder + zonal-count + ratio pipeline unchanged; only the *threshold number* changes. A new shared pre-step computes valid-land TWI percentile cutoffs (`T_P`) per VPU and CONUS for each TWI source; the `carea_map` builder gains a `threshold_mode` switch (`absolute` = 8.0/15.6; `percentile` = `T_P`, scalar for conus/single-VPU or a per-cell array via a rasterised HRU→VPU lookup). Defaults for the percentile are derived by inverting 8.0/15.6 through VPU 01's ArcPy-TWI CDF.

**Tech Stack:** Python 3, numpy, rasterio/GDAL (osgeo), geopandas/pyogrio, gdptools (exactextract), pixi env, SLURM. Tests run under `pixi run -e dev pytest` (CI gate — never on the HPC head node).

**Spec:** `docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md`
**Branch:** `feat/twi-percentile-carea-smidx`

---

## Conventions for every task

- Run python in-env: `pixi run --as-is python ...`. Run tests: `pixi run -e dev pytest <path> -v`.
- **Never run the full `pytest` on the HPC head node interactively** beyond the single-file invocations below; CI is the gate. Single-file unit runs of pure-function tests are fine.
- Paths/fabric inputs come from the active profile in `configs/base_config.yml` via `require_config_key(config, key, script_name)` — never hardcoded.
- Commit after each task with the message shown. Keep commits atomic.

---

## File structure (created / modified)

**Created:**
- `src/gfv2_params/shared_rasters/twi_reference.py` — pure percentile/CDF math + the `build_twi_reference` shared-raster builder.
- `src/gfv2_params/depstor_builders/vpu_id.py` — rasterise HRU→VPU code onto the template grid.
- `tests/test_twi_reference.py`, `tests/test_vpu_id.py`, `tests/test_carea_map_threshold.py` — unit tests.
- `slurm_batch/submit_twi_completion.sh` — finish `twi.vrt` (VPUs 02–18) + build both TWI VRTs.

**Modified:**
- `src/gfv2_params/shared_rasters/build_vrt.py` — add `twi_hydrodem` VRT type + named-CRS (EPSG:5070) assignment.
- `src/gfv2_params/shared_rasters/__init__.py`, `configs/shared_rasters/shared_rasters.yml` — register `build_twi_reference`.
- `src/gfv2_params/depstor.py` — `compute_carea_map_binary` accepts a scalar **or** per-cell-array threshold.
- `src/gfv2_params/depstor_builders/__init__.py`, `configs/depstor/depstor_rasters.yml` — register `vpu_id`; add `threshold_mode`/percentile keys to `carea_map`.
- `src/gfv2_params/depstor_builders/context.py` — add `vpu` (profile scalar) field.
- `src/gfv2_params/depstor_builders/carea_map.py` — threshold-mode resolution.
- `scripts/build_depstor_rasters.py` — pass `vpu` into `BuildContext`.
- `configs/base_config.yml` — oregon `vpu: "17"`; percentile keys; #94 caveat update.
- `README.md`, `slurm_batch/RUNME.md` — document the new stages.

---

# PHASE A — TWI source completion

Build `twi_hydrodem.vrt` and finish the ArcPy `twi.vrt` for VPUs 02–18 so both sources exist for the A/B and so percentile mode has CONUS-complete TWI.

## Task A1: Add `twi_hydrodem` VRT type with a named CRS

**Files:**
- Modify: `src/gfv2_params/shared_rasters/build_vrt.py`
- Test: `tests/test_build_vrt_srs.py` (create)

The `Twi_hydrodem_*.tif` tiles report an `"unnamed"` Albers CRS; carea_map's strict `src.crs != template.crs` check needs a *named* EPSG:5070. Add the VRT type and a small pure helper that maps a VRT name to the SRS it must be stamped with.

- [ ] **Step 1: Write the failing test**

Create `tests/test_build_vrt_srs.py`:

```python
"""Unit tests for build_vrt's per-type SRS override (twi_hydrodem needs a
named EPSG:5070 because the source tiles report an 'unnamed' Albers CRS)."""

import importlib

build_vrt = importlib.import_module("gfv2_params.shared_rasters.build_vrt")


def test_twi_hydrodem_registered_with_nodata():
    assert "twi_hydrodem" in build_vrt.RASTER_TYPES
    pattern, src_nodata = build_vrt.RASTER_TYPES["twi_hydrodem"][:2]
    assert pattern == "Twi_hydrodem_*.tif"
    assert src_nodata == "-9999"


def test_srs_override_only_for_twi_hydrodem():
    assert build_vrt._srs_override("twi_hydrodem") == "EPSG:5070"
    assert build_vrt._srs_override("twi") is None
    assert build_vrt._srs_override("fdr") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_build_vrt_srs.py -v`
Expected: FAIL (`twi_hydrodem` not in RASTER_TYPES / `_srs_override` undefined).

- [ ] **Step 3: Implement**

In `build_vrt.py`, add the new type to `RASTER_TYPES` (after the `twi` entry):

```python
    "twi":         ("Twi_merged_*.tif", "-9999"),
    # Open-source WhiteboxTools TWI (issue #94): CONUS-complete, drop-in grid
    # with fdr.vrt. Tiles report an "unnamed" Albers CRS, so the VRT must be
    # stamped with a named EPSG:5070 to satisfy carea_map's CRS-equality check.
    "twi_hydrodem": ("Twi_hydrodem_*.tif", "-9999"),
```

Add the helper above `build()`:

```python
# VRT types whose source tiles carry an unnamed/implicit CRS and must be
# stamped with an explicit EPSG so strict CRS-equality checks downstream pass.
_SRS_OVERRIDES = {"twi_hydrodem": "EPSG:5070"}


def _srs_override(vrt_name: str) -> str | None:
    """EPSG string to force onto the built VRT, or None to keep source CRS."""
    return _SRS_OVERRIDES.get(vrt_name)
```

In `build()`, right after the `vrt_ds.FlushCache()` / `del vrt_ds` block, apply the override:

```python
        vrt_ds.FlushCache()
        del vrt_ds

        epsg = _srs_override(vrt_name)
        if epsg is not None:
            from osgeo import osr
            srs = osr.SpatialReference()
            srs.SetFromUserInput(epsg)
            ds = gdal.Open(str(vrt_path), gdal.GA_Update)
            ds.SetProjection(srs.ExportToWkt())
            ds.FlushCache()
            del ds
            logger.info("Stamped %s with %s", vrt_path, epsg)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_build_vrt_srs.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/shared_rasters/build_vrt.py tests/test_build_vrt_srs.py
git commit -m "feat(shared): add twi_hydrodem VRT type with named EPSG:5070 (#94)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task A2: SLURM script to finish `twi.vrt` + build both TWI VRTs

**Files:**
- Create: `slurm_batch/submit_twi_completion.sh`

This is operational (no unit test): run the already-registered `merge_rpu_by_vpu_twi` step for all VPUs (02–18 are empty; 01 exists), then `build_vrt` (now also builds `twi_hydrodem.vrt`).

- [ ] **Step 1: Create the submit script**

Create `slurm_batch/submit_twi_completion.sh`:

```bash
#!/usr/bin/env bash
# Finish twi.vrt (ArcPy Twi_merged for VPUs 02-18 were never merged) and build
# both TWI VRTs (twi.vrt + twi_hydrodem.vrt). Inputs are all staged: per-RPU
# TWI at input/twi/<rpu>/twi.tif (59 RPUs) and per-VPU land masks for all 18.
# Pure on-cluster; no ArcPy. Run from a shell with ~/.pixi/bin on PATH.
#
#   bash slurm_batch/submit_twi_completion.sh
set -euo pipefail
cd "$(dirname "$0")/.."
CFG=configs/shared_rasters/shared_rasters.yml

# 1) merge ArcPy per-RPU TWI -> per-VPU Twi_merged for every VPU (idempotent;
#    --force re-merges 01 too so all 18 are consistent).
merge=$(sbatch --parsable --job-name=twi_merge \
  --wrap="pixi run --as-is python scripts/build_shared_rasters.py \
          --config $CFG --step merge_rpu_by_vpu_twi --force")
echo "merge_rpu_by_vpu_twi: $merge"

# 2) (re)build CONUS VRTs after the merge — builds twi.vrt AND twi_hydrodem.vrt.
vrt=$(sbatch --parsable --dependency=afterok:$merge --job-name=twi_vrt \
  --wrap="pixi run --as-is python scripts/build_shared_rasters.py \
          --config $CFG --step build_vrt --force")
echo "build_vrt: $vrt"
echo "Submitted. When done, verify with: scripts/build_shared_rasters.py logs + gdalinfo on both VRTs."
```

- [ ] **Step 2: Make executable + commit**

```bash
chmod +x slurm_batch/submit_twi_completion.sh
git add slurm_batch/submit_twi_completion.sh
git commit -m "feat(slurm): submit_twi_completion.sh to finish twi.vrt + build twi_hydrodem.vrt (#94)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

- [ ] **Step 3: Run it (operational) and verify**

```bash
bash slurm_batch/submit_twi_completion.sh
# After both jobs finish (watch: squeue -u $USER):
DR=$(awk '/^data_root:/ {print $2}' configs/base_config.yml)
pixi run --as-is python - <<PY
import rasterio, numpy as np
DR="$DR"
for name in ("twi.vrt", "twi_hydrodem.vrt"):
    p=f"{DR}/shared/conus/vrt/{name}"
    with rasterio.open(p) as ds:
        w=ds.read(1, out_shape=(1, ds.height//50, ds.width//50))
        nod=ds.nodata
        valid=np.isfinite(w) & (w!=nod) if nod is not None else np.isfinite(w)
        print(name, "crs=", ds.crs, "valid%=", round(100*valid.mean(),1))
PY
```

Expected: both VRTs report `crs=EPSG:5070`; `twi.vrt` valid% now substantially > 0 across CONUS (not just VPU 01); `twi_hydrodem.vrt` valid% high.

---

# PHASE B — Reference-percentile pre-step

Compute valid-land TWI percentile cutoffs (`T_P`) per VPU and CONUS for each source, with defaults derived by inverting 8.0/15.6 through VPU 01's ArcPy CDF.

## Task B1: Pure percentile + CDF-inversion helpers

**Files:**
- Create: `src/gfv2_params/shared_rasters/twi_reference.py`
- Test: `tests/test_twi_reference.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_twi_reference.py`:

```python
"""Unit tests for the pure percentile / CDF-inversion helpers used to derive
TWI threshold cutoffs (issue #55 Stage 1)."""

import numpy as np
import pytest

from gfv2_params.shared_rasters.twi_reference import (
    percentile_of_values,
    rank_of_value,
)


def test_percentile_of_values_basic():
    vals = np.arange(1, 101, dtype="float64")  # 1..100
    p = percentile_of_values(vals, [75.0, 95.0])
    assert p[0] == pytest.approx(75.25, abs=0.5)
    assert p[1] == pytest.approx(95.05, abs=0.5)


def test_percentile_drops_nan_and_nodata():
    vals = np.array([1.0, 2.0, 3.0, 4.0, np.nan, -9999.0], dtype="float64")
    p = percentile_of_values(vals, [50.0], nodata=-9999.0)
    # median of {1,2,3,4} == 2.5
    assert p[0] == pytest.approx(2.5)


def test_rank_of_value_is_inverse_of_percentile():
    vals = np.arange(1, 101, dtype="float64")
    # value 75 sits at ~the 75th percentile
    assert rank_of_value(vals, 75.0) == pytest.approx(75.0, abs=1.0)


def test_rank_of_value_handles_nodata():
    vals = np.array([1.0, 2.0, 3.0, 4.0, -9999.0], dtype="float64")
    # 3.0 is >= 3 of 4 valid values -> 75%
    assert rank_of_value(vals, 3.0, nodata=-9999.0) == pytest.approx(75.0)


def test_percentile_empty_raises():
    with pytest.raises(ValueError, match="no valid"):
        percentile_of_values(np.array([-9999.0, np.nan]), [50.0], nodata=-9999.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_twi_reference.py -v`
Expected: FAIL (module/functions undefined).

- [ ] **Step 3: Implement the helpers**

Create `src/gfv2_params/shared_rasters/twi_reference.py` with the pure helpers (the builder is added in B2):

```python
"""Valid-land TWI percentile cutoffs for distribution-invariant carea_max /
smidx_coef (issues #94 + #55 Stage 1).

The cutoff that classifies a cell as "wet" used to be a hardcoded TWI value
(8.0 / 15.6) calibrated to the ArcPy TWI distribution. Here we derive it from
the data instead: the P-th percentile of valid-land TWI over a reference
population (per-VPU or CONUS). Because it is recomputed from whatever TWI
source is in play, swapping the source preserves each cell's rank, so the
parameters become invariant to the source.

This module holds the pure math (percentile / CDF-inversion) plus the
`build_twi_reference` shared-raster builder that samples the staged TWI tiles.
"""

from __future__ import annotations

import numpy as np


def _valid(values: np.ndarray, nodata: float | None) -> np.ndarray:
    """Return the finite, non-nodata subset as a 1-D float64 array."""
    v = np.asarray(values, dtype="float64").ravel()
    mask = np.isfinite(v)
    if nodata is not None:
        mask &= v != nodata
    return v[mask]


def percentile_of_values(values: np.ndarray, ps, nodata: float | None = None):
    """The P-th percentile(s) of the valid values. `ps` is a list of [0,100]."""
    valid = _valid(values, nodata)
    if valid.size == 0:
        raise ValueError("percentile_of_values: no valid (finite, non-nodata) values")
    return [float(x) for x in np.percentile(valid, ps)]


def rank_of_value(values: np.ndarray, value: float, nodata: float | None = None) -> float:
    """Percentile rank (0-100) of `value` in the valid distribution: the
    fraction of valid values <= `value`, x100. Inverse of percentile_of_values;
    used to find what percentile the legacy 8.0 / 15.6 occupy."""
    valid = _valid(values, nodata)
    if valid.size == 0:
        raise ValueError("rank_of_value: no valid values")
    return float(100.0 * np.count_nonzero(valid <= value) / valid.size)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_twi_reference.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/shared_rasters/twi_reference.py tests/test_twi_reference.py
git commit -m "feat(shared): TWI percentile + CDF-inversion helpers (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task B2: `build_twi_reference` builder + sampling + table assembly

**Files:**
- Modify: `src/gfv2_params/shared_rasters/twi_reference.py`
- Test: `tests/test_twi_reference.py` (extend)

The builder samples land-masked TWI per VPU for a source, derives default percentiles by inverting 8.0/15.6 on ArcPy VPU 01, evaluates `T_P` per VPU + CONUS, and writes a CSV. We unit-test the *table assembly* with an injected sampler so no rasters are needed.

- [ ] **Step 1: Write the failing test (extend the file)**

Append to `tests/test_twi_reference.py`:

```python
from gfv2_params.shared_rasters.twi_reference import assemble_reference_table


def test_assemble_reference_table_uses_inverted_defaults():
    # Fake per-VPU valid-land TWI samples for two VPUs of one source.
    samples = {
        "01": np.arange(1, 101, dtype="float64"),       # 1..100
        "17": np.arange(1, 201, dtype="float64") / 2.0,  # 0.5..100
    }

    def sampler(vpu):
        return samples[vpu]

    rows = assemble_reference_table(
        source="hydrodem",
        vpus=["01", "17"],
        sampler=sampler,
        # invert 8.0/15.6 against this ArcPy VPU01 sample to get the percentiles
        arcpy_vpu01_sample=np.arange(1, 101, dtype="float64"),
        legacy_carea=8.0,
        legacy_smidx=15.6,
    )
    by = {(r["scope"], r["vpu"]): r for r in rows}
    # Inversion: 8.0 -> ~8th pct, 15.6 -> ~16th pct of 1..100
    assert by[("vpu", "01")]["p_carea"] == pytest.approx(8.0, abs=1.0)
    assert by[("vpu", "01")]["p_smidx"] == pytest.approx(15.6, abs=1.0)
    # CONUS row pools all VPUs and uses the same percentiles
    assert ("conus", "CONUS") in by
    # t_carea is the p_carea-th percentile of that scope's sample
    assert by[("vpu", "17")]["t_carea"] < by[("vpu", "01")]["t_carea"]


def test_assemble_reference_table_explicit_percentiles_skip_inversion():
    samples = {"01": np.arange(1, 101, dtype="float64")}
    rows = assemble_reference_table(
        source="arcpy", vpus=["01"], sampler=lambda v: samples[v],
        p_carea=75.0, p_smidx=95.0,
    )
    r = next(x for x in rows if x["scope"] == "vpu")
    assert r["p_carea"] == 75.0 and r["p_smidx"] == 95.0
    assert r["t_carea"] == pytest.approx(75.25, abs=0.5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_twi_reference.py -v`
Expected: FAIL (`assemble_reference_table` undefined).

- [ ] **Step 3: Implement table assembly + the builder**

Append to `twi_reference.py`:

```python
from pathlib import Path

import csv

import rasterio

# Legacy ArcPy thresholds (docs/0b_TB_depr_stor.py); used to derive default
# percentiles by inversion so percentile-mode reproduces VPU 01 by construction.
LEGACY_CAREA_THRESHOLD = 8.0
LEGACY_SMIDX_THRESHOLD = 15.6

_TABLE_FIELDS = ["source", "scope", "vpu", "p_carea", "p_smidx", "t_carea", "t_smidx"]


def assemble_reference_table(
    source: str,
    vpus: list[str],
    sampler,
    *,
    arcpy_vpu01_sample=None,
    legacy_carea: float = LEGACY_CAREA_THRESHOLD,
    legacy_smidx: float = LEGACY_SMIDX_THRESHOLD,
    p_carea: float | None = None,
    p_smidx: float | None = None,
    nodata: float | None = -9999.0,
) -> list[dict]:
    """Build the reference-percentile rows for one TWI source.

    `sampler(vpu) -> 1-D array` supplies valid-land TWI samples per VPU. If
    `p_carea`/`p_smidx` are not given, they are derived by inverting
    legacy_carea/legacy_smidx through `arcpy_vpu01_sample` (the CDF-inversion
    default). One `vpu`-scope row per VPU plus one pooled `conus` row.
    """
    if p_carea is None or p_smidx is None:
        if arcpy_vpu01_sample is None:
            raise ValueError(
                "assemble_reference_table: provide p_carea/p_smidx or "
                "arcpy_vpu01_sample to derive them by inversion"
            )
        p_carea = rank_of_value(arcpy_vpu01_sample, legacy_carea, nodata=nodata)
        p_smidx = rank_of_value(arcpy_vpu01_sample, legacy_smidx, nodata=nodata)

    rows: list[dict] = []
    pooled = []
    for vpu in vpus:
        s = sampler(vpu)
        valid = _valid(s, nodata)
        if valid.size == 0:
            continue
        pooled.append(valid)
        tc, ts = percentile_of_values(valid, [p_carea, p_smidx])
        rows.append({
            "source": source, "scope": "vpu", "vpu": vpu,
            "p_carea": round(p_carea, 4), "p_smidx": round(p_smidx, 4),
            "t_carea": tc, "t_smidx": ts,
        })
    if pooled:
        allv = np.concatenate(pooled)
        tc, ts = percentile_of_values(allv, [p_carea, p_smidx])
        rows.append({
            "source": source, "scope": "conus", "vpu": "CONUS",
            "p_carea": round(p_carea, 4), "p_smidx": round(p_smidx, 4),
            "t_carea": tc, "t_smidx": ts,
        })
    return rows


def write_reference_table(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_TABLE_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _sample_land_masked_twi(twi_path: Path, mask_path: Path, decimate: int, nodata):
    """Decimated valid-land TWI sample (1-D). Reads both rasters at a coarse
    overview to keep CONUS-scale sampling cheap; mask==1 marks land."""
    with rasterio.open(twi_path) as t:
        oh, ow = max(1, t.height // decimate), max(1, t.width // decimate)
        twi = t.read(1, out_shape=(1, oh, ow))
        tnod = t.nodata if nodata is None else nodata
    with rasterio.open(mask_path) as m:
        land = m.read(1, out_shape=(1, oh, ow)) == 1
    sample = twi[land]
    return _valid(sample, tnod)


# Maps the depstor "twi family" to the per-VPU tile filename prefix.
_SOURCE_PREFIX = {"arcpy": "Twi_merged", "hydrodem": "Twi_hydrodem"}


def build(step_cfg: dict, ctx, logger) -> dict:
    """shared-raster builder: write reference-percentile CSVs for each source.

    step_cfg keys:
      sources    list[str] subset of {"arcpy","hydrodem"} (default both)
      percentiles {carea_max, smidx}  optional explicit percentiles; if absent,
                  derived by inverting 8.0/15.6 on the ArcPy VPU 01 sample.
      decimate   int overview factor for sampling (default 20)
    """
    sources = step_cfg.get("sources", ["arcpy", "hydrodem"])
    pcfg = step_cfg.get("percentiles", {})
    p_carea = pcfg.get("carea_max")
    p_smidx = pcfg.get("smidx")
    decimate = int(step_cfg.get("decimate", 20))
    nodata = -9999.0
    out_dir = ctx.conus_dir
    produced = {}

    def mask_path(vpu):
        return ctx.per_vpu_dir / vpu / f"land_mask_{vpu}.tif"

    # ArcPy VPU 01 sample drives the inverted default percentiles.
    arcpy01 = None
    if p_carea is None or p_smidx is None:
        twi01 = ctx.per_vpu_dir / "01" / "Twi_merged_01.tif"
        arcpy01 = _sample_land_masked_twi(twi01, mask_path("01"), decimate, nodata)
        logger.info("build_twi_reference: derived default percentiles by "
                    "inverting 8.0/15.6 on ArcPy VPU 01 (%d samples)", arcpy01.size)

    for source in sources:
        prefix = _SOURCE_PREFIX[source]

        def sampler(vpu, _prefix=prefix):
            twi = ctx.per_vpu_dir / vpu / f"{_prefix}_{vpu}.tif"
            return _sample_land_masked_twi(twi, mask_path(vpu), decimate, nodata)

        rows = assemble_reference_table(
            source=source, vpus=list(ctx.vpus), sampler=sampler,
            arcpy_vpu01_sample=arcpy01, p_carea=p_carea, p_smidx=p_smidx,
            nodata=nodata,
        )
        out_path = out_dir / f"twi_reference_percentiles.{source}.csv"
        write_reference_table(rows, out_path)
        logger.info("build_twi_reference: wrote %d rows -> %s", len(rows), out_path)
        produced[f"twi_reference_{source}"] = out_path

    return produced
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_twi_reference.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/shared_rasters/twi_reference.py tests/test_twi_reference.py
git commit -m "feat(shared): build_twi_reference builder + table assembly (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task B3: Register `build_twi_reference` in the shared-raster DAG

**Files:**
- Modify: `src/gfv2_params/shared_rasters/__init__.py`
- Modify: `configs/shared_rasters/shared_rasters.yml`

- [ ] **Step 1: Register the builder**

In `src/gfv2_params/shared_rasters/__init__.py`: add `twi_reference` to the imports, the `BUILDERS` dict, `STEP_ORDER`, and `PLANNED_STEPS`. It runs **after** `build_vrt` (needs per-VPU tiles; independent of derived rasters):

```python
from . import (
    build_border_dem,
    build_derived_rasters,
    build_lulc_rasters,
    build_vpu_landmask,
    build_vrt,
    compute_dem_derivatives,
    merge_rpu_by_vpu,
    twi_reference,
)
```

```python
    "build_vrt":               build_vrt.build,
    "twi_reference":           twi_reference.build,
    "build_derived_rasters":   build_derived_rasters.build,
```

```python
    "build_vrt",
    "twi_reference",
    "build_derived_rasters",
```

Add `"twi_reference"` to `STEP_ORDER` in the same position; `PLANNED_STEPS = list(STEP_ORDER)` already mirrors it.

- [ ] **Step 2: Add the config block**

In `configs/shared_rasters/shared_rasters.yml`, add after the `build_vrt` step:

```yaml
  # Stage 2a': valid-land TWI percentile cutoffs per source (issue #55/#94).
  # Defaults derived by inverting 8.0/15.6 on ArcPy VPU 01; override with
  # `percentiles: {carea_max: <P>, smidx: <P>}`.
  - name: twi_reference
    sources: [arcpy, hydrodem]
    decimate: 20
```

- [ ] **Step 3: Verify registration imports cleanly**

Run: `pixi run --as-is python -c "from gfv2_params.shared_rasters import BUILDERS, STEP_ORDER; assert 'twi_reference' in BUILDERS and 'twi_reference' in STEP_ORDER; print('ok', STEP_ORDER)"`
Expected: prints `ok [...]` with `twi_reference` after `build_vrt`.

- [ ] **Step 4: Commit**

```bash
git add src/gfv2_params/shared_rasters/__init__.py configs/shared_rasters/shared_rasters.yml
git commit -m "feat(shared): register twi_reference step in the shared DAG (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

- [ ] **Step 5: Run the step (operational, after Phase A)**

```bash
pixi run --as-is python scripts/build_shared_rasters.py \
  --config configs/shared_rasters/shared_rasters.yml --step twi_reference --force
DR=$(awk '/^data_root:/ {print $2}' configs/base_config.yml)
column -s, -t "$DR/shared/conus/twi_reference_percentiles.arcpy.csv" | head
column -s, -t "$DR/shared/conus/twi_reference_percentiles.hydrodem.csv" | head
```

Expected: two CSVs; the ArcPy VPU 01 `t_carea`≈8.0 and `t_smidx`≈15.6 (inversion sanity); `p_carea`/`p_smidx` near 8/16 (the inverted percentiles).

---

# PHASE C — Percentile-mode `carea_map`

## Task C1: `compute_carea_map_binary` accepts a per-cell-array threshold

**Files:**
- Modify: `src/gfv2_params/depstor.py`
- Test: `tests/test_carea_map_threshold.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_carea_map_threshold.py`:

```python
"""compute_carea_map_binary must accept a per-cell-array threshold (percentile
mode, per-VPU) and match the scalar path cell-for-cell when the array is
constant (issue #55)."""

import numpy as np

from gfv2_params.depstor import compute_carea_map_binary


def _inputs():
    perv = np.array([[1, 1, 1], [1, 0, 1]], dtype="uint8")
    onstream = np.array([[0, 0, 1], [0, 0, 0]], dtype="uint8")
    twi = np.array([[5.0, 9.0, 5.0], [20.0, 9.0, -9999.0]], dtype="float32")
    land = np.ones((2, 3), dtype=bool)
    return perv, onstream, twi, land


def test_scalar_threshold_unchanged():
    perv, onstream, twi, land = _inputs()
    out = compute_carea_map_binary(perv, onstream, twi, 8.0, -9999.0, land)
    # keep where perv & land & (twi>8 or onstream); 255 elsewhere
    expected = np.array([[255, 1, 1], [1, 255, 255]], dtype="uint8")
    assert np.array_equal(out, expected)


def test_constant_array_threshold_matches_scalar():
    perv, onstream, twi, land = _inputs()
    scalar = compute_carea_map_binary(perv, onstream, twi, 8.0, -9999.0, land)
    arr = np.full(twi.shape, 8.0, dtype="float64")
    out = compute_carea_map_binary(perv, onstream, twi, arr, -9999.0, land)
    assert np.array_equal(out, scalar)


def test_per_cell_array_threshold_varies():
    perv, onstream, twi, land = _inputs()
    # column 1 has twi=9; threshold 10 there -> excluded; threshold 8 -> included
    arr = np.array([[8.0, 10.0, 8.0], [8.0, 10.0, 8.0]], dtype="float64")
    out = compute_carea_map_binary(perv, onstream, twi, arr, -9999.0, land)
    assert out[0, 1] == 255  # 9 !> 10
    assert out[0, 2] == 1    # onstream rescues
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_carea_map_threshold.py -v`
Expected: PASS for scalar tests; the array tests may already pass if `twi > threshold` broadcasts — run to confirm. If all pass, **still** add the docstring note in Step 3 (no behaviour change needed); if any fail, fix per Step 3.

- [ ] **Step 3: Confirm/Update implementation**

In `src/gfv2_params/depstor.py`, update `compute_carea_map_binary`'s signature type hint and docstring to make the array contract explicit (the numpy `twi > threshold` already broadcasts a scalar or same-shape array, so no logic change):

```python
def compute_carea_map_binary(
    perv: np.ndarray,
    onstream: np.ndarray,
    twi: np.ndarray,
    threshold,  # float scalar OR np.ndarray broadcastable to twi (per-cell T_P)
    twi_nodata: Optional[float],
    land_valid: np.ndarray,
) -> np.ndarray:
```

Add to the docstring:

```
    `threshold` may be a scalar (absolute mode, or percentile-conus / single-VPU)
    or a per-cell float array the same shape as `twi` (percentile per-VPU mode,
    where each cell carries its HRU's home-VPU T_P). `twi > threshold` broadcasts
    either way.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_carea_map_threshold.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor.py tests/test_carea_map_threshold.py
git commit -m "feat(depstor): carea_map binary accepts per-cell array threshold (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task C2: `vpu_id` builder — rasterise HRU→VPU code onto the template

**Files:**
- Create: `src/gfv2_params/depstor_builders/vpu_id.py`
- Test: `tests/test_vpu_id.py` (create)
- Modify: `src/gfv2_params/depstor_builders/context.py` (add `vpu` field)

- [ ] **Step 1: Write the failing test**

Create `tests/test_vpu_id.py`:

```python
"""Unit tests for vpu_id code mapping + resolution precedence (issue #55)."""

import pytest

from gfv2_params.depstor_builders.vpu_id import vpu_to_code, resolve_vpu_source


def test_vpu_to_code_zero_padded():
    assert vpu_to_code("01") == 1
    assert vpu_to_code("17") == 17
    assert vpu_to_code("18") == 18


def test_vpu_to_code_rejects_garbage():
    with pytest.raises(ValueError):
        vpu_to_code("not-a-vpu")


def test_resolve_prefers_profile_scalar():
    # profile vpu scalar wins even if the fabric has an attribute
    kind, value = resolve_vpu_source(profile_vpu="17", fabric_has_vpu_attr=True)
    assert kind == "scalar" and value == "17"


def test_resolve_falls_back_to_attribute():
    kind, value = resolve_vpu_source(profile_vpu=None, fabric_has_vpu_attr=True)
    assert kind == "attribute" and value == "vpu"


def test_resolve_errors_when_neither():
    with pytest.raises(ValueError, match="requires a profile `vpu`"):
        resolve_vpu_source(profile_vpu=None, fabric_has_vpu_attr=False)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_vpu_id.py -v`
Expected: FAIL (module undefined).

- [ ] **Step 3: Implement the builder**

Create `src/gfv2_params/depstor_builders/vpu_id.py`:

```python
"""Rasterise each HRU's home VPU code onto the template grid.

Used by carea_map percentile-`vpu` mode on multi-VPU fabrics (gfv2): each cell
gets the integer VPU code of the HRU that covers it, so the builder can map
`vpu_code -> T_P` into a per-cell threshold array. Because the HRU polygon's
`vpu` value is burned, every cell of an HRU carries that HRU's home VPU — the
exact per-HRU home-VPU assignment the spec requires.

For single-VPU fabrics the profile declares `vpu:` and the raster is a constant
fill (or carea_map uses the scalar T_P directly and skips this step).
"""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize

from ..depstor import RasterInfo
from .context import BuildContext

VPU_NODATA = 0  # 0 is not a valid VPU code (VPUs are 01..18 -> 1..18)


def vpu_to_code(vpu: str) -> int:
    """'01'..'18' -> 1..18. Raises on anything that isn't a VPU label."""
    try:
        code = int(str(vpu).lstrip("0") or "0")
    except (TypeError, ValueError):
        raise ValueError(f"Not a VPU label: {vpu!r}")
    if not 1 <= code <= 21:  # NHDPlus VPUs run 01..18 (+ a few sub-regions)
        raise ValueError(f"VPU code out of range: {vpu!r} -> {code}")
    return code


def resolve_vpu_source(profile_vpu, fabric_has_vpu_attr: bool):
    """Resolution precedence: profile scalar > fabric attribute > error."""
    if profile_vpu:
        return "scalar", str(profile_vpu)
    if fabric_has_vpu_attr:
        return "attribute", "vpu"
    raise ValueError(
        "carea_map percentile `vpu` scope requires a profile `vpu` scalar "
        "(single-VPU fabric) or a `vpu` attribute on the HRU layer."
    )


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    out = ctx.resolve_output(step_cfg["output"])
    if out.exists() and not ctx.force:
        logger.info("  vpu_id exists — skipping (pass --force)")
        return {"vpu_id": out}

    info = RasterInfo.from_path(ctx.template_path)
    profile = {
        "driver": "GTiff", "height": info.height, "width": info.width, "count": 1,
        "dtype": "uint8", "crs": info.crs, "transform": info.transform,
        "nodata": VPU_NODATA, "compress": "LZW", "tiled": True,
        "blockxsize": 256, "blockysize": 256, "BIGTIFF": "YES",
    }

    hru = gpd.read_file(ctx.hru_gpkg, layer=ctx.hru_layer)
    kind, value = resolve_vpu_source(ctx.vpu, "vpu" in hru.columns)
    logger.info("--- vpu_id (%s) ---", kind)

    out.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out, "w", **profile) as dst:
        if kind == "scalar":
            dst.write(np.full((info.height, info.width), vpu_to_code(value),
                              dtype="uint8"), 1)
        else:
            if hru.crs != info.crs:
                hru = hru.to_crs(info.crs)
            shapes = ((geom, vpu_to_code(v)) for geom, v in zip(hru.geometry, hru[value]))
            arr = rasterize(
                shapes, out_shape=(info.height, info.width),
                transform=info.transform, fill=VPU_NODATA, dtype="uint8",
                all_touched=True,
            )
            dst.write(arr, 1)
    logger.info("  Wrote vpu_id raster -> %s", out)
    return {"vpu_id": out}
```

Add the `vpu` field to `BuildContext` in `context.py` (after `twi_raster`):

```python
    twi_raster: Path | None = None
    vpu: str | None = None  # single-VPU fabric's VPU label (e.g. "17"); None = use fabric `vpu` attr
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_vpu_id.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/vpu_id.py src/gfv2_params/depstor_builders/context.py tests/test_vpu_id.py
git commit -m "feat(depstor): vpu_id builder rasterising HRU home-VPU codes (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task C3: Threshold resolution in the `carea_map` builder

**Files:**
- Modify: `src/gfv2_params/depstor_builders/carea_map.py`
- Test: `tests/test_carea_map_threshold.py` (extend)

- [ ] **Step 1: Write the failing test (extend)**

Append to `tests/test_carea_map_threshold.py`:

```python
import csv
from pathlib import Path

from gfv2_params.depstor_builders.carea_map import (
    load_reference_table,
    resolve_scalar_thresholds,
)


def _write_table(tmp_path) -> Path:
    p = tmp_path / "twi_reference_percentiles.hydrodem.csv"
    with open(p, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["source", "scope", "vpu", "p_carea", "p_smidx", "t_carea", "t_smidx"])
        w.writeheader()
        w.writerow({"source": "hydrodem", "scope": "conus", "vpu": "CONUS", "p_carea": 8, "p_smidx": 16, "t_carea": 7.7, "t_smidx": 14.9})
        w.writerow({"source": "hydrodem", "scope": "vpu", "vpu": "17", "p_carea": 8, "p_smidx": 16, "t_carea": 6.2, "t_smidx": 12.1})
    return p


def test_load_reference_table_indexes_by_scope_vpu(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    assert table[("conus", "CONUS")]["t_carea"] == 7.7
    assert table[("vpu", "17")]["t_smidx"] == 12.1


def test_resolve_scalar_conus(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    tc, ts = resolve_scalar_thresholds(table, scope="conus", vpu=None)
    assert (tc, ts) == (7.7, 14.9)


def test_resolve_scalar_single_vpu(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    tc, ts = resolve_scalar_thresholds(table, scope="vpu", vpu="17")
    assert (tc, ts) == (6.2, 12.1)


def test_resolve_scalar_missing_vpu_raises(tmp_path):
    table = load_reference_table(_write_table(tmp_path))
    import pytest
    with pytest.raises(KeyError, match="no reference row"):
        resolve_scalar_thresholds(table, scope="vpu", vpu="09")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_carea_map_threshold.py -v`
Expected: FAIL (`load_reference_table` / `resolve_scalar_thresholds` undefined).

- [ ] **Step 3: Implement the resolution helpers**

In `carea_map.py`, add near the top (after imports):

```python
import csv


def load_reference_table(path) -> dict:
    """Load a twi_reference_percentiles.<source>.csv into {(scope, vpu): row}."""
    table = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            for k in ("p_carea", "p_smidx", "t_carea", "t_smidx"):
                row[k] = float(row[k])
            table[(row["scope"], row["vpu"])] = row
    return table


def resolve_scalar_thresholds(table: dict, scope: str, vpu):
    """Return (t_carea, t_smidx) scalars for conus scope or a single VPU."""
    key = ("conus", "CONUS") if scope == "conus" else ("vpu", str(vpu))
    if key not in table:
        raise KeyError(f"no reference row for {key}; have {sorted(table)}")
    row = table[key]
    return row["t_carea"], row["t_smidx"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_carea_map_threshold.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/carea_map.py tests/test_carea_map_threshold.py
git commit -m "feat(depstor): reference-table loaders for carea_map percentile mode (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task C4: Wire `threshold_mode` through `carea_map.build`

**Files:**
- Modify: `src/gfv2_params/depstor_builders/carea_map.py`
- Modify: `configs/depstor/depstor_rasters.yml`

Drive the per-strip classify with a resolved threshold: scalar (`absolute`, `percentile`+conus, or `percentile`+vpu on a single-VPU fabric) or a per-cell array from the `vpu_id` raster (`percentile`+vpu on a multi-VPU fabric).

- [ ] **Step 1: Update `carea_map.build`**

Replace the `thresholds = step_cfg["thresholds"]` / `runs = [...]` block and the per-strip loop's threshold use. After computing `info = RasterInfo.from_path(ctx.template_path)` and before the `ExitStack`, resolve the threshold plan:

```python
    mode = step_cfg.get("threshold_mode", "absolute")
    outputs = step_cfg["outputs"]

    if mode == "absolute":
        thr = step_cfg["thresholds"]
        carea_t, smidx_t = float(thr["carea_max"]), float(thr["smidx"])
        per_cell = False
        # Guard: absolute thresholds are calibrated to ArcPy TWI only.
        if ctx.twi_raster and "hydrodem" in ctx.twi_raster.name:
            logger.warning(
                "  threshold_mode=absolute paired with a non-ArcPy TWI source "
                "(%s) — 8.0/15.6 are calibrated to ArcPy TWI; this is a "
                "validation-only counterexample, not a shippable output.",
                ctx.twi_raster.name,
            )
    elif mode == "percentile":
        scope = step_cfg["reference_scope"]          # "conus" | "vpu"
        table = load_reference_table(step_cfg["reference_table"])
        if scope == "conus" or ctx.vpu:
            # single threshold pair (CONUS, or a single-VPU fabric's VPU)
            carea_t, smidx_t = resolve_scalar_thresholds(
                table, scope, ctx.vpu if scope == "vpu" else None)
            per_cell = False
        else:
            # multi-VPU fabric, per-VPU scope -> per-cell threshold via vpu_id
            per_cell = True
            vpu_id_path = ctx.require("vpu_id")
            carea_lut = _threshold_lut(table, "t_carea")
            smidx_lut = _threshold_lut(table, "t_smidx")
    else:
        raise ValueError(f"carea_map: unknown threshold_mode {mode!r}")

    runs = [
        (ctx.resolve_output(outputs["carea_max"]), "carea_max"),
        (ctx.resolve_output(outputs["smidx"]),     "smidx_coef"),
    ]
```

Add the LUT helper near `load_reference_table`:

```python
def _threshold_lut(table: dict, column: str):
    """Build a code-indexed lookup array: lut[vpu_code] = threshold.

    vpu_code is the integer from vpu_id.vpu_to_code ('17' -> 17). Index 0 is the
    vpu_id nodata code; fill it with +inf so unmapped cells never pass twi>thr.
    """
    import numpy as np
    from .vpu_id import vpu_to_code
    rows = {vpu: row for (scope, vpu), row in table.items() if scope == "vpu"}
    size = max((vpu_to_code(v) for v in rows), default=0) + 1
    lut = np.full(size, np.inf, dtype="float64")
    for vpu, row in rows.items():
        lut[vpu_to_code(vpu)] = row[column]
    return lut
```

In the per-strip loop, build the threshold(s) per strip and pass to `compute_carea_map_binary`. Replace the existing `for i, (thresh, _, _) in enumerate(runs):` body:

```python
        if per_cell:
            vpu_id_src = stack.enter_context(rasterio.open(vpu_id_path))
            _assert_aligned(vpu_id_src, info, "vpu_id")

        for row_off in range(0, info.height, STRIP_ROWS):
            h = min(STRIP_ROWS, info.height - row_off)
            window = Window(0, row_off, info.width, h)
            land_valid = landmask_src.read(1, window=window) == 1
            perv = perv_src.read(1, window=window)
            onstream = onstream_src.read(1, window=window)
            twi = twi_vrt.read(1, window=window)
            if per_cell:
                codes = vpu_id_src.read(1, window=window)
                thresholds = [carea_lut[codes], smidx_lut[codes]]
            else:
                thresholds = [carea_t, smidx_t]
            for i, (out_path, _label) in enumerate(runs):
                out = compute_carea_map_binary(
                    perv, onstream, twi, thresholds[i], twi_nodata, land_valid
                )
                dsts[i].write(out, 1, window=window)
                counts[i] += int((out == 1).sum())
```

Update the `dsts = [...]` and the `if not ctx.force and all(out.exists()...)` / logging lines to use `runs` as `(out_path, label)` pairs (drop the old 3-tuple threshold element). Keep the early-return dict keys `{"carea_max": ..., "smidx": ...}`.

- [ ] **Step 2: Update the config block**

In `configs/depstor/depstor_rasters.yml`, replace the `carea_map` block:

```yaml
  - name: carea_map
    # threshold_mode: absolute (legacy 8.0/15.6, ArcPy TWI) | percentile (#55).
    # Percentile reads the reference table for the matching twi_source; pick the
    # source by pointing the profile's twi_raster at twi.vrt or twi_hydrodem.vrt.
    threshold_mode: percentile
    reference_scope: vpu          # vpu | conus
    reference_table: "{data_root}/shared/conus/twi_reference_percentiles.hydrodem.csv"
    # Used only when threshold_mode: absolute
    thresholds:
      carea_max: 8.0
      smidx:     15.6
    outputs:
      carea_max: carea_map_t8_binary.tif
      smidx:     carea_map_t156_binary.tif
```

Add a `vpu_id` step **before** `carea_map` (multi-VPU fabrics need it; single-VPU fabrics use the scalar path but the step is cheap and harmless):

```yaml
  - name: vpu_id
    output: vpu_id.tif
```

- [ ] **Step 3: Run the existing carea_map tests + import check**

Run: `pixi run -e dev pytest tests/test_carea_map_threshold.py -v`
Run: `pixi run --as-is python -c "import gfv2_params.depstor_builders.carea_map as m; print('ok')"`
Expected: PASS; `ok`.

- [ ] **Step 4: Commit**

```bash
git add src/gfv2_params/depstor_builders/carea_map.py configs/depstor/depstor_rasters.yml
git commit -m "feat(depstor): threshold_mode (absolute|percentile) in carea_map (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task C5: Register `vpu_id` in the depstor DAG + pass `vpu` into context

**Files:**
- Modify: `src/gfv2_params/depstor_builders/__init__.py`
- Modify: `scripts/build_depstor_rasters.py`

- [ ] **Step 1: Register the builder**

In `src/gfv2_params/depstor_builders/__init__.py`: add `vpu_id` to the imports, `BUILDERS`, and `STEP_ORDER` (immediately before `carea_map`):

```python
from . import carea_map, dprst, imperv, intersect, landmask, perv, routing, streambuffer, vpu_id, waterbody
```

```python
    "drains_imperv": intersect.build,
    "vpu_id":        vpu_id.build,
    "carea_map":     carea_map.build,
```

```python
    "drains_imperv",
    "vpu_id",
    "carea_map",
```

- [ ] **Step 2: Map `vpu_id`'s output + pass `vpu` into BuildContext**

In `scripts/build_depstor_rasters.py` `_expected_outputs`, add `vpu_id` to the single-output map:

```python
        single_key = {
            "landmask": "landmask",
            "imperv": "imperv",
            "streambuffer": "stream_buffer",
            "perv": "perv",
            "routing": "drains_to_dprst",
            "vpu_id": "vpu_id",
        }
```

In `_build_context`, pass the profile's optional `vpu`:

```python
        twi_raster=Path(config["twi_raster"]) if config.get("twi_raster") else None,
        vpu=config.get("vpu"),
        imperv_source=Path(config["imperv_source"]) if config.get("imperv_source") else None,
```

- [ ] **Step 3: Import + DAG check**

Run: `pixi run --as-is python -c "from gfv2_params.depstor_builders import STEP_ORDER; assert STEP_ORDER.index('vpu_id') < STEP_ORDER.index('carea_map'); print('ok', STEP_ORDER)"`
Expected: `ok [...]` with `vpu_id` before `carea_map`.

- [ ] **Step 4: Commit**

```bash
git add src/gfv2_params/depstor_builders/__init__.py scripts/build_depstor_rasters.py
git commit -m "feat(depstor): register vpu_id step + thread profile vpu into context (#55)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task C6: Profile wiring (oregon `vpu`, gfv2 attribute, twi sources)

**Files:**
- Modify: `configs/base_config.yml`

- [ ] **Step 1: Edit the oregon + gfv2 profiles**

In the **oregon** profile: add the single-VPU declaration and switch the depstor TWI source to the open-source VRT (percentile mode pairs with hydrodem). Replace the `twi_raster` line + #94 caveat block:

```yaml
    # Single-VPU fabric: declare the VPU so vpu_id is a constant fill and the
    # per-VPU reference threshold resolves to VPU 17.
    vpu: "17"
    # Depstor TWI source for percentile mode: open-source Twi_hydrodem (CONUS-
    # complete; #94). carea_map percentile mode reads
    # twi_reference_percentiles.hydrodem.csv (set in depstor_rasters.yml).
    twi_raster: "{data_root}/shared/conus/vrt/twi_hydrodem.vrt"
```

In the **gfv2** profile: do NOT set `vpu` (it must use the per-HRU `vpu` attribute). Switch its depstor `twi_raster` to `twi_hydrodem.vrt` as well, and update the existing TWI-coverage comment to note percentile mode + the per-HRU `vpu` attribute drives per-VPU thresholds.

```yaml
    twi_raster: "{data_root}/shared/conus/vrt/twi_hydrodem.vrt"
```

- [ ] **Step 2: Validate config loads for both fabrics**

Run:
```bash
pixi run --as-is python -c "
from gfv2_params.config import load_config
from pathlib import Path
for fab in ('oregon','gfv2'):
    c=load_config(Path('configs/depstor/depstor_rasters.yml'), fabric=fab)
    print(fab, 'twi_raster=', c.get('twi_raster'), 'vpu=', c.get('vpu'))
"
```
Expected: oregon → `twi_hydrodem.vrt`, `vpu=17`; gfv2 → `twi_hydrodem.vrt`, `vpu=None`.

- [ ] **Step 3: Commit**

```bash
git add configs/base_config.yml
git commit -m "feat(config): wire oregon/gfv2 to percentile mode + twi_hydrodem (#55/#94)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

# PHASE D — Validation (operational)

Run on the cluster after Phases A–C land. These produce the evidence in spec §5.

## Task D1: Oregon non-degeneracy

- [ ] **Step 1: Build vpu_id + carea_map for oregon (percentile)**

```bash
FABRIC=oregon pixi run --as-is python scripts/build_depstor_rasters.py \
  --config configs/depstor/depstor_rasters.yml --from vpu_id --force
```

- [ ] **Step 2: Verify the two binaries are no longer identical**

```bash
DR=$(awk '/^data_root:/ {print $2}' configs/base_config.yml)
pixi run --as-is python - <<PY
import rasterio, numpy as np
d="$DR/oregon/depstor_rasters"
a=rasterio.open(f"{d}/carea_map_t8_binary.tif").read(1)
b=rasterio.open(f"{d}/carea_map_t156_binary.tif").read(1)
print("identical:", np.array_equal(a,b), "  t8 ones:", int((a==1).sum()), " t156 ones:", int((b==1).sum()))
PY
```
Expected: `identical: False`, with `t8 ones > t156 ones` (smidx threshold is higher).

## Task D2: VPU 01 calibration A/B

- [ ] **Step 1: Run absolute (ArcPy) and percentile on gfv2_vpu01**

Run depstor rasters twice on `gfv2_vpu01`, once with the carea_map config `threshold_mode: absolute` + a temporary `twi_raster` pointing at the ArcPy `twi.vrt`, once with `threshold_mode: percentile`. Then run the params stage (`derive_depstor_params.py --mode zonal/merge/ratios` for `carea_t8_frac`, `carea_t156_frac`, `perv_frac`) for each and compare the merged `nhm_carea_max_params.csv` / `nhm_smidx_coef_params.csv`.

- [ ] **Step 2: Compare per-HRU**

```bash
pixi run --as-is python - <<'PY'
import pandas as pd, numpy as np
abs_ = pd.read_csv("ABSOLUTE/nhm_carea_max_params.csv")
pct  = pd.read_csv("PERCENTILE/nhm_carea_max_params.csv")
m = abs_.merge(pct, on="nat_hru_id", suffixes=("_abs","_pct"))
d = m["carea_max_pct"] - m["carea_max_abs"]
print("carea_max  mean|Δ|=", float(d.abs().mean()), " corr=", float(m["carea_max_abs"].corr(m["carea_max_pct"])))
PY
```
Expected: with the inverted-default percentiles, `mean|Δ|` small and correlation high — percentile mode reproduces the calibrated absolute output on VPU 01.

## Task D3: Source A/B (invariance proof)

- [ ] **Step 1: Run percentile mode on VPU 01 against both sources**

Build carea_map on `gfv2_vpu01` with `threshold_mode: percentile` twice — once with `twi_raster: twi.vrt` + `reference_table: ...arcpy.csv`, once with `twi_hydrodem.vrt` + `...hydrodem.csv` — and also `threshold_mode: absolute` against both sources.

- [ ] **Step 2: Quantify movement**

Compare the resulting `carea_max`/`smidx_coef` between sources for each mode (same per-HRU diff as D2).
Expected: percentile-mode parameters barely move between ArcPy and hydrodem (small `mean|Δ|`); absolute-mode parameters move a lot — demonstrating the invariance claim (spec §5).

---

# PHASE E — Docs + issue housekeeping

## Task E1: Documentation

**Files:**
- Modify: `README.md`, `slurm_batch/RUNME.md`, `configs/base_config.yml`, `src/gfv2_params/shared_rasters/build_vrt.py`

- [ ] **Step 1: Update docs**

- `slurm_batch/RUNME.md`: add a stage for `submit_twi_completion.sh` (finish `twi.vrt` + build `twi_hydrodem.vrt`), the `twi_reference` step, and percentile-mode carea_map (note the `vpu_id` step + mode↔source pairing).
- `README.md`: replace the line that says the calibration thresholds reference the canonical ArcPy TWI with a pointer to the percentile refactor + the spec; note `twi_hydrodem.vrt` is now a first-class source.
- `build_vrt.py`: soften the "DO NOT SWAP" comment on the `twi` type to: absolute mode still requires ArcPy TWI, but percentile mode (`twi_reference` + carea_map `threshold_mode: percentile`) makes `twi_hydrodem` safe — cross-reference the spec.
- `configs/base_config.yml`: replace the oregon `⚠️ #94` carea-degenerate caveat with a note that percentile mode + `twi_hydrodem.vrt` resolves it.

- [ ] **Step 2: Commit**

```bash
git add README.md slurm_batch/RUNME.md configs/base_config.yml src/gfv2_params/shared_rasters/build_vrt.py
git commit -m "docs(depstor): document TWI completion, twi_reference, percentile carea_map (#55/#94)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

## Task E2: Issue housekeeping + PR

- [ ] **Step 1: Open the combined umbrella issue + cross-link**

```bash
gh issue comment 94 --body "Folded into the percentile refactor on feat/twi-percentile-carea-smidx (see docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md). Resolved by building twi_hydrodem.vrt + finishing twi.vrt + data-derived percentile thresholds."
gh issue comment 55 --body "Stage 1 implemented on feat/twi-percentile-carea-smidx (per-HRU/VPU + CONUS percentile cutoffs, valid-land reference, CDF-inverted defaults). Stage 2 (NWI/SSURGO/NLCD observational decoupling) remains open."
```

- [ ] **Step 2: Run pre-commit + open the PR**

```bash
pixi run -e dev pre-commit run --all-files
git push -u origin feat/twi-percentile-carea-smidx
gh pr create --fill --title "Distribution-invariant carea_max/smidx_coef: TWI percentile thresholds (#55/#94)"
```

Lead the PR body with a scope summary: TWI source completion (Phase A), reference-percentile pre-step (B), percentile-mode carea_map reusing the binary builder (C), validation evidence (D), docs (E). Note #55 Stage 2 is deferred. Let CI run `pytest` (the test gate).

---

## Self-review notes (for the implementer)

- **Spec coverage:** §4.1 → A1/A2; §4.2 + §3.4 → B1/B2/B3; §4.3 → C1/C3/C4; §4.4 (pairing + guard) → C4 Step 1 guard + C6; §4.5 (per-VPU) → C2/C4/C5/C6; §5 → D1/D2/D3; §6 docs → E1; issue housekeeping → E2.
- **Threshold contract:** `compute_carea_map_binary(threshold)` is scalar-or-array (C1); `_threshold_lut` fills nodata code 0 with `+inf` so unmapped cells never pass — keep that invariant.
- **Single-VPU fast path:** when `ctx.vpu` is set, percentile-`vpu` uses `resolve_scalar_thresholds` (no vpu_id read needed), so oregon does not depend on the multi-VPU array path.
