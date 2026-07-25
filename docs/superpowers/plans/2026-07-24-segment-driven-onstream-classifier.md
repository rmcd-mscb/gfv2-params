# Segment-Driven On-Stream Classifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change the source of the depression-storage on-stream waterbody set from NHD flowline topology to the model's own `nsegment` network — a waterbody is on-stream iff some segment intersects it with positive length — and move `oregon`/`gfv2_dev`/`tjc` onto the current `nhd_waterbodies.gpkg`.

**Architecture:** A new pure-logic module `gfv2_params.segment_wbody` plus a thin builder `depstor_builders/segment_wbody.py` mirror the existing `endorheic` split. The builder writes `segment_waterbody_comids.parquet`; `wbody_connectivity` consumes it as its **required primary** on-stream source, with the two NHD COMID tables demoted to opt-in comparison inputs. Every existing guard downstream — endorheic subtraction, Playa/Ice Mass never-on-stream, the `endorheic_wbody` exemption in `dprst`, the routing barrier, the same-HRU restriction — is untouched.

**Tech Stack:** Python 3.12, geopandas 1.1.3, shapely 2.1.2, pandas 3.0.2, rasterio, pyarrow/parquet, pytest, pixi.

**Spec:** [`docs/superpowers/specs/2026-07-24-segment-driven-onstream-classifier-design.md`](../specs/2026-07-24-segment-driven-onstream-classifier-design.md)

## Global Constraints

- **Never run `pytest` on the HPC head node.** CI (`.github/workflows/ci.yml`) is the test gate. Local verification is `pixi run --as-is python -m py_compile <file>` and import checks only. Where a step says "run the test", that means push and let CI run it — or run it on a compute node via `srun`.
- Every test must run in CI with **no data root**. CI has no `/caldera` mount. Synthetic geometry or a committed fixture only.
- Add deps via `pyproject.toml`, never `environment.yml`. No new deps are needed for this plan.
- Paths and fabric inputs come from the profile via `require_config_key(...)` / `config.get(...)`, never hardcoded.
- Run `pixi run -e dev pre-commit run --all-files` before pushing.
- **Atomic commits.** One commit per task minimum; split further if a task produces separable changes.
- The positive-length rule is measured in **linear CRS units (metres)**. Both layers must be in the same projected CRS before intersecting.
- `n_segments` counts **distinct segments**, and pair rows are keyed on the **waterbody row index**, never COMID — the layer has 448,124 rows for 447,907 distinct COMIDs, so a COMID-keyed merge duplicates pairs.
- The FTYPE (Playa/Ice Mass) guardrail stays in `wbody_connectivity` and is **not** duplicated into `segment_wbody`; the builder is deliberately FTYPE-agnostic so the guard applies identically to the segment and NHD paths.

---

## File Structure

**Create:**
- `src/gfv2_params/segment_wbody.py` — pure logic: sjoin, geometry repair, chunked intersection length, per-COMID aggregation, parquet I/O, floor check. No I/O of config, no rasterio.
- `src/gfv2_params/depstor_builders/segment_wbody.py` — builder: reads profile inputs, extent guard, calls pure logic, writes the parquet, registers `segment_wbody_comids`.
- `tests/test_segment_wbody.py` — synthetic-geometry unit tests for the pure logic and the builder.
- `tests/test_segment_onstream_config.py` — invariants on the real `configs/base_config.yml`.
- `tests/data/OR_waterbody_nseg_intersection.csv` — the handoff crosswalk, as versioned provenance.
- `tests/data/segment_wbody_oregon_fixture.gpkg` — 11 real Oregon COMIDs + their intersecting segments, for a real-geometry CI test.
- `tests/data/README.md` — how the fixture was cut, so it is reproducible.
- `scripts/diagnose/ab_segment_vs_nhd_onstream.py` — A/B the builder's output against the NHD tables.

**Modify:**
- `src/gfv2_params/depstor_builders/context.py` — add `min_onstream_comids`.
- `src/gfv2_params/depstor_builders/__init__.py` — register builder in `BUILDERS`, insert into `STEP_ORDER`, document the new output key.
- `scripts/build_depstor_rasters.py` — pass `min_onstream_comids` into the context; map the new step in `_expected_outputs`.
- `configs/depstor/depstor_rasters.yml` — add the `segment_wbody` step.
- `src/gfv2_params/depstor_builders/wbody_connectivity.py` — segment table required and primary; NHD tables optional with a loud warning.
- `tests/test_wbody_connectivity.py` — every builder test needs a segment table wired in; one test changes meaning.
- `configs/base_config.yml` — 4 fabric profiles.
- `CLAUDE.md`, `docs/ARCHITECTURE.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`, the depstor workflow docs page, `scripts/render_depstor_figures.py`.

**Delete:**
- `scripts/diagnose/measure_segment_onstream.py` — an untracked throwaway with hardcoded paths that produced the spec's CONUS numbers. Superseded by the builder plus `ab_segment_vs_nhd_onstream.py`. It is untracked, so this is `rm`, not a git delete.

---

## Task 1: Pure logic — `gfv2_params.segment_wbody`

**Files:**
- Create: `src/gfv2_params/segment_wbody.py`
- Test: `tests/test_segment_wbody.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `CHUNK: int = 5000`
  - `repair_invalid(gdf: GeoDataFrame, *, name: str, logger=None) -> tuple[GeoDataFrame, int]`
  - `segment_waterbody_pairs(seg_gdf, wb_gdf, *, logger=None) -> DataFrame[comid:int64, wb_index:int64, seg_index:int64, overlap_m:float64]`
  - `segment_waterbody_comids(pairs: DataFrame) -> set[int]`
  - `segment_comid_frame(pairs: DataFrame) -> DataFrame[comid:int64, n_segments:int64, overlap_m:float64]`
  - `write_segment_comids(df: DataFrame, out_path: Path) -> None`
  - `load_segment_comids(path: Path) -> set[int]`
  - `check_onstream_floor(n: int, *, fabric: str, floor: int | None, source) -> None`

- [ ] **Step 1: Write the failing test file**

Create `tests/test_segment_wbody.py`:

```python
"""Tests for the segment-driven on-stream waterbody classifier (pure logic).

Fully synthetic geometry: CI runs with no data root. The five edge cases below are
DELIBERATE decisions recorded in the design spec, not incidental behaviour —
notably that a shoreline-collinear segment and a segment terminating inside a
waterbody both PROMOTE it.
"""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import LineString, Polygon

from gfv2_params.segment_wbody import (
    check_onstream_floor,
    load_segment_comids,
    repair_invalid,
    segment_comid_frame,
    segment_waterbody_comids,
    segment_waterbody_pairs,
    write_segment_comids,
)

# A 10x10 unit square lake at the origin; all fixtures below are placed against it.
_LAKE = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])

# The five verified edge cases. `expected` is whether the rule PROMOTES.
_EDGE_CASES = [
    ("crosses_interior", LineString([(-1, 5), (11, 5)]), 10.0, True),
    ("single_point_touch", LineString([(-1, -1), (0, 0), (-1, 1)]), 0.0, False),
    ("corner_clip_through_vertex", LineString([(-1, 1), (1, -1)]), 0.0, False),
    ("collinear_with_shoreline", LineString([(2, 0), (8, 0)]), 6.0, True),
    ("endpoint_inside", LineString([(-1, 5), (5, 5)]), 5.0, True),
]


def _wb(comids, geoms=None, ftypes=None):
    geoms = geoms if geoms is not None else [_LAKE] * len(comids)
    data = {"COMID": comids}
    if ftypes is not None:
        data["FTYPE"] = ftypes
    return gpd.GeoDataFrame(data, geometry=geoms, crs="EPSG:5070")


def _seg(geoms):
    return gpd.GeoDataFrame(geometry=list(geoms), crs="EPSG:5070")


@pytest.mark.parametrize("name,line,expected_len,expected_kept", _EDGE_CASES)
def test_edge_case_overlap_and_promotion(name, line, expected_len, expected_kept):
    pairs = segment_waterbody_pairs(_seg([line]), _wb([7]))
    assert len(pairs) == 1, f"{name}: sjoin should match the lake"
    assert pairs["overlap_m"].iloc[0] == pytest.approx(expected_len), name
    assert (segment_waterbody_comids(pairs) == {7}) is expected_kept, name


def test_disjoint_segment_produces_no_pair_and_no_comid():
    pairs = segment_waterbody_pairs(_seg([LineString([(50, 50), (60, 60)])]), _wb([7]))
    assert pairs.empty
    assert segment_waterbody_comids(pairs) == set()
    assert segment_comid_frame(pairs).empty


def test_multi_segment_lake_aggregates_to_one_comid():
    # Three distinct segments crossing the same lake -> one COMID, n_segments == 3,
    # overlap_m summed. n_segments must count SEGMENTS, not pair rows.
    lines = [
        LineString([(-1, 2), (11, 2)]),
        LineString([(-1, 5), (11, 5)]),
        LineString([(-1, 8), (11, 8)]),
    ]
    frame = segment_comid_frame(segment_waterbody_pairs(_seg(lines), _wb([7])))
    assert frame["comid"].tolist() == [7]
    assert frame["n_segments"].iloc[0] == 3
    assert frame["overlap_m"].iloc[0] == pytest.approx(30.0)


def test_multi_row_comid_does_not_inflate_n_segments():
    # A multi-part waterbody is several ROWS sharing one COMID (448,124 rows /
    # 447,907 COMIDs on the real layer). One segment crossing both rows must give
    # n_segments == 1, not 2 -- this is why pairs are keyed on the row index.
    left = Polygon([(0, 0), (4, 0), (4, 10), (0, 10)])
    right = Polygon([(6, 0), (10, 0), (10, 10), (6, 10)])
    pairs = segment_waterbody_pairs(
        _seg([LineString([(-1, 5), (11, 5)])]), _wb([7, 7], geoms=[left, right])
    )
    assert len(pairs) == 2, "one segment x two rows = two pair rows"
    assert pairs["wb_index"].nunique() == 2
    frame = segment_comid_frame(pairs)
    assert frame["comid"].tolist() == [7]
    assert frame["n_segments"].iloc[0] == 1


def test_ftype_is_not_filtered_here():
    # The Playa/Ice Mass guardrail lives in wbody_connectivity so it applies to the
    # NHD comparison path too. This builder must stay FTYPE-agnostic.
    pairs = segment_waterbody_pairs(
        _seg([LineString([(-1, 5), (11, 5)])]),
        _wb([7], ftypes=["Playa"]),
    )
    assert segment_waterbody_comids(pairs) == {7}


def test_invalid_geometry_is_repaired_and_still_classified():
    bowtie = Polygon([(0, 0), (10, 10), (10, 0), (0, 10)])  # self-intersecting
    repaired, n = repair_invalid(_wb([7], geoms=[bowtie]), name="waterbodies")
    assert n == 1
    pairs = segment_waterbody_pairs(_seg([LineString([(-1, 2), (11, 2)])]), repaired)
    assert segment_waterbody_comids(pairs) == {7}


def test_repair_invalid_is_a_noop_on_valid_geometry():
    gdf, n = repair_invalid(_wb([7]), name="waterbodies")
    assert n == 0
    assert gdf.geometry.iloc[0].equals(_LAKE)


def test_non_numeric_comid_rows_are_dropped():
    wb = gpd.GeoDataFrame(
        {"COMID": ["7", "not-a-comid"]},
        geometry=[_LAKE, Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])],
        crs="EPSG:5070",
    )
    seg = _seg([LineString([(-1, 5), (11, 5)]), LineString([(19, 5), (31, 5)])])
    pairs = segment_waterbody_pairs(seg, wb)
    assert segment_waterbody_comids(pairs) == {7}
    assert pairs["comid"].dtype == np.dtype("int64")


def test_missing_comid_column_raises():
    wb = gpd.GeoDataFrame({"GNIS_NAME": ["x"]}, geometry=[_LAKE], crs="EPSG:5070")
    with pytest.raises(KeyError, match="COMID"):
        segment_waterbody_pairs(_seg([LineString([(-1, 5), (11, 5)])]), wb)


def test_geographic_crs_raises():
    # The rule is a length in metres; a degree-based CRS would silently compare
    # degrees against zero and "work", so refuse it.
    wb = _wb([7]).to_crs("EPSG:4326")
    seg = _seg([LineString([(-1, 5), (11, 5)])]).to_crs("EPSG:4326")
    with pytest.raises(ValueError, match="projected"):
        segment_waterbody_pairs(seg, wb)


def test_missing_crs_raises():
    wb = gpd.GeoDataFrame({"COMID": [7]}, geometry=[_LAKE], crs=None)
    with pytest.raises(ValueError, match="CRS"):
        segment_waterbody_pairs(_seg([LineString([(-1, 5), (11, 5)])]), wb)


def test_segments_are_reprojected_to_the_waterbody_crs():
    # A segment layer in a different projected CRS must be reprojected, not rejected:
    # the oregon fabric and the CONUS waterbody layer are both 5070 today, but a
    # fabric-local CRS must not silently produce zero pairs.
    seg = _seg([LineString([(-1, 5), (11, 5)])]).to_crs("EPSG:3857")
    pairs = segment_waterbody_pairs(seg, _wb([7]))
    assert segment_waterbody_comids(pairs) == {7}


def test_parquet_round_trip(tmp_path):
    frame = segment_comid_frame(
        segment_waterbody_pairs(_seg([LineString([(-1, 5), (11, 5)])]), _wb([7]))
    )
    path = tmp_path / "segment_waterbody_comids.parquet"
    write_segment_comids(frame, path)
    assert load_segment_comids(path) == {7}


def test_empty_frame_round_trips_with_a_stable_schema(tmp_path):
    path = tmp_path / "empty.parquet"
    write_segment_comids(segment_comid_frame(pd.DataFrame(
        {"comid": pd.array([], dtype="int64"), "wb_index": pd.array([], dtype="int64"),
         "seg_index": pd.array([], dtype="int64"),
         "overlap_m": pd.array([], dtype="float64")}
    )), path)
    assert load_segment_comids(path) == set()


def test_check_onstream_floor_raises_below_floor(tmp_path):
    with pytest.raises(ValueError, match="min_onstream_comids"):
        check_onstream_floor(12, fabric="gfv2", floor=30000, source=tmp_path / "t.parquet")


def test_check_onstream_floor_is_opt_in(tmp_path):
    check_onstream_floor(0, fabric="tjc", floor=None, source=tmp_path / "t.parquet")


def test_check_onstream_floor_passes_at_floor(tmp_path):
    check_onstream_floor(30000, fabric="gfv2", floor=30000, source=tmp_path / "t.parquet")
```

- [ ] **Step 2: Run the test to verify it fails**

On a compute node (never the head node):

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_segment_wbody.py -x -q
```

Expected: collection error — `ModuleNotFoundError: No module named 'gfv2_params.segment_wbody'`.

- [ ] **Step 3: Write the implementation**

Create `src/gfv2_params/segment_wbody.py`:

```python
"""Segment-driven on-stream waterbody classifier.

A waterbody is ON-STREAM iff a stream segment from the MODEL's own routing network
(the fabric's `segments_gpkg`) intersects it with POSITIVE LENGTH. This replaces the
NHD-flowline question ("is this waterbody on the NHD network?") with the one the
parameters actually need ("is it on the network the model routes?"): NHD's network is
far finer, so a waterbody NHD routes but the model does not had no representation at
all -- neither stream routing nor depression storage.

Positive length, not bare `intersects`: a segment grazing a shoreline at a single
point promotes the whole waterbody, the same failure mode `CLAUDE.md` warns about for
containment tests. On the retired `conus_waterbodies.gpkg` 19.0% of oregon's candidate
pairs were zero-length grazes; on the current `nhd_waterbodies.gpkg` it is 3.1% (2,300
of 73,343 CONUS pairs), so the rule is cheap insurance against shoreline-vintage
mis-registration rather than a large correction. WBAREACOMI corroborates it: of the 6
COMIDs it drops in oregon, 4 have no NHD signal at all and 2 are Playa.

Two consequences of "positive length" are DELIBERATE, verified against shapely, and
pinned by tests:

  * a segment COLLINEAR with the shoreline has positive length and IS promoted --
    a line the modeller drew along the lake edge is a routing decision;
  * a segment TERMINATING inside a waterbody IS promoted, whereas NHD's flow-through
    rule required both inflow and outflow. That discrimination is gone here, so the
    `endorheic` subtraction in `wbody_connectivity` is what still demotes genuinely
    closed terminal lakes.

This module is FTYPE-agnostic on purpose. The Playa/Ice Mass never-on-stream guardrail
lives in `wbody_connectivity`, at the chokepoint both this source and the opt-in NHD
comparison sources pass through.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

# Pair intersections are computed in chunks so ONE unrepairable geometry cannot abort a
# whole CONUS run (73,343 candidate pairs). Big enough that the vectorised GEOS call
# dominates, small enough to bound the per-row fallback.
CHUNK = 5000

_PAIR_DTYPES = {
    "comid": "int64",
    "wb_index": "int64",
    "seg_index": "int64",
    "overlap_m": "float64",
}
_FRAME_DTYPES = {"comid": "int64", "n_segments": "int64", "overlap_m": "float64"}


def _empty_pairs() -> pd.DataFrame:
    return pd.DataFrame({k: pd.array([], dtype=v) for k, v in _PAIR_DTYPES.items()})


def repair_invalid(gdf: gpd.GeoDataFrame, *, name: str, logger=None):
    """Return `(gdf, n_repaired)` with `shapely.make_valid` applied to invalid rows.

    Found the hard way: the CONUS measurement run died with
    `GEOSException: TopologyException: side location conflict` after 73,343 candidate
    pairs. `sjoin` survives invalid polygons because prepared predicates are tolerant;
    `.intersection()` does not. 193 of the 448,124 CONUS waterbody polygons need this.
    Oregon exposes only 10, and a synthetic test none at all, so this is a CONUS-only
    failure -- the same family as the NHD measured-3D XYZM -> `Point()` crash (2a67d85).

    Raises if a geometry survives `make_valid` still invalid: proceeding would take the
    per-row fallback in `_overlap_lengths` for every pair it appears in, and a NaN
    overlap scored as zero-length would demote a waterbody to dprst on a geometry error.
    """
    geoms = np.asarray(gdf.geometry.values)
    invalid = ~shapely.is_valid(geoms)
    n_invalid = int(invalid.sum())
    if not n_invalid:
        return gdf, 0
    geoms = geoms.copy()
    geoms[invalid] = shapely.make_valid(geoms[invalid])
    n_still = int((~shapely.is_valid(geoms)).sum())
    if n_still:
        raise ValueError(
            f"{n_still} {name} geometr(ies) are still invalid after make_valid — the "
            f"intersection-length pass cannot measure them, and scoring an unmeasurable "
            f"pair as zero-length would silently demote a waterbody to depression "
            f"storage. Fix the source layer."
        )
    if logger:
        logger.info("  %s: repaired %d invalid geometr(ies) with make_valid", name, n_invalid)
    return gdf.set_geometry(
        gpd.GeoSeries(geoms, index=gdf.index, crs=gdf.crs)
    ), n_invalid


def _overlap_lengths(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    """Intersection length per pair, chunked, with a per-row fallback.

    A GEOS failure inside one chunk must not lose the other 4,999 pairs, so the chunk is
    retried row by row and only the genuinely unmeasurable pair is scored NaN. The caller
    RAISES on any NaN -- see `segment_waterbody_pairs`.
    """
    out = np.full(len(left), np.nan, dtype="float64")
    for start in range(0, len(left), CHUNK):
        stop = min(start + CHUNK, len(left))
        try:
            out[start:stop] = shapely.length(
                shapely.intersection(left[start:stop], right[start:stop])
            )
        except Exception:
            for i in range(start, stop):
                try:
                    out[i] = shapely.length(shapely.intersection(left[i], right[i]))
                except Exception:
                    out[i] = np.nan
    return out


def segment_waterbody_pairs(seg_gdf, wb_gdf, *, logger=None) -> pd.DataFrame:
    """One row per intersecting (segment, waterbody-ROW) pair, with `overlap_m`.

    Columns: comid, wb_index, seg_index, overlap_m.

    Keyed on the waterbody ROW index, never on COMID: the layer holds 448,124 rows for
    447,907 distinct COMIDs, so merging pair rows back on COMID duplicates them (it
    inflated the measurement script's CONUS pair count from 73,343 to 73,723). Harmless
    for a COMID set, but it would corrupt `n_segments` and `overlap_m`.
    """
    if "COMID" not in wb_gdf.columns:
        raise KeyError(
            "waterbody layer has no COMID column — the segment on-stream classifier "
            "emits a COMID table and cannot run. Use a fabric whose waterbody layer "
            "carries COMID (e.g. `gfv2`, `oregon`), not the NHM_01_draft `wbs` layer."
        )
    if seg_gdf.crs is None or wb_gdf.crs is None:
        raise ValueError(
            f"the segment on-stream classifier needs a CRS on both layers to measure an "
            f"intersection length (segments: {seg_gdf.crs}, waterbodies: {wb_gdf.crs}). "
            f"Without one the layers could be in different units and every pair would be "
            f"mis-measured, silently."
        )
    if not wb_gdf.crs.is_projected:
        raise ValueError(
            f"the waterbody layer is in the geographic CRS {wb_gdf.crs}; the "
            f"positive-length rule is a LENGTH and must be measured in a projected CRS "
            f"(the pipeline uses EPSG:5070). In degrees the comparison against zero would "
            f"still 'work' while the threshold meant nothing."
        )
    if seg_gdf.crs != wb_gdf.crs:
        if logger:
            logger.info(
                "  reprojecting segments from %s to the waterbody CRS %s",
                seg_gdf.crs, wb_gdf.crs,
            )
        seg_gdf = seg_gdf.to_crs(wb_gdf.crs)

    seg_ok = seg_gdf.geometry.notna() & ~seg_gdf.geometry.is_empty
    wb_ok = wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty
    seg = gpd.GeoDataFrame(
        geometry=seg_gdf.geometry[seg_ok].reset_index(drop=True), crs=wb_gdf.crs
    )
    wb = gpd.GeoDataFrame(
        {"COMID": pd.to_numeric(wb_gdf["COMID"][wb_ok], errors="coerce").to_numpy()},
        geometry=wb_gdf.geometry[wb_ok].reset_index(drop=True),
        crs=wb_gdf.crs,
    )
    if seg.empty or wb.empty:
        return _empty_pairs()

    seg, _ = repair_invalid(seg, name="segments", logger=logger)
    wb, _ = repair_invalid(wb, name="waterbodies", logger=logger)

    joined = gpd.sjoin(seg, wb[["geometry"]], how="inner", predicate="intersects")
    if joined.empty:
        return _empty_pairs()
    seg_index = joined.index.to_numpy()
    wb_index = joined["index_right"].to_numpy()
    if logger:
        logger.info("  %d candidate (segment, waterbody) pairs", len(joined))

    overlap = _overlap_lengths(
        np.asarray(seg.geometry.values)[seg_index],
        np.asarray(wb.geometry.values)[wb_index],
    )
    n_unmeasurable = int(np.isnan(overlap).sum())
    if n_unmeasurable:
        raise ValueError(
            f"{n_unmeasurable} of {len(overlap)} (segment, waterbody) pairs could not be "
            f"intersected even after make_valid. Refusing to score them: an unmeasurable "
            f"pair treated as zero-length would demote its waterbody to depression "
            f"storage on a geometry error rather than a hydrologic result."
        )

    comid = wb["COMID"].to_numpy()[wb_index]
    pairs = pd.DataFrame(
        {
            "comid": comid,
            "wb_index": wb_index.astype("int64"),
            "seg_index": seg_index.astype("int64"),
            "overlap_m": overlap,
        }
    )
    n_bad = int(pairs["comid"].isna().sum())
    if n_bad:
        pairs = pairs[pairs["comid"].notna()]
        if logger:
            logger.warning(
                "  dropped %d pair(s) whose waterbody COMID is non-numeric/NaN", n_bad
            )
    n_zero = int((pairs["overlap_m"] == 0).sum())
    if logger:
        logger.info(
            "  %d of %d pairs are zero-length grazes (%.1f%%) and are dropped",
            n_zero, len(pairs), 100.0 * n_zero / max(len(pairs), 1),
        )
    return pairs.astype(_PAIR_DTYPES).reset_index(drop=True)


def segment_waterbody_comids(pairs: pd.DataFrame) -> set[int]:
    """COMIDs with at least one POSITIVE-LENGTH pair — the on-stream set."""
    if pairs.empty:
        return set()
    return {int(c) for c in pairs.loc[pairs["overlap_m"] > 0, "comid"].unique()}


def segment_comid_frame(pairs: pd.DataFrame) -> pd.DataFrame:
    """Per-COMID aggregate over POSITIVE-LENGTH pairs: comid, n_segments, overlap_m.

    `n_segments` counts DISTINCT segments, so a multi-part waterbody (several rows
    sharing a COMID) crossed by one segment reports 1, not one per row.
    """
    positive = pairs[pairs["overlap_m"] > 0] if not pairs.empty else pairs
    if positive.empty:
        return pd.DataFrame({k: pd.array([], dtype=v) for k, v in _FRAME_DTYPES.items()})
    return (
        positive.groupby("comid", as_index=False)
        .agg(n_segments=("seg_index", "nunique"), overlap_m=("overlap_m", "sum"))
        .astype(_FRAME_DTYPES)
    )


def write_segment_comids(df: pd.DataFrame, out_path: Path) -> None:
    """Write the on-stream COMID table with pinned dtypes.

    Dtypes are pinned so an empty table round-trips with the same schema as a populated
    one, instead of writing all-null columns `load_segment_comids` has to guess at.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.astype(_FRAME_DTYPES).to_parquet(out_path, index=False)


def load_segment_comids(path: Path) -> set[int]:
    """Load the segment-derived on-stream COMID set."""
    df = pd.read_parquet(path, columns=["comid"])
    if df.empty:
        return set()
    return {int(c) for c in df["comid"].to_numpy()}


def check_onstream_floor(n: int, *, fabric: str, floor: int | None, source) -> None:
    """Raise if a fabric declaring `min_onstream_comids` has a collapsed on-stream set.

    Opt-in per fabric, exactly like `min_endorheic_comids`: `gfv2` measures 48,529 and
    `oregon` 770, while a small fabric may legitimately have few. Applied at BOTH the
    producing builder and the consuming `wbody_connectivity`, because `--from
    wbody_connectivity` (the documented cascade-rebuild recipe) leaves the producer out
    of the run list and the orchestrator hydrates its table off disk unvalidated — a
    guard living only in the producer never runs on the path operators actually use.
    """
    if floor is None:
        return
    if n < floor:
        raise ValueError(
            f"segment on-stream table for fabric '{fabric}' carries {n} COMIDs, below its "
            f"declared `min_onstream_comids` floor of {floor} ({source}). That is a "
            f"collapsed classifier: nearly every waterbody would become depression "
            f"storage. Check that `segments_gpkg` covers the fabric domain and shares a "
            f"projected CRS with `waterbody_gpkg`, then re-run the `segment_wbody` step "
            f"with --force. Lower or remove `min_onstream_comids` in the fabric profile "
            f"ONLY if this domain genuinely has that few on-stream waterbodies."
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_segment_wbody.py -q
```

Expected: all pass (23 tests, counting the 5 parametrised edge cases).

- [ ] **Step 5: Lint and commit**

```bash
pixi run -e dev pre-commit run --files src/gfv2_params/segment_wbody.py tests/test_segment_wbody.py
git add src/gfv2_params/segment_wbody.py tests/test_segment_wbody.py
git commit -m "feat(depstor): segment-driven on-stream classifier (pure logic)

A waterbody is on-stream iff a model nsegment intersects it with positive
length. Pairs are keyed on the waterbody row index, not COMID, because the
layer has 448,124 rows for 447,907 distinct COMIDs. Invalid geometry is
repaired with make_valid and an unmeasurable pair RAISES rather than scoring
zero-length, which would demote a waterbody to dprst on a geometry error."
```

---

## Task 2: Builder, DAG registration, config, context plumbing

**Files:**
- Create: `src/gfv2_params/depstor_builders/segment_wbody.py`
- Modify: `src/gfv2_params/depstor_builders/context.py`, `src/gfv2_params/depstor_builders/__init__.py`, `scripts/build_depstor_rasters.py`, `configs/depstor/depstor_rasters.yml`
- Test: `tests/test_segment_wbody.py` (append builder tests)

**Interfaces:**
- Consumes: everything Task 1 produced.
- Produces:
  - `depstor_builders.segment_wbody.build(step_cfg: dict, ctx: BuildContext, logger) -> dict` returning `{"segment_wbody_comids": Path}`
  - `BuildContext.min_onstream_comids: int | None`
  - `BUILDERS["segment_wbody"]`, and `"segment_wbody"` in `STEP_ORDER` between `endorheic` and `wbody_connectivity`
  - `_expected_outputs` maps the `segment_wbody` step to `{"segment_wbody_comids": <filename>}`

- [ ] **Step 1: Write the failing builder tests**

Append to `tests/test_segment_wbody.py`:

```python
# ---------------------------------------------------------------------------
# Builder tests
# ---------------------------------------------------------------------------

import logging

import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

_STEP_CFG = {"output": "segment_waterbody_comids.parquet"}


def _write_template(path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="float32",
        crs="EPSG:5070", transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(np.full((n, n), 100.0, dtype=np.float32), 1)


def _builder_ctx(tmp_path, *, seg_geoms, wb_comids, wb_geoms, **kw):
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    _write_template(template)
    seg_gpkg = tmp_path / "seg.gpkg"
    wb_gpkg = tmp_path / "wb.gpkg"
    gpd.GeoDataFrame({"seg_id": range(len(seg_geoms))}, geometry=list(seg_geoms),
                     crs="EPSG:5070").to_file(seg_gpkg, layer="nsegment", driver="GPKG")
    gpd.GeoDataFrame({"COMID": list(wb_comids), "FTYPE": ["LakePond"] * len(wb_comids)},
                     geometry=list(wb_geoms),
                     crs="EPSG:5070").to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    return BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        segments_gpkg=seg_gpkg, segments_layer="nsegment",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies", **kw,
    )


def test_builder_writes_only_positive_length_comids(tmp_path):
    from gfv2_params.depstor_builders import segment_wbody as builder

    # COMID 10: crossed through the interior -> on-stream.
    # COMID 20: grazed at a single boundary point -> NOT on-stream.
    ctx = _builder_ctx(
        tmp_path,
        seg_geoms=[LineString([(-10, 285), (70, 285)]), LineString([(230, -10), (240, 0)])],
        wb_comids=[10, 20],
        wb_geoms=[box(0, 270, 60, 300), box(240, 0, 300, 30)],
    )
    produced = builder.build(_STEP_CFG, ctx, logging.getLogger("test"))
    path = produced["segment_wbody_comids"]
    assert path == tmp_path / "segment_waterbody_comids.parquet"
    assert load_segment_comids(path) == {10}
    frame = pd.read_parquet(path)
    assert sorted(frame.columns) == ["comid", "n_segments", "overlap_m"]


def test_builder_requires_segments_gpkg(tmp_path):
    from gfv2_params.depstor_builders import segment_wbody as builder
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    _write_template(template)
    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="nhru",
        segments_gpkg=None,
        waterbody_gpkg=tmp_path / "x.gpkg", waterbody_layer="waterbodies",
    )
    with pytest.raises(KeyError, match="segments_gpkg"):
        builder.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_builder_raises_when_segments_do_not_overlap_the_template(tmp_path):
    # A segments_gpkg mis-wired to another fabric would otherwise make every
    # waterbody depression storage and exit 0.
    from gfv2_params.depstor_builders import segment_wbody as builder

    ctx = _builder_ctx(
        tmp_path,
        seg_geoms=[LineString([(9_000_000, 9_000_000), (9_000_100, 9_000_100)])],
        wb_comids=[10],
        wb_geoms=[box(0, 270, 60, 300)],
    )
    with pytest.raises(ValueError, match="does not overlap"):
        builder.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_builder_enforces_the_floor(tmp_path):
    from gfv2_params.depstor_builders import segment_wbody as builder

    ctx = _builder_ctx(
        tmp_path,
        seg_geoms=[LineString([(-10, 285), (70, 285)])],
        wb_comids=[10],
        wb_geoms=[box(0, 270, 60, 300)],
        min_onstream_comids=500,
    )
    with pytest.raises(ValueError, match="min_onstream_comids"):
        builder.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_builder_floor_also_fires_on_the_skip_path(tmp_path):
    # A stale/collapsed table left on disk must not sail through the exists-skip.
    from gfv2_params.depstor_builders import segment_wbody as builder

    ctx = _builder_ctx(
        tmp_path,
        seg_geoms=[LineString([(-10, 285), (70, 285)])],
        wb_comids=[10],
        wb_geoms=[box(0, 270, 60, 300)],
    )
    builder.build(_STEP_CFG, ctx, logging.getLogger("test"))  # writes 1 COMID
    ctx.min_onstream_comids = 500
    with pytest.raises(ValueError, match="min_onstream_comids"):
        builder.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_builder_registers_in_the_dag():
    from gfv2_params.depstor_builders import BUILDERS, STEP_ORDER

    assert "segment_wbody" in BUILDERS
    assert STEP_ORDER.index("endorheic") < STEP_ORDER.index("segment_wbody")
    assert STEP_ORDER.index("segment_wbody") < STEP_ORDER.index("wbody_connectivity")


def test_step_is_configured_and_mapped():
    # `_hydrate_existing_outputs` calls `_expected_outputs` for every step NOT in the
    # run list, so an unmapped step raises a bare KeyError on any --step/--from run.
    import yaml

    from scripts.build_depstor_rasters import _expected_outputs

    cfg = yaml.safe_load(
        (Path(__file__).resolve().parent.parent
         / "configs" / "depstor" / "depstor_rasters.yml").read_text()
    )
    step = next(s for s in cfg["steps"] if s["name"] == "segment_wbody")
    assert _expected_outputs(step) == {
        "segment_wbody_comids": "segment_waterbody_comids.parquet"
    }
```

Add `from pathlib import Path` to the test file's imports if not already present.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_segment_wbody.py -q -k "builder or dag or configured"
```

(The `-k` expression must be quoted — unquoted, the shell splits `or` into separate
arguments and pytest treats them as paths.)

Expected: failures — no `depstor_builders.segment_wbody` module, `min_onstream_comids` is not a `BuildContext` field, `segment_wbody` not in `BUILDERS`/`STEP_ORDER`/the YAML.

- [ ] **Step 3: Add the `min_onstream_comids` context field**

In `src/gfv2_params/depstor_builders/context.py`, immediately after the `min_endorheic_comids` field (line 52):

```python
    # Optional per-fabric floor on the number of on-stream COMIDs the `segment_wbody`
    # builder must produce. Same opt-in contract as `min_endorheic_comids`: absent means
    # "this domain may legitimately have few". `gfv2` measures 48,529 and `oregon` 770,
    # so both declare one — a collapsed segment/waterbody join would otherwise turn
    # nearly every waterbody into depression storage and exit 0.
    min_onstream_comids: int | None = None
```

- [ ] **Step 4: Write the builder**

Create `src/gfv2_params/depstor_builders/segment_wbody.py`:

```python
"""Emit the segment-derived on-stream COMID table consumed by `wbody_connectivity`.

Runs after `endorheic` and before `wbody_connectivity`. Reads the fabric's own
`segments_gpkg` (the model routing network) and `waterbody_gpkg`, and writes the COMIDs
a segment intersects with positive length — the PRIMARY on-stream source.

No raster inputs, so this is cheap: measured 42 s wall / 2.0 GB peak RSS at CONUS scale
(186,709 segments x 448,124 polygons), unlike the ~384 G full-grid `waterbody`/`dprst`
steps. No windowing needed.

Deliberately FTYPE-agnostic — the Playa/Ice Mass never-on-stream guardrail lives at the
`wbody_connectivity` chokepoint so it applies to the opt-in NHD comparison sources too.
"""

from __future__ import annotations

import geopandas as gpd
import rasterio
from shapely.geometry import box

from ..segment_wbody import (
    check_onstream_floor,
    load_segment_comids,
    segment_comid_frame,
    segment_waterbody_comids,
    segment_waterbody_pairs,
    write_segment_comids,
)
from .context import BuildContext


def _assert_overlaps_template(seg: gpd.GeoDataFrame, template_path, logger) -> None:
    """Fail loud if the segment layer and the template grid are disjoint.

    A `segments_gpkg` mis-wired to another fabric would match zero waterbodies, make
    every waterbody depression storage, and exit 0. The floor catches that on a fabric
    that declares one; this catches it everywhere and names the actual cause.
    """
    with rasterio.open(template_path) as src:
        tmpl = gpd.GeoSeries([box(*src.bounds)], crs=src.crs)
    if seg.crs is not None and tmpl.crs != seg.crs:
        tmpl = tmpl.to_crs(seg.crs)
    if not box(*seg.total_bounds).intersects(tmpl.iloc[0]):
        raise ValueError(
            f"the segment layer's extent {seg.total_bounds.tolist()} does not overlap "
            f"the template grid {tmpl.total_bounds.tolist()} (in the segment CRS) — the "
            f"on-stream classifier would match nothing and every waterbody would become "
            f"depression storage. Check `segments_gpkg` points at THIS fabric's segments."
        )
    logger.info("  segment layer overlaps the template grid")


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.segments_gpkg is None:
        raise KeyError(
            "segment_wbody step needs `segments_gpkg` in the fabric profile — it is the "
            "model routing network the on-stream classifier is built on."
        )
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "segment_wbody step needs `waterbody_gpkg` and `waterbody_layer`."
        )
    output_path = ctx.resolve_output(step_cfg["output"])

    logger.info("--- segment_wbody ---")
    logger.info("  Segments  : %s (layer=%s)", ctx.segments_gpkg, ctx.segments_layer)
    logger.info("  Waterbody : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Output    : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        # Honour the floor on the skip path too: a stale or collapsed table left by an
        # aborted run would otherwise sail straight through into wbody_connectivity.
        check_onstream_floor(
            len(load_segment_comids(output_path)),
            fabric=ctx.fabric, floor=ctx.min_onstream_comids, source=output_path,
        )
        return {"segment_wbody_comids": output_path}

    if not ctx.segments_gpkg.exists():
        raise FileNotFoundError(f"segments gpkg not found: {ctx.segments_gpkg}")
    if not ctx.waterbody_gpkg.exists():
        raise FileNotFoundError(f"waterbody gpkg not found: {ctx.waterbody_gpkg}")

    seg = gpd.read_file(ctx.segments_gpkg, layer=ctx.segments_layer, use_arrow=True)
    logger.info("  %d segments", len(seg))
    _assert_overlaps_template(seg, ctx.template_path, logger)

    wb = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer, use_arrow=True)
    logger.info("  %d waterbody polygons", len(wb))

    pairs = segment_waterbody_pairs(seg, wb, logger=logger)
    comids = segment_waterbody_comids(pairs)
    frame = segment_comid_frame(pairs)
    check_onstream_floor(
        len(comids), fabric=ctx.fabric, floor=ctx.min_onstream_comids,
        source=output_path,
    )
    write_segment_comids(frame, output_path)
    logger.info(
        "  %d on-stream COMIDs from %d positive-length pairs (of %d waterbody polygons)",
        len(comids), int((pairs["overlap_m"] > 0).sum()) if len(pairs) else 0, len(wb),
    )
    return {"segment_wbody_comids": output_path}
```

- [ ] **Step 5: Register the builder in the DAG**

In `src/gfv2_params/depstor_builders/__init__.py`:

1. Add `segment_wbody` to the `from . import (...)` block, alphabetically after `same_hru_drains`.
2. Add to `BUILDERS`, after the `endorheic` entry:

```python
    "segment_wbody":     segment_wbody.build,
```

3. Add to the output-key documentation comment, after the `endorheic` entry:

```python
#   segment_wbody      -> "segment_wbody_comids"    segment_waterbody_comids.parquet
#                                                    (comid, n_segments, overlap_m) —
#                                                    the PRIMARY on-stream source
```

4. Insert into `STEP_ORDER` between `"endorheic"` and `"wbody_connectivity"`:

```python
    "endorheic",
    "segment_wbody",
    "wbody_connectivity",
```

- [ ] **Step 6: Plumb the config through the orchestrator**

In `scripts/build_depstor_rasters.py`, in `_build_context`, immediately after the `min_endorheic_comids` argument (lines 104-107):

```python
        min_onstream_comids=(
            int(config["min_onstream_comids"])
            if config.get("min_onstream_comids") is not None else None
        ),
```

In `_expected_outputs`, add to the `single_key` dict after `"endorheic": "endorheic_comids",`:

```python
            "segment_wbody": "segment_wbody_comids",
```

- [ ] **Step 7: Add the step to the depstor config**

In `configs/depstor/depstor_rasters.yml`, between the `endorheic` and `wbody_connectivity` steps:

```yaml
  # The PRIMARY on-stream source: COMIDs a model nsegment intersects with positive
  # length. Replaces NHD flowline topology (WBAREACOMI + flow-through), which is now
  # an opt-in comparison mode — see the commented-out `connected_comids_table` /
  # `flowthrough_comids_table` keys in base_config.yml. Cheap (vector sjoin; 42 s /
  # 2.0 GB at CONUS), so it is not a memory consideration for the depstor batch.
  - name: segment_wbody
    output: segment_waterbody_comids.parquet
```

- [ ] **Step 8: Run the tests to verify they pass**

```bash
srun -p cpu -A impd --mem=8G --time=00:20:00 \
  pixi run -e dev pytest tests/test_segment_wbody.py tests/test_expected_outputs.py -q
```

Expected: all pass. `tests/test_expected_outputs.py` is generic and now covers the new step automatically.

- [ ] **Step 9: Lint and commit**

```bash
pixi run -e dev pre-commit run --files \
  src/gfv2_params/depstor_builders/segment_wbody.py \
  src/gfv2_params/depstor_builders/context.py \
  src/gfv2_params/depstor_builders/__init__.py \
  scripts/build_depstor_rasters.py \
  configs/depstor/depstor_rasters.yml tests/test_segment_wbody.py
git add src/gfv2_params/depstor_builders/segment_wbody.py \
        src/gfv2_params/depstor_builders/context.py \
        src/gfv2_params/depstor_builders/__init__.py \
        scripts/build_depstor_rasters.py \
        configs/depstor/depstor_rasters.yml tests/test_segment_wbody.py
git commit -m "feat(depstor): segment_wbody builder + DAG registration

Runs between endorheic and wbody_connectivity, writing
segment_waterbody_comids.parquet. Adds min_onstream_comids to BuildContext,
enforced in the builder AND on its exists-skip path so a stale table cannot
sail through. Extent guard fails loud when segments_gpkg is mis-wired to
another fabric, which would otherwise make every waterbody dprst and exit 0."
```

---

## Task 3: Switch `wbody_connectivity` to the segment source

**Files:**
- Modify: `src/gfv2_params/depstor_builders/wbody_connectivity.py`
- Modify: `tests/test_wbody_connectivity.py`

**Interfaces:**
- Consumes: `segment_wbody.load_segment_comids`, `segment_wbody.check_onstream_floor`, `ctx.min_onstream_comids`, `ctx.paths["segment_wbody_comids"]`.
- Produces: unchanged public contract — `build(step_cfg, ctx, logger) -> {"connected_wbody": Path, "endorheic_wbody": Path}`.

**Note on scope:** this task changes the meaning of `test_wbody_connectivity_requires_table` (a `None` `connected_comids_table` is now legal) and requires a segment table in **16 existing tests plus the `_endorheic_ctx` helper**. That churn is mechanical but unavoidable — do not skip it by making the segment table optional.

- [ ] **Step 1: Add the failing tests for the new contract**

Append to `tests/test_wbody_connectivity.py`:

```python
# ---------------------------------------------------------------------------
# Segment-driven on-stream source (the PRIMARY source; NHD tables are opt-in)
# ---------------------------------------------------------------------------


def _write_segment_table(tmp_path: Path, comids, name="segment_waterbody_comids.parquet") -> Path:
    """A segment_wbody output table wired into ctx.paths as the primary on-stream source."""
    path = tmp_path / name
    pd.DataFrame({
        "comid": pd.array(list(comids), dtype="int64"),
        "n_segments": pd.array([1] * len(list(comids)), dtype="int64"),
        "overlap_m": pd.array([100.0] * len(list(comids)), dtype="float64"),
    }).to_parquet(path, index=False)
    return path


def _segment_ctx(tmp_path, *, segment_comids, comids=(10, 20), **kw):
    """Two waterbodies, on-stream purely via the segment table (no NHD tables)."""
    from shapely.geometry import box

    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)
    gpd.GeoDataFrame(
        {"COMID": list(comids), "member_comid": [str(c) for c in comids],
         "FTYPE": ["LakePond"] * len(comids)},
        geometry=[box(0, 270, 60, 300), box(240, 0, 300, 30)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=None, flowthrough_comids_table=None, **kw,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)
    ctx.paths["segment_wbody_comids"] = _write_segment_table(tmp_path, segment_comids)
    return ctx


def test_segment_table_alone_drives_the_onstream_mask(tmp_path):
    """Segments-only is the DEFAULT path: no NHD table configured at all."""
    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _segment_ctx(tmp_path, segment_comids=[10])
    produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))
    with rasterio.open(produced["connected_wbody"]) as src:
        arr = src.read(1)
    assert arr[0, 0] == 1     # COMID 10: a segment runs through it
    assert arr[9, 9] != 1     # COMID 20: no segment -> depression storage


def test_missing_segment_table_raises(tmp_path):
    """The segment table is REQUIRED — without it every waterbody becomes dprst."""
    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _segment_ctx(tmp_path, segment_comids=[10])
    del ctx.paths["segment_wbody_comids"]
    with pytest.raises(KeyError, match="segment_wbody"):
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_onstream_floor_is_enforced_at_the_consuming_end(tmp_path):
    """`--from wbody_connectivity` skips the producer, so the floor must fire here too."""
    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _segment_ctx(tmp_path, segment_comids=[10], min_onstream_comids=500)
    with pytest.raises(ValueError, match="min_onstream_comids"):
        wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))


def test_nhd_tables_union_in_and_warn_when_configured(tmp_path, caplog):
    """The NHD tables are opt-in COMPARISON inputs, and saying so must be loud."""
    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _segment_ctx(tmp_path, segment_comids=[10])
    conn = tmp_path / "connected.parquet"
    pd.DataFrame({"comid": pd.array([20], dtype="int64")}).to_parquet(conn, index=False)
    ctx.connected_comids_table = conn

    with caplog.at_level(logging.WARNING):
        produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))
    with rasterio.open(produced["connected_wbody"]) as src:
        arr = src.read(1)
    assert arr[0, 0] == 1   # COMID 10 from the segment table
    assert arr[9, 9] == 1   # COMID 20 unioned in from WBAREACOMI
    assert "COMPARISON MODE" in caplog.text


def test_endorheic_still_subtracts_from_the_segment_set(tmp_path):
    """The endorheic guard is unchanged and still demotes a segment-promoted lake.

    This is the Great Salt Lake path under the new source: a model segment runs
    through it, so the segment classifier promotes it, and the endorheic
    subtraction is the only thing that takes it back out. It matters MORE here
    than under NHD, because a segment TERMINATING inside a waterbody now
    promotes it -- the inflow-AND-outflow discrimination is gone.
    """
    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _segment_ctx(tmp_path, segment_comids=[10, 20])
    endo = tmp_path / "endorheic_gsl.parquet"
    pd.DataFrame(
        {"comid": pd.array([20], dtype="int64"), "frac_own": [1.0],
         "by_terminus": [True], "by_closed_huc12": [False]}
    ).to_parquet(endo, index=False)
    ctx.paths["endorheic_comids"] = endo

    produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))
    with rasterio.open(produced["connected_wbody"]) as src:
        connected = src.read(1)
    with rasterio.open(produced["endorheic_wbody"]) as src:
        endorheic = src.read(1)
    assert connected[0, 0] == 1              # COMID 10 stays on-stream
    assert connected[9, 9] != 1              # COMID 20 demoted by endorheic
    assert (endorheic[9, 8:10] == 1).any()   # ...but still in endorheic_wbody.tif


def test_playa_promoted_by_a_segment_is_still_dropped(tmp_path):
    """The FTYPE guardrail is unchanged and still applies to the segment source."""
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)
    gpd.GeoDataFrame(
        {"COMID": [10, 20], "member_comid": ["10", "20"],
         "FTYPE": ["LakePond", "Playa"]},
        geometry=[box(0, 270, 60, 300), box(240, 0, 300, 30)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=None,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = _write_empty_endorheic(tmp_path)
    ctx.paths["segment_wbody_comids"] = _write_segment_table(tmp_path, [10, 20])

    produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))
    with rasterio.open(produced["connected_wbody"]) as src:
        arr = src.read(1)
    assert arr[0, 0] == 1   # LakePond on-stream
    assert arr[9, 9] != 1   # Playa force-dprst despite a segment through it
```

Add `import pytest` at module level (the file currently imports it inside functions) and `from pathlib import Path` is already present.

- [ ] **Step 2: Run to verify the new tests fail**

```bash
srun -p cpu -A impd --mem=8G --time=00:20:00 \
  pixi run -e dev pytest tests/test_wbody_connectivity.py -q
```

Expected: the 6 new tests fail (the builder ignores `segment_wbody_comids` and still requires `connected_comids_table`).

- [ ] **Step 3: Rewrite the on-stream set assembly in the builder**

In `src/gfv2_params/depstor_builders/wbody_connectivity.py`:

1. Add the import after the `..endorheic` import block:

```python
from ..segment_wbody import check_onstream_floor, load_segment_comids
```

2. Replace the `connected_comids_table` requirement (lines 102-107) with:

```python
    if "segment_wbody_comids" not in ctx.paths:
        raise KeyError(
            "wbody_connectivity step needs `segment_wbody_comids` in the build context, "
            "but the `segment_wbody` step has not run and produced no output on disk for "
            "this fabric. That table is the PRIMARY on-stream source; without it every "
            "waterbody in the domain would be classified as depression storage. Run the "
            "`segment_wbody` step first (e.g. `--from segment_wbody`), or run the full "
            "DAG so it runs in order."
        )
```

3. **Delete** the whole block from `if not ctx.connected_comids_table.exists():` through
`connected = connected | flowthrough` (currently lines 125-151 — the unconditional
existence check, `info = RasterInfo.from_path(...)`, `connected =
load_connected_comids(...)`, `n_wbareacomi`, and the `flowthrough_comids_table` branch).
**Replace** it with:

```python
    info = RasterInfo.from_path(ctx.template_path)

    # PRIMARY source: COMIDs a model nsegment intersects with positive length. The
    # floor is applied HERE as well as in the producing builder — `--from
    # wbody_connectivity` (the documented cascade-rebuild recipe) leaves `segment_wbody`
    # out of the run list and `_hydrate_existing_outputs` pulls its table off disk with
    # no validation at all.
    segment_table = ctx.require("segment_wbody_comids")
    connected = load_segment_comids(segment_table)
    n_segment = len(connected)
    check_onstream_floor(
        n_segment, fabric=ctx.fabric, floor=ctx.min_onstream_comids,
        source=segment_table,
    )

    # OPT-IN COMPARISON SOURCES. Both NHD tables are absent from every fabric profile by
    # default (commented out in base_config.yml). Present means an operator deliberately
    # asked to union NHD's answer back in for an A/B, which is NOT the production
    # definition of on-stream — so say so loudly rather than let a stale table quietly
    # become a second definition.
    n_wbareacomi = 0
    n_flowthrough = 0
    if ctx.connected_comids_table is not None or ctx.flowthrough_comids_table is not None:
        logger.warning(
            "  COMPARISON MODE: an NHD COMID table is configured (connected=%s, "
            "flowthrough=%s), so NHD flowline topology is being UNIONED into the "
            "segment-derived on-stream set. This is NOT the production classifier — "
            "comment those keys out of the fabric profile for a production run.",
            ctx.connected_comids_table, ctx.flowthrough_comids_table,
        )
    if ctx.connected_comids_table is not None:
        if not ctx.connected_comids_table.exists():
            raise FileNotFoundError(
                f"Connected-COMID table not found: {ctx.connected_comids_table}. Run "
                f"`python -m gfv2_params.download.nhd_flowlines` first, or remove "
                f"`connected_comids_table` from the profile."
            )
        wbareacomi = load_connected_comids(ctx.connected_comids_table)
        n_wbareacomi = len(wbareacomi - connected)
        connected = connected | wbareacomi
    if ctx.flowthrough_comids_table is not None:
        if not ctx.flowthrough_comids_table.exists():
            raise FileNotFoundError(
                f"Flow-through COMID table not found: "
                f"{ctx.flowthrough_comids_table}. Run "
                f"`python -m gfv2_params.download.nhd_flowthrough` first, or "
                f"remove `flowthrough_comids_table` from the profile."
            )
        flowthrough = load_connected_comids(ctx.flowthrough_comids_table)
        if not flowthrough:
            raise ValueError(
                "configured flow-through table is empty → it would promote no "
                "waterbodies and silently degrade to a no-op; re-run "
                "nhd_flowthrough or remove the key"
            )
        n_flowthrough = len(flowthrough - connected)
        connected = connected | flowthrough
```

4. Update the log line at the end of the assembly (lines 215-219) to:

```python
    logger.info(
        "  on-stream COMIDs: %d segment + %d new WBAREACOMI + %d new flow-through "
        "- %d endorheic = %d total",
        n_segment, n_wbareacomi, n_flowthrough, n_endorheic, len(connected),
    )
```

5. Update the logging block near the top (lines 113-119) to log the segment table instead of implying the connected table is primary:

```python
    logger.info("--- wbody_connectivity ---")
    logger.info("  Waterbody gpkg : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Primary source : segment_wbody_comids (model nsegment intersection)")
    if ctx.connected_comids_table is not None:
        logger.info("  [comparison] WBAREACOMI table: %s", ctx.connected_comids_table)
    if ctx.flowthrough_comids_table is not None:
        logger.info("  [comparison] flow-through table: %s", ctx.flowthrough_comids_table)
    logger.info("  Output (connected): %s", connected_path)
    logger.info("  Output (endorheic): %s", endorheic_path)
```

6. Update the `len(sel) == 0` raise message (lines 266-272) so it names the segment source:

```python
        raise ValueError(
            f"wbody_connectivity matched 0 of {len(wb_gdf)} waterbodies against "
            f"{len(connected)} on-stream COMIDs — this would misclassify every "
            f"waterbody as depression storage. Check that {segment_table} is complete "
            f"(the `segment_wbody` step) and that its COMID/member_comid join keys "
            f"align with the waterbody layer."
        )
```

7. Rewrite the module docstring's first paragraph:

```python
"""Rasterise the on-stream waterbody polygons to a uint8 binary mask.

On-stream status comes from the MODEL's own routing network: the COMIDs a fabric
`nsegment` intersects with positive length, staged by the `segment_wbody` step. NHD
flowline topology (WBAREACOMI via `nhd_flowlines`, geometric flow-through via
`nhd_flowthrough`) is retained as an OPT-IN comparison union — absent from every fabric
profile by default, and logged as a WARNING when present, because it is not the
production definition of on-stream.
```

- [ ] **Step 4: Migrate the 16 existing builder tests**

Every existing test in `tests/test_wbody_connectivity.py` that calls `wbody_connectivity.build` needs a segment table. Two mechanical edits plus one semantic replacement:

**(a)** In `_endorheic_ctx` (line ~918), after `ctx.paths["endorheic_comids"] = endo`:

```python
    ctx.paths["segment_wbody_comids"] = _write_segment_table(tmp_path, [10, 20])
```

**(b)** In each of these 15 tests, add one line immediately after the existing
`ctx.paths["endorheic_comids"] = ...` line (or after `ctx.paths["landmask"] = landmask`
where there is no endorheic assignment), with the COMIDs that test expects on-stream —
matching whatever its `connected.parquet` contains, so behaviour is unchanged:

| test | line to add |
| --- | --- |
| `test_wbody_connectivity_rasterizes_only_connected` | `ctx.paths["segment_wbody_comids"] = _write_segment_table(tmp_path, [10])` |
| `test_wbody_connectivity_zero_match_raises` | `... _write_segment_table(tmp_path, [999])` |
| `test_wbody_connectivity_drops_non_land_cells` | `... _write_segment_table(tmp_path, [10])` |
| `test_wbody_connectivity_flowthrough_only_waterbody_burned` | `... _write_segment_table(tmp_path, [10])` |
| `test_wbody_connectivity_flowthrough_missing_raises` | `... _write_segment_table(tmp_path, [10])` |
| `test_wbody_connectivity_flowthrough_empty_raises` | `... _write_segment_table(tmp_path, [10])` |
| `test_wbody_connectivity_force_dprst_ftypes_excluded` | `... _write_segment_table(tmp_path, [10, 20, 30])` |
| `test_wbody_connectivity_missing_ftype_column_raises` | `... _write_segment_table(tmp_path, [10, 20, 30])` |
| `test_wbody_connectivity_flowthrough_none_is_silent_noop` | `... _write_segment_table(tmp_path, [10])` |
| `test_endorheic_comid_is_demoted_from_the_connected_raster` | `... _write_segment_table(tmp_path, [1, 2])` |
| `test_endorheic_comids_missing_from_context_raises` | `... _write_segment_table(tmp_path, [10])` |
| `test_endorheic_comids_empty_table_is_a_legitimate_noop` | `... _write_segment_table(tmp_path, [10])` |
| `test_endorheic_subtraction_never_widens_the_onstream_set` | `... _write_segment_table(tmp_path, [1])` |
| `test_wbody_connectivity_writes_endorheic_wbody_raster` | `... _write_segment_table(tmp_path, [1, 2])` |
| `test_endorheic_wbody_raster_is_land_masked` | `... _write_segment_table(tmp_path, [10])` |

For `test_wbody_connectivity_flowthrough_missing_raises` and
`test_wbody_connectivity_flowthrough_empty_raises`, the ctx has no `landmask`/`endorheic`
wiring in some cases — add the segment table line after the `BuildContext(...)`
construction in each.

**(c)** Replace `test_wbody_connectivity_requires_table` entirely. A `None`
`connected_comids_table` is now legal, so the old assertion is wrong:

```python
def test_wbody_connectivity_no_nhd_table_is_the_default(tmp_path):
    """`connected_comids_table = None` is now the DEFAULT, not an error.

    Replaces the former test_wbody_connectivity_requires_table: the WBAREACOMI table
    is an opt-in comparison input, and the required primary source is the segment
    table (see test_missing_segment_table_raises).
    """
    from gfv2_params.depstor_builders import wbody_connectivity

    ctx = _segment_ctx(tmp_path, segment_comids=[10])
    assert ctx.connected_comids_table is None
    produced = wbody_connectivity.build(_STEP_CFG, ctx, logging.getLogger("test"))
    assert "connected_wbody" in produced
```

- [ ] **Step 5: Run the full connectivity + dprst suites**

```bash
srun -p cpu -A impd --mem=16G --time=00:30:00 \
  pixi run -e dev pytest tests/test_wbody_connectivity.py tests/test_segment_wbody.py \
    tests/test_build_depstor_dprst.py tests/test_endorheic.py tests/test_expected_outputs.py -q
```

Expected: all pass.

- [ ] **Step 6: Lint and commit**

```bash
pixi run -e dev pre-commit run --files \
  src/gfv2_params/depstor_builders/wbody_connectivity.py tests/test_wbody_connectivity.py
git add src/gfv2_params/depstor_builders/wbody_connectivity.py tests/test_wbody_connectivity.py
git commit -m "feat(depstor)!: segment table is the primary on-stream source

wbody_connectivity now REQUIRES segment_wbody_comids and treats the two NHD
COMID tables as opt-in comparison inputs, logging a COMPARISON MODE warning
when either is configured so a stale table cannot quietly become a second
definition of on-stream. min_onstream_comids is enforced here as well as in
the producer, because --from wbody_connectivity hydrates the table off disk
unvalidated.

The endorheic subtraction and the Playa/Ice Mass guardrail are unchanged, and
matter MORE under this source: a segment terminating inside a waterbody now
promotes it, so NHD's inflow-AND-outflow discrimination is gone."
```

---

## Task 4: Fabric profiles and config invariants

**Files:**
- Modify: `configs/base_config.yml`
- Create: `tests/test_segment_onstream_config.py`

**Interfaces:**
- Consumes: `min_onstream_comids` from Task 2's context plumbing.
- Produces: no code interface. Profile state: no fabric has an active NHD COMID table; `gfv2`/`gfv2_dev` declare `min_onstream_comids: 30000`; `oregon` declares `500`; `oregon`/`gfv2_dev`/`tjc` read `nhd_waterbodies.gpkg`.

- [ ] **Step 1: Write the failing invariant test**

Create `tests/test_segment_onstream_config.py`:

```python
"""Invariants on the REAL configs/base_config.yml for the segment-driven classifier.

Follows the precedent of tests/test_expected_outputs.py (which asserts against the real
depstor config): these are cheap, data-root-free checks that catch a profile drifting
back to the retired waterbody layer or silently re-enabling the NHD comparison union in
production.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
BASE_CONFIG = REPO_ROOT / "configs" / "base_config.yml"

# gfv2_vpu01 reads a fabric-local `wbs` layer, not a CONUS waterbody layer, and its
# depstor DAG deliberately fail-fasts (no COMID column). It is out of scope here.
_CONUS_WATERBODY_FABRICS = ("gfv2", "gfv2_dev", "oregon", "tjc")


def _fabrics() -> dict:
    return yaml.safe_load(BASE_CONFIG.read_text())["fabrics"]


def test_no_fabric_reads_the_retired_conus_waterbodies_layer():
    for name, profile in _fabrics().items():
        assert "conus_waterbodies.gpkg" not in str(profile.get("waterbody_gpkg", "")), (
            f"fabric '{name}' still reads the retired conus_waterbodies.gpkg. Its "
            f"shoreline vintage is poorly registered against NHM segment geometry "
            f"(19.0% zero-length grazes vs 3.1% on nhd_waterbodies)."
        )


def test_conus_waterbody_fabrics_all_read_nhd_waterbodies():
    fabrics = _fabrics()
    for name in _CONUS_WATERBODY_FABRICS:
        assert fabrics[name]["waterbody_gpkg"].endswith("input/nhd/nhd_waterbodies.gpkg"), (
            f"fabric '{name}' must read the current nhd_waterbodies.gpkg so every "
            f"fabric classifies against one shoreline vintage."
        )


def test_no_fabric_enables_the_nhd_comparison_union_by_default():
    # Presence of either key unions NHD flowline topology back into the on-stream set.
    # That is a deliberate A/B mode, never a production profile.
    for name, profile in _fabrics().items():
        for key in ("connected_comids_table", "flowthrough_comids_table"):
            assert key not in profile, (
                f"fabric '{name}' has an ACTIVE `{key}`, which unions NHD flowline "
                f"topology into the segment-derived on-stream set. Comment it out — it "
                f"is an opt-in comparison mode, not the production classifier."
            )


def test_gfv2_and_oregon_declare_an_onstream_floor():
    fabrics = _fabrics()
    assert fabrics["gfv2"]["min_onstream_comids"] == 30000      # measured 48,529
    assert fabrics["gfv2_dev"]["min_onstream_comids"] == 30000
    assert fabrics["oregon"]["min_onstream_comids"] == 500      # measured 770


def test_every_depstor_fabric_declares_segments():
    # segments_gpkg is now a REQUIRED depstor input, not a legacy leftover.
    fabrics = _fabrics()
    for name in _CONUS_WATERBODY_FABRICS:
        assert fabrics[name].get("segments_gpkg"), (
            f"fabric '{name}' has no segments_gpkg — the on-stream classifier "
            f"cannot run without the model routing network."
        )
```

- [ ] **Step 2: Run to verify it fails**

```bash
srun -p cpu -A impd --mem=4G --time=00:10:00 \
  pixi run -e dev pytest tests/test_segment_onstream_config.py -q
```

Expected: 4 of 5 fail (`test_every_depstor_fabric_declares_segments` already passes).

- [ ] **Step 3: Edit the `gfv2` profile**

In `configs/base_config.yml`, replace lines 51-58 (the `connected_comids_table` comment
block through `flowthrough_comids_table`) with:

```yaml
    # --- OPT-IN COMPARISON ONLY — not the production on-stream source ---------
    # On-stream status now comes from the MODEL's own nsegment network (the
    # `segment_wbody` depstor step; see
    # docs/superpowers/specs/2026-07-24-segment-driven-onstream-classifier-design.md).
    # Uncommenting either key UNIONS NHD flowline topology back into the on-stream set
    # for an A/B; wbody_connectivity logs a COMPARISON MODE warning when it sees one.
    # Never leave them enabled for a production run.
    #   connected_comids_table: "{data_root}/input/nhd/connected_waterbody_comids.parquet"
    #   flowthrough_comids_table: "{data_root}/input/nhd/flowthrough_waterbody_comids.parquet"
    # Floor on the on-stream COMIDs the `segment_wbody` builder must produce. CONUS
    # measures 48,529 (73,343 candidate pairs, 3.1% zero-length grazes dropped); a
    # result below this floor means the segment/waterbody join collapsed and nearly
    # every waterbody would become depression storage. Same opt-in contract as
    # min_endorheic_comids — omit on a fabric that legitimately has few.
    min_onstream_comids: 30000
```

Also rewrite the `segments_gpkg` comment at lines 33-39, replacing the now-false
"segments no longer feed any depstor step" sentence:

```yaml
    # Stream segments: one CONUS nsegment layer assembled by
    # scripts/merge_vpu_segments.py from the input/fabric/NHM_<vpu>_draft.gpkg
    # drafts (the fabric-merge notebook merges only nhru, so segments are merged
    # separately). This layer is the ON-STREAM CLASSIFIER's primary input: the
    # `segment_wbody` depstor step promotes a waterbody to on-stream iff an nsegment
    # here intersects it with positive length. 186,709 segments / 1,668,778 km.
    # Run: pixi run --as-is python scripts/merge_vpu_segments.py --fabric gfv2
```

- [ ] **Step 4: Edit the `gfv2_dev` profile**

Change line 129 to the current layer:

```yaml
    waterbody_gpkg: "{data_root}/input/nhd/nhd_waterbodies.gpkg"
```

Replace lines 131-136 (`connected_comids_table` through `flowthrough_comids_table`) with:

```yaml
    # OPT-IN COMPARISON ONLY — see the gfv2 profile above. Commented out so gfv2_dev
    # mirrors gfv2's production classifier, which is the whole point of this fabric.
    #   connected_comids_table: "{data_root}/input/nhd/connected_waterbody_comids.parquet"
    #   flowthrough_comids_table: "{data_root}/input/nhd/flowthrough_waterbody_comids.parquet"
    min_onstream_comids: 30000
```

- [ ] **Step 5: Edit the `oregon` profile**

Change the `waterbody_gpkg` line (~226) and its comment to:

```yaml
    # CONUS NHDPlusV2 waterbodies (POLYGON, EPSG:5070) — the SAME layer gfv2 reads, so
    # both fabrics classify against one shoreline vintage. The retired
    # conus_waterbodies.gpkg produced 19.0% zero-length segment grazes here against
    # 3.1% on this layer. The waterbody builder reprojects to the template grid and
    # clips to the Oregon land mask.
    waterbody_gpkg: "{data_root}/input/nhd/nhd_waterbodies.gpkg"
    waterbody_layer: waterbodies
```

Replace the `connected_comids_table` line (~228-230) and its comment with:

```yaml
    # OPT-IN COMPARISON ONLY — see the gfv2 profile.
    #   connected_comids_table: "{data_root}/input/nhd/connected_waterbody_comids.parquet"
    # 770 on-stream COMIDs measured on this fabric (1,453 candidate pairs, 45 zero-length
    # grazes dropped); in-domain that is 768 against 1,550 under the old NHD union.
    min_onstream_comids: 500
```

- [ ] **Step 6: Edit the `tjc` profile**

Change the `waterbody_gpkg` line (~271) and its comment:

```yaml
    # Shared CONUS NHDPlusV2 waterbodies — the same layer gfv2/oregon read; the
    # waterbody builder reprojects to the template grid and clips to the tjc land mask.
    waterbody_gpkg: "{data_root}/input/nhd/nhd_waterbodies.gpkg"
    waterbody_layer: waterbodies
```

Replace the `connected_comids_table` line (~273) and its comment with:

```yaml
    # OPT-IN COMPARISON ONLY — see the gfv2 profile. No `min_onstream_comids`: tjc is a
    # small domain and omits `min_endorheic_comids` for the same reason.
    #   connected_comids_table: "{data_root}/input/nhd/connected_waterbody_comids.parquet"
```

- [ ] **Step 7: Update the `gfv2_vpu01` note**

The profile is otherwise unchanged. Extend its existing "No `connected_comids_table`"
comment (line ~176) to name the new step:

```yaml
    # No `connected_comids_table`: the `wbs` layer has no COMID/member_comid
    # column, so neither the segment on-stream classifier, the WBAREACOMI join,
    # nor the COMID-keyed endorheic classifier can run. The depstor DAG therefore
    # fail-fasts on this profile — at the `endorheic` step first, and at
    # `segment_wbody` next, both of which raise on a waterbody layer with no
    # COMID column. Use the `gfv2` profile for depstor validation.
```

- [ ] **Step 8: Verify config resolution and the invariants**

```bash
pixi run --as-is python -c "
from pathlib import Path
from gfv2_params.config import load_config
for f in ('gfv2','gfv2_dev','oregon','tjc'):
    c = load_config(Path('configs/depstor/depstor_rasters.yml'), fabric=f)
    print(f, '| wb:', Path(c['waterbody_gpkg']).name,
          '| floor:', c.get('min_onstream_comids'),
          '| nhd keys:', [k for k in ('connected_comids_table','flowthrough_comids_table') if c.get(k)])
"
srun -p cpu -A impd --mem=4G --time=00:10:00 \
  pixi run -e dev pytest tests/test_segment_onstream_config.py tests/test_config.py -q
```

Expected: each fabric prints `nhd_waterbodies.gpkg`, its floor (`None` for tjc), and an
empty NHD-key list; all tests pass.

- [ ] **Step 9: Lint and commit**

```bash
pixi run -e dev pre-commit run --files configs/base_config.yml tests/test_segment_onstream_config.py
git add configs/base_config.yml tests/test_segment_onstream_config.py
git commit -m "feat(config): segments-only on-stream + one waterbody layer everywhere

Comments the two NHD COMID tables out of every fabric profile (opt-in
comparison mode only) and adds min_onstream_comids: gfv2/gfv2_dev 30000
(measured 48,529), oregon 500 (measured 770).

Moves oregon, gfv2_dev and tjc from the retired conus_waterbodies.gpkg to the
nhd_waterbodies.gpkg gfv2 already read. gfv2_dev reading a different layer
from gfv2 was a defect in its own right: it exists to mirror gfv2 for
unproven rebuilds, so a CONUS shakedown there would not have represented
gfv2. Also corrects the gfv2 segments_gpkg comment, which claimed segments no
longer feed any depstor step."
```

---

## Task 5: Real-geometry Oregon fixture

**Files:**
- Create: `tests/data/OR_waterbody_nseg_intersection.csv`, `tests/data/segment_wbody_oregon_fixture.gpkg`, `tests/data/README.md`
- Test: `tests/test_segment_wbody_fixture.py`

**Interfaces:**
- Consumes: `segment_waterbody_pairs`, `segment_waterbody_comids`, `segment_comid_frame` from Task 1.
- Produces: no code interface.

The synthetic tests pin the rule; this pins it against real NHD and NHM geometry,
including a genuine invalid polygon and genuine shoreline grazes. The fixture must be
cut on the HPC (where the data lives) and committed.

- [ ] **Step 1: Cut the fixture (run once, on a compute node)**

```bash
srun -p cpu -A impd --mem=16G --time=00:30:00 pixi run --as-is python - <<'PY'
from pathlib import Path
import geopandas as gpd, pyogrio, pandas as pd
root = Path('/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2')
out = Path('tests/data'); out.mkdir(parents=True, exist_ok=True)

# 11 real oregon COMIDs: 6 graze-only (must be DROPPED by positive length), 3 clean
# through-flow, 1 Playa and 1 Ice Mass that the geometry rule KEEPS (the FTYPE guard
# in wbody_connectivity is what drops them, not this builder).
COMIDS = [24083423, 24032328, 24051153, 24067917, 24079145, 24079105,
          23794331, 120052284, 120055365, 24032198, 120050246]

seg = gpd.read_file(root/'oregon/fabric/model_layers 9.gpkg', layer='nsegment')
wb = pyogrio.read_dataframe(
    root/'input/nhd/nhd_waterbodies.gpkg', layer='waterbodies',
    bbox=tuple(seg.total_bounds),
    columns=['COMID', 'FTYPE', 'member_comid'], use_arrow=True)
wb['COMID'] = pd.to_numeric(wb['COMID'], errors='coerce').astype('Int64')
wb_f = wb[wb.COMID.isin(COMIDS)].reset_index(drop=True)
hit = gpd.sjoin(seg, wb_f[['geometry']], how='inner', predicate='intersects')
seg_f = seg.loc[sorted(set(hit.index)), ['segment_id', 'geometry']].reset_index(drop=True)

fx = out/'segment_wbody_oregon_fixture.gpkg'
if fx.exists():
    fx.unlink()
seg_f.to_file(fx, layer='nsegment', driver='GPKG')
wb_f.to_file(fx, layer='waterbodies', driver='GPKG')
print(f'fixture: {len(seg_f)} segments, {len(wb_f)} waterbody rows, '
      f'{wb_f.COMID.nunique()} COMIDs, {fx.stat().st_size/1024:.0f} KB')

import shutil
shutil.copy(root/'input/staging/OR_waterbody_nseg_intersection.csv',
            out/'OR_waterbody_nseg_intersection.csv')

# The values the test will assert. Print them and paste into the test if they differ.
import numpy as np, shapely
j = gpd.sjoin(seg_f, wb_f[['COMID','geometry']], how='inner', predicate='intersects')
L = shapely.length(shapely.intersection(
    np.asarray(seg_f.geometry.values)[j.index.to_numpy()],
    np.asarray(wb_f.geometry.values)[j['index_right'].to_numpy()]))
print('positive-length COMIDs:', sorted(set(j.COMID[L > 0].dropna().astype(int))))
print('graze-only COMIDs    :', sorted(set(j.COMID.dropna().astype(int))
                                       - set(j.COMID[L > 0].dropna().astype(int))))
print('n_segments for 23794331:',
      j[(L > 0) & (j.COMID == 23794331)].index.nunique())
PY
```

Confirm the printed `positive-length COMIDs` is
`[23794331, 24032198, 120050246, 120052284, 120055365]` and `graze-only COMIDs` is
`[24032328, 24051153, 24067917, 24079105, 24079145, 24083423]`. If they differ, the
source layers changed vintage — update the constants in Step 2 and say so in the commit.

- [ ] **Step 2: Write the fixture test**

Create `tests/test_segment_wbody_fixture.py`:

```python
"""The positive-length rule, pinned against REAL oregon NHM + NHD geometry.

The synthetic tests in test_segment_wbody.py fix the rule's semantics; this fixes it
against the geometry it actually runs on — genuine shoreline grazes, a multi-part
waterbody, and NHD polygons that need make_valid. Cut from
`oregon/fabric/model_layers 9.gpkg` (nsegment) and `input/nhd/nhd_waterbodies.gpkg`;
see tests/data/README.md for the exact recipe.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from gfv2_params.segment_wbody import (
    segment_comid_frame,
    segment_waterbody_comids,
    segment_waterbody_pairs,
)

DATA = Path(__file__).resolve().parent / "data"
FIXTURE = DATA / "segment_wbody_oregon_fixture.gpkg"
CROSSWALK = DATA / "OR_waterbody_nseg_intersection.csv"

# Waterbodies a segment traverses with POSITIVE length -> on-stream. Includes a Playa
# (24032198) and an Ice Mass (120050246) on purpose: this builder is FTYPE-agnostic,
# and `wbody_connectivity`'s never-on-stream guardrail is what drops them.
EXPECTED_ONSTREAM = {23794331, 24032198, 120050246, 120052284, 120055365}

# Waterbodies a segment only GRAZES (zero-length intersection) -> depression storage.
EXPECTED_GRAZE_ONLY = {24032328, 24051153, 24067917, 24079105, 24079145, 24083423}


@pytest.fixture(scope="module")
def layers():
    seg = gpd.read_file(FIXTURE, layer="nsegment")
    wb = gpd.read_file(FIXTURE, layer="waterbodies")
    return seg, wb


def test_positive_length_rule_on_real_geometry(layers):
    seg, wb = layers
    pairs = segment_waterbody_pairs(seg, wb)
    assert segment_waterbody_comids(pairs) == EXPECTED_ONSTREAM


def test_graze_only_waterbodies_are_dropped(layers):
    seg, wb = layers
    pairs = segment_waterbody_pairs(seg, wb)
    intersecting = set(pairs["comid"].unique())
    # Bare `intersects` would promote all of these; positive length must not.
    assert EXPECTED_GRAZE_ONLY <= intersecting, "fixture must contain the graze pairs"
    assert not (EXPECTED_GRAZE_ONLY & segment_waterbody_comids(pairs))


def test_every_graze_pair_measures_exactly_zero(layers):
    seg, wb = layers
    pairs = segment_waterbody_pairs(seg, wb)
    grazes = pairs[pairs["comid"].isin(EXPECTED_GRAZE_ONLY)]
    assert not grazes.empty
    assert (grazes["overlap_m"] == 0).all()


def test_multi_segment_waterbody_aggregates(layers):
    seg, wb = layers
    frame = segment_comid_frame(segment_waterbody_pairs(seg, wb))
    row = frame[frame["comid"] == 23794331].iloc[0]
    assert row["n_segments"] > 1, "COMID 23794331 is crossed by many segments"
    assert row["overlap_m"] > 1000.0


def test_crosswalk_provenance_is_committed():
    # The handoff crosswalk is a versioned audit trail, NOT a reproduction target: it
    # was built with bare `intersects` against the retired conus_waterbodies.gpkg
    # (1,629 pairs / 783 COMIDs). Against nhd_waterbodies the same join gives 1,453
    # pairs; the delta is the layer swap and is documented in the design spec.
    cw = pd.read_csv(CROSSWALK)
    assert list(cw.columns) == ["wb_comid", "nseg_id"]
    assert len(cw) == 1629
    assert cw["wb_comid"].nunique() == 783
    # Every fixture COMID that the CSV also found must be present in it.
    assert EXPECTED_GRAZE_ONLY <= set(cw["wb_comid"])
```

- [ ] **Step 3: Write the fixture provenance note**

Create `tests/data/README.md`:

```markdown
# Test fixtures

## `segment_wbody_oregon_fixture.gpkg`

Real oregon geometry for `tests/test_segment_wbody_fixture.py`, which pins the
positive-length on-stream rule against the layers it actually runs on.

Layers: `nsegment` (every segment intersecting one of the COMIDs below) and
`waterbodies` (`COMID`, `FTYPE`, `member_comid`).

Cut from, on the HPC:
- segments: `{data_root}/oregon/fabric/model_layers 9.gpkg`, layer `nsegment`
- waterbodies: `{data_root}/input/nhd/nhd_waterbodies.gpkg`, layer `waterbodies`

COMIDs, chosen to cover every branch of the rule:

| COMID | role |
| --- | --- |
| 24083423, 24032328, 24051153, 24067917 | LakePond, zero-length shoreline graze — dropped |
| 24079145, 24079105 | Playa, zero-length graze — dropped |
| 23794331 | crossed by many segments; largest overlap in the fabric |
| 120052284, 120055365 | clean through-flow |
| 24032198 | Playa KEPT by geometry (the FTYPE guard in `wbody_connectivity` drops it) |
| 120050246 | Ice Mass KEPT by geometry (same) |

Regenerate with the snippet in Task 1/Step 1 of
`docs/superpowers/plans/2026-07-24-segment-driven-onstream-classifier.md`.

## `OR_waterbody_nseg_intersection.csv`

The handoff crosswalk that motivated the segment-driven classifier: 1,629 rows, 783
distinct `wb_comid`, 1,353 distinct `nseg_id` (= the oregon `nsegment` layer's
`segment_id`, **not** `nhm_seg_id`).

Reproduced exactly by a bare
`gpd.sjoin(nsegment, waterbodies, predicate="intersects")` against the now-retired
`input/nhd/conus_waterbodies.gpkg`.

It is **provenance, not a reproduction target**: production reads
`nhd_waterbodies.gpkg`, where the same join gives 1,453 pairs (1,451 shared, 2 new, 178
CSV-only) and 776 COMIDs (7 CSV COMIDs absent). See
`docs/superpowers/specs/2026-07-24-segment-driven-onstream-classifier-design.md`.
```

- [ ] **Step 4: Run the fixture test**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_segment_wbody_fixture.py -q
```

Expected: 5 pass.

- [ ] **Step 5: Check the fixture size, then commit**

```bash
du -h tests/data/*
git add tests/data/
git commit -m "test(depstor): real-geometry oregon fixture for the on-stream rule

Pins the positive-length rule against genuine NHM segment and NHD waterbody
geometry: 6 zero-length shoreline grazes that must be dropped, 3 clean
through-flow lakes, and a Playa + Ice Mass that the geometry rule KEEPS
because the FTYPE guard belongs to wbody_connectivity, not this builder.

Commits the handoff crosswalk as versioned provenance. It is not a
reproduction target: it was built with bare intersects against the retired
conus_waterbodies.gpkg, and the delta against nhd_waterbodies is the layer
swap, documented in the spec and tests/data/README.md."
```

If the gpkg exceeds ~1 MB, cut COMID 23794331 (the largest polygon) from the fixture,
replace `test_multi_segment_waterbody_aggregates` with COMID 120055365, and update
`EXPECTED_ONSTREAM` and `tests/data/README.md` accordingly.

---

## Task 6: A/B diagnostic

**Files:**
- Create: `scripts/diagnose/ab_segment_vs_nhd_onstream.py`
- Delete: `scripts/diagnose/measure_segment_onstream.py` (untracked — plain `rm`)
- Test: `tests/test_ab_segment_vs_nhd_onstream.py`

**Interfaces:**
- Consumes: `load_segment_comids` (Task 1), `gfv2_params.depstor.load_connected_comids`.
- Produces: `compare_onstream_sets(segment: set[int], wbareacomi: set[int], flowthrough: set[int]) -> dict[str, int]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_ab_segment_vs_nhd_onstream.py`:

```python
"""The A/B comparison arithmetic, isolated from any file or data root."""

from __future__ import annotations

from scripts.diagnose.ab_segment_vs_nhd_onstream import compare_onstream_sets


def test_compare_counts_each_direction():
    out = compare_onstream_sets(
        segment={1, 2, 3}, wbareacomi={2, 3, 4}, flowthrough={3, 5}
    )
    assert out["n_segment"] == 3
    assert out["n_wbareacomi"] == 3
    assert out["n_flowthrough"] == 2
    assert out["n_nhd_union"] == 4              # {2,3,4,5}
    assert out["n_shared"] == 2                 # {2,3}
    assert out["n_segment_only"] == 1           # {1}
    assert out["n_nhd_only"] == 2               # {4,5}


def test_compare_with_no_nhd_tables():
    out = compare_onstream_sets(segment={1, 2}, wbareacomi=set(), flowthrough=set())
    assert out["n_nhd_union"] == 0
    assert out["n_segment_only"] == 2
    assert out["n_shared"] == 0
```

- [ ] **Step 2: Run to verify it fails**

```bash
srun -p cpu -A impd --mem=4G --time=00:10:00 \
  pixi run -e dev pytest tests/test_ab_segment_vs_nhd_onstream.py -q
```

Expected: `ModuleNotFoundError` / `ImportError`.

- [ ] **Step 3: Write the script**

Create `scripts/diagnose/ab_segment_vs_nhd_onstream.py`:

```python
"""A/B the segment-derived on-stream set against NHD flowline topology.

Reads the `segment_wbody` step's output for a fabric and compares it to the two NHD
COMID tables, so the classifier switch can be quantified without wiring the NHD tables
back into a production profile. This replaces the throwaway measurement script that
produced the design spec's CONUS numbers (48,529 on-stream COMIDs; 16,939 segment-only
vs WBAREACOMI, 6,265 vs flow-through).

  pixi run --as-is python scripts/diagnose/ab_segment_vs_nhd_onstream.py --fabric gfv2

Paths come from the fabric profile, never hardcoded. The NHD tables are read from
`{data_root}/input/nhd/` because the profile keys are commented out by design.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import load_connected_comids
from gfv2_params.log import configure_logging
from gfv2_params.segment_wbody import load_segment_comids


def compare_onstream_sets(
    segment: set[int], wbareacomi: set[int], flowthrough: set[int]
) -> dict[str, int]:
    """Counts in each direction between the segment set and the NHD union."""
    nhd = wbareacomi | flowthrough
    return {
        "n_segment": len(segment),
        "n_wbareacomi": len(wbareacomi),
        "n_flowthrough": len(flowthrough),
        "n_nhd_union": len(nhd),
        "n_shared": len(segment & nhd),
        "n_segment_only": len(segment - nhd),
        "n_nhd_only": len(nhd - segment),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/depstor/depstor_rasters.yml")
    parser.add_argument("--base_config", default=None)
    parser.add_argument("--fabric", default=None)
    args = parser.parse_args()

    logger = configure_logging("ab_segment_vs_nhd_onstream")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    data_root = Path(config["data_root"])
    output_dir = Path(
        require_config_key(config, "output_dir", "ab_segment_vs_nhd_onstream")
        .replace("{data_root}", str(data_root))
        .replace("{fabric}", config["fabric"])
    )
    segment_table = output_dir / "segment_waterbody_comids.parquet"
    if not segment_table.exists():
        raise FileNotFoundError(
            f"{segment_table} not found — run the `segment_wbody` depstor step for "
            f"fabric '{config['fabric']}' first."
        )

    segment = load_segment_comids(segment_table)
    nhd_dir = data_root / "input" / "nhd"
    wbareacomi = _maybe_load(nhd_dir / "connected_waterbody_comids.parquet", logger)
    flowthrough = _maybe_load(nhd_dir / "flowthrough_waterbody_comids.parquet", logger)

    logger.info("=== segment vs NHD on-stream, fabric=%s ===", config["fabric"])
    for key, value in compare_onstream_sets(segment, wbareacomi, flowthrough).items():
        logger.info("  %-16s %d", key, value)


def _maybe_load(path: Path, logger) -> set[int]:
    if not path.exists():
        logger.warning("  %s not staged — treating as empty", path)
        return set()
    return load_connected_comids(path)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test and remove the throwaway**

```bash
srun -p cpu -A impd --mem=4G --time=00:10:00 \
  pixi run -e dev pytest tests/test_ab_segment_vs_nhd_onstream.py -q
rm -f scripts/diagnose/measure_segment_onstream.py
git status --short scripts/diagnose/
```

Expected: 2 tests pass; `git status` shows only the new file as untracked (the throwaway
was never tracked, so it simply disappears).

- [ ] **Step 5: Lint and commit**

```bash
pixi run -e dev pre-commit run --files \
  scripts/diagnose/ab_segment_vs_nhd_onstream.py tests/test_ab_segment_vs_nhd_onstream.py
git add scripts/diagnose/ab_segment_vs_nhd_onstream.py tests/test_ab_segment_vs_nhd_onstream.py
git commit -m "feat(diagnose): A/B the segment on-stream set against NHD topology

Quantifies the classifier switch without wiring the NHD tables back into a
production profile. Replaces the hardcoded-path throwaway that produced the
spec's CONUS numbers."
```

---

## Task 7: Documentation sweep

**Files:**
- Modify: `CLAUDE.md`, `docs/ARCHITECTURE.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`, the depstor workflow docs page, `scripts/render_depstor_figures.py`

**Interfaces:** none — documentation only.

- [ ] **Step 1: Find every claim the change invalidates**

```bash
grep -rn "UNION of two COMID sources\|connected_comids_table\|flowthrough_comids_table\|WBAREACOMI\|conus_waterbodies" \
  CLAUDE.md README.md docs/ slurm_batch/*.md mkdocs.yml scripts/render_depstor_figures.py \
  --include='*.md' --include='*.py' --include='*.yml' | grep -v superpowers/specs | grep -v superpowers/plans
```

Work the list; the four below are known and must be handled.

- [ ] **Step 2: Rewrite CLAUDE.md's first non-obvious-conventions bullet**

Replace the whole bullet beginning **"The dprst/on-stream split is driven by the UNION of two COMID sources."** with:

```markdown
- **The dprst/on-stream split is driven by the MODEL's own segment network.**
  `segment_wbody` promotes a waterbody to on-stream iff an `nsegment` from the
  fabric's `segments_gpkg` intersects it with **positive length** (a zero-length
  shoreline graze does not count — 3.1% of candidate pairs CONUS-wide). This asks
  "is it on the network the model routes?", not "is it on the NHD network": NHD's
  network is far finer, and a waterbody NHD routes but the model does not had no
  representation at all. `wbody_connectivity` consumes that COMID table as its
  **required primary** source, then applies the two unchanged subtractions —
  `endorheic` and the Playa/Ice Mass guardrail. Consequences that are deliberate:
  a segment collinear with a shoreline promotes, and a segment **terminating
  inside** a waterbody promotes, so NHD's inflow-AND-outflow discrimination is
  gone and the **endorheic subtraction is what still demotes terminal lakes**.
  NHD flowline topology (`nhd_flowlines` WBAREACOMI, `nhd_flowthrough`,
  `nhd_topology`) is retained as an **opt-in comparison union**: the profile keys
  are commented out on every fabric, and `wbody_connectivity` logs a
  `COMPARISON MODE` warning if it ever sees one. Because they are opt-in, the
  "`nhd_topology` must run before both `nhd_flowlines` and `nhd_flowthrough`"
  ordering constraint now applies only to that comparison path. If
  `drains_to_dprst` over-extends, fix the **classifier** — never add a cap or
  tuning knob to routing. A cap cannot distinguish a legitimately large endorheic
  basin from a spurious one and damages the correct cases.
```

Also update the `min_endorheic_comids` bullet's closing sentence to mention its sibling:

```markdown
  The same both-ends contract applies to `min_onstream_comids`, the floor on the
  `segment_wbody` COMID count (gfv2 30000 against 48,529 measured; oregon 500
  against 770).
```

- [ ] **Step 3: Update `docs/ARCHITECTURE.md`**

- Add `segment_wbody` to the depstor step list between `endorheic` and
  `wbody_connectivity`, with output `segment_waterbody_comids.parquet` and registered key
  `segment_wbody_comids`.
- Add `min_onstream_comids` to the per-key required-field table as an optional per-fabric
  key.
- Note that `segments_gpkg`/`segments_layer` are now **required** depstor inputs (they
  were previously described as retained for other tooling).
- State that `waterbody_gpkg` is `input/nhd/nhd_waterbodies.gpkg` on every CONUS fabric.

- [ ] **Step 4: Update the runbooks**

In `slurm_batch/RUNME.md` and `slurm_batch/HPC_REFERENCE.md`:

- Insert `segment_wbody` into the documented depstor step order.
- Change the cascade-rebuild recipe: for a classifier-only change it is
  `--from segment_wbody`; for this change (which also swaps the waterbody layer) it is
  `--from waterbody`, and `waterbody`/`dprst` are the ~384G full-grid steps.
- Note that `segment_wbody` is cheap — 42 s / 2.0 GB at CONUS — so it does not affect
  the batch's `--mem` sizing.
- Remove any instruction to stage `nhd_flowlines` / `nhd_flowthrough` / `nhd_topology` as
  a prerequisite of a normal depstor run; mark them comparison-only.

- [ ] **Step 5: Update the depstor workflow docs page and the DAG figure**

Find the page:

```bash
grep -rln "wbody_connectivity" docs/*.md docs/**/*.md
```

Update its DAG/prose the same way. Then in `scripts/render_depstor_figures.py`, the DAG
figure (around the `"topology": ("nhd_topology", 0.25, 0.92, 0.07)` node and the caption
at ~line 1449) must show `segment_wbody` as the primary path into
`wbody_connectivity`, with `nhd_topology`/`nhd_flowlines`/`nhd_flowthrough` de-emphasised
as the opt-in comparison branch. Re-render to confirm it still runs:

```bash
srun -p cpu -A impd --mem=8G --time=00:20:00 \
  pixi run --as-is python scripts/render_depstor_figures.py
```

- [ ] **Step 6: Verify no stale claim survives**

```bash
grep -rn "UNION of two COMID sources\|segments no longer feed\|conus_waterbodies" \
  CLAUDE.md README.md docs/ slurm_batch/*.md configs/ \
  | grep -v superpowers/specs | grep -v superpowers/plans | grep -v tests/data
srun -p cpu -A impd --mem=8G --time=00:20:00 pixi run -e dev pytest tests/ -q
pixi run -e dev pre-commit run --all-files
```

Expected: the grep returns nothing outside the spec/plan/fixture-README (which
deliberately record the superseded state); the full suite passes.

- [ ] **Step 7: Commit**

```bash
git add CLAUDE.md docs/ slurm_batch/ scripts/render_depstor_figures.py
git commit -m "docs(depstor): on-stream comes from the model segment network

Rewrites CLAUDE.md's 'UNION of two COMID sources' bullet, which this change
invalidates, and records the two deliberate consequences of the
positive-length rule: a shoreline-collinear segment promotes, and a segment
terminating inside a waterbody promotes, so the endorheic subtraction is now
the only thing demoting terminal lakes.

Adds segment_wbody to the ARCHITECTURE step list and the runbook cascade,
notes that segments_gpkg is a required depstor input again, and marks the
nhd_topology ordering constraint as applying only to the opt-in comparison
path."
```

---

## Self-Review

**Spec coverage** — every section of the design spec maps to a task:

| Spec section | Task |
| --- | --- |
| The rule (positive length) | 1 |
| Edge cases, verified and deliberate | 1 (5 parametrised tests) |
| `gfv2_params/segment_wbody.py` | 1 |
| `depstor_builders/segment_wbody.py` | 2 |
| `wbody_connectivity` modifications | 3 |
| Config (`depstor_rasters.yml`) | 2 |
| Config (`base_config.yml`, 4 profiles + vpu01 note) | 4 |
| Waterbody layer swap | 4 |
| Guard 1 (segment table required) | 3 |
| Guard 2 (floor at both ends) | 2 (producer) + 3 (consumer) |
| Guard 3 (extent guard) | 2 |
| Guard 4 (coverage logging) | 1 (pair/graze counts) + 2 (COMID counts) |
| Guard 5 (loud union warning) | 3 |
| Guard 6 (Playa/Ice Mass unchanged) | 3 |
| Guard 7 (non-numeric COMID dropped) | 1 |
| Guard 8 (invalid geometry repaired) | 1 |
| Join on row index, not COMID | 1 |
| Tests (synthetic) | 1, 2, 3 |
| Fixture provenance | 5 |
| Docs | 7 |
| Reservoir FTYPE anomaly | no task — spec-documented finding, no code |
| Rollout | no task — operational, see below |

**Not covered by any task, deliberately:** the rollout (oregon → gfv2_dev → gfv2 rebuilds
and the Stage B re-derive) is operational work, not code. It runs after this plan merges,
following the spec's Rollout section. Flag to the operator: the CONUS cascade starts at
`--from waterbody`, three fabrics rebuild, and `gfv2` must not be touched until the
`gfv2_dev` product is checked.

**Placeholder scan:** no TBD/TODO; every code step carries runnable code; the fixture
COMIDs, expected sets, floor values, and config line numbers are all concrete.

**Type consistency:** `segment_wbody_comids` is the context key in Tasks 2/3 and in
`_expected_outputs`; `segment_waterbody_comids.parquet` is the filename in Tasks 2/5/6;
the parquet schema is `comid`/`n_segments`/`overlap_m` in Tasks 1/2/3/5; the pair schema
is `comid`/`wb_index`/`seg_index`/`overlap_m` in Tasks 1/5. `check_onstream_floor` and
`load_segment_comids` keep one signature across Tasks 1, 2, 3 and 6.

**One risk worth naming:** Task 3's 16-test migration is the largest source of churn and
the easiest place to silently weaken coverage. Do not make the segment table optional to
avoid it — that would reintroduce exactly the silent-fallback failure mode the spec's
guard 1 exists to prevent.
