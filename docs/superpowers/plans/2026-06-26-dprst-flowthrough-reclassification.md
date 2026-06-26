# dprst Flow-Through Reclassification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `drains_to_dprst` from over-extending by reclassifying through-flow
waterbodies (channel inflow AND outflow) from depression-storage to on-stream, so
they are no longer routing pour-points — with no cap and no kernel change.

**Architecture:** A new NHD-staging module distills a second auditable set of
"on-stream" waterbody COMIDs from flowline/area topology
(`flowthrough_waterbody_comids.parquet`), mirroring the existing
`connected_waterbody_comids.parquet` (WBAREACOMI) pattern. The depstor
`wbody_connectivity` builder unions the two COMID sets; everything downstream
(`dprst` → `routing` → …) is unchanged code that simply sees a larger on-stream
set.

**Tech Stack:** Python 3.12, geopandas/shapely 2.x, pyogrio, pandas, pyarrow,
rasterio, numpy. Env via pixi (`pixi run -e dev pytest`). Tests run in CI, not on
the HPC head node.

## Global Constraints

- **No cap/knob on routing.** The fix is classification only; never add a
  distance/area/accumulation threshold or a QA_MA/contributing-area tie-breaker.
- **Endorheic guardrail.** `FTYPE ∈ {"Playa", "Ice Mass"}` waterbodies always
  stay dprst — drop them before any promotion test.
- **Strictly additive.** Flow-through COMIDs are *unioned* into the on-stream set;
  nothing currently on-stream is ever removed.
- **Conveyance flowline FTYPEs:** `{"StreamRiver", "ArtificialPath", "Connector",
  "CanalDitch"}`. **Conveyance NHDArea FTYPEs:** `{"StreamRiver"}`.
- **Paths from the profile**, never hardcoded — read via `require_config_key` /
  `config.get(...)` and the `{data_root}`/`{fabric}` placeholders.
- **Builder + test together.** Every new module ships a `tests/test_*.py`.
- **Run `pixi run -e dev pre-commit run --all-files` before every commit.** CI is
  the test gate.
- **Docs check on the same branch** (ARCHITECTURE, RUNME, HPC_REFERENCE, config
  comments).
- Branch already created: `feat/dprst-flowthrough-reclassification`.

---

## File Structure

**Create:**
- `src/gfv2_params/download/nhd_flowthrough.py` — topology classifier (pure
  function `flowthrough_comids`) + per-VPU staging `main()`.
- `tests/test_nhd_flowthrough.py` — synthetic-geometry unit tests for the
  classifier.
- `scripts/diagnose_drains_to_dprst.py` — repeatable CONUS coverage diagnostic.
- `tests/test_diagnose_drains_to_dprst.py` — smoke test of the per-VPU coverage
  math on a tiny synthetic raster.

**Modify:**
- `src/gfv2_params/depstor_builders/context.py` — add `flowthrough_comids_table`.
- `src/gfv2_params/depstor_builders/wbody_connectivity.py` — union the two COMID
  sets.
- `scripts/build_depstor_rasters.py` — wire `flowthrough_comids_table` into
  `BuildContext`.
- `configs/base_config.yml` — add `flowthrough_comids_table` to `gfv2` and
  `gfv2_dev` profiles.
- `tests/test_wbody_connectivity.py` (if present; else create) — union behavior.
- `docs/ARCHITECTURE.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`.

---

## Task 1: Core flow-through classifier (pure function)

The heart of the change: a CRS-agnostic, I/O-free function that takes three
GeoDataFrames and returns the set of waterbody COMIDs that are on-stream by
flow-through topology. Unit-tested entirely with synthetic shapely geometry.

**Files:**
- Create: `src/gfv2_params/download/nhd_flowthrough.py`
- Test: `tests/test_nhd_flowthrough.py`

**Interfaces:**
- Produces:
  - `CONVEYANCE_FTYPES: set[str]`, `CONVEYANCE_AREA_FTYPES: set[str]`,
    `FORCE_DPRST_FTYPES: set[str]`
  - `flowthrough_comids(waterbodies, flowlines, areas=None) -> set[int]`
    where `waterbodies` has columns `COMID, FTYPE, geometry` (polygons),
    `flowlines` has `FTYPE, FLOWDIR, geometry` (lines), `areas` (optional) has
    `FTYPE, geometry` (polygons). All three share one CRS.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_nhd_flowthrough.py
"""Unit tests for the flow-through waterbody classifier (synthetic geometry)."""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import LineString, Polygon

from gfv2_params.download.nhd_flowthrough import flowthrough_comids

CRS = "EPSG:4269"


def _wb(rows):
    return gpd.GeoDataFrame(
        rows, columns=["COMID", "FTYPE", "geometry"], crs=CRS
    )


def _fl(rows):
    return gpd.GeoDataFrame(
        rows, columns=["FTYPE", "FLOWDIR", "geometry"], crs=CRS
    )


# A unit square waterbody centred near (0,0)..(2,2).
SQUARE = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])


def test_single_line_passes_through_is_onstream():
    # T1: one conveyance line crosses the boundary twice (enters west, exits east)
    wb = _wb([[101, "SwampMarsh", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == {101}


def test_split_inflow_and_outflow_is_onstream():
    # T2: line A ends inside (inflow), line B starts inside (outflow); neither
    # alone crosses twice, but together they pair to flow-through.
    wb = _wb([[102, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])],  # downstream end inside -> inflow
        ["StreamRiver", "With Digitized", LineString([(1, 1), (3, 1)])],   # upstream end inside -> outflow
    ])
    assert flowthrough_comids(wb, fl) == {102}


def test_terminal_sink_inflow_only_stays_dprst():
    # Inflow only (line ends inside, nothing leaves) -> NOT promoted.
    wb = _wb([[103, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_spilling_pothole_outflow_only_stays_dprst():
    # Outflow only (line starts inside, nothing enters) -> NOT promoted.
    wb = _wb([[104, "SwampMarsh", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_isolated_waterbody_stays_dprst():
    wb = _wb([[105, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(5, 5), (7, 5)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_playa_force_dprst_even_with_throughflow():
    # Endorheic guardrail: a Playa with a line straight through stays dprst.
    wb = _wb([[106, "Playa", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_ice_mass_force_dprst():
    wb = _wb([[107, "Ice Mass", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_non_conveyance_line_ignored():
    # A Pipeline through the waterbody is not a stream -> not flow-through.
    wb = _wb([[108, "LakePond", SQUARE]])
    fl = _fl([["Pipeline", "With Digitized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == set()


def test_uninitialized_flowdir_still_caught_by_t1():
    # FLOWDIR unreliable, but a single line crossing twice (T1) is direction-free.
    wb = _wb([[109, "SwampMarsh", SQUARE]])
    fl = _fl([["StreamRiver", "Uninitialized", LineString([(-1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl) == {109}


def test_uninitialized_split_pair_not_paired_by_t2():
    # Two separate Uninitialized lines (one ends inside, one starts inside):
    # T2 must NOT trust their direction, so they don't pair -> stays dprst.
    wb = _wb([[110, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "Uninitialized", LineString([(-1, 1), (1, 1)])],
        ["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])],
    ])
    assert flowthrough_comids(wb, fl) == set()


def test_nhdarea_coincidence_is_onstream():
    # T3: waterbody overlaps a StreamRiver NHDArea polygon (2-D channel).
    wb = _wb([[111, "LakePond", SQUARE]])
    fl = _fl([])  # no flowlines at all
    areas = gpd.GeoDataFrame(
        [["StreamRiver", Polygon([(1, -1), (3, -1), (3, 3), (1, 3)])]],
        columns=["FTYPE", "geometry"], crs=CRS,
    )
    assert flowthrough_comids(wb, fl, areas) == {111}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_nhd_flowthrough.py -v`
Expected: FAIL — `ModuleNotFoundError` / `ImportError: cannot import name 'flowthrough_comids'`.

- [ ] **Step 3: Write the classifier**

```python
# src/gfv2_params/download/nhd_flowthrough.py  (module top + classifier; main() added in Task 2)
"""Distil flow-through (on-stream) waterbody COMIDs from NHD topology.

WBAREACOMI (see nhd_flowlines) only flags waterbodies NHD drew an artificial
path through. Many through-flow swamps/marshes carry no WBAREACOMI and are
wrongly left in depression storage, so their whole upstream watershed counts as
draining to dprst. This module adds a second, geometry-based on-stream signal:
a waterbody that a stream demonstrably flows THROUGH (channel inflow AND
outflow) is on-stream/lake, not a depression. Endorheic terminal sinks (Playa,
Ice Mass) are force-kept as dprst.

The COMID set written here is unioned with connected_waterbody_comids.parquet by
the depstor wbody_connectivity builder.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# A conveyance NHDFlowline carries channelised flow (vs. Pipeline, Coastline...).
CONVEYANCE_FTYPES = {"StreamRiver", "ArtificialPath", "Connector", "CanalDitch"}
# NHDArea polygons that ARE the 2-D channel (wide/braided rivers).
CONVEYANCE_AREA_FTYPES = {"StreamRiver"}
# Endorheic guardrail: these never get promoted out of dprst, regardless of
# topology (and FLOWDIR is unreliable around them).
FORCE_DPRST_FTYPES = {"Playa", "Ice Mass"}

_DIGITIZED = "With Digitized"  # FLOWDIR value where geometry direction is trusted


def _endpoints(geom):
    """(upstream_point, downstream_point) for a (Multi)LineString: first & last coord."""
    if geom.geom_type == "MultiLineString":
        parts = list(geom.geoms)
        first = parts[0].coords[0]
        last = parts[-1].coords[-1]
    else:
        first = geom.coords[0]
        last = geom.coords[-1]
    return Point(first), Point(last)


def flowthrough_comids(
    waterbodies: gpd.GeoDataFrame,
    flowlines: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame | None = None,
) -> set[int]:
    """COMIDs of waterbodies classified on-stream by flow-through topology.

    `waterbodies`: polygons with COMID, FTYPE. `flowlines`: lines with FTYPE,
    FLOWDIR. `areas`: optional NHDArea polygons with FTYPE. All share one CRS.

    A waterbody is on-stream if ANY of:
      T1  a single conveyance flowline crosses its boundary >= 2 times,
      T2  it has >= 1 inflow (a 'With Digitized' conveyance line whose
          downstream end is inside) AND >= 1 outflow (upstream end inside),
      T3  it overlaps a conveyance NHDArea polygon.
    Playa / Ice Mass waterbodies are dropped first and never returned.
    """
    wb = waterbodies[~waterbodies["FTYPE"].isin(FORCE_DPRST_FTYPES)].copy()
    if wb.empty:
        return set()
    wb = wb.reset_index(drop=True)
    wb["_wbidx"] = wb.index

    conv = flowlines[flowlines["FTYPE"].isin(CONVEYANCE_FTYPES)].copy()
    onstream: set[int] = set()

    if not conv.empty:
        conv = conv.reset_index(drop=True)
        # Candidate (waterbody, flowline) pairs that intersect at all.
        pairs = gpd.sjoin(
            conv[["FTYPE", "FLOWDIR", "geometry"]],
            wb[["_wbidx", "geometry"]],
            how="inner", predicate="intersects",
        )

        # --- T1: a single line crosses the boundary >= 2 times ---
        t1_idx: set[int] = set()
        for line_pos, wbidx in zip(pairs.index, pairs["_wbidx"]):
            line = conv.geometry.iloc[line_pos]
            poly = wb.geometry.iloc[wbidx]
            crossing = line.intersection(poly.boundary)
            n = 0 if crossing.is_empty else (
                len(crossing.geoms) if crossing.geom_type.startswith("Multi") else 1
            )
            if n >= 2:
                t1_idx.add(int(wbidx))

        # --- T2: inflow endpoint AND outflow endpoint, trusting digitization ---
        has_inflow: set[int] = set()
        has_outflow: set[int] = set()
        dig = pairs[pairs["FLOWDIR"] == _DIGITIZED]
        for line_pos, wbidx in zip(dig.index, dig["_wbidx"]):
            up, down = _endpoints(conv.geometry.iloc[line_pos])
            poly = wb.geometry.iloc[wbidx]
            if poly.covers(down):   # downstream end inside -> water flows IN
                has_inflow.add(int(wbidx))
            if poly.covers(up):     # upstream end inside -> water flows OUT
                has_outflow.add(int(wbidx))
        t2_idx = has_inflow & has_outflow

        for wbidx in t1_idx | t2_idx:
            onstream.add(int(wb.loc[wbidx, "COMID"]))

    # --- T3: overlap a conveyance NHDArea polygon ---
    if areas is not None and not areas.empty:
        ca = areas[areas["FTYPE"].isin(CONVEYANCE_AREA_FTYPES)]
        if not ca.empty:
            hit = gpd.sjoin(
                wb[["COMID", "geometry"]], ca[["geometry"]],
                how="inner", predicate="intersects",
            )
            onstream |= {int(c) for c in hit["COMID"].unique()}

    return onstream
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_nhd_flowthrough.py -v`
Expected: PASS (11 tests).

- [ ] **Step 5: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add src/gfv2_params/download/nhd_flowthrough.py tests/test_nhd_flowthrough.py
git commit -m "feat(nhd): flow-through waterbody classifier (T1/T2/T3, Playa/Ice Mass force-dprst)"
```

---

## Task 2: Per-VPU staging `main()` (read snapshots → write parquet)

Wrap the classifier in the per-VPU NHDSnapshot loop, reusing `nhd_flowlines`'
download/extract and parquet writer. Adds the I/O the classifier deliberately
omits.

**Files:**
- Modify: `src/gfv2_params/download/nhd_flowthrough.py`
- Test: `tests/test_nhd_flowthrough.py` (add helper-locate + FTYPE-normalise tests)

**Interfaces:**
- Consumes (imported from `gfv2_params.download.nhd_flowlines`):
  `vpu_index: dict[str,str]`, `download_snapshot(dd, vpu, download_dir,
  extract_dir) -> Path | None`, `write_connected_comids(comids, out_path)`.
- Produces: `locate_layer(flowline_shp: Path, layer: str) -> Path | None`,
  `read_layer(path, columns) -> gpd.GeoDataFrame` (case-insensitive field
  normalise, with geometry), `main() -> None` writing
  `input/nhd/flowthrough_waterbody_comids.parquet`.

- [ ] **Step 1: Write the failing tests for the locate + read helpers**

```python
# append to tests/test_nhd_flowthrough.py
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon

from gfv2_params.download.nhd_flowthrough import locate_layer, read_layer


def test_locate_layer_finds_sibling(tmp_path):
    hydro = tmp_path / "NHDPlus17" / "NHDSnapshot" / "Hydrography"
    hydro.mkdir(parents=True)
    (hydro / "NHDFlowline.shp").write_bytes(b"")
    (hydro / "NHDWaterbody.shp").write_bytes(b"")
    flowline = hydro / "NHDFlowline.shp"
    assert locate_layer(flowline, "NHDWaterbody") == hydro / "NHDWaterbody.shp"
    assert locate_layer(flowline, "NHDArea") is None


def test_read_layer_normalises_field_casing(tmp_path):
    # Mixed-case fields (VPU 13 ships ComID/FType) must normalise to upper-case.
    p = tmp_path / "wb.gpkg"
    gpd.GeoDataFrame(
        {"ComID": [9], "FType": ["SwampMarsh"],
         "geometry": [Polygon([(0, 0), (1, 0), (1, 1)])]},
        crs="EPSG:4269",
    ).to_file(p)
    out = read_layer(p, ["COMID", "FTYPE"])
    assert list(out.columns) == ["COMID", "FTYPE", "geometry"]
    assert out["FTYPE"].iloc[0] == "SwampMarsh"
```

- [ ] **Step 2: Run to verify failure**

Run: `pixi run -e dev pytest tests/test_nhd_flowthrough.py -k "locate or normalise" -v`
Expected: FAIL — `ImportError: cannot import name 'locate_layer'`.

- [ ] **Step 3: Implement the helpers + `main()`**

```python
# append to src/gfv2_params/download/nhd_flowthrough.py
from pathlib import Path

import pyogrio

from gfv2_params.config import load_base_config
from gfv2_params.download.nhd_flowlines import (
    download_snapshot,
    vpu_index,
    write_connected_comids,
)
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_flowthrough")


def locate_layer(flowline_shp: Path, layer: str) -> Path | None:
    """Find a sibling NHDSnapshot layer (e.g. NHDWaterbody) next to NHDFlowline."""
    candidate = flowline_shp.with_name(f"{layer}.shp")
    return candidate if candidate.exists() else None


def read_layer(path: Path, columns: list[str]) -> gpd.GeoDataFrame:
    """Read `columns` (case-insensitive) + geometry, normalised to upper-case.

    NHD field casing is inconsistent across VPU snapshots; requesting exact
    upper-case names would make pyogrio silently drop a mismatched-case column.
    """
    available = list(pyogrio.read_info(path)["fields"])
    by_upper = {name.upper(): name for name in available}
    rename = {}
    for canon in columns:
        actual = by_upper.get(canon)
        if actual is None:
            raise KeyError(
                f"{path}: layer has no '{canon}' field (case-insensitive). "
                f"Available: {available}"
            )
        rename[actual] = canon
    gdf = gpd.read_file(path, columns=list(rename), use_arrow=True)
    return gdf.rename(columns=rename)[[*columns, "geometry"]]


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    onstream: set[int] = set()
    failures = []
    for vpu, dd in vpu_index.items():
        flowline = download_snapshot(dd, vpu, download_dir, extract_dir)
        if flowline is None:
            failures.append(vpu)
            continue
        wb_path = locate_layer(flowline, "NHDWaterbody")
        if wb_path is None:
            failures.append(vpu)
            continue
        waterbodies = read_layer(wb_path, ["COMID", "FTYPE"])
        flowlines = read_layer(flowline, ["FTYPE", "FLOWDIR"])
        area_path = locate_layer(flowline, "NHDArea")
        areas = read_layer(area_path, ["FTYPE"]) if area_path else None
        vpu_set = flowthrough_comids(waterbodies, flowlines, areas)
        logger.info(f"VPU {vpu}: {len(vpu_set)} flow-through waterbody COMIDs")
        onstream |= vpu_set

    if failures:
        raise RuntimeError(
            f"NHDSnapshot flow-through staging failed for VPU(s): {failures}"
        )

    out_path = data_root / "input/nhd/flowthrough_waterbody_comids.parquet"
    write_connected_comids(onstream, out_path)
    logger.info(f"Wrote {len(onstream)} flow-through COMIDs -> {out_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the helper tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_nhd_flowthrough.py -v`
Expected: PASS (all, including the two new helper tests).

- [ ] **Step 5: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add src/gfv2_params/download/nhd_flowthrough.py tests/test_nhd_flowthrough.py
git commit -m "feat(nhd): per-VPU flow-through staging main() -> flowthrough_waterbody_comids.parquet"
```

> **Manual integration run (not a unit test — needs S3 + snapshots, not on the
> head node):** the module is exercised at staging time via
> `pixi run --as-is python -m gfv2_params.download.nhd_flowthrough`. Snapshots are
> already extracted under `shared/source/<vpu>/`, so this re-reads them.

---

## Task 3: Wire `flowthrough_comids_table` through context + config

**Files:**
- Modify: `src/gfv2_params/depstor_builders/context.py:28`
- Modify: `scripts/build_depstor_rasters.py:84-87`
- Modify: `configs/base_config.yml` (gfv2 ~line 48, gfv2_dev ~line 86)

**Interfaces:**
- Produces: `BuildContext.flowthrough_comids_table: Path | None = None`
  populated from the `flowthrough_comids_table` profile key.

- [ ] **Step 1: Add the context field**

In `src/gfv2_params/depstor_builders/context.py`, after line 28
(`connected_comids_table: Path | None = None`) add:

```python
    flowthrough_comids_table: Path | None = None
```

- [ ] **Step 2: Wire it into `_build_context`**

In `scripts/build_depstor_rasters.py`, in the `BuildContext(...)` call (after the
`connected_comids_table=...` block at lines 84-87) add:

```python
        flowthrough_comids_table=(
            Path(config["flowthrough_comids_table"])
            if config.get("flowthrough_comids_table") else None
        ),
```

- [ ] **Step 3: Add the config key to both CONUS profiles**

In `configs/base_config.yml`, immediately after the `connected_comids_table:`
line in the `gfv2` profile (line 48):

```yaml
    # Flow-through waterbody COMIDs from NHD stream/area topology (see
    # gfv2_params.download.nhd_flowthrough). Unioned with connected_comids_table
    # by wbody_connectivity to catch through-flow swamps NHD never tagged with
    # WBAREACOMI. Optional: absent -> WBAREACOMI-only behaviour.
    flowthrough_comids_table: "{data_root}/input/nhd/flowthrough_waterbody_comids.parquet"
```

And the identical key after the `connected_comids_table:` line in the `gfv2_dev`
profile (line 86).

- [ ] **Step 4: Verify config loads + key resolves**

Run:
```bash
pixi run --as-is python -c "from gfv2_params.config import load_fabric_config; \
print(load_fabric_config('gfv2').get('flowthrough_comids_table'))"
```
Expected: a path ending `input/nhd/flowthrough_waterbody_comids.parquet`
(placeholders resolved). If the helper name differs, use the same loader the
existing tests use for `connected_comids_table` — check
`tests/test_config.py` for the resolve helper and mirror it.

- [ ] **Step 5: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add src/gfv2_params/depstor_builders/context.py scripts/build_depstor_rasters.py configs/base_config.yml
git commit -m "feat(config): add flowthrough_comids_table to BuildContext + gfv2/gfv2_dev profiles"
```

---

## Task 4: Union the two COMID sets in `wbody_connectivity`

**Files:**
- Modify: `src/gfv2_params/depstor_builders/wbody_connectivity.py`
- Test: `tests/test_wbody_connectivity.py` (create if absent)

**Interfaces:**
- Consumes: `ctx.flowthrough_comids_table` (Task 3),
  `load_connected_comids` (depstor.py:256), `select_connected_waterbodies`
  (depstor.py:262).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_wbody_connectivity.py
"""The on-stream set is the union of WBAREACOMI + flow-through COMIDs."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from gfv2_params.depstor import select_connected_waterbodies


def _sq(x):
    return Polygon([(x, 0), (x + 1, 0), (x + 1, 1), (x, 1)])


def test_union_promotes_flowthrough_only_waterbody():
    # WB 200 is WBAREACOMI-connected; WB 201 is ONLY flow-through. The union
    # must flag both; WBAREACOMI alone would miss 201.
    wb = gpd.GeoDataFrame(
        {"COMID": [200, 201, 202],
         "member_comid": [200, 201, 202],
         "geometry": [_sq(0), _sq(2), _sq(4)]},
        crs="EPSG:4269",
    )
    connected = {200}
    flowthrough = {201}
    union = connected | flowthrough
    sel = select_connected_waterbodies(wb, union)
    assert set(sel["COMID"]) == {200, 201}
```

- [ ] **Step 2: Run to verify it passes already (union is set-level)**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -v`
Expected: PASS — this pins the *intended* union semantics before wiring the
builder. (`select_connected_waterbodies` already handles a pre-unioned set.)

- [ ] **Step 3: Wire the union into the builder**

In `src/gfv2_params/depstor_builders/wbody_connectivity.py`, replace the
connected-set load (line 54) with a union that folds in the optional
flow-through table:

```python
    connected = load_connected_comids(ctx.connected_comids_table)
    n_wbareacomi = len(connected)
    n_flowthrough = 0
    if ctx.flowthrough_comids_table is not None:
        if not ctx.flowthrough_comids_table.exists():
            raise FileNotFoundError(
                f"Flow-through COMID table not found: "
                f"{ctx.flowthrough_comids_table}. Run "
                f"`python -m gfv2_params.download.nhd_flowthrough` first, or "
                f"remove `flowthrough_comids_table` from the profile."
            )
        flowthrough = load_connected_comids(ctx.flowthrough_comids_table)
        n_flowthrough = len(flowthrough - connected)
        connected = connected | flowthrough
    logger.info(
        "  on-stream COMIDs: %d WBAREACOMI + %d new flow-through = %d total",
        n_wbareacomi, n_flowthrough, len(connected),
    )
```

(Keep the existing `select_connected_waterbodies(wb_gdf, connected)` call and
everything after it unchanged.)

- [ ] **Step 4: Run the full builder test suite**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py tests/test_nhd_flowthrough.py -v`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add src/gfv2_params/depstor_builders/wbody_connectivity.py tests/test_wbody_connectivity.py
git commit -m "feat(depstor): union flow-through COMIDs into wbody_connectivity on-stream set"
```

---

## Task 5: CONUS coverage diagnostic (formalise the 2026-06-25 run)

A repeatable script + smoke test. The script is the CONUS acceptance gate (run
manually pre/post rebuild); the test pins its per-VPU coverage math.

**Files:**
- Create: `scripts/diagnose_drains_to_dprst.py`
- Test: `tests/test_diagnose_drains_to_dprst.py`

**Interfaces:**
- Produces: `vpu_coverage(drains: np.ndarray, vpu_id: np.ndarray, land: np.ndarray)
  -> dict[int, float]` returning, per VPU code, the fraction of land cells where
  `drains == 1`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_diagnose_drains_to_dprst.py
import numpy as np

from scripts.diagnose_drains_to_dprst import vpu_coverage


def test_vpu_coverage_fraction_of_land():
    # VPU 1: 4 land cells, 2 drain -> 0.5. VPU 2: 2 land cells, 0 drain -> 0.0.
    vpu_id = np.array([[1, 1, 2], [1, 1, 2]], dtype=np.uint8)
    drains = np.array([[1, 1, 0], [0, 0, 0]], dtype=np.uint8)
    land = np.ones((2, 3), dtype=bool)
    cov = vpu_coverage(drains, vpu_id, land)
    assert cov[1] == 0.5
    assert cov[2] == 0.0


def test_vpu_coverage_ignores_non_land():
    vpu_id = np.array([[1, 1]], dtype=np.uint8)
    drains = np.array([[1, 1]], dtype=np.uint8)
    land = np.array([[True, False]])  # second cell is ocean
    cov = vpu_coverage(drains, vpu_id, land)
    assert cov[1] == 1.0  # only the 1 land cell counts, and it drains
```

- [ ] **Step 2: Run to verify failure**

Run: `pixi run -e dev pytest tests/test_diagnose_drains_to_dprst.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement the script**

```python
# scripts/diagnose_drains_to_dprst.py
"""Per-VPU drains_to_dprst coverage diagnostic.

Reports, per VPU, the fraction of land cells flagged drains_to_dprst==1. Run
before and after a dprst rebuild: humid open-drainage VPUs (Lower Miss,
S. Atl-Gulf, Great Lakes) should drop sharply while endorheic VPUs (Great Basin,
Rio Grande) stay ~flat. Windowed read to stay within memory at CONUS scale.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window


def vpu_coverage(
    drains: np.ndarray, vpu_id: np.ndarray, land: np.ndarray
) -> dict[int, float]:
    """Fraction of land cells where drains==1, per VPU code (excludes code 0)."""
    out: dict[int, float] = {}
    for code in np.unique(vpu_id):
        if code == 0:
            continue
        sel = (vpu_id == code) & land
        n = int(sel.sum())
        if n == 0:
            continue
        out[int(code)] = float((drains[sel] == 1).sum()) / n
    return out


def _accumulate(drains_path: Path, vpu_path: Path, land_path: Path,
                strip: int = 2048) -> dict[int, float]:
    """Stream the CONUS rasters in row-strips; sum land + draining cells per VPU."""
    num: dict[int, int] = {}  # land cells with drains==1
    den: dict[int, int] = {}  # land cells total
    with rasterio.open(drains_path) as d, rasterio.open(vpu_path) as v, \
         rasterio.open(land_path) as lm:
        h, w = d.height, d.width
        for r0 in range(0, h, strip):
            hh = min(strip, h - r0)
            win = Window(0, r0, w, hh)
            dr = d.read(1, window=win)
            vp = v.read(1, window=win)
            ld = lm.read(1, window=win) == 1
            for code in np.unique(vp):
                if code == 0:
                    continue
                sel = (vp == code) & ld
                den[int(code)] = den.get(int(code), 0) + int(sel.sum())
                num[int(code)] = num.get(int(code), 0) + int((dr[sel] == 1).sum())
    return {c: num[c] / den[c] for c in den if den[c]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--drains", required=True, type=Path)
    ap.add_argument("--vpu-id", required=True, type=Path)
    ap.add_argument("--land", required=True, type=Path)
    args = ap.parse_args()
    cov = _accumulate(args.drains, args.vpu_id, args.land)
    print("VPU  drains_to_dprst coverage (fraction of land)")
    for code in sorted(cov):
        print(f"{code:>3}  {cov[code]:.4f}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run to verify pass**

Run: `pixi run -e dev pytest tests/test_diagnose_drains_to_dprst.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add scripts/diagnose_drains_to_dprst.py tests/test_diagnose_drains_to_dprst.py
git commit -m "feat(diag): repeatable per-VPU drains_to_dprst coverage diagnostic"
```

---

## Task 6: Docs + memory updates

**Files:**
- Modify: `docs/ARCHITECTURE.md` (connectivity/dprst data flow + new staging step)
- Modify: `slurm_batch/RUNME.md` (add the `nhd_flowthrough` staging step before
  the depstor `wbody_connectivity` build; note the rebuild cascade)
- Modify: `slurm_batch/HPC_REFERENCE.md` (per-stage detail + memory/runtime note:
  vector per-VPU job, no CONUS-grid memory)
- Modify: `CLAUDE.md` "Non-obvious conventions" — one bullet on the flow-through
  signal being unioned with WBAREACOMI and the Playa/Ice Mass guardrail.

- [ ] **Step 1: Update ARCHITECTURE.md** — in the connectivity/dprst section,
  document that the on-stream signal is now the union of two COMID sources
  (`connected_waterbody_comids.parquet` from WBAREACOMI **and**
  `flowthrough_waterbody_comids.parquet` from `nhd_flowthrough` topology), and
  that `dprst`/`routing` are unchanged consumers.

- [ ] **Step 2: Update RUNME.md** — add the staging command
  `pixi run --as-is python -m gfv2_params.download.nhd_flowthrough` alongside the
  existing `nhd_flowlines` step, and note that rebuilding requires the dprst
  cascade (`--from wbody_connectivity` or a clean dprst rebuild).

- [ ] **Step 3: Update HPC_REFERENCE.md** — note the new step is a per-VPU vector
  spatial join (sized like `nhd_flowlines`, no 384G concern) and that the
  downstream rebuild is the standard dprst cascade
  (`dprst → perv/routing/drains_*/carea_map`, ~384G ceiling at `dprst`/`waterbody`).

- [ ] **Step 4: Update CLAUDE.md** — add a bullet under "Non-obvious conventions":
  the dprst/on-stream split is driven by WBAREACOMI **unioned** with a geometric
  flow-through test; Playa/Ice Mass are force-dprst; never add a routing cap.

- [ ] **Step 5: Commit docs**

```bash
git add docs/ARCHITECTURE.md slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md CLAUDE.md
git commit -m "docs(depstor): document flow-through reclassification staging + rebuild cascade"
```

- [ ] **Step 6: Update auto-memory** — edit
  `~/.claude/projects/.../memory/drains_to_dprst_overextension.md` from OPEN to
  the implemented design (union of WBAREACOMI + topology flow-through; Playa/Ice
  Mass guardrail; no cap), and add the one-line pointer if the slug/title
  changes. (Memory edit, not a repo commit.)

---

## Post-implementation: CONUS validation (manual, off-plan)

After merge, on the HPC cluster (not in CI):

1. Stage: `pixi run --as-is python -m gfv2_params.download.nhd_flowthrough`.
2. Snapshot the *before* diagnostic on the current `drains_to_dprst.tif`.
3. Rebuild the dprst cascade (`wbody_connectivity → … → carea_map`) at the
   documented memory (`--mem 384G` for dprst/waterbody, `96G` routing).
4. Run the *after* diagnostic. **Acceptance:** humid open-drainage VPUs drop
   sharply; Great Basin / Rio Grande ~flat; the through-flow members of the 30
   largest dprst waterbodies flip to on-stream while genuine playas/potholes do
   not. If residual over-extension remains, escalate to the PlusFlowlineVAA
   node-topology fallback (documented in the design spec, not this plan).

---

## Self-Review Notes

- **Spec coverage:** T1/T2/T3 + force-dprst → Task 1; geometric-vs-PlusFlow
  (geometric chosen, VAA fallback) → Task 1 + post-impl note; architecture
  (second parquet, union in wbody_connectivity) → Tasks 2-4; rebuild cascade +
  closed water-balance → covered by existing onstream→carea_map path (no perv
  change; pinned by Task 4 union test + design's verified note); validation
  (unit fixture + CONUS diagnostic) → Tasks 1 & 5 + post-impl; out-of-scope
  (QA_MA/ratio, open-vs-closed, VAA) → not implemented, recorded. All covered.
- **Type consistency:** `flowthrough_comids(waterbodies, flowlines, areas=None)
  -> set[int]` used identically in Task 1 tests and Task 2 `main()`;
  `load_connected_comids`/`write_connected_comids` reused unchanged;
  `flowthrough_comids_table` named identically across context.py, the script, and
  base_config.yml.
- **No placeholders:** every code step shows full code; commands have expected
  output.
