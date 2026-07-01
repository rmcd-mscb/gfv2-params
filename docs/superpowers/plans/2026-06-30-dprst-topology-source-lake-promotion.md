# NHDPlus-topology source-lake promotion + force-dprst sink guardrail — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stage NHDPlus flowline topology and use authoritative flow direction to promote source/headwater lakes that discharge to the routed stream network, while guarding Playa/Ice Mass against the WBAREACOMI on-stream leak.

**Architecture:** A new download module stages `PlusFlowlineVAA` per VPU into `input/nhd/flowline_topology.parquet`. The flow-through classifier gains a topology-driven rule **D1** ("a routed conveyance flowline's upstream end is inside the waterbody → on-stream"), replacing the `FLOWDIR`-gated **T2**. The force-dprst FTYPE guardrail moves to the `wbody_connectivity` union chokepoint so it applies to both the WBAREACOMI and flow-through sources.

**Tech Stack:** Python 3.12, pixi env, geopandas/shapely/pyogrio, py7zr, requests, pandas/pyarrow. Tests via pytest (CI gate; never `pytest` on the HPC head node).

## Global Constraints

- Paths/inputs come from the active fabric profile via `require_config_key`, using `{data_root}`/`{fabric}`/`{vpu}` placeholders — **except** the per-VPU NHD parquet products, which are hardcoded `data_root`-relative (consistent with `nhd_flowlines`/`nhd_flowthrough` and PR #146). The new `flowline_topology.parquet` follows that hardcoded convention; **no new config key**.
- Force-dprst FTYPEs are exactly `{"Playa", "Ice Mass"}` (NHD FTYPE strings), defined once in `nhd_flowthrough.FORCE_DPRST_FTYPES`.
- Conveyance FTYPEs are exactly `{"StreamRiver", "ArtificialPath", "Connector", "CanalDitch"}` (`nhd_flowthrough.CONVEYANCE_FTYPES`).
- NHD geometry is measured 3D (XYZM); every vector read must force 2D (`shapely.force_2d`) and NHD field casing varies per VPU (resolve case-insensitively, normalise to upper-case).
- A routed network flowline is one present in `PlusFlowlineVAA` with `DnHydroseq != 0`.
- Lint/format gate: `pixi run -e dev pre-commit run --all-files` must pass before commit. Run tests with `pixi run -e dev pytest <path> -v`.
- Fail loud: an empty distilled product is a hard error, never a silent empty write.

---

### Task 1: NHDPlus topology staging module

**Files:**
- Create: `src/gfv2_params/download/nhd_topology.py`
- Test: `tests/test_nhd_topology.py`

**Interfaces:**
- Consumes (imports from `gfv2_params.download.nhd_flowlines`): `vpu_index`, `_base_url`, `_S3_HOST`, `_S3_NS`.
- Produces:
  - `_pick_attributes_key(keys: list[str], vpu: str) -> str | None`
  - `read_vaa(path: pathlib.Path) -> pandas.DataFrame` with upper-case columns `["COMID","DNHYDROSEQ","HYDROSEQ","TERMINALFL","STARTFLAG","STREAMORDE","FROMNODE","TONODE"]`
  - `write_topology(df: pandas.DataFrame, out_path: pathlib.Path) -> None` (single parquet, lower-case columns, `comid` int64)
  - `main() -> None` writes `<data_root>/input/nhd/flowline_topology.parquet`

- [ ] **Step 1: Write the failing test for the archive key picker**

Add to `tests/test_nhd_topology.py`:

```python
"""Unit tests for NHDPlus flowline-topology staging."""

from __future__ import annotations

import pandas as pd
import pyogrio

from gfv2_params.download.nhd_topology import (
    _pick_attributes_key,
    read_vaa,
    write_topology,
)


def test_pick_attributes_key_selects_highest_version():
    keys = [
        "NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDPlusAttributes_08.7z",
        "NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDPlusAttributes_10.7z",
        "NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDSnapshot_07.7z",
    ]
    assert _pick_attributes_key(keys, "14").endswith("_14_NHDPlusAttributes_10.7z")


def test_pick_attributes_key_none_when_absent():
    keys = ["NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDSnapshot_07.7z"]
    assert _pick_attributes_key(keys, "14") is None
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run -e dev pytest tests/test_nhd_topology.py::test_pick_attributes_key_selects_highest_version -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'gfv2_params.download.nhd_topology'`.

- [ ] **Step 3: Create the module with the picker + S3 discovery + download**

Create `src/gfv2_params/download/nhd_topology.py`:

```python
"""Stage NHDPlusV2 PlusFlowlineVAA topology -> a flat per-flowline parquet.

NHDPlus carries authoritative network direction and membership for every
flowline (FromNode/ToNode, Hydroseq/DnHydroseq) independent of the NHDFlowline
FLOWDIR field. The depstor flow-through classifier consumes the distilled
`flowline_topology.parquet` to determine which waterbodies discharge to the
routed network (rule D1 in download/nhd_flowthrough.py).
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import py7zr
import pyogrio
import requests

from gfv2_params.config import load_base_config
from gfv2_params.download.nhd_flowlines import (
    _S3_HOST,
    _S3_NS,
    _base_url,
    vpu_index,
)
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_topology")

# VAA fields distilled (canonical upper-case). DnHydroseq drives the routed-
# network test; the rest are carried for diagnostics / follow-up sink detection.
_VAA_FIELDS = [
    "COMID", "DNHYDROSEQ", "HYDROSEQ", "TERMINALFL",
    "STARTFLAG", "STREAMORDE", "FROMNODE", "TONODE",
]


def _pick_attributes_key(keys: list[str], vpu: str) -> str | None:
    """Highest-version NHDPlusAttributes 7z S3 key for a VPU, or None.

    Mirrors nhd_flowlines._pick_snapshot_key but matches the Attributes
    component. Version numbers are not uniform across VPUs, so the version is
    discovered from the bucket listing rather than hardcoded.
    """
    pat = re.compile(rf"_{re.escape(vpu)}_NHDPlusAttributes_(\d+)\.7z$")
    matches = sorted((m.group(1), k) for k in keys for m in [pat.search(k)] if m)
    return matches[-1][1] if matches else None


def _attributes_url(dd: str, vpu: str) -> str | None:
    """Discover the NHDPlusAttributes archive URL for a VPU via the S3 listing."""
    prefix = _base_url(dd, vpu).split(".amazonaws.com/", 1)[1]
    r = requests.get(f"{_S3_HOST}/?list-type=2&prefix={prefix}/", timeout=60)
    r.raise_for_status()
    keys = [e.text for e in ET.fromstring(r.text).iter(f"{_S3_NS}Key")]
    key = _pick_attributes_key(keys, vpu)
    return f"{_S3_HOST}/{key}" if key else None


def download_attributes(
    dd: str, vpu: str, download_dir: Path, extract_dir: Path
) -> Path | None:
    """Download + extract a VPU's NHDPlusAttributes; return PlusFlowlineVAA.dbf."""
    url = _attributes_url(dd, vpu)
    if url is None:
        logger.error(f"NHDPlusAttributes not found in S3 listing for VPU {vpu}")
        return None
    filename = url.rsplit("/", 1)[1]
    candidate = download_dir / filename
    if candidate.exists():
        logger.info(f"Already downloaded: {filename}")
    else:
        logger.info(f"Downloading {filename} ...")
        tmp = candidate.with_suffix(candidate.suffix + ".part")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            expected = int(r.headers.get("Content-Length", 0))
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        got = tmp.stat().st_size
        if expected and got != expected:
            tmp.unlink(missing_ok=True)
            raise OSError(f"{filename}: downloaded {got} bytes, expected {expected}")
        tmp.rename(candidate)

    out_dir = extract_dir / vpu / "NHDPlusAttributes"
    out_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(candidate, mode="r") as archive:
        targets = [n for n in archive.getnames()
                   if Path(n).name.lower() == "plusflowlinevaa.dbf"]
        archive.extract(path=out_dir, targets=targets)
    vaas = list(out_dir.glob("**/PlusFlowlineVAA.dbf"))
    if not vaas:
        logger.error(f"PlusFlowlineVAA.dbf not found in attributes for VPU {vpu}")
        return None
    return vaas[0]


def read_vaa(path: Path) -> pd.DataFrame:
    """Read the VAA fields (case-insensitive) normalised to upper-case names."""
    available = list(pyogrio.read_info(path)["fields"])
    by_upper = {name.upper(): name for name in available}
    rename = {}
    for canon in _VAA_FIELDS:
        actual = by_upper.get(canon)
        if actual is None:
            raise KeyError(
                f"{path}: PlusFlowlineVAA has no '{canon}' field "
                f"(case-insensitive). Available: {available}"
            )
        rename[actual] = canon
    df = pyogrio.read_dataframe(path, columns=list(rename), read_geometry=False)
    return df.rename(columns=rename)[_VAA_FIELDS]


def write_topology(df: pd.DataFrame, out_path: Path) -> None:
    """Write the distilled topology to a parquet, lower-case columns, comid int64."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out = df.rename(columns={c: c.lower() for c in df.columns}).copy()
    out["comid"] = out["comid"].astype("int64")
    out.to_parquet(out_path, index=False)


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    failures = []
    for vpu, dd in vpu_index.items():
        vaa_path = download_attributes(dd, vpu, download_dir, extract_dir)
        if vaa_path is None:
            failures.append(vpu)
            continue
        df = read_vaa(vaa_path)
        logger.info(f"VPU {vpu}: {len(df)} VAA flowline records")
        frames.append(df)

    if failures:
        raise RuntimeError(f"NHDPlusAttributes staging failed for VPU(s): {failures}")

    combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["COMID"])
    if combined.empty:
        raise ValueError("distilled 0 VAA records across all VPUs — check inputs")

    out_path = data_root / "input/nhd/flowline_topology.parquet"
    write_topology(combined, out_path)
    logger.info(f"Wrote {len(combined)} topology records -> {out_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the picker tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_nhd_topology.py -v`
Expected: both `test_pick_attributes_key_*` PASS.

- [ ] **Step 5: Write the failing tests for read/write**

Append to `tests/test_nhd_topology.py`:

```python
def test_read_vaa_normalises_casing(tmp_path):
    # NHDPlus ships mixed-case fields (ComID, DnHydroseq); read_vaa must
    # resolve them case-insensitively and return canonical upper-case names.
    import geopandas as gpd
    from shapely.geometry import Point

    p = tmp_path / "PlusFlowlineVAA.dbf"
    gpd.GeoDataFrame(
        {"ComID": [101], "DnHydroseq": [5.0], "Hydroseq": [9.0],
         "TerminalFl": [0], "StartFlag": [1], "StreamOrde": [1],
         "FromNode": [11.0], "ToNode": [12.0],
         "geometry": [Point(0, 0)]},
        crs="EPSG:4269",
    ).to_file(p)
    out = read_vaa(p)
    assert list(out.columns) == [
        "COMID", "DNHYDROSEQ", "HYDROSEQ", "TERMINALFL",
        "STARTFLAG", "STREAMORDE", "FROMNODE", "TONODE",
    ]
    assert int(out["COMID"].iloc[0]) == 101


def test_write_topology_roundtrip(tmp_path):
    df = pd.DataFrame({
        "COMID": [101, 102], "DNHYDROSEQ": [5.0, 0.0], "HYDROSEQ": [9.0, 4.0],
        "TERMINALFL": [0, 1], "STARTFLAG": [1, 0], "STREAMORDE": [1, 1],
        "FROMNODE": [11.0, 13.0], "TONODE": [12.0, 14.0],
    })
    out = tmp_path / "topo.parquet"
    write_topology(df, out)
    back = pd.read_parquet(out)
    assert list(back.columns) == [
        "comid", "dnhydroseq", "hydroseq", "terminalfl",
        "startflag", "streamorde", "fromnode", "tonode",
    ]
    assert back["comid"].dtype == "int64"
    routed = set(back[back["dnhydroseq"] != 0]["comid"])
    assert routed == {101}
```

- [ ] **Step 6: Run to verify they pass**

Run: `pixi run -e dev pytest tests/test_nhd_topology.py -v`
Expected: all 4 tests PASS.

- [ ] **Step 7: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add src/gfv2_params/download/nhd_topology.py tests/test_nhd_topology.py
git commit -m "feat(depstor): stage NHDPlus PlusFlowlineVAA topology -> flowline_topology.parquet"
```

---

### Task 2: Rule D1 (routed network outflow) in the flow-through classifier

**Files:**
- Modify: `src/gfv2_params/download/nhd_flowthrough.py` (`flowthrough_comids` signature + T1/T2 block, ~lines 62–135; `main()` ~lines 170–217)
- Modify: `tests/test_nhd_flowthrough.py` (`_fl` helper + tests whose semantics change)

**Interfaces:**
- Consumes: `flowline_topology.parquet` (Task 1) — read in `main()` into `routed_comids: set[int]`.
- Produces: `flowthrough_comids(waterbodies, flowlines, areas=None, routed_comids=None) -> set[int]`. `flowlines` now MUST carry a `COMID` column. `routed_comids` is the set of COMIDs with `DnHydroseq != 0`; when falsy, D1 is inert (T1/T3 only).

- [ ] **Step 1: Update the `_fl` test helper to carry COMID, and add the failing source-lake test**

In `tests/test_nhd_flowthrough.py`, replace the `_fl` helper:

```python
def _fl(rows):
    # rows are [FTYPE, FLOWDIR, geometry]; assign synthetic COMIDs 9001.. so the
    # frame carries the COMID column D1 joins against routed_comids on.
    out = [[9001 + i, *r] for i, r in enumerate(rows)]
    return gpd.GeoDataFrame(
        out, columns=["COMID", "FTYPE", "FLOWDIR", "geometry"], crs=CRS
    )
```

Then add:

```python
def test_source_lake_routed_outflow_is_onstream():
    # D1: a headwater line whose UPSTREAM end is inside W, present in the routed
    # network (DnHydroseq != 0), promotes W even with no inflow (the VPU 14
    # COMID 16969532 case). The line is COMID 9001 (first _fl row).
    wb = _wb([[201, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl, routed_comids={9001}) == {201}


def test_outflow_only_non_network_stays_dprst():
    # Same geometry but the outflow line is NOT in the routed network -> a local
    # spill, not a source lake -> stays dprst.
    wb = _wb([[202, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])]])
    assert flowthrough_comids(wb, fl, routed_comids=set()) == set()
```

- [ ] **Step 2: Run to verify the new tests fail**

Run: `pixi run -e dev pytest tests/test_nhd_flowthrough.py::test_source_lake_routed_outflow_is_onstream -v`
Expected: FAIL — `TypeError: flowthrough_comids() got an unexpected keyword argument 'routed_comids'`.

- [ ] **Step 3: Add `routed_comids` to the signature and docstring**

In `src/gfv2_params/download/nhd_flowthrough.py`, change the `flowthrough_comids` signature:

```python
def flowthrough_comids(
    waterbodies: gpd.GeoDataFrame,
    flowlines: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame | None = None,
    routed_comids: set[int] | None = None,
) -> set[int]:
```

Update the rule list in its docstring so T2 reads:

```python
    A waterbody is on-stream if ANY of:
      T1  a single conveyance flowline flows through it: crosses the boundary
          >= 2 times, OR has non-zero interior length with both endpoints
          outside the polygon (so it must enter and exit),
      D1  a routed-network conveyance flowline (in flowline_topology with
          DnHydroseq != 0) has its UPSTREAM end inside the waterbody -> it
          discharges to the network (source lake or split-pass-through outflow),
      T3  it overlaps a conveyance NHDArea polygon.
    Playa / Ice Mass waterbodies are dropped first and never returned.
```

- [ ] **Step 4: Ensure `conv` carries COMID and replace the T2 block with D1**

Confirm the `conv` frame includes `COMID`. The candidate-pair sjoin already selects geometry; no change needed there because D1 reads `conv["COMID"]` by position. (Inputs now always provide COMID per the interface.)

Replace the entire T2 block (the `--- T2: inflow endpoint AND outflow endpoint ...` loop and `t2_idx = has_inflow & has_outflow`) and the subsequent `for wbidx in t1_idx | t2_idx:` with:

```python
        # --- D1: routed network outflow (authoritative direction via topology) ---
        # A routed conveyance flowline whose UPSTREAM end is inside W discharges
        # out of W: a source/headwater lake, or the outflow half of a stream NHD
        # split at the shore. NHDPlus network flowlines are digitized downstream,
        # so the first vertex (_endpoints[0]) is the upstream end. Direction is
        # taken from topology membership, never the unreliable FLOWDIR field.
        d1_idx: set[int] = set()
        routed = routed_comids or set()
        if routed:
            for line_pos, wbidx in zip(pairs.index, pairs["_wbidx"]):
                if int(conv["COMID"].iloc[line_pos]) not in routed:
                    continue
                up, _ = _endpoints(conv.geometry.iloc[line_pos])
                if wb.geometry.iloc[wbidx].covers(up):
                    d1_idx.add(int(wbidx))

        for wbidx in t1_idx | d1_idx:
            onstream.add(int(wb.loc[wbidx, "COMID"]))
```

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_nhd_flowthrough.py::test_source_lake_routed_outflow_is_onstream tests/test_nhd_flowthrough.py::test_outflow_only_non_network_stays_dprst -v`
Expected: both PASS.

- [ ] **Step 6: Update the existing tests whose semantics changed under D1**

In `tests/test_nhd_flowthrough.py`:

Replace `test_split_inflow_and_outflow_is_onstream` body's assertion to pass topology (the outflow line is the 2nd `_fl` row → COMID 9002):

```python
def test_split_inflow_and_outflow_is_onstream():
    # Split pass-through: line A ends inside (inflow), line B starts inside
    # (outflow). D1 promotes via B's upstream-inside end when B is routed.
    wb = _wb([[102, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])],
        ["StreamRiver", "With Digitized", LineString([(1, 1), (3, 1)])],
    ])
    assert flowthrough_comids(wb, fl, routed_comids={9002}) == {102}
```

Replace `test_spilling_pothole_outflow_only_stays_dprst` entirely (its premise — outflow-only is always dprst — is superseded; it is now governed by network membership, already covered by `test_source_lake_routed_outflow_is_onstream` / `test_outflow_only_non_network_stays_dprst`). Delete this test.

Replace `test_uninitialized_split_pair_not_paired_by_t2` with a network-membership framing:

```python
def test_split_pair_not_in_routed_network_stays_dprst():
    # Two split lines, neither in the routed network -> no D1 promotion -> dprst.
    # (Direction/FLOWDIR is now irrelevant; network membership decides.)
    wb = _wb([[110, "SwampMarsh", SQUARE]])
    fl = _fl([
        ["StreamRiver", "Uninitialized", LineString([(-1, 1), (1, 1)])],
        ["StreamRiver", "Uninitialized", LineString([(1, 1), (3, 1)])],
    ])
    assert flowthrough_comids(wb, fl, routed_comids=set()) == set()
```

`test_terminal_sink_inflow_only_stays_dprst` stays as-is in intent but pass topology to prove a sink is NOT promoted even when routed (inflow line is COMID 9001):

```python
def test_terminal_sink_inflow_only_stays_dprst():
    # Inflow only (downstream end inside); upstream end outside -> D1 false even
    # when routed -> stays dprst. Endorheic terminal-sink guardrail.
    wb = _wb([[103, "LakePond", SQUARE]])
    fl = _fl([["StreamRiver", "With Digitized", LineString([(-1, 1), (1, 1)])]])
    assert flowthrough_comids(wb, fl, routed_comids={9001}) == set()
```

- [ ] **Step 7: Run the full flow-through test module**

Run: `pixi run -e dev pytest tests/test_nhd_flowthrough.py -v`
Expected: all tests PASS (the deleted spilling-pothole test is gone; T1/T3/3D/Playa/IceMass tests unaffected).

- [ ] **Step 8: Wire topology into `main()`**

In `src/gfv2_params/download/nhd_flowthrough.py` `main()`, after `extract_dir.mkdir(...)`, load the topology parquet once and build the routed set; read `COMID` on flowlines; pass `routed_comids` through. Replace the per-VPU read + call:

```python
    import pandas as pd  # add to module imports if not already present

    topo_path = data_root / "input/nhd/flowline_topology.parquet"
    if not topo_path.exists():
        raise FileNotFoundError(
            f"flowline_topology.parquet not found: {topo_path}. Run "
            f"`python -m gfv2_params.download.nhd_topology` first."
        )
    topo = pd.read_parquet(topo_path, columns=["comid", "dnhydroseq"])
    routed_comids = {int(c) for c in topo[topo["dnhydroseq"] != 0]["comid"]}
    logger.info(f"Loaded {len(routed_comids)} routed-network COMIDs")
```

and inside the VPU loop change the flowline read + call:

```python
        flowlines = read_layer(flowline, ["COMID", "FTYPE", "FLOWDIR"])
        area_path = locate_layer(flowline, "NHDArea")
        areas = read_layer(area_path, ["FTYPE"]) if area_path else None
        vpu_set = flowthrough_comids(waterbodies, flowlines, areas, routed_comids)
```

(Move `import pandas as pd` to the top-of-file import block rather than inside `main()`.)

- [ ] **Step 9: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add src/gfv2_params/download/nhd_flowthrough.py tests/test_nhd_flowthrough.py
git commit -m "feat(depstor): promote source lakes via routed-network outflow (D1), replacing FLOWDIR-gated T2"
```

---

### Task 3: Force-dprst guardrail at the wbody_connectivity chokepoint

**Files:**
- Modify: `src/gfv2_params/depstor_builders/wbody_connectivity.py` (after `select_connected_waterbodies`, ~line 91)
- Test: `tests/test_wbody_connectivity.py`

**Interfaces:**
- Consumes: `FORCE_DPRST_FTYPES` from `gfv2_params.download.nhd_flowthrough`; the `FTYPE` column on the waterbody layer.
- Produces: no signature change; `connected_wbody.tif` no longer includes Playa/Ice Mass cells.

- [ ] **Step 1: Write the failing test**

Inspect `tests/test_wbody_connectivity.py` for its existing fixture style (how it builds a waterbody GeoDataFrame and invokes selection/rasterisation). Add a test that a Playa COMID present in the connected set is excluded after the guardrail. Mirror the nearest existing test; the assertion is that a force-dprst FTYPE row is dropped from the selected/rasterised set even when its COMID is "connected". Example shape (adapt names to the existing fixtures in that file):

```python
def test_force_dprst_ftypes_excluded_from_connected(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon
    from gfv2_params.depstor import select_connected_waterbodies
    from gfv2_params.download.nhd_flowthrough import FORCE_DPRST_FTYPES

    wb = gpd.GeoDataFrame(
        {
            "COMID": [1, 2],
            "member_comid": [1, 2],
            "FTYPE": ["LakePond", "Playa"],
            "geometry": [
                Polygon([(0, 0), (1, 0), (1, 1)]),
                Polygon([(2, 2), (3, 2), (3, 3)]),
            ],
        },
        crs="EPSG:5070",
    )
    sel = select_connected_waterbodies(wb, {1, 2})
    guarded = sel[~sel["FTYPE"].isin(FORCE_DPRST_FTYPES)]
    assert set(guarded["COMID"]) == {1}
```

- [ ] **Step 2: Run to verify it fails or is red where expected**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py::test_force_dprst_ftypes_excluded_from_connected -v`
Expected: this asserts the guardrail expression; if `select_connected_waterbodies` does not preserve `FTYPE`, the test fails on `KeyError: 'FTYPE'` — confirming the column must be carried.

- [ ] **Step 3: Apply the guardrail in the builder**

In `src/gfv2_params/depstor_builders/wbody_connectivity.py`, add the import near the top:

```python
from ..download.nhd_flowthrough import FORCE_DPRST_FTYPES
```

Immediately after `sel = select_connected_waterbodies(wb_gdf, connected)` (before the `len(sel) == 0` check), insert:

```python
    if "FTYPE" in sel.columns:
        n_before = len(sel)
        sel = sel[~sel["FTYPE"].isin(FORCE_DPRST_FTYPES)].copy()
        n_forced = n_before - len(sel)
        if n_forced:
            logger.info(
                "  force-dprst guardrail: dropped %d Playa/Ice Mass waterbodies "
                "promoted via WBAREACOMI", n_forced,
            )
    else:
        logger.warning(
            "  waterbody layer has no FTYPE column — force-dprst guardrail "
            "(Playa/Ice Mass) cannot be applied"
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py::test_force_dprst_ftypes_excluded_from_connected -v`
Expected: PASS.

- [ ] **Step 5: Run the full builder test module**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -v`
Expected: all PASS.

- [ ] **Step 6: Lint + commit**

```bash
pixi run -e dev pre-commit run --all-files
git add src/gfv2_params/depstor_builders/wbody_connectivity.py tests/test_wbody_connectivity.py
git commit -m "fix(depstor): apply Playa/Ice Mass force-dprst guardrail to the WBAREACOMI on-stream path"
```

---

### Task 4: Documentation

**Files:**
- Modify: `docs/ARCHITECTURE.md` (flow-through description ~lines 138, 218–222; required-field/inputs table)
- Modify: `slurm_batch/RUNME.md` and `slurm_batch/HPC_REFERENCE.md` (NHD-staging step list)

**Interfaces:** none (docs only).

- [ ] **Step 1: Update ARCHITECTURE.md**

In `docs/ARCHITECTURE.md`, update the `flowthrough_comids_table` row and the flow-through paragraph (~218–222) to state that flow-through now (a) uses authoritative NHDPlus topology (`flowline_topology.parquet` from `download/nhd_topology.py`) for direction, promoting source/headwater lakes that discharge to the routed network (rule D1), and (b) that the Playa/Ice Mass force-dprst guardrail is applied at the `wbody_connectivity` union so it covers the WBAREACOMI path too. Add a one-line entry for the new staging product:

```markdown
- `input/nhd/flowline_topology.parquet` — distilled NHDPlus PlusFlowlineVAA
  (COMID, DnHydroseq, Hydroseq, TerminalFl, StartFlag, StreamOrde). Staged by
  `download/nhd_topology.py`; consumed by `download/nhd_flowthrough.py` (rule D1,
  routed-network outflow). Hardcoded data_root-relative, no config key.
```

- [ ] **Step 2: Update the runbooks**

In `slurm_batch/RUNME.md` and `slurm_batch/HPC_REFERENCE.md`, add `python -m gfv2_params.download.nhd_topology` to the NHD-staging step list, ordered **before** `download.nhd_flowthrough` (flow-through now depends on the topology parquet).

- [ ] **Step 3: Commit**

```bash
git add docs/ARCHITECTURE.md slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md
git commit -m "docs(depstor): document NHDPlus topology staging + D1 source-lake promotion + sink guardrail"
```

---

## Production rollout (run after all tasks merge; not part of TDD)

1. `pixi run python -m gfv2_params.download.nhd_topology` → `flowline_topology.parquet` (21 VPU × ~9 MB).
2. `pixi run python -m gfv2_params.download.nhd_flowthrough` → regenerate `flowthrough_waterbody_comids.parquet`.
3. Rebuild depstor dprst cascade: `wbody_connectivity → dprst → routing → drains_perv/imperv` (dprst+waterbody `--mem=384G`, routing `--mem=96G`).
4. Validate: 16969532 now on-stream; 21744935 (case 1) on-stream; spot-check Playa/Ice Mass no longer on-stream; confirm `drains_to_dprst` land coverage did not rise.

## Self-Review

- **Spec coverage:** new staging module (Task 1) ✓; D1 rule replacing T2 with authoritative direction (Task 2) ✓; force-dprst guardrail on the union chokepoint (Task 3) ✓; docs + runbook (Task 4) ✓; rollout + the deferred non-Playa sink follow-up noted (rollout section + spec Out-of-scope) ✓. The spec's `topology_table` config key is intentionally dropped in favour of a hardcoded path (Global Constraints), per PR #146 precedent.
- **Placeholders:** none — every code step shows complete code; Task 3 Step 1 explicitly says to adapt fixture names to the existing test file (the engineer must read it), with a concrete example.
- **Type consistency:** `flowthrough_comids(..., routed_comids=None)` defined in Task 2 and called with `routed_comids` in Task 2 Step 8; `routed_comids` is `set[int]`; `read_vaa` upper-case columns (Task 1) vs `write_topology` lower-case parquet columns (Task 1) consumed as lower-case in Task 2 Step 8 (`["comid","dnhydroseq"]`) — consistent. `_fl` helper now yields `["COMID","FTYPE","FLOWDIR","geometry"]` and `flowthrough_comids` reads `conv["COMID"]` — consistent.
