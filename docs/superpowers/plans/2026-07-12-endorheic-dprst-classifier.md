# Endorheic Depression-Storage Classifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop classifying terminal endorheic lakes (Great Salt Lake, Mono, Salton Sea, …) as on-stream, by adding a hydrology-first demotion signal — *a waterbody is depression storage iff its water's terminus lies inside itself* — plus a complementary WBD closed-basin signal and NHDPlus `BurnAddWaterbody` as new depression area.

**Architecture:** Two new NHDPlus/WBD staging modules; one new pure-compute module (`endorheic.py`) holding both signals; one new depstor builder step (`endorheic`) that emits a COMID parquet; a three-line strict subtraction in `wbody_connectivity`; and a `BurnAddWaterbody` union in `waterbody`. `routing` is unchanged — giving a sink a polygon is what makes it a pour-point.

**Tech Stack:** Python 3.12, pixi, geopandas/pyogrio, rasterio, numpy, numba (via `d8_routing`), py7zr + requests (NHDPlus S3), pytest.

**Spec:** [`docs/superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md`](../specs/2026-07-12-endorheic-dprst-classifier-design.md) (commit `c976f34`).

## Global Constraints

- **Environment:** every command runs under pixi. Tests: `pixi run -e dev pytest tests/... -v`. Never run pytest on the HPC head node in bulk — CI is the gate; single-test runs and `py_compile` are fine.
- **Never widen the on-stream set.** The new signals may only *subtract* from the on-stream COMID set. Any change that could add a COMID to `connected` is a bug.
- **Predicate discipline:** containment tests use **majority-area** (`> 0.5`), never `intersects` and never `within`. A geometry touching another's boundary with zero interior overlap returns `True` from `intersects`; this artifact produced a false result during design.
- **Fail loud, never default.** An unrecognised `PurpCode`, a missing `COMID`/`FTYPE` column, or an empty staged table must raise — never silently fall back.
- **ESRI D8 codes:** `1=E 2=SE 4=S 8=SW 16=W 32=NW 64=N 128=NE`. `0` and `255` (nodata) are **termini** (`d8_routing.py:29`).
- **NHDPlus VPU codes** come from `gfv2_params.download.nhd_flowlines.vpu_index` — 21 archive codes (`03N`/`03S`/`03W`, `10L`/`10U`, …). Do **not** substitute the 18 consolidated raster-tile list.
- **New profile keys are optional.** Absent ⇒ today's behaviour, so `oregon`/`tjc`/`gfv2_vpu01` are unaffected until opted in.
- Every code change needs a docs check (Task 8). Atomic commits.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/gfv2_params/download/nhd_burn_components.py` | **new** — stage `Sink.shp` + `BurnAddWaterbody.shp` from `NHDPlusBurnComponents` |
| `src/gfv2_params/download/wbd_huc12.py` | **new** — stage the full WBD from `WBDSnapshot` |
| `src/gfv2_params/endorheic.py` | **new** — pure compute: Signal A (terminus-inside-itself), Signal B (closed basin), combiner, parquet IO |
| `src/gfv2_params/depstor_builders/endorheic.py` | **new** — builder step wiring `endorheic.py` to `BuildContext` |
| `src/gfv2_params/depstor_builders/__init__.py` | register `endorheic` in `BUILDERS` + `STEP_ORDER` |
| `src/gfv2_params/depstor_builders/context.py` | 3 new optional fields |
| `src/gfv2_params/depstor_builders/wbody_connectivity.py` | strict `connected -= endorheic` |
| `src/gfv2_params/depstor_builders/waterbody.py` | union in `BurnAddWaterbody` |
| `scripts/build_depstor_rasters.py` | pass the 3 new config keys into `BuildContext` |
| `configs/base_config.yml` | 3 new optional profile keys on `gfv2` + `gfv2_dev` |
| `configs/depstor/depstor_rasters.yml` | register the `endorheic` step |
| `tests/test_endorheic.py` | **new** — both signals, synthetic |
| `tests/test_nhd_burn_components.py` | **new** — PurpCode mapping + fail-loud |
| `tests/test_waterbody.py` | extend — BurnAdd union + negative-COMID assertion |

---

### Task 1: Stage `Sink.shp` + `BurnAddWaterbody.shp` from NHDPlusBurnComponents

**Files:**
- Create: `src/gfv2_params/download/nhd_burn_components.py`
- Test: `tests/test_nhd_burn_components.py`

**Interfaces:**
- Consumes: `nhd_flowlines.vpu_index`, `nhd_flowlines._base_url`, `nhd_flowlines._S3_HOST`, `nhd_flowlines._S3_NS`
- Produces:
  - `PURPCODE_TO_FTYPE: dict[int, str]` = `{4: "Playa", 8: "LakePond"}`
  - `pick_component_key(keys: list[str], vpu: str) -> str | None`
  - `burn_add_to_waterbody_frame(gdf: GeoDataFrame) -> GeoDataFrame` → columns `GNIS_ID, GNIS_NAME, COMID, FTYPE, member_comid, area_sqkm, geometry`
  - `download_burn_components(dd, vpu, download_dir, extract_dir) -> tuple[Path | None, Path | None]` (sink_shp, burnadd_shp)
  - Writes `{data_root}/input/nhd/sink_points.parquet` and `{data_root}/input/nhd/burn_add_waterbodies.parquet`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_nhd_burn_components.py`:

```python
"""Unit tests for the NHDPlusBurnComponents staging module (synthetic frames)."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from gfv2_params.download.nhd_burn_components import (
    PURPCODE_TO_FTYPE,
    burn_add_to_waterbody_frame,
    pick_component_key,
)

CRS = "EPSG:4269"
SQ = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def _baw(rows):
    # BurnAddWaterbody.shp columns as shipped by NHDPlus
    return gpd.GeoDataFrame(
        rows, columns=["PolyID", "PurpCode", "PurpDesc", "OnOffNet", "FCode", "geometry"],
        crs=CRS,
    )


def test_pick_component_key_takes_the_highest_version():
    keys = [
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_NHDPlusBurnComponents_01.7z",
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_NHDPlusBurnComponents_02.7z",
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_NHDSnapshot_08.7z",
    ]
    key = pick_component_key(keys, "16")
    assert key.endswith("NHDPlusBurnComponents_02.7z")


def test_pick_component_key_returns_none_when_absent():
    assert pick_component_key(["some/other/file.7z"], "16") is None


def test_burn_add_maps_purpcode_to_ftype():
    g = _baw([[-367111, 4, "BurnAddWaterbody Playa", 1, 36100, SQ],
              [-367116, 8, "BurnAddWaterbody closed lake", 1, 39001, SQ]])
    out = burn_add_to_waterbody_frame(g)
    assert list(out.FTYPE) == ["Playa", "LakePond"]
    # PolyID is the (negative) COMID, and member_comid mirrors it so
    # depstor.select_connected_waterbodies can join without a KeyError.
    assert list(out.COMID) == [-367111, -367116]
    assert list(out.member_comid) == [-367111, -367116]
    assert set(out.columns) >= {
        "GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm", "geometry",
    }


def test_burn_add_comids_are_all_negative():
    # The negative PolyID is what makes BurnAdd waterbodies structurally
    # incapable of matching a WBAREACOMI / flow-through COMID.
    g = _baw([[-367111, 4, "BurnAddWaterbody Playa", 1, 36100, SQ]])
    assert (burn_add_to_waterbody_frame(g).COMID < 0).all()


def test_burn_add_fails_loud_on_unknown_purpcode():
    # An unrecognised PurpCode must NOT default to a FTYPE: FTYPE drives
    # NEVER_ONSTREAM_FTYPES, so a mis-defaulted Playa becomes promotable on-stream.
    g = _baw([[-1, 99, "Something New", 1, 12345, SQ]])
    with pytest.raises(ValueError, match="unrecognised PurpCode"):
        burn_add_to_waterbody_frame(g)


def test_purpcode_table_is_exactly_playa_and_lakepond():
    assert PURPCODE_TO_FTYPE == {4: "Playa", 8: "LakePond"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_nhd_burn_components.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'gfv2_params.download.nhd_burn_components'`

- [ ] **Step 3: Implement the module**

Create `src/gfv2_params/download/nhd_burn_components.py`:

```python
"""Stage NHDPlusV2 `NHDPlusBurnComponents`: Sink.shp + BurnAddWaterbody.shp.

Two products, one archive per VPU:

* `sink_points.parquet` — NHDPlus's authoritative sink list. Kept for PROVENANCE
  (`PURPCODE`/`PURPDESC`) and for the BurnAddWaterbody linkage
  (`SOURCEFC`/`FEATUREID`). It is **not** a classifier signal: the endorheic
  classifier reads the FDR grid the router reads (see gfv2_params.endorheic).

  The pre-made `input/nhd/NHD_sink_points.gpkg` is a STRICT SUBSET of this — 537
  sinks in VPU 16 against 3,222 here — because it omits `PURPCODE 1`
  ("BurnLineEvent network end") entirely, which is precisely the class NHDPlus uses
  to mark where a burned flowline's network terminates. It therefore contains 0 sinks
  inside Great Salt Lake, where NHDPlus has 29. Do not use it.

* `burn_add_waterbodies.parquet` — waterbody polygons NHDPlus added for the burn that
  are absent from NHDWaterbody. These are genuinely new depression AREA (VPU 16 alone:
  23 polygons, 374.5 km², largest a 136.8 km² playa; 0 of 23 overlap an existing
  waterbody). `waterbody.py` unions them into the waterbody layer, after which they
  flow through waterbody -> dprst -> routing untouched and become dprst pour-points.
"""

from __future__ import annotations

import re
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

import geopandas as gpd
import pandas as pd
import py7zr
import requests

from gfv2_params.config import load_base_config
from gfv2_params.download.nhd_flowlines import (
    _S3_HOST,
    _S3_NS,
    _base_url,
    vpu_index,
)
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_burn_components")

# BurnAddWaterbody PurpCode -> NHD FTYPE. Deliberately exhaustive: an unrecognised
# code raises rather than defaulting, because FTYPE drives NEVER_ONSTREAM_FTYPES
# (a mis-defaulted Playa would become promotable on-stream).
PURPCODE_TO_FTYPE = {4: "Playa", 8: "LakePond"}


def pick_component_key(keys: list[str], vpu: str) -> str | None:
    """Highest-version NHDPlusBurnComponents 7z S3 key for a VPU, or None.

    Mirrors nhd_flowlines._pick_snapshot_key. Version numbers are not uniform across
    VPUs, so the version is discovered from the bucket listing, not hardcoded.
    """
    pat = re.compile(rf"_{re.escape(vpu)}_NHDPlusBurnComponents_(\d+)\.7z$")
    matches = sorted((m.group(1), k) for k in keys for m in [pat.search(k)] if m)
    return matches[-1][1] if matches else None


def _component_url(dd: str, vpu: str) -> str | None:
    prefix = _base_url(dd, vpu).split(".amazonaws.com/", 1)[1]
    r = requests.get(f"{_S3_HOST}/?list-type=2&prefix={prefix}/", timeout=60)
    r.raise_for_status()
    keys = [e.text for e in ET.fromstring(r.text).iter(f"{_S3_NS}Key")]
    key = pick_component_key(keys, vpu)
    return f"{_S3_HOST}/{key}" if key else None


def download_burn_components(
    dd: str, vpu: str, download_dir: Path, extract_dir: Path
) -> tuple[Path | None, Path | None]:
    """Download + extract a VPU's NHDPlusBurnComponents.

    Returns (Sink.shp, BurnAddWaterbody.shp); either may be None if the archive
    genuinely lacks it (VPU 16 has both; some VPUs have no BurnAddWaterbody).
    """
    url = _component_url(dd, vpu)
    if url is None:
        logger.error(f"NHDPlusBurnComponents not found in S3 listing for VPU {vpu}")
        return None, None
    filename = url.rsplit("/", 1)[1]
    archive = download_dir / filename
    if archive.exists():
        logger.info(f"Already downloaded: {filename}")
    else:
        logger.info(f"Downloading {filename} ...")
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(archive, "wb") as fh:
                shutil.copyfileobj(r.raw, fh)

    target = extract_dir / f"burncomponents_{vpu}"
    if not target.exists():
        with py7zr.SevenZipFile(archive, mode="r") as a:
            a.extractall(path=target)

    sink = next(iter(target.rglob("Sink.shp")), None)
    burn = next(iter(target.rglob("BurnAddWaterbody.shp")), None)
    return sink, burn


def burn_add_to_waterbody_frame(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reshape BurnAddWaterbody.shp into the waterbody-layer schema.

    `PolyID` (always negative) becomes both COMID and member_comid, so
    depstor.select_connected_waterbodies can join without a KeyError — and so these
    polygons can never match a WBAREACOMI / flow-through COMID (all positive). That
    makes them structurally incapable of on-stream promotion, which is correct:
    NHDPlus flagged every one of them as a sink.
    """
    unknown = sorted(set(gdf["PurpCode"].astype(int)) - set(PURPCODE_TO_FTYPE))
    if unknown:
        raise ValueError(
            f"BurnAddWaterbody carries unrecognised PurpCode(s) {unknown}; refusing "
            f"to guess a FTYPE. FTYPE drives NEVER_ONSTREAM_FTYPES, so a wrong "
            f"default would let a Playa be promoted on-stream. Known codes: "
            f"{sorted(PURPCODE_TO_FTYPE)} — extend PURPCODE_TO_FTYPE deliberately."
        )
    out = gpd.GeoDataFrame(
        {
            "GNIS_ID": pd.Series([None] * len(gdf), dtype="object"),
            "GNIS_NAME": pd.Series([None] * len(gdf), dtype="object"),
            "COMID": gdf["PolyID"].astype("int64").to_numpy(),
            "FTYPE": gdf["PurpCode"].astype(int).map(PURPCODE_TO_FTYPE).to_numpy(),
            "member_comid": gdf["PolyID"].astype("int64").to_numpy(),
            "area_sqkm": (gdf.to_crs(5070).geometry.area / 1e6).to_numpy(),
        },
        geometry=gdf.geometry.to_numpy(),
        crs=gdf.crs,
    )
    if (out["COMID"] >= 0).any():
        raise ValueError(
            "BurnAddWaterbody PolyID is expected to be negative (that is what makes "
            "these polygons unable to match a positive WBAREACOMI/flow-through COMID). "
            "A non-negative PolyID would silently become on-stream-promotable."
        )
    return out


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    out_dir = data_root / "input/nhd"
    for d in (download_dir, extract_dir, out_dir):
        d.mkdir(parents=True, exist_ok=True)

    sinks, burns, failures = [], [], []
    for vpu, dd in vpu_index.items():
        sink_shp, burn_shp = download_burn_components(dd, vpu, download_dir, extract_dir)
        if sink_shp is None:
            failures.append(vpu)
            continue
        s = gpd.read_file(sink_shp)
        s["vpu"] = vpu
        logger.info(f"VPU {vpu}: {len(s)} sinks")
        sinks.append(s)
        if burn_shp is None:
            logger.info(f"VPU {vpu}: no BurnAddWaterbody (this is normal for some VPUs)")
            continue
        b = gpd.read_file(burn_shp)
        if len(b) == 0:
            continue
        logger.info(f"VPU {vpu}: {len(b)} BurnAddWaterbody polygons")
        burns.append(burn_add_to_waterbody_frame(b).to_crs(5070))

    if failures:
        raise RuntimeError(
            f"NHDPlusBurnComponents staging failed for VPU(s): {failures}. A silently "
            f"dropped VPU under-stages the sink/BurnAdd set there — fix, do not skip."
        )

    sink_out = out_dir / "sink_points.parquet"
    gpd.GeoDataFrame(pd.concat(sinks, ignore_index=True), crs=sinks[0].crs).to_crs(
        5070
    ).to_parquet(sink_out)
    logger.info(f"Wrote {sink_out} ({sum(len(s) for s in sinks)} sinks)")

    if not burns:
        raise ValueError(
            "0 BurnAddWaterbody polygons staged across all VPUs — that would add no "
            "depression area at all. Expected >= 23 in VPU 16 alone; investigate."
        )
    burn_out = out_dir / "burn_add_waterbodies.parquet"
    combined = gpd.GeoDataFrame(pd.concat(burns, ignore_index=True), crs=5070)
    combined.to_parquet(burn_out)
    logger.info(
        f"Wrote {burn_out} ({len(combined)} polygons, "
        f"{combined.geometry.area.sum() / 1e6:,.1f} km2)"
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_nhd_burn_components.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/download/nhd_burn_components.py tests/test_nhd_burn_components.py
git commit -m "feat(download): stage NHDPlus Sink.shp + BurnAddWaterbody.shp

BurnAddWaterbody supplies real depression AREA absent from NHDWaterbody (VPU 16:
23 polygons, 374.5 km2, 0 overlapping an existing waterbody). PolyID is negative,
so these can never match a WBAREACOMI/flow-through COMID -- asserted, not assumed.
An unrecognised PurpCode fails loud rather than defaulting to a FTYPE.

Sink.shp is staged for provenance + the BurnAdd linkage only. The pre-made
NHD_sink_points.gpkg is a strict subset (537 vs 3,222 in VPU 16) that omits
PURPCODE 1 entirely -- 0 sinks inside Great Salt Lake vs 29 in the source."
```

---

### Task 2: Stage the full WBD from NHDPlus `WBDSnapshot`

**Files:**
- Create: `src/gfv2_params/download/wbd_huc12.py`
- Test: `tests/test_wbd_huc12.py`

**Interfaces:**
- Produces:
  - `pick_wbd_key(keys: list[str], vpu: str) -> str | None`
  - `closed_basin_frame(gdf: GeoDataFrame) -> GeoDataFrame` → rows where `HU_12_TYPE == "C"`, columns `HUC_12, HU_12_TYPE, geometry`
  - Writes `{data_root}/input/wbd/wbd_huc12.parquet`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_wbd_huc12.py`:

```python
"""Unit tests for the WBD HUC12 staging module (synthetic frames)."""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import Polygon

from gfv2_params.download.wbd_huc12 import closed_basin_frame, pick_wbd_key

CRS = "EPSG:4269"
SQ = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def _wbd(rows):
    return gpd.GeoDataFrame(
        rows, columns=["HUC_12", "HU_12_TYPE", "NCONTRB_A", "geometry"], crs=CRS
    )


def test_pick_wbd_key_takes_the_highest_version():
    keys = [
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_WBDSnapshot_02.7z",
        "NHDPlusV21/Data/NHDPlusGB/NHDPlusV21_GB_16_WBDSnapshot_03.7z",
    ]
    assert pick_wbd_key(keys, "16").endswith("WBDSnapshot_03.7z")


def test_pick_wbd_key_returns_none_when_absent():
    assert pick_wbd_key(["nope.7z"], "16") is None


def test_closed_basin_frame_keeps_only_type_C():
    # We filter HU_12_TYPE == 'C' ourselves rather than trusting any upstream
    # selection: the pre-made closed_huc12.gpkg carried 219 non-C rows, 212 of them
    # fully CONTRIBUTING HUC12s (NCONTRB_A == 0) that merely drain into closed ones.
    # Demoting lakes on their internal stream network would be wrong.
    g = _wbd([
        ["160203100200", "C", 100.0, SQ],   # closed
        ["160203100201", "S", 0.0, SQ],     # standard -- drains onward
        ["160203100202", "F", 0.0, SQ],     # frontal -- drains to the coast
        ["160203100203", "W", 0.0, SQ],     # water
    ])
    out = closed_basin_frame(g)
    assert list(out.HUC_12) == ["160203100200"]
    assert set(out.columns) == {"HUC_12", "HU_12_TYPE", "geometry"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_wbd_huc12.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'gfv2_params.download.wbd_huc12'`

- [ ] **Step 3: Implement the module**

Create `src/gfv2_params/download/wbd_huc12.py`:

```python
"""Stage the full WBD HUC12 layer from NHDPlusV2's per-VPU `WBDSnapshot`.

`HU_12_TYPE == 'C'` marks a closed basin (a HUC12 that contributes flow to nothing).
It is Signal B of the endorheic classifier (see gfv2_params.endorheic).

Why the FULL WBD and not `input/nhd/closed_huc12.gpkg`: that pre-made extract has
23 type-C HUC12s in the Great Basin against 141 here, and resolves only 1 of the 10
classic terminal lakes (the full WBD resolves 5 — it adds Pyramid, Lake Abert, Walker
and Summer, each of which the extract reports at frac_in = 0.000).
"""

from __future__ import annotations

import re
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

import geopandas as gpd
import pandas as pd
import py7zr
import requests

from gfv2_params.config import load_base_config
from gfv2_params.download.nhd_flowlines import (
    _S3_HOST,
    _S3_NS,
    _base_url,
    vpu_index,
)
from gfv2_params.log import configure_logging

logger = configure_logging("download_wbd_huc12")

_KEEP = ["HUC_12", "HU_12_TYPE", "geometry"]


def pick_wbd_key(keys: list[str], vpu: str) -> str | None:
    """Highest-version WBDSnapshot 7z S3 key for a VPU, or None."""
    pat = re.compile(rf"_{re.escape(vpu)}_WBDSnapshot_(\d+)\.7z$")
    matches = sorted((m.group(1), k) for k in keys for m in [pat.search(k)] if m)
    return matches[-1][1] if matches else None


def _wbd_url(dd: str, vpu: str) -> str | None:
    prefix = _base_url(dd, vpu).split(".amazonaws.com/", 1)[1]
    r = requests.get(f"{_S3_HOST}/?list-type=2&prefix={prefix}/", timeout=60)
    r.raise_for_status()
    keys = [e.text for e in ET.fromstring(r.text).iter(f"{_S3_NS}Key")]
    key = pick_wbd_key(keys, vpu)
    return f"{_S3_HOST}/{key}" if key else None


def download_wbd(dd: str, vpu: str, download_dir: Path, extract_dir: Path) -> Path | None:
    """Download + extract a VPU's WBDSnapshot; return WBD_Subwatershed.shp."""
    url = _wbd_url(dd, vpu)
    if url is None:
        logger.error(f"WBDSnapshot not found in S3 listing for VPU {vpu}")
        return None
    filename = url.rsplit("/", 1)[1]
    archive = download_dir / filename
    if archive.exists():
        logger.info(f"Already downloaded: {filename}")
    else:
        logger.info(f"Downloading {filename} ...")
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(archive, "wb") as fh:
                shutil.copyfileobj(r.raw, fh)

    target = extract_dir / f"wbd_{vpu}"
    if not target.exists():
        with py7zr.SevenZipFile(archive, mode="r") as a:
            a.extractall(path=target)
    return next(
        (p for p in target.rglob("*.shp") if "subwatershed" in p.name.lower()), None
    )


def closed_basin_frame(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Rows whose HU_12_TYPE is 'C' (closed basin), trimmed to the keep columns.

    We apply the type filter ourselves rather than trusting an upstream selection.
    """
    if "HU_12_TYPE" not in gdf.columns:
        raise KeyError(
            "WBD layer has no HU_12_TYPE column — cannot identify closed basins; "
            "refusing to stage a table that would silently demote nothing."
        )
    return gdf[gdf["HU_12_TYPE"] == "C"][_KEEP].copy()


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    out_dir = data_root / "input/wbd"
    for d in (download_dir, extract_dir, out_dir):
        d.mkdir(parents=True, exist_ok=True)

    frames, failures = [], []
    for vpu, dd in vpu_index.items():
        shp = download_wbd(dd, vpu, download_dir, extract_dir)
        if shp is None:
            failures.append(vpu)
            continue
        g = gpd.read_file(shp).to_crs(5070)
        closed = closed_basin_frame(g)
        logger.info(f"VPU {vpu}: {len(g)} HUC12s, {len(closed)} type-C (closed)")
        frames.append(closed)

    if failures:
        raise RuntimeError(f"WBDSnapshot staging failed for VPU(s): {failures}")

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=5070)
    combined = combined.drop_duplicates(subset="HUC_12")
    if combined.empty:
        raise ValueError(
            "0 closed (type-C) HUC12s staged across all VPUs → Signal B would demote "
            "nothing. Expected >= 141 in VPU 16 alone; investigate."
        )
    out = out_dir / "wbd_huc12.parquet"
    combined.to_parquet(out)
    logger.info(f"Wrote {out} ({len(combined)} closed HUC12s)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_wbd_huc12.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/download/wbd_huc12.py tests/test_wbd_huc12.py
git commit -m "feat(download): stage the full WBD HUC12 layer from NHDPlus WBDSnapshot

HU_12_TYPE == 'C' (closed basin) is Signal B of the endorheic classifier. The
pre-made closed_huc12.gpkg is an incomplete extract -- 23 type-C HUC12s in the
Great Basin vs 141 here, and it resolves only 1 of the 10 classic terminal lakes
(the full WBD resolves 5). We filter type C ourselves rather than trust it."
```

---

### Task 3: Signal B — closed-basin COMIDs (pure)

**Files:**
- Create: `src/gfv2_params/endorheic.py`
- Test: `tests/test_endorheic.py`

**Interfaces:**
- Produces: `closed_basin_comids(wb_gdf: GeoDataFrame, closed_gdf: GeoDataFrame, min_frac: float = 0.5) -> set[int]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_endorheic.py`:

```python
"""Unit tests for the endorheic dprst classifier (synthetic geometry + FDR arrays)."""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import Polygon

from gfv2_params.endorheic import closed_basin_comids

CRS = "EPSG:5070"


def _box(x0, y0, x1, y1):
    return Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])


def _wb(rows):
    return gpd.GeoDataFrame(rows, columns=["COMID", "geometry"], crs=CRS)


def _closed(polys):
    return gpd.GeoDataFrame({"HUC_12": [str(i) for i in range(len(polys))]},
                            geometry=polys, crs=CRS)


def test_closed_basin_keeps_a_waterbody_fully_inside():
    wb = _wb([[101, _box(1, 1, 2, 2)]])
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == {101}


def test_closed_basin_rejects_a_boundary_graze():
    # THE regression that matters. A polygon touching the closed-HUC12 boundary with
    # ZERO interior overlap returns True from `intersects` -- this artifact produced a
    # false "Cedar Lake routes out of its closed basin" reading during design, and in
    # the real data Eagle Lake / Middle Alkali Lake graze at frac_in = 0.000.
    # Majority-area must reject them; `intersects` must never be substituted back in.
    wb = _wb([[102, _box(10, 0, 12, 2)]])          # shares only the x=10 edge
    closed = _closed([_box(0, 0, 10, 10)])
    assert wb.geometry.iloc[0].intersects(closed.geometry.iloc[0])  # the trap
    assert closed_basin_comids(wb, closed) == set()


def test_closed_basin_keeps_a_majority_overlap():
    # Great Salt Lake sits at frac_in = 0.989 -- it spills ~1% into a neighbouring
    # HUC12, so a strict `within` predicate would drop it. Majority-area keeps it.
    wb = _wb([[103, _box(8, 0, 11, 2)]])           # 2/3 inside the closed box
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == {103}


def test_closed_basin_rejects_a_minority_overlap():
    wb = _wb([[104, _box(9, 0, 12, 2)]])           # 1/3 inside
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == set()


def test_closed_basin_dissolves_adjacent_huc12s():
    # A lake straddling two ADJACENT closed HUC12s is fully inside the closed system
    # but majority-inside neither polygon on its own. Dissolve first, then measure.
    wb = _wb([[105, _box(4, 1, 6, 2)]])            # half in each of two closed boxes
    closed = _closed([_box(0, 0, 5, 10), _box(5, 0, 10, 10)])
    assert closed_basin_comids(wb, closed) == {105}


def test_closed_basin_empty_closed_set_demotes_nothing():
    wb = _wb([[106, _box(1, 1, 2, 2)]])
    empty = gpd.GeoDataFrame({"HUC_12": []}, geometry=[], crs=CRS)
    assert closed_basin_comids(wb, empty) == set()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_endorheic.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'gfv2_params.endorheic'`

- [ ] **Step 3: Implement Signal B**

Create `src/gfv2_params/endorheic.py`:

```python
"""Endorheic (closed-basin) depression-storage classifier.

The existing on-stream tests (WBAREACOMI, flow-through) are LOCAL: "does a Network
flowline enter and leave this waterbody?" NHD draws Network artificial paths between
the arms of a terminal lake, so the lake reads as through-flowing and is promoted
on-stream. No local test can see that a whole basin is endorheic — which is why the
Great Salt Lake is currently classified on-stream.

Two signals fix that, both of which can only SUBTRACT from the on-stream set:

Signal A (primary) — terminus-inside-itself, on the FDR grid.
    A waterbody is depression storage iff its water's terminus lies INSIDE ITSELF.
    GSL's water ends in GSL. A pond upstream in the same closed basin ends in GSL,
    not in the pond, so it stays on-stream. Lewis and Clark Lake's water ends in the
    Gulf of Mexico. (This is why the rule is terminus-INSIDE-ITSELF and not merely
    terminates-at-a-sink: the latter demotes every on-stream reservoir in a closed
    basin.)

Signal B (complement) — majority-inside a WBD type-C (closed) HUC12. Earns its place
    because Walker Lake contains no FDR terminal cell, so Signal A misses it.

Signal A reads the same grid — and runs the same kernel — that `routing` reads, so
the classifier and the router agree BY CONSTRUCTION: d8_routing treats code 0 as a
terminus, so a waterbody the router dead-ends in IS a depression.
"""

from __future__ import annotations

import geopandas as gpd

# Share of a waterbody's area that must lie inside the closed-basin union (Signal B),
# and share of its cells that must reach its own terminus (Signal A). NOT a tuned
# knob: frac_own is bimodal (204 of 239 CONUS candidates at >= 0.95, only 3 in
# 0.45-0.55) and the demotion set moves ~3% across thresholds 0.3-0.7.
MIN_FRAC = 0.5


def closed_basin_comids(
    wb_gdf: gpd.GeoDataFrame,
    closed_gdf: gpd.GeoDataFrame,
    min_frac: float = MIN_FRAC,
) -> set[int]:
    """COMIDs of waterbodies majority-inside the DISSOLVED union of closed HUC12s.

    Dissolve first, then measure: a lake straddling two *adjacent* closed HUC12s is
    fully inside the closed system but majority-inside neither polygon on its own.

    Majority-area — NOT `intersects`, NOT `within`:
      * `within` fails on Great Salt Lake, which spills 1.1% into a neighbouring
        HUC12 (frac_in = 0.989).
      * `intersects` returns True for a ZERO-interior-overlap boundary touch, which
        wrongly grabs lakes grazing a closed boundary at frac_in = 0.000 (Eagle Lake,
        Middle Alkali Lake). Do not "simplify" this predicate back to `intersects`.
    """
    if closed_gdf.empty:
        return set()
    closed = closed_gdf.to_crs(wb_gdf.crs) if closed_gdf.crs != wb_gdf.crs else closed_gdf
    union = closed.geometry.union_all()
    area = wb_gdf.geometry.area
    frac = wb_gdf.geometry.intersection(union).area / area.where(area > 0)
    return {int(c) for c in wb_gdf.loc[frac > min_frac, "COMID"]}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_endorheic.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/endorheic.py tests/test_endorheic.py
git commit -m "feat(endorheic): Signal B -- closed-basin COMIDs by majority-area

Majority-area against the DISSOLVED type-C union, never intersects and never
within. `within` drops Great Salt Lake (frac_in = 0.989); `intersects` grabs
zero-interior-overlap boundary grazes (Eagle Lake, Middle Alkali Lake at 0.000).
Regression test pins the boundary-graze trap."
```

---

### Task 4: Signal A — terminus-inside-itself (pure)

**Files:**
- Modify: `src/gfv2_params/endorheic.py`
- Test: `tests/test_endorheic.py`

**Interfaces:**
- Consumes: `gfv2_params.d8_routing.drains_to_dprst_kernel(fdr, pour, barrier, fdr_nodata) -> (out, n_cycles)` — **returns a TUPLE**; `out` is 1 where the cell reaches a pour point.
- Produces:
  - `terminal_cells(fdr_path: Path) -> GeoDataFrame` (point per code-0 cell)
  - `frac_own_for_window(fdr: ndarray, inside: ndarray, fdr_nodata: int) -> float`
  - `terminus_own_fraction(wb_gdf, fdr_path, terminal, min_frac=MIN_FRAC, logger=None) -> DataFrame[comid, n_terminal, frac_own]`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_endorheic.py`:

```python
import numpy as np

from gfv2_params.endorheic import frac_own_for_window

# ESRI D8: 1=E 2=SE 4=S 8=SW 16=W 32=NW 64=N 128=NE. 0 and 255 are termini.
NOD = 255


def test_frac_own_endorheic_lake_drains_to_its_own_terminus():
    # A 3x3 lake (rows/cols 1..3) whose every cell flows to a code-0 cell at its
    # centre (2,2) -- the Great Salt Lake shape. All 9 cells reach their OWN
    # terminus, so frac_own == 1.0.
    fdr = np.full((5, 5), NOD, dtype=np.uint8)
    fdr[1, 1], fdr[1, 2], fdr[1, 3] = 2, 4, 8        # SE, S, SW -> (2,2)
    fdr[2, 1], fdr[2, 2], fdr[2, 3] = 1, 0, 16       # E, SINK, W -> (2,2)
    fdr[3, 1], fdr[3, 2], fdr[3, 3] = 128, 64, 32    # NE, N, NW -> (2,2)
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    assert frac_own_for_window(fdr, inside, NOD) == 1.0


def test_frac_own_through_flowing_lake_rejects():
    # The Lewis and Clark Lake shape: a through-flowing reservoir that happens to
    # contain ONE stray terminal cell. Every other cell flows E and leaves the lake,
    # so only the sink cell itself reaches an inside terminus -> 1/9 = 0.111.
    # Rule A ("contains a terminal cell") would wrongly demote this; Signal A does not.
    fdr = np.full((5, 5), 1, dtype=np.uint8)         # everything flows East
    fdr[1, 1] = 0                                     # the stray terminal cell
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    frac = frac_own_for_window(fdr, inside, NOD)
    assert frac == pytest.approx(1 / 9, abs=1e-6)
    assert frac < 0.5


def test_frac_own_is_zero_when_the_lake_has_no_terminal_cell():
    fdr = np.full((5, 5), 1, dtype=np.uint8)
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    assert frac_own_for_window(fdr, inside, NOD) == 0.0


def test_frac_own_ignores_a_terminal_cell_OUTSIDE_the_lake():
    # A pond upstream in a closed basin drains to the BASIN's terminus, not its own.
    # Signal A must not demote it. Here the lake's cells all flow E to a sink that
    # lies outside the lake.
    fdr = np.full((5, 5), 1, dtype=np.uint8)
    fdr[2, 4] = 0                                     # terminus OUTSIDE the lake
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    assert frac_own_for_window(fdr, inside, NOD) == 0.0
```

Add `import pytest` to the test file's imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_endorheic.py -k frac_own -v`
Expected: FAIL — `ImportError: cannot import name 'frac_own_for_window'`

- [ ] **Step 3: Implement Signal A**

Append to `src/gfv2_params/endorheic.py` (and add the imports at the top of the file):

```python
# --- add to the imports block at the top of endorheic.py -------------------------
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds
from shapely.geometry import Point

from .d8_routing import drains_to_dprst_kernel

# Minimum window pad around a waterbody, in metres, so a flow path that circles
# within the lake's own basin stays inside the window.
MIN_PAD_M = 20_000.0
# ---------------------------------------------------------------------------------


def terminal_cells(fdr_path: Path) -> gpd.GeoDataFrame:
    """Every FDR code-0 (terminal sink) cell, as a point on the FDR grid.

    These are the cells NHDPlus deliberately left UNFILLED in its HydroDEM, and they
    are what `d8_routing` already dead-ends at. The CONUS FDR has 15,262 of them.
    Scanned block-by-block: a full-grid array would be ~17 GB at CONUS scale.
    """
    xs, ys = [], []
    with rasterio.open(fdr_path) as src:
        crs = src.crs
        for _, win in src.block_windows(1):
            a = src.read(1, window=win)
            if not (a == 0).any():
                continue
            rows, cols = np.where(a == 0)
            x, y = rasterio.transform.xy(src.window_transform(win), rows, cols)
            xs.extend(np.atleast_1d(x))
            ys.extend(np.atleast_1d(y))
    return gpd.GeoDataFrame(
        geometry=[Point(x, y) for x, y in zip(xs, ys)], crs=crs
    )


def frac_own_for_window(
    fdr: np.ndarray, inside: np.ndarray, fdr_nodata: int
) -> float:
    """Share of the waterbody's cells whose D8 path reaches a terminus INSIDE it.

    `pour` is seeded from the terminal cells that fall inside the waterbody, then the
    same kernel `routing` uses resolves which cells reach one.
    """
    pour = np.zeros(fdr.shape, dtype=np.uint8)
    pour[inside & (fdr == 0)] = 1
    n_inside = int(inside.sum())
    if n_inside == 0 or pour.sum() == 0:
        return 0.0
    barrier = np.zeros(fdr.shape, dtype=np.uint8)
    # NOTE: drains_to_dprst_kernel returns a TUPLE (out, n_cycles).
    reach, _n_cycles = drains_to_dprst_kernel(fdr, pour, barrier, fdr_nodata=fdr_nodata)
    return float(((reach == 1) & inside).sum()) / n_inside


def terminus_own_fraction(
    wb_gdf: gpd.GeoDataFrame,
    fdr_path: Path,
    terminal: gpd.GeoDataFrame,
    logger=None,
) -> pd.DataFrame:
    """Per-COMID `frac_own` for every waterbody containing >= 1 terminal cell.

    Waterbodies with no terminal cell inside them cannot be endorheic under Signal A
    and are not evaluated (that is the cheap pre-filter — 6,429 of 448,124 CONUS
    waterbodies contain one).

    Returns columns: comid, n_terminal, frac_own.
    """
    hits = gpd.sjoin(
        terminal.to_crs(wb_gdf.crs), wb_gdf[["COMID", "geometry"]],
        how="inner", predicate="within",
    )
    counts = hits.groupby("COMID").size()
    cand = wb_gdf[wb_gdf["COMID"].isin(counts.index)].copy()
    # A multi-part waterbody appears as several rows sharing one COMID; keep the
    # largest part so the window (and `inside`) is the one that holds the terminus.
    cand = cand.assign(_a=cand.geometry.area).sort_values("_a", ascending=False)
    cand = cand.drop_duplicates(subset="COMID")

    rows = []
    with rasterio.open(fdr_path) as src:
        nodata = int(src.nodata) if src.nodata is not None else 255
        for i, rec in enumerate(cand.itertuples()):
            b = rec.geometry.bounds
            pad = max(MIN_PAD_M, 0.5 * max(b[2] - b[0], b[3] - b[1]))
            win = from_bounds(
                b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad, transform=src.transform
            ).round_offsets().round_lengths()
            if win.width < 3 or win.height < 3:
                continue
            fdr = src.read(1, window=win, boundless=True, fill_value=nodata)
            inside = geometry_mask(
                [rec.geometry], out_shape=fdr.shape,
                transform=src.window_transform(win), invert=True,
            )
            rows.append({
                "comid": int(rec.COMID),
                "n_terminal": int(counts.loc[rec.COMID]),
                "frac_own": frac_own_for_window(fdr, inside, nodata),
            })
            if logger and (i + 1) % 500 == 0:
                logger.info("  terminus scan: %d/%d waterbodies", i + 1, len(cand))
    return pd.DataFrame(rows, columns=["comid", "n_terminal", "frac_own"])
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_endorheic.py -v`
Expected: PASS (10 tests)

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/endorheic.py tests/test_endorheic.py
git commit -m "feat(endorheic): Signal A -- terminus-inside-itself on the FDR grid

A waterbody is depression storage iff its water's terminus lies INSIDE ITSELF.
GSL's water ends in GSL (frac_own = 1.000). A pond upstream in the same closed
basin ends in GSL, not the pond, so it stays on-stream. Lewis and Clark Lake --
a Missouri mainstem reservoir with one stray terminal cell -- ends in the Gulf
of Mexico (frac_own = 0.007) and is rejected.

Reuses d8_routing.drains_to_dprst_kernel, so the classifier and the router read
the same grid AND run the same traversal: they agree by construction."
```

---

### Task 5: The `endorheic` builder step + config + context

**Files:**
- Create: `src/gfv2_params/depstor_builders/endorheic.py`
- Modify: `src/gfv2_params/endorheic.py` (combiner + parquet IO)
- Modify: `src/gfv2_params/depstor_builders/__init__.py`
- Modify: `src/gfv2_params/depstor_builders/context.py`
- Modify: `scripts/build_depstor_rasters.py:65-105`
- Modify: `configs/depstor/depstor_rasters.yml`
- Modify: `configs/base_config.yml`
- Test: `tests/test_endorheic.py`

**Interfaces:**
- Produces:
  - `endorheic_frame(wb_gdf, fdr_path, closed_gdf=None, logger=None) -> DataFrame[comid, frac_own, by_terminus, by_closed_huc12]`
  - `write_endorheic_comids(df, path)` / `load_endorheic_comids(path) -> set[int]`
  - Builder registers ctx key `"endorheic_comids"` → `endorheic_waterbody_comids.parquet`
  - New `BuildContext` fields: `wbd_huc12_table`, `burn_add_waterbody_table`, `sink_points_table` (all `Path | None`)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_endorheic.py`:

```python
from gfv2_params.endorheic import load_endorheic_comids, write_endorheic_comids


def test_endorheic_parquet_roundtrip(tmp_path):
    df = pd.DataFrame({
        "comid": [1, 2, 3],
        "frac_own": [1.0, 0.007, 0.0],
        "by_terminus": [True, False, False],
        "by_closed_huc12": [False, False, True],
    })
    p = tmp_path / "endorheic.parquet"
    write_endorheic_comids(df, p)
    # Only rows flagged by at least one signal are demotions.
    assert load_endorheic_comids(p) == {1, 3}


def test_load_endorheic_comids_rejects_an_empty_table(tmp_path):
    p = tmp_path / "endorheic.parquet"
    write_endorheic_comids(
        pd.DataFrame(columns=["comid", "frac_own", "by_terminus", "by_closed_huc12"]), p
    )
    with pytest.raises(ValueError, match="0 endorheic COMIDs"):
        load_endorheic_comids(p)
```

Add `import pandas as pd` to the test imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_endorheic.py -k endorheic_parquet -v`
Expected: FAIL — `ImportError: cannot import name 'write_endorheic_comids'`

- [ ] **Step 3a: Add the combiner + IO to `src/gfv2_params/endorheic.py`**

```python
def endorheic_frame(
    wb_gdf: gpd.GeoDataFrame,
    fdr_path: Path,
    closed_gdf: gpd.GeoDataFrame | None = None,
    min_frac: float = MIN_FRAC,
    logger=None,
) -> pd.DataFrame:
    """Combine Signal A and Signal B into one provenance-carrying frame.

    Columns: comid, frac_own, by_terminus, by_closed_huc12. A COMID appears iff at
    least one signal flags it.
    """
    terminal = terminal_cells(fdr_path)
    if logger:
        logger.info("  %d FDR terminal (code-0) cells", len(terminal))
    own = terminus_own_fraction(wb_gdf, fdr_path, terminal, logger=logger)
    a = set(own.loc[own["frac_own"] > min_frac, "comid"].astype(int))
    b = closed_basin_comids(wb_gdf, closed_gdf, min_frac) if closed_gdf is not None else set()
    if logger:
        logger.info(
            "  Signal A (terminus-inside-itself): %d; Signal B (closed basin): %d; "
            "union: %d", len(a), len(b), len(a | b),
        )
    frac = dict(zip(own["comid"].astype(int), own["frac_own"]))
    comids = sorted(a | b)
    return pd.DataFrame({
        "comid": pd.Series(comids, dtype="int64"),
        "frac_own": [float(frac.get(c, 0.0)) for c in comids],
        "by_terminus": [c in a for c in comids],
        "by_closed_huc12": [c in b for c in comids],
    })


def write_endorheic_comids(df: pd.DataFrame, out_path: Path) -> None:
    """Write the endorheic COMID table (with per-signal provenance)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.astype({"comid": "int64"}).to_parquet(out_path, index=False)


def load_endorheic_comids(path: Path) -> set[int]:
    """Load the endorheic COMID set. Fails loud on an empty table.

    An empty table means the classifier found nothing and would degrade silently to
    the old behaviour — including leaving the Great Salt Lake on-stream.
    """
    df = pd.read_parquet(path, columns=["comid"])
    comids = {int(c) for c in df["comid"].to_numpy()}
    if not comids:
        raise ValueError(
            f"{path} lists 0 endorheic COMIDs → the demotion would be a silent no-op "
            f"and Great Salt Lake would stay on-stream. Re-run the `endorheic` step."
        )
    return comids
```

- [ ] **Step 3b: Create the builder `src/gfv2_params/depstor_builders/endorheic.py`**

```python
"""Emit the endorheic-waterbody COMID table consumed by `wbody_connectivity`.

Runs BEFORE wbody_connectivity. Signal A needs only `fdr_raster` (already a required
profile key on every fabric), so it works everywhere with no new configuration.
Signal B is optional and activates when `wbd_huc12_table` is present.
"""

from __future__ import annotations

import geopandas as gpd

from ..endorheic import endorheic_frame, write_endorheic_comids
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError("endorheic step needs `waterbody_gpkg` and `waterbody_layer`.")
    if ctx.fdr_raster is None:
        raise KeyError("endorheic step needs `fdr_raster` in the fabric profile.")
    output_path = ctx.resolve_output(step_cfg["output"])

    logger.info("--- endorheic ---")
    logger.info("  FDR       : %s", ctx.fdr_raster)
    logger.info("  Waterbody : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  WBD closed: %s", ctx.wbd_huc12_table or "(not configured — Signal B off)")
    logger.info("  Output    : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"endorheic_comids": output_path}
    if not ctx.fdr_raster.exists():
        raise FileNotFoundError(f"FDR raster not found: {ctx.fdr_raster}")

    wb = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer, use_arrow=True)
    if "COMID" not in wb.columns:
        raise KeyError(
            "waterbody layer has no COMID column — the endorheic classifier emits a "
            "COMID table and cannot run. Use a fabric whose waterbody layer carries "
            "COMID (e.g. `gfv2`), not the NHM_01_draft `wbs` layer."
        )
    wb = wb[wb.geometry.notna() & ~wb.geometry.is_empty]

    closed = None
    if ctx.wbd_huc12_table is not None:
        if not ctx.wbd_huc12_table.exists():
            raise FileNotFoundError(
                f"WBD HUC12 table not found: {ctx.wbd_huc12_table}. Run "
                f"`python -m gfv2_params.download.wbd_huc12` first, or remove "
                f"`wbd_huc12_table` from the profile."
            )
        closed = gpd.read_parquet(ctx.wbd_huc12_table)

    df = endorheic_frame(wb, ctx.fdr_raster, closed_gdf=closed, logger=logger)
    if df.empty:
        raise ValueError(
            "endorheic classifier flagged 0 waterbodies — that is a silent no-op "
            "(Great Salt Lake would stay on-stream). Check that fdr_raster has "
            "code-0 cells and that the waterbody layer overlaps it."
        )
    write_endorheic_comids(df, output_path)
    logger.info(
        "  %d endorheic COMIDs (%d by terminus, %d by closed basin)",
        len(df), int(df.by_terminus.sum()), int(df.by_closed_huc12.sum()),
    )
    return {"endorheic_comids": output_path}
```

- [ ] **Step 3c: Register the step**

In `src/gfv2_params/depstor_builders/__init__.py` — add `endorheic` to the `from . import (...)` list (alphabetical, after `dprst_depth`), add to `BUILDERS`:

```python
    "endorheic":         endorheic.build,
```

and insert into `STEP_ORDER` **between `waterbody` and `wbody_connectivity`**:

```python
STEP_ORDER = [
    "landmask",
    "imperv",
    "waterbody",
    "endorheic",
    "wbody_connectivity",
    ...
]
```

Add to the key table comment:

```
#   endorheic          -> "endorheic_comids"       endorheic_waterbody_comids.parquet
#                                                   (comid, frac_own, by_terminus,
#                                                    by_closed_huc12)
```

- [ ] **Step 3d: Add the `BuildContext` fields**

In `src/gfv2_params/depstor_builders/context.py`, after `flowthrough_comids_table`:

```python
    # --- endorheic classifier inputs ---------------------------------------
    # Full WBD HUC12 layer (type-C rows), staged by gfv2_params.download.wbd_huc12.
    # Optional: absent -> Signal B off, Signal A (FDR terminus) still runs.
    wbd_huc12_table: Path | None = None
    # BurnAddWaterbody polygons (gfv2_params.download.nhd_burn_components), unioned
    # into the waterbody layer by the `waterbody` builder. Optional.
    burn_add_waterbody_table: Path | None = None
    # NHDPlus Sink.shp — provenance + the BurnAdd linkage only, NOT a classifier
    # signal (the classifier reads the FDR grid). Optional.
    sink_points_table: Path | None = None
```

- [ ] **Step 3e: Pass them through in `scripts/build_depstor_rasters.py`**

Inside `_build_context`'s `BuildContext(...)` call, after `flowthrough_comids_table=...`:

```python
        wbd_huc12_table=(
            Path(config["wbd_huc12_table"]) if config.get("wbd_huc12_table") else None
        ),
        burn_add_waterbody_table=(
            Path(config["burn_add_waterbody_table"])
            if config.get("burn_add_waterbody_table") else None
        ),
        sink_points_table=(
            Path(config["sink_points_table"]) if config.get("sink_points_table") else None
        ),
```

- [ ] **Step 3f: Register the step in `configs/depstor/depstor_rasters.yml`**

Insert between the `waterbody` and `wbody_connectivity` steps:

```yaml
  - name: endorheic
    output: endorheic_waterbody_comids.parquet
```

- [ ] **Step 3g: Add the profile keys in `configs/base_config.yml`**

Under **both** the `gfv2` and `gfv2_dev` profiles, after `flowthrough_comids_table`:

```yaml
    # --- endorheic classifier (see docs/superpowers/specs/2026-07-12-endorheic-*) ---
    # Full WBD HUC12 layer; the `endorheic` builder filters HU_12_TYPE == 'C' itself.
    # Stage with: pixi run python -m gfv2_params.download.wbd_huc12
    # NOTE: do NOT point this at input/nhd/closed_huc12.gpkg — that is an incomplete
    # extract (23 type-C HUC12s in the Great Basin vs 141 in the full WBD).
    wbd_huc12_table: "{data_root}/input/wbd/wbd_huc12.parquet"
    # NHDPlus BurnAddWaterbody polygons — new depression AREA, unioned into the
    # waterbody layer. Stage with:
    #   pixi run python -m gfv2_params.download.nhd_burn_components
    burn_add_waterbody_table: "{data_root}/input/nhd/burn_add_waterbodies.parquet"
    # NHDPlus Sink.shp — provenance + the BurnAdd linkage only. NOT a classifier
    # signal: the endorheic classifier reads the FDR grid the router reads.
    sink_points_table: "{data_root}/input/nhd/sink_points.parquet"
```

- [ ] **Step 4: Run the tests**

Run: `pixi run -e dev pytest tests/test_endorheic.py tests/test_config.py -v`
Expected: PASS

Also verify the module imports cleanly (head-node-safe):

Run: `pixi run --as-is python -c "from gfv2_params.depstor_builders import BUILDERS, STEP_ORDER; assert 'endorheic' in BUILDERS; assert STEP_ORDER.index('endorheic') < STEP_ORDER.index('wbody_connectivity'); print('ok')"`
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/endorheic.py src/gfv2_params/depstor_builders/ \
        scripts/build_depstor_rasters.py configs/ tests/test_endorheic.py
git commit -m "feat(depstor): add the `endorheic` builder step

Emits endorheic_waterbody_comids.parquet (comid, frac_own, by_terminus,
by_closed_huc12) before wbody_connectivity. Signal A needs only fdr_raster, so it
runs on every fabric with no new config; Signal B activates when wbd_huc12_table
is configured. Fails loud on an empty result -- a silent no-op would leave Great
Salt Lake on-stream."
```

---

### Task 6: `wbody_connectivity` — the strict subtraction

**Files:**
- Modify: `src/gfv2_params/depstor_builders/wbody_connectivity.py`
- Test: `tests/test_wbody_connectivity.py`

**Interfaces:**
- Consumes: `endorheic.load_endorheic_comids(path) -> set[int]`; ctx key `"endorheic_comids"`

- [ ] **Step 1: Write the failing test**

`tests/test_wbody_connectivity.py` already carries builder-level fixtures — `_write_template()`, `_write_landmask()`, and `test_wbody_connectivity_rasterizes_only_connected(tmp_path)`, which drives the real builder through a real `BuildContext`. **Extend that pattern**; do not write a set-arithmetic assertion (it would test the stdlib, not this code).

Append to `tests/test_wbody_connectivity.py`:

```python
def test_endorheic_comid_is_demoted_from_the_connected_raster(tmp_path):
    """A waterbody the endorheic classifier flags must NOT reach connected_wbody.tif.

    This is the Great Salt Lake path: it IS in the WBAREACOMI/flow-through union
    (both local classifiers promote it), and the endorheic subtraction is the only
    thing that takes it back out.
    """
    import logging

    import geopandas as gpd
    import pandas as pd
    import rasterio
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    _write_template(template)
    _write_landmask(landmask)

    # Two on-stream waterbodies; COMID 2 is also flagged endorheic.
    wb = gpd.GeoDataFrame(
        {"COMID": [1, 2], "member_comid": [1, 2], "FTYPE": ["LakePond", "LakePond"]},
        geometry=[box(1, 1, 3, 3), box(5, 5, 7, 7)],
        crs="EPSG:5070",
    )
    wb_path = tmp_path / "wb.gpkg"
    wb.to_file(wb_path, layer="waterbodies", driver="GPKG")

    conn = tmp_path / "connected.parquet"
    pd.DataFrame({"comid": [1, 2]}).to_parquet(conn, index=False)

    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        {"comid": [2], "frac_own": [1.0], "by_terminus": [True],
         "by_closed_huc12": [False]}
    ).to_parquet(endo, index=False)

    ctx = BuildContext(
        fabric="test", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_path, hru_layer="waterbodies",
        waterbody_gpkg=wb_path, waterbody_layer="waterbodies",
        connected_comids_table=conn,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo

    wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("t")
    )
    with rasterio.open(tmp_path / "connected_wbody.tif") as src:
        arr = src.read(1)

    # COMID 1 (on-stream, not endorheic) is rasterised; COMID 2 (demoted) is not.
    assert (arr[1:3, 1:3] == 1).any(), "COMID 1 should still be on-stream"
    assert not (arr[5:7, 5:7] == 1).any(), "COMID 2 was endorheic and must be demoted"


def test_endorheic_subtraction_never_widens_the_onstream_set(tmp_path):
    """An endorheic COMID that is NOT on-stream must be a pure no-op.

    The safety invariant: these signals may only SUBTRACT. If this ever fails, the
    endorheic table has become capable of ADDING to the on-stream mask.
    """
    import logging

    import geopandas as gpd
    import pandas as pd
    import rasterio
    from shapely.geometry import box

    from gfv2_params.depstor_builders import wbody_connectivity
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    _write_template(template)
    _write_landmask(landmask)

    wb = gpd.GeoDataFrame(
        {"COMID": [1], "member_comid": [1], "FTYPE": ["LakePond"]},
        geometry=[box(1, 1, 3, 3)], crs="EPSG:5070",
    )
    wb_path = tmp_path / "wb.gpkg"
    wb.to_file(wb_path, layer="waterbodies", driver="GPKG")

    conn = tmp_path / "connected.parquet"
    pd.DataFrame({"comid": [1]}).to_parquet(conn, index=False)

    # COMID 999 is endorheic but was never on-stream: subtracting it changes nothing.
    endo = tmp_path / "endorheic.parquet"
    pd.DataFrame(
        {"comid": [999], "frac_own": [1.0], "by_terminus": [True],
         "by_closed_huc12": [False]}
    ).to_parquet(endo, index=False)

    ctx = BuildContext(
        fabric="test", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_path, hru_layer="waterbodies",
        waterbody_gpkg=wb_path, waterbody_layer="waterbodies",
        connected_comids_table=conn,
    )
    ctx.paths["landmask"] = landmask
    ctx.paths["endorheic_comids"] = endo

    wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("t")
    )
    with rasterio.open(tmp_path / "connected_wbody.tif") as src:
        arr = src.read(1)
    assert (arr[1:3, 1:3] == 1).any(), "COMID 1 must remain on-stream — no widening"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -k endorheic -v`
Expected: FAIL — the builder ignores `ctx.paths["endorheic_comids"]`, so COMID 2 is still rasterised into `connected_wbody.tif`.

- [ ] **Step 3: Implement the subtraction**

In `src/gfv2_params/depstor_builders/wbody_connectivity.py`:

Add the import:

```python
from ..endorheic import load_endorheic_comids
```

Then, immediately **after** the `connected = connected | flowthrough` block and **before** `logger.info("  on-stream COMIDs: ...")`, insert:

```python
    # Endorheic demotion (see gfv2_params.endorheic). A STRICT SUBTRACTION: these
    # signals can only remove COMIDs from the on-stream set, never add one — so the
    # on-stream mask can never be inflated by them. This is what finally takes the
    # Great Salt Lake off-stream: both local classifiers promote it, because NHD
    # draws Network artificial paths between its arms.
    n_endorheic = 0
    if "endorheic_comids" in ctx.paths:
        endorheic = load_endorheic_comids(ctx.require("endorheic_comids"))
        n_endorheic = len(connected & endorheic)
        connected = connected - endorheic
        logger.info(
            "  endorheic demotion: %d of %d endorheic COMIDs were on-stream → dprst",
            n_endorheic, len(endorheic),
        )
```

And extend the existing summary log line to mention it:

```python
    logger.info(
        "  on-stream COMIDs: %d WBAREACOMI + %d new flow-through - %d endorheic "
        "= %d total",
        n_wbareacomi, n_flowthrough, n_endorheic, len(connected),
    )
```

- [ ] **Step 4: Run the tests**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/wbody_connectivity.py tests/test_wbody_connectivity.py
git commit -m "feat(depstor): demote endorheic waterbodies from the on-stream set

connected -= endorheic. A strict subtraction, structurally identical to the
existing NEVER_ONSTREAM_FTYPES guardrail: it can only remove COMIDs, so the
on-stream mask can never be inflated. Takes Great Salt Lake off-stream."
```

---

### Task 7: `waterbody` — union in the BurnAddWaterbody polygons

**Files:**
- Modify: `src/gfv2_params/depstor_builders/waterbody.py`
- Test: `tests/test_waterbody.py`

**Interfaces:**
- Consumes: `ctx.burn_add_waterbody_table`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_waterbody.py`:

```python
import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from gfv2_params.depstor_builders.waterbody import merge_burn_add

CRS = "EPSG:5070"
WB_COLS = ["GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm"]


def _sq(x0, y0, s=100):
    return Polygon([(x0, y0), (x0 + s, y0), (x0 + s, y0 + s), (x0, y0 + s)])


def _frame(rows):
    return gpd.GeoDataFrame(rows, columns=WB_COLS + ["geometry"], crs=CRS)


def test_merge_burn_add_appends_the_polygons():
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(500, 500)]])
    out = merge_burn_add(base, burn)
    assert len(out) == 2
    assert set(out.COMID) == {111, -367111}


def test_merge_burn_add_rejects_a_non_negative_comid():
    # A positive BurnAdd COMID could match a WBAREACOMI / flow-through COMID and be
    # promoted on-stream -- but NHDPlus flagged every BurnAddWaterbody as a SINK.
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, 222, "Playa", 222, 0.01, _sq(500, 500)]])
    with pytest.raises(ValueError, match="negative"):
        merge_burn_add(base, burn)


def test_merge_burn_add_is_a_noop_when_not_configured():
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    assert merge_burn_add(base, None) is base


def test_merge_burn_add_rejects_an_overlapping_polygon():
    """A BurnAdd polygon overlapping an existing waterbody must FAIL LOUD.

    `clump_regions` labels 8-connected components, so an overlap merges the BurnAdd
    playa and the existing waterbody into ONE region. If that waterbody is on-stream,
    `regions_touching_mask` excludes the whole clump — silently DELETING the playa's
    depression area, the exact opposite of why we staged it, with nothing in the log
    to say so. The VPU 16 spike measured 0/23 overlaps; CONUS-wide is unverified, so
    this is checked at build time and not left to a diagnostic.
    """
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(50, 50)]])  # overlaps
    with pytest.raises(ValueError, match="overlap"):
        merge_burn_add(base, burn)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run -e dev pytest tests/test_waterbody.py -k burn_add -v`
Expected: FAIL — `ImportError: cannot import name 'merge_burn_add'`

- [ ] **Step 3: Implement it**

In `src/gfv2_params/depstor_builders/waterbody.py`, add:

```python
import pandas as pd


def merge_burn_add(
    wb_gdf: gpd.GeoDataFrame, burn_gdf: gpd.GeoDataFrame | None
) -> gpd.GeoDataFrame:
    """Union NHDPlus BurnAddWaterbody polygons into the waterbody layer.

    These are closed lakes / playas NHDPlus added for the DEM burn that are absent
    from NHDWaterbody — genuinely new depression AREA (0 of 23 overlap an existing
    waterbody in VPU 16). Once they are waterbody polygons they flow through
    waterbody -> dprst -> routing untouched and become dprst pour-points, which is
    why `routing` needs no change.

    Their COMID (NHDPlus `PolyID`) is NEGATIVE, so it can never match a WBAREACOMI or
    flow-through COMID (all positive) — that is what makes them structurally incapable
    of on-stream promotion. Asserted here rather than left to luck.
    """
    if burn_gdf is None or len(burn_gdf) == 0:
        return wb_gdf
    if (burn_gdf["COMID"] >= 0).any():
        raise ValueError(
            "BurnAddWaterbody COMID must be negative (NHDPlus PolyID). A non-negative "
            "value could match a positive WBAREACOMI/flow-through COMID and be promoted "
            "on-stream — but NHDPlus flagged every BurnAddWaterbody as a sink."
        )
    burn = burn_gdf.to_crs(wb_gdf.crs) if burn_gdf.crs != wb_gdf.crs else burn_gdf

    # A BurnAdd polygon overlapping an existing waterbody would be MERGED with it by
    # clump_regions (8-connected labelling). If that waterbody is on-stream,
    # regions_touching_mask then excludes the whole clump — silently DELETING the
    # BurnAdd playa's depression area, the opposite of why we staged it, and invisible
    # in the logs. VPU 16 measured 0 of 23 overlapping; CONUS-wide is unverified, so
    # this fails loud rather than corrupting the product quietly.
    hits = gpd.sjoin(
        burn[["COMID", "geometry"]], wb_gdf[["COMID", "geometry"]],
        how="inner", predicate="intersects",
    )
    if not hits.empty:
        bad = sorted(set(hits["COMID_left"]))[:10]
        raise ValueError(
            f"{hits['COMID_left'].nunique()} BurnAddWaterbody polygon(s) overlap an "
            f"existing waterbody (e.g. {bad}). clump_regions would merge them into one "
            f"region, so an on-stream neighbour would silently drag the BurnAdd "
            f"depression out of dprst. Investigate the overlap — do not suppress this."
        )

    return gpd.GeoDataFrame(
        pd.concat([wb_gdf, burn[wb_gdf.columns]], ignore_index=True), crs=wb_gdf.crs
    )
```

Then in `build()`, immediately after the `wb_gdf = wb_gdf[wb_gdf.geometry.notna() ...]` line and **before** the FTYPE/Ice-Mass filter:

```python
    if ctx.burn_add_waterbody_table is not None:
        if not ctx.burn_add_waterbody_table.exists():
            raise FileNotFoundError(
                f"BurnAddWaterbody table not found: {ctx.burn_add_waterbody_table}. "
                f"Run `python -m gfv2_params.download.nhd_burn_components` first, or "
                f"remove `burn_add_waterbody_table` from the profile."
            )
        burn = gpd.read_parquet(ctx.burn_add_waterbody_table)
        n_before = len(wb_gdf)
        wb_gdf = merge_burn_add(wb_gdf, burn)
        logger.info(
            "  merged %d BurnAddWaterbody polygons (%.1f km2) into %d waterbodies",
            len(wb_gdf) - n_before,
            float(burn.to_crs(info.crs).geometry.area.sum() / 1e6),
            n_before,
        )
```

- [ ] **Step 4: Run the tests**

Run: `pixi run -e dev pytest tests/test_waterbody.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/waterbody.py tests/test_waterbody.py
git commit -m "feat(depstor): union BurnAddWaterbody polygons into the waterbody layer

Closed lakes/playas NHDPlus added for the DEM burn that NHDWaterbody lacks --
genuinely new depression AREA. Their PolyID is negative, so they can never match
a positive WBAREACOMI/flow-through COMID: structurally unable to be promoted
on-stream, which is correct since NHDPlus flagged every one as a sink. Asserted.

Once they are waterbody polygons they become dprst pour-points via the existing
waterbody -> dprst -> routing chain, so `routing` needs no change."
```

---

### Task 8: Documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`

- [ ] **Step 1: Correct the FdrFac claim in `CLAUDE.md`**

Find the bullet beginning **"Depstor template/fdr come from a fabric-bounds clip"**. Replace the sentence

> `fdr.vrt` is the **official NHDPlus V2 `FdrFac`** (NHDPlus HydroDEM: stream-burned + walled + fully depression-filled), so `drains_to_dprst` routes on a fully drainage-enforced FDR with interior sinks removed

with:

> `fdr.vrt` is the **official NHDPlus V2 `FdrFac`** (NHDPlus HydroDEM: stream-burned + walled + depression-filled **everywhere except at NHDPlus's own sinks**). It is **not** fully drainage-enforced: it contains exactly **15,262 code-0 (terminal) cells**, and 8,591 of the 8,611 NHD sink points land on one. NHDPlus leaves those sinks unfilled *by design*. The "FDR code-0 warnings" from #145 **are that sink set**, and they are the primary signal of the endorheic dprst classifier (a waterbody is depression storage iff its water's terminus lies inside itself — see `gfv2_params.endorheic` and `docs/superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md`).

- [ ] **Step 2: Add the endorheic bullet to `CLAUDE.md`'s gotchas**

After the `wbody_connectivity` bullet, add:

```markdown
- **Endorheic demotion is a STRICT SUBTRACTION, and its input is the FDR — not a
  vector sink file.** `wbody_connectivity` subtracts an endorheic COMID set from the
  on-stream union. Signal A ("terminus-inside-itself") reads the FDR's code-0 cells
  and runs `d8_routing`'s own kernel, so the classifier and the router agree by
  construction. **Do not** substitute `input/nhd/NHD_sink_points.gpkg` — it is a
  strict subset of NHDPlus's `Sink.shp` (537 vs 3,222 in VPU 16) that omits
  `PURPCODE 1` entirely and therefore has **0 sinks inside Great Salt Lake** where
  NHDPlus has 29. Likewise `input/nhd/closed_huc12.gpkg` (23 type-C HUC12s in the
  Great Basin vs 141 in the full WBD) — stage from the authoritative source.
  Containment tests use **majority-area**, never `intersects` (a zero-interior-overlap
  boundary touch returns `True`) and never `within` (it drops GSL at 0.989).
```

- [ ] **Step 3: `docs/ARCHITECTURE.md`**

In the depstor step list, insert `endorheic` between `waterbody` and `wbody_connectivity`, describing its two signals and its `endorheic_waterbody_comids.parquet` output. Add `wbd_huc12_table`, `burn_add_waterbody_table` and `sink_points_table` to the per-key required-field table as **optional**.

- [ ] **Step 4: `slurm_batch/RUNME.md` + `HPC_REFERENCE.md`**

Add the two staging commands ahead of the depstor build, in the same style as the existing `nhd_topology` / `nhd_flowlines` steps:

```bash
# Endorheic classifier inputs (run once; CONUS-shared, fabric-independent)
pixi run python -m gfv2_params.download.nhd_burn_components   # Sink.shp + BurnAddWaterbody
pixi run python -m gfv2_params.download.wbd_huc12             # full WBD (type-C closed basins)
```

Note the rebuild cascade: changing the waterbody layer or the on-stream set re-runs
`waterbody → endorheic → wbody_connectivity → dprst → routing → drains_perv/imperv`
(`--mem=384G` for `waterbody`/`dprst`, `96G` for `routing`).

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md docs/ARCHITECTURE.md slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md
git commit -m "docs: correct the FdrFac claim and document the endorheic classifier

CLAUDE.md said the NHDPlus FdrFac is 'fully depression-filled ... interior sinks
removed'. It is not: 15,262 code-0 cells, 8,591 of them under an NHD sink point.
NHDPlus leaves them unfilled by design, and they are the classifier's primary
signal. Also records that the pre-made NHD_sink_points.gpkg and closed_huc12.gpkg
extracts must not be used, and pins the majority-area predicate."
```

---

### Task 9: CONUS staging + the VPU 16 validation gate

**Files:**
- Create: `scripts/diagnose/endorheic_fixtures.py`
- No source changes.

**This task is the gate. Do not merge without it.**

- [ ] **Step 1: Stage the two new inputs**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
pixi run python -m gfv2_params.download.nhd_burn_components
pixi run python -m gfv2_params.download.wbd_huc12
```

Expected: `sink_points.parquet` (≫ 8,611 sinks — NHDPlus's full set, not the 8,611-row extract), `burn_add_waterbodies.parquet`, `wbd_huc12.parquet` (≥ 141 type-C HUC12s in VPU 16 alone).

- [ ] **Step 2: Write the named-fixture check**

Create `scripts/diagnose/endorheic_fixtures.py`:

```python
"""Assert the 20 named fixtures the endorheic classifier exists to get right.

Ten must become depression storage; ten must stay on-stream. The ten on-stream ones
are all DOMAIN EXITS — terminal only because the CONUS model ends there — and they
are what broke every attribute-based guard tried during design.

Run AFTER the `endorheic` step:
    pixi run python scripts/diagnose/endorheic_fixtures.py --fabric gfv2
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_config

MUST_BE_DPRST = {
    946020001: "Great Salt Lake", 948100002: "Salton Sea", 11310757: "Pyramid Lake",
    120053921: "Mono Lake", 10734232: "Walker Lake", 24040174: "Lake Abert",
    24032426: "Summer Lake", 20296729: "Honey Lake", 120052284: "Goose Lake",
    120052521: "Devils Lake",
}
MUST_STAY_ONSTREAM = {
    904140248: "Lake Michigan", 904140243: "Lake Superior", 904140244: "Lake Huron",
    904140245: "Lake Erie", 904140246: "Lake Ontario", 15447630: "Lake Champlain",
    120052195: "Lake of the Woods", 22762810: "Lake Borgne",
    120055431: "Everglades SwampMarsh", 11758154: "Lewis and Clark Lake",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fabric", required=True)
    args = ap.parse_args()
    cfg = load_config("configs/depstor/depstor_rasters.yml", fabric=args.fabric)
    path = Path(cfg["output_dir"]) / "endorheic_waterbody_comids.parquet"
    demoted = set(pd.read_parquet(path, columns=["comid"])["comid"].astype(int))
    print(f"{path}: {len(demoted):,} endorheic COMIDs\n")

    failures = []
    for comid, name in MUST_BE_DPRST.items():
        ok = comid in demoted
        print(f"  {'PASS' if ok else 'FAIL'}  dprst      {name} ({comid})")
        if not ok:
            failures.append(f"{name} should be dprst but was not demoted")
    print()
    for comid, name in MUST_STAY_ONSTREAM.items():
        ok = comid not in demoted
        print(f"  {'PASS' if ok else 'FAIL'}  on-stream  {name} ({comid})")
        if not ok:
            failures.append(f"{name} is a DOMAIN EXIT and must NOT be demoted")

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll 20 named fixtures pass.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run the `endorheic` step on CONUS + check the fixtures**

```bash
pixi run --as-is python scripts/build_depstor_rasters.py --fabric gfv2 --step endorheic --force
pixi run --as-is python scripts/diagnose/endorheic_fixtures.py --fabric gfv2
```

Expected: **all 20 fixtures pass.** Expected scale: ~216 demotions by terminus, ≥ that by the union with Signal B (the full WBD adds at least Walker Lake). If Lake Michigan or Lewis and Clark Lake is demoted, **stop** — the rule has regressed to "contains a terminal cell."

- [ ] **Step 4: Threshold-insensitivity check**

The 0.5 threshold must remain inert — that is what makes it a physical fact rather than
a tuned knob. Add this to `scripts/diagnose/endorheic_fixtures.py`'s `main()`, just
before the `return 0`:

```python
    d = pd.read_parquet(path)
    counts = {t: int((d["frac_own"] > t).sum()) for t in (0.3, 0.5, 0.7)}
    swing = (counts[0.3] - counts[0.7]) / max(counts[0.5], 1)
    print(f"\nthreshold sweep (Signal A): {counts}   swing = {swing:.1%}")
    if swing > 0.10:
        print(
            "  WARNING: frac_own is no longer bimodal (design measured a 3% swing "
            "across 0.3-0.7). The 0.5 threshold has become load-bearing — "
            "re-examine the rule before proceeding."
        )
        return 1
```

Expected: `swing` ≈ 3% (design measured 223 → 216 → 208 polygons). A swing > 10% means
`frac_own` has stopped being bimodal and the rule needs re-examination.

- [ ] **Step 5: VPU 16 A/B on `drains_to_dprst`**

Rebuild the cascade and A/B it. GSL flips from routing **barrier** to **pour-point**, so `drains_to_dprst` *will* grow in VPU 16 — the gate is that it grows only where GSL is the same-HRU depression.

```bash
# Preserve the current product before rebuilding
cp -r ${DATA_ROOT}/gfv2/depstor_rasters ${DATA_ROOT}/gfv2/depstor_rasters_pre_endorheic_$(date +%F)

pixi run --as-is python scripts/build_depstor_rasters.py --fabric gfv2 --from waterbody --force
pixi run --as-is python scripts/diagnose/ab_drains_to_dprst.py \
    --fabric gfv2 --vpu 16 \
    --before ${DATA_ROOT}/gfv2/depstor_rasters_pre_endorheic_$(date +%F)/drains_to_dprst.tif \
    --after  ${DATA_ROOT}/gfv2/depstor_rasters/drains_to_dprst.tif
```

Confirm: (a) `drains_to_dprst` growth in VPU 16 is concentrated around GSL; (b) the six PRMS ratios in `merged/` move sanely; (c) `dprst_binary` area rises by roughly the expected ~8% (7,877 km² from Signal A + Signal B + the BurnAddWaterbody area).

- [ ] **Step 6: BurnAddWaterbody disjointness + the imperv/dprst/perv partition**

BurnAdd polygons must not overlap existing waterbodies (the VPU 16 spike measured
0/23) — an overlap would merge two clumps in `clump_regions` and let one region's
on-stream status leak into the other.

```bash
pixi run --as-is python -c "
import geopandas as gpd, numpy as np, rasterio
from pathlib import Path
from gfv2_params.config import load_config
cfg = load_config('configs/depstor/depstor_rasters.yml', fabric='gfv2')
out = Path(cfg['output_dir'])
burn = gpd.read_parquet(cfg['burn_add_waterbody_table'])
wb = gpd.read_file(cfg['waterbody_gpkg'], layer=cfg['waterbody_layer'],
                   columns=['COMID'], use_arrow=True).to_crs(burn.crs)
ov = gpd.sjoin(burn[['COMID','geometry']], wb, how='inner', predicate='intersects')
print(f'BurnAdd polygons overlapping an existing waterbody: {ov.COMID_left.nunique()} of {len(burn)}')
assert ov.empty, 'BurnAdd overlaps an existing waterbody -- clump_regions would merge them'

# imperv / dprst / perv must stay a disjoint partition
a = {}
for k in ('imperv_binary','dprst_binary','perv_binary'):
    with rasterio.open(out/f'{k}.tif') as s:
        a[k] = s.read(1) == 1
for x, y in (('imperv_binary','dprst_binary'), ('imperv_binary','perv_binary'), ('dprst_binary','perv_binary')):
    n = int((a[x] & a[y]).sum())
    print(f'{x} AND {y}: {n} cells')
    assert n == 0, f'{x}/{y} overlap -- the partition is no longer disjoint'
print('partition disjoint; BurnAdd disjoint')
"
```

Expected: 0 overlapping BurnAdd polygons, 0 cells in any pairwise intersection.

- [ ] **Step 7: Commit the diagnostic + open the PR**

```bash
git add scripts/diagnose/endorheic_fixtures.py
git commit -m "test(endorheic): assert the 20 named fixtures

Ten must be dprst (GSL, Salton Sea, Pyramid, Mono, Walker, Abert, Summer, Honey,
Goose, Devils Lake). Ten must stay on-stream -- all DOMAIN EXITS, terminal only
because the CONUS model ends there (the 5 Great Lakes, Lake Champlain, Lake of the
Woods, Lake Borgne, the Everglades, Lewis and Clark Lake). These twenty are what
broke every attribute-based guard tried during design."

pixi run -e dev pre-commit run --all-files
```

Open the PR and let CI run `pytest`. Lead the PR body with the Great Salt Lake fix, the `CLAUDE.md` FdrFac correction, and the fact that all three pre-made `input/nhd/` extracts were found defective.

---

## Deferred (do NOT build here — filed as follow-ups)

1. **Footprints for the 983 area-less sinks** (WBD closed-HUC12 sinks, topographic depressions, karst sinkholes). `fill(hydrodem) − hydrodem` isolates exactly them, yielding area *and* depth (feeding `dprst_depth_avg`, #173). Phase-0 spike, mirroring #173. **Note:** the orphan-sink counts in the spec are provisional floors — they derive from the incomplete `NHD_sink_points.gpkg`, so recompute them against `sink_points.parquet` once Task 1 has run.
2. **The network-terminal classifier.** Dropped as redundant: Signal A catches every lake it found *and* rejects Lake Champlain, Lake of the Woods and the Everglades, which it could not. Dropping it removes the classifier's only dependency on the fabric segment network, so the fabric↔NHD crosswalk is not a blocker.
3. **The SwampMarsh question for Andy Bock.** His `conus_waterbodies` layer drops all 66,488 SwampMarsh (149,468 km², including the Everglades). We keep `waterbody_layer: waterbodies`, so nothing here presumes an answer. If he confirms SwampMarsh should be excluded from depression storage, that is a separate, deliberate change.
