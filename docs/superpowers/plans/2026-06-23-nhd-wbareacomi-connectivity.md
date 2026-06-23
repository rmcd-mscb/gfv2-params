# NHD WBAREACOMI Waterbody Connectivity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 60 m stream-buffer proximity heuristic for on-stream vs. depression-storage waterbody classification with NHD's authoritative `WBAREACOMI` artificial-path topology, joined to `conus_waterbodies.gpkg` by COMID.

**Architecture:** A new download module stages NHDPlusV2 `NHDFlowline` attribute tables per VPU and distills them to a flat "connected waterbody COMID" parquet. A new depstor builder (`wbody_connectivity`) rasterizes the waterbody polygons whose COMID is in that set, producing `connected_wbody.tif`. `dprst.py` consumes that mask in place of `stream_buffer.tif` (integration option A — drop-in mask, clump/region and impervious logic unchanged), and the `streambuffer` step is retired.

**Tech Stack:** Python, geopandas/pyogrio, pandas, rasterio, scipy.ndimage, py7zr, requests; pixi-managed env; pipeline driven by `configs/depstor/depstor_rasters.yml` + fabric profiles in `configs/base_config.yml`.

## Global Constraints

- **Paths come from the profile, never hardcoded.** Read fabric inputs via `require_config_key`/`config.get` against the active fabric profile in `configs/base_config.yml`; use `{data_root}`/`{fabric}`/`{vpu}` placeholders. (CLAUDE.md)
- **Builder + test together.** A new pipeline step is a builder module + DAG registration + config block *and* a `tests/test_<builder>.py`. No standalone scripts or orphan YAML. (CLAUDE.md)
- **Do not run pytest on the HPC head node.** CI (`.github/workflows/ci.yml`) is the authoritative test gate. Local `py_compile`/import checks are fine. (CLAUDE.md)
- **Add deps via `pyproject.toml`** (conda-forge vs pypi split per its comment block), then `pixi install`. `py7zr`, `requests`, `pyogrio`, `geopandas`, `pandas`, `pyarrow` are already present (used by `download/rpu_rasters.py` and the depstor builders).
- **SLURM batches invoke `pixi run --as-is`** — never a flow that mutates the env per task.
- **Every code change needs a docs check** — audit `docs/`, `README.md`, `slurm_batch/RUNME.md` + `HPC_REFERENCE.md`; update on this branch.
- **Atomic commits** — one deliverable per commit. End commit messages with the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.
- **Branch:** all work lands on `feat/wbareacomi-connectivity` (already created; spec already committed there).
- **WBAREACOMI sentinel:** `0` and null mean "not through a waterbody" — excluded from the connected set.

---

### Task 1: Connected-COMID extraction helper (pure)

The testable core of the staging step: given flowline attributes, produce the set of connected waterbody COMIDs. No network, no disk — pure pandas.

**Files:**
- Create: `src/gfv2_params/download/nhd_flowlines.py` (helper portion only this task)
- Test: `tests/test_download_nhd_flowlines.py`

**Interfaces:**
- Produces:
  - `connected_comids_from_flowlines(df: pd.DataFrame) -> set[int]` — given a DataFrame with a `WBAREACOMI` column, return distinct non-zero, non-null `WBAREACOMI` values as a set of `int`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_download_nhd_flowlines.py
"""Unit tests for the NHD flowline -> connected-waterbody-COMID distillation."""

import pandas as pd

from gfv2_params.download.nhd_flowlines import connected_comids_from_flowlines


def test_connected_comids_distinct_nonzero():
    df = pd.DataFrame(
        {
            "COMID": [1, 2, 3, 4, 5],
            "FTYPE": ["ArtificialPath", "ArtificialPath", "StreamRiver",
                      "ArtificialPath", "ArtificialPath"],
            # 0 = not through a waterbody; duplicates collapse; None excluded.
            "WBAREACOMI": [100, 100, 0, 200, None],
        }
    )
    assert connected_comids_from_flowlines(df) == {100, 200}


def test_connected_comids_empty_when_all_zero():
    df = pd.DataFrame({"WBAREACOMI": [0, 0, 0]})
    assert connected_comids_from_flowlines(df) == set()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_download_nhd_flowlines.py -v`
Expected: FAIL — `ModuleNotFoundError` / `ImportError: cannot import name 'connected_comids_from_flowlines'`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/gfv2_params/download/nhd_flowlines.py
"""Stage NHDPlusV2 NHDFlowline attributes and distill connected-waterbody COMIDs.

NHD encodes waterbody connectivity directly: an artificial-path NHDFlowline that
runs through a waterbody carries WBAREACOMI = that waterbody's COMID. The distinct
set of populated WBAREACOMI values is the set of on-stream (connected) waterbodies.
This module downloads the per-VPU NHDSnapshot archives, reads NHDFlowline, and
writes a flat parquet of connected COMIDs consumed by the depstor
`wbody_connectivity` builder.
"""

from __future__ import annotations

import pandas as pd

# WBAREACOMI == 0 (and null) means the flowline does not pass through a waterbody.
_NO_WATERBODY = 0


def connected_comids_from_flowlines(df: pd.DataFrame) -> set[int]:
    """Distinct non-zero, non-null WBAREACOMI values as a set of ints."""
    col = pd.to_numeric(df["WBAREACOMI"], errors="coerce")
    vals = col[(col.notna()) & (col != _NO_WATERBODY)]
    return {int(v) for v in vals.unique()}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_download_nhd_flowlines.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/download/nhd_flowlines.py tests/test_download_nhd_flowlines.py
git commit -m "feat(nhd): connected-COMID distillation from WBAREACOMI

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: NHD flowline download/staging module + SLURM batch

Wrap the Task-1 helper in the network/disk glue that downloads per-VPU NHDSnapshot archives, reads `NHDFlowline`, and writes the connected-COMID parquet. This mirrors `download/rpu_rasters.py`. Network and 7z extraction are not exercised in CI; the unit-tested core is the parquet writer.

**Files:**
- Modify: `src/gfv2_params/download/nhd_flowlines.py`
- Create: `slurm_batch/download_nhd_flowlines.batch`
- Test: `tests/test_download_nhd_flowlines.py` (add a writer test)

**Interfaces:**
- Consumes: `connected_comids_from_flowlines` (Task 1); `load_base_config` from `gfv2_params.config`; `configure_logging` from `gfv2_params.log`.
- Produces:
  - `write_connected_comids(comids: set[int], out_path: Path) -> None` — write a single-column (`comid`, int64) parquet, sorted ascending, to `out_path` (parent dirs created).
  - `read_flowline_attrs(flowline_path: Path) -> pd.DataFrame` — read `COMID`, `FTYPE`, `WBAREACOMI` from an OGR-readable NHDFlowline source (shp/dbf), no geometry.
  - `main() -> None` — download every VPU's NHDSnapshot, accumulate connected COMIDs across all VPUs, write `{data_root}/input/nhd/connected_waterbody_comids.parquet`.
  - Module entrypoint runnable as `python -m gfv2_params.download.nhd_flowlines`.

NHDSnapshot is a **per-VPU** component (no RPU suffix): the archive is `NHDPlusV21_{dd}_{vpu}_NHDSnapshot_{ver}.7z` and unpacks `NHDSnapshot/Hydrography/NHDFlowline.shp` (+ sidecars).

> **Implementation note (verify before coding the path template):** confirm the exact NHDSnapshot archive name and version-candidate list against a live S3 listing of `https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlus{dd}/...`. Reuse the `version_candidates = ["05","04","03","02","01"]` HEAD-probe fallback and the base-URL branch (some VPUs nest under `/NHDPlus{vpu}`) from `rpu_rasters.py:154-191` verbatim. The relative path of `NHDFlowline.shp` inside the archive can vary in case (`NHDSnapshot` vs `NHDSnapshot/Hydrography`); glob for `**/NHDFlowline.shp` after extraction rather than hardcoding.

- [ ] **Step 1: Write the failing test (parquet writer round-trip)**

```python
# add to tests/test_download_nhd_flowlines.py
import pandas as pd
from gfv2_params.download.nhd_flowlines import write_connected_comids


def test_write_connected_comids_roundtrip(tmp_path):
    out = tmp_path / "nested" / "connected_waterbody_comids.parquet"
    write_connected_comids({300, 100, 200}, out)

    assert out.exists()
    got = pd.read_parquet(out)
    assert list(got.columns) == ["comid"]
    assert got["comid"].tolist() == [100, 200, 300]  # sorted ascending
    assert str(got["comid"].dtype) in ("int64", "Int64")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_download_nhd_flowlines.py::test_write_connected_comids_roundtrip -v`
Expected: FAIL — `ImportError: cannot import name 'write_connected_comids'`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/gfv2_params/download/nhd_flowlines.py`:

```python
from pathlib import Path

import py7zr
import pyogrio
import requests

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_flowlines")

# VPU -> drainage-area code (DD). NHDSnapshot is per-VPU (no RPU split).
vpu_index = {
    "01": "NE", "02": "MA", "03N": "SA", "03S": "SA", "03W": "SA",
    "04": "GL", "05": "MS", "06": "MS", "07": "MS", "08": "MS",
    "09": "SR", "10L": "MS", "10U": "MS", "11": "MS", "12": "TX",
    "13": "RG", "14": "CO", "15": "CO", "16": "GB", "17": "PN", "18": "CA",
}

_VERSION_CANDIDATES = ["05", "04", "03", "02", "01"]


def write_connected_comids(comids: set[int], out_path: Path) -> None:
    """Write the connected COMIDs to a single-column int64 parquet, sorted."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"comid": sorted(int(c) for c in comids)}).astype({"comid": "int64"})
    df.to_parquet(out_path, index=False)


def read_flowline_attrs(flowline_path: Path) -> pd.DataFrame:
    """Read COMID/FTYPE/WBAREACOMI from an NHDFlowline source (no geometry)."""
    return pyogrio.read_dataframe(
        flowline_path,
        columns=["COMID", "FTYPE", "WBAREACOMI"],
        read_geometry=False,
    )


def _base_url(dd: str, vpu: str) -> str:
    nested = {"03", "10", "05", "06", "07", "08", "11", "14", "15"}
    if any(code in vpu for code in nested):
        return f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlus{dd}/NHDPlus{vpu}"
    return f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlus{dd}"


def download_snapshot(dd: str, vpu: str, download_dir: Path, extract_dir: Path) -> Path | None:
    """Download + extract a VPU's NHDSnapshot; return the NHDFlowline.shp path."""
    base_url = _base_url(dd, vpu)
    local_path = None
    for version in _VERSION_CANDIDATES:
        filename = f"NHDPlusV21_{dd}_{vpu}_NHDSnapshot_{version}.7z"
        candidate = download_dir / filename
        url = f"{base_url}/{filename}"
        if candidate.exists():
            local_path = candidate
            break
        logger.info(f"Checking: {url}")
        if requests.head(url, timeout=60).status_code == 200:
            logger.info(f"Downloading {filename} ...")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(candidate, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            local_path = candidate
            break

    if local_path is None:
        logger.error(f"NHDSnapshot not found for VPU {vpu}")
        return None

    out_dir = extract_dir / vpu / "NHDSnapshot"
    out_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(local_path, mode="r") as archive:
        archive.extractall(path=out_dir)

    shps = list(out_dir.glob("**/NHDFlowline.shp"))
    if not shps:
        logger.error(f"NHDFlowline.shp not found in extracted snapshot for VPU {vpu}")
        return None
    return shps[0]


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    connected: set[int] = set()
    failures = []
    for vpu, dd in vpu_index.items():
        flowline = download_snapshot(dd, vpu, download_dir, extract_dir)
        if flowline is None:
            failures.append(vpu)
            continue
        df = read_flowline_attrs(flowline)
        vpu_connected = connected_comids_from_flowlines(df)
        logger.info(f"VPU {vpu}: {len(vpu_connected)} connected waterbody COMIDs")
        connected |= vpu_connected

    if failures:
        # A silently dropped VPU under-flags connectivity there — make it loud.
        raise RuntimeError(f"NHDSnapshot download/read failed for VPU(s): {failures}")

    out_path = data_root / "input/nhd/connected_waterbody_comids.parquet"
    write_connected_comids(connected, out_path)
    logger.info(f"Wrote {len(connected)} connected COMIDs -> {out_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test + import check**

Run: `pixi run -e dev pytest tests/test_download_nhd_flowlines.py -v`
Expected: PASS (3 passed).
Run: `pixi run --as-is python -c "import gfv2_params.download.nhd_flowlines"`
Expected: no output, exit 0.

- [ ] **Step 5: Create the SLURM batch**

```bash
# slurm_batch/download_nhd_flowlines.batch
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=download_nhd_flow
#SBATCH --output=logs/job_%j.out
#SBATCH --error=logs/job_%j.err
#SBATCH --time=12:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G

cd "$SLURM_SUBMIT_DIR"
pixi run --as-is python -m gfv2_params.download.nhd_flowlines
```

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/download/nhd_flowlines.py tests/test_download_nhd_flowlines.py slurm_batch/download_nhd_flowlines.batch
git commit -m "feat(nhd): per-VPU NHDSnapshot download -> connected-COMID parquet

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Connectivity join helper (pure, in depstor.py)

Given the waterbody GeoDataFrame and the connected-COMID set, select the connected polygons. Pure compute, lives with the other depstor helpers.

**Files:**
- Modify: `src/gfv2_params/depstor.py` (add two functions)
- Test: `tests/test_wbody_connectivity.py` (new file; helper tests here, builder test added in Task 5)

**Interfaces:**
- Produces:
  - `load_connected_comids(path: Path) -> set[int]` — read the parquet from Task 2 into a set of ints.
  - `select_connected_waterbodies(wb_gdf, connected: set[int]) -> GeoDataFrame` — return the subset of rows whose `COMID` **or** `member_comid` is in `connected`. `member_comid` is a single COMID string (equals `COMID` in 99.94% of rows; 280 multipart exceptions differ) — coerce to numeric and union the two membership tests.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_wbody_connectivity.py
"""Tests for WBAREACOMI-driven waterbody connectivity (helper + builder)."""

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from gfv2_params.depstor import load_connected_comids, select_connected_waterbodies


def _wb_gdf():
    geoms = [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])] * 4
    return gpd.GeoDataFrame(
        {
            "COMID": [10, 20, 30, 40],
            # row 3 is a multipart case: COMID 30 not connected, but its
            # member_comid 999 is.
            "member_comid": ["10", "20", "999", "40"],
        },
        geometry=geoms,
        crs="EPSG:5070",
    )


def test_select_connected_by_comid_or_member():
    out = select_connected_waterbodies(_wb_gdf(), {10, 999})
    assert sorted(out["COMID"].tolist()) == [10, 30]  # 10 by COMID, 30 by member


def test_select_connected_empty_set():
    out = select_connected_waterbodies(_wb_gdf(), set())
    assert len(out) == 0


def test_load_connected_comids(tmp_path):
    p = tmp_path / "c.parquet"
    pd.DataFrame({"comid": [5, 7, 9]}).to_parquet(p, index=False)
    assert load_connected_comids(p) == {5, 7, 9}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -v`
Expected: FAIL — `ImportError: cannot import name 'load_connected_comids'`.

- [ ] **Step 3: Write minimal implementation**

Add to `src/gfv2_params/depstor.py` (near the other vector/region helpers):

```python
def load_connected_comids(path: Path) -> set[int]:
    """Load the connected-waterbody COMID parquet into a set of ints."""
    df = pd.read_parquet(path, columns=["comid"])
    return {int(v) for v in df["comid"].to_numpy()}


def select_connected_waterbodies(wb_gdf, connected: set[int]):
    """Subset waterbodies whose COMID or member_comid is in `connected`."""
    comid = pd.to_numeric(wb_gdf["COMID"], errors="coerce")
    member = pd.to_numeric(wb_gdf["member_comid"], errors="coerce")
    mask = comid.isin(connected) | member.isin(connected)
    return wb_gdf[mask].copy()
```

> Ensure `import pandas as pd` and `from pathlib import Path` are present at the top of `depstor.py` (add if missing).

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor.py tests/test_wbody_connectivity.py
git commit -m "feat(depstor): connected-waterbody selection by COMID/member_comid

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Thread the connected-COMID table path through config + context

Make the connected-COMID parquet a profile-declared input, threaded into `BuildContext` like `waterbody_gpkg`.

**Files:**
- Modify: `configs/base_config.yml` (add `connected_comids_table` to the `gfv2`, `oregon`, `tjc` profiles that use `conus_waterbodies.gpkg`)
- Modify: `src/gfv2_params/depstor_builders/context.py:24-33` (add field)
- Modify: `scripts/build_depstor_rasters.py:74-89` (wire it)

**Interfaces:**
- Produces: `BuildContext.connected_comids_table: Path | None` — absolute path to the connected-COMID parquet, or `None` if the profile omits it.

- [ ] **Step 1: Add the profile key**

In `configs/base_config.yml`, under each profile that sets `waterbody_gpkg: "{data_root}/input/nhd/conus_waterbodies.gpkg"` (the `gfv2`, `oregon`, and `tjc` profiles), add directly beneath the `waterbody_layer` line:

```yaml
    # Connected-waterbody COMIDs distilled from NHD WBAREACOMI artificial paths
    # (see gfv2_params.download.nhd_flowlines). Drives wbody_connectivity.
    connected_comids_table: "{data_root}/input/nhd/connected_waterbody_comids.parquet"
```

> The VPU-01 `gfv2_vpu01` profile uses `NHM_01_draft.gpkg` (layer `wbs`), which has no COMID column; do **not** add the key there — `wbody_connectivity` will require it and that profile is not a target for this feature. (Validation runs use the `gfv2` profile.)

- [ ] **Step 2: Add the BuildContext field**

In `src/gfv2_params/depstor_builders/context.py`, add after the `waterbody_layer` field (line 27):

```python
    connected_comids_table: Path | None = None
```

- [ ] **Step 3: Wire it in the orchestrator**

In `scripts/build_depstor_rasters.py`, inside `_build_context`'s `BuildContext(...)` call (after the `waterbody_layer=` line, ~line 83), add:

```python
        connected_comids_table=(
            Path(config["connected_comids_table"])
            if config.get("connected_comids_table") else None
        ),
```

- [ ] **Step 4: Import + config sanity check**

Run: `pixi run --as-is python -c "from gfv2_params.depstor_builders.context import BuildContext; BuildContext(fabric='x', template_path='t', output_dir='o', hru_gpkg='h', hru_layer='l')"`
Expected: exit 0 (field defaults to None).
Run: `pixi run --as-is python -c "import yaml; yaml.safe_load(open('configs/base_config.yml'))"`
Expected: exit 0 (YAML still parses).

- [ ] **Step 5: Commit**

```bash
git add configs/base_config.yml src/gfv2_params/depstor_builders/context.py scripts/build_depstor_rasters.py
git commit -m "feat(depstor): thread connected_comids_table through profile + context

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `wbody_connectivity` builder + registration + config block

The new depstor step: rasterize the connected waterbody polygons to `connected_wbody.tif`.

**Files:**
- Create: `src/gfv2_params/depstor_builders/wbody_connectivity.py`
- Modify: `src/gfv2_params/depstor_builders/__init__.py` (register builder, STEP_ORDER, key table comment)
- Modify: `scripts/build_depstor_rasters.py:111-128` (`_expected_outputs` single-key map)
- Modify: `configs/depstor/depstor_rasters.yml` (add step block)
- Test: `tests/test_wbody_connectivity.py` (add builder test)

**Interfaces:**
- Consumes: `BuildContext.waterbody_gpkg`, `.waterbody_layer`, `.connected_comids_table`, `.template_path`; `ctx.require("landmask")`; `select_connected_waterbodies`, `load_connected_comids` (Task 3); `RasterInfo`, `rasterize_binary`, `read_land_mask`, `write_uint8_binary` from `..depstor`.
- Produces: registered output key `connected_wbody` → `connected_wbody.tif` (uint8: 1 = connected waterbody cell, 255 = nodata/off-land).

- [ ] **Step 1: Write the failing builder test**

```python
# add to tests/test_wbody_connectivity.py
import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from gfv2_params.depstor_builders.context import BuildContext
from gfv2_params.depstor_builders import wbody_connectivity


def _write_template(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="float32",
        crs="EPSG:5070", transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(np.full((n, n), 100.0, dtype=np.float32), 1)


def _write_landmask(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="uint8",
        crs="EPSG:5070", transform=transform, nodata=255,
    ) as dst:
        dst.write(np.ones((n, n), dtype=np.uint8), 1)  # all land


def test_wbody_connectivity_rasterizes_only_connected(tmp_path):
    from shapely.geometry import box

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    table = tmp_path / "connected.parquet"
    _write_template(template)
    _write_landmask(landmask)

    import geopandas as gpd
    import pandas as pd

    # Connected polygon (COMID 10) at top-left; disconnected (COMID 20) bottom-right.
    gdf = gpd.GeoDataFrame(
        {"COMID": [10, 20], "member_comid": ["10", "20"]},
        geometry=[box(0, 270, 60, 300), box(240, 0, 300, 30)],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")
    pd.DataFrame({"comid": [10]}).to_parquet(table, index=False)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=table,
    )
    ctx.paths["landmask"] = landmask

    produced = wbody_connectivity.build(
        {"output": "connected_wbody.tif"}, ctx, logging.getLogger("test")
    )

    out = produced["connected_wbody"]
    with rasterio.open(out) as src:
        arr = src.read(1)
        assert src.nodata == 255
    assert arr[0, 0] == 1     # connected polygon burned
    assert arr[9, 9] != 1     # disconnected polygon NOT burned
    assert int((arr == 1).sum()) > 0


def test_wbody_connectivity_requires_table(tmp_path):
    import pytest

    template = tmp_path / "template.tif"
    _write_template(template)
    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="waterbodies",
        waterbody_gpkg=tmp_path / "x.gpkg", waterbody_layer="waterbodies",
        connected_comids_table=None,
    )
    with pytest.raises(KeyError):
        wbody_connectivity.build({"output": "connected_wbody.tif"}, ctx, logging.getLogger("test"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -k connectivity -v`
Expected: FAIL — `ModuleNotFoundError: ... wbody_connectivity`.

- [ ] **Step 3: Write the builder**

```python
# src/gfv2_params/depstor_builders/wbody_connectivity.py
"""Rasterise the NHD-connected waterbody polygons to a uint8 binary mask.

Connectivity comes from NHD's WBAREACOMI artificial-path topology (staged by
gfv2_params.download.nhd_flowlines into a connected-COMID parquet), joined to the
waterbody polygons by COMID / member_comid. Replaces the old streambuffer mask as
the on-stream signal consumed by the dprst step.
"""

from __future__ import annotations

import geopandas as gpd

from ..depstor import (
    RasterInfo,
    load_connected_comids,
    rasterize_binary,
    read_land_mask,
    select_connected_waterbodies,
    write_uint8_binary,
)
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "wbody_connectivity step needs `waterbody_gpkg` and `waterbody_layer`."
        )
    if ctx.connected_comids_table is None:
        raise KeyError(
            "wbody_connectivity step needs `connected_comids_table` in the fabric "
            "profile. Stage it first: "
            "`python -m gfv2_params.download.nhd_flowlines`."
        )
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")

    logger.info("--- wbody_connectivity ---")
    logger.info("  Waterbody gpkg : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Connected table: %s", ctx.connected_comids_table)
    logger.info("  Output         : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output already exists — skipping (pass --force to rebuild)")
        return {"connected_wbody": output_path}

    if not ctx.connected_comids_table.exists():
        raise FileNotFoundError(
            f"Connected-COMID table not found: {ctx.connected_comids_table}. "
            f"Run `python -m gfv2_params.download.nhd_flowlines` first."
        )

    info = RasterInfo.from_path(ctx.template_path)
    connected = load_connected_comids(ctx.connected_comids_table)
    try:
        wb_gdf = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        wb_gdf = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer)

    if wb_gdf.crs != info.crs:
        logger.info("  Reprojecting wbodies from %s to %s", wb_gdf.crs, info.crs)
        wb_gdf = wb_gdf.to_crs(info.crs)
    wb_gdf = wb_gdf[wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty]

    sel = select_connected_waterbodies(wb_gdf, connected)
    logger.info(
        "  %d connected COMIDs; %d of %d waterbody polygons flagged connected",
        len(connected), len(sel), len(wb_gdf),
    )

    binary = rasterize_binary(sel, info, all_touched=False)
    binary[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    write_uint8_binary(binary, info, output_path)
    n_in = int((binary == 1).sum())
    logger.info("  %d connected-waterbody cells after land mask", n_in)

    return {"connected_wbody": output_path}
```

- [ ] **Step 4: Register the builder + STEP_ORDER**

In `src/gfv2_params/depstor_builders/__init__.py`:
- Add `wbody_connectivity` to the import line (line 17): `from . import carea_map, dprst, imperv, intersect, landmask, perv, routing, vpu_id, waterbody, wbody_connectivity` (note: `streambuffer` removed in Task 6).
- Add to `BUILDERS` (replace the `"streambuffer": streambuffer.build,` line): `"wbody_connectivity": wbody_connectivity.build,`
- In `STEP_ORDER`, replace `"streambuffer",` with `"wbody_connectivity",`.
- Update the key-table comment block: replace the `streambuffer -> "stream_buffer"` line with `wbody_connectivity -> "connected_wbody"      connected_wbody.tif (uint8, 1=connected)`.

> Task 6 performs the matching `streambuffer` removals; if executing strictly in order, leave the `streambuffer` import/entry intact here and do the swap in Task 6. Either way the end state has `wbody_connectivity` registered and `streambuffer` gone.

- [ ] **Step 5: Add `_expected_outputs` mapping**

In `scripts/build_depstor_rasters.py`, in the `single_key` dict (~line 120), add `"wbody_connectivity": "connected_wbody",` (and remove the `"streambuffer": "stream_buffer",` entry in Task 6).

- [ ] **Step 6: Add the config step block**

In `configs/depstor/depstor_rasters.yml`, replace the `streambuffer` block (lines 30-32) with:

```yaml
  - name: wbody_connectivity
    output: connected_wbody.tif
```

- [ ] **Step 7: Run tests + import check**

Run: `pixi run -e dev pytest tests/test_wbody_connectivity.py -v`
Expected: PASS (5 passed: 3 helper + 2 builder).
Run: `pixi run --as-is python -c "from gfv2_params.depstor_builders import BUILDERS, STEP_ORDER; assert 'wbody_connectivity' in BUILDERS and 'wbody_connectivity' in STEP_ORDER"`
Expected: exit 0.

- [ ] **Step 8: Commit**

```bash
git add src/gfv2_params/depstor_builders/wbody_connectivity.py src/gfv2_params/depstor_builders/__init__.py scripts/build_depstor_rasters.py configs/depstor/depstor_rasters.yml tests/test_wbody_connectivity.py
git commit -m "feat(depstor): wbody_connectivity builder (WBAREACOMI -> connected_wbody.tif)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Swap dprst.py to `connected_wbody` + retire streambuffer

Point `dprst` at the new mask and remove the streambuffer step entirely.

**Files:**
- Modify: `src/gfv2_params/depstor_builders/dprst.py:27,40,46,50-53`
- Modify: `src/gfv2_params/depstor_builders/__init__.py` (remove streambuffer import/BUILDERS/STEP_ORDER/comment — if not already done in Task 5)
- Modify: `scripts/build_depstor_rasters.py` (remove `streambuffer` from `single_key`)
- Modify: `src/gfv2_params/depstor_builders/context.py` (optional: drop now-unused `segments_gpkg`/`segments_layer`? **No** — leave them; other tooling/profiles still declare segments. Out of scope.)
- Test: any test referencing `stream_buffer` as a dprst input (search and update).

**Interfaces:**
- Consumes: registered key `connected_wbody` (Task 5) instead of `stream_buffer`.
- Produces: unchanged — `dprst`, `onstream`.

- [ ] **Step 1: Search for stream_buffer references**

Run: `grep -rn "stream_buffer\|streambuffer" src/ scripts/ configs/ tests/`
Expected: hits in `dprst.py`, `__init__.py`, `build_depstor_rasters.py`, `depstor_rasters.yml`, and `streambuffer.py` itself. Record each; all but `streambuffer.py` (the retired module, left on disk) must be updated.

- [ ] **Step 2: Edit dprst.py**

- Line 27: replace `stream_buffer_path = ctx.require("stream_buffer")` with `connected_path = ctx.require("connected_wbody")`.
- Line 40: replace `stream_binary = read_aligned_uint8(stream_buffer_path, info)` with `connected_binary = read_aligned_uint8(connected_path, info)`.
- Line 46: replace `stream_regions = regions_touching_mask(regions, stream_binary)` with `onstream_regions = regions_touching_mask(regions, connected_binary)`.
- Line 48: replace `excluded = stream_regions | imperv_regions` with `excluded = onstream_regions | imperv_regions`.
- Lines 50-53 logger: replace `%d touch stream` / `len(stream_regions)` with `%d touch connected wbody` / `len(onstream_regions)`.
- Update the module docstring (line 1) to `"""Combine wbody regions + connected-wbody mask + imperv into dprst + onstream."""`.

- [ ] **Step 3: Remove streambuffer from the registry (if not already)**

In `__init__.py`: drop `streambuffer` from the import line, the `BUILDERS` dict, `STEP_ORDER`, and the key-table comment. In `build_depstor_rasters.py`: remove `"streambuffer": "stream_buffer",` from `single_key`. In `depstor_rasters.yml`: ensure the streambuffer block is gone (done in Task 5 Step 6).

- [ ] **Step 4: Update/confirm tests**

If Step 1 found a test asserting `stream_buffer` as a dprst input, update it to provide `connected_wbody` instead. Then run the depstor suite:

Run: `pixi run -e dev pytest tests/ -k "depstor or dprst or wbody or connectivity" -v`
Expected: PASS (no references to a missing `stream_buffer` key).
Run: `pixi run --as-is python -c "from gfv2_params.depstor_builders import STEP_ORDER; assert 'streambuffer' not in STEP_ORDER"`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/dprst.py src/gfv2_params/depstor_builders/__init__.py scripts/build_depstor_rasters.py
git commit -m "feat(depstor): dprst consumes connected_wbody; retire streambuffer step

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Docs + memory updates

**Files:**
- Modify: `docs/ARCHITECTURE.md` (depstor step list / data-flow)
- Modify: `slurm_batch/RUNME.md` and `slurm_batch/HPC_REFERENCE.md` (add the NHD-flowline download step; replace streambuffer with wbody_connectivity in the depstor stage)
- Modify: `README.md` if it enumerates depstor steps or NHD inputs
- Modify: memory notes `dprst_connectivity_via_nhd_wbareacomi.md`, `depstor_segments_streambuffer_only.md`, and `MEMORY.md` index

- [ ] **Step 1: Audit the docs**

Run: `grep -rni "streambuffer\|stream buffer\|stream_buffer\|60 m\|60m\|WBAREACOMI" docs/ README.md slurm_batch/*.md`
Expected: a list of every place describing the old buffer step and any NHD-download enumeration.

- [ ] **Step 2: Update prose**

For each hit: replace the streambuffer description with the WBAREACOMI/`wbody_connectivity` mechanism (connected = NHD artificial path through the waterbody, joined by COMID; full-NHD scope; fabric segments no longer feed the depstor connectivity classification). Add the new staging command to the runbook before the depstor raster stage:

```bash
# Stage NHD-connected waterbody COMIDs (one-time, CONUS):
sbatch slurm_batch/download_nhd_flowlines.batch
```

- [ ] **Step 3: Update memory**

- In `dprst_connectivity_via_nhd_wbareacomi.md`: mark implemented — connectivity now comes from `download/nhd_flowlines.py` → `connected_waterbody_comids.parquet` → `wbody_connectivity` builder → `connected_wbody.tif`; join on `COMID`/`member_comid`; integration option A; streambuffer retired (resurrect from git if needed).
- In `depstor_segments_streambuffer_only.md`: note that the streambuffer step is retired and `segments_gpkg` no longer feeds any depstor step; depstor connectivity is now NHD-WBAREACOMI-driven.
- Reflect any one-line changes in `MEMORY.md` hooks.

- [ ] **Step 4: Commit**

```bash
git add docs/ README.md slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md
git commit -m "docs: NHD WBAREACOMI connectivity replaces streambuffer in depstor

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
# memory files are outside the repo; write them separately via the memory tool.
```

---

### Task 8: Pre-PR verification + validation run

**Files:** none (verification only)

- [ ] **Step 1: Lint + full local-safe checks**

Run: `pixi run -e dev pre-commit run --all-files`
Expected: all hooks pass (or auto-fix, then re-stage and amend the relevant commit).

- [ ] **Step 2: Full test suite (CI is the gate; this is a local dry check if not on the head node)**

Run (only off the HPC head node): `pixi run -e dev pytest tests/ -q`
Expected: PASS. On the head node, skip and rely on CI.

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feat/wbareacomi-connectivity
gh pr create --title "feat(depstor): NHD WBAREACOMI waterbody connectivity (retire streambuffer)" --body "<summary + spec/plan links + scope-expansion callout if any>"
```

- [ ] **Step 4: CONUS validation (after merge-ready)**

Once `connected_waterbody_comids.parquet` is staged, run the `gfv2` depstor stack and compare `dprst_frac` / `onstream_storage_frac` against the prior buffer-based outputs on a small fabric; spot-check known on-stream impoundments vs. isolated ponds. (Tracked in the spec's "Validation after first run".)

---

## Self-Review

**Spec coverage:**
- Component 1 (download module) → Tasks 1, 2. ✓
- Component 2 (connected-COMID table) → Task 2 (`write_connected_comids`, `main`). ✓
- Component 3 (`wbody_connectivity` builder) → Tasks 3 (helpers), 4 (config/context wiring), 5 (builder). ✓
- Component 4 (modify dprst + retire streambuffer) → Task 6. ✓
- Component 5 (docs + memory) → Task 7. ✓
- Error handling (missing table fail-fast, WBAREACOMI=0 sentinel, download VPU failure loud, coverage logging) → Task 2 `main` RuntimeError, Task 1 sentinel, Task 5 fail-fast + count logging. ✓
- Decisions (medium-res, full-NHD, build download, integration A) → reflected throughout; no segment↔COMID reconciliation introduced. ✓
- Validation → Task 8 Step 4. ✓

**Placeholder scan:** No TBD/TODO/"handle edge cases" — the one open item (exact NHDSnapshot archive name) is called out as a pre-coding verification with a concrete fallback (HEAD-probe version candidates + glob), not a deferred design hole.

**Type consistency:** `connected_comids_from_flowlines(df)->set[int]`, `write_connected_comids(set,Path)`, `load_connected_comids(Path)->set[int]`, `select_connected_waterbodies(gdf,set)->gdf`, builder output key `connected_wbody` — names match across Tasks 1–6. `dprst.py` consumes `connected_wbody` (Task 5 produces it, Task 6 requires it). Profile key `connected_comids_table` consistent across config/context/orchestrator/builder.
