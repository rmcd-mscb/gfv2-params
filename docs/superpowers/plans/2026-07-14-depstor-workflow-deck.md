# Depression-Storage Workflow Deck (v2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `docs/presentations/2026-07-depression-storage-workflow.slides.md` as a ~27-slide technical design review that presents each depression-storage classification rule as a rule and shows it firing on real hydrography — including PR #178 (endorheic classifier), which the current deck contradicts.

**Architecture:** One rewritten renderer, `scripts/render_depstor_figures.py`, built around a `tile(comid, ...)` workhorse that composites four layers (classification raster, waterbody outline, network-colored flowlines, FDR code-0 terminal cells) for any named waterbody. Pure, testable helpers are separated from the matplotlib drawing so CI can gate them. The deck is Marp markdown referencing the emitted PNGs.

**Tech Stack:** Python (rasterio, geopandas, pyogrio, matplotlib, pandas), Marp for the deck, pixi for the environment, SLURM (`srun`) for the render.

**Spec:** `docs/superpowers/specs/2026-07-14-depstor-workflow-deck-design.md`

## Global Constraints

These apply to **every** task. They are hard-won repo rules (CLAUDE.md); violating them silently corrupts outputs or wastes hours.

- **Worktree:** all work happens in `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params-deck` on branch `docs/depstor-deck-v2`. Derive absolute paths from `git rev-parse --show-toplevel`, never from a hardcoded `/caldera/.../gfv2-params` path (that is the *other* checkout).
- **NEVER load a full-grid CONUS array.** The template is 153,830 × 109,901 ≈ 16.9 B cells. Every raster read is windowed (`rasterio.windows.from_bounds`) AND decimated (`out_shape`). No exceptions, including the CONUS-wide figure.
- **Never run heavy compute on the HPC login node.** Every figure render goes through `srun --account=impd`. Quick `py_compile` / import checks on the login node are fine.
- **Never run `pytest` on the login node.** CI (`.github/workflows/ci.yml`) is the test gate. To check a test locally, run it under `srun`.
- **Paths come from the fabric profile**, read via `load_config(...)` + `require_config_key(...)`. Never hardcode a `/caldera/...` data path in committed code. The one permitted exception is the existing `_FALLBACK_DATA_ROOT` pattern already in the renderer.
- **Waterbody geometry comes from the profile's `waterbody_gpkg`** (`conus_waterbodies.gpkg`, layer `waterbodies`), NOT `nhd_waterbodies.parquet`. The rasters were built from the former. GSL is 4,368.9 km² in the gpkg vs. 4,309.7 km² in the parquet; the vetoing marsh is 49.1 vs. 38.7 km². Using the parquet would misalign outlines with pixels and contradict PR #178's own numbers.
- **NHDFlowline field casing varies by VPU.** VPU 16 ships `ComID`/`WBAreaComI`/`FCode`; VPUs 01/08 ship `COMID`/`WBAREACOMI`/`FCODE`. Upper-case all field names on read. Flowlines are **EPSG:4269** and must be reprojected to **EPSG:5070** (the raster CRS, which the waterbody gpkg is already in).
- **`pixi run --as-is`** for anything under SLURM (no env mutation, no lock check).
- Run `pixi run -e dev pre-commit run --all-files` before the final push.

## Key facts (verified on disk 2026-07-14 — do not re-derive, do not guess)

| Fact | Value |
|---|---|
| before rasters | `{data_root}/gfv2/depstor_rasters_pre_endorheic_2026-07-13/` |
| after rasters | `{data_root}/gfv2/depstor_rasters/` |
| `data_root` | `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2` |
| endorheic table | `{after}/endorheic_waterbody_comids.parquet` — 22,970 rows; cols `comid`, `frac_own`, `by_terminus`, `by_closed_huc12` |
| endorheic breakdown | by_terminus 6,364 · by_closed_huc12 21,503 · both 4,925 · Signal-A candidates (`frac_own > 0`) 6,427 |
| waterbody layer | `{data_root}/input/nhd/conus_waterbodies.gpkg`, layer `waterbodies` — 448,124 rows, **EPSG:5070**, fields `GNIS_ID, GNIS_NAME, COMID, FTYPE, member_comid, area_sqkm` |
| FTYPE counts | LakePond 373,503 · SwampMarsh 66,488 · Reservoir 5,224 · Playa 1,684 · Ice Mass 1,220 · Estuary 4 |
| flowlines | `{data_root}/shared/source/{vpu}/NHDSnapshot/**/Hydrography/NHDFlowline.shp` (glob — the middle dirs vary, e.g. `NHDPlusGB/NHDPlus16/`), EPSG:4269 |
| Network membership | `{data_root}/input/nhd/flowline_topology.parquet` — 2,691,339 rows, col `comid` |
| BurnAdd | `{data_root}/input/nhd/burn_add_waterbodies.parquet` — 1,658 rows; FTYPE: LakePond 1,550 · Playa 103 · SwampMarsh 5 |
| closed HUC12s | `{data_root}/input/wbd/wbd_huc12.parquet` — 1,142 rows, cols `HUC_12`, `HU_12_TYPE`, `geometry` (already filtered to type C) |
| FDR | profile key `fdr_raster` → `{data_root}/gfv2/shared/gfv2_fdr.vrt`; **code 0 = terminal cell** |

### Example COMIDs (verified: `frac_own`, flags, area, EPSG:5070 bounds)

| COMID | Name | frac_own | terminus | huc12 | FTYPE | km² |
|---|---|---|---|---|---|---|
| 946020001 | Great Salt Lake | **1.000** | ✔ | ✔ | LakePond | 4,368.9 |
| 10273192 | *the vetoing SwampMarsh* | — | ✘ | ✘ | SwampMarsh | **49.1** |
| 948100002 | Salton Sea | 1.000 | ✔ | ✘ | LakePond | 944.0 |
| 11310757 | Pyramid Lake | 1.000 | ✔ | ✔ | LakePond | 463.0 |
| 120053921 | Mono Lake | 1.000 | ✔ | ✘ | LakePond | 161.1 |
| **10734232** | **Walker Lake** | **0.000** | **✘** | **✔** | LakePond | 143.8 |
| 24040174 | Lake Abert | 0.990 | ✔ | ✔ | LakePond | 164.0 |
| **11758154** | **Lewis and Clark Lake** | **0.007** | ✘ | ✘ | LakePond | 120.5 |
| 904140248 | Lake Michigan | — | ✘ | ✘ | LakePond | 57,516.6 |
| 15447630 | Lake Champlain | — | ✘ | ✘ | LakePond | 664.1 |
| 120055431 | Everglades SwampMarsh | — | ✘ | ✘ | SwampMarsh | 960.1 |
| 120050227 | *largest Playa* | — | — | — | Playa | 490.1 |
| 120050242 | *largest Ice Mass* | — | — | — | Ice Mass | 72.0 |

**Walker Lake is the Signal-B slide** (`frac_own = 0.000` — Signal A alone misses it entirely).
**Lewis and Clark is the Signal-A negative control** (`frac_own = 0.007` — its terminus is the Gulf).

## File Structure

| File | Responsibility |
|---|---|
| `scripts/render_depstor_figures.py` | **Rewrite.** Renderer. Split internally: pure helpers (importable, tested) → data readers → `tile()` compositor → per-figure functions → `main()` with `--only` selection. |
| `tests/test_render_depstor_figures.py` | **Create.** Gates the pure helpers: field-casing normalization, category-array precedence, `frac_own` stats. No I/O, no HPC data. |
| `docs/presentations/2026-07-depression-storage-workflow.slides.md` | **Rewrite.** The 27-slide deck. |
| `docs/presentations/README.md` | **Modify.** Update the deck's description bullet. |
| `docs/figures/depstor/*.png` | **Regenerate.** 14 figures in, 2 stale ones deleted. |

---

### Task 1: Renderer skeleton + tested pure helpers

Establishes the testable core before any matplotlib. The three helpers here are where the real correctness hazards live (per-VPU field casing, class precedence, the threshold sweep).

**Files:**
- Modify: `scripts/render_depstor_figures.py` (full rewrite; currently 347 lines / 4 figures)
- Create: `tests/test_render_depstor_figures.py`

**Interfaces:**
- Consumes: `gfv2_params.config.load_config`, `require_config_key`
- Produces, for all later tasks:
  - `normalize_fields(gdf) -> GeoDataFrame` — upper-cases all non-geometry column names
  - `classification_array(dprst, dprst_nodata, onstream, onstream_nodata) -> np.ndarray` (uint8: 0 land, 1 dprst, 2 on-stream)
  - `frac_own_stats(df) -> dict` with keys `candidates`, `at_or_above_95`, `in_band_45_55`, `sweep` (dict of threshold→count), `swing`
  - `paths(fabric="gfv2") -> dict` with keys `data_root`, `before`, `after`, `waterbody_gpkg`, `waterbody_layer`, `fdr`, `topology`, `burn_add`, `huc12`, `endorheic`, `source_root`
  - `OUT` — `Path` to `docs/figures/depstor/`

- [ ] **Step 1: Write the failing test**

Create `tests/test_render_depstor_figures.py`:

```python
"""Gate the pure helpers in scripts/render_depstor_figures.py.

These are the renderer's correctness hazards: NHDFlowline field casing varies
by VPU (16 ships `ComID`, 01/08 ship `COMID`), the land/dprst/on-stream class
precedence must put dprst last, and the frac_own threshold sweep is the deck's
"0.5 is not a tuned knob" claim.

No HPC data, no I/O — CI runs these.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import LineString

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_renderer():
    """Import the renderer by path (it lives in scripts/, not the package)."""
    path = REPO_ROOT / "scripts" / "render_depstor_figures.py"
    spec = importlib.util.spec_from_file_location("render_depstor_figures", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["render_depstor_figures"] = module
    spec.loader.exec_module(module)
    return module


rdf = _load_renderer()


def test_normalize_fields_uppercases_vpu16_casing():
    """VPU 16 ships ComID/WBAreaComI; VPUs 01/08 ship COMID/WBAREACOMI."""
    gdf = gpd.GeoDataFrame(
        {"ComID": [1], "WBAreaComI": [7], "FCode": [46006]},
        geometry=[LineString([(0, 0), (1, 1)])],
        crs="EPSG:4269",
    )
    out = rdf.normalize_fields(gdf)
    assert set(out.columns) == {"COMID", "WBAREACOMI", "FCODE", "geometry"}
    assert out["COMID"].iloc[0] == 1
    assert out.crs == gdf.crs


def test_normalize_fields_is_idempotent_on_uppercase_vpus():
    gdf = gpd.GeoDataFrame(
        {"COMID": [1], "WBAREACOMI": [7]},
        geometry=[LineString([(0, 0), (1, 1)])],
        crs="EPSG:4269",
    )
    out = rdf.normalize_fields(gdf)
    assert set(out.columns) == {"COMID", "WBAREACOMI", "geometry"}


def test_classification_array_dprst_wins_over_onstream():
    """A cell flagged in both masks is dprst — dprst is written last.

    This mirrors the product: endorheic cells recovered by the clump-veto
    exemption are dprst even where the region touches the on-stream mask.
    """
    dprst = np.array([[0, 1, 1]], dtype=np.uint8)
    onstream = np.array([[0, 0, 1]], dtype=np.uint8)
    cat = rdf.classification_array(dprst, 0, onstream, 0)
    assert cat.tolist() == [[0, 1, 1]]


def test_classification_array_marks_land_dprst_onstream():
    dprst = np.array([[255, 1, 255]], dtype=np.uint8)
    onstream = np.array([[255, 255, 1]], dtype=np.uint8)
    cat = rdf.classification_array(dprst, 255, onstream, 255)
    assert cat.tolist() == [[0, 1, 2]]


def test_frac_own_stats_reports_bimodality_and_sweep():
    """The deck claims frac_own is bimodal and the 0.5 threshold is inert."""
    df = pd.DataFrame(
        {
            "comid": [1, 2, 3, 4, 5, 6],
            # 4 high, 1 mid-band, 1 zero (a Signal-B-only waterbody)
            "frac_own": [1.0, 1.0, 0.99, 0.96, 0.50, 0.0],
            "by_terminus": [True, True, True, True, False, False],
            "by_closed_huc12": [False, False, False, False, False, True],
        }
    )
    stats = rdf.frac_own_stats(df)
    # Candidates = waterbodies with a computed frac_own (> 0), not all rows.
    assert stats["candidates"] == 5
    assert stats["at_or_above_95"] == 4
    assert stats["in_band_45_55"] == 1
    assert stats["sweep"] == {0.3: 5, 0.5: 4, 0.7: 4}
    assert stats["swing"] == pytest.approx(0.25)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "$(git rev-parse --show-toplevel)"
srun --account=impd --time=10 --mem=8G pixi run --as-is python -m pytest tests/test_render_depstor_figures.py -v
```
Expected: FAIL — `AttributeError: module 'render_depstor_figures' has no attribute 'normalize_fields'`.

- [ ] **Step 3: Rewrite the renderer's head with the helpers**

Replace the whole of `scripts/render_depstor_figures.py` with this skeleton. Later tasks append figure functions; **do not** delete the old figure functions' logic for `pipeline_dag` / `lower_miss_before_after` until Tasks 4 and 5 re-add them (keep a copy — `git show HEAD:scripts/render_depstor_figures.py`).

```python
"""Render the depression-storage workflow figures for the presentation deck.

Emits the 14 PNGs under ``docs/figures/depstor/`` that back
``docs/presentations/2026-07-depression-storage-workflow.slides.md``.

The deck is rule-first: each classification rule gets a real map tile at a named
waterbody, with the evidence the rule actually reads drawn on top. The workhorse
is ``tile()``, which composites four layers for any COMID:

1. the land / dprst / on-stream classification raster,
2. the waterbody outline (from the profile's ``waterbody_gpkg`` -- see below),
3. NHD flowlines colored by Network vs. Non-Network membership,
4. FDR code-0 (terminal) cells, which is what Signal A actually reads.

Three data gotchas this module exists to respect
------------------------------------------------
**Waterbody geometry comes from the profile's ``waterbody_gpkg``**
(``conus_waterbodies.gpkg``), not ``nhd_waterbodies.parquet``. The rasters were
built from the former; the latter is staged-from-source but not yet wired into
the profile. Their shorelines differ (Great Salt Lake: 4,368.9 vs. 4,309.7 km2),
so drawing from the parquet would misalign outlines with pixels.

**NHDFlowline field casing varies by VPU** -- VPU 16 ships ``ComID`` /
``WBAreaComI``; VPUs 01 and 08 ship ``COMID`` / ``WBAREACOMI``. Everything is
upper-cased on read (the same gotcha ``download/nhd_flowlines.py`` handles).
Flowlines are EPSG:4269 and are reprojected to the raster CRS, EPSG:5070.

**Never load a full-grid array.** The CONUS template is 153,830 x 109,901 ~ 16.9
billion cells (CLAUDE.md's CONUS-memory rule). Every read here is windowed to a
bounding box AND decimated via ``out_shape``, so GDAL streams a small array
rather than materializing the window.

Run (under SLURM -- never on the login node):

    srun --account=impd --time=60 --mem=64G \\
        pixi run --as-is python scripts/render_depstor_figures.py

    # or a single figure while iterating:
    srun ... python scripts/render_depstor_figures.py --only rule_terminus_gsl
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import argparse  # noqa: E402
import glob  # noqa: E402
from pathlib import Path  # noqa: E402

import geopandas as gpd  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import rasterio  # noqa: E402
from matplotlib.colors import ListedColormap  # noqa: E402
from rasterio.windows import from_bounds  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT = REPO_ROOT / "docs" / "figures" / "depstor"

RASTER_CRS = "EPSG:5070"

# The "before" snapshot: the CONUS product as it stood before the endorheic
# classifier (PR #178) landed. Kept alongside the live product on disk.
BEFORE_DIRNAME = "depstor_rasters_pre_endorheic_2026-07-13"

# Target max array side for every decimated read. A VPU window is tens of
# thousands of native 30 m cells per side; this keeps the read -- and the
# in-memory array -- small regardless of window size.
_MAX_SIDE = 900

_FALLBACK_DATA_ROOT = "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2"


# --------------------------------------------------------------------------
# Pure helpers (no I/O -- gated by tests/test_render_depstor_figures.py)
# --------------------------------------------------------------------------


def normalize_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Upper-case every non-geometry column name.

    NHDPlus ships different field casing per VPU: VPU 16's NHDFlowline has
    ``ComID`` / ``WBAreaComI`` / ``FCode``, while VPUs 01 and 08 have ``COMID``
    / ``WBAREACOMI`` / ``FCODE``. Callers index by the upper-case name.
    """
    renames = {c: c.upper() for c in gdf.columns if c != gdf.geometry.name}
    return gdf.rename(columns=renames)


def classification_array(dprst, dprst_nodata, onstream, onstream_nodata) -> np.ndarray:
    """Composite a 0/1/2 categorical array: land / dprst / on-stream.

    dprst is written LAST and therefore wins a tie. That mirrors the product:
    the clump-veto exemption recovers an endorheic waterbody's own cells as
    dprst even where its 8-connected region touches the on-stream mask.
    """
    cat = np.zeros(dprst.shape, dtype=np.uint8)
    cat[onstream != onstream_nodata] = 2
    cat[dprst != dprst_nodata] = 1
    return cat


def frac_own_stats(df: pd.DataFrame) -> dict:
    """Summarise the Signal-A distribution behind the deck's bimodality claim.

    ``candidates`` counts waterbodies with a computed ``frac_own`` (> 0) -- not
    every row in the table, most of which are Signal-B-only (flagged by a closed
    HUC12, never evaluated for a terminus).

    ``swing`` is how much the answer moves across a 0.3 -> 0.7 threshold sweep,
    relative to the count at 0.5. A small swing is the evidence that 0.5 is not
    a tuned knob.
    """
    candidates = df[df["frac_own"] > 0]
    sweep = {t: int((df["frac_own"] > t).sum()) for t in (0.3, 0.5, 0.7)}
    swing = (sweep[0.3] - sweep[0.7]) / max(sweep[0.5], 1)
    return {
        "candidates": int(len(candidates)),
        "at_or_above_95": int((df["frac_own"] >= 0.95).sum()),
        "in_band_45_55": int(df["frac_own"].between(0.45, 0.55).sum()),
        "sweep": sweep,
        "swing": swing,
    }


# --------------------------------------------------------------------------
# Config-driven paths
# --------------------------------------------------------------------------


def paths(fabric: str = "gfv2") -> dict:
    """Resolve every input path from the active fabric profile.

    Paths live in the profile (CLAUDE.md: never hardcode a data path). Falls
    back to the known data root only for a config-less checkout, matching the
    pattern this script already used.
    """
    try:
        from gfv2_params.config import load_config, require_config_key

        cfg = load_config(
            REPO_ROOT / "configs" / "depstor" / "depstor_rasters.yml", fabric=fabric
        )
        data_root = Path(cfg["data_root"])
        after = Path(cfg["output_dir"])
        waterbody_gpkg = Path(require_config_key(cfg, "waterbody_gpkg", "render_depstor_figures"))
        waterbody_layer = require_config_key(cfg, "waterbody_layer", "render_depstor_figures")
        fdr = Path(require_config_key(cfg, "fdr_raster", "render_depstor_figures"))
    except Exception:  # pragma: no cover - config-less checkout
        data_root = Path(_FALLBACK_DATA_ROOT)
        after = data_root / "gfv2" / "depstor_rasters"
        waterbody_gpkg = data_root / "input" / "nhd" / "conus_waterbodies.gpkg"
        waterbody_layer = "waterbodies"
        fdr = data_root / "gfv2" / "shared" / "gfv2_fdr.vrt"

    return {
        "data_root": data_root,
        "after": after,
        "before": after.parent / BEFORE_DIRNAME,
        "waterbody_gpkg": waterbody_gpkg,
        "waterbody_layer": waterbody_layer,
        "fdr": fdr,
        "endorheic": after / "endorheic_waterbody_comids.parquet",
        "topology": data_root / "input" / "nhd" / "flowline_topology.parquet",
        "burn_add": data_root / "input" / "nhd" / "burn_add_waterbodies.parquet",
        "huc12": data_root / "input" / "wbd" / "wbd_huc12.parquet",
        "source_root": data_root / "shared" / "source",
    }


# --------------------------------------------------------------------------
# Readers (all windowed / bbox-filtered -- never a full read)
# --------------------------------------------------------------------------


def read_window(path: Path, bbox, max_side: int = _MAX_SIDE):
    """Read *path* windowed to *bbox*, decimated so neither side exceeds *max_side*.

    Uses ``out_shape`` so GDAL decimates while reading and never materializes
    the full-resolution window in memory.
    """
    minx, miny, maxx, maxy = bbox
    with rasterio.open(path) as ds:
        win = from_bounds(minx, miny, maxx, maxy, ds.transform)
        win_h, win_w = int(round(win.height)), int(round(win.width))
        scale = max(1, win_h // max_side, win_w // max_side)
        out_h, out_w = max(1, win_h // scale), max(1, win_w // scale)
        arr = ds.read(1, window=win, out_shape=(1, out_h, out_w))
        return arr, ds.nodata


def read_classification(depstor_dir: Path, bbox) -> np.ndarray:
    """land / dprst / on-stream categorical array, windowed to *bbox*."""
    dprst, dprst_nodata = read_window(depstor_dir / "dprst_binary.tif", bbox)
    onstream, onstream_nodata = read_window(depstor_dir / "onstream_binary.tif", bbox)
    return classification_array(dprst, dprst_nodata, onstream, onstream_nodata)


def read_waterbodies(comids: list[int] | None = None, bbox=None) -> gpd.GeoDataFrame:
    """Read the profile's waterbody layer, filtered by COMID or bbox.

    Never reads all 448,124 rows: pyogrio pushes both the ``where`` clause and
    the ``bbox`` down to OGR.
    """
    p = paths()
    where = None
    if comids:
        where = "COMID IN (" + ",".join(str(int(c)) for c in comids) + ")"
    gdf = gpd.read_file(
        p["waterbody_gpkg"], layer=p["waterbody_layer"], where=where, bbox=bbox
    )
    return normalize_fields(gdf)


def read_flowlines(vpu: str, bbox) -> gpd.GeoDataFrame:
    """Read NHDFlowline for *vpu*, bbox-filtered, with a ``network`` bool column.

    ``network`` is membership in ``flowline_topology.parquet`` (NHDPlus
    PlusFlowlineVAA). Non-Network flowlines are the cartographic artificial
    paths NHD draws through essentially every closed-basin lake -- the ones the
    #161 gate exists to ignore. Reprojected from EPSG:4269 to the raster CRS.
    """
    p = paths()
    pattern = str(p["source_root"] / vpu / "NHDSnapshot" / "**" / "Hydrography" / "NHDFlowline.shp")
    hits = glob.glob(pattern, recursive=True)
    if not hits:
        raise FileNotFoundError(f"No NHDFlowline.shp for VPU {vpu} under {p['source_root']}")

    # bbox is in EPSG:5070; the shapefile is EPSG:4269. Convert the box, don't
    # reproject the layer (that would read all of it).
    box_4269 = (
        gpd.GeoSeries.from_wkt([f"POLYGON(({bbox[0]} {bbox[1]},{bbox[2]} {bbox[1]},"
                                f"{bbox[2]} {bbox[3]},{bbox[0]} {bbox[3]},{bbox[0]} {bbox[1]}))"],
                               crs=RASTER_CRS)
        .to_crs("EPSG:4269")
        .total_bounds
    )
    gdf = normalize_fields(gpd.read_file(hits[0], bbox=tuple(box_4269)))
    topo = pd.read_parquet(p["topology"], columns=["comid"])
    network = set(topo["comid"].astype("int64"))
    gdf["network"] = gdf["COMID"].astype("int64").isin(network)
    return gdf.to_crs(RASTER_CRS)


def read_terminal_cells(bbox) -> tuple[np.ndarray, np.ndarray]:
    """Return (x, y) coords of FDR code-0 (terminal) cells inside *bbox*.

    Code 0 is what makes Signal A possible: the NHDPlus FdrFac is depression-
    filled EVERYWHERE EXCEPT at NHDPlus's own sinks, which it leaves unfilled by
    design. Those 15,262 code-0 cells ARE the sink set, and ``d8_routing``
    already treats code 0 as a terminus -- so the classifier and the router read
    the same grid.
    """
    p = paths()
    minx, miny, maxx, maxy = bbox
    with rasterio.open(p["fdr"]) as ds:
        win = from_bounds(minx, miny, maxx, maxy, ds.transform)
        # Terminal cells are sparse and single-pixel -- decimating would drop
        # them, so read this window at FULL resolution. Safe because tiles are
        # single-waterbody windows, not CONUS. Callers must not pass a CONUS bbox.
        arr = ds.read(1, window=win)
        transform = ds.window_transform(win)
    rows, cols = np.nonzero(arr == 0)
    xs, ys = rasterio.transform.xy(transform, rows, cols)
    return np.asarray(xs), np.asarray(ys)


# --------------------------------------------------------------------------
# Styling
# --------------------------------------------------------------------------

CLASS_CMAP = ListedColormap(["#f0f0f0", "#3182bd", "#e6550d"])  # land, dprst, on-stream
CLASS_LABELS = ["land", "depression storage (dprst)", "on-stream waterbody"]

NETWORK_COLOR = "#08519c"      # Network Flowline -- counts as connectivity
NONNETWORK_COLOR = "#cc44aa"   # Non-Network cartographic path -- does NOT
TERMINUS_COLOR = "#111111"     # FDR code-0 terminal cell


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", help="render just this figure (stem, no .png)")
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    # CONTRACT: each key is exactly the PNG stem the function writes, so
    # `--only <stem>` always matches the filename the deck references.
    figures = {}  # populated by later tasks
    if args.only:
        if args.only not in figures:
            raise SystemExit(f"Unknown figure {args.only!r}. Known: {sorted(figures)}")
        figures = {args.only: figures[args.only]}
    for fn in figures.values():
        print(fn())


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd "$(git rev-parse --show-toplevel)"
srun --account=impd --time=10 --mem=8G pixi run --as-is python -m pytest tests/test_render_depstor_figures.py -v
```
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add scripts/render_depstor_figures.py tests/test_render_depstor_figures.py
git commit -m "feat(viz): renderer skeleton + tested helpers for the depstor deck

Pure helpers gate the three correctness hazards: per-VPU NHDFlowline field
casing (VPU 16 ships ComID, 01/08 ship COMID), land/dprst/on-stream class
precedence, and the frac_own threshold sweep behind the deck's
'0.5 is not a tuned knob' claim.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: The `tile()` workhorse + the Signal-A figure

Proves the whole compositing stack end-to-end on the deck's marquee figure before scaling to 13 more.

**Files:**
- Modify: `scripts/render_depstor_figures.py`

**Interfaces:**
- Consumes: everything from Task 1.
- Produces:
  - `waterbody_bbox(gdf, pad_frac=0.35) -> tuple` — padded EPSG:5070 bounds
  - `draw_tile(ax, bbox, depstor_dir, *, outlines=None, vpu=None, show_terminals=False, title=None)` — composites the four layers onto an axis
  - `fig_terminus_gsl() -> Path` — writes `rule_terminus_gsl.png`

- [ ] **Step 1: Add the compositor and the figure**

Append to `scripts/render_depstor_figures.py` (before `main`):

```python
# --------------------------------------------------------------------------
# The tile compositor
# --------------------------------------------------------------------------


def waterbody_bbox(gdf: gpd.GeoDataFrame, pad_frac: float = 0.35) -> tuple:
    """Padded EPSG:5070 bounds around *gdf*, so context is visible around it."""
    minx, miny, maxx, maxy = gdf.total_bounds
    pad = max(maxx - minx, maxy - miny) * pad_frac
    return (minx - pad, miny - pad, maxx + pad, maxy + pad)


def draw_tile(
    ax,
    bbox,
    depstor_dir: Path,
    *,
    outlines: gpd.GeoDataFrame | None = None,
    vpu: str | None = None,
    show_terminals: bool = False,
    title: str | None = None,
) -> None:
    """Composite the classification raster + outlines + flowlines + terminals."""
    cat = read_classification(depstor_dir, bbox)
    ax.imshow(
        cat,
        cmap=CLASS_CMAP,
        vmin=0,
        vmax=2,
        interpolation="nearest",
        extent=(bbox[0], bbox[2], bbox[1], bbox[3]),
        origin="upper",
    )

    if vpu is not None:
        fl = read_flowlines(vpu, bbox)
        net = fl[fl["network"]]
        non = fl[~fl["network"]]
        if len(non):
            non.plot(ax=ax, color=NONNETWORK_COLOR, linewidth=1.4, linestyle="--", zorder=3)
        if len(net):
            net.plot(ax=ax, color=NETWORK_COLOR, linewidth=1.0, zorder=4)

    if outlines is not None and len(outlines):
        outlines.boundary.plot(ax=ax, color="black", linewidth=0.9, zorder=5)

    if show_terminals:
        xs, ys = read_terminal_cells(bbox)
        if len(xs):
            ax.scatter(xs, ys, s=14, c=TERMINUS_COLOR, marker="x", linewidths=1.1, zorder=6)

    ax.set_xlim(bbox[0], bbox[2])
    ax.set_ylim(bbox[1], bbox[3])
    ax.set_xticks([])
    ax.set_yticks([])
    if title:
        ax.set_title(title, fontsize=11)


def _legend_handles(*, flowlines: bool = False, terminals: bool = False) -> list:
    import matplotlib.lines as mlines
    import matplotlib.patches as mpatches

    handles = [
        mpatches.Patch(color=CLASS_CMAP.colors[i], label=CLASS_LABELS[i]) for i in range(3)
    ]
    if flowlines:
        handles += [
            mlines.Line2D([], [], color=NETWORK_COLOR, lw=1.6, label="Network Flowline"),
            mlines.Line2D(
                [], [], color=NONNETWORK_COLOR, lw=1.6, ls="--",
                label="Non-Network path (cartographic)",
            ),
        ]
    if terminals:
        handles += [
            mlines.Line2D(
                [], [], color=TERMINUS_COLOR, marker="x", ls="none",
                label="FDR code-0 terminal cell",
            )
        ]
    return handles


# --------------------------------------------------------------------------
# Figures
# --------------------------------------------------------------------------

GREAT_SALT_LAKE = 946020001
LEWIS_AND_CLARK = 11758154


def fig_terminus_gsl() -> Path:
    """Signal A: the terminus-inside-itself rule, and its negative control.

    Great Salt Lake's water ends IN Great Salt Lake (frac_own = 1.000): its FDR
    code-0 terminal cells sit inside its own polygon. Lewis and Clark Lake -- a
    Missouri mainstem reservoir with one stray terminal cell -- ends in the Gulf
    of Mexico (frac_own = 0.007), so it stays on-stream. The rule is
    "terminus INSIDE ITSELF", not merely "terminates at a sink": the latter
    would demote every on-stream reservoir in the Great Basin.
    """
    p = paths()
    end = pd.read_parquet(p["endorheic"])

    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    for ax, comid, vpu, name in (
        (axes[0], GREAT_SALT_LAKE, "16", "Great Salt Lake"),
        (axes[1], LEWIS_AND_CLARK, "10L", "Lewis and Clark Lake"),
    ):
        wb = read_waterbodies(comids=[comid])
        row = end[end["comid"] == comid]
        frac = float(row["frac_own"].iloc[0]) if len(row) else 0.0
        verdict = "dprst" if frac > 0.5 else "on-stream"
        draw_tile(
            ax,
            waterbody_bbox(wb),
            p["after"],
            outlines=wb,
            vpu=vpu,
            show_terminals=True,
            title=f"{name}\nfrac_own = {frac:.3f}  →  {verdict}",
        )

    fig.legend(
        handles=_legend_handles(flowlines=True, terminals=True),
        loc="lower center",
        ncol=3,
        frameon=False,
        fontsize=9,
    )
    fig.suptitle(
        "Signal A — a waterbody is depression storage iff its water's terminus lies inside itself"
    )
    fig.tight_layout(rect=(0, 0.10, 1, 0.94))
    out_path = OUT / "rule_terminus_gsl.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path
```

And register it in `main`:

```python
    figures = {
        "rule_terminus_gsl": fig_terminus_gsl,
    }
```

- [ ] **Step 2: Render it**

```bash
cd "$(git rev-parse --show-toplevel)"
srun --account=impd --time=30 --mem=32G \
  pixi run --as-is python scripts/render_depstor_figures.py --only rule_terminus_gsl
```
Expected: prints `.../docs/figures/depstor/rule_terminus_gsl.png`.

- [ ] **Step 3: Look at the figure and verify the rule is visible**

Open `docs/figures/depstor/rule_terminus_gsl.png` and confirm all four claims:
1. Great Salt Lake renders **blue (dprst)** — it did not before #178.
2. Black `×` terminal cells are visible **inside** the GSL outline.
3. Lewis and Clark renders **orange (on-stream)** with Network flowlines running through it.
4. If GSL's tile shows a **dashed magenta** Non-Network path, that is the #161 story and is expected.

If VPU `10L` is wrong for Lewis and Clark (it sits on the Missouri near the SD/NE line), find the right one:
`ls "$(python -c 'print()')"` → instead, list candidates with
`ls /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/shared/source/`
and try `10U`. Fix the constant and re-render.

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add scripts/render_depstor_figures.py docs/figures/depstor/rule_terminus_gsl.png
git commit -m "feat(viz): tile compositor + the Signal-A figure

draw_tile() composites the classification raster, the waterbody outline,
Network- vs Non-Network-colored flowlines, and FDR code-0 terminal cells.
rule_terminus_gsl.png shows the terminus-inside-itself rule firing: Great Salt
Lake frac_own=1.000 with its terminal cells inside its own polygon, against
Lewis and Clark Lake at 0.007 (its terminus is the Gulf).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: The remaining classifier rule tiles (7 figures)

**Files:**
- Modify: `scripts/render_depstor_figures.py`

**Interfaces:**
- Consumes: `draw_tile`, `waterbody_bbox`, `read_waterbodies`, `paths` (Tasks 1–2).
- Produces: `fig_icemass`, `fig_wbareacomi`, `fig_flowthrough`, `fig_network_gate`, `fig_closed_huc12_walker`, `fig_domain_exits`, `fig_playa_guardrail` — each returns a `Path`.

Each figure follows the Task-2 pattern: resolve COMID(s) → `read_waterbodies` → `waterbody_bbox` → `draw_tile` → legend → suptitle stating the rule → save.

- [ ] **Step 1: Add the seven figure functions**

Append to `scripts/render_depstor_figures.py`. Constants first:

```python
WALKER_LAKE = 10734232
MONO_LAKE = 120053921
LAKE_MICHIGAN = 904140248
LAKE_CHAMPLAIN = 15447630
EVERGLADES = 120055431
LARGEST_PLAYA = 120050227
LARGEST_ICE_MASS = 120050242
GSL_VETOING_MARSH = 10273192
```

Then, in order:

**`fig_network_gate()`** → `rule_network_gate.png`. Mono Lake (VPU 16, `frac_own = 1.000`). One panel, `vpu="16"`, `show_terminals=False`. Suptitle: *"The Network-Flowline gate (#161) — NHD draws Non-Network artificial paths through essentially every closed-basin lake. Only Network membership counts as connectivity."* The dashed magenta path through a blue (dprst) lake **is** the figure. Caption in the deck notes ~1,039 waterbodies / 842 km² across VPUs 13/15/16/18 were wrongly promoted before the gate.

**`fig_flowthrough()`** → `rule_flowthrough.png`. Two panels: Lewis and Clark (VPU `10U`/`10L`, in **and** out → on-stream) vs. Mono Lake (inflow only → stays dprst). Both with `vpu` set so the flowlines show. Suptitle: *"On-stream evidence B — a Network flowline must demonstrably enter AND exit. Terminal sinks (inflow only) and locally-spilling potholes (outflow only) stay dprst."*

**`fig_wbareacomi()`** → `rule_wbareacomi.png`. One panel on Lewis and Clark, but color the flowlines by whether their `WBAREACOMI` equals the waterbody's COMID — that is literally the join the rule performs. Draw matching artificial paths in `NETWORK_COLOR`, thick; others thin/grey:

```python
def fig_wbareacomi() -> Path:
    """On-stream evidence A: the WBAREACOMI artificial-path join.

    NHD tags a flowline with the COMID of the waterbody it threads
    (``WBAREACOMI``). If any Network flowline carries this waterbody's COMID,
    the waterbody is on-stream. The gate on Network membership is what stops
    this from promoting closed-basin lakes (#161).
    """
    p = paths()
    wb = read_waterbodies(comids=[LEWIS_AND_CLARK])
    bbox = waterbody_bbox(wb)
    fl = read_flowlines("10U", bbox)
    threading = fl[
        (fl["WBAREACOMI"].astype("int64") == LEWIS_AND_CLARK) & fl["network"]
    ]

    fig, ax = plt.subplots(figsize=(8, 6.5))
    draw_tile(ax, bbox, p["after"], outlines=wb, title=None)
    fl.plot(ax=ax, color="#999999", linewidth=0.6, zorder=3)
    if len(threading):
        threading.plot(ax=ax, color=NETWORK_COLOR, linewidth=2.6, zorder=5)
    ax.set_title(
        f"Lewis and Clark Lake — {len(threading)} Network flowline(s) carry "
        f"WBAREACOMI = {LEWIS_AND_CLARK}\n→ on-stream",
        fontsize=11,
    )
    fig.suptitle("On-stream evidence A — the WBAREACOMI artificial-path join")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    out_path = OUT / "rule_wbareacomi.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path
```

**`fig_closed_huc12_walker()`** → `rule_closed_huc12_walker.png`. Walker Lake (VPU 16), one panel, plus the type-C HUC12 polygon(s) from `paths()["huc12"]` (read with `gpd.read_parquet`, bbox-filtered) drawn as a dashed outline. Annotate `frac_own = 0.000` — **Signal A misses this lake entirely** (no FDR terminal cell inside it); Signal B catches it because the lake lies majority-inside a closed HUC12. Suptitle must state why containment is **majority-area**, never `intersects` (Eagle Lake grazes a closed basin at frac = 0.000) and never `within` (it drops GSL, which spills 1.1% out at frac = 0.989).

**`fig_domain_exits()`** → `rule_domain_exits.png`. Three panels — Lake Michigan, Lake Champlain, Everglades SwampMarsh — all rendering **orange (on-stream)** in the `after` snapshot. Suptitle: *"Guardrail — domain exits stay on-stream. These are terminal only because the CONUS model ends there. Demoting Lake Michigan to a pothole would be catastrophic; all three are in the 20 named fixtures."* No flowlines needed (`vpu=None`) — the point is what did **not** move.

**`fig_playa_guardrail()`** → `rule_playa_guardrail.png`. Two panels: the largest Playa (`120050227`) rendering dprst, and the largest Ice Mass (`120050242`) rendering **land** (grey — excluded from the waterbody classification entirely, falls back to perv/imperv via LULC). Suptitle: *"Two hard guardrails, and they are NOT equivalent: Playa IS depression storage (force-dprst, never promoted on-stream). Ice Mass is NOT depression storage — it is excluded from the classification and falls back to land."*

**`fig_icemass()`** → fold into `fig_playa_guardrail` (they are the same slide's two halves). **Do not** emit a separate `rule_icemass.png` — update the spec's figure count from 14 to 13 in Task 7.

Register all of them in `main`'s `figures` dict.

- [ ] **Step 2: Render them**

```bash
cd "$(git rev-parse --show-toplevel)"
for f in rule_network_gate rule_flowthrough rule_wbareacomi \
         rule_closed_huc12_walker rule_domain_exits rule_playa_guardrail; do
  srun --account=impd --time=30 --mem=32G \
    pixi run --as-is python scripts/render_depstor_figures.py --only "$f"
done
```
Expected: six paths printed, six PNGs on disk.

- [ ] **Step 3: Inspect each figure against its claim**

This is the gate. For each, confirm the rule is *visible*, not merely asserted:

| Figure | Must show |
|---|---|
| `rule_network_gate` | a **dashed magenta** Non-Network path through a **blue (dprst)** Mono Lake |
| `rule_flowthrough` | Lewis and Clark orange with flowlines in **and** out; Mono blue with inflow only |
| `rule_wbareacomi` | ≥ 1 thick blue threading flowline; the printed count in the title is **not zero** |
| `rule_closed_huc12_walker` | Walker Lake **blue**, inside a dashed closed-HUC12 outline, **no** terminal-cell markers |
| `rule_domain_exits` | all three panels **orange** |
| `rule_playa_guardrail` | Playa **blue**; Ice Mass **grey/land**, not blue |

If any claim fails, the figure is wrong — fix the renderer, not the caption.

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add scripts/render_depstor_figures.py docs/figures/depstor/rule_*.png
git commit -m "feat(viz): the six remaining classifier rule tiles

Each rule now has a real map tile: the Non-Network gate (Mono Lake), the
flow-through in-AND-out test, the WBAREACOMI join, Signal B (Walker Lake,
frac_own=0.000 -- Signal A misses it), the domain-exit guardrail, and the
Playa/Ice Mass guardrails (which are not equivalent).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Schematics and the bimodality histogram (4 figures)

The non-map figures: the rule ladder, the DAG, the `frac_own` distribution, and the BurnAdd `PurpCode` breakdown.

**Files:**
- Modify: `scripts/render_depstor_figures.py`

**Interfaces:**
- Consumes: `paths`, `frac_own_stats`, `OUT`.
- Produces: `fig_rule_ladder`, `fig_pipeline_dag`, `fig_frac_own_bimodal`, `fig_burnadd_purpcode`.

- [ ] **Step 1: `fig_frac_own_bimodal()` — the "0.5 is not a knob" figure**

```python
def fig_frac_own_bimodal() -> Path:
    """frac_own is bimodal, so the 0.5 threshold is inert -- not a tuned knob.

    Reads the classifier table directly rather than transcribing the PR body,
    so the deck's numbers cannot drift from the product.
    """
    p = paths()
    df = pd.read_parquet(p["endorheic"])
    stats = frac_own_stats(df)
    candidates = df[df["frac_own"] > 0]["frac_own"]

    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.hist(candidates, bins=np.linspace(0, 1, 51), color="#3182bd", edgecolor="white")
    ax.axvline(0.5, color="#cc2222", lw=2, ls="--", label="threshold = 0.5")
    ax.axvspan(0.45, 0.55, color="#cc2222", alpha=0.10)
    ax.set_yscale("log")
    ax.set_xlabel("frac_own  (share of the waterbody's cells whose D8 path ends inside itself)")
    ax.set_ylabel("waterbodies (log scale)")
    ax.legend(loc="upper center")
    ax.set_title(
        f"{stats['candidates']:,} Signal-A candidates · "
        f"{stats['at_or_above_95']:,} at frac_own ≥ 0.95 · "
        f"only {stats['in_band_45_55']:,} in the 0.45–0.55 band\n"
        f"threshold sweep 0.3→0.7: {stats['sweep'][0.3]:,} → {stats['sweep'][0.7]:,} "
        f"({stats['swing']:.1%} swing) — the threshold is inert",
        fontsize=11,
    )
    fig.tight_layout()
    out_path = OUT / "frac_own_bimodal.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  frac_own stats (put these in the deck): {stats}")
    return out_path
```

- [ ] **Step 2: `fig_burnadd_purpcode()` — what BurnAddWaterbody actually is**

Read `paths()["burn_add"]` (1,658 rows, the **kept** sink-purpose rows) and render a bar chart of its FTYPE mix (LakePond 1,550 · Playa 103 · SwampMarsh 5). Annotate the rule that produced it:

> `BurnAddWaterbody` is **not** a sink layer. It is every waterbody NHDPlus added to the DEM burn; only rows with a sink `PurpCode` (4 Playa, 5/8 closed lake) are sinks. VPU 01 ships **702 NULL-`PurpCode` rows against ZERO sinks in its own `Sink.shp`** — 503 on-network, including StreamRiver and CanalDitch FCodes. Merging the layer wholesale turns canals and river reaches into depression storage. `FTYPE` comes from **`FCODE`**, not `PurpCode` (`PurpCode` 5 spans both Playa and SwampMarsh, and a Playa mislabelled LakePond loses force-dprst).

Draw two bars side by side: "all BurnAdd rows in VPU 01" (702 NULL-PurpCode, 503 on-network) vs. "kept CONUS-wide" (1,658 sink-purpose). These are **static, verified numbers** from PR #178 — hardcode them in the figure's annotation text with a comment citing the PR, and derive the 1,658/FTYPE mix from the parquet.

- [ ] **Step 3: `fig_rule_ladder()` — the decision ladder**

A matplotlib schematic (same style as the old `fig_decision_schematic`, which this replaces). Five stacked stages, top to bottom:

1. **Every NHD waterbody** (448,124) — *minus* Ice Mass (1,220 → land), *plus* BurnAdd sink-purpose rows (1,658).
2. **Default: dprst.** A waterbody is depression storage unless proven on-stream.
3. **On-stream evidence (UNION), both Network-gated:** WBAREACOMI join ∪ geometric flow-through (in **and** out).
4. **Endorheic demotion (STRICT SUBTRACTION):** Signal A terminus-inside-itself ∪ Signal B majority-inside a closed HUC12. → 725 demotions.
5. **Guardrails:** Playa force-dprst · Ice Mass excluded · domain exits stay on-stream.
   → **Products:** `dprst_binary.tif`, `onstream_binary.tif`, `endorheic_wbody.tif`.

Annotate stage 4 with the arrow direction: *subtraction can only ever remove a COMID from the on-stream set, never add one.*

- [ ] **Step 4: `fig_pipeline_dag()` — regenerate the DAG**

Start from the previous version (`git show HEAD~N:scripts/render_depstor_figures.py`, function `fig_pipeline_dag`) and add the steps #178 introduced. Nodes and edges:

```python
    nodes = {
        # inputs
        "nhd": ("NHD\n(waterbodies, flowlines)", ...),
        "wbd": ("WBD\n(closed HUC12s)", ...),
        "fdr": ("FDR\n(fdr.vrt — code 0 = sink)", ...),
        "twi": ("TWI", ...),
        "lulc": ("LULC\n(NLCD)", ...),
        # staging
        "topology": ("nhd_topology", ...),        # MUST precede both COMID steps
        "flowlines": ("nhd_flowlines\n(WBAREACOMI)", ...),
        "flowthrough": ("nhd_flowthrough\n(geometric)", ...),
        # classification
        "waterbody": ("waterbody", ...),
        "endorheic": ("endorheic\n(Signal A + B)", ...),
        "wbody_conn": ("wbody_connectivity\n(union − endorheic)", ...),
        "dprst": ("dprst\n(+ clump-veto exemption)", ...),
        # routing → params
        "routing": ("routing\n(D8 + on-stream barrier)", ...),
        "same_hru": ("same_hru_drains", ...),
        "depth": ("dprst_depth", ...),
        "params": ("PRMS params\n(6 spatial)", ...),
    }
```

Edges must encode the hard ordering constraint: `topology → flowlines` and `topology → flowthrough` (both fail loud without it), `fdr → endorheic`, `endorheic → wbody_conn`, `endorheic → dprst` (the `endorheic_wbody.tif` exemption path), `wbody_conn → dprst → routing → same_hru → params`, `dprst → depth`.

- [ ] **Step 5: Render and inspect**

```bash
cd "$(git rev-parse --show-toplevel)"
for f in rule_ladder pipeline_dag frac_own_bimodal rule_burnadd_purpcode; do
  srun --account=impd --time=20 --mem=16G \
    pixi run --as-is python scripts/render_depstor_figures.py --only "$f"
done
```

**Record the printed `frac_own` stats** — the deck's Act-2 numbers come from that line, not from the PR body. Expected to match: 6,427 candidates, 6,298 at ≥ 0.95, 10 in the 0.45–0.55 band, ~0.5% swing. **If they do not match, the deck uses the printed values and Task 6 flags the discrepancy** — the product is the source of truth.

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add scripts/render_depstor_figures.py docs/figures/depstor/
git commit -m "feat(viz): rule ladder, updated DAG, frac_own bimodality, BurnAdd mix

frac_own_bimodal.png reads the classifier table directly, so the deck's
'0.5 is not a tuned knob' numbers are derived from the product rather than
transcribed from the PR body. The DAG gains nhd_topology (which must precede
both COMID steps), endorheic, and the endorheic_wbody exemption edge into dprst.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: The before/after figures (3 figures)

The impact evidence. These are the only figures that read the `before` snapshot.

**Files:**
- Modify: `scripts/render_depstor_figures.py`

**Interfaces:**
- Consumes: `read_classification`, `read_window`, `paths`, `draw_tile`.
- Produces: `fig_clump_veto_gsl`, `fig_lower_miss_before_after`, `fig_conus_dprst_before_after`, and `conus_area_deltas() -> dict`.

- [ ] **Step 1: `fig_clump_veto_gsl()` — the second bug #178 uncovered**

Two panels over the GSL bbox (padded to include the marsh at COMID 10273192): `before` vs `after`. Outlines: **both** GSL and the marsh, so the reader sees the 49.1 km² feature that vetoed the 4,368.9 km² lake.

```python
def fig_clump_veto_gsl() -> Path:
    """The clump veto, and why endorheic evidence overrides it.

    clump_regions 8-connects Great Salt Lake to a 49.1 km2 SwampMarsh
    (COMID 10273192) whose water drains INTO the lake -- so the marsh's terminus
    is GSL, not itself, and it is CORRECTLY left on-stream. But
    regions_touching_mask excludes a WHOLE region if any one cell touches the
    on-stream mask, so that one marsh vetoed all 4,854,156 GSL cells: GSL came
    out 0% dprst even though connected_wbody.tif no longer contained it.

    Fixed by exempting an endorheic waterbody's own not-on-stream cells from the
    region-level exclusion. The clump rule is a heuristic PROXY for connectivity;
    the terminus rule is direct hydrologic EVIDENCE. Evidence overrides proxy --
    but only where we have evidence, and only for the waterbody's own cells. A
    cell that is itself on-stream (the marsh) always stays excluded.
    """
    p = paths()
    wb = read_waterbodies(comids=[GREAT_SALT_LAKE, GSL_VETOING_MARSH])
    bbox = waterbody_bbox(wb, pad_frac=0.10)

    fig, axes = plt.subplots(1, 2, figsize=(12, 6.5))
    for ax, d, title in (
        (axes[0], p["before"], "before — the marsh vetoes the whole clump\nGSL: 0% dprst"),
        (axes[1], p["after"], "after — endorheic cells exempted\nGSL: 100% dprst"),
    ):
        draw_tile(ax, bbox, d, outlines=wb, title=title)

    fig.legend(handles=_legend_handles(), loc="lower center", ncol=3, frameon=False)
    fig.suptitle(
        "A 49.1 km² SwampMarsh was vetoing a 4,368.9 km² lake\n"
        "Evidence overrides proxy — but only where we have evidence",
        fontsize=13,
    )
    fig.tight_layout(rect=(0, 0.07, 1, 0.92))
    out_path = OUT / "clump_veto_gsl.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path
```

- [ ] **Step 2: `fig_lower_miss_before_after()` — the routing barrier**

Carry the previous version forward (`git show`, function `fig_drains_before_after`), keeping its `LOWER_MISS` bbox constant `(161095.96, 680908.25, 689198.95, 1659807.87)` and its `drains_to_dprst.tif` presence logic. **Change the `before` directory** to the pre-endorheic snapshot and update the caption: the on-stream barrier fix (#158/#159) is what this shows, and it is a strict subtraction — it can only *remove* false `drains_to_dprst` coverage.

- [ ] **Step 3: `conus_area_deltas()` + `fig_conus_dprst_before_after()`**

CONUS-wide, decimated. **The `_MAX_SIDE` guard is what makes this safe** — a full read is 16.9 B cells.

```python
def conus_area_deltas() -> dict:
    """Re-derive the deck's headline km² from the two snapshots.

    Derived, never transcribed: the results table and the maps cannot disagree.
    Counts are taken from a decimated read, so they are approximate -- scaled by
    the decimation factor. The deck quotes the exact figures from the PR's own
    A/B gate (scripts/diagnose/ab_endorheic_rebuild.py); this function exists to
    CONFIRM they are in the right ballpark, and to fail loud if they are not.
    """
    p = paths()
    with rasterio.open(p["after"] / "dprst_binary.tif") as ds:
        bounds = ds.bounds
        cell_km2 = abs(ds.transform.a * ds.transform.e) / 1e6

    out = {}
    for label, d in (("before", p["before"]), ("after", p["after"])):
        arr, nodata = read_window(d / "dprst_binary.tif", tuple(bounds), max_side=_MAX_SIDE)
        # Decimation samples 1 cell per (scale x scale) block, so scale the count.
        with rasterio.open(d / "dprst_binary.tif") as ds:
            scale = ds.height / arr.shape[0]
        out[label] = float((arr != nodata).sum() * scale * scale * cell_km2)
    out["delta_pct"] = (out["after"] - out["before"]) / out["before"] * 100
    return out
```

`fig_conus_dprst_before_after()` renders the two decimated CONUS `dprst_binary` arrays side by side, with the derived areas in the titles and the **exact** PR-gate figures (42,535 → 51,930 km², +22.1%) in the suptitle.

- [ ] **Step 4: Render, and check the derived CONUS areas against the gate**

```bash
cd "$(git rev-parse --show-toplevel)"
srun --account=impd --time=60 --mem=64G \
  pixi run --as-is python scripts/render_depstor_figures.py --only clump_veto_gsl
srun --account=impd --time=60 --mem=64G \
  pixi run --as-is python scripts/render_depstor_figures.py --only lower_miss_before_after
srun --account=impd --time=90 --mem=64G \
  pixi run --as-is python scripts/render_depstor_figures.py --only conus_dprst_before_after
```

The decimated CONUS areas are approximate. They must land in the **right ballpark** of the gate's exact numbers (42,535 → 51,930 km², +22.1%) — a decimated estimate within ~±15% and, critically, with the **same sign and rough magnitude of change**. If the derived delta is negative or wildly off, something is wrong with the snapshot pairing — **stop and investigate; do not paper over it in the caption.**

`clump_veto_gsl.png` is the gate for this task: GSL must be **grey/orange (not dprst) on the left** and **blue (dprst) on the right**. If the left panel already shows blue, the `before` snapshot is wrong.

- [ ] **Step 5: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add scripts/render_depstor_figures.py docs/figures/depstor/
git commit -m "feat(viz): before/after evidence — clump veto, Lower Miss, CONUS

clump_veto_gsl.png is the headline: a 49.1 km² SwampMarsh vetoing a 4,368.9 km²
lake, before and after the endorheic exemption. CONUS areas are re-derived from
the two snapshots (decimated) and cross-checked against the A/B gate's exact
figures rather than transcribed.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Write the deck

**Files:**
- Modify: `docs/presentations/2026-07-depression-storage-workflow.slides.md` (full rewrite)

Follow the spec's 27-slide structure exactly (Acts 0–4). Keep the existing Marp front-matter **verbatim** — theme, `paginate`, `size: 16:9`, and the inline `style:` block (`.callout`, `.warn`, `.caption`, `.footnote`, `.small`, `.two`). Only the `title:` may change.

**Rules for the prose:**

- **Rule-first.** Each Act-2 slide: state the rule in a blockquote → show the figure → give the number it changed. The before/after pair appears only where a rule actually moved something.
- **Every number is derived.** `frac_own` stats come from the line Task 4 Step 5 printed. Fixture names/COMIDs come from `scripts/diagnose/endorheic_fixtures.py`. CONUS deltas come from PR #178's A/B gate (`ab_endorheic_rebuild.py`), cross-checked by Task 5.
- **State the SET-vs-DEMOTION distinction explicitly** on the endorheic slides, or the numbers look broken: the endorheic table holds **22,970** COMIDs (6,364 by terminus, 21,503 by closed HUC12, 4,925 by both), but only the ones *also* in the on-stream union get demoted — hence **725 demotions**, not 22,970. Signal A's candidate population is the **6,427** waterbodies with a computed `frac_own`.
- **Delete the false claim.** The current deck's "The endorheic fix" slide credits the Network-Flowline gate with fixing endorheic classification. It did not: with the gate in place, Great Salt Lake still came out **0% dprst**. The gate is necessary but not sufficient — it is now Act-2 rule 14, and Signal A (rule 15) is the actual fix. Do not soften this; it is the deck's central correction.
- **Act 4 must state that #178 is unmerged**, and carry the two "not in this PR" items: the profile still points at the hand-made waterbody layer, and `dprst_depth` must be regenerated.

- [ ] **Step 1: Write the deck**

Replace the file. 27 slides, `---` separators, figures by relative path (`![](../figures/depstor/rule_terminus_gsl.png)`).

- [ ] **Step 2: Render it to check it builds and paginates**

```bash
cd "$(git rev-parse --show-toplevel)"
pixi install -e marp && pixi run -e marp marp-setup
pixi run -e marp render-deck docs/presentations/2026-07-depression-storage-workflow.slides.md --pdf
```
Expected: a PDF, no missing-image warnings. **Check every slide fits** — the deck's CSS caps `img { max-height: 440px; }`; a slide whose figure + prose overflow is a slide nobody can read. Trim prose, don't shrink the figure.

- [ ] **Step 3: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add docs/presentations/2026-07-depression-storage-workflow.slides.md
git commit -m "docs(viz): rewrite the depstor deck rule-first, through PR #178

The previous deck credited the Network-Flowline gate with fixing endorheic
classification. It did not — with the gate in place, Great Salt Lake still came
out 0% depression storage. The gate is necessary but not sufficient; Signal A
(terminus-inside-itself) is the actual fix. Each classification rule is now
stated as a rule and shown firing on real hydrography.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Docs audit, cite verification, cleanup

CLAUDE.md requires a docs check on **every** code change. This is it.

**Files:**
- Modify: `docs/presentations/README.md`
- Delete: `docs/figures/depstor/decision_schematic.png`, `docs/figures/depstor/great_basin_before_after.png`
- Modify: `docs/superpowers/specs/2026-07-14-depstor-workflow-deck-design.md` (figure count 14 → 13; `rule_icemass` folded into `rule_playa_guardrail`)

- [ ] **Step 1: Delete the two stale figures**

```bash
cd "$(git rev-parse --show-toplevel)"
git rm docs/figures/depstor/decision_schematic.png \
       docs/figures/depstor/great_basin_before_after.png
```

`great_basin_before_after.png` must go: its caption credits the Network gate with the endorheic fix — the exact claim #178 disproves. `decision_schematic.png` is superseded by `rule_ladder.png`.

- [ ] **Step 2: Verify no figure is orphaned and no reference is dangling**

```bash
cd "$(git rev-parse --show-toplevel)"
echo "--- referenced by the deck but missing on disk (must be empty) ---"
grep -o 'figures/depstor/[a-z_0-9]*\.png' docs/presentations/*.slides.md | \
  sed 's|.*/||' | sort -u | while read -r f; do
    [ -f "docs/figures/depstor/$f" ] || echo "MISSING: $f"
  done
echo "--- on disk but referenced by nothing (orphans) ---"
ls docs/figures/depstor/ | while read -r f; do
    grep -qr "$f" docs/ || echo "ORPHAN: $f"
  done
```
Expected: both lists empty.

- [ ] **Step 3: Update `docs/presentations/README.md`**

Rewrite the depstor bullet. It currently says the deck contrasts "the legacy ArcPy pipeline vs. the current open-source pipeline" — still true, but it must now say the deck is **rule-first**, is a **technical design review**, and **covers PR #178 (the endorheic classifier), which is unmerged**. Note that its figures are generated by `scripts/render_depstor_figures.py`, which **must be run under `srun`** (it reads CONUS rasters).

- [ ] **Step 4: Verify every file:line cite in the deck**

The deck cites code. Each cite must resolve **in this worktree**:

```bash
cd "$(git rev-parse --show-toplevel)"
grep -oE '`[a-z_/]+\.(py|yml|md)(:[0-9]+)?`' docs/presentations/2026-07-depression-storage-workflow.slides.md | \
  tr -d '`' | sort -u
```
For each path, confirm it exists; for each `:line`, open it and confirm the line says what the deck claims. The known one from the legacy slide is `docs/0b_TB_depr_stor.py:214` (the `Con(rSro == hru)` same-HRU restriction) — **verify it, don't assume**; issue bodies and older docs drift.

- [ ] **Step 5: Fix the spec's figure count**

Task 3 folded `rule_icemass` into `rule_playa_guardrail`. Update the spec's inventory table (14 → 13 figures) so the spec and the product agree.

- [ ] **Step 6: Remove the scratch dir and run pre-commit**

```bash
cd "$(git rev-parse --show-toplevel)"
rm -rf .scratch
pixi run -e dev pre-commit run --all-files
```
Expected: all hooks pass. Fix anything ruff flags in the renderer.

- [ ] **Step 7: Commit and push**

```bash
cd "$(git rev-parse --show-toplevel)"
git add -A
git commit -m "docs(viz): audit — retire the stale figures, update the deck README

great_basin_before_after.png is deleted, not regenerated: its caption credits
the Network-Flowline gate with fixing endorheic classification, which is the
claim PR #178 disproves. decision_schematic.png is superseded by rule_ladder.png.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
git push -u origin docs/depstor-deck-v2
```

CI runs `pytest tests/` on push — `tests/test_render_depstor_figures.py` is the gate.

---

## Open the PR

Base the PR on **`feat/endorheic-dprst-classifier`**, not `main` — this branch is stacked on PR #178 and its deck describes #178's code.

`gh` is blocked on this HPC (Go TLS ClientHello dropped by a DPI middlebox). Use `gh auth token` + `curl`, and pass the payload with `--data-binary @file` (`-d @file` returns 400):

```bash
cd "$(git rev-parse --show-toplevel)"
cat > /tmp/pr.json <<'EOF'
{"title":"docs(viz): depression-storage workflow deck, rule-first, through PR #178",
 "head":"docs/depstor-deck-v2","base":"feat/endorheic-dprst-classifier","body":"..."}
EOF
curl -s -H "Authorization: token $(gh auth token)" \
     -H "Accept: application/vnd.github+json" \
     --data-binary @/tmp/pr.json \
     https://api.github.com/repos/rmcd-mscb/gfv2-params/pulls
```

The PR body should lead with **why the old deck was wrong** (it credited the Network gate with the endorheic fix; GSL was still 0% dprst), then the rule ladder, then the figure inventory.
