# Per-VPU Tiled WBT Routing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the depstor `routing` step run at CONUS scale by tiling the WhiteboxTools `Watershed` computation per VPU instead of loading the full 16.9-billion-cell grid (which OOMs above the cluster's 503 GB node ceiling).

**Architecture:** Move `vpu_id` before `routing` in the step order so routing can read the per-cell VPU partition. Refactor `routing.build` to: align the FDR once (streaming `gdal.Warp`, unchanged), then loop the VPU codes present in `vpu_id.tif` — for each, window to that VPU's bbox, mask the FDR to the VPU (so WBT can't route across the boundary), run WBT `Watershed` on the window, and mosaic the labelled cells back into a CONUS `drains_to_dprst.tif` for that VPU's cells only. The bbox/mask/mosaic arithmetic lives in pure, unit-tested helpers in `depstor.py`.

**Tech Stack:** Python, numpy, rasterio, GDAL (`gdal.Warp`), WhiteboxTools (`Watershed`), pytest, pixi.

**Spec:** `docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md`

---

## File Structure

- `src/gfv2_params/depstor_builders/__init__.py` — **modify**: reorder `STEP_ORDER` (`vpu_id` before `routing`).
- `configs/depstor/depstor_rasters.yml` — **modify**: reorder the step list to match (cosmetic; `STEP_ORDER` is authoritative).
- `src/gfv2_params/depstor.py` — **modify**: add pure per-VPU helpers (`vpu_codes_present`, `vpu_bbox`, `mask_fdr_to_vpu`, `vpu_pour_points`, `assign_vpu_drains`).
- `src/gfv2_params/depstor_builders/routing.py` — **modify**: refactor `build()` to the per-VPU loop; drop the obsolete CONUS `_prepare_pour_points` / `_watershed_to_binary`; add `_write_window_tif`.
- `tests/test_routing_tiling.py` — **create**: unit tests for the pure helpers + the `STEP_ORDER` invariant.
- `docs/ARCHITECTURE.md`, `slurm_batch/RUNME.md` — **modify**: note routing now tiles per VPU.

---

## Task 1: Move `vpu_id` before `routing` in the step order

**Files:**
- Modify: `src/gfv2_params/depstor_builders/__init__.py` (the `STEP_ORDER` list)
- Modify: `configs/depstor/depstor_rasters.yml` (step list order — cosmetic)
- Test: `tests/test_routing_tiling.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_routing_tiling.py` with:

```python
from gfv2_params.depstor_builders import STEP_ORDER


def test_vpu_id_runs_before_routing():
    # routing tiles by vpu_id, so the partition must be built first.
    assert STEP_ORDER.index("vpu_id") < STEP_ORDER.index("routing")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py::test_vpu_id_runs_before_routing -v`
Expected: FAIL (currently `vpu_id` is after `routing`).

- [ ] **Step 3: Reorder `STEP_ORDER`**

In `src/gfv2_params/depstor_builders/__init__.py`, change the `STEP_ORDER` list so `vpu_id` precedes `routing`:

```python
STEP_ORDER = [
    "landmask",
    "imperv",
    "streambuffer",
    "waterbody",
    "dprst",
    "perv",
    "vpu_id",
    "routing",
    "drains_perv",
    "drains_imperv",
    "carea_map",
]
```

- [ ] **Step 4: Reorder the YAML step list to match (cosmetic)**

In `configs/depstor/depstor_rasters.yml`, move the `- name: vpu_id` block so it appears before `- name: routing`. (Execution order is driven by `STEP_ORDER`; this is only for readability.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py::test_vpu_id_runs_before_routing -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/depstor_builders/__init__.py configs/depstor/depstor_rasters.yml tests/test_routing_tiling.py
git commit -m "refactor(depstor): run vpu_id before routing so routing can tile by VPU"
```

---

## Task 2: Add pure per-VPU helpers to `depstor.py`

**Files:**
- Modify: `src/gfv2_params/depstor.py` (append the five helpers)
- Test: `tests/test_routing_tiling.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_routing_tiling.py`:

```python
import numpy as np

from gfv2_params.depstor import (
    assign_vpu_drains,
    mask_fdr_to_vpu,
    vpu_bbox,
    vpu_codes_present,
    vpu_pour_points,
)


def test_vpu_codes_present_excludes_nodata():
    v = np.array([[0, 1, 1], [2, 2, 0]], dtype=np.uint8)
    assert vpu_codes_present(v) == [1, 2]


def test_vpu_bbox_bounds_the_code_and_is_slice_ready():
    v = np.array(
        [[0, 0, 0, 0],
         [0, 1, 1, 0],
         [0, 1, 0, 0],
         [0, 0, 0, 0]], dtype=np.uint8)
    assert vpu_bbox(v, 1) == (1, 3, 1, 3)  # rows [1,3), cols [1,3)
    assert vpu_bbox(v, 9) is None


def test_mask_fdr_to_vpu_sets_outside_to_nodata():
    fdr = np.array([[1, 2], [4, 8]], dtype=np.uint8)
    vpu = np.array([[1, 2], [1, 1]], dtype=np.uint8)
    out = mask_fdr_to_vpu(fdr, vpu, code=1, nodata=255)
    assert out.tolist() == [[1, 255], [4, 8]]


def test_vpu_pour_points_only_this_vpu_depressions():
    dprst = np.array([[1, 1], [1, 255]], dtype=np.uint8)
    vpu = np.array([[1, 2], [1, 1]], dtype=np.uint8)
    out = vpu_pour_points(dprst, vpu, code=1)
    assert out.tolist() == [[1, 0], [1, 0]]


def test_assign_vpu_drains_isolates_by_vpu_even_with_overlapping_bbox():
    # VPU 1 (rows 0 and 2) has a bbox spanning the whole grid, which contains
    # the VPU 2 cell at (1,1). A run that labels everything in VPU 1's bbox must
    # still only mark VPU 1 cells.
    vpu = np.array([[1, 1, 1], [0, 2, 0], [1, 1, 1]], dtype=np.uint8)
    drains = np.full((3, 3), np.uint8(255), dtype=np.uint8)

    b1 = vpu_bbox(vpu, 1)            # (0, 3, 0, 3)
    ws1 = np.ones((3, 3), dtype=np.int32)
    assign_vpu_drains(drains, vpu, 1, b1, ws1, ws_nodata=0)
    assert drains[1, 1] == 255       # VPU 2 cell untouched by VPU 1's run
    assert (drains[0, :] == 1).all() and (drains[2, :] == 1).all()
    assert drains[1, 0] == 255 and drains[1, 2] == 255  # vpu nodata cells

    b2 = vpu_bbox(vpu, 2)            # (1, 2, 1, 2)
    ws2 = np.ones((1, 1), dtype=np.int32)
    assign_vpu_drains(drains, vpu, 2, b2, ws2, ws_nodata=0)
    assert drains[1, 1] == 1


def test_assign_vpu_drains_unlabelled_cells_stay_nodata():
    vpu = np.array([[1, 1]], dtype=np.uint8)
    drains = np.full((1, 2), np.uint8(255), dtype=np.uint8)
    ws = np.array([[5, 0]], dtype=np.int32)  # left labelled (5), right nodata (0)
    assign_vpu_drains(drains, vpu, 1, (0, 1, 0, 2), ws, ws_nodata=0)
    assert drains.tolist() == [[1, 255]]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py -v`
Expected: FAIL with `ImportError` (helpers not defined yet).

- [ ] **Step 3: Implement the helpers**

Append to `src/gfv2_params/depstor.py` (it already imports `numpy as np`):

```python
def vpu_codes_present(vpu_id: np.ndarray, nodata: int = 0) -> list[int]:
    """Sorted VPU codes present in a vpu_id raster, excluding `nodata`."""
    return [int(c) for c in np.unique(vpu_id) if int(c) != nodata]


def vpu_bbox(vpu_id: np.ndarray, code: int) -> tuple[int, int, int, int] | None:
    """(row_start, row_stop, col_start, col_stop) bounding cells == `code`.

    Stops are exclusive so the tuple is directly slice-ready. Returns None when
    the code is absent.
    """
    rows = np.any(vpu_id == code, axis=1)
    cols = np.any(vpu_id == code, axis=0)
    if not rows.any():
        return None
    r = np.where(rows)[0]
    c = np.where(cols)[0]
    return int(r[0]), int(r[-1]) + 1, int(c[0]), int(c[-1]) + 1


def mask_fdr_to_vpu(fdr_win: np.ndarray, vpu_win: np.ndarray, code: int,
                    nodata: int = 255) -> np.ndarray:
    """FDR restricted to one VPU: cells where vpu != code become `nodata`.

    WBT Watershed treats nodata d8 cells as background, so masking confines the
    routing to this VPU (no cross-boundary flow).
    """
    out = fdr_win.copy()
    out[vpu_win != code] = nodata
    return out


def vpu_pour_points(dprst_win: np.ndarray, vpu_win: np.ndarray,
                    code: int) -> np.ndarray:
    """0/1 pour-points: 1 where this VPU has a depression cell, else 0.

    WBT Watershed treats every non-zero cell as a pour-point and ignores the
    nodata tag, so background must be 0 (matches the legacy pour-point encoding).
    """
    return ((dprst_win == 1) & (vpu_win == code)).astype(np.uint8)


def assign_vpu_drains(drains: np.ndarray, vpu_id: np.ndarray, code: int,
                      bbox: tuple[int, int, int, int],
                      watershed_win: np.ndarray, ws_nodata) -> None:
    """Mark drains==1 for this VPU's cells that the watershed labelled (in place).

    Only cells where vpu_id == code are touched, so per-VPU windows may overlap
    without cross-contamination. `drains[r0:r1, c0:c1]` is a basic-slice view, so
    the masked assignment writes through to `drains`.
    """
    r0, r1, c0, c1 = bbox
    if ws_nodata is None:
        labelled = watershed_win > 0
    elif isinstance(ws_nodata, float) and np.isnan(ws_nodata):
        labelled = ~np.isnan(watershed_win)
    else:
        labelled = watershed_win != ws_nodata
    sel = (vpu_id[r0:r1, c0:c1] == code) & labelled
    drains[r0:r1, c0:c1][sel] = 1
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py -v`
Expected: PASS (all 7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor.py tests/test_routing_tiling.py
git commit -m "feat(depstor): pure per-VPU tile/mask/mosaic helpers for routing"
```

---

## Task 3: Refactor `routing.build` to tile per VPU

**Files:**
- Modify: `src/gfv2_params/depstor_builders/routing.py`

No new unit test (this orchestrates the WBT subprocess + rasterio I/O); it is verified by the existing suite still passing plus the Oregon regression (Task 5).

- [ ] **Step 1: Replace `routing.py` with the tiled implementation**

Overwrite `src/gfv2_params/depstor_builders/routing.py` with:

```python
"""WhiteboxTools Watershed from dprst pour-points, tiled per VPU.

Routing the full-CONUS FDR + pour-points through WBT Watershed OOMs (WBT loads
every raster as f64; ~3 x 135 GB > the 503 GB node ceiling). NHDPlus VPU
boundaries follow drainage divides, so each VPU's contributing area is local: we
route each VPU in isolation (FDR masked to the VPU) and mosaic the per-VPU
results — see docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md.
"""

from __future__ import annotations

import numpy as np
import rasterio
from osgeo import gdal
from rasterio.windows import Window
from rasterio.windows import transform as window_transform

from ..depstor import (
    RasterInfo,
    assign_vpu_drains,
    mask_fdr_to_vpu,
    read_land_mask,
    vpu_bbox,
    vpu_codes_present,
    vpu_pour_points,
    write_uint8_binary,
)
from ..wbt import find_whitebox_tools_binary, run_streamed
from .context import BuildContext


def _align_fdr_to_dprst_grid(fdr_path, dprst_path, out_path, logger) -> None:
    """Materialise the FDR onto the dprst grid as a WBT-readable GeoTIFF.

    Streams via gdal.Warp (block-by-block, bounded RAM) rather than an in-memory
    rioxarray.reproject_match: the latter materialised the full 16.9-billion-cell
    CONUS array plus float intermediates and OOM-killed the step at ~400 GB on a
    uint8 source that is only ~17 GB. The FDR clip already shares the dprst grid,
    so this is a near-identity nearest-neighbour resample that just realises the
    VRT into a concrete raster WBT can read.
    """
    logger.info("  Aligning FDR to dprst grid (gdal.Warp, streaming)...")
    gdal.UseExceptions()
    with rasterio.open(dprst_path) as d:
        b = d.bounds
        width, height, dst_srs = d.width, d.height, d.crs.to_wkt()
    with rasterio.open(fdr_path) as f:
        src_nodata = f.nodata
    if out_path.exists():
        out_path.unlink()
    ds = gdal.Warp(
        str(out_path),
        str(fdr_path),
        options=gdal.WarpOptions(
            format="GTiff",
            outputBounds=[b.left, b.bottom, b.right, b.top],
            width=width,
            height=height,
            dstSRS=dst_srs,
            resampleAlg="near",
            outputType=gdal.GDT_Byte,
            srcNodata=src_nodata,
            dstNodata=255,
            multithread=True,
            warpMemoryLimit=2_000_000_000,
            creationOptions=["COMPRESS=LZW", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256", "BIGTIFF=YES"],
        ),
    )
    if ds is None:
        raise RuntimeError(f"gdal.Warp produced no dataset for {out_path} — FDR alignment failed.")
    ds = None  # flush/close


def _run_whitebox_watershed(fdr_path, pour_pts_path, output_path, logger) -> None:
    import os

    runner = find_whitebox_tools_binary()
    cmd = [
        runner,
        f"--wd={os.getcwd()}",
        "--max_procs=-1",
        "-r=Watershed",
        f"--d8_pntr={fdr_path}",
        f"--pour_pts={pour_pts_path}",
        f"--output={output_path}",
        "--esri_pntr",
        "-v",
    ]
    run_streamed(cmd, tool="Watershed", logger=logger)


def _write_window_tif(arr, window, info, out_path, nodata) -> None:
    """Write a windowed uint8 raster carrying the window's geotransform."""
    profile = {
        "driver": "GTiff", "height": arr.shape[0], "width": arr.shape[1], "count": 1,
        "dtype": "uint8", "crs": info.crs,
        "transform": window_transform(window, info.transform),
        "nodata": nodata, "compress": "LZW", "tiled": True,
        "blockxsize": 256, "blockysize": 256, "BIGTIFF": "YES",
    }
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr, 1)


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.fdr_raster is None:
        raise KeyError("routing step needs `fdr_raster` in fabric profile.")
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    dprst_path = ctx.require("dprst")
    vpu_id_path = ctx.require("vpu_id")
    keep_intermediates = bool(step_cfg.get("keep_intermediates", False))

    if not ctx.fdr_raster.exists():
        raise FileNotFoundError(f"FDR raster not found: {ctx.fdr_raster}")

    logger.info("--- routing (per-VPU tiled) ---")
    logger.info("  FDR    : %s", ctx.fdr_raster)
    logger.info("  vpu_id : %s", vpu_id_path)
    logger.info("  Output : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"drains_to_dprst": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fdr_aligned = output_path.parent / "fdr_aligned.tif"

    _align_fdr_to_dprst_grid(ctx.fdr_raster, dprst_path, fdr_aligned, logger)

    with rasterio.open(vpu_id_path) as src:
        vpu_id = src.read(1)
    codes = vpu_codes_present(vpu_id)
    logger.info("  Tiling routing over %d VPU(s): %s", len(codes), codes)

    drains = np.full((info.height, info.width), np.uint8(255), dtype=np.uint8)

    with rasterio.open(fdr_aligned) as fdr_src, rasterio.open(dprst_path) as dprst_src:
        for code in codes:
            bbox = vpu_bbox(vpu_id, code)
            r0, r1, c0, c1 = bbox
            window = Window(c0, r0, c1 - c0, r1 - r0)
            vpu_win = vpu_id[r0:r1, c0:c1]
            fdr_win = fdr_src.read(1, window=window)
            dprst_win = dprst_src.read(1, window=window)

            fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
            pour = vpu_pour_points(dprst_win, vpu_win, code)

            tile_fdr = output_path.parent / f"_fdr_vpu{code}.tif"
            tile_pour = output_path.parent / f"_pour_vpu{code}.tif"
            tile_ws = output_path.parent / f"_ws_vpu{code}.tif"
            try:
                _write_window_tif(fdr_masked, window, info, tile_fdr, nodata=255)
                _write_window_tif(pour, window, info, tile_pour, nodata=0)
                _run_whitebox_watershed(tile_fdr, tile_pour, tile_ws, logger)
                with rasterio.open(tile_ws) as ws_src:
                    ws_win = ws_src.read(1)
                    ws_nodata = ws_src.nodata
                assign_vpu_drains(drains, vpu_id, code, bbox, ws_win, ws_nodata)
                n_vpu = int((drains[r0:r1, c0:c1][vpu_win == code] == 1).sum())
                logger.info("  VPU %d: %d cells drain to dprst", code, n_vpu)
            finally:
                if not keep_intermediates:
                    for p in (tile_fdr, tile_pour, tile_ws):
                        if p.exists():
                            p.unlink()

    del vpu_id  # free the CONUS uint8 partition before the final mask
    if not keep_intermediates and fdr_aligned.exists():
        fdr_aligned.unlink()

    drains[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    n_in = int((drains == 1).sum())
    pct = 100 * n_in / drains.size
    if pct > 50:
        logger.warning(
            "Drains-to-dprst coverage is %.2f%% of the grid — unusually high. "
            "Check pour-points (nodata=0) and FDR/vpu_id alignment.", pct,
        )
    write_uint8_binary(drains, info, output_path)
    logger.info(
        "  Drains-to-dprst mask written: %s (%d cells, %.4f%% of grid)",
        output_path, n_in, pct,
    )
    return {"drains_to_dprst": output_path}
```

- [ ] **Step 2: Verify the whole test suite still imports/passes for the touched modules**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py tests/test_depstor_helpers.py -v`
Expected: PASS. (CI runs the full `pytest tests/` on push — do not run the full suite on the HPC head node.)

- [ ] **Step 3: Quick import check (head-node safe)**

Run: `pixi run --as-is python -c "import gfv2_params.depstor_builders.routing as r; print('vpu_id required:', 'vpu_id' in r.build.__code__.co_consts or True); print('helpers wired:', hasattr(r, '_align_fdr_to_dprst_grid'))"`
Expected: prints without ImportError.

- [ ] **Step 4: Commit**

```bash
git add src/gfv2_params/depstor_builders/routing.py
git commit -m "refactor(depstor): tile WBT routing per VPU to fit CONUS in memory"
```

---

## Task 4: Update docs

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Modify: `slurm_batch/RUNME.md`

- [ ] **Step 1: Note the tiling in ARCHITECTURE.md**

In `docs/ARCHITECTURE.md`, find the depstor `routing` description (the step list / DAG section) and add a sentence:

> `routing` tiles the WBT Watershed per VPU (partitioned by `vpu_id`, which therefore runs *before* `routing`): each VPU is routed in isolation and the binary results are mosaicked. This keeps CONUS memory bounded (~50–100 GB/VPU vs ~405 GB whole-grid) and is correct because VPU boundaries follow drainage divides.

- [ ] **Step 2: Note the step reorder + memory in RUNME.md**

In `slurm_batch/RUNME.md`, in the Stage 2d "11-step DAG" description, update the step order to put `vpu_id` before `routing` and add:

> `routing` now tiles WBT Watershed per VPU (it consumes `vpu_id`), so whole-CONUS FDR is never held in memory. After a clean CONUS run, the `build_depstor_rasters.batch` `--mem` default can be right-sized down from 384G.

- [ ] **Step 3: Commit**

```bash
git add docs/ARCHITECTURE.md slurm_batch/RUNME.md
git commit -m "docs(depstor): routing tiles per VPU; vpu_id runs before routing"
```

---

## Task 5: Oregon regression (correctness anchor — execution, not CI)

**Goal:** Confirm the refactor does not change single-VPU output. Oregon is one VPU, so tiled == non-tiled. Its depstor outputs already exist on disk, so capture the current `drains_to_dprst.tif` first, then rebuild and diff.

- [ ] **Step 1: Back up Oregon's current routing output**

```bash
DR=/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2
cp "$DR/oregon/depstor_rasters/drains_to_dprst.tif" /tmp/oregon_drains_baseline.tif
```

- [ ] **Step 2: Rebuild just Oregon's routing with the refactor**

(`vpu_id` runs first via `--from vpu_id`; Oregon is small, run on a modest alloc.)

```bash
FABRIC=oregon sbatch --time=02:00:00 --mem=64G \
    slurm_batch/build_depstor_rasters.batch --from vpu_id --force
```

- [ ] **Step 3: Diff the rebuilt output against the baseline**

After the job completes (check `sacct -j <id>`):

```bash
DR=/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2
pixi run --as-is python - <<'PY'
import numpy as np, rasterio
a = rasterio.open("/tmp/oregon_drains_baseline.tif").read(1)
b = rasterio.open(f"{__import__('os').environ.get('DR','/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2')}/oregon/depstor_rasters/drains_to_dprst.tif").read(1)
diff = int((a != b).sum())
print("shape match:", a.shape == b.shape, "| differing cells:", diff)
assert a.shape == b.shape and diff == 0, "Oregon routing changed — investigate before CONUS"
print("PASS: single-VPU routing unchanged by the tiling refactor")
PY
```

Expected: `differing cells: 0` / PASS. If non-zero, stop and investigate before running CONUS.

---

## Task 6: Run CONUS gfv2 routing onward

- [ ] **Step 1: Resubmit the depstor build (resumes at `vpu_id` → `routing`)**

`landmask`/`imperv`/`streambuffer`/`waterbody`/`dprst`/`perv` are on disk (skip-on-exist); `vpu_id` and `routing` will run, then `drains_perv`/`drains_imperv`/`carea_map`. Per-VPU routing fits well under the old ceiling — request 192G.

```bash
FABRIC=gfv2 sbatch --mem=192G slurm_batch/build_depstor_rasters.batch
```

Do NOT pass `--from routing`: `vpu_id.tif` does not exist yet, and a plain run builds it first (everything earlier skips on existence).

- [ ] **Step 2: Watch to completion and confirm the full stack finished**

After the job ends:

```bash
sacct -j <id> -o JobID,State,Elapsed,MaxRSS
ls -la /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/gfv2/depstor_rasters/
```

Expected: `State COMPLETED`, all 14 depstor rasters present (incl. `drains_to_dprst.tif`, `carea_map_t8_binary.tif`, `carea_map_t156_binary.tif`). Then Track B3 (`submit_depstor_params.sh`) can run.

---

## Self-Review

**Spec coverage:**
- Independent per-VPU routing (no halo) → Task 3 (`mask_fdr_to_vpu` confines flow; `assign_vpu_drains` assigns only `vpu==code`). ✓
- Partition from `vpu_id`, DAG reorder → Task 1. ✓
- Pure, unit-tested tile/mask/mosaic helper → Task 2. ✓
- `gdal.Warp` align retained, `_watershed_to_binary`/`_prepare_pour_points` folded in → Task 3. ✓
- Memory ~50–100 GB/VPU, `--mem` droppable → Task 6 (192G) + Task 4 doc note. ✓
- Unit test (synthetic 2-VPU isolation+mosaic) + Oregon single-VPU regression → Task 2 + Task 5. ✓
- Reuse of existing `vpu` attribute (no new requirement) → no code needed; `vpu_id`/`resolve_vpu_source` unchanged (loud error already exists). ✓

**Placeholder scan:** none — every code/command step is concrete.

**Type consistency:** helper names (`vpu_codes_present`, `vpu_bbox`, `mask_fdr_to_vpu`, `vpu_pour_points`, `assign_vpu_drains`) and signatures match between Task 2 (definition + tests) and Task 3 (imports + call sites). `vpu_bbox` returns exclusive-stop tuples used consistently as slices and as `Window(c0, r0, c1-c0, r1-r0)`.
