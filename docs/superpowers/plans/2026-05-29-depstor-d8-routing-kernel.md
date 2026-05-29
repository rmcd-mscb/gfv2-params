# Cycle-safe D8 routing kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hanging WhiteboxTools `Watershed` subprocess in the depstor `routing` step with an in-process, cycle-safe, O(N) numba D8 traversal kernel — keeping the existing per-VPU full-array mosaic unchanged.

**Architecture:** A new pure module `src/gfv2_params/d8_routing.py` holds a `@njit` memoized downstream-traversal kernel (`drains_to_dprst_kernel`) that marks every cell whose ESRI-D8 path reaches a depression pour-point. A 4-state coloring (`0`=unknown, `1`=drains, `2`=does-not, `3`=in-progress) makes it physically unable to loop on flow cycles — the precise fix for the WBT hang. `routing.build` calls it on each masked per-VPU window instead of shelling out to WBT, eliminating the subprocess and all per-tile disk I/O.

**Tech Stack:** Python, numba (`@njit`), numpy, rasterio. Tests via pytest (CI gate; do not run pytest on the HPC head node — `py_compile`/import checks only there).

**Spec:** [`docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md`](../specs/2026-05-29-depstor-d8-routing-kernel-design.md)

---

## Task 1: D8 traversal kernel + unit tests

**Files:**
- Create: `src/gfv2_params/d8_routing.py`
- Test: `tests/test_drains_kernel.py`

This task is self-contained: it builds and fully tests the pure kernel with no
dependency on `routing.py`. The kernel signature
(`drains_to_dprst_kernel(fdr_win, pour_win, fdr_nodata=255) -> uint8[ny,nx]`)
is what Task 2 wires into the builder.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_drains_kernel.py`:

```python
import numpy as np

from gfv2_params.d8_routing import drains_to_dprst_kernel

# ESRI D8 codes used in the fixtures:
#   1=E  2=SE  4=S  8=SW  16=W  32=NW  64=N  128=NE   255=nodata/sink


def test_pour_point_itself_drains():
    # A lone pour point with no inflow still counts as draining.
    fdr = np.array([[255]], dtype=np.uint8)
    pour = np.array([[1]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)
    assert out.tolist() == [[1]]


def test_straight_chain_into_pour_point():
    # Row of cells all flowing East (code 1) into a pour point at the right end.
    # cells:  ->  ->  ->  [pour]
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)
    # every upstream cell reaches the pour point
    assert out.tolist() == [[1, 1, 1, 1]]


def test_chain_draining_away_is_not_marked():
    # Cells flow West (code 16) away from the only pour point on the right.
    # The pour point drains (itself); nothing upstream of it exists.
    fdr = np.array([[16, 16, 16, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)
    # cells 0..2 flow further West off-grid -> do not reach the pour point
    assert out.tolist() == [[0, 0, 0, 1]]


def test_two_cell_cycle_with_no_pour_terminates_and_marks_zero():
    # Regression for the WBT hang: two cells point at each other.
    # left flows East (1) into right; right flows West (16) into left.
    fdr = np.array([[1, 16]], dtype=np.uint8)
    pour = np.array([[0, 0]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)  # must return, not hang
    assert out.tolist() == [[0, 0]]


def test_four_cell_cycle_with_no_pour_terminates_and_marks_zero():
    # 2x2 rotational cycle: (0,0)->E->(0,1)->S->(1,1)->W->(1,0)->N->(0,0)
    fdr = np.array([[1, 4],
                    [64, 16]], dtype=np.uint8)
    pour = np.zeros((2, 2), dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)  # must return, not hang
    assert out.tolist() == [[0, 0], [0, 0]]


def test_cell_upstream_of_cycle_not_marked():
    # A feeder cell flows into a closed cycle that never reaches a pour point.
    # layout (1 row, 3 cols): feeder(E) -> A(E) -> B(W back to A)
    #   col0 -> col1 -> col2, col2 -> col1  => cycle between col1 and col2
    fdr = np.array([[1, 1, 16]], dtype=np.uint8)
    pour = np.array([[0, 0, 0]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)  # must return, not hang
    assert out.tolist() == [[0, 0, 0]]


def test_nodata_sink_does_not_drain():
    # Single non-pour sink cell.
    fdr = np.array([[255]], dtype=np.uint8)
    pour = np.array([[0]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)
    assert out.tolist() == [[0]]


def test_branching_tributaries_all_reach_pour():
    # Two tributaries merge then flow into a pour point.
    #   (0,0) SE(2) ->(1,1)
    #   (0,2) SW(8) ->(1,1)
    #   (1,1) S(4)  ->(2,1) = pour
    fdr = np.array([[2, 255, 8],
                    [255, 4, 255],
                    [255, 255, 255]], dtype=np.uint8)
    pour = np.zeros((3, 3), dtype=np.uint8)
    pour[2, 1] = 1
    out = drains_to_dprst_kernel(fdr, pour)
    assert out[0, 0] == 1   # NW tributary
    assert out[0, 2] == 1   # NE tributary
    assert out[1, 1] == 1   # confluence
    assert out[2, 1] == 1   # pour point
    assert out[0, 1] == 0   # untouched nodata cell


def test_off_window_flow_does_not_drain():
    # A cell flowing North off the top edge terminates as does-not-drain.
    fdr = np.array([[64]], dtype=np.uint8)
    pour = np.array([[0]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour)
    assert out.tolist() == [[0]]


def test_custom_nodata_value_terminates():
    # fdr_nodata is configurable; 0 here marks the sink.
    fdr = np.array([[1, 0]], dtype=np.uint8)
    pour = np.array([[0, 0]], dtype=np.uint8)
    out = drains_to_dprst_kernel(fdr, pour, fdr_nodata=0)
    assert out.tolist() == [[0, 0]]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_drains_kernel.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'gfv2_params.d8_routing'`

- [ ] **Step 3: Implement the kernel**

Create `src/gfv2_params/d8_routing.py`:

```python
"""In-process D8 upslope traversal for the depstor routing step.

Replaces WhiteboxTools `Watershed`, which traces each cell downstream with no
memoization and no cycle guard and so hangs on flow cycles / pathological flats
in the CONUS FDR (it stalled mid-VPU-2 for 3+ hours; see
docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md).

`drains_to_dprst_kernel` answers, for every cell: does following the ESRI-D8
flow pointer downstream eventually reach a depression pour-point? It is the
upslope contributing area of the pour-point set on a functional (out-degree-1)
flow graph — a textbook O(N) traversal.

This is the ONLY numba user in the package; it is deliberately isolated here so
the widely-imported `depstor.py` stays numba-free.

ESRI D8 encoding (value -> downstream neighbour):
    1=E  2=SE  4=S  8=SW  16=W  32=NW  64=N  128=NE
Any other value (notably nodata 255, or 0) is treated as a sink/terminus.
"""

from __future__ import annotations

import numpy as np
from numba import njit

# State coloring used during traversal.
_UNKNOWN = 0
_DRAINS = 1
_NOT = 2
_ACTIVE = 3  # currently on the path being walked (detects cycles)


@njit(cache=True)
def _resolve(fdr, pour, fdr_nodata):
    ny, nx = fdr.shape
    st = np.zeros((ny, nx), dtype=np.uint8)

    # Seed: every pour-point cell drains (to itself / the depression).
    for r in range(ny):
        for c in range(nx):
            if pour[r, c] == 1:
                st[r, c] = _DRAINS

    # Reusable path stack (flat r/c), grown on demand. Holds only the single
    # downstream path currently being walked — bounded by the longest flow
    # path, not by N — so it stays small (a few MB) in practice.
    cap = 1 << 20
    stack_r = np.empty(cap, dtype=np.int64)
    stack_c = np.empty(cap, dtype=np.int64)

    for sr in range(ny):
        for sc in range(nx):
            if st[sr, sc] != _UNKNOWN:
                continue
            n = 0
            cr = sr
            cc = sc
            result = _NOT
            while True:
                s = st[cr, cc]
                if s == _DRAINS:
                    result = _DRAINS
                    break
                if s == _NOT:
                    result = _NOT
                    break
                if s == _ACTIVE:
                    # Re-entered the active path => cycle. It never reached a
                    # pour point, so the whole path does not drain.
                    result = _NOT
                    break

                # Unknown: mark active and push onto the path.
                st[cr, cc] = _ACTIVE
                if n >= cap:
                    new_cap = cap * 2
                    nr_ = np.empty(new_cap, dtype=np.int64)
                    nc_ = np.empty(new_cap, dtype=np.int64)
                    nr_[:cap] = stack_r
                    nc_[:cap] = stack_c
                    stack_r = nr_
                    stack_c = nc_
                    cap = new_cap
                stack_r[n] = cr
                stack_c[n] = cc
                n += 1

                code = fdr[cr, cc]
                if code == fdr_nodata:
                    result = _NOT
                    break
                if code == 1:
                    dr = 0
                    dc = 1
                elif code == 2:
                    dr = 1
                    dc = 1
                elif code == 4:
                    dr = 1
                    dc = 0
                elif code == 8:
                    dr = 1
                    dc = -1
                elif code == 16:
                    dr = 0
                    dc = -1
                elif code == 32:
                    dr = -1
                    dc = -1
                elif code == 64:
                    dr = -1
                    dc = 0
                elif code == 128:
                    dr = -1
                    dc = 1
                else:
                    # Any other value is a sink/terminus.
                    result = _NOT
                    break

                nr2 = cr + dr
                nc2 = cc + dc
                if nr2 < 0 or nr2 >= ny or nc2 < 0 or nc2 >= nx:
                    result = _NOT  # flows off the window
                    break
                cr = nr2
                cc = nc2

            # Path compression: every cell on the path resolves to `result`.
            for i in range(n):
                st[stack_r[i], stack_c[i]] = result

    out = np.zeros((ny, nx), dtype=np.uint8)
    for r in range(ny):
        for c in range(nx):
            if st[r, c] == _DRAINS:
                out[r, c] = 1
    return out


def drains_to_dprst_kernel(fdr_win, pour_win, fdr_nodata=255):
    """Mark cells whose ESRI-D8 path reaches a depression pour-point.

    Parameters
    ----------
    fdr_win : ndarray[uint8]
        ESRI-D8 flow-direction window. Values in {1,2,4,8,16,32,64,128} are
        flow directions; `fdr_nodata` and any other value terminate as sinks.
    pour_win : ndarray[uint8]
        Pour-point mask: 1 = depression cell, 0 = background.
    fdr_nodata : int, default 255
        FDR nodata value (treated as a sink).

    Returns
    -------
    ndarray[uint8]
        1 where the cell drains to a pour-point (including the pour-points
        themselves), else 0.
    """
    fdr = np.ascontiguousarray(fdr_win, dtype=np.uint8)
    pour = np.ascontiguousarray(pour_win, dtype=np.uint8)
    return _resolve(fdr, pour, np.uint8(fdr_nodata))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_drains_kernel.py -v`
Expected: PASS (10 tests). First run includes a one-time `@njit` compile (a few seconds).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/d8_routing.py tests/test_drains_kernel.py
git commit -m "feat(depstor): cycle-safe numba D8 routing kernel

In-process replacement for WBT Watershed: a memoized downstream traversal
that marks cells draining to depression pour-points. A 4-state coloring
detects flow cycles, so it cannot hang the way WBT did on VPU 2. Pure,
O(N), isolated in d8_routing.py (the only numba user in the package).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Wire the kernel into `routing.build`; remove WBT path

**Files:**
- Modify: `src/gfv2_params/depstor_builders/routing.py`

Swap the per-tile *write-files → WBT-subprocess → read-back* block for an
in-memory kernel call, and delete the now-dead WBT helpers, the per-tile
GeoTIFF writer, and their imports. The per-VPU full-array mosaic (whole-CONUS
`vpu_id` read + `drains` accumulator + `assign_vpu_drains` + final land-mask)
is unchanged.

- [ ] **Step 1: Replace the imports block**

In `src/gfv2_params/depstor_builders/routing.py`, replace lines 12-31:

```python
import os

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
```

with:

```python
import numpy as np
import rasterio
from osgeo import gdal
from rasterio.windows import Window

from ..d8_routing import drains_to_dprst_kernel
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
from .context import BuildContext
```

(`os` and `window_transform` are dropped — they were only used by the deleted
WBT/tile-write helpers; the `..wbt` import is dropped; `drains_to_dprst_kernel`
is added.)

- [ ] **Step 2: Delete the WBT helper and the per-tile GeoTIFF writer**

Delete the entire `_run_whitebox_watershed` function (currently lines 76-89)
and the entire `_write_window_tif` function (currently lines 92-102). Leave
`_align_fdr_to_dprst_grid` untouched.

- [ ] **Step 3: Replace the per-VPU loop body**

Replace the loop + cleanup block (currently lines 139-172):

```python
    try:
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
    finally:
        if not keep_intermediates and fdr_aligned.exists():
            fdr_aligned.unlink()
```

with:

```python
    try:
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

                # In-process D8 traversal (replaces WBT Watershed). Output is
                # 1 where the cell drains to a pour-point, else 0, so
                # assign_vpu_drains treats 0 as nodata.
                ws_win = drains_to_dprst_kernel(fdr_masked, pour, fdr_nodata=255)
                assign_vpu_drains(drains, vpu_id, code, bbox, ws_win, ws_nodata=0)
                n_vpu = int((drains[r0:r1, c0:c1][vpu_win == code] == 1).sum())
                logger.info("  VPU %d: %d cells drain to dprst", code, n_vpu)
    finally:
        if not keep_intermediates and fdr_aligned.exists():
            fdr_aligned.unlink()
```

- [ ] **Step 4: Update the module docstring**

Replace the module docstring (currently lines 1-8):

```python
"""WhiteboxTools Watershed from dprst pour-points, tiled per VPU.

Routing the full-CONUS FDR + pour-points through WBT Watershed OOMs (WBT loads
every raster as f64; ~3 x 135 GB > the 503 GB node ceiling). NHDPlus VPU
boundaries follow drainage divides, so each VPU's contributing area is local: we
route each VPU in isolation (FDR masked to the VPU) and mosaic the per-VPU
results — see docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md.
"""
```

with:

```python
"""Upslope-of-depression routing, tiled per VPU.

For each cell, marks whether its ESRI-D8 flow path reaches a depression
pour-point (`drains_to_dprst.tif`). The per-tile computation is the in-process
`d8_routing.drains_to_dprst_kernel` — a cycle-safe O(N) traversal that replaced
WhiteboxTools `Watershed`, which hung on CONUS VPU 2 (a flow-cycle /
pathological-trace stall, not OOM). See
docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md.

NHDPlus VPU boundaries follow drainage divides, so each VPU's contributing area
is local: we route each VPU in isolation (FDR masked to the VPU via vpu_id) and
mosaic the per-VPU results into the CONUS grid. Memory note: this keeps the
whole-CONUS `vpu_id` + `drains` arrays in RAM (~34 GB); the file-based
~6-9 GB workstation variant is tracked in issue #129.
"""
```

- [ ] **Step 5: Verify the module imports and compiles**

Run: `pixi run -e dev python -c "import gfv2_params.depstor_builders.routing"`
Expected: no output, exit 0 (no NameError for removed `os`/`window_transform`/WBT names).

- [ ] **Step 6: Run the routing-tiling test suite**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py tests/test_drains_kernel.py -v`
Expected: PASS — the existing mosaic-helper tests and the kernel tests all pass.

- [ ] **Step 7: Commit**

```bash
git add src/gfv2_params/depstor_builders/routing.py
git commit -m "refactor(depstor): route via in-process D8 kernel, drop WBT Watershed

routing.build now calls d8_routing.drains_to_dprst_kernel on each masked
per-VPU window instead of writing tile GeoTIFFs and shelling out to WBT
Watershed (which hung on VPU 2). Removes _run_whitebox_watershed,
_write_window_tif, the per-tile temp files, and the unused os/window_transform/
wbt imports. Per-VPU full-array mosaic is otherwise unchanged. wbt.py stays
(used by compute_dem_derivatives).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Documentation audit (same branch, per CLAUDE.md)

**Files:**
- Modify: `docs/depstor_workflow.md`
- Modify: `docs/depstor_port_summary.md`
- Modify: `docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md`
- Modify: `slurm_batch/build_depstor_rasters.batch` (comment only, if present)
- Modify: `slurm_batch/RUNME.md` (if it names WBT in the routing step)

- [ ] **Step 1: Update `docs/depstor_workflow.md` item 7**

In the `getHruSro_to_dprst` block (item 7, "Level Two"), find the line
describing watershed delineation:

```
   - Runs watersheds upstream of depressions returned from `getDprst` function
     (`res1` in script, lines 191–197)
```

Add immediately after the item-7 bullet list (before item 8) a gfv2-params note
paragraph:

```
   - **gfv2-params**: the open-source port computes the upslope-of-depression
     mask in-process via `src/gfv2_params/d8_routing.py`
     (`drains_to_dprst_kernel`), a cycle-safe O(N) ESRI-D8 traversal. It
     replaced WhiteboxTools `Watershed`, which hung on CONUS VPU 2. The
     per-VPU tiling and `drains_to_dprst.tif` output schema are unchanged. See
     `docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md`.
```

- [ ] **Step 2: Update `docs/depstor_port_summary.md` — the two WBT routing rows**

In the port-mapping table, the two rows whose "What it does" column reads
`Subprocess wrapper around WhiteboxTools `Watershed`` and `Run WBT `Watershed`
against FDR + dprst pour points` both point at `routing.py`. Update both
"What it does" cells to:

```
In-process cycle-safe D8 upslope traversal (`d8_routing.drains_to_dprst_kernel`); replaced WBT `Watershed` (#129 spec 2026-05-29)
```

- [ ] **Step 3: Annotate bug #4 in `docs/depstor_port_summary.md`**

At the end of the "### 4. WhiteboxTools `Watershed` silently treats nodata as
pour points" subsection, append:

```
**Update (2026-05-29):** moot for this step — `routing` no longer uses WBT
`Watershed`. It now runs the in-process `d8_routing.drains_to_dprst_kernel`,
which takes a 1/0 pour mask directly and has no nodata-as-pour-point pitfall.
The WBT hang that prompted the switch is documented in
`docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md`.
```

- [ ] **Step 4: Add a superseded-by pointer to the 2026-05-28 spec**

At the top of `docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md`,
immediately under the `**Status:**` line, insert:

```
> **Superseded in part (2026-05-29):** the WhiteboxTools-memory framing below is
> obsolete — the real CONUS blocker was a WBT `Watershed` *hang*, not OOM. The
> per-VPU tiling this spec introduced is retained; the per-tile WBT subprocess
> is replaced by an in-process D8 kernel. See
> `2026-05-29-depstor-d8-routing-kernel-design.md`.
```

- [ ] **Step 5: Check the batch + RUNME comments**

Run: `grep -rni "watershed\|whitebox\|wbt" slurm_batch/`
For each hit in `build_depstor_rasters.batch` or `RUNME.md` that describes the
`routing` step as WBT-bound (e.g. a "sized for the WhiteboxTools routing
long-pole" comment), update the wording to "sized for the per-VPU D8 routing
pass". Do not change `compute_dem_derivatives` WBT references (still valid).
If there are no such hits, this step is a no-op — note that and move on.

- [ ] **Step 6: Commit**

```bash
git add docs/depstor_workflow.md docs/depstor_port_summary.md \
        docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md \
        slurm_batch/
git commit -m "docs(depstor): routing uses in-process D8 kernel, not WBT Watershed

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Pre-commit + oregon regression verification

**Files:** none (verification only)

- [ ] **Step 1: Run pre-commit on the changed files**

Run: `pixi run -e dev pre-commit run --all-files`
Expected: PASS (or auto-fix + re-stage, then re-run to green). Fix any ruff
findings in `d8_routing.py` / `routing.py` and amend the relevant commit.

- [ ] **Step 2: Push the branch and open the PR (let CI run pytest)**

CI is the authoritative test gate (not the head node). Push and open a PR:

```bash
git push -u origin depstor-conus-routing
gh pr create --fill --base main
```

Confirm the CI `pytest tests/` job passes (it runs `test_drains_kernel.py` and
`test_routing_tiling.py`).

- [ ] **Step 3: oregon single-VPU regression (cluster, not head node)**

The spec's correctness anchor: the kernel must equal WBT on acyclic FDR. With
the existing WBT-produced `oregon` output saved aside, rebuild routing on
`oregon` and diff. Submit from a shell with `~/.pixi/bin` on `PATH`:

```bash
# 1. save the existing (WBT) output for comparison
cp {data_root}/oregon/depstor_rasters/drains_to_dprst.tif /tmp/drains_oregon_wbt.tif

# 2. rebuild just the routing step with the new kernel
sbatch -p cpu -A impd --time=02:00:00 --ntasks=1 --cpus-per-task=8 --mem=64G \
  --output=logs/job_%j.out --error=logs/job_%j.err \
  --wrap="pixi run --as-is python scripts/build_depstor_rasters.py \
          --fabric oregon --step routing --force"
```

After it completes, diff the two rasters:

```bash
pixi run --as-is python - <<'PY'
import numpy as np, rasterio
a = rasterio.open("/tmp/drains_oregon_wbt.tif").read(1)
b = rasterio.open("{data_root}/oregon/depstor_rasters/drains_to_dprst.tif").read(1)
diff = int((a != b).sum())
print("differing cells:", diff, "of", a.size, f"({100*diff/a.size:.4f}%)")
PY
```

Expected: 0 differing cells (or a handful of flat-edge cells — investigate
anything beyond a tiny fraction). Resolve `{data_root}` from the active fabric
profile in `configs/base_config.yml`.

- [ ] **Step 4: CONUS gfv2 routing smoke run (the original blocker)**

Confirm the step that hung now completes. Submit:

```bash
sbatch -p cpu -A impd --time=08:00:00 --ntasks=1 --cpus-per-task=16 --mem=64G \
  --output=logs/job_%j.out --error=logs/job_%j.err \
  --wrap="pixi run --as-is python scripts/build_depstor_rasters.py \
          --fabric gfv2 --step routing --force"
```

Expected: every VPU logs a `VPU <n>: <count> cells drain to dprst` line
(including VPU 2, which previously hung), and the step writes
`drains_to_dprst.tif`. Watch `logs/job_<id>.err` for the per-VPU progress.

---

## Self-Review

**Spec coverage:**
- Diagnosis / goal → Tasks 1-2 (kernel + integration). ✓
- Memoized cycle-safe kernel + ESRI decode table → Task 1 (kernel code + decode ladder). ✓
- `d8_routing.py` quarantines numba → Task 1 (module is the only numba import). ✓
- Integration delete/keep list; `assign_vpu_drains(ws_nodata=0)`; `wbt.py` retained → Task 2. ✓
- Full-array mosaic unchanged / no DAG change → Task 2 (loop body only; `vpu_id`/`drains`/land-mask/write untouched). ✓
- Tests incl. 2-/3-cell cycle regressions + nodata/off-window + oregon anchor → Task 1 + Task 4 Step 3. ✓
- Docs to update (routing docstring, workflow item 7, port summary rows + bug #4, 2026-05-28 spec pointer, batch/RUNME) → Task 2 Step 4 + Task 3. ✓
- File-based mosaic future enhancement → already captured in spec + issue #129 (no task; out of scope by decision). ✓

**Placeholder scan:** `{data_root}` in Task 4 is an intentional profile-resolved path with explicit resolution instructions, not a TODO. No "TBD"/"implement later"/"add error handling" placeholders. All code steps show complete code.

**Type consistency:** `drains_to_dprst_kernel(fdr_win, pour_win, fdr_nodata=255) -> uint8` is defined identically in Task 1 and called identically in Task 2 (`drains_to_dprst_kernel(fdr_masked, pour, fdr_nodata=255)`). Output is 1/0; `assign_vpu_drains(..., ws_nodata=0)` matches. State constants `_UNKNOWN/_DRAINS/_NOT/_ACTIVE` are internal to `_resolve`. Consistent.
