# Same-HRU restriction for `sro_to_dprst_*` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the legacy same-HRU restriction on `sro_to_dprst_perv/imperv` — a pervious/impervious cell counts only if it drains to a depression in its **own** HRU — additively, without changing the merged binary `drains_to_dprst` path.

**Architecture:** Three new depstor raster builders upstream of the unchanged parameter aggregation: `hru_id` (rasterize `nat_hru_id`), `routing_hru` (a barrier-aware **labeled** D8 trace → per-cell reached-HRU), and `same_hru_drains` (raster-space `(labeled == hru_id) & perv/imperv`, replacing the two `intersect` steps and writing the same filenames). `depstor_params.yml` is untouched, so `sro_to_dprst_*` are corrected transparently.

**Tech Stack:** Python, NumPy, numba (`@njit`), rasterio, GDAL/`rasterio.features`, GeoPandas; pytest; pixi.

## Global Constraints

- Env is pixi-managed; run tests as `pixi run -e dev --as-is pytest <path>` with `~/.pixi/bin` on PATH. Do NOT run the full suite or pytest generally on the HPC head node; run only the specific new/changed test files.
- The kernel is the only numba user; `_resolve_labeled` must stay numba-compatible (typed scalars, no Python objects inside `@njit`).
- Paths/inputs come from the active fabric profile via `require_config_key` / `BuildContext`, never hardcoded. Use `{data_root}`/`{fabric}` placeholders in YAML.
- ESRI-D8 encoding: `1=E 2=SE 4=S 8=SW 16=W 32=NW 64=N 128=NE`; `255`/other = sink.
- New builders must not materialize a full-CONUS in-memory array except the one already-established full-CONUS `vpu_id` load; the int32 labeled output is written per-VPU windowed (it is 4× the uint8 binary — a full-grid int32 is ~68 GB).
- `same_hru_drains` writes the SAME output filenames/keys as the `intersect` step it replaces (`drains_perv_binary.tif`/`drains_imperv_binary.tif`, keys `drains_perv`/`drains_imperv`), so `depstor_params.yml` needs zero changes.
- Add a builder + a test together; register every new step in `BUILDERS` and `STEP_ORDER` (`src/gfv2_params/depstor_builders/__init__.py`) and in `configs/depstor/depstor_rasters.yml`.
- Run `pixi run -e dev pre-commit run --files <changed>` before the docs commit.

---

### Task 1: Barrier support in the labeled D8 kernel

**Files:**
- Modify: `src/gfv2_params/d8_routing.py` (`_resolve_labeled`, `drains_to_dprst_labeled_kernel`, module/function docstrings)
- Modify: `scripts/diagnose/ab_drains_to_dprst.py` (the labeled call — migrate to the new required arg)
- Test: `tests/test_drains_kernel.py` (labeled-kernel barrier cases) and migrate `tests/test_d8_routing_labeled.py`

**Interfaces:**
- Produces: `drains_to_dprst_labeled_kernel(fdr_win, label_win, barrier_win, fdr_nodata=255) -> (out_int32, n_cycles)`. `barrier_win` is a REQUIRED 3rd positional uint8 array (1 = barrier). A cell whose path reaches a barrier before a labeled depression is `0` in `out`. Labels (dprst) win over barriers on overlap (disjoint by construction).

- [ ] **Step 1: Write the failing tests** — append to `tests/test_drains_kernel.py`:

```python
from gfv2_params.d8_routing import drains_to_dprst_labeled_kernel


def test_labeled_barrier_blocks_upslope():
    # cell0 -> cell1 -> [barrier] -> [dprst label 7]
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    label = np.array([[0, 0, 0, 7]], dtype=np.int32)
    barrier = np.array([[0, 0, 1, 0]], dtype=np.uint8)
    out, n = drains_to_dprst_labeled_kernel(fdr, label, barrier)
    assert out.tolist() == [[0, 0, 0, 7]]
    assert n == 0


def test_labeled_no_barrier_matches_unbarriered():
    # all-zero barrier reproduces the straight-chain label fill
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    label = np.array([[0, 0, 0, 7]], dtype=np.int32)
    barrier = np.zeros_like(label, dtype=np.uint8)
    out, n = drains_to_dprst_labeled_kernel(fdr, label, barrier)
    assert out.tolist() == [[7, 7, 7, 7]]
    assert n == 0


def test_labeled_first_waterbody_wins():
    # cell0 -> [dprst 5] -> [barrier]: label reached before barrier
    fdr = np.array([[1, 1, 255]], dtype=np.uint8)
    label = np.array([[0, 5, 0]], dtype=np.int32)
    barrier = np.array([[0, 0, 1]], dtype=np.uint8)
    out, n = drains_to_dprst_labeled_kernel(fdr, label, barrier)
    assert out.tolist() == [[5, 5, 0]]
    assert n == 0
```

Also migrate every existing `drains_to_dprst_labeled_kernel(fdr, label)` call (in `tests/test_d8_routing_labeled.py`) to pass `np.zeros_like(label, dtype=np.uint8)` as the 3rd positional arg.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pixi run -e dev --as-is pytest tests/test_drains_kernel.py tests/test_d8_routing_labeled.py -q`
Expected: FAIL — `_resolve_labeled()` / `drains_to_dprst_labeled_kernel()` got an unexpected positional argument.

- [ ] **Step 3: Implement the barrier** — in `src/gfv2_params/d8_routing.py`, change the signature and seed barriers after the label seeding:

```python
@njit(cache=True)
def _resolve_labeled(fdr, label, barrier, fdr_nodata):
    ny, nx = fdr.shape
    st = np.zeros((ny, nx), dtype=np.uint8)
    lab = np.zeros((ny, nx), dtype=np.int32)

    # Seed: each depression cell drains to its own label.
    for r in range(ny):
        for c in range(nx):
            if label[r, c] > 0:
                st[r, c] = _DRAINS
                lab[r, c] = label[r, c]

    # Seed barriers (on-stream waterbodies) as non-draining termini, but only
    # where not already a label cell — labels win any overlap.
    for r in range(ny):
        for c in range(nx):
            if barrier[r, c] == 1 and st[r, c] == _UNKNOWN:
                st[r, c] = _NOT
```

(The rest of `_resolve_labeled` is unchanged.) Update the wrapper:

```python
def drains_to_dprst_labeled_kernel(fdr_win, label_win, barrier_win, fdr_nodata=255):
    ...
    fdr = np.ascontiguousarray(fdr_win, dtype=np.uint8)
    label = np.ascontiguousarray(label_win, dtype=np.int32)
    barrier = np.ascontiguousarray(barrier_win, dtype=np.uint8)
    return _resolve_labeled(fdr, label, barrier, np.uint8(fdr_nodata))
```

Update the docstrings to document `barrier_win` and the "first waterbody wins / labels win overlap" semantics. Then in `scripts/diagnose/ab_drains_to_dprst.py`, change the labeled call to pass a no-op barrier:

```python
labeled, _ = drains_to_dprst_labeled_kernel(fdr_masked, label_win, np.zeros_like(label_win, dtype=np.uint8), fdr_nodata=255)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run -e dev --as-is pytest tests/test_drains_kernel.py tests/test_d8_routing_labeled.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/d8_routing.py scripts/diagnose/ab_drains_to_dprst.py tests/test_drains_kernel.py tests/test_d8_routing_labeled.py
git commit -m "feat(routing): add barrier mask to labeled D8 kernel"
```

---

### Task 2: `hru_id` raster builder

**Files:**
- Modify: `src/gfv2_params/depstor.py` (add `rasterize_ids` helper)
- Modify: `src/gfv2_params/depstor_builders/context.py` (add `id_feature` field)
- Modify: `scripts/build_depstor_rasters.py` (read `id_feature`, pass to `BuildContext`)
- Create: `src/gfv2_params/depstor_builders/hru_id.py`
- Modify: `src/gfv2_params/depstor_builders/__init__.py` (register), `configs/depstor/depstor_rasters.yml` (add step)
- Test: `tests/test_hru_id.py`

**Interfaces:**
- Consumes: `RasterInfo.from_path`, `write_int32_regions(arr, info, out_path)` (existing).
- Produces: `rasterize_ids(gdf, id_field, info) -> np.ndarray[int32]` (0 = no polygon); `hru_id` builder writing `hru_id.tif` (int32) and registering key `"hru_id"`. `BuildContext.id_feature: str`.

- [ ] **Step 1: Write the failing test** — `tests/test_hru_id.py`:

```python
import geopandas as gpd
import numpy as np
from shapely.geometry import box

from gfv2_params.depstor import RasterInfo, rasterize_ids


def test_rasterize_ids_burns_attribute(tmp_path):
    # 4x4 grid, 1.0 cell size, origin (0, 4) north-up
    import rasterio
    from rasterio.transform import from_origin
    tpl = tmp_path / "tpl.tif"
    transform = from_origin(0, 4, 1, 1)
    with rasterio.open(tpl, "w", driver="GTiff", height=4, width=4, count=1,
                       dtype="uint8", crs="EPSG:5070", transform=transform) as d:
        d.write(np.zeros((4, 4), np.uint8), 1)
    info = RasterInfo.from_path(tpl)
    gdf = gpd.GeoDataFrame(
        {"nat_hru_id": [11, 22]},
        geometry=[box(0, 0, 2, 4), box(2, 0, 4, 4)], crs="EPSG:5070",
    )
    out = rasterize_ids(gdf, "nat_hru_id", info)
    assert out.dtype == np.int32
    assert out[0, 0] == 11 and out[0, 3] == 22   # left half 11, right half 22
```

- [ ] **Step 2: Run to verify it fails**

Run: `pixi run -e dev --as-is pytest tests/test_hru_id.py -q`
Expected: FAIL — `cannot import name 'rasterize_ids'`.

- [ ] **Step 3: Implement `rasterize_ids`** — in `src/gfv2_params/depstor.py` (near `rasterize_binary`):

```python
def rasterize_ids(gdf, id_field: str, info: "RasterInfo") -> np.ndarray:
    """Burn an integer id attribute onto the template grid (0 = no polygon)."""
    from rasterio.features import rasterize
    shapes = ((geom, int(val)) for geom, val in zip(gdf.geometry, gdf[id_field]))
    return rasterize(
        shapes, out_shape=(info.height, info.width), transform=info.transform,
        fill=0, dtype="int32",
    ).astype(np.int32, copy=False)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pixi run -e dev --as-is pytest tests/test_hru_id.py -q`
Expected: PASS.

- [ ] **Step 5: Thread `id_feature` + add the builder + register + config**

In `src/gfv2_params/depstor_builders/context.py`, add after `hru_layer: str`:

```python
    id_feature: str = "nat_hru_id"
```

In `scripts/build_depstor_rasters.py`, alongside the `hru_layer` read (~line 70):

```python
    id_feature = require_config_key(config, "id_feature", "build_depstor_rasters")
```

and pass `id_feature=id_feature,` into the `BuildContext(...)` call.

Create `src/gfv2_params/depstor_builders/hru_id.py`:

```python
"""Build hru_id.tif: per-cell HRU id (nat_hru_id) rasterised onto the template.

The open-source equivalent of the legacy `nhrug`. Consumed by `routing_hru`
(to label depressions by HRU) and `same_hru_drains` (the same-HRU test). This
is a raster-space HRU identity used only for the same-HRU restriction; per-HRU
parameter COUNTS still use gdptools zonal weights downstream.
"""
from __future__ import annotations

import geopandas as gpd

from ..depstor import RasterInfo, rasterize_ids, write_int32_regions
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    output_path = ctx.resolve_output(step_cfg["output"])
    if not ctx.template_path.exists():
        raise FileNotFoundError(f"Template raster not found: {ctx.template_path}")
    if not ctx.hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {ctx.hru_gpkg}")

    logger.info("--- hru_id ---")
    logger.info("  HRU fabric: %s (layer=%s, id=%s)", ctx.hru_gpkg, ctx.hru_layer, ctx.id_feature)
    logger.info("  Output    : %s", output_path)
    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"hru_id": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    gdf = gpd.read_file(ctx.hru_gpkg, layer=ctx.hru_layer)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    if (gdf[ctx.id_feature] <= 0).any():
        raise ValueError(f"{ctx.id_feature} must be positive (0 is the no-HRU sentinel).")
    ids = rasterize_ids(gdf, ctx.id_feature, info)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_int32_regions(ids, info, output_path)
    n = int((ids > 0).sum())
    logger.info("  Rasterised %d HRUs | %d labelled cells (%.2f%%)", len(gdf), n, 100 * n / ids.size)
    return {"hru_id": output_path}
```

In `src/gfv2_params/depstor_builders/__init__.py`: import `hru_id`, add `"hru_id": hru_id.build` to `BUILDERS`, and insert `"hru_id"` into `STEP_ORDER` right after `"perv"`.

In `configs/depstor/depstor_rasters.yml`, add after the `perv` step:

```yaml
  - name: hru_id
    output: hru_id.tif
```

- [ ] **Step 6: Verify + commit**

Run: `pixi run -e dev --as-is pytest tests/test_hru_id.py -q`
Expected: PASS.

```bash
git add src/gfv2_params/depstor.py src/gfv2_params/depstor_builders/context.py \
  scripts/build_depstor_rasters.py src/gfv2_params/depstor_builders/hru_id.py \
  src/gfv2_params/depstor_builders/__init__.py configs/depstor/depstor_rasters.yml \
  tests/test_hru_id.py
git commit -m "feat(depstor): hru_id raster (rasterised nat_hru_id) builder"
```

---

### Task 3: Promote the FDR-alignment helper to shared `depstor.py`

**Files:**
- Modify: `src/gfv2_params/depstor.py` (add public `align_fdr_to_dprst_grid`)
- Modify: `src/gfv2_params/depstor_builders/routing.py` (import it; delete the local `_align_fdr_to_dprst_grid`)
- Test: existing `tests/test_routing_tiling.py` (regression — routing unchanged)

**Interfaces:**
- Produces: `align_fdr_to_dprst_grid(fdr_path, dprst_path, out_path, logger) -> None` in `depstor.py` (moved verbatim from routing).

- [ ] **Step 1: Move the function** — cut the body of `_align_fdr_to_dprst_grid` from `src/gfv2_params/depstor_builders/routing.py` into `src/gfv2_params/depstor.py` as public `align_fdr_to_dprst_grid` (identical body; it already uses `gdal`, `rasterio` which `depstor.py` imports — add imports if missing). In `routing.py`, delete the local def and import it: add `align_fdr_to_dprst_grid` to the `from ..depstor import (...)` block, and change the call site `_align_fdr_to_dprst_grid(...)` → `align_fdr_to_dprst_grid(...)`.

- [ ] **Step 2: Run routing regression tests**

Run: `pixi run -e dev --as-is pytest tests/test_routing_tiling.py -q`
Expected: PASS (behavior unchanged).

- [ ] **Step 3: Commit**

```bash
git add src/gfv2_params/depstor.py src/gfv2_params/depstor_builders/routing.py
git commit -m "refactor(depstor): promote align_fdr_to_dprst_grid to shared depstor"
```

---

### Task 4: `routing_hru` builder (barrier-aware labeled trace)

**Files:**
- Create: `src/gfv2_params/depstor_builders/routing_hru.py`
- Modify: `src/gfv2_params/depstor_builders/__init__.py` (register), `configs/depstor/depstor_rasters.yml` (add step)
- Test: `tests/test_routing_hru.py`

**Interfaces:**
- Consumes: `drains_to_dprst_labeled_kernel(fdr, label, barrier, fdr_nodata=255)` (Task 1); `align_fdr_to_dprst_grid` (Task 3); `hru_id` key (Task 2); existing `mask_fdr_to_vpu`, `vpu_pour_points`, `vpu_bbox`, `vpu_codes_present`, `read_aligned_uint8`, `RasterInfo`.
- Produces: `drains_to_dprst_hru.tif` (int32; per draining cell = `nat_hru_id` of the reached depression, 0 else), key `"drains_to_dprst_hru"`.

- [ ] **Step 1: Write the failing test** — `tests/test_routing_hru.py` (helper-level composition, no file I/O; mirrors `test_routing_tiling.py` style):

```python
import numpy as np
from gfv2_params.depstor import mask_fdr_to_vpu, vpu_pour_points
from gfv2_params.d8_routing import drains_to_dprst_labeled_kernel


def test_labeled_trace_attributes_to_reached_hru_with_barrier():
    # 1x5 single VPU (code 1). land -> land -> [dprst in HRU 42] , and a second
    # chain where an on-stream barrier blocks the reach.
    #   col: 0    1        2(dprst,HRU42)   3(onstream)    4(dprst,HRU9)
    # flow all East; col4 pours to HRU9 but col3 is a barrier.
    vpu = np.ones((1, 5), dtype=np.uint8)
    fdr = np.array([[1, 1, 255, 1, 255]], dtype=np.uint8)
    dprst = np.array([[0, 0, 1, 0, 1]], dtype=np.uint8)
    onstream = np.array([[0, 0, 0, 1, 0]], dtype=np.uint8)
    hru = np.array([[7, 7, 42, 8, 9]], dtype=np.int32)

    fdr_m = mask_fdr_to_vpu(fdr, vpu, code=1)
    label = np.where((dprst == 1) & (vpu == 1), hru, 0).astype(np.int32)
    barrier = vpu_pour_points(onstream, vpu, code=1)
    out, _ = drains_to_dprst_labeled_kernel(fdr_m, label, barrier)
    # col0,col1 reach dprst HRU42; col2 is the dprst (label 42); col3 barrier=0;
    # col4 is its own dprst (label 9).
    assert out.tolist() == [[42, 42, 42, 0, 9]]
```

- [ ] **Step 2: Run to verify it fails / passes**

Run: `pixi run -e dev --as-is pytest tests/test_routing_hru.py -q`
Expected: PASS once Task 1 is in (this pins the composition `routing_hru.build` relies on). Acceptable per the additive design (documents intended wiring).

- [ ] **Step 3: Implement the builder** — `src/gfv2_params/depstor_builders/routing_hru.py`. It mirrors `routing.build` but runs the labeled kernel and writes int32 **per-VPU windowed** (the labeled output is int32 ≈ 4× the binary — never held whole-CONUS; read-modify-write each VPU window so overlapping bboxes don't clobber neighbours):

```python
"""HRU-labeled, barrier-aware upslope routing → drains_to_dprst_hru.tif.

Same per-VPU D8 tiling as `routing`, but each depression cell is labelled by its
HRU id (hru_id.tif) and the labeled kernel attributes every draining cell to the
HRU of the depression it reaches. On-stream waterbodies are barriers. Written
per-VPU windowed: the int32 output is ~4x the binary drains, so it is never held
whole-CONUS.
"""
from __future__ import annotations

import numpy as np
import rasterio
from rasterio.windows import Window

from ..d8_routing import drains_to_dprst_labeled_kernel
from ..depstor import (
    RasterInfo, align_fdr_to_dprst_grid, mask_fdr_to_vpu, read_aligned_uint8,
    vpu_bbox, vpu_codes_present, vpu_pour_points,
)
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.fdr_raster is None or not ctx.fdr_raster.exists():
        raise FileNotFoundError(f"routing_hru needs fdr_raster: {ctx.fdr_raster}")
    output_path = ctx.resolve_output(step_cfg["output"])
    dprst_path = ctx.require("dprst")
    onstream_path = ctx.require("onstream")
    vpu_id_path = ctx.require("vpu_id")
    hru_id_path = ctx.require("hru_id")
    keep_intermediates = bool(step_cfg.get("keep_intermediates", False))

    logger.info("--- routing_hru (per-VPU labeled) ---")
    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"drains_to_dprst_hru": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fdr_aligned = output_path.parent / "fdr_aligned_hru.tif"

    try:
        align_fdr_to_dprst_grid(ctx.fdr_raster, dprst_path, fdr_aligned, logger)
        vpu_id = read_aligned_uint8(vpu_id_path, info)
        codes = vpu_codes_present(vpu_id)

        profile = dict(
            driver="GTiff", height=info.height, width=info.width, count=1,
            dtype="int32", crs=info.crs, transform=info.transform, nodata=0,
            compress="LZW", tiled=True, blockxsize=256, blockysize=256, bigtiff="YES",
        )
        with rasterio.open(output_path, "w", **profile) as dst, \
                rasterio.open(fdr_aligned) as fdr_src, \
                rasterio.open(dprst_path) as dprst_src, \
                rasterio.open(onstream_path) as onstream_src, \
                rasterio.open(hru_id_path) as hru_src:
            for code in codes:
                bbox = vpu_bbox(vpu_id, code)
                r0, r1, c0, c1 = bbox
                window = Window(c0, r0, c1 - c0, r1 - r0)
                vpu_win = vpu_id[r0:r1, c0:c1]
                fdr_win = fdr_src.read(1, window=window)
                dprst_win = dprst_src.read(1, window=window)
                onstream_win = onstream_src.read(1, window=window)
                hru_win = hru_src.read(1, window=window)

                fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
                label = np.where((dprst_win == 1) & (vpu_win == code), hru_win, 0).astype(np.int32)
                barrier = vpu_pour_points(onstream_win, vpu_win, code)
                out, n_cycles = drains_to_dprst_labeled_kernel(fdr_masked, label, barrier, fdr_nodata=255)
                if n_cycles:
                    logger.warning("  VPU %d: %d flow cycle(s) — cells non-draining", code, n_cycles)

                # read-modify-write only this VPU's cells (bboxes overlap at corners)
                existing = dst.read(1, window=window)
                sel = (vpu_win == code) & (out > 0)
                existing[sel] = out[sel]
                dst.write(existing, 1, window=window)
                logger.info("  VPU %d: %d labelled drain cells", code, int(sel.sum()))
    finally:
        if not keep_intermediates and fdr_aligned.exists():
            fdr_aligned.unlink()
    return {"drains_to_dprst_hru": output_path}
```

Register in `__init__.py`: import `routing_hru`, add `"routing_hru": routing_hru.build` to `BUILDERS`, insert `"routing_hru"` in `STEP_ORDER` right after `"routing"`. In `configs/depstor/depstor_rasters.yml`, add after the `routing` step:

```yaml
  - name: routing_hru
    output: drains_to_dprst_hru.tif
```

- [ ] **Step 4: Run the test**

Run: `pixi run -e dev --as-is pytest tests/test_routing_hru.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/routing_hru.py src/gfv2_params/depstor_builders/__init__.py \
  configs/depstor/depstor_rasters.yml tests/test_routing_hru.py
git commit -m "feat(depstor): routing_hru labeled barrier-aware trace (drains_to_dprst_hru)"
```

---

### Task 5: `same_hru_drains` builder (replaces the two `intersect` steps)

**Files:**
- Create: `src/gfv2_params/depstor_builders/same_hru_drains.py`
- Modify: `src/gfv2_params/depstor_builders/__init__.py` (`drains_perv`/`drains_imperv` → `same_hru_drains.build`), `configs/depstor/depstor_rasters.yml` (repoint the two steps' `inputs`)
- Test: `tests/test_same_hru_drains.py`

**Interfaces:**
- Consumes: keys `drains_to_dprst_hru` (int32), `hru_id` (int32), `perv`/`imperv` (uint8); `RasterInfo`, `uint8_binary_profile`, `assert_raster_aligned`.
- Produces: `drains_perv_binary.tif` / `drains_imperv_binary.tif` (uint8 1/255), keys `drains_perv`/`drains_imperv` (via `output_key`) — SAME as the replaced `intersect` step.

- [ ] **Step 1: Write the failing test** — `tests/test_same_hru_drains.py`:

```python
import numpy as np
from gfv2_params.depstor import same_hru_intersect


def test_same_hru_intersect_keeps_only_matching_hru():
    labeled = np.array([[42, 42, 9, 0]], dtype=np.int32)   # reached-HRU per cell
    hru_id = np.array([[42, 8, 9, 5]], dtype=np.int32)     # cell's own HRU
    land = np.array([[1, 1, 1, 1]], dtype=np.uint8)         # perv everywhere
    out = same_hru_intersect(labeled, hru_id, land)
    # col0: 42==42 & perv -> 1 ; col1: 42!=8 -> 255 ; col2: 9==9 -> 1 ; col3: 0!=5 -> 255
    assert out.tolist() == [[1, 255, 1, 255]]
```

- [ ] **Step 2: Run to verify it fails**

Run: `pixi run -e dev --as-is pytest tests/test_same_hru_drains.py -q`
Expected: FAIL — `cannot import name 'same_hru_intersect'`.

- [ ] **Step 3: Implement helper + builder**

In `src/gfv2_params/depstor.py` (near `intersect_binaries`):

```python
def same_hru_intersect(labeled: np.ndarray, hru_id: np.ndarray, land: np.ndarray) -> np.ndarray:
    """1 where a land cell drains to a depression in its OWN HRU, else 255."""
    hit = (labeled == hru_id) & (labeled > 0) & (land == 1)
    out = np.full(land.shape, np.uint8(255), dtype=np.uint8)
    out[hit] = 1
    return out
```

Create `src/gfv2_params/depstor_builders/same_hru_drains.py`:

```python
"""Same-HRU drains: land cells draining to a depression in their OWN HRU.

Replaces the plain `intersect` for drains_perv/drains_imperv. The same-HRU
restriction is a RASTER-SPACE intersection (labeled drains == rasterised hru_id),
applied before aggregation -- NOT a gdptools operation -- because it is a
per-cell comparison gdptools' partial-pixel weights cannot express. It
reproduces the legacy `Con(rSro == hru)` (docs/0b_TB_depr_stor.py:214). The
per-HRU COUNT downstream still uses gdptools.
"""
from __future__ import annotations

import rasterio
from rasterio.windows import Window

from ..depstor import RasterInfo, assert_raster_aligned, same_hru_intersect, uint8_binary_profile
from .context import BuildContext

STRIP_ROWS = 1024


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    name = step_cfg["name"]
    inputs = step_cfg["inputs"]  # [drains_to_dprst_hru, hru_id, perv|imperv]
    if not isinstance(inputs, list) or len(inputs) != 3:
        raise ValueError(f"same_hru_drains step '{name}' needs inputs: [labeled, hru_id, land]")
    labeled_path = ctx.require(inputs[0])
    hru_path = ctx.require(inputs[1])
    land_path = ctx.require(inputs[2])
    output_path = ctx.resolve_output(step_cfg["output"])
    output_key = step_cfg.get("output_key", name)

    logger.info("--- %s (same-HRU) ---", name)
    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {output_key: output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    n_hit = 0
    with rasterio.open(labeled_path) as lab_src, rasterio.open(hru_path) as hru_src, \
            rasterio.open(land_path) as land_src, \
            rasterio.open(output_path, "w", **uint8_binary_profile(info)) as dst:
        assert_raster_aligned(lab_src, info, inputs[0])
        assert_raster_aligned(hru_src, info, inputs[1])
        assert_raster_aligned(land_src, info, inputs[2])
        for row_off in range(0, info.height, STRIP_ROWS):
            h = min(STRIP_ROWS, info.height - row_off)
            window = Window(0, row_off, info.width, h)
            out = same_hru_intersect(lab_src.read(1, window=window),
                                     hru_src.read(1, window=window),
                                     land_src.read(1, window=window))
            dst.write(out, 1, window=window)
            n_hit += int((out == 1).sum())
    logger.info("  %d same-HRU %s cells", n_hit, output_key)
    return {output_key: output_path}
```

In `__init__.py`: import `same_hru_drains`; change `"drains_perv": intersect.build` and `"drains_imperv": intersect.build` to `same_hru_drains.build`. (`intersect` stays imported only if still referenced elsewhere; if not, drop it from the import.) In `configs/depstor/depstor_rasters.yml`, repoint the two steps' inputs:

```yaml
  - name: drains_perv
    inputs: [drains_to_dprst_hru, hru_id, perv]
    output_key: drains_perv
    output: drains_perv_binary.tif
  - name: drains_imperv
    inputs: [drains_to_dprst_hru, hru_id, imperv]
    output_key: drains_imperv
    output: drains_imperv_binary.tif
```

- [ ] **Step 4: Run the test**

Run: `pixi run -e dev --as-is pytest tests/test_same_hru_drains.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor.py src/gfv2_params/depstor_builders/same_hru_drains.py \
  src/gfv2_params/depstor_builders/__init__.py configs/depstor/depstor_rasters.yml \
  tests/test_same_hru_drains.py
git commit -m "feat(depstor): same_hru_drains restricts drains_perv/imperv to own HRU"
```

---

### Task 6: Docs

**Files:**
- Modify: `docs/ARCHITECTURE.md`, `CLAUDE.md`

- [ ] **Step 1: ARCHITECTURE.md** — in the depstor section, document the new chain (`hru_id` → `routing_hru` → `same_hru_drains`) and state, verbatim in intent: the same-HRU restriction on `sro_to_dprst_perv/imperv` is a **raster-space intersection** — labeled drains (`drains_to_dprst_hru.tif`) compared cell-by-cell against a hard-rasterised `hru_id.tif` — applied **before** aggregation, **not** a gdptools operation, **because** it is a per-cell test (reached-HRU vs. own HRU) that gdptools' partial-pixel weighting cannot express; the per-HRU **count still uses gdptools**; it reproduces legacy `Con(rSro == hru)` (`0b_TB_depr_stor.py:214`); tradeoff is a 1-pixel HRU-boundary approximation, immaterial vs. the basin-scale signal. Note `drains_to_dprst`/`drains_to_dprst_frac` stay HRU-agnostic.

- [ ] **Step 2: CLAUDE.md** — add a depstor gotchas bullet: `sro_to_dprst_perv/imperv` are same-HRU-restricted via a raster intersection (`drains_to_dprst_hru == hru_id`) in `same_hru_drains`, NOT via gdptools — because it's a per-cell reached-HRU-vs-own-HRU test; per-HRU counts still use gdptools; matches legacy `Con(rSro == hru)`. `drains_to_dprst` itself stays HRU-agnostic.

- [ ] **Step 3: pre-commit + commit**

Run: `pixi run -e dev pre-commit run --files docs/ARCHITECTURE.md CLAUDE.md`
Expected: PASS (or auto-fix, re-stage).

```bash
git add docs/ARCHITECTURE.md CLAUDE.md
git commit -m "docs(depstor): document raster-space same-HRU restriction for sro_to_dprst_*"
```

---

## Self-Review notes

- **Spec coverage:** hru_id (T2), labeled-kernel barrier (T1), routing_hru (T4, needs the T3 shared align helper), same_hru_drains replacing intersect (T5), docs incl. the raster-vs-gdptools requirement (T6), CONUS per-VPU-windowed int32 write (T4). `depstor_params.yml` unchanged (verified — `same_hru_drains` reuses filenames/keys).
- **Type consistency:** labeled kernel returns int32; `hru_id`/`drains_to_dprst_hru` are int32; `same_hru_intersect(labeled:int32, hru_id:int32, land:uint8) -> uint8`; `rasterize_ids -> int32`. Keys `drains_to_dprst_hru`, `hru_id`, `drains_perv`, `drains_imperv` consistent across T2/T4/T5 and the config.
- **No same-HRU restriction leaks into `drains_to_dprst_frac`:** that param still sources `drains_to_dprst.tif` from `routing` (unchanged).
- **Validation (post-merge, HPC):** rebuild from `hru_id` (or full depstor), then re-aggregate `sro_to_dprst_*`; expect the VPU-15 shift (0.0256 → 0.0207 area-weighted; ~945 cross-HRU HRUs drop toward 0). Not a unit-test step.
