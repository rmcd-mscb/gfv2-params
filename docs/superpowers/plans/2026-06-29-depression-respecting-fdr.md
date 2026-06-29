# Depression-respecting FDR for drains_to_dprst — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a breached (depression-respecting) FDR as an additional derived raster and an A/B harness that compares `drains_to_dprst` across three flow fields on VPU 09 (Prairie Pothole) and VPU 16 (Great Basin), to decide whether breach beats the fully-filled FdrFac for depression contributing area (#147).

**Architecture:** A new opt-in shared-raster builder (`compute_breached_fdr`) runs WhiteboxTools `BreachDepressionsLeastCost → D8Pointer` on the already-staged `Hydrodem_merged_fixed_<vpu>.tif`, producing `Fdr_breached_<vpu>.tif` plus a registered `fdr_breached.vrt`. A diagnostics script warps any candidate FDR onto the dprst grid (reusing routing.py's streaming alignment) and runs the existing in-process D8 kernel per VPU; a new *labeled* kernel variant attributes contributing area per depression. Nothing existing is swapped — `fdr.vrt`/`Fdr_hydrodem`/`Fdr_merged` are untouched.

**Tech Stack:** Python, WhiteboxTools (via `gfv2_params.wbt`), GDAL/rasterio, numba (the D8 kernel), pixi env, SLURM for the CONUS-VPU compute runs.

## Global Constraints

- **Never run `pytest` or heavy geo-compute on the HPC head node.** CI (`.github/workflows/ci.yml`) is the test gate; `py_compile`/import checks on the head node are fine. The CONUS-VPU breach runs go through SLURM (`pixi run --as-is`).
- **The new FDR is strictly additional.** Do not modify `fdr.vrt`, `Fdr_hydrodem_*`, `Fdr_merged_*`, or any fabric profile's `fdr_raster`. No cap or tuning knob is added to routing (#147 is a flow-field choice, not a threshold).
- **WhiteboxTools cannot read LZW + `predictor=2` GeoTIFFs** — every raster handed to a WBT subprocess (the fixed DEM, breached DEM) must be written LZW *without* `predictor=2`.
- **Paths come from the fabric/shared profile**, read via `require_config_key` / `ctx` properties with `{data_root}`/`{vpu}` placeholders — never hardcoded literals.
- **`compute_breached_fdr` is opt-in:** registered in `BUILDERS`/`STEP_ORDER` but NOT added to the default `steps:` list of `configs/shared_rasters/shared_rasters.yml` (same posture as `compute_dem_derivatives`).
- **ESRI-D8 encoding:** valid FDR codes are `{1,2,4,8,16,32,64,128}`; nodata is `255`; any other value is a sink.
- **Add a builder + its test together;** match the nearest existing test for style. Run `pixi run -e dev pre-commit run --all-files` before pushing.

---

### Task 1: Relocate diagnostics under `scripts/diagnose/`

Mechanical move so the new A/B harness has a home and the diagnostics are grouped. Isolated from the rest — its own reviewable deliverable.

**Files:**
- Create: `scripts/diagnose/__init__.py`
- Move: `scripts/diagnose_drains_to_dprst.py` → `scripts/diagnose/diagnose_drains_to_dprst.py`
- Modify: `tests/test_diagnose_drains_to_dprst.py:3` (import path)
- Modify: `slurm_batch/HPC_REFERENCE.md`, `slurm_batch/RUNME.md` (any cited path)

**Interfaces:**
- Produces: importable `scripts.diagnose.diagnose_drains_to_dprst.vpu_coverage(drains, vpu_id, land) -> dict[int, float]` (unchanged signature, new module path).

- [ ] **Step 1: Move the script and add the package marker**

```bash
mkdir -p scripts/diagnose
git mv scripts/diagnose_drains_to_dprst.py scripts/diagnose/diagnose_drains_to_dprst.py
printf '"""Per-VPU drains_to_dprst diagnostics (coverage + A/B harness)."""\n' > scripts/diagnose/__init__.py
```

- [ ] **Step 2: Update the test import**

In `tests/test_diagnose_drains_to_dprst.py` change line 3:

```python
from scripts.diagnose.diagnose_drains_to_dprst import vpu_coverage
```

- [ ] **Step 3: Update doc references**

Grep and fix any cited path:

```bash
grep -rn "scripts/diagnose_drains_to_dprst.py" slurm_batch/ docs/ README.md
```

Replace each hit (in `slurm_batch/HPC_REFERENCE.md` and `slurm_batch/RUNME.md` if present) with `scripts/diagnose/diagnose_drains_to_dprst.py`. Do NOT edit files under `docs/superpowers/plans/` from past work (those are historical records).

- [ ] **Step 4: Run the moved test**

Run: `pixi run -e dev pytest tests/test_diagnose_drains_to_dprst.py -v`
Expected: PASS (same assertions, new import resolves).

- [ ] **Step 5: Commit**

```bash
git add scripts/diagnose/ tests/test_diagnose_drains_to_dprst.py slurm_batch/
git rm --cached scripts/diagnose_drains_to_dprst.py 2>/dev/null || true
git commit -m "refactor(diagnose): move drains_to_dprst diagnostics under scripts/diagnose/ (#147)"
```

---

### Task 2: Labeled D8 kernel for per-depression contributing area

The existing `drains_to_dprst_kernel` returns a binary mask; the #147 hypothesis is about *per-depression* contributing-area size, which needs to know *which* depression each cell drains to. Add a labeled variant beside it. D8 is out-degree-1, so each cell reaches exactly one pour-point — labeling is well-defined.

**Files:**
- Modify: `src/gfv2_params/d8_routing.py` (add `_resolve_labeled` njit + public `drains_to_dprst_labeled_kernel`)
- Test: `tests/test_d8_routing_labeled.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `drains_to_dprst_labeled_kernel(fdr_win: ndarray[uint8], label_win: ndarray[int32], fdr_nodata: int = 255) -> tuple[ndarray[int32], int]` — returns `(out, n_cycles)` where `out[r,c]` is the int32 label of the depression that cell drains to (0 = none/sink). `label_win` carries each depression's unique positive id at its cells, 0 elsewhere.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_d8_routing_labeled.py
"""Per-depression labeled D8 attribution (drains_to_dprst_labeled_kernel)."""

from __future__ import annotations

import numpy as np

from gfv2_params.d8_routing import drains_to_dprst_labeled_kernel

# ESRI D8: 1=E 2=SE 4=S 8=SW 16=W 32=NW 64=N 128=NE


def test_two_depressions_get_distinct_labels_and_local_areas():
    # 1x5 row: cells 0,1 flow east into depression label 7 at col 2;
    # cells 4,3 flow west into depression label 9 at... use a 1x6 row:
    # cols: [0]->E [1]->E [2]=dep7  [3]=dep9 [4]->W [5]->W
    fdr = np.array([[1, 1, 0, 0, 16, 16]], dtype=np.uint8)
    labels = np.array([[0, 0, 7, 9, 0, 0]], dtype=np.int32)
    out, n_cycles = drains_to_dprst_labeled_kernel(fdr, labels, fdr_nodata=255)
    assert n_cycles == 0
    # cols 0,1,2 attributed to depression 7; cols 3,4,5 to depression 9
    assert out.tolist() == [[7, 7, 7, 9, 9, 9]]
    # per-depression contributing area (cells, incl. the pour cell)
    counts = np.bincount(out.ravel())
    assert counts[7] == 3
    assert counts[9] == 3


def test_cell_flowing_to_sink_gets_zero_label():
    # col0 -> E into col1; col1 has FDR nodata (sink, no depression)
    fdr = np.array([[1, 255]], dtype=np.uint8)
    labels = np.array([[0, 0]], dtype=np.int32)
    out, n_cycles = drains_to_dprst_labeled_kernel(fdr, labels, fdr_nodata=255)
    assert out.tolist() == [[0, 0]]


def test_cycle_marked_zero_and_counted():
    # col0 -> E (col1), col1 -> W (col0): a 2-cell cycle, no depression reached
    fdr = np.array([[1, 16]], dtype=np.uint8)
    labels = np.array([[0, 0]], dtype=np.int32)
    out, n_cycles = drains_to_dprst_labeled_kernel(fdr, labels, fdr_nodata=255)
    assert n_cycles == 1
    assert out.tolist() == [[0, 0]]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_d8_routing_labeled.py -v`
Expected: FAIL with `ImportError: cannot import name 'drains_to_dprst_labeled_kernel'`.

- [ ] **Step 3: Implement the labeled kernel**

Append to `src/gfv2_params/d8_routing.py` (after `_resolve`/`drains_to_dprst_kernel`). It mirrors `_resolve` but carries an int32 label array and propagates the reached pour-point's label up the path:

```python
@njit(cache=True)
def _resolve_labeled(fdr, label, fdr_nodata):
    ny, nx = fdr.shape
    st = np.zeros((ny, nx), dtype=np.uint8)
    lab = np.zeros((ny, nx), dtype=np.int32)

    # Seed: each depression cell drains to its own label.
    for r in range(ny):
        for c in range(nx):
            if label[r, c] > 0:
                st[r, c] = _DRAINS
                lab[r, c] = label[r, c]

    cap = 1 << 20
    stack_r = np.empty(cap, dtype=np.int64)
    stack_c = np.empty(cap, dtype=np.int64)
    n_cycles = 0

    for sr in range(ny):
        for sc in range(nx):
            if st[sr, sc] != _UNKNOWN:
                continue
            n = 0
            cr = sr
            cc = sc
            result = _NOT
            result_lab = 0
            while True:
                s = st[cr, cc]
                if s == _DRAINS:
                    result = _DRAINS
                    result_lab = lab[cr, cc]
                    break
                if s == _NOT:
                    result = _NOT
                    break
                if s == _ACTIVE:
                    n_cycles += 1
                    result = _NOT
                    break

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
                    dr = 0; dc = 1
                elif code == 2:
                    dr = 1; dc = 1
                elif code == 4:
                    dr = 1; dc = 0
                elif code == 8:
                    dr = 1; dc = -1
                elif code == 16:
                    dr = 0; dc = -1
                elif code == 32:
                    dr = -1; dc = -1
                elif code == 64:
                    dr = -1; dc = 0
                elif code == 128:
                    dr = -1; dc = 1
                else:
                    result = _NOT
                    break

                nr2 = cr + dr
                nc2 = cc + dc
                if nr2 < 0 or nr2 >= ny or nc2 < 0 or nc2 >= nx:
                    result = _NOT
                    break
                cr = nr2
                cc = nc2

            for i in range(n):
                rr = stack_r[i]
                ric = stack_c[i]
                st[rr, ric] = result
                if result == _DRAINS:
                    lab[rr, ric] = result_lab

    out = np.zeros((ny, nx), dtype=np.int32)
    for r in range(ny):
        for c in range(nx):
            if st[r, c] == _DRAINS:
                out[r, c] = lab[r, c]
    return out, n_cycles


def drains_to_dprst_labeled_kernel(fdr_win, label_win, fdr_nodata=255):
    """Per-cell label of the depression its ESRI-D8 path reaches (0 = none).

    Like ``drains_to_dprst_kernel`` but attributes each draining cell to a
    specific depression. ``label_win`` carries each depression region's unique
    positive id at its cells (0 background); the return holds that id for every
    cell that drains there, enabling per-depression contributing-area counts via
    ``np.bincount(out.ravel())``. Returns ``(out_int32, n_cycles)``.
    """
    fdr = np.ascontiguousarray(fdr_win, dtype=np.uint8)
    label = np.ascontiguousarray(label_win, dtype=np.int32)
    return _resolve_labeled(fdr, label, np.uint8(fdr_nodata))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_d8_routing_labeled.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/d8_routing.py tests/test_d8_routing_labeled.py
git commit -m "feat(routing): labeled D8 kernel for per-depression contributing area (#147)"
```

---

### Task 3: `compute_breached_fdr` shared-raster builder

The new artifact: breach the staged fixed Hydrodem and D8 it into `Fdr_breached_<vpu>.tif`. Single-purpose (FDR only — no FAC/slope/TWI). Reuses `_fix_dem_nodata`/`_run_wbt` from `compute_dem_derivatives`.

**Files:**
- Create: `src/gfv2_params/shared_rasters/compute_breached_fdr.py`
- Modify: `src/gfv2_params/shared_rasters/__init__.py` (register in `BUILDERS`, `STEP_ORDER`, doc table)
- Test: `tests/test_compute_breached_fdr.py`

**Interfaces:**
- Consumes: `_fix_dem_nodata`, `_run_wbt`, `DEM_NODATA` from `compute_dem_derivatives`; `find_whitebox_tools_binary` from `gfv2_params.wbt`.
- Produces: `build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict` (returns `{}`, per-VPU outputs discovered by glob, like `compute_dem_derivatives`); per-VPU `_process_vpu(vpu, input_dir, output_dir, runner, force, logger)`; module constants `BREACH_DIST`, `BREACH_FILL`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_compute_breached_fdr.py
"""Builder-level tests for compute_breached_fdr (WBT mocked).

Mirrors tests/test_wbt.py: the WBT subprocess is never actually run — we
monkeypatch _run_wbt and assert the orchestration contract (reuse the staged
fixed DEM, then issue BreachDepressionsLeastCost followed by D8Pointer with the
ESRI pointer flag; skip when the output already exists).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

import gfv2_params.shared_rasters.compute_breached_fdr as cbf

LOGGER = logging.getLogger("test_compute_breached_fdr")


def _touch(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"")


def test_reuses_fixed_dem_then_breach_then_d8(monkeypatch, tmp_path):
    vpu = "09"
    out_dir = tmp_path / "per_vpu"
    vpu_dir = out_dir / vpu
    _touch(vpu_dir / f"Hydrodem_merged_fixed_{vpu}.tif")  # already staged

    calls: list[tuple[str, list[str]]] = []

    def fake_run_wbt(runner, tool, args, logger):
        calls.append((tool, args))
        # emulate WBT writing its --output so the next step's input exists
        for a in args:
            if a.startswith("--output="):
                _touch(Path(a.split("=", 1)[1]))

    def fail_fix(*a, **k):
        raise AssertionError("_fix_dem_nodata must not run when fixed DEM exists")

    monkeypatch.setattr(cbf, "_run_wbt", fake_run_wbt)
    monkeypatch.setattr(cbf, "_fix_dem_nodata", fail_fix)

    cbf._process_vpu(vpu, out_dir, out_dir, runner="wbt", force=False, logger=LOGGER)

    tools = [t for t, _ in calls]
    assert tools == ["BreachDepressionsLeastCost", "D8Pointer"]
    breach_args = " ".join(calls[0][1])
    assert f"Hydrodem_breached_{vpu}.tif" in breach_args
    assert f"--dist={cbf.BREACH_DIST}" in breach_args
    d8_args = " ".join(calls[1][1])
    assert "--esri_pntr" in d8_args
    assert (vpu_dir / f"Fdr_breached_{vpu}.tif").exists()


def test_skips_when_output_exists_and_not_force(monkeypatch, tmp_path):
    vpu = "16"
    out_dir = tmp_path / "per_vpu"
    _touch(out_dir / vpu / f"Fdr_breached_{vpu}.tif")

    def boom(*a, **k):
        raise AssertionError("must not invoke WBT when output exists")

    monkeypatch.setattr(cbf, "_run_wbt", boom)
    cbf._process_vpu(vpu, out_dir, out_dir, runner="wbt", force=False, logger=LOGGER)


def test_missing_fixed_and_source_raises(monkeypatch, tmp_path):
    vpu = "10"
    out_dir = tmp_path / "per_vpu"
    (out_dir / vpu).mkdir(parents=True)
    monkeypatch.setattr(cbf, "_run_wbt", lambda *a, **k: None)
    with pytest.raises(FileNotFoundError):
        cbf._process_vpu(vpu, out_dir, out_dir, runner="wbt", force=False, logger=LOGGER)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_compute_breached_fdr.py -v`
Expected: FAIL (module `compute_breached_fdr` does not exist).

- [ ] **Step 3: Implement the builder**

```python
# src/gfv2_params/shared_rasters/compute_breached_fdr.py
"""Depression-respecting (breached) FDR — an additional derived raster (#147).

Opt-in shared-raster step. For each VPU it breaches the already-staged
``Hydrodem_merged_fixed_<vpu>.tif`` with WhiteboxTools
``BreachDepressionsLeastCost`` (least-cost outlet carving that PRESERVES real
closed depressions, unlike a full fill) and runs ``D8Pointer`` to produce
``Fdr_breached_<vpu>.tif``. This is strictly ADDITIONAL: it never touches the
production ``fdr.vrt`` (NHDPlus FdrFac), ``Fdr_hydrodem_*`` (richdem fill-all),
or ``Fdr_merged_*``. It exists to A/B ``drains_to_dprst`` contributing area
against the fully-filled flow field — see
docs/superpowers/specs/2026-06-29-depression-respecting-fdr-design.md and #147.

Single-purpose (FDR only): no FAC/slope/aspect/TWI — those are TWI-side
derivatives produced by compute_dem_derivatives, irrelevant to depression
routing. Reuses that module's nodata-fix and WBT-runner helpers.

Registered in BUILDERS/STEP_ORDER but NOT in the default ``steps:`` list of
configs/shared_rasters/shared_rasters.yml — users opt in explicitly.

Outputs (per VPU, in {data_root}/shared/per_vpu/<vpu>/):
- Hydrodem_breached_<vpu>.tif  (WBT BreachDepressionsLeastCost, LZW no predictor)
- Fdr_breached_<vpu>.tif       (WBT D8 pointer, Esri encoding)
"""

from __future__ import annotations

from pathlib import Path

from gfv2_params.wbt import find_whitebox_tools_binary

from .compute_dem_derivatives import DEM_NODATA, _fix_dem_nodata, _run_wbt  # noqa: F401
from .context import SharedRastersContext

# BreachDepressionsLeastCost search radius (cells). Too small -> pits that can't
# be breached within --dist fall back to fill (re-introducing the #145
# over-connection); too large -> over-carves real depressions. Start at 100 and
# tune on VPU 09 (see #147), then pin the chosen value here with rationale.
BREACH_DIST = 100
# --fill: fill any pit not breachable within --dist, so the FDR has no interior
# 0-sinks the routing kernel cannot leave. Keep True (a breach-or-fill hybrid is
# still far less over-connecting than a global fill).
BREACH_FILL = True


def _breach_and_d8(dem_fixed: Path, dem_breached: Path, fdr_out: Path,
                   runner: str, logger) -> None:
    """WBT BreachDepressionsLeastCost on the fixed DEM, then D8Pointer."""
    breach_args = [
        f"--dem={dem_fixed}",
        f"--output={dem_breached}",
        f"--dist={BREACH_DIST}",
    ]
    if BREACH_FILL:
        breach_args.append("--fill")
    _run_wbt(runner, "BreachDepressionsLeastCost", breach_args, logger)
    _run_wbt(
        runner, "D8Pointer",
        [f"--dem={dem_breached}", f"--output={fdr_out}", "--esri_pntr"],
        logger,
    )


def _process_vpu(vpu: str, input_dir: Path, output_dir: Path, runner: str,
                 force: bool, logger) -> None:
    vpu_dir = output_dir / vpu
    vpu_dir.mkdir(parents=True, exist_ok=True)

    dem_src = input_dir / vpu / f"Hydrodem_merged_{vpu}.tif"
    dem_fixed = vpu_dir / f"Hydrodem_merged_fixed_{vpu}.tif"
    dem_breached = vpu_dir / f"Hydrodem_breached_{vpu}.tif"
    fdr_out = vpu_dir / f"Fdr_breached_{vpu}.tif"

    if not force and fdr_out.exists():
        logger.info("[VPU %s] breached FDR exists (use --force to rebuild): %s",
                    vpu, fdr_out)
        return

    # Reuse the fixed DEM if compute_dem_derivatives already staged it; else
    # re-encode nodata from the source Hydrodem (shared helper).
    if force or not dem_fixed.exists():
        if not dem_src.exists():
            raise FileNotFoundError(
                f"Neither fixed DEM nor source Hydrodem found for VPU {vpu}: "
                f"{dem_fixed} / {dem_src}"
            )
        logger.info("[VPU %s] re-encoding Hydrodem nodata -> fixed DEM", vpu)
        _fix_dem_nodata(dem_src, dem_fixed, logger)
    else:
        logger.info("[VPU %s] reusing staged fixed DEM: %s", vpu, dem_fixed)

    logger.info("[VPU %s] --- WBT BreachDepressionsLeastCost (dist=%d, fill=%s) ---",
                vpu, BREACH_DIST, BREACH_FILL)
    _breach_and_d8(dem_fixed, dem_breached, fdr_out, runner, logger)
    logger.info("[VPU %s] wrote breached FDR: %s", vpu, fdr_out)


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Breach + D8 every VPU in ``ctx.vpus``. Opt-in; returns {} (per-VPU)."""
    input_dir = Path(step_cfg.get("input_dir", ctx.per_vpu_dir))
    output_dir = Path(step_cfg.get("output_dir", ctx.per_vpu_dir))

    if not ctx.vpus:
        logger.warning("compute_breached_fdr: ctx.vpus is empty, nothing to do")
        return {}

    runner = find_whitebox_tools_binary()
    logger.info("WhiteboxTools binary: %s", runner)
    for vpu in ctx.vpus:
        _process_vpu(vpu, input_dir, output_dir, runner, ctx.force, logger)
    return {}
```

- [ ] **Step 4: Register the builder**

In `src/gfv2_params/shared_rasters/__init__.py`: add `compute_breached_fdr` to the `from . import (...)` block, add `"compute_breached_fdr": compute_breached_fdr.build,` to `BUILDERS` (right after `compute_dem_derivatives`), add `"compute_breached_fdr",  # optional / parallel (#147)` to `STEP_ORDER` after `compute_dem_derivatives`, and add a doc-table line:

```
#   compute_breached_fdr     -> (none; per-VPU, optional)      shared/per_vpu/{vpu}/Fdr_breached_*.tif (depression-respecting FDR, #147)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_compute_breached_fdr.py -v`
Expected: PASS (3 tests).

Also confirm the registry imports cleanly:
Run: `pixi run python -c "from gfv2_params.shared_rasters import BUILDERS, STEP_ORDER; assert 'compute_breached_fdr' in BUILDERS and 'compute_breached_fdr' in STEP_ORDER; print('ok')"`
Expected: `ok`.

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/shared_rasters/compute_breached_fdr.py src/gfv2_params/shared_rasters/__init__.py tests/test_compute_breached_fdr.py
git commit -m "feat(shared): compute_breached_fdr builder — depression-respecting FDR (#147)"
```

---

### Task 4: Register `fdr_breached.vrt` in `build_vrt`

So sub-CONUS/custom fabrics can clip the breached FDR and point `fdr_raster` at it exactly as they do `fdr.vrt`.

**Files:**
- Modify: `src/gfv2_params/shared_rasters/build_vrt.py` (one `RASTER_TYPES` entry)
- Test: `tests/test_build_vrt.py` (add one case)

**Interfaces:**
- Consumes: existing `build_vrt.build`.
- Produces: VRT key `fdr_breached_vrt` → `shared/conus/vrt/fdr_breached.vrt` when `Fdr_breached_*.tif` tiles exist.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_build_vrt.py` (uses the existing `_make_tiny_tif` helper; FDR tiles are Byte but a Float32 tiny tile is fine for the registration/produced-key assertion):

```python
class TestBreachedFdrRegistered:
    def test_fdr_breached_builds_and_is_keyed(self, tmp_path, caplog):
        from gfv2_params.shared_rasters import build_vrt

        per_vpu = tmp_path / "per_vpu"
        (per_vpu / "09").mkdir(parents=True)
        _make_tiny_tif(per_vpu / "09" / "Fdr_breached_09.tif", value=1.0, nodata=255.0)

        class Ctx:
            pass
        ctx = Ctx()
        ctx.per_vpu_dir = per_vpu
        ctx.borders_dir = tmp_path / "nonexistent_borders"
        ctx.vrt_dir = tmp_path / "vrt"

        produced = build_vrt.build({}, ctx, __import__("logging").getLogger("t"))
        assert "fdr_breached_vrt" in produced
        assert produced["fdr_breached_vrt"].name == "fdr_breached.vrt"
        assert produced["fdr_breached_vrt"].exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_build_vrt.py::TestBreachedFdrRegistered -v`
Expected: FAIL (`fdr_breached_vrt` not in produced).

- [ ] **Step 3: Add the RASTER_TYPES entry**

In `src/gfv2_params/shared_rasters/build_vrt.py`, add to the `RASTER_TYPES` dict (after the `"fdr"` line), with a comment:

```python
    # Depression-respecting breached FDR (#147), opt-in additional artifact.
    # Same Byte/nodata=255 ESRI-D8 convention as fdr; separate VRT so fabrics
    # can opt in via fdr_raster without disturbing the production fdr.vrt.
    "fdr_breached": ("Fdr_breached_*.tif", "255"),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_build_vrt.py -v`
Expected: PASS (existing cases + the new one).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/shared_rasters/build_vrt.py tests/test_build_vrt.py
git commit -m "feat(shared): register fdr_breached.vrt for custom-fabric opt-in (#147)"
```

---

### Task 5: A/B harness `scripts/diagnose/ab_drains_to_dprst.py`

Run `drains_to_dprst` for one VPU on a selectable FDR (production / fill / breach), reusing routing.py's streaming warp + the existing kernel, and (optionally) emit per-depression contributing-area counts via the labeled kernel.

**Files:**
- Create: `scripts/diagnose/ab_drains_to_dprst.py`
- Test: `tests/test_ab_drains_to_dprst.py`

**Interfaces:**
- Consumes: `_align_fdr_to_dprst_grid` from `gfv2_params.depstor_builders.routing`; `mask_fdr_to_vpu`, `vpu_pour_points`, `read_aligned_uint8`, `vpu_bbox`, `RasterInfo` from `gfv2_params.depstor`; `drains_to_dprst_kernel`, `drains_to_dprst_labeled_kernel` from `gfv2_params.d8_routing`.
- Produces: `resolve_fdr_path(which: str, vpu: str, *, fdr_vrt: Path, per_vpu_dir: Path) -> Path` (selects production/fill/breach FDR); `per_depression_counts(labeled: ndarray) -> dict[int, int]` (label → contributing-area cell count, label 0 dropped); a `main()` CLI.

- [ ] **Step 1: Write the failing test (pure helpers, CI-safe — no WBT/GDAL warp)**

```python
# tests/test_ab_drains_to_dprst.py
"""Unit tests for the A/B harness helpers (no WBT, no warp — pure logic)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from scripts.diagnose.ab_drains_to_dprst import (
    per_depression_counts,
    resolve_fdr_path,
)


def test_resolve_fdr_path_selects_each_source():
    fdr_vrt = Path("/data/shared/gfv2_fdr.vrt")
    per_vpu = Path("/data/shared/per_vpu")
    assert resolve_fdr_path("production", "09", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == fdr_vrt
    assert resolve_fdr_path("fill", "09", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == \
        per_vpu / "09" / "Fdr_hydrodem_09.tif"
    assert resolve_fdr_path("breach", "16", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == \
        per_vpu / "16" / "Fdr_breached_16.tif"


def test_resolve_fdr_path_rejects_unknown():
    with pytest.raises(ValueError):
        resolve_fdr_path("bogus", "09", fdr_vrt=Path("x"), per_vpu_dir=Path("y"))


def test_per_depression_counts_drops_background_and_counts_labels():
    labeled = np.array([[0, 7, 7], [9, 9, 9]], dtype=np.int32)
    counts = per_depression_counts(labeled)
    assert counts == {7: 2, 9: 3}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_ab_drains_to_dprst.py -v`
Expected: FAIL (module does not exist).

- [ ] **Step 3: Implement the harness**

```python
# scripts/diagnose/ab_drains_to_dprst.py
"""A/B drains_to_dprst on one VPU across three FDR conditionings (#147).

For a single test VPU, warp a chosen flow-direction raster onto the dprst grid
(reusing the routing builder's streaming gdal.Warp) and run the in-process D8
kernel, writing a per-VPU drains_to_dprst raster. With --labels (a labeled
depression raster, e.g. wbody_regions masked to dprst) it also writes per-
depression contributing-area counts using the labeled kernel.

FDR sources (--fdr):
  production : the fabric fdr.vrt (NHDPlus FdrFac, stream-burned + filled)
  fill       : Fdr_hydrodem_<vpu>.tif (richdem fill-all on the same Hydrodem)
  breach     : Fdr_breached_<vpu>.tif (depression-respecting, this work)

The fill source shares its DEM with breach, so production-vs-fill isolates the
DEM/stream-burn difference and fill-vs-breach isolates the conditioning.

Analysis tool, not a pipeline builder (cf. diagnose_drains_to_dprst.py): paths
are passed on the CLI; nothing is registered in the DAG.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window

from gfv2_params.d8_routing import (
    drains_to_dprst_kernel,
    drains_to_dprst_labeled_kernel,
)
from gfv2_params.depstor import (
    RasterInfo,
    mask_fdr_to_vpu,
    read_aligned_uint8,
    vpu_bbox,
    vpu_pour_points,
)
from gfv2_params.depstor_builders.routing import _align_fdr_to_dprst_grid

_FDR_CHOICES = ("production", "fill", "breach")


def resolve_fdr_path(which: str, vpu: str, *, fdr_vrt: Path,
                     per_vpu_dir: Path) -> Path:
    """Map an FDR choice to its on-disk raster for this VPU."""
    if which == "production":
        return fdr_vrt
    if which == "fill":
        return per_vpu_dir / vpu / f"Fdr_hydrodem_{vpu}.tif"
    if which == "breach":
        return per_vpu_dir / vpu / f"Fdr_breached_{vpu}.tif"
    raise ValueError(f"unknown FDR choice {which!r}; expected one of {_FDR_CHOICES}")


def per_depression_counts(labeled: np.ndarray) -> dict[int, int]:
    """Label -> contributing-area cell count (background label 0 dropped)."""
    counts = np.bincount(labeled.ravel().astype(np.int64))
    return {int(lab): int(n) for lab, n in enumerate(counts) if lab > 0 and n > 0}


def _run_one_vpu(fdr_path, dprst_path, vpu_id_path, template_path, vpu_code,
                 labels_path, out_tif, out_csv, logger):
    info = RasterInfo.from_path(template_path)
    fdr_aligned = out_tif.parent / f"_fdr_aligned_{vpu_code}.tif"
    _align_fdr_to_dprst_grid(fdr_path, dprst_path, fdr_aligned, logger)
    try:
        vpu_id = read_aligned_uint8(vpu_id_path, info)
        code = int(vpu_code)
        bbox = vpu_bbox(vpu_id, code)
        if bbox is None:
            raise SystemExit(f"VPU {code} not present in {vpu_id_path}")
        r0, r1, c0, c1 = bbox
        window = Window(c0, r0, c1 - c0, r1 - r0)
        vpu_win = vpu_id[r0:r1, c0:c1]
        with rasterio.open(fdr_aligned) as fsrc, rasterio.open(dprst_path) as dsrc:
            fdr_win = fsrc.read(1, window=window)
            dprst_win = dsrc.read(1, window=window)
        fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
        pour = vpu_pour_points(dprst_win, vpu_win, code)
        drains, n_cycles = drains_to_dprst_kernel(fdr_masked, pour, fdr_nodata=255)
        n_land = int((vpu_win == code).sum())
        n_drain = int((drains[vpu_win == code] == 1).sum())
        logger.info("VPU %d [%s]: %d/%d land cells drain (%.4f); %d cycles",
                    code, out_tif.stem, n_drain, n_land,
                    (n_drain / n_land if n_land else 0.0), n_cycles)
        _write_window_uint8(drains, info, bbox, out_tif)

        if labels_path is not None:
            with rasterio.open(labels_path) as lsrc:
                label_win = lsrc.read(1, window=window).astype(np.int32)
            label_win[vpu_win != code] = 0
            labeled, _ = drains_to_dprst_labeled_kernel(fdr_masked, label_win,
                                                        fdr_nodata=255)
            counts = per_depression_counts(labeled)
            with open(out_csv, "w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["depression_label", "contributing_cells"])
                for lab in sorted(counts):
                    w.writerow([lab, counts[lab]])
            logger.info("Wrote per-depression areas (%d depressions): %s",
                        len(counts), out_csv)
    finally:
        if fdr_aligned.exists():
            fdr_aligned.unlink()


def _write_window_uint8(drains, info, bbox, out_tif):
    """Write the per-VPU drains window as a standalone GeoTIFF (nodata=255)."""
    from rasterio.transform import Affine
    r0, r1, c0, c1 = bbox
    win = drains[r0:r1, c0:c1].copy()
    # Offset the template transform to the window's top-left (col0,row0).
    transform = info.transform * Affine.translation(c0, r0)
    profile = {
        "driver": "GTiff", "dtype": "uint8", "nodata": 255,
        "width": c1 - c0, "height": r1 - r0, "count": 1,
        "crs": info.crs, "transform": transform,
        "compress": "lzw", "tiled": True, "blockxsize": 256, "blockysize": 256,
        "BIGTIFF": "YES",
    }
    out_tif.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_tif, "w", **profile) as dst:
        dst.write(win, 1)


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger("ab_drains_to_dprst")

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--vpu", required=True)
    ap.add_argument("--fdr", required=True, choices=_FDR_CHOICES)
    ap.add_argument("--fdr-vrt", required=True, type=Path,
                    help="production fdr.vrt (also the dprst-grid template)")
    ap.add_argument("--per-vpu-dir", required=True, type=Path)
    ap.add_argument("--dprst", required=True, type=Path)
    ap.add_argument("--vpu-id", required=True, type=Path)
    ap.add_argument("--template", required=True, type=Path,
                    help="dprst-grid template (the fabric fdr.vrt clip)")
    ap.add_argument("--labels", type=Path, default=None,
                    help="optional labeled depression raster for per-depression areas")
    ap.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args()

    fdr_path = resolve_fdr_path(args.fdr, args.vpu, fdr_vrt=args.fdr_vrt,
                                per_vpu_dir=args.per_vpu_dir)
    if not fdr_path.exists():
        raise SystemExit(f"FDR not found: {fdr_path}")
    out_tif = args.out_dir / f"drains_to_dprst_{args.vpu}_{args.fdr}.tif"
    out_csv = args.out_dir / f"per_depression_area_{args.vpu}_{args.fdr}.csv"
    _run_one_vpu(fdr_path, args.dprst, args.vpu_id, args.template, args.vpu,
                 args.labels, out_tif, out_csv, logger)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_ab_drains_to_dprst.py -v`
Expected: PASS (3 tests).

Also import-check the script compiles (head-node-safe):
Run: `pixi run python -c "import ast; ast.parse(open('scripts/diagnose/ab_drains_to_dprst.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add scripts/diagnose/ab_drains_to_dprst.py tests/test_ab_drains_to_dprst.py
git commit -m "feat(diagnose): A/B drains_to_dprst harness across 3 FDR conditionings (#147)"
```

---

### Task 6: SLURM batch + docs

Wire the opt-in breach step into a SLURM batch and update the canonical docs (every code change needs a docs check).

**Files:**
- Create: `slurm_batch/stage_breached_fdr.batch`
- Modify: `docs/ARCHITECTURE.md`, `slurm_batch/HPC_REFERENCE.md`

**Interfaces:** none (ops + docs).

- [ ] **Step 1: Write the SLURM batch**

Model it on the existing shared-raster batch. Confirm the exact pattern first:

```bash
sed -n '1,60p' slurm_batch/build_shared_rasters.batch
```

Create `slurm_batch/stage_breached_fdr.batch` matching that header/env idiom (SBATCH directives, `pixi run --as-is`), invoking the shared-raster orchestrator restricted to the `compute_breached_fdr` step for the two test VPUs. The orchestrator's CLI is `--config` (required), `--step` (singular; it accepts a registered builder even when not in the default `steps:` list — see `build_shared_rasters.py:88-94`), `--vpus` (csv), `--force`. The body runs:

```bash
pixi run --as-is python scripts/build_shared_rasters.py \
    --config configs/shared_rasters/shared_rasters.yml \
    --step compute_breached_fdr \
    --vpus 09,16
```

Set `--mem` per the depstor memory note (breach is per-VPU, not whole-CONUS; start `--mem=96G`, raise if WBT OOMs on VPU 09/16).

- [ ] **Step 2: Update ARCHITECTURE.md**

Add a short note in the shared-raster / FDR section: `compute_breached_fdr` is an opt-in step producing `Fdr_breached_<vpu>.tif` + `fdr_breached.vrt`, a depression-respecting FDR for #147; it is additional (never swaps `fdr.vrt`); custom fabrics may clip `fdr_breached.vrt` and point `fdr_raster` at it. Cross-link the design spec.

- [ ] **Step 3: Update HPC_REFERENCE.md**

Add the `stage_breached_fdr.batch` invocation and the A/B usage (the three `ab_drains_to_dprst.py --fdr {production,fill,breach}` runs + the `diagnose/diagnose_drains_to_dprst.py` coverage check) under a "#147 depression-respecting FDR A/B" subsection.

- [ ] **Step 4: Head-node-safe sanity check**

Run: `pixi run python -c "from gfv2_params.shared_rasters import BUILDERS; print('compute_breached_fdr' in BUILDERS)"`
Expected: `True` (confirms the `--step compute_breached_fdr` batch will resolve). Do NOT run the step here (it's heavy WBT compute — SLURM only).

- [ ] **Step 5: Commit**

```bash
git add slurm_batch/stage_breached_fdr.batch docs/ARCHITECTURE.md slurm_batch/HPC_REFERENCE.md
git commit -m "docs(shared): document opt-in breached FDR step + A/B runbook (#147)"
```

---

### Task 7: Investigation execution (HPC; user-overseen — NOT a CI/TDD task)

The empirical A/B itself. Heavy WBT compute and CONUS-window reads → SLURM, not the head node. Run after Tasks 1–6 merge. This task produces the #147 write-up, not code.

**Files:** none (ops + issue comment).

- [ ] **Step 1: Stage the breached FDR for the test VPUs**

```bash
sbatch slurm_batch/stage_breached_fdr.batch
```

Confirm `Fdr_breached_09.tif` and `Fdr_breached_16.tif` land in `shared/per_vpu/{09,16}/`. (Both VPUs already have the staged `Hydrodem_merged_fixed_*`, so only breach+D8 runs.)

- [ ] **Step 2: Run the 6-way A/B**

For each VPU in {09, 16} and each `--fdr` in {production, fill, breach}, run `scripts/diagnose/ab_drains_to_dprst.py` (via a short SLURM job or interactive compute node — never the login node) with the fabric's `dprst`, `vpu_id`, `fdr.vrt` template, and a labeled depression raster (`wbody_regions` masked to dprst). Produces `drains_to_dprst_<vpu>_<fdr>.tif` + `per_depression_area_<vpu>_<fdr>.csv`.

- [ ] **Step 3: Compare**

- Coverage: run `scripts/diagnose/diagnose_drains_to_dprst.py` (or read each run's logged land-fraction). Expect VPU 09 to drop production/fill → breach; VPU 16 to stay ~flat.
- Per-depression area: compare the `per_depression_area_*.csv` distributions (median / 90th-percentile contributing cells) across the three FDRs per VPU. Breach should shrink genuine-depression catchments toward local sizes on VPU 09 without zeroing VPU 16's terminal-playa depressions.

- [ ] **Step 4: Reference check & decision write-up**

Qualitatively compare the breach dprst-storage pattern against Driscoll et al. (2020) (PPR/glaciated-plains high, major-river corridors low). Post a #147 comment with the coverage table, per-depression distribution summary, and the decision: adopt breach (→ open a CONUS scale-up follow-up) only if VPU 09 areas shrink to plausible local catchments AND VPU 16 endorheic chains do not collapse; otherwise record why and what to try next (depth/area-thresholded fill, or the hybrid stream-burn+breach FDR).

---

## Notes for the executor

- **Run order:** Tasks 1–6 are code/docs (CI-gated, do in a worktree). Task 7 is the HPC investigation, run after merge under the user's oversight.
- **`--dist` tuning:** `BREACH_DIST`/`BREACH_FILL` are starting values; the first VPU-09 breach run (Task 7 Step 1) may warrant adjusting them. If so, update the constants (with rationale) and re-run — that is expected, not scope creep.
- **`_align_fdr_to_dprst_grid` import:** it is a module-level function in `routing.py`; importing it from the A/B script is intentional reuse (the spec's "factor for reuse") with no behavior change to the routing builder.
