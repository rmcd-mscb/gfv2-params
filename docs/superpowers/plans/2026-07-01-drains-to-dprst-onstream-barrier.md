# drains_to_dprst On-Stream Waterbody Barrier — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make on-stream (non-dprst) waterbodies act as barriers in the D8 routing traversal, so land upslope of an on-stream lake is no longer wrongly attributed to a downstream depression-storage waterbody.

**Architecture:** Add an explicit `barrier` mask to the in-process D8 kernel (`d8_routing.py`): barrier cells are seeded non-draining and terminate any flow path that reaches them, giving "first waterbody on the path wins" semantics. The `routing` builder reads the `onstream_binary.tif` the `dprst` step already emits, scopes it per-VPU with the existing `vpu_pour_points` helper, and passes it as the barrier. The result is a strict subtraction from today's `drains_to_dprst` raster.

**Tech Stack:** Python, NumPy, numba (`@njit`), rasterio, GDAL; pytest; pixi-managed env.

## Global Constraints

- Env is pixi-managed; run tests with `pixi run -e dev pytest ...`. Do **not** run pytest on the HPC head node — CI is the gate, but local single-file pytest runs here are done in the pixi dev env, not the login node.
- The kernel is the only numba user; keep it numba-compatible (typed scalars, no Python objects inside `@njit`).
- `barrier_win` is a **required positional** argument (non-backward-compatible change, approved). Every caller must pass a barrier.
- ESRI-D8 encoding: `1=E 2=SE 4=S 8=SW 16=W 32=NW 64=N 128=NE`; `255` (and any other value) = nodata/sink.
- dprst and on-stream masks are disjoint by construction; when both seed the same cell, dprst (`_DRAINS`) wins.
- Run `pixi run -e dev pre-commit run --all-files` before the final commit.

---

### Task 1: Kernel barrier support

**Files:**
- Modify: `src/gfv2_params/d8_routing.py` (the `_resolve` `@njit` function, the `drains_to_dprst_kernel` wrapper, and the module docstring)
- Test: `tests/test_drains_kernel.py`

**Interfaces:**
- Produces: `drains_to_dprst_kernel(fdr_win, pour_win, barrier_win, fdr_nodata=255) -> (out: uint8 ndarray, n_cycles: int)`. `barrier_win` is uint8, `1` = barrier cell (on-stream waterbody), `0` = background, same shape as `fdr_win`/`pour_win`. A cell whose D8 path reaches a barrier cell before a pour cell is `0` in `out`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_drains_kernel.py`:

```python
def test_barrier_blocks_upslope_from_pour():
    # Row flowing East into a pour point at the right end, with a barrier in
    # the middle:  cell0 ->  cell1 -> [barrier] -> [pour]
    # cell0/cell1 hit the barrier before the pour => not draining.
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    barrier = np.array([[0, 0, 1, 0]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    # cell0, cell1 blocked; barrier itself non-draining; pour drains itself.
    assert out.tolist() == [[0, 0, 0, 1]]
    assert n_cycles == 0


def test_barrier_downstream_of_pour_does_not_unmark():
    # Path reaches the pour BEFORE the barrier: first-waterbody-wins => drains.
    # cell0 -> [pour] -> [barrier]
    fdr = np.array([[1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 1, 0]], dtype=np.uint8)
    barrier = np.array([[0, 0, 1]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    assert out.tolist() == [[1, 1, 0]]
    assert n_cycles == 0


def test_no_barrier_is_equivalent_to_old_behavior():
    # An all-zero barrier reproduces the pre-barrier straight-chain result.
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    barrier = np.zeros_like(pour)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    assert out.tolist() == [[1, 1, 1, 1]]
    assert n_cycles == 0


def test_pour_wins_when_cell_is_both_pour_and_barrier():
    # Defensive: overlap is impossible by construction, but if a cell is both,
    # dprst (_DRAINS) must win over the barrier seed.
    fdr = np.array([[1, 255]], dtype=np.uint8)
    pour = np.array([[0, 1]], dtype=np.uint8)
    barrier = np.array([[0, 1]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    assert out.tolist() == [[1, 1]]
    assert n_cycles == 0
```

Then migrate every **existing** call in this file to pass a barrier. Mechanical rule: insert `np.zeros_like(pour)` (or `np.zeros((H, W), dtype=np.uint8)` where the fixture uses a fresh pour array) as the **third positional argument**, before any `fdr_nodata=` keyword. Representative before/after:

```python
# before
out, n_cycles = drains_to_dprst_kernel(fdr, pour)
# after
out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))

# before (custom nodata)
out, n_cycles = drains_to_dprst_kernel(fdr, pour, fdr_nodata=0)
# after
out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour), fdr_nodata=0)
```

Apply to all pre-existing tests: `test_pour_point_itself_drains`, `test_straight_chain_into_pour_point`, `test_chain_draining_away_is_not_marked`, `test_two_cell_cycle_with_no_pour_terminates_and_marks_zero`, `test_four_cell_cycle_with_no_pour_terminates_and_marks_zero`, `test_cell_upstream_of_cycle_not_marked`, `test_cycle_containing_pour_point_marks_drains`, `test_nodata_sink_does_not_drain`, `test_branching_tributaries_all_reach_pour`, `test_all_eight_directions_decode_into_a_central_pour`, `test_off_window_flow_does_not_drain`, `test_custom_nodata_value_terminates`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run -e dev pytest tests/test_drains_kernel.py -q`
Expected: FAIL — `drains_to_dprst_kernel()` / `_resolve()` got an unexpected positional argument (the new `barrier` param does not exist yet).

- [ ] **Step 3: Implement the kernel change**

In `src/gfv2_params/d8_routing.py`, change the `@njit` signature and add barrier seeding after the pour seeding:

```python
@njit(cache=True)
def _resolve(fdr, pour, barrier, fdr_nodata):
    ny, nx = fdr.shape
    st = np.zeros((ny, nx), dtype=np.uint8)

    # Seed: every pour-point cell drains (to itself / the depression).
    for r in range(ny):
        for c in range(nx):
            if pour[r, c] == 1:
                st[r, c] = _DRAINS

    # Seed barriers as non-draining termini (on-stream waterbodies). A flow
    # path that reaches a barrier before a pour resolves _NOT and stops there,
    # so its upslope land is not attributed to a downstream depression. dprst
    # wins any overlap (disjoint by construction; only seed still-_UNKNOWN cells).
    for r in range(ny):
        for c in range(nx):
            if barrier[r, c] == 1 and st[r, c] == _UNKNOWN:
                st[r, c] = _NOT
```

Update the wrapper to accept and forward `barrier_win`:

```python
def drains_to_dprst_kernel(fdr_win, pour_win, barrier_win, fdr_nodata=255):
    ...
    fdr = np.ascontiguousarray(fdr_win, dtype=np.uint8)
    pour = np.ascontiguousarray(pour_win, dtype=np.uint8)
    barrier = np.ascontiguousarray(barrier_win, dtype=np.uint8)
    return _resolve(fdr, pour, barrier, np.uint8(fdr_nodata))
```

Also update the module docstring and the `drains_to_dprst_kernel` docstring: note that pour-points (dprst) seed `_DRAINS`, barrier cells (on-stream waterbodies) seed `_NOT`, and semantics are "first waterbody on the flow path wins"; document the new `barrier_win` parameter.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_drains_kernel.py -q`
Expected: PASS (all migrated + 4 new tests green).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/d8_routing.py tests/test_drains_kernel.py
git commit -m "feat(routing): add barrier mask to D8 drains_to_dprst kernel"
```

---

### Task 2: Wire the on-stream barrier through the routing builder

**Files:**
- Modify: `src/gfv2_params/depstor_builders/routing.py` (require `onstream`, read per-VPU window, pass barrier to the kernel, module docstring)
- Test: `tests/test_routing_tiling.py`

**Interfaces:**
- Consumes: `drains_to_dprst_kernel(fdr_masked, pour, barrier, fdr_nodata=255)` from Task 1; `vpu_pour_points(mask_win, vpu_win, code) -> uint8 ndarray` (reused for the on-stream barrier — it is the generic "this VPU's cells where mask==1" op); `ctx.require("onstream")` returning the `onstream_binary.tif` path emitted by the `dprst` step.
- Produces: unchanged output contract — `{"drains_to_dprst": output_path}`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_routing_tiling.py` (it already imports `vpu_pour_points` and NumPy; add `from gfv2_params.d8_routing import drains_to_dprst_kernel` at the top if not present). This pins the tiled-helper behavior the builder relies on, without file I/O:

```python
def test_onstream_barrier_blocks_drainage_at_helper_level():
    # Single VPU (code 1). Row flows East into a dprst pour at the right end,
    # with an on-stream waterbody cell in the middle acting as a barrier.
    #   land -> land -> [onstream] -> [dprst]
    from gfv2_params.d8_routing import drains_to_dprst_kernel

    vpu = np.ones((1, 4), dtype=np.uint8)
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    dprst = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    onstream = np.array([[0, 0, 1, 0]], dtype=np.uint8)

    pour = vpu_pour_points(dprst, vpu, code=1)
    barrier = vpu_pour_points(onstream, vpu, code=1)  # reused: mask ∩ VPU
    out, _ = drains_to_dprst_kernel(fdr, pour, barrier)

    assert out.tolist() == [[0, 0, 0, 1]]  # upslope land blocked by on-stream cell
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py::test_onstream_barrier_blocks_drainage_at_helper_level -q`
Expected: FAIL — `drains_to_dprst_kernel()` missing the `barrier` positional (only if Task 1 not yet merged) OR, once Task 1 is in, the test is the new spec and passes only after confirming the helper wiring. (If Task 1 is already committed, this test passes immediately; that is acceptable — it documents the builder's intended composition and guards against regressions in the reused helper.)

- [ ] **Step 3: Implement the builder change**

In `src/gfv2_params/depstor_builders/routing.py`:

Add the require next to the other requires (after `dprst_path = ctx.require("dprst")`):

```python
    onstream_path = ctx.require("onstream")
```

Open the on-stream raster alongside FDR and dprst in the `with` block:

```python
        with rasterio.open(fdr_aligned) as fdr_src, \
                rasterio.open(dprst_path) as dprst_src, \
                rasterio.open(onstream_path) as onstream_src:
            for code in codes:
                bbox = vpu_bbox(vpu_id, code)
                r0, r1, c0, c1 = bbox
                window = Window(c0, r0, c1 - c0, r1 - r0)
                vpu_win = vpu_id[r0:r1, c0:c1]
                fdr_win = fdr_src.read(1, window=window)
                dprst_win = dprst_src.read(1, window=window)
                onstream_win = onstream_src.read(1, window=window)

                fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
                pour = vpu_pour_points(dprst_win, vpu_win, code)
                # On-stream waterbodies of THIS vpu are traversal barriers, so
                # land captured by an on-stream lake is not attributed to a
                # downstream dprst. vpu_pour_points is the generic mask∩VPU op.
                barrier = vpu_pour_points(onstream_win, vpu_win, code)
```

Pass the barrier to the kernel (replace the existing call):

```python
                ws_win, n_cycles = drains_to_dprst_kernel(
                    fdr_masked, pour, barrier, fdr_nodata=255
                )
```

Add a barrier-cell count to the per-VPU logging (place next to the existing `n_vpu` log, in the `else` branch or just before it):

```python
                n_barrier = int((barrier == 1).sum())
                if n_barrier:
                    logger.info("  VPU %d: %d on-stream barrier cell(s)", code, n_barrier)
```

Update the module docstring: state that on-stream waterbody cells (`onstream_binary.tif`) are passed as barriers so the traversal stops at the first waterbody on each flow path (dprst → drains, on-stream → not), and that `drains_to_dprst` is therefore a strict subtraction from the pre-barrier raster.

- [ ] **Step 4: Run the test to verify it passes**

Run: `pixi run -e dev pytest tests/test_routing_tiling.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/routing.py tests/test_routing_tiling.py
git commit -m "feat(routing): treat on-stream waterbodies as drains_to_dprst barriers"
```

---

### Task 3: Repo-level docs

**Files:**
- Modify: `docs/ARCHITECTURE.md` (depstor / routing section)
- Modify: `CLAUDE.md` (the `drains_to_dprst` / depstor gotchas region)

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update ARCHITECTURE.md**

In the depstor routing description, add that the `routing` step consumes `onstream_binary.tif` (from the `dprst` step) as a **barrier**: a cell is `drains_to_dprst` only if its D8 flow path reaches a depression-storage cell **before** any on-stream waterbody cell. State that this makes `drains_to_dprst` a strict subtraction from the pre-barrier behavior (coverage can only decrease) and that playas need no special handling (classified dprst, never on-stream).

- [ ] **Step 2: Update CLAUDE.md**

In the depstor conventions, add a bullet: on-stream (non-dprst) waterbodies act as traversal barriers in `routing`; land upslope of an on-stream waterbody is captured by that waterbody's stream/lake routing and must not be attributed to a downstream depression. The barrier set is the full `onstream` mask (no size filtering); the fix is a strict subtraction that can only reduce `drains_to_dprst` coverage.

- [ ] **Step 3: Run pre-commit and commit**

Run: `pixi run -e dev pre-commit run --all-files`
Expected: PASS (or auto-fix, then re-stage).

```bash
git add docs/ARCHITECTURE.md CLAUDE.md
git commit -m "docs(depstor): document on-stream waterbody barrier in drains_to_dprst"
```

---

## Validation (post-implementation, on HPC — not part of the unit-test cycle)

This is the runtime check folded in from the spec. It is **not** a pytest step; it runs the pipeline on real data.

1. Rebuild `dprst` first so the current classifier output (and `onstream_binary.tif`) is on disk. Per the depstor rebuild cascade, changing/regenerating dprst cascades into `routing`.
2. Run the `routing` step; confirm the new per-VPU "on-stream barrier cell(s)" log lines appear.
3. On VPU 15: compare `drains_to_dprst` area before vs. after — expect a decrease concentrated below on-stream lakes/reservoirs; coverage must not increase anywhere.
4. Spot-check that no legitimate terminal-basin land was dropped by a mislabeled on-stream cell (a data-quality check on the on-stream mask, not the logic).

## Self-Review notes

- **Spec coverage:** kernel barrier (Task 1), builder wiring + onstream require + per-VPU scoping + logging (Task 2), docstrings (Tasks 1–2), ARCHITECTURE.md + CLAUDE.md (Task 3), validation (runtime section). Playa handling requires no code (classifier already excludes them from `onstream`) — documented, not implemented.
- **Non-backward-compatible kernel signature:** all 12 existing kernel tests migrated in Task 1 Step 1; the builder is the only production caller and is updated in Task 2.
- **DRY:** the on-stream barrier reuses `vpu_pour_points` rather than adding a duplicate helper.
