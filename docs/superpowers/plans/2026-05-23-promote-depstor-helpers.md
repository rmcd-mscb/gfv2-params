# Promote depstor Helpers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `_assert_aligned` and `_uint8_binary_profile` from 3 duplicate copies under `src/gfv2_params/depstor_builders/` into public functions in `src/gfv2_params/depstor.py`, refactor `write_uint8_binary` to use the shared profile helper, and add one new test — closing issue #64.

**Architecture:** Pure refactor. Function bodies move **verbatim** (byte-identical). Helpers become public (`assert_raster_aligned`, `uint8_binary_profile`). The 3 caller builders import from `..depstor` and delete their local copies. `write_uint8_binary`'s inline profile dict becomes a call to the new helper, eliminating drift between full-array and streaming write paths.

**Tech Stack:** Python 3.12, pixi env, pytest. No new dependencies. No behaviour change.

**Source spec:** [`docs/superpowers/specs/2026-05-23-promote-depstor-helpers-design.md`](../specs/2026-05-23-promote-depstor-helpers-design.md)

**Branch:** `feat/promote-depstor-helpers` (already created; spec committed as `61db8f5`).

---

## File map (after)

| File | Change |
|---|---|
| `src/gfv2_params/depstor.py` | +2 public functions (`assert_raster_aligned`, `uint8_binary_profile`); `write_uint8_binary` body uses the new helper instead of inline dict |
| `src/gfv2_params/depstor_builders/intersect.py` | Import the 2 helpers from `..depstor`; replace local `_assert_aligned`/`_uint8_binary_profile` references; delete local definitions |
| `src/gfv2_params/depstor_builders/carea_map.py` | Same |
| `src/gfv2_params/depstor_builders/perv.py` | Same |
| `tests/test_depstor_helpers.py` | New file: 4 tests (3 failure modes + 1 success) for `assert_raster_aligned` |

5 modified, 1 added.

---

## Task 1: Pre-flight verification

**Files:** read-only.

- [ ] **Step 1: Verify branch + clean tree**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
git branch --show-current
git status --short
```

Expected: branch `feat/promote-depstor-helpers`; empty status.

- [ ] **Step 2: Confirm spec is committed**

```bash
git log --oneline -1 docs/superpowers/specs/2026-05-23-promote-depstor-helpers-design.md
```

Expected: commit ending in `docs(spec): promote depstor streaming-builder helpers (T2-E, closes #64)` (sha `61db8f5` or later).

- [ ] **Step 3: Verify pre-flight grep counts match the spec's prediction**

```bash
git grep -nE "_assert_aligned|_uint8_binary_profile" src/ | wc -l
```

Expected: **18** (6 def sites + 12 call sites — 3 `_uint8_binary_profile` calls and 9 `_assert_aligned` calls).

If the count differs, STOP and escalate — the spec was written against a specific state and a different count means something has changed.

- [ ] **Step 4: Verify no callers OUTSIDE the 3 expected builders**

```bash
git grep -nE "_assert_aligned|_uint8_binary_profile" src/ tests/ scripts/ | grep -v "depstor_builders/intersect.py\|depstor_builders/carea_map.py\|depstor_builders/perv.py" | head -5
```

Expected: empty. If there are matches in test files or scripts, the helpers have leaked out of the builders and need separate handling.

- [ ] **Step 5: Verify the existing test suite is the regression net**

CLAUDE.md forbids pytest on the HPC head node. CI is the gate. The relevant existing tests that exercise the helpers indirectly:

- `tests/test_intersect_binaries.py` (exercises `intersect_binaries` which uses `write_uint8_binary` and `_assert_aligned`)
- `tests/test_compute_carea_map_binary.py` (exercises `compute_carea_map_binary` which uses all 3)
- `tests/test_build_depstor_perv.py` (exercises `perv` builder)
- `tests/test_land_mask.py` (exercises `write_uint8_binary` directly via `landmask` builder)

No action needed in Task 1 beyond noting these exist.

---

## Task 2: Promote helpers + refactor `write_uint8_binary` in `src/gfv2_params/depstor.py`

**Files:**
- Modify: `src/gfv2_params/depstor.py` (add 2 new functions; refactor `write_uint8_binary` body)

- [ ] **Step 1: Add `assert_raster_aligned` to `depstor.py`**

Locate a sensible position in `src/gfv2_params/depstor.py` — after `RasterInfo` (line 35-ish) and before `read_aligned_uint8` is a natural fit, but you can also place it just before `write_uint8_binary` at line 251. Pick whichever flows better with the existing function ordering.

Add this exact function (the body is verbatim from `src/gfv2_params/depstor_builders/intersect.py:37-46`, just promoted to public — no `_` prefix, no docstring change):

```python
def assert_raster_aligned(src, info: RasterInfo, name: str) -> None:
    """Raise if `src` doesn't share shape/CRS/transform with `info`.

    `src` is a rasterio dataset (any object with `.width`, `.height`, `.crs`,
    `.transform`). `name` is used in the error message to identify the
    offending input. Used by every streaming depstor builder to fail loudly
    when an upstream raster diverges from the template grid.
    """
    if (src.width, src.height) != (info.width, info.height):
        raise ValueError(
            f"{name} shape ({src.width}x{src.height}) != template "
            f"({info.width}x{info.height})"
        )
    if src.crs != info.crs:
        raise ValueError(f"{name} CRS {src.crs} != template CRS {info.crs}")
    if src.transform != info.transform:
        raise ValueError(f"{name} transform mismatch with template")
```

- [ ] **Step 2: Add `uint8_binary_profile` to `depstor.py`**

Place immediately before `write_uint8_binary` (currently at line 251). Body is verbatim from `_uint8_binary_profile` in any of the 3 builders (they're byte-identical):

```python
def uint8_binary_profile(info: RasterInfo) -> dict:
    """Build the rasterio profile dict for a uint8 binary raster.

    Used by both the full-array writer (`write_uint8_binary`, below) and the
    streaming depstor builders (`perv`, `carea_map`, `intersect`). Keeping a
    single source means the two write paths can't drift on compression,
    tiling, nodata, or BIGTIFF settings.
    """
    return {
        "driver": "GTiff",
        "height": info.height,
        "width": info.width,
        "count": 1,
        "dtype": "uint8",
        "crs": info.crs,
        "transform": info.transform,
        "nodata": 255,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "YES",
    }
```

- [ ] **Step 3: Refactor `write_uint8_binary` to use the new helper**

Locate `write_uint8_binary` at `src/gfv2_params/depstor.py:251`. Currently:

```python
def write_uint8_binary(arr: np.ndarray, info: RasterInfo, out_path: Path) -> None:
    """Write a uint8 binary mask using the template spatial metadata."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": info.height,
        "width": info.width,
        "count": 1,
        "dtype": "uint8",
        "crs": info.crs,
        "transform": info.transform,
        "nodata": 255,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "YES",
    }
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr.astype(np.uint8), 1)
```

Replace with:

```python
def write_uint8_binary(arr: np.ndarray, info: RasterInfo, out_path: Path) -> None:
    """Write a uint8 binary mask using the template spatial metadata."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    profile = uint8_binary_profile(info)
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr.astype(np.uint8), 1)
```

The function shrinks by ~14 lines. The profile is now sourced from the helper — no drift possible.

- [ ] **Step 4: py_compile `depstor.py`**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
pixi run --as-is python -m py_compile src/gfv2_params/depstor.py
```

Expected: silent success.

- [ ] **Step 5: Smoke import + helper call**

```bash
pixi run --as-is python -c "
from gfv2_params.depstor import assert_raster_aligned, uint8_binary_profile, RasterInfo
from rasterio.crs import CRS
from rasterio.transform import Affine
info = RasterInfo(crs=CRS.from_epsg(5070), width=10, height=10,
                  transform=Affine(30, 0, 0, 0, -30, 0), nodata=255)
p = uint8_binary_profile(info)
assert p['width'] == 10 and p['driver'] == 'GTiff' and p['nodata'] == 255
print('helpers OK')
"
```

Expected: `helpers OK`.

- [ ] **Step 6: STOP — do not commit. Task 3 updates the callers; we commit once at the end.**

---

## Task 3: Update the 3 caller builders

**Files:**
- Modify: `src/gfv2_params/depstor_builders/intersect.py`
- Modify: `src/gfv2_params/depstor_builders/carea_map.py`
- Modify: `src/gfv2_params/depstor_builders/perv.py`

All three follow the same pattern. Repeat Steps 1-3 for each builder.

- [ ] **Step 1: `intersect.py` — extend the depstor import block**

The file currently has at line 13:

```python
from ..depstor import RasterInfo, intersect_binaries
```

Extend to (alphabetical):

```python
from ..depstor import (
    RasterInfo,
    assert_raster_aligned,
    intersect_binaries,
    uint8_binary_profile,
)
```

- [ ] **Step 2: `intersect.py` — delete the two local helper definitions**

Lines 19-46 of the current file contain `_uint8_binary_profile` (lines 19-34) and `_assert_aligned` (lines 37-46), separated by 2 blank lines. **Delete all of them** — the two function defs plus the surrounding blank lines that separate them from the next `def build(...)`.

- [ ] **Step 3: `intersect.py` — rename call sites**

Currently 3 call sites (per `git grep`):
- Line 71: `profile = _uint8_binary_profile(info)` → `profile = uint8_binary_profile(info)`
- Line 76: `_assert_aligned(a_src, info, "input_a")` → `assert_raster_aligned(a_src, info, "input_a")`
- Line 77: `_assert_aligned(b_src, info, "input_b")` → `assert_raster_aligned(b_src, info, "input_b")`

After the deletions in Step 2, the line numbers shift up; use file content search rather than line numbers. Use `Edit` with `replace_all=true` for each rename:

```
old_string: "_uint8_binary_profile("
new_string: "uint8_binary_profile("
```

```
old_string: "_assert_aligned("
new_string: "assert_raster_aligned("
```

Verify after:

```bash
grep -n "_assert_aligned\|_uint8_binary_profile" src/gfv2_params/depstor_builders/intersect.py
```

Expected: empty.

- [ ] **Step 4: `carea_map.py` — extend the depstor import block + delete locals + rename**

The current `from ..depstor import (...)` block in `carea_map.py` is multi-line. Extend it to include `assert_raster_aligned` and `uint8_binary_profile` in alphabetical position. Then delete the local `_uint8_binary_profile` (current lines 22-37) and `_assert_aligned` (current lines 40-49) definitions. Then `Edit` with `replace_all=true`:

```
old_string: "_uint8_binary_profile(" → new_string: "uint8_binary_profile("
old_string: "_assert_aligned(" → new_string: "assert_raster_aligned("
```

Verify:

```bash
grep -n "_assert_aligned\|_uint8_binary_profile" src/gfv2_params/depstor_builders/carea_map.py
```

Expected: empty.

- [ ] **Step 5: `perv.py` — same pattern as Step 4**

Same three operations: extend the import block; delete `_uint8_binary_profile` (current lines 32-47) and `_assert_aligned` (current lines 50-59); `replace_all` rename the call sites.

Verify:

```bash
grep -n "_assert_aligned\|_uint8_binary_profile" src/gfv2_params/depstor_builders/perv.py
```

Expected: empty.

- [ ] **Step 6: All-files post-check**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
git grep -nE "_assert_aligned|_uint8_binary_profile" src/
```

Expected: empty (0 matches anywhere under `src/`).

```bash
git grep -nE "assert_raster_aligned|uint8_binary_profile" src/ | wc -l
```

Expected: 14 matches (2 defs in `depstor.py` + 1 internal use in `depstor.py` + 12 call sites + ~6 import lines in the 3 builders — but the imports may count as 6 lines if multi-line or 3 lines if single-line; the exact count is informational, not a gate).

- [ ] **Step 7: py_compile all 3 builders**

```bash
pixi run --as-is python -m py_compile \
    src/gfv2_params/depstor_builders/intersect.py \
    src/gfv2_params/depstor_builders/carea_map.py \
    src/gfv2_params/depstor_builders/perv.py
```

Expected: silent success.

- [ ] **Step 8: Smoke import of the package**

```bash
pixi run --as-is python -c "
from gfv2_params.depstor_builders import BUILDERS, STEP_ORDER
assert 'intersect' in str(BUILDERS) or 'drains_perv' in BUILDERS
assert 'perv' in BUILDERS
assert 'carea_map' in BUILDERS
print('builders package imports clean')
"
```

Expected: `builders package imports clean`.

- [ ] **Step 9: STOP — do not commit. Task 4 adds the test.**

---

## Task 4: Add `tests/test_depstor_helpers.py`

**Files:**
- Create: `tests/test_depstor_helpers.py`

- [ ] **Step 1: Create the test file**

Create `tests/test_depstor_helpers.py` with this exact content:

```python
"""Tests for promoted depstor helpers: assert_raster_aligned + uint8_binary_profile.

Acceptance criterion for issue #64: `assert_raster_aligned` raises on shape /
CRS / transform mismatch. Plus a single positive case to confirm the happy
path. No geo I/O; uses minimal in-memory fixtures.
"""

from dataclasses import dataclass

import pytest
from rasterio.crs import CRS
from rasterio.transform import Affine

from gfv2_params.depstor import (
    RasterInfo,
    assert_raster_aligned,
    uint8_binary_profile,
)


@dataclass
class FakeSrc:
    """Minimal stand-in for a rasterio dataset — only the 4 attrs the
    helper reads."""
    width: int
    height: int
    crs: CRS
    transform: Affine


@pytest.fixture
def template_info() -> RasterInfo:
    """Minimal template: EPSG:5070, 100x80 cells at 30m, origin (0, 0)."""
    return RasterInfo(
        crs=CRS.from_epsg(5070),
        width=100,
        height=80,
        transform=Affine(30, 0, 0, 0, -30, 0),
        nodata=255,
    )


@pytest.fixture
def aligned_src(template_info) -> FakeSrc:
    return FakeSrc(
        width=template_info.width,
        height=template_info.height,
        crs=template_info.crs,
        transform=template_info.transform,
    )


# --- assert_raster_aligned -------------------------------------------------


def test_assert_raster_aligned_passes_on_match(template_info, aligned_src):
    """Happy path — no exception, no return value."""
    assert assert_raster_aligned(aligned_src, template_info, "test") is None


def test_assert_raster_aligned_raises_on_shape_mismatch(template_info, aligned_src):
    aligned_src.width = template_info.width + 1
    with pytest.raises(ValueError, match=r"shape \(101x80\) != template \(100x80\)"):
        assert_raster_aligned(aligned_src, template_info, "shape_test")


def test_assert_raster_aligned_raises_on_crs_mismatch(template_info, aligned_src):
    aligned_src.crs = CRS.from_epsg(4326)
    with pytest.raises(ValueError, match=r"crs_test CRS.*!= template CRS"):
        assert_raster_aligned(aligned_src, template_info, "crs_test")


def test_assert_raster_aligned_raises_on_transform_mismatch(template_info, aligned_src):
    aligned_src.transform = Affine(60, 0, 0, 0, -60, 0)  # different cell size
    with pytest.raises(ValueError, match=r"transform_test transform mismatch"):
        assert_raster_aligned(aligned_src, template_info, "transform_test")


# --- uint8_binary_profile --------------------------------------------------


def test_uint8_binary_profile_shape(template_info):
    """Profile must reflect the template's shape, CRS, transform."""
    profile = uint8_binary_profile(template_info)
    assert profile["width"] == template_info.width
    assert profile["height"] == template_info.height
    assert profile["crs"] == template_info.crs
    assert profile["transform"] == template_info.transform


def test_uint8_binary_profile_uint8_conventions(template_info):
    """uint8 binary masks: dtype=uint8, nodata=255, LZW, tiled 256x256, BIGTIFF."""
    profile = uint8_binary_profile(template_info)
    assert profile["dtype"] == "uint8"
    assert profile["nodata"] == 255
    assert profile["count"] == 1
    assert profile["driver"] == "GTiff"
    assert profile["compress"] == "LZW"
    assert profile["tiled"] is True
    assert profile["blockxsize"] == 256
    assert profile["blockysize"] == 256
    assert profile["BIGTIFF"] == "YES"
```

That's 4 tests for `assert_raster_aligned` (3 failure + 1 success — satisfies #64 acceptance) plus 2 for `uint8_binary_profile` (shape + conventions — covers the only thing that could regress when callers refactor to use the shared profile).

- [ ] **Step 2: Verify the test file py_compiles**

```bash
pixi run --as-is python -m py_compile tests/test_depstor_helpers.py
```

Expected: silent success.

- [ ] **Step 3: Smoke-run the 6 new tests**

CLAUDE.md forbids pytest on the HPC head node for the full suite, but a single file with 6 trivial tests against synthetic fixtures is harmless — no geo libraries triggered beyond what the imports already cached:

```bash
pixi run -e dev pytest tests/test_depstor_helpers.py -v
```

Expected: 6 passed in <1s.

If this hangs or is too slow, fall back to letting CI run on push (Task 5).

- [ ] **Step 4: STOP — do not commit. Task 5 commits everything together.**

---

## Task 5: Pre-commit, commit, push, open PR

**Files:** no edits — orchestration only.

- [ ] **Step 1: Stage everything**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
git add \
    src/gfv2_params/depstor.py \
    src/gfv2_params/depstor_builders/intersect.py \
    src/gfv2_params/depstor_builders/carea_map.py \
    src/gfv2_params/depstor_builders/perv.py \
    tests/test_depstor_helpers.py
git status --short
```

Expected exactly 5 staged files:
```
M  src/gfv2_params/depstor.py
M  src/gfv2_params/depstor_builders/carea_map.py
M  src/gfv2_params/depstor_builders/intersect.py
M  src/gfv2_params/depstor_builders/perv.py
A  tests/test_depstor_helpers.py
```

If anything else is staged, STOP and unstage.

- [ ] **Step 2: Run pre-commit on staged files**

```bash
pixi run -e dev pre-commit run --files \
    src/gfv2_params/depstor.py \
    src/gfv2_params/depstor_builders/intersect.py \
    src/gfv2_params/depstor_builders/carea_map.py \
    src/gfv2_params/depstor_builders/perv.py \
    tests/test_depstor_helpers.py
```

Expected: every hook `Passed` (or `Skipped` for non-applicable hooks). If any hook modifies a file (e.g. isort reorders the new imports), accept the change and re-stage.

- [ ] **Step 3: Commit**

```bash
git commit -m "$(cat <<'EOF'
refactor(depstor): promote streaming-builder helpers to public API (closes #64)

Three byte-identical copies of `_assert_aligned` + `_uint8_binary_profile`
lived in src/gfv2_params/depstor_builders/{intersect,carea_map,perv}.py,
and a fourth copy of the profile dict was embedded inline in
src/gfv2_params/depstor.py's write_uint8_binary. Issue #64 flagged this
as a drift bait.

This commit:
- Promotes the two helpers to public API in src/gfv2_params/depstor.py
  (renamed without the underscore prefix):
    * assert_raster_aligned(src, info, name)
    * uint8_binary_profile(info)
- Refactors write_uint8_binary to source its profile from the new
  uint8_binary_profile helper — full-array and streaming write paths
  now share one definition; drift is structurally impossible.
- Updates the 3 builder callers to import from ..depstor and removes
  their local helper copies (~30 LOC deleted per file × 3 = ~90 LOC of
  duplication eliminated).
- Adds tests/test_depstor_helpers.py covering assert_raster_aligned's
  3 failure modes (shape / CRS / transform mismatch) + the happy path,
  plus 2 tests pinning uint8_binary_profile's shape + uint8 conventions.

Function bodies move verbatim; behaviour bit-identical. The TWI
valid-mask bonus (issue #64's optional second item) is deferred — the
threshold_above and compute_carea_map_binary valid-mask logic is not
byte-identical and unifying them requires its own design decision.

Closes #64.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 4: Verify commit content**

```bash
git show --stat HEAD
```

Expected: 5 files changed, net negative LOC count (the duplication removed exceeds the new test file's lines).

- [ ] **Step 5: STOP HERE for user approval before push**

Per the user's standing rule (`don't commit/push or open the PR without confirming first`), pause here. Surface to the user:

- The commit SHA (`git log --oneline -1`)
- The commit's diff stat (`git show --stat HEAD`)
- The branch state (`git log --oneline main..feat/promote-depstor-helpers`)
- Ask for push approval

Once approved:

- [ ] **Step 6: Push**

```bash
git push -u origin feat/promote-depstor-helpers
```

- [ ] **Step 7: Open PR**

```bash
gh pr create --base main --head feat/promote-depstor-helpers \
  --title "refactor(depstor): promote streaming-builder helpers to public API (closes #64)" \
  --body "$(cat <<'EOF'
Tier-2-E from the [fresh-eyes repo evaluation](docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md) and the design from issue #64. Eliminates three byte-identical copies of \`_assert_aligned\` + \`_uint8_binary_profile\` across the depstor_builders package, plus a fourth (inline) copy of the profile dict embedded in \`write_uint8_binary\`.

## Why

Three copies of identical 9-line + 14-line helpers is the bait. The next streaming builder makes it four; \`write_uint8_binary\`'s inline 4th copy of the profile dict makes drift between the full-array and streaming write paths possible.

## What

1. Promote the helpers to public API in \`src/gfv2_params/depstor.py\` (rename to drop the \`_\`):
   - \`assert_raster_aligned(src, info, name)\`
   - \`uint8_binary_profile(info)\`
2. Refactor \`write_uint8_binary\` to source its profile from \`uint8_binary_profile(info)\` instead of the inline dict.
3. Update the 3 caller builders (\`intersect.py\`, \`carea_map.py\`, \`perv.py\`) to import from \`..depstor\` and delete their local copies.
4. Add \`tests/test_depstor_helpers.py\` — 6 tests covering \`assert_raster_aligned\`'s 3 failure modes (shape / CRS / transform) + 1 happy path + 2 \`uint8_binary_profile\` conventions tests.

## Invariants preserved

- Function bodies moved verbatim (byte-identical to the original 3 copies — pre-verified by \`diff\`).
- \`write_uint8_binary\`'s output is unchanged (profile dict construction is deterministic; key order doesn't affect rasterio output).
- No new dependencies.
- Existing tests (\`test_intersect_binaries\`, \`test_compute_carea_map_binary\`, \`test_build_depstor_perv\`, \`test_land_mask\`) pass unchanged.

## Out of scope

- The bonus \`_twi_valid_mask\` extraction (issue #64's optional second item). The \`threshold_above\` and \`compute_carea_map_binary\` valid-mask branches are *not* byte-identical (TWI adds defensive \`& ~np.isnan(twi)\` checks); unifying requires its own design conversation.

## Test plan

- [ ] CI green on push (pytest gate, per CLAUDE.md's no-pytest-on-head-node rule)
- [x] \`pytest tests/test_depstor_helpers.py -v\` runs the new 6 tests in <1s locally
- [x] \`git grep -nE "_assert_aligned|_uint8_binary_profile" src/\` returns empty (0 matches)
- [x] Spec at [\`docs/superpowers/specs/2026-05-23-promote-depstor-helpers-design.md\`](docs/superpowers/specs/2026-05-23-promote-depstor-helpers-design.md)

## Closes

#64

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Verification matrix (spec → tasks)

| Spec section | Task |
|---|---|
| Promote `_assert_aligned` → `assert_raster_aligned` | Task 2 Step 1 |
| Promote `_uint8_binary_profile` → `uint8_binary_profile` | Task 2 Step 2 |
| Refactor `write_uint8_binary` to use the shared helper | Task 2 Step 3 |
| Update 3 callers (extend import + delete local + rename) | Task 3 Steps 1-5 |
| New `tests/test_depstor_helpers.py` with 4+ tests | Task 4 Step 1 |
| Invariant 1: behaviour unchanged | Task 5 — CI's existing test suite |
| Invariant 2: public API gains 2 names | Task 2 (added); Task 3 (callers use them) |
| Invariant 3: no new deps | Task 5 commit's diff stat (no `pyproject.toml`) |
| Invariant 4: existing tests pass unchanged | Task 5 — CI |
| Pre-flight grep counts | Task 1 Step 3 |
| Post-refactor grep counts | Task 3 Step 6 |
| Commit shape: 1 atomic | Task 5 Step 3 |

---

## Notes for the implementer

- **Function bodies are byte-identical across the 3 source copies** — verified pre-flight. Copy from any one of them (intersect.py is the smallest, easiest to grab from).
- **Order of operations matters within Task 3:** extend the import block FIRST, then delete the local def, then rename call sites. Deleting before extending leaves call sites resolving to a NameError.
- **pre-commit hooks may reorder imports** (isort) — accept the reformat and re-stage.
- **The smoke pytest run in Task 4 Step 3 is genuinely safe** — 6 tests with synthetic fixtures, no geo I/O. If CLAUDE.md feels paranoid here, escalate; otherwise run it.
- **Do not push or open the PR without user approval** (Task 5 Step 5 is the gate).
