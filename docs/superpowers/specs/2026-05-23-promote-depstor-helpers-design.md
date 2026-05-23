# Promote depstor streaming-builder helpers (Tier 2-E, closes #64)

**Status:** approved 2026-05-23.
**Closes:** issue #64 (refactor(depstor): consolidate streaming-builder helpers).
**Related:** T2-E in the fresh-eyes review
([`docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md`](../reviews/2026-05-23-repo-fresh-eyes.md)),
hotspot H.

## Why

Three copies of two helper functions live in the depstor builders:

- `_assert_aligned(src, info, name)` — 9-line shape/CRS/transform alignment check
- `_uint8_binary_profile(info)` — 14-line rasterio profile dict for uint8 binaries

Across [`intersect.py`](../../../src/gfv2_params/depstor_builders/intersect.py),
[`carea_map.py`](../../../src/gfv2_params/depstor_builders/carea_map.py), and
[`perv.py`](../../../src/gfv2_params/depstor_builders/perv.py). Verified
byte-identical via `diff`. A fourth instance of the profile dict is embedded
inline in `write_uint8_binary` at [`depstor.py:251-271`](../../../src/gfv2_params/depstor.py),
making the full-array and streaming write paths drift-prone.

Three identical copies is the bait; the next streaming builder makes it four,
the existing `write_uint8_binary`'s embedded copy makes drift between full-array
and streaming write paths possible. Fresh-eyes review flagged this as hotspot H.

## What

### Promote both helpers to public API in `src/gfv2_params/depstor.py`

Move (and rename to drop the `_` prefix — they become public):

- `_assert_aligned(src, info, name)` → `assert_raster_aligned(src, info: RasterInfo, name: str) -> None`
- `_uint8_binary_profile(info)` → `uint8_binary_profile(info: RasterInfo) -> dict`

Both go into `src/gfv2_params/depstor.py` as new module-level functions, alongside
the existing `RasterInfo`, `read_aligned_uint8`, `read_land_mask`,
`write_uint8_binary`, `threshold_above`, etc. Function bodies move **verbatim** —
this is a pure promotion (no logic edits).

### Refactor `write_uint8_binary` to use the shared profile helper

[`depstor.py:251-271`](../../../src/gfv2_params/depstor.py) currently embeds a
4th copy of the profile dict inline. Replace the inline dict with a call to
the new `uint8_binary_profile(info)`. The full-array and streaming write paths
now share one profile source — they can't drift.

### Update the 3 callers

[`intersect.py`](../../../src/gfv2_params/depstor_builders/intersect.py),
[`carea_map.py`](../../../src/gfv2_params/depstor_builders/carea_map.py),
[`perv.py`](../../../src/gfv2_params/depstor_builders/perv.py):

- Import `assert_raster_aligned` and `uint8_binary_profile` from `..depstor`
  (extend the existing `from ..depstor import (...)` block).
- Replace every `_assert_aligned(...)` call with `assert_raster_aligned(...)`.
- Replace every `_uint8_binary_profile(...)` call with `uint8_binary_profile(...)`.
- Delete the local `_assert_aligned` and `_uint8_binary_profile` definitions.

### Add one test

New file `tests/test_depstor_helpers.py` covers `assert_raster_aligned`'s
three failure cases (issue #64 acceptance criterion: "One small new test
confirming `assert_raster_aligned` raises on mismatched shape / CRS /
transform"). Use a tiny in-memory raster fixture (no geo I/O):

```python
import pytest
import rasterio
from affine import Affine
from rasterio.crs import CRS
from gfv2_params.depstor import RasterInfo, assert_raster_aligned

# Fixture: template RasterInfo + a fake `src` object with width/height/crs/transform
# that pytest can mutate to provoke each failure mode.

def test_assert_raster_aligned_raises_on_shape_mismatch():
    ...

def test_assert_raster_aligned_raises_on_crs_mismatch():
    ...

def test_assert_raster_aligned_raises_on_transform_mismatch():
    ...

def test_assert_raster_aligned_passes_on_match():
    ...
```

The exact fixture shape will be worked out at implementation time; the test
file matches the acceptance criterion.

## File map

| File | Change |
|---|---|
| `src/gfv2_params/depstor.py` | Add 2 new public functions (`assert_raster_aligned`, `uint8_binary_profile`); refactor `write_uint8_binary` body to use `uint8_binary_profile` instead of inline dict |
| `src/gfv2_params/depstor_builders/intersect.py` | Extend import from `..depstor`; replace 2 call sites; delete the 2 local helpers (`_assert_aligned`, `_uint8_binary_profile`) |
| `src/gfv2_params/depstor_builders/carea_map.py` | Same |
| `src/gfv2_params/depstor_builders/perv.py` | Same |
| `tests/test_depstor_helpers.py` | New file: 4 tests (3 failure modes + 1 success) for `assert_raster_aligned` |

5 files modified, 1 file added. No config, no docs touch (the helpers were
private; no doc referenced them).

## Invariants

1. **Behaviour unchanged.** Function bodies move verbatim. `write_uint8_binary`'s
   refactor uses the same profile dict (just sourced from the helper instead
   of inline) — bit-identical output.
2. **Public API surface gains 2 names** in `gfv2_params.depstor`. The original
   3 callers and any future depstor builder import from there. Existing
   `gfv2_params.depstor` consumers (`write_uint8_binary`, `RasterInfo`, etc.)
   are unaffected.
3. **No new dependencies.** No `pyproject.toml` / `pixi.lock` edits.
4. **Existing tests pass unchanged.** `tests/test_intersect_binaries.py`,
   `tests/test_compute_carea_map_binary.py`, `tests/test_build_depstor_perv.py`
   exercise these helpers indirectly. Plus the existing
   `tests/test_land_mask.py` exercises `write_uint8_binary` directly.

## Out of scope (deferred)

- **`_twi_valid_mask` extraction (issue #64's bonus item).** The
  `threshold_above` and `compute_carea_map_binary` valid-mask logic is *not*
  byte-identical — the TWI variant adds defensive `& ~np.isnan(twi)` checks
  in two branches. Unifying them requires either choosing one behaviour
  (subtle change for the other's callers) or adding dtype-awareness — a
  separate concern with its own design decisions. Will file as a follow-up
  issue if it bites.
- Public exports beyond the 2 helpers. The shared profile-build pattern could
  arguably grow into a wider "raster I/O conventions" module; YAGNI for now.

## Risks

| Risk | Mitigation |
|---|---|
| The verbatim-move invariant breaks if I edit the function body during the rename | TDD discipline: write the new test first, run pytest, confirm pass before any change. Verbatim by `diff`-checking the body before/after. |
| Caller import edits accidentally drop another import already in the block | Use targeted `Edit` with full surrounding-context snippet; pre-commit's `isort` + `ruff` catch any drift. |
| `write_uint8_binary` refactor changes the output (e.g. dict key order matters somewhere) | rasterio constructs `profile` as a plain dict; key order doesn't affect output. Existing tests exercise this path. |
| Test imports `gfv2_params.depstor` which triggers a chain that needs fixtures | `depstor.py` has no SLURM-array startup heartbeat (unlike `zonal_runners`); imports are clean. Test file uses synthetic minimal fixtures. |
| New test depends on a fixture I haven't built yet | Spec lists the 4 test names + behaviour; plan will materialize the fixture using `rasterio.transform.from_bounds` + `CRS.from_epsg(5070)` (same pattern as `tests/test_land_mask.py`). |

## Verification

1. **Pre-flight (current state):** `git grep -n "_assert_aligned\|_uint8_binary_profile" src/` shows 18 matches: 6 def sites (2 per builder) + 12 call sites (3 `_uint8_binary_profile` calls + 9 `_assert_aligned` calls — 4 in carea_map, 3 in perv, 2 in intersect). After the refactor: 0 matches for the underscore-prefixed names.
2. **Post-refactor:** `git grep -n "assert_raster_aligned\|uint8_binary_profile" src/` shows:
   - 2 def sites in `depstor.py`
   - 1 internal use in `depstor.py` (`write_uint8_binary` calling `uint8_binary_profile`)
   - 12 call sites across the 3 builders (same as before, just renamed)
   - 2 import lines per builder (in the `from ..depstor import (...)` block)
3. **Pytest:** the new test file passes; existing tests pass unchanged.
4. **No behaviour change in `write_uint8_binary`:** the function still writes
   the same bytes to disk. Quick verification via existing
   `tests/test_build_depstor_perv.py` (writes a uint8 binary).
5. **Pre-commit clean** on all touched files.

## Commit shape

**1 atomic commit** since the changes are tightly coupled (promote +
refactor write_uint8_binary + update 3 callers + add test all touch the same
DRY surface). PR description references issue #64.

## Docs check

Per CLAUDE.md: no doc updates needed.
- No `README.md` / `CLAUDE.md` / `RUNME.md` reference to `_assert_aligned` or
  `_uint8_binary_profile` (private; never docs).
- `docs/depstor_workflow.md` and `docs/depstor_port_summary.md` describe the
  pipeline, not the helper internals.

## Audience

Implementer (Claude on a fresh agent dispatch via subagent-driven-development).
Reader assumed to know Python imports, refactor discipline, and the
depstor_builders package layout.
