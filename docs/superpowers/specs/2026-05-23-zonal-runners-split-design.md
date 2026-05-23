# Split `zonal_runners.py` into a package (Tier 2-A)

**Status:** approved 2026-05-23.
**Closes:** T2-A in the fresh-eyes review
(`docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md`), hotspot Z.
**Related:** depstor_builders/ pattern is the precedent — this split mirrors it
for the Part-2 zonal-pass pipeline.

## Why

`src/gfv2_params/zonal_runners.py` is **638 LOC in one file**, hosting 6 public
`run_*` functions + 2 private soils helpers. The depstor side did a comparable
refactor in PR #72 — `depstor_builders/` is a package of 11 small modules. The
asymmetry was flagged by the fresh-eyes review as the single most impactful
structural change for plug-ability:

> Adding a new `script:` tag means editing a 638-line module rather than adding
> a new file. After this refactor, "add a new param family" becomes "add a new
> submodule + register in `__init__.py`" — the same shape as adding a new
> depstor step.

This refactor restores symmetry between the two pipeline halves and removes
the friction point flagged in the review.

## What

### File layout (after)

```
src/gfv2_params/zonal_runners/
├── __init__.py        # Re-exports + dispatch table + heavy imports + GDAL toggle + startup heartbeat
├── zonal.py           # run_zonal_batch                              (~60 LOC)
├── soils.py           # run_soils_batch + _process_soils + _process_soil_moist_max  (~90 LOC)
├── lulc.py            # run_lulc_batch                               (~160 LOC)
├── ssflux.py          # run_ssflux_batch                             (~135 LOC)
├── weights.py         # run_build_weights                            (~60 LOC)
└── merge.py           # run_merge                                    (~60 LOC)
```

The pre-refactor `src/gfv2_params/zonal_runners.py` is **deleted** (Python
forbids a `.py` and a package directory of the same name).

### `__init__.py` contents

The init module is the single place that fires module-import side effects
shared across submodules. In order:

1. **Startup heartbeat** — `print(f"[startup pid={os.getpid()} host={...} task={...}] python {sys.version.split()[0]} interpreter up, importing geo libs...", flush=True)` — verbatim from the current `zonal_runners.py:36-41`. Fires once at first import of any submodule (Python caches the package init). Preserves the SLURM-array hang debugging artifact described in the original docstring.
2. **Geo-library imports** — `geopandas`, `numpy`, `pandas`, `rioxarray`, `gdptools.{UserTiffData, WeightGenP2P, ZonalGen}`, `osgeo.{gdal, osr}` — verbatim from lines 44-49. Heavy imports happen once; submodules access them via `from . import gdal` or similar (relative re-import is cheap because Python caches the module).
3. **Geo-library completion heartbeat** — `print(f"[startup] geo-library imports complete in {time.time() - _t_imports:.1f}s", flush=True)` — verbatim from line 51.
4. **GDAL/OSR exception toggles** — `gdal.UseExceptions(); osr.UseExceptions()` — verbatim from lines 70-71. Process-global, must fire exactly once.
5. **Submodule imports** — `from . import zonal, soils, lulc, ssflux, weights, merge`. Order is alphabetical (no inter-submodule dependencies).
6. **Public re-exports** — names that external callers see:
   ```python
   from .zonal import run_zonal_batch
   from .soils import run_soils_batch
   from .lulc import run_lulc_batch
   from .ssflux import run_ssflux_batch
   from .weights import run_build_weights
   from .merge import run_merge
   ```
7. **`BATCH_RUNNERS` dispatch table** — moved from `scripts/derive_zonal_params.py:41`. Public (no leading underscore). Maps the `script:` tag in `configs/zonal/zonal_params.yml` to the runner function:
   ```python
   BATCH_RUNNERS = {
       "zonal":  run_zonal_batch,
       "soils":  run_soils_batch,
       "lulc":   run_lulc_batch,
       "ssflux": run_ssflux_batch,
   }
   ```
8. **`__all__`** — explicit export list of the 6 `run_*` functions plus `BATCH_RUNNERS`.

### Submodule structure (each `.py` file)

Each submodule follows the same template:

```python
"""<one-line purpose>"""

from __future__ import annotations

from pathlib import Path

# Geo libs are imported at the package level (zonal_runners/__init__.py) so the
# startup heartbeat fires exactly once on first package import. Submodules
# re-import the names they actually use here — Python caches the modules, so
# this is cheap.
import geopandas as gpd  # only if used
import rioxarray         # only if used
# ... etc, only what this submodule actually needs

from ..lulc import (...)         # only in soils/lulc/ssflux as needed
from ..raster_ops import (...)   # only in zonal as needed


def run_<name>_batch(config: dict, batch_id: int, logger) -> None:
    """..."""
    # function body verbatim from current zonal_runners.py
```

The function bodies move **verbatim**. The only edits are:

- Add the module docstring + relative imports the function actually uses
- Drop the `_BATCH_RUNNERS` reference (it now lives in `__init__.py`)

### Caller-side changes

**`scripts/derive_zonal_params.py:32-39`**

Before:
```python
from gfv2_params.zonal_runners import (
    run_build_weights,
    run_lulc_batch,
    run_merge,
    run_soils_batch,
    run_ssflux_batch,
    run_zonal_batch,
)

_BATCH_RUNNERS = {
    "zonal":  run_zonal_batch,
    "soils":  run_soils_batch,
    "lulc":   run_lulc_batch,
    "ssflux": run_ssflux_batch,
}
```

After:
```python
from gfv2_params.zonal_runners import BATCH_RUNNERS, run_build_weights, run_merge
```

The orchestrator drops the local dispatch dict; uses `BATCH_RUNNERS` directly.
The `_BATCH_RUNNERS[script_tag]` call site renames to `BATCH_RUNNERS[script_tag]`
(one-line edit). All other references to `run_zonal_batch` / `run_soils_batch`
/ `run_lulc_batch` / `run_ssflux_batch` go away because they were only used to
build the dispatch dict.

**`tests/test_merge_params.py:14`** — `from gfv2_params.zonal_runners import run_merge as process_files` — keeps working unchanged because `run_merge` is re-exported from `__init__.py`.

### Internal helper placement

- **`_process_soils`** (current line 176) and **`_process_soil_moist_max`** (current line 205) move to `soils.py`. Both are private helpers of `run_soils_batch`; they stay as `_`-prefixed module-private functions.
- No other private helpers exist in the current file.

## Invariants

These properties must hold after the refactor:

1. **Public import surface unchanged.** `from gfv2_params.zonal_runners import run_zonal_batch` (and the other 5 `run_*` names) works exactly as it does today.
2. **Behaviour unchanged.** No function body edits. CI's existing `pytest tests/` suite passes without modification.
3. **Startup heartbeat fires exactly once per process.** Same print, same content as the current `zonal_runners.py:36-41`.
4. **GDAL/OSR exception toggle fires exactly once per process** at first package import.
5. **No new dependencies.** No additions to `pyproject.toml`; no new pixi tasks.

## Out of scope

- Renaming any public function (would break callers).
- Merging `_process_soils` + `_process_soil_moist_max` (considered and dropped — the dispatcher-with-two-helpers shape is clearer than a single helper with internal if/else).
- Extracting a shared gdptools `UserTiffData/ZonalGen` scaffold helper (real DRY win available; YAGNI for this PR — file as a follow-up if it bites).
- Touching `derive_zonal_params.py` beyond the import-line + dispatch-dict removal.
- Touching the SLURM batches or configs.
- Touching tests (the suite should pass unchanged).

## Risks

| Risk | Mitigation |
|---|---|
| The startup heartbeat fires multiple times if a submodule is imported before the package `__init__.py` finishes | Python guarantees `__init__.py` runs before any submodule load. The order is: import statement → `__init__.py` (heartbeat + imports + GDAL toggle) → submodule. No double-fire possible. |
| A submodule's `from . import gdal` re-runs the geo-library import chain | `__init__.py` already imported them; Python returns the cached module. Re-import is a dict lookup, ~µs. |
| Test discovery breaks because `pytest` indexes the deleted `.py` file | `pytest` reads from disk on each run; once the file is deleted and the package directory is in place, discovery resolves to the package. Run pytest locally on a tiny test (or in CI) to confirm. |
| Caller code anywhere in the repo imports a now-internal name | A pre-flight `grep` of `from gfv2_params.zonal_runners` shows only two consumers (`scripts/derive_zonal_params.py`, `tests/test_merge_params.py`); both are handled in this design. |
| Two `lulc.py` modules confuse a future reader | The path is unambiguous: `src/gfv2_params/lulc.py` (source-of-truth helpers) vs `src/gfv2_params/zonal_runners/lulc.py` (the lulc batch runner). Inside `zonal_runners/lulc.py`, `from ..lulc import ...` reaches the parent module unambiguously. The naming is also consistent with `depstor_builders/`'s precedent. |
| `notebooks/oregon/README.md` or other doc references the old `zonal_runners.py` path | Search the repo for `zonal_runners.py` references and update any that remain — but the package layout makes `gfv2_params.zonal_runners` the canonical reference. |

## Verification

1. **Pre-flight grep:** `grep -rn "zonal_runners" src/ scripts/ tests/ docs/ slurm_batch/ notebooks/ configs/` — every match accounted for.
2. **Import surface:** after the refactor, `pixi run -e dev python -c "from gfv2_params.zonal_runners import run_zonal_batch, run_soils_batch, run_lulc_batch, run_ssflux_batch, run_build_weights, run_merge, BATCH_RUNNERS; print('OK')"` succeeds.
3. **Heartbeat fires once:** `pixi run -e dev python -c "from gfv2_params.zonal_runners import run_merge"` prints two `[startup ...]` lines (heartbeat + completion) and no other startup output. A second import in the same process is a no-op.
4. **Tests pass:** `pixi run -e dev pytest tests/ -q` — the existing 21 test files (zonal_orchestrator, lulc, batching, merge_params, etc.) must all pass. CI is the gate; local HPC head node `pytest` is forbidden per CLAUDE.md.
5. **Orchestrator runs end-to-end** on the small `gfv2_vpu01` fabric: `pixi run python scripts/derive_zonal_params.py --mode zonal --param elevation --batch_id 0 --config configs/zonal/zonal_params.yml --base_config configs/base_config.yml --fabric gfv2_vpu01`. The first 5 lines of output should be the two heartbeats followed by the batch logger. (This is an optional smoke test — not a gate, since the unit tests already cover the runner functions.)
6. **No new dependencies:** `git diff main -- pyproject.toml pixi.lock` is empty.

## Commit shape

One PR, two commits:

1. `refactor(zonal_runners): split into a package mirroring depstor_builders` — the file move + `__init__.py` creation + submodule splits + verbatim function moves.
2. `refactor(zonal_runners): move BATCH_RUNNERS dispatch into the package` — moves the dispatch table from `scripts/derive_zonal_params.py` into `zonal_runners/__init__.py`, renames `_BATCH_RUNNERS` → `BATCH_RUNNERS`, removes the now-redundant imports in the orchestrator.

Both commits are individually revertible. The PR closes the T2-A action item in the fresh-eyes review.

## Docs check

Per CLAUDE.md, every code change needs a docs audit. Touch points:

- **`README.md` "Zonal-pass parameter pipeline" section** (~line 349-380): currently says "per-step library functions live under `src/gfv2_params/zonal_runners.py`". Update to "the `src/gfv2_params/zonal_runners/` package."
- **`slurm_batch/RUNME.md`:** no direct path reference; no edits needed (verified via `grep zonal_runners slurm_batch/RUNME.md` — should be 0 hits).
- **`CLAUDE.md`** Architecture table (line 71): currently says `src/gfv2_params/zonal_runners.py` in the Builders column for the Part 2b zonal-params stage. Update to `src/gfv2_params/zonal_runners/` to reflect the package shape.
- The fresh-eyes review (`docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md`) describes the pre-refactor state; **do not edit** — it's a snapshot.

## Audience

This spec is for the implementer (likely Claude on a fresh agent dispatch via
`writing-plans` → `subagent-driven-development`). The reader is assumed to know
Python packages, relative imports, and the depstor_builders precedent.
