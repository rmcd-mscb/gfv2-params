# zonal_runners.py → zonal_runners/ Package Split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the 638-LOC `src/gfv2_params/zonal_runners.py` into a 7-file
`src/gfv2_params/zonal_runners/` package mirroring `src/gfv2_params/depstor_builders/`,
preserving the public import surface and behaviour, and move the `BATCH_RUNNERS`
dispatch table into the package so adding a new param family is one fewer edit
point.

**Architecture:** Pure refactor. Function bodies move verbatim from
`zonal_runners.py` into 6 generic-named submodules (`zonal.py`, `soils.py`,
`lulc.py`, `ssflux.py`, `weights.py`, `merge.py`). The package's `__init__.py`
holds the cross-cutting concerns that must fire exactly once per process
(startup heartbeat for SLURM-array-hang debugging, heavy geo-library imports,
`gdal.UseExceptions()` / `osr.UseExceptions()` toggles) and re-exports the 6
public `run_*` functions plus the `BATCH_RUNNERS` dispatch dict (moved from
`scripts/derive_zonal_params.py`).

**Tech Stack:** Python 3.12, pixi env, pytest. No new dependencies. No behaviour
change.

**Source spec:** [docs/superpowers/specs/2026-05-23-zonal-runners-split-design.md](../specs/2026-05-23-zonal-runners-split-design.md)

**Branch:** `feat/zonal-runners-split` (already created; spec already
committed as `a4ff5df`)

---

## File-layout reference (after)

```
src/gfv2_params/zonal_runners/
├── __init__.py        (re-exports + BATCH_RUNNERS + heavy imports + GDAL toggle + startup heartbeat)
├── zonal.py           (run_zonal_batch)                              — was lines 78–134
├── soils.py           (run_soils_batch + _process_soils + _process_soil_moist_max) — was 137–222
├── lulc.py            (run_lulc_batch)                               — was 225–382
├── ssflux.py          (run_ssflux_batch)                             — was 385–512
├── weights.py         (run_build_weights)                            — was 519–571
└── merge.py           (run_merge)                                    — was 578–638
```

The pre-refactor `src/gfv2_params/zonal_runners.py` is **deleted** in Task 3.

### Per-submodule import requirements

These were confirmed by a per-function grep of the current file. Each submodule
imports only what its function body actually references:

| Submodule | Stdlib | Geo libs | gdptools | gfv2_params relative |
|---|---|---|---|---|
| `zonal.py` | `pathlib.Path` | `geopandas as gpd`, `rioxarray` | `UserTiffData`, `ZonalGen` | — |
| `soils.py` | `pathlib.Path` | `geopandas as gpd`, `rioxarray` | `UserTiffData`, `ZonalGen` | — |
| `lulc.py` | `pathlib.Path` | `geopandas as gpd`, `rioxarray` | `UserTiffData`, `ZonalGen` | `from ..lulc import assign_cov_type, class_percentages_from_histogram, compute_covden, compute_interception, compute_retention, load_crosswalk` |
| `ssflux.py` | `pathlib.Path` | `geopandas as gpd`, `numpy as np`, `pandas as pd` | — | `from ..raster_ops import deg_to_fraction` |
| `weights.py` | `pathlib.Path` | `geopandas as gpd`, `numpy as np` | `WeightGenP2P` | — |
| `merge.py` | `pathlib.Path` | `pandas as pd` | — | — |

No submodule uses `gdal.` or `osr.` directly. The `gdal.UseExceptions()` /
`osr.UseExceptions()` calls happen once at package init.

---

## Task 1: Pre-flight verification

**Files:** read-only. No edits.

**Purpose:** Confirm the working tree is clean, on the right branch, and that
the spec's caller-surface inventory is still accurate (no new consumers since
the spec was written).

- [ ] **Step 1: Verify branch + working tree clean**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
git branch --show-current
git status --short
```

Expected:
```
feat/zonal-runners-split
```
(empty `git status` output)

- [ ] **Step 2: Confirm spec is committed**

```bash
git log --oneline -1 docs/superpowers/specs/2026-05-23-zonal-runners-split-design.md
```

Expected: a commit ending in `docs(spec): zonal_runners.py -> zonal_runners/ package split (T2-A)` (sha `a4ff5df` or later).

- [ ] **Step 3: Re-verify the caller surface (must match spec)**

```bash
grep -rn "from gfv2_params\.zonal_runners\|from gfv2_params import zonal_runners\|gfv2_params\.zonal_runners\." \
    src/ scripts/ tests/ slurm_batch/ notebooks/ configs/ 2>/dev/null
```

Expected output should contain **exactly two import statements**:
- `scripts/derive_zonal_params.py:32:from gfv2_params.zonal_runners import (`
- `tests/test_merge_params.py:14:from gfv2_params.zonal_runners import run_merge as process_files`

Plus 3 docstring references (which need no code change):
- `scripts/derive_zonal_params.py:8` and `:11`
- `tests/test_merge_params.py:4`
- `slurm_batch/derive_zonal_params.batch:18`

If you find additional consumers, **stop and update the spec** before continuing.

- [ ] **Step 4: Confirm existing test suite is green on this branch**

CI is the test gate. If you're working on the HPC head node, do not run pytest
locally — push the branch and wait for CI. For an Agent on a non-HPC dev box,
run:

```bash
pixi run -e dev pytest tests/ -q
```

Expected: all tests pass on the unmodified branch.

---

## Task 2: Create the `zonal_runners/` package (all 7 files)

**Files:**
- Create: `src/gfv2_params/zonal_runners/__init__.py`
- Create: `src/gfv2_params/zonal_runners/zonal.py`
- Create: `src/gfv2_params/zonal_runners/soils.py`
- Create: `src/gfv2_params/zonal_runners/lulc.py`
- Create: `src/gfv2_params/zonal_runners/ssflux.py`
- Create: `src/gfv2_params/zonal_runners/weights.py`
- Create: `src/gfv2_params/zonal_runners/merge.py`
- Read-only reference: `src/gfv2_params/zonal_runners.py` (lines copied verbatim out of this file)

**Note on Python resolution:** once `src/gfv2_params/zonal_runners/__init__.py`
exists, Python's import resolution prefers the package over the same-named
`.py` file. This means **Steps 1–7 below stage the new files but the new
package will not be active until Step 8 deletes the old `.py`**. Do NOT run any
import or test of `gfv2_params.zonal_runners` between Step 1 and Step 8 — the
result is undefined (Python may use either the .py or the package depending
on cache state).

- [ ] **Step 1: Create the directory + `__init__.py`**

Create `src/gfv2_params/zonal_runners/__init__.py` with this exact content:

```python
"""Library functions for the Part 2 zonal-pass parameter pipeline.

Each ``run_*`` function performs the per-batch (or CONUS-once) work for one
parameter type. The unified ``scripts/derive_zonal_params.py`` orchestrator
delegates here so the work logic lives in exactly one place.

The ``config`` dict each function receives is a flat mapping containing the
keys that ``configs/zonal/zonal_params.yml`` already provides (source_type,
source_raster, batch_dir, target_layer, id_feature, output_dir, merged_file,
categorical, fabric, the fabric-profile hru_gpkg/hru_layer that
run_build_weights reads, plus per-script extras like canopy_raster,
crosswalk_file, keep_raster, source_shapefile, merged_slope_file,
weight_dir, k_perm_min, flux_params). The orchestrator builds this dict by
flattening the active param entry in ``configs/zonal/zonal_params.yml`` onto
the top-level ``defaults:`` block (plus the resolved fabric profile).

Refactor invariant: the existing per-script behaviour is preserved verbatim
— the functions in the submodules below are the prior single-file
``zonal_runners.py`` extracted unchanged (PR #75 → followup; mirrors the
``depstor_builders/`` pattern from PR #72).

Package-level concerns (heavy geo-library imports, GDAL exception toggle,
startup heartbeat) live here in ``__init__.py`` so they fire exactly once
per process at first package import.
"""

from __future__ import annotations

import os
import sys
import time

# Pre-import heartbeats so a future hang in the geo-library import chain
# (rasterio/GDAL/PROJ/pyogrio init under shared-FS metadata contention,
# observed once on VPU01 issue-#61 array run with zero stdout from one task)
# can be localised. Printed unconditionally with flush=True so SLURM's
# stdout/stderr line buffering doesn't swallow them.
print(
    f"[startup pid={os.getpid()} host={os.uname().nodename} "
    f"task={os.environ.get('SLURM_ARRAY_TASK_ID', '-')}] "
    f"python {sys.version.split()[0]} interpreter up, importing geo libs...",
    flush=True,
)
_t_imports = time.time()

import geopandas as gpd  # noqa: F401  (re-imported by submodules; cached)
import numpy as np  # noqa: F401
import pandas as pd  # noqa: F401
import rioxarray  # noqa: F401
from gdptools import UserTiffData, WeightGenP2P, ZonalGen  # noqa: F401
from osgeo import gdal, osr

print(f"[startup] geo-library imports complete in {time.time() - _t_imports:.1f}s", flush=True)

# Opt into the GDAL 4.0 default of raising Python exceptions instead of C-style
# error codes. Silences the FutureWarning that osgeo emits when neither
# UseExceptions/DontUseExceptions is set. NB: GDAL state is process-global —
# importing this module from a notebook or test harness will flip exception
# handling on for the whole process. That is the desired behaviour (GDAL 4.0's
# default) and what the slurm batches expect, but worth knowing if anyone
# embeds this module elsewhere.
gdal.UseExceptions()
osr.UseExceptions()

# Public re-exports. External callers (scripts/derive_zonal_params.py,
# tests/test_merge_params.py) import these names from gfv2_params.zonal_runners
# directly.
from .lulc import run_lulc_batch
from .merge import run_merge
from .soils import run_soils_batch
from .ssflux import run_ssflux_batch
from .weights import run_build_weights
from .zonal import run_zonal_batch

__all__ = [
    "run_build_weights",
    "run_lulc_batch",
    "run_merge",
    "run_soils_batch",
    "run_ssflux_batch",
    "run_zonal_batch",
]
```

Note: `BATCH_RUNNERS` is **not** added in this commit — it's the subject of
Task 4 (commit 2).

- [ ] **Step 2: Create `zonal.py`**

Create `src/gfv2_params/zonal_runners/zonal.py` with this preamble, then
**copy lines 78–135 from `src/gfv2_params/zonal_runners.py` verbatim** as the
function body:

```python
"""Per-batch continuous-zonal stats from a single CONUS raster.

Used for ``elevation``, ``slope``, ``aspect``, and any other ``script: zonal``
entry in ``configs/zonal/zonal_params.yml``.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen


# vvv  paste lines 78-134 of zonal_runners.py here verbatim  vvv
# (def run_zonal_batch(config: dict, batch_id: int, logger) -> None:
#  ... full body through the function's closing line ...)
```

Exact extraction command (run from repo root, to capture the source lines):

```bash
sed -n '78,134p' src/gfv2_params/zonal_runners.py
```

Paste that output as-is at the bottom of the new file. Do not edit the function
body. Do not add or remove blank lines within the function. Do leave one blank
line between the imports block and the `def`.

- [ ] **Step 3: Create `soils.py`**

Create `src/gfv2_params/zonal_runners/soils.py`. Preamble:

```python
"""Per-batch zonal stats for soils (categorical) and soil_moist_max (continuous).

The ``run_soils_batch`` dispatcher branches on ``source_type`` into one of two
private helpers (``_process_soils`` for the categorical histogram-then-argmax
path, ``_process_soil_moist_max`` for the continuous mean). Both helpers share
gdptools' ``UserTiffData`` / ``ZonalGen`` setup but diverge on post-processing.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen


# vvv  paste lines 137-222 of zonal_runners.py here verbatim  vvv
```

Extraction:

```bash
sed -n '137,222p' src/gfv2_params/zonal_runners.py
```

- [ ] **Step 4: Create `lulc.py`**

Create `src/gfv2_params/zonal_runners/lulc.py`. Preamble:

```python
"""Per-batch LULC parameter derivation.

One ``run_lulc_batch`` covers all four LULC source types (nhm_v11, nalcms,
nlcd, foresce) — the orchestrator normalises ``source_type`` to ``lulc_<source>``
so each source writes per-batch CSVs to its own subdir. Five-step pipeline:
categorical zonal stats on the LULC raster, continuous zonal stats on the
canopy raster, optional zonal stats on a ``keep`` raster, crosswalk lookup +
cov_type assignment, interception/covden/retention computation.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from ..lulc import (
    assign_cov_type,
    class_percentages_from_histogram,
    compute_covden,
    compute_interception,
    compute_retention,
    load_crosswalk,
)


# vvv  paste lines 225-382 of zonal_runners.py here verbatim  vvv
```

Extraction:

```bash
sed -n '225,382p' src/gfv2_params/zonal_runners.py
```

**Naming caution:** this new file (`src/gfv2_params/zonal_runners/lulc.py`) is
*not* the same as `src/gfv2_params/lulc.py`. The two coexist:
- `src/gfv2_params/lulc.py` (318 LOC) — LULC source-of-truth helpers
  (`load_crosswalk`, `assign_cov_type`, `compute_*`, etc.)
- `src/gfv2_params/zonal_runners/lulc.py` (this file, ~160 LOC) — the
  per-batch runner that calls into the source-of-truth helpers.

The `from ..lulc import (...)` line reaches the parent's `lulc.py`
unambiguously (relative import, one level up from the current submodule).
External callers see `gfv2_params.zonal_runners.run_lulc_batch` only.

- [ ] **Step 5: Create `ssflux.py`**

Create `src/gfv2_params/zonal_runners/ssflux.py`. Preamble:

```python
"""Per-batch ssflux parameter derivation.

Uses the CONUS-wide P2P weight matrix produced by ``run_build_weights`` (in
``weights.py``); chains in the merged slope CSV (from a prior ``merge`` of the
``slope`` zonal output) to compute ssflux family params (soil2gw_max,
ssr2gw_rate, fastcoef_lin, slowcoef_lin, gwflow_coef, dprst_seep_rate_open,
dprst_flow_coef).
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from ..raster_ops import deg_to_fraction


# vvv  paste lines 385-512 of zonal_runners.py here verbatim  vvv
```

Extraction:

```bash
sed -n '385,512p' src/gfv2_params/zonal_runners.py
```

- [ ] **Step 6: Create `weights.py`**

Create `src/gfv2_params/zonal_runners/weights.py`. Preamble:

```python
"""CONUS-once P2P weight matrix construction for ssflux.

``run_build_weights`` is invoked once per fabric, before any ssflux array
task; the resulting weight file under ``{data_root}/shared/conus/weights/``
is consumed by ``run_ssflux_batch`` (in ``ssflux.py``).
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
from gdptools import WeightGenP2P


# vvv  paste lines 519-571 of zonal_runners.py here verbatim  vvv
```

Extraction:

```bash
sed -n '519,571p' src/gfv2_params/zonal_runners.py
```

- [ ] **Step 7: Create `merge.py`**

Create `src/gfv2_params/zonal_runners/merge.py`. Preamble:

```python
"""Concat per-batch CSVs for one param into its merged output.

Same function used by both the unified orchestrator (--mode merge) and the
legacy ``scripts/merge_params.py`` (retired in PR #85; the library function
stayed). Sorted by HRU id; writes to ``{output_dir}/{merged_subdir}/{merged_file}``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


# vvv  paste lines 578-638 of zonal_runners.py here verbatim  vvv
```

Extraction:

```bash
sed -n '578,638p' src/gfv2_params/zonal_runners.py
```

- [ ] **Step 8: Static-compile every new file**

Verify each new module is syntactically valid Python (does not import / execute
geo libs, just parses):

```bash
pixi run --as-is python -m py_compile \
    src/gfv2_params/zonal_runners/__init__.py \
    src/gfv2_params/zonal_runners/zonal.py \
    src/gfv2_params/zonal_runners/soils.py \
    src/gfv2_params/zonal_runners/lulc.py \
    src/gfv2_params/zonal_runners/ssflux.py \
    src/gfv2_params/zonal_runners/weights.py \
    src/gfv2_params/zonal_runners/merge.py
```

Expected: silent success (exit 0). If any file has a syntax error, fix it
before continuing.

- [ ] **Step 9: Stop here — DO NOT delete the old file yet**

The new package exists alongside the old `zonal_runners.py`. **Do not run any
import of `gfv2_params.zonal_runners` at this point** — Python's behaviour
is undefined when both a `.py` file and a same-named package directory exist.
Task 3 handles the atomic swap.

---

## Task 3: Atomic swap — delete the old file, verify behaviour, update docs, commit

**Files:**
- Delete: `src/gfv2_params/zonal_runners.py`
- Modify: `README.md` (lines 357, 379 — path references)
- Modify: `CLAUDE.md` (line 71 — Architecture table path reference)

- [ ] **Step 1: Delete the old `zonal_runners.py`**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
git rm src/gfv2_params/zonal_runners.py
```

Expected output:
```
rm 'src/gfv2_params/zonal_runners.py'
```

- [ ] **Step 2: Verify the public import surface**

```bash
pixi run --as-is python -c "from gfv2_params.zonal_runners import run_zonal_batch, run_soils_batch, run_lulc_batch, run_ssflux_batch, run_build_weights, run_merge; print('import surface OK')"
```

Expected:
- First line: `[startup pid=... host=... task=...] python 3.12.X interpreter up, importing geo libs...`
- Next: `[startup] geo-library imports complete in N.Ns`
- Final: `import surface OK`

If any name fails to import, check the corresponding submodule's preamble +
extracted body match the spec exactly.

- [ ] **Step 3: Verify heartbeat fires once (not per submodule)**

```bash
pixi run --as-is python -c "
from gfv2_params.zonal_runners import run_merge
from gfv2_params.zonal_runners import run_zonal_batch
from gfv2_params.zonal_runners import run_lulc_batch
print('cached re-imports done')
"
```

Expected: heartbeat (`[startup ...]`) and completion (`[startup] geo-library
imports complete in ...`) print **exactly once** at the top. Subsequent imports
do not re-fire the heartbeat. Final line `cached re-imports done` appears.

- [ ] **Step 4: Update `CLAUDE.md` line 71 (path reference)**

Find the line in the Architecture table:

```
| Part 2b zonal params | `scripts/derive_zonal_params.py` | `configs/zonal/zonal_params.yml` | `src/gfv2_params/zonal_runners.py` |
```

Change the Builders column to point at the package directory:

```
| Part 2b zonal params | `scripts/derive_zonal_params.py` | `configs/zonal/zonal_params.yml` | `src/gfv2_params/zonal_runners/` |
```

- [ ] **Step 5: Update `README.md` lines 357 and 379 (path references)**

There are two references to the file path. Around line 357:

```markdown
  under [src/gfv2_params/zonal_runners.py](src/gfv2_params/zonal_runners.py).
```

Change to:

```markdown
  under [src/gfv2_params/zonal_runners/](src/gfv2_params/zonal_runners/).
```

And around line 379 (same `[src/gfv2_params/zonal_runners.py](...)` link
pattern, different surrounding sentence):

```markdown
[src/gfv2_params/zonal_runners.py](src/gfv2_params/zonal_runners.py).
```

Change to:

```markdown
[src/gfv2_params/zonal_runners/](src/gfv2_params/zonal_runners/).
```

Verify both are updated:

```bash
grep -n "zonal_runners\.py" README.md
```

Expected: empty (no `.py` suffix remaining in README's path references).

- [ ] **Step 6: Run the test suite**

Push the branch and let CI run pytest (CLAUDE.md forbids pytest on the HPC
head node). For an Agent on a non-HPC dev box:

```bash
pixi run -e dev pytest tests/ -q
```

Expected: all tests pass. Particular attention to:
- `tests/test_zonal_orchestrator.py` (exercises the dispatch path)
- `tests/test_merge_params.py` (imports `run_merge` from the package)
- `tests/test_lulc.py` (exercises the source-of-truth `gfv2_params.lulc` —
  must NOT be confused with the new `gfv2_params.zonal_runners.lulc`)

- [ ] **Step 7: Run pre-commit on changed files**

```bash
pixi run -e dev pre-commit run --files \
    src/gfv2_params/zonal_runners/__init__.py \
    src/gfv2_params/zonal_runners/zonal.py \
    src/gfv2_params/zonal_runners/soils.py \
    src/gfv2_params/zonal_runners/lulc.py \
    src/gfv2_params/zonal_runners/ssflux.py \
    src/gfv2_params/zonal_runners/weights.py \
    src/gfv2_params/zonal_runners/merge.py \
    README.md CLAUDE.md
```

Expected: isort + ruff + end-of-file-fixer + trailing-whitespace + nbstripout
all pass. If isort or ruff reorder anything, accept the change and re-stage.

- [ ] **Step 8: Commit (commit 1 of 2)**

```bash
git add src/gfv2_params/zonal_runners/ README.md CLAUDE.md
git status --short  # verify only the expected files are staged
git commit -m "$(cat <<'EOF'
refactor(zonal_runners): split into a package mirroring depstor_builders/

Closes T2-A from docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md.

src/gfv2_params/zonal_runners.py (638 LOC, one file) -> a 7-file package
under src/gfv2_params/zonal_runners/:

  __init__.py  re-exports + heavy imports + GDAL toggle + startup heartbeat
  zonal.py     run_zonal_batch
  soils.py     run_soils_batch + 2 private helpers
  lulc.py      run_lulc_batch
  ssflux.py    run_ssflux_batch
  weights.py   run_build_weights
  merge.py     run_merge

Mirrors the depstor_builders/ pattern from PR #72. Restores symmetry between
the two Part-2 pipeline halves: adding a new param family is now "add a
submodule" instead of "edit a 638-line module" -- the same shape as adding a
new depstor step.

Pure refactor: function bodies move verbatim. Public import surface
(scripts/derive_zonal_params.py + tests/test_merge_params.py) unchanged.
Startup heartbeat + GDAL exception toggle fire exactly once on first package
import (verified). No new dependencies.

The dispatch-table move (_BATCH_RUNNERS -> BATCH_RUNNERS in __init__.py) is
the separate follow-up commit on this branch.

Docs: README.md and CLAUDE.md path references updated to the package path.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Move `BATCH_RUNNERS` dispatch into the package (commit 2 of 2)

**Files:**
- Modify: `src/gfv2_params/zonal_runners/__init__.py` (add the `BATCH_RUNNERS` dict + extend `__all__`)
- Modify: `scripts/derive_zonal_params.py:32-46` (import `BATCH_RUNNERS`, drop the local `_BATCH_RUNNERS` dict, rename references)

- [ ] **Step 1: Add `BATCH_RUNNERS` to `__init__.py`**

Open `src/gfv2_params/zonal_runners/__init__.py`. After the `__all__ = [...]`
block, append:

```python

# Dispatch table: `script:` tag in configs/zonal/zonal_params.yml -> runner.
# Used by scripts/derive_zonal_params.py to route a per-batch invocation to
# the right submodule. Adding a new `script:` tag means (a) adding a new
# submodule under zonal_runners/, (b) re-exporting its run_*_batch above,
# and (c) adding an entry below — keep them in sync.
BATCH_RUNNERS = {
    "zonal":  run_zonal_batch,
    "soils":  run_soils_batch,
    "lulc":   run_lulc_batch,
    "ssflux": run_ssflux_batch,
}
```

Then update `__all__` to include `"BATCH_RUNNERS"`:

```python
__all__ = [
    "BATCH_RUNNERS",
    "run_build_weights",
    "run_lulc_batch",
    "run_merge",
    "run_soils_batch",
    "run_ssflux_batch",
    "run_zonal_batch",
]
```

- [ ] **Step 2: Update `scripts/derive_zonal_params.py` imports + dispatch references**

Open `scripts/derive_zonal_params.py`. Locate the import block at lines 32–39
and the dispatch dict at lines 41–46. Replace:

```python
from gfv2_params.zonal_runners import (
    run_build_weights,
    run_lulc_batch,
    run_merge,
    run_soils_batch,
    run_ssflux_batch,
    run_zonal_batch,
)

# Dispatch table: `script:` tag in zonal_params.yml -> run_<script>_batch function
_BATCH_RUNNERS = {
    "zonal":  run_zonal_batch,
    "soils":  run_soils_batch,
    "lulc":   run_lulc_batch,
    "ssflux": run_ssflux_batch,
}
```

With:

```python
from gfv2_params.zonal_runners import BATCH_RUNNERS, run_build_weights, run_merge
```

- [ ] **Step 3: Rename `_BATCH_RUNNERS` references in the orchestrator**

Find the two remaining uses of `_BATCH_RUNNERS` in `scripts/derive_zonal_params.py`
and rename them to `BATCH_RUNNERS` (no leading underscore):

```bash
grep -n "_BATCH_RUNNERS" scripts/derive_zonal_params.py
```

Expected before edit: 2 matches inside `run_zonal()` (the `if script_tag not in
_BATCH_RUNNERS:` check and the `_BATCH_RUNNERS[script_tag](...)` call).

Edit both to drop the leading underscore. Verify no `_BATCH_RUNNERS` references
remain:

```bash
grep -n "_BATCH_RUNNERS" scripts/derive_zonal_params.py
```

Expected: empty.

- [ ] **Step 4: Verify the new import path works**

```bash
pixi run --as-is python -c "from gfv2_params.zonal_runners import BATCH_RUNNERS; print(sorted(BATCH_RUNNERS.keys()))"
```

Expected:
- Startup heartbeat (one set of `[startup ...]` lines)
- `['lulc', 'soils', 'ssflux', 'zonal']`

- [ ] **Step 5: Verify the orchestrator script still py_compiles**

```bash
pixi run --as-is python -m py_compile scripts/derive_zonal_params.py
```

Expected: silent success.

- [ ] **Step 6: Run the test suite**

```bash
pixi run -e dev pytest tests/ -q
```

(Or push and let CI run, per the HPC-head-node rule.)

Expected: all tests pass. `tests/test_zonal_orchestrator.py` should be the most
sensitive to this change.

- [ ] **Step 7: Run pre-commit on changed files**

```bash
pixi run -e dev pre-commit run --files \
    src/gfv2_params/zonal_runners/__init__.py \
    scripts/derive_zonal_params.py
```

Expected: pass.

- [ ] **Step 8: Commit (commit 2 of 2)**

```bash
git add src/gfv2_params/zonal_runners/__init__.py scripts/derive_zonal_params.py
git status --short  # verify only those two files are staged
git commit -m "$(cat <<'EOF'
refactor(zonal_runners): move BATCH_RUNNERS dispatch into the package

Follow-up commit to the file split (previous commit on this branch). Moves
the `script:` tag -> runner function dispatch dict out of
scripts/derive_zonal_params.py and into gfv2_params.zonal_runners.__init__,
where the runners themselves live.

Before: adding a new `script:` tag required (a) new submodule, (b) re-export
in __init__.py, (c) register in derive_zonal_params._BATCH_RUNNERS.

After: only (a) + (b) + (c'), and (c') is in the same file as (b) -- the
package owns its own dispatch table.

Also renames _BATCH_RUNNERS -> BATCH_RUNNERS (no leading underscore) since
it's now a public package export, consumed by scripts/derive_zonal_params.py
across module boundaries.

scripts/derive_zonal_params.py: dispatch dict removed; import block trimmed
from 6 names to 3 (the 4 batch runners are now reached via BATCH_RUNNERS).

No behaviour change. Test suite unchanged.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Push + open PR

**Files:** none. Branch push + PR.

- [ ] **Step 1: Show the final commit ladder**

```bash
git log --oneline feat/zonal-runners-split ^main
```

Expected (top-to-bottom — newest first):
```
<sha2> refactor(zonal_runners): move BATCH_RUNNERS dispatch into the package
<sha1> refactor(zonal_runners): split into a package mirroring depstor_builders/
a4ff5df docs(spec): zonal_runners.py -> zonal_runners/ package split (T2-A)
```

- [ ] **Step 2: Push the branch**

```bash
git push -u origin feat/zonal-runners-split
```

- [ ] **Step 3: Open the PR**

```bash
gh pr create --base main --head feat/zonal-runners-split \
  --title "refactor(zonal_runners): split into a package mirroring depstor_builders/ (T2-A)" \
  --body "$(cat <<'EOF'
Tier-2-A from the [fresh-eyes repo evaluation](../blob/main/docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md) — splits the 638-LOC \`src/gfv2_params/zonal_runners.py\` into a 7-file \`src/gfv2_params/zonal_runners/\` package mirroring the existing \`src/gfv2_params/depstor_builders/\` pattern (precedent: PR #72).

## Why

The review flagged \`zonal_runners.py\` as the single most impactful structural change for plug-ability:

> Adding a new \`script:\` tag means editing a 638-line module rather than adding a new file. After this refactor, "add a new param family" becomes "add a new submodule + register in __init__.py" — the same shape as adding a new depstor step.

## What

Three commits on this branch:

1. \`docs(spec): zonal_runners.py -> zonal_runners/ package split (T2-A)\` (already on the branch)
2. \`refactor(zonal_runners): split into a package mirroring depstor_builders/\` — file split + docs path updates
3. \`refactor(zonal_runners): move BATCH_RUNNERS dispatch into the package\` — dispatch table moves to package __init__.py

Each refactor commit is individually revertible.

### Layout after

\`\`\`
src/gfv2_params/zonal_runners/
├── __init__.py   re-exports + BATCH_RUNNERS + heavy imports + GDAL toggle + startup heartbeat
├── zonal.py      run_zonal_batch         (~60 LOC)
├── soils.py      run_soils_batch + 2 private helpers   (~90 LOC)
├── lulc.py       run_lulc_batch          (~160 LOC)
├── ssflux.py     run_ssflux_batch        (~135 LOC)
├── weights.py    run_build_weights       (~60 LOC)
└── merge.py      run_merge               (~60 LOC)
\`\`\`

## Invariants preserved

- Public import surface unchanged (\`from gfv2_params.zonal_runners import run_*\` works as before)
- Behaviour unchanged — function bodies moved verbatim
- Startup heartbeat + \`gdal.UseExceptions()\` / \`osr.UseExceptions()\` fire exactly once per process at first package import (verified)
- No new dependencies

## Test plan

- [ ] CI green on push (pytest gate, per CLAUDE.md no-pytest-on-head-node rule)
- [ ] \`from gfv2_params.zonal_runners import run_zonal_batch, run_soils_batch, run_lulc_batch, run_ssflux_batch, run_build_weights, run_merge, BATCH_RUNNERS\` succeeds
- [ ] Heartbeat fires once per process (verified locally)
- [ ] \`tests/test_zonal_orchestrator.py\` exercises the dispatch path
- [ ] \`tests/test_merge_params.py\` import path (\`from gfv2_params.zonal_runners import run_merge\`) unchanged
- [ ] Optional vpu01 smoke test: \`pixi run python scripts/derive_zonal_params.py --mode zonal --param elevation --batch_id 0 --fabric gfv2_vpu01 ...\` produces the same per-batch CSV as on main

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 4: Report PR URL**

The \`gh pr create\` output is the PR URL. Surface it to the user.

---

## Verification matrix (mapping spec → tasks)

| Spec section | Task |
|---|---|
| File layout | Task 2 (creates all 7 files) |
| `__init__.py` contents (heartbeat / heavy imports / GDAL toggle / re-exports / dispatch) | Task 2 Step 1 + Task 4 Step 1 |
| Submodule template + per-module imports | Task 2 Steps 2–7 (one per submodule, exact preambles in each step) |
| Caller-side changes (derive_zonal_params.py + test_merge_params.py) | Task 4 Steps 2–3 (orchestrator) + nothing needed for test_merge_params.py (re-export covers it) |
| Internal helper placement (`_process_soils`, `_process_soil_moist_max` in soils.py) | Task 2 Step 3 (extraction range 137–222 includes both) |
| Invariant 1: public import surface unchanged | Task 3 Step 2 |
| Invariant 2: behaviour unchanged | Task 3 Step 6 + Task 4 Step 6 (test suite) |
| Invariant 3: heartbeat fires once | Task 3 Step 3 |
| Invariant 4: GDAL toggle once | Implicit (only one call in `__init__.py`; verified by tests/CI) |
| Invariant 5: no new deps | Task 5 Step 1 (commit ladder shows no `pyproject.toml` / `pixi.lock` edits) |
| Risks: pytest discovery breaks | Task 3 Step 6 |
| Risks: caller imports a now-internal name | Task 1 Step 3 (pre-flight grep — re-verified at execution time) |
| Risks: two `lulc.py` modules confuse a reader | Task 2 Step 4 caution block |
| Risks: doc references the old path | Task 3 Steps 4–5 (CLAUDE.md + README) |
| Verification check 1: pre-flight grep | Task 1 Step 3 |
| Verification check 2: import surface | Task 3 Step 2 |
| Verification check 3: heartbeat fires once | Task 3 Step 3 |
| Verification check 4: tests pass | Task 3 Step 6 + Task 4 Step 6 |
| Verification check 5: orchestrator end-to-end | Optional, mentioned in PR body |
| Verification check 6: no new deps | Implicit + commit ladder verification |
| Commit shape: 2 atomic refactor commits | Task 3 Step 8 + Task 4 Step 8 |
| Docs check: CLAUDE.md, README | Task 3 Steps 4–5 |

---

## Notes for the implementer

- **Run every Task 1 step before touching files.** If pre-flight finds an unexpected consumer, escalate before writing code.
- **Do not run any test of `gfv2_params.zonal_runners` between Task 2 Step 1 and Task 3 Step 1.** The .py file and the package directory coexist transiently; Python's behaviour with both is undefined and may even silently use the wrong one.
- **The extraction commands (`sed -n 'X,Yp' src/gfv2_params/zonal_runners.py`) are authoritative.** Copy the output verbatim into the new file — do not retype or "clean up" the function bodies.
- **No new tests are needed.** This is a pure refactor; the existing test suite is the regression net.
- **If pre-commit reformats anything (isort, ruff), accept the reformatting and re-stage.** Do not fight the linters.
- **CLAUDE.md forbids pytest on the HPC head node.** If you're on that host, the test verification steps move to "push and wait for CI."
