# PRMS Parameter Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make "which parameters feed PRMS runoff?" answerable from the repo, by adding a PRMS-metadata axis to the existing per-stage configs and generating an index from it.

**Architecture:** Every parameter file already flows through one function — `iter_declared_params` in `scripts/merge_and_fill_params.py` — which unions four configs. That function moves into the package, gains a `prms:` block per config entry mapping *emitted column → PRMS parameter → consuming process*, and two guards keep the declaration honest. A generator renders the index from it. **No source file moves**: the by-process view is a metadata gap, not a layout gap.

**Tech Stack:** Python 3.12/3.13, PyYAML, pandas, pytest, mkdocs-material, pixi.

**Spec:** `docs/superpowers/specs/2026-08-04-prms-parameter-index-design.md` (Task 2 is outside the spec — see Self-Review)
**Branch:** `docs/prms-parameter-index-spec`

## Global Constraints

- **Never run `pytest` on the HPC head node** (CLAUDE.md). Concurrent geo-library imports trigger shared-FS metadata storms. Local verification is `python -m py_compile` and bare `yaml.safe_load` only. **The authoritative test gate is CI** (`.github/workflows/ci.yml`, `runs-on: ubuntu-latest`, fires on push to `main` and every PR). Where a step below says "verify the test fails/passes", that means *push the branch and read the CI result*, unless the step explicitly says the check is import-free.
- **CI has no data root.** Any test touching `{data_root}` must `pytest.skip` when absent. `scripts/merge_and_fill_params.py:341-346` records this contract: "tests may parse these files but must not touch a real data root."
- **`params:` stays a flat list and every `name:` value stays stable.** `slurm_batch/submit_zonal_params.sh:68-79` carries a hardcoded bash array and `derive_zonal_params.py:64-71` does a flat linear scan. Adding keys *inside* an entry is invisible to both. Adding a new element to `means:`/`ratios:`/`constants:` is safe (no bash array mirrors them); adding one to `params:` or `fractions:` is **not**.
- **Add deps via `pyproject.toml`**, then `pixi install` — not `environment.yml`.
- **Run `pixi run -e dev pre-commit run --all-files` before pushing.**
- **Atomic commits.** Split combined fixes before pushing.
- **Every code change needs a docs check** — audit `docs/`, `README.md`, `slurm_batch/RUNME.md`, `HPC_REFERENCE.md`.
- **Fabric-agnostic:** read paths via `require_config_key` against the active profile; never hardcode `/caldera/...`.

## File Structure

| File | Responsibility |
| --- | --- |
| `docs/parameter_index.md` | The deliverable. Three views over the mapping: by PRMS process, by config entry, by builder. Hand-written in Task 1, generated from Task 8 on. |
| `src/gfv2_params/params_index.py` | **New.** `DeclaredParam`, `iter_declared_params`, `load_declared_params`, `params_for_process`. Pure YAML — no geo imports, no data root. |
| `scripts/merge_and_fill_params.py` | Loses `iter_declared_params`/`DeclaredParam`/`_load_declared_params`; imports them instead. Behaviour unchanged. |
| `configs/zonal/zonal_params.yml` | Gains `prms:` per entry; `slope` also gains `derived_columns:`. |
| `configs/depstor/depstor_params.yml` | Gains `prms:` on `means:`/`ratios:`; gains a `constants:` list. |
| `configs/snarea/snarea_library.yml` | Gains a top-level `prms:`. |
| `scripts/derive_depstor_params.py` | Gains `--mode copy_constants`. |
| `src/gfv2_params/zonal_runners/merge.py` | Applies `derived_columns:` after concat. |
| `tests/test_params_index.py` | **New.** Guard 1 (declaration superset) + unit coverage ported from `test_merge_and_fill_params.py`. |
| `tests/test_params_index_ondisk.py` | **New.** Guard 2, data-root-gated, skips in CI. |
| `scripts/build_parameter_index.py` | **New.** Renders `docs/parameter_index.md`. |
| `docs/ADDING_A_PARAMETER.md` | Step 5 corrected — the submit wrapper does not read the YAML. |
| `tests/test_submit_wrapper_param_lists.py` | **New.** Guards both wrappers' hardcoded arrays against their configs. Deleted when the Snakemake migration retires the wrappers. |

---

### Task 1: Hand-write `docs/parameter_index.md` (PR 1 — standalone)

Ships the answer before any schema work, and becomes the acceptance target for Task 8's generator. Reviewed row-by-row by a hydrologist: this is the step that removes the need to trust the mapping.

**Files:**
- Create: `docs/parameter_index.md`
- Modify: `mkdocs.yml` (nav, after line 113 `- API reference: api.md`)
- Modify: `docs/index.md` (the "Where to start" list, after the "Adding a new HRU parameter?" bullet)

**Interfaces:**
- Consumes: Deliverable 1 of the spec (the 19-row mapping table) — the authoritative row data.
- Produces: `docs/parameter_index.md` with three `##` sections whose headings Task 8 must reproduce exactly: `## By PRMS process`, `## By config entry`, `## By builder`.

- [ ] **Step 1: Create `docs/parameter_index.md`**

Transcribe the spec's Deliverable 1 table into three views. Required frontmatter paragraph and section skeleton:

```markdown
# PRMS parameter index

Every parameter this pipeline emits to `{fabric}/params/merged/`, mapped to the PRMS
process that consumes it, the config entry that declares it, and the builder that computes
it.

Process membership is from `pywatershed.<Process>.get_parameters()` (pywatershed 2.0.4, the
`reference` pixi env), not inference. Column lists are observed on-disk headers from
`gfv2/params/merged/`.

> **Hand-maintained as of 2026-08-04.** Generated from `configs/` once
> `scripts/build_parameter_index.py` lands — see
> `docs/superpowers/specs/2026-08-04-prms-parameter-index-design.md`.

## Reading this index

- ⚠️ marks a column whose **emitted name is not the PRMS parameter name**.
- **DEFECTIVE** marks a column that does not currently represent the PRMS parameter at all.
- *provenance* marks an emitted column that is not a PRMS parameter — a diagnostic or an
  intermediate.

## By PRMS process

### PRMSRunoff (`srunoff_smidx`) — 11 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `soil_moist_max` | `nhm_soil_moist_max_params.csv` | `soil_moist_max` | `zonal_params.yml:81` | `zonal_runners/soils.py` |
| `dprst_seep_rate_open` | `nhm_ssflux_params.csv` | `dprst_seep_rate_open` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `dprst_flow_coef` | `nhm_ssflux_params.csv` | `dprst_flow_coef` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `sro_to_dprst_perv` | `nhm_sro_to_dprst_perv_params.csv` | `sro_to_dprst_perv` | `depstor_params.yml:134` | `same_hru_drains.py` + `perv.py` |
| `sro_to_dprst_imperv` | `nhm_sro_to_dprst_imperv_params.csv` | `sro_to_dprst_imperv` | `depstor_params.yml:141` | `same_hru_drains.py` + `imperv.py` |
| `carea_max` | `nhm_carea_max_params.csv` | `carea_max` | `depstor_params.yml:148` | `carea_map.py` |
| `smidx_coef` | `nhm_smidx_coef_params.csv` | `smidx_coef` | `depstor_params.yml:155` | `carea_map.py` |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | `depstor_params.yml:164` | `imperv.py` + `landmask.py` |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | `depstor_params.yml:177` | `dprst.py` + `landmask.py` |
| `dprst_depth_avg` | `nhm_dprst_depth_avg_params.csv` | `dprst_depth_avg` | `depstor_params.yml:107` | `dprst_depth.py` + `dprst_depth/aggregate.py` |
| `op_flow_thres` | **`op_flow_thres_params.csv`** (not in `merged/`) | `op_flow_thres` | `depstor_rasters.yml:83` | `dprst_depth.py:361` |

### PRMSSoilzone — 8 parameters
### PRMSSnow — 7 parameters
### PRMSCanopy — 6 parameters
### PRMSGroundwater — 1 parameter
### PRMSSolarGeometry / PRMSAtmosphere — 2 parameters
### Not consumed by any pywatershed process
```

Fill the remaining `###` sections from the spec's Deliverable 1 rows using the identical
column layout. Three rows need their specific wording carried over verbatim:

- **`hru_slope`** — currently emitted as `mean` in **degrees**; PRMS wants decimal fraction
  rise/run. State the required conversion `tan(radians(mean))` and that Task 7 will emit it
  directly. Include the worked example: gfv2 HRU 1, `mean = 4.4252` → `hru_slope = 0.0774`.
- **`hru_aspect`** — mark **DEFECTIVE — not `hru_aspect`**. Arithmetic mean of a circular
  variable; TM6B9:603 requires `atan2(mean(sin), mean(cos))`. Link issue #201.
- **`retention`** on `lulc_nalcms`/`nlcd`/`foresce` — mark **unverified**, *not*
  `rad_trncf`. The alias applies only to `lulc_nhm_v11`.

Then `## By config entry` (rows grouped by the four configs) and `## By builder` (rows
grouped by module path), same source data, and close with a `## Known gaps` section
transcribing the spec's "Could not verify" list.

- [ ] **Step 2: Add the mkdocs nav entry**

In `mkdocs.yml`, insert after the `- API reference: api.md` line:

```yaml
  - Parameter index: parameter_index.md
```

- [ ] **Step 3: Add the `docs/index.md` pointer**

In the "Where to start" list, after the "Adding a new HRU parameter?" bullet:

```markdown
- **Which parameters feed a given PRMS process?** →
  [Parameter index](parameter_index.md) maps every emitted parameter to its
  PRMS process, config entry, and builder.
```

- [ ] **Step 4: Verify the docs build (login-node safe — mkdocs imports no geo libs)**

Run: `pixi run -e docs docs-build`
Expected: exits 0; `site/parameter_index/index.html` exists; no `WARNING - Doc file ... contains a link to ... which is not found`.

- [ ] **Step 5: Run pre-commit**

Run: `pixi run -e dev pre-commit run --files docs/parameter_index.md mkdocs.yml docs/index.md`
Expected: all hooks pass (prettier may reformat the markdown; re-stage if so).

- [ ] **Step 6: Commit and open PR 1**

```bash
git add docs/parameter_index.md mkdocs.yml docs/index.md
git commit -m "docs: add the PRMS parameter index

Maps every parameter emitted to {fabric}/params/merged/ to its PRMS process,
config entry, and builder. Hand-written; generated from configs/ once
scripts/build_parameter_index.py lands.

Flags three rows a consumer would otherwise get wrong: hru_slope is degrees
on disk (PRMS wants rise/run, ~57x), hru_aspect is an arithmetic mean of a
circular variable (issue #201), and op_flow_thres is a PRMSRunoff parameter
written outside merged/."
git push -u origin docs/prms-parameter-index-spec
```

---

### Task 2: Close the add-a-param trap — doc fix + bash-array guard (also PR 1)

Adding a param requires editing a hardcoded bash array that **no documentation mentions and
no test guards**. Miss it and `submit_zonal_params.sh` runs the other params, skips yours,
and **exits 0**. The walkthrough a newcomer follows currently states the opposite of the
truth.

This is the cheapest real-defect fix in the plan and it is independent of everything else,
so it ships alongside Task 1.

**Files:**
- Modify: `docs/ADDING_A_PARAMETER.md:239-241`
- Create: `tests/test_submit_wrapper_param_lists.py`

**Interfaces:**
- Consumes: nothing. Pure text + `yaml.safe_load`; no package imports, so it is unaffected by Task 3's refactor.
- Produces: nothing other tasks consume. Deleted when the Snakemake migration retires the wrappers (see `docs/superpowers/specs/2026-08-04-snakemake-migration-design.md`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_submit_wrapper_param_lists.py`:

```python
"""The submit wrappers retype the config's `name:` lists; nothing else checks them.

`slurm_batch/submit_zonal_params.sh` does NOT read the YAML -- it carries a hardcoded
bash array (`PARAMS`) and exports PARAM=<name> per element, and
`scripts/derive_zonal_params.py` then does a flat linear scan for a matching `name:`.
`submit_depstor_params.sh` has the same shape with `FRACTIONS`.

So a param added to the YAML and forgotten in the array is silently NOT RUN, and the
wrapper still exits 0. This test is the only thing that catches that.

DELETE ME when the Snakemake migration retires the wrappers -- a Snakefile reads the
YAML directly and the duplication disappears.

Pure text + yaml.safe_load: no geo imports, CI-safe.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _bash_array(script: Path, name: str) -> list[str]:
    """Extract a `NAME=( ... )` bash array, dropping comments and blank lines."""
    text = script.read_text()
    m = re.search(rf"^{name}=\((.*?)^\)", text, re.S | re.M)
    assert m, f"could not find a {name}=( ... ) array in {script}"
    return [
        line.split("#")[0].strip()
        for line in m.group(1).strip().splitlines()
        if line.split("#")[0].strip()
    ]


@pytest.mark.parametrize(
    ("script_rel", "array_name", "config_rel", "config_key"),
    [
        ("slurm_batch/submit_zonal_params.sh", "PARAMS",
         "configs/zonal/zonal_params.yml", "params"),
        ("slurm_batch/submit_depstor_params.sh", "FRACTIONS",
         "configs/depstor/depstor_params.yml", "fractions"),
    ],
)
def test_wrapper_array_matches_config_names(script_rel, array_name, config_rel, config_key):
    script = _REPO_ROOT / script_rel
    config = _REPO_ROOT / config_rel
    if not script.exists():
        pytest.skip(f"{script_rel} retired by the Snakemake migration -- delete this test")

    from_bash = _bash_array(script, array_name)
    from_yaml = [e["name"] for e in yaml.safe_load(config.read_text())[config_key]]

    assert from_bash == from_yaml, (
        f"{script_rel}'s {array_name} array has drifted from {config_rel}'s "
        f"`{config_key}:` names.\n"
        f"  bash: {from_bash}\n"
        f"  yaml: {from_yaml}\n"
        f"Order matters as well as membership: the wrapper submits in array order, and "
        f"ssflux must follow slope because it reads the merged slope CSV at zonal time."
    )
```

- [ ] **Step 2: Verify it passes today, and would catch a real drift (login-node safe)**

Run: `pixi run --as-is python -c "
import re, yaml, pathlib
def arr(p, n):
    m = re.search(rf'^{n}=\((.*?)^\)', pathlib.Path(p).read_text(), re.S|re.M)
    return [l.split('#')[0].strip() for l in m.group(1).strip().splitlines() if l.split('#')[0].strip()]
b = arr('slurm_batch/submit_zonal_params.sh','PARAMS')
y = [e['name'] for e in yaml.safe_load(open('configs/zonal/zonal_params.yml'))['params']]
print('zonal match:', b == y)
b2 = arr('slurm_batch/submit_depstor_params.sh','FRACTIONS')
y2 = [e['name'] for e in yaml.safe_load(open('configs/depstor/depstor_params.yml'))['fractions']]
print('depstor match:', b2 == y2)
"`
Expected: `zonal match: True` and `depstor match: True`. Both arrays are in sync today — the drift this guards is latent, not actual, which is exactly why it needs a test rather than a fix.

Now prove it fails on drift: temporarily append `  fake_param` inside the `PARAMS=( ... )` array, re-run, expect `zonal match: False`, then **revert the edit**.

- [ ] **Step 3: Fix `docs/ADDING_A_PARAMETER.md`**

Replace step 5 (lines 239-241), which currently reads:

```markdown
5. **Submit the full SLURM array** via
   [`slurm_batch/submit_zonal_params.sh`](../slurm_batch/submit_zonal_params.sh)
   from a shell that has `pixi` on `PATH`. This loops every param in the
   YAML and chains array zonal -> merge per param.
```

with:

```markdown
5. **Add the param to the submit wrapper's `PARAMS` array.**
   [`slurm_batch/submit_zonal_params.sh`](../slurm_batch/submit_zonal_params.sh)
   does **not** read the YAML — it carries a hardcoded bash array
   (`PARAMS`, lines 68-79) and exports `PARAM=<name>` per element.
   A param present in the YAML but absent from that array is silently
   **not run**, and the wrapper still exits 0.

   Order matters: keep `slope` before `ssflux`, which reads the merged
   slope CSV at zonal time. If your param needs the CONUS weight matrix or
   an upstream merge, add it to `NEEDS_WEIGHTS` (line 94) or
   `NEEDS_MERGE_OF` (line 101) as well.

   `tests/test_submit_wrapper_param_lists.py` guards this, so CI will catch
   a forgotten entry — but only after you push.

6. **Submit the full SLURM array** via
   [`slurm_batch/submit_zonal_params.sh`](../slurm_batch/submit_zonal_params.sh)
   from a shell that has `pixi` on `PATH`. It submits an array zonal job
   plus a chained merge for each param in `PARAMS`.
```

- [ ] **Step 4: Check the same claim isn't repeated elsewhere**

Run: `grep -rn "loops every param\|loops that exact\|loops the" --include=*.md --include=*.sh . | grep -v node_modules | grep -v "^./site/"`
Expected: remaining hits are in `slurm_batch/submit_zonal_params.sh`'s own header and `scripts/merge_and_fill_params.py`'s docstring, both of which describe the *intent* correctly ("if you add or remove entries there, also update PARAMS below"). Fix any other doc that repeats the false claim.

- [ ] **Step 5: Run pre-commit**

Run: `pixi run -e dev pre-commit run --files docs/ADDING_A_PARAMETER.md tests/test_submit_wrapper_param_lists.py`
Expected: all hooks pass (shellcheck does not run on the test; ruff formats the Python).

- [ ] **Step 6: Commit**

```bash
git add docs/ADDING_A_PARAMETER.md tests/test_submit_wrapper_param_lists.py
git commit -m "fix(docs): ADDING_A_PARAMETER wrongly said the submit wrapper loops the YAML

It does not. submit_zonal_params.sh carries a hardcoded PARAMS bash array
(:68-79) and exports PARAM=<name> per element; derive_zonal_params.py then
does a flat linear scan for a matching name:. A param added to the YAML and
forgotten in the array is silently NOT RUN, and the wrapper exits 0.

The script's own header states the requirement, but the walkthrough a
newcomer follows stated the opposite.

Adds tests/test_submit_wrapper_param_lists.py, which asserts both wrappers'
arrays match their configs' name: lists in membership AND order (ssflux must
follow slope). Both are in sync today -- the drift is latent, which is why it
needs a guard rather than a fix. Delete the test when the Snakemake migration
retires the wrappers."
```

**PR 1 stops here** — Tasks 1 and 2 together. Tasks 3-8 continue on the same branch after review, or on a follow-up branch.

---

### Task 3: `params_index.py` — move `iter_declared_params`, widen `DeclaredParam`

**Files:**
- Create: `src/gfv2_params/params_index.py`
- Create: `tests/test_params_index.py`
- Modify: `scripts/merge_and_fill_params.py:341-450` (delete the moved block, import instead)
- Modify: `tests/test_merge_and_fill_params.py:630`, `:631`, `:634`

**Interfaces:**
- Produces: `DeclaredParam(name: str, merged_file: str, fill_columns: list, fabric_columns: dict, prms: dict)` — a `NamedTuple` with `prms` defaulting to `{}`; `iter_declared_params(zonal_cfg, depstor_cfg, snarea_cfg=None) -> list[DeclaredParam]`; `load_declared_params() -> list[DeclaredParam]` reading the four repo configs.
- Consumed by: Tasks 4, 5, 6, 7, 8 and `scripts/merge_and_fill_params.py`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_params_index.py`:

```python
"""Unit coverage for gfv2_params.params_index.

Pure YAML parsing -- no geo imports, no data root. Safe on CI's ubuntu-latest.
"""
from __future__ import annotations

from gfv2_params import params_index as pi


def test_declared_param_has_prms_field_defaulting_to_empty():
    d = pi.DeclaredParam("elevation", "nhm_elevation_params.csv", ["mean"], {})
    assert d.prms == {}
    assert d.name == "elevation"


def test_iter_declared_params_unions_four_config_families():
    zonal = {"params": [{"name": "elevation", "merged_file": "nhm_elevation_params.csv",
                         "fill_columns": ["mean"]}]}
    depstor = {
        "fractions": [{"name": "perv_frac", "merged_file": "nhm_perv_frac_params.csv"}],
        "means": [{"name": "dprst_depth_avg", "merged_file": "nhm_dprst_depth_avg_params.csv",
                   "fill_columns": ["dprst_depth_avg"]}],
        "ratios": [{"name": "dprst_frac", "output_file": "nhm_dprst_frac_params.csv",
                    "fill_columns": ["dprst_frac"]}],
    }
    declared = pi.iter_declared_params(zonal, depstor)
    names = {d.name for d in declared}
    assert names == {"elevation", "dprst_depth_avg", "dprst_frac"}
    # fractions are intermediates -- excluded despite carrying a merged_file key
    assert "perv_frac" not in names


def test_iter_declared_params_carries_prms_block():
    zonal = {"params": [{
        "name": "slope", "merged_file": "nhm_slope_params.csv", "fill_columns": ["mean"],
        "prms": {"columns": {"hru_slope": {"prms": "hru_slope",
                                           "processes": ["PRMSSolarGeometry"]}}},
    }]}
    declared = pi.iter_declared_params(zonal, {})
    assert declared[0].prms["columns"]["hru_slope"]["prms"] == "hru_slope"
```

- [ ] **Step 2: Verify it fails (import-free check — login-node safe)**

Run: `pixi run --as-is python -c "import gfv2_params.params_index"`
Expected: `ModuleNotFoundError: No module named 'gfv2_params.params_index'`

- [ ] **Step 3: Create `src/gfv2_params/params_index.py`**

Move the block verbatim from `scripts/merge_and_fill_params.py:341-450`, adding the fifth field and its default:

```python
"""Every parameter with a canonical `merged/` output, and what it declares.

Moved here from scripts/merge_and_fill_params.py so the declaration is importable
by the index generator and the guards, not only by the fill sweep.

Pure YAML: only the static name/merged_file/output_file/fill_columns/fabric_columns/
prms fields are read, none of which are {data_root}/{fabric}-templated. That keeps
this import safe with no data root -- the CI contract recorded at
scripts/merge_and_fill_params.py's config-path block.
"""
from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ZONAL_PARAMS_CONFIG = _REPO_ROOT / "configs" / "zonal" / "zonal_params.yml"
DEPSTOR_PARAMS_CONFIG = _REPO_ROOT / "configs" / "depstor" / "depstor_params.yml"
SNAREA_LIBRARY_CONFIG = _REPO_ROOT / "configs" / "snarea" / "snarea_library.yml"


class DeclaredParam(NamedTuple):
    """One config entry's contract: what file, what may be filled, what PRMS calls it.

    A NamedTuple rather than a bare tuple because this record has now been widened
    three times (fill_columns, fabric_columns, prms) and each widening reaches
    several consumption sites. `prms` carries a default so positional construction
    in existing tests keeps working; tuple-EQUALITY assertions do not survive and
    must be rewritten to attribute access.
    """

    name: str
    merged_file: str
    fill_columns: list
    fabric_columns: dict
    prms: dict = {}


def _load_yaml_doc(path: Path) -> dict:
    """Bare yaml.safe_load of a config file, no placeholder resolution."""
    return yaml.safe_load(path.read_text()) or {}


def iter_declared_params(
    zonal_cfg: dict, depstor_cfg: dict, snarea_cfg: dict | None = None
) -> list[DeclaredParam]:
    """Every param with a canonical `merged/` output.

    `zonal_cfg["params"]` and `depstor_cfg["means"]` name their file `merged_file`;
    `depstor_cfg["ratios"]` names it `output_file`. `depstor_cfg["fractions"]` is
    DELIBERATELY EXCLUDED: its merged_file key names a per-fraction COUNT csv under
    merged/_intermediates/, never a merged/ output.

    `snarea_cfg` does not share the list-of-entries shape -- the whole document IS
    the entry, so a `prms:` block there is a top-level key.
    """
    def _record(name: str, merged_file: str, entry: dict) -> DeclaredParam:
        return DeclaredParam(
            name=name,
            merged_file=merged_file,
            fill_columns=list(entry.get("fill_columns") or []),
            fabric_columns=dict(entry.get("fabric_columns") or {}),
            prms=dict(entry.get("prms") or {}),
        )

    declared: list[DeclaredParam] = []

    for entry in zonal_cfg.get("params", []) or []:
        if entry.get("merged_file"):
            declared.append(_record(entry["name"], entry["merged_file"], entry))

    for entry in depstor_cfg.get("means", []) or []:
        if entry.get("merged_file"):
            declared.append(_record(entry["name"], entry["merged_file"], entry))

    for entry in depstor_cfg.get("ratios", []) or []:
        if entry.get("output_file"):
            declared.append(_record(entry["name"], entry["output_file"], entry))

    for entry in depstor_cfg.get("constants", []) or []:
        if entry.get("merged_file"):
            declared.append(_record(entry["name"], entry["merged_file"], entry))

    if snarea_cfg and snarea_cfg.get("params_file"):
        declared.append(_record("snarea_curve", snarea_cfg["params_file"], snarea_cfg))

    return declared


def load_declared_params() -> list[DeclaredParam]:
    """iter_declared_params over the real in-repo configs."""
    return iter_declared_params(
        _load_yaml_doc(ZONAL_PARAMS_CONFIG),
        _load_yaml_doc(DEPSTOR_PARAMS_CONFIG),
        _load_yaml_doc(SNAREA_LIBRARY_CONFIG),
    )
```

The `constants:` loop is included now so Task 6 adds only config plus a copy mode. It is a
no-op until then.

- [ ] **Step 4: Rewrite `scripts/merge_and_fill_params.py` to import**

Delete lines 341-450 (the `_REPO_ROOT`/config-path block, `_load_yaml_doc`, `DeclaredParam`, `iter_declared_params`, `_load_declared_params`). Add to the import block:

```python
from gfv2_params.params_index import DeclaredParam, iter_declared_params, load_declared_params
```

Replace the single call site `declared_params = _load_declared_params()` in `main()` with `declared_params = load_declared_params()`. `DeclaredParam` and `iter_declared_params` stay re-exported by that import so `tests/test_merge_and_fill_params.py`'s `maf.DeclaredParam` / `maf.iter_declared_params` references keep resolving.

- [ ] **Step 5: Rewrite the three tuple-equality assertions**

In `tests/test_merge_and_fill_params.py`, replace line 630:

```python
    assert by_name["dprst_frac"] == ("dprst_frac", "nhm_dprst_frac_params.csv", ["dprst_frac"], {})
```

with:

```python
    d = by_name["dprst_frac"]
    assert (d.name, d.merged_file, d.fill_columns, d.fabric_columns) == (
        "dprst_frac", "nhm_dprst_frac_params.csv", ["dprst_frac"], {})
```

Replace line 631 the same way for `elevation` / `nhm_elevation_params.csv` / `["mean"]`. Replace line 634:

```python
    assert declared == [("snarea_curve", "nhm_snarea_curve_params.csv", ["hru_deplcrv"], {})]
```

with:

```python
    assert len(declared) == 1
    d = declared[0]
    assert (d.name, d.merged_file, d.fill_columns, d.fabric_columns) == (
        "snarea_curve", "nhm_snarea_curve_params.csv", ["hru_deplcrv"], {})
```

The positional `maf.DeclaredParam(...)` constructions at `:737`, `:751`, `:762` and the `d[0]` indexing at `:624`/`:629` need **no change** — a defaulted fifth field leaves both working.

- [ ] **Step 6: Verify imports compile (login-node safe)**

Run: `pixi run --as-is python -m py_compile src/gfv2_params/params_index.py scripts/merge_and_fill_params.py tests/test_params_index.py tests/test_merge_and_fill_params.py`
Expected: exits 0, no output.

Run: `pixi run --as-is python -c "from gfv2_params.params_index import load_declared_params; d=load_declared_params(); print(len(d)); print(sorted(x.name for x in d))"`
Expected: prints `16` and the sorted names — `aspect, carea_max, dprst_depth_avg, dprst_frac, elevation, hru_percent_imperv, lulc_foresce, lulc_nalcms, lulc_nhm_v11, lulc_nlcd, slope, snarea_curve, soil_moist_max, soils, sro_to_dprst_imperv, sro_to_dprst_perv, ssflux`. (That is 17 names for 17 declared entries; adjust the printed count assertion to match what the configs actually hold — the point is that no entry vanished in the move.)

- [ ] **Step 7: Commit**

```bash
git add src/gfv2_params/params_index.py tests/test_params_index.py \
        scripts/merge_and_fill_params.py tests/test_merge_and_fill_params.py
git commit -m "refactor: move iter_declared_params into gfv2_params.params_index

Widens DeclaredParam with a defaulted `prms` field. Tuple-equality assertions
do not survive a NamedTuple widening, so the three at test_merge_and_fill_params
:630/:631/:634 are rewritten to attribute access; the positional constructions
at :737/:751/:762 and the d[0] indexing survive the default unchanged.

The DeclaredParam docstring records why this matters: the last widening broke
four consumption sites while the suite stayed green, because its fixtures
hand-built the old tuple shape."
```

- [ ] **Step 8: Push and confirm CI is green**

Run: `git push`
Expected: `pytest tests/` passes on CI. `tests/test_params_index.py` runs; `tests/test_merge_and_fill_params.py` still passes with the rewritten assertions.

---

### Task 4: `prms:` metadata + Guard 1 (declaration superset)

**Files:**
- Modify: `configs/zonal/zonal_params.yml` (10 entries), `configs/depstor/depstor_params.yml` (1 mean + 6 ratios), `configs/snarea/snarea_library.yml` (top-level)
- Modify: `tests/test_params_index.py`

**Interfaces:**
- Consumes: `DeclaredParam.prms` from Task 3.
- Produces: `prms.columns: {emitted_column: {prms: str, processes: list[str]}}` and `prms.provenance: {emitted_column: str}` on every declared entry.

- [ ] **Step 1: Write the failing guard**

Append to `tests/test_params_index.py`:

```python
import pytest


def _flatten(fill_columns):
    """fill_columns entries may be a list of alias alternatives, not only a string.

    zonal_params.yml's lulc_nhm_v11 carries [retention, rad_trncf] because
    lulc_prederived.py renamed the same computed quantity; fabrics built before the
    rename carry `retention`, after carry `rad_trncf`. EVERY alternative must be
    declared, so a fabric of either vintage is covered.
    """
    out = []
    for item in fill_columns:
        if isinstance(item, (list, tuple)):
            out.extend(item)
        else:
            out.append(item)
    return out


@pytest.mark.parametrize("declared", pi.load_declared_params(), ids=lambda d: d.name)
def test_guard1_prms_declares_every_fillable_column(declared):
    """prms.columns | prms.provenance is the DECLARED COMPLETE column list.

    Guard 1 only proves the declaration is self-consistent -- it is declaration ->
    declaration and cannot see disk. Guard 2 (test_params_index_ondisk.py) is the
    one that catches a new column appearing with no PRMS decision recorded.
    """
    assert declared.prms, (
        f"{declared.name} has no `prms:` block. It is mandatory, not optional -- "
        f"without it this guard passes vacuously."
    )
    known = set(declared.prms.get("columns") or {}) | set(declared.prms.get("provenance") or {})
    required = set(_flatten(declared.fill_columns)) | set(declared.fabric_columns or {})
    missing = required - known
    assert not missing, (
        f"{declared.name}: {sorted(missing)} are declared fillable but appear in neither "
        f"prms.columns nor prms.provenance. Every emitted column needs a PRMS decision."
    )
```

- [ ] **Step 2: Verify it fails (login-node safe — pure YAML, no geo imports)**

Run: `pixi run --as-is python -c "
from gfv2_params.params_index import load_declared_params
for d in load_declared_params():
    if not d.prms: print('NO prms block:', d.name)
"`
Expected: lists all 17 entries — none has a `prms:` block yet.

- [ ] **Step 3: Add `prms:` to `configs/zonal/zonal_params.yml`**

For `elevation` (and identically for `slope`/`aspect`, changing only the mapped name and processes):

```yaml
    prms:
      columns:
        mean: {prms: hru_elev, processes: [temp_distribution]}
      provenance:
        count: exactextract cell count
        std: within-HRU standard deviation
        min: within-HRU minimum
        "25%": within-HRU 25th percentile
        "50%": within-HRU median
        "75%": within-HRU 75th percentile
        max: within-HRU maximum
        sum: within-HRU sum
```

`provenance` here is **not** a restatement of "unfilled" — these eight ARE in `fill_columns`, and `zonal_params.yml:50-54` states explicitly they are not provenance in the "not derivable by design" sense. `prms.provenance` answers a different question: *is this a PRMS parameter?* They are filled and they are not PRMS parameters.

For `aspect`, mark the defect rather than a mapping:

```yaml
    prms:
      columns: {}   # DEFECTIVE -- `mean` is an arithmetic mean of a circular variable.
                    # TM6B9:603 requires atan2(mean(sin), mean(cos)). See issue #201.
      provenance:
        mean: arithmetic mean of a circular field -- NOT hru_aspect, see issue #201
        count: exactextract cell count
        std: within-HRU standard deviation
        min: within-HRU minimum
        "25%": within-HRU 25th percentile
        "50%": within-HRU median
        "75%": within-HRU 75th percentile
        max: within-HRU maximum
        sum: within-HRU sum
```

For `ssflux`, note `hru_area` comes from `fabric_columns`, not `fill_columns`, and is **not** PRMS `hru_area`:

```yaml
    prms:
      columns:
        soil2gw_max:          {prms: soil2gw_max,          processes: [PRMSSoilzone]}
        ssr2gw_rate:          {prms: ssr2gw_rate,          processes: [PRMSSoilzone]}
        fastcoef_lin:         {prms: fastcoef_lin,         processes: [PRMSSoilzone]}
        slowcoef_lin:         {prms: slowcoef_lin,         processes: [PRMSSoilzone]}
        gwflow_coef:          {prms: gwflow_coef,          processes: [PRMSGroundwater]}
        dprst_seep_rate_open: {prms: dprst_seep_rate_open, processes: [PRMSRunoff]}
        dprst_flow_coef:      {prms: dprst_flow_coef,      processes: [PRMSRunoff]}
      provenance:
        k_perm_wtd: litho-weighted permeability, flux-normalisation input
        mean_slope_fraction: tan(radians(slope mean)), flux-normalisation input
        hru_area: fabric geometry.area in m2 -- NOT PRMS hru_area, which is acres
```

For `lulc_nhm_v11`, both alias alternatives map to the same PRMS name:

```yaml
    prms:
      columns:
        cov_type:    {prms: cov_type,    processes: [PRMSCanopy, PRMSSnow, PRMSSoilzone]}
        covden_sum:  {prms: covden_sum,  processes: [PRMSCanopy, PRMSSnow]}
        covden_win:  {prms: covden_win,  processes: [PRMSCanopy, PRMSSnow]}
        srain_intcp: {prms: srain_intcp, processes: [PRMSCanopy]}
        wrain_intcp: {prms: wrain_intcp, processes: [PRMSCanopy]}
        snow_intcp:  {prms: snow_intcp,  processes: [PRMSCanopy]}
        retention:   {prms: rad_trncf,   processes: [PRMSSnow]}
        rad_trncf:   {prms: rad_trncf,   processes: [PRMSSnow]}
      provenance: {}
```

For `lulc_nalcms` / `lulc_nlcd` / `lulc_foresce`, `retention` is **unverified** — `lulc.py:186-193` computes `keep/100` or a crosswalk column, not `lulc_prederived.py`'s Beer's-law `rad_trncf`:

```yaml
    prms:
      columns:
        cov_type:    {prms: cov_type,    processes: [PRMSCanopy, PRMSSnow, PRMSSoilzone]}
        covden_sum:  {prms: covden_sum,  processes: [PRMSCanopy, PRMSSnow]}
        covden_win:  {prms: covden_win,  processes: [PRMSCanopy, PRMSSnow]}
        srain_intcp: {prms: srain_intcp, processes: [PRMSCanopy]}
        wrain_intcp: {prms: wrain_intcp, processes: [PRMSCanopy]}
        snow_intcp:  {prms: snow_intcp,  processes: [PRMSCanopy]}
      provenance:
        retention: UNVERIFIED -- lulc.py's derivation differs from lulc_prederived.py's
                   Beer's-law rad_trncf; do not assume they are the same parameter
```

`soils` → `{prms: soil_type, processes: [PRMSSoilzone]}`; `soil_moist_max` → `{prms: soil_moist_max, processes: [PRMSRunoff, PRMSSoilzone]}`.

- [ ] **Step 4: Add `prms:` to `configs/depstor/depstor_params.yml`**

On the `dprst_depth_avg` mean entry:

```yaml
    prms:
      columns:
        dprst_depth_avg: {prms: dprst_depth_avg, processes: [PRMSRunoff]}
      provenance:
        dprst_depth_provenance: how each HRU's depth was derived (3dep_raw /
                                hollister_flat / calibrated / no_dprst_cells)
```

On each of the six ratios, a single mapped column and empty provenance — e.g.:

```yaml
    prms:
      columns:
        carea_max: {prms: carea_max, processes: [PRMSRunoff]}
      provenance: {}
```

`hru_percent_imperv` and `dprst_frac` take `processes: [PRMSRunoff, PRMSSoilzone, PRMSEt]`; `sro_to_dprst_perv`/`sro_to_dprst_imperv`/`smidx_coef` take `[PRMSRunoff]`.

Leave `fractions:` untouched — `iter_declared_params` excludes them.

- [ ] **Step 5: Add the top-level `prms:` to `configs/snarea/snarea_library.yml`**

`iter_declared_params` passes the whole document as the entry, so this is a top-level key beside `library_file`/`params_file`:

```yaml
prms:
  columns:
    hru_deplcrv:     {prms: hru_deplcrv,   processes: [PRMSSnow]}
    snarea_thresh:   {prms: snarea_thresh, processes: [PRMSSnow]}
    snarea_curve_0:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_1:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_2:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_3:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_4:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_5:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_6:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_7:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_8:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_9:  {prms: snarea_curve,  processes: [PRMSSnow]}
    snarea_curve_10: {prms: snarea_curve,  processes: [PRMSSnow]}
  provenance:
    cv_assign: assigned CV bin
    cv_subgrid: sub-grid CV estimate
    cv_empirical: empirical CV -- derivable for only ~42% of HRUs BY DESIGN
    cv_source: which CV estimate was used
    sdc_status: derivation status
    sca_class: snow-covered-area class
    similarity: scale-free SDC similarity metric
    n_seasons: seasons contributing
    n_peak_years: peak years contributing
    peak_swe_mm: peak SWE driving snarea_thresh
```

- [ ] **Step 6: Verify the guard now passes (login-node safe)**

Run: `pixi run --as-is python -c "
from gfv2_params.params_index import load_declared_params
bad = 0
for d in load_declared_params():
    known = set((d.prms.get('columns') or {})) | set((d.prms.get('provenance') or {}))
    req = set()
    for i in d.fill_columns:
        req.update(i if isinstance(i, (list, tuple)) else [i])
    req |= set(d.fabric_columns or {})
    missing = req - known
    if not d.prms or missing:
        print('FAIL', d.name, sorted(missing)); bad += 1
print('failures:', bad)
"`
Expected: `failures: 0`

- [ ] **Step 7: Run pre-commit (yamllint will check the configs)**

Run: `pixi run -e dev pre-commit run --files configs/zonal/zonal_params.yml configs/depstor/depstor_params.yml configs/snarea/snarea_library.yml tests/test_params_index.py`
Expected: all hooks pass.

- [ ] **Step 8: Commit and push**

```bash
git add configs/ tests/test_params_index.py
git commit -m "feat(config): declare PRMS metadata per emitted column + Guard 1

Adds a prms: block to every declared config entry, mapping emitted column ->
PRMS parameter -> consuming process. processes: is PER-COLUMN, not per-entry:
ssflux alone spans PRMSSoilzone, PRMSGroundwater and PRMSRunoff across
different columns, so an entry-level key would report 5 non-runoff parameters
as feeding runoff.

prms.provenance is ORTHOGONAL to fill_columns, not a restatement of it.
fill_columns asks 'is this KNN-interpolable?'; prms.provenance asks 'is this a
PRMS parameter?'. The elevation/slope/aspect stats are the load-bearing case:
filled AND not PRMS parameters.

Guard 1 asserts prms.columns | prms.provenance is a superset of
fill_columns | fabric_columns, flattening alias lists so both alternatives of
lulc_nhm_v11's [retention, rad_trncf] must be declared.

Additive to entry bodies only: params: stays a flat list and every name: is
unchanged, so submit_zonal_params.sh and submit_depstor_params.sh are
untouched."
git push
```

---

### Task 5: Guard 2 — on-disk header check (data-root-gated)

Guard 1 is declaration → declaration and **cannot** catch the D2/D3 class it was written for. Guard 2 is the one that sees disk.

**Files:**
- Create: `tests/test_params_index_ondisk.py`

**Interfaces:**
- Consumes: `load_declared_params()` from Task 3, `load_base_config`/`require_config_key` from `gfv2_params.config`.

- [ ] **Step 1: Write the test**

```python
"""Guard 2: the declared column list must equal the on-disk header.

DATA-ROOT-GATED. CI (ubuntu-latest) has no data root, so every case SKIPS there
and CI reports green while proving nothing -- record this test's result by SLURM
job id, never infer it from a CI badge. See the companion spec's Testing section.

Pure pandas + yaml: no rasterio/GDAL/pyogrio, so it is head-node safe.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.params_index import load_declared_params

_DECLARED = load_declared_params()


def _merged_dir():
    base = load_base_config(None, fabric=None)
    return Path(base["data_root"]) / base["fabric"] / "params" / "merged", \
        require_config_key(base, "id_feature", "test_params_index_ondisk")


@pytest.mark.parametrize("declared", _DECLARED, ids=lambda d: d.name)
def test_guard2_declared_columns_match_disk(declared):
    merged_dir, id_feature = _merged_dir()
    path = merged_dir / declared.merged_file
    if not path.exists():
        pytest.skip(f"not built for this fabric: {path}")

    header = set(pd.read_csv(path, nrows=0).columns)
    declared_cols = set(declared.prms.get("columns") or {}) | set(
        declared.prms.get("provenance") or {})

    # Alias columns: only one alternative is present on any given fabric, so
    # declared-but-absent is expected. Absent-but-undeclared is the failure.
    undeclared = header - declared_cols - {id_feature}
    assert not undeclared, (
        f"{declared.name}: {sorted(undeclared)} exist in {path.name} but are declared "
        f"in neither prms.columns nor prms.provenance. A new column reached disk with "
        f"no PRMS decision recorded -- that is exactly how the hru_slope and hru_aspect "
        f"defects arose."
    )
```

- [ ] **Step 2: Verify it compiles and skips cleanly without a data root**

Run: `pixi run --as-is python -m py_compile tests/test_params_index_ondisk.py`
Expected: exits 0.

- [ ] **Step 3: Run it once against the real data root — on a COMPUTE NODE, never the login node**

```bash
srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=2 --mem=16G \
  pixi run --as-is python -m pytest tests/test_params_index_ondisk.py -v
```
Expected: passes for every built param, skips `lulc_nlcd`/`lulc_foresce` (inputs unstaged). Any failure names a real undeclared column — fix the config, not the test. **Record the job id.**

- [ ] **Step 4: Commit and push**

```bash
git add tests/test_params_index_ondisk.py
git commit -m "test: Guard 2 -- declared columns must match the on-disk header

Guard 1 is declaration -> declaration and is a tautology on 12 entries, vacuous
on 3 (ssflux declares 7 of 10 columns, snarea 13 of 23, dprst_depth_avg 1 of 2).
Guard 2 is the one that catches a new column reaching disk with no PRMS
decision recorded.

Data-root-gated, so it SKIPS on CI and reports green while proving nothing --
its result must be recorded by SLURM job id."
git push
```

---

### Task 6: `constants:` + `op_flow_thres` into `merged/` (D1)

**Files:**
- Modify: `configs/depstor/depstor_params.yml` (new top-level `constants:` list)
- Modify: `scripts/derive_depstor_params.py` (new `--mode copy_constants`)
- Modify: `tests/test_params_index.py`

**Interfaces:**
- Consumes: the `constants:` loop already present in `iter_declared_params` (Task 3).
- Produces: `merged/nhm_op_flow_thres_params.csv`.

- [ ] **Step 1: Add the config block**

Append to `configs/depstor/depstor_params.yml`:

```yaml
# Constant per-HRU params a depstor BUILDER writes directly, with no zonal pass.
# Copied into merged/ by `--mode copy_constants` so the "everything a consumer
# needs is in merged/" invariant holds. NOT a `means:` entry: run_mean_zonal does
# Path(spec["source_raster"]) unconditionally and _find_mean advertises every
# means[].name as a runnable --mean target, so a raster-less means entry is a
# KeyError waiting for the first operator who types --mean op_flow_thres.
constants:
  - name: op_flow_thres
    source: "{data_root}/{fabric}/depstor_rasters/op_flow_thres_params.csv"
    merged_file: nhm_op_flow_thres_params.csv
    fill_columns: [op_flow_thres]
    prms:
      columns:
        op_flow_thres: {prms: op_flow_thres, processes: [PRMSRunoff]}
      provenance: {}
```

- [ ] **Step 2: Write the failing test**

Append to `tests/test_params_index.py`:

```python
def test_op_flow_thres_is_declared_with_a_merged_file():
    """D1: op_flow_thres is a PRMSRunoff param that was written outside merged/,
    so iter_declared_params and warn_undeclared_merged_files both missed it."""
    declared = {d.name: d for d in pi.load_declared_params()}
    assert "op_flow_thres" in declared
    assert declared["op_flow_thres"].merged_file == "nhm_op_flow_thres_params.csv"
    assert declared["op_flow_thres"].prms["columns"]["op_flow_thres"]["processes"] == ["PRMSRunoff"]
```

- [ ] **Step 3: Verify it fails, then passes (login-node safe)**

Run before Step 1's edit: `pixi run --as-is python -c "
from gfv2_params.params_index import load_declared_params
print('op_flow_thres' in {d.name for d in load_declared_params()})"`
Expected before: `False`. Expected after Step 1: `True`.

- [ ] **Step 4: Add `--mode copy_constants` to `scripts/derive_depstor_params.py`**

Add to the `--mode` choices list, and add the handler:

```python
def run_copy_constants(args, logger) -> None:
    """Copy each `constants:` source into merged/ under its canonical name.

    A straight copy, not a computation: the builder already wrote one row per HRU
    (depstor_builders/dprst_depth.py builds the id column from ctx.hru_gpkg), so
    there is nothing to aggregate and the fill declaration is a no-op.
    """
    config = _load_resolved_config(args)
    merged_dir = Path(config["output_dir"]) / config["merged_subdir"]
    merged_dir.mkdir(parents=True, exist_ok=True)

    for entry in config.get("constants", []) or []:
        src = Path(entry["source"])
        if not src.exists():
            raise FileNotFoundError(
                f"constants entry '{entry['name']}' source not found: {src}. "
                f"Run the depstor raster build first -- this file is written by a "
                f"builder, not by a zonal pass."
            )
        dst = merged_dir / entry["merged_file"]
        df = pd.read_csv(src)
        df.to_csv(dst, index=False)
        logger.info("copy_constants: %s -> %s (%d rows)", src, dst, len(df))
```

Wire it into the `main()` dispatch alongside the existing modes.

- [ ] **Step 5: Verify it compiles**

Run: `pixi run --as-is python -m py_compile scripts/derive_depstor_params.py`
Expected: exits 0.

- [ ] **Step 6: Run it against gfv2 — compute node**

```bash
srun -p cpu -A impd --time=00:10:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run --as-is python scripts/derive_depstor_params.py \
    --config configs/depstor/depstor_params.yml \
    --base_config configs/base_config.yml --fabric gfv2 --mode copy_constants
```
Expected: logs one copy, 361471 rows. Verify: `head -2 $(pixi run data-root)/gfv2/params/merged/nhm_op_flow_thres_params.csv` shows `nat_hru_id,op_flow_thres` then `1,1.0`.

- [ ] **Step 7: Docs check + commit**

Add a `copy_constants` line to `slurm_batch/RUNME.md`'s Step 4 and `HPC_REFERENCE.md`'s mode table.

```bash
git add configs/depstor/depstor_params.yml scripts/derive_depstor_params.py \
        tests/test_params_index.py slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md
git commit -m "feat(depstor): copy op_flow_thres into merged/ via a constants: list

op_flow_thres is a PRMSRunoff parameter written to depstor_rasters/, so it was
invisible to iter_declared_params AND to warn_undeclared_merged_files (which
globs merged_dir). Anyone assembling a parameter file by globbing
merged/nhm_*_params.csv silently dropped it.

A constants: list rather than a means: entry -- run_mean_zonal reads
spec['source_raster'] unconditionally and op_flow_thres has no raster."
git push
```

---

### Task 7: `derived_columns:` + emit `hru_slope` (D0a)

**Files:**
- Modify: `configs/zonal/zonal_params.yml` (the `slope` entry)
- Modify: `src/gfv2_params/zonal_runners/merge.py`
- Modify: `tests/test_params_index.py`

**Interfaces:**
- Consumes: `raster_ops.deg_to_fraction` (`np.tan(np.deg2rad(x))`, already exists at `raster_ops.py:264-266`).
- Produces: an `hru_slope` column in `merged/nhm_slope_params.csv`.

- [ ] **Step 1: Add the config block to the `slope` entry**

```yaml
    # PRMS hru_slope is a decimal fraction rise/run (TM6B9:536); slope.vrt is
    # rd.TerrainAttribute(..., "slope_degrees"), so `mean` is DEGREES -- a ~57x
    # discrepancy for small angles. Emit the PRMS quantity directly rather than
    # leaving a footgun. ssflux.py:63 already applies this same conversion.
    #
    # KNOWN APPROXIMATION: tan(mean) != mean(tan), and tan is convex, so this
    # underestimates. Measured over all 361,471 gfv2 HRUs (2nd-order Taylor from
    # on-disk mean/std): median 0.2%, p90 2.4%, p99 5.6%. Too small to justify a
    # CONUS fractional-slope VRT -- none exists.
    derived_columns:
      hru_slope: {from: mean, transform: deg_to_fraction}
    prms:
      columns:
        hru_slope: {prms: hru_slope, processes: [PRMSSolarGeometry, PRMSAtmosphere]}
      provenance:
        mean: mean cell slope in DEGREES -- the raw stat hru_slope is derived from
        count: exactextract cell count
        std: within-HRU standard deviation
        min: within-HRU minimum
        "25%": within-HRU 25th percentile
        "50%": within-HRU median
        "75%": within-HRU 75th percentile
        max: within-HRU maximum
        sum: within-HRU sum
```

- [ ] **Step 2: Write the failing test**

Append to `tests/test_params_index.py`:

```python
import math

from gfv2_params.zonal_runners.merge import apply_derived_columns


def test_apply_derived_columns_converts_slope_degrees_to_rise_run():
    import pandas as pd
    df = pd.DataFrame({"nat_hru_id": [1, 2], "mean": [4.4252, 45.0]})
    out = apply_derived_columns(df, {"hru_slope": {"from": "mean",
                                                   "transform": "deg_to_fraction"}})
    assert math.isclose(out["hru_slope"][0], 0.077398, rel_tol=1e-4)
    assert math.isclose(out["hru_slope"][1], 1.0, rel_tol=1e-9)  # tan(45 deg) == 1
    assert "mean" in out.columns  # the raw stat is kept as provenance


def test_apply_derived_columns_rejects_an_unknown_transform():
    import pandas as pd
    import pytest
    df = pd.DataFrame({"mean": [1.0]})
    with pytest.raises(ValueError, match="not a known transform"):
        apply_derived_columns(df, {"x": {"from": "mean", "transform": "nope"}})
```

- [ ] **Step 3: Implement `apply_derived_columns` in `src/gfv2_params/zonal_runners/merge.py`**

```python
from gfv2_params import raster_ops

# Transforms a `derived_columns:` entry may name. Deliberately a whitelist rather
# than getattr(raster_ops, name): a typo must raise, not silently resolve to some
# other module-level function.
_TRANSFORMS = {
    "deg_to_fraction": raster_ops.deg_to_fraction,
}


def apply_derived_columns(df, derived_columns: dict | None):
    """Add each declared derived column to a merged frame.

    Applied AFTER concat so it runs once per param rather than once per batch.
    The source column is kept -- it is declared provenance, not a temporary.
    """
    for out_col, spec in (derived_columns or {}).items():
        src, tname = spec["from"], spec["transform"]
        if tname not in _TRANSFORMS:
            raise ValueError(
                f"`{tname}` is not a known transform for derived column '{out_col}'. "
                f"Known: {sorted(_TRANSFORMS)}."
            )
        if src not in df.columns:
            raise ValueError(
                f"derived column '{out_col}' reads '{src}', which is not in the merged "
                f"frame (columns: {sorted(df.columns)})."
            )
        df[out_col] = df[src].astype(float).apply(_TRANSFORMS[tname])
    return df
```

Call it in `run_merge` immediately after the per-batch concat and before the CSV write, passing `config.get("derived_columns")`.

- [ ] **Step 4: Verify it compiles**

Run: `pixi run --as-is python -m py_compile src/gfv2_params/zonal_runners/merge.py`
Expected: exits 0. (Do not import it locally — `zonal_runners/__init__.py` pulls the geo stack.)

- [ ] **Step 5: Re-run the slope merge — compute node. No zonal re-run needed.**

```bash
srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G \
  pixi run --as-is python scripts/derive_zonal_params.py \
    --config configs/zonal/zonal_params.yml --base_config configs/base_config.yml \
    --fabric gfv2 --mode merge --param slope
```
Expected: `nhm_slope_params.csv` gains an `hru_slope` column. Verify:
`head -2 $(pixi run data-root)/gfv2/params/merged/nhm_slope_params.csv` — header ends `...,sum,hru_slope`, and row 1's `hru_slope` ≈ 0.0774 against `mean` ≈ 4.4252.

- [ ] **Step 6: Docs check + commit**

Update `docs/parameter_index.md`'s slope row from "conversion required" to "emitted".

```bash
git add configs/zonal/zonal_params.yml src/gfv2_params/zonal_runners/merge.py \
        tests/test_params_index.py docs/parameter_index.md
git commit -m "feat(zonal): emit hru_slope in rise/run via derived_columns:

slope.vrt is rd.TerrainAttribute(..., 'slope_degrees'), so nhm_slope_params.csv's
`mean` is DEGREES while PRMS hru_slope is a decimal fraction rise/run
(TM6B9:536) -- ~57x for small angles. gfv2 HRU 1: mean=4.4252, hru_slope=0.0774.

Applied in run_merge, so no zonal re-run is needed. Reuses
raster_ops.deg_to_fraction, which ssflux.py:63 already applies, so this makes
the rest of the pipeline consistent with what ssflux assumed rather than
introducing a new convention.

Transforms are a whitelist, not getattr(raster_ops, name): a typo must raise."
git push
```

---

### Task 8: `build_parameter_index.py` — generate the index

**Files:**
- Create: `scripts/build_parameter_index.py`
- Modify: `docs/parameter_index.md` (replaced by generated output)
- Modify: `tests/test_params_index.py`

**Interfaces:**
- Consumes: `load_declared_params()`, and a new `params_for_process(process: str) -> list[tuple[str, DeclaredParam]]` added to `params_index.py`.

- [ ] **Step 1: Add `params_for_process` to `src/gfv2_params/params_index.py`**

```python
def params_for_process(process: str, declared=None) -> list[tuple[str, DeclaredParam]]:
    """Every (emitted_column, DeclaredParam) whose column feeds `process`.

    Column-grained, not entry-grained: ssflux alone spans PRMSSoilzone,
    PRMSGroundwater and PRMSRunoff across different columns, so returning whole
    entries would report 5 non-runoff parameters as feeding runoff.
    """
    out = []
    for d in declared if declared is not None else load_declared_params():
        for col, spec in (d.prms.get("columns") or {}).items():
            if process in (spec.get("processes") or []):
                out.append((col, d))
    return out
```

- [ ] **Step 2: Write the failing test**

```python
def test_params_for_process_is_column_grained_not_entry_grained():
    """ssflux spans three processes; asking for PRMSRunoff must return only its
    two runoff columns, not the whole 10-column file."""
    hits = pi.params_for_process("PRMSRunoff")
    ssflux_cols = {col for col, d in hits if d.name == "ssflux"}
    assert ssflux_cols == {"dprst_seep_rate_open", "dprst_flow_coef"}
    assert "gwflow_coef" not in ssflux_cols  # PRMSGroundwater
    assert "soil2gw_max" not in ssflux_cols  # PRMSSoilzone


def test_prms_runoff_has_the_expected_parameter_count():
    hits = pi.params_for_process("PRMSRunoff")
    assert len({spec_col for spec_col, _ in hits}) == 11
```

- [ ] **Step 3: Verify (login-node safe)**

Run: `pixi run --as-is python -c "
from gfv2_params.params_index import params_for_process
h = params_for_process('PRMSRunoff')
print(len(h)); print(sorted(c for c, _ in h))"`
Expected: `11` and the sorted column list matching the spec's Deliverable 1 PRMSRunoff rows.

- [ ] **Step 4: Write `scripts/build_parameter_index.py`**

Render three sections whose `##` headings match Task 1's exactly (`## By PRMS process`, `## By config entry`, `## By builder`), plus the `## Reading this index` and `## Known gaps` preamble/postamble carried from Task 1's hand-written version. Emit the same table columns Task 1 used. Write to `docs/parameter_index.md`.

Builder attribution is not in the configs, so add a `builder:` key to each `prms:` block in the same commit (e.g. `builder: zonal_runners/soils.py`) rather than hardcoding a lookup table in the script.

- [ ] **Step 5: Regenerate and diff against the hand-written index**

Run: `pixi run --as-is python scripts/build_parameter_index.py && git diff --stat docs/parameter_index.md`
Expected: differences are formatting only. **Any row that changes meaning is a real finding** — reconcile before committing, because Task 1's version was hydrologist-reviewed and the generator's was not.

- [ ] **Step 6: Verify the docs still build**

Run: `pixi run -e docs docs-build`
Expected: exits 0, no broken-link warnings.

- [ ] **Step 7: Commit and push**

```bash
git add scripts/build_parameter_index.py src/gfv2_params/params_index.py \
        configs/ docs/parameter_index.md tests/test_params_index.py
git commit -m "feat(docs): generate the parameter index from configs/

params_for_process is column-grained: ssflux spans PRMSSoilzone,
PRMSGroundwater and PRMSRunoff across different columns, so an entry-grained
query would report 5 non-runoff parameters as feeding runoff.

The generated output is diffed against the hand-written, hydrologist-reviewed
index from PR 1; only formatting may differ."
git push
```

---

## Self-Review

**Spec coverage.** Design A (`prms:` metadata) → Task 4. Design B (`params_index.py`, `DeclaredParam` widening, the six affected test sites) → Task 3. Design C (two guards) → Tasks 4 and 5. Design D (`constants:`/`op_flow_thres`) → Task 6. Design E (generated index) → Tasks 1 and 8. Design F (`derived_columns:`/`hru_slope`) → Task 7. Deliverable 1 → Task 1. D0b is scoped out to issue #201 and appears only as the DEFECTIVE marking in Tasks 1 and 4 — intentional, per the spec's "Scoped out" section.

**Task 2 is not from the spec.** It closes a live defect found while answering "what files must I modify to add a param?": the submit wrappers' hardcoded arrays are undocumented and unguarded, and `ADDING_A_PARAMETER.md:239-241` asserts the opposite of the truth. Independent of the index work, near-zero risk, and deleted when the Snakemake migration retires the wrappers — so it rides in PR 1 rather than waiting.

**Gap found and closed while reviewing:** Task 8 needs builder attribution to render the "By builder" view, but that data lives in no config. Added a `builder:` key to Task 8 Step 4 rather than leaving the generator with a hardcoded lookup that would drift.

**Placeholder scan.** No TBD/TODO. Every code step carries real code. Task 1 Step 1 cites the spec's Deliverable 1 for row data rather than repeating 19 rows — that data exists and is committed, so it is a citation, not a placeholder; the section skeleton and the three rows needing special wording are given in full.

**Type consistency.** `DeclaredParam` is 5-field with `prms: dict = {}` throughout. `iter_declared_params(zonal_cfg, depstor_cfg, snarea_cfg=None)`, `load_declared_params()`, `params_for_process(process, declared=None)`, `apply_derived_columns(df, derived_columns)` are used with those exact signatures in every task that references them. `prms.columns` values are `{prms: str, processes: list[str]}` in Tasks 4, 6, 7 and read that way in Task 8.

**Adaptation to this repo's constraints.** The skill's standard "run the test, watch it fail" cycle cannot run locally — CLAUDE.md forbids pytest on the head node. Every failing-test step is therefore either an import-free one-liner (pure YAML/`py_compile`) or an explicit `srun` on a compute node, with CI as the authoritative gate. This is stated in Global Constraints and applied per-step.
