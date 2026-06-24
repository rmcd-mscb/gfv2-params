# Snakemake Spike — tjc Stage 4 + SLURM Env Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove a Snakemake-driven Part 2 zonal pipeline runs end-to-end on SLURM under our pixi env and reproduces the existing tjc outputs numerically, retiring the two biggest unknowns (pixi-env-under-the-slurm-plugin, and output parity) before committing to a full refactor.

**Architecture:** A throwaway, fully non-destructive spike under `workflow/spike/`. A `Snakefile` expresses the Stage-4 zonal DAG declaratively (per-batch zonal → merge, plus the `ssflux` cross-dependency on both `build_weights` and the merged `slope` CSV). Rules shell out to the **existing** `scripts/derive_zonal_params.py` orchestrator via `pixi run --as-is` (frozen default env, race-safe — the proven activation path), so no science code changes. A spike-only zonal config redirects all writes to `{data_root}/tjc/params_snakemake_spike/`, leaving the golden `{data_root}/tjc/params/merged/` outputs untouched for parity comparison. The Snakemake controller runs in a new `workflow` pixi env; the slurm executor plugin submits each rule as its own job.

**Tech Stack:** Snakemake ≥8, `snakemake-executor-plugin-slurm`, pixi, SLURM, pandas (parity check). Target fabric: **tjc** (VPU 12, 1584 HRUs, single batch).

## Global Constraints

- **SLURM jobs run geo work via `pixi run --as-is`** (= `--no-install --frozen`); never a flow that mutates the env per task. `pixi` must be on `PATH` at submit time (login shell with `~/.pixi/bin`). Copied verbatim from CLAUDE.md.
- **Never run `pytest` or any geo-importing Python on the HPC login/head node.** `snakemake --dry-run` and the pure-pandas parity check (no rasterio/GDAL import) ARE login-node safe. Actual rule execution happens only inside SLURM jobs.
- **Paths come from the active fabric profile** in `configs/base_config.yml` via `{data_root}`/`{fabric}` placeholders — never hardcoded literals in configs. The spike config follows this.
- **SLURM account/partition:** `-A impd`, `-p cpu` (matches every existing `.batch` file).
- **tjc identity (do not change):** `id_feature: model_hru_idx`, `expected_max_hru_id: 1584`, `n_batches: 1` (batch `0000`).
- **Atomic commits;** every code change needs a docs check (audit `docs/`, `README.md`, `slurm_batch/RUNME.md` + `HPC_REFERENCE.md`). For this spike the doc deliverable is the findings note in Task 7.
- **This is a spike:** all artifacts live under `workflow/spike/` and a new `params_snakemake_spike/` output tree. Nothing under `configs/`, `scripts/`, `src/`, or `slurm_batch/` is modified except the additive pixi env in `pyproject.toml`.

---

## File Structure

- `pyproject.toml` — **modify**: add a `workflow` pixi feature (snakemake + slurm executor plugin) and a `workflow` environment. Additive only.
- `workflow/spike/zonal_params.spike.yml` — **create**: a copy of the 3 relevant param entries from `configs/zonal/zonal_params.yml` with `output_dir`/`weight_dir`/`merged_slope_file` redirected to the spike tree.
- `workflow/spike/Snakefile` — **create**: the Stage-4 zonal DAG (rules: `zonal_batch`, `merge_param`, `build_weights`, `ssflux_batch`, `all`).
- `workflow/spike/profile/config.yaml` — **create**: snakemake v8 workflow profile selecting the slurm executor + default resources (account/partition).
- `tests/test_snakemake_spike_parity.py` — **create**: pure-pandas parity check, spike merged CSVs vs golden merged CSVs.

Golden reference (already on disk, read-only): `{data_root}/tjc/params/merged/nhm_{elevation,slope,ssflux}_params.csv` where `{data_root}` = `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`.

---

### Task 1: Add the `workflow` pixi environment

**Files:**
- Modify: `pyproject.toml` (`[tool.pixi.feature.*]` + `[tool.pixi.environments]`)

**Interfaces:**
- Produces: a `workflow` pixi env containing the full default geo stack (the top-level `[tool.pixi.dependencies]` is implicitly in every env) **plus** `snakemake` and `snakemake-executor-plugin-slurm`. Invoked as `pixi run -e workflow snakemake ...`.

- [ ] **Step 1: Add the feature block**

Add after the existing `[tool.pixi.feature.dev.dependencies]` block in `pyproject.toml`:

```toml
[tool.pixi.feature.workflow.dependencies]
# Snakemake spike controller (docs/superpowers/plans/2026-06-23-snakemake-spike-tjc-stage4.md).
# Both are on conda-forge. Kept in a feature (like dev/docs), NOT in
# [project.dependencies] — this is orchestration tooling, not a runtime dep.
snakemake = ">=8"
snakemake-executor-plugin-slurm = "*"
```

- [ ] **Step 2: Register the environment**

In `[tool.pixi.environments]`, add the `workflow` line alongside the others:

```toml
workflow = { features = ["workflow"], solve-group = "default" }
```

- [ ] **Step 3: Materialise the env**

Run: `pixi install`
Expected: solves and writes `.pixi/envs/workflow/`; updates `pixi.lock`. (Login-node safe — this is a solve, not a geo import.)

- [ ] **Step 4: Verify snakemake + the slurm plugin are importable**

Run: `pixi run -e workflow snakemake --version`
Expected: prints a version `8.x` (or higher) with no error.

Run: `pixi run -e workflow python -c "import snakemake_executor_plugin_slurm; print('slurm plugin ok')"`
Expected: `slurm plugin ok`

- [ ] **Step 5: Commit**

```bash
git checkout -b spike/snakemake-tjc-stage4
git add pyproject.toml pixi.lock
git commit -m "chore(spike): add workflow pixi env (snakemake + slurm executor)"
```

---

### Task 2: Spike zonal config + SLURM profile

**Files:**
- Create: `workflow/spike/zonal_params.spike.yml`
- Create: `workflow/spike/profile/config.yaml`

**Interfaces:**
- Produces: a zonal config consumed by `scripts/derive_zonal_params.py --config workflow/spike/zonal_params.spike.yml`. Output tree resolves to `{data_root}/tjc/params_snakemake_spike/`. Defines exactly three params: `elevation`, `slope`, `ssflux`.
- Produces: a snakemake workflow profile directory `workflow/spike/profile/` selecting `executor: slurm` with `slurm_account: impd`, `slurm_partition: cpu`.

- [ ] **Step 1: Create the spike zonal config**

Create `workflow/spike/zonal_params.spike.yml` (mirrors `configs/zonal/zonal_params.yml`, but `output_dir`/`weight_dir`/`merged_slope_file` redirect to the spike tree so golden outputs are never touched):

```yaml
# THROWAWAY spike config — Stage-4 zonal subset for the Snakemake tjc spike.
# Identical to the matching entries in configs/zonal/zonal_params.yml EXCEPT
# every write target is redirected under params_snakemake_spike/ so the golden
# {data_root}/tjc/params/merged/ outputs stay intact for parity comparison.
# id_feature / hru_gpkg / expected_max_hru_id come from the tjc profile in
# configs/base_config.yml (injected by load_config), same as production.
defaults:
  batch_dir:     "{data_root}/{fabric}/batches"          # golden batches, read-only
  target_layer:  nhru
  output_dir:    "{data_root}/{fabric}/params_snakemake_spike"
  merged_subdir: merged
  weight_dir:    "{data_root}/{fabric}/params_snakemake_spike/weights"

params:
  - name: elevation
    script: zonal
    source_raster: "{data_root}/shared/conus/vrt/elevation.vrt"
    categorical:   false
    merged_file:   nhm_elevation_params.csv

  - name: slope
    script: zonal
    source_raster: "{data_root}/shared/conus/vrt/slope.vrt"
    categorical:   false
    merged_file:   nhm_slope_params.csv

  - name: ssflux
    script: ssflux
    depends_on:    build_weights
    source_shapefile:  "{data_root}/input/soils_litho/Lithology_exp_Konly_Project.shp"
    merged_slope_file: "{data_root}/{fabric}/params_snakemake_spike/merged/nhm_slope_params.csv"
    merged_file:       nhm_ssflux_params.csv
    k_perm_min: -16.48
    flux_params:
      - {name: soil2gw_max,          min: 0.1,   max: 0.3}
      - {name: ssr2gw_rate,          min: 0.3,   max: 0.7}
      - {name: fastcoef_lin,         min: 0.01,  max: 0.6}
      - {name: slowcoef_lin,         min: 0.005, max: 0.3}
      - {name: gwflow_coef,          min: 0.005, max: 0.3}
      - {name: dprst_seep_rate_open, min: 0.005, max: 0.2}
      - {name: dprst_flow_coef,      min: 0.005, max: 0.5}
```

- [ ] **Step 2: Verify the orchestrator accepts the spike config (no execution)**

Run:
```bash
pixi run --as-is python scripts/derive_zonal_params.py \
    --config workflow/spike/zonal_params.spike.yml \
    --base_config configs/base_config.yml --fabric tjc \
    --mode zonal --param elevation --batch_id 0 --help
```
Expected: argparse help prints with exit 0 (confirms the script imports and the args are valid). This does NOT run zonal work, so it is login-node safe.

- [ ] **Step 3: Create the snakemake workflow profile**

Create `workflow/spike/profile/config.yaml`:

```yaml
# Snakemake v8 workflow profile for the tjc Stage-4 spike.
# Selects the SLURM executor and sets cluster-wide defaults. Per-rule mem_mb /
# runtime / cpus_per_task come from each rule's `resources:` in the Snakefile.
executor: slurm
jobs: 8                      # max concurrent SLURM jobs (tjc is tiny)
printshellcmds: True
default-resources:
  slurm_account: impd
  slurm_partition: cpu
  mem_mb: 8000
  runtime: 60               # minutes
```

- [ ] **Step 4: Commit**

```bash
git add workflow/spike/zonal_params.spike.yml workflow/spike/profile/config.yaml
git commit -m "feat(spike): spike zonal config + snakemake slurm profile"
```

---

### Task 3: Write the Snakefile and verify the DAG (login-node safe)

**Files:**
- Create: `workflow/spike/Snakefile`

**Interfaces:**
- Consumes: `configs/base_config.yml` (for `data_root`), `workflow/spike/zonal_params.spike.yml` (param list), `{data_root}/tjc/batches/manifest.yml` (`n_batches`).
- Produces: targets `{OUTDIR}/merged/nhm_{elevation,slope,ssflux}_params.csv` where `OUTDIR = {data_root}/tjc/params_snakemake_spike`. The DAG encodes: `build_weights` + `merge_param(slope)` → `ssflux_batch` → `merge_param(ssflux)`; `elevation`/`slope` fan trivially through `zonal_batch` → `merge_param`.

- [ ] **Step 1: Write the Snakefile**

Create `workflow/spike/Snakefile`:

```python
# Snakemake spike: tjc Stage 4 (zonal params) over SLURM.
# Rules shell out to the existing orchestrator via `pixi run --as-is` so all
# geo work runs in the frozen default env (race-safe; CLAUDE.md constraint).
# See docs/superpowers/plans/2026-06-23-snakemake-spike-tjc-stage4.md.
import yaml

FABRIC = "tjc"
BASE_CONFIG = "configs/base_config.yml"
ZONAL_CONFIG = "workflow/spike/zonal_params.spike.yml"

DATA_ROOT = yaml.safe_load(open(BASE_CONFIG))["data_root"]
PARAMS = {p["name"]: p for p in yaml.safe_load(open(ZONAL_CONFIG))["params"]}
OUTDIR = f"{DATA_ROOT}/{FABRIC}/params_snakemake_spike"

_manifest = yaml.safe_load(open(f"{DATA_ROOT}/{FABRIC}/batches/manifest.yml"))
BATCHES = [f"{i:04d}" for i in range(_manifest["n_batches"])]

# Shell prefix: frozen default env via pixi --as-is (NOT the workflow env).
DERIVE = (
    f"pixi run --as-is python scripts/derive_zonal_params.py "
    f"--config {ZONAL_CONFIG} --base_config {BASE_CONFIG} --fabric {FABRIC}"
)

# Per-batch CSV name written by zonal_runners (see merge.py glob):
#   base_nhm_<param>_<fabric>_batch_<NNNN>_param.csv  under  <OUTDIR>/<param>/
def batch_csv(param):
    return f"{OUTDIR}/{param}/base_nhm_{param}_{FABRIC}_batch_{{batch}}_param.csv"

# merged_file is nhm_<param>_params.csv for all three params (matches the
# `merged_file:` values in the spike config and the golden output names).


rule all:
    input:
        [f"{OUTDIR}/merged/{PARAMS[p]['merged_file']}" for p in PARAMS]


rule zonal_batch:
    output:
        batch_csv("{param}")
    wildcard_constraints:
        param="elevation|slope",        # ssflux has its own rule
        batch=r"\d{4}",
    resources:
        mem_mb=8000, runtime=60, cpus_per_task=2,
    shell:
        DERIVE + " --mode zonal --param {wildcards.param} --batch_id {wildcards.batch}"


rule build_weights:
    output:
        f"{OUTDIR}/weights/lith_weights_{FABRIC}.csv"
    resources:
        mem_mb=16000, runtime=30, cpus_per_task=2,
    shell:
        DERIVE + " --mode build_weights"


rule ssflux_batch:
    input:
        weights=f"{OUTDIR}/weights/lith_weights_{FABRIC}.csv",
        slope=f"{OUTDIR}/merged/nhm_slope_params.csv",
    output:
        f"{OUTDIR}/ssflux/base_nhm_ssflux_{FABRIC}_batch_{{batch}}_param.csv"
    wildcard_constraints:
        batch=r"\d{4}",
    resources:
        mem_mb=8000, runtime=60, cpus_per_task=2,
    shell:
        DERIVE + " --mode zonal --param ssflux --batch_id {wildcards.batch}"


rule merge_param:
    input:
        lambda w: expand(batch_csv(w.param), batch=BATCHES)
    output:
        f"{OUTDIR}/merged/nhm_{{param}}_params.csv"
    wildcard_constraints:
        param="elevation|slope|ssflux",
    resources:
        mem_mb=8000, runtime=30, cpus_per_task=2,
    shell:
        DERIVE + " --mode merge --param {wildcards.param}"
```

- [ ] **Step 2: Dry-run to verify the DAG resolves**

Run (login-node safe — snakemake imports are light, no geo libs, no jobs submitted):
```bash
pixi run -e workflow snakemake -s workflow/spike/Snakefile -n -p
```
Expected: a job summary table listing counts — `all 1`, `build_weights 1`, `merge_param 3`, `ssflux_batch 1`, `zonal_batch 2` → **total 8 jobs**. No `MissingInputException`, no `AmbiguousRuleException`.

- [ ] **Step 3: Verify the cross-dependency edges**

Run:
```bash
pixi run -e workflow snakemake -s workflow/spike/Snakefile --dag 2>/dev/null | grep -E "ssflux_batch|build_weights|merge_param" | head
```
Expected: the DAG dot output shows `ssflux_batch` with incoming edges from `build_weights` and from `merge_param` (the slope merge). This confirms the chain `build_weights + merge(slope) → ssflux_batch → merge(ssflux)` is encoded by filenames alone.

- [ ] **Step 4: Commit**

```bash
git add workflow/spike/Snakefile
git commit -m "feat(spike): Snakefile for tjc Stage-4 zonal DAG (dry-run verified)"
```

---

### Task 4: Env/SLURM integration gate — execute ONE rule on a compute node

This is the highest-risk unknown: does a rule the slurm plugin submits find `pixi` on `PATH` and run geo work in the frozen default env? Prove it with a single rule before the full run.

**Files:** none (execution + verification only)

**Interfaces:**
- Consumes: the Snakefile and profile from Tasks 2–3.
- Produces: `{OUTDIR}/elevation/base_nhm_elevation_tjc_batch_0000_param.csv` (one per-batch CSV), written by a SLURM job.

- [ ] **Step 1: Submit a single target via the slurm executor**

Run from a login shell with `~/.pixi/bin` on `PATH`:
```bash
pixi run -e workflow snakemake -s workflow/spike/Snakefile \
    --workflow-profile workflow/spike/profile \
    "$(pixi run --as-is python -c 'import yaml; r=yaml.safe_load(open("configs/base_config.yml"))["data_root"]; print(f"{r}/tjc/params_snakemake_spike/elevation/base_nhm_elevation_tjc_batch_0000_param.csv")')"
```
Expected: snakemake submits 1 job (`zonal_batch`), waits, and reports `1 of 1 steps (100%) done`. (The shell substitution resolves the absolute target path so we run exactly one rule.)

- [ ] **Step 2: Confirm the output exists and is non-empty**

Run:
```bash
ls -l "$(pixi run --as-is python -c 'import yaml; r=yaml.safe_load(open("configs/base_config.yml"))["data_root"]; print(r)')/tjc/params_snakemake_spike/elevation/base_nhm_elevation_tjc_batch_0000_param.csv"
```
Expected: a CSV of non-zero size.

- [ ] **Step 3: If the job failed with `pixi: command not found`** (fallback path)

If the SLURM job log (`.snakemake/slurm_logs/...`) shows `pixi` not found, the plugin did not export `PATH`. Fix by making the shell prefix self-contained — edit `DERIVE` in `workflow/spike/Snakefile`:

```python
DERIVE = (
    f"$HOME/.pixi/bin/pixi run --as-is python scripts/derive_zonal_params.py "
    f"--config {ZONAL_CONFIG} --base_config {BASE_CONFIG} --fabric {FABRIC}"
)
```
Then re-run Step 1. (If Step 1 already passed, skip this step entirely.)

- [ ] **Step 4: Commit (only if the fallback edit was needed)**

```bash
git add workflow/spike/Snakefile
git commit -m "fix(spike): use absolute pixi path in rule shell (slurm PATH export)"
```

---

### Task 5: Full spike run — exercise the ssflux cross-dependency

**Files:** none (execution only)

**Interfaces:**
- Produces: all three merged CSVs under `{OUTDIR}/merged/` plus the weight matrix `{OUTDIR}/weights/lith_weights_tjc.csv`.

- [ ] **Step 1: Run the whole DAG to completion**

Run from a login shell with `~/.pixi/bin` on `PATH`:
```bash
pixi run -e workflow snakemake -s workflow/spike/Snakefile \
    --workflow-profile workflow/spike/profile all
```
Expected: snakemake submits and completes 8 jobs in dependency order, finishing with `8 of 8 steps (100%) done`. `ssflux_batch` must start only after both `build_weights` and the slope `merge_param` finish (visible in the scheduling order).

- [ ] **Step 2: Confirm all merged outputs exist**

Run:
```bash
DR=$(pixi run --as-is python -c 'import yaml; print(yaml.safe_load(open("configs/base_config.yml"))["data_root"])')
ls -l "$DR/tjc/params_snakemake_spike/merged/"
```
Expected: `nhm_elevation_params.csv`, `nhm_slope_params.csv`, `nhm_ssflux_params.csv`, all non-empty.

- [ ] **Step 3: Confirm resumability (skip-if-done)**

Run the same command from Step 1 again:
```bash
pixi run -e workflow snakemake -s workflow/spike/Snakefile \
    --workflow-profile workflow/spike/profile all
```
Expected: `Nothing to be done (all requested files are present and up to date)` — proving the file-target skip logic works (the headline win over the hand-rolled `afterok` bash).

---

### Task 6: Parity check vs golden outputs

**Files:**
- Create: `tests/test_snakemake_spike_parity.py`

**Interfaces:**
- Consumes: spike merged CSVs (`{data_root}/tjc/params_snakemake_spike/merged/`) and golden merged CSVs (`{data_root}/tjc/params/merged/`).
- Produces: a pass/fail parity verdict per param. Pure pandas — no geo imports — so it is login-node safe and runnable the moment Task 5 finishes.

- [ ] **Step 1: Write the failing parity test**

Create `tests/test_snakemake_spike_parity.py`:

```python
"""Parity: Snakemake-spike Stage-4 merged CSVs vs the golden tjc outputs.

Pure pandas (no rasterio/GDAL) so it is HPC-login-node safe. Skips cleanly if
the spike run (Task 5 of the snakemake spike plan) has not produced outputs.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

ID = "model_hru_idx"  # tjc id_feature (configs/base_config.yml)
PARAMS = ["nhm_elevation_params.csv", "nhm_slope_params.csv", "nhm_ssflux_params.csv"]

_DATA_ROOT = Path(yaml.safe_load(open("configs/base_config.yml"))["data_root"])
GOLDEN = _DATA_ROOT / "tjc" / "params" / "merged"
SPIKE = _DATA_ROOT / "tjc" / "params_snakemake_spike" / "merged"


@pytest.mark.parametrize("fname", PARAMS)
def test_spike_matches_golden(fname):
    golden_path, spike_path = GOLDEN / fname, SPIKE / fname
    if not spike_path.exists():
        pytest.skip(f"spike output not present yet: {spike_path}")
    assert golden_path.exists(), f"golden reference missing: {golden_path}"

    g = pd.read_csv(golden_path).sort_values(ID).reset_index(drop=True)
    s = pd.read_csv(spike_path).sort_values(ID).reset_index(drop=True)

    # Same HRU id set, same row count.
    assert list(g[ID]) == list(s[ID]), f"{fname}: HRU id sets differ"
    assert set(g.columns) == set(s.columns), f"{fname}: column sets differ"

    # Numeric columns equal within float tolerance; non-numeric exactly equal.
    for col in g.columns:
        if pd.api.types.is_numeric_dtype(g[col]):
            assert np.allclose(g[col].to_numpy(), s[col].to_numpy(), rtol=1e-6,
                               atol=1e-9, equal_nan=True), f"{fname}:{col} differs"
        else:
            assert g[col].equals(s[col]), f"{fname}:{col} (non-numeric) differs"
```

- [ ] **Step 2: Run the parity test** (login-node safe — pure pandas)

Run: `pixi run -e dev pytest tests/test_snakemake_spike_parity.py -v`
Expected: 3 PASSED (or SKIPPED if Step 5 outputs are absent — in which case re-run Task 5 first). A FAIL prints the exact `param:column` that diverged — that is a real finding, not a test defect; investigate before declaring the spike successful.

- [ ] **Step 3: Commit**

```bash
git add tests/test_snakemake_spike_parity.py
git commit -m "test(spike): parity check, snakemake tjc Stage-4 vs golden outputs"
```

---

### Task 7: Findings write-up + docs note

**Files:**
- Create: `docs/superpowers/plans/2026-06-23-snakemake-spike-findings.md`

**Interfaces:**
- Consumes: observed results from Tasks 4–6.
- Produces: a short findings note that converts the spike into a real per-stage cost estimate and a go/no-go on the full refactor.

- [ ] **Step 1: Write the findings note**

Create `docs/superpowers/plans/2026-06-23-snakemake-spike-findings.md` filling in the bracketed observations from the run:

```markdown
# Snakemake Spike — Findings (tjc Stage 4)

**Date:** 2026-06-23
**Scope:** Stage-4 zonal (elevation, slope, ssflux) on tjc via Snakemake + slurm executor plugin.

## Did it work?
- Env integration (Task 4): [pixi-on-PATH worked as-is | needed the $HOME/.pixi/bin fallback].
- Full DAG (Task 5): [8/8 jobs completed | notes]. ssflux correctly waited on build_weights + slope merge: [yes/no].
- Resumability (Task 5 Step 3): [Nothing to be done | notes].
- Parity (Task 6): [3/3 params byte/float-identical | list any diverging param:column and root cause].

## Effort actually spent
- Per-task wall-clock and any surprises: [...]

## Extrapolation to a full refactor
- Confirmed: the science code (zonal_runners) needed zero changes — rules just shell to derive_zonal_params.py.
- Open questions resolved / still open: [...]
- Revised per-stage estimate vs the original 8–15 day (Approach A) figure: [...]

## Recommendation
[Proceed with phased A→B refactor | adjust approach | blockers to resolve first]
```

- [ ] **Step 2: Audit docs for required updates**

Per CLAUDE.md, every code change needs a docs check. Confirm whether `slurm_batch/RUNME.md`, `HPC_REFERENCE.md`, or `README.md` need a pointer to the spike. For a throwaway spike the answer is usually "no production-doc change, findings note suffices" — record that conclusion explicitly in the findings note.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-06-23-snakemake-spike-findings.md
git commit -m "docs(spike): snakemake tjc Stage-4 findings + refactor go/no-go"
```

---

## Self-Review

**Spec coverage:** The spike's two goals — (1) env/SLURM-plugin integration and (2) output parity — map to Task 4 (single-rule SLURM execution) and Task 6 (parity test) respectively. The cross-dependency stress (`ssflux` needs `build_weights` + merged `slope`) is covered by the Snakefile's `ssflux_batch` rule and verified in Task 3 Step 3 (DAG edges) and Task 5 Step 1 (ordered execution). Non-destructiveness is guaranteed by the redirected `output_dir`/`weight_dir`/`merged_slope_file` in Task 2's spike config. Resumability (the headline benefit) is checked in Task 5 Step 3.

**Placeholder scan:** No "TBD"/"handle errors"/"similar to" placeholders. Every code block is complete (Snakefile, spike config, profile, parity test). The findings note (Task 7) intentionally contains bracketed `[observation]` slots — those are fields to fill from the run, not code placeholders.

**Type/name consistency:** Per-batch filename `base_nhm_<param>_<fabric>_batch_<NNNN>_param.csv` matches `merge.py`'s glob and `zonal.py`'s `file_prefix`. Weight file `lith_weights_<fabric>.csv` matches `weights.py` and `ssflux.py`. Merged name `nhm_<param>_params.csv` matches the spike config `merged_file:` values and the golden filenames listed in File Structure. `id_feature` is `model_hru_idx` everywhere (parity test, tjc profile). `OUTDIR` is computed identically in the Snakefile and the spike config (`{data_root}/tjc/params_snakemake_spike`).
