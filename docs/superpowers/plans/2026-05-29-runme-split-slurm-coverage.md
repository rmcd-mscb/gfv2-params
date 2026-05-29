# RUNME runbook/reference split + SLURM coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `slurm_batch/RUNME.md` into a lean hydrologist runbook + a co-located HPC reference, and add SLURM wrappers for the compute/memory-intensive steps currently run on the login node.

**Architecture:** Four new `.batch` wrappers around existing scripts (no script edits); a new `slurm_batch/HPC_REFERENCE.md` that retains the detailed stage taxonomy and relocated (trimmed) rationale; a rewritten `RUNME.md` that is a single linear CONUS-gfv2 command sequence; and cross-reference updates in the live docs that point at RUNME.

**Tech Stack:** Bash/SLURM batch scripts, Markdown, pixi. No Python changes. Verification is `bash -n`, `sbatch --test-only`, `--help` arg checks, and `grep` audits — there are no unit tests for batch/doc files.

**Spec:** [`docs/superpowers/specs/2026-05-29-runme-split-and-slurm-coverage-design.md`](../specs/2026-05-29-runme-split-and-slurm-coverage-design.md) (issue #131)

## File structure

- Create: `slurm_batch/prepare_fabric.batch`, `slurm_batch/merge_and_fill_params.batch`, `slurm_batch/merge_vpu_segments.batch`, `slurm_batch/render_figures.batch`
- Create: `slurm_batch/HPC_REFERENCE.md`
- Rewrite: `slurm_batch/RUNME.md`
- Modify (cross-refs): `CLAUDE.md`, `README.md`, `docs/ARCHITECTURE.md`, `docs/depstor_workflow.md`, `docs/ADDING_A_PARAMETER.md`, `docs/hpc-workflow.md`
- Leave untouched (archival point-in-time records): everything under `docs/superpowers/specs/` and `docs/superpowers/plans/`.

---

## Task 1: Add the four SLURM wrappers

**Files:**
- Create: `slurm_batch/prepare_fabric.batch`
- Create: `slurm_batch/merge_and_fill_params.batch`
- Create: `slurm_batch/merge_vpu_segments.batch`
- Create: `slurm_batch/render_figures.batch`

These wrap existing scripts unchanged, mirroring the convention in
`slurm_batch/build_depstor_rasters.batch` (`cd "$SLURM_SUBMIT_DIR"`,
`BASE_CONFIG`/`FABRIC` env defaults, `pixi run --as-is`, forward `"$@"`).

- [ ] **Step 1: Create `slurm_batch/prepare_fabric.batch`**

```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=prepare_fabric
#SBATCH --output=logs/job_%j.out
#SBATCH --error=logs/job_%j.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#
# Spatially batch a merged fabric into per-batch geopackages (KD-tree recursive
# bisection) + a manifest. Loads the whole CONUS fabric into memory, so it runs
# as a SLURM job rather than on the login node. Override FABRIC for other
# fabrics:  FABRIC=oregon sbatch slurm_batch/prepare_fabric.batch
# --mem/--time are starting points; right-size from `sacct -j <id> -o MaxRSS`.
cd "$SLURM_SUBMIT_DIR"
BASE_CONFIG=${BASE_CONFIG:-configs/base_config.yml}
FABRIC=${FABRIC:-gfv2}

pixi run --as-is python scripts/prepare_fabric.py \
    --fabric "$FABRIC" \
    --base_config "$BASE_CONFIG" \
    "$@"
```

- [ ] **Step 2: Create `slurm_batch/merge_and_fill_params.batch`**

```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=merge_and_fill_params
#SBATCH --output=logs/job_%j.out
#SBATCH --error=logs/job_%j.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#
# KNN gap-fill of missing parameter values against the fabric (Stage 7). Fits
# sklearn NearestNeighbors over every HRU and fills all param columns, so it
# runs as a SLURM job rather than on the login node. The fabric is read from
# base_config's default_fabric (the script takes no --fabric).
# --mem/--time are starting points; right-size from `sacct -j <id> -o MaxRSS`.
cd "$SLURM_SUBMIT_DIR"
BASE_CONFIG=${BASE_CONFIG:-configs/base_config.yml}

pixi run --as-is python scripts/merge_and_fill_params.py \
    --base_config "$BASE_CONFIG" \
    "$@"
```

- [ ] **Step 3: Create `slurm_batch/merge_vpu_segments.batch`**

```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=merge_vpu_segments
#SBATCH --output=logs/job_%j.out
#SBATCH --error=logs/job_%j.err
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#
# Merge the per-VPU `nsegment` layers into one CONUS stream-segments gpkg for the
# depstor `streambuffer` step (VPU-based fabrics only). Concatenates 21 VPU
# geometry layers in memory, so it runs as a SLURM job rather than on the login
# node.  FABRIC=gfv2 sbatch slurm_batch/merge_vpu_segments.batch
cd "$SLURM_SUBMIT_DIR"
BASE_CONFIG=${BASE_CONFIG:-configs/base_config.yml}
FABRIC=${FABRIC:-gfv2}

pixi run --as-is python scripts/merge_vpu_segments.py \
    --fabric "$FABRIC" \
    --base_config "$BASE_CONFIG" \
    "$@"
```

- [ ] **Step 4: Create `slurm_batch/render_figures.batch`**

```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=render_figures
#SBATCH --output=logs/job_%j.out
#SBATCH --error=logs/job_%j.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=96G
#
# Headlessly (re)generate the fabric_results figures (nbconvert --execute). Loads
# the full HRU fabric (~361k polygons for CONUS gfv2), so it runs as a SLURM job
# rather than on the login node. Uses the `notebooks` pixi env (jupyter/nbconvert);
# the script already forces MPLBACKEND=Agg.
#   FABRIC=oregon sbatch slurm_batch/render_figures.batch
# --mem/--time are starting points; right-size from `sacct -j <id> -o MaxRSS`.
cd "$SLURM_SUBMIT_DIR"
FABRIC=${FABRIC:-gfv2}

pixi run --as-is -e notebooks python scripts/render_figures.py \
    --fabric "$FABRIC" \
    "$@"
```

- [ ] **Step 5: Syntax-check all four batch files**

Run:
```bash
for f in prepare_fabric merge_and_fill_params merge_vpu_segments render_figures; do
  bash -n slurm_batch/$f.batch && echo "$f: syntax OK"
done
```
Expected: four `… syntax OK` lines, no errors.

- [ ] **Step 6: Verify each wrapped script accepts the flags the batch passes**

Run (argparse `--help` exits 0 and lists the flags; quick, login-safe single imports):
```bash
pixi run --as-is python scripts/prepare_fabric.py --help | grep -E -- "--fabric|--base_config"
pixi run --as-is python scripts/merge_and_fill_params.py --help | grep -E -- "--base_config"
pixi run --as-is python scripts/merge_vpu_segments.py --help | grep -E -- "--fabric|--base_config"
pixi run --as-is -e notebooks python scripts/render_figures.py --help | grep -E -- "--fabric"
```
Expected: each `grep` prints the matching option line(s). If `merge_and_fill_params.py` shows a `--fabric` you did not expect, leave the batch as-is (it correctly passes only `--base_config`).

- [ ] **Step 7: Validate the SBATCH directives without running**

Run:
```bash
for f in prepare_fabric merge_and_fill_params merge_vpu_segments render_figures; do
  sbatch --test-only slurm_batch/$f.batch 2>&1 | sed "s/^/$f: /"
done
```
Expected: each prints a `sbatch: Job N to start at …` line (directives parse; nothing is submitted).

- [ ] **Step 8: Commit**

```bash
git add slurm_batch/prepare_fabric.batch slurm_batch/merge_and_fill_params.batch \
        slurm_batch/merge_vpu_segments.batch slurm_batch/render_figures.batch
git commit -m "feat(slurm): wrappers for prepare_fabric, gap-fill, segment-merge, render (#131)

Add batch wrappers so the compute/memory-intensive steps RUNME previously
told users to run on the login node now go through SLURM:
prepare_fabric (CONUS fabric + KD-tree), merge_and_fill_params (KNN gap-fill),
merge_vpu_segments (CONUS geometry concat), render_figures (notebooks env).
Wrappers only; the scripts are unchanged.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Write `slurm_batch/HPC_REFERENCE.md`

**Files:**
- Create: `slurm_batch/HPC_REFERENCE.md`

The reference is the new home for everything the runbook will not carry. It
**retains the detailed stage taxonomy** (Stage 0, 1, 1b, 1c1, 1c2, 2a, 2a',
2b, 2c, 2d, 3, 4A, 4B, 5–9) so existing "RUNME Stage N" references in other
docs resolve here after Task 4 repoints them. Rationale is trimmed to brief
notes; PR/issue numbers are dropped (git history records them).

- [ ] **Step 1: Write the reference document**

Create `slurm_batch/HPC_REFERENCE.md` with these top-level sections, populated by
**relocating and trimming** the corresponding content from the current
`RUNME.md` (use `git show HEAD:slurm_batch/RUNME.md` to read the pre-rewrite
source while writing this — do Task 2 before Task 3 so the source is intact):

1. **Title + intro** — "Reference detail for the GFv2 HPC pipeline. The
   step-by-step happy path is in [RUNME.md](RUNME.md); this file holds the
   environment internals, per-stage detail, alternate execution paths,
   per-fabric instructions, recovery, and the script→config map."
2. **Environment internals** — from current RUNME §Prerequisites: the
   `pixi run --as-is` (= `--no-install --frozen`) rationale and the
   `~/.pixi/bin`-on-PATH-at-submit requirement, trimmed to ~4 sentences; the
   `pixi shell -e …` interactive envs; the `geoenv` deprecation note.
3. **Array concurrency throttle** — from current RUNME §Selecting a fabric: the
   `SUBMIT_JOBS_MAX_CONCURRENT` / 5th-positional cap, why it exists (shared-FS
   geo-import contention), one paragraph.
4. **Data directory layout** — the full tree from current RUNME §Data Directory
   Layout, plus the `migrate_to_shared_layout.py` legacy-upgrade note.
5. **Selecting / running other fabrics** — `--fabric` / `FABRIC` / `default_fabric`
   precedence; `gfv2_vpu01` and `oregon` resource overrides.
6. **Part 1 stage detail** — the per-stage narrative for Stages 0, 1, 1b, 1c1,
   1c2, 2a, 2a', 2b, 2c (keep the `### Stage N — <name>` headers verbatim so
   anchors resolve), each trimmed to what it does + what it depends on + the
   exact `sbatch`/orchestrator command. Heavy single-step rebuilds shown via
   `sbatch slurm_batch/build_shared_rasters.batch --step <name>` (NOT login
   `pixi run`); note only genuinely quick inspection steps may be run with
   `pixi run … --step` on the login node.
7. **Stage 2d depstor detail** — template/fdr clip, `vpu_id`, `carea_map`
   threshold modes + TWI source pairing; the routing memory note (~80 GB
   measured, `--mem=96G`).
8. **Stage 3 fabric prep detail** — `merge_vpu_targets` (marimo) documented as
   **compute-node only** (JupyterHub or `salloc`, never login); `merge_vpu_segments`
   and `prepare_fabric` via their new batches (Task 1).
9. **Stage 4A — incremental per-parameter runs** — the full by-parameter
   zonal + depstor-fraction recipes (keep the `Stage 4A` label).
10. **Stage 4B — wholesale wrappers** — `submit_zonal_params.sh` /
    `submit_depstor_params.sh` detail; depstor output layout (merged vs
    _intermediates).
11. **Stages 5–9** — merge/validate, ssflux, KNN gap-fill (via
    `merge_and_fill_params.batch`), NHM defaults (via
    `merge_default_output_params.batch`), view results (via
    `render_figures.batch`, with JupyterHub as the interactive alternative).
12. **Adding a new fabric** — Case A (pre-merged) / Case B (per-VPU), trimmed.
13. **Partial reruns & recovery** — single-batch rerun; refill-a-VPU recipe.
14. **Monitoring** — `squeue` / `sacct` / `tail`.
15. **Script → config → entry-point map** — the existing table, **plus rows for
    the four new batches** from Task 1.

Content rules: drop every `PR #NN` / `issue #NN` / "the legacy X was dropped"
clause; convert "(resolved by PR #95)" style asides to plain present tense; keep
every command exact.

- [ ] **Step 2: Verify every command/file the reference names exists**

Run:
```bash
# all .batch / .sh / .py tokens referenced should exist
grep -oE "slurm_batch/[A-Za-z0-9_]+\.(batch|sh)|scripts/[A-Za-z0-9_]+\.py" slurm_batch/HPC_REFERENCE.md \
  | sort -u | while read -r p; do [ -e "$p" ] && echo "OK $p" || echo "MISSING $p"; done
```
Expected: every line starts `OK` (no `MISSING`). The four new batches resolve because Task 1 created them.

- [ ] **Step 3: Confirm no PR/issue archaeology leaked in**

Run: `grep -nE "PR #[0-9]+|issue #[0-9]+|#[0-9]{2,}" slurm_batch/HPC_REFERENCE.md`
Expected: no output (archaeology trimmed). If a hit is a genuine cross-link you intend to keep, leave it and note why.

- [ ] **Step 4: Commit**

```bash
git add slurm_batch/HPC_REFERENCE.md
git commit -m "docs(slurm): add HPC_REFERENCE.md (relocated RUNME detail) (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Rewrite `slurm_batch/RUNME.md` as the lean runbook

**Files:**
- Rewrite: `slurm_batch/RUNME.md`

Target ~150–180 lines. A hydrologist reads top-to-bottom and copy-pastes. Every
heavy step is an `sbatch`; rationale links to `HPC_REFERENCE.md`.

- [ ] **Step 1: Replace the file with the runbook**

Overwrite `slurm_batch/RUNME.md` with exactly these sections:

1. **Heading + audience line**: "GFv2 Pipeline — Runbook (CONUS `gfv2`). The
   commands to take a fresh data root to finished parameters, in order. Running
   a different fabric, re-running one piece, internals, and recovery are in
   [HPC_REFERENCE.md](HPC_REFERENCE.md)."
2. **Before you start** (bullets, each ≤1 line, "why?" → HPC_REFERENCE):
   - `pixi install` once; ensure `~/.pixi/bin` is on `PATH`.
   - Always run `sbatch`/`submit_*.sh` from a shell where `~/.pixi/bin` is on
     `PATH` (SLURM inherits it).
   - Run everything from the repo root (`cd <repo>`).
3. **Pipeline at a glance** — a numbered list naming the 8 steps below (one line
   each) so the whole arc is visible first.
4. **Run it (CONUS gfv2)** — numbered steps; each = one fenced command block +
   one "**What it does**" line + one "**Wait for**" line. Use these exact
   command blocks:

   - **0 · Initialize + stage inputs**
     ```bash
     pixi run init-data-root
     sbatch slurm_batch/download_rpu_rasters.batch
     sbatch slurm_batch/download_nalcms.batch
     sbatch slurm_batch/download_nhm_v11.batch
     sbatch slurm_batch/stage_twi.batch
     pixi run init-data-root --check     # after downloads + manual inputs are in place
     ```
     What it does: scaffolds `data_root`, downloads the public rasters, stages
     per-RPU TWI; `--check` verifies manually-staged inputs. (Manual-input table
     + provenance → HPC_REFERENCE "Stage 0".)

   - **1 · Build shared (CONUS) rasters**
     ```bash
     sbatch slurm_batch/build_shared_rasters.batch
     ```
     What it does: walks the whole shared-raster DAG (merge per-VPU → slope/aspect
     → border DEM → land mask → TWI merge → VRTs → derived + LULC rasters).

   - **2 · Prepare the fabric**
     ```bash
     # merge_vpu_targets is an interactive marimo notebook — run it on a COMPUTE
     # node (JupyterHub or `salloc`), never the login node. See HPC_REFERENCE.
     pixi run -e notebooks marimo run notebooks/merge_vpu_targets.py   # nhru merge (compute node)
     sbatch slurm_batch/merge_vpu_segments.batch                        # nsegment merge (depstor)
     sbatch slurm_batch/prepare_fabric.batch                            # spatial batching + manifest
     ```
     What it does: merges per-VPU `nhru`/`nsegment` into the CONUS fabric, then
     batches it into per-batch geopackages.

   - **3 · Build depstor rasters**
     ```bash
     pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric gfv2   # tiny VRT (login OK)
     sbatch slurm_batch/build_depstor_rasters.batch
     ```
     What it does: clips the fabric-bounds FDR template, then builds the full
     depression-storage raster stack.

   - **4 · Generate parameters**
     ```bash
     BATCHES=$(pixi run --as-is python -c "import yaml;print(yaml.safe_load(open('configs/base_config.yml'))['data_root'])")/gfv2/batches
     slurm_batch/submit_zonal_params.sh   "$BATCHES" gfv2 configs/base_config.yml
     slurm_batch/submit_depstor_params.sh "$BATCHES" gfv2 configs/base_config.yml
     ```
     What it does: fans out the zonal + depstor param array jobs and chains their
     merges (+ ratios / ssflux weights) via `afterok`.

   - **5 · Gap-fill missing values**
     ```bash
     sbatch slurm_batch/merge_and_fill_params.batch
     ```
     What it does: KNN-fills any missing per-HRU parameter values.

   - **6 · (optional) Merge NHM defaults**
     ```bash
     sbatch slurm_batch/merge_default_output_params.batch
     ```

   - **7 · View results**
     ```bash
     sbatch slurm_batch/render_figures.batch     # PNGs -> docs/figures/gfv2/
     ```
     What it does: renders the fabric_results figure set headlessly. (Interactive
     viewing via JupyterHub → HPC_REFERENCE "Stage 9".)

5. **Monitoring**
   ```bash
   squeue -u "$USER"
   sacct -j <JOBID> -o JobID,State,Elapsed,MaxRSS
   tail -n 200 logs/job_<JOBID>.err
   ```
6. **Where outputs land** — `{data_root}/gfv2/params/merged/` (final CSVs, incl.
   the 6 depstor ratios) and `…/merged/_intermediates/`; figures in
   `docs/figures/gfv2/`.
7. **Need more?** — bullet links into `HPC_REFERENCE.md`: other fabrics; running
   one parameter at a time (Stage 4A); single-step raster rebuilds; recovery /
   partial reruns; environment + throttle internals; the script→config map.

Each "Wait for" line names the gating artifact or job state (e.g. "Wait for: all
array + merge jobs `COMPLETED` in `squeue`") so a non-SLURM user knows when to
proceed to the next step.

- [ ] **Step 2: Verify every command references a real batch/script + the reference link resolves**

Run:
```bash
grep -oE "slurm_batch/[A-Za-z0-9_]+\.(batch|sh)|scripts/[A-Za-z0-9_]+\.py|notebooks/[A-Za-z0-9_]+\.py" slurm_batch/RUNME.md \
  | sort -u | while read -r p; do [ -e "$p" ] && echo "OK $p" || echo "MISSING $p"; done
test -e slurm_batch/HPC_REFERENCE.md && echo "OK reference link target exists"
```
Expected: all `OK`, no `MISSING`.

- [ ] **Step 3: Confirm the runbook is lean and login-heavy commands are gone**

Run:
```bash
wc -l slurm_batch/RUNME.md
# no heavy script run directly via login 'pixi run' (clip/init are the only allowed ones):
grep -nE "pixi run( --as-is)? python scripts/(prepare_fabric|merge_and_fill_params|merge_vpu_segments|render_figures|merge_default_params)\.py" slurm_batch/RUNME.md
```
Expected: line count ≈150–200; the second `grep` returns **no output** (those run via `sbatch` now).

- [ ] **Step 4: Commit**

```bash
git add slurm_batch/RUNME.md
git commit -m "docs(slurm): rewrite RUNME.md as a lean CONUS-gfv2 runbook (#131)

Linear, copy-paste happy path; every heavy step via sbatch; rationale and
alternate paths moved to HPC_REFERENCE.md.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Update cross-references in the live docs

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/depstor_workflow.md`
- Modify: `docs/ADDING_A_PARAMETER.md`
- Modify: `docs/hpc-workflow.md`

Repoint references so the runbook is "the happy path" and stage-numbered detail
points at `HPC_REFERENCE.md` (which retains the stage labels). Do **not** edit
files under `docs/superpowers/` (archival).

- [ ] **Step 1: `CLAUDE.md`** — update the line (currently ~57):
  `` `slurm_batch/RUNME.md` is the authoritative step-by-step HPC workflow; ``
  to: `` `slurm_batch/RUNME.md` is the step-by-step runbook (the CONUS-gfv2 happy path); `slurm_batch/HPC_REFERENCE.md` holds the per-stage detail, alternate paths, and recovery; ``
  Leave the doc-audit-rule mention of RUNME (~line 96) but add `/ HPC_REFERENCE` alongside it.

- [ ] **Step 2: `docs/ARCHITECTURE.md`** — the bullet (currently ~215)
  `- [\`slurm_batch/RUNME.md\`](../slurm_batch/RUNME.md) — authoritative HPC workflow walkthrough`
  becomes two bullets:
  ```
  - [`slurm_batch/RUNME.md`](../slurm_batch/RUNME.md) — the step-by-step runbook (CONUS-gfv2 happy path)
  - [`slurm_batch/HPC_REFERENCE.md`](../slurm_batch/HPC_REFERENCE.md) — per-stage detail, alternate paths, recovery, script→config map
  ```

- [ ] **Step 3: `README.md`** — repoint the stage-specific references so they
  resolve in the reference (which keeps the stage taxonomy). Edit these lines:
  - ~119 "See `slurm_batch/RUNME.md` **Part 1** …" → "See `slurm_batch/HPC_REFERENCE.md` **Part 1** …"
  - ~166 "See `slurm_batch/RUNME.md` **Stage 4A** …" → "… `slurm_batch/HPC_REFERENCE.md` **Stage 4A** …"
  - ~178 "`slurm_batch/RUNME.md` **Part 2 / Stage 4** …" → "… `slurm_batch/HPC_REFERENCE.md` **Part 2 / Stage 4** …"
  - ~279 "Stage 2d in `slurm_batch/RUNME.md` lists …" → "Stage 2d in `slurm_batch/HPC_REFERENCE.md` lists …"
  - ~241 "See `slurm_batch/RUNME.md` for the full step-by-step workflow." → keep pointing at RUNME (it IS the runbook) but reword: "See `slurm_batch/RUNME.md` for the runbook; `slurm_batch/HPC_REFERENCE.md` for per-stage detail."
  - ~22, ~66, ~216 (generic "RUNME is the walkthrough" mentions) → reword to "RUNME.md is the runbook; HPC_REFERENCE.md the detail" where it reads naturally; otherwise leave the plain RUNME link.

- [ ] **Step 4: `docs/depstor_workflow.md`** — line ~122
  "Runs as **RUNME Stage 1c1**, before the TWI merge." → "Runs as **Stage 1c1**
  (see `slurm_batch/HPC_REFERENCE.md`), before the TWI merge."

- [ ] **Step 5: `docs/ADDING_A_PARAMETER.md`** — line ~239
  `- [\`slurm_batch/RUNME.md\`](../slurm_batch/RUNME.md) — Stage 4A walks the …`
  → point at HPC_REFERENCE: `- [\`slurm_batch/HPC_REFERENCE.md\`](../slurm_batch/HPC_REFERENCE.md) — Stage 4A walks the …`

- [ ] **Step 6: `docs/hpc-workflow.md`** — this mkdocs page `include-markdown`s
  RUNME. Update it to surface both docs. Read the current file, then make it
  include the runbook and add a second include (or a link) for the reference,
  e.g. keep `include-markdown "../slurm_batch/RUNME.md"` and add below it:
  ```
  {%
     include-markdown "../slurm_batch/HPC_REFERENCE.md"
  %}
  ```
  with a `## Reference` heading between them. (Match the existing include block's
  exact syntax/indentation in that file.)

- [ ] **Step 7: Verify no live doc still implies RUNME carries the moved detail**

Run:
```bash
grep -rn "RUNME" CLAUDE.md README.md docs/ARCHITECTURE.md docs/depstor_workflow.md docs/ADDING_A_PARAMETER.md docs/hpc-workflow.md
grep -rn "HPC_REFERENCE" CLAUDE.md README.md docs/ARCHITECTURE.md docs/depstor_workflow.md docs/ADDING_A_PARAMETER.md docs/hpc-workflow.md
```
Expected: each remaining RUNME mention is either the runbook-as-happy-path link or paired with an HPC_REFERENCE pointer; stage-numbered references now name HPC_REFERENCE.

- [ ] **Step 8: Commit**

```bash
git add CLAUDE.md README.md docs/ARCHITECTURE.md docs/depstor_workflow.md \
        docs/ADDING_A_PARAMETER.md docs/hpc-workflow.md
git commit -m "docs: repoint RUNME cross-refs to runbook + HPC_REFERENCE (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Final verification + pre-commit

**Files:** none (verification only)

- [ ] **Step 1: pre-commit on all changed files**

Run:
```bash
pixi run -e dev pre-commit run --files \
  slurm_batch/prepare_fabric.batch slurm_batch/merge_and_fill_params.batch \
  slurm_batch/merge_vpu_segments.batch slurm_batch/render_figures.batch \
  slurm_batch/HPC_REFERENCE.md slurm_batch/RUNME.md \
  CLAUDE.md README.md docs/ARCHITECTURE.md docs/depstor_workflow.md \
  docs/ADDING_A_PARAMETER.md docs/hpc-workflow.md
```
Expected: PASS (ShellCheck may lint the `.batch` files — fix any real warnings, e.g. quote expansions; amend the Task 1 commit if so). Re-run to green.

- [ ] **Step 2: Repo-wide broken-reference sweep**

Run:
```bash
# every batch/script token across the two slurm docs exists on disk:
grep -rhoE "slurm_batch/[A-Za-z0-9_]+\.(batch|sh)|scripts/[A-Za-z0-9_]+\.py" \
  slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md | sort -u \
  | while read -r p; do [ -e "$p" ] || echo "MISSING $p"; done
echo "sweep done"
```
Expected: only `sweep done` (no `MISSING` lines).

- [ ] **Step 3: Push and open the PR**

```bash
git push -u origin docs/runme-split-slurm-coverage
gh pr create --base main --fill
```
Then verify CI passes. (CI runs `pytest`; this branch has no Python changes, so it should pass unchanged. The four new batches are validated by `sbatch --test-only` in Task 1, not CI.)

---

## Self-Review

**Spec coverage:**
- Part A (4 new batches) → Task 1 (full content + verification). ✓
- Part B doc fixes: `merge_default_params` via batch → Task 3 step 1 (step 6) + HPC_REFERENCE §11; heavy `build_shared_rasters --step` via sbatch → HPC_REFERENCE §6; `merge_vpu_targets` compute-node-only → RUNME step 2 + HPC_REFERENCE §8. ✓
- Part C split: HPC_REFERENCE → Task 2; RUNME rewrite → Task 3; cross-refs → Task 4. ✓
- Reference retains stage taxonomy (spec's "incremental Stage 4A" etc.) → Task 2 intro + §6/§9. ✓ (refinement: makes inbound stage refs resolve.)
- Verification (commands exist, lossless relocation, docs gate) → Tasks 1/2/3 verify steps + Task 5. ✓
- Newly discovered: `docs/hpc-workflow.md` include-markdown → Task 4 step 6 (not in spec; added). ✓

**Placeholder scan:** no "TBD/TODO/handle edge cases". The doc tasks specify exact section lists + exact command blocks; connective prose is the deliverable written against that structure (not a placeholder). All four batch files are shown in full. `<JOBID>`/`<id>` are user-supplied runtime values, not plan gaps.

**Type/name consistency:** batch filenames, script paths, and `--fabric`/`--base_config` flags are identical across Tasks 1–5; `HPC_REFERENCE.md` spelled consistently; the `BATCHES=` derivation in RUNME step 4 matches `submit_*.sh`'s documented positional args.
