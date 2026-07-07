# Depression-storage: pyWatershed gap doc + colleague presentation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce two documentation deliverables — a pyWatershed depression-storage requirements gap doc and an old-vs-new workflow slide deck — with no changes to pipeline code.

**Architecture:** Port the Marp deck toolchain from the sibling snarea-presentation worktree; write the gap doc from ground-truthed pyWatershed 2.0.4 metadata; write a headless-matplotlib figure script that reads the on-disk CONUS depstor rasters; assemble the Marp deck; wire doc pointers.

**Tech Stack:** Marp (markdown→PDF/HTML via marp-cli/npx), pixi (`marp` + `reference` envs), matplotlib (headless), rasterio, pyWatershed 2.0.4.

## Global Constraints

- **Worktree/branch:** all work on `worktree-depstor-presentation` at `.claude/worktrees/depstor-presentation`. Derive absolute paths from `git rev-parse --show-toplevel`, never hardcode `/caldera/...` repo paths.
- **No pipeline code changes.** No new builder, no DAG edits, no emitting the 3 missing params. #154/#155/#156/#157/#147 are *noted*, not fixed.
- **Data root:** `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`, fabric `gfv2`. Read paths via the profile (`require_config_key`) where a builder helper exists; hardcoded read-only paths are acceptable in a one-off figure script but prefer the profile.
- **CONUS memory rule:** never load a full CONUS grid; clip/window rasters to a region before reading (template is 153830×109901).
- **No head-node pytest.** CI is the test gate. `py_compile`, `--version`, and single-file renders on the head node are fine.
- **pyWatershed env:** `pixi run -e reference` (python 3.10, isolated). pyWatershed is NOT in the default env.
- **Commit trailer:** end every commit message with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **Committed deck artifacts:** commit only the `.md` deck + PNG figures (match snarea — it did NOT commit rendered `.html`/`.pdf`).

## File Structure

| File | Responsibility |
|---|---|
| `pyproject.toml` | + `[tool.pixi.feature.marp.*]` blocks, `marp` environment (ported) |
| `scripts/render_deck.py` | marp-cli wrapper resolving puppeteer chrome (ported verbatim) |
| `docs/presentations/README.md` | render guide + naming convention (adapted) |
| `docs/pywatershed_depression_storage_requirements.md` | Deliverable A — gap doc |
| `scripts/render_depstor_figures.py` | headless-matplotlib figure generator |
| `docs/figures/depstor/*.png` | committed figures |
| `docs/presentations/2026-07-depression-storage-workflow.slides.md` | Deliverable B — the deck |
| `docs/ARCHITECTURE.md` | + one-line pointer to deck & gap doc (docs check) |

---

## Task 1: Port the Marp deck toolchain

**Files:**
- Modify: `pyproject.toml` (add marp feature blocks + environment)
- Create: `scripts/render_deck.py`
- Create: `docs/presentations/README.md`

**Interfaces:**
- Produces: a working `pixi run -e marp render-deck <slides.md> --html` path used by Task 4.

- [ ] **Step 1: Copy `render_deck.py` verbatim from the snarea worktree**

Source of truth (already reviewed/hardened via snarea PR #209):
`.claude/worktrees/snarea-presentation/scripts/render_deck.py`. Copy it byte-for-byte to `scripts/render_deck.py` in this worktree. Do not re-author it.

```bash
ROOT=$(git rev-parse --show-toplevel)
SN="$ROOT/../snarea-presentation"   # sibling worktree
cp "$SN/scripts/render_deck.py" "$ROOT/scripts/render_deck.py"
```

- [ ] **Step 2: Add the Marp feature blocks to `pyproject.toml`**

Insert these blocks (copied from the snarea worktree `pyproject.toml`) after the `[tool.pixi.dependencies]`/`whitebox` block. Verbatim content:

```toml
[tool.pixi.feature.marp.dependencies]
# Marp slide-deck rendering toolchain (nodejs + marp-cli via npx). Kept in its
# own feature (not dev/notebooks) so pipeline operators don't pay the nodejs +
# chrome download (~250 MB) unless they render presentation decks. Two-step:
#   pixi install -e marp          # nodejs (+ linux chrome system deps below)
#   pixi run -e marp marp-setup   # downloads chrome-headless-shell via puppeteer
# Chromium isn't on conda-forge, so we use puppeteer's chrome-headless-shell
# (smaller, fewer system deps than full chrome — works on bare HPC).
# python is declared here (not inherited) because the env is `no-default-feature`
# (below) to stay light — render_deck.py uses only the stdlib, no geo stack.
python = "3.12.*"
nodejs = ">=20"

[tool.pixi.feature.marp.target.linux-64.dependencies]
# chrome-headless-shell on bare HPC needs libgbm (off-screen buffer alloc, used
# even headless) and alsa-lib (chrome touches the audio subsystem on startup).
libgbm = ">=1.0"
alsa-lib = ">=1.2"

[tool.pixi.feature.marp.target.linux-64.activation.env]
# Chrome is a non-conda binary downloaded by marp-setup with no rpath into the
# pixi env; prepend $CONDA_PREFIX/lib so it finds libgbm + alsa-lib. The
# ${VAR:+:$VAR} guard avoids a trailing ':' (== "search cwd") when unset.
LD_LIBRARY_PATH = "$CONDA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

[tool.pixi.feature.marp.tasks]
marp-setup = { cmd = "npx --yes puppeteer browsers install chrome-headless-shell", description = "One-shot: download chrome-headless-shell via puppeteer (works on bare HPC)" }
render-deck = { cmd = "python scripts/render_deck.py", description = "Render a Marp .slides.md deck (PDF/HTML/server) via marp-cli" }
```

- [ ] **Step 3: Register the `marp` environment**

In the `[tool.pixi.environments]` table add (matching snarea):

```toml
marp = { features = ["marp"], solve-group = "marp", no-default-feature = true }
```

- [ ] **Step 4: Verify the lock resolves**

Run: `pixi install -e marp`
Expected: solves and materialises `.pixi/envs/marp` with no error. (Chrome is a separate `marp-setup` step, not needed to validate the lock.)

- [ ] **Step 5: Verify `render_deck.py` imports cleanly**

Run: `python -c "import ast; ast.parse(open('scripts/render_deck.py').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 6: Write `docs/presentations/README.md`**

Adapt from the snarea worktree's `docs/presentations/README.md`. Content: what the directory holds, the `*.slides.md` naming convention (date-prefixed, no project suffix for method-focused decks), and the two render commands:

```markdown
# Presentations

Marp slide decks (`*.slides.md`). Rendered with the `marp` pixi environment.

## Decks

- `2026-07-depression-storage-workflow.slides.md` — the PRMS/NHM depression-storage
  parameter workflow: the legacy ArcPy pipeline (`docs/0b_TB_depr_stor.py`) vs. the
  current open-source pipeline. Method/workflow-focused; spans fabrics.

## Rendering

One-time chrome download (bare HPC / fresh checkout):

    pixi install -e marp
    pixi run -e marp marp-setup

Render:

    pixi run -e marp render-deck docs/presentations/<deck>.slides.md --html
    pixi run -e marp render-deck docs/presentations/<deck>.slides.md --pdf
    pixi run -e marp render-deck docs/presentations/ --server   # live preview
```

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml pixi.lock scripts/render_deck.py docs/presentations/README.md
git commit -m "build(marp): port Marp deck toolchain from snarea worktree

Additive [tool.pixi.feature.marp.*] blocks + marp env + render_deck.py
wrapper, identical to the snarea-presentation branch. Second presentation
PR to merge resolves the trivial duplicate-block conflict.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: pyWatershed depression-storage requirements gap doc

**Files:**
- Create: `docs/pywatershed_depression_storage_requirements.md`

**Interfaces:**
- Produces: the content of the deck's "Products" slide (Task 4, slide 14). Values below are verified from installed pyWatershed 2.0.4 `meta.find_variables(...)` and `PRMSRunoff.get_parameters()`.

- [ ] **Step 1: Re-verify the metadata (don't trust this plan's transcript)**

Run:
```bash
pixi run -e reference python -c "
import pywatershed as pws
from pywatershed.hydrology.prms_runoff import PRMSRunoff
params = set(PRMSRunoff.get_parameters())
for p in ['dprst_flow_coef','dprst_seep_rate_open','dprst_seep_rate_clos','smidx_exp',
          'dprst_depth_avg','dprst_et_coef','dprst_frac_init','dprst_frac_open',
          'imperv_stor_max','op_flow_thres','va_clos_exp','va_open_exp',
          'dprst_frac','sro_to_dprst_perv','sro_to_dprst_imperv','carea_max',
          'smidx_coef','hru_percent_imperv','hru_area']:
    m = pws.meta.find_variables([p]).get(p, {})
    print(f'{p:22s} in_runoff={p in params} default={m.get(\"default\")!r} units={m.get(\"units\")!r}')
"
```
Expected: confirms the values used in Step 2 (defaults below). If any differ, use the freshly-printed values in the doc and note the pyWatershed version.

- [ ] **Step 2: Write the gap doc**

Create `docs/pywatershed_depression_storage_requirements.md` with these sections and exact values:

Header — purpose, that it was verified against pyWatershed 2.0.4 (`pixi run -e reference`), and that the module is `pywatershed.hydrology.prms_runoff.PRMSRunoff`.

**Requirement table (bucket 1 — spatial, we produce):**

| Param | pyWatershed default | units | Our product |
|---|---|---|---|
| `dprst_frac` | 0.0 | decimal fraction | `gfv2/params/merged/nhm_dprst_frac_params.csv` |
| `sro_to_dprst_perv` | 0.2 | decimal fraction | `nhm_sro_to_dprst_perv_params.csv` |
| `sro_to_dprst_imperv` | 0.2 | decimal fraction | `nhm_sro_to_dprst_imperv_params.csv` |
| `carea_max` | 0.6 | decimal fraction | `nhm_carea_max_params.csv` |
| `smidx_coef` | 0.005 | decimal fraction | `nhm_smidx_coef_params.csv` |
| `hru_percent_imperv` | 0.0 | decimal fraction | `nhm_hru_percent_imperv_params.csv` |

State the CSV schema: two columns, `nat_hru_id,<param>`; join key `nat_hru_id`. Note these were rebuilt Jul 5 2026 from the fully-grounded classifier.

**Requirement table (bucket 2 — constant defaults, legacy `0b` emitted):**

| Param | legacy `0b` value | pyWatershed default | units | note |
|---|---|---|---|---|
| `dprst_depth_avg` | 132 | 132.0 | inches | matches |
| `dprst_et_coef` | 1 | 1.0 | fraction | matches |
| `dprst_frac_init` | 0.5 | 0.5 | fraction | matches |
| `dprst_frac_open` | 1 | 1.0 | fraction | matches |
| `imperv_stor_max` | 0.05 | 0.05 | inches | matches |
| `op_flow_thres` | 1 | 1.0 | fraction | matches |
| `va_clos_exp` | 0.001 | 0.001 | none | matches |
| `va_open_exp` | 0.001 | 0.001 | none | matches |

**Requirement table (bucket 3 — gaps: pyWatershed needs, legacy `0b` never emitted):**

| Param | pyWatershed default | units | recommendation |
|---|---|---|---|
| `dprst_flow_coef` | 0.05 | fraction/day | adopt pyWatershed default (a priori, no spatial basis) |
| `dprst_seep_rate_open` | 0.02 | fraction/day | adopt pyWatershed default |
| `smidx_exp` | 0.3 | 1.0/inch | adopt pyWatershed default |

**Naming + value discrepancies to flag explicitly:**
- Rename: legacy `dprst_seep_rate_close` → pyWatershed `dprst_seep_rate_clos` (drop the `e`). pyWatershed splits seepage into `_open` and `_clos`.
- **Value discrepancy (call out):** legacy `dprst_seep_rate_close` = **0.2**, but pyWatershed `dprst_seep_rate_clos` default = **0.02** — a 10× difference. Document the choice: adopt the pyWatershed default (0.02) unless the NHM paramdb specifies otherwise; note this needs a modeler's sign-off.
- `hru_area` is a fabric-geometry input, not produced by `0b` or by this pipeline's depstor stage — note it as supplied by the fabric.

**Verdict section:** state plainly that `gfv2/params/merged/` supplies the 6 spatial params pyWatershed's `PRMSRunoff` requires; the remaining 11 non-spatial params are a priori constants; 8 match the legacy defaults, and 3 (`dprst_flow_coef`, `dprst_seep_rate_open`, `smidx_exp`) plus the `dprst_seep_rate_clos` value need a documented default adopted from pyWatershed. One-line action per gap. No code is produced.

- [ ] **Step 3: Verify the doc renders and links resolve**

Run: `python -c "import pathlib; t=pathlib.Path('docs/pywatershed_depression_storage_requirements.md').read_text(); assert 'dprst_flow_coef' in t and 'dprst_seep_rate_clos' in t and 'nat_hru_id' in t; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add docs/pywatershed_depression_storage_requirements.md
git commit -m "docs: pyWatershed depression-storage requirements + gap analysis

Ground-truthed against pyWatershed 2.0.4 PRMSRunoff.get_parameters().
6 spatial params supplied by params/merged; 8 constant defaults match
legacy 0b; 3 gaps (dprst_flow_coef, dprst_seep_rate_open, smidx_exp)
+ the dprst_seep_rate_close->clos 10x value discrepancy documented with
recommended pyWatershed defaults.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Figure-generation script + committed figures

**Files:**
- Create: `scripts/render_depstor_figures.py`
- Create: `docs/figures/depstor/*.png` (committed outputs)

**Interfaces:**
- Consumes: on-disk rasters under `{data_root}/gfv2/depstor_rasters/` and the pre-fix snapshot `{data_root}/gfv2/depstor_rasters_pre_flowthrough_2026-06-26/`.
- Produces: PNG paths referenced by the deck (Task 4).

Raster inventory (verified on disk):
- current: `depstor_rasters/{dprst_binary,onstream_binary,drains_to_dprst,drains_to_dprst_hru,connected_wbody,vpu_id}.tif`
- pre-fix snapshot: `depstor_rasters_pre_flowthrough_2026-06-26/` (same names)

- [ ] **Step 1: Write the figure script skeleton**

Create `scripts/render_depstor_figures.py`. Headless matplotlib (`matplotlib.use("Agg")`). Structure:
- module docstring: what it makes, that it reads the CONUS rasters windowed (never full-grid), and the run command.
- `OUT = Path("docs/figures/depstor")`, `mkdir(parents=True, exist_ok=True)`.
- `DATA_ROOT` resolved from the fabric profile if importable, else the literal `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2` (read-only).
- Region windows defined as bounding boxes in the raster CRS (Albers). Provide `GREAT_BASIN` and `LOWER_MISS` as `(minx, miny, maxx, maxy)` — resolve exact values in Step 3 from `vpu_id.tif` (Great Basin ≈ VPU 16; Lower Miss ≈ VPU 08).
- A `read_window(path, bbox)` helper using `rasterio.windows.from_bounds` so only the region is read into memory.
- `main()` calls each figure function and prints the written paths.

- [ ] **Step 2: Verify it compiles**

Run: `python -m py_compile scripts/render_depstor_figures.py && echo ok`
Expected: `ok`

- [ ] **Step 3: Resolve the two region windows**

Run (windowed read of `vpu_id.tif`, no full-grid load):
```bash
pixi run python -c "
import rasterio
from rasterio.features import dataset_features
p='/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/gfv2/depstor_rasters/vpu_id.tif'
with rasterio.open(p) as ds:
    print('crs', ds.crs); print('bounds', ds.bounds)
"
```
Use the printed CRS/bounds to hand-pick a Great Basin bbox (endorheic showcase) and a Lower Mississippi bbox (over-extension showcase). Hardcode the two bboxes into the script as `GREAT_BASIN`/`LOWER_MISS`. Keep each window under a few thousand cells per side.

- [ ] **Step 4: Implement the schematic + DAG figures (no raster reads)**

Add `fig_decision_schematic()` (legacy 60 m buffer vs. network-connectivity split — a labeled matplotlib diagram) and `fig_pipeline_dag()` (NHD + FDR + TWI + LULC → waterbody → wbody_connectivity → dprst → routing → drains_perv/imperv → PRMS params). Both pure matplotlib, no data. Write `decision_schematic.png`, `pipeline_dag.png`.

- [ ] **Step 5: Implement the two before/after maps**

Add `fig_before_after(region_name, bbox)` reading `dprst_binary`+`onstream_binary` from BOTH `depstor_rasters/` and `depstor_rasters_pre_flowthrough_2026-06-26/`, windowed to `bbox`, and drawing a 2-panel (before | after) map. Call for Great Basin → `great_basin_before_after.png` and Lower Mississippi → `lower_miss_before_after.png`.

- [ ] **Step 6: Run the script and confirm PNGs exist**

Run: `pixi run python scripts/render_depstor_figures.py`
Expected: prints 4 written paths; `ls docs/figures/depstor/*.png` shows `decision_schematic.png`, `pipeline_dag.png`, `great_basin_before_after.png`, `lower_miss_before_after.png`. Eyeball each PNG opens and is non-empty.

- [ ] **Step 7: Commit**

```bash
git add scripts/render_depstor_figures.py docs/figures/depstor/*.png
git commit -m "feat(figures): depstor workflow figures (schematic, DAG, before/after maps)

Headless-matplotlib script reading the on-disk CONUS depstor rasters
windowed per region. Great Basin (endorheic fix) + Lower Mississippi
(over-extension fix) before/after from the pre_flowthrough_2026-06-26
snapshot vs current.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: The deck + doc pointers

**Files:**
- Create: `docs/presentations/2026-07-depression-storage-workflow.slides.md`
- Modify: `docs/ARCHITECTURE.md` (add pointer)

**Interfaces:**
- Consumes: figures from Task 3 (relative `../figures/depstor/*.png`); Products content from Task 2.

- [ ] **Step 1: Write the deck**

Create `docs/presentations/2026-07-depression-storage-workflow.slides.md`. Marp frontmatter:

```markdown
---
marp: true
theme: default
paginate: true
title: PRMS/NHM Depression-Storage Parameter Workflow
---
```

Then ~16 slides (`---` separated) following the spec outline (§B2). Per-slide content:
1. Title — "Depression-Storage Parameters for PRMS/NHM: from ArcGIS to an open-source CONUS pipeline".
2. What depression storage is in PRMS + the 6 spatial params (`dprst_frac`, `sro_to_dprst_perv/imperv`, `carea_max`, `smidx_coef`, `hru_percent_imperv`).
3. Why it matters — `dprst_frac`/`sro_to_dprst_*` → surface-runoff partition/timing.
4. Legacy ArcPy workflow (`0b_TB_depr_stor.py`) at a glance.
5. Two weaknesses: 60 m Euclidean segment buffer ≠ connectivity; endorheic lakes over-promoted.
6. The new principle: ground the on-stream/depression split in the NHD stream network.
7. Open-source stack + reproducibility + CONUS scale (pixi/SLURM); `![](../figures/depstor/pipeline_dag.png)`.
8. Identifying dprst waterbodies — two-source union + Network-Flowline gate (Playa force-dprst, Ice Mass excluded).
9. The endorheic fix — `![](../figures/depstor/great_basin_before_after.png)`.
10. `drains_to_dprst` — D8 kernel + on-stream barrier.
11. Over-extension fix — `![](../figures/depstor/lower_miss_before_after.png)`; cite Lower Miss 70%→8.6% land coverage.
12. same-HRU restriction on `sro_to_dprst_*` (legacy `Con(rSro == hru)` reproduced in raster space).
13. Legacy-vs-new decision schematic — `![](../figures/depstor/decision_schematic.png)`.
14. Products — what pyWatershed consumes (6 spatial from `params/merged` + a priori constants; link the gap doc).
15. Known open refinements — #154 (Reservoir bucket), #155 (permanence gate), #156 (~4% clump-merge), #157 (~0.05% seam), #147 (depression-respecting FDR).
16. Summary + references (`docs/0b_TB_depr_stor.py`; NHDPlus V2 PlusFlowlineVAA; `docs/pywatershed_depression_storage_requirements.md`).

Keep prose terse (bullet points; speaker fills detail). Define terms in plain English.

- [ ] **Step 2: Render to HTML (no chrome needed) to validate Marp syntax + figure links**

Run: `pixi run -e marp render-deck docs/presentations/2026-07-depression-storage-workflow.slides.md --html`
Expected: writes `docs/presentations/2026-07-depression-storage-workflow.slides.html` with no marp error; the 4 figure `![]()` refs resolve (no broken-image warnings for `../figures/depstor/*.png`).

- [ ] **Step 3: (optional) Render to PDF if chrome is available**

Run: `pixi run -e marp marp-setup && pixi run -e marp render-deck docs/presentations/2026-07-depression-storage-workflow.slides.md --pdf`
Expected: writes the `.pdf`. If chrome download is blocked on the head node, skip — HTML render in Step 2 is sufficient validation. Do NOT commit the `.html`/`.pdf` (match snarea).

- [ ] **Step 4: Add doc pointers (CLAUDE.md docs check)**

In `docs/ARCHITECTURE.md`, in the depression-storage / Part 2 depstor section, add a one-line pointer:

```markdown
> **Narrative overview:** see the slide deck
> [`docs/presentations/2026-07-depression-storage-workflow.slides.md`](presentations/2026-07-depression-storage-workflow.slides.md)
> and the pyWatershed parameter contract
> [`docs/pywatershed_depression_storage_requirements.md`](pywatershed_depression_storage_requirements.md).
```

Find the right anchor with: `grep -n -i "depression\|depstor\|dprst" docs/ARCHITECTURE.md | head`.

- [ ] **Step 5: Confirm the deck is not accidentally added to mkdocs nav**

Run: `grep -n "depression-storage-workflow\|pywatershed_depression" mkdocs.yml || echo "not in nav (ok — link only)"`
Expected: `not in nav (ok — link only)` (raw Marp renders poorly as a doc page; link from ARCHITECTURE.md is enough). If the repo convention is to nav-list docs, add the gap doc (not the deck) under an appropriate nav section.

- [ ] **Step 6: Commit**

```bash
git add docs/presentations/2026-07-depression-storage-workflow.slides.md docs/ARCHITECTURE.md
git commit -m "docs(deck): depression-storage workflow presentation (old vs new)

~16-slide Marp deck: legacy ArcPy 0b_TB_depr_stor.py vs the open-source
CONUS pipeline. Headline proprietary->open-source/reproducible;
connectivity-based classification, endorheic + over-extension fixes,
pyWatershed products, and open refinements (#154-157/#147). Pointers
added from ARCHITECTURE.md.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Finalize — pre-commit + PR

**Files:** none (integration)

- [ ] **Step 1: Run pre-commit on all changed files**

Run: `pixi run -e dev pre-commit run --files pyproject.toml scripts/render_deck.py scripts/render_depstor_figures.py docs/pywatershed_depression_storage_requirements.md docs/presentations/README.md docs/presentations/2026-07-depression-storage-workflow.slides.md docs/ARCHITECTURE.md`
Expected: all hooks pass (or auto-fix; re-add + amend the relevant commit if a hook reformats).

- [ ] **Step 2: Push the branch**

```bash
git push -u origin worktree-depstor-presentation
```

- [ ] **Step 3: Open the PR (curl+REST — gh CLI is blocked on HPC)**

Body must lead with a **Marp-tooling coordination callout**: this PR and the snarea-presentation PR both add identical `[tool.pixi.feature.marp.*]` blocks + `scripts/render_deck.py`; whichever merges second resolves the trivial duplicate-block conflict. Summarize the two deliverables + that no pipeline code changed and #154-157/#147 are noted-not-fixed. Use the `gh auth token` + `curl --data-binary @payload.json` pattern (see the `gh_cli_blocked_use_curl_rest` note; `-d @file` sends malformed → use `--data-binary`).

- [ ] **Step 4: Let CI run** — CI is the test gate. Confirm green before requesting review.

---

## Self-Review notes (author)

- **Spec coverage:** Deliverable A → Task 2; Deliverable B deck → Task 4; figures → Task 3; Marp port + coordination → Task 1 + Task 5 Step 3; docs pointers → Task 4 Step 4; atomic commits → one per task; non-goals (no code, #157/#156 noted) → enforced in Global Constraints + slide 15.
- **Placeholder scan:** region bboxes are the only deferred values, resolved by an explicit command in Task 3 Step 3 (not a silent TODO). pyWatershed defaults are concrete (verified) with a re-verify guard in Task 2 Step 1.
- **Type/name consistency:** figure filenames (`decision_schematic.png`, `pipeline_dag.png`, `great_basin_before_after.png`, `lower_miss_before_after.png`) match between Task 3 outputs and Task 4 slide refs; CSV join key `nat_hru_id` consistent; param name `dprst_seep_rate_clos` (pyWatershed) vs `dprst_seep_rate_close` (legacy) used deliberately and consistently.
