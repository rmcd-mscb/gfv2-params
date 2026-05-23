# `docs/ARCHITECTURE.md` Implementation Plan (T2-C)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `docs/ARCHITECTURE.md` as the single canonical source for the project's architecture; slim `CLAUDE.md` (147 → ~85 lines) and `README.md` (430 → ~280 lines) to link to it. Also satisfies the per-key fabric-profile table recommendation from the fresh-eyes review.

**Architecture:** Pure docs PR. Three atomic commits in one PR: (1) add the new doc, (2) slim CLAUDE.md, (3) slim README.md. Each commit is independently revertible. No code touched.

**Tech Stack:** Markdown. No dependencies.

**Source spec:** [`docs/superpowers/specs/2026-05-23-architecture-doc-design.md`](../specs/2026-05-23-architecture-doc-design.md)

**Branch:** `docs/architecture-md` (already created; spec committed as `d593b63`).

---

## File map (after)

| File | Change | Net LOC |
|---|---|---|
| `docs/ARCHITECTURE.md` | new (~200 LOC) | +200 |
| `CLAUDE.md` | slim (147 → ~85) | −60 |
| `README.md` | slim (430 → ~280) | −150 |
| `docs/superpowers/INDEX.md` | one-line cross-ref add | +1 |

4 files modified/created. Net: roughly −10 LOC overall.

---

## Task 1: Pre-flight verification

**Files:** read-only.

- [ ] **Step 1: Verify branch + clean tree**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
git branch --show-current
git status --short
```

Expected: branch `docs/architecture-md`; empty status.

- [ ] **Step 2: Confirm spec is committed**

```bash
git log --oneline -1 docs/superpowers/specs/2026-05-23-architecture-doc-design.md
```

Expected: commit ending in `docs(spec): consolidate architecture knowledge into ARCHITECTURE.md (T2-C)` (sha `d593b63` or later).

- [ ] **Step 3: Re-verify the current CLAUDE.md + README.md line counts and section headers**

```bash
wc -l CLAUDE.md README.md
grep -n "^#" CLAUDE.md
grep -n "^##" README.md
```

Expected:
- CLAUDE.md: 147 lines, 9 sections (matches spec).
- README.md: 430 lines, 16 `##` sections (matches spec).

If counts differ significantly, ESCALATE — the spec was written against a specific state.

---

## Task 2: Add `docs/ARCHITECTURE.md` (commit 1 of 3)

**Files:**
- Create: `docs/ARCHITECTURE.md`

This is the new canonical source. Write the file with the following exact structure. The content draws from CLAUDE.md (architecture sections) + README's pipeline sections + the two package `__init__.py` docstrings, consolidated and deduplicated.

- [ ] **Step 1: Write `docs/ARCHITECTURE.md`**

Create the file with this exact content:

```markdown
# Architecture

The single canonical source for the project's architecture. If anything here
contradicts CLAUDE.md or README.md, **this doc wins** — the others link to
here as the truth.

## Overview

`gfv2-params` generates PRMS/NHM hydrologic-model parameters by running zonal
statistics over CONUS source rasters (DEM, soils, lithology, LULC,
depression-storage) against a watershed fabric of HRU polygons. Production
runs are CONUS-scale on a USGS HPC cluster under SLURM; smaller fabrics
(e.g. `gfv2_vpu01`, `oregon`) are used for development and validation.

## Data-root layout (the key invariant)

A single on-disk `data_root` is set in `configs/base_config.yml`. Everything
the pipeline reads or writes lives under it, in three top-level subtrees:

```
data_root/
├── input/                      # Manually staged or downloaded external data
│   ├── fabric/                 # Per-VPU watershed fabric gpkgs
│   ├── soils_litho/            # TEXT_PRMS.tif, AWC.tif, Lithology_exp_Konly_Project.*
│   ├── lulc_veg/               # RootDepth.tif, CNPY.tif, Imperv.tif (+ per-source subdirs)
│   ├── lulc/                   # NLCD impervious, NALCMS land cover (downloadable)
│   ├── depstor/                # Per-fabric depression-storage inputs
│   ├── twi/<rpu>/              # Per-RPU TWI (staged via stage_twi.sh)
│   ├── nhm_default/            # NHM default parameter files
│   └── nhd_downloads/          # Raw NHDPlus zip archives
├── shared/                     # Fabric-INDEPENDENT intermediates (reused by every fabric)
│   ├── source/                 # Unzipped per-RPU NHDPlus rasters
│   ├── per_vpu/<vpu>/          # Per-VPU merged GeoTIFFs (NED, Hydrodem, Fdr, Fac, Twi, slope, aspect, landmask)
│   └── conus/
│       ├── vrt/                # CONUS GDAL virtual rasters (elevation/slope/aspect/fdr/twi/twi_hydrodem)
│       ├── derived/            # soil_moist_max.tif, radtrn, resampled CNPY/keep
│       ├── borders/            # Copernicus border-DEM fill (Canada/Mexico)
│       └── weights/            # P2P polygon weights for ssflux
└── {fabric}/                   # Per-fabric outputs (gfv2/, gfv2_vpu01/, oregon/, ...)
    ├── fabric/                 # Merged fabric gpkg
    ├── batches/                # Per-batch gpkgs + manifest.yml
    ├── depstor_rasters/        # Depression-storage intermediate rasters
    └── params/                 # Parameter outputs + merged/ + filled
```

**The invariant: every fabric reuses the same `shared/` rasters.** Per-VPU
iteration happens *inside* builders, not in per-VPU SLURM submissions. A new
fabric needs new `input/fabric/<gpkg>` + a new `{fabric}/` output tree; it
does NOT need new `shared/` content.

## Part 1 vs Part 2

The pipeline splits into two halves that share `data_root` but execute
independently:

- **Part 1 — fabric-independent.** Produces `shared/` content from `input/`.
  One run per CONUS, reused by every fabric. Driven by `build_shared_rasters.py`.
- **Part 2 — fabric-dependent.** Produces `{fabric}/` content by combining
  the fabric's HRU geometry with `shared/` rasters. Splits further into
  **2a (depstor)** and **2b (zonal)** which can run in parallel after Part 1
  finishes.

The natural parallelism boundary: Part 1 once per CONUS, Part 2 N times (one
per fabric). For most regional fabrics Part 1 can be scoped to the VPUs the
fabric overlaps (e.g. `VPUS=17` for `oregon`).

## Orchestrator + builder + unified-config pattern

Each pipeline stage is **one orchestrator script + one unified YAML config +
a package of per-step builder modules**. The orchestrators walk a step DAG
and dispatch into library functions; the SLURM `*.batch` wrappers are thin
shells around the same builders. The four stages:

| Stage | Orchestrator | Config | Builders |
|---|---|---|---|
| Part 1 shared rasters | `scripts/build_shared_rasters.py` | `configs/shared_rasters/shared_rasters.yml` | `src/gfv2_params/shared_rasters/` |
| Part 2a depstor rasters | `scripts/build_depstor_rasters.py` | `configs/depstor/depstor_rasters.yml` | `src/gfv2_params/depstor_builders/` |
| Part 2a depstor params | `scripts/derive_depstor_params.py` | `configs/depstor/depstor_params.yml` | `src/gfv2_params/depstor_ratios.py` |
| Part 2b zonal params | `scripts/derive_zonal_params.py` | `configs/zonal/zonal_params.yml` | `src/gfv2_params/zonal_runners/` |

Orchestrators support `--step <name>` (one step), `--from <name>` (resume),
and `--force` (rebuild outputs that already exist). The zonal orchestrator
also supports `--mode zonal|merge|build_weights` for per-batch debugging.

SLURM submission wrappers (`slurm_batch/submit_*.sh`) chain array jobs →
merges → ratios via `afterok` dependencies.

### Per-package details

Each builders package has its own `__init__.py` documenting the per-step
contract:

- [`src/gfv2_params/shared_rasters/__init__.py`](../src/gfv2_params/shared_rasters/__init__.py) — Part 1 builders (10 modules)
- [`src/gfv2_params/depstor_builders/__init__.py`](../src/gfv2_params/depstor_builders/__init__.py) — Part 2a raster builders (11 modules)
- [`src/gfv2_params/zonal_runners/__init__.py`](../src/gfv2_params/zonal_runners/__init__.py) — Part 2b param runners (6 modules)

Each `build(step_cfg, ctx, logger)` function produces named outputs that
downstream steps can reach via the shared context.

## Fabric profiles — the single source of truth

`configs/base_config.yml` holds the `data_root` and a `fabrics:` mapping of
profiles. **Every shared, required per-fabric input lives in its profile** —
never as a required CLI arg, never inferred from a naming convention.
Scripts read keys via `require_config_key(config, key, script_name)` from
`src/gfv2_params/config.py`, which also resolves placeholder substitution
(`{data_root}`, `{fabric}`, `{vpu}`, `{raster_vpu}`). Per-step configs are
fabric-agnostic templates resolved at runtime.

### Active fabric resolution (highest precedence first)

1. `--fabric <name>` CLI flag on any script
2. `FABRIC` env var (typical for `sbatch --export=ALL,FABRIC=...`)
3. `default_fabric` in `configs/base_config.yml` (currently `gfv2`)

### Required profile keys

Register a new fabric with `pixi run init-data-root --add-fabric <name>` to
append a profile stub; fill the stub's TODOs. Required keys depend on
whether the depstor pipeline will be run for the fabric:

| Key | Always required | Depstor only | Notes |
|---|:-:|:-:|---|
| `hru_gpkg` | ✓ | — | Path to the fabric geopackage (post-merge for VPU-based fabrics) |
| `hru_layer` | ✓ | — | Layer name inside `hru_gpkg` (typically `nhru`) |
| `id_feature` | ✓ | — | The HRU id column in the fabric (e.g. `nat_hru_id` for gfv2, `hru_id` for oregon); flows through to merged parameter CSVs |
| `expected_max_hru_id` | ✓ | — | Used by `merge_and_fill_params` to detect gaps in the merged output |
| `batch_size` | ✓ | — | Target features per spatial batch in `prepare_fabric` |
| `template_raster` | — | ✓ | Fabric-bounds clip of `fdr.vrt`; produced by `clip_shared_to_fabric.py` |
| `fdr_raster` | — | ✓ | Same fabric-bounds clip (typically points at the same file as `template_raster`) |
| `twi_raster` | — | ✓ | CONUS `twi.vrt` (ArcPy, calibrated) or `twi_hydrodem.vrt` (open-source, CONUS-complete) |
| `segments_gpkg` | — | ✓ | Stream segments for the depstor routing step; for a single-file fabric can point at `hru_gpkg` |
| `segments_layer` | — | ✓ | Layer name inside `segments_gpkg` (typically `nsegment`) |
| `waterbody_gpkg` | — | ✓ | NHDPlus waterbodies; depstor's `waterbody` step **raises** if unset |
| `waterbody_layer` | — | ✓ | Layer name inside `waterbody_gpkg` |

For `template_raster`/`fdr_raster`, stage the clip with:

```bash
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric <name>
# writes {data_root}/<name>/shared/<name>_fdr.vrt
```

Every depstor builder sizes its arrays to the `template_raster` grid, so the
clip scopes compute to the fabric extent while staying VPU-agnostic (works
for fabrics that straddle VPU boundaries).

### Common fabrics

- **`gfv2`** — CONUS production fabric (~361k HRUs).
- **`gfv2_vpu01`** — small-scale validation overlay (~11k HRUs in VPU 01).
- **`oregon`** — current regional test fabric (~38k HRUs incidental to VPU 17).

## Non-obvious conventions & gotchas

These are hard-won; violating them silently corrupts outputs.

- **Depstor template/fdr come from a fabric-bounds clip** of `fdr.vrt`
  ([`scripts/clip_shared_to_fabric.py`](../scripts/clip_shared_to_fabric.py)),
  not from CONUS VRTs or per-VPU tiles. The clip must come from the
  hydrology lattice (`fdr.vrt` / `twi.vrt`); `elevation.vrt` is on the
  offset DEM lattice and `carea_map` requires `template ≡ twi` alignment.
- **Land masking.** Every depstor raster is masked against `land_mask.tif`
  (the HRU fabric rasterised by the `landmask` step). Never use hydro-DEM
  nodata or FDR as a land mask.
- **WhiteboxTools cannot read LZW + `predictor=2` GeoTIFFs** — it silently
  corrupts them. Never pass `predictor=2` rasters to WBT subprocesses.
- **`carea_max`/`smidx_coef` threshold mode.** The legacy `absolute`
  thresholds (8.0/15.6) are only calibrated against VPU 01's ArcPy TWI
  distribution. For any other fabric, use `threshold_mode: percentile` (the
  default in `configs/depstor/depstor_rasters.yml`) with `twi_raster`
  pointing at `twi_hydrodem.vrt` and run the `twi_reference` shared-raster
  step first. See [`docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md`](superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md).

## How to add a new pipeline step

Same recipe for every stage (new shared raster, new depstor builder, new
zonal param family):

1. **Write the builder module** under the appropriate package
   (`src/gfv2_params/shared_rasters/`, `src/gfv2_params/depstor_builders/`,
   or `src/gfv2_params/zonal_runners/`). Export a single
   `build(step_cfg, ctx, logger) -> dict[str, Path]` (raster builders) or
   `run_<name>_batch(config, batch_id, logger) -> None` (zonal runners).
2. **Register in the package's `__init__.py`** — add to the `BUILDERS` /
   `STEP_ORDER` / `BATCH_RUNNERS` registries as appropriate.
3. **Add a config block** in the matching unified config under `configs/`.
4. **Add a test** under `tests/test_<name>.py`. CI (`.github/workflows/ci.yml`)
   gates the merge; the head-node-pytest prohibition (see CLAUDE.md) does
   not apply to PR-driven CI.

Do NOT add a new standalone script or a new YAML file. The
orchestrator + builder + unified-config pattern is the only way new steps
land.

## Related docs

- [`README.md`](../README.md) — user-facing setup + usage
- [`CLAUDE.md`](../CLAUDE.md) — project rules for Claude (atomic commits, doc audit, etc.)
- [`slurm_batch/RUNME.md`](../slurm_batch/RUNME.md) — authoritative HPC workflow walkthrough
- [`docs/superpowers/INDEX.md`](superpowers/INDEX.md) — index of design specs, implementation plans, and reviews
- [`docs/depstor_workflow.md`](depstor_workflow.md), [`docs/depstor_port_summary.md`](depstor_port_summary.md), [`docs/depstor_vpu01_validation_results.md`](depstor_vpu01_validation_results.md) — depstor pipeline reference (historical and current)
```

- [ ] **Step 2: Verify the file renders cleanly**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
wc -l docs/ARCHITECTURE.md
# Confirm all relative links inside the doc resolve to existing files
for link in $(grep -oE '\]\(\.\./?[^)]+\)' docs/ARCHITECTURE.md | sed 's/^](//; s/)$//'); do
    target=$(realpath -m "docs/$link" 2>/dev/null)
    [ -f "$target" ] && echo "  ✓ $link" || echo "  ✗ $link MISSING (resolved to $target)"
done
```

Expected: every link resolves; line count between 180–230.

- [ ] **Step 3: Pre-commit + stage + commit (1 of 3)**

```bash
git add docs/ARCHITECTURE.md
pixi run -e dev pre-commit run --files docs/ARCHITECTURE.md 2>&1 | tail -8
git status --short
git commit -m "$(cat <<'EOF'
docs(architecture): add canonical ARCHITECTURE.md (T2-C)

Add docs/ARCHITECTURE.md as the single canonical source for the project's
architecture. Consolidates the orchestrator+builder pattern, fabric profile
model, data-root layout, and non-obvious gotchas — currently scattered
across CLAUDE.md, README.md, and the two builder package __init__.py
docstrings.

Sections:
- Overview (1 paragraph)
- Data-root layout (the input/ -> shared/ -> {fabric}/ invariant)
- Part 1 vs Part 2 split
- Orchestrator + builder + unified-config pattern (4-stage table)
- Fabric profiles - single source of truth + per-key required-field table
  (also satisfies the per-key table recommendation from the fresh-eyes review)
- Non-obvious conventions & gotchas (clip lattice, land masking, WBT
  predictor=2, carea threshold mode)
- How to add a new pipeline step (4-edit recipe)
- Cross-references to README, CLAUDE.md, RUNME.md, INDEX.md, depstor docs

Subsequent commits on this branch slim CLAUDE.md and README.md to link
here. The package __init__.py docstrings stay as-is (per-package context).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

Verify commit landed:
```bash
git log --oneline -1
```

Expected: `<sha> docs(architecture): add canonical ARCHITECTURE.md (T2-C)`.

---

## Task 3: Slim `CLAUDE.md` (commit 2 of 3)

**Files:**
- Modify: `CLAUDE.md` (147 → ~85 lines)

### Pre-slim content map

Read the **pre-slim CLAUDE.md** end-to-end and mark each section's fate.

| CLAUDE.md current | Lines | Fate |
|---|---|---|
| Title + intro | 1–3 | KEEP unchanged |
| `## What this is` | 5–10 | KEEP unchanged |
| `## Environment & commands` | 12–35 | KEEP unchanged |
| `### Testing on the HPC head node` | 37–43 | KEEP unchanged |
| `## Architecture` | 45–57 | COLLAPSE to 3-line summary + link to ARCHITECTURE.md |
| `### Orchestrator + builder-module pattern` | 59–80 | COLLAPSE to 2-line summary + link |
| `### Fabric profiles — the single source of truth` | 82–102 | COLLAPSE to 2-line summary + link |
| `## Non-obvious conventions & gotchas` | 104–117 | KEEP (load-bearing for Claude; quick-reference) |
| `## Working in this repo` | 119–130 | KEEP unchanged |
| `### Code conventions` | 132–147 | KEEP unchanged |

- [ ] **Step 1: Replace the 3 collapse sections with a single combined `## Architecture` section**

Using the Edit tool, replace the entire block from line 45 ("`## Architecture`") through line 102 (the last line of the Fabric profiles section, ending with `current regional test fabric.`) with this exact content:

```markdown
## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the canonical source
covering: data-root layout (`input/` → `shared/` → `{fabric}/`), Part 1 vs
Part 2 split, the **orchestrator + builder + unified-config pattern** for the
4 pipeline stages, fabric profiles as the single source of truth (with the
per-key required-field table), and how to add a new pipeline step.

`slurm_batch/RUNME.md` is the authoritative step-by-step HPC workflow;
`README.md` covers user-facing setup and usage.
```

The block being replaced spans:
- `## Architecture` (line 45)
- The "Part 1/Part 2" paragraph (line 47–50)
- The "Layout of `data_root`" paragraph (line 52–57)
- `### Orchestrator + builder-module pattern` (line 59) + intro paragraph + 4-row table + closing 2 paragraphs (through line 80)
- `### Fabric profiles — the single source of truth` (line 82) + 3 paragraphs (through line 102)

Use `Edit` with the exact `old_string` including the line-45 `## Architecture` heading and ending with the line-102 `... current regional test fabric.` sentence. After the edit, the `## Non-obvious conventions & gotchas` section (was line 104) becomes the immediate next section.

- [ ] **Step 2: Verify line count + section structure**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
wc -l CLAUDE.md
grep -n "^##" CLAUDE.md
```

Expected:
- Line count: 80–90 (was 147; expected ~85)
- Sections in order: `## What this is`, `## Environment & commands`, `### Testing on the HPC head node`, `## Architecture` (the new short version), `## Non-obvious conventions & gotchas`, `## Working in this repo`, `### Code conventions`.

If the line count is dramatically off (>100 lines or <70), recheck the Edit.

- [ ] **Step 3: Verify the link to ARCHITECTURE.md resolves**

```bash
grep -n "ARCHITECTURE.md" CLAUDE.md
# Should show 1 or 2 references; verify the path docs/ARCHITECTURE.md exists
ls -la docs/ARCHITECTURE.md
```

- [ ] **Step 4: Pre-commit + stage + commit (2 of 3)**

```bash
git add CLAUDE.md
pixi run -e dev pre-commit run --files CLAUDE.md 2>&1 | tail -6
git commit -m "$(cat <<'EOF'
docs(CLAUDE): slim architecture sections; link to ARCHITECTURE.md (T2-C)

CLAUDE.md grew to 147 lines with a 58-line architecture block (orchestrator
+ builder pattern + fabric profiles + data layout) that duplicated content
now in docs/ARCHITECTURE.md.

Replace the ## Architecture, ### Orchestrator + builder-module pattern, and
### Fabric profiles sections (was 45-102) with a single 8-line ##
Architecture section that links to docs/ARCHITECTURE.md.

Kept verbatim:
- ## What this is (Claude needs project context)
- ## Environment & commands (Claude needs to know how to run things)
- ### Testing on the HPC head node (load-bearing rule)
- ## Non-obvious conventions & gotchas (load-bearing for Claude; quick reference)
- ## Working in this repo (Claude rules)
- ### Code conventions (Claude rules)

The 3 gotcha bullets stay in CLAUDE.md as a quick-reference — Claude needs
them at the top of mind, not behind a link. ARCHITECTURE.md has the same
3 with a 4th (the carea_max/smidx_coef threshold-mode note).

CLAUDE.md: 147 -> ~85 lines.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Slim `README.md` (commit 3 of 3)

**Files:**
- Modify: `README.md` (430 → ~280 lines)

### Pre-slim content map

| README.md current | Lines | Fate |
|---|---|---|
| Title + intro | 1–6 | KEEP unchanged |
| `## Setup` | 7–33 | KEEP unchanged |
| `## Project Structure` | 35–77 | SHRINK: keep the 12-line tree, drop verbose per-line comments, add 1-line pointer |
| `## Output Directory Structure` | 79–112 | REMOVE: replace with 3-line summary + link to ARCHITECTURE.md |
| `## Usage` (incl. 4 subsections) | 114–217 | KEEP unchanged |
| `## Custom Fabric` | 219–288 | SHRINK: keep the user workflow; replace the wall-of-text on depstor keys with a 1-line "see ARCHITECTURE.md for per-key requirements" pointer |
| `## Shared rasters pipeline` | 290–320 | SHRINK to ~6-line summary + link |
| `## Depression-storage pipeline` | 322–345 | SHRINK to ~6-line summary + link |
| `## Zonal-pass parameter pipeline` | 347–381 | SHRINK to ~8-line summary + link |
| `## Viewing fabric results` | 383–412 | KEEP unchanged |
| `## Configuration` | 414–419 | KEEP unchanged |
| `## Logging` | 421–426 | KEEP unchanged |
| `## License` | 428–430 | KEEP unchanged |

- [ ] **Step 1: Shrink `## Project Structure` (lines 35–77)**

Replace the entire `## Project Structure` block with a tighter version that keeps the tree but drops the inline per-line annotations (those duplicate the layout already in ARCHITECTURE.md). Use `Edit` to replace from the `## Project Structure` heading through the closing fence ` ``` ` at line 77.

Replacement content:

```markdown
## Project Structure

```
gfv2-params/
├── src/gfv2_params/          # Installable Python package
│   ├── config.py             # Config loading, fabric profile resolution
│   ├── raster_ops.py         # Raster utilities
│   ├── batching.py           # Spatial batching
│   ├── lulc.py               # LULC reclassification helpers
│   ├── depstor.py            # Depression-storage helpers
│   ├── depstor_builders/     # Per-step depstor raster builders
│   ├── depstor_ratios.py     # PRMS Level-5 ratio arithmetic
│   ├── shared_rasters/       # Part 1 CONUS raster builders
│   ├── zonal_runners/        # Part 2 zonal-pass runners
│   ├── log.py                # Logging setup
│   └── download/             # Data download utilities
├── scripts/                  # CLI orchestrators + standalone helpers
├── configs/                  # Per-stage YAML configs (base + shared_rasters/ + depstor/ + zonal/)
├── slurm_batch/              # HPC SLURM batch scripts (RUNME.md is the workflow walkthrough)
├── docs/                     # ARCHITECTURE.md, depstor docs, superpowers/ design tree
├── notebooks/                # Interactive notebooks (fabric_results/, oregon/, _archive/)
├── tests/                    # Unit tests
├── pyproject.toml            # Package + pixi config
├── pixi.lock                 # Pinned pixi environment
└── environment.yml           # Legacy conda environment (deprecated fallback)
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the orchestrator +
builder + unified-config pattern that shapes `src/gfv2_params/`,
`scripts/`, and `configs/`.
```

- [ ] **Step 2: Replace `## Output Directory Structure` (lines 79–112) with a brief pointer**

Use `Edit` to replace the entire `## Output Directory Structure` block (heading + intro sentence + the ~30-line data_root tree) with:

```markdown
## Output Directory Structure

All source data and outputs live under `data_root` (set in
`configs/base_config.yml`) in the `input/` → `shared/` → `{fabric}/`
layout. See [`docs/ARCHITECTURE.md#data-root-layout-the-key-invariant`](docs/ARCHITECTURE.md#data-root-layout-the-key-invariant)
for the canonical tree.
```

- [ ] **Step 3: Shrink `## Custom Fabric` depstor-keys wall (lines ~219–288)**

The "Pre-merged fabric" step 1 currently contains a ~50-line wall of prose
describing each required profile key. Replace it with a 5-line summary
pointing at the canonical per-key table in ARCHITECTURE.md. Use `Edit`
targeting just step 1's prose paragraph (keep the numbered structure + the
surrounding `1.`/`2.`/`3.` items intact).

Find the block starting with:
```
1. Register the fabric and scaffold its output dirs in one step:
   `pixi run init-data-root --add-fabric oregon` appends a profile stub under
```

…and ending with:
```
   `docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md`.
```

Replace the entire step 1 prose body (everything between `1.` and the start of `2. Place the fabric gpkg ...`) with:

```markdown
1. Register the fabric and scaffold its output dirs:
   `pixi run init-data-root --add-fabric oregon` appends a profile stub under
   `fabrics:` in `configs/base_config.yml`. Then fill the stub's TODO
   placeholders. **All shared, required fabric inputs live in the profile**
   — see [`docs/ARCHITECTURE.md#required-profile-keys`](docs/ARCHITECTURE.md#required-profile-keys)
   for the per-key required-field table (which keys are always required, which
   are depstor-only, and how to stage the fabric-bounds clip for
   `template_raster`/`fdr_raster` via
   `scripts/clip_shared_to_fabric.py`).

   For non-VPU-01 fabrics with depstor, use `threshold_mode: percentile` in
   `configs/depstor/depstor_rasters.yml` with `twi_raster` pointing at
   `twi_hydrodem.vrt`, and run the `twi_reference` step first (Stage 2a' in
   `slurm_batch/RUNME.md`).
```

- [ ] **Step 4: Shrink `## Shared rasters pipeline` (lines ~290–320)**

Replace the entire `## Shared rasters pipeline` section (heading through the closing paragraph about per-script entrypoints being preserved) with:

```markdown
## Shared rasters pipeline

Part 1 (fabric-independent CONUS raster prep) is driven by
[`scripts/build_shared_rasters.py`](scripts/build_shared_rasters.py) over
[`configs/shared_rasters/shared_rasters.yml`](configs/shared_rasters/shared_rasters.yml),
walking the step DAG via builder modules in
[`src/gfv2_params/shared_rasters/`](src/gfv2_params/shared_rasters/).
Outputs land under `{data_root}/shared/` and are reused by every fabric.

The DAG covers per-VPU NHDPlus prep, border-DEM fill, per-VPU HRU landmask,
masked TWI merge, CONUS VRT assembly, the TWI percentile reference, and
CONUS derived rasters. `compute_dem_derivatives` is an opt-in parallel
open-source TWI pipeline.

See [`docs/ARCHITECTURE.md#orchestrator--builder--unified-config-pattern`](docs/ARCHITECTURE.md#orchestrator--builder--unified-config-pattern)
for the pattern, and the package's
[`__init__.py`](src/gfv2_params/shared_rasters/__init__.py) for per-step
detail.
```

- [ ] **Step 5: Shrink `## Depression-storage pipeline` (lines ~322–345)**

Replace the entire section with:

```markdown
## Depression-storage pipeline

Part 2a (per-fabric depstor) is driven by two orchestrators over two
unified configs:

- [`scripts/build_depstor_rasters.py`](scripts/build_depstor_rasters.py)
  + [`configs/depstor/depstor_rasters.yml`](configs/depstor/depstor_rasters.yml)
  → 10-step raster DAG via
  [`src/gfv2_params/depstor_builders/`](src/gfv2_params/depstor_builders/).
- [`scripts/derive_depstor_params.py`](scripts/derive_depstor_params.py)
  + [`configs/depstor/depstor_params.yml`](configs/depstor/depstor_params.yml)
  → 10 fractions + 6 PRMS Level-5 ratios. The slurm wrapper
  [`slurm_batch/submit_depstor_params.sh`](slurm_batch/submit_depstor_params.sh)
  chains 10 zonal arrays → 10 merges → 1 ratios job via `afterok`.

See [`docs/depstor_workflow.md`](docs/depstor_workflow.md) and
[`docs/depstor_port_summary.md`](docs/depstor_port_summary.md) for the
historical port reference. Stage 2d in `slurm_batch/RUNME.md` lists the
build order.
```

- [ ] **Step 6: Shrink `## Zonal-pass parameter pipeline` (lines ~347–381)**

Replace the entire section with:

```markdown
## Zonal-pass parameter pipeline

Part 2b (per-fabric zonal-pass params) is driven by
[`scripts/derive_zonal_params.py`](scripts/derive_zonal_params.py) over
[`configs/zonal/zonal_params.yml`](configs/zonal/zonal_params.yml),
dispatching every Part-2 param type (`elevation`, `slope`, `aspect`,
`soils`, `soil_moist_max`, `lulc_{nhm_v11,nalcms,nlcd,foresce}`, `ssflux`)
into the matching `run_*_batch` function in
[`src/gfv2_params/zonal_runners/`](src/gfv2_params/zonal_runners/) via the
package's `BATCH_RUNNERS` dispatch table.

Three modes: `--mode zonal --param <name> --batch_id <N>`,
`--mode merge --param <name>`, `--mode build_weights` (CONUS-once ssflux
prereq).

The slurm wrapper
[`slurm_batch/submit_zonal_params.sh`](slurm_batch/submit_zonal_params.sh)
loops every entry in `params:` and chains per-param array + merge jobs via
`afterok`. When an entry carries `depends_on: build_weights` (typically
`ssflux`), the wrapper first submits `build_zonal_weights.batch` and
chains ssflux on its `afterok`.

```bash
bash slurm_batch/submit_zonal_params.sh \
    {data_root}/gfv2_vpu01/batches gfv2_vpu01 configs/base_config.yml
```

See [`docs/ARCHITECTURE.md#orchestrator--builder--unified-config-pattern`](docs/ARCHITECTURE.md#orchestrator--builder--unified-config-pattern)
for the pattern.
```

- [ ] **Step 7: Verify line count + section structure**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
wc -l README.md
grep -n "^##" README.md
```

Expected:
- Line count: 250–290 (was 430; target ~280).
- All 16 `##` sections still present, in the same order. None deleted.

If the line count is far off, recheck the Edits.

- [ ] **Step 8: Verify all links resolve**

```bash
# Internal repo links from README
for link in $(grep -oE '\]\([^)]+\)' README.md | sed 's/^](//; s/)$//' | grep -v '^http' | grep -v '^#'); do
    if [ -f "$link" ] || [ -d "$link" ]; then
        echo "  ✓ $link"
    else
        echo "  ✗ $link MISSING"
    fi
done | head -30
echo "---"
# Confirm the anchor links to ARCHITECTURE.md sections target real headings
grep -oE "ARCHITECTURE.md#[a-z0-9-]+" README.md | sort -u
echo "---"
# Confirm those anchor IDs exist (GitHub auto-anchors lowercase + hyphens)
grep -oE "^#{2,3} [a-zA-Z]" docs/ARCHITECTURE.md | sort -u | head
```

Spot-check at minimum:
- `docs/ARCHITECTURE.md` exists
- `docs/ARCHITECTURE.md#data-root-layout-the-key-invariant` resolves (heading "Data-root layout (the key invariant)" → GitHub anchor: `data-root-layout-the-key-invariant`)
- `docs/ARCHITECTURE.md#required-profile-keys` resolves (heading "Required profile keys" → `required-profile-keys`)
- `docs/ARCHITECTURE.md#orchestrator--builder--unified-config-pattern` resolves (heading "Orchestrator + builder + unified-config pattern" → `orchestrator--builder--unified-config-pattern` — double-hyphens because GitHub collapses ` + ` to `-` *per character*, so `a + b` becomes `a--b`).

If any anchor is wrong, fix the link to match GitHub's anchor-generation rules.

- [ ] **Step 9: Pre-commit + stage + commit (3 of 3)**

```bash
git add README.md
pixi run -e dev pre-commit run --files README.md 2>&1 | tail -6
git commit -m "$(cat <<'EOF'
docs(README): slim pipeline sections; link to ARCHITECTURE.md (T2-C)

README.md grew to 430 lines with 7 sections (Project Structure, Output
Directory Structure, Custom Fabric depstor-keys wall, Shared rasters
pipeline, Depression-storage pipeline, Zonal-pass parameter pipeline)
covering architectural content now consolidated in docs/ARCHITECTURE.md.

Slim each of those sections to a 5-10 line summary + a link to the
relevant ARCHITECTURE.md anchor:

- Project Structure: kept the 12-line tree (it's a user-facing landmark)
  but dropped the inline per-line annotations that duplicate the data-root
  layout in ARCHITECTURE.md; added a 1-line pointer.
- Output Directory Structure: removed the ~30-line tree; replaced with a
  3-line summary + link.
- Custom Fabric (step 1): replaced the ~50-line wall-of-prose describing
  each depstor key with a 5-line summary pointing at the per-key required-
  field table in ARCHITECTURE.md.
- Shared rasters pipeline / Depression-storage pipeline / Zonal-pass
  parameter pipeline: each shrinks to ~6-10 line summary + link to
  ARCHITECTURE.md and the package's __init__.py.

Kept verbatim:
- Setup, Usage (the 4-step workflow + single-batch debug)
- Viewing fabric results, Configuration, Logging, License

Net: 430 -> ~280 lines. All 16 ## sections still present, in the same
order. The first 200 lines of README are now firmly focused on setup +
run, not architectural context.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Update `docs/superpowers/INDEX.md` cross-reference (optional 4th commit)

**Files:**
- Modify: `docs/superpowers/INDEX.md`

INDEX.md's "Cross-references" section currently lists CLAUDE.md / README / RUNME / depstor docs. Add ARCHITECTURE.md as the first entry.

- [ ] **Step 1: Edit INDEX.md "Cross-references" section**

Find the existing "Cross-references" section and add ARCHITECTURE.md as the first bullet:

```markdown
- Project architecture (canonical): [`docs/ARCHITECTURE.md`](../ARCHITECTURE.md)
- Project conventions and architectural invariants: [`CLAUDE.md`](../../CLAUDE.md)
- User-facing setup + usage: [`README.md`](../../README.md)
- HPC workflow walkthrough: [`slurm_batch/RUNME.md`](../../slurm_batch/RUNME.md)
- Pipeline reference docs (live, not snapshots): [`docs/depstor_workflow.md`](../depstor_workflow.md), [`docs/depstor_port_summary.md`](../depstor_port_summary.md), [`docs/depstor_vpu01_validation_results.md`](../depstor_vpu01_validation_results.md)
```

- [ ] **Step 2: Pre-commit + commit**

```bash
git add docs/superpowers/INDEX.md
pixi run -e dev pre-commit run --files docs/superpowers/INDEX.md 2>&1 | tail -4
git commit -m "$(cat <<'EOF'
docs(superpowers/INDEX): add ARCHITECTURE.md to cross-references (T2-C)

The new docs/ARCHITECTURE.md is the canonical architecture source; add it
as the first cross-reference in the INDEX so a fresh visitor lands there
when looking for "how is this project shaped."

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Verify branch state + STOP for user push approval

**Files:** no edits — orchestration only.

- [ ] **Step 1: Show the commit ladder**

```bash
cd /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params
git log --oneline main..HEAD
git status --short  # expect empty
```

Expected (newest first):
```
<sha4> docs(superpowers/INDEX): add ARCHITECTURE.md to cross-references (T2-C)   ← optional 4th commit
<sha3> docs(README): slim pipeline sections; link to ARCHITECTURE.md (T2-C)
<sha2> docs(CLAUDE): slim architecture sections; link to ARCHITECTURE.md (T2-C)
<sha1> docs(architecture): add canonical ARCHITECTURE.md (T2-C)
d593b63 docs(spec): consolidate architecture knowledge into ARCHITECTURE.md (T2-C)
```

- [ ] **Step 2: Final cross-verification**

```bash
wc -l docs/ARCHITECTURE.md CLAUDE.md README.md
```

Expected approximately:
```
~200 docs/ARCHITECTURE.md
 ~85 CLAUDE.md
~280 README.md
```

- [ ] **Step 3: Final link check on the new + slimmed docs**

```bash
# Every relative link from each file
for f in docs/ARCHITECTURE.md CLAUDE.md README.md docs/superpowers/INDEX.md; do
    echo "=== $f ==="
    for link in $(grep -oE '\]\([^)#]+\)' "$f" | sed 's/^](//; s/)$//' | grep -v '^http'); do
        # Resolve relative to file's dir
        target=$(cd "$(dirname $f)" && realpath -m "$link" 2>/dev/null)
        if [ -e "$target" ]; then :; else
            echo "  ✗ $link → $target MISSING"
        fi
    done
done
echo "(empty = clean)"
```

- [ ] **Step 4: STOP — surface to user for push approval**

Surface:
- The 3 (or 4) commit ladder
- The wc -l numbers
- The link-check result
- Ask for push approval

Once approved:

- [ ] **Step 5: Push**

```bash
git push -u origin docs/architecture-md
```

- [ ] **Step 6: Open PR**

```bash
gh pr create --base main --head docs/architecture-md \
  --title "docs(architecture): canonical ARCHITECTURE.md + slim CLAUDE.md/README (T2-C)" \
  --body "$(cat <<'EOF'
Tier-2-C from the [fresh-eyes repo evaluation](docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md). Consolidates architecture knowledge into one canonical doc; slims CLAUDE.md and README to link to it. Also satisfies the per-key fabric-profile table recommendation from the same review.

## What

3 (or 4) atomic commits in one PR:

1. \`docs(architecture)\` — add new \`docs/ARCHITECTURE.md\` (~200 LOC)
2. \`docs(CLAUDE)\` — slim CLAUDE.md (147 → ~85 lines); keep Claude-rules and the 3 gotchas
3. \`docs(README)\` — slim README (430 → ~280 lines); keep Setup, Usage, Viewing fabric results unchanged
4. \`docs(superpowers/INDEX)\` — add ARCHITECTURE.md to cross-references

Each commit is independently revertible.

## What's in ARCHITECTURE.md

- Overview (1 paragraph)
- Data-root layout — the \`input/\` → \`shared/\` → \`{fabric}/\` invariant
- Part 1 vs Part 2 split
- **Orchestrator + builder + unified-config pattern** — the 4-stage table (canonical statement)
- **Fabric profiles** — single source of truth + the **per-key required-field table**
- **Non-obvious conventions & gotchas** — clip lattice, land masking, WBT predictor=2, carea threshold mode
- How to add a new pipeline step (4-edit recipe)
- Cross-references

## What's NOT changed

- Source code (zero code touched)
- \`slurm_batch/RUNME.md\` (the workflow walkthrough; out of scope)
- The two package \`__init__.py\` docstrings (useful per-package context, stay as-is)
- README's Setup + Usage sections (the load-bearing user flow)
- CLAUDE.md's Environment + Working-in-this-repo + Code conventions sections

## Invariants

- **No information lost** — KEEP/COLLAPSE/MOVED line-range maps in the spec; spec-reviewer checklist verifies each line is accounted for.
- **CLAUDE.md remains usable as Claude project instructions** — the 3 gotchas stay quick-reference (Claude needs them at-top-of-mind, not behind a link).
- **README's first 200 lines stay focused on setup + run.**
- All Markdown links verified to resolve.

## Test plan

- [ ] CI green (no code touched; pre-commit + lint passes)
- [x] All relative links resolve in all 4 modified files
- [x] CLAUDE.md sections in expected order; load-bearing sections untouched
- [x] README's 16 \`##\` sections all present in the original order
- [x] Pre-commit clean

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Verification matrix (spec → tasks)

| Spec section | Task |
|---|---|
| Add `docs/ARCHITECTURE.md` (~200 lines) | Task 2 |
| Slim CLAUDE.md (architecture sections → 3-line summary + link) | Task 3 |
| Slim README (5 sections shrunk; usage flow preserved) | Task 4 |
| Per-key fabric-profile table | Task 2 Step 1 (in ARCHITECTURE.md "Required profile keys") |
| Update INDEX.md cross-references | Task 5 |
| Invariant: no information lost | Task 4 Step 7 + Task 6 Step 2 (line counts + section grep) |
| Invariant: CLAUDE.md still works as Claude instructions | Task 3 content map (KEEP for Env / Testing / Working-in-this-repo / Code conventions) |
| Invariant: README first 200 lines focused on setup+run | Task 4 keeps lines 1–217 unchanged |
| Invariant: no code change | implicit (no source file touched) |
| Commit shape: 3 (or 4) atomic | Task 2/3/4 (and 5) each produce 1 commit |
| Risk: load-bearing rule dropped from CLAUDE.md | Task 3 pre-slim content map enumerated |
| Risk: load-bearing instruction dropped from README | Task 4 pre-slim content map enumerated |
| Risk: anchor links wrong (GitHub heading anchor rules) | Task 4 Step 8 verifies anchors |

---

## Notes for the implementer

- **The KEEP/COLLAPSE/MOVED line-range maps in this plan are based on the line numbers in the pre-slim files at the time the spec was written.** If anyone else has edited CLAUDE.md or README.md since the spec was committed (`d593b63`), re-verify line numbers before applying Edits.
- **GitHub Markdown anchors:** lowercase, spaces and `+` and `/` become `-`, multiple-character punctuation can become multiple hyphens. The example `Orchestrator + builder + unified-config pattern` heading → anchor `orchestrator--builder--unified-config-pattern` (double-hyphens because each `+` becomes `-` and the surrounding spaces also become `-`). Verify by viewing the rendered ARCHITECTURE.md on GitHub after push.
- **Pre-commit may reorder anything?** Probably not in markdown files. If something does change, accept the reformat.
- **Do NOT push or open the PR without user approval** (Task 6 Step 4 is the gate).
