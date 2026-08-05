# Repository Review — Actionable Issues

Generated from a comprehensive review of `gfv2-params`. Issues are grouped by
theme and ordered by priority within each group. Each item is self-contained so
it can be copied into a GitHub/GitLab issue tracker individually.

---

## Usability (Earth Scientist / Non-CS User)

### US-1 · Add a "Quick Start for Scientists" section to RUNME.md
**Status: DONE — PR #192.**
**Priority:** High

RUNME.md is written for engineers who already understand SLURM job arrays,
`afterok` chaining, and pixi environments. An earth scientist running the
pipeline for the first time has no short path to follow.

**Proposed fix:**
Add a collapsible or clearly separated "Quick Start" section at the top of
`slurm_batch/RUNME.md` that covers the CONUS `gfv2` happy path in ~20 lines,
with no inline commentary about opt-in comparisons, memory sizing, or
classifier design. Link to the full steps for anything non-standard.

---

### US-2 · Replace inline bash variable setup with a helper script
**Status: DONE — PR #194.**
**Priority:** High

Step 4 of RUNME.md requires the user to run a Python one-liner inside bash to
extract `data_root` from `base_config.yml`:

```bash
BATCHES=$(pixi run --as-is python -c \
  "import yaml;print(yaml.safe_load(open('configs/base_config.yml'))['data_root'])")/gfv2/batches
```

This is error-prone and opaque to non-programmers.

**Proposed fix:**
Add a small helper script (e.g. `scripts/get_data_root.sh`) or a pixi task
(`pixi run data-root`) that prints `data_root` cleanly, so RUNME.md can read:

```bash
BATCHES=$(pixi run data-root)/gfv2/batches
```

---

### US-3 · Promote the wholesale submit wrappers to the primary path in RUNME.md
**Status: DONE — PR #196.**
**Priority:** Medium

`submit_zonal_params.sh` and `submit_depstor_params.sh` are currently
described as a "Convenience" option at the bottom of Step 4. The per-parameter
manual submission block above them is the primary presentation, which means a
scientist reads ~60 lines of bash before learning the one-command alternative
exists.

**Proposed fix:**
Invert the order: present the wholesale wrappers first as the standard path,
and move the per-parameter manual commands to a collapsible "Run one parameter
at a time" detail block for debugging use.

---

### US-4 · Document which steps are required vs. optional more clearly
**Status: DONE — PR #198.**
**Priority:** Medium

Several steps in RUNME.md are marked "opt-in comparison only" or "(optional)"
but are interspersed with required steps in the same code blocks. A scientist
cannot easily tell what to skip.

**Proposed fix:**
Use a consistent visual marker (e.g. a `> **Optional:**` blockquote) for every
optional step, and group all opt-in comparison staging commands into a single
clearly-labeled optional section rather than inline with required commands.

---

## Configuration

### CFG-1 · Remove committed-but-commented opt-in keys from fabric profiles
**Priority:** High

`base_config.yml` fabric profiles contain commented-out keys
(`connected_comids_table`, `flowthrough_comids_table`) that are "opt-in
comparison only." These are invisible to a new user and easy to accidentally
uncomment in a production run.

**Proposed fix:**
Remove these keys from the production profiles entirely. Document the A/B
comparison workflow in `slurm_batch/HPC_REFERENCE.md` with instructions to
add the keys temporarily when needed, rather than leaving them silently
commented in the canonical config.

---

### CFG-2 · Eliminate `gfv2_dev` profile duplication with YAML anchors
**Priority:** High

The `gfv2_dev` profile duplicates nearly every key from `gfv2`, with only
`hru_gpkg` and `segments_gpkg` pinned to the `gfv2` paths. Any change to the
`gfv2` profile must be manually mirrored to `gfv2_dev`.

**Proposed fix:**
Use YAML anchors to define the shared base once:

```yaml
fabrics:
  gfv2: &gfv2_base
    expected_max_hru_id: 361471
    # ... all shared keys ...

  gfv2_dev:
    <<: *gfv2_base
    hru_gpkg: "{data_root}/gfv2/fabric/gfv2_nhru_merged.gpkg"
    segments_gpkg: "{data_root}/gfv2/fabric/gfv2_nsegment_merged.gpkg"
```

---

### CFG-3 · Move long inline comments out of `base_config.yml` profiles
**Priority:** Low

Several profile keys carry multi-paragraph inline comments explaining NHD
provenance, classifier design decisions, and historical context. This makes
the config file hard to scan and mixes documentation with configuration.

**Proposed fix:**
Replace long inline comments with a single-line reference to the relevant
section of `docs/ARCHITECTURE.md` or `slurm_batch/HPC_REFERENCE.md`. The
detailed explanations already exist in those documents.

---

### CFG-4 · Consolidate config directory structure
**Priority:** Low

There are 7 separate config subdirectories for one pipeline. A scientist
looking for "where do I change the TWI threshold" has no obvious starting
point.

**Proposed fix:**
Add a one-page `configs/README.md` that maps each config file to the pipeline
stage it controls and the script that reads it. This does not require
restructuring the files themselves.

---

## Code Quality and Maintainability

### CODE-1 · Deduplicate `_resolve_nested` across orchestrator scripts
**Priority:** Medium

The `_resolve_nested` function is independently defined in both
`scripts/build_shared_rasters.py` and `scripts/derive_zonal_params.py` with
identical logic.

**Proposed fix:**
Move the canonical implementation into `src/gfv2_params/config.py` and import
it in both scripts.

---

### CODE-2 · Move one-time migration and diagnostic scripts out of `scripts/` root
**Priority:** Medium

`scripts/` contains 25 files, including active pipeline orchestrators alongside
one-time migration tools (`migrate_filled_params.py`,
`migrate_to_shared_layout.py`) and standalone diagnostics
(`find_missing_hru_ids.py`, `build_carea_twi_artifact.py`). This makes it hard
to identify what is part of the active pipeline.

**Proposed fix:**
Move migration scripts to `scripts/migrate/` and diagnostic/one-off scripts to
`scripts/diagnose/` (a subdirectory that already exists for some diagnostics).
Update any references in RUNME.md and HPC_REFERENCE.md.

---

### CODE-3 · Clarify the `dprst_depth` SLURM exception in STEP_ORDER
**Priority:** Medium

`dprst_depth` is registered in `STEP_ORDER` as a normal builder step, but it
cannot be run by the ordinary `build_depstor_rasters.batch` walk at CONUS
scale — it must be pre-run via its own SLURM array. This structural
inconsistency is a trap for future maintainers.

**Proposed fix:**
Add a comment in `depstor_builders/__init__.py` directly above `"dprst_depth"`
in `STEP_ORDER` that explicitly states it is a SLURM-array-only step at CONUS
scale and references `submit_dprst_depth.sh`. Consider also adding a runtime
guard in the orchestrator that warns (or raises) if `dprst_depth` is reached
without a pre-populated `batch_dir`.

---

### CODE-4 · Archive or delete `notebooks/_archive/`
**Priority:** Medium

`notebooks/_archive/` contains ~20 exploratory scripts from earlier
development phases. They are not tests, not documentation, and not production
code. They add noise to code search and create a false impression that they
may be needed.

**⚠️ Superseded in part — do NOT delete wholesale.** `notebooks/_archive/` is
currently the ONLY written record of the known aspect-circularity simplification
(the arithmetic-vs-circular mean of `hru_aspect`). That note is what lets
[#201](https://github.com/rmcd-mscb/gfv2-params/issues/201) be framed as a
*measured limitation* rather than an oversight, and both `CLAUDE.md` and
`docs/superpowers/specs/2026-08-04-prms-parameter-index-design.md` now depend on
the tree surviving. PR #205 excluded it from linting on the same basis.

**Proposed fix (revised):**
Port the aspect-circularity note into `docs/` FIRST — it belongs beside the
`hru_aspect` entry in `docs/parameter_index.md`'s Known gaps. Only then consider
pruning the rest of the directory. Git history is not a sufficient home for a
caveat that a reader needs to find without knowing it exists.

---

## Repository Hygiene

### HYG-1 · Gitignore `logs/`, `site/`, and `.superpowers/sdd/`
**Priority:** High

Three directories contain runtime or generated artifacts that are committed to
the repository:

- `logs/` — SLURM job output files (`job_*.err`, `job_*.out`). These are
  runtime artifacts that change on every run.
- `site/` — Built MkDocs HTML output. Should be generated by CI, not
  committed.
- `.superpowers/sdd/` — AI-generated review diffs, task briefs, and progress
  reports from the development process. These are ~40 files that add noise to
  `git log` and `git grep`.

**Proposed fix:**
Add all three to `.gitignore` and remove the committed files. For `site/`,
add a CI step to build and publish docs on merge to main.

---

### HYG-2 · Rename `CLAUDE.md` to `CONTRIBUTING.md`
**Priority:** Low

`CLAUDE.md` contains valuable project conventions (atomic commits, doc audit
rules, head-node pytest prohibition) but is named after an AI assistant, which
is confusing to human contributors.

**Proposed fix:**
Rename to `CONTRIBUTING.md` and update any references in README.md and
ARCHITECTURE.md. The content is already written as human-facing guidance and
requires no changes.

---

## Architecture / Longer-Term

### ARCH-1 · Investigate Snakemake (or similar) for SLURM orchestration
**Priority:** Medium

The current SLURM orchestration pattern — shell scripts that manually chain
`sbatch --dependency=afterok:$AID` — is fragile, hard to resume after partial
failure, and requires the user to track job IDs manually. A workflow manager
would handle dependency resolution, partial reruns, and job monitoring
automatically.

A Snakemake spike was already investigated
(`docs/superpowers/plans/2026-06-23-snakemake-spike-tjc-stage4.md`). The
existing orchestrator + builder pattern maps cleanly onto Snakemake rules, and
the YAML configs could serve as Snakemake config files with minimal changes.

**Proposed fix:**
Revisit the Snakemake spike. Even a partial adoption — replacing the
`submit_zonal_params.sh` / `submit_depstor_params.sh` wrappers with a
Snakemake workflow for Step 4 — would significantly reduce the manual
job-chaining burden for scientists.

---

### ARCH-2 · Reduce the number of pixi environments
**Priority:** Low

Seven pixi environments (`default`, `dev`, `notebooks`, `docs`, `reference`,
`marp`, `all`) add cognitive overhead for new users. The `reference`
(pywatershed, Python 3.10) and `marp` (slide rendering) environments are
specialized enough that they are rarely needed by pipeline operators.

**Proposed fix:**
Document `reference` and `marp` as "install separately if needed" in README.md
rather than presenting all 7 environments as equally relevant. No code change
required — just documentation.

---

*Review conducted: 2025. See `docs/ARCHITECTURE.md` for the canonical pipeline
description.*
