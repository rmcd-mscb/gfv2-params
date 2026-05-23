# `docs/superpowers/` index

A catalogue of the design artifacts under this directory: specifications,
implementation plans, and repo reviews produced by the
[superpowers](https://github.com/anthropic-experimental/claude-plugins) skills
during collaborative work on this project. As of 2026-05-23 the tree contains
**8 specs, 10 plans, and 1 review** spanning March → May 2026.

If you're a new contributor (human or agent) trying to understand what's been
designed/built and why, **start here**.

## What lives here

| Subdir | Purpose | Produced by |
|---|---|---|
| `specs/` | The "what + why" — design specifications agreed-upon before implementation. Each spec frames the problem, lists invariants, and scopes what's in/out. | [`superpowers:brainstorming`](https://github.com/anthropic-experimental/claude-plugins) skill |
| `plans/` | The "how" — step-by-step implementation plans derived from a spec. Each plan is a checklist of bite-sized tasks with exact code, commands, and verification steps. | [`superpowers:writing-plans`](https://github.com/anthropic-experimental/claude-plugins) skill |
| `reviews/` | Repo-wide audits, retrospective evaluations, or post-merge analyses. | various — most recently [`superpowers:writing-skills`](https://github.com/anthropic-experimental/claude-plugins)–style multi-iteration self-review |

The intended flow:

```
brainstorming → spec → writing-plans → plan → subagent-driven-development
                                                  ↓
                                             working code + tests
                                                  ↓
                                                 PR
```

Specs and plans are **dated snapshots** — they freeze the state of the world
at the moment they were written. They are NOT updated after the work lands;
the code becomes the source of truth. If a stale reference to (e.g.)
`zonal_runners.py` shows up in an older spec, that's not a bug — it's the
record of what the repo looked like then.

## Current status (as of 2026-05-23)

The [2026-05-23 fresh-eyes review](reviews/2026-05-23-repo-fresh-eyes.md)
is the most recent project-wide audit. Its 15-item action list has been
worked through in this order:

| Item | Status | PR |
|---|---|---|
| Tier 1 (T1-A through T1-F): doc refresh, `_legacy/` cleanup, pre-commit cleanup, `submit_jobs.sh` fix, `node_modules/` gitignore, `nbstripout` | ✅ landed | [#105](https://github.com/rmcd-mscb/gfv2-params/pull/105) |
| T2-A — split `zonal_runners.py` into a package | ✅ landed | [#106](https://github.com/rmcd-mscb/gfv2-params/pull/106) |
| T2-E — promote depstor helpers (closes #64) | ✅ landed | [#107](https://github.com/rmcd-mscb/gfv2-params/pull/107) |
| T2-B — archive scratch notebooks | ✅ landed | [#108](https://github.com/rmcd-mscb/gfv2-params/pull/108) |
| T2-D — `docs/superpowers/INDEX.md` (this file) | in flight | (this PR) |
| T2-C — `docs/ARCHITECTURE.md` + slim CLAUDE.md/README | in flight | (this branch) |
| Tier 3 (discussion items) | open | — |

## Topical index (specs + plans)

Most recent first. Each row pairs the spec (the "what + why") with the plan
(the "how"); both are not always present — many older topics have plans
without specs (the brainstorming step pre-dates the convention) or vice versa.

| Date | Topic | Spec | Plan | Landed |
|---|---|---|---|---|
| 2026-05-23 | **Promote depstor helpers** — eliminate 3 duplicate copies of `_assert_aligned` / `_uint8_binary_profile` by promoting them to public API in `depstor.py`; refactor `write_uint8_binary` to use the shared profile helper. Closes issue #64. | [spec](specs/2026-05-23-promote-depstor-helpers-design.md) | [plan](plans/2026-05-23-promote-depstor-helpers.md) | [#107](https://github.com/rmcd-mscb/gfv2-params/pull/107) |
| 2026-05-23 | **Zonal runners split** — split the 638-LOC `zonal_runners.py` into a 7-module `zonal_runners/` package mirroring `depstor_builders/`; move `BATCH_RUNNERS` dispatch table into the package. Restores symmetry between the two Part-2 pipeline halves. | [spec](specs/2026-05-23-zonal-runners-split-design.md) | [plan](plans/2026-05-23-zonal-runners-split.md) | [#106](https://github.com/rmcd-mscb/gfv2-params/pull/106) |
| 2026-05-22 | **carea threshold sweep** — build a per-HRU TWI-histogram artifact + marimo notebook for interactively tuning the `carea_max`/`smidx_coef` thresholds without re-running the cluster pipeline. The marimo tool lives at `notebooks/carea_threshold_sweep.py`. | [spec](specs/2026-05-22-carea-threshold-sweep-design.md) | [plan](plans/2026-05-22-carea-threshold-sweep.md) | [#96](https://github.com/rmcd-mscb/gfv2-params/pull/96) |
| 2026-05-21 | **TWI percentile carea/smidx** — add `threshold_mode: percentile` to `carea_map` so cutoffs are derived from each TWI source's own distribution (makes CONUS-complete `twi_hydrodem.vrt` safe to use); finish the ArcPy `twi.vrt` staging for VPUs 02–18. Closes #94 and #55 Stage 1. | [spec](specs/2026-05-21-carea-smidx-twi-percentile-design.md) | [plan](plans/2026-05-21-twi-percentile-carea-smidx.md) | [#95](https://github.com/rmcd-mscb/gfv2-params/pull/95) |
| 2026-05-17 | **Step 4 — zonal-pass consolidation** — consolidate the Part 2 zonal-pass into a single orchestrator (`derive_zonal_params.py`) + unified config (`zonal_params.yml`) + library functions in `zonal_runners.py`, mirroring the Part 1 + depstor pattern. Introduced the original `zonal_runners.py` later split by T2-A. | _(plan-only)_ | [plan](plans/2026-05-17-step4-zonal-consolidation.md) | [#83](https://github.com/rmcd-mscb/gfv2-params/pull/83) |
| 2026-05-15 | **Depstor consolidation** — replace 40+ small files of the depstor pipeline with 2 orchestrators + 2 unified configs + a `depstor_builders/` package. The precedent that every later "orchestrator + builder" refactor mirrors. | _(plan-only)_ | [plan](plans/2026-05-15-depstor-consolidation.md) | [#72](https://github.com/rmcd-mscb/gfv2-params/pull/72) |
| 2026-04-11 | **Border DEM fix** — add Copernicus GLO-30 border DEM fill for HRUs crossing into Canada/Mexico, where NHDPlus DEM coverage ends at the US border. | [spec](specs/2026-04-11-border-dem-fix-design.md) | [plan](plans/2026-04-11-border-dem-fix.md) | [#35](https://github.com/rmcd-mscb/gfv2-params/pull/35) (plus follow-up [#76](https://github.com/rmcd-mscb/gfv2-params/pull/76)) |
| 2026-04-02 | **Multi-source LULC parameterization** — add 7 per-HRU LULC parameters from any of FORE-SCE / NLCD / NALCMS sources via a crosswalk-mediated, source-agnostic architecture. Lives at [`src/gfv2_params/lulc.py`](../../src/gfv2_params/lulc.py) + the per-source configs under `configs/shared_rasters/lulc/`. | [spec](specs/2026-04-02-lulc-parameterization-design.md) | [plan](plans/2026-04-02-lulc-parameterization.md) | [#25](https://github.com/rmcd-mscb/gfv2-params/pull/25) |
| 2026-03-26 | **Spatial batching + fabric namespacing** — replace per-VPU chunking with spatial (KD-tree recursive bisection) batching for parameter generation; introduce fabric namespacing to isolate per-fabric outputs; reorganize the data directory by provenance (input/intermediates/outputs). | [spec](specs/2026-03-26-spatial-batching-fabric-design.md) | [plan](plans/2026-03-26-spatial-batching-fabric.md) | [#23](https://github.com/rmcd-mscb/gfv2-params/pull/23) |
| 2026-03-23 | **Repo restructure** — make the repo an installable Python package (`gfv2_params`); add custom-fabric support and structured logging. The foundation everything else builds on. | [spec](specs/2026-03-23-repo-restructure-design.md) | [plan](plans/2026-03-23-repo-restructure.md) | [#19](https://github.com/rmcd-mscb/gfv2-params/pull/19) |

## Reviews

| Date | Topic | Doc | Drove |
|---|---|---|---|
| 2026-05-23 | **Fresh-eyes repo evaluation** — 5-iteration autonomous review covering clean structure / non-developer usability / long-term maintenance / plug-ability / docs / readability for geoscientists. Produced a 14-row signal table, ranked hotspots A–H + Z, and a 15-item action list across 3 tiers. | [review](reviews/2026-05-23-repo-fresh-eyes.md) | PRs #105–#108 (Tier 1 + T2-A/B/E) |

## How to add a new spec / plan / review

**Naming convention:** `YYYY-MM-DD-<topic>[-design].md` (specs use the
`-design` suffix; plans and reviews don't). Use the date the doc was started,
not when the underlying work landed.

**Where:**
- New spec → `docs/superpowers/specs/2026-MM-DD-<topic>-design.md`
- New plan → `docs/superpowers/plans/2026-MM-DD-<topic>.md`
- New review → `docs/superpowers/reviews/2026-MM-DD-<topic>.md`

**Recommended flow** (matches the established cadence):
1. `superpowers:brainstorming` (asks 2–4 clarifying questions, presents the design, writes the spec)
2. `superpowers:writing-plans` (turns the spec into a checklist of bite-sized tasks)
3. `superpowers:subagent-driven-development` (executes the plan with two-stage review per task)
4. Update this `INDEX.md` with the new entry once the PR lands (paired commit with the work, or a follow-up housekeeping PR)

**The atomic-commit + scope-expansion-callout rule from CLAUDE.md applies to
all three** — split combined fixes into separate commits before pushing, and
lead PR descriptions with a scope-expansion callout if source changes exceed
what the spec/plan originally scoped.

## Cross-references

- Project architecture (canonical): [`docs/ARCHITECTURE.md`](../ARCHITECTURE.md)
- Project conventions and Claude project rules: [`CLAUDE.md`](../../CLAUDE.md)
- User-facing setup + usage: [`README.md`](../../README.md)
- HPC workflow walkthrough: [`slurm_batch/RUNME.md`](../../slurm_batch/RUNME.md)
- Pipeline reference docs (live, not snapshots): [`docs/depstor_workflow.md`](../depstor_workflow.md), [`docs/depstor_port_summary.md`](../depstor_port_summary.md), [`docs/depstor_vpu01_validation_results.md`](../depstor_vpu01_validation_results.md)

If you're a fresh visitor and want the single best entry point to "what does
this project do and how does it run," start at [`README.md`](../../README.md);
if you want "why is the code shaped the way it is," start at the most-recent
[fresh-eyes review](reviews/2026-05-23-repo-fresh-eyes.md) and trace back
through the topical index above.
