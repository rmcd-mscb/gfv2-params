# `docs/ARCHITECTURE.md` design (T2-C)

**Status:** approved 2026-05-23.
**Closes:** T2-C from
[`docs/superpowers/reviews/2026-05-23-repo-fresh-eyes.md`](../reviews/2026-05-23-repo-fresh-eyes.md)
(also satisfies the per-key fabric-profile table recommendation from the
same review).

## Why

Architecture knowledge currently lives in **three places** with significant overlap:

- `CLAUDE.md` (147 lines): a dedicated "Architecture" section with
  orchestrator+builder pattern + fabric profiles + non-obvious gotchas.
- `README.md` (430 lines): three pipeline sections (Shared rasters /
  Depression-storage / Zonal-pass) that restate the orchestrator+builder
  pattern; a "Project Structure" section; an "Output Directory Structure"
  section; a "Custom Fabric" section.
- The two builder package `__init__.py` docstrings
  ([`depstor_builders`](../../../src/gfv2_params/depstor_builders/__init__.py),
  [`shared_rasters`](../../../src/gfv2_params/shared_rasters/__init__.py))
  re-describe the per-package shape.

The fresh-eyes review's Tier-2-C action: **consolidate architecture
knowledge into one canonical doc**, then slim the others to link to it. A
secondary action from the same review (a per-key fabric-profile table in
README's "Custom Fabric") fits naturally in the new doc's "Fabric profiles"
section, so this PR consolidates both.

The package `__init__.py` docstrings stay as-is — they describe their own
package, not the whole architecture, and they're useful in-context.

## What

### New file: `docs/ARCHITECTURE.md` (~180–220 lines)

Single canonical source for the project's architecture. Sections:

1. **Overview** (1 paragraph) — what the pipeline does, who runs it, where.
2. **Data-root layout** — the `input/` → `shared/` → `{fabric}/` invariant, with
   a concise on-disk tree. Currently duplicated across CLAUDE.md ("Layout of
   `data_root`") and README ("Output Directory Structure").
3. **Part 1 vs Part 2** — the fabric-independent / fabric-dependent split, the
   single `data_root` they share, the natural parallelism boundary.
4. **Orchestrator + builder + unified-config pattern** — the 4-stage table
   that currently lives in CLAUDE.md "Architecture" section, the 3 pipeline
   sections of README, and is implicit in the two builder `__init__.py` docs.
   One canonical statement here; the README sections shrink to "see
   ARCHITECTURE.md" + a 2-line summary.
5. **Fabric profiles — single source of truth** — currently split between
   CLAUDE.md ("Fabric profiles — the single source of truth") and README
   ("Custom Fabric" paragraph 1). Here it gets the **per-key required-field
   table** (which the fresh-eyes review separately recommended for README).
6. **Non-obvious conventions & gotchas** — the 3 hard-won gotchas in
   CLAUDE.md (template-clip lattice, land-masking, WBT predictor=2). Reads
   verbatim from CLAUDE.md so the deduplication is exact.
7. **How to add a new pipeline step** — the 4-edit recipe (builder module +
   `BUILDERS` registration + `STEP_ORDER` entry + config block + test). Same
   recipe for new param families (post-T2-A: `zonal_runners/` is now a
   package, so adding a new `script:` tag follows the same shape).

### `CLAUDE.md` slim (147 → ~85 lines)

**KEEP unchanged:**
- `## What this is` (line 5–10) — Claude needs project context.
- `## Environment & commands` (lines 12–35) — Claude needs to know how to run things.
- `### Testing on the HPC head node` (lines 37–43) — load-bearing rule.
- `## Working in this repo` (lines 119–128) — Claude rules (atomic commits, doc audit, etc.).
- `### Code conventions` (lines 130–145) — Claude rules.

**COLLAPSE:**
- `## Architecture` (lines 45–57) → 3-line summary + link to
  `docs/ARCHITECTURE.md`.
- `### Orchestrator + builder-module pattern` (lines 59–80) → 1-line summary
  + link.
- `### Fabric profiles — the single source of truth` (lines 82–102) → 2-line
  summary + link.
- `## Non-obvious conventions & gotchas` (lines 104–117) → keep the 3
  bullets as a **quick-reference for Claude** (these are load-bearing —
  Claude must always know them). Annotate that ARCHITECTURE.md has the full
  context.

**Net:** ~60 lines removed from CLAUDE.md; the file becomes Claude-rules-focused
rather than half-architecture-doc.

### `README.md` slim (430 → ~280 lines)

**KEEP unchanged:**
- Title + intro (lines 1–6)
- `## Setup` (lines 7–33)
- `## Usage` (lines 114–217) — the 4-step workflow + single-batch debug
- `## Viewing fabric results` (lines 383–412)
- `## Configuration` (lines 414–419)
- `## Logging` (lines 421–426)
- `## License` (line 428–430)

**SHRINK:**
- `## Project Structure` (lines 35–77) → keep the 12-line `gfv2-params/` tree
  (it's a user-facing landmark); drop the per-line comments after each line
  (those duplicate ARCHITECTURE.md); add a 1-line pointer to ARCHITECTURE.md.
- `## Output Directory Structure` (lines 79–112) → remove (now in
  ARCHITECTURE.md); replace with a 3-line summary + link.
- `## Custom Fabric` (lines 219–288) — this is where the most aggressive
  shrink happens. The first paragraph (registering a new fabric) stays.
  The wall of prose describing each required key gets replaced with the
  **per-key table** (also added to ARCHITECTURE.md; the README version is a
  user-facing quick reference). The deeper "why each key matters" content
  moves to ARCHITECTURE.md.
- `## Shared rasters pipeline` (lines 290–320), `## Depression-storage
  pipeline` (lines 322–345), `## Zonal-pass parameter pipeline` (lines
  347–381) → each shrinks to ~5-line summary + link to ARCHITECTURE.md +
  link to the package's `__init__.py` for per-step detail.

**Net:** ~150 lines removed from README; the file becomes "how to set up and
run the pipeline" rather than "set up + run + here's all the design notes."

## File map

| File | Change | Net LOC |
|---|---|---|
| `docs/ARCHITECTURE.md` | new | +~200 |
| `CLAUDE.md` | slim | −~60 |
| `README.md` | slim | −~150 |
| `docs/superpowers/INDEX.md` | one-line update (add ARCHITECTURE.md to the cross-references) | +~1 |

4 files modified/created. Net diff: roughly −10 LOC overall (the new doc
absorbs roughly what the slims remove).

## Invariants

1. **No information lost.** Everything currently in CLAUDE.md, README, or the
   package `__init__.py` docstrings either stays in place, moves to
   ARCHITECTURE.md, or is collapsed to a short summary that links to where
   the full content now lives. The implementer's spec-review checklist
   includes a "before → after" content map.
2. **CLAUDE.md remains usable as Claude project instructions.** Specifically:
   the "Environment & commands", "Testing on the HPC head node", "Working in
   this repo", and "Code conventions" sections are untouched. The 3 most
   load-bearing gotchas stay as a quick-reference in CLAUDE.md (Claude needs
   them at-the-top-of-mind, not behind a link).
3. **README's first 200 lines stay focused on setup + run.** Anyone scanning
   the README to figure out "how do I get started" reaches the actual
   `pixi install` + `init-data-root` + downloads + Part 1 + Part 2 sequence
   without wading through 70 lines of architectural context first. The
   shrunk sections come *after* the usage flow.
4. **No code change.** Pure docs PR. CI's pytest runs are unaffected.
5. **No referenced doc paths break.** The fresh-eyes review's references to
   "CLAUDE.md line 71" (architecture table) etc. point at the line ranges
   in the *snapshot* the review captured; they're intentionally not
   updated. New cross-references in the slimmed CLAUDE.md/README point at
   `docs/ARCHITECTURE.md`.

## Out of scope

- README's "Setup" section (pixi install) — not touched.
- README's "Usage" section — not touched; user-facing flow stays as-is.
- Any restructure of `docs/` beyond adding ARCHITECTURE.md (no
  `docs/architecture/` sub-tree; just the single file).
- `slurm_batch/RUNME.md` — not in scope. RUNME is the workflow walkthrough,
  not the architecture doc; it stays as-is. ARCHITECTURE.md will link to it
  from the appropriate spot.
- The package `__init__.py` docstrings — useful per-package context, stay
  as-is. ARCHITECTURE.md will link to them as the per-package reference.
- The two `docs/depstor_*.md` files — historical pipeline-reference docs;
  out of scope for T2-C (T3 might consider their fate).

## Risks

| Risk | Mitigation |
|---|---|
| Slim accidentally drops a load-bearing rule from CLAUDE.md | Spec-reviewer checklist: read pre-slim CLAUDE.md line-by-line, mark each line as KEEP/COLLAPSE/MOVED. Anything not accounted for is a regression. |
| Slim accidentally drops user-facing instructions from README | Same checklist applied to README. |
| ARCHITECTURE.md's content drifts from the code over time | ARCHITECTURE.md will live next to other reference docs under `docs/`. If a refactor changes the orchestrator+builder shape, the CLAUDE.md "Every code change needs a docs check" rule requires updating ARCHITECTURE.md on the same branch. |
| The per-key fabric-profile table is wrong | Cross-check against `scripts/init_data_root.py` (the canonical source for fabric profile stub generation). |
| Reviewers can't easily verify the slim correctness | Diff is big (~250 LOC removed across 2 files; ~200 added in ARCHITECTURE.md). Split into 3 atomic commits (1: add ARCHITECTURE.md; 2: slim CLAUDE.md; 3: slim README) so each commit's diff is independently reviewable. |

## Commit shape

**3 atomic commits in one PR**:

1. `docs(architecture): add canonical ARCHITECTURE.md (T2-C)` — net +~200 LOC. Just adds the new file; touches nothing else.
2. `docs(CLAUDE): slim architecture sections; link to ARCHITECTURE.md` — net −~60 LOC. Slims CLAUDE.md. Independently reverts cleanly.
3. `docs(README): slim pipeline sections; link to ARCHITECTURE.md` — net −~150 LOC. Slims README. Independently reverts cleanly.

Plus the optional fourth commit (if needed): `docs(superpowers/INDEX): add ARCHITECTURE.md to cross-references`.

## Docs check

Per CLAUDE.md: docs are the whole point. The check is internal — each commit
self-audits via the spec's "before → after content map."

## Audience

Implementer (Claude subagent via subagent-driven-development). Reader is
assumed to know markdown, the project's CLAUDE.md/README/RUNME layout, and
the orchestrator+builder pattern from prior T2 PRs.
