# gfv2-params — Amazon Q Workspace Rules

## Memory sync

This file (`.amazonq/rules/workflow.md`) and `CLAUDE.md` are kept in sync:
- **This file** is the single source of truth for the issue+PR workflow,
  completed/upcoming issues, and repo facts (pixi tasks, CI, branch conventions).
- **`CLAUDE.md`** holds deep architectural gotchas and code conventions that
  are too detailed for this file.
- When either is updated, sync the other. Completed issues and new pixi tasks
  belong in both; architectural detail belongs only in `CLAUDE.md`.

## Issue + PR workflow

The established pattern for every work item:

1. `git checkout main && git pull origin main` — always start from a fresh main
2. `gh issue create` — create the GitHub issue first, capture the URL
3. `git checkout -b <branch>` — branch name matches the work (e.g. `docs/step4-wholesale-first`, `feat/data-root-pixi-task`)
4. Make changes — use Python scripts in `/tmp/` for multiline string replacements to avoid shell quoting issues
5. `git add <files> && git commit -m "<conventional commit msg>\n\nCloses #<N>"` — atomic commit, closes the issue
6. `git push -u origin <branch>`
7. `gh pr create --title "..." --body "Closes #<N>\n\n## What\n..." --base main`
8. After user confirms merge: `git checkout main && git pull origin main`

## Multiline string replacement

Never use `sed` for multiline replacements. Write a Python script to `/tmp/patch.py`,
use `assert old in text` to verify the match before replacing, then `python3 /tmp/patch.py`.

## Conventional commit prefixes

- `docs(runbook):` — RUNME.md / HPC_REFERENCE.md changes
- `docs(params):` — parameter documentation
- `feat(config):` — pyproject.toml / config changes
- `fix(...):`  — bug fixes

## Active issue tracker

Issues are tracked in `docs/repo_review_issues.md`. Groups: Usability (US-*),
Configuration (CFG-*), Code Quality (CODE-*), Hygiene (HYG-*), Architecture (ARCH-*).

### Completed
- US-1 → PR #192 — Quick Start for Scientists section in RUNME.md
- US-2 → PR #194 — `pixi run data-root` task; replaced 3× Python one-liner in RUNME.md
- US-3 → PR #196 — wholesale submit wrappers promoted to primary path in Step 4
- US-4 → PR #198 — optional steps marked consistently with `> **Optional:**` blockquotes

### Up next (priority order)
- CFG-1 — remove commented opt-in keys from fabric profiles in base_config.yml
- CFG-2 — eliminate gfv2_dev profile duplication with YAML anchors
- CODE-1 — deduplicate `_resolve_nested` into config.py
- CODE-2 — move migration/diagnostic scripts to scripts/migrate/ and scripts/diagnose/

## Repo facts

- Remote: `git@github.com:rmcd-mscb/gfv2-params.git`
- Default branch: `main`
- CI runs `pytest tests/` on push to main and every PR
- Do not run pytest on the HPC login node
- `sbatch slurm_batch/ab_drains_to_dprst.batch [VPU] [FABRIC]` — the #147 FDR
  A/B (production vs fill vs breach) on one VPU; defaults VPU 16 / gfv2_dev.
  Already run; result recorded in CLAUDE.md's FDR bullet (production ≈ breach,
  so do not swap the FDR chasing contributing-area magnitude)
- Environment: pixi (`pyproject.toml`); SLURM batches use `pixi run --as-is`
- `pixi run data-root` — prints `data_root` from base_config.yml (added PR #194)
- `pixi run init-data-root` — scaffolds the data directory tree
