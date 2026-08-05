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

### Triage first: verify the issue's PREMISE, not just its cites

Before working a filed issue, check whether what it describes still exists. Not
only its `file:line` citations (those drift) — the **premise**.

The depstor classifier is the specific hazard: it was rewritten repeatedly
(#145 → #152 → #158 → #161 → #187), so an issue filed against it more than a few
weeks earlier is suspect. On 2026-08-04, three of three triaged issues had been
overtaken:

- **#156** proposed `dprst = wbody_binary & ~connected_wbody` — verbatim the
  global per-cell carve that #145/#158/#161 evaluated and **rejected**.
- **#155** asked whether NHD flowline permanence should gate promotion — but
  #187 moved the on-stream source to the model's `nsegment` network, which
  carries no `FCode` at all.
- **#188** reported a classifier gap that a D8 trace showed was correct
  behaviour.

All three read as authoritative and quantified. The evidence in them was real;
the framing had expired.

Checks that catch this cheaply:

1. Read the builder the issue targets **as it exists now**, plus CLAUDE.md's
   bullet for that subsystem — those bullets record *rejected designs*, which is
   what a stale issue most often re-proposes. Tells: "considered and rejected",
   "deliberately narrower than", "do NOT restore".
2. Treat the issue's "candidate fix" as the highest-risk section.
3. **Re-measure any number you rely on.** The global-carve figure was quoted in
   five places, disagreed with itself, and matched no current measurement — while
   being the sole justification for a design decision.
4. Closing as not-a-defect is a good outcome. State what changed and cite the
   PR that made it obsolete.

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
- PRMS parameter index (spec-driven, not from this tracker):
  - PR #202 — hand-written `docs/parameter_index.md`; fixed the add-a-param trap
    (`submit_zonal_params.sh` carries a hardcoded `PARAMS` array the docs claimed
    was read from the YAML) + `tests/test_submit_wrapper_param_lists.py`
  - PR #203 — `prms:` metadata on all 19 declared entries, two guards, generated
    index, `hru_slope` emitted in rise/run, `op_flow_thres` into `merged/`

### Up next (priority order)
- CFG-1 — remove commented opt-in keys from fabric profiles in base_config.yml
- CFG-2 — eliminate gfv2_dev profile duplication with YAML anchors
- CODE-1 — deduplicate `_resolve_nested` into config.py
- CODE-2 — move migration/diagnostic scripts to scripts/migrate/ and scripts/diagnose/

## Repo facts

- Remote: `git@github.com:rmcd-mscb/gfv2-params.git`
- Default branch: `main`
- CI runs `pytest tests/` on push to main and every PR — and **only on those two
  triggers**. Pushing a feature branch runs NOTHING; the Actions API returns zero
  runs, which reads like "queued" but never resolves. Open the PR to start the gate.
- CI runs **pytest only** — no pre-commit, no ruff, no yamllint, no shellcheck, no
  docs-build. `pixi run -e dev pre-commit run --files <changed>` locally is the only
  lint that will ever happen.
- **shellcheck cannot run on the HPC.** Its pre-commit hook shells out to docker
  (`docker system info`) and docker is not installed, so the hook errors rather than
  passing. Combined with the point above, `.sh`/`.batch` files are linted by nothing
  on any machine anyone actually uses. Substitute `bash -n` plus a dry run against a
  stubbed `sbatch` (a script on PATH that echoes `Submitted batch job 123`), which
  catches dependency-chain and expansion mistakes `bash -n` cannot — and say
  explicitly that shellcheck did not run.
- `pre-commit run --all-files` does NOT pass on main today: 4 pre-existing `E402`s in
  `scripts/derive_depstor_params.py`'s startup-heartbeat import block and 14 yamllint
  `braces` errors in `configs/zonal/zonal_params.yml`'s `flux_params:` block. Check
  new findings against `main` (via `git stash`) before attributing them to your diff.
- Do not run pytest on the HPC login node
- `sbatch slurm_batch/ab_drains_to_dprst.batch [VPU] [FABRIC]` — the #147 FDR
  A/B (production vs fill vs breach) on one VPU; defaults VPU 16 / gfv2_dev.
  Already run; result recorded in CLAUDE.md's FDR bullet (production ≈ breach,
  so do not swap the FDR chasing contributing-area magnitude)
- Environment: pixi (`pyproject.toml`); SLURM batches use `pixi run --as-is`
- `pixi run data-root` — prints `data_root` from base_config.yml (added PR #194)
- `python scripts/build_parameter_index.py` — regenerates the three marked tables in
  `docs/parameter_index.md` from the `prms:` blocks in `configs/`. `--check` exits 1
  if stale, and `tests/test_params_index.py::test_generated_index_is_up_to_date`
  enforces it, so a config edit without a regenerate fails CI (added PR #203)
- `scripts/derive_depstor_params.py --mode copy_constants` — copies every
  `constants:` entry into `merged/`. Chained `afterok` by
  `submit_depstor_params.sh`; do not un-chain it (added PR #203)
- `pixi run init-data-root` — scaffolds the data directory tree
