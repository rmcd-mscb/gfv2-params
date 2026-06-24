# Snakemake Spike — Findings (tjc Stage 4)

**Date:** 2026-06-23
**Scope:** Stage-4 zonal (elevation, slope, ssflux) on the tjc fabric (1584 HRUs, 1 batch) via Snakemake ≥8 + `snakemake-executor-plugin-slurm`, comparing outputs to the existing golden tjc pipeline outputs.
**Branch:** `worktree-snakemake-spike`
**Plan:** [2026-06-23-snakemake-spike-tjc-stage4.md](2026-06-23-snakemake-spike-tjc-stage4.md)

## Did it work? — yes, both goals met

### 1. Env / SLURM-plugin integration — RESOLVED POSITIVE (no fallback needed)
- The snakemake controller (workflow pixi env) ran on `login1` and submitted each rule as its own SLURM job via the slurm executor plugin.
- The single-rule gate (elevation batch) ran as SLURM **jobid 202210 on compute node `cn014`**, COMPLETED in 4:36, python step MaxRSS ≈ 960 MB — proof the geo stack (rioxarray/gdptools/exactextract) executed on the compute node.
- Rules shell out via `pixi run --as-is python scripts/...`; **`pixi` resolved on the compute node with no PATH problem.** The plan's `$HOME/.pixi/bin/pixi` fallback (Task 4 Step 3) was **not required** — sbatch's default `--export=ALL` carried the submitting shell's PATH.

### 2. Full DAG + cross-dependency — CORRECT
- Full `all` run: **7/7 steps completed** (exit 0).
- The `ssflux` coupling held: ssflux_batch (jobid 202261) started only after **both** `build_weights` (202233) and the slope `merge_param` (202251) finished; the ssflux `merge_param` (202263) ran last. Monotonic jobids confirm the ordering the Snakefile expresses purely through `input:`/`output:` filenames — no hand-written `afterok`.

### 3. Resumability — CONFIRMED
- Re-running `all` after completion: `Nothing to be done (all requested files are present and up to date).` File-target skip-if-done works out of the box — the headline win over the hand-rolled `submit_zonal_params.sh` dependency bash.

### 4. Parity vs golden — 3/3 IDENTICAL
- `tests/test_snakemake_spike_parity.py` (pure pandas, head-node safe): **3 passed.**
- All three spike merged CSVs match the golden references on HRU-id set, column set, numeric values (rtol 1e-6 / atol 1e-9), and exact non-numeric values.
- Even the on-disk byte sizes are identical (266133 / 252981 / 302018), and the spike wrote to a separate `params_snakemake_spike/` tree, so the golden outputs were never touched (non-destructive guarantee held).

## One real defect found — a plan bug, not just execution

**Solve-group coupling silently mutated the production env.** The plan put the `workflow` pixi env in `solve-group = "default"` (mirroring dev/docs/notebooks). Because snakemake pulls heavier constraints than those features, the shared re-solve **downgraded the default (production) env's pandas 3.0.2 → 2.3.3** and bumped numpy — exactly the env `pixi run --as-is` uses to produce real outputs.

- **Fix:** give `workflow` its **own** solve-group (drop `solve-group` entirely → `workflow = { features = ["workflow"] }`). The default/dev/docs/notebooks/all envs reverted to baseline pandas 3.0.2 / numpy 2.4.3; the downgrade is now confined to the workflow env, which only runs the controller (geo work happens in the frozen default env via `pixi run --as-is`).
- **Also:** snakemake is on **bioconda**, not conda-forge — the `[tool.pixi.workspace] channels` list needed `bioconda` added. Strict channel priority keeps the production geo stack on conda-forge; bioconda supplies only snakemake + its plugins, all confined to the workflow env.
- **Lesson for the full refactor:** any new orchestration env must be solve-group-isolated, and channel additions must be checked for default-env drift (diff `pixi.lock` per-env, not just "does it solve").

## Effort actually spent
- 7 tasks via subagent-driven development; one fix loop (the solve-group defect, caught in Task 1 review before it could pollute later results).
- Wall-clock was dominated by SLURM job runtime, not authoring. At tjc scale the long pole was `build_weights` (6:15, WeightGenP2P over the lithology shapefile) and the per-batch zonal jobs (~4:00 each, mostly geo-library import + exactextract). The full DAG finished in ≈8 min wall including queue + sequential cross-deps.
- The science code (`zonal_runners/`, `derive_zonal_params.py`) needed **zero changes** — confirmed. Snakemake rules just shell to the existing orchestrator.

## Extrapolation to a full refactor
- **Confirmed cheap:** rules wrapping the existing CLIs (Approach A) is a thin, low-risk layer. The 8–15 day Approach-A estimate for the whole pipeline still holds; nothing in the spike surprised upward.
- **One-time setup cost, now paid:** the env/SLURM-plugin integration and the solve-group isolation are pipeline-wide, not per-stage — they don't recur as you add stages.
- **Per-stage cost going forward:** add a few rules + per-rule `resources:` (encoding the CONUS mem ceilings from CLAUDE.md for the depstor/shared-raster stages) ≈ well under a day per stage once the scaffold exists.
- **Still unproven (out of spike scope):** CONUS-scale resource tuning (the 384G/96G ceilings), the per-VPU fan-out width on gfv2, and the interactive marimo `merge_vpu_targets` step (must stay an external input — Snakemake can't own an interactive notebook).

## Recommendation
**Proceed with the phased A→B refactor**, adopting Snakemake on the next greenfield stage first. The two headline unknowns are retired: (1) pixi-under-the-slurm-plugin works with no special handling, and (2) the Snakemake-driven pipeline reproduces existing outputs exactly. Roll the solve-group isolation pattern and the per-env lock-drift check into the refactor's setup step so the production env is never perturbed.

## Docs audit (per CLAUDE.md "every code change needs a docs check")
This is a throwaway spike confined to `workflow/spike/`, a new `params_snakemake_spike/` output tree, an additive `workflow` pixi env, and one new test. It does **not** change any production pipeline stage, so `slurm_batch/RUNME.md`, `HPC_REFERENCE.md`, and `README.md` need **no update**. This findings note + the plan are the record. A production-doc rewrite happens only if/when the full refactor lands.
