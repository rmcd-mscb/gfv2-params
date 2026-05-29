# RUNME split into runbook + reference, and full SLURM coverage

**Status:** Design — 2026-05-29 · tracked in [#131](https://github.com/rmcd-mscb/gfv2-params/issues/131)

## Problem

`slurm_batch/RUNME.md` has grown to 826 lines and serves two audiences badly at
once. A hydrologist who is **not** an HPC/SLURM expert cannot find "what do I run,
in what order" — the ~8-command happy path is scattered across the file, every
command is wrapped in HPC rationale (`pixi --as-is` metadata-race story,
throttle/import-storm notes, memory tuning), PR/issue archaeology ("resolved by
PR #95"), and two equivalent execution paths (Stage 4A incremental vs 4B
wholesale) are presented as peers.

Separately, an audit of every command RUNME tells the user to run **directly on
the login node** found compute/memory-intensive steps with no SLURM wrapper —
they run on the login node today, against the project rule that intensive work
belongs in a batch job.

## Goals

1. **Split** RUNME into a lean linear **runbook** (the hydrologist's path) and a
   co-located **reference** (the HPC/maintainer detail).
2. **Close the SLURM-coverage gaps**: every compute/memory-intensive step gets a
   batch wrapper; login-node use is reserved for genuinely trivial commands.
3. Keep all the technical detail — relocate it, do not delete it (except PR/issue
   archaeology, which git already records).

## SLURM-coverage audit (login-node commands in RUNME today)

| Script | What it does | Intensity | Wrapper today | Verdict |
|---|---|---|---|---|
| `init-data-root` (`--check`) | scaffold dirs / verify | trivial | — | login OK |
| `migrate_to_shared_layout` | `os.rename` only | trivial | — | login OK |
| `clip_shared_to_fabric` | read HRU bounds → tiny VRT | light | — | login OK |
| `prepare_fabric` | load whole CONUS fabric + KD-tree bisection | heavy | none | **needs SLURM** |
| `merge_and_fill_params` (gap-fill) | sklearn KNN over all HRUs × params | heavy | none | **needs SLURM** |
| `merge_vpu_segments` | concat 21 VPU `nsegment` layers | moderate | none | **needs SLURM** |
| `render_figures` | nbconvert-execute notebooks; ~361k polys | heavy (headless) | none | **needs SLURM** |
| `merge_vpu_targets` (marimo) | merge 21 VPU `nhru` → CONUS | heavy, interactive | none | compute-node only (doc) |
| `merge_default_params` | pandas CSV merges | moderate | `merge_default_output_params.batch` | use the batch (doc fix) |
| `build_shared_rasters --step X` (direct) | single raster step (some 384 G) | heavy for some | `build_shared_rasters.batch` (`--step`) | use the batch (doc fix) |

## Part A — new SLURM wrappers

Four new `slurm_batch/*.batch` files, each mirroring the existing batch
conventions (`#SBATCH` header; `cd "$SLURM_SUBMIT_DIR"`;
`BASE_CONFIG=${BASE_CONFIG:-configs/base_config.yml}`; `FABRIC=${FABRIC:-gfv2}`
where applicable; `pixi run --as-is python …`; forward `"$@"`). Default
resources are starting points, flagged in-comment as "right-size after first
run via `sacct MaxRSS`."

| New batch | Command | env | default `--mem` / `--time` / cpus |
|---|---|---|---|
| `prepare_fabric.batch` | `prepare_fabric.py --fabric "$FABRIC" --base_config "$BASE_CONFIG" "$@"` | default | 64G / 02:00:00 / 4 |
| `merge_and_fill_params.batch` | `merge_and_fill_params.py --base_config "$BASE_CONFIG" "$@"` | default | 64G / 02:00:00 / 8 |
| `merge_vpu_segments.batch` | `merge_vpu_segments.py --fabric "$FABRIC" "$@"` | default | 32G / 01:00:00 / 4 |
| `render_figures.batch` | `render_figures.py --fabric "$FABRIC" "$@"` | `-e notebooks` | 96G / 02:00:00 / 4 |

Notes:
- `render_figures.batch` uses `pixi run --as-is -e notebooks python …` (the only
  new batch needing the `notebooks` env; the script already forces
  `MPLBACKEND=Agg`).
- `prepare_fabric.batch` / `merge_vpu_segments.batch` honour `FABRIC`;
  `merge_and_fill_params.py` resolves the fabric from `base_config`'s
  `default_fabric` (no `--fabric`), so its batch only passes `--base_config`.
- These wrap **existing** scripts unchanged — no script edits in Part A.

## Part B — doc-consistency fixes

- `merge_default_params` shown via `sbatch slurm_batch/merge_default_output_params.batch`,
  not a login `pixi run`.
- Heavy single-step `build_shared_rasters` runs shown via
  `sbatch slurm_batch/build_shared_rasters.batch --step <name>` (the orchestrator
  batch already forwards `--step`), not login `pixi run … --step`. Trivial/quick
  inspection steps may still be noted as login-runnable in reference.
- `merge_vpu_targets` (marimo): documented as **compute-node only** — run under
  JupyterHub or an `salloc` allocation, never the login node. No code change.

## Part C — the split

### `slurm_batch/RUNME.md` — the runbook (target ~150–180 lines)

A hydrologist reads top-to-bottom and copy-pastes. Sections:

1. **Title + "who this is for"** — one line.
2. **Before you start** — 3 bullets: `pixi install` + `~/.pixi/bin` on PATH;
   always submit from a PATH-enabled shell; `cd` to repo root. "Why?" links to
   reference.
3. **Pipeline at a glance** — short ordered list of the stages (the whole arc).
4. **Run it (CONUS gfv2)** — one numbered linear sequence; each step is *one*
   command block + a one-line "what it does" + "wait for: …". Spine:
   - 0 · init data root + stage inputs (`init-data-root`; download `sbatch`es;
     `stage_twi`) → input table
   - 1 · build shared rasters (`sbatch build_shared_rasters.batch`)
   - 2 · prepare fabric (`merge_vpu_targets` on a compute node; `sbatch
     merge_vpu_segments.batch`; `sbatch prepare_fabric.batch`)
   - 3 · build depstor rasters (`sbatch build_depstor_rasters.batch`)
   - 4 · generate params — **fully explicit** (addendum 2026-05-29): the runbook
     spells out the per-parameter batch jobs to run in order
     (`derive_zonal_params.batch` + `merge_zonal_param.batch` per zonal param,
     `build_zonal_weights.batch` for ssflux, `create_depstor_zonal.batch` +
     `merge_depstor_fraction.batch` per fraction, then `derive_depstor_ratios.batch`),
     naming every parameter/fraction and the `slope`→`ssflux` order. The
     `submit_zonal_params.sh` / `submit_depstor_params.sh` wrappers are presented
     **only as the wholesale convenience** that runs exactly that sequence
     (afterok-chained). Rationale: the shell wrappers are opaque to a
     non-HPC reader; the batch scripts and their order must be visible.
     HPC_REFERENCE's Stage 4A keeps the finer detail (throttle, single-param
     rerun) and points back to the runbook for the sequence.
   - 5 · gap-fill (`sbatch merge_and_fill_params.batch`)
   - 6 · (optional) NHM defaults (`sbatch merge_default_output_params.batch`)
   - 7 · view results (`sbatch render_figures.batch`)
5. **Monitoring** — `squeue` / `sacct` / `tail` (3 lines).
6. **Where the outputs land** — the `{fabric}/params/` paths.
7. **Need more?** — link list into the reference (other fabrics, incremental
   4A runs, recovery, internals).

Defaults (gfv2, `--mem`, throttle) are baked into the batches, so the runbook
says "submit it" without tuning prose.

### `slurm_batch/HPC_REFERENCE.md` — the reference (by topic, trimmed)

Relocated, trimmed-to-brief-notes, PR/issue numbers dropped:
- Environment: `pixi --as-is` + PATH requirement (brief).
- Concurrency: the array throttle knob + why (one paragraph).
- Data-directory layout (the tree).
- Running other fabrics: `gfv2_vpu01`, `oregon` (`FABRIC=` overrides).
- Adding a new fabric (Case A pre-merged / Case B per-VPU).
- Incremental / per-parameter runs (today's Stage 4A) and `--step`/`--from`.
- Partial reruns + VPU recovery.
- Memory notes (routing ~80 GB measured; per-VPU merge 384 G; the new batches'
  right-sizing).
- depstor `carea_map` threshold modes + TWI source pairing.
- Script → config → entry-point mapping table (incl. the 4 new batches).
- `migrate_to_shared_layout` (legacy-layout upgrade).

### Cross-reference updates (same change)

- `CLAUDE.md`: the line calling RUNME "the authoritative step-by-step HPC
  workflow" → RUNME = runbook (the happy path), HPC_REFERENCE = details.
- `docs/ARCHITECTURE.md` and any doc that links "RUNME Stage 4A" / "RUNME
  Stage N" → repoint to `HPC_REFERENCE.md` (the runbook drops the old stage
  numbering). Audit inbound references with
  `grep -rn "RUNME" docs/ README.md *.md src/`.
- `README.md`: if it points at RUNME for HPC detail, confirm the link still
  lands somewhere sensible.

## Verification

- Every command in the new runbook checked against the actual batch/script names
  and argument names (no invented flags); `grep`-confirm each referenced
  `.batch`/`.sh`/`.py` exists.
- The 4 new batches: confirm they parse and submit
  (`sbatch --test-only` or a real submit) and that a real run COMPLETEs and
  reports a sane `MaxRSS` (the proof the `--mem` default is right-sized). The
  CONUS submits are cluster runs the user may prefer to do.
- Relocation is lossless except the deliberately-cut PR/issue archaeology: spot
  every non-trivial "why" paragraph from the old RUNME and confirm it has a home
  in HPC_REFERENCE (trimmed) or was intentionally dropped.
- Docs gate (per CLAUDE.md): no other doc still describes a heavy step as a
  login-node `pixi run`.

## Non-goals

- No changes to the scripts the new batches wrap (Part A is wrappers only).
- Not converting `merge_vpu_targets` (marimo) into a headless script — it stays
  interactive, documented as compute-node-only.
- No change to the pipeline's behaviour, configs, or outputs.
- Not re-sizing the *existing* batches' resources (only the 4 new ones get
  starting defaults).

## Risks

- **Batch `--mem` defaults too low.** Mitigated by the "right-size after first
  run" note and the `sacct MaxRSS` check; over-provisioned starting values
  chosen deliberately.
- **Broken inbound links** to old RUNME stage anchors. Mitigated by the
  `grep -rn "RUNME"` audit in the cross-reference step.
- **Runbook drift** if the reference and runbook disagree later. Mitigated by
  keeping commands only in the runbook and rationale only in the reference (one
  home per fact).
