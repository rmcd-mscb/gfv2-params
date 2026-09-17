# Complete per-fabric workflow re-run

**Status:** design approved, not yet implemented
**Date:** 2026-08-11
**Supersedes:** the product-staleness audit attempted under
[#215](https://github.com/rmcd-mscb/gfv2-params/issues/215) and withdrawn in
[#218](https://github.com/rmcd-mscb/gfv2-params/pull/218)

## Problem

Two days were spent building, fixing and then withdrawing a tool that answered *"which
merged products are out of date?"* It shipped three false negatives, was fixed three
times, and a five-agent review then found roughly 28 more — including a wrong verdict on
the live data root. A second attempt, a SLURM wrapper that would accept an
already-satisfied prerequisite, produced four more Criticals in one 62-line change.

Both failed for the same reason, and it was not implementation skill. They tried to infer
a semantic fact — *is this product derived from current inputs?* — from filesystem mtimes
and git dates, across a pipeline with per-batch stages, in-place fill rewrites, VRT
indirection, fraction-named directories, cross-param reads and a raster DAG. Every one of
those is a special case, and every special case degraded to silence.

**The question was the mistake.** "Which products are stale?" only needs asking because
the pipeline is rebuilt piecemeal while its code changes underneath it. If re-running a
whole fabric is routine — one command, a known wall-clock, clearly documented — the
question stops needing an answer. You do not detect staleness; you remove it.

This also dissolves two open issues rather than solving them: #215 ("audit and re-derive
the remaining merged products") becomes "run the workflow on each fabric", and #217 (the
depstor cascade rollout) becomes a subset of the same act.

## Scope

**The unit is one fabric.** A re-run rebuilds everything under `{data_root}/{fabric}/`
and stops there.

**It starts from the existing batches.** `{fabric}/batches/` is assumed current. This is
what makes the whole sequence `sbatch`-able: the one genuinely un-automatable step —
`pixi run -e notebooks marimo run notebooks/merge_vpu_targets.py`, which needs an
interactive compute node — lives in fabric *preparation*, a first-build concern. Changing
the fabric geometry is a different, documented act that invalidates everything downstream.

**Shared rasters are out of scope but not out of sight.** `build_shared_rasters` (12h,
96G) writes `shared/`, not `{fabric}/`, and is reused by every fabric. The driver skips
it. The generated doc still renders it, with the warning that rebuilding it obliges a
re-run of *every* fabric — which is precisely the trap that produced the elevation drift
(#215): the shared rasters were rebuilt 2026-06-30/07-01 and every fabric's elevation
product silently went stale.

## Design

### Three components

| component | job |
| --- | --- |
| `configs/workflow/fabric_rerun.yml` | the ordered stage list — the single source of truth |
| `slurm_batch/submit_fabric_rerun.sh` | walks the manifest, submits, chains each stage on the previous |
| `scripts/build_workflow_doc.py` | renders the runbook section from the same manifest |

The generated doc is checked into `slurm_batch/RUNME.md` between `<!-- BEGIN GENERATED -->`
markers, and CI fails if it is stale — the mechanism `build_parameter_index.py` already
uses. The individual commands a scientist copies are therefore the same strings the driver
runs; they cannot disagree.

### Manifest granularity: one entry per workflow wrapper

**This is the decision that keeps adding a new workflow cheap**, and it was chosen against
that criterion explicitly. Four workflow wrappers exist today
(`submit_zonal_params.sh`, `submit_depstor_params.sh`, `submit_snarea_pipeline.sh`,
`submit_dprst_depth.sh`), and each already chains its own internal `afterok` steps. That
is the established extension point: a new workflow adds a new wrapper.

So the manifest lists *wrappers*, not commands, and each wrapper keeps owning its
internals. Adding a workflow costs what it costs today plus one YAML entry, and the
runbook updates itself.

```yaml
- name: depstor_rasters
  command: sbatch slurm_batch/build_depstor_rasters.batch
  scope: fabric          # `fabric` runs; `shared` is rendered in the doc but skipped
  gate: true             # the next stage chains on this one's terminal job
  consumes: [shared rasters, fabric batches]
  produces: "{fabric}/depstor_rasters/"
  resources: "18h / 384G / 8 cpu"
```

`gate: false` would let a stage run concurrently with the next. Every stage today is a
gate; the field exists because the snarea pipeline is genuinely independent of the depstor
chain and could run alongside it, and encoding that is cheaper than rediscovering it.

Per-*command* granularity was rejected: it would make every new workflow an edit to a long
list of individual `sbatch` lines, which is exactly the cost this design exists to avoid.

### The wrapper contract

The driver cannot chain without each stage's **terminal** job id. Today
`submit_zonal_params.sh` prints `Done. Submitted 1 params; last merge job ID: 2622932`,
`submit_depstor_params.sh` prints something similar in a different format, and the other
two print none.

Each of the four wrappers therefore gains two things:

- accept `--after <jobid>`, prepended to its first submission's `--dependency`;
- print a final, machine-readable `TERMINAL_JOB_ID=<id>` line.

Four files touched before the driver does anything useful, but this is what makes the
chain possible without a babysitting process, and each half is independently testable.

**A shepherd process was rejected.** A long-lived driver that submits stage N, polls until
it finishes, then submits stage N+1 is simpler to write, but on this cluster a
shell-attached process dies with its session — observed directly during the #215 rebuilds,
where a subagent's `srun` jobs were SIGKILLed at ~55s when its shell ended. An `sbatch`
shepherd avoids that but burns an allocation sitting idle for hours.

### `--force`, normalised

It currently means three different things:

| orchestrator | today | after |
| --- | --- | --- |
| `build_shared_rasters` | overwrite existing outputs | unchanged |
| `build_depstor_rasters` | overwrite existing outputs | unchanged |
| `derive_zonal_params` | **`build_weights` only** | renamed `--force-weights`; `--force` gains the standard meaning |
| `derive_depstor_params` | **absent** | added |

One meaning everywhere: **rebuild regardless of what exists**. The zonal stage already
always rebuilds, so `--force` is a no-op there — the doc will say that plainly rather than
implying otherwise.

### Failure handling

Stages chain on `afterok`, so a failed stage leaves its dependents in
`DependencyNeverSatisfied` and SLURM cancels them. The chain stops instead of running a
stage against incomplete inputs — the failure mode the withdrawn wrapper change could not
prevent.

The driver prints the full stage → job-id chain before returning, so an operator can
`scancel` the tail deliberately. Resume is `--from <stage>`, documented alongside.

### Testing

`--dry-run` prints the exact submission sequence without calling `sbatch`. It is what
makes the driver testable and what a scientist reads before committing hours of compute.

Tests use a fake `sbatch` on `PATH` returning **distinct, incrementing** job ids, so a test
can assert that stage N+1's `--dependency` actually carries stage N's terminal id. That
assertion is the one the withdrawn wrapper's test could not make — its fake returned a
constant `12345`, so the test passed while the chaining line was deleted. The fixture
pattern is reused deliberately; only its subject was withdrawn, not its shape.

Coverage:

- each stage submits in manifest order;
- stage N+1 chains on stage N's terminal job id, not merely on *some* id;
- `--from <stage>` starts where told and chains the remainder;
- a `scope: shared` entry is skipped by the driver but rendered by the doc generator;
- the generated doc matches the manifest (CI-visible staleness check);
- `--force` propagates to every stage that accepts it.

## Sequencing

Three phases, each independently verifiable, so no phase leaves the repo in a state whose
value depends on the next one landing:

1. **`--force` normalisation** — four orchestrators, no new machinery. Independently
   useful: it removes today's ambiguity about whether a re-run rebuilt anything.
2. **Wrapper contract** — `--after` and `TERMINAL_JOB_ID` on the four wrappers, with
   tests. Useful on its own: it makes hand-chaining two stages possible, which is what
   had to be done by hand during the #215 rebuilds.
3. **Manifest, driver, doc generator** — the one-command path, on top of a contract that
   already works.

## Validation

Full re-run on **tjc** — 1,584 HRUs, 1 batch, minutes rather than hours, and it exercises
every stage including the depstor cascade. Every product is backed up first and diffed
after.

Both outcomes are informative. Products that come back identical prove the workflow is
faithful. tjc's depstor products *should* change: they date from 2026-06-08 and predate
the segment-driven on-stream classifier (#187, merged 2026-07-24/25), so a faithful re-run
closes the tjc half of #217 as a side effect.

Only after tjc passes does oregon or gfv2 follow.

## What this does not do

- It does not detect staleness. That was the withdrawn approach, twice.
- It does not rebuild shared rasters, or fabric geometry.
- It does not remove the wait between stages — it removes the *operator* from that wait.
- It does not make a partial rebuild safe. Partial rebuilds remain the hazard; the point
  is to make the complete one cheap enough that partial ones stop being attractive.

## Rejected alternatives

- **A staleness detector.** Two attempts, both withdrawn. See #215 and #218.
- **A shepherd driver.** Dies with its shell, or burns an allocation idling.
- **Docs as source of truth, driver tested against them.** Parsing prose is brittle and a
  test could only check stage names, not that the commands match.
- **Driver as source of truth, docs summarising it.** Weakens the explicit requirement
  that individual commands be cleanly documented for hand-running.
- **Per-command manifest granularity.** Makes every new workflow expensive to add, which
  is the criterion this design was chosen against.
