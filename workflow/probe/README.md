# Snakemake I1–I5 investigation probes

Scratch Snakefiles for the five investigation questions in
[`docs/superpowers/specs/2026-08-04-snakemake-migration-design.md`](../../docs/superpowers/specs/2026-08-04-snakemake-migration-design.md).

These are **probes, not production workflow**. They exist to answer questions whose
answers determine whether the migration is viable at all, and they should be deleted
(or promoted deliberately) once the spec records the findings.

Run everything through SLURM. Per `CLAUDE.md`, nothing heavy runs on the login node —
and `--dry-run`, which is login-node safe, is specifically NOT sufficient for I1: a
dry run never submits, so it cannot count submissions.

| Probe | Question |
| --- | --- |
| `i1_i2_group.smk` | Does `group:` collapse per-rule-instance submissions, and does it re-create the import storm? |
| `i3_containment.smk` | Do `ancient()` shared inputs stop a shared-VRT `touch` from triggering rebuilds? |
| `i4_partial_failure.smk` | How does the DAG behave when one param is unbuildable? |

---

## Finding that applies to ALL rules, not one probe

**A nested `pixi run --as-is` inherits the parent's environment selection.**

Snakemake lives in the `workflow` env, so it must be launched as
`pixi run -e workflow snakemake ...`. That exports `PIXI_ENVIRONMENT_NAME=workflow`,
and every rule's shell command inherits it. Measured 2026-08-05:

| invocation | resolves to |
| --- | --- |
| `pixi run --as-is python` from a clean shell | `.pixi/envs/default` |
| `pixi run --as-is python` from inside `pixi run -e workflow` | `.pixi/envs/**workflow**` |
| `pixi run --as-is -e default python` from either | `.pixi/envs/default` |

So **every rule must pin `-e default` explicitly**. A bare `pixi run --as-is` in a
rule does not do what it says.

### This falsifies a claimed property of the PR #142 spike

`workflow/spike/Snakefile` states:

> Rules shell out to the existing orchestrator via `pixi run --as-is` so all geo
> work runs in the frozen default env (race-safe; CLAUDE.md constraint).

That was never true of the spike as written. Its rules resolved to the **workflow**
env, not `default`. It appeared to work only because the spike declared
`workflow = { features = ["workflow"] }` — which *includes* the default feature, so
that env happened to contain the whole geo stack too.

Two consequences:

1. The spike's central race-safety claim was untested, because the mechanism it
   documented was not the mechanism operating.
2. The failure is **silent until it isn't**. Give the controller env
   `no-default-feature = true` — the correct thing to do, so operators don't pay
   ~2 GB of geo stack to run a scheduler — and every rule dies with
   `ModuleNotFoundError: No module named 'geopandas'`. That is exactly what
   happened here on the first real submission.

**Recommendation for the migration:** the controller env must be
`no-default-feature = true` (a scheduler has no business carrying GDAL), and every
rule's shell prefix must read `pixi run --as-is -e default`. A test should assert
that no rule in the production Snakefile contains a bare `pixi run --as-is` without
an explicit `-e`.
