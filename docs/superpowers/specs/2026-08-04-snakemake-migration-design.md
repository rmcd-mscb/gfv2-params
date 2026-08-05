# Snakemake migration — design

**Date:** 2026-08-04
**Status:** design, **deliberately undated** — see "Why there is no schedule"
**Issue:** #141 (Snakemake adoption); PoC in PR #142
**Companion:** `2026-08-04-prms-parameter-index-design.md` (independent; neither blocks the
other)

Replace the hand-rolled `slurm_batch/` orchestration — 30 `.batch` files plus 6 `submit_*.sh`
wrappers chaining `--dependency=afterok:` by hand — with Snakemake, per issue #141's phased
A→B plan.

An earlier draft of this spec carried a dated tranche schedule (T0 2026-08-29 → T4
2026-12-12). **A four-reviewer audit retired it.** The dates rested on validation baselines
that do not exist, gates that cannot execute, and a job-count question the proposed method
cannot answer. This revision replaces the schedule with an investigation phase whose exit
criteria are the questions that must be settled before any date is credible.

---

## Problem

### 1. Orchestration retypes what the configs already declare

Bash cannot read YAML, so the wrappers carry hardcoded duplicates:

| Location | Duplicates | Guarded? | Drifted today? |
| --- | --- | --- | --- |
| `submit_zonal_params.sh:68-79` `PARAMS` | 10 `name:` values in `zonal_params.yml` | no test | in sync (verified) |
| `submit_zonal_params.sh:94` `NEEDS_WEIGHTS` | `depends_on: build_weights` (`zonal_params.yml:210`) | YAML side only (`test_zonal_orchestrator.py:113`) | in sync |
| `submit_zonal_params.sh:101` `NEEDS_MERGE_OF` | **nothing** — ssflux→slope exists only in bash | no | n/a |
| `submit_depstor_params.sh:63-74` `FRACTIONS` | 10 `name:` values in `depstor_params.yml` | no test | in sync (verified) |

The drift is **latent, not actual**. But nothing enforces it: add an 11th param, forget the
array, and the wrapper runs 10 and **exits 0**. Note also that `PARAMS` is only a default —
`submit_zonal_params.sh:81-88` lets `ZONAL_PARAMS` override it, and gfv2 in practice runs 8
of 10 (see I4).

### 2. The higher-cost problem: ordering hazards live in runbook prose

`RUNME.md:204-255` encodes Stage 3's internal ordering (3a → 3b → 3c → 3d) as prose,
including:

> "skipping straight to 3c would burn the new depths against the PREVIOUS run's dprst mask
> — built by whatever classifier was in place before — and **exit 0** with
> `dprst_depth_avg` silently inconsistent with the new `dprst_frac`."

The only guard is a scientist reading a comment and waiting for a job to finish. Ranked by
cost-of-being-wrong this **outranks** the list duplication: hardcoded arrays risk a silently
*missing* parameter; this risks a silently *wrong* one. It is the strongest argument for the
migration, and the reason Stage 3 is the highest-value target even though Stage 4 is the
cheapest to prove.

---

## Prior art: PR #142 — what it proved, and what it did not

The spike (branch `snakemake-refactor-spike`, open, unmerged, **193 commits behind `main`**,
cut from `80302b9`) ran Stage-4 zonal on `tjc` end to end.

**Extrapolates — environment and lock properties, scale-independent:**

| Result | Evidence |
| --- | --- |
| `pixi run --as-is` under `snakemake-executor-plugin-slurm` | ran on `cn014` (jobid 202210); no `$HOME/.pixi/bin` fallback — `--export=ALL` carried PATH |
| Zero science-code change for Approach A | rules shell to existing `scripts/*.py` |
| Solve-group isolation is required | putting `workflow` in `solve-group = "default"` silently downgraded the **production** env's pandas 3.0.2 → 2.3.3; also needs `bioconda` (snakemake is not on conda-forge) |

**Does not extrapolate — every one of these is a CONUS-scale unknown:**

1. **"Parity 3/3, byte-identical."** 3 params, **1 batch**, 1,584 HRUs, `model_hru_idx`,
   62 MB. gfv2 is 10 params × 64 batches × 361,471 HRUs, `nat_hru_id`, 144 GB. The merge
   step's 64-file glob, row ordering, dtype coercion and float formatting are the entire
   surface a 1-batch fabric cannot touch.
2. **"Resumability — CONFIRMED."** The most load-bearing untested claim. Demonstrated against
   `params_snakemake_spike/`, a tree **only Snakemake ever wrote**. See I3.
3. **"Reads the param list straight from YAML."** It read `workflow/spike/zonal_params.spike.yml`
   — 41 lines, 3 params — not `configs/zonal/zonal_params.yml` (10 params, two unbuildable).
   **The design's headline property was never tested against the production config.** See I4.
4. **Job count and failure handling.** 7 jobs, `jobs: 8` — the cap never bound. Nothing about
   submission rate, `group:`, `--group-components`, or `retries:`. Critically, **the spike
   never saw a single failure**, while real CONUS runs show 0.5–20% OOM/TIMEOUT rates.
5. **Controller lifetime.** 8 minutes, on `login1`. CONUS needs 12–45 h — so the spike's own
   precedent is the pattern `CLAUDE.md` forbids scaling up. Either a persistent session or a
   controller inside a long SLURM job; decide before Stage 3.
6. **Resource sizing.** Flat `mem_mb=8000, runtime=60` on every rule. Nothing at 384 G,
   nothing at 12 h, nothing that OOMs.
7. **The `%4` throttle.** `tjc` has 1 batch — the throttle is definitionally inert there.

The branch is **not revivable**. It predates the segment-driven classifier (#187),
`dprst_depth` (#173/#177), the snarea pipeline (#170), and the param-fill convention (#189).
Port the findings note, the Snakefile pattern, and the solve-group lesson to a fresh branch;
do not rebase. Note `tests/test_snakemake_spike_parity.py` exists **only on that branch** —
port it, don't look for it on `main`.

---

## Why there is no schedule

Three classes of blocker, each of which must clear before dates mean anything.

### The validation baselines do not exist

`gfv2_dev/` contains only `batches/`, `depstor_rasters/`, `diagnose/` — **no `params/`
directory at all**, and no `dprst_depth.tif` / `dprst_depth_polygons.parquet` /
`op_flow_thres_params.csv`. Every gate the previous draft proposed was of the form "gfv2_dev
re-run matches", which has no left-hand side.

Two consequences:

- **Baseline cost is real and unbudgeted.** From `sacct` on the 2026-07-13→15 gfv2 depstor
  rebuild: ~45.5 h wall, ~340 CPU-h, `dprst_depth_batch` at 650 tasks / 268 CPU-h / 3
  TIMEOUTs, `routing_hru` OOM at 96 G. A "matches" gate needs two such passes per A/B round.
- **The baseline is a moving target.** `CLAUDE.md`'s triage rule records the depstor
  classifier rewritten #145 → #152 → #158 → #161 → #187 in five months. Any classifier change
  invalidates a gfv2_dev baseline. A migration window needs a stated code freeze on
  `depstor_builders/`, or an accepted re-baseline cost.

`gfv2`'s own `merged/` is not a usable reference either: it mixes three code vintages (zonal
2026-05-29, snarea 2026-07-07, depstor 2026-07-15), and the PR #189 fill convention has never
run on it — `filled_nhm_soil_moist_max_params.csv` and `filled_nhm_ssflux_params.csv` are
still present, with no `_unfilled/`.

**Free saving when dates are drawn:** `gfv2_dev`'s `hru_gpkg` (`base_config.yml:166`) and
`segments_gpkg` (`:134`) are *pinned to gfv2's own files* — identical 361,471 HRUs. The
snarea products are therefore identical by construction and prove nothing on gfv2_dev;
validate those on `oregon` and reserve gfv2_dev for the depstor fractions.

### The gates cannot execute

`.github/workflows/ci.yml:21` is `runs-on: ubuntu-latest` with no data root.
`tests/test_snakemake_spike_parity.py:33-34` `pytest.skip`s when outputs are absent — in CI
**both** paths are absent, so it **skips and reports green**. `CLAUDE.md` forbids pytest on
the head node. So a parity gate has nowhere to run: not CI, not the login node.

Any schedule must first define where a gate executes (see I5), or every tranche is an
unenforced manual step that CI will bless.

### Five design questions are open

See below. One of them — I1 — cannot be answered by the method the previous draft proposed.

---

## Investigation phase — exit criteria, not dates

Each item is a question with a method. Migration dates get set **after** all five are
answered, when tranche cost is knowable rather than guessed.

> ### ✅ RUN 2026-08-05 — all five answered
>
> Probes and full findings: [`workflow/probe/`](../../../workflow/probe/). Snakemake
> **9.25.1** in the `workflow` pixi env. Verdict per item below; two of the five
> corrected assumptions in this spec, and one finding falls outside all five.
>
> | | Result |
> | --- | --- |
> | **I1** | **Answered — yes.** `--group-components=N` collapses submissions exactly at the divisor (16 instances → 4 job ids at N=4). Production 650 → **41** at N=16, so the spec's "~640 → 40" is achievable. |
> | **I2** | **Partially answered.** The feared mechanism is RULED OUT — grouped members start serialised (+0.0/+3.2/+6.3/+9.1s), not simultaneously. Warm-cache import cost is a wash (0.190s grouped vs 0.177s ungrouped). Cold-cache-under-grouping remains **open** and is not manufacturable without root. |
> | **I3** | **Answered, and this spec's premise was WRONG.** See correction below. |
> | **I4** | **Answered.** Snakemake's default builds **0 of 2** good targets when one is unbuildable; bash builds 8 of 10 today. `--keep-going` recovers them and still exits 1. Recommend `--keep-going` as standard; `enabled: false` only as a documented second mechanism, never alone — it exits 0 and makes a broken entry invisible. |
> | **I5** | **Answered.** `slurm_batch/data_root_tests.batch`, plus the convention that a data-root-gated result is recorded by SLURM job id. The pattern already had a precedent: Guard 2 (`tests/test_params_index_ondisk.py`, PR #203) is the same shape. |
>
> #### Correction to I3 — the stated mechanism and its test are both wrong
>
> This spec says "every `.batch` run bumps mtimes and makes downstream Snakemake
> targets stale". **Measured false.** Snakemake 9 decides staleness from input
> CONTENT, not mtime, despite `mtime` being in the default `--rerun-triggers` set:
>
> | change to a shared input | rerun? |
> | --- | --- |
> | `touch` (mtime only) | **no** — with *or* without `ancient()` |
> | content change, no `ancient()` | yes |
> | content change, with `ancient()` | **no**, output preserved |
>
> Consequently the proposed verification — "`--dry-run` after a deliberate `touch`
> of a shared VRT, zero rules should trigger" — **passes even with no containment
> at all** and cannot distinguish a contained workflow from an uncontained one. Do
> not use it. `ancient()` is still the right rule, verified against the case that
> actually matters (content change), but the check should be a STATIC test
> asserting every `shared/`/`input/` path in the Snakefile is wrapped — checkable
> in CI, no data root, cannot pass vacuously.
>
> The hazard itself is confirmed live: `carussel` owns `gfv2_car/` and
> `gfv2_car_conus/` on the same `{data_root}`, consuming the same `shared/` and
> `input/` trees.
>
> #### Finding outside the five — a nested `pixi run` inherits its parent's env
>
> This one falsifies a claimed property of PR #142. Snakemake must be launched from
> the `workflow` env, which exports `PIXI_ENVIRONMENT_NAME=workflow`; every rule's
> shell command inherits it, so a bare nested `pixi run --as-is` resolves to
> **`.pixi/envs/workflow`**, not `default`.
>
> `workflow/spike/Snakefile` claims its rules "shell out via `pixi run --as-is` so
> all geo work runs in the frozen default env (race-safe; CLAUDE.md constraint)".
> That was never true as written — the spike's rules ran in the *workflow* env. It
> only appeared to work because the spike declared
> `workflow = { features = ["workflow"] }`, which *includes* the default feature and
> therefore the whole geo stack. Give the controller env `no-default-feature = true`
> — correct, so a scheduler does not carry GDAL — and every rule dies with
> `ModuleNotFoundError: No module named 'geopandas'`, which is exactly what happened
> on the first real submission here.
>
> **Every rule must pin `pixi run --as-is -e default`**, and a test should assert no
> rule carries a bare `pixi run --as-is` without an explicit `-e`.
>
> #### What this does NOT settle
>
> The two blockers that gate a schedule are untouched by these five: the validation
> baselines still do not exist (`gfv2_dev` has no `params/`), and the baseline cost
> (~45.5 h wall / ~340 CPU-h per depstor cascade, two passes per A/B round) is still
> unbudgeted. `retries:` semantics against a real OOM also remain unmeasured.

### I1 — Does `group:` collapse submissions into one SLURM job?

**Why it matters.** Today one param = one array job of 64 tasks throttled `%4`; ten params =
10 submissions. Per-rule-instance submission makes that 64 × 10 = **640 plus 10 merges**.
Levers: `group:` + `--group-components` (the array-job equivalent, ~640 → 40); coarser rule
granularity (640 → 10, but forfeits per-batch resumability, the main thing being bought);
`jobs:` (caps concurrency only, does not reduce count).

**Method.** The previous draft proposed `--dry-run`. **A dry-run never submits, so it cannot
answer this.** Requires a real submitted probe on `oregon` or `tjc` with `group:` set,
verified by `sacct` job count.

### I2 — Does `group:` re-create the import storm?

**Why it matters.** `HPC_REFERENCE.md:40-47`: the `%4` cap exists because "concurrent
geo-library imports (rasterio / GDAL / PROJ / pyogrio) can deadlock under shared-FS metadata
contention when many tasks start simultaneously." `%N` is a per-array modulo; Snakemake's
`jobs:` is a **global DAG-wide** cap with no per-rule form. Packing 16 rule instances into one
allocation either serialises them (losing I1's win) or starts them together (re-creating the
storm). Getting this wrong does not slow the run — it hangs it.

**Method.** The I1 probe, instrumented: record per-task import wall-time from the existing
startup heartbeat (`zonal_runners/__init__.py`) and compare against a `%4` array baseline.

### I3 — Can two orchestrators share a data root?

**Why it matters.** The spike's clean "Nothing to be done" was against a tree only Snakemake
wrote. Production writes `{fabric}/params/` and `{fabric}/depstor_rasters/` — the same paths
the `.batch` tree writes — for the whole coexistence window. Every `.batch` run bumps mtimes
and makes downstream Snakemake targets stale. Worse, key inputs are **not fabric-keyed**:
`weight_dir: {data_root}/shared/conus/weights` (`zonal_params.yml:38`),
`shared/conus/vrt/*.vrt`, all of `input/`. And the data root has **another active user** —
`carussel` owns `gfv2_car_conus/`, consuming the same `shared/` and `input/` trees. A DAG
that decides a shared VRT is stale rebuilds it under them.

**Method.** Decide and document the containment rule before any production rule is written:
mark all `shared/`/`input/` paths `ancient()`, and scope every rule's outputs to fabric-keyed
paths. Verify with a `--dry-run` after a deliberate `touch` of a shared VRT — zero rules
should trigger.

### I4 — How does the DAG behave under partial failure?

**Why it matters.** `zonal_params.yml:154` (`lulc_nlcd`) and `:179` (`lulc_foresce`) point at
`input/lulc_veg/nlcd/` and `input/lulc_veg/foresce/` — **neither directory exists on disk**.
The 2026-05-29 CONUS run: `zonal_param` n=640, 512 COMPLETED, **128 FAILED** = exactly 2
params × 64 batches; the other 8 merged fine. Bash tolerates that. A `rule all` built from
`{p["name"]: p for p in ...["params"]}` demands both targets and aborts without
`--keep-going`. **The spike dodged this entirely** by using a 3-param spike config.

More generally, every real CONUS run shows 0.5–20% OOM/TIMEOUT; the spike saw zero failures,
so `--keep-going` and `retries:` semantics are wholly untested.

**Method.** Decide how unbuildable entries are expressed — an `enabled: false` key, a
profile-level target list, or `--keep-going` as standard — and prove it on `oregon` with one
deliberately-broken param.

### I5 — Where does a parity gate run?

**Method.** A `parity.batch` SLURM job, plus a stated convention that its result is recorded
by job id in #141 rather than inferred from a green CI badge.

---

## Design (unchanged by the audit)

### Modular, stage-aligned layout

Mirrors `RUNME.md`'s stages 1:1, so a scientist navigates a structure they already know:

```
workflow/
  Snakefile              # config load, fabric-profile bridge, `rule all`, includes
  rules/
    stage0_inputs.smk    stage4_params.smk
    stage1_shared.smk    stage5_fill.smk
    stage2_fabric.smk    stage6_defaults.smk
    stage3_depstor.smk   stage7_figures.smk
  profile/
    slurm.yaml
```

Modularity is for authoring, **not** job count — Snakemake `include:`s every file into one
DAG, so 640 rule instances are 640 whether they sit in one file or eight. Do not split finer
than the stages.

Three properties the spike did not have:

1. **Fully config-driven.** Wildcard constraints and the ssflux special-case derive from the
   `script:` and `depends_on:` fields in the YAML — the spike hardcoded
   `wildcard_constraints: param="elevation|slope"` and a separate `ssflux_batch` rule, which
   is the same hardcoding relocated from bash to Python.
2. **Stage 2 declares the merged gpkg as an input it does not produce.** `merge_vpu_targets`
   is an interactive marimo notebook that must run on a compute node (`RUNME.md:160-161`);
   making the manual gate explicit in the DAG is a feature. Note Stage 2 is only *real* on
   `gfv2`: `merge_vpu_segments.py:98` writes `{fabric}_nsegment_merged.gpkg`, but `gfv2_dev`'s
   profile pins gfv2's (`base_config.yml:134`), so a gfv2_dev Stage-2 run produces an orphan
   nothing consumes. The marimo gate cannot be validated on the dev fabric.
3. **Per-rule `resources:`** — with the caveat below.

### `resources:` must not be transcribed from prose

An earlier draft proposed baking "waterbody/dprst 384G, routing 96G, dprst_depth 64G/12h"
into the rules. Those figures are incomplete and partly known-wrong:

- `routing_hru` has **no documented memory figure anywhere** and OOM'd at 96 G twice
  (2026-07-13, 2026-07-14), completing at 256 G / 128 G.
- `imperv` OOM'd at 96 G on gfv2_dev 2026-07-25, completing at 384 G.
- `dprst_depth` at 64 G / **12 h TIMED OUT** 3/650 on gfv2 and 2/170 on 2026-07-25.
- `build_shared_rasters.batch:10` ships `--mem=96G`, but job 282569 requested 503 G and hit
  355 GB MaxRSS.

Baking prose numbers in turns an operator's `sbatch --mem=` override — which is what every one
of those recoveries used — into a code change. Resources must be **profile-overridable per
rule**, and the figures re-measured from `sacct` rather than copied from docs.

---

## `slurm_batch/` retirement — invariant corrected

The previous draft's invariant was: *"tranches must be retired whole, so no intermediate state
has a dangling reference."* **That is unachievable**, for two reasons the audit established.

1. **The runbooks are the densest referrer in the repo.** `RUNME.md` and `HPC_REFERENCE.md`
   cite **34 of the 35** retiring files (`build_depstor_rasters.batch` alone: 16× in RUNME,
   17× in HPC_REFERENCE). Any schedule retiring the runbooks last guarantees dangling
   references at every intermediate state, in the operator's entry point.
2. **~20 referrers never retire at all.** The configs name their own wrappers as the
   orchestration entry point (`zonal_params.yml:7`, `depstor_params.yml:5`); `src/` and
   `scripts/` name them in **runtime output** — `scripts/prepare_fabric.py:62-64` *logs*
   `./slurm_batch/submit_zonal_params.sh` as the operator's next step, and
   `dprst_depth/tiling.py:733` / `scripts/run_dprst_depth_batch.py:97` name
   `submit_dprst_depth.sh` inside **raised error messages**. Also `tests/`,
   `.github/workflows/ci.yml:5`, `environment.yml:3`.

**Replacement invariant:** a **per-tranche reference sweep gate**. At each tranche,
`grep -rn "<retiring filename>" --exclude-dir=slurm_batch .` must return zero hits, and the
corresponding runbook *stage section* is rewritten in the **same** tranche as the files it
documents — not all at the end.

Known intra-directory violations to fix when tranches are drawn (all verified against the
previous draft's T1/T2/T3 partition):

| File (draft tranche) | References (draft tranche) | Cite |
| --- | --- | --- |
| `submit_jobs.sh` (T3) | `merge_zonal_param.batch`, `merge_depstor_fraction.batch`, `submit_zonal_params.sh`, `submit_depstor_params.sh` (all T1) | `:18`, `:20`, `:78`, `:81`, `:82` — `:82` is **printed to the operator at runtime** |
| `submit_dprst_depth.sh` (T2) | `submit_depstor_params.sh`, `submit_zonal_params.sh` (T1) | `:15`, `:43` (comments) |
| `mean_zonal_dprst_depth.batch` (T2) | `create_depstor_zonal.batch` (T1) | `:15` (sizing rationale) |

The last two are comment-only, which **collides with the freeze rule** — you cannot fix a
comment in a file the rule says delete-don't-patch. Strip or inline them before their referent
retires.

**Cost-shaping note for whoever draws the tranches:** all of the depstor tranche's cost is in
the `dprst_depth` chain (268 of ~290 CPU-h). Splitting it from the raster-stack files is clean
— `submit_dprst_depth.sh` references only depstor-tranche files — and lets the expensive half
carry its own date.

### Freeze rule — keep, with three amendments

> Once a tranche lands, its `.batch`/`.sh` files are frozen: no edits, only deletion.

Implementable — a CI step intersecting `git diff --name-only origin/main...HEAD` with files
carrying a `# FROZEN` header is ~10 lines, and `ci.yml` already fires on every PR. But:

1. **State that resource problems are handled by `sbatch --mem/--time` override, never an
   edit.** Every emergency in the `sacct` record is a resource problem (`routing_hru`
   96G→256G, `imperv` 96G→384G, three `dprst_depth` timeouts), and every one was fixed by
   override. This removes almost all pressure on the rule; without saying so, operators will
   think they need an exception when they do not.
2. **Make the CI check a logged opt-out, not a hard block** — a `FROZEN-OVERRIDE: #141` commit
   trailer recording job id and reason. A hard block people route around is worse than a
   logged exception. The genuine gap is a *logic* bug in an early-tranche file found while
   later stages are still on bash, where no `.smk` replacement exists yet. Edit frequency is
   not negligible: `build_depstor_rasters.batch` 6 commits, `create_depstor_zonal.batch` 5,
   `run_dprst_depth_batch.batch` 4, `submit_dprst_depth.sh` 3.
3. **Pair it with branch protection on `main`.** This repo's convention allows small tested
   fixes direct to main, and `ci.yml` runs on `push: branches: [main]` — *after* the push. A
   direct-to-main patch to a frozen file lands, then turns main's CI red: a post-hoc alarm,
   not a gate.

**Scope limit:** the freeze rule protects the *source* tree. The coexistence hazard that costs
data is two orchestrators writing the same `{fabric}/` paths (I3), which it cannot touch.

---

## Repo structure

### What does not change

`src/gfv2_params/`'s **7** subpackages (`aggregate/`, `depstor_builders/`, `download/`,
`dprst_depth/`, `shared_rasters/`, `snarea/`, `zonal_runners/`) and `configs/`'s **5**
subdirectories (`aggregate/`, `depstor/`, `shared_rasters/`, `snarea/`, `zonal/`) keep their
current boundaries, along with `tests/`, `crosswalks/`, and the 10 root files. This migration
changes orchestration, not organisation.

### Change ledger

Phase labels, not dates. `T*` refer to tranches drawn after the investigation phase.

| Action | Path | Phase | Rationale |
| --- | --- | --- | --- |
| create | `workflow/` (10 files) | scaffold | above |
| move | `slurm_batch/RUNME.md` → `docs/runbook.md` | final | canonical source enters `docs_dir` |
| move | `slurm_batch/HPC_REFERENCE.md` → `docs/hpc-reference.md` | final | ″ |
| move | `slurm_batch/ab_drains_to_dprst.batch` → `scripts/diagnose/` | cleanup | diagnostic, not a pipeline stage. Also update `CLAUDE.md:123` and `.amazonq/rules/workflow.md:95` (a runnable command); `CLAUDE.md`'s own sync rule means both change together |
| move | `scripts/migrate_filled_params.py`, `scripts/migrate_to_shared_layout.py` → `scripts/migrate/` | cleanup | revised CODE-2 |
| delete | `slurm_batch/` — **29** `.batch` + 6 `.sh` + `.gitignore` | per tranche; dir removed at final | `ab_drains_to_dprst.batch` is a *move*, not a delete |
| delete | `docs/hpc-workflow.md` | final | a 16-line `include-markdown` shim that exists only to pull the runbooks out of `slurm_batch/`; also update `docs/index.md:13` |
| delete | `notebooks/_archive/` (21 files) | cleanup | CODE-4 — **but audit before deleting**, see below |
| delete | `scripts/find_missing_hru_ids.py` | cleanup | orphaned — but fix `config.py:48` in the same commit |

### Reference-update surface

Enumerated so none is discovered late. The audit found the previous draft's list incomplete in
every category.

**Runbook referrers** beyond `mkdocs.yml`, `README.md`, `CLAUDE.md`, `docs/ARCHITECTURE.md`,
`.amazonq/rules/workflow.md`: `docs/ADDING_A_PARAMETER.md:243` (+`:40`, `:234`),
`docs/python-patterns.md:298` (+`:169`, `:177`), `docs/depstor_workflow.md:122`,
`docs/dprst_depth_spike.md:427-428`, `docs/repo_review_issues.md:20/:91/:130`,
`docs/pywatershed_depression_storage_requirements.md:48-49`,
`docs/depstor_port_summary.md:232/:242`, `docs/dprst_depth_avg_reference.md:174`,
`environment.yml:3`, `.github/workflows/ci.yml:5`, `src/gfv2_params/endorheic.py:121`,
`src/gfv2_params/depstor_builders/wbody_connectivity.py:228`,
`tests/test_wbody_connectivity.py:957`, `scripts/prepare_fabric.py:65`,
`scripts/diagnose/measure_global_carve.py:18`.

**`mkdocs.yml`** — drop `slurm_batch/RUNME.md` from `watch:` (`:23`); replace the
`HPC workflow: hpc-workflow.md` nav entry (`:101`) with `Runbook` + `HPC reference`.

**`scripts/migrate_to_shared_layout.py`** — `README.md:112`, `HPC_REFERENCE.md:90`, `:1370`.

**`scripts/find_missing_hru_ids.py`** — five references, not three: two
`docs/superpowers/plans/`, one `docs/superpowers/specs/2026-03-23-repo-restructure-design.md`,
`docs/repo_review_issues.md:170`, and **`src/gfv2_params/config.py:48`** (a docstring naming it
a `load_base_config` consumer). No import or test exists, so the delete is safe, but it leaves
a stale cite **in source**.

**Tests loading scripts by hardcoded path** — **8 files, 9 sites**, not 4:
`test_add_fabric.py:10`, `test_clip_shared_to_fabric.py:19`, `test_dprst_depth.py:531,563`,
`test_dprst_depth_probe.py:4`, `test_merge_and_fill_params.py:13`,
`test_merge_vpu_segments.py:21`, `test_migrate_filled_params.py:15`,
`test_validate_inputs.py:18`. Only the `migrate_*` move breaks one; they fail at CI
**collection**, which is the loud failure we want. (`test_render_depstor_figures.py` uses a
normal package import, not `importlib`.)

**`pyproject.toml`** — only two real pixi tasks reference scripts (`init-data-root` `:88`,
`render-deck` `:148`); `render_figures.py` and `render_snarea_figures.py` appear only in
dependency **comments** (`:168`, `:174-175`).

**`.snakemake/`** — not gitignored on `main` (only on the spike branch). Snakemake takes a
workdir-level lock and writes per-output metadata; add it to `.gitignore` at scaffold time.

---

## `scripts/` cleanup — CODE-2, revised

`docs/repo_review_issues.md:164-176` overlaps this work; its premise is partly stale.

- **Count.** CODE-2 says 25 files; `git ls-files` shows **23 at `scripts/` root** plus 9 under
  `scripts/diagnose/` = 32.
- **`migrate_filled_params.py` is not out-of-pipeline** — cited in `RUNME.md:446`,
  `HPC_REFERENCE.md`, `ARCHITECTURE.md`, `merge_and_fill_params.batch:21`, with
  `tests/test_migrate_filled_params.py`. Moving it is fine; calling it a one-time leftover is
  not. **It has never run on gfv2** (`filled_*.csv` still present, no `_unfilled/`) — run it on
  the current path before the cleanup, not during.
- **`build_carea_twi_artifact.py` is tooling, not a diagnostic** — the artifact builder for the
  documented threshold-sweep workflow (`docs/depstor_workflow.md`,
  `notebooks/carea_threshold_sweep.py`). **Leave at root.**
- **`find_missing_hru_ids.py`** — delete, fixing `config.py:48` in the same commit.

Sequence it **after** the migration, not before: most `.batch` files disappear anyway.

### CODE-4 — audit `notebooks/_archive/` before deleting it

CODE-4 calls the directory "exploratory scripts… not tests, not documentation, and not
production code" and proposes deleting it on the grounds that git history preserves the
files. That reasoning is weaker than it looks: git history preserves *content*, but nothing
surfaces it to someone who does not already know a file existed.

Concrete counterexample found while writing the companion parameter-index spec:
`notebooks/_archive/check_params.ipynb` is the **only** place in the repo recording that
`nhm_aspect_params.csv`'s arithmetic mean is a known simplification of a circular variable.
That note is what distinguishes "a defect nobody noticed" from "a documented trade-off whose
magnitude was never measured" — a materially different thing to put in front of a reviewer.

So: audit the 21 files for recorded findings and port them to `docs/` **first**, then delete.
The audit is cheap and one-time; the deletion is irreversible in practice.

### The out-of-repo `{data_root}/<fabric>/diagnose/` split

Keep it. `gfv2/diagnose/README.md` states the rule: those probes live outside the repo
"because they are hardcoded to two COMIDs and answer a question that is now closed — kept as
provenance for the finding, not as reusable tooling." Sound boundary — repo holds
parameterised reusable diagnostics; the data root holds single-question probes.

Write the rule into `docs/conventions.md`. But **port the conclusions in**: that README carries
the #188 resolution and a forward-looking caveat (the Alvord terminus sits 2 m outside the
playa polygon, so any future "terminus inside an already-dprst waterbody" rule needs a buffer
or it misses the motivating case). That belongs in `CLAUDE.md`'s rejected-designs bullets, not
on an unversioned data volume.

---

## Correction to HYG-1

`repo_review_issues.md` HYG-1 claims `logs/`, `site/`, and `.superpowers/sdd/` are committed.
**`git ls-files` returns 0 tracked for all three**, so nothing needs removing. But only two are
ignored: `.gitignore:187` (`logs/`) and `:75`/`:150` (`site/`, listed twice). **`superpowers`
appears nowhere in `.gitignore`** — `.superpowers/` is untracked but *not* ignored. HYG-1 should
be closed as substantially resolved, with a one-line `.gitignore` addition for `.superpowers/`
and the duplicate `site/` entry removed.

Only CODE-4's `notebooks/_archive/` (21 tracked) remains real.

---

## Could not verify

- **I1–I5 are, by construction, the unverified set.** They are the reason this spec carries no
  dates.
- **Whether `snakemake-executor-plugin-slurm` submits a `group:` as a single SLURM job** in
  current versions — the `workflow` env is not on `main`, so the plugin could not be inspected.
- **Whether PR #142's 8–15 day Approach-A estimate holds.** It measured *authoring* effort on
  the one stage with no CONUS gate. It says nothing about gate *compute*, which the audit shows
  is the real constraint.
- No `pytest` was run, and no CONUS-scale compute (head-node prohibition).
