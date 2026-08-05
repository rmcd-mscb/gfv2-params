# I1 + I2 -- does `group:` collapse submissions, and does it re-create the import storm?
#
# WHY A SYNTHETIC PROBE RATHER THAN REAL PARAMS. The questions are about
# SNAKEMAKE'S behaviour (how many sbatch submissions does N rule instances become,
# and do grouped instances start together or serially), not about any fabric's
# data. tjc has 1 batch and oregon has 2 -- far too few instances to see grouping
# at all, and far too few to create a storm. So this probe generates N synthetic
# instances that each do the ONE thing that matters for I2: import the real geo
# stack and record how long that import took.
#
# I2's measurement comes from the same startup heartbeat the production scripts
# print (scripts/derive_depstor_params.py:42-48), so the numbers are comparable
# to a real run's.
#
# RUN IT (never on the login node -- these submit real SLURM jobs):
#
#   # A) ungrouped: how many submissions does N instances become?
#   PROBE_N=16 pixi run -e workflow snakemake -s workflow/probe/i1_i2_group.smk \
#       --profile workflow/probe/profile_nogroup
#
#   # B) grouped: same N, with --groups + --group-components
#   PROBE_N=16 pixi run -e workflow snakemake -s workflow/probe/i1_i2_group.smk \
#       --profile workflow/probe/profile_group
#
# then count actual submissions and read the import timings:
#   sacct -X --starttime now-2hours --format=JobID,JobName%24,Elapsed,State
#   cat $(pixi run data-root)/tjc/_i1i2_probe/*.json
#
# EXIT CRITERIA
#   I1: (B)'s sacct job count is materially lower than (A)'s. If equal, `group:`
#       does not collapse submissions and the 640-job projection stands.
#   I2: grouped instances' import wall-times are NOT worse than ungrouped. If
#       they degrade, or if concurrency within a group exceeds the %4 the arrays
#       deliberately cap at, grouping re-creates the documented storm -- which
#       does not slow the run, it hangs it (HPC_REFERENCE.md:40-47).

import os
from pathlib import Path

configfile: "configs/base_config.yml"

DATA_ROOT = config["data_root"]
FABRIC = os.environ.get("PROBE_FABRIC", "tjc")
N = int(os.environ.get("PROBE_N", "16"))
OUT = f"{DATA_ROOT}/{FABRIC}/_i1i2_probe"

INSTANCES = [f"{i:03d}" for i in range(N)]


rule all:
    input:
        expand(f"{OUT}/import_{{i}}.json", i=INSTANCES),


rule import_probe:
    """Import the real geo stack and record how long it took.

    Shells out via `pixi run --as-is` exactly as the production rules would, so
    the import happens in the frozen default env -- the same env, and the same
    shared-FS contention, that the %4 array cap exists to protect.
    """
    output:
        f"{OUT}/import_{{i}}.json",
    # NOTE: `group:` is NOT set here. It is applied per-arm from the profile via
    # `--groups import_probe=importers`, which "overwrites any group definitions
    # from the workflow" -- so one rule definition serves both arms and the two
    # runs differ ONLY in the thing being tested. (A `**{...}` conditional inside
    # a rule body is not valid Snakemake DSL; the rule block is not real Python.)
    resources:
        mem_mb=8000,
        runtime=20,
        cpus_per_task=2,
    shell:
        # `-e default` is LOAD-BEARING and must never be dropped. snakemake runs
        # from the `workflow` env, which exports PIXI_ENVIRONMENT_NAME=workflow;
        # a nested bare `pixi run --as-is` INHERITS that and resolves to the
        # workflow env, not default. Measured: bare -> .pixi/envs/workflow,
        # explicit -> .pixi/envs/default. Without -e default this probe dies with
        # ModuleNotFoundError: No module named 'geopandas'.
        "pixi run --as-is -e default python workflow/probe/import_timer.py {output} {wildcards.i}"


# ============================ FINDINGS (2026-08-05) ============================
#
# Three runs of N=16 on tjc. Arm A was run twice -- the second time deliberately,
# as a control, because arm B's numbers looked too good to be true.
#
#   run                              hosts  SLURM job ids  geo import median
#   -------------------------------  -----  -------------  -----------------
#   A  ungrouped        (first, cold)   4         16             13.592s
#   B  --group-components=4 (warm)      2          4              0.190s
#   A  ungrouped     (re-run, warm)     3         16              0.177s
#   solo, one job alone on a node       1          1         1.27s / 1.61s
#
# ---- I1: ANSWERED, YES ----
#
# `group:` + `--group-components=N` collapses submissions exactly at the divisor:
# 16 instances -> 4 SLURM job ids at N=4. This is structural and cache-independent
# (arm A gave 16 job ids on BOTH runs, cold and warm).
#
# Extrapolated to the production shape (10 params x 64 batches + 10 merges = 650):
#     --group-components=4  -> 163 submissions
#     --group-components=8  ->  82
#     --group-components=16 ->  41
# So the spec's "~640 -> 40" projection is achievable, at N=16.
#
# ---- I2: PARTIALLY ANSWERED ----
#
# RULED OUT -- the specific mechanism the spec feared. Grouping does NOT start
# its members simultaneously. Within every allocation the four instances began at
# +0.0s / +3.2s / +6.3s / +9.1s -- serialised, one after another. So `group:`
# cannot re-create the storm by firing N imports at once, because it does not
# fire them at once.
#
# NO MEASURABLE IMPORT COST -- on a warm node. Grouped 0.190s vs ungrouped 0.177s
# is a wash. Arm B's apparent 71x speedup over arm A's first run was PAGE CACHE,
# not grouping: the arm A re-run, ungrouped, on warmed nodes, matched arm B
# exactly. Do not cite that 71x as a benefit of grouping -- it is not one.
#
# STILL OPEN -- cold-cache behaviour under grouping. The only cold measurement
# (13.592s median, 8-way concurrent, ungrouped, 4 hosts) has no grouped
# counterpart, and one cannot be manufactured: clearing a compute node's page
# cache needs root, and node assignment is not controllable from a batch script.
# What the numbers DO establish is that import cost swings ~70x on cache state
# alone -- far more than on any orchestration choice -- so the `%4` cap's real
# protective value is about limiting CONCURRENT COLD imports specifically.
#
# CONSEQUENCE FOR THE MIGRATION. The residual risk the spec identifies is
# unchanged and remains correct: snakemake's `jobs:` is a GLOBAL DAG-wide cap
# with no per-rule form, whereas `%N` is per-array. A DAG mixing a 640-instance
# zonal rule with cheap rules cannot throttle only the expensive one. Grouping
# does not solve that; it reduces submission COUNT, not concurrency.
