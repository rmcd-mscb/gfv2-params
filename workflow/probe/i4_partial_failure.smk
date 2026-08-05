# I4 -- how does the DAG behave when one target is unbuildable?
#
# WHY IT MATTERS. Two production entries point at input directories that do not
# exist on disk: zonal_params.yml's `lulc_nlcd` ({data_root}/input/lulc_veg/nlcd/)
# and `lulc_foresce` ({data_root}/input/lulc_veg/foresce/). The 2026-05-29 CONUS
# run shows what bash does with that: zonal_param n=640, 512 COMPLETED, 128 FAILED
# -- exactly 2 params x 64 batches -- and the other 8 params merged normally. The
# wrapper loop tolerates a failing param because each param is its own submission.
#
# A `rule all` built from `{p["name"]: p for p in cfg["params"]}` does NOT: it
# demands every target and aborts the whole DAG. The spike never hit this because
# it used a purpose-built 3-param config that excluded both broken entries.
#
# More generally every real CONUS run shows 0.5-20% OOM/TIMEOUT, and the spike saw
# zero failures, so `--keep-going` and `retries:` semantics are wholly untested.
#
# THE THREE CANDIDATE ANSWERS (the spec asks which):
#   (a) an `enabled: false` key per config entry, filtered when building `rule all`
#   (b) a profile-level explicit target list
#   (c) `--keep-going` as standard operating procedure
#
# WHAT THIS PROBE MEASURES. Three synthetic targets, one of which always fails,
# under each policy -- so the difference between them is observable rather than
# argued:
#
#   PROBE_POLICY=none      pixi run -e workflow snakemake -s .../i4_partial_failure.smk --cores 1
#   PROBE_POLICY=none      ... --cores 1 --keep-going          # (c)
#   PROBE_POLICY=enabled   ... --cores 1                       # (a)
#
# EXIT CRITERION: for each policy, how many of the two GOOD targets exist
# afterwards? Bash's behaviour today is 2 of 2. Any policy that yields fewer is a
# regression in operator experience, not just a semantic difference.
#
# Runs locally (--cores 1); no SLURM needed, since the question is DAG semantics
# rather than scheduling.

import os

configfile: "configs/base_config.yml"

DATA_ROOT = config["data_root"]
FABRIC = os.environ.get("PROBE_FABRIC", "tjc")
OUT = f"{DATA_ROOT}/{FABRIC}/_i4_probe"
POLICY = os.environ.get("PROBE_POLICY", "none")

# Mirrors the real shape: a list of entries, one of which cannot be built because
# its declared input does not exist. `enabled` is candidate (a).
ENTRIES = [
    {"name": "good_one", "enabled": True, "buildable": True},
    {"name": "broken", "enabled": POLICY != "enabled", "buildable": False},
    {"name": "good_two", "enabled": True, "buildable": True},
]

BUILDABLE = {e["name"]: e["buildable"] for e in ENTRIES}
TARGETS = [e["name"] for e in ENTRIES if e["enabled"]]


rule all:
    input:
        expand(f"{OUT}/{{name}}.txt", name=TARGETS),


rule build:
    output:
        f"{OUT}/{{name}}.txt",
    params:
        # An unbuildable entry fails the way the real ones do: its input is simply
        # not there, so the underlying command exits non-zero.
        buildable=lambda w: BUILDABLE[w.name],
    shell:
        "mkdir -p $(dirname {output}) && "
        "if [ '{params.buildable}' = 'True' ]; then echo built > {output}; "
        "else echo 'input directory does not exist' >&2; exit 1; fi"


# ============================ FINDINGS (2026-08-05) ============================
#
#   policy                                exit   GOOD targets built
#   ------------------------------------  ----   ------------------
#   baseline: all targets, no keep-going     1          0 of 2
#   (c) --keep-going                         1          2 of 2
#   (a) enabled:false filters rule all       0          2 of 2
#
# 1. THE DEFAULT IS A REGRESSION AGAINST BASH. Today the wrapper loop builds 8 of
#    10 params when lulc_nlcd and lulc_foresce fail. Snakemake's default builds
#    ZERO -- one unbuildable target aborts work that has nothing to do with it.
#    Any migration that does not address this makes a CONUS run strictly worse,
#    because every run has 0.5-20% OOM/TIMEOUT.
#
# 2. RECOMMENDATION: (c) `--keep-going` as standard, NOT (a) alone.
#    Both recover the good targets, but they differ on the thing that matters:
#      * (c) exits 1 -- the failure stays VISIBLE, matching the convention
#        merge_and_fill_params.py already uses ("%d of %d param(s) FAILED ...",
#        return 1). Build everything buildable, then report loudly.
#      * (a) exits 0. An entry marked `enabled: false` is removed from the target
#        set, so it never surfaces as a problem again. That is precisely how
#        lulc_nlcd/lulc_foresce would stay quietly broken forever -- the silent
#        skip this repo keeps having to fix (cf. CLAUDE.md on the endorheic floor,
#        and copy_constants shipping unchained in PR #203).
#
# 3. (a) IS STILL USEFUL, BUT ONLY AS A SECOND MECHANISM, never the only one:
#    `enabled: false` is the right way to express "this entry is KNOWN unbuildable
#    and that is a deliberate, documented decision" -- as opposed to "this entry
#    failed today". Pair it with --keep-going so unexpected failures still show.
#    If it is used, an enabled:false entry must be reported at INFO on every run,
#    or it becomes invisible.
#
# 4. STILL UNTESTED: `retries:`. The spike saw zero failures, so retry semantics
#    against a real OOM (as opposed to a deterministic missing-input failure) are
#    unmeasured. An OOM that succeeds on retry and one that never will are
#    indistinguishable to the DAG, and a retry loop on a 268-CPU-h rule is
#    expensive to get wrong.
