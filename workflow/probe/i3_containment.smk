# I3 -- can two orchestrators share a data root?   ANSWERED 2026-08-05.
#
# ============================ FINDINGS ============================
#
# 1. THE SPEC'S STATED MECHANISM IS WRONG. It says "every `.batch` run bumps
#    mtimes and makes downstream Snakemake targets stale". Measured on snakemake
#    9.25.1, an mtime bump alone triggers NOTHING:
#
#       touch an input (content identical)  -> "Nothing to be done"
#       change the input's CONTENT          -> "reason: Updated input files"
#
#    Snakemake 9 decides staleness from input CONTENT, not raw mtime, even though
#    `mtime` is listed in the default --rerun-triggers set. So mtime churn from a
#    concurrent .batch run is harmless; only a genuine content change is not.
#
# 2. THE SPEC'S PROPOSED VERIFICATION IS VACUOUS. It says: "Verify with a
#    --dry-run after a deliberate `touch` of a shared VRT -- zero rules should
#    trigger." Zero rules trigger after a touch WITH OR WITHOUT `ancient()`, so
#    that test passes even with no containment whatsoever. It cannot distinguish
#    a contained workflow from an uncontained one. Do not use it.
#
# 3. `ancient()` IS STILL THE RIGHT CONTAINMENT RULE -- verified against the case
#    that actually matters. With a real content change to a shared input:
#
#       without ancient()  -> rule triggers, would rebuild under the other user
#       with ancient()     -> "Nothing to be done", output preserved intact
#
#    So the rule stands, but for a narrower and more specific reason than the
#    spec gives: it protects against another writer genuinely CHANGING a shared
#    input, not against mtime noise.
#
# 4. THE HAZARD IS REAL AND CURRENT. Verified on disk: `carussel` owns
#    gfv2_car/ and gfv2_car_conus/ under the same {data_root}, consuming the same
#    shared/ and input/ trees that rmcd owns. The exposed non-fabric-keyed paths:
#      weight_dir: {data_root}/shared/conus/weights   (zonal_params.yml:38)
#      {data_root}/shared/conus/vrt/*.vrt             (6 VRTs, 7 config refs)
#      {data_root}/input/**                           (13 distinct subtrees)
#
# ========================== RECOMMENDATION ==========================
#
# Keep the containment rule (ancient() on every shared/ + input/ path; every
# rule's outputs scoped to fabric-keyed paths), but replace the verification.
# A `touch` test proves nothing. Use instead a STATIC test over the production
# Snakefile: assert that every input path resolving under {data_root}/shared or
# {data_root}/input is wrapped in ancient(). That is checkable in CI, needs no
# data root, and cannot pass vacuously -- unlike the runtime touch probe.
#
# ============================== METHOD ==============================
#
# The mechanism was established in an isolated lab directory, NOT against
# production files -- proving `ancient()` requires CHANGING an input's content,
# and no probe should be capable of that to a shared VRT another user depends on.
# Reproduce:
#
#   mkdir -p /tmp/i3lab && cd /tmp/i3lab && echo original > shared_input.txt
#   cat > Snakefile <<'EOF'
#   rule all:
#       input: "out.txt"
#   rule make:
#       input: ancient("shared_input.txt")   # drop ancient() for the control
#       output: "out.txt"
#       shell: "cat {input} > {output}"
#   EOF
#   pixi run -e workflow snakemake -d /tmp/i3lab -s /tmp/i3lab/Snakefile --cores 1
#   echo modified > shared_input.txt
#   pixi run -e workflow snakemake -d /tmp/i3lab -s /tmp/i3lab/Snakefile --dry-run
#
# The rule below is retained as a shape reference for a production Snakefile:
# shared inputs wrapped, outputs confined to a fabric-keyed path. It is safe to
# run (it writes only under {fabric}/_i3_probe/) but it no longer proves
# anything on its own -- see finding 2.

import os

import yaml

configfile: "configs/base_config.yml"

DATA_ROOT = config["data_root"]
FABRIC = os.environ.get("PROBE_FABRIC", "tjc")
SCRATCH = f"{DATA_ROOT}/{FABRIC}/_i3_probe"

SHARED_VRTS = [
    f"{DATA_ROOT}/shared/conus/vrt/elevation.vrt",
    f"{DATA_ROOT}/shared/conus/vrt/slope.vrt",
]


rule all:
    input:
        f"{SCRATCH}/contained.txt",


rule consumes_shared_inputs:
    input:
        # ancient(): this workflow READS these and never owns them. Another user
        # writes the same tree; without this, a content change on their side marks
        # our targets stale and we would rebuild a shared artifact under them.
        vrts=[ancient(v) for v in SHARED_VRTS],
    output:
        # Fabric-keyed. Never shared/, never input/.
        f"{SCRATCH}/contained.txt",
    shell:
        "mkdir -p $(dirname {output}) && "
        "echo 'i3 probe: consumed shared inputs without claiming ownership' > {output}"
