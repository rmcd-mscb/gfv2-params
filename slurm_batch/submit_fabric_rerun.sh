#!/bin/bash
# Usage: ./submit_fabric_rerun.sh [--dry-run] [--from STAGE] [--force] <batches_dir> <fabric> [base_config]
#
# Re-runs the COMPLETE workflow for ONE fabric, chaining each stage on the previous
# stage's terminal SLURM job. The stage list is configs/workflow/fabric_rerun.yml, which
# scripts/build_workflow_doc.py also renders into slurm_batch/RUNME.md -- so the commands
# printed here are exactly the ones documented there, and CI fails if they drift.
#
# WHY: products go stale when the pipeline is rebuilt piecemeal while its code changes
# underneath it. Two attempts to DETECT staleness were built and withdrawn (#215, #218).
# Making the complete re-run cheap removes the question instead of answering it.
#
# SCOPE: one fabric. Stages with `scope: shared` are SKIPPED -- build_shared_rasters
# writes shared/, which EVERY fabric reads, so rebuilding it here would stale the others
# as a side effect. Fabric GEOMETRY is assumed unchanged: this starts from the existing
# {fabric}/batches/. Changing the fabric is a different, documented act (prepare_fabric)
# that invalidates everything downstream.
#
# FAILURE: stages chain on afterok, so a failed stage leaves its dependents in
# DependencyNeverSatisfied and SLURM cancels them. The chain stops rather than running a
# stage against incomplete inputs. Fix the cause, then resume with --from <stage>.
#
# ALWAYS --dry-run FIRST. It prints the exact submission sequence without submitting
# anything, and is safe on the login node.

set -euo pipefail

MANIFEST="${FABRIC_RERUN_MANIFEST:-configs/workflow/fabric_rerun.yml}"

usage() {
    echo "Usage: $0 [--dry-run] [--from STAGE] [--force] <batches_dir> <fabric> [base_config]"
    echo "  --dry-run     print the plan; submit nothing"
    echo "  --from STAGE  resume at STAGE instead of the first (the resumed stage starts"
    echo "                with no inbound dependency -- its predecessors are assumed done)"
    echo "  --force       append --force to the stages that accept it (see the manifest's"
    echo "                accepts_force; today only depstor_rasters, the one stage whose"
    echo "                builders skip existing outputs)"
    echo "  batches_dir   path to {fabric}/batches/ (contains manifest.yml)"
    echo "  fabric        fabric name, e.g. tjc / oregon / gfv2"
    echo "  base_config   optional path to base_config.yml (default: configs/base_config.yml)"
}

DRY_RUN=0
FROM_STAGE=""
FORCE=0
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --from)
            FROM_STAGE="${2:-}"
            if [ -z "$FROM_STAGE" ]; then
                echo "ERROR: --from requires a stage name" >&2
                exit 1
            fi
            shift 2 ;;
        --force) FORCE=1; shift ;;
        -h|--help) usage; exit 0 ;;
        --) shift; break ;;
        -*) echo "ERROR: unknown option '$1'" >&2; usage >&2; exit 1 ;;
        *) break ;;
    esac
done

if [ $# -lt 2 ]; then
    usage >&2
    exit 1
fi
BATCHES="$1"
FABRIC="$2"
BASE_CONFIG="${3:-configs/base_config.yml}"

if [ ! -f "$MANIFEST" ]; then
    echo "ERROR: stage manifest not found: $MANIFEST" >&2
    exit 1
fi
if [ ! -f "$BATCHES/manifest.yml" ]; then
    echo "ERROR: no batch manifest at $BATCHES/manifest.yml" >&2
    echo "       A re-run starts from the EXISTING batches. Run scripts/prepare_fabric.py" >&2
    echo "       first if this fabric has never been prepared." >&2
    exit 1
fi

# Emit "name<TAB>kind<TAB>accepts_force<TAB>command" for fabric-scope stages, in manifest
# order, placeholders substituted. Python owns the YAML; bash owns the submission.
#
# `pixi run --as-is python`, not bare `python3`: the system python on this cluster's login
# node has no PyYAML, so a bare interpreter works under pytest (which already runs inside
# the pixi env) and then fails for the scientist running this by hand -- the worst place
# to discover it. --as-is is the repo-wide rule for pixi under SLURM; here it also keeps
# startup at ~0.4 s by skipping the lock check.
STAGES=$(pixi run --as-is python - "$MANIFEST" "$BATCHES" "$FABRIC" "$BASE_CONFIG" <<'PY'
import sys
import yaml

manifest, batches, fabric, base_config = sys.argv[1:5]
for s in yaml.safe_load(open(manifest))["stages"]:
    if s["scope"] != "fabric":
        continue
    cmd = (s["command"].replace("{batches}", batches)
                       .replace("{fabric}", fabric)
                       .replace("{base_config}", base_config))
    print(f"{s['name']}\t{s['kind']}\t{int(bool(s['accepts_force']))}\t{cmd}")
PY
)

ALL_NAMES=$(printf '%s\n' "$STAGES" | cut -f1 | tr '\n' ' ')

if [ -n "$FROM_STAGE" ] && ! printf '%s\n' "$STAGES" | cut -f1 | grep -qxF -- "$FROM_STAGE"; then
    echo "ERROR: --from '$FROM_STAGE' is not a fabric-scope stage in $MANIFEST" >&2
    echo "       Known stages: $ALL_NAMES" >&2
    exit 1
fi

echo "Complete re-run for FABRIC=$FABRIC"
echo "  batches:   $BATCHES"
echo "  manifest:  $MANIFEST"
[ -n "$FROM_STAGE" ] && echo "  resuming at: $FROM_STAGE"
[ "$FORCE" -eq 1 ] && echo "  --force:   on (applied only where accepts_force)"
[ "$DRY_RUN" -eq 1 ] && echo "  DRY RUN -- nothing will be submitted"
echo

PREV_JOB=""
STARTED=0
CHAIN=""
while IFS=$'\t' read -r NAME KIND ACCEPTS_FORCE CMD; do
    [ -n "$NAME" ] || continue

    if [ -n "$FROM_STAGE" ] && [ "$STARTED" -eq 0 ]; then
        if [ "$NAME" != "$FROM_STAGE" ]; then
            continue
        fi
        STARTED=1
    fi

    # Build the stage's argv. The two kinds take an inbound dependency differently:
    #   wrapper -> a leading `--after <id>` it parses itself (and applies to every one of
    #              its own independent submissions)
    #   sbatch  -> a `--dependency=afterok:<id>` flag we insert right after `sbatch`
    # PREV_JOB may be COLON-JOINED (submit_zonal_params.sh fans out and reports every
    # merge job); `afterok:a:b:c` is SLURM's own "after all of these", so it is forwarded
    # whole rather than split.
    FULL_CMD="$CMD"
    if [ -n "$PREV_JOB" ]; then
        if [ "$KIND" = "wrapper" ]; then
            HEAD="${CMD%% *}"
            REST="${CMD#* }"
            FULL_CMD="$HEAD --after $PREV_JOB $REST"
        else
            HEAD="${CMD%% *}"
            REST="${CMD#* }"
            FULL_CMD="$HEAD --dependency=afterok:$PREV_JOB $REST"
        fi
    fi
    # --force only where the manifest says the command accepts it. The submit wrappers
    # reject unknown leading flags by design, so appending it blindly would abort the run.
    if [ "$FORCE" -eq 1 ] && [ "$ACCEPTS_FORCE" = "1" ]; then
        FULL_CMD="$FULL_CMD --force"
    fi

    echo "# $NAME"
    echo "  $FULL_CMD"

    if [ "$DRY_RUN" -eq 1 ]; then
        JOB="<$NAME>"
    else
        if ! OUT=$(eval "$FULL_CMD" 2>&1); then
            echo "$OUT" >&2
            echo "ERROR: stage '$NAME' failed to submit; the rest of the chain was NOT" >&2
            echo "       submitted. Fix the cause and resume with --from $NAME." >&2
            exit 1
        fi
        echo "$OUT"
        if [ "$KIND" = "wrapper" ]; then
            JOB=$(printf '%s\n' "$OUT" | sed -n 's/^TERMINAL_JOB_ID=//p' | tail -1)
            if [ -z "$JOB" ]; then
                echo "ERROR: stage '$NAME' printed no TERMINAL_JOB_ID; cannot chain the rest." >&2
                echo "       Every kind: wrapper stage must print it -- see the workflow-wrapper" >&2
                echo "       contract in slurm_batch/HPC_REFERENCE.md. Refusing to continue" >&2
                echo "       unchained: later stages would run against incomplete inputs and" >&2
                echo "       still report COMPLETED." >&2
                exit 1
            fi
        else
            JOB=$(printf '%s\n' "$OUT" | sed -n 's/^Submitted batch job \([0-9][0-9]*\).*/\1/p' | tail -1)
            if [ -z "$JOB" ]; then
                echo "ERROR: stage '$NAME' printed no sbatch job id; cannot chain the rest." >&2
                echo "       Expected a line like 'Submitted batch job 12345'." >&2
                exit 1
            fi
        fi
    fi

    CHAIN="$CHAIN $NAME=$JOB"
    PREV_JOB="$JOB"
    echo
done <<< "$STAGES"

echo "Chain:$CHAIN"
if [ "$DRY_RUN" -eq 1 ]; then
    echo "(dry run -- nothing submitted; job ids above are placeholders)"
else
    echo "Monitor: squeue -u \$USER"
    echo "A failed stage cancels its dependents (DependencyNeverSatisfied); resume with --from <stage>."
fi
