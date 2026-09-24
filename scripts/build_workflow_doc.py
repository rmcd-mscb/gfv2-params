"""Render the complete-workflow runbook section from the stage manifest.

Rewrites ONE marker-delimited region in `slurm_batch/RUNME.md`:

    <!-- BEGIN GENERATED: workflow -->  ...  <!-- END GENERATED: workflow -->

Everything outside those markers -- the surrounding prose, the per-step walkthrough --
is hand-maintained and untouched.

WHY GENERATE IT. The requirement is that a fabric can be re-run with ONE command *and*
that every individual stage command is cleanly documented for running by hand. Those two
things drift the moment they are maintained separately: prose describing a command is not
the command. Both now come from `configs/workflow/fabric_rerun.yml` -- the same file
`slurm_batch/submit_fabric_rerun.sh` executes -- so the lines a scientist copies are the
lines the driver runs, and `--check` in CI keeps it that way.

Same marker convention and `--check` contract as scripts/build_parameter_index.py.

    python scripts/build_workflow_doc.py            # rewrite the region
    python scripts/build_workflow_doc.py --check    # exit 1 if it would change
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = _REPO_ROOT / "configs" / "workflow" / "fabric_rerun.yml"
RUNME_PATH = _REPO_ROOT / "slurm_batch" / "RUNME.md"
REGION = "workflow"


def load_manifest(manifest_path: Path = MANIFEST_PATH) -> dict:
    return yaml.safe_load(manifest_path.read_text())


def load_stages(manifest_path: Path = MANIFEST_PATH) -> list[dict]:
    return load_manifest(manifest_path)["stages"]


def _as_list(value) -> list[str]:
    """`consumes` is a YAML list; tolerate a bare string so a hand edit renders sanely."""
    return list(value) if isinstance(value, list) else [str(value)]


def render_one_command(stages: list[dict], zonal_params: str = "") -> str:
    """The one-command path, plus the resume and force recipes an operator needs."""
    fabric_names = [s["name"] for s in stages if s["scope"] == "fabric"]
    if not fabric_names:
        # --check is the CI gate; an IndexError traceback is a confusing way for a gate
        # to fail, and a manifest with nothing to run is a real (if unlikely) edit.
        raise SystemExit(
            "The manifest declares no fabric-scope stage, so there is no re-run to "
            "document. Every stage is `scope: shared`?"
        )
    return "\n".join(
        [
            "### One command",
            "",
            "```bash",
            'BATCHES="$(pixi run data-root)/<fabric>/batches"',
            "",
            "# Required on this data root -- see ZONAL_PARAMS below. One unstaged param",
            "# stops the whole remaining chain.",
            f'export ZONAL_PARAMS="{zonal_params}"',
            "",
            "# ALWAYS dry-run first: prints the exact submission sequence, submits nothing,",
            "# and is safe on the login node. Flags go BEFORE the positionals; a trailing",
            "# --dry-run is rejected rather than read as the base_config argument.",
            './slurm_batch/submit_fabric_rerun.sh --dry-run "$BATCHES" <fabric>',
            "",
            "# Then, for real. --force is applied only to the stages that accept it.",
            './slurm_batch/submit_fabric_rerun.sh --force "$BATCHES" <fabric>',
            "```",
            "",
            "Each stage chains on the previous stage's terminal SLURM job, so the whole",
            "sequence runs unattended. A failed stage stops the chain rather than running a",
            "stage against incomplete inputs, but on this cluster SLURM does **not** cancel",
            "the jobs behind it (`kill_invalid_depend` is not set). They stay in the queue",
            "indefinitely: the failed job's direct dependents show",
            "`PENDING (DependencyNeverSatisfied)` in `squeue`, everything after them",
            "`PENDING (Dependency)`. So an empty `squeue` never signals the end of a failed",
            "chain; a `DependencyNeverSatisfied` row does. Clear the stale chain, fix the",
            "cause, then resume:",
            "",
            "```bash",
            "# Cancels ALL of your pending jobs; cancel by job id instead if you have others.",
            'scancel -u "$USER" --state=PENDING',
            f'./slurm_batch/submit_fabric_rerun.sh --from {fabric_names[1] if len(fabric_names) > 1 else fabric_names[0]} "$BATCHES" <fabric>',
            "```",
            "",
            f"Valid `--from` stages: {', '.join(f'`{n}`' for n in fabric_names)}.",
            "",
            "> `--force` is not decoration. It reaches only the stages whose builders skip",
            "> existing outputs; every other stage always rebuilds. Which stages those are is",
            "> marked below, and is verified against the real scripts by",
            "> `tests/test_fabric_rerun_manifest.py`.",
            "",
            "#### Two environment knobs you will usually need",
            "",
            "The driver passes the environment through to every stage.",
            "",
            "**`ZONAL_PARAMS`** — `submit_zonal_params.sh` runs all 10 params by default, and",
            "any whose source is unstaged will fail. Because `depstor_params` waits on *every*",
            "zonal merge, one unstaged param stops the whole remaining chain. Set it to the",
            "subset your data root can actually build — `recommended_zonal_params` in the",
            "manifest records the subset that works here (`lulc_nlcd` and `lulc_foresce` are",
            "the two normally left out, their CONUS sources being unstaged):",
            "",
            "```bash",
            f'export ZONAL_PARAMS="{zonal_params}"',
            "```",
            "",
            "Keep `slope` before `ssflux` — ssflux reads the merged slope CSV at zonal time.",
            "",
            "**`SBATCH_MEM_PER_NODE` / `SBATCH_TIMELIMIT`** — the batch scripts are sized for",
            "CONUS. `build_depstor_rasters.batch` asks for 384G/18h, which is right for `gfv2`",
            "and absurd for `tjc`'s 1,584 HRUs; on a busy cluster the scheduler will put a",
            "small-fabric re-run a day out purely on the size of the request. SLURM's own",
            "environment variables override a script's `#SBATCH` directives:",
            "",
            "```bash",
            "SBATCH_MEM_PER_NODE=64G SBATCH_TIMELIMIT=02:00:00 \\",
            '  ./slurm_batch/submit_fabric_rerun.sh --force "$BATCHES" tjc',
            "```",
            "",
            "> They reach every job that does **not** set `--mem`/`--time` on its own",
            "> `sbatch` line — a command-line option beats the environment variable. Two",
            "> stages do: `submit_snarea_pipeline.sh`'s Stage 2 (`STAGE2_MEM` defaults to",
            "> **384G** for every fabric but `oregon`; override with `STAGE2_MEM` /",
            "> `STAGE2_TIME`) and `submit_dprst_depth.sh`'s build job (fixed at 64G/2h,",
            "> already small-fabric sized). Use them only when every stage genuinely fits —",
            "> true for a small fabric, false for `gfv2`, where the CONUS defaults are the",
            "> right numbers and this would OOM the depstor clump ops.",
        ]
    )


def render_stage(stage: dict) -> str:
    """One stage: what it is, the exact command, and what it reads and writes."""
    out: list[str] = []
    shared = stage["scope"] == "shared"

    heading = f"#### {stage['name']}"
    if shared:
        heading += " — shared, **skipped by the one-command re-run**"
    out.append(heading)
    out.append("")

    if shared:
        out.append(
            "> **Not part of a fabric re-run.** Run it deliberately, on its own, and then "
            "re-run *every* fabric."
        )
        out.append("")

    out.append("```bash")
    out.append(stage["command"])
    out.append("```")
    out.append("")

    rows = [
        ("Consumes", ", ".join(_as_list(stage["consumes"]))),
        ("Produces", stage["produces"]),
        ("Resources", stage["resources"]),
        (
            "`--force`",
            # NOT "always rebuilds" for the false case: zonal_params does have one
            # exists-skipped artefact (the lithology weight matrix), which --force cannot
            # reach and FORCE=1 does. Say what the flag does here, and leave the exception
            # to the stage's own note.
            "**applies here**"
            if stage["accepts_force"]
            else "not accepted (the driver does not pass it)",
        ),
    ]
    out.append("| | |")
    out.append("|---|---|")
    for label, value in rows:
        out.append(f"| {label} | {value} |")

    if stage.get("note"):
        out.append("")
        # Paragraph breaks survive. Collapsing the whole note to one line turned the most
        # operationally important warning in the document into a ~600-character
        # single-line blockquote -- the least readable thing in it.
        # Split on ANY run of newlines: in a YAML folded scalar (`>-`, which every note
        # uses) a blank source line yields a SINGLE "\n", not two.
        paragraphs = [" ".join(p.split()) for p in re.split(r"\n+", stage["note"]) if p.strip()]
        for i, para in enumerate(paragraphs):
            if i:
                out.append(">")
            out.append(f"> {para}")

    return "\n".join(out)


# Visible to a reader of the RENDERED page, unlike the HTML-comment markers. It must be
# emitted from inside render(), or the next run wipes it.
_GENERATED_BANNER = (
    "*Generated from [`configs/workflow/fabric_rerun.yml`](../configs/workflow/"
    "fabric_rerun.yml) by `scripts/build_workflow_doc.py`. Edits below are overwritten — "
    "edit the manifest instead.*"
)
_GENERATED_FOOTER = "*(end of generated section — hand-written prose resumes below)*"


def render(stages: list[dict], zonal_params: str = "") -> str:
    parts = [
        _GENERATED_BANNER,
        "",
        render_one_command(stages, zonal_params),
        "",
        "### The stages, individually",
        "",
    ]
    parts.append(
        "Run any of these on its own — they are the same strings the driver submits. "
        "Placeholders `{batches}`, `{fabric}` and `{base_config}` are substituted by the "
        "driver; substitute them yourself when running by hand."
    )
    parts.append("")
    for stage in stages:
        parts.append(render_stage(stage))
        parts.append("")
    parts.append(_GENERATED_FOOTER)
    return "\n".join(parts).rstrip()


def _replace_region(text: str, name: str, body: str) -> str:
    begin, end = f"<!-- BEGIN GENERATED: {name} -->", f"<!-- END GENERATED: {name} -->"
    pattern = re.compile(rf"{re.escape(begin)}.*?{re.escape(end)}", re.S)
    if not pattern.search(text):
        raise SystemExit(
            f"No '{name}' generated region found. Expected a\n"
            f"  {begin}\n  ...\n  {end}\n"
            f"pair -- the generator only rewrites marked regions, so it never appends "
            f"the section somewhere arbitrary and never clobbers hand-written prose."
        )
    # Literal replacement: `body` contains backslashes and `\g`-like sequences would
    # otherwise be read as group references by re.sub.
    return pattern.sub(lambda _m: f"{begin}\n{body}\n{end}", text)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(_REPO_ROOT))
    except ValueError:
        return str(path)


def build(check: bool = False, path: Path = RUNME_PATH) -> int:
    manifest = load_manifest()
    stages = manifest["stages"]
    text = path.read_text()
    new = _replace_region(
        text, REGION, render(stages, manifest.get("recommended_zonal_params", ""))
    )

    if new == text:
        print(f"{_display_path(path)} workflow section is up to date.")
        return 0
    if check:
        print(
            f"{_display_path(path)} workflow section is STALE -- re-run "
            f"`python scripts/build_workflow_doc.py`.",
            file=sys.stderr,
        )
        return 1
    path.write_text(new)
    print(f"Wrote {_display_path(path)} ({len(stages)} stages).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if the section is stale instead of rewriting it",
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=RUNME_PATH,
        help="target markdown file (default: slurm_batch/RUNME.md; tests point it elsewhere)",
    )
    args = parser.parse_args()
    raise SystemExit(build(check=args.check, path=args.path))


if __name__ == "__main__":
    main()
