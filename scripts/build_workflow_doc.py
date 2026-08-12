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


def load_stages(manifest_path: Path = MANIFEST_PATH) -> list[dict]:
    return yaml.safe_load(manifest_path.read_text())["stages"]


def _as_list(value) -> list[str]:
    """`consumes` is a YAML list; tolerate a bare string so a hand edit renders sanely."""
    return list(value) if isinstance(value, list) else [str(value)]


def render_one_command(stages: list[dict]) -> str:
    """The one-command path, plus the resume and force recipes an operator needs."""
    fabric_names = [s["name"] for s in stages if s["scope"] == "fabric"]
    return "\n".join(
        [
            "### One command",
            "",
            "```bash",
            'BATCHES="$(pixi run data-root)/<fabric>/batches"',
            "",
            "# ALWAYS dry-run first: prints the exact submission sequence, submits nothing,",
            "# and is safe on the login node.",
            './slurm_batch/submit_fabric_rerun.sh --dry-run "$BATCHES" <fabric>',
            "",
            "# Then, for real. --force is applied only to the stages that accept it.",
            './slurm_batch/submit_fabric_rerun.sh --force "$BATCHES" <fabric>',
            "```",
            "",
            "Each stage chains on the previous stage's terminal SLURM job, so the whole",
            "sequence runs unattended. A failed stage leaves its dependents in",
            "`DependencyNeverSatisfied` and SLURM cancels them — the chain stops rather than",
            "running a stage against incomplete inputs. Fix the cause, then resume:",
            "",
            "```bash",
            f'./slurm_batch/submit_fabric_rerun.sh --from {fabric_names[1] if len(fabric_names) > 1 else fabric_names[0]} "$BATCHES" <fabric>',
            "```",
            "",
            f"Valid `--from` stages: {', '.join(f'`{n}`' for n in fabric_names)}.",
            "",
            "> `--force` is not decoration. It reaches only the stages whose builders skip",
            "> existing outputs; every other stage always rebuilds. Which stages those are is",
            "> marked below, and is verified against the real scripts by",
            "> `tests/test_fabric_rerun_manifest.py`.",
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
            "**applies here**" if stage["accepts_force"] else "no effect (always rebuilds)",
        ),
    ]
    out.append("| | |")
    out.append("|---|---|")
    for label, value in rows:
        out.append(f"| {label} | {value} |")

    if stage.get("note"):
        out.append("")
        note = " ".join(stage["note"].split())
        out.append(f"> {note}")

    return "\n".join(out)


def render(stages: list[dict]) -> str:
    parts = [render_one_command(stages), "", "### The stages, individually", ""]
    parts.append(
        "Run any of these on its own — they are the same strings the driver submits. "
        "Placeholders `{batches}`, `{fabric}` and `{base_config}` are substituted by the "
        "driver; substitute them yourself when running by hand."
    )
    parts.append("")
    for stage in stages:
        parts.append(render_stage(stage))
        parts.append("")
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
    stages = load_stages()
    text = path.read_text()
    new = _replace_region(text, REGION, render(stages))

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
