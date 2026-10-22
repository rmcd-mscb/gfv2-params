"""Regenerate the PRMS-parameter coverage tables in README.md.

The covered list comes from the export plan (`gfv2_params.prms_export.plan_exports`, i.e.
the `prms:` blocks in configs/); the not-covered list is every other row of
docs/HI_paramDB_DataDictionary.csv, grouped by rules over the dictionary's own columns.
Nothing is typed by hand, so the README cannot drift from the configs or the dictionary.

Same marker convention and `--check` contract as scripts/build_workflow_doc.py and
scripts/build_parameter_index.py: only the marked regions are rewritten, and CI fails
(tests/test_build_prms_coverage_doc.py) when they are stale.

    python scripts/build_prms_coverage_doc.py          # rewrite
    python scripts/build_prms_coverage_doc.py --check  # exit 1 if stale
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from gfv2_params.prms_coverage import render_covered, render_not_covered, render_summary
from gfv2_params.prms_export import load_dictionary, plan_exports

_REPO_ROOT = Path(__file__).resolve().parent.parent
README_PATH = _REPO_ROOT / "README.md"
REGIONS = ("prms-summary", "prms-covered", "prms-not-covered")


def render() -> dict[str, str]:
    items, dictionary = plan_exports(), load_dictionary()
    return {
        "prms-summary": render_summary(items, dictionary),
        "prms-covered": render_covered(items, dictionary),
        "prms-not-covered": render_not_covered(items, dictionary),
    }


def _replace_region(text: str, name: str, body: str) -> str:
    begin, end = f"<!-- BEGIN GENERATED: {name} -->", f"<!-- END GENERATED: {name} -->"
    pattern = re.compile(rf"{re.escape(begin)}.*?{re.escape(end)}", re.S)
    if not pattern.search(text):
        raise SystemExit(
            f"No '{name}' generated region found. Expected a\n  {begin}\n  ...\n  {end}\n"
            "pair -- the generator only rewrites marked regions, so it never appends the "
            "section somewhere arbitrary and never clobbers hand-written prose."
        )
    # Literal replacement: `body` may contain backslashes (escaped pipes).
    return pattern.sub(lambda _m: f"{begin}\n{body}\n{end}", text)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(_REPO_ROOT))
    except ValueError:
        return str(path)


def build(check: bool = False, path: Path = README_PATH) -> int:
    text = path.read_text()
    new = text
    for name, body in render().items():
        new = _replace_region(new, name, body)

    if new == text:
        print(f"{_display_path(path)} PRMS coverage section is up to date.")
        return 0
    if check:
        print(
            f"{_display_path(path)} PRMS coverage section is STALE -- re-run "
            "`python scripts/build_prms_coverage_doc.py`.",
            file=sys.stderr,
        )
        return 1
    path.write_text(new)
    print(f"Wrote {_display_path(path)} ({len(REGIONS)} regions).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if stale instead of rewriting")
    parser.add_argument(
        "--path", type=Path, default=README_PATH,
        help="target markdown file (default: README.md; tests point it elsewhere)",
    )
    args = parser.parse_args()
    raise SystemExit(build(check=args.check, path=args.path))


if __name__ == "__main__":
    main()
