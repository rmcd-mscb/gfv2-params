"""One-time migration off the retired `filled_` prefix onto the canonical convention.

`merged/<name>.csv` is now the single canonical, always-gap-filled per-HRU file
(see scripts/merge_and_fill_params.py's write_filled_in_place), with the
pre-fill copy preserved at `merged/_unfilled/<name>.csv`. Existing products
(gfv2, oregon) still have some params split across `<name>.csv` (pre-fill) and
`filled_<name>.csv` (the actual filled product) from before that convention
existed. This script moves each pair onto the new layout:

    1. `<name>.csv`        -> `_unfilled/<name>.csv`   (skipped if already there)
    2. `filled_<name>.csv` -> `<name>.csv`

Step 1's skip is load-bearing: if `_unfilled/<name>.csv` already exists (e.g. a
previous run of this script, or merge_and_fill_params.py has already run once
since), the on-disk `<name>.csv` is ALREADY the filled product -- moving it
over the preserved copy would destroy the true pre-fill version irreversibly,
on a shared filesystem with no version control. See
test_migration_refuses_when_raw_already_preserved.

Defaults to --dry-run (prints the plan, moves nothing); pass --apply to
execute it. Always prints every move before making it.

    pixi run --as-is python scripts/migrate_filled_params.py --merged_dir <path>          # dry-run
    pixi run --as-is python scripts/migrate_filled_params.py --merged_dir <path> --apply   # execute
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

# Source of truth: scripts/merge_and_fill_params.py's UNFILLED_DIRNAME.
try:
    from merge_and_fill_params import UNFILLED_DIRNAME
except ImportError:
    # Fallback for contexts where scripts/ isn't on sys.path (e.g. this module
    # loaded standalone via importlib, as tests do) -- must match the literal
    # in scripts/merge_and_fill_params.py.
    UNFILLED_DIRNAME = "_unfilled"

FILLED_PREFIX = "filled_"


def plan_migration(merged_dir: Path) -> list[tuple[Path, Path, Path]]:
    """Pair every `filled_*.csv` under `merged_dir` with its canonical/raw targets.

    Returns `(filled_file, canonical_target, raw_target)` triples, one per
    `filled_*.csv` found. Does not touch the filesystem.
    """
    merged_dir = Path(merged_dir)
    plan = []
    for filled_file in sorted(merged_dir.glob(f"{FILLED_PREFIX}*.csv")):
        canonical_name = filled_file.name[len(FILLED_PREFIX):]
        canonical_target = merged_dir / canonical_name
        raw_target = merged_dir / UNFILLED_DIRNAME / canonical_name
        plan.append((filled_file, canonical_target, raw_target))
    return plan


def apply_migration(plan: list[tuple[Path, Path, Path]]) -> None:
    """Execute a migration plan from `plan_migration`.

    Per triple: move the canonical (pre-fill) file to `_unfilled/` -- skipped
    if that target already exists, see module docstring -- then move the
    `filled_` file onto the now-vacated canonical name. Prints every move (or
    skip) before performing it.
    """
    for filled_file, canonical_target, raw_target in plan:
        if raw_target.exists():
            print(f"  SKIP  {canonical_target} -> {raw_target} (already preserved)")
        elif canonical_target.exists():
            raw_target.parent.mkdir(parents=True, exist_ok=True)
            print(f"  MOVE  {canonical_target} -> {raw_target}")
            shutil.move(str(canonical_target), str(raw_target))
        else:
            print(f"  SKIP  {canonical_target} does not exist -- nothing to preserve")

        print(f"  MOVE  {filled_file} -> {canonical_target}")
        shutil.move(str(filled_file), str(canonical_target))


def print_plan(plan: list[tuple[Path, Path, Path]]) -> None:
    """Print what `apply_migration` would do, without touching the filesystem."""
    for filled_file, canonical_target, raw_target in plan:
        note = " (already preserved -- would SKIP)" if raw_target.exists() else ""
        print(f"  {filled_file.name}")
        print(f"    {canonical_target.name} -> {raw_target.parent.name}/{raw_target.name}{note}")
        print(f"    {filled_file.name} -> {canonical_target.name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--merged_dir", required=True,
        help="Path to a fabric's params/merged/ directory to migrate.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Execute the migration. Without this flag, only prints the plan.",
    )
    args = parser.parse_args()

    merged_dir = Path(args.merged_dir)
    if not merged_dir.is_dir():
        raise FileNotFoundError(f"Not a directory: {merged_dir}")

    plan = plan_migration(merged_dir)
    if not plan:
        print(f"No filled_*.csv found under {merged_dir}; nothing to migrate.")
        return 0

    print(f"{'APPLYING' if args.apply else 'DRY RUN'}: {len(plan)} move(s) planned under {merged_dir}")
    print_plan(plan)

    if not args.apply:
        print("\nDry run only -- pass --apply to execute.")
        return 0

    print("\nApplying...")
    apply_migration(plan)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
