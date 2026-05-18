"""Build the CONUS shared-raster stack from a single unified config.

Walks the dependency DAG declared in configs/shared_rasters/shared_rasters.yml,
dispatching each step to its builder under src/gfv2_params/shared_rasters/
via the BUILDERS dict in that package's __init__.py.

These rasters are fabric-independent — they live under {data_root}/shared/ and
are reused across every fabric profile.

Flags:
  --step <name>     run only that one step (upstream outputs must exist)
  --from <name>     resume from this step (run it + everything after)
  --vpus <csv>      restrict per-VPU steps to this comma-separated list
  --force           rebuild outputs even if they already exist
"""

import argparse
import re
import sys
import time
from pathlib import Path

import yaml

from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import (
    BUILDERS,
    PLANNED_STEPS,
    STEP_ORDER,
    SharedRastersContext,
)

_DEFAULT_BASE_CONFIG = Path(__file__).resolve().parent.parent / "configs" / "base_config.yml"


# Placeholders resolved later by per-VPU builders inside their iteration
# loop (not at config-load time). Mirrors how `{fabric}` is treated by the
# depstor orchestrator — builder-owned, not orchestrator-owned.
_DEFERRED_PLACEHOLDERS = {"vpu"}


def _resolve_nested(value, replacements: dict):
    """Recursively resolve {placeholder} substrings in nested str/list/dict."""
    if isinstance(value, str):
        for ph, rep in replacements.items():
            value = value.replace(f"{{{ph}}}", str(rep))
        remaining = [r for r in re.findall(r"\{(\w+)\}", value)
                     if r not in _DEFERRED_PLACEHOLDERS]
        if remaining:
            raise ValueError(
                f"Unresolved placeholder(s) {remaining} in value '{value}'. "
                f"Available now: {sorted(replacements)}. "
                f"Deferred to builders: {sorted(_DEFERRED_PLACEHOLDERS)}."
            )
        return value
    if isinstance(value, list):
        return [_resolve_nested(v, replacements) for v in value]
    if isinstance(value, dict):
        return {k: _resolve_nested(v, replacements) for k, v in value.items()}
    return value


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _load_data_root(base_config_path: Path) -> str:
    """Read just `data_root` from base_config.yml.

    Shared-raster steps are fabric-independent; we deliberately bypass
    `load_base_config` so that running this orchestrator does not require
    a default_fabric to be set or any fabric profile to validate.
    """
    with open(base_config_path) as f:
        base = yaml.safe_load(f)
    if not base or "data_root" not in base:
        raise ValueError(f"`data_root` not found in {base_config_path}")
    return base["data_root"]


def _select_steps(all_steps, only_step, from_step):
    names = [s["name"] for s in all_steps]
    if only_step:
        if only_step not in names:
            raise ValueError(f"--step '{only_step}' not in config; available: {names}")
        return [s for s in all_steps if s["name"] == only_step]
    if from_step:
        if from_step not in names:
            raise ValueError(f"--from '{from_step}' not in config; available: {names}")
        idx = names.index(from_step)
        return all_steps[idx:]
    return all_steps


def main():
    parser = argparse.ArgumentParser(description="Build all CONUS shared rasters.")
    parser.add_argument("--config", required=True, help="Path to configs/shared_rasters/shared_rasters.yml")
    parser.add_argument("--base_config", default=None, help="Path to configs/base_config.yml")
    parser.add_argument("--step", default=None, help="Run only this one step")
    parser.add_argument("--from", dest="from_step", default=None, help="Resume from this step")
    parser.add_argument("--vpus", default=None, help="Comma-separated VPU scope override (e.g., '01,02')")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    if args.step and args.from_step:
        parser.error("--step and --from are mutually exclusive")

    logger = configure_logging("build_shared_rasters")
    t_start = time.time()

    base_config_path = Path(args.base_config) if args.base_config else _DEFAULT_BASE_CONFIG
    data_root = _load_data_root(base_config_path)

    with open(args.config) as f:
        raw_config = yaml.safe_load(f) or {}

    replacements = {"data_root": data_root}
    config = {k: _resolve_nested(v, replacements) for k, v in raw_config.items()}

    vpus = config.get("vpus", [])
    if args.vpus:
        vpus = [v.strip() for v in args.vpus.split(",")]

    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    all_steps = config.get("steps", []) or []
    step_index = {s["name"]: s for s in all_steps}
    unknown = set(step_index) - set(STEP_ORDER)
    if unknown:
        raise ValueError(
            f"Step(s) {sorted(unknown)} listed in config but not registered. "
            f"Registered (STEP_ORDER): {STEP_ORDER}. "
            f"Planned but not yet migrated: {[s for s in PLANNED_STEPS if s not in STEP_ORDER]}."
        )
    ordered_steps = [step_index[n] for n in STEP_ORDER if n in step_index]
    run_steps = _select_steps(ordered_steps, args.step, args.from_step)

    logger.info("=== build_shared_rasters ===")
    logger.info("data_root        : %s", data_root)
    logger.info("output_dir       : %s", output_dir)
    logger.info("VPU scope        : %s", vpus or "(none configured)")
    logger.info("Registered steps : %s", STEP_ORDER or "(none yet)")
    pending = [s for s in PLANNED_STEPS if s not in STEP_ORDER]
    if pending:
        logger.info("Planned (pending): %s", pending)
    logger.info("Running %d step(s): %s", len(run_steps), [s["name"] for s in run_steps])

    if not run_steps:
        logger.info(
            "No steps to run. Migration in progress — see PLANNED_STEPS in "
            "src/gfv2_params/shared_rasters/__init__.py."
        )

    ctx = SharedRastersContext(
        data_root=Path(data_root),
        vpus=vpus,
        output_dir=output_dir,
        force=args.force,
    )

    for step in run_steps:
        name = step["name"]
        builder = BUILDERS[name]
        t_step = time.time()
        try:
            produced = builder(step, ctx, logger)
        except Exception:
            logger.exception("Step '%s' failed", name)
            sys.exit(1)
        ctx.paths.update(produced)
        logger.info("  %s done in %s", name, _elapsed(t_step))

    logger.info("=== build_shared_rasters complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
