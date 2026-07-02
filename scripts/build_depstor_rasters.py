"""Build the full depression-storage raster stack from a single unified config.

Replaces the 10 per-step scripts that used to live under scripts/build_depstor_*.py.
Reads configs/depstor/depstor_rasters.yml + the active fabric profile in base_config.yml,
then walks STEP_ORDER (defined in gfv2_params.depstor_builders) calling each
builder in dependency order.

Flags:
  --step <name>     run only that one step (must already have its upstream
                    outputs on disk)
  --from <name>     resume from this step (run it + everything after)
  --force           rebuild outputs even if they already exist
"""

import argparse
import re
import sys
import time
from pathlib import Path

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor_builders import BUILDERS, STEP_ORDER, BuildContext
from gfv2_params.log import configure_logging


def _resolve_nested(value, replacements: dict):
    """Recursively resolve {placeholder} substrings in nested str/list/dict."""
    if isinstance(value, str):
        for ph, rep in replacements.items():
            value = value.replace(f"{{{ph}}}", str(rep))
        remaining = re.findall(r"\{(\w+)\}", value)
        if remaining:
            raise ValueError(
                f"Unresolved placeholder(s) {remaining} in value '{value}'. "
                f"Available: {sorted(replacements)}"
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


def _select_steps(all_steps, only_step: str | None, from_step: str | None):
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


def _build_context(config: dict, force: bool) -> BuildContext:
    fabric = config["fabric"]
    output_dir = Path(config["output_dir"])
    template_path = Path(require_config_key(config, "template_raster", "build_depstor_rasters"))
    hru_gpkg = Path(require_config_key(config, "hru_gpkg", "build_depstor_rasters"))
    hru_layer = require_config_key(config, "hru_layer", "build_depstor_rasters")
    id_feature = require_config_key(config, "id_feature", "build_depstor_rasters")

    output_dir.mkdir(parents=True, exist_ok=True)

    return BuildContext(
        fabric=fabric,
        template_path=template_path,
        output_dir=output_dir,
        hru_gpkg=hru_gpkg,
        hru_layer=hru_layer,
        id_feature=id_feature,
        segments_gpkg=Path(config["segments_gpkg"]) if config.get("segments_gpkg") else None,
        segments_layer=config.get("segments_layer", "nsegment"),
        waterbody_gpkg=Path(config["waterbody_gpkg"]) if config.get("waterbody_gpkg") else None,
        waterbody_layer=config.get("waterbody_layer"),
        connected_comids_table=(
            Path(config["connected_comids_table"])
            if config.get("connected_comids_table") else None
        ),
        flowthrough_comids_table=(
            Path(config["flowthrough_comids_table"])
            if config.get("flowthrough_comids_table") else None
        ),
        fdr_raster=Path(config["fdr_raster"]) if config.get("fdr_raster") else None,
        twi_raster=Path(config["twi_raster"]) if config.get("twi_raster") else None,
        vpu=config.get("vpu"),
        imperv_source=Path(config["imperv_source"]) if config.get("imperv_source") else None,
        force=force,
    )


def _hydrate_existing_outputs(ctx: BuildContext, all_steps: list, run_steps: list, logger) -> None:
    """Pre-fill ctx.paths with already-existing outputs from earlier steps.

    Lets --step / --from invocations look up upstream outputs without re-running
    them. Walks every step that is NOT in the run list and checks expected
    output filenames against disk.
    """
    run_names = {s["name"] for s in run_steps}
    for step in all_steps:
        if step["name"] in run_names:
            continue
        produced = _expected_outputs(step)
        for key, filename in produced.items():
            path = ctx.output_dir / filename
            if path.exists():
                ctx.paths[key] = path
                logger.debug("Found existing upstream output %s -> %s", key, path)


def _expected_outputs(step: dict) -> dict:
    """Map each step's `output(s)` block to its registered output keys."""
    name = step["name"]
    if "output" in step:
        if name == "drains_perv":
            return {"drains_perv": step["output"]}
        if name == "drains_imperv":
            return {"drains_imperv": step["output"]}
        # landmask, imperv, wbody_connectivity, perv, routing each map to a single key.
        single_key = {
            "landmask": "landmask",
            "imperv": "imperv",
            "wbody_connectivity": "connected_wbody",
            "perv": "perv",
            "routing": "drains_to_dprst",
            "vpu_id": "vpu_id",
            "hru_id": "hru_id",
            "routing_hru": "drains_to_dprst_hru",
        }
        return {single_key[name]: step["output"]}
    outputs = step["outputs"]
    if name == "waterbody":
        return {"wbody_binary": outputs["binary"], "wbody_regions": outputs["regions"]}
    if name == "dprst":
        return {"dprst": outputs["dprst"], "onstream": outputs["onstream"]}
    if name == "carea_map":
        return {"carea_max": outputs["carea_max"], "smidx": outputs["smidx"]}
    raise ValueError(f"Unrecognised step name: {name}")


def main():
    parser = argparse.ArgumentParser(description="Build all depression-storage rasters.")
    parser.add_argument("--config", required=True, help="Path to configs/depstor/depstor_rasters.yml")
    parser.add_argument("--base_config", default=None, help="Path to configs/base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--step", default=None, help="Run only this one step")
    parser.add_argument("--from", dest="from_step", default=None, help="Resume from this step")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    if args.step and args.from_step:
        parser.error("--step and --from are mutually exclusive")

    logger = configure_logging("build_depstor_rasters")
    t_start = time.time()

    raw_config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    # `load_config` resolves top-level scalar placeholders. The unified config
    # has nested step blocks (lists of dicts) whose path templates won't be
    # touched, so resolve them here against the same substitution map.
    replacements = {"data_root": raw_config["data_root"], "fabric": raw_config["fabric"]}
    config = {k: _resolve_nested(v, replacements) for k, v in raw_config.items()}

    all_steps = config["steps"]
    # Order the configured steps by canonical STEP_ORDER so users can list them
    # in any order in the YAML.
    step_index = {s["name"]: s for s in all_steps}
    unknown = set(step_index) - set(STEP_ORDER)
    if unknown:
        raise ValueError(f"Unknown step(s) in config: {sorted(unknown)}; expected subset of {STEP_ORDER}")
    ordered_steps = [step_index[n] for n in STEP_ORDER if n in step_index]

    run_steps = _select_steps(ordered_steps, args.step, args.from_step)

    logger.info("=== build_depstor_rasters ===")
    logger.info("Fabric    : %s", config["fabric"])
    logger.info("Output dir: %s", config["output_dir"])
    logger.info("Running %d step(s): %s", len(run_steps), [s["name"] for s in run_steps])

    ctx = _build_context(config, force=args.force)
    _hydrate_existing_outputs(ctx, ordered_steps, run_steps, logger)

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

    logger.info("=== build_depstor_rasters complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
