"""Drive every Part 2 zonal-pass param from configs/zonal/zonal_params.yml.

Three modes:
  --mode zonal --param <name> --batch_id <N>
        Array task: run the per-batch zonal/soils/lulc/ssflux work for one
        param over one HRU batch. Dispatches on the entry's `script:` tag
        (zonal / soils / lulc / ssflux) into the matching
        gfv2_params.zonal_runners.run_*_batch function.
  --mode merge --param <name>
        Combine per-batch CSVs for one param into the merged CSV.
        Same gfv2_params.zonal_runners.run_merge function that drove the
        legacy scripts/merge_params.py (retired in PR #85).
  --mode build_weights
        Compute the CONUS-wide P2P weight matrix that ssflux consumes.
        Honours --force.

The slurm wrapper slurm_batch/submit_zonal_params.sh loops every entry in
configs/zonal/zonal_params.yml's `params:` list and chains all three modes into a
per-param afterok DAG (with build_weights submitted first for entries that
carry `depends_on: build_weights`).

Pattern mirrors scripts/derive_depstor_params.py (PR #72).
"""

import argparse
import re
from pathlib import Path

from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.zonal_runners import BATCH_RUNNERS, run_build_weights, run_merge


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


def _load_resolved_config(args) -> dict:
    """Load zonal_params.yml + base_config.yml, resolve every {placeholder}."""
    raw = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    replacements = {"data_root": raw["data_root"], "fabric": raw["fabric"]}
    return {k: _resolve_nested(v, replacements) for k, v in raw.items()}


def _find_param(config: dict, name: str) -> dict:
    """Look up the param entry by `name`, raising with a helpful message if missing."""
    params = config["params"]
    for spec in params:
        if spec["name"] == name:
            return spec
    available = [s["name"] for s in params]
    raise ValueError(f"Param '{name}' not in config; available: {available}")


def _build_param_cfg(config: dict, entry: dict) -> dict:
    """Flatten defaults + entry into the shape zonal_runners.run_*_batch expects.

    The library functions read keys like `source_type`, `source_raster`,
    `batch_dir`, `output_dir`, `merged_file`, etc. We populate those by
    layering the entry over the defaults block + the fabric profile fields.

    `source_type` is set from `entry["name"]` so per-batch CSV subdirs +
    file prefixes namespace cleanly when multiple LULC sources run in
    parallel.
    """
    defaults = config.get("defaults", {})
    param_cfg = {**defaults, **entry}
    # source_type drives output_subdir + file_prefix in every run_*_batch.
    param_cfg["source_type"] = entry["name"]
    # Pull fabric + expected_max_hru_id from the resolved base config.
    param_cfg["fabric"] = config["fabric"]
    if "expected_max_hru_id" in config:
        param_cfg["expected_max_hru_id"] = config["expected_max_hru_id"]
    # id_feature is a fabric property (base_config.yml profile), not a per-step
    # default — pull it from the resolved base config so it flows through to
    # the merged parameter CSVs.
    param_cfg["id_feature"] = require_config_key(config, "id_feature", "derive_zonal_params")
    # hru_gpkg/hru_layer are also fabric properties (base_config.yml profile).
    # run_build_weights reads them off param_cfg instead of inferring a
    # {fabric}_nhru_merged.gpkg path. Required for every fabric (mirrors
    # id_feature), since the gpkg is also what prepare_fabric batched.
    param_cfg["hru_gpkg"] = require_config_key(config, "hru_gpkg", "derive_zonal_params")
    param_cfg["hru_layer"] = config.get("hru_layer", "nhru")
    return param_cfg


def run_zonal(args, logger) -> None:
    """Dispatch one batch to the right run_*_batch function based on `script:` tag."""
    config = _load_resolved_config(args)
    entry = _find_param(config, args.param)
    script_tag = entry.get("script")
    if script_tag not in BATCH_RUNNERS:
        raise ValueError(
            f"Param '{args.param}' has unknown script tag '{script_tag}'. "
            f"Available: {sorted(BATCH_RUNNERS)}"
        )
    param_cfg = _build_param_cfg(config, entry)
    logger.info("=== zonal: param=%s script=%s batch=%d ===",
                args.param, script_tag, args.batch_id)
    BATCH_RUNNERS[script_tag](param_cfg, args.batch_id, logger)


def run_merge_mode(args, logger) -> None:
    """Concat per-batch CSVs for one param into its merged output."""
    config = _load_resolved_config(args)
    entry = _find_param(config, args.param)
    param_cfg = _build_param_cfg(config, entry)
    logger.info("=== merge: param=%s ===", args.param)
    run_merge(param_cfg, logger)


def run_build_weights_mode(args, logger) -> None:
    """Build the CONUS-wide P2P weight matrix consumed by ssflux.

    Finds the ssflux entry in params: (or whatever entry carries
    `depends_on: build_weights`) and runs the weights computation against
    its source_shapefile + weight_dir.
    """
    config = _load_resolved_config(args)
    # Find the entry that declares the weights prereq. There should be
    # exactly one — typically `name: ssflux`.
    weights_consumers = [
        s for s in config["params"] if s.get("depends_on") == "build_weights"
    ]
    if not weights_consumers:
        raise ValueError(
            "No param entry declares `depends_on: build_weights`; nothing "
            "to do for --mode build_weights."
        )
    if len(weights_consumers) > 1:
        names = [s["name"] for s in weights_consumers]
        raise ValueError(
            f"Multiple param entries declare `depends_on: build_weights`: {names}. "
            "Expected exactly one (typically `ssflux`)."
        )
    entry = weights_consumers[0]
    param_cfg = _build_param_cfg(config, entry)
    logger.info("=== build_weights: param=%s ===", entry["name"])
    run_build_weights(param_cfg, logger, force=args.force)


def main():
    parser = argparse.ArgumentParser(description="Drive zonal-pass parameter derivation.")
    parser.add_argument("--config", required=True, help="Path to configs/zonal/zonal_params.yml")
    parser.add_argument("--base_config", default=None, help="Path to configs/base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--mode", required=True, choices=["zonal", "merge", "build_weights"])
    parser.add_argument("--param", default=None, help="Param name (required for zonal/merge)")
    parser.add_argument("--batch_id", type=int, default=None, help="Batch ID (zonal mode only)")
    parser.add_argument("--force", action="store_true", help="build_weights only: overwrite existing weight file")
    args = parser.parse_args()

    if args.mode in {"zonal", "merge"} and not args.param:
        parser.error(f"--param is required for --mode {args.mode}")
    if args.mode == "zonal" and args.batch_id is None:
        parser.error("--batch_id is required for --mode zonal")

    logger = configure_logging(f"derive_zonal_params:{args.mode}")
    if args.mode == "zonal":
        run_zonal(args, logger)
    elif args.mode == "merge":
        run_merge_mode(args, logger)
    elif args.mode == "build_weights":
        run_build_weights_mode(args, logger)
    else:
        # argparse already constrained choices; defensive
        parser.error(f"Unknown mode: {args.mode}")


if __name__ == "__main__":
    main()
