"""Pre-compute derived rasters (soil_moist_max) from source inputs.

Thin CLI shell over the ``gfv2_params.shared_rasters.build_derived_rasters``
library entrypoint. Preserves the original argparse interface so existing
sbatch jobs keep working unchanged.

For unified DAG-style invocation across all shared rasters, prefer:
    python scripts/build_shared_rasters.py --config configs/shared_rasters.yml
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import SharedRastersContext
from gfv2_params.shared_rasters.build_derived_rasters import build


def main():
    parser = argparse.ArgumentParser(description="Build derived rasters from source inputs.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    logger = configure_logging("build_derived_rasters")

    base = load_base_config(
        Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    data_root = Path(base["data_root"])
    ctx = SharedRastersContext(
        data_root=data_root,
        vpus=[],  # CONUS-once; no per-VPU iteration
        output_dir=data_root / "work",
        force=args.force,
    )
    build({}, ctx, logger)


if __name__ == "__main__":
    main()
