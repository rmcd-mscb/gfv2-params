"""Build VRT files from per-VPU merged GeoTIFFs and optional fill layers.

Thin CLI shell over the ``gfv2_params.shared_rasters.build_vrt`` library
entrypoint. Preserves the original argparse interface so existing
invocations keep working unchanged.

For unified DAG-style invocation across all shared rasters, prefer:
    python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import SharedRastersContext
from gfv2_params.shared_rasters.build_vrt import build


def main():
    parser = argparse.ArgumentParser(description="Build CONUS-wide VRTs from per-VPU rasters.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    args = parser.parse_args()

    logger = configure_logging("build_vrt")

    base = load_base_config(
        Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    data_root = Path(base["data_root"])
    ctx = SharedRastersContext(
        data_root=data_root,
        vpus=[],  # build_vrt is CONUS-once; discovers per-VPU sources by glob
        output_dir=data_root / "work",
    )
    build({}, ctx, logger)


if __name__ == "__main__":
    main()
