"""Pre-compute LULC derived rasters (radiation transmission).

Thin CLI shell over the ``gfv2_params.shared_rasters.build_lulc_rasters``
library entrypoint. Preserves the original argparse interface so existing
per-source sbatch jobs (one invocation per LULC source) keep working
unchanged.

For unified DAG-style invocation across all shared rasters — including
processing every LULC source in one walk via the ``sources:`` list — prefer:
    python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import SharedRastersContext
from gfv2_params.shared_rasters.build_lulc_rasters import build


def main():
    parser = argparse.ArgumentParser(description="Build LULC derived rasters.")
    parser.add_argument("--config", required=True, help="Path to LULC step config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    logger = configure_logging("build_lulc_rasters")

    # Use load_base_config just to resolve data_root; the LULC builder loads
    # the --config YAML itself via a fabric-independent path.
    base = load_base_config(
        Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    data_root = Path(base["data_root"])
    ctx = SharedRastersContext(
        data_root=data_root,
        vpus=[],
        output_dir=data_root / "work",
        force=args.force,
    )
    build({"sources": [args.config]}, ctx, logger)


if __name__ == "__main__":
    main()
