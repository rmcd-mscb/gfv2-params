"""Open-source TWI/FDR/FAC/slope/aspect from per-VPU merged Hydrodem.

Thin CLI shell over the ``gfv2_params.shared_rasters.compute_dem_derivatives``
library entrypoint. Preserves the original argparse interface so existing
sbatch jobs keep working unchanged.

**Status: parallel-artifact pipeline, not the canonical TWI source.** See
the library module docstring (and the [[twi_canonical_source]] memory) for
why this output is not consumed by the downstream PRMS parameter pipeline.

For unified DAG-style invocation across all shared rasters, prefer:
    python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml
(opt in by adding `compute_dem_derivatives` to the steps: list).
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import SharedRastersContext
from gfv2_params.shared_rasters.compute_dem_derivatives import build


def main():
    parser = argparse.ArgumentParser(
        description="Compute open-source DEM derivatives (filled DEM, slope, aspect, TWI) from Hydrodem.",
    )
    parser.add_argument("--config", required=True, help="Path to compute_dem_derivatives.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 06")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    logger = configure_logging("compute_dem_derivatives")

    config = load_config(Path(args.config), vpu=args.vpu, fabric=args.fabric)
    data_root = Path(config["data_root"])
    ctx = SharedRastersContext(
        data_root=data_root,
        vpus=[args.vpu],
        output_dir=data_root / "work",
        force=args.force,
    )
    step_cfg = {
        "input_dir": config["input_dir"],
        "output_dir": config["output_dir"],
    }
    build(step_cfg, ctx, logger)


if __name__ == "__main__":
    main()
