"""Build the per-VPU HRU-fabric land mask consumed by the TWI pipeline.

Thin CLI shell over the ``gfv2_params.shared_rasters.build_vpu_landmask``
library entrypoint. Preserves the original argparse interface so existing
sbatch jobs keep working unchanged.

For unified DAG-style invocation across all shared rasters, prefer:
    python scripts/build_shared_rasters.py --config configs/shared_rasters/shared_rasters.yml
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import SharedRastersContext
from gfv2_params.shared_rasters.build_vpu_landmask import build, build_vpu_landmask  # noqa: F401 — re-exported for tests


def main():
    parser = argparse.ArgumentParser(description="Build per-VPU HRU-fabric land mask for the TWI pipeline.")
    parser.add_argument("--config", required=True, help="Path to vpu_landmask_raster.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_vpu_landmask")

    # No --fabric: the per-VPU land mask is a fabric-independent product. The
    # config pins the HRU source to the canonical CONUS gfv2_nhru_merged.gpkg
    # and the output to shared/per_vpu/<vpu>/.
    config = load_config(
        Path(args.config),
        vpu=args.vpu,
        base_config_path=Path(args.base_config) if args.base_config else None,
    )

    template_raster = require_config_key(config, "template_raster", "build_vpu_landmask")
    hru_gpkg = require_config_key(config, "hru_gpkg", "build_vpu_landmask")
    hru_layer = require_config_key(config, "hru_layer", "build_vpu_landmask")
    output_raster = config["output_raster"]

    data_root = Path(config["data_root"])
    ctx = SharedRastersContext(
        data_root=data_root,
        vpus=[args.vpu],
        output_dir=data_root / "work",
        force=args.force,
    )
    # CLI-resolved paths already have {vpu} substituted (via load_config). The
    # builder's per-VPU loop calls .replace("{vpu}", vpu) which is a no-op on
    # strings with no placeholder, so passing the resolved paths through works
    # for both single-VPU (CLI) and multi-VPU (orchestrator) invocations.
    step_cfg = {
        "template_raster": str(template_raster),
        "hru_gpkg": hru_gpkg,
        "hru_layer": hru_layer,
        "output_raster": str(output_raster),
    }
    build(step_cfg, ctx, logger)


if __name__ == "__main__":
    main()
