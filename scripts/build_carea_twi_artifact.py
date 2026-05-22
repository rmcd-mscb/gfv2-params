"""Build the carea/smidx threshold-sweep artifact for a fabric.

Reads the fabric profile (twi_raster, template_raster, hru_gpkg/layer, id_feature,
vpu) and the depstor rasters under {data_root}/{fabric}/depstor_rasters/, then runs
gfv2_params.threshold_sweep.build_artifact and saves a .npz the sweep notebook
loads. Heavy (full template grid) -> sbatch for large fabrics.

  pixi run --as-is python scripts/build_carea_twi_artifact.py --fabric oregon
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.threshold_sweep import build_artifact


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--base_config", default=None)
    ap.add_argument("--output", default=None,
                    help="Output .npz (default: {data_root}/{fabric}/params/carea_twi_artifact.npz)")
    ap.add_argument("--bin-width", type=float, default=0.05)
    args = ap.parse_args()

    logger = configure_logging("build_carea_twi_artifact")
    config = load_base_config(Path(args.base_config) if args.base_config else None,
                              fabric=args.fabric)
    data_root = config["data_root"]
    fabric = config["fabric"]
    twi_raster = Path(require_config_key(config, "twi_raster", "build_carea_twi_artifact"))
    template = Path(require_config_key(config, "template_raster", "build_carea_twi_artifact"))
    hru_gpkg = Path(require_config_key(config, "hru_gpkg", "build_carea_twi_artifact"))
    hru_layer = require_config_key(config, "hru_layer", "build_carea_twi_artifact")
    id_feature = require_config_key(config, "id_feature", "build_carea_twi_artifact")
    vpu_column = "vpu"  # multi-VPU fabrics carry it; single-VPU fabrics fall back to ""
    depstor = Path(data_root) / fabric / "depstor_rasters"
    out = Path(args.output) if args.output else Path(data_root) / fabric / "params" / "carea_twi_artifact.npz"

    twi_source = "hydrodem" if "hydrodem" in twi_raster.name else "arcpy"
    logger.info("Building artifact: fabric=%s twi=%s (%s)", fabric, twi_raster.name, twi_source)

    artifact = build_artifact(
        fabric=fabric, twi_raster=twi_raster, template_raster=template,
        hru_gpkg=hru_gpkg, hru_layer=hru_layer, id_feature=id_feature,
        perv_path=depstor / "perv_binary.tif",
        onstream_path=depstor / "onstream_binary.tif",
        landmask_path=depstor / "land_mask.tif",
        vpu_column=vpu_column, twi_source=twi_source,
        bin_width=args.bin_width, logger=logger,
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    artifact.save(out)
    logger.info("Wrote artifact -> %s (%d HRUs)", out, len(artifact.ids))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
