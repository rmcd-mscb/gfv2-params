"""Build the pervious-area binary raster from imperv and dprst inputs.

A cell is pervious where it is neither impervious nor depression-storage.
Implements the PRMS-standard cell-wise interpretation of depstor workflow
Level Three step `GetPervAreaTotal` (docs/depstor_workflow.md) — NOT depstor's
`remove_all_overlap=True` all-or-nothing exclusion, which diverges from the
documented design intent.

Inputs (both on the same template grid):
- imperv_binary.tif (uint8 1/255) [from build_depstor_imperv.py]
- dprst_binary.tif  (uint8 1/255) [from build_depstor_dprst.py]

Output:
- perv_binary.tif : 1 where (imperv != 1) AND (dprst != 1), else 255.
"""

import argparse
import time
from pathlib import Path

import numpy as np

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import RasterInfo, read_aligned_uint8, write_uint8_binary
from gfv2_params.log import configure_logging


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def main():
    parser = argparse.ArgumentParser(description="Build depstor perv_binary.tif.")
    parser.add_argument("--config", required=True, help="Path to depstor_perv_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_perv")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    template_path = Path(require_config_key(config, "template_raster", "build_depstor_perv"))
    imperv_path = Path(config["imperv_raster"])
    dprst_path = Path(config["dprst_raster"])
    perv_path = Path(config["perv_raster"])

    for p in (template_path, imperv_path, dprst_path):
        if not p.exists():
            raise FileNotFoundError(f"Required input not found: {p}")

    logger.info("=== build_depstor_perv ===")
    logger.info("Template      : %s", template_path)
    logger.info("Imperv binary : %s", imperv_path)
    logger.info("Dprst binary  : %s", dprst_path)
    logger.info("Perv out      : %s", perv_path)

    if perv_path.exists() and not args.force:
        logger.info("Output exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)

    logger.info("--- Step 1/2: Read aligned inputs ---")
    t1 = time.time()
    imperv_binary = read_aligned_uint8(imperv_path, info)
    dprst_binary = read_aligned_uint8(dprst_path, info)
    logger.info("  Inputs read in %s", _elapsed(t1))

    logger.info("--- Step 2/2: Build perv_binary (NOT imperv AND NOT dprst) ---")
    t2 = time.time()
    perv = np.where((imperv_binary != 1) & (dprst_binary != 1), np.uint8(1), np.uint8(255))
    write_uint8_binary(perv, info, perv_path)
    n_perv = int((perv == 1).sum())
    logger.info(
        "  %d cells marked pervious (%.4f%% of grid). Written in %s",
        n_perv, 100 * n_perv / perv.size, _elapsed(t2),
    )

    logger.info("=== build_depstor_perv complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
