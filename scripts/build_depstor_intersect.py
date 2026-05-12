"""Build a binary intersection raster from two aligned uint8 inputs.

A cell is 1 in the output where BOTH `input_a` and `input_b` equal 1 (the
depstor 1/255 binary convention); 255 otherwise. Used for the two PRMS Level-5
intersection masks needed by issue #61:

- drains_perv_binary.tif  = drains_to_dprst.tif  ∩ perv_binary.tif
- drains_imperv_binary.tif = drains_to_dprst.tif ∩ imperv_binary.tif

These per-cell intersections are aggregated to per-HRU fractions downstream by
`create_zonal_params.py`; pairing each with the matching denominator fraction
(perv_frac, imperv_frac) gives the PRMS `sro_to_dprst_perv` and
`sro_to_dprst_imperv` ratios (see scripts/derive_depstor_ratios.py).
"""

import argparse
import time
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import RasterInfo, intersect_binaries
from gfv2_params.log import configure_logging

STRIP_ROWS = 1024


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _uint8_binary_profile(info: RasterInfo) -> dict:
    return {
        "driver": "GTiff",
        "height": info.height,
        "width": info.width,
        "count": 1,
        "dtype": "uint8",
        "crs": info.crs,
        "transform": info.transform,
        "nodata": 255,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "YES",
    }


def _assert_aligned(src, info: RasterInfo, name: str) -> None:
    if (src.width, src.height) != (info.width, info.height):
        raise ValueError(
            f"{name} shape ({src.width}x{src.height}) != template "
            f"({info.width}x{info.height})"
        )
    if src.crs != info.crs:
        raise ValueError(f"{name} CRS {src.crs} != template CRS {info.crs}")
    if src.transform != info.transform:
        raise ValueError(f"{name} transform mismatch with template")


def main():
    parser = argparse.ArgumentParser(description="Intersect two uint8 binary rasters cell-wise.")
    parser.add_argument("--config", required=True, help="Path to depstor intersect config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_intersect")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    template_path = Path(require_config_key(config, "template_raster", "build_depstor_intersect"))
    input_a_path = Path(config["input_a_raster"])
    input_b_path = Path(config["input_b_raster"])
    output_path = Path(config["output_raster"])

    for p in (template_path, input_a_path, input_b_path):
        if not p.exists():
            raise FileNotFoundError(f"Required input not found: {p}")

    logger.info("=== build_depstor_intersect ===")
    logger.info("Template : %s", template_path)
    logger.info("Input A  : %s", input_a_path)
    logger.info("Input B  : %s", input_b_path)
    logger.info("Output   : %s", output_path)

    if output_path.exists() and not args.force:
        logger.info("Output exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("--- Streaming intersection over %d-row strips ---", STRIP_ROWS)
    t1 = time.time()
    n_hit = 0
    profile = _uint8_binary_profile(info)

    with rasterio.open(input_a_path) as a_src, \
         rasterio.open(input_b_path) as b_src, \
         rasterio.open(output_path, "w", **profile) as dst:
        _assert_aligned(a_src, info, "input_a")
        _assert_aligned(b_src, info, "input_b")

        for row_off in range(0, info.height, STRIP_ROWS):
            h = min(STRIP_ROWS, info.height - row_off)
            window = Window(0, row_off, info.width, h)
            a = a_src.read(1, window=window)
            b = b_src.read(1, window=window)
            out = intersect_binaries(a, b)
            dst.write(out, 1, window=window)
            n_hit += int((out == 1).sum())

    total = info.height * info.width
    logger.info(
        "  %d intersection cells (%.4f%% of grid). Built in %s",
        n_hit, 100 * n_hit / total, _elapsed(t1),
    )

    logger.info("=== build_depstor_intersect complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
