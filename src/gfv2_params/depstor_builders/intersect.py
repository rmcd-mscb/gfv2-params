"""Cell-wise intersection of two aligned uint8 binary rasters.

Used for both drains_perv (drains_to_dprst x perv) and drains_imperv
(drains_to_dprst x imperv). The step config names the inputs by their
upstream-output key (`drains_to_dprst`, `perv`, `imperv`).
"""

from __future__ import annotations

import rasterio
from rasterio.windows import Window

from ..depstor import RasterInfo, intersect_binaries
from .context import BuildContext

STRIP_ROWS = 1024


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


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    name = step_cfg["name"]
    inputs = step_cfg["inputs"]
    if not isinstance(inputs, list) or len(inputs) != 2:
        raise ValueError(f"intersect step '{name}' requires `inputs: [a, b]`")
    input_a_path = ctx.require(inputs[0])
    input_b_path = ctx.require(inputs[1])
    output_path = ctx.resolve_output(step_cfg["output"])
    output_key = step_cfg.get("output_key", name)

    logger.info("--- %s ---", name)
    logger.info("  Input A: %s", input_a_path)
    logger.info("  Input B: %s", input_b_path)
    logger.info("  Output : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {output_key: output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
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
        "  %d intersection cells (%.4f%% of grid)",
        n_hit, 100 * n_hit / total,
    )

    return {output_key: output_path}
