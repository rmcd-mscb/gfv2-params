"""Same-HRU drains: land cells draining to a depression in their OWN HRU.

Replaces the plain `intersect` for drains_perv/drains_imperv. The same-HRU
restriction is a RASTER-SPACE intersection (labeled drains == rasterised hru_id),
applied before aggregation -- NOT a gdptools operation -- because it is a
per-cell comparison gdptools' partial-pixel weights cannot express. It
reproduces the legacy `Con(rSro == hru)` (docs/0b_TB_depr_stor.py:214). The
per-HRU COUNT downstream still uses gdptools.
"""
from __future__ import annotations

import rasterio
from rasterio.windows import Window

from ..depstor import RasterInfo, assert_raster_aligned, same_hru_intersect, uint8_binary_profile
from .context import BuildContext

STRIP_ROWS = 1024


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    name = step_cfg["name"]
    inputs = step_cfg["inputs"]  # [drains_to_dprst_hru, hru_id, perv|imperv]
    if not isinstance(inputs, list) or len(inputs) != 3:
        raise ValueError(f"same_hru_drains step '{name}' needs inputs: [labeled, hru_id, land]")
    labeled_path = ctx.require(inputs[0])
    hru_path = ctx.require(inputs[1])
    land_path = ctx.require(inputs[2])
    output_path = ctx.resolve_output(step_cfg["output"])
    output_key = step_cfg.get("output_key", name)

    logger.info("--- %s (same-HRU) ---", name)
    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {output_key: output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    n_hit = 0
    with rasterio.open(labeled_path) as lab_src, rasterio.open(hru_path) as hru_src, \
            rasterio.open(land_path) as land_src, \
            rasterio.open(output_path, "w", **uint8_binary_profile(info)) as dst:
        assert_raster_aligned(lab_src, info, inputs[0])
        assert_raster_aligned(hru_src, info, inputs[1])
        assert_raster_aligned(land_src, info, inputs[2])
        for row_off in range(0, info.height, STRIP_ROWS):
            h = min(STRIP_ROWS, info.height - row_off)
            window = Window(0, row_off, info.width, h)
            out = same_hru_intersect(lab_src.read(1, window=window),
                                     hru_src.read(1, window=window),
                                     land_src.read(1, window=window))
            dst.write(out, 1, window=window)
            n_hit += int((out == 1).sum())
    if n_hit == 0:
        logger.warning(
            "  0 same-HRU %s cells — suspicious for drains_perv (expect some "
            "same-HRU pervious drainage on almost any fabric), but can be "
            "legitimate for drains_imperv on a low-impervious fabric. Not "
            "raising here: routing_hru's all-empty guard already hard-catches "
            "upstream truncation of drains_to_dprst_hru.", output_key,
        )
    else:
        logger.info("  %d same-HRU %s cells", n_hit, output_key)
    return {output_key: output_path}
