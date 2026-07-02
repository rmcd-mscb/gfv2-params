"""Cell-wise intersection of two aligned uint8 binary rasters.

`build()` here is no longer wired into `drains_perv`/`drains_imperv` — those
steps were rewired to `same_hru_drains.build()` (a same-HRU-restricted
intersection, not a plain one; see that module's docstring), and this module
is not imported by `depstor_builders/__init__.py`'s `BUILDERS`/`STEP_ORDER`.
It is retained as a generic two-input intersection builder in case a future
step needs a plain cell-wise AND. `intersect_binaries` in `depstor.py` (the
helper this module wraps) is still exercised directly by
`tests/test_intersect_binaries.py`.
"""

from __future__ import annotations

import rasterio
from rasterio.windows import Window

from ..depstor import (
    RasterInfo,
    assert_raster_aligned,
    intersect_binaries,
    uint8_binary_profile,
)
from .context import BuildContext

STRIP_ROWS = 1024


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
    profile = uint8_binary_profile(info)

    with rasterio.open(input_a_path) as a_src, \
         rasterio.open(input_b_path) as b_src, \
         rasterio.open(output_path, "w", **profile) as dst:
        assert_raster_aligned(a_src, info, "input_a")
        assert_raster_aligned(b_src, info, "input_b")

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
