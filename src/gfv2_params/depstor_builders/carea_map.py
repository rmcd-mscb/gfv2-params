"""Build the two carea_map binary rasters (PRMS TWI thresholds 8.0 and 15.6)."""

from __future__ import annotations

from contextlib import ExitStack

import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from ..depstor import RasterInfo, compute_carea_map_binary
from .context import BuildContext

# TWI is float32 — ~4x the per-strip memory of the uint8 inputs.
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
    if ctx.twi_raster is None:
        raise KeyError("carea_map step needs `twi_raster` in fabric profile.")
    if not ctx.twi_raster.exists():
        raise FileNotFoundError(f"TWI raster not found: {ctx.twi_raster}")

    landmask_path = ctx.require("landmask")
    perv_path = ctx.require("perv")
    onstream_path = ctx.require("onstream")

    thresholds = step_cfg["thresholds"]
    outputs = step_cfg["outputs"]
    runs = [
        (float(thresholds["carea_max"]), ctx.resolve_output(outputs["carea_max"]), "carea_max"),
        (float(thresholds["smidx"]),     ctx.resolve_output(outputs["smidx"]),     "smidx_coef"),
    ]

    logger.info("--- carea_map ---")
    logger.info("  TWI     : %s", ctx.twi_raster)
    for _, out, label in runs:
        logger.info("  Output (%s): %s", label, out)

    if not ctx.force and all(out.exists() for _, out, _ in runs):
        logger.info("  All outputs exist — skipping (pass --force to rebuild)")
        return {"carea_max": runs[0][1], "smidx": runs[1][1]}

    info = RasterInfo.from_path(ctx.template_path)
    for _, out, _ in runs:
        out.parent.mkdir(parents=True, exist_ok=True)

    counts = [0 for _ in runs]
    profile = _uint8_binary_profile(info)

    with ExitStack() as stack:
        landmask_src = stack.enter_context(rasterio.open(landmask_path))
        perv_src = stack.enter_context(rasterio.open(perv_path))
        onstream_src = stack.enter_context(rasterio.open(onstream_path))
        twi_src = stack.enter_context(rasterio.open(ctx.twi_raster))
        _assert_aligned(landmask_src, info, "land_mask")
        _assert_aligned(perv_src, info, "perv")
        _assert_aligned(onstream_src, info, "onstream")
        if twi_src.crs != info.crs:
            raise ValueError(f"TWI CRS {twi_src.crs} != template CRS {info.crs}")

        # Nearest-neighbour warping is only exact when origin offsets are
        # whole-cell multiples — verify before opening the VRT.
        col_offset = twi_src.transform.c - info.transform.c
        row_offset = twi_src.transform.f - info.transform.f
        cell_x = info.transform.a
        cell_y = info.transform.e
        col_frac = (col_offset / cell_x) - round(col_offset / cell_x)
        row_frac = (row_offset / cell_y) - round(row_offset / cell_y)
        if abs(col_frac) > 1e-6 or abs(row_frac) > 1e-6:
            raise ValueError(
                f"TWI origin not whole-cell-aligned with template: "
                f"col_offset={col_offset}, row_offset={row_offset}, "
                f"cell=({cell_x}, {cell_y}), fractional pixel offset = "
                f"({col_frac:.2e}, {row_frac:.2e}). Re-stage TWI on the template grid."
            )

        vrt_options = {
            "crs": info.crs,
            "transform": info.transform,
            "width": info.width,
            "height": info.height,
            "resampling": Resampling.nearest,
            "nodata": twi_src.nodata,
        }
        twi_nodata = twi_src.nodata

        twi_vrt = stack.enter_context(WarpedVRT(twi_src, **vrt_options))
        dsts = [stack.enter_context(rasterio.open(out, "w", **profile)) for _, out, _ in runs]

        for row_off in range(0, info.height, STRIP_ROWS):
            h = min(STRIP_ROWS, info.height - row_off)
            window = Window(0, row_off, info.width, h)
            land_valid = landmask_src.read(1, window=window) == 1
            perv = perv_src.read(1, window=window)
            onstream = onstream_src.read(1, window=window)
            twi = twi_vrt.read(1, window=window)
            for i, (thresh, _, _) in enumerate(runs):
                out = compute_carea_map_binary(
                    perv, onstream, twi, thresh, twi_nodata, land_valid
                )
                dsts[i].write(out, 1, window=window)
                counts[i] += int((out == 1).sum())

    total = info.height * info.width
    for (thresh, out, label), n in zip(runs, counts):
        logger.info(
            "  %s: %d cells (%.4f%% of grid) -> %s",
            label, n, 100 * n / total, out,
        )

    return {"carea_max": runs[0][1], "smidx": runs[1][1]}
