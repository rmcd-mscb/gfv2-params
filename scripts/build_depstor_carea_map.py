"""Build binary carea_map rasters for the two PRMS TWI thresholds.

A cell is 1 in each output where it is pervious AND either TWI exceeds the
configured threshold or it is on-stream storage; 255 otherwise. Mirrors the
ArcPy `getCareaMap` (docs/0b_TB_depr_stor.py:315-350) in binary form — HRU
identity is recovered downstream via gdptools polygon overlay at zonal-stats
time, so no HRU-ID burn is needed in the raster itself.

Produces two outputs in one pass to amortise the cost of reading perv,
onstream, and (reprojected) TWI strips:

- threshold 8.0  -> feeds PRMS `carea_max`
- threshold 15.6 -> feeds PRMS `smidx_coef`

TWI input is reprojected on the fly via rasterio WarpedVRT — the source TWI
sits on the same 30 m grid as the template but with a different extent/origin,
so nearest-neighbour resampling is exact (no fractional shift). Template cells
outside the source TWI extent receive the TWI nodata sentinel, so they fail
the `twi_valid` check in compute_carea_map_binary and end up as 255 in the
output regardless of their perv/onstream status.
"""

import argparse
import time
from contextlib import ExitStack
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import RasterInfo, compute_carea_map_binary
from gfv2_params.log import configure_logging

# TWI is float32 — roughly 4x the per-strip memory of the uint8 inputs. At
# CONUS width (~150k cols) one 1024-row strip is ~600 MB for TWI; well under
# the default 32 G sbatch allocation but worth noting.
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
    parser = argparse.ArgumentParser(description="Build depstor carea_map binary rasters.")
    parser.add_argument("--config", required=True, help="Path to depstor_carea_map_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_carea_map")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    template_path = Path(require_config_key(config, "template_raster", "build_depstor_carea_map"))
    twi_path = Path(require_config_key(config, "twi_raster", "build_depstor_carea_map"))
    perv_path = Path(config["perv_raster"])
    onstream_path = Path(config["onstream_raster"])

    runs = [
        (
            float(config["threshold_carea_max"]),
            Path(config["output_raster_carea_max"]),
            "carea_max (threshold 8.0)",
        ),
        (
            float(config["threshold_smidx"]),
            Path(config["output_raster_smidx"]),
            "smidx_coef (threshold 15.6)",
        ),
    ]

    for p in (template_path, twi_path, perv_path, onstream_path):
        if not p.exists():
            raise FileNotFoundError(f"Required input not found: {p}")

    logger.info("=== build_depstor_carea_map ===")
    logger.info("Template : %s", template_path)
    logger.info("Perv     : %s", perv_path)
    logger.info("Onstream : %s", onstream_path)
    logger.info("TWI      : %s", twi_path)
    for thresh, out, label in runs:
        logger.info("Output (%s): %s", label, out)

    if not args.force and all(out.exists() for _, out, _ in runs):
        logger.info("All outputs exist — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    for _, out, _ in runs:
        out.parent.mkdir(parents=True, exist_ok=True)

    logger.info("--- Streaming carea_map over %d-row strips ---", STRIP_ROWS)
    t1 = time.time()
    counts = [0 for _ in runs]
    profile = _uint8_binary_profile(info)

    with ExitStack() as stack:
        perv_src = stack.enter_context(rasterio.open(perv_path))
        onstream_src = stack.enter_context(rasterio.open(onstream_path))
        twi_src = stack.enter_context(rasterio.open(twi_path))
        _assert_aligned(perv_src, info, "perv")
        _assert_aligned(onstream_src, info, "onstream")
        if twi_src.crs != info.crs:
            raise ValueError(f"TWI CRS {twi_src.crs} != template CRS {info.crs}")

        # Nearest-neighbour warping is exact only when origin offsets are
        # whole-cell multiples — otherwise we'd be silently snapping to the
        # wrong source pixel. Verify before opening the VRT. The transforms
        # carry float64 rasterio truncation noise (~1e-9), so check that the
        # fractional pixel offset is near zero rather than exactly zero.
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
                f"({col_frac:.2e}, {row_frac:.2e}). Nearest-neighbour "
                f"resampling would lose alignment — re-stage the TWI on the "
                f"template grid."
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
            perv = perv_src.read(1, window=window)
            onstream = onstream_src.read(1, window=window)
            twi = twi_vrt.read(1, window=window)
            for i, (thresh, _, _) in enumerate(runs):
                out = compute_carea_map_binary(perv, onstream, twi, thresh, twi_nodata)
                dsts[i].write(out, 1, window=window)
                counts[i] += int((out == 1).sum())

    total = info.height * info.width
    for (thresh, out, label), n in zip(runs, counts):
        logger.info(
            "  %s: %d cells (%.4f%% of grid) -> %s",
            label, n, 100 * n / total, out,
        )

    logger.info("=== build_depstor_carea_map complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
