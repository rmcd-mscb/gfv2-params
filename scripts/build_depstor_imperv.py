"""Build the imperviousness binary raster used by the depression-storage pipeline.

Threshold the staged Imperv.tif at imperv_threshold (default 50%), warp/snap to the
elevation-VRT template grid, and write a uint8 binary raster (1 = impervious,
255 = nodata) to {fabric}/depstor_rasters/imperv_binary.tif.

Logic ported from depstor/scripts/DepStor.py:452-518 — minus the HRU-tagging step.
"""

import argparse
import time
from pathlib import Path

import rasterio
from osgeo import gdal, gdalconst

from gfv2_params.config import load_config
from gfv2_params.depstor import RasterInfo, threshold_above, write_uint8_binary
from gfv2_params.log import configure_logging


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _warp_to_template(src_path: Path, info: RasterInfo, out_path: Path) -> None:
    """Warp src_path to the template grid (EPSG:5070, 30m, exact bounds).

    Uses bilinear resampling (Imperv is a continuous percentage 0-100).
    """
    output_bounds = (info.bounds.left, info.bounds.bottom, info.bounds.right, info.bounds.top)
    warp_ds = gdal.Warp(
        str(out_path),
        str(src_path),
        format="GTiff",
        outputBounds=output_bounds,
        width=info.width,
        height=info.height,
        dstSRS=info.crs.to_string(),
        resampleAlg=gdalconst.GRA_Bilinear,
        outputType=gdal.GDT_Float32,
        creationOptions=[
            "COMPRESS=LZW", "TILED=YES",
            "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
        ],
    )
    if warp_ds is None:
        raise RuntimeError(
            f"gdal.Warp failed: {src_path} -> {out_path} ({gdal.GetLastErrorMsg()})"
        )
    warp_ds.FlushCache()
    del warp_ds


def main():
    parser = argparse.ArgumentParser(description="Build depstor imperv_binary.tif.")
    parser.add_argument("--config", required=True, help="Path to depstor_imperv_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_imperv")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )

    template_path = Path(config["template_raster"])
    imperv_path = Path(config["imperv_raster"])
    output_path = Path(config["output_raster"])
    threshold = float(config.get("imperv_threshold", 50))

    if not template_path.exists():
        raise FileNotFoundError(f"Template raster not found: {template_path}")
    if not imperv_path.exists():
        raise FileNotFoundError(f"Imperv raster not found: {imperv_path}")

    logger.info("=== build_depstor_imperv ===")
    logger.info("Template : %s", template_path)
    logger.info("Imperv   : %s", imperv_path)
    logger.info("Output   : %s", output_path)
    logger.info("Threshold: %s%% (cells >= threshold marked impervious)", threshold)

    if output_path.exists() and not args.force:
        logger.info("Output already exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    logger.info("Template grid: %dx%d, CRS=%s", info.width, info.height, info.crs)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    warped_path = output_path.with_suffix(".warped.tif")
    try:
        logger.info("--- Step 1/2: Warp imperv to template grid ---")
        t1 = time.time()
        _warp_to_template(imperv_path, info, warped_path)
        logger.info("  Warp done in %s: %s", _elapsed(t1), warped_path)

        logger.info("--- Step 2/2: Threshold and write uint8 binary ---")
        t2 = time.time()
        with rasterio.open(warped_path) as src:
            data = src.read(1)
            src_nodata = src.nodata
        binary = threshold_above(data, threshold, src_nodata)
        write_uint8_binary(binary, info, output_path)
        n_impervious = int((binary == 1).sum())
        n_total = binary.size
        logger.info(
            "  Threshold + write done in %s | %d / %d cells impervious (%.2f%%)",
            _elapsed(t2), n_impervious, n_total, 100 * n_impervious / n_total,
        )
    finally:
        if warped_path.exists():
            warped_path.unlink()
            logger.debug("  Cleaned up intermediate: %s", warped_path)

    logger.info("=== build_depstor_imperv complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
