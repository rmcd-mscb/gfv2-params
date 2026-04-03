import logging
import math
import time
from pathlib import Path

import numpy as np
import rasterio
from osgeo import gdal, gdalconst

logger = logging.getLogger(__name__)


def resample(
    src_path: str,
    template_path: str,
    intermediate_path: str,
    output_path: str,
    mask_values=(128, 0),
    mask_negative=True,
) -> None:
    """Reproject and resample src_path raster to match template_path's spatial reference.

    Writes the result to intermediate_path, applies NoData masking,
    and saves the final raster to output_path.
    """
    src = gdal.Open(src_path, gdalconst.GA_ReadOnly)
    if src is None:
        raise FileNotFoundError(f"Source raster not found: {src_path}")
    src_proj = src.GetProjection()

    tmpl = gdal.Open(template_path, gdalconst.GA_ReadOnly)
    if tmpl is None:
        raise FileNotFoundError(f"Template raster not found: {template_path}")
    tmpl_proj = tmpl.GetProjection()
    tmpl_geotrans = tmpl.GetGeoTransform()
    width = tmpl.RasterXSize
    height = tmpl.RasterYSize

    driver = gdal.GetDriverByName("GTiff")
    if driver is None:
        raise RuntimeError("GDAL GTiff driver not available")
    dst = driver.Create(
        intermediate_path,
        width,
        height,
        1,
        gdalconst.GDT_Float32,
        options=["COMPRESS=LZW", "TILED=YES", "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES"],
    )
    if dst is None:
        raise RuntimeError(f"Failed to create output raster: {intermediate_path}")
    dst.SetGeoTransform(tmpl_geotrans)
    dst.SetProjection(tmpl_proj)

    logger.info(
        "  GDAL ReprojectImage: %s -> %s  (%d x %d pixels)",
        Path(src_path).name,
        Path(intermediate_path).name,
        width,
        height,
    )
    t_gdal = time.time()
    err = gdal.ReprojectImage(src, dst, src_proj, tmpl_proj, gdalconst.GRA_NearestNeighbour)
    if err != 0:
        raise RuntimeError(f"GDAL ReprojectImage failed with error code {err}")
    del dst
    del src
    del tmpl
    logger.info("  GDAL reproject done in %.0f s", time.time() - t_gdal)

    logger.info("  Applying NoData mask (block-wise) and writing LZW output: %s", Path(output_path).name)
    t_mask = time.time()
    with rasterio.open(intermediate_path) as src_rio:
        profile = src_rio.profile
        profile.update(
            dtype=rasterio.float64,
            count=1,
            compress="lzw",
            tiled=True,
            blockxsize=512,
            blockysize=512,
            BIGTIFF="YES",
        )
        n_windows = sum(1 for _ in src_rio.block_windows(1))
        log_every = max(1, n_windows // 10)
        logger.info("  Masking %d windows", n_windows)

        with rasterio.open(output_path, "w", **profile) as dst_rio:
            for i, (_, window) in enumerate(src_rio.block_windows(1), start=1):
                data = src_rio.read(1, window=window).astype(np.float64)
                for val in mask_values:
                    data[data == val] = np.nan
                if mask_negative:
                    data[data < 0] = np.nan
                dst_rio.write(data, 1, window=window)
                if i % log_every == 0 or i == n_windows:
                    elapsed = time.time() - t_mask
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (n_windows - i) / rate if rate > 0 else 0
                    logger.info(
                        "  Mask window %d/%d (%.0f%%) | elapsed=%.0fs | ETA=%.0fs",
                        i, n_windows, 100 * i / n_windows, elapsed, eta,
                    )
    logger.info("  NoData mask + write done in %.0f s", time.time() - t_mask)
    # remove large intermediate
    Path(intermediate_path).unlink(missing_ok=True)
    logger.info("  Removed intermediate: %s", intermediate_path)


def mult_rasters(
    rast1_path: str,
    rast2_path: str,
    out_path: str,
    nodata_value: float = None,
) -> None:
    """Multiply two single-band rasters and write the result.

    Handles NoData values. Assumes input rasters are aligned.
    """
    with rasterio.open(rast1_path) as src1, rasterio.open(rast2_path) as src2:
        if src1.shape != src2.shape:
            raise ValueError("Input rasters do not have the same shape.")
        if src1.transform != src2.transform:
            raise ValueError("Input rasters do not have the same geotransform.")
        if src1.crs != src2.crs:
            raise ValueError("Input rasters do not have the same CRS.")

        arr1 = src1.read(1).astype(np.float64)
        arr2 = src2.read(1).astype(np.float64)

        nodata1 = src1.nodata
        nodata2 = src2.nodata

        mask = np.full(arr1.shape, False, dtype=bool)
        if nodata1 is not None:
            mask |= arr1 == nodata1
        if nodata2 is not None:
            mask |= arr2 == nodata2

        result = np.where(~mask, arr1 * arr2, np.nan)

        profile = src1.profile.copy()
        profile.update(
            dtype=rasterio.float64,
            count=1,
            compress="lzw",
            nodata=nodata_value if nodata_value is not None else np.nan,
        )

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(result, 1)


def compute_radtrn(
    lulc_path: str,
    cnpy_path: str,
    keep_path: str,
    out_path: str,
    tree_threshold: int = 3,
    block_size: int = 2048,
) -> None:
    """Compute radiation transmission raster.

    radtrn = (cnpy * keep / 100) where lulc >= tree_threshold, else 0.
    Processes in blocks to handle CONUS-scale rasters.
    Assumes all three input rasters are aligned (same shape, transform, CRS).
    """
    with rasterio.open(lulc_path) as lulc_src, \
         rasterio.open(cnpy_path) as cnpy_src, \
         rasterio.open(keep_path) as keep_src:

        if not (lulc_src.shape == cnpy_src.shape == keep_src.shape):
            raise ValueError("Input rasters do not have the same shape.")

        profile = lulc_src.profile.copy()
        profile.update(dtype=rasterio.float32, count=1, compress="lzw", nodata=0.0)

        height, width = lulc_src.shape
        n_row_blocks = math.ceil(height / block_size)
        n_col_blocks = math.ceil(width / block_size)
        total_blocks = n_row_blocks * n_col_blocks
        log_every = max(1, total_blocks // 10)  # ~10% increments
        logger.info(
            "  Grid: %d x %d pixels | block_size=%d | %d x %d = %d blocks",
            height, width, block_size, n_row_blocks, n_col_blocks, total_blocks,
        )
        t_radtrn = time.time()
        block_num = 0

        with rasterio.open(out_path, "w", **profile) as dst:
            for row_off in range(0, height, block_size):
                for col_off in range(0, width, block_size):
                    win_height = min(block_size, height - row_off)
                    win_width = min(block_size, width - col_off)
                    window = rasterio.windows.Window(col_off, row_off, win_width, win_height)

                    lulc = lulc_src.read(1, window=window).astype(np.int16)
                    cnpy = cnpy_src.read(1, window=window).astype(np.float32)
                    keep = keep_src.read(1, window=window).astype(np.float32)

                    result = np.where(lulc >= tree_threshold, cnpy * keep / 100.0, 0.0)
                    dst.write(result.astype(np.float32), 1, window=window)

                    block_num += 1
                    if block_num % log_every == 0 or block_num == total_blocks:
                        pct = 100 * block_num / total_blocks
                        elapsed = time.time() - t_radtrn
                        rate = block_num / elapsed if elapsed > 0 else 0
                        eta = (total_blocks - block_num) / rate if rate > 0 else 0
                        logger.info(
                            "  Block %d/%d (%.0f%%) | row_off=%d | elapsed=%.0fs | ETA=%.0fs",
                            block_num, total_blocks, pct, row_off, elapsed, eta,
                        )
        logger.info("  compute_radtrn done in %.0f s", time.time() - t_radtrn)


def deg_to_fraction(slope_deg: float) -> float:
    """Convert slope from degrees to fractional slope (rise/run)."""
    return np.tan(np.deg2rad(slope_deg))
