import os
import yaml
from pathlib import Path
import rasterio
import numpy as np
from osgeo import gdal, gdalconst

def load_config(path: Path) -> dict:
    with open(path, "r") as file:
        config = yaml.safe_load(file)
    return config

def resample(
    src_path: str,
    template_path: str,
    intermediate_path: str,
    output_path: str,
    mask_values=(128, 0),
    mask_negative=True
) -> None:
    """
    Reprojects and resamples src_path raster to match template_path's spatial reference,
    writes the result to intermediate_path, applies NoData masking, and saves final raster to output_path.

    Parameters
    ----------
    src_path : str
        Path to source raster to reproject.
    template_path : str
        Path to template raster whose grid/projection will be matched.
    intermediate_path : str
        Path to temporary file for resampled raster.
    output_path : str
        Path to write the final processed raster.
    mask_values : tuple or list, default=(128, 0)
        Values to set to np.nan in the final output.
    mask_negative : bool, default=True
        If True, set all negative values to np.nan in the final output.
    """
    # Open source and template rasters with GDAL
    src = gdal.Open(src_path, gdalconst.GA_ReadOnly)
    if src is None:
        raise FileNotFoundError(f"Source raster not found: {src_path}")
    src_proj = src.GetProjection()
    src_geotrans = src.GetGeoTransform()

    tmpl = gdal.Open(template_path, gdalconst.GA_ReadOnly)
    if tmpl is None:
        raise FileNotFoundError(f"Template raster not found: {template_path}")
    tmpl_proj = tmpl.GetProjection()
    tmpl_geotrans = tmpl.GetGeoTransform()
    width = tmpl.RasterXSize
    height = tmpl.RasterYSize

    # Create the destination raster
    driver = gdal.GetDriverByName('GTiff')
    dst = driver.Create(intermediate_path, width, height, 1, gdalconst.GDT_Float32)
    dst.SetGeoTransform(tmpl_geotrans)
    dst.SetProjection(tmpl_proj)

    # Reproject and resample
    gdal.ReprojectImage(
        src, dst, src_proj, tmpl_proj, gdalconst.GRA_NearestNeighbour
    )
    del dst  # Ensure it's written to disk

    # Mask and save final raster with rasterio
    with rasterio.open(intermediate_path) as src_rio:
        data = src_rio.read(1)
        profile = src_rio.profile
        profile.update(dtype=rasterio.float64, count=1, compress='lzw')

        # Mask values
        for val in mask_values:
            data[data == val] = np.nan
        if mask_negative:
            data[data < 0] = np.nan

        # Write final raster
        with rasterio.open(output_path, 'w', **profile) as dst_rio:
            dst_rio.write(data.astype(rasterio.float64), 1)

def mult_rasters(
    rast1_path: str,
    rast2_path: str,
    out_path: str,
    nodata_value: float = None
) -> None:
    """
    Multiplies two single-band rasters and writes the result to out_path.
    Handles NoData values and assumes input rasters are perfectly aligned.

    Parameters
    ----------
    rast1_path : str
        Path to the first raster file.
    rast2_path : str
        Path to the second raster file.
    out_path : str
        Path to write the output raster.
    nodata_value : float, optional
        Value to use for NoData in the output (defaults to None, meaning use input raster's NoData).

    Returns
    -------
    None
    """
    with rasterio.open(rast1_path) as src1, rasterio.open(rast2_path) as src2:
        # Check dimensions and transforms match
        if src1.shape != src2.shape:
            raise ValueError("Input rasters do not have the same shape.")
        if src1.transform != src2.transform:
            raise ValueError("Input rasters do not have the same geotransform.")
        if src1.crs != src2.crs:
            raise ValueError("Input rasters do not have the same CRS.")

        arr1 = src1.read(1).astype(np.float64)
        arr2 = src2.read(1).astype(np.float64)

        # Get NoData values
        nodata1 = src1.nodata
        nodata2 = src2.nodata

        # Create mask for NoData values
        mask = np.full(arr1.shape, False, dtype=bool)
        if nodata1 is not None:
            mask |= arr1 == nodata1
        if nodata2 is not None:
            mask |= arr2 == nodata2

        # Perform multiplication only where data is valid
        result = np.where(~mask, arr1 * arr2, np.nan)

        # Prepare output profile
        profile = src1.profile.copy()
        profile.update(
            dtype=rasterio.float64,
            count=1,
            compress='lzw',
            nodata=nodata_value if nodata_value is not None else np.nan,
        )

        # Write output raster
        with rasterio.open(out_path, 'w', **profile) as dst:
            dst.write(result, 1)