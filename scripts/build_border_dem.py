"""Build Copernicus GLO-30 elevation fill for border HRUs (Canada/Mexico).

Downloads Copernicus 30m tiles covering border zones, mosaics them,
reprojects to EPSG:5070 at 30m, then builds a composite elevation surface
by overlaying NHDPlus VPU tiles on top of Copernicus (NHDPlus takes priority
in the overlap zone via GDAL VRT last-source-wins ordering). Slope and aspect
are computed via RichDEM on this composite, then masked to retain only pixels
in the fill zone (where Copernicus has data but NHDPlus does not).

Output tiles are placed in work/nhd_merged/copernicus_fill/ where
build_vrt.py picks them up as lower-priority fill behind NHDPlus tiles.

Dependency: must run AFTER compute_slope_aspect.py (per-VPU), because it
needs the NHDPlus _fixed_ elevation tiles for the composite.
"""

import argparse
import time
from pathlib import Path

import numpy as np
import richdem as rd
from osgeo import gdal

from gfv2_params.config import load_base_config
from gfv2_params.download.copernicus_dem import download_tiles, tiles_for_bbox
from gfv2_params.log import configure_logging

# Border bounding boxes in EPSG:4326 (south, north, west, east).
# Deliberately generous — extra ocean tiles are skipped (404) and
# NHDPlus takes priority in overlapping areas via VRT source ordering.
BORDER_ZONES = {
    "canada": (41.0, 55.0, -141.0, -52.0),
    "mexico": (25.0, 33.0, -118.0, -96.0),
}

# Output nodata must match the pipeline convention (build_vrt.py srcNodata).
OUTPUT_NODATA = -9999

# Glob pattern for NHDPlus _fixed_ elevation tiles (written by compute_slope_aspect.py).
NHDPLUS_FIXED_PATTERN = "NEDSnapshot_merged_fixed_*.tif"
FILL_DIRS = {"copernicus_fill"}


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _build_nhdplus_vrt(nhd_merged_dir: Path, output_vrt: Path) -> Path:
    """Build a VRT from NHDPlus _fixed_ tiles only (no fill layers).

    Returns the VRT path, or raises if no tiles are found.
    """
    primary_files = sorted(
        f for f in nhd_merged_dir.glob(f"*/{NHDPLUS_FIXED_PATTERN}")
        if f.parent.name not in FILL_DIRS
    )
    if not primary_files:
        raise FileNotFoundError(
            f"No NHDPlus _fixed_ tiles found in {nhd_merged_dir}. "
            "Run compute_slope_aspect.py first."
        )
    vrt_options = gdal.BuildVRTOptions(
        resolution="highest", srcNodata=str(OUTPUT_NODATA),
    )
    vrt_ds = gdal.BuildVRT(
        str(output_vrt), [str(f) for f in primary_files], options=vrt_options,
    )
    if vrt_ds is None:
        raise RuntimeError("gdal.BuildVRT failed for NHDPlus-only VRT")
    vrt_ds.FlushCache()
    del vrt_ds
    return output_vrt


def _build_composite_vrt(
    copernicus_elev: Path, nhdplus_vrt: Path, output_vrt: Path,
) -> Path:
    """Build a composite elevation VRT: Copernicus first (low priority),
    NHDPlus last (high priority, wins in overlap).
    """
    vrt_options = gdal.BuildVRTOptions(
        resolution="highest", srcNodata=str(OUTPUT_NODATA),
    )
    vrt_ds = gdal.BuildVRT(
        str(output_vrt),
        [str(copernicus_elev), str(nhdplus_vrt)],
        options=vrt_options,
    )
    if vrt_ds is None:
        raise RuntimeError("gdal.BuildVRT failed for composite elevation")
    vrt_ds.FlushCache()
    del vrt_ds
    return output_vrt


def _mask_to_fill_zone(
    raw_raster: Path,
    copernicus_elev: Path,
    nhdplus_vrt: Path,
    output: Path,
) -> None:
    """Mask a raster to retain only pixels in the fill zone.

    Fill zone = pixels where Copernicus has valid data AND NHDPlus has nodata.
    """
    # Read the raw computed raster
    raw_ds = gdal.Open(str(raw_raster))
    raw_band = raw_ds.GetRasterBand(1)
    raw_data = raw_band.ReadAsArray().astype(np.float32)
    geotransform = raw_ds.GetGeoTransform()
    projection = raw_ds.GetProjection()
    rows, cols = raw_data.shape
    del raw_ds

    # The raw raster has composite extent (union of Copernicus + NHDPlus).
    # Both Copernicus and NHDPlus must be warped to match this extent.
    output_bounds = [
        geotransform[0],
        geotransform[3] + rows * geotransform[5],
        geotransform[0] + cols * geotransform[1],
        geotransform[3],
    ]
    warp_kwargs = dict(
        format="MEM",
        outputBounds=output_bounds,
        xRes=abs(geotransform[1]),
        yRes=abs(geotransform[5]),
        dstNodata=OUTPUT_NODATA,
        srcNodata=OUTPUT_NODATA,
    )

    # Read Copernicus elevation aligned to composite extent
    cop_ds = gdal.Warp("", str(copernicus_elev), **warp_kwargs)
    if cop_ds is None:
        raise RuntimeError("gdal.Warp to MEM failed for Copernicus readback")
    cop_data = cop_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    del cop_ds

    # Read NHDPlus VRT aligned to composite extent
    nhd_ds = gdal.Warp("", str(nhdplus_vrt), **warp_kwargs)
    if nhd_ds is None:
        raise RuntimeError("gdal.Warp to MEM failed for NHDPlus VRT readback")
    nhd_data = nhd_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    del nhd_ds

    # Build fill mask: Copernicus valid AND NHDPlus nodata
    fill_mask = (cop_data != OUTPUT_NODATA) & (nhd_data == OUTPUT_NODATA)
    masked = np.where(fill_mask, raw_data, np.float32(OUTPUT_NODATA))

    # Write output
    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(
        str(output), cols, rows, 1, gdal.GDT_Float32,
        options=[
            "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES",
            "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
        ],
    )
    out_ds.SetGeoTransform(geotransform)
    out_ds.SetProjection(projection)
    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(OUTPUT_NODATA)
    out_band.WriteArray(masked)
    out_ds.FlushCache()
    del out_ds


def main():
    parser = argparse.ArgumentParser(
        description="Build Copernicus DEM fill for border HRUs (Canada/Mexico).",
    )
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    logger = configure_logging("build_border_dem")
    t_start = time.time()

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = Path(base["data_root"])

    raw_dir = data_root / "input" / "copernicus_dem" / "raw"
    nhd_merged_dir = data_root / "work" / "nhd_merged"
    fill_dir = nhd_merged_dir / "copernicus_fill"
    fill_dir.mkdir(parents=True, exist_ok=True)

    elev_out = fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif"
    slope_out = fill_dir / "NEDSnapshot_merged_slope_copernicus.tif"
    aspect_out = fill_dir / "NEDSnapshot_merged_aspect_copernicus.tif"

    # --- Step 1: Compute tile list and download ---
    logger.info("=== Step 1/5: Download Copernicus GLO-30 tiles ===")
    all_labels = []
    for zone_name, (south, north, west, east) in BORDER_ZONES.items():
        labels = tiles_for_bbox(south, north, west, east)
        logger.info("  %s zone: %d tiles (%.0f°N–%.0f°N, %.0f°W–%.0f°W)",
                     zone_name, len(labels), south, north, abs(west), abs(east))
        all_labels.extend(labels)

    # Deduplicate (zones may overlap slightly)
    all_labels = sorted(set(all_labels))
    logger.info("  Total unique tiles: %d", len(all_labels))

    t1 = time.time()
    tile_paths = download_tiles(all_labels, raw_dir)
    logger.info("  Download complete in %s: %d tiles available", _elapsed(t1), len(tile_paths))

    if not tile_paths:
        logger.error("No tiles downloaded — cannot build border DEM")
        return

    # --- Step 2: Mosaic raw tiles and reproject ---
    logger.info("=== Step 2/5: Mosaic → reproject to EPSG:5070 ===")
    raw_vrt = fill_dir / "copernicus_raw.vrt"
    vrt_ds = gdal.BuildVRT(
        str(raw_vrt),
        [str(p) for p in tile_paths],
    )
    if vrt_ds is None:
        raise RuntimeError("gdal.BuildVRT failed for Copernicus raw tiles")
    vrt_ds.FlushCache()
    del vrt_ds
    logger.info("  Raw VRT: %s (%d sources)", raw_vrt, len(tile_paths))

    # Warp to EPSG:5070, 30m, nodata=-9999
    if not elev_out.exists() or args.force:
        logger.info("  Warping to EPSG:5070 at 30m (bilinear)...")
        t2 = time.time()
        warp_ds = gdal.Warp(
            str(elev_out),
            str(raw_vrt),
            dstSRS="EPSG:5070",
            xRes=30,
            yRes=30,
            resampleAlg="bilinear",
            dstNodata=OUTPUT_NODATA,
            outputType=gdal.GDT_Float32,
            creationOptions=[
                "COMPRESS=LZW",
                "PREDICTOR=2",
                "TILED=YES",
                "BLOCKXSIZE=512",
                "BLOCKYSIZE=512",
                "BIGTIFF=YES",
            ],
        )
        if warp_ds is None:
            raise RuntimeError("gdal.Warp failed")
        warp_ds.FlushCache()
        del warp_ds
        logger.info("  Warp complete in %s: %s", _elapsed(t2), elev_out)
    else:
        logger.info("  Elevation fill already exists: %s", elev_out)

    # --- Step 3: Build composite elevation (Copernicus + NHDPlus) ---
    logger.info("=== Step 3/5: Build composite elevation VRT ===")
    nhdplus_vrt = fill_dir / "nhdplus_only.vrt"
    composite_vrt = fill_dir / "composite_elevation.vrt"

    _build_nhdplus_vrt(nhd_merged_dir, nhdplus_vrt)
    logger.info("  NHDPlus-only VRT: %s", nhdplus_vrt)

    _build_composite_vrt(elev_out, nhdplus_vrt, composite_vrt)
    logger.info("  Composite VRT: %s", composite_vrt)

    # Clip composite to Copernicus extent — the composite VRT covers the
    # union of NHDPlus (all CONUS) + Copernicus, but we only need slope/aspect
    # for the Copernicus extent. Loading the full union into RichDEM would be
    # an unnecessary memory burden. The NHDPlus data in the overlap zone is
    # still included (it falls within the Copernicus bounds at 41-55°N).
    composite_clipped = fill_dir / "composite_elevation_clipped.tif"

    # --- Step 4: Compute slope/aspect from composite ---
    slope_raw = fill_dir / "slope_raw.tif"
    aspect_raw = fill_dir / "aspect_raw.tif"

    if not slope_out.exists() or not aspect_out.exists() or args.force:
        logger.info("=== Step 4/5: Compute slope/aspect from composite via RichDEM ===")

        # Get Copernicus extent to clip the composite
        cop_ds = gdal.Open(str(elev_out))
        cop_gt = cop_ds.GetGeoTransform()
        cop_cols = cop_ds.RasterXSize
        cop_rows = cop_ds.RasterYSize
        cop_bounds = [
            cop_gt[0],
            cop_gt[3] + cop_rows * cop_gt[5],
            cop_gt[0] + cop_cols * cop_gt[1],
            cop_gt[3],
        ]
        del cop_ds

        logger.info("  Clipping composite to Copernicus extent...")
        clip_ds = gdal.Warp(
            str(composite_clipped),
            str(composite_vrt),
            outputBounds=cop_bounds,
            xRes=30, yRes=30,
            dstNodata=OUTPUT_NODATA,
            srcNodata=OUTPUT_NODATA,
            outputType=gdal.GDT_Float32,
            creationOptions=[
                "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES",
                "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
            ],
        )
        if clip_ds is None:
            raise RuntimeError("gdal.Warp failed for composite clipping")
        clip_ds.FlushCache()
        del clip_ds

        logger.info("  Loading clipped composite DEM: %s", composite_clipped)
        t3 = time.time()
        dem = rd.LoadGDAL(str(composite_clipped), no_data=OUTPUT_NODATA)

        logger.info("  Computing slope (degrees)...")
        slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
        rd.SaveGDAL(str(slope_raw), slope)
        logger.info("  Raw slope saved: %s", slope_raw)

        logger.info("  Computing aspect...")
        aspect = rd.TerrainAttribute(dem, attrib="aspect")
        rd.SaveGDAL(str(aspect_raw), aspect)
        logger.info("  Raw aspect saved: %s", aspect_raw)
        logger.info("  Slope/aspect computation complete in %s", _elapsed(t3))

        # --- Step 5: Mask slope/aspect to fill zone ---
        logger.info("=== Step 5/5: Mask slope/aspect to fill zone ===")
        t4 = time.time()

        logger.info("  Masking slope to fill zone...")
        _mask_to_fill_zone(slope_raw, elev_out, nhdplus_vrt, slope_out)
        logger.info("  Masked slope saved: %s", slope_out)

        logger.info("  Masking aspect to fill zone...")
        _mask_to_fill_zone(aspect_raw, elev_out, nhdplus_vrt, aspect_out)
        logger.info("  Masked aspect saved: %s", aspect_out)

        logger.info("  Masking complete in %s", _elapsed(t4))

        # Clean up raw intermediates
        slope_raw.unlink(missing_ok=True)
        aspect_raw.unlink(missing_ok=True)
        logger.info("  Cleaned up raw slope/aspect intermediates")
    else:
        logger.info("  Slope/aspect outputs already exist — skipping")

    # Clean up intermediates
    for f in [raw_vrt, nhdplus_vrt, composite_vrt, composite_clipped]:
        if f.exists():
            f.unlink()
    logger.info("  Cleaned up intermediate files")

    logger.info("=== build_border_dem complete in %s ===", _elapsed(t_start))
    logger.info("  Outputs in: %s", fill_dir)
    logger.info("  Run build_vrt.py to rebuild VRTs with the fill layer.")


if __name__ == "__main__":
    main()
