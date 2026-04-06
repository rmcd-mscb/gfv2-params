"""Build Copernicus GLO-30 elevation fill for border HRUs (Canada/Mexico).

Downloads Copernicus 30m tiles covering border zones, mosaics them,
reprojects to EPSG:5070 at 30m, and computes slope/aspect via RichDEM.
The output tiles are placed in work/nhd_merged/copernicus_fill/ where
build_vrt.py picks them up as lower-priority fill behind NHDPlus tiles.
"""

import argparse
import time
from pathlib import Path

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

# Copernicus GLO-30 nodata: the COG tiles declare NO nodata value (None).
# We do not set srcNodata in gdal.Warp — all Copernicus pixel values
# (including legitimate 0m sea-level elevations) pass through as-is.
# The VRT source ordering ensures NHDPlus takes priority wherever it has
# valid data; Copernicus only contributes in the border gaps.

# Output nodata must match the pipeline convention (build_vrt.py srcNodata).
OUTPUT_NODATA = -9999


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


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
    fill_dir = data_root / "work" / "nhd_merged" / "copernicus_fill"
    fill_dir.mkdir(parents=True, exist_ok=True)

    elev_out = fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif"
    slope_out = fill_dir / "NEDSnapshot_merged_slope_copernicus.tif"
    aspect_out = fill_dir / "NEDSnapshot_merged_aspect_copernicus.tif"

    # --- Step 1: Compute tile list and download ---
    logger.info("=== Step 1/3: Download Copernicus GLO-30 tiles ===")
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
    logger.info("=== Step 2/3: Mosaic → reproject to EPSG:5070 ===")
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

    # --- Step 3: Compute slope and aspect via RichDEM ---
    if not slope_out.exists() or not aspect_out.exists() or args.force:
        logger.info("=== Step 3/3: Compute slope/aspect via RichDEM ===")
        logger.info("  Loading DEM: %s", elev_out)
        t3 = time.time()
        dem = rd.LoadGDAL(str(elev_out), no_data=OUTPUT_NODATA)

        logger.info("  Computing slope (degrees)...")
        slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
        rd.SaveGDAL(str(slope_out), slope)
        logger.info("  Slope saved: %s", slope_out)

        logger.info("  Computing aspect...")
        aspect = rd.TerrainAttribute(dem, attrib="aspect")
        rd.SaveGDAL(str(aspect_out), aspect)
        logger.info("  Aspect saved: %s", aspect_out)
        logger.info("  Slope/aspect complete in %s", _elapsed(t3))
    else:
        logger.info("  Slope/aspect outputs already exist — skipping")

    # Clean up raw VRT (intermediate, only if we created it this run)
    if raw_vrt.exists():
        raw_vrt.unlink()
        logger.info("  Cleaned up intermediate VRT: %s", raw_vrt)

    logger.info("=== build_border_dem complete in %s ===", _elapsed(t_start))
    logger.info("  Outputs in: %s", fill_dir)
    logger.info("  Run build_vrt.py to rebuild VRTs with the fill layer.")


if __name__ == "__main__":
    main()
