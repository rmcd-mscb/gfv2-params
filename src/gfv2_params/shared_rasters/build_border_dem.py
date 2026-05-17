"""Copernicus GLO-30 elevation fill for border HRUs (Canada/Mexico).

Library entrypoint for the shared-raster orchestrator. The thin CLI shell at
scripts/build_border_dem.py delegates here so existing sbatch jobs keep
working unchanged.

Downloads Copernicus 30m tiles covering border zones, mosaics them,
reprojects to EPSG:5070 at 30m, then builds a composite elevation surface
by overlaying NHDPlus VPU tiles on top of Copernicus (NHDPlus takes priority
in the overlap zone via GDAL VRT last-source-wins ordering). Slope and aspect
are computed via RichDEM on this composite, then masked to retain only pixels
in the fill zone (where Copernicus has data but NHDPlus does not).

Output tiles are placed in ``work/nhd_merged/copernicus_fill/`` where the
build_vrt step lists them before NHDPlus tiles in the VRT (GDAL
last-source-wins means NHDPlus takes priority).

Dependency: must run AFTER compute_slope_aspect (per-VPU), because it needs
the NHDPlus ``_fixed_`` elevation tiles for the composite.
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import richdem as rd
from osgeo import gdal

from gfv2_params.download.copernicus_dem import download_tiles, tiles_for_bbox

from .context import SharedRastersContext

# Border bounding boxes in EPSG:4326 (south, north, west, east).
# Deliberately generous — extra ocean tiles are skipped (404) and NHDPlus
# takes priority in overlapping areas because the build_vrt step lists
# Copernicus fill tiles before NHDPlus tiles (GDAL last-source-wins).
BORDER_ZONES = {
    "canada": (41.0, 55.0, -141.0, -52.0),
    "mexico": (25.0, 33.0, -118.0, -96.0),
}

# Output nodata must match the pipeline convention (build_vrt srcNodata).
OUTPUT_NODATA = -9999

# Glob pattern for NHDPlus _fixed_ elevation tiles (written by compute_slope_aspect).
NHDPLUS_FIXED_PATTERN = "NEDSnapshot_merged_fixed_*.tif"
FILL_DIRS = {"copernicus_fill"}


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _build_nhdplus_vrt(nhd_merged_dir: Path, output_vrt: Path) -> Path:
    """Build a VRT from NHDPlus _fixed_ tiles only (no fill layers)."""
    primary_files = sorted(
        f for f in nhd_merged_dir.glob(f"*/{NHDPLUS_FIXED_PATTERN}")
        if f.parent.name not in FILL_DIRS
    )
    if not primary_files:
        raise FileNotFoundError(
            f"No NHDPlus _fixed_ tiles found in {nhd_merged_dir}. "
            "Run compute_slope_aspect first."
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
    """Composite elevation VRT: Copernicus first (low priority), NHDPlus last (wins overlap)."""
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


def _compute_fill_mask(
    copernicus_elev: Path,
    nhdplus_vrt: Path,
    geotransform: tuple,
    rows: int,
    cols: int,
) -> np.ndarray:
    """Fill zone = pixels where Copernicus has valid data AND NHDPlus has nodata."""
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

    cop_ds = gdal.Warp("", str(copernicus_elev), **warp_kwargs)
    if cop_ds is None:
        raise RuntimeError(
            f"gdal.Warp to MEM failed for Copernicus readback: "
            f"{copernicus_elev} — {gdal.GetLastErrorMsg()}"
        )
    cop_data = cop_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    del cop_ds

    nhd_ds = gdal.Warp("", str(nhdplus_vrt), **warp_kwargs)
    if nhd_ds is None:
        raise RuntimeError(
            f"gdal.Warp to MEM failed for NHDPlus VRT readback: "
            f"{nhdplus_vrt} — {gdal.GetLastErrorMsg()}"
        )
    nhd_data = nhd_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    del nhd_ds

    if cop_data.shape != (rows, cols) or nhd_data.shape != (rows, cols):
        raise RuntimeError(
            f"Shape mismatch after warp: expected ({rows}, {cols}), "
            f"got cop={cop_data.shape}, nhd={nhd_data.shape}"
        )

    # -9999 is exactly representable in float32, so equality is safe.
    return (cop_data != OUTPUT_NODATA) & (nhd_data == OUTPUT_NODATA)


def _apply_fill_mask(raw_raster: Path, fill_mask: np.ndarray, output: Path) -> None:
    raw_ds = gdal.Open(str(raw_raster))
    if raw_ds is None:
        raise RuntimeError(
            f"gdal.Open failed for raw raster: {raw_raster} — {gdal.GetLastErrorMsg()}"
        )
    raw_data = raw_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    geotransform = raw_ds.GetGeoTransform()
    projection = raw_ds.GetProjection()
    rows, cols = raw_data.shape
    del raw_ds

    if fill_mask.shape != (rows, cols):
        raise RuntimeError(
            f"Shape mismatch in _apply_fill_mask for {output.name}: "
            f"raw=({rows}, {cols}), mask={fill_mask.shape}"
        )

    masked = np.where(fill_mask, raw_data, np.float32(OUTPUT_NODATA))

    driver = gdal.GetDriverByName("GTiff")
    if driver is None:
        raise RuntimeError("GTiff driver not available — check GDAL installation")
    out_ds = driver.Create(
        str(output), cols, rows, 1, gdal.GDT_Float32,
        options=[
            "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES",
            "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
        ],
    )
    if out_ds is None:
        raise RuntimeError(
            f"gdal driver.Create failed for output: {output} — {gdal.GetLastErrorMsg()}"
        )
    out_ds.SetGeoTransform(geotransform)
    out_ds.SetProjection(projection)
    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(OUTPUT_NODATA)
    err = out_band.WriteArray(masked)
    if err != gdal.CE_None:
        raise RuntimeError(
            f"WriteArray failed for {output} — GDAL error code {err}: "
            f"{gdal.GetLastErrorMsg()}"
        )
    out_ds.FlushCache()
    del out_ds


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Build Copernicus DEM fill for Canada/Mexico border HRUs.

    CONUS-once: does not iterate ctx.vpus. NHDPlus source tiles are discovered
    by globbing ``nhd_merged_dir`` for per-VPU _fixed_ tiles.

    step_cfg keys (optional):
      raw_dir         — Copernicus tile cache. Defaults to
                        ``{data_root}/input/copernicus_dem/raw``.
      nhd_merged_dir  — NHDPlus merged-tile directory. Defaults to
                        ``{data_root}/work/nhd_merged``.

    Returns a dict with the three CONUS fill outputs registered for any
    downstream consumer that wants them.
    """
    t_start = time.time()

    raw_dir = Path(step_cfg.get(
        "raw_dir", ctx.data_root / "input" / "copernicus_dem" / "raw",
    ))
    nhd_merged_dir = Path(step_cfg.get(
        "nhd_merged_dir", ctx.data_root / "work" / "nhd_merged",
    ))
    fill_dir = nhd_merged_dir / "copernicus_fill"
    fill_dir.mkdir(parents=True, exist_ok=True)

    elev_out = fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif"
    slope_out = fill_dir / "NEDSnapshot_merged_slope_copernicus.tif"
    aspect_out = fill_dir / "NEDSnapshot_merged_aspect_copernicus.tif"

    # Early-exit if every output is already on disk. Without this the
    # orchestrator re-runs the Copernicus download on every walk, even
    # when the three output tiles are cached — slow (network-bound) and
    # fragile (the >20% shortfall guard at line ~259 trips intermittently
    # on transient 404s from the deliberately-generous border bbox).
    # Mirrors the skip-if-exists pattern every per-VPU builder uses.
    if (
        not ctx.force
        and elev_out.exists()
        and slope_out.exists()
        and aspect_out.exists()
    ):
        logger.info(
            "=== build_border_dem: all outputs exist, skipping "
            "(use --force to rebuild) ==="
        )
        logger.info("  elevation: %s", elev_out)
        logger.info("  slope    : %s", slope_out)
        logger.info("  aspect   : %s", aspect_out)
        return {
            "border_elevation": elev_out,
            "border_slope": slope_out,
            "border_aspect": aspect_out,
        }

    # --- Step 1: Compute tile list and download ---
    logger.info("=== Step 1/5: Download Copernicus GLO-30 tiles ===")
    all_labels = []
    for zone_name, (south, north, west, east) in BORDER_ZONES.items():
        labels = tiles_for_bbox(south, north, west, east)
        logger.info("  %s zone: %d tiles (%.0fN-%.0fN, %.0fW-%.0fW)",
                    zone_name, len(labels), south, north, abs(west), abs(east))
        all_labels.extend(labels)

    all_labels = sorted(set(all_labels))
    logger.info("  Total unique tiles: %d", len(all_labels))

    t1 = time.time()
    tile_paths = download_tiles(all_labels, raw_dir)
    logger.info("  Download complete in %s: %d tiles available", _elapsed(t1), len(tile_paths))

    if not tile_paths:
        raise RuntimeError(
            "No Copernicus tiles downloaded — check network access and tile labels"
        )

    n_requested = len(all_labels)
    n_downloaded = len(tile_paths)
    if n_downloaded < n_requested:
        shortfall_pct = 100 * (n_requested - n_downloaded) / n_requested
        msg = (
            f"Only {n_downloaded}/{n_requested} tiles downloaded "
            f"({shortfall_pct:.0f}% shortfall) — border DEM may have coverage gaps"
        )
        if shortfall_pct > 20:
            raise RuntimeError(msg)
        logger.warning(msg)

    # --- Step 2: Mosaic raw tiles and reproject ---
    logger.info("=== Step 2/5: Mosaic -> reproject to EPSG:5070 ===")
    raw_vrt = fill_dir / "copernicus_raw.vrt"
    vrt_ds = gdal.BuildVRT(str(raw_vrt), [str(p) for p in tile_paths])
    if vrt_ds is None:
        raise RuntimeError("gdal.BuildVRT failed for Copernicus raw tiles")
    vrt_ds.FlushCache()
    del vrt_ds
    logger.info("  Raw VRT: %s (%d sources)", raw_vrt, len(tile_paths))

    if not elev_out.exists() or ctx.force:
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
                "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES",
                "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
            ],
        )
        if warp_ds is None:
            raise RuntimeError(
                f"gdal.Warp failed: {raw_vrt} -> {elev_out} (EPSG:5070, 30m) "
                f"— {gdal.GetLastErrorMsg()}"
            )
        warp_ds.FlushCache()
        del warp_ds
        logger.info("  Warp complete in %s: %s", _elapsed(t2), elev_out)
    else:
        logger.info("  Elevation fill already exists: %s", elev_out)

    # --- Steps 3-5: Build composite, compute slope/aspect, mask ---
    nhdplus_vrt = fill_dir / "nhdplus_only.vrt"
    composite_vrt = fill_dir / "composite_elevation.vrt"
    composite_clipped = fill_dir / "composite_elevation_clipped.tif"
    slope_raw = fill_dir / "slope_raw.tif"
    aspect_raw = fill_dir / "aspect_raw.tif"
    intermediates = [slope_raw, aspect_raw, nhdplus_vrt, composite_vrt, composite_clipped]

    if not slope_out.exists() or not aspect_out.exists() or ctx.force:
        try:
            logger.info("=== Step 3/5: Build composite elevation VRT ===")
            _build_nhdplus_vrt(nhd_merged_dir, nhdplus_vrt)
            logger.info("  NHDPlus-only VRT: %s", nhdplus_vrt)
            _build_composite_vrt(elev_out, nhdplus_vrt, composite_vrt)
            logger.info("  Composite VRT: %s", composite_vrt)

            # Clip composite to Copernicus extent — the composite VRT covers the
            # union of NHDPlus + Copernicus, but we only need slope/aspect for
            # the Copernicus extent. Loading the full union into RichDEM would
            # be an unnecessary memory burden.
            cop_ds = gdal.Open(str(elev_out))
            if cop_ds is None:
                raise RuntimeError(
                    f"gdal.Open failed for Copernicus elevation: {elev_out} "
                    f"— {gdal.GetLastErrorMsg()}"
                )
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
                raise RuntimeError(
                    f"gdal.Warp failed for composite clipping: {composite_vrt} -> "
                    f"{composite_clipped} — {gdal.GetLastErrorMsg()}"
                )
            clip_ds.FlushCache()
            del clip_ds

            logger.info("=== Step 4/5: Compute slope/aspect from composite via RichDEM ===")
            logger.info("  Loading clipped composite DEM: %s", composite_clipped)
            t3 = time.time()
            dem = rd.LoadGDAL(str(composite_clipped), no_data=OUTPUT_NODATA)

            logger.info("  Computing slope (degrees)...")
            slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
            rd.SaveGDAL(str(slope_raw), slope)
            if not slope_raw.exists() or slope_raw.stat().st_size == 0:
                raise RuntimeError(f"rd.SaveGDAL produced no output for slope: {slope_raw}")
            logger.info("  Raw slope saved: %s", slope_raw)

            logger.info("  Computing aspect...")
            aspect = rd.TerrainAttribute(dem, attrib="aspect")
            rd.SaveGDAL(str(aspect_raw), aspect)
            if not aspect_raw.exists() or aspect_raw.stat().st_size == 0:
                raise RuntimeError(f"rd.SaveGDAL produced no output for aspect: {aspect_raw}")
            logger.info("  Raw aspect saved: %s", aspect_raw)
            logger.info("  Slope/aspect computation complete in %s", _elapsed(t3))

            del dem, slope, aspect

            logger.info("=== Step 5/5: Mask slope/aspect to fill zone ===")
            t4 = time.time()

            ref_ds = gdal.Open(str(slope_raw))
            if ref_ds is None:
                raise RuntimeError(
                    f"gdal.Open failed for slope raw raster: {slope_raw} "
                    f"— {gdal.GetLastErrorMsg()}"
                )
            ref_gt = ref_ds.GetGeoTransform()
            ref_rows = ref_ds.RasterYSize
            ref_cols = ref_ds.RasterXSize
            del ref_ds

            fill_mask = _compute_fill_mask(
                elev_out, nhdplus_vrt, ref_gt, ref_rows, ref_cols,
            )

            logger.info("  Masking slope to fill zone...")
            _apply_fill_mask(slope_raw, fill_mask, slope_out)
            logger.info("  Masked slope saved: %s", slope_out)

            logger.info("  Masking aspect to fill zone...")
            _apply_fill_mask(aspect_raw, fill_mask, aspect_out)
            logger.info("  Masked aspect saved: %s", aspect_out)

            logger.info("  Masking complete in %s", _elapsed(t4))

        finally:
            for f in intermediates:
                if f.exists():
                    f.unlink()
                    logger.debug("  Cleaned up intermediate: %s", f)
            logger.info("  Cleaned up intermediate files")
    else:
        logger.info("  Slope/aspect outputs already exist — skipping")

    if raw_vrt.exists():
        raw_vrt.unlink()

    logger.info("=== build_border_dem complete in %s ===", _elapsed(t_start))
    logger.info("  Outputs in: %s", fill_dir)

    return {
        "border_elevation": elev_out,
        "border_slope": slope_out,
        "border_aspect": aspect_out,
    }
