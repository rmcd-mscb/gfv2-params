"""Copernicus GLO-30 elevation fill for border HRUs (Canada/Mexico).

Library entrypoint for the ``build_border_dem`` step in the shared-raster
orchestrator (``scripts/build_shared_rasters.py``). Registered via the
BUILDERS dict in ``shared_rasters/__init__.py`` and called by the
orchestrator's STEP_ORDER walk.

Downloads Copernicus 30m tiles covering border zones, mosaics them,
reprojects to EPSG:5070 at 30m, then builds a composite elevation surface
by overlaying NHDPlus VPU tiles on top of Copernicus (NHDPlus takes priority
in the overlap zone via GDAL VRT last-source-wins ordering). Slope and aspect
are computed via RichDEM on this composite, then masked to retain only pixels
in the fill zone (where Copernicus has data but NHDPlus does not).

Output tiles are placed in ``ctx.borders_dir`` (``shared/conus/borders/`` by
default) where the build_vrt step lists them before NHDPlus tiles in the VRT
(GDAL last-source-wins means NHDPlus takes priority).

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

from .cog import cog_temp, to_cog
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


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _build_nhdplus_vrt(per_vpu_dir: Path, output_vrt: Path) -> Path:
    """Build a VRT from NHDPlus _fixed_ tiles only (no fill layers).

    In the new layout the per-VPU directory contains only per-VPU subdirs;
    Copernicus fill lives in a peer ``borders/`` directory under conus/.
    """
    primary_files = sorted(per_vpu_dir.glob(f"*/{NHDPLUS_FIXED_PATTERN}"))
    if not primary_files:
        raise FileNotFoundError(
            f"No NHDPlus _fixed_ tiles found in {per_vpu_dir}. "
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


# Windowed-strip height for the fill-mask computation and mask application.
# The fill zone spans the full continental Copernicus extent at 30 m. The
# previous approach loaded it whole — two gdal.Warp(format="MEM") full-extent
# float32 readbacks plus their .astype copies (~4 full-extent arrays,
# ~560 GB combined) — which exceeded the 503 GB node and OOM'd (numpy
# _ArrayMemoryError). Both _write_fill_mask and _apply_fill_mask now process
# STRIP_ROWS rows at a time, holding only strip-sized arrays. Mirrors
# carea_map's windowed-strip pattern (see the CONUS-scale memory note in
# docs/ARCHITECTURE.md). Larger STRIP_ROWS = fewer warp calls (less I/O
# overhead) but proportionally more resident memory per strip.
STRIP_ROWS = 4096


def _write_fill_mask(
    copernicus_elev: Path,
    nhdplus_vrt: Path,
    ref_raster: Path,
    mask_out: Path,
) -> None:
    """Stream a UInt8 fill-zone mask onto ``ref_raster``'s grid.

    Fill zone = 1 where Copernicus has valid data AND NHDPlus is nodata, else 0.
    Warps Copernicus and NHDPlus into each horizontal strip's window in memory
    (strip-sized, never full-extent) and writes the mask strip-by-strip.
    """
    ref_ds = gdal.Open(str(ref_raster))
    if ref_ds is None:
        raise RuntimeError(
            f"gdal.Open failed for ref raster: {ref_raster} — {gdal.GetLastErrorMsg()}"
        )
    gt = ref_ds.GetGeoTransform()
    proj = ref_ds.GetProjection()
    cols = ref_ds.RasterXSize
    rows = ref_ds.RasterYSize
    del ref_ds

    driver = gdal.GetDriverByName("GTiff")
    if driver is None:
        raise RuntimeError("GTiff driver not available — check GDAL installation")
    mask_ds = driver.Create(
        str(mask_out), cols, rows, 1, gdal.GDT_Byte,
        options=["COMPRESS=LZW", "TILED=YES", "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES"],
    )
    if mask_ds is None:
        raise RuntimeError(
            f"gdal driver.Create failed for mask: {mask_out} — {gdal.GetLastErrorMsg()}"
        )
    mask_ds.SetGeoTransform(gt)
    mask_ds.SetProjection(proj)
    mask_band = mask_ds.GetRasterBand(1)

    for r0 in range(0, rows, STRIP_ROWS):
        strip_h = min(STRIP_ROWS, rows - r0)
        # Strip bounds in the ref CRS (gt[5] is negative for a north-up grid).
        strip_bounds = [
            gt[0],
            gt[3] + (r0 + strip_h) * gt[5],
            gt[0] + cols * gt[1],
            gt[3] + r0 * gt[5],
        ]
        warp_kwargs = dict(
            format="MEM",
            outputBounds=strip_bounds,
            width=cols,
            height=strip_h,
            dstNodata=OUTPUT_NODATA,
            srcNodata=OUTPUT_NODATA,
        )
        cop_ds = gdal.Warp("", str(copernicus_elev), **warp_kwargs)
        if cop_ds is None:
            raise RuntimeError(
                f"gdal.Warp to MEM failed for Copernicus strip (row {r0}): "
                f"{copernicus_elev} — {gdal.GetLastErrorMsg()}"
            )
        cop = cop_ds.GetRasterBand(1).ReadAsArray()
        del cop_ds
        if cop is None:
            raise RuntimeError(
                f"ReadAsArray returned None for Copernicus strip (row {r0}): "
                f"{copernicus_elev} — {gdal.GetLastErrorMsg()}"
            )
        nhd_ds = gdal.Warp("", str(nhdplus_vrt), **warp_kwargs)
        if nhd_ds is None:
            raise RuntimeError(
                f"gdal.Warp to MEM failed for NHDPlus strip (row {r0}): "
                f"{nhdplus_vrt} — {gdal.GetLastErrorMsg()}"
            )
        nhd = nhd_ds.GetRasterBand(1).ReadAsArray()
        del nhd_ds
        if nhd is None:
            raise RuntimeError(
                f"ReadAsArray returned None for NHDPlus strip (row {r0}): "
                f"{nhdplus_vrt} — {gdal.GetLastErrorMsg()}"
            )

        # -9999 is exactly representable in float32, so equality is safe.
        strip_mask = ((cop != OUTPUT_NODATA) & (nhd == OUTPUT_NODATA)).astype(np.uint8)
        err = mask_band.WriteArray(strip_mask, 0, r0)
        if err != gdal.CE_None:
            raise RuntimeError(
                f"WriteArray failed for mask strip (row {r0}) — code {err}: "
                f"{gdal.GetLastErrorMsg()}"
            )

    mask_ds.FlushCache()
    del mask_ds


def _apply_fill_mask(
    raw_raster: Path,
    mask_raster: Path,
    output: Path,
    overview_resampling: str,
) -> None:
    """Stream-apply ``mask_raster`` to ``raw_raster``, writing ``output`` as a COG.

    Keeps raw values where the mask is 1, writes nodata elsewhere, one
    STRIP_ROWS-tall window at a time (never the full-extent array). The plain
    windowed write goes to a temp, then to_cog reorganizes it into a COG (tiled
    512 + overviews + ZSTD/pred3) — border slope/aspect fill feed the same VRTs
    as the per-VPU tiles and are GDAL/QGIS-consumed (never WBT). Aspect passes
    overview_resampling="NEAREST" (circular 0/360 field).
    """
    raw_ds = gdal.Open(str(raw_raster))
    if raw_ds is None:
        raise RuntimeError(
            f"gdal.Open failed for raw raster: {raw_raster} — {gdal.GetLastErrorMsg()}"
        )
    mask_ds = gdal.Open(str(mask_raster))
    if mask_ds is None:
        raise RuntimeError(
            f"gdal.Open failed for mask raster: {mask_raster} — {gdal.GetLastErrorMsg()}"
        )
    gt = raw_ds.GetGeoTransform()
    proj = raw_ds.GetProjection()
    cols = raw_ds.RasterXSize
    rows = raw_ds.RasterYSize
    if (mask_ds.RasterXSize, mask_ds.RasterYSize) != (cols, rows):
        raise RuntimeError(
            f"Shape mismatch in _apply_fill_mask for {output.name}: "
            f"raw=({rows}, {cols}), mask=({mask_ds.RasterYSize}, {mask_ds.RasterXSize})"
        )
    raw_band = raw_ds.GetRasterBand(1)
    mask_band = mask_ds.GetRasterBand(1)

    driver = gdal.GetDriverByName("GTiff")
    if driver is None:
        raise RuntimeError("GTiff driver not available — check GDAL installation")
    with cog_temp(output) as tmp:
        out_ds = driver.Create(
            str(tmp), cols, rows, 1, gdal.GDT_Float32,
            options=["COMPRESS=LZW", "TILED=YES", "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES"],
        )
        if out_ds is None:
            raise RuntimeError(
                f"gdal driver.Create failed for output: {tmp} — {gdal.GetLastErrorMsg()}"
            )
        out_ds.SetGeoTransform(gt)
        out_ds.SetProjection(proj)
        out_band = out_ds.GetRasterBand(1)
        out_band.SetNoDataValue(OUTPUT_NODATA)

        for r0 in range(0, rows, STRIP_ROWS):
            strip_h = min(STRIP_ROWS, rows - r0)
            raw = raw_band.ReadAsArray(0, r0, cols, strip_h)
            if raw is None:
                raise RuntimeError(
                    f"ReadAsArray returned None for raw strip (row {r0}): "
                    f"{raw_raster} — {gdal.GetLastErrorMsg()}"
                )
            mask = mask_band.ReadAsArray(0, r0, cols, strip_h)
            if mask is None:
                raise RuntimeError(
                    f"ReadAsArray returned None for mask strip (row {r0}): "
                    f"{mask_raster} — {gdal.GetLastErrorMsg()}"
                )
            masked = np.where(
                mask.astype(bool), raw.astype(np.float32), np.float32(OUTPUT_NODATA)
            )
            err = out_band.WriteArray(masked, 0, r0)
            if err != gdal.CE_None:
                raise RuntimeError(
                    f"WriteArray failed for {tmp} strip (row {r0}) — code {err}: "
                    f"{gdal.GetLastErrorMsg()}"
                )

        out_ds.FlushCache()
        del out_ds
        del raw_ds, mask_ds
        to_cog(tmp, output, overview_resampling=overview_resampling, predictor=3)


# Baseline fraction of requested border tiles that 404 deterministically. The
# BORDER_ZONES bbox is deliberately generous, so a large share cover open ocean
# / no land and 404 every run (1238/1557 downloaded = 20.5% shortfall for the
# current Canada+Mexico zones). Abort only *above* this, where a count-based
# shortfall implies a real coverage failure rather than the expected ocean
# baseline. This threshold cannot see non-404 download errors — those are
# treated as a hard error separately (see download_tiles' `failed` list).
OCEAN_SHORTFALL_PCT = 30


def _check_shortfall(n_requested: int, n_downloaded: int) -> str | None:
    """Classify a tile-count shortfall against the ocean baseline.

    Returns None when every requested tile is present, a warning message for a
    shortfall within the expected open-ocean 404 baseline (<= OCEAN_SHORTFALL_PCT,
    strict), and raises RuntimeError for a gross shortfall above it (a likely
    real coverage failure). Non-404 download failures are NOT visible here and
    must be gated by the caller before calling this.
    """
    if n_downloaded >= n_requested:
        return None
    shortfall_pct = 100 * (n_requested - n_downloaded) / n_requested
    msg = (
        f"Only {n_downloaded}/{n_requested} tiles downloaded "
        f"({shortfall_pct:.0f}% shortfall) — border DEM may have coverage gaps"
    )
    if shortfall_pct > OCEAN_SHORTFALL_PCT:
        raise RuntimeError(msg)
    return msg


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Build Copernicus DEM fill for Canada/Mexico border HRUs.

    CONUS-once: does not iterate ctx.vpus. NHDPlus source tiles are discovered
    by globbing ``per_vpu_dir`` for per-VPU _fixed_ tiles.

    step_cfg keys (all optional; defaults reference context properties):
      raw_dir       — Copernicus tile cache. Default
                      ``{data_root}/input/copernicus_dem/raw``.
      per_vpu_dir   — NHDPlus per-VPU merged-tile directory. Default
                      ``ctx.per_vpu_dir``.
      borders_dir   — output directory for border fill tiles. Default
                      ``ctx.borders_dir``.

    Returns a dict with the three CONUS fill outputs registered for any
    downstream consumer that wants them.
    """
    t_start = time.time()

    raw_dir = Path(step_cfg.get(
        "raw_dir", ctx.data_root / "input" / "copernicus_dem" / "raw",
    ))
    per_vpu_dir = Path(step_cfg.get("per_vpu_dir", ctx.per_vpu_dir))
    fill_dir = Path(step_cfg.get("borders_dir", ctx.borders_dir))
    fill_dir.mkdir(parents=True, exist_ok=True)

    elev_out = fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif"
    slope_out = fill_dir / "NEDSnapshot_merged_slope_copernicus.tif"
    aspect_out = fill_dir / "NEDSnapshot_merged_aspect_copernicus.tif"

    # Early-exit if every output is already on disk. Without this the
    # orchestrator re-runs the Copernicus download on every walk, even
    # when the three output tiles are cached — slow (network-bound) and
    # fragile (the shortfall guard, _check_shortfall, trips intermittently
    # on transient failures from the deliberately-generous border bbox).
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
    tile_paths, failed = download_tiles(all_labels, raw_dir)
    logger.info("  Download complete in %s: %d tiles available", _elapsed(t1), len(tile_paths))

    if not tile_paths:
        raise RuntimeError(
            "No Copernicus tiles downloaded — check network access and tile labels"
        )

    # Non-404 failures (timeout / 5xx / DNS) are real download errors, not the
    # expected open-ocean baseline. Abort regardless of the overall shortfall so
    # a partial network outage can't silently drop land tiles from the border
    # fill — the count-based ocean-baseline check below cannot distinguish them.
    if failed:
        preview = ", ".join(failed[:10]) + (" ..." if len(failed) > 10 else "")
        raise RuntimeError(
            f"{len(failed)} Copernicus tile(s) failed to download for a non-404 "
            f"reason (network/HTTP error, not open-ocean): {preview} — this is a "
            f"real download failure, not the expected ocean baseline; rerun once "
            f"resolved"
        )

    warning = _check_shortfall(len(all_labels), len(tile_paths))
    if warning:
        logger.warning(warning)

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
        # Warp to a plain temp, then reorganize into a COG (tiled 512 +
        # overviews + ZSTD/pred3) — the elevation fill feeds elevation.vrt and
        # is GDAL/QGIS-consumed (never WBT), matching the per-VPU _fixed_ tiles.
        with cog_temp(elev_out) as elev_tmp:
            warp_ds = gdal.Warp(
                str(elev_tmp),
                str(raw_vrt),
                dstSRS="EPSG:5070",
                xRes=30,
                yRes=30,
                resampleAlg="bilinear",
                dstNodata=OUTPUT_NODATA,
                outputType=gdal.GDT_Float32,
                creationOptions=[
                    "COMPRESS=LZW", "TILED=YES",
                    "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
                ],
            )
            if warp_ds is None:
                raise RuntimeError(
                    f"gdal.Warp failed: {raw_vrt} -> {elev_tmp} (EPSG:5070, 30m) "
                    f"— {gdal.GetLastErrorMsg()}"
                )
            warp_ds.FlushCache()
            del warp_ds
            to_cog(elev_tmp, elev_out, overview_resampling="BILINEAR", predictor=3)
        logger.info("  Warp complete in %s: %s", _elapsed(t2), elev_out)
    else:
        logger.info("  Elevation fill already exists: %s", elev_out)

    # --- Steps 3-5: Build composite, compute slope/aspect, mask ---
    nhdplus_vrt = fill_dir / "nhdplus_only.vrt"
    composite_vrt = fill_dir / "composite_elevation.vrt"
    composite_clipped = fill_dir / "composite_elevation_clipped.tif"
    slope_raw = fill_dir / "slope_raw.tif"
    aspect_raw = fill_dir / "aspect_raw.tif"
    fill_mask_raster = fill_dir / "fill_mask.tif"
    intermediates = [
        slope_raw, aspect_raw, nhdplus_vrt, composite_vrt, composite_clipped,
        fill_mask_raster,
    ]

    if not slope_out.exists() or not aspect_out.exists() or ctx.force:
        try:
            logger.info("=== Step 3/5: Build composite elevation VRT ===")
            _build_nhdplus_vrt(per_vpu_dir, nhdplus_vrt)
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

            # Compute the fill-zone mask once (streamed to disk on slope_raw's
            # grid), then stream-apply it to both slope and aspect. Windowed
            # throughout so peak memory is strip-sized, not full-extent (the
            # old full-array _compute_fill_mask OOM'd on CONUS — see STRIP_ROWS).
            logger.info("  Computing fill-zone mask (streaming)...")
            _write_fill_mask(elev_out, nhdplus_vrt, slope_raw, fill_mask_raster)

            logger.info("  Masking slope to fill zone...")
            _apply_fill_mask(slope_raw, fill_mask_raster, slope_out, "BILINEAR")
            logger.info("  Masked slope saved: %s", slope_out)

            logger.info("  Masking aspect to fill zone...")
            _apply_fill_mask(aspect_raw, fill_mask_raster, aspect_out, "NEAREST")
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
