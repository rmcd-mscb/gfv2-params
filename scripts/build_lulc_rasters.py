"""Pre-compute LULC derived rasters (radiation transmission).

Resamples canopy and keep rasters to match the LULC grid, then
computes the radiation transmission coefficient raster:
    radtrn = (cnpy * keep / 100) where lulc >= tree_threshold, else 0.
"""

import argparse
import time
from pathlib import Path

import rasterio

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import compute_radtrn, resample


def _is_valid_raster(path: Path) -> bool:
    """Return True if path exists and rasterio can open it."""
    if not path.exists():
        return False
    try:
        with rasterio.open(path):
            return True
    except Exception:
        return False


def _raster_info(path: Path) -> str:
    """Return a one-line summary of a raster: shape, CRS, pixel size, file size."""
    with rasterio.open(path) as src:
        h, w = src.height, src.width
        crs = src.crs.to_epsg() if src.crs else "unknown CRS"
        res_x, res_y = abs(src.transform.a), abs(src.transform.e)
    size_mb = path.stat().st_size / 1024 ** 2
    return (
        f"{h:,} rows x {w:,} cols | {res_x:.1f} m pixels | "
        f"EPSG:{crs} | {size_mb:.0f} MB"
    )


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def main():
    parser = argparse.ArgumentParser(description="Build LULC derived rasters.")
    parser.add_argument("--config", required=True, help="Path to LULC step config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    logger = configure_logging("build_lulc_rasters")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )

    lulc_source = config.get("lulc_source", "lulc")
    logger.info("=== build_lulc_rasters  source=%s ===", lulc_source)
    logger.info("Config : %s", args.config)

    lulc_raster = Path(config["source_raster"])
    cnpy_raster = Path(config["canopy_raster"])

    # radtrn_raster and keep_raster are optional (NLCD/NALCMS may not have them)
    radtrn_raster_str = config.get("radtrn_raster")
    radtrn_raster = Path(radtrn_raster_str) if radtrn_raster_str else None
    keep_raster_str = config.get("keep_raster")
    keep_raster = Path(keep_raster_str) if keep_raster_str else None

    if not lulc_raster.exists():
        raise FileNotFoundError(f"LULC raster not found: {lulc_raster}")
    if not cnpy_raster.exists():
        raise FileNotFoundError(f"Canopy raster not found: {cnpy_raster}")

    logger.info("LULC raster  : %s", lulc_raster)
    logger.info("             : %s", _raster_info(lulc_raster))
    logger.info("Canopy raster: %s", cnpy_raster)
    logger.info("             : %s", _raster_info(cnpy_raster))
    if keep_raster:
        logger.info("Keep raster  : %s", keep_raster)
        if keep_raster.exists():
            logger.info("             : %s", _raster_info(keep_raster))

    data_root = Path(config["data_root"])
    derived_dir = radtrn_raster.parent if radtrn_raster else data_root / "work" / "derived_rasters"
    derived_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Output dir   : %s", derived_dir)

    # ------------------------------------------------------------------
    # Step 1: Resample CNPY to LULC grid
    # ------------------------------------------------------------------
    cnpy_resampled = derived_dir / f"cnpy_resampled_{lulc_source}.tif"
    if not _is_valid_raster(cnpy_resampled) or args.force:
        intermediate = derived_dir / "cnpy_resample_intermediate.tif"
        logger.info("--- Step 1/3: Resample canopy raster to LULC grid ---")
        logger.info("  Input    : %s", cnpy_raster)
        logger.info("  Template : %s", lulc_raster)
        logger.info("  Intermediate: %s", intermediate)
        logger.info("  Output   : %s", cnpy_resampled)
        logger.info("  (This step creates a CONUS-scale GeoTIFF and may take 30-60 min)")
        t1 = time.time()
        # mask_values=(128,) — do NOT mask 0; value 0 = no canopy, a valid measurement
        resample(str(cnpy_raster), str(lulc_raster), str(intermediate), str(cnpy_resampled),
                 mask_values=(128,))
        logger.info("  Done in %s — written: %s", _elapsed(t1), cnpy_resampled)
        logger.info("  Result: %s", _raster_info(cnpy_resampled))
    else:
        logger.info("--- Step 1/3: Canopy resample already exists — skipping ---")
        logger.info("  %s", cnpy_resampled)
        logger.info("  %s", _raster_info(cnpy_resampled))

    # ------------------------------------------------------------------
    # Step 2: Resample keep to LULC grid
    # ------------------------------------------------------------------
    if keep_raster is not None:
        if not keep_raster.exists():
            raise FileNotFoundError(f"Keep raster not found: {keep_raster}")

        keep_resampled = derived_dir / f"keep_resampled_{lulc_source}.tif"
        if not _is_valid_raster(keep_resampled) or args.force:
            intermediate = derived_dir / "keep_resample_intermediate.tif"
            logger.info("--- Step 2/3: Resample keep raster to LULC grid ---")
            logger.info("  Input    : %s", keep_raster)
            logger.info("  Template : %s", lulc_raster)
            logger.info("  Intermediate: %s", intermediate)
            logger.info("  Output   : %s", keep_resampled)
            logger.info("  (This step creates a CONUS-scale GeoTIFF and may take 30-60 min)")
            t2 = time.time()
            # mask_values=(128,) — do NOT mask 0; value 0 = fully deciduous, a valid measurement
            resample(str(keep_raster), str(lulc_raster), str(intermediate), str(keep_resampled),
                     mask_values=(128,))
            logger.info("  Done in %s — written: %s", _elapsed(t2), keep_resampled)
            logger.info("  Result: %s", _raster_info(keep_resampled))
        else:
            logger.info("--- Step 2/3: Keep resample already exists — skipping ---")
            logger.info("  %s", keep_resampled)
            logger.info("  %s", _raster_info(keep_resampled))

        # ------------------------------------------------------------------
        # Step 3: Compute radiation transmission
        # ------------------------------------------------------------------
        if radtrn_raster is None:
            logger.warning(
                "keep_raster is configured but radtrn_raster path is missing "
                "from config; skipping radtrn"
            )
        elif not _is_valid_raster(radtrn_raster) or args.force:
            logger.info("--- Step 3/3: Compute radiation transmission raster ---")
            logger.info("  LULC   : %s", lulc_raster)
            logger.info("  CNPY   : %s", cnpy_resampled)
            logger.info("  keep   : %s", keep_resampled)
            logger.info("  Output : %s", radtrn_raster)
            logger.info("  (Block-wise processing — CONUS scale, ~5-20 min)")
            t3 = time.time()
            compute_radtrn(
                str(lulc_raster),
                str(cnpy_resampled),
                str(keep_resampled),
                str(radtrn_raster),
            )
            logger.info("  Done in %s — written: %s", _elapsed(t3), radtrn_raster)
            logger.info("  Result: %s", _raster_info(radtrn_raster))
        else:
            logger.info("--- Step 3/3: Radiation transmission raster already exists — skipping ---")
            logger.info("  %s", radtrn_raster)
            logger.info("  %s", _raster_info(radtrn_raster))
    else:
        logger.info("--- Steps 2-3: No keep raster configured — skipping ---")

    logger.info("=== build_lulc_rasters complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
