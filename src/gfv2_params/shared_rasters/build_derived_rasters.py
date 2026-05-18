"""Pre-compute derived rasters (soil_moist_max) from source inputs.

Library entrypoint for the ``build_derived_rasters`` step in the shared-raster
orchestrator (``scripts/build_shared_rasters.py``). Registered via the
BUILDERS dict in ``shared_rasters/__init__.py`` and called by the
orchestrator's STEP_ORDER walk.

CONUS-once builder. Eliminates race conditions when multiple SLURM batch
jobs would otherwise try to create the same derived rasters simultaneously.
"""

from __future__ import annotations

from pathlib import Path

from gfv2_params.raster_ops import mult_rasters, resample

from .context import SharedRastersContext


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Build soil_moist_max from RootDepth and AWC source rasters.

    step_cfg keys (all optional, with conventional defaults):
      root_depth_raster  — ``{data_root}/input/lulc_veg/RootDepth.tif``
      awc_raster         — ``{data_root}/input/soils_litho/AWC.tif``
      output_dir         — ``ctx.derived_dir`` (``shared/conus/derived``)

    Returns ``{"soil_moist_max": path}`` for downstream consumers.
    """
    data_root = ctx.data_root
    rd_rast = Path(step_cfg.get("root_depth_raster", data_root / "input" / "lulc_veg" / "RootDepth.tif"))
    awc_rast = Path(step_cfg.get("awc_raster", data_root / "input" / "soils_litho" / "AWC.tif"))

    derived_dir = Path(step_cfg.get("output_dir", ctx.derived_dir))
    derived_dir.mkdir(parents=True, exist_ok=True)
    intermediate_rast = derived_dir / "rd_250_intermediate.tif"
    rd_resampled = derived_dir / "rd_250_raw.tif"
    soil_moist_max_rast = derived_dir / "soil_moist_max.tif"

    if not rd_rast.exists():
        raise FileNotFoundError(f"RootDepth raster not found: {rd_rast}")
    if not awc_rast.exists():
        raise FileNotFoundError(f"AWC raster not found: {awc_rast}")

    # Step 1: Resample RootDepth to match AWC grid.
    # mask_values=(0,) — 0 = cropland = no natural root depth; mask it to
    # NaN so downstream soil_moist_max doesn't multiply zeros through. The
    # int8 -128 nodata sentinel is caught separately by mask_negative=True.
    if not rd_resampled.exists() or ctx.force:
        logger.info("Resampling RootDepth to AWC grid...")
        resample(str(rd_rast), str(awc_rast), str(intermediate_rast), str(rd_resampled),
                 mask_values=(0,))
        logger.info("Written: %s", rd_resampled)
    else:
        logger.info("Resampled RootDepth already exists: %s", rd_resampled)

    # Step 2: Multiply resampled RootDepth x AWC -> soil_moist_max
    if not soil_moist_max_rast.exists() or ctx.force:
        logger.info("Computing soil_moist_max = RootDepth * AWC...")
        mult_rasters(str(rd_resampled), str(awc_rast), str(soil_moist_max_rast))
        logger.info("Written: %s", soil_moist_max_rast)
    else:
        logger.info("soil_moist_max raster already exists: %s", soil_moist_max_rast)

    logger.info("build_derived_rasters complete")
    return {"soil_moist_max": soil_moist_max_rast}
