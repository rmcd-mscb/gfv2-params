"""Per-step builders for the CONUS shared-raster pipeline.

Each submodule exposes a single ``build(step_cfg, ctx, logger) -> dict[str, Path]``
function that produces named outputs in the shared raster store under
``{data_root}/shared/``. The orchestrator at ``scripts/build_shared_rasters.py``
walks STEP_ORDER, calling each builder in dependency order; outputs are
recorded in ``ctx.paths`` so downstream steps can reference them by short name.

Mirrors the ``depstor_builders`` pattern for fabric-independent CONUS rasters:
per-VPU NHDPlus prep, border DEM fill, per-VPU landmask, CONUS VRT assembly,
and CONUS-scale derived rasters. Unlike the depstor pipeline, there is no
fabric concept — these rasters are reused across every fabric. Per-VPU steps
iterate ``ctx.vpus`` internally rather than being launched once per VPU.
"""

from __future__ import annotations

from . import (
    build_border_dem,
    build_derived_rasters,
    build_lulc_rasters,
    build_vpu_landmask,
    build_vrt,
    compute_breached_fdr,
    compute_dem_derivatives,
    compute_slope_aspect,
    merge_rpu_by_vpu,
    twi_reference,
)
from .context import SharedRastersContext

# The DAG. The two `merge_rpu_by_vpu*` invocations share a single builder —
# `merge_rpu_by_vpu_twi` runs after `build_vpu_landmask` because the TWI
# dataset is masked against the per-VPU HRU land mask (issue #70).
# `compute_dem_derivatives` is registered but NOT in the default steps: list
# of configs/shared_rasters/shared_rasters.yml — it produces a parallel/optional artifact
# (Twi_hydrodem_*.tif) not consumed by the canonical PRMS pipeline. See its
# module docstring for the calibration-threshold caveat.
BUILDERS: dict = {
    "merge_rpu_by_vpu":        merge_rpu_by_vpu.build,
    "compute_slope_aspect":    compute_slope_aspect.build,
    "build_border_dem":        build_border_dem.build,
    "build_vpu_landmask":      build_vpu_landmask.build,
    "compute_dem_derivatives": compute_dem_derivatives.build,
    "compute_breached_fdr":    compute_breached_fdr.build,
    "merge_rpu_by_vpu_twi":    merge_rpu_by_vpu.build,  # post-landmask invocation
    "build_vrt":               build_vrt.build,
    "twi_reference":           twi_reference.build,
    "build_derived_rasters":   build_derived_rasters.build,
    "build_lulc_rasters":      build_lulc_rasters.build,
}

# What each step produces (consumable downstream via `ctx.require(<key>)` for
# CONUS-scale outputs, or by re-templating off conventional per-VPU patterns).
# Each builder's `build()` returns a dict that the orchestrator merges into
# ctx.paths after the step runs (see scripts/build_shared_rasters.py).
# Per-VPU steps return {} on purpose — their outputs are discovered by
# globbing ctx.per_vpu_dir, not by key lookup.
#
#   step                     -> registered key(s)              on-disk artifact
#   merge_rpu_by_vpu         -> (none; per-VPU)                shared/per_vpu/{vpu}/NEDSnapshot_merged_*.tif, Fdr_merged_*.tif, Twi_merged_*.tif
#   compute_slope_aspect     -> (none; per-VPU)                shared/per_vpu/{vpu}/NEDSnapshot_merged_{fixed,slope,aspect}_*.tif
#   build_border_dem         -> "border_elevation",            shared/conus/borders/border_elevation.tif
#                               "border_slope",                shared/conus/borders/border_slope.tif
#                               "border_aspect"                shared/conus/borders/border_aspect.tif
#   build_vpu_landmask       -> (none; per-VPU)                shared/per_vpu/{vpu}/land_mask_{vpu}.tif
#   compute_dem_derivatives  -> (none; per-VPU, optional)      shared/per_vpu/{vpu}/Twi_hydrodem_*.tif (open-source TWI)
#   compute_breached_fdr     -> (none; per-VPU, optional)      shared/per_vpu/{vpu}/Fdr_breached_*.tif (depression-respecting FDR, #147)
#   merge_rpu_by_vpu_twi     -> (none; per-VPU)                shared/per_vpu/{vpu}/Twi_*_{vpu}.tif (post-landmask invocation)
#   build_vrt                -> "elevation_vrt", "slope_vrt",  shared/conus/vrt/{elevation,slope,aspect,fdr,twi,twi_hydrodem}.vrt
#                               "aspect_vrt", "fdr_vrt",
#                               "twi_vrt", "twi_hydrodem_vrt"
#   twi_reference            -> "twi_reference_arcpy",         shared/conus/twi_reference_percentiles.arcpy.csv
#                               "twi_reference_hydrodem"       shared/conus/twi_reference_percentiles.hydrodem.csv
#   build_derived_rasters    -> "soil_moist_max"               shared/conus/derived/soil_moist_max.tif
#   build_lulc_rasters       -> "cnpy_resampled_<source>",     shared/conus/derived/cnpy_resampled_<source>.tif,
#                               "keep_resampled_<source>",     keep_resampled_<source>.tif,
#                               "radtrn_<source>"              radtrn_<source>.tif (one set per LULC source)
STEP_ORDER: list[str] = [
    "merge_rpu_by_vpu",
    "compute_slope_aspect",
    "build_border_dem",
    "build_vpu_landmask",
    "compute_dem_derivatives",  # optional / parallel
    "compute_breached_fdr",     # optional / parallel (#147)
    "merge_rpu_by_vpu_twi",
    "build_vrt",
    "twi_reference",
    "build_derived_rasters",
    "build_lulc_rasters",
]

# Migration complete: every step from PLANNED_STEPS is now registered in
# STEP_ORDER + BUILDERS. The list is preserved as a documentation
# reference so the production pipeline shape stays visible in one place.
PLANNED_STEPS = list(STEP_ORDER)

__all__ = ["BUILDERS", "STEP_ORDER", "PLANNED_STEPS", "SharedRastersContext"]
