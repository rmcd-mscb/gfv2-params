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
    "merge_rpu_by_vpu_twi":    merge_rpu_by_vpu.build,  # post-landmask invocation
    "build_vrt":               build_vrt.build,
    "twi_reference":           twi_reference.build,
    "build_derived_rasters":   build_derived_rasters.build,
    "build_lulc_rasters":      build_lulc_rasters.build,
}

STEP_ORDER: list[str] = [
    "merge_rpu_by_vpu",
    "compute_slope_aspect",
    "build_border_dem",
    "build_vpu_landmask",
    "compute_dem_derivatives",  # optional / parallel
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
