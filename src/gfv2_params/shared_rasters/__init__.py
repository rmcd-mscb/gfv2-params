"""Per-step builders for the CONUS shared-raster pipeline.

Each submodule exposes a single ``build(step_cfg, ctx, logger) -> dict[str, Path]``
function that produces named outputs in the shared raster store under
``{data_root}/work/``. The orchestrator at ``scripts/build_shared_rasters.py``
walks STEP_ORDER, calling each builder in dependency order; outputs are
recorded in ``ctx.paths`` so downstream steps can reference them by short name.

Mirrors the ``depstor_builders`` pattern for fabric-independent CONUS rasters:
per-VPU NHDPlus prep, border DEM fill, per-VPU landmask, CONUS VRT assembly,
and CONUS-scale derived rasters. Unlike the depstor pipeline, there is no
fabric concept — these rasters are reused across every fabric.

Steps are migrated to library mode incrementally; STEP_ORDER and BUILDERS
will fill in as each cluster lands on this branch.
"""

from __future__ import annotations

from .context import SharedRastersContext

BUILDERS: dict = {}

STEP_ORDER: list[str] = []

# Roadmap. Each name will move from PLANNED_STEPS into STEP_ORDER + BUILDERS
# as its cluster lands:
#   cluster 2b: merge_rpu_by_vpu, compute_slope_aspect, compute_dem_derivatives
#   cluster 2c: build_border_dem, build_vpu_landmask, build_vrt
#   cluster 2d: build_derived_rasters, build_lulc_rasters
PLANNED_STEPS = [
    "merge_rpu_by_vpu",
    "compute_slope_aspect",
    "compute_dem_derivatives",
    "build_border_dem",
    "build_vpu_landmask",
    "build_vrt",
    "build_derived_rasters",
    "build_lulc_rasters",
]

__all__ = ["BUILDERS", "STEP_ORDER", "PLANNED_STEPS", "SharedRastersContext"]
