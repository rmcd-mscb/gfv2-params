"""Per-step builders for the CONUS shared-raster pipeline.

Each submodule exposes a single ``build(step_cfg, ctx, logger) -> dict[str, Path]``
function that produces named outputs in the shared raster store under
``{data_root}/work/``. The orchestrator at ``scripts/build_shared_rasters.py``
walks STEP_ORDER, calling each builder in dependency order; outputs are
recorded in ``ctx.paths`` so downstream steps can reference them by short name.

Mirrors the ``depstor_builders`` pattern for fabric-independent CONUS rasters:
per-VPU NHDPlus prep, border DEM fill, per-VPU landmask, CONUS VRT assembly,
and CONUS-scale derived rasters. Unlike the depstor pipeline, there is no
fabric concept — these rasters are reused across every fabric. Per-VPU steps
iterate ``ctx.vpus`` internally rather than being launched once per VPU.

Steps are migrated to library mode incrementally; STEP_ORDER and BUILDERS
fill in as each cluster lands on this branch.
"""

from __future__ import annotations

from . import compute_slope_aspect, merge_rpu_by_vpu
from .context import SharedRastersContext

# The DAG. The two `merge_rpu_by_vpu*` invocations share a single builder —
# `merge_rpu_by_vpu_twi` runs after `build_vpu_landmask` because the TWI
# dataset is masked against the per-VPU HRU land mask (issue #70).
BUILDERS: dict = {
    "merge_rpu_by_vpu":     merge_rpu_by_vpu.build,
    "compute_slope_aspect": compute_slope_aspect.build,
    "merge_rpu_by_vpu_twi": merge_rpu_by_vpu.build,  # post-landmask invocation
}

STEP_ORDER: list[str] = [
    "merge_rpu_by_vpu",
    "compute_slope_aspect",
    "merge_rpu_by_vpu_twi",
]

# Roadmap. Names move from PLANNED_STEPS into STEP_ORDER + BUILDERS as each
# cluster lands. Standard production flow:
#   2b (DONE): merge_rpu_by_vpu, compute_slope_aspect, merge_rpu_by_vpu_twi
#   2d:       build_border_dem, build_vpu_landmask, build_vrt
#   2e:       build_derived_rasters, build_lulc_rasters
#
# Optional / parallel pipeline (NOT in the canonical production flow):
#   2c: compute_dem_derivatives — open-source alternative to ArcPy-derived
#       TWI. See module docstring for the calibration-threshold caveat that
#       keeps it out of the canonical PRMS parameter pipeline.
PLANNED_STEPS = [
    "merge_rpu_by_vpu",
    "compute_slope_aspect",
    "build_border_dem",
    "build_vpu_landmask",
    "merge_rpu_by_vpu_twi",
    "build_vrt",
    "build_derived_rasters",
    "build_lulc_rasters",
]

__all__ = ["BUILDERS", "STEP_ORDER", "PLANNED_STEPS", "SharedRastersContext"]
