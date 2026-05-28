"""Per-step depression-storage raster builders.

Each submodule exposes a single `build(step_cfg, ctx, logger) -> dict[str, Path]`
function that produces one named output (or two, for waterbody / dprst /
carea_map). The orchestrator at `scripts/build_depstor_rasters.py` walks the
ordered step list in `configs/depstor/depstor_rasters.yml`, calls these in dependency
order, and collects the returned output paths so downstream steps can find
their inputs without each builder having to repeat the
`{data_root}/{fabric}/depstor_rasters/*` path templating.

Pure compute logic continues to live in [src/gfv2_params/depstor.py](depstor.py).
These modules are thin orchestration wrappers around those helpers.
"""

from __future__ import annotations

from . import carea_map, dprst, imperv, intersect, landmask, perv, routing, streambuffer, vpu_id, waterbody
from .context import BuildContext

BUILDERS = {
    "landmask":      landmask.build,
    "imperv":        imperv.build,
    "streambuffer":  streambuffer.build,
    "waterbody":     waterbody.build,
    "dprst":         dprst.build,
    "perv":          perv.build,
    "routing":       routing.build,
    "drains_perv":   intersect.build,
    "drains_imperv": intersect.build,
    "vpu_id":        vpu_id.build,
    "carea_map":     carea_map.build,
}

# Outputs registered into ctx.paths by each builder (consumable downstream via
# `ctx.require(<key>)`). Each builder's `build()` returns a dict that the
# orchestrator merges into ctx.paths after the step runs (see
# scripts/build_depstor_rasters.py). Keys are stable string handles — change
# them only with a coordinated update of every downstream consumer.
#
#   step              -> registered key(s)        on-disk artifact (default name)
#   landmask          -> "landmask"               land_mask.tif (uint8, 1=land)
#   imperv            -> "imperv"                 imperv_binary.tif
#   streambuffer      -> "stream_buffer"          stream_buffer.tif
#   waterbody         -> "wbody_binary",          wbody_binary.tif,
#                        "wbody_regions"          wbody_regions.tif
#   dprst             -> "dprst",                 dprst_binary.tif,
#                        "onstream"               onstream_binary.tif
#   perv              -> "perv"                   perv_binary.tif
#   routing           -> "drains_to_dprst"        drains_to_dprst.tif (WBT watershed)
#   drains_perv       -> "drains_perv"            drains_perv_binary.tif (output_key from config)
#   drains_imperv     -> "drains_imperv"          drains_imperv_binary.tif (output_key from config)
#   vpu_id            -> "vpu_id"                 vpu_id.tif (per-cell VPU code, multi-VPU only)
#   carea_map         -> "carea_max",             carea_max_*.tif,
#                        "smidx"                  smidx_*.tif
STEP_ORDER = [
    "landmask",
    "imperv",
    "streambuffer",
    "waterbody",
    "dprst",
    "perv",
    "vpu_id",
    "routing",
    "drains_perv",
    "drains_imperv",
    "carea_map",
]

__all__ = ["BUILDERS", "STEP_ORDER", "BuildContext"]
