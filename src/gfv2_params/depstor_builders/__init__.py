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

STEP_ORDER = [
    "landmask",
    "imperv",
    "streambuffer",
    "waterbody",
    "dprst",
    "perv",
    "routing",
    "drains_perv",
    "drains_imperv",
    "vpu_id",
    "carea_map",
]

__all__ = ["BUILDERS", "STEP_ORDER", "BuildContext"]
