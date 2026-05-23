"""Library functions for the Part 2 zonal-pass parameter pipeline.

Each ``run_*`` function performs the per-batch (or CONUS-once) work for one
parameter type. The unified ``scripts/derive_zonal_params.py`` orchestrator
delegates here so the work logic lives in exactly one place.

The ``config`` dict each function receives is a flat mapping containing the
keys that ``configs/zonal/zonal_params.yml`` already provides (source_type,
source_raster, batch_dir, target_layer, id_feature, output_dir, merged_file,
categorical, fabric, the fabric-profile hru_gpkg/hru_layer that
run_build_weights reads, plus per-script extras like canopy_raster,
crosswalk_file, keep_raster, source_shapefile, merged_slope_file,
weight_dir, k_perm_min, flux_params). The orchestrator builds this dict by
flattening the active param entry in ``configs/zonal/zonal_params.yml`` onto
the top-level ``defaults:`` block (plus the resolved fabric profile).

Refactor invariant: the existing per-script behaviour is preserved verbatim
— the functions in the submodules below are the prior single-file
``zonal_runners.py`` extracted unchanged (PR #75 → followup; mirrors the
``depstor_builders/`` pattern from PR #72).

Package-level concerns (heavy geo-library imports, GDAL exception toggle,
startup heartbeat) live here in ``__init__.py`` so they fire exactly once
per process at first package import.
"""

# ruff: noqa: E402  -- heartbeat print must fire before geo imports (see below); E402 is expected
from __future__ import annotations

import os
import sys
import time

# Pre-import heartbeats so a future hang in the geo-library import chain
# (rasterio/GDAL/PROJ/pyogrio init under shared-FS metadata contention,
# observed once on VPU01 issue-#61 array run with zero stdout from one task)
# can be localised. Printed unconditionally with flush=True so SLURM's
# stdout/stderr line buffering doesn't swallow them.
print(
    f"[startup pid={os.getpid()} host={os.uname().nodename} "
    f"task={os.environ.get('SLURM_ARRAY_TASK_ID', '-')}] "
    f"python {sys.version.split()[0]} interpreter up, importing geo libs...",
    flush=True,
)
_t_imports = time.time()

import geopandas as gpd  # noqa: F401  (re-imported by submodules; cached)
import numpy as np  # noqa: F401
import pandas as pd  # noqa: F401
import rioxarray  # noqa: F401
from gdptools import UserTiffData, WeightGenP2P, ZonalGen  # noqa: F401
from osgeo import gdal, osr

print(f"[startup] geo-library imports complete in {time.time() - _t_imports:.1f}s", flush=True)

# Opt into the GDAL 4.0 default of raising Python exceptions instead of C-style
# error codes. Silences the FutureWarning that osgeo emits when neither
# UseExceptions/DontUseExceptions is set. NB: GDAL state is process-global —
# importing this module from a notebook or test harness will flip exception
# handling on for the whole process. That is the desired behaviour (GDAL 4.0's
# default) and what the slurm batches expect, but worth knowing if anyone
# embeds this module elsewhere.
gdal.UseExceptions()
osr.UseExceptions()

# Public re-exports. External callers (scripts/derive_zonal_params.py,
# tests/test_merge_params.py) import these names from gfv2_params.zonal_runners
# directly.
from .lulc import run_lulc_batch
from .merge import run_merge
from .soils import run_soils_batch
from .ssflux import run_ssflux_batch
from .weights import run_build_weights
from .zonal import run_zonal_batch

__all__ = [
    "BATCH_RUNNERS",
    "run_build_weights",
    "run_lulc_batch",
    "run_merge",
    "run_soils_batch",
    "run_ssflux_batch",
    "run_zonal_batch",
]


# Dispatch table: `script:` tag in configs/zonal/zonal_params.yml -> runner.
# Used by scripts/derive_zonal_params.py to route a per-batch invocation to
# the right submodule. Adding a new `script:` tag means (a) adding a new
# submodule under zonal_runners/, (b) re-exporting its run_*_batch above,
# and (c) adding an entry below — keep them in sync.
BATCH_RUNNERS = {
    "zonal":  run_zonal_batch,
    "soils":  run_soils_batch,
    "lulc":   run_lulc_batch,
    "ssflux": run_ssflux_batch,
}
