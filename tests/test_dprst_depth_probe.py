import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "dprst_depth_probe",
    Path(__file__).resolve().parent.parent / "scripts" / "diagnose" / "dprst_depth_probe.py",
)
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)

# The topo-function unit tests formerly here (dprst_polygons, resolution_class,
# depth_to_spill, is_hydroflattened, volume_mean_depth, _normalize_nodata,
# lake_max_depth) moved to tests/test_dprst_depth_topo.py, which imports them
# directly from gfv2_params.dprst_depth.topo (issue #173 Task 1 promotion).
# This file is retained (with the importlib load above, kept working via the
# probe's re-exported `from gfv2_params.dprst_depth.topo import (...)`) for
# probe-specific CLI/analysis-mode tests (--audit/--flatness/--freeboard/
# --hollister/--regression/--hollister-validation) as they are added.
