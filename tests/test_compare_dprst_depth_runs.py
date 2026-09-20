import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

# Same loading idiom as tests/test_dprst_depth_probe.py: scripts/ is not a package.
_spec = importlib.util.spec_from_file_location(
    "compare_dprst_depth_runs",
    Path(__file__).resolve().parent.parent / "scripts" / "diagnose" / "compare_dprst_depth_runs.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
compare = _mod.compare


def test_compare_classifies_rows():
    old = pd.DataFrame({"COMID": [1, 2, 3, 4], "dprst_depth_m": [1.0, 2.0, np.nan, 4.0],
                        "method": ["measured", "measured", "calibrated_hollister", "measured"]})
    new = pd.DataFrame({"COMID": [1, 2, 3, 4], "dprst_depth_m": [1.0, 2.5, 3.0, 4.0],
                        "method": ["measured", "measured", "measured", "measured"],
                        "source": ["A", "B", "A", "A"]})
    r = compare(old, new, expect_identical={1, 2})
    assert r["identical"] == 2          # COMIDs 1 and 4
    assert r["changed"] == 1            # COMID 2: 2.0 -> 2.5
    assert r["newly_measured"] == 1     # COMID 3: filled -> measured
    assert r["violations"] == [2]       # COMID 2 was expected identical but changed
