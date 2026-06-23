"""Stage NHDPlusV2 NHDFlowline attributes and distill connected-waterbody COMIDs.

NHD encodes waterbody connectivity directly: an artificial-path NHDFlowline that
runs through a waterbody carries WBAREACOMI = that waterbody's COMID. The distinct
set of populated WBAREACOMI values is the set of on-stream (connected) waterbodies.
This module downloads the per-VPU NHDSnapshot archives, reads NHDFlowline, and
writes a flat parquet of connected COMIDs consumed by the depstor
`wbody_connectivity` builder.
"""

from __future__ import annotations

import pandas as pd

# WBAREACOMI == 0 (and null) means the flowline does not pass through a waterbody.
_NO_WATERBODY = 0


def connected_comids_from_flowlines(df: pd.DataFrame) -> set[int]:
    """Distinct non-zero, non-null WBAREACOMI values as a set of ints."""
    col = pd.to_numeric(df["WBAREACOMI"], errors="coerce")
    vals = col[(col.notna()) & (col != _NO_WATERBODY)]
    return {int(v) for v in vals.unique()}
