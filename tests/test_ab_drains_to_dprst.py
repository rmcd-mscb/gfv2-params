"""Unit tests for the A/B harness helpers (no WBT, no warp — pure logic)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from scripts.diagnose.ab_drains_to_dprst import (
    per_depression_counts,
    resolve_fdr_path,
)


def test_resolve_fdr_path_selects_each_source():
    fdr_vrt = Path("/data/shared/gfv2_fdr.vrt")
    per_vpu = Path("/data/shared/per_vpu")
    assert resolve_fdr_path("production", "09", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == fdr_vrt
    assert resolve_fdr_path("fill", "09", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == \
        per_vpu / "09" / "Fdr_hydrodem_09.tif"
    assert resolve_fdr_path("breach", "16", fdr_vrt=fdr_vrt, per_vpu_dir=per_vpu) == \
        per_vpu / "16" / "Fdr_breached_16.tif"


def test_resolve_fdr_path_rejects_unknown():
    with pytest.raises(ValueError):
        resolve_fdr_path("bogus", "09", fdr_vrt=Path("x"), per_vpu_dir=Path("y"))


def test_per_depression_counts_drops_background_and_counts_labels():
    labeled = np.array([[0, 7, 7], [9, 9, 9]], dtype=np.int32)
    counts = per_depression_counts(labeled)
    assert counts == {7: 2, 9: 3}
