"""Parity: Snakemake-spike Stage-4 merged CSVs vs the golden tjc outputs.

Pure pandas (no rasterio/GDAL) so it is HPC-login-node safe. Skips cleanly if
the spike run (Task 5 of the snakemake spike plan) has not produced outputs.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

ID = "model_hru_idx"  # tjc id_feature (configs/base_config.yml)
PARAMS = ["nhm_elevation_params.csv", "nhm_slope_params.csv", "nhm_ssflux_params.csv"]

_DATA_ROOT = Path(yaml.safe_load(open("configs/base_config.yml"))["data_root"])
GOLDEN = _DATA_ROOT / "tjc" / "params" / "merged"
SPIKE = _DATA_ROOT / "tjc" / "params_snakemake_spike" / "merged"


@pytest.mark.parametrize("fname", PARAMS)
def test_spike_matches_golden(fname):
    golden_path, spike_path = GOLDEN / fname, SPIKE / fname
    if not spike_path.exists():
        pytest.skip(f"spike output not present yet: {spike_path}")
    assert golden_path.exists(), f"golden reference missing: {golden_path}"

    g = pd.read_csv(golden_path).sort_values(ID).reset_index(drop=True)
    s = pd.read_csv(spike_path).sort_values(ID).reset_index(drop=True)

    # Same HRU id set, same row count.
    assert list(g[ID]) == list(s[ID]), f"{fname}: HRU id sets differ"
    assert set(g.columns) == set(s.columns), f"{fname}: column sets differ"

    # Numeric columns equal within float tolerance; non-numeric exactly equal.
    for col in g.columns:
        if pd.api.types.is_numeric_dtype(g[col]):
            assert np.allclose(g[col].to_numpy(), s[col].to_numpy(), rtol=1e-6,
                               atol=1e-9, equal_nan=True), f"{fname}:{col} differs"
        else:
            assert g[col].equals(s[col]), f"{fname}:{col} (non-numeric) differs"
