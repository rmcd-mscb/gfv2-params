"""Guard 4: `params/prms/` must be exactly the planned split of the FILLED `merged/` product.

DATA-ROOT-GATED, exactly like Guards 2 and 3 (tests/test_params_index_ondisk.py,
tests/test_merged_products_ondisk.py). CI has no data root, so every case SKIPS there
and CI reports green while proving nothing -- record this test's result by SLURM job id,
never infer it from a CI badge:

    FABRIC=<fabric> srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 \\
      --mem=16G pixi run -e dev --as-is pytest tests/test_prms_export_ondisk.py -q -rs

A fabric whose `params/prms/` does not exist yet SKIPS (the export has not been run).
Once it exists, each case asserts the file is the right shape AND carries the same
values as the merged column it came from -- the check that catches an export taken
before a re-fill, which leaves a complete-looking directory holding the stale half.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from gfv2_params.config import load_base_config
from gfv2_params.prms_export import (
    DIM_NDEPLVAL,
    PRMS_SUBDIR,
    SNAREA_CURVE_NAME,
    SNAREA_CURVE_POINTS,
    plan_exports,
)

_PLAN = plan_exports()


def _dirs() -> tuple[Path, Path, dict]:
    base = load_base_config(None, fabric=None)
    params = Path(base["data_root"]) / base["fabric"] / "params"
    return params / "merged", params / PRMS_SUBDIR, base


def _prms_dir_or_skip() -> tuple[Path, Path, dict]:
    merged, prms, base = _dirs()
    if not prms.exists():
        pytest.skip(f"export_prms has not been run for this fabric: {prms}")
    return merged, prms, base


@pytest.mark.parametrize("item", _PLAN, ids=lambda i: i.prms_name)
def test_exported_file_matches_its_filled_source(item):
    merged, prms, base = _prms_dir_or_skip()
    path = prms / f"{item.prms_name}.csv"
    assert path.exists(), f"{path} missing -- the export did not write {item.prms_name}"
    out = pd.read_csv(path, float_precision="round_trip")

    if item.dimension == DIM_NDEPLVAL:
        assert list(out.columns) == ["ndeplval_index", SNAREA_CURVE_NAME]
        assert len(out) % SNAREA_CURVE_POINTS == 0 and len(out) > 0
        assert out[SNAREA_CURVE_NAME].notna().all()
        return

    id_feature = base["id_feature"]
    assert list(out.columns) == [id_feature, item.prms_name]
    assert len(out) == base["expected_max_hru_id"]
    assert out[item.prms_name].notna().all(), f"{path.name} has NaN"

    src = pd.read_csv(merged / item.merged_file, low_memory=False, float_precision="round_trip")
    col = next(c for c in item.source_cols if c in src.columns)
    src = src.sort_values(id_feature).reset_index(drop=True)
    assert np.array_equal(out[id_feature].to_numpy(), src[id_feature].to_numpy())
    np.testing.assert_array_equal(
        out[item.prms_name].to_numpy(float), src[col].to_numpy(float),
        err_msg=f"{path.name} differs from merged/{item.merged_file}:{col} -- re-run export_prms after the fill",
    )


def test_hru_deplcrv_indexes_into_the_exported_curve():
    _, prms, _ = _prms_dir_or_skip()
    ndepl = len(pd.read_csv(prms / f"{SNAREA_CURVE_NAME}.csv")) // SNAREA_CURVE_POINTS
    idx = pd.read_csv(prms / "hru_deplcrv.csv")["hru_deplcrv"]
    assert 1 <= idx.min() and idx.max() <= ndepl


def test_prms_dir_holds_nothing_outside_the_plan():
    _, prms, _ = _prms_dir_or_skip()
    stray = sorted(p.name for p in prms.iterdir() if p.stem not in {i.prms_name for i in _PLAN})
    assert not stray, f"{stray} in {prms} are not in the export plan (stale from an older plan?)"
