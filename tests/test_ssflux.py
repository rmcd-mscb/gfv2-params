"""Map/reduce behaviour for ssflux (no geo fixtures -- the reducer is pure)."""

import numpy as np
import pandas as pd
import pytest

from gfv2_params.zonal_runners import MERGE_REDUCERS, run_ssflux_reduce
from gfv2_params.zonal_runners.ssflux_math import PARAM_NAMES

ID = "nat_hru_id"

FLUX_PARAMS = [
    {"name": "soil2gw_max", "min": 0.1, "max": 0.3},
    {"name": "ssr2gw_rate", "min": 0.3, "max": 0.7},
    {"name": "fastcoef_lin", "min": 0.01, "max": 0.6},
    {"name": "slowcoef_lin", "min": 0.005, "max": 0.3},
    {"name": "gwflow_coef", "min": 0.005, "max": 0.3},
    {"name": "dprst_seep_rate_open", "min": 0.005, "max": 0.2},
    {"name": "dprst_flow_coef", "min": 0.005, "max": 0.1},
]


class _Logger:
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def warning(self, *a, **k): pass


def _frame(n=50, with_vpu=False):
    rng = np.random.default_rng(0)
    df = pd.DataFrame({ID: np.arange(1, n + 1)})
    for p in PARAM_NAMES:
        df[f"L_{p}"] = rng.normal(-12.0, 2.0, n)
    if with_vpu:
        df["vpu"] = np.where(df[ID] <= n // 2, "01", "02")
    return df


def _cfg(scope="fabric"):
    return {"id_feature": ID, "flux_params": FLUX_PARAMS, "norm_scope": scope}


def test_reduce_maps_into_configured_ranges_and_drops_L_columns():
    out = run_ssflux_reduce(_frame(), _cfg(), _Logger())
    for p in FLUX_PARAMS:
        col = out[p["name"]]
        assert col.min() == pytest.approx(p["min"])
        assert col.max() == pytest.approx(p["max"])
    assert not [c for c in out.columns if c.startswith("L_")]


def test_reduce_is_registered():
    assert MERGE_REDUCERS["ssflux"] is run_ssflux_reduce


def test_batch_invariance_is_bit_exact():
    """The issue's acceptance criterion: identical output across batch splits."""
    df = _frame(40)
    whole = run_ssflux_reduce(df.copy(), _cfg(), _Logger())
    # same rows, different partition + arrival order
    shuffled = pd.concat([df.iloc[20:], df.iloc[:20]], ignore_index=True)
    split = run_ssflux_reduce(shuffled, _cfg(), _Logger()).sort_values(ID).reset_index(drop=True)
    pd.testing.assert_frame_equal(whole, split)

    # `shuffled` still has a fresh, monotonic RangeIndex (ignore_index=True),
    # under which `out.index.get_indexer(idx)` is the identity by
    # construction -- a broken positional-assignment path would pass
    # unnoticed. Feed a reversed, non-monotonic index and confirm the result
    # is unchanged.
    reindexed = df.copy()
    reindexed.index = reindexed.index[::-1]
    reversed_out = (
        run_ssflux_reduce(reindexed, _cfg(), _Logger()).sort_values(ID).reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(whole, reversed_out)


def test_nan_rows_survive_for_gap_fill():
    df = _frame(20)
    df.loc[df[ID] == 5, [f"L_{p}" for p in PARAM_NAMES]] = np.nan
    out = run_ssflux_reduce(df, _cfg(), _Logger())
    assert out.loc[out[ID] == 5, "soil2gw_max"].isna().all()
    assert out["soil2gw_max"].notna().sum() == 19


def test_vpu_scope_normalises_within_each_region():
    out = run_ssflux_reduce(_frame(40, with_vpu=True), _cfg("vpu"), _Logger())
    for vpu in ("01", "02"):
        sub = out[out["vpu"] == vpu]["soil2gw_max"]
        assert sub.min() == pytest.approx(0.1)
        assert sub.max() == pytest.approx(0.3)


def test_vpu_scope_requires_the_column():
    with pytest.raises(ValueError, match="vpu"):
        run_ssflux_reduce(_frame(10), _cfg("vpu"), _Logger())


def test_vpu_scope_rejects_partial_null_vpu():
    """A partially-null vpu column must raise, not silently vanish under
    groupby(dropna=True) -- that would produce NaN params indistinguishable
    from the legitimate no-lithology NaN and get silently KNN gap-filled."""
    df = _frame(40, with_vpu=True)
    df.loc[df[ID] == 3, "vpu"] = None
    with pytest.raises(ValueError, match="null"):
        run_ssflux_reduce(df, _cfg("vpu"), _Logger())


def test_vpu_scope_handles_mixed_dtype_and_nonnumeric_labels():
    """NHDPlus VPU labels include non-numeric ones (10L, 10U); independent
    per-batch-file read_csv dtype inference can also give int vs str for the
    same numeric-looking label. Both must group and sort without raising."""
    df = _frame(30)
    df["vpu"] = ["10L"] * 10 + ["10U"] * 10 + [1] * 10
    out = run_ssflux_reduce(df, _cfg("vpu"), _Logger())
    assert set(out["vpu"]) == {"10L", "10U", "1"}
    for vpu in ("10L", "10U", "1"):
        sub = out[out["vpu"] == vpu]["soil2gw_max"]
        assert sub.min() == pytest.approx(0.1)
        assert sub.max() == pytest.approx(0.3)


def test_unknown_norm_scope_raises():
    with pytest.raises(ValueError, match="norm_scope"):
        run_ssflux_reduce(_frame(10), _cfg("galaxy"), _Logger())


def test_missing_L_column_raises():
    df = _frame(10).drop(columns=["L_gwflow_coef"])
    with pytest.raises(ValueError, match="L_gwflow_coef"):
        run_ssflux_reduce(df, _cfg(), _Logger())


def test_csv_round_trip_is_lossless(tmp_path):
    """The map/reduce split writes L_* to CSV and reads them back."""
    df = _frame(30)
    p = tmp_path / "batch.csv"
    df.to_csv(p, index=False)
    back = pd.read_csv(p)
    pd.testing.assert_frame_equal(df, back)


def test_output_is_not_degenerate():
    """#175's acceptance criteria, on a realistic log-normal-ish input.

    The pre-fix product had >=89.5% of HRUs within 1% of the range minimum and
    an IQR spanning 0.1% of the range. Both thresholds here would have failed it.
    """
    out = run_ssflux_reduce(_frame(5000), _cfg(), _Logger())
    for p in FLUX_PARAMS:
        col = out[p["name"]].to_numpy()
        rng = p["max"] - p["min"]
        at_min = np.mean(col <= p["min"] + 0.01 * rng)
        iqr = np.subtract(*np.percentile(col, [75, 25])) / rng
        assert at_min <= 0.20, f"{p['name']}: {at_min:.1%} pinned at range minimum"
        assert iqr >= 0.10, f"{p['name']}: IQR spans only {iqr:.3f} of the range"
