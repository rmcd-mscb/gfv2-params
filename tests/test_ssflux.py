"""Map/reduce behaviour for ssflux (no geo fixtures -- the reducer is pure)."""

import numpy as np
import pandas as pd
import pytest

from gfv2_params.zonal_runners import MERGE_REDUCERS, run_ssflux_reduce
from gfv2_params.zonal_runners.ssflux import _canonical_vpu_label, _coverage_kwargs
from gfv2_params.zonal_runners.ssflux_math import (
    FFLUX_NO_OVERLAP,
    PARAM_NAMES,
    aggregate_k_perm_log,
    derive_log_params,
    validate_weight_coverage,
)

# The 8 real discrete Gleeson log10-permeability classes (see the design doc
# and ssflux_math.py) -- used by test_output_is_not_degenerate so that test
# exercises the actual achievable spatial pattern, not an arbitrary
# distribution the production pipeline could never see.
GLEESON_K_PERM_CLASSES = np.array(
    [-10.87, -11.79, -12.47, -12.50, -12.78, -14.05, -15.05, -16.48]
)
K_PERM_MIN_LEGACY = -16.48  # pre-#175 floor substituted for the no-data flag

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
    def __init__(self):
        self.info_calls = []

    def info(self, msg, *args, **k):
        self.info_calls.append((msg, args))

    def debug(self, *a, **k): pass
    def warning(self, *a, **k): pass

    def n_groups(self):
        """Pull the group count out of run_ssflux_reduce's own summary log
        line ("Normalised %d ssflux params over scope=%s (%d group(s))"),
        without needing the reducer to expose its internal `groups` list."""
        for msg, args in self.info_calls:
            if "Normalised" in msg:
                return args[-1]
        raise AssertionError("expected a 'Normalised ...' info log call")


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
    """Input labels are zero-padded ("01"/"02") and must come back out of the
    reducer UNCHANGED -- `vpu` is declared in `prms: provenance` as "per-HRU
    VPU" and must record the fabric's label verbatim, not a canonicalised
    form (canonicalisation is for grouping only, see
    test_vpu_scope_normalises_mixed_leading_zero_and_int_labels_into_one_group).
    The two distinct raw labels must still normalise as two groups."""
    logger = _Logger()
    out = run_ssflux_reduce(_frame(40, with_vpu=True), _cfg("vpu"), logger)
    assert set(out["vpu"]) == {"01", "02"}
    for vpu in ("01", "02"):
        sub = out[out["vpu"] == vpu]["soil2gw_max"]
        assert sub.min() == pytest.approx(0.1)
        assert sub.max() == pytest.approx(0.3)
    assert logger.n_groups() == 2


def test_vpu_column_is_not_mutated_by_reducer():
    """Explicit guard on the fidelity requirement: the emitted `vpu` values
    must be byte-for-byte identical to what was passed in, including dtype --
    the reducer may build a throwaway canonicalised key for grouping, but
    must never assign it back onto the returned frame's `vpu` column."""
    df = _frame(30)
    df["vpu"] = ["10L"] * 10 + ["10U"] * 10 + [1] * 10
    out = run_ssflux_reduce(df, _cfg("vpu"), _Logger())
    merged = df[[ID, "vpu"]].merge(
        out[[ID, "vpu"]], on=ID, suffixes=("_in", "_out")
    )
    assert (merged["vpu_in"] == merged["vpu_out"]).all()
    assert list(merged["vpu_in"]) == list(merged["vpu_out"])


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
    same numeric-looking label. Both must group and sort without raising, and
    the emitted `vpu` column must still carry the RAW input values verbatim
    (int 1, not canonicalised str "1")."""
    df = _frame(30)
    df["vpu"] = ["10L"] * 10 + ["10U"] * 10 + [1] * 10
    out = run_ssflux_reduce(df, _cfg("vpu"), _Logger())
    assert set(out["vpu"]) == {"10L", "10U", 1}
    for vpu in ("10L", "10U", 1):
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


def test_stale_pre175_batch_raises_via_retired_kperm_wtd_column():
    """Simulates a REAL stale merge, not the impossible frame the old test used.

    A genuine pre-#175 batch CSV has schema <id>, k_perm_wtd,
    mean_slope_fraction, hru_area, <7 param columns> -- it has NEITHER
    k_perm_log_wtd NOR L_*/fflux at all. pd.concat's column union backfills
    those with NaN for every row the stale file contributed (passing the
    `missing` check, since the fresh batch has L_*), and k_perm_log_wtd is
    ALSO NaN for those rows (never populated) -- so the retired guard
    (`has_k & L_isna`) could never fire on this real shape; it required a
    hand-built frame that forced k_perm_log_wtd populated on "stale" rows,
    which is not a state a real stale file produces. The retired 'k_perm_wtd'
    column itself is what makes this conclusively detectable.
    """
    fresh = _frame(10)
    fresh["k_perm_log_wtd"] = -12.0
    fresh["fflux"] = 0.9
    old_schema = pd.DataFrame(
        {
            ID: np.arange(11, 14),
            "k_perm_wtd": [1.2e-12, 3.4e-13, 5.6e-14],
            "mean_slope_fraction": [0.1, 0.2, 0.3],
            "hru_area": [1e5, 2e5, 3e5],
        }
    )
    for p in PARAM_NAMES:
        old_schema[p] = 0.05
    merged = pd.concat([fresh, old_schema], ignore_index=True)
    with pytest.raises(ValueError, match="k_perm_wtd"):
        run_ssflux_reduce(merged, _cfg(), _Logger())


def test_foreign_batch_without_fflux_raises_even_without_retired_column():
    """A foreign/corrupted file carrying neither the retired k_perm_wtd NOR
    fflux must still be caught -- by the coverage-column check, which is why
    both checks are needed and not just the retired-column one."""
    fresh = _frame(10)
    fresh["k_perm_log_wtd"] = -12.0
    fresh["fflux"] = 0.9
    foreign = pd.DataFrame({ID: np.arange(11, 13)})
    merged = pd.concat([fresh, foreign], ignore_index=True)
    with pytest.raises(ValueError, match="fflux"):
        run_ssflux_reduce(merged, _cfg(), _Logger())


def test_legitimate_nan_k_perm_with_nan_L_does_not_raise():
    """A genuine no-lithology HRU (NaN k_perm_log_wtd, NaN L_*) must NOT raise.

    This is exactly the case the retired `has_k` check could never actually
    distinguish from a stale row (see the test above) -- what separates it is
    that `fflux` is POPULATED here (FFLUX_NO_OVERLAP, -1.0), never NaN, unlike
    a genuinely stale/foreign row.
    """
    df = _frame(10)
    df["k_perm_log_wtd"] = -12.0
    df["fflux"] = 0.9
    df.loc[df[ID] == 5, "k_perm_log_wtd"] = np.nan
    df.loc[df[ID] == 5, "fflux"] = FFLUX_NO_OVERLAP
    df.loc[df[ID] == 5, [f"L_{p}" for p in PARAM_NAMES]] = np.nan
    out = run_ssflux_reduce(df, _cfg(), _Logger())
    assert out.loc[out[ID] == 5, "soil2gw_max"].isna().all()
    assert out["soil2gw_max"].notna().sum() == 9


def test_csv_round_trip_is_lossless(tmp_path):
    """The map/reduce split writes L_* to CSV and reads them back."""
    df = _frame(30)
    p = tmp_path / "batch.csv"
    df.to_csv(p, index=False)
    back = pd.read_csv(p)
    pd.testing.assert_frame_equal(df, back)


def test_output_is_not_degenerate():
    """#175's acceptance criteria, end to end through the real pipeline
    functions on a realistic Gleeson-class input.

    The prior version of this test fed i.i.d. Gaussians straight into
    run_ssflux_reduce -- it never called aggregate_k_perm_log or
    derive_log_params, so it would still pass with the entire #175 fix
    reverted (it tests only that interpolate_to_range maps a normal
    distribution onto a range, which was never broken). This version instead
    builds a weight frame from the 8 real discrete Gleeson k_perm classes
    (plus a no-data slice) and runs it through aggregate_k_perm_log ->
    derive_log_params -> run_ssflux_reduce -- the actual #175 pipeline.

    It then proves the fix mattered by computing the INVERSE: reconstructing
    the retired pre-#175 pipeline (extensive aggregation via
    10**k_perm * area_weight/flux_id_area, then plain linear min-max on the
    raw, not log10, values -- verbatim from git c5a896d^:ssflux.py) on the
    IDENTICAL input, and asserting it fails every threshold. If it didn't,
    this test would not be distinguishing the fix from the bug it replaced.
    """
    rng = np.random.default_rng(0)
    n_hru = 3000
    ids = np.arange(1, n_hru + 1)
    # ~5% no-data, echoing Gleeson's measured ~4.7% (see ssflux_math.py).
    is_nodata = rng.random(n_hru) < 0.05
    k_perm = np.where(is_nodata, 0.0, rng.choice(GLEESON_K_PERM_CLASSES, size=n_hru))
    slope_fraction = rng.uniform(0.01, 1.2, n_hru)
    hru_area = rng.uniform(1e5, 5e7, n_hru)

    # One fully-covering source polygon per HRU (area_weight/flux_id_area ==
    # normalized_area_weight == 1) -- the common single-dominant-lithology
    # case, and the simplest input on which the OLD extensive form and the
    # NEW intensive form are directly comparable: no artificial area-ratio
    # skew is needed to demonstrate the bug, linear min-max on a
    # log-distributed variable already fails it on its own (see below).
    weights = pd.DataFrame(
        {
            ID: ids,
            "k_perm": k_perm,
            "normalized_area_weight": 1.0,
            "area_weight": 1.0,
            "flux_id_area": 1.0,
        }
    )
    meta = pd.DataFrame({ID: ids, "mean_slope_fraction": slope_fraction, "hru_area": hru_area})

    # ---- the actual #175 pipeline: aggregate -> derive -> reduce ----
    agg = aggregate_k_perm_log(weights, ID)
    df_new = agg.merge(meta, on=ID)
    log_params = derive_log_params(
        df_new["k_perm_log_wtd"].to_numpy(),
        df_new["mean_slope_fraction"].to_numpy(),
        df_new["hru_area"].to_numpy(),
    )
    for name, values in log_params.items():
        df_new[f"L_{name}"] = values
    out_new = run_ssflux_reduce(df_new, _cfg(), _Logger())

    for p in FLUX_PARAMS:
        col = out_new[p["name"]].dropna().to_numpy()
        span = p["max"] - p["min"]
        at_min = np.mean(col <= p["min"] + 0.01 * span)
        iqr = np.subtract(*np.percentile(col, [75, 25])) / span
        assert at_min <= 0.20, f"{p['name']}: {at_min:.1%} pinned at range minimum"
        assert iqr >= 0.10, f"{p['name']}: IQR spans only {iqr:.3f} of the range"

    # ---- the inverse: the retired extensive + linear pipeline, by hand ----
    old = weights.copy()
    old["k_perm"] = old["k_perm"].replace(0.0, K_PERM_MIN_LEGACY)
    old["k_perm_actual"] = 10.0 ** old["k_perm"]
    old["k_perm_wtd_sum"] = old["k_perm_actual"] * (old["area_weight"] / old["flux_id_area"])
    extensive = old.groupby(ID)["k_perm_wtd_sum"].sum().rename("k_perm_wtd").reset_index()
    old_df = extensive.merge(meta, on=ID)
    old_df["r_soil2gw_max"] = old_df["k_perm_wtd"] ** 3
    old_df["r_ssr2gw_rate"] = old_df["k_perm_wtd"] * (1 - old_df["mean_slope_fraction"])
    old_df["r_slowcoef_lin"] = (
        old_df["k_perm_wtd"] * old_df["mean_slope_fraction"]
    ) / old_df["hru_area"]
    old_df["r_fastcoef_lin"] = 2 * old_df["r_slowcoef_lin"]
    old_df["r_gwflow_coef"] = old_df["r_slowcoef_lin"]
    old_df["r_dprst_seep_rate_open"] = old_df["r_ssr2gw_rate"]
    old_df["r_dprst_flow_coef"] = old_df["r_fastcoef_lin"]

    for p in FLUX_PARAMS:
        rcol = f"r_{p['name']}"
        vv = old_df[rcol].to_numpy()
        lo_in, hi_in = vv.min(), vv.max()
        scaled = (vv - lo_in) / (hi_in - lo_in) * (p["max"] - p["min"]) + p["min"]
        span = p["max"] - p["min"]
        at_min = np.mean(scaled <= p["min"] + 0.01 * span)
        iqr = np.subtract(*np.percentile(scaled, [75, 25])) / span
        assert at_min > 0.20 or iqr < 0.10, (
            f"{p['name']}: the retired extensive+linear pipeline passed the "
            "acceptance thresholds on this input -- this test no longer "
            "distinguishes the #175 fix from the bug it replaced"
        )


def test_vpu_scope_normalises_mixed_leading_zero_and_int_labels_into_one_group():
    """Per-file read_csv dtype inference can strip leading zeros
    inconsistently ACROSS batch files -- "01" from one file, int 1 from
    another, for the SAME VPU -- which a bare astype(str) would leave as two
    distinct groupby keys ("01" vs "1"), silently splitting one VPU's HRUs
    into two normalisation groups. Both must land in ONE group -- proven via
    the reducer's own group-count log line, since the emitted `vpu` column no
    longer collapses to a single canonical value (it stays verbatim, see
    test_vpu_column_is_not_mutated_by_reducer)."""
    assert _canonical_vpu_label("01") == "1"
    assert _canonical_vpu_label(1) == "1"
    assert _canonical_vpu_label(" 02 ") == "2"
    assert _canonical_vpu_label("10L") == "10L"

    df = _frame(20)
    df["vpu"] = ["01"] * 10 + [1] * 10
    logger = _Logger()
    out = run_ssflux_reduce(df, _cfg("vpu"), logger)
    # emitted verbatim -- both raw forms still present, NOT collapsed to "1"
    assert set(out["vpu"]) == {"01", 1}
    assert logger.n_groups() == 1
    assert out["soil2gw_max"].min() == pytest.approx(0.1)
    assert out["soil2gw_max"].max() == pytest.approx(0.3)


def test_coverage_threshold_config_override_is_honoured():
    """median_tol/max_bad_fraction/band_lo/band_hi/magnitude_bad_fraction are
    optional keys on the ssflux entry in configs/zonal/zonal_params.yml;
    run_ssflux_batch threads whichever ones are present into
    validate_weight_coverage via _coverage_kwargs. Prove both ends:
    _coverage_kwargs extracts only the keys actually set (absent keys keep
    today's behaviour exactly), and threading a real override changes
    validate_weight_coverage's outcome."""
    config = {"id_feature": ID, "magnitude_bad_fraction": 0.9, "unrelated_key": 123}
    kwargs = _coverage_kwargs(config)
    assert kwargs == {"magnitude_bad_fraction": 0.9}

    # 60 HRUs at sum=1.0, 40 at sum=0.05: median stays 1.0 (so the median
    # check doesn't fire) but 40% of sums fall outside the default [0.5, 2.0]
    # magnitude band, past the default magnitude_bad_fraction=0.05 ceiling.
    # Raises by default; must not raise once the config's looser ceiling is
    # threaded through.
    rows = [(i, -12.0, 1.0) for i in range(1, 61)] + [
        (i, -12.0, 0.05) for i in range(61, 101)
    ]
    w = pd.DataFrame(rows, columns=[ID, "k_perm", "normalized_area_weight"])
    with pytest.raises(ValueError, match="band"):
        validate_weight_coverage(w, ID, _Logger())
    validate_weight_coverage(w, ID, _Logger(), **kwargs)
