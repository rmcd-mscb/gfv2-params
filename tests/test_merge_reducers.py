"""The optional reduce hook on run_merge."""

import pandas as pd
import pytest

from gfv2_params.zonal_runners import MERGE_REDUCERS, run_merge


class _Logger:
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def warning(self, *a, **k): pass


def _write_batches(tmp_path, n_batches=2):
    d = tmp_path / "ssflux"
    d.mkdir(parents=True)
    rows = [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)]
    per = len(rows) // n_batches
    for i in range(n_batches):
        chunk = rows[i * per:(i + 1) * per]
        pd.DataFrame(chunk, columns=["nat_hru_id", "L_x"]).to_csv(
            d / f"base_nhm_ssflux_testfab_batch_{i:04d}_param.csv", index=False
        )
    return {
        "source_type": "ssflux",
        "id_feature": "nat_hru_id",
        "merged_file": "out.csv",
        "fabric": "testfab",
        "output_dir": str(tmp_path),
    }


def test_merge_without_reducer_is_unchanged(tmp_path):
    cfg = _write_batches(tmp_path)
    run_merge(cfg, _Logger())
    out = pd.read_csv(tmp_path / "merged" / "out.csv")
    assert list(out.columns) == ["nat_hru_id", "L_x"]
    assert out["L_x"].tolist() == [10.0, 20.0, 30.0, 40.0]


def test_reducer_receives_full_frame_and_its_result_is_written(tmp_path):
    cfg = _write_batches(tmp_path)
    seen = {}

    def reducer(df, config, logger):
        seen["n"] = len(df)
        return df.assign(scaled=df["L_x"] / df["L_x"].max()).drop(columns=["L_x"])

    run_merge(cfg, _Logger(), reducer=reducer)
    out = pd.read_csv(tmp_path / "merged" / "out.csv")
    assert seen["n"] == 4, "reducer must see all batches, not one"
    assert "L_x" not in out.columns
    assert out["scaled"].max() == pytest.approx(1.0)


def test_reducer_must_return_a_dataframe(tmp_path):
    cfg = _write_batches(tmp_path)
    with pytest.raises(TypeError, match="DataFrame"):
        run_merge(cfg, _Logger(), reducer=lambda df, config, logger: None)


def test_merge_reducers_is_a_dict():
    assert isinstance(MERGE_REDUCERS, dict)


def test_reducer_that_drops_a_row_raises(tmp_path):
    """MERGE_REDUCERS is explicitly designed for extension beyond ssflux, so
    this protects every future reducer: pre-reducer validation (duplicate
    check, gap warning) is worthless if the reducer itself can silently drop
    rows on the way out."""
    cfg = _write_batches(tmp_path)

    def reducer(df, config, logger):
        return df.iloc[:-1]

    with pytest.raises(ValueError, match="row count"):
        run_merge(cfg, _Logger(), reducer=reducer)


def test_reducer_that_relabels_an_id_raises(tmp_path):
    """Same row count, but a different id set -- must also raise; count alone
    isn't enough to prove row identity survived."""
    cfg = _write_batches(tmp_path)

    def reducer(df, config, logger):
        out = df.copy()
        out.loc[out.index[0], "nat_hru_id"] = 999
        return out

    with pytest.raises(ValueError, match="nat_hru_id"):
        run_merge(cfg, _Logger(), reducer=reducer)


def test_reducer_that_drops_the_id_column_raises(tmp_path):
    cfg = _write_batches(tmp_path)

    def reducer(df, config, logger):
        return df.drop(columns=["nat_hru_id"])

    with pytest.raises(ValueError, match="nat_hru_id"):
        run_merge(cfg, _Logger(), reducer=reducer)


def test_reducer_that_preserves_rows_and_ids_is_unaffected(tmp_path):
    """The invariant check must not false-positive on a well-behaved reducer."""
    cfg = _write_batches(tmp_path)

    def reducer(df, config, logger):
        return df.assign(scaled=df["L_x"] * 2)

    run_merge(cfg, _Logger(), reducer=reducer)
    out = pd.read_csv(tmp_path / "merged" / "out.csv")
    assert len(out) == 4
    assert set(out["nat_hru_id"]) == {1, 2, 3, 4}


# ---------------------------------------------------------------------------
# read_dtypes: per-file dtype inference is not stable across batch files
# ---------------------------------------------------------------------------
# This reproduces the real ssflux `vpu` defect: pandas infers a categorical
# column's dtype PER FILE. If one batch's values are all numeric-looking with
# leading zeros ("01", "02") and another batch mixes in an alphanumeric label
# ("03N"), the first batch reads as int64 (silently stripping the leading
# zero: "01" -> 1) and the second as str -- so the same label ends up split
# across two representations in the merged frame. Measured on a real gfv2_dev
# rebuild: 28 distinct vpu labels merged where the fabric has only 21.


def _write_mixed_dtype_batches(tmp_path):
    d = tmp_path / "ssflux"
    d.mkdir(parents=True)
    # Batch 0: all-numeric-looking labels -> pandas infers int64, drops the
    # leading zero ("01" -> 1, "02" -> 2).
    pd.DataFrame(
        {"nat_hru_id": [1, 2], "vpu": ["01", "02"]}
    ).to_csv(d / "base_nhm_ssflux_testfab_batch_0000_param.csv", index=False)
    # Batch 1: mixed alphanumeric label -> pandas infers str/object.
    pd.DataFrame(
        {"nat_hru_id": [3, 4], "vpu": ["01", "03N"]}
    ).to_csv(d / "base_nhm_ssflux_testfab_batch_0001_param.csv", index=False)
    return {
        "source_type": "ssflux",
        "id_feature": "nat_hru_id",
        "merged_file": "out.csv",
        "fabric": "testfab",
        "output_dir": str(tmp_path),
    }


def test_merge_without_read_dtypes_exhibits_the_vpu_split(tmp_path):
    """Documents the failure mode: without `read_dtypes`, the same "01" label
    survives as the string "01" in the mixed batch but as the int 1 in the
    all-numeric batch."""
    cfg = _write_mixed_dtype_batches(tmp_path)
    run_merge(cfg, _Logger())
    out = pd.read_csv(tmp_path / "merged" / "out.csv", dtype={"nat_hru_id": int})
    labels = set(out["vpu"].astype(str))
    # The defect: "01" got silently split into "1" (from the int64 batch) and
    # "01" (from the str batch) -- two labels in the merged frame for one
    # fabric VPU.
    assert labels == {"1", "2", "01", "03N"}


def test_merge_with_read_dtypes_preserves_labels_verbatim(tmp_path):
    """`read_dtypes: {vpu: str}` forces every batch to be read identically,
    so no leading zero is stripped and no label splits."""
    cfg = _write_mixed_dtype_batches(tmp_path)
    cfg["read_dtypes"] = {"vpu": "str"}
    run_merge(cfg, _Logger())
    out = pd.read_csv(tmp_path / "merged" / "out.csv", dtype={"vpu": str})
    assert set(out["vpu"]) == {"01", "02", "03N"}
    # nat_hru_id 1 and 3 both carry "01" -- confirm it's the same string, not
    # coincidentally-equal ints and strs.
    assert out.set_index("nat_hru_id")["vpu"].to_dict() == {1: "01", 2: "02", 3: "01", 4: "03N"}
