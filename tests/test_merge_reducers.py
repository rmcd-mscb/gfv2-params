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
