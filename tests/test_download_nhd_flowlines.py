"""Unit tests for the NHD flowline -> connected-waterbody-COMID distillation."""

import pandas as pd

from gfv2_params.download.nhd_flowlines import (
    connected_comids_from_flowlines,
    write_connected_comids,
)


def test_connected_comids_distinct_nonzero():
    df = pd.DataFrame(
        {
            "COMID": [1, 2, 3, 4, 5],
            "FTYPE": ["ArtificialPath", "ArtificialPath", "StreamRiver",
                      "ArtificialPath", "ArtificialPath"],
            # 0 = not through a waterbody; duplicates collapse; None excluded.
            "WBAREACOMI": [100, 100, 0, 200, None],
        }
    )
    assert connected_comids_from_flowlines(df) == {100, 200}


def test_connected_comids_empty_when_all_zero():
    df = pd.DataFrame({"WBAREACOMI": [0, 0, 0]})
    assert connected_comids_from_flowlines(df) == set()


def test_write_connected_comids_roundtrip(tmp_path):
    out = tmp_path / "nested" / "connected_waterbody_comids.parquet"
    write_connected_comids({300, 100, 200}, out)

    assert out.exists()
    got = pd.read_parquet(out)
    assert list(got.columns) == ["comid"]
    assert got["comid"].tolist() == [100, 200, 300]  # sorted ascending
    assert str(got["comid"].dtype) in ("int64", "Int64")
