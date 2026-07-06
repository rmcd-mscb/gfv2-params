import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from gfv2_params.config import load_config


def test_snodas_source_entry_resolves():
    cfg = load_config(
        Path("configs/aggregate/aggregate_sources.yml"),
        base_config_path=Path("configs/base_config.yml"),
        fabric="oregon",
    )
    assert cfg["fabric"] == "oregon"
    src = next(s for s in cfg["sources"] if s["name"] == "snodas")
    assert src["output_prefix"] == "snodas"
    # output_dir is a top-level key, so load_config resolves it fully.
    assert cfg["output_dir"] == "{data_root}/oregon/snodas".format(data_root=cfg["data_root"])
    assert "{fabric}" not in cfg["output_dir"]
    assert cfg["weight_dir"] == "{data_root}/oregon/weights_agg".format(data_root=cfg["data_root"])


def test_batch_dir_resolves():
    cfg = load_config(
        Path("configs/aggregate/aggregate_sources.yml"),
        base_config_path=Path("configs/base_config.yml"),
        fabric="oregon",
    )
    assert "batch_dir" in cfg
    assert cfg["batch_dir"] == "{data_root}/oregon/batches".format(data_root=cfg["data_root"])
    assert "{fabric}" not in cfg["batch_dir"]


def _make_batch_nc(path: Path, hru_ids: list[int]) -> None:
    time = pd.date_range("2010-01-01", periods=3, freq="D")
    ds = xr.Dataset(
        {
            "swe": (("time", "hru_id"), np.arange(len(time) * len(hru_ids)).reshape(len(time), len(hru_ids)).astype(float)),
            "scov": (("time", "hru_id"), np.zeros((len(time), len(hru_ids)))),
        },
        coords={"time": time, "hru_id": hru_ids},
    )
    ds.to_netcdf(path)


def test_run_merge_concatenates_batches_and_sorts(tmp_path):
    from scripts.derive_aggregate import run_merge

    batches_dir = tmp_path / "_batches"
    batches_dir.mkdir()
    _make_batch_nc(batches_dir / "snodas_batch0000_agg_2010.nc", [1, 2])
    _make_batch_nc(batches_dir / "snodas_batch0001_agg_2010.nc", [3, 4])

    logger = logging.getLogger("test_run_merge")

    written = run_merge(tmp_path, "snodas", "hru_id", logger)

    assert len(written) == 1
    out = written[0]
    assert out.name == "snodas_agg_2010.nc"
    assert out.parent == tmp_path

    merged = xr.open_dataset(out)
    try:
        assert list(merged["hru_id"].values) == [1, 2, 3, 4]
        assert "swe" in merged.data_vars
        assert "scov" in merged.data_vars
    finally:
        merged.close()


def test_consolidate_weights_concats_per_batch(tmp_path):
    import logging

    import pandas as pd

    from scripts.derive_aggregate import consolidate_weights

    wdir = tmp_path / "weights_agg"
    wdir.mkdir()
    # two disjoint-HRU per-batch weight tables
    pd.DataFrame({"hru_id": [1, 1, 2], "wght": [0.1, 0.2, 0.9]}).to_csv(
        wdir / "snodas_weights_oregon_batch0000.csv", index=False)
    pd.DataFrame({"hru_id": [3, 4, 4], "wght": [0.5, 0.4, 0.6]}).to_csv(
        wdir / "snodas_weights_oregon_batch0001.csv", index=False)

    out = consolidate_weights(wdir, "snodas", "oregon", logging.getLogger("t"))
    assert out == wdir / "snodas_weights_oregon.csv"
    combined = pd.read_csv(out)
    assert len(combined) == 6                                  # all rows preserved
    assert sorted(combined["hru_id"].unique()) == [1, 2, 3, 4]  # both batches present


def test_consolidate_weights_raises_when_no_batches(tmp_path):
    import logging

    import pytest

    from scripts.derive_aggregate import consolidate_weights

    (tmp_path / "weights_agg").mkdir()
    with pytest.raises(FileNotFoundError, match="per-batch weight"):
        consolidate_weights(tmp_path / "weights_agg", "snodas", "oregon", logging.getLogger("t"))
