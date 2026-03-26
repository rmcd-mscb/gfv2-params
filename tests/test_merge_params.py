"""Tests for scripts/merge_params.py"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

import importlib.util

_spec = importlib.util.spec_from_file_location(
    "merge_params",
    Path(__file__).resolve().parent.parent / "scripts" / "merge_params.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

process_files = _mod.process_files


def _make_config(tmp_path, fabric="gfv2", source_type="elevation", expected_max=None):
    output_dir = tmp_path / "params"
    (output_dir / source_type).mkdir(parents=True)
    config = {
        "source_type": source_type,
        "id_feature": "nat_hru_id",
        "merged_file": f"nhm_{source_type}_params.csv",
        "fabric": fabric,
        "output_dir": str(output_dir),
    }
    if expected_max is not None:
        config["expected_max_hru_id"] = expected_max
    return config


def _write_batch_csv(output_dir, source_type, fabric, batch_id, ids, values):
    path = Path(output_dir) / source_type / f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param.csv"
    df = pd.DataFrame({"nat_hru_id": ids, "mean": values})
    df.to_csv(path, index=False)


class TestProcessFiles:
    def test_merges_and_sorts(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        config = _make_config(tmp_path)
        _write_batch_csv(config["output_dir"], "elevation", "gfv2", 0, [3, 4], [30, 40])
        _write_batch_csv(config["output_dir"], "elevation", "gfv2", 1, [1, 2], [10, 20])

        process_files(config, logger)

        merged = pd.read_csv(Path(config["output_dir"]) / "merged" / "nhm_elevation_params.csv")
        assert list(merged["nat_hru_id"]) == [1, 2, 3, 4]
        assert list(merged["mean"]) == [10, 20, 30, 40]

    def test_raises_on_duplicates(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        config = _make_config(tmp_path)
        _write_batch_csv(config["output_dir"], "elevation", "gfv2", 0, [1, 2], [10, 20])
        _write_batch_csv(config["output_dir"], "elevation", "gfv2", 1, [2, 3], [20, 30])

        with pytest.raises(ValueError, match="Duplicate"):
            process_files(config, logger)

    def test_warns_on_gaps(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        config = _make_config(tmp_path, expected_max=5)
        _write_batch_csv(config["output_dir"], "elevation", "gfv2", 0, [1, 2, 4, 5], [10, 20, 40, 50])

        process_files(config, logger)

        merged = pd.read_csv(Path(config["output_dir"]) / "merged" / "nhm_elevation_params.csv")
        assert len(merged) == 4
        assert 3 not in merged["nat_hru_id"].values

    def test_no_gap_warning_without_expected_max(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        config = _make_config(tmp_path)  # no expected_max
        _write_batch_csv(config["output_dir"], "elevation", "gfv2", 0, [1, 3], [10, 30])

        process_files(config, logger)  # should not raise

    def test_raises_on_no_files(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        config = _make_config(tmp_path)

        with pytest.raises(FileNotFoundError, match="No batch files"):
            process_files(config, logger)

    def test_raises_on_missing_id_column(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        config = _make_config(tmp_path)
        # Write a CSV without nat_hru_id
        path = Path(config["output_dir"]) / "elevation" / "base_nhm_elevation_gfv2_batch_0000_param.csv"
        pd.DataFrame({"wrong_col": [1], "mean": [10]}).to_csv(path, index=False)

        with pytest.raises(ValueError, match="nat_hru_id"):
            process_files(config, logger)
