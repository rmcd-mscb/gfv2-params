"""`--mode copy_constants` — the depstor mode that puts builder-written params in merged/.

This mode BLESSES bytes as a canonical `merged/nhm_*_params.csv`, so every guard it
carries is load-bearing. It had no tests when it shipped; these were added after a
review pointed out that a truncated or wrong-fabric source would be copied, logged
with a row count nobody compares to anything, and become canonical -- and that for a
genuinely constant column the fill sweep's KNN interpolation would then produce the
right value by luck, making the truncation undetectable forever.

`_load_resolved_config` is monkeypatched rather than exercised: it needs a real
base_config + data root, and the contract at scripts/merge_and_fill_params.py's
config-path block says tests must not touch one. Pure pandas + tmp_path, CI-safe.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "derive_depstor_params", _REPO_ROOT / "scripts" / "derive_depstor_params.py"
)
ddp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ddp)

_LOGGER = logging.getLogger("test_copy_constants")


def _config(tmp_path: Path, *, rows=3, expected_max=3, id_feature="nat_hru_id",
            constants=None, source_rows=None):
    """A resolved-config dict shaped like _load_resolved_config's output."""
    src = tmp_path / "depstor_rasters" / "op_flow_thres_params.csv"
    src.parent.mkdir(parents=True, exist_ok=True)
    frame = source_rows if source_rows is not None else pd.DataFrame(
        {id_feature: range(1, rows + 1), "op_flow_thres": [1.0] * rows}
    )
    frame.to_csv(src, index=False)

    if constants is None:
        constants = [{
            "name": "op_flow_thres",
            "source": str(src),
            "merged_file": "nhm_op_flow_thres_params.csv",
            "fill_columns": ["op_flow_thres"],
        }]
    config = {
        "defaults": {
            "output_dir": str(tmp_path / "params"),
            "merged_subdir": "merged",
            "merged_intermediates_subdir": "merged/_intermediates",
            "id_feature": id_feature,
        },
        "constants": constants,
    }
    if expected_max is not None:
        config["expected_max_hru_id"] = expected_max
    return config, src


def _run(monkeypatch, config):
    monkeypatch.setattr(ddp, "_load_resolved_config", lambda args: config)
    args = SimpleNamespace(config="configs/depstor/depstor_params.yml")
    ddp.run_copy_constants(args, _LOGGER)
    return Path(config["defaults"]["output_dir"]) / "merged"


def test_copies_into_merged_under_the_canonical_name(tmp_path, monkeypatch):
    """The LOCATION is the point of this mode, so assert the destination exactly.

    Catches a merged_file/source key swap, and a write into _intermediates/ --
    _merge_paths returns (intermediates, ratios) in that order, so binding the
    wrong element of the tuple is a live mistake this pins.
    """
    config, src = _config(tmp_path)
    merged = _run(monkeypatch, config)

    dst = merged / "nhm_op_flow_thres_params.csv"
    assert dst.exists()
    assert not (merged / "_intermediates" / "nhm_op_flow_thres_params.csv").exists()
    pd.testing.assert_frame_equal(pd.read_csv(dst), pd.read_csv(src))


def test_creates_merged_dir_when_absent(tmp_path, monkeypatch):
    config, _ = _config(tmp_path)
    assert not (tmp_path / "params" / "merged").exists()
    assert _run(monkeypatch, config).is_dir()


def test_missing_source_raises_rather_than_skipping(tmp_path, monkeypatch):
    """Fail loud: a missing source means the builder never ran.

    A skip here would leave exactly the hole this mode exists to close -- the
    parameter absent from merged/ while every downstream signal reads green.
    """
    config, src = _config(tmp_path)
    src.unlink()
    with pytest.raises(FileNotFoundError, match="op_flow_thres"):
        _run(monkeypatch, config)


def test_empty_constants_list_raises(tmp_path, monkeypatch):
    """An empty `constants:` is a CONFIG state, not a data state.

    Unlike an empty endorheic table (a legitimate result for a domain with no
    closed basin), there is no fabric for which "asked to copy constants, found
    none" is correct. The realistic cause is a wrong --config, which must not
    exit 0 having copied nothing.
    """
    config, _ = _config(tmp_path, constants=[])
    with pytest.raises(ValueError, match="nothing to do"):
        _run(monkeypatch, config)


def test_source_missing_the_fabrics_id_column_raises(tmp_path, monkeypatch):
    config, _ = _config(
        tmp_path,
        source_rows=pd.DataFrame({"hru_id": [1, 2, 3], "op_flow_thres": [1.0] * 3}),
    )
    with pytest.raises(ValueError, match="no 'nat_hru_id' column"):
        _run(monkeypatch, config)


def test_duplicate_ids_raise(tmp_path, monkeypatch):
    config, _ = _config(
        tmp_path,
        source_rows=pd.DataFrame({"nat_hru_id": [1, 2, 2], "op_flow_thres": [1.0] * 3}),
    )
    with pytest.raises(ValueError, match="duplicate"):
        _run(monkeypatch, config)


def test_truncated_source_raises(tmp_path, monkeypatch):
    """The failure mode that motivated validation.

    A short file copied silently becomes canonical; the fill sweep then KNN-fills
    the gap, and because op_flow_thres is a constant 1.0 the interpolated value is
    indistinguishable from the truth. The truncation would never be noticed.
    """
    config, _ = _config(tmp_path, rows=2, expected_max=3)
    with pytest.raises(ValueError, match="2 rows but this fabric has 3"):
        _run(monkeypatch, config)


def test_row_count_unchecked_when_the_profile_declares_no_expected_max(tmp_path, monkeypatch):
    """expected_max_hru_id is optional per fabric; absent, the check is skipped."""
    config, _ = _config(tmp_path, rows=2, expected_max=None)
    assert (_run(monkeypatch, config) / "nhm_op_flow_thres_params.csv").exists()
