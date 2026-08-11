"""Tests for the per-param merge logic.

The merge work was lifted from scripts/merge_params.py:process_files into
gfv2_params.zonal_runners.run_merge as part of Step 4. The library function
is the source of truth; the CLI shell in scripts/merge_params.py now
delegates to it. These tests target the library function directly.
"""

import math
from pathlib import Path

import pandas as pd
import pytest

from gfv2_params.zonal_runners import run_merge as process_files
from gfv2_params.zonal_runners.merge import apply_derived_columns


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


# ---------------------------------------------------------------------------
# derived_columns: PRMS-quantity columns computed at merge time (D0a).
#
# Lives here rather than in tests/test_params_index.py because importing
# gfv2_params.zonal_runners pulls the geo stack (raster_ops -> rasterio/GDAL),
# and that module is deliberately geo-import-free.
# ---------------------------------------------------------------------------


def test_apply_derived_columns_converts_slope_degrees_to_rise_run():
    """gfv2 HRU 1 is the worked example: mean = 4.4252 deg -> hru_slope = 0.0774.

    Copied verbatim without this conversion, `mean` declares a 77-degree cliff
    instead of a 4.4-degree hillslope.

    tan(radians(4.4252)) is 0.07738825, not the 0.077398 the plan quoted -- close
    enough to pass a 1e-3 tolerance and fail a 1e-4 one, which is exactly what it
    did. The value here is the computed one.
    """
    df = pd.DataFrame({"nat_hru_id": [1, 2], "mean": [4.4252, 45.0]})
    out = apply_derived_columns(
        df, {"hru_slope": {"from": "mean", "transform": "deg_to_fraction"}}
    )
    assert math.isclose(out["hru_slope"][0], 0.07738825, rel_tol=1e-6)
    assert math.isclose(out["hru_slope"][1], 1.0, rel_tol=1e-9)  # tan(45 deg) == 1
    assert "mean" in out.columns  # the raw stat is kept as declared provenance


def test_apply_derived_columns_rejects_an_unknown_transform():
    """A whitelist, not getattr(raster_ops, name): a typo must raise."""
    df = pd.DataFrame({"mean": [1.0]})
    with pytest.raises(ValueError, match="not a known transform"):
        apply_derived_columns(df, {"x": {"from": "mean", "transform": "nope"}})


def test_apply_derived_columns_rejects_a_missing_source_column():
    df = pd.DataFrame({"mean": [1.0]})
    with pytest.raises(ValueError, match="not in the merged"):
        apply_derived_columns(
            df, {"x": {"from": "absent", "transform": "deg_to_fraction"}}
        )


def test_apply_derived_columns_is_a_noop_when_undeclared():
    df = pd.DataFrame({"mean": [1.0]})
    assert list(apply_derived_columns(df, None).columns) == ["mean"]
    assert list(apply_derived_columns(df, {}).columns) == ["mean"]


def test_atan2_deg_recovers_the_circular_mean_across_the_wrap():
    """The defect, stated as a test: 350 deg and 10 deg average to 0, not 180.

    Arithmetic mean of [350, 10] is 180 -- due SOUTH for two nearly-north-facing
    cells. That is issue #201 in two numbers.
    """
    sin_mean = pd.Series([(math.sin(math.radians(350)) + math.sin(math.radians(10))) / 2])
    cos_mean = pd.Series([(math.cos(math.radians(350)) + math.cos(math.radians(10))) / 2])
    out = apply_derived_columns(
        pd.DataFrame({"mean_sin": sin_mean, "mean_cos": cos_mean}),
        {"hru_aspect": {"from": ["mean_sin", "mean_cos"], "transform": "atan2_deg"}},
    )
    assert math.isclose(out["hru_aspect"][0], 0.0, abs_tol=1e-9)


def test_atan2_deg_normalises_into_zero_to_360():
    """numpy's arctan2 returns -180..180; PRMS hru_aspect is 0-360 (TM6B9:603)."""
    # 225 deg (south-west) -> arctan2 gives -135 without the modulo.
    df = pd.DataFrame({
        "mean_sin": [math.sin(math.radians(225)), math.sin(math.radians(90))],
        "mean_cos": [math.cos(math.radians(225)), math.cos(math.radians(90))],
    })
    out = apply_derived_columns(
        df, {"hru_aspect": {"from": ["mean_sin", "mean_cos"], "transform": "atan2_deg"}}
    )
    assert math.isclose(out["hru_aspect"][0], 225.0, abs_tol=1e-9)
    assert math.isclose(out["hru_aspect"][1], 90.0, abs_tol=1e-9)
    assert (out["hru_aspect"] >= 0).all() and (out["hru_aspect"] < 360).all()


def test_atan2_deg_preserves_nan_for_an_hru_with_no_sloped_cells():
    """An all-flat HRU has NaN means; hru_aspect must stay NaN, not become 0 (north).

    The fill sweep supplies neighbours' means and Task 3 re-derives from those.
    """
    df = pd.DataFrame({"mean_sin": [float("nan")], "mean_cos": [float("nan")]})
    out = apply_derived_columns(
        df, {"hru_aspect": {"from": ["mean_sin", "mean_cos"], "transform": "atan2_deg"}}
    )
    assert math.isnan(out["hru_aspect"][0])


def test_apply_derived_columns_rejects_a_missing_column_in_a_list_source():
    """The list form gets the same eager validation as the string form."""
    df = pd.DataFrame({"mean_sin": [0.0]})
    with pytest.raises(ValueError, match="not in the merged"):
        apply_derived_columns(
            df, {"x": {"from": ["mean_sin", "absent"], "transform": "atan2_deg"}}
        )


def test_run_merge_emits_the_declared_derived_column(tmp_path):
    """End to end through run_merge: config -> merged CSV with hru_slope."""
    config = _make_config(tmp_path, source_type="slope")
    config["derived_columns"] = {
        "hru_slope": {"from": "mean", "transform": "deg_to_fraction"}
    }
    batch_dir = Path(config["output_dir"]) / "slope"
    pd.DataFrame({"nat_hru_id": [1, 2], "mean": [4.4252, 45.0]}).to_csv(
        batch_dir / "base_nhm_slope_gfv2_batch_0_param.csv", index=False
    )

    import logging

    process_files(config, logging.getLogger("test_derived"))

    out = pd.read_csv(
        Path(config["output_dir"]) / "merged" / "nhm_slope_params.csv"
    )
    assert "hru_slope" in out.columns
    # Same computed value as the unit test above -- NOT the plan's 0.077398, which
    # sat here passing on 2.5% of a 1e-5 budget while a docstring 55 lines up
    # declared it wrong.
    assert math.isclose(
        out.loc[out["nat_hru_id"] == 1, "hru_slope"].iloc[0], 0.07738825, rel_tol=1e-6
    )
