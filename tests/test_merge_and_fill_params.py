"""Tests for scripts/merge_and_fill_params.py"""

import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point, box

import importlib.util

_spec = importlib.util.spec_from_file_location(
    "merge_and_fill_params",
    Path(__file__).resolve().parent.parent / "scripts" / "merge_and_fill_params.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

find_missing_ids = _mod.find_missing_ids
fill_missing_values_knn = _mod.fill_missing_values_knn


class TestFindMissingIds:
    def test_finds_missing_ids(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": [1, 2, 4, 5], "val": [10, 20, 40, 50]})
        df.to_csv(csv, index=False)

        param_df, missing = find_missing_ids(csv, 5, logger)
        assert missing == [3]
        assert len(param_df) == 4

    def test_no_missing_ids(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": [1, 2, 3], "val": [10, 20, 30]})
        df.to_csv(csv, index=False)

        _, missing = find_missing_ids(csv, 3, logger)
        assert missing == []

    def test_all_missing(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": pd.Series(dtype=int), "val": pd.Series(dtype=float)})
        df.to_csv(csv, index=False)

        _, missing = find_missing_ids(csv, 3, logger)
        assert missing == [1, 2, 3]


class TestMergedGpkgPathResolution:
    def test_default_path_uses_new_filename(self, tmp_path):
        targets_dir = tmp_path / "targets"
        targets_dir.mkdir()
        expected = targets_dir / "gfv2_nhru_merged.gpkg"
        old_name = targets_dir / "gfv2_merged_simplified.gpkg"
        assert expected.name == "gfv2_nhru_merged.gpkg"
        assert expected.name != old_name.name


class TestFileNotFoundBehavior:
    def test_raises_when_gpkg_missing(self, tmp_path):
        merged_gpkg = tmp_path / "nonexistent.gpkg"
        assert not merged_gpkg.exists()
        with pytest.raises(FileNotFoundError, match="prepare_fabric.py"):
            if not merged_gpkg.exists():
                raise FileNotFoundError(
                    f"Merged geopackage not found: {merged_gpkg}\n"
                    "Run notebooks/merge_vpu_targets.py or scripts/prepare_fabric.py first."
                )


class TestFillMissingValuesKnn:
    def test_knn_fills_with_nearest_value(self):
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )

        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2],
            "hru_id": [1, 2],
            "my_param": [100.0, 200.0],
        })

        result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, logger)
        assert len(result) == 3
        assert 3 in result["nat_hru_id"].values
        filled_val = result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0]
        assert filled_val in [100.0, 200.0]

    def test_knn_no_missing_returns_original(self):
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({"nat_hru_id": [1], "my_param": [42.0]})

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, logger)
        assert len(result) == 1
        assert result["my_param"].iloc[0] == 42.0
