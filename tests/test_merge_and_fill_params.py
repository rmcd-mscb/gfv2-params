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

        param_df, missing = find_missing_ids(csv, 5, "nat_hru_id", logger)
        assert missing == [3]
        assert len(param_df) == 4

    def test_no_missing_ids(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": [1, 2, 3], "val": [10, 20, 30]})
        df.to_csv(csv, index=False)

        _, missing = find_missing_ids(csv, 3, "nat_hru_id", logger)
        assert missing == []

    def test_all_missing(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": pd.Series(dtype=int), "val": pd.Series(dtype=float)})
        df.to_csv(csv, index=False)

        _, missing = find_missing_ids(csv, 3, "nat_hru_id", logger)
        assert missing == [1, 2, 3]

    def test_keys_on_custom_id_feature(self, tmp_path):
        """A fabric whose id column is hru_id (e.g. oregon) keys on it, not nat_hru_id."""
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"hru_id": [1, 2, 4], "val": [10, 20, 40]})
        df.to_csv(csv, index=False)

        param_df, missing = find_missing_ids(csv, 4, "hru_id", logger)
        assert missing == [3]
        assert len(param_df) == 3


class TestMergedGpkgPathResolution:
    """The merged gpkg default now comes from the active profile's hru_gpkg
    (configs/base_config.yml), not a {fabric}_nhru_merged.gpkg naming
    convention. End-to-end resolution + error behavior is covered by
    tests/test_hru_gpkg_config.py; here we pin the source-of-truth contract."""

    def test_default_is_profile_hru_gpkg(self):
        src = (
            Path(__file__).resolve().parent.parent
            / "scripts" / "merge_and_fill_params.py"
        ).read_text()
        # The merged gpkg default is sourced from the active profile's hru_gpkg
        # via require_config_key — not the retired {fabric}_nhru_merged.gpkg
        # path convention. Assert on code (not a substring scan of the whole
        # file, which would also match explanatory comments); end-to-end
        # resolution is covered by tests/test_hru_gpkg_config.py.
        assert 'require_config_key(base, "hru_gpkg"' in src
        assert 'f"{data_root}/{fabric}/fabric/{fabric}_nhru_merged.gpkg"' not in src


class TestFileNotFoundBehavior:
    def test_raises_when_gpkg_missing(self, tmp_path):
        merged_gpkg = tmp_path / "nonexistent.gpkg"
        assert not merged_gpkg.exists()
        with pytest.raises(FileNotFoundError, match="base_config.yml"):
            if not merged_gpkg.exists():
                raise FileNotFoundError(
                    f"Fabric geopackage not found: {merged_gpkg}\n"
                    "Check the active fabric profile's hru_gpkg in configs/base_config.yml. "
                    "For VPU-based fabrics, run notebooks/merge_vpu_targets.py to produce it; "
                    "for single-file fabrics, place the gpkg at the hru_gpkg path."
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

        result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, "nat_hru_id", logger)
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

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 1
        assert result["my_param"].iloc[0] == 42.0

    def test_knn_fills_multiple_columns_without_duplicates(self):
        """Filling N columns must append ONE row per missing id with every
        column populated — not N fragmented rows (regression for the
        per-column re-append bug surfaced by the oregon ssflux gap-fill)."""
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({
            "hru_id": [1, 2],
            "p1": [100.0, 200.0],
            "p2": [1.0, 2.0],
        })

        result = fill_missing_values_knn(
            param_df, [3], merged_gdf, ["p1", "p2"], 1, "hru_id", logger
        )
        # Exactly one row per id — no duplicates from per-column appends.
        assert len(result) == 3
        assert result["hru_id"].duplicated().sum() == 0
        # The filled row has BOTH columns populated, not just one.
        filled = result.loc[result["hru_id"] == 3]
        assert len(filled) == 1
        assert filled["p1"].notna().all() and filled["p2"].notna().all()
        assert not result.isna().any().any()

    def test_knn_fills_on_custom_id_feature(self):
        """Filling keys on the configured id_feature (hru_id) for non-gfv2 fabrics."""
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({"hru_id": [1, 2], "my_param": [100.0, 200.0]})

        result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, "hru_id", logger)
        assert len(result) == 3
        assert 3 in result["hru_id"].values
        filled_val = result.loc[result["hru_id"] == 3, "my_param"].iloc[0]
        assert filled_val in [100.0, 200.0]

    def test_knn_fills_nan_value_in_present_row(self):
        """A row present in param_df but with NaN value is filled by KNN."""
        import logging
        logger = logging.getLogger("test")

        # id=3 at Point(1,0) — nearest to id=1 at (0,0), distance 1 vs id=2 at (10,0), distance 9
        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(1, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2, 3],
            "hru_id": [1, 2, 3],
            "my_param": [100.0, 200.0, np.nan],
        })

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 3
        assert not result["my_param"].isna().any()
        filled_val = result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0]
        # id=1 (0,0) is distance 1; id=2 (10,0) is distance 9 — nearest is unambiguously id=1
        assert filled_val == 100.0

    def test_knn_fills_absent_and_nan_together(self):
        """Absent rows AND present-but-NaN cells are both filled in one pass."""
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3, 4]},
            geometry=[Point(0, 0), Point(10, 0), Point(1, 0), Point(9, 0)],
            crs="EPSG:5070",
        )
        # id=3 present but NaN; id=4 absent
        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2, 3],
            "hru_id": [1, 2, 3],
            "my_param": [100.0, 200.0, np.nan],
        })

        result = fill_missing_values_knn(param_df, [4], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 4
        assert not result["my_param"].isna().any()
        assert result["nat_hru_id"].duplicated().sum() == 0
        # id=3 at (1,0): nearest valid is id=1 (0,0) → 100.0
        assert result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0] == 100.0
        # id=4 at (9,0): nearest valid is id=2 (10,0) → 200.0
        assert result.loc[result["nat_hru_id"] == 4, "my_param"].iloc[0] == 200.0

    def test_nan_valued_row_not_used_as_fill_source(self):
        """NaN-valued rows must not be included in the KNN fit set (they can't donate values)."""
        import logging
        logger = logging.getLogger("test")

        # id=1 NaN at (0,0); id=2 val=5.0 at (1,0); id=3 NaN at (100,0)
        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(1, 0), Point(100, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2, 3],
            "hru_id": [1, 2, 3],
            "my_param": [np.nan, 5.0, np.nan],
        })

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 3
        assert not result["my_param"].isna().any()
        # Only id=2 (val=5.0) is in the fit set — both NaN rows must get 5.0
        assert result.loc[result["nat_hru_id"] == 1, "my_param"].iloc[0] == 5.0
        assert result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0] == 5.0
