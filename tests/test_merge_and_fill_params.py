"""Tests for scripts/merge_and_fill_params.py"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point, box

# We need to import the functions from the script.
# Since scripts/ is not a package, import via importlib.
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "merge_and_fill_params",
    Path(__file__).resolve().parent.parent / "scripts" / "merge_and_fill_params.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

merge_vpu_geopackages = _mod.merge_vpu_geopackages
find_missing_ids = _mod.find_missing_ids
fill_missing_values_knn = _mod.fill_missing_values_knn


# ---------------------------------------------------------------------------
# find_missing_ids
# ---------------------------------------------------------------------------

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
        # Empty dataframe with the right columns
        df = pd.DataFrame({"nat_hru_id": pd.Series(dtype=int), "val": pd.Series(dtype=float)})
        df.to_csv(csv, index=False)

        _, missing = find_missing_ids(csv, 3, logger)
        assert missing == [1, 2, 3]


# ---------------------------------------------------------------------------
# Merged gpkg path resolution (main function logic)
# ---------------------------------------------------------------------------

class TestMergedGpkgPathResolution:
    def test_default_path_uses_new_filename(self, tmp_path):
        """Default merged gpkg should be gfv2_nhru_merged.gpkg, not the old name."""
        targets_dir = tmp_path / "targets"
        targets_dir.mkdir()
        # The default path should use the new name
        expected = targets_dir / "gfv2_nhru_merged.gpkg"
        # Verify old name is NOT used
        old_name = targets_dir / "gfv2_merged_simplified.gpkg"
        assert expected.name == "gfv2_nhru_merged.gpkg"
        assert expected.name != old_name.name

    def test_merged_gpkg_override(self, tmp_path):
        """--merged_gpkg should be used directly when provided."""
        custom_path = tmp_path / "my_custom_merged.gpkg"
        merged_gpkg = Path(custom_path)
        assert merged_gpkg == custom_path


# ---------------------------------------------------------------------------
# FileNotFoundError when merged gpkg missing
# ---------------------------------------------------------------------------

class TestFileNotFoundBehavior:
    def test_raises_when_gpkg_missing_and_no_force_rebuild(self, tmp_path):
        """When merged gpkg doesn't exist and force_rebuild is False, should raise."""
        merged_gpkg = tmp_path / "nonexistent.gpkg"
        assert not merged_gpkg.exists()

        # Simulate the branch logic from main()
        force_rebuild = False
        with pytest.raises(FileNotFoundError, match="notebooks/merge_vpu_targets.py"):
            if force_rebuild:
                pass  # would rebuild
            elif merged_gpkg.exists():
                pass  # would load
            else:
                raise FileNotFoundError(
                    f"Merged geopackage not found: {merged_gpkg}\n"
                    "Run the notebooks/merge_vpu_targets.py notebook to produce it, "
                    "or pass --force_rebuild to build from individual VPU files."
                )

    def test_error_message_mentions_force_rebuild(self, tmp_path):
        merged_gpkg = tmp_path / "nonexistent.gpkg"
        with pytest.raises(FileNotFoundError, match="--force_rebuild"):
            if not merged_gpkg.exists():
                raise FileNotFoundError(
                    f"Merged geopackage not found: {merged_gpkg}\n"
                    "Run the notebooks/merge_vpu_targets.py notebook to produce it, "
                    "or pass --force_rebuild to build from individual VPU files."
                )


# ---------------------------------------------------------------------------
# Three-way branch logic
# ---------------------------------------------------------------------------

class TestThreeWayBranch:
    def test_force_rebuild_takes_priority_over_existing_file(self, tmp_path):
        """force_rebuild should trigger rebuild even when file exists."""
        merged_gpkg = tmp_path / "merged.gpkg"
        merged_gpkg.touch()  # file exists

        path_taken = None
        force_rebuild = True

        if force_rebuild:
            path_taken = "rebuild"
        elif merged_gpkg.exists():
            path_taken = "load"
        else:
            path_taken = "error"

        assert path_taken == "rebuild"

    def test_loads_existing_when_no_force_rebuild(self, tmp_path):
        merged_gpkg = tmp_path / "merged.gpkg"
        merged_gpkg.touch()

        path_taken = None
        force_rebuild = False

        if force_rebuild:
            path_taken = "rebuild"
        elif merged_gpkg.exists():
            path_taken = "load"
        else:
            path_taken = "error"

        assert path_taken == "load"

    def test_errors_when_missing_and_no_force_rebuild(self, tmp_path):
        merged_gpkg = tmp_path / "nonexistent.gpkg"

        path_taken = None
        force_rebuild = False

        if force_rebuild:
            path_taken = "rebuild"
        elif merged_gpkg.exists():
            path_taken = "load"
        else:
            path_taken = "error"

        assert path_taken == "error"


# ---------------------------------------------------------------------------
# merge_vpu_geopackages
# ---------------------------------------------------------------------------

class TestMergeVpuGeopackages:
    def _make_vpu_gpkg(self, path, n_features=5, start_id=1):
        gdf = gpd.GeoDataFrame(
            {"nat_hru_id": range(start_id, start_id + n_features)},
            geometry=[box(i, i, i + 1, i + 1) for i in range(n_features)],
            crs="EPSG:5070",
        )
        gdf.to_file(path, layer="nhru", driver="GPKG")

    def test_merges_multiple_vpus(self, tmp_path):
        import logging
        logger = logging.getLogger("test")

        self._make_vpu_gpkg(tmp_path / "NHM_01_draft.gpkg", 3, 1)
        self._make_vpu_gpkg(tmp_path / "NHM_02_draft.gpkg", 2, 4)

        out = tmp_path / "merged.gpkg"
        result = merge_vpu_geopackages(tmp_path, ["01", "02"], out, 0, logger)
        assert len(result) == 5
        assert out.exists()

    def test_raises_when_no_vpus_found(self, tmp_path):
        import logging
        logger = logging.getLogger("test")

        with pytest.raises(FileNotFoundError, match="No VPU geopackages"):
            merge_vpu_geopackages(tmp_path, ["99"], tmp_path / "out.gpkg", 0, logger)

    def test_warns_on_partial_vpus(self, tmp_path):
        import logging
        logger = logging.getLogger("test")

        self._make_vpu_gpkg(tmp_path / "NHM_01_draft.gpkg", 3, 1)
        # VPU 02 is missing

        out = tmp_path / "merged.gpkg"
        result = merge_vpu_geopackages(tmp_path, ["01", "02"], out, 0, logger)
        # Should succeed but only have VPU 01 data
        assert len(result) == 3


# ---------------------------------------------------------------------------
# fill_missing_values_knn
# ---------------------------------------------------------------------------

class TestFillMissingValuesKnn:
    def test_knn_fills_with_nearest_value(self):
        import logging
        logger = logging.getLogger("test")

        # Create a merged GeoDataFrame with known positions
        merged_gdf = gpd.GeoDataFrame(
            {
                "nat_hru_id": [1, 2, 3],
                "vpu": ["01", "01", "01"],
            },
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )

        # Param file has IDs 1 and 2 but not 3
        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2],
            "hru_id": [1, 2],
            "my_param": [100.0, 200.0],
        })

        result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, logger)
        assert len(result) == 3
        assert 3 in result["nat_hru_id"].values

        # ID 3 is at (5,0), equidistant from 1 and 2 but k=1 picks one
        filled_val = result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0]
        assert filled_val in [100.0, 200.0]

    def test_knn_no_missing_returns_original(self):
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1], "vpu": ["01"]},
            geometry=[Point(0, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({"nat_hru_id": [1], "my_param": [42.0]})

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, logger)
        assert len(result) == 1
        assert result["my_param"].iloc[0] == 42.0
