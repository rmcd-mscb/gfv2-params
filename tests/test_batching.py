import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import yaml
from shapely.geometry import box

from gfv2_params.batching import _recursive_bisect, spatial_batch, write_batches


class TestRecursiveBisect:
    def test_single_partition_below_min_batch_size(self):
        centroids = np.array([[0, 0], [1, 1], [2, 2]])
        indices = np.arange(3)
        result = _recursive_bisect(centroids, indices, max_depth=5, min_batch_size=10)
        assert len(result) == 1
        assert np.array_equal(result[0], indices)

    def test_splits_into_multiple_batches(self):
        centroids = np.array([[i, 0] for i in range(100)])
        indices = np.arange(100)
        result = _recursive_bisect(centroids, indices, max_depth=3, min_batch_size=1)
        assert len(result) > 1
        assert len(result) <= 8
        all_indices = np.concatenate(result)
        assert len(all_indices) == 100
        assert set(all_indices) == set(range(100))

    def test_alternates_axes(self):
        centroids = np.array([[0, 0], [0, 10], [10, 0], [10, 10]])
        indices = np.arange(4)
        result = _recursive_bisect(centroids, indices, max_depth=2, min_batch_size=1)
        assert len(result) == 4

    def test_equal_coordinates_no_empty_partition(self):
        centroids = np.array([[5, i] for i in range(10)])
        indices = np.arange(10)
        result = _recursive_bisect(centroids, indices, max_depth=3, min_batch_size=1)
        for batch in result:
            assert len(batch) > 0


class TestSpatialBatch:
    def _make_gdf(self, n=100):
        return gpd.GeoDataFrame(
            {"nat_hru_id": range(1, n + 1)},
            geometry=[box(i % 10, i // 10, i % 10 + 1, i // 10 + 1) for i in range(n)],
            crs="EPSG:5070",
        )

    def test_empty_geodataframe(self):
        gdf = gpd.GeoDataFrame({"nat_hru_id": []}, geometry=[], crs="EPSG:5070")
        result = spatial_batch(gdf, batch_size=10)
        assert "batch_id" in result.columns
        assert len(result) == 0

    def test_single_batch_when_all_fit(self):
        gdf = self._make_gdf(10)
        result = spatial_batch(gdf, batch_size=100)
        assert "batch_id" in result.columns
        assert result["batch_id"].nunique() == 1
        assert (result["batch_id"] == 0).all()

    def test_multi_batch_partitioning(self):
        gdf = self._make_gdf(200)
        result = spatial_batch(gdf, batch_size=50)
        assert "batch_id" in result.columns
        assert result["batch_id"].nunique() > 1
        assert len(result) == 200

    def test_all_features_assigned_exactly_once(self):
        gdf = self._make_gdf(500)
        result = spatial_batch(gdf, batch_size=100)
        assert len(result) == 500
        assert result["batch_id"].notna().all()

    def test_original_data_preserved(self):
        gdf = self._make_gdf(50)
        result = spatial_batch(gdf, batch_size=10)
        assert "nat_hru_id" in result.columns
        assert set(result["nat_hru_id"]) == set(range(1, 51))


class TestWriteBatches:
    def _make_batched_gdf(self, n=100, batch_size=25):
        gdf = gpd.GeoDataFrame(
            {"nat_hru_id": range(1, n + 1)},
            geometry=[box(i % 10, i // 10, i % 10 + 1, i // 10 + 1) for i in range(n)],
            crs="EPSG:5070",
        )
        return spatial_batch(gdf, batch_size=batch_size)

    def test_writes_correct_number_of_files(self, tmp_path):
        gdf = self._make_batched_gdf(100, 25)
        n_batches = gdf["batch_id"].nunique()
        manifest = write_batches(gdf, tmp_path, "gfv2", "nat_hru_id", batch_size=25)
        gpkg_files = sorted(tmp_path.glob("batch_*.gpkg"))
        assert len(gpkg_files) == n_batches
        assert manifest["n_batches"] == n_batches

    def test_manifest_content(self, tmp_path):
        gdf = self._make_batched_gdf(100, 25)
        manifest = write_batches(gdf, tmp_path, "testfabric", "nat_hru_id", batch_size=25)
        assert manifest["fabric"] == "testfabric"
        assert manifest["n_features"] == 100
        assert manifest["id_feature"] == "nat_hru_id"
        assert manifest["target_layer"] == "nhru"
        assert manifest["batch_size"] == 25
        manifest_path = tmp_path / "manifest.yml"
        assert manifest_path.exists()
        loaded = yaml.safe_load(manifest_path.read_text())
        assert loaded["fabric"] == "testfabric"

    def test_batch_gpkgs_are_readable(self, tmp_path):
        gdf = self._make_batched_gdf(50, 15)
        write_batches(gdf, tmp_path, "gfv2", "nat_hru_id", batch_size=15)
        for gpkg in tmp_path.glob("batch_*.gpkg"):
            batch_gdf = gpd.read_file(gpkg, layer="nhru")
            assert len(batch_gdf) > 0
            assert "nat_hru_id" in batch_gdf.columns

    def test_all_features_across_batches(self, tmp_path):
        gdf = self._make_batched_gdf(100, 25)
        write_batches(gdf, tmp_path, "gfv2", "nat_hru_id", batch_size=25)
        all_ids = []
        for gpkg in tmp_path.glob("batch_*.gpkg"):
            batch_gdf = gpd.read_file(gpkg, layer="nhru")
            all_ids.extend(batch_gdf["nat_hru_id"].tolist())
        assert sorted(all_ids) == list(range(1, 101))

    def test_four_digit_padding(self, tmp_path):
        gdf = self._make_batched_gdf(50, 15)
        write_batches(gdf, tmp_path, "gfv2", "nat_hru_id", batch_size=15)
        gpkg_files = sorted(tmp_path.glob("batch_*.gpkg"))
        for f in gpkg_files:
            stem = f.stem
            assert len(stem) == len("batch_0000")
