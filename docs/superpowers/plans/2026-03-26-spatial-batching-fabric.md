# Spatial Batching & Fabric-Aware Pipeline Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace VPU-based chunking with KD-tree spatial batching, add fabric namespacing for multi-fabric support, and reorganize the data directory to separate inputs/intermediates/outputs.

**Architecture:** A new `batching.py` module partitions fabric polygons into spatially compact groups. A `prepare_fabric.py` script writes per-batch geopackages + manifest. Processing scripts switch from `--vpu` to `--batch_id`, reading small per-batch gpkgs. Config gains `{fabric}` placeholder to namespace all outputs. SLURM submission wrapper reads manifest for array range.

**Tech Stack:** Python 3.12, geopandas, numpy, GDAL (BuildVRT), gdptools (exactextract, WeightGenP2P), rioxarray, pyyaml, pytest

**Spec:** `docs/superpowers/specs/2026-03-26-spatial-batching-fabric-design.md`

---

## File Map

### New files
| File | Responsibility |
|---|---|
| `src/gfv2_params/batching.py` | Spatial batching: `_recursive_bisect()`, `spatial_batch()`, `write_batches()` |
| `tests/test_batching.py` | Tests for batching module |
| `scripts/prepare_fabric.py` | CLI: read fabric gpkg → spatial batch → write per-batch gpkgs + manifest |
| `scripts/build_vrt.py` | CLI: glob per-VPU GeoTIFFs → GDAL BuildVRT → CONUS-wide VRTs |
| `scripts/build_derived_rasters.py` | CLI: resample RootDepth × AWC → soil_moist_max.tif |
| `scripts/build_weights.py` | CLI: WeightGenP2P → CONUS-wide weight table for ssflux |
| `slurm_batch/submit_jobs.sh` | Shell wrapper: reads manifest → sbatch --array |

### Modified files
| File | What changes |
|---|---|
| `src/gfv2_params/config.py` | Add `{fabric}` to placeholder resolution |
| `tests/test_config.py` | Add `{fabric}` tests |
| `configs/base_config.yml` | Add `fabric: gfv2` |
| `configs/elev_param.yml` | New paths: `source_raster` → VRT, `batch_dir`, `output_dir` with `{fabric}` |
| `configs/slope_param.yml` | Same pattern |
| `configs/aspect_param.yml` | Same pattern |
| `configs/soils_param.yml` | Same pattern |
| `configs/soilmoistmax_param.yml` | `source_dir` → `source_raster` pointing at derived raster |
| `configs/ssflux_param.yml` | Add `merged_slope_file`, update paths |
| `scripts/create_zonal_params.py` | `--vpu` → `--batch_id + --base_config`, read batch gpkg |
| `scripts/create_soils_params.py` | `--vpu` → `--batch_id`, simplify soil_moist_max path |
| `scripts/create_ssflux_params.py` | `--vpu` → `--batch_id`, use pre-computed weights + merged slope |
| `scripts/merge_params.py` | Add `--base_config`, new glob pattern, validation |
| `scripts/merge_and_fill_params.py` | Remove VPU logic, fabric-aware paths |
| `scripts/merge_default_params.py` | Fabric-aware paths, `--dict` default |
| `scripts/merge_rpu_by_vpu.py` | Update output paths to `work/nhd_merged/` |
| `scripts/compute_slope_aspect.py` | Update paths to `work/nhd_merged/` |
| `scripts/find_missing_hru_ids.py` | Update paths for fabric-aware layout; functionality largely subsumed by `merge_params.py` validation |
| `slurm_batch/*.batch` | `--batch_id` instead of VPU array, `--base_config`, lower memory |
| `slurm_batch/RUNME.md` | Complete rewrite for new pipeline stages |
| `README.md` | Update structure, directory layout, usage examples |
| `tests/test_merge_and_fill_params.py` | Update for new interface (remove VPU assumptions) |

---

### Task 1: Config system — add `{fabric}` placeholder

**Files:**
- Modify: `src/gfv2_params/config.py`
- Modify: `configs/base_config.yml`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write failing test for `{fabric}` placeholder resolution**

```python
# Add to tests/test_config.py

def test_load_config_resolves_fabric_placeholder():
    """Config with {fabric} placeholder should resolve from base config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "fabric": "gfv2",
            "expected_max_hru_id": 100,
        }))

        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_type": "elevation",
            "source_raster": "{data_root}/work/nhd_merged/elevation.vrt",
            "batch_dir": "{data_root}/{fabric}/batches",
            "output_dir": "{data_root}/{fabric}/params",
            "target_layer": "nhru",
            "id_feature": "nat_hru_id",
            "categorical": False,
        }))

        config = load_config(step_config, base_config_path=base_config)
        assert config["batch_dir"] == "/fake/root/gfv2/batches"
        assert config["output_dir"] == "/fake/root/gfv2/params"
        assert config["fabric"] == "gfv2"


def test_load_config_fabric_with_vpu():
    """Both {fabric} and {vpu} should resolve when vpu is provided."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "fabric": "gfv2",
            "expected_max_hru_id": 100,
        }))

        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "target_gpkg": "{data_root}/input/fabrics/NHM_{vpu}_draft.gpkg",
            "output_dir": "{data_root}/{fabric}/work",
        }))

        config = load_config(step_config, vpu="03N", base_config_path=base_config)
        assert config["target_gpkg"] == "/fake/root/input/fabrics/NHM_03N_draft.gpkg"
        assert config["output_dir"] == "/fake/root/gfv2/work"


def test_load_config_without_fabric_still_works():
    """Existing configs without {fabric} placeholder should still work."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_config = Path(tmpdir) / "base_config.yml"
        base_config.write_text(yaml.dump({
            "data_root": "/fake/root",
            "expected_max_hru_id": 100,
        }))

        step_config = Path(tmpdir) / "step.yml"
        step_config.write_text(yaml.dump({
            "source_raster": "{data_root}/rasters/dem.tif",
        }))

        config = load_config(step_config, base_config_path=base_config)
        assert config["source_raster"] == "/fake/root/rasters/dem.tif"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/miniforge3/bin/python -m pytest tests/test_config.py::test_load_config_resolves_fabric_placeholder tests/test_config.py::test_load_config_fabric_with_vpu tests/test_config.py::test_load_config_without_fabric_still_works -v`
Expected: FAIL — `{fabric}` is not resolved, so `Unresolved placeholder` error or literal `{fabric}` in path

- [ ] **Step 3: Update `config.py` to resolve `{fabric}` placeholder**

In `src/gfv2_params/config.py`, modify `load_config()` to read `fabric` from base config and add it to the replacement map:

```python
def load_config(
    step_config_path: Path,
    vpu: str | None = None,
    base_config_path: Path | None = None,
) -> dict:
    if base_config_path is None:
        base_config_path = _DEFAULT_BASE_CONFIG

    base = _load_yaml(base_config_path)
    step = _load_yaml(step_config_path)

    data_root = base["data_root"]

    # Build replacement map
    replacements = {"data_root": data_root}

    # Add fabric if present in base config
    fabric = base.get("fabric")
    if fabric is not None:
        replacements["fabric"] = fabric

    if vpu is not None:
        raster_vpu, gpkg_vpu = resolve_vpu(vpu)
        replacements["vpu"] = gpkg_vpu
        replacements["raster_vpu"] = raster_vpu

    # Resolve placeholders in step config
    resolved_step = _resolve_placeholders(step, replacements)

    # Merge: base config provides defaults, step config overrides
    merged = {**base, **resolved_step}

    # Add vpu to config if provided
    if vpu is not None:
        merged["vpu"] = vpu

    return merged
```

Also update the error message in `_resolve_placeholders()` to mention `fabric`:

```python
# In _resolve_placeholders, change the error message:
                raise ValueError(
                    f"Unresolved placeholder(s) {remaining} in config key '{key}'. "
                    f"Value: '{value}'. Check --vpu or fabric in base_config."
                )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/miniforge3/bin/python -m pytest tests/test_config.py -v`
Expected: ALL PASS (including existing tests)

- [ ] **Step 5: Update `base_config.yml`**

```yaml
data_root: /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param
fabric: gfv2
expected_max_hru_id: 361471
```

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/config.py tests/test_config.py configs/base_config.yml
git commit -m "feat: add {fabric} placeholder to config system"
```

---

### Task 2: Batching module — `spatial_batch()` and `write_batches()`

**Files:**
- Create: `src/gfv2_params/batching.py`
- Create: `tests/test_batching.py`

- [ ] **Step 1: Write failing tests for `_recursive_bisect`**

```python
# tests/test_batching.py
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
        # max_depth=3 → up to 2^3=8 batches
        assert len(result) > 1
        assert len(result) <= 8
        # All indices covered exactly once
        all_indices = np.concatenate(result)
        assert len(all_indices) == 100
        assert set(all_indices) == set(range(100))

    def test_alternates_axes(self):
        # Grid of points — depth 0 splits on x, depth 1 splits on y
        centroids = np.array([[0, 0], [0, 10], [10, 0], [10, 10]])
        indices = np.arange(4)
        result = _recursive_bisect(centroids, indices, max_depth=2, min_batch_size=1)
        assert len(result) == 4

    def test_equal_coordinates_no_empty_partition(self):
        # All same x — should not produce empty partitions
        centroids = np.array([[5, i] for i in range(10)])
        indices = np.arange(10)
        result = _recursive_bisect(centroids, indices, max_depth=3, min_batch_size=1)
        for batch in result:
            assert len(batch) > 0
```

- [ ] **Step 2: Run tests to verify they fail (module not found)**

Run: `~/miniforge3/bin/python -m pytest tests/test_batching.py::TestRecursiveBisect -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gfv2_params.batching'`

- [ ] **Step 3: Implement `_recursive_bisect`**

```python
# src/gfv2_params/batching.py
"""Spatial batching: assign features to spatially contiguous groups.

Group polygon features into spatially contiguous batches using KD-tree
recursive bisection. Each batch's bounding box is compact, which is
critical for efficient spatial subsetting of source rasters.

Ported from hydro-param (https://github.com/rmcd-mscb/hydro-param).
"""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import yaml

logger = logging.getLogger(__name__)


def _recursive_bisect(
    centroids: np.ndarray,
    indices: np.ndarray,
    depth: int = 0,
    max_depth: int = 7,
    min_batch_size: int = 50,
) -> list[np.ndarray]:
    """Recursively bisect features along alternating axes (KD-tree style).

    Parameters
    ----------
    centroids : np.ndarray
        Shape (N, 2) array of centroid coordinates (x, y).
    indices : np.ndarray
        1-D integer array of indices into the original GeoDataFrame.
    depth : int
        Current recursion depth (0 = x-axis, 1 = y-axis, alternating).
    max_depth : int
        Maximum recursion depth. Produces up to 2^max_depth batches.
    min_batch_size : int
        Stop splitting if a partition has fewer features than this.

    Returns
    -------
    list[np.ndarray]
        List of 1-D integer arrays, one per batch.
    """
    if depth >= max_depth or len(indices) <= min_batch_size:
        return [indices]

    axis = depth % 2
    coords = centroids[indices, axis]
    median = np.median(coords)
    left_mask = coords <= median
    right_mask = ~left_mask

    if not left_mask.any() or not right_mask.any():
        return [indices]

    left = _recursive_bisect(centroids, indices[left_mask], depth + 1, max_depth, min_batch_size)
    right = _recursive_bisect(centroids, indices[right_mask], depth + 1, max_depth, min_batch_size)
    return left + right
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/miniforge3/bin/python -m pytest tests/test_batching.py::TestRecursiveBisect -v`
Expected: ALL PASS

- [ ] **Step 5: Write failing tests for `spatial_batch`**

Add to `tests/test_batching.py`:

```python
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
        # All features assigned
        assert len(result) == 200

    def test_all_features_assigned_exactly_once(self):
        gdf = self._make_gdf(500)
        result = spatial_batch(gdf, batch_size=100)
        assert len(result) == 500
        # batch_id is set for every row
        assert result["batch_id"].notna().all()

    def test_original_data_preserved(self):
        gdf = self._make_gdf(50)
        result = spatial_batch(gdf, batch_size=10)
        assert "nat_hru_id" in result.columns
        assert set(result["nat_hru_id"]) == set(range(1, 51))
```

- [ ] **Step 6: Implement `spatial_batch`**

Add to `src/gfv2_params/batching.py`:

```python
def spatial_batch(
    gdf: gpd.GeoDataFrame,
    batch_size: int = 500,
) -> gpd.GeoDataFrame:
    """Assign spatially contiguous batch IDs via KD-tree recursive bisection.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Target fabric with polygon geometries.
    batch_size : int
        Target number of features per batch. Default is 500.

    Returns
    -------
    gpd.GeoDataFrame
        Copy of input with a ``batch_id`` column (int, 0-indexed).
    """
    if gdf.empty:
        result = gdf.copy()
        result["batch_id"] = np.array([], dtype=int)
        return result

    if len(gdf) <= batch_size:
        result = gdf.copy()
        result["batch_id"] = 0
        logger.info(
            "Spatial batching: %d features -> 1 batch (all fit in batch_size=%d)",
            len(gdf), batch_size,
        )
        return result

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*geographic CRS.*centroid.*")
        centroids = np.column_stack(
            [gdf.geometry.centroid.x.values, gdf.geometry.centroid.y.values]
        )

    n_batches = max(1, len(gdf) // batch_size)
    max_depth = max(1, int(np.ceil(np.log2(n_batches))))

    batches = _recursive_bisect(
        centroids,
        np.arange(len(gdf)),
        max_depth=max_depth,
        min_batch_size=max(1, batch_size // 2),
    )

    batch_ids = np.empty(len(gdf), dtype=int)
    for batch_id, indices in enumerate(batches):
        batch_ids[indices] = batch_id

    result = gdf.copy()
    result["batch_id"] = batch_ids

    logger.info(
        "Spatial batching: %d features -> %d batches (target size=%d, actual range=%d-%d)",
        len(gdf), len(batches), batch_size,
        min(len(b) for b in batches), max(len(b) for b in batches),
    )

    return result
```

- [ ] **Step 7: Run spatial_batch tests**

Run: `~/miniforge3/bin/python -m pytest tests/test_batching.py::TestSpatialBatch -v`
Expected: ALL PASS

- [ ] **Step 8: Write failing tests for `write_batches`**

Add to `tests/test_batching.py`:

```python
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
        # Also check the YAML file was written
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
        write_batches(gdf, tmp_path, "gfv2", "nat_hru_id", batch_size=15)
        all_ids = []
        for gpkg in tmp_path.glob("batch_*.gpkg"):
            batch_gdf = gpd.read_file(gpkg, layer="nhru")
            all_ids.extend(batch_gdf["nat_hru_id"].tolist())
        assert sorted(all_ids) == list(range(1, 101))

    def test_four_digit_padding(self, tmp_path):
        gdf = self._make_batched_gdf(50, 15)
        write_batches(gdf, tmp_path, "gfv2", "nat_hru_id", batch_size=15)
        gpkg_files = sorted(tmp_path.glob("batch_*.gpkg"))
        # All filenames should be batch_NNNN.gpkg
        for f in gpkg_files:
            stem = f.stem  # e.g. "batch_0000"
            assert len(stem) == len("batch_0000")
```

- [ ] **Step 9: Implement `write_batches`**

Add to `src/gfv2_params/batching.py`:

```python
def write_batches(
    gdf: gpd.GeoDataFrame,
    batch_dir: Path | str,
    fabric: str,
    id_feature: str,
    batch_size: int,
    target_layer: str = "nhru",
) -> dict:
    """Write per-batch geopackages and a manifest file.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame with a ``batch_id`` column (from ``spatial_batch``).
    batch_dir : Path or str
        Directory to write batch files into. Created if it doesn't exist.
    fabric : str
        Fabric name (e.g., "gfv2", "oregon"). Recorded in manifest.
    id_feature : str
        Name of the feature ID column (e.g., "nat_hru_id").
    target_layer : str
        Layer name to use in the output geopackages. Default "nhru".

    Returns
    -------
    dict
        Manifest dictionary (also written to ``batch_dir/manifest.yml``).
    """
    batch_dir = Path(batch_dir)
    batch_dir.mkdir(parents=True, exist_ok=True)

    batch_ids = sorted(gdf["batch_id"].unique())

    for bid in batch_ids:
        batch_gdf = gdf[gdf["batch_id"] == bid].drop(columns=["batch_id"])
        out_path = batch_dir / f"batch_{bid:04d}.gpkg"
        batch_gdf.to_file(out_path, layer=target_layer, driver="GPKG")

    manifest = {
        "fabric": fabric,
        "batch_size": batch_size,
        "n_batches": len(batch_ids),
        "n_features": len(gdf),
        "id_feature": id_feature,
        "target_layer": target_layer,
        "created": datetime.now(timezone.utc).isoformat(),
    }

    manifest_path = batch_dir / "manifest.yml"
    with open(manifest_path, "w") as f:
        yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)

    logger.info(
        "Wrote %d batch gpkgs + manifest to %s", len(batch_ids), batch_dir,
    )

    return manifest
```

- [ ] **Step 10: Run all batching tests**

Run: `~/miniforge3/bin/python -m pytest tests/test_batching.py -v`
Expected: ALL PASS

- [ ] **Step 11: Commit**

```bash
git add src/gfv2_params/batching.py tests/test_batching.py
git commit -m "feat: add spatial batching module with write_batches"
```

---

### Task 3: `prepare_fabric.py` script

**Files:**
- Create: `scripts/prepare_fabric.py`

- [ ] **Step 1: Write the script**

```python
# scripts/prepare_fabric.py
"""Prepare a watershed fabric for batch processing.

Reads a merged fabric geopackage, partitions it into spatially compact
batches using KD-tree recursive bisection, and writes per-batch
geopackages plus a manifest file.
"""

import argparse
from pathlib import Path

import geopandas as gpd

from gfv2_params.batching import spatial_batch, write_batches
from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Prepare fabric for batch processing.")
    parser.add_argument("--fabric_gpkg", required=True, help="Path to merged fabric geopackage")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_size", type=int, default=500, help="Target features per batch (default 500)")
    parser.add_argument("--layer", default="nhru", help="Layer name in the geopackage (default nhru)")
    args = parser.parse_args()

    logger = configure_logging("prepare_fabric")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = base["data_root"]
    fabric = base["fabric"]

    fabric_gpkg = Path(args.fabric_gpkg)
    if not fabric_gpkg.exists():
        raise FileNotFoundError(f"Fabric geopackage not found: {fabric_gpkg}")

    logger.info("Reading fabric: %s (layer=%s)", fabric_gpkg, args.layer)
    gdf = gpd.read_file(fabric_gpkg, layer=args.layer)
    logger.info("Loaded %d features", len(gdf))

    batched = spatial_batch(gdf, batch_size=args.batch_size)

    batch_dir = Path(data_root) / fabric / "batches"
    id_feature = base.get("id_feature", "nat_hru_id")
    manifest = write_batches(batched, batch_dir, fabric, id_feature, batch_size=args.batch_size, target_layer=args.layer)

    n = manifest["n_batches"]
    logger.info("Fabric '%s' prepared: %d features -> %d batches in %s", fabric, len(gdf), n, batch_dir)
    logger.info("Use: ./submit_jobs.sh %s <batch_script.batch>", batch_dir)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify syntax**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/prepare_fabric.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add scripts/prepare_fabric.py
git commit -m "feat: add prepare_fabric.py script"
```

---

### Task 4: `build_vrt.py` script

**Files:**
- Create: `scripts/build_vrt.py`

- [ ] **Step 1: Write the script**

```python
# scripts/build_vrt.py
"""Build CONUS-wide VRT files from per-VPU merged GeoTIFFs.

Creates GDAL virtual rasters that reference per-VPU source files,
allowing them to be read as a single CONUS-wide raster without
duplicating data on disk.
"""

import argparse
from pathlib import Path

from osgeo import gdal

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

# Mapping from VRT name to the GeoTIFF filename pattern within each VPU directory.
# Each entry: vrt_name -> glob pattern matching the source raster in work/nhd_merged/<VPU>/
RASTER_TYPES = {
    "elevation": "NEDSnapshot_merged_fixed_*.tif",
    "slope": "NEDSnapshot_merged_slope_*.tif",
    "aspect": "NEDSnapshot_merged_aspect_*.tif",
}


def main():
    parser = argparse.ArgumentParser(description="Build CONUS-wide VRTs from per-VPU rasters.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    args = parser.parse_args()

    logger = configure_logging("build_vrt")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = Path(base["data_root"])
    nhd_merged_dir = data_root / "work" / "nhd_merged"

    if not nhd_merged_dir.exists():
        raise FileNotFoundError(f"NHD merged directory not found: {nhd_merged_dir}")

    for vrt_name, pattern in RASTER_TYPES.items():
        source_files = sorted(nhd_merged_dir.glob(f"*/{pattern}"))
        if not source_files:
            logger.warning("No source files found for %s (pattern: */%s)", vrt_name, pattern)
            continue

        vrt_path = nhd_merged_dir / f"{vrt_name}.vrt"
        logger.info("Building %s from %d source files", vrt_path, len(source_files))

        vrt_options = gdal.BuildVRTOptions(resolution="highest")
        vrt_ds = gdal.BuildVRT(str(vrt_path), [str(f) for f in source_files], options=vrt_options)
        if vrt_ds is None:
            raise RuntimeError(f"gdal.BuildVRT failed for {vrt_name}")
        vrt_ds.FlushCache()
        del vrt_ds

        logger.info("Written: %s (%d sources)", vrt_path, len(source_files))

    logger.info("VRT build complete")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify syntax**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/build_vrt.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add scripts/build_vrt.py
git commit -m "feat: add build_vrt.py for CONUS-wide virtual rasters"
```

---

### Task 5: `build_derived_rasters.py` script

**Files:**
- Create: `scripts/build_derived_rasters.py`

- [ ] **Step 1: Write the script**

This script extracts the raster derivation logic from `create_soils_params.py`'s `process_soil_moist_max()` function into a standalone pre-processing step.

```python
# scripts/build_derived_rasters.py
"""Pre-compute derived rasters (soil_moist_max) from source inputs.

Eliminates race conditions when multiple SLURM batch jobs would
otherwise try to create the same derived rasters simultaneously.
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import mult_rasters, resample


def main():
    parser = argparse.ArgumentParser(description="Build derived rasters from source inputs.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    logger = configure_logging("build_derived_rasters")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = Path(base["data_root"])

    # Source inputs
    rd_rast = data_root / "input" / "lulc_veg" / "RootDepth.tif"
    awc_rast = data_root / "input" / "soils_litho" / "AWC.tif"

    # Derived outputs
    derived_dir = data_root / "work" / "derived_rasters"
    derived_dir.mkdir(parents=True, exist_ok=True)
    intermediate_rast = derived_dir / "rd_250_intermediate.tif"
    rd_resampled = derived_dir / "rd_250_raw.tif"
    soil_moist_max_rast = derived_dir / "soil_moist_max.tif"

    if not rd_rast.exists():
        raise FileNotFoundError(f"RootDepth raster not found: {rd_rast}")
    if not awc_rast.exists():
        raise FileNotFoundError(f"AWC raster not found: {awc_rast}")

    # Step 1: Resample RootDepth to match AWC grid
    if not rd_resampled.exists() or args.force:
        logger.info("Resampling RootDepth to AWC grid...")
        resample(str(rd_rast), str(awc_rast), str(intermediate_rast), str(rd_resampled))
        logger.info("Written: %s", rd_resampled)
    else:
        logger.info("Resampled RootDepth already exists: %s", rd_resampled)

    # Step 2: Multiply resampled RootDepth × AWC → soil_moist_max
    if not soil_moist_max_rast.exists() or args.force:
        logger.info("Computing soil_moist_max = RootDepth * AWC...")
        mult_rasters(str(rd_resampled), str(awc_rast), str(soil_moist_max_rast))
        logger.info("Written: %s", soil_moist_max_rast)
    else:
        logger.info("soil_moist_max raster already exists: %s", soil_moist_max_rast)

    logger.info("Derived rasters complete")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify syntax**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/build_derived_rasters.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add scripts/build_derived_rasters.py
git commit -m "feat: add build_derived_rasters.py for soil_moist_max pre-computation"
```

---

### Task 6: Update per-step config files

**Files:**
- Modify: `configs/elev_param.yml`
- Modify: `configs/slope_param.yml`
- Modify: `configs/aspect_param.yml`
- Modify: `configs/soils_param.yml`
- Modify: `configs/soilmoistmax_param.yml`
- Modify: `configs/ssflux_param.yml`

- [ ] **Step 1: Update all per-step configs to use `{fabric}` and new paths**

`configs/elev_param.yml`:
```yaml
source_type: elevation
source_raster: "{data_root}/work/nhd_merged/elevation.vrt"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_elevation_params.csv
categorical: false
```

`configs/slope_param.yml`:
```yaml
source_type: slope
source_raster: "{data_root}/work/nhd_merged/slope.vrt"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_slope_params.csv
categorical: false
```

`configs/aspect_param.yml`:
```yaml
source_type: aspect
source_raster: "{data_root}/work/nhd_merged/aspect.vrt"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_aspect_params.csv
categorical: false
```

`configs/soils_param.yml`:
```yaml
source_type: soils
source_raster: "{data_root}/input/soils_litho/TEXT_PRMS.tif"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_soils_params.csv
categorical: true
```

`configs/soilmoistmax_param.yml`:
```yaml
source_type: soil_moist_max
source_raster: "{data_root}/work/derived_rasters/soil_moist_max.tif"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
merged_file: nhm_soil_moist_max_params.csv
categorical: false
```

`configs/ssflux_param.yml`:
```yaml
source_type: ssflux
source_shapefile: "{data_root}/input/soils_litho/Lithology_exp_Konly_Project.shp"
batch_dir: "{data_root}/{fabric}/batches"
target_layer: nhru
id_feature: nat_hru_id
output_dir: "{data_root}/{fabric}/params"
weight_dir: "{data_root}/work/weights"
merged_slope_file: "{data_root}/{fabric}/params/merged/nhm_slope_params.csv"
merged_file: nhm_ssflux_params.csv
categorical: false

k_perm_min: -16.48

flux_params:
  - name: soil2gw_max
    min: 0.1
    max: 0.3
  - name: ssr2gw_rate
    min: 0.3
    max: 0.7
  - name: fastcoef_lin
    min: 0.01
    max: 0.6
  - name: slowcoef_lin
    min: 0.005
    max: 0.3
  - name: gwflow_coef
    min: 0.005
    max: 0.3
  - name: dprst_seep_rate_open
    min: 0.005
    max: 0.2
  - name: dprst_flow_coef
    min: 0.005
    max: 0.5
```

- [ ] **Step 2: Commit**

```bash
git add configs/elev_param.yml configs/slope_param.yml configs/aspect_param.yml \
    configs/soils_param.yml configs/soilmoistmax_param.yml configs/ssflux_param.yml
git commit -m "feat: update per-step configs with {fabric} paths and batch_dir"
```

---

### Task 7: Modify `create_zonal_params.py` — VPU to batch

**Files:**
- Modify: `scripts/create_zonal_params.py`

- [ ] **Step 1: Rewrite script to use `--batch_id` and `--base_config`**

```python
# scripts/create_zonal_params.py
"""Create zonal parameters (elevation, slope, aspect) from rasters by HRU polygon."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Create zonal parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID (from SLURM_ARRAY_TASK_ID)")
    args = parser.parse_args()

    logger = configure_logging("create_zonal_params")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    # Resolve paths
    raster_path = Path(config["source_raster"])
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{args.batch_id:04d}.gpkg"
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")

    logger.info("Raster: %s", raster_path)
    logger.info("Batch GPKG: %s", batch_gpkg)

    # Load batch polygons
    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), args.batch_id)

    # Load raster
    ned_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", ned_da.shape, ned_da.rio.crs)

    # Build file prefix for output
    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{args.batch_id:04d}_param"

    # Create zonal stats
    data = UserTiffData(
        var=source_type,
        ds=ned_da,
        proj_ds=ned_da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics complete. Shape: %s", stats.shape)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify syntax**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/create_zonal_params.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add scripts/create_zonal_params.py
git commit -m "feat: convert create_zonal_params.py from --vpu to --batch_id"
```

---

### Task 8: Modify `create_soils_params.py` — VPU to batch

**Files:**
- Modify: `scripts/create_soils_params.py`

- [ ] **Step 1: Rewrite script**

Key changes:
- `--vpu` → `--batch_id` + `--base_config`
- Reads batch gpkg from `batch_dir`
- `process_soil_moist_max()` simplified: reads pre-built `source_raster` directly (no more `source_dir`, no resample/multiply at runtime)
- Output naming uses `{fabric}_batch_{batch_id:04d}`

```python
# scripts/create_soils_params.py
"""Create soils and soil_moist_max parameters from raster data."""

import argparse
from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def process_soils(source_da, nhru_gdf, output_path, source_type, file_prefix, categorical, id_feature, logger):
    """Process categorical soils data: zonal stats -> dominant category -> CSV."""
    data = UserTiffData(
        var="soils", ds=source_da, proj_ds=source_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    zonal_gen = ZonalGen(
        user_data=data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_path, file_prefix=f"{file_prefix}_temp", jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed")

    # Remove temp file
    zg_file = output_path / f"{file_prefix}_temp.csv"
    if zg_file.exists():
        zg_file.unlink()

    # Dominant category per feature
    category_cols = [col for col in stats.columns if str(col) not in ("count",)]
    top_stats = stats.copy()
    top_stats["max_category"] = top_stats[category_cols].idxmax(axis=1)
    result = top_stats[["max_category"]].rename(columns={"max_category": "soils"})
    result.sort_index(inplace=True)

    result_csv = output_path / f"{file_prefix}.csv"
    result.to_csv(result_csv)
    logger.info("Soils parameters saved to: %s", result_csv)


def process_soil_moist_max(source_da, nhru_gdf, output_path, source_type, file_prefix, categorical, id_feature, logger):
    """Process soil_moist_max: zonal mean from pre-built raster."""
    data = UserTiffData(
        var=source_type, ds=source_da, proj_ds=source_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    zonal_gen = ZonalGen(
        user_data=data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_path, file_prefix=f"{file_prefix}_temp", jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed for soil_moist_max")

    mean_stats = stats[["mean"]].rename(columns={"mean": "soil_moist_max"})
    result_csv = output_path / f"{file_prefix}.csv"
    mean_stats.to_csv(result_csv)
    logger.info("soil_moist_max parameters saved to: %s", result_csv)


def main():
    parser = argparse.ArgumentParser(description="Create soils parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID")
    args = parser.parse_args()

    logger = configure_logging("create_soils_params")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load batch polygons
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{args.batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), args.batch_id)

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{args.batch_id:04d}_param"

    # Load source raster (works for both soils and soil_moist_max)
    raster_path = Path(config["source_raster"])
    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    source_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    if source_type == "soils":
        process_soils(source_da, nhru_gdf, output_dir, source_type, file_prefix, categorical, id_feature, logger)
    elif source_type == "soil_moist_max":
        process_soil_moist_max(source_da, nhru_gdf, output_dir, source_type, file_prefix, categorical, id_feature, logger)
    else:
        raise ValueError(f"Unknown source_type: {source_type}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify syntax**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/create_soils_params.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add scripts/create_soils_params.py
git commit -m "feat: convert create_soils_params.py to batch-based processing"
```

---

### Task 9: `build_weights.py` + modify `create_ssflux_params.py`

**Files:**
- Create: `scripts/build_weights.py`
- Modify: `scripts/create_ssflux_params.py`

- [ ] **Step 1: Write `build_weights.py`**

Extracts the weight computation from `create_ssflux_params.py` into a standalone pre-processing step that computes CONUS-wide weights once.

```python
# scripts/build_weights.py
"""Pre-compute CONUS-wide polygon-to-polygon weights for ssflux.

Runs WeightGenP2P between the full merged fabric and the lithology
shapefile. Writes a single weight table that batch jobs can subset.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from gdptools import WeightGenP2P

from gfv2_params.config import load_base_config, load_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Pre-compute P2P weights for ssflux.")
    parser.add_argument("--config", required=True, help="Path to ssflux config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing weight file")
    args = parser.parse_args()

    logger = configure_logging("build_weights")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = Path(base["data_root"])
    fabric = base["fabric"]
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]

    weight_dir = Path(config["weight_dir"])
    weight_dir.mkdir(parents=True, exist_ok=True)
    weight_file = weight_dir / f"lith_weights_{fabric}.csv"

    if weight_file.exists() and not args.force:
        logger.info("Weight file already exists: %s (use --force to overwrite)", weight_file)
        return

    # Load full merged fabric
    fabric_gpkg = data_root / fabric / "fabric" / f"{fabric}_nhru_merged.gpkg"
    if not fabric_gpkg.exists():
        raise FileNotFoundError(f"Merged fabric not found: {fabric_gpkg}")
    target_gdf = gpd.read_file(fabric_gpkg, layer=target_layer)
    logger.info("Loaded target fabric: %d features", len(target_gdf))

    # Load source lithology
    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))
    logger.info("Loaded lithology: %d features", len(source_gdf))

    # Compute weights
    logger.info("Computing P2P weights (this may take a while)...")
    weight_gen = WeightGenP2P(
        target_poly=target_gdf,
        target_poly_idx=id_feature,
        source_poly=source_gdf,
        source_poly_idx="flux_id",
        method="serial",
        weight_gen_crs="5070",
        output_file=weight_file,
    )
    weights = weight_gen.calculate_weights()
    logger.info("Weights computed: %d rows -> %s", len(weights), weight_file)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Rewrite `create_ssflux_params.py` to use pre-computed weights + merged slope**

```python
# scripts/create_ssflux_params.py
"""Create subsurface flux parameters using litho-weighted approach.

Requires pre-computed CONUS-wide weights (from build_weights.py)
and merged slope parameters (from merge_params.py).
"""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import deg_to_fraction


def main():
    parser = argparse.ArgumentParser(description="Create ssflux parameters.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID")
    args = parser.parse_args()

    logger = configure_logging("create_ssflux_params")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    output_dir = Path(config["output_dir"])
    weight_dir = Path(config["weight_dir"])
    fabric = config["fabric"]

    # Load batch polygons
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{args.batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    target_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    batch_ids = set(target_gdf[id_feature].values)
    logger.info("Loaded %d features (batch %d)", len(target_gdf), args.batch_id)

    # Load pre-computed CONUS weights and filter to this batch
    weight_file = weight_dir / f"lith_weights_{fabric}.csv"
    if not weight_file.exists():
        raise FileNotFoundError(
            f"Weight file not found: {weight_file}\n"
            "Run scripts/build_weights.py first."
        )
    all_weights = pd.read_csv(weight_file)
    weights = all_weights[all_weights[id_feature].isin(batch_ids)].copy()
    logger.info("Loaded weights: %d rows (from %d total)", len(weights), len(all_weights))

    # Load merged slope CSV
    merged_slope_file = Path(config["merged_slope_file"])
    if not merged_slope_file.exists():
        raise FileNotFoundError(
            f"Merged slope file not found: {merged_slope_file}\n"
            "Run merge_params.py for slope first."
        )
    all_slope = pd.read_csv(merged_slope_file)
    slope_df = all_slope[all_slope[id_feature].isin(batch_ids)].copy()
    slope_df["mean_slope_fraction"] = slope_df["mean"].astype(float).apply(deg_to_fraction)
    logger.info("Loaded slope for %d features", len(slope_df))

    # Load source lithology for k_perm lookup
    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))

    # Merge weights with source attributes
    weights["flux_id"] = weights["flux_id"].astype(str)
    source_gdf["flux_id"] = source_gdf["flux_id"].astype(str)

    w = weights.merge(source_gdf[["flux_id", "k_perm"]], on="flux_id")

    k_perm_min = config["k_perm_min"]
    w["k_perm"] = w["k_perm"].replace(0, k_perm_min)
    w["k_perm_actual"] = 10 ** w["k_perm"]

    w["k_perm_wtd_sum"] = w["k_perm_actual"] * (w["area_weight"] / w["flux_id_area"])

    extensive_agg = (
        w.groupby(id_feature)
        .agg(k_perm_wtd=("k_perm_wtd_sum", "sum"))
        .reset_index()
    )
    extensive_agg[id_feature] = extensive_agg[id_feature].astype(int)
    extensive_sorted = extensive_agg.sort_values(by=id_feature).reset_index(drop=True)

    # Merge with slope and area
    slope_merge = slope_df[[id_feature, "mean_slope_fraction"]].copy()
    slope_merge[id_feature] = pd.to_numeric(slope_merge[id_feature], errors="coerce").astype("int64")

    target_gdf["hru_area"] = target_gdf.geometry.area
    area_df = target_gdf[[id_feature, "hru_area"]].copy()
    area_df[id_feature] = pd.to_numeric(area_df[id_feature], errors="coerce").astype("int64")

    df = extensive_sorted.merge(slope_merge, on=id_feature, how="left").copy()
    df = df.merge(area_df, on=id_feature, how="left")

    # Compute raw PRMS fluxes
    df["r_soil2gw_max"] = df["k_perm_wtd"] ** 3
    df["r_ssr2gw_rate"] = df["k_perm_wtd"] * (1 - df["mean_slope_fraction"])
    df["r_slowcoef_lin"] = (df["k_perm_wtd"] * df["mean_slope_fraction"]) / df["hru_area"]
    df["r_fastcoef_lin"] = 2 * df["r_slowcoef_lin"]
    df["r_gwflow_coef"] = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"] = df["r_fastcoef_lin"]

    # Normalize using config-driven bounds
    flux_params = config["flux_params"]
    param_names = [fp["name"] for fp in flux_params]
    param_maxes = [fp["max"] for fp in flux_params]
    param_mins = [fp["min"] for fp in flux_params]

    df_r = df[[f"r_{p}" for p in param_names]].agg(["min", "max"])
    df_r.loc["range"] = df_r.loc["max"] - df_r.loc["min"]

    for i, p in enumerate(param_names):
        rcol = f"r_{p}"
        min_in, rng_in = df_r.at["min", rcol], df_r.at["range", rcol]
        min_out, max_out = param_mins[i], param_maxes[i]
        rng_out = max_out - min_out
        if rng_in == 0:
            logger.warning("Range is zero for %s; using midpoint of output range", p)
            df[p] = (min_out + max_out) / 2.0
        else:
            norm = (df[rcol] - min_in) / rng_in
            df[p] = norm * rng_out + min_out

    df.drop(columns=[f"r_{p}" for p in param_names], inplace=True)

    ssflux_dir = output_dir / "ssflux"
    ssflux_dir.mkdir(parents=True, exist_ok=True)
    file_prefix = f"base_nhm_ssflux_{fabric}_batch_{args.batch_id:04d}_param"
    df.to_csv(ssflux_dir / f"{file_prefix}.csv", index=False)
    logger.info("SSFlux parameters saved (batch %d)", args.batch_id)


if __name__ == "__main__":
    main()
```

**Note on normalization**: The flux parameter normalization (min/max scaling) operates per-batch, just as the current code operates per-VPU. This means the same raw permeability value may map to slightly different normalized values in different batches. This matches the existing behavior and is acceptable for the current pipeline. If CONUS-wide normalization is needed in the future, a two-pass approach (compute CONUS-wide min/max first, then normalize per batch) can be added.

- [ ] **Step 3: Verify syntax for both scripts**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/build_weights.py').read()); ast.parse(open('scripts/create_ssflux_params.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 4: Commit**

```bash
git add scripts/build_weights.py scripts/create_ssflux_params.py
git commit -m "feat: add build_weights.py, convert create_ssflux_params.py to batch-based"
```

---

### Task 10: Modify `merge_params.py` — batch-aware with validation

**Files:**
- Modify: `scripts/merge_params.py`

- [ ] **Step 1: Rewrite script**

```python
# scripts/merge_params.py
"""Merge per-batch parameter CSVs into a single file, sorted by id_feature.

Validates completeness: raises on duplicate IDs, warns on gaps.
"""

import argparse
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def process_files(config, logger):
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    merged_file = config["merged_file"]
    fabric = config["fabric"]
    expected_max = config.get("expected_max_hru_id")

    input_dir = Path(config["output_dir"]) / source_type
    final_output_dir = Path(config["output_dir"]) / "merged"
    final_output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    file_pattern = f"base_nhm_{source_type}_{fabric}_batch_*_param.csv"
    files = sorted(input_dir.glob(file_pattern))

    if not files:
        raise FileNotFoundError(f"No batch files found matching: {input_dir / file_pattern}")

    logger.info("Found %d batch files for %s", len(files), source_type)

    dfs = []
    for file in files:
        logger.debug("Reading: %s", file)
        df = pd.read_csv(file)
        if id_feature not in df.columns:
            raise ValueError(f"'{id_feature}' column not found in file: {file}")
        dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df = merged_df.sort_values(id_feature).reset_index(drop=True)

    # Validate: check for duplicates
    dupes = merged_df[merged_df[id_feature].duplicated(keep=False)]
    if len(dupes) > 0:
        dupe_ids = sorted(dupes[id_feature].unique())
        raise ValueError(
            f"Duplicate {id_feature} values found ({len(dupe_ids)} IDs). "
            f"First 10: {dupe_ids[:10]}. This indicates overlapping batches."
        )

    # Validate: check for gaps (optional, based on expected_max_hru_id)
    if expected_max is not None:
        existing_ids = set(merged_df[id_feature])
        expected_ids = set(range(1, expected_max + 1))
        gaps = sorted(expected_ids - existing_ids)
        if gaps:
            logger.warning(
                "%d missing %s values (expected 1-%d, got %d). First 10: %s",
                len(gaps), id_feature, expected_max, len(existing_ids), gaps[:10],
            )

    output_path = final_output_dir / merged_file
    merged_df.to_csv(output_path, index=False)
    logger.info("Merged %d rows -> %s", len(merged_df), output_path)


def main():
    parser = argparse.ArgumentParser(description="Merge per-batch parameter CSVs.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    args = parser.parse_args()

    logger = configure_logging("merge_params")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    process_files(config, logger)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify syntax**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/merge_params.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add scripts/merge_params.py
git commit -m "feat: convert merge_params.py to batch-based with validation"
```

---

### Task 11: Modify `merge_and_fill_params.py` — fabric-aware

**Files:**
- Modify: `scripts/merge_and_fill_params.py`
- Modify: `tests/test_merge_and_fill_params.py`

- [ ] **Step 1: Rewrite script — remove VPU logic, use fabric paths**

Key changes:
- Remove `--targets_dir`, `--force_rebuild`, `--simplify_tolerance`
- Remove `VPUS_DETAILED` import and `merge_vpu_geopackages()` function
- Add `--base_config`; derive all default paths from `data_root` + `fabric`
- `--merged_gpkg` defaults to `{fabric}/fabric/{fabric}_nhru_merged.gpkg`
- `--param_file` defaults to `{fabric}/params/merged/nhm_ssflux_params.csv`
- `--output_dir` defaults to `{fabric}/params/merged/`
- Remove `vpu` column references

```python
# scripts/merge_and_fill_params.py
"""Fill missing parameter values using KNN interpolation against a merged nhru geopackage.

The merged geopackage is produced by the notebooks/merge_vpu_targets.py notebook
or the prepare_fabric.py script.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def find_missing_ids(param_file, expected_max, logger):
    logger.info("Finding missing nat_hru_id values...")
    param_df = pd.read_csv(param_file)
    existing_ids = set(param_df["nat_hru_id"])
    expected_ids = set(range(1, expected_max + 1))
    missing_ids = sorted(expected_ids - existing_ids)
    logger.info("Found %d missing nat_hru_id values out of %d", len(missing_ids), expected_max)
    return param_df, missing_ids


def fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_column, k, logger):
    logger.info("Filling missing values using KNN interpolation (k=%d)...", k)

    if not missing_ids:
        logger.info("No missing values to fill!")
        return param_df

    merged_gdf["centroid"] = merged_gdf["geometry"].centroid
    merged_gdf["x"] = merged_gdf["centroid"].x
    merged_gdf["y"] = merged_gdf["centroid"].y

    existing_df = param_df.merge(merged_gdf[["nat_hru_id", "x", "y"]], on="nat_hru_id", how="left")
    missing_df = merged_gdf[merged_gdf["nat_hru_id"].isin(missing_ids)][["nat_hru_id", "x", "y"]]

    existing_coords = existing_df[["x", "y"]].values
    missing_coords = missing_df[["x", "y"]].values
    existing_values = existing_df[param_column].values

    knn = NearestNeighbors(n_neighbors=k)
    knn.fit(existing_coords)
    distances, indices = knn.kneighbors(missing_coords)

    interpolated_values = []
    for neighbor_indices in tqdm(indices, desc="Filling missing HRUs"):
        neighbor_values = existing_values[neighbor_indices]
        interpolated_values.append(np.mean(neighbor_values))

    missing_filled = pd.DataFrame({
        "nat_hru_id": missing_df["nat_hru_id"].values,
        param_column: interpolated_values,
    })
    missing_filled["hru_id"] = missing_filled["nat_hru_id"]

    complete_df = pd.concat([param_df, missing_filled], ignore_index=True)
    complete_df = complete_df.sort_values("nat_hru_id").reset_index(drop=True)
    logger.info("Filled %d missing values", len(missing_ids))
    return complete_df


def main():
    parser = argparse.ArgumentParser(description="Fill missing parameter values using KNN interpolation.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--merged_gpkg", default=None, help="Path to merged nhru geopackage")
    parser.add_argument("--param_file", default=None, help="Path to merged parameter CSV to fill")
    parser.add_argument("--output_dir", default=None, help="Output directory for filled file")
    parser.add_argument("--k_neighbors", type=int, default=1)
    args = parser.parse_args()

    logger = configure_logging("merge_and_fill_params")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = base["data_root"]
    fabric = base["fabric"]
    expected_max = base["expected_max_hru_id"]

    # Resolve defaults from fabric namespace
    if args.merged_gpkg is None:
        args.merged_gpkg = f"{data_root}/{fabric}/fabric/{fabric}_nhru_merged.gpkg"
    if args.param_file is None:
        args.param_file = f"{data_root}/{fabric}/params/merged/nhm_ssflux_params.csv"
    if args.output_dir is None:
        args.output_dir = f"{data_root}/{fabric}/params/merged"

    merged_gpkg = Path(args.merged_gpkg)
    param_file = Path(args.param_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not merged_gpkg.exists():
        raise FileNotFoundError(
            f"Merged geopackage not found: {merged_gpkg}\n"
            "Run notebooks/merge_vpu_targets.py or scripts/prepare_fabric.py first."
        )

    logger.info("Loading merged geopackage: %s", merged_gpkg)
    try:
        merged_gdf = gpd.read_file(merged_gpkg, layer="nhru")
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read merged geopackage: {merged_gpkg}\n"
            "The file may be corrupt."
        ) from exc
    logger.info("Loaded %d features", len(merged_gdf))

    filled_param_file = output_dir / f"filled_{param_file.name}"

    param_df, missing_ids = find_missing_ids(param_file, expected_max, logger)

    if missing_ids:
        param_columns = [col for col in param_df.columns if col not in ["hru_id", "nat_hru_id"]]
        if not param_columns:
            raise ValueError("No parameter columns found in the data")

        logger.info("Filling parameter columns: %s", param_columns)

        complete_df = param_df
        for param_column in param_columns:
            logger.info("Filling parameter column: %s", param_column)
            complete_df = fill_missing_values_knn(complete_df, missing_ids, merged_gdf, param_column, args.k_neighbors, logger)
        complete_df.to_csv(filled_param_file, index=False)
        logger.info("Filled parameter file saved to: %s", filled_param_file)

        final_ids = set(complete_df["nat_hru_id"])
        expected_ids = set(range(1, expected_max + 1))
        still_missing = expected_ids - final_ids

        if still_missing:
            logger.warning("%d IDs are still missing", len(still_missing))
        else:
            logger.info("All missing values have been filled successfully!")
    else:
        logger.info("No missing values found in the parameter file")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Rewrite `tests/test_merge_and_fill_params.py`**

The test file uses `importlib` to load the script. Several classes/imports must change:
- **Remove** the `merge_vpu_geopackages` import (function deleted) — this will cause ALL tests to fail at import time if not removed
- **Remove** `TestMergeVpuGeopackages` class entirely (function removed)
- **Remove** `TestThreeWayBranch` class (three-way `force_rebuild` logic removed)
- **Update** `TestFileNotFoundBehavior` — error message no longer mentions `--force_rebuild`; now mentions `prepare_fabric.py`
- **Update** `TestFillMissingValuesKnn` — `merged_gdf` fixture no longer needs `vpu` column, result no longer has `vpu`

Replace the import block at the top:

```python
_spec = importlib.util.spec_from_file_location(
    "merge_and_fill_params",
    Path(__file__).resolve().parent.parent / "scripts" / "merge_and_fill_params.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

# Only import functions that still exist
find_missing_ids = _mod.find_missing_ids
fill_missing_values_knn = _mod.fill_missing_values_knn
```

Replace `TestFileNotFoundBehavior`:

```python
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
```

Replace `TestFillMissingValuesKnn`:

```python
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
```

Remove `TestMergeVpuGeopackages` and `TestThreeWayBranch` classes entirely.

- [ ] **Step 3: Run updated tests**

Run: `~/miniforge3/bin/python -m pytest tests/test_merge_and_fill_params.py -v`
Expected: ALL PASS (after removing vpu-dependent tests and updating fixtures)

- [ ] **Step 4: Commit**

```bash
git add scripts/merge_and_fill_params.py tests/test_merge_and_fill_params.py
git commit -m "feat: convert merge_and_fill_params.py to fabric-aware, remove VPU logic"
```

---

### Task 12: Modify `merge_default_params.py` — fabric-aware paths

**Files:**
- Modify: `scripts/merge_default_params.py`

- [ ] **Step 1: Update script with new default paths**

Changes:
- `--dict` defaults to `{data_root}/input/nhm_defaults/param_dict.csv`
- `--base_dir` defaults to `{data_root}/input/nhm_defaults/`
- `--output_dir` defaults to `{data_root}/{fabric}/params/defaults_merged/`
- Add `--base_config` argument

The internal logic (VPU-based subdirectory scanning, `$id` → `hru_id` renaming, cumulative offset) stays the same — this script operates on NHM default files that are inherently per-VPU.

Replace the `main()` function's default path logic:

```python
def main():
    parser = argparse.ArgumentParser(description="Merge default parameter files by nat_hru_id.")
    parser.add_argument("--dict", default=None, help="Path to parameter dictionary CSV file")
    parser.add_argument("--base_dir", default=None, help="Base directory containing parameter files")
    parser.add_argument("--output_dir", default=None, help="Output directory for merged files")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    args = parser.parse_args()

    logger = configure_logging("merge_default_params")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = base["data_root"]
    fabric = base["fabric"]

    if args.dict is None:
        args.dict = f"{data_root}/input/nhm_defaults/param_dict.csv"
    if args.base_dir is None:
        args.base_dir = f"{data_root}/input/nhm_defaults"
    if args.output_dir is None:
        args.output_dir = f"{data_root}/{fabric}/params/defaults_merged"

    # ... rest unchanged
```

- [ ] **Step 2: Verify syntax**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/merge_default_params.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add scripts/merge_default_params.py
git commit -m "feat: update merge_default_params.py with fabric-aware paths"
```

---

### Task 13: Update VPU-based scripts and utilities for new directory layout

**Files:**
- Modify: `scripts/merge_rpu_by_vpu.py`
- Modify: `scripts/compute_slope_aspect.py`
- Modify: `scripts/find_missing_hru_ids.py`
- Modify: `configs/merge_rpu_by_vpu.yml`
- Modify: `configs/slope_aspect.yml`

These scripts stay VPU-based but need path updates:
- Output moves from `source_data/NHDPlus_*` to `work/nhd_merged/`, `work/nhd_extracted/`
- Input fabrics move from `targets/` to `input/fabrics/`
- `find_missing_hru_ids.py`: update default paths to use `{fabric}/params/merged/` instead of `nhm_params/nhm_params_merged/`. Note: its core functionality (gap detection) is now also in `merge_params.py`, but keep the script as a standalone diagnostic tool.

- [ ] **Step 1: Read current scripts and configs to understand exact path changes needed**

Read `scripts/merge_rpu_by_vpu.py`, `scripts/compute_slope_aspect.py`, `configs/merge_rpu_by_vpu.yml`, `configs/slope_aspect.yml` and update paths to reference `work/` and `input/` directories.

- [ ] **Step 2: Update configs and scripts**

Update `configs/merge_rpu_by_vpu.yml` paths to use `work/nhd_extracted/` and `work/nhd_merged/`.
Update `configs/slope_aspect.yml` paths to use `work/nhd_merged/`.
Update any hardcoded paths in the scripts.

- [ ] **Step 3: Verify syntax for both scripts**

Run: `~/miniforge3/bin/python -c "import ast; ast.parse(open('scripts/merge_rpu_by_vpu.py').read()); ast.parse(open('scripts/compute_slope_aspect.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 4: Commit**

```bash
git add scripts/merge_rpu_by_vpu.py scripts/compute_slope_aspect.py \
    configs/merge_rpu_by_vpu.yml configs/slope_aspect.yml
git commit -m "feat: update VPU-based scripts for work/ directory layout"
```

---

### Task 14: Update download scripts for new directory layout

**Files:**
- Modify: `src/gfv2_params/download/rpu_rasters.py`
- Modify: `src/gfv2_params/download/mrlc_impervious.py`

- [ ] **Step 1: Update download paths**

These scripts write to directories that now live under `input/` or `work/`:
- `rpu_rasters.py`: downloads go to `input/nhd_downloads/`, extracts to `work/nhd_extracted/`
- `mrlc_impervious.py`: downloads go to `input/mrlc_impervious/`

Read the scripts and update the path construction to match the new layout.

- [ ] **Step 2: Commit**

```bash
git add src/gfv2_params/download/rpu_rasters.py src/gfv2_params/download/mrlc_impervious.py
git commit -m "feat: update download scripts for input/work directory layout"
```

---

### Task 15: SLURM batch files and submission wrapper

**Files:**
- Create: `slurm_batch/submit_jobs.sh`
- Modify: all `slurm_batch/*.batch` files (parameter generation ones)

- [ ] **Step 1: Create `submit_jobs.sh`**

```bash
#!/bin/bash
# Usage: ./submit_jobs.sh /path/to/{fabric}/batches <batch_script.batch>
#
# Reads the batch count from manifest.yml and submits the batch script
# as a SLURM array job with the appropriate range.

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <batches_dir> <batch_script>"
    echo "  batches_dir: path to {fabric}/batches/ (contains manifest.yml)"
    echo "  batch_script: SLURM batch file to submit"
    exit 1
fi

FABRIC_DIR="$1"
BATCH_SCRIPT="$2"
MANIFEST="$FABRIC_DIR/manifest.yml"

if [ ! -f "$MANIFEST" ]; then
    echo "Error: manifest not found: $MANIFEST"
    echo "Run scripts/prepare_fabric.py first."
    exit 1
fi

N_BATCHES=$(grep '^n_batches:' "$MANIFEST" | awk '{print $2}')
LAST_IDX=$((N_BATCHES - 1))

echo "Submitting $BATCH_SCRIPT with --array=0-$LAST_IDX ($N_BATCHES batches)"
sbatch --array=0-"$LAST_IDX" "$BATCH_SCRIPT"
```

- [ ] **Step 2: Update parameter generation batch files**

For each batch file that currently uses VPU arrays, replace with `--batch_id $SLURM_ARRAY_TASK_ID` and `--base_config`. Remove the hardcoded VPU array and `--array` directive (array supplied by `submit_jobs.sh`). Reduce memory from 256G to 32G.

Example for `create_zonal_elev_params.batch`:
```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=elev_zonal
#SBATCH --output=logs/job_%A_%a.out
#SBATCH --error=logs/job_%A_%a.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G

module load miniforge/latest
conda activate geoenv

python /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/gfv2-params/scripts/create_zonal_params.py \
    --config /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/gfv2-params/configs/elev_param.yml \
    --base_config /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/gfv2-params/configs/base_config.yml \
    --batch_id $SLURM_ARRAY_TASK_ID
```

Apply same pattern to: `create_zonal_slope_params.batch`, `create_zonal_aspect_params.batch`, `create_soils_params.batch`, `create_soilmoistmax_params.batch`, `create_ssflux_params.batch`.

VPU-based batch files (`merge_rpu_by_vpu.batch`, `compute_slope_aspect.batch`) keep their hardcoded VPU arrays.

Update `merge_output_params.batch` to use the new merge_params.py interface.

- [ ] **Step 3: Make submit_jobs.sh executable**

```bash
chmod +x slurm_batch/submit_jobs.sh
```

- [ ] **Step 4: Commit**

```bash
git add slurm_batch/
git commit -m "feat: update SLURM batch files for batch-based processing"
```

---

### Task 16: Update documentation — RUNME.md and README.md

**Files:**
- Modify: `slurm_batch/RUNME.md`
- Modify: `README.md`

- [ ] **Step 1: Rewrite `slurm_batch/RUNME.md`**

Complete rewrite following the pipeline stages from the spec. Include:
- Prerequisites (conda env, pip install, data staging into `input/`)
- Stage 1: Raster prep (VPU-based, existing)
- Stage 2a: Build VRTs
- Stage 2b: Build derived rasters
- Stage 3: Prepare fabric (with custom fabric example)
- Stage 4: Generate parameters (with `submit_jobs.sh`)
- Stage 5: Merge & validate
- Stage 6: SSFlux (build_weights + batch jobs + merge)
- Stage 7: KNN gap-fill
- Stage 8: Merge defaults
- Partial reruns and troubleshooting
- Custom fabric workflow (Oregon example end-to-end)

- [ ] **Step 2: Update `README.md`**

Update:
- Project structure section (new scripts)
- Output directory structure (input/work/{fabric}/ layout)
- Usage section (new CLI examples with --batch_id)
- Custom fabric workflow

- [ ] **Step 3: Commit**

```bash
git add slurm_batch/RUNME.md README.md
git commit -m "docs: rewrite RUNME.md and README.md for spatial batching pipeline"
```

---

### Task 17: Run full test suite and verify

**Files:**
- All test files

- [ ] **Step 1: Run full test suite**

Run: `~/miniforge3/bin/python -m pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 2: Verify all scripts parse**

Run: `for f in scripts/*.py; do ~/miniforge3/bin/python -c "import ast; ast.parse(open('$f').read())"; done && echo "ALL OK"`
Expected: ALL OK

- [ ] **Step 3: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: address test failures from integration"
```
