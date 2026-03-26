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
    """Recursively bisect features along alternating axes (KD-tree style)."""
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


def spatial_batch(
    gdf: gpd.GeoDataFrame,
    batch_size: int = 500,
) -> gpd.GeoDataFrame:
    """Assign spatially contiguous batch IDs via KD-tree recursive bisection."""
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


def write_batches(
    gdf: gpd.GeoDataFrame,
    batch_dir: Path | str,
    fabric: str,
    id_feature: str,
    batch_size: int,
    target_layer: str = "nhru",
) -> dict:
    """Write per-batch geopackages and a manifest file."""
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

    logger.info("Wrote %d batch gpkgs + manifest to %s", len(batch_ids), batch_dir)

    return manifest
