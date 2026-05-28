"""Merge the per-VPU `nsegment` layers into one CONUS stream-segments GeoPackage.

The depstor `streambuffer` step needs a single stream-network layer covering the
whole fabric, but a VPU-based fabric like ``gfv2`` only ships per-VPU draft
geopackages (``input/fabric/NHM_<vpu>_draft.gpkg``). The fabric-merge notebook
(``notebooks/merge_vpu_targets.py``) merges only the ``nhru`` layer, so the
segments were never assembled to CONUS. This script does the equivalent merge for
``nsegment``: it concatenates every VPU's segment lines into
``{data_root}/{fabric}/fabric/{fabric}_nsegment_merged.gpkg`` (layer ``nsegment``)
and stamps each row with its ``source_vpu``.

``streambuffer`` only buffers the geometry (it ignores every attribute), so no id
reconciliation is needed — a plain geometry concat with a common CRS is enough.

Usage:
  pixi run --as-is python scripts/merge_vpu_segments.py --fabric gfv2
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd

from gfv2_params.config import VPUS_DETAILED, load_base_config
from gfv2_params.log import configure_logging


def concat_segments(gdfs: list[gpd.GeoDataFrame], target_crs=None) -> gpd.GeoDataFrame:
    """Reproject to a common CRS, drop null/empty geometries, and concatenate.

    Pure (no I/O) so the merge contract is unit-testable without staged data.
    ``target_crs`` defaults to the CRS of the first frame that carries one.
    """
    if not gdfs:
        raise ValueError("No segment layers to merge.")
    if target_crs is None:
        target_crs = next((g.crs for g in gdfs if g.crs is not None), None)

    cleaned = []
    for gdf in gdfs:
        gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
        if target_crs is not None and gdf.crs is not None and gdf.crs != target_crs:
            gdf = gdf.to_crs(target_crs)
        cleaned.append(gdf)

    merged = gpd.GeoDataFrame(pd.concat(cleaned, ignore_index=True), crs=target_crs)
    return merged


def _read_layer(path: Path, layer: str, logger) -> gpd.GeoDataFrame:
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable; falling back to fiona for %s", path.name)
        return gpd.read_file(path, layer=layer)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fabric", required=True, help="Active fabric name (profile in base_config.yml).")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml (default: packaged configs/base_config.yml).")
    parser.add_argument("--input-dir", default=None, help="Dir holding NHM_<vpu>_draft.gpkg (default: {data_root}/input/fabric).")
    parser.add_argument("--layer", default="nsegment", help="Source/output layer name (default: nsegment).")
    parser.add_argument("--output", default=None, help="Output gpkg (default: {data_root}/{fabric}/fabric/{fabric}_nsegment_merged.gpkg).")
    parser.add_argument("--force", action="store_true", help="Overwrite the output gpkg if it exists.")
    args = parser.parse_args()

    logger = configure_logging("merge_vpu_segments")
    base_config = Path(args.base_config) if args.base_config else None
    config = load_base_config(base_config, fabric=args.fabric)
    data_root = Path(config["data_root"])
    fabric = config["fabric"]

    input_dir = Path(args.input_dir) if args.input_dir else data_root / "input" / "fabric"
    output = Path(args.output) if args.output else data_root / fabric / "fabric" / f"{fabric}_nsegment_merged.gpkg"

    logger.info("--- merge_vpu_segments ---")
    logger.info("  Fabric : %s", fabric)
    logger.info("  Input  : %s/NHM_<vpu>_draft.gpkg (layer=%s)", input_dir, args.layer)
    logger.info("  Output : %s", output)

    if output.exists() and not args.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return 0

    gdfs: list[gpd.GeoDataFrame] = []
    skipped: list[str] = []
    for vpu in VPUS_DETAILED:
        path = input_dir / f"NHM_{vpu}_draft.gpkg"
        if not path.exists():
            logger.warning("  VPU %-4s: MISSING — %s", vpu, path)
            skipped.append(vpu)
            continue
        gdf = _read_layer(path, args.layer, logger)
        gdf["source_vpu"] = path.stem
        logger.info("  VPU %-4s: %6d segments", vpu, len(gdf))
        gdfs.append(gdf)

    if not gdfs:
        raise RuntimeError(f"No per-VPU '{args.layer}' layers loaded from {input_dir}")
    if skipped:
        logger.warning("  %d VPU(s) skipped (missing): %s", len(skipped), skipped)

    merged = concat_segments(gdfs)
    logger.info("  Total: %d segments | CRS: %s", len(merged), merged.crs)

    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(".tmp.gpkg")
    if tmp.exists():
        tmp.unlink()
    merged.to_file(tmp, layer=args.layer, driver="GPKG")
    os.replace(tmp, output)
    logger.info("  Wrote %d segments → %s (layer=%s)", len(merged), output, args.layer)
    logger.info("  Point the fabric profile's segments_gpkg at this file (segments_layer: %s).", args.layer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
