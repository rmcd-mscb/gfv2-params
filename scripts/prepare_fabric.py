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
