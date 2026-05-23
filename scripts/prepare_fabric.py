"""Prepare a watershed fabric for batch processing.

Reads a merged fabric geopackage, partitions it into spatially compact
batches using KD-tree recursive bisection, and writes per-batch
geopackages plus a manifest file.
"""

import argparse
from pathlib import Path

import geopandas as gpd

from gfv2_params.batching import spatial_batch, write_batches
from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Prepare fabric for batch processing.")
    parser.add_argument("--fabric_gpkg", default=None, help="Path to fabric geopackage (default: active profile's hru_gpkg)")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--batch_size", type=int, default=None, help="Target features per batch (overrides base_config.yml)")
    parser.add_argument("--layer", default=None, help="Layer name in the geopackage (default: active profile's hru_layer)")
    args = parser.parse_args()

    logger = configure_logging("prepare_fabric")

    base = load_base_config(
        Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    batch_size = args.batch_size if args.batch_size is not None else base.get("batch_size", 500)
    data_root = base["data_root"]
    fabric = base["fabric"]

    # Fabric gpkg + layer come from the active profile's hru_gpkg/hru_layer in
    # base_config.yml; --fabric_gpkg/--layer are optional overrides. This is the
    # single source of truth — no {fabric}_nhru_merged.gpkg naming convention.
    if args.fabric_gpkg:
        fabric_gpkg = Path(args.fabric_gpkg)
    else:
        fabric_gpkg = Path(require_config_key(base, "hru_gpkg", "prepare_fabric"))
    layer = args.layer if args.layer is not None else base.get("hru_layer", "nhru")

    if not fabric_gpkg.exists():
        raise FileNotFoundError(f"Fabric geopackage not found: {fabric_gpkg}")

    logger.info("Reading fabric: %s (layer=%s)", fabric_gpkg, layer)
    gdf = gpd.read_file(fabric_gpkg, layer=layer)
    logger.info("Loaded %d features", len(gdf))

    batched = spatial_batch(gdf, batch_size=batch_size)

    batch_dir = Path(data_root) / fabric / "batches"
    id_feature = require_config_key(base, "id_feature", "prepare_fabric")
    manifest = write_batches(batched, batch_dir, fabric, id_feature, batch_size=batch_size, target_layer=layer)

    n = manifest["n_batches"]
    logger.info("Fabric '%s' prepared: %d features -> %d batches in %s", fabric, len(gdf), n, batch_dir)
    logger.info("Next: submit Part-2 jobs. For the chained per-param workflow (recommended):")
    logger.info("  ./slurm_batch/submit_zonal_params.sh   %s %s configs/base_config.yml", batch_dir, fabric)
    logger.info("  ./slurm_batch/submit_depstor_params.sh %s %s configs/base_config.yml", batch_dir, fabric)
    logger.info("For a single batch script: ./slurm_batch/submit_jobs.sh %s <batch_script.batch>", batch_dir)
    logger.info("Per-parameter (incremental) recipes are in slurm_batch/RUNME.md Stage 4A.")


if __name__ == "__main__":
    main()
