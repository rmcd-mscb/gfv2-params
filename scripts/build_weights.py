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
    if weights is None or len(weights) == 0:
        raise RuntimeError(
            "WeightGenP2P returned no weights. Check that target and source "
            "polygons overlap spatially."
        )
    if not weight_file.exists():
        raise RuntimeError(f"WeightGenP2P did not write output file: {weight_file}")
    logger.info("Weights computed: %d rows -> %s", len(weights), weight_file)


if __name__ == "__main__":
    main()
