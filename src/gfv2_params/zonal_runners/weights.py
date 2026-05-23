"""CONUS-once P2P weight matrix construction for ssflux.

``run_build_weights`` is invoked once per fabric, before any ssflux array
task; the resulting weight file under ``{data_root}/shared/conus/weights/``
is consumed by ``run_ssflux_batch`` (in ``ssflux.py``).
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
from gdptools import WeightGenP2P


def run_build_weights(config: dict, logger, force: bool = False) -> None:
    """Pre-compute the CONUS-wide P2P weight matrix that ssflux consumes.

    Originally extracted from the now-retired scripts/build_weights.py
    (see PR #85). One CSV per fabric, written
    to ``config['weight_dir']/lith_weights_<fabric>.csv``. Idempotent: skips
    if the file exists unless force=True.

    The target fabric is read from ``config['hru_gpkg']``/``hru_layer`` (the
    active base_config.yml profile, threaded in via _build_param_cfg) — the
    single source of truth, not a {fabric}_nhru_merged.gpkg naming convention.
    """
    fabric = config["fabric"]
    id_feature = config["id_feature"]
    hru_gpkg = Path(config["hru_gpkg"])
    hru_layer = config.get("hru_layer", "nhru")

    weight_dir = Path(config["weight_dir"])
    weight_dir.mkdir(parents=True, exist_ok=True)
    weight_file = weight_dir / f"lith_weights_{fabric}.csv"

    if weight_file.exists() and not force:
        logger.info("Weight file already exists: %s (use --force to overwrite)", weight_file)
        return

    if not hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {hru_gpkg}")
    target_gdf = gpd.read_file(hru_gpkg, layer=hru_layer)
    logger.info("Loaded target fabric: %d features", len(target_gdf))

    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))
    logger.info("Loaded lithology: %d features", len(source_gdf))

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
