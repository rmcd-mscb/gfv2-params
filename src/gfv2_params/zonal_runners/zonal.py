"""Per-batch continuous-zonal stats from a single CONUS raster.

Used for ``elevation``, ``slope``, ``aspect``, and any other ``script: zonal``
entry in ``configs/zonal/zonal_params.yml``.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen


def run_zonal_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of continuous-zonal stats from a single raster.

    Drives the elevation/slope/aspect param types. Originally extracted from the now-retired scripts/create_zonal_params.py
    (see PR #85). Uses the gdptools NEW API
    (source_var/source_ds/source_crs/target_gdf/target_id).
    """
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    raster_path = Path(config["source_raster"])
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{batch_id:04d}.gpkg"
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")

    logger.info("Raster: %s", raster_path)
    logger.info("Batch GPKG: %s", batch_gpkg)

    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), batch_id)

    ned_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", ned_da.shape, ned_da.rio.crs)

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"

    data = UserTiffData(
        source_var=source_type,
        source_ds=ned_da,
        source_crs=ned_da.rio.crs,
        source_x_coord="x",
        source_y_coord="y",
        band=1,
        bname="band",
        target_gdf=nhru_gdf,
        target_id=id_feature,
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
