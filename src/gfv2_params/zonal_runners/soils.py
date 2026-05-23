"""Per-batch zonal stats for soils (categorical) and soil_moist_max (continuous).

The ``run_soils_batch`` dispatcher branches on ``source_type`` into one of two
private helpers (``_process_soils`` for the categorical histogram-then-argmax
path, ``_process_soil_moist_max`` for the continuous mean). Both helpers share
gdptools' ``UserTiffData`` / ``ZonalGen`` setup but diverge on post-processing.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen


def run_soils_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of soils (categorical) or soil_moist_max (continuous).

    Originally extracted from the now-retired scripts/create_soils_params.py
    (see PR #85). Uses the gdptools OLD API (var/ds/proj_ds/f_feature/id_feature) — preserved verbatim because
    that's what the per-batch helper functions below also use.
    """
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), batch_id)

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"

    raster_path = Path(config["source_raster"])
    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    source_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    if source_type == "soils":
        _process_soils(source_da, nhru_gdf, output_dir, file_prefix, categorical, id_feature, logger)
    elif source_type == "soil_moist_max":
        _process_soil_moist_max(source_da, nhru_gdf, output_dir, source_type, file_prefix, categorical, id_feature, logger)
    else:
        raise ValueError(f"Unknown source_type for soils dispatch: {source_type}")


def _process_soils(source_da, nhru_gdf, output_path, file_prefix, categorical, id_feature, logger):
    """Categorical soils: zonal histogram -> dominant category -> CSV."""
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

    zg_file = output_path / f"{file_prefix}_temp.csv"
    if zg_file.exists():
        zg_file.unlink()

    category_cols = [col for col in stats.columns if str(col) not in ("count",)]
    top_stats = stats.copy()
    top_stats["max_category"] = top_stats[category_cols].idxmax(axis=1)
    result = top_stats[["max_category"]].rename(columns={"max_category": "soils"})
    result.sort_index(inplace=True)

    result_csv = output_path / f"{file_prefix}.csv"
    result.to_csv(result_csv)
    logger.info("Soils parameters saved to: %s", result_csv)


def _process_soil_moist_max(source_da, nhru_gdf, output_path, source_type, file_prefix, categorical, id_feature, logger):
    """Continuous soil_moist_max: zonal mean from the pre-built raster."""
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
