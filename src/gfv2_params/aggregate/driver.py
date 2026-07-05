"""gdptools-backed aggregation driver: weights once, AggGen per variable/year.

Ports the aggregation core of nhf-spatial-targets' aggregate/_driver.py, trimmed
to gfv2-params (no manifest/lineage/release). Weights depend only on grid∩fabric
geometry, so they are computed once (from the first year's grid) and reused for
every variable and every year.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import xarray as xr
from gdptools import AggGen, UserCatData, WeightGen

from .adapter import SourceAdapter

logger = logging.getLogger(__name__)

WEIGHT_GEN_CRS = 5070  # NAD83 / CONUS Albers (equal-area)


def _period_bounds(ds: xr.Dataset, time_coord: str) -> tuple[str, str]:
    t = pd.to_datetime(ds[time_coord].values)
    return (str(t.min().date()), str(t.max().date()))


def compute_or_load_weights(
    adapter: SourceAdapter,
    sample_ds: xr.Dataset,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    period: tuple[str, str],
    weight_file: Path,
) -> pd.DataFrame:
    """Return grid→polygon weights, computing and caching them if absent."""
    if weight_file.exists():
        logger.info("Loading cached weights: %s", weight_file)
        return pd.read_csv(weight_file)

    user_data = UserCatData(
        source_ds=sample_ds,
        source_crs=adapter.source_crs,
        source_x_coord=adapter.x_coord,
        source_y_coord=adapter.y_coord,
        source_t_coord=adapter.time_coord,
        source_var=[adapter.grid_variable],
        target_gdf=fabric_gdf,
        target_crs=WEIGHT_GEN_CRS,
        target_id=id_col,
        source_time_period=[period[0], period[1]],
    )
    wg = WeightGen(user_data=user_data, method="serial", weight_gen_crs=WEIGHT_GEN_CRS)
    weights = wg.calculate_weights()
    if weights is None or len(weights) == 0:
        raise RuntimeError(
            "WeightGen returned no weights — check grid/fabric spatial overlap."
        )
    weight_file.parent.mkdir(parents=True, exist_ok=True)
    weights.to_csv(weight_file, index=False)
    logger.info("Weights computed: %d rows -> %s", len(weights), weight_file)
    return weights


def aggregate_variables(
    adapter: SourceAdapter,
    source_ds: xr.Dataset,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    weights: pd.DataFrame,
    period: tuple[str, str],
) -> xr.Dataset:
    """Run AggGen once per declared variable; merge on the HRU id dimension."""
    per_var: list[xr.Dataset] = []
    for var in adapter.variables:
        user_data = UserCatData(
            source_ds=source_ds,
            source_crs=adapter.source_crs,
            source_x_coord=adapter.x_coord,
            source_y_coord=adapter.y_coord,
            source_t_coord=adapter.time_coord,
            source_var=[var],
            target_gdf=fabric_gdf,
            target_crs=WEIGHT_GEN_CRS,
            target_id=id_col,
            source_time_period=[period[0], period[1]],
        )
        agg = AggGen(
            user_data=user_data,
            stat_method=adapter.stat_method,
            agg_engine="serial",
            agg_writer="none",
            weights=weights,
        )
        _gdf, ds = agg.calculate_agg()
        per_var.append(ds)
    return xr.merge(per_var)


def _year_of(path: Path) -> int:
    m = re.search(r"(\d{4})", path.stem)
    if not m:
        raise ValueError(f"Cannot parse a 4-digit year from filename: {path.name}")
    return int(m.group(1))


def aggregate_source(
    adapter: SourceAdapter,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    input_dir: Path,
    output_dir: Path,
    weight_file: Path,
    output_prefix: str,
    years: list[int] | None = None,
) -> list[Path]:
    """Aggregate every per-year file matching the adapter glob to the fabric.

    Writes one NetCDF per calendar year: ``{output_prefix}_agg_{year}.nc`` with
    dims (time, <id_col>) and one data var per adapter variable.
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob(adapter.files_glob))
    if not files:
        raise FileNotFoundError(f"No files match {input_dir / adapter.files_glob}")

    written: list[Path] = []
    weights: pd.DataFrame | None = None
    for f in files:
        year = _year_of(f)
        if years is not None and year not in years:
            continue
        ds = xr.open_dataset(f)
        if adapter.pre_aggregate_hook is not None:
            ds = adapter.pre_aggregate_hook(ds)
        period = _period_bounds(ds, adapter.time_coord)
        if weights is None:
            weights = compute_or_load_weights(
                adapter, ds, fabric_gdf, id_col, period, weight_file
            )
        hru_ds = aggregate_variables(adapter, ds, fabric_gdf, id_col, weights, period)
        out = output_dir / f"{output_prefix}_agg_{year}.nc"
        hru_ds.to_netcdf(out)
        written.append(out)
        logger.info("Aggregated %s -> %s", f.name, out.name)
    return written
