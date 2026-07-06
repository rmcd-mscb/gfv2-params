"""gdptools-backed aggregation driver: weights once, one multi-variable AggGen pass per year.

Ports the aggregation core of nhf-spatial-targets' aggregate/_driver.py, trimmed
to gfv2-params (no manifest/lineage/release). Weights depend only on grid∩fabric
geometry, so they are computed once (from the first year's grid) and reused for
every year; all adapter variables are aggregated in a single AggGen call.
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
    logger.info(
        "Computing grid→polygon weights for %d HRUs (once; cached thereafter)...",
        len(fabric_gdf),
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
    """Aggregate all declared variables to the fabric in a single AggGen pass.

    gdptools ``UserCatData``/``AggGen`` accept a *list* of source variables and
    ``calculate_agg`` returns a Dataset with all of them, so we build one
    ``UserCatData`` + one ``AggGen`` for the whole variable set. This replaces a
    per-variable loop that built a separate ``UserCatData`` (re-running grid
    checks, the spatial-subset dict, and weight prep) and re-subset/re-loaded
    the source once per variable — wasteful, especially when one variable is
    derived from another (e.g. SNODAS ``scov`` from ``swe``). One pass also
    removes the ``xr.merge`` of per-variable results.
    """
    logger.info("    aggregating %s (%s)...", list(adapter.variables), adapter.stat_method)
    user_data = UserCatData(
        source_ds=source_ds,
        source_crs=adapter.source_crs,
        source_x_coord=adapter.x_coord,
        source_y_coord=adapter.y_coord,
        source_t_coord=adapter.time_coord,
        source_var=list(adapter.variables),
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
    missing = [v for v in adapter.variables if v not in ds.data_vars]
    if missing:
        raise RuntimeError(
            f"AggGen returned no data for {missing}; expected every adapter "
            f"variable {list(adapter.variables)} in the aggregated Dataset "
            f"(got {list(ds.data_vars)})."
        )
    return ds


def _year_of(path: Path) -> int:
    m = re.search(r"(\d{4})", path.stem)
    if not m:
        raise ValueError(f"Cannot parse a 4-digit year from filename: {path.name}")
    return int(m.group(1))


def subset_to_gdf_bounds(
    ds: xr.Dataset,
    gdf: gpd.GeoDataFrame,
    source_crs,
    x_coord: str,
    y_coord: str,
    margin_m: float = 2000.0,
) -> xr.Dataset:
    """Clip the source grid to the target polygons' bounding box (+ margin).

    The target polygons are reprojected to the source-grid CRS and the grid is
    sliced to their total bounds padded by ``margin_m`` (default 2000 m ≈ 2
    cells on the SNODAS 1-km grid, so boundary cells the polygons partially
    overlap are retained for area weighting — retune for a coarser/finer
    source). Selecting by the filtered coordinate labels (not a slice) is
    order-agnostic, so a descending ``y`` axis is handled correctly.

    Applied to a plain (non-dask) ``open_dataset`` result, this ``.sel`` is a
    lazy index, so whatever runs next materializes ONLY the target extent as an
    in-memory numpy array once per year: the ``pre_aggregate_hook`` if the
    adapter has one, otherwise gdptools' own per-variable ``.load()``. Either
    way gdptools sees an already-small source, so its ``.load()`` does no
    repeated chunk reads/decompression — measurably faster than handing gdptools
    a lazy full-grid source and letting it subset per variable.
    """
    minx, miny, maxx, maxy = gdf.to_crs(source_crs).total_bounds
    x = ds[x_coord]
    y = ds[y_coord]
    xsel = x[(x >= minx - margin_m) & (x <= maxx + margin_m)]
    ysel = y[(y >= miny - margin_m) & (y <= maxy + margin_m)]
    if xsel.size == 0 or ysel.size == 0:
        raise ValueError(
            "Source grid does not overlap the target polygons' bounds "
            f"(x in [{minx}, {maxx}], y in [{miny}, {maxy}]). Check CRS/extent."
        )
    return ds.sel({x_coord: xsel, y_coord: ysel})


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
    if years is not None:
        files = [f for f in files if _year_of(f) in years]
    if not files:
        raise FileNotFoundError(f"No files match {input_dir / adapter.files_glob}")

    logger.info(
        "Aggregating %s: %d HRUs, %d year-file(s), vars=%s, stat=%s -> %s",
        adapter.source_key, len(fabric_gdf), len(files),
        list(adapter.variables), adapter.stat_method, output_dir,
    )

    written: list[Path] = []
    weights: pd.DataFrame | None = None
    for i, f in enumerate(files, start=1):
        year = _year_of(f)
        # Clip the source grid to the target extent BEFORE the hook (lazy .sel
        # index on a plain open_dataset), so only the target extent is ever
        # materialized as in-memory numpy — neither the hook (if any) nor
        # gdptools touches the full source grid, and gdptools' per-variable
        # .load() reuses in-memory data instead of re-reading chunks.
        ds = xr.open_dataset(f)
        ds = subset_to_gdf_bounds(
            ds, fabric_gdf, adapter.source_crs, adapter.x_coord, adapter.y_coord
        )
        if adapter.pre_aggregate_hook is not None:
            ds = adapter.pre_aggregate_hook(ds)
        period = _period_bounds(ds, adapter.time_coord)
        if weights is None:
            weights = compute_or_load_weights(
                adapter, ds, fabric_gdf, id_col, period, weight_file
            )
        logger.info("[%d/%d] year %d: aggregating %s ...",
                    i, len(files), year, list(adapter.variables))
        hru_ds = aggregate_variables(adapter, ds, fabric_gdf, id_col, weights, period)
        out = output_dir / f"{output_prefix}_agg_{year}.nc"
        hru_ds.to_netcdf(out)
        written.append(out)
        logger.info("[%d/%d] year %d: wrote %s", i, len(files), year, out.name)
    logger.info("Done: %d per-year file(s) -> %s", len(written), output_dir)
    return written
