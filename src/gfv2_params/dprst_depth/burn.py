"""Burn per-polygon `dprst_depth_m` onto the 30 m template grid (#173 Task 6).

Task 5 (`fill.py`) guarantees every dprst polygon has a finite, positive
`dprst_depth_m` in metres. This module burns those per-polygon depths onto
`dprst_depth.tif`, aligned pixel-for-pixel with the fabric `template_raster`,
so the EXISTING gdptools area-weighted zonal step (Task 8) can compute a
per-HRU `dprst_depth_avg` from it with no new aggregation logic. Because each
dprst cell carries its OWN polygon's V/A mean depth, an area-weighted zonal
mean over an HRU's dprst cells reduces exactly to that HRU's Sum(V)/Sum(A)
across the dprst polygons it touches — the reason this builder rasterizes
depth directly rather than emitting a per-HRU table itself.

CONUS memory: the template is 153830x109901 (~16.9e9 cells); a full float32
grid is ~68 GB. This module NEVER materializes the full grid. It streams by
row-strips exactly like `depstor_builders/carea_map.py`'s `STRIP_ROWS`
pattern: for each strip, `rasterize_binary`-style, it (1) queries the
polygon spatial index for candidates whose bounding box intersects the
strip's bounding box, (2) rasterizes only those polygons against the
strip's window transform, (3) masks to land, (4) writes the strip, (5)
discards it. Peak memory is one strip's worth of float32 arrays, not the
CONUS grid.
"""
from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize as rio_rasterize
from rasterio.windows import Window
from rasterio.windows import bounds as window_bounds

from ..depstor import RasterInfo, assert_raster_aligned

__all__ = ["STRIP_ROWS", "DEPTH_NODATA", "burn_depth"]

# Row-strip height for the streamed burn. float32 output at this height is a
# tiny fraction of a CONUS-width strip (~109901 cols * 1024 rows * 4 B ~= 450
# MB) — matches the memory order of magnitude carea_map's TWI strip uses.
STRIP_ROWS = 1024

DEPTH_NODATA = -9999.0


def _depth_profile(info: RasterInfo) -> dict:
    """Rasterio profile for the float32 `dprst_depth.tif` output.

    LZW (no `predictor=2`, per repo convention — WhiteboxTools cannot read
    LZW+predictor=2 GeoTIFFs; this raster isn't WBT input today, but the
    convention is kept uniform across depstor rasters), tiled 256x256,
    BIGTIFF — mirrors `depstor.uint8_binary_profile`'s layout choices.
    """
    return {
        "driver": "GTiff",
        "height": info.height,
        "width": info.width,
        "count": 1,
        "dtype": "float32",
        "crs": info.crs,
        "transform": info.transform,
        "nodata": DEPTH_NODATA,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "YES",
    }


def burn_depth(
    dprst_gdf_with_depth: gpd.GeoDataFrame,
    template_path: str | Path,
    land_mask_path: str | Path,
    out_tif: str | Path,
    logger: logging.Logger,
) -> Path:
    """Rasterize each polygon's `dprst_depth_m` onto the template grid.

    Streams by `STRIP_ROWS`-tall row-strips (never allocates a full-grid
    array): for each strip, only the polygons whose bounding box intersects
    the strip's bounding box are rasterized against that strip's window
    transform, then masked to `land_mask_path` (value 1 = land, the repo-wide
    `land_mask.tif` convention — see `depstor.read_land_mask`).

    Args:
        dprst_gdf_with_depth: GeoDataFrame with a `dprst_depth_m` column
            (metres, finite, > 0 — Task 5's `fill_flat` output joined back
            to `dprst_polygons` geometry) and polygon geometries in any CRS
            (reprojected to the template CRS if needed).
        template_path: path to the fabric `template_raster` (defines output
            CRS/transform/shape).
        land_mask_path: path to `land_mask.tif`, aligned to `template_path`.
        out_tif: output path for `dprst_depth.tif`.
        logger: logger for progress/summary messages.

    Returns:
        `out_tif` as a `Path`.

    Raises:
        KeyError: if `dprst_gdf_with_depth` lacks `dprst_depth_m`.
        ValueError: if any depth is non-positive/non-finite, if the
            GeoDataFrame has no CRS, or if `land_mask_path` isn't aligned to
            the template grid.
    """
    if "dprst_depth_m" not in dprst_gdf_with_depth.columns:
        raise KeyError("burn_depth: dprst_gdf_with_depth missing 'dprst_depth_m'")
    if dprst_gdf_with_depth.crs is None:
        raise ValueError("burn_depth: dprst_gdf_with_depth has no CRS")

    out_tif = Path(out_tif)
    out_tif.parent.mkdir(parents=True, exist_ok=True)

    info = RasterInfo.from_path(Path(template_path))

    gdf = dprst_gdf_with_depth
    if gdf.crs != info.crs:
        gdf = gdf.to_crs(info.crs)

    valid = gdf[
        gdf.geometry.notna()
        & ~gdf.geometry.is_empty
        & gdf["dprst_depth_m"].notna()
    ]
    bad = valid[~np.isfinite(valid["dprst_depth_m"]) | (valid["dprst_depth_m"] <= 0)]
    if len(bad):
        raise ValueError(
            f"burn_depth: {len(bad)} polygon(s) with non-finite/non-positive "
            f"dprst_depth_m (min={bad['dprst_depth_m'].min()}); Task 5's "
            f"fill_flat guarantees positive, finite depths — investigate upstream."
        )

    n_dropped = len(gdf) - len(valid)
    if n_dropped:
        logger.warning(
            "burn_depth: dropping %d row(s) with null/empty geometry or null depth",
            n_dropped,
        )

    sindex = valid.sindex if len(valid) else None

    profile = _depth_profile(info)
    total_burned = 0

    with rasterio.open(land_mask_path) as lm_src:
        assert_raster_aligned(lm_src, info, "land_mask")

        with rasterio.open(out_tif, "w", **profile) as dst:
            for row_off in range(0, info.height, STRIP_ROWS):
                h = min(STRIP_ROWS, info.height - row_off)
                window = Window(0, row_off, info.width, h)
                win_transform = rasterio.windows.transform(window, info.transform)

                land_valid = lm_src.read(1, window=window) == 1

                strip = np.full((h, info.width), DEPTH_NODATA, dtype=np.float32)

                if sindex is not None:
                    left, bottom, right, top = window_bounds(window, info.transform)
                    cand_pos = list(sindex.intersection((left, bottom, right, top)))
                    if cand_pos:
                        cand = valid.iloc[cand_pos]
                        shapes = (
                            (geom, float(depth))
                            for geom, depth in zip(cand.geometry, cand["dprst_depth_m"])
                        )
                        strip = rio_rasterize(
                            shapes=shapes,
                            out_shape=(h, info.width),
                            transform=win_transform,
                            fill=DEPTH_NODATA,
                            dtype=np.float32,
                            all_touched=False,
                        )

                strip = np.where(land_valid, strip, DEPTH_NODATA).astype(np.float32, copy=False)
                dst.write(strip, 1, window=window)
                total_burned += int((strip != DEPTH_NODATA).sum())

    total_cells = info.height * info.width
    logger.info(
        "burn_depth: %d cells burned (%.4f%% of grid) -> %s",
        total_burned, 100 * total_burned / total_cells if total_cells else 0.0, out_tif,
    )
    return out_tif
