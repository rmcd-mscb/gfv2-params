"""Raster-creation utilities for the depression-storage pipeline.

Ported from depstor's RasterPipeline (depstor/scripts/DepStor.py:42-410), but
operating on numpy arrays + a shared raster template. The depstor "rasterize
HRU polygons and tag cells with HRU IDs" pattern is intentionally NOT carried
over — gdptools handles HRU overlay directly during zonal aggregation.

All raster outputs use the conventions:
- uint8 binary masks: value 1 = present, value 255 = nodata
- int32 region labels: value 0 = nodata
- LZW-compressed, tiled 256x256 (LZW chosen over ZSTD because
  WhiteboxTools only reads PACKBITS/LZW/DEFLATE)
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.coords import BoundingBox
from rasterio.features import rasterize as rio_rasterize
from rasterio.transform import Affine
from scipy import ndimage


@dataclass
class RasterInfo:
    """Spatial metadata for the template raster."""
    crs: CRS
    width: int
    height: int
    transform: Affine
    nodata: Optional[float]
    bounds: BoundingBox

    @classmethod
    def from_path(cls, path: Path) -> "RasterInfo":
        with rasterio.open(path) as src:
            return cls(
                crs=src.crs,
                width=src.width,
                height=src.height,
                transform=src.transform,
                nodata=src.nodata,
                bounds=src.bounds,
            )


def rasterize_binary(gdf, info: RasterInfo, all_touched: bool = False) -> np.ndarray:
    """Burn polygons in `gdf` to a uint8 binary raster aligned to `info`.

    Returns an array shaped (info.height, info.width) where 1 = covered by any
    geometry and 255 = uncovered (nodata convention).
    Geometries are reprojected to `info.crs` first if needed.
    """
    if gdf.crs is None:
        raise ValueError("Input GeoDataFrame has no CRS")
    if info.crs is None:
        raise ValueError("RasterInfo has no CRS")
    if gdf.crs != info.crs:
        gdf = gdf.to_crs(info.crs)

    valid = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    if valid.empty:
        return np.full((info.height, info.width), 255, dtype=np.uint8)

    shapes = ((geom, 1) for geom in valid.geometry)
    out = rio_rasterize(
        shapes=shapes,
        out_shape=(info.height, info.width),
        transform=info.transform,
        fill=255,
        dtype=np.uint8,
        all_touched=all_touched,
    )
    return out


def threshold_above(values: np.ndarray, threshold: float, src_nodata) -> np.ndarray:
    """Return uint8 binary mask: 1 where values >= threshold, else 255 (nodata).

    Nodata pixels in the source map to 255 in the output.
    """
    if src_nodata is not None and isinstance(src_nodata, float) and np.isnan(src_nodata):
        valid = ~np.isnan(values)
    elif src_nodata is not None:
        valid = values != src_nodata
    else:
        valid = np.ones_like(values, dtype=bool)

    above = valid & (values >= threshold)
    return np.where(above, np.uint8(1), np.uint8(255))


def read_land_mask(landmask_path: Path) -> np.ndarray:
    """Read land_mask.tif and return a boolean land mask (True = land/in-fabric).

    `land_mask.tif` (built by the `landmask` step of `build_depstor_rasters.py`,
    via `gfv2_params.depstor_builders.landmask`) is the uint8 1/255 rasterised
    HRU fabric — the authoritative depstor land/domain mask. Every depstor
    builder masks its output against it so ocean / off-domain cells are never
    marked present.

    Whole-array builders call this helper once; streaming builders open
    land_mask.tif themselves and test ``strip == 1`` per window. The mask is
    NOT derived from the template DEM's nodata footprint — the hydro-
    conditioned DEM carries valid elevations over coastal ocean, so its nodata
    extent bulges into the sea.
    """
    with rasterio.open(landmask_path) as src:
        return src.read(1) == 1


def read_land_mask_for_grid(
    landmask_path: Path,
    transform: Affine,
    height: int,
    width: int,
) -> np.ndarray:
    """Read land_mask.tif onto an arbitrary target grid via a windowed read.

    Returns a (height, width) boolean array — True where the land mask reads 1
    (inside the HRU fabric). Cells outside the land_mask coverage fall back to
    False (ocean), so a target extent that escapes the mask is handled
    defensively rather than raising.

    Assumes the target grid is co-aligned with the land mask (same CRS, same
    pixel size, anchored to the same lattice). The `landmask` step of
    `build_depstor_rasters.py` rasterises onto the fabric's `template_raster`,
    so any raster on that template grid — or a sub-extent of it — is co-aligned
    by construction.
    Use this for masking products that share the lattice but not the extent
    (e.g. per-VPU `Twi_merged_<vpu>.tif`, which sits inside the CONUS
    template grid).
    """
    from rasterio.windows import from_bounds as _from_bounds

    left = transform.c
    top = transform.f
    right = left + width * transform.a
    bottom = top + height * transform.e
    with rasterio.open(landmask_path) as src:
        win = _from_bounds(left, bottom, right, top, transform=src.transform)
        data = src.read(1, window=win, boundless=True, fill_value=255)
    if data.shape != (height, width):
        raise ValueError(
            f"Land-mask window read returned shape {data.shape}, expected "
            f"({height}, {width}). Target grid is likely not co-aligned with "
            f"{landmask_path}."
        )
    return data == 1


def intersect_binaries(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Return uint8 binary mask: 1 where both inputs are 1, else 255 (nodata).

    Inputs are the 1/255 binary convention used throughout depstor outputs.
    """
    mask = (a == 1) & (b == 1)
    out = np.full_like(a, 255, dtype=np.uint8)
    out[mask] = 1
    return out


def compute_carea_map_binary(
    perv: np.ndarray,
    onstream: np.ndarray,
    twi: np.ndarray,
    threshold,  # float scalar OR np.ndarray broadcastable to twi (per-cell T_P)
    twi_nodata: Optional[float],
    land_valid: np.ndarray,
) -> np.ndarray:
    """Build the binary carea_map mask used for getCarea-style PRMS params.

    Per cell, returns 1 where the cell is valid land AND pervious AND either
    (a) TWI exceeds the threshold or (b) the cell is on-stream storage; 255
    otherwise. Mirrors `getCareaMap` in the ArcPy reference
    (docs/0b_TB_depr_stor.py:315-350) but drops HRU-ID tagging — HRU identity
    is recovered at zonal-stats time.

    `land_valid` is the rasterised HRU-fabric land mask (see `read_land_mask`).
    It is applied explicitly rather than relying on the perv gate alone — the
    on-stream branch can otherwise rescue off-land cells, and a stale
    perv_binary should not be able to leak ocean into carea_map.

    Short-circuits on the perv test first so NaN / sentinel TWI values in
    non-perv cells do not pollute the result. Handles both NaN and the
    -9999-style sentinel nodata that the merged TWI rasters carry.

    `threshold` may be a scalar (absolute mode, or percentile-conus / single-VPU)
    or a per-cell float array the same shape as `twi` (percentile per-VPU mode,
    where each cell carries its HRU's home-VPU T_P). `twi > threshold` broadcasts
    either way.
    """
    is_perv = perv == 1
    is_onstream = onstream == 1

    if twi_nodata is not None and isinstance(twi_nodata, float) and np.isnan(twi_nodata):
        twi_valid = ~np.isnan(twi)
    elif twi_nodata is not None:
        twi_valid = (twi != twi_nodata) & ~np.isnan(twi)
    else:
        twi_valid = ~np.isnan(twi)

    above_thresh = twi_valid & (twi > threshold)
    keep = land_valid & is_perv & (above_thresh | is_onstream)

    out = np.full(perv.shape, 255, dtype=np.uint8)
    out[keep] = 1
    return out


def clump_regions(binary_arr: np.ndarray) -> np.ndarray:
    """Label connected components in a binary raster (8-connectivity).

    Replaces depstor's broken `wbe.clump(diag=True)` (DepStor.py:657-661).
    Treats cells with value 1 as foreground; anything else (including the
    255 nodata sentinel) is background. Returns int32 labels with 0 =
    background/nodata.
    """
    foreground = (binary_arr == 1)
    structure = np.ones((3, 3), dtype=bool)  # 8-connectivity
    labels, _ = ndimage.label(foreground, structure=structure)
    return labels.astype(np.int32)


def regions_touching_mask(regions: np.ndarray, mask: np.ndarray) -> set[int]:
    """Return the set of region IDs that share at least one cell with `mask`.

    `regions`: int32 label array (0 = background).
    `mask`: uint8 binary (1 = present, 255 = nodata).
    """
    touched = regions[mask == 1]
    ids = set(int(v) for v in np.unique(touched) if v != 0)
    return ids


def regions_to_binary(regions: np.ndarray, keep_ids: set[int]) -> np.ndarray:
    """Convert a region-label raster back to a uint8 binary mask.

    Cells whose region ID is in `keep_ids` become 1; all others become 255.
    """
    if not keep_ids:
        return np.full(regions.shape, 255, dtype=np.uint8)
    keep = np.isin(regions, list(keep_ids))
    return np.where(keep, np.uint8(1), np.uint8(255))


def write_uint8_binary(arr: np.ndarray, info: RasterInfo, out_path: Path) -> None:
    """Write a uint8 binary mask using the template spatial metadata."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": info.height,
        "width": info.width,
        "count": 1,
        "dtype": "uint8",
        "crs": info.crs,
        "transform": info.transform,
        "nodata": 255,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "YES",
    }
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr.astype(np.uint8), 1)


def write_int32_regions(arr: np.ndarray, info: RasterInfo, out_path: Path) -> None:
    """Write an int32 region-label raster using the template spatial metadata."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": info.height,
        "width": info.width,
        "count": 1,
        "dtype": "int32",
        "crs": info.crs,
        "transform": info.transform,
        "nodata": 0,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "YES",
    }
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr.astype(np.int32), 1)


def read_aligned_uint8(path: Path, info: RasterInfo) -> np.ndarray:
    """Read a uint8 raster, asserting it matches the template grid exactly.

    Use this when consuming intermediates we wrote ourselves with
    `write_uint8_binary`. Raises if the input does not align with `info`.
    """
    with rasterio.open(path) as src:
        if (src.width, src.height) != (info.width, info.height):
            raise ValueError(
                f"Raster {path} has shape ({src.width}x{src.height}); "
                f"expected ({info.width}x{info.height})"
            )
        if src.crs != info.crs:
            raise ValueError(f"Raster {path} CRS {src.crs} != template CRS {info.crs}")
        if src.transform != info.transform:
            raise ValueError(f"Raster {path} transform mismatch with template")
        return src.read(1)
