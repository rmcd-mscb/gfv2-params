"""Clip a shared CONUS raster to a fabric's bounds as a zero-copy VRT.

Depression-storage builders size every array to the *template* grid and warp
each source raster onto it, so the template choice controls compute extent.
A CONUS template forces CONUS-scale memory/time even for a regional fabric;
a per-VPU tile is cheap but does not generalise to fabrics that straddle VPU
boundaries. This script produces a fabric-scoped template by clipping a shared
CONUS raster to the fabric's HRU bounds (snapped outward to the source grid,
plus a cell buffer for `all_touched` edge HRUs).

Why clip `fdr.vrt` specifically (the default source):
  - The shared CONUS VRTs sit on two lattices: the DEM family (elevation,
    slope, aspect) is offset by a fractional cell from the hydrology family
    (fdr, twi). `carea_map` hard-requires the template and TWI to be
    whole-cell aligned, so the template must come from the hydrology lattice.
  - Of the hydrology rasters, `fdr.vrt` has the larger extent and covers every
    current fabric (gfv2, oregon); `twi.vrt` does not (it misses gfv2's
    northern HRUs). So `fdr.vrt` is the universal Lattice-B template source.

The output VRT serves as BOTH `template_raster` (grid only — pixel values are
never read) and `fdr_raster` (D8 values, read by the routing step). Because it
is a windowed VRT, `routing`'s whole-source `reproject_match` reads only the
fabric window rather than the full ~18 GB CONUS FDR.

Usage:
  pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric oregon
"""

import argparse
import math
from pathlib import Path

import geopandas as gpd
import rasterio
from osgeo import gdal

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging

gdal.UseExceptions()


def _snap_bounds_to_grid(bounds, transform, buffer_cells: int):
    """Expand HRU bounds by `buffer_cells` then snap OUTWARD to the source grid.

    Returns (ulx, uly, lrx, lry) on the source pixel lattice, guaranteed to
    fully contain the (buffered) input bounds. Works for a north-up raster
    (transform.a > 0, transform.e < 0).
    """
    minx, miny, maxx, maxy = bounds
    px = transform.a          # +cellsize
    py = transform.e          # -cellsize
    ox = transform.c          # left edge of column 0
    oy = transform.f          # top edge of row 0
    # The floor/ceil snapping below is only correct for an axis-aligned, north-up
    # raster. Fail loudly on a rotated/flipped --source rather than emitting a
    # silently-wrong window.
    if transform.b != 0 or transform.d != 0 or px <= 0 or py >= 0:
        raise ValueError(
            f"Source raster is not axis-aligned north-up (a={px}, e={py}, "
            f"b={transform.b}, d={transform.d}); _snap_bounds_to_grid assumes it."
        )
    cell_x = abs(px)
    cell_y = abs(py)

    minx -= buffer_cells * cell_x
    maxx += buffer_cells * cell_x
    miny -= buffer_cells * cell_y
    maxy += buffer_cells * cell_y

    c_left = math.floor((minx - ox) / cell_x)
    c_right = math.ceil((maxx - ox) / cell_x)
    r_top = math.floor((oy - maxy) / cell_y)
    r_bot = math.ceil((oy - miny) / cell_y)

    ulx = ox + c_left * cell_x
    lrx = ox + c_right * cell_x
    uly = oy - r_top * cell_y
    lry = oy - r_bot * cell_y
    return ulx, uly, lrx, lry


def _whole_cell_offset(t_a, ref_transform):
    """Fractional-cell offset of transform origin vs a reference lattice."""
    col = (t_a.c - ref_transform.c) / ref_transform.a
    row = (t_a.f - ref_transform.f) / ref_transform.e
    return col - round(col), row - round(row)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fabric", required=True, help="Active fabric name (profile in base_config.yml).")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml (default: packaged configs/base_config.yml).")
    parser.add_argument("--source", default=None,
                        help="Shared raster to clip (default: {data_root}/shared/conus/vrt/fdr.vrt).")
    parser.add_argument("--output", default=None,
                        help="Output VRT path (default: {data_root}/{fabric}/shared/{fabric}_fdr.vrt).")
    parser.add_argument("--buffer-cells", type=int, default=8,
                        help="Cells of margin added around the HRU bounds before snapping (default: 8).")
    parser.add_argument("--force", action="store_true", help="Overwrite the output VRT if it exists.")
    args = parser.parse_args()

    logger = configure_logging("clip_shared_to_fabric")
    base_config = Path(args.base_config) if args.base_config else None
    config = load_base_config(base_config, fabric=args.fabric)

    data_root = config["data_root"]
    fabric = config["fabric"]
    source = Path(args.source) if args.source else Path(data_root) / "shared" / "conus" / "vrt" / "fdr.vrt"
    output = Path(args.output) if args.output else Path(data_root) / fabric / "shared" / f"{fabric}_fdr.vrt"
    hru_gpkg = Path(require_config_key(config, "hru_gpkg", "clip_shared_to_fabric"))
    hru_layer = require_config_key(config, "hru_layer", "clip_shared_to_fabric")

    logger.info("--- clip_shared_to_fabric ---")
    logger.info("  Fabric : %s", fabric)
    logger.info("  Source : %s", source)
    logger.info("  HRU    : %s (layer=%s)", hru_gpkg, hru_layer)
    logger.info("  Output : %s", output)

    if not source.exists():
        raise FileNotFoundError(f"Source raster not found: {source}")
    if not hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {hru_gpkg}")
    if output.exists() and not args.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return 0

    with rasterio.open(source) as src:
        src_transform = src.transform
        src_crs = src.crs
        src_bounds = src.bounds

    hru = gpd.read_file(hru_gpkg, layer=hru_layer)
    n_all = len(hru)
    hru = hru[hru.geometry.notna() & ~hru.geometry.is_empty]
    if len(hru) == 0:
        raise ValueError(
            f"HRU layer '{hru_layer}' in {hru_gpkg} has no valid geometries "
            f"(read {n_all} rows; all null/empty). Check the hru_layer name."
        )
    if len(hru) < n_all:
        logger.info("  Dropped %d null/empty HRU geometries (%d remain)", n_all - len(hru), len(hru))
    if hru.crs != src_crs:
        logger.info("  Reprojecting HRU bounds from %s to source CRS %s", hru.crs, src_crs)
        hru = hru.to_crs(src_crs)
    hb = tuple(hru.total_bounds)
    logger.info("  HRU bounds (source CRS): %s", [round(x) for x in hb])

    # Fail loudly if the fabric falls outside the source extent — a clip cannot
    # recover data the source does not have.
    if not (hb[0] >= src_bounds.left and hb[1] >= src_bounds.bottom
            and hb[2] <= src_bounds.right and hb[3] <= src_bounds.top):
        raise ValueError(
            f"Fabric '{fabric}' bounds {[round(x) for x in hb]} are not fully "
            f"inside the source raster extent {[round(x) for x in src_bounds]}. "
            f"Pick a source raster that covers the fabric."
        )

    ulx, uly, lrx, lry = _snap_bounds_to_grid(hb, src_transform, args.buffer_cells)
    width = int(round((lrx - ulx) / abs(src_transform.a)))
    height = int(round((uly - lry) / abs(src_transform.e)))
    logger.info(
        "  Clip window: ul=(%.0f, %.0f) lr=(%.0f, %.0f) -> %dx%d cells (%.3fB)",
        ulx, uly, lrx, lry, width, height, width * height / 1e9,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    # gdal.UseExceptions() makes error-level GDAL failures raise, but some failure
    # modes return a None dataset without raising — check explicitly so a botched
    # clip can't leave a stale/missing VRT that the self-checks then read.
    ds = gdal.Translate(str(output), str(source), projWin=[ulx, uly, lrx, lry], format="VRT")
    if ds is None:
        raise RuntimeError(f"gdal.Translate produced no dataset for {output} — clip failed.")
    ds = None  # close/flush

    # Self-checks: clip covers the fabric and is whole-cell aligned with twi.vrt.
    with rasterio.open(output) as clip:
        cb = clip.bounds
        covers = (hb[0] >= cb.left and hb[1] >= cb.bottom and hb[2] <= cb.right and hb[3] <= cb.top)
        if not covers:
            raise RuntimeError(f"Clip {[round(x) for x in cb]} does not cover HRU bounds — snapping bug.")
        twi = Path(data_root) / "shared" / "conus" / "vrt" / "twi.vrt"
        if twi.exists():
            with rasterio.open(twi) as t:
                col_frac, row_frac = _whole_cell_offset(clip.transform, t.transform)
            aligned = abs(col_frac) < 1e-6 and abs(row_frac) < 1e-6
            logger.info("  twi.vrt whole-cell aligned: %s (col_frac=%.2e, row_frac=%.2e)",
                        aligned, col_frac, row_frac)
            if not aligned:
                raise RuntimeError(
                    "Clip is not whole-cell aligned with twi.vrt; carea_map would "
                    "reject it. Is the source on the hydrology (fdr/twi) lattice?"
                )
        else:
            logger.warning(
                "  twi.vrt not found at %s — SKIPPED the whole-cell alignment check. "
                "Clip lattice is UNVERIFIED; if the source is not on the hydrology "
                "(fdr/twi) lattice, carea_map will reject the template downstream.", twi,
            )
    logger.info("  Wrote fabric-scoped VRT: %s (%dx%d)", output, width, height)
    logger.info("  Point the profile's template_raster AND fdr_raster at this file.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
