"""Per-batch circular-mean aspect stats from the CONUS aspect + slope VRTs.

Drives the ``aspect`` entry in ``configs/zonal/zonal_params.yml``
(``script: aspect``). Separate from the generic ``zonal`` runner because
``hru_aspect`` is a CIRCULAR mean: TM 6-B9 §603 requires
``atan2(mean(sin(aspect)), mean(cos(aspect)))``, and an arithmetic mean of a
wrapped 0-360 field measures how symmetric an HRU's cells are about 180 deg, not
which way it faces. Measured on gfv2's 361,471 HRUs the old column's median was
179.4 deg with an IQR of 151.7-207.5 -- that central tendency is the artifact
(issue #201).

Three exactextract passes over ONE clipped window:

  1. raw aspect, every valid cell -> count, mean, std, min, 25/50/75%, max, sum
     (byte-comparable with the pre-fix product -- see `_STAT_COLUMNS`)
  2. sin(aspect), non-flat cells only -> mean_sin, and count as n_aspect_cells
  3. cos(aspect), non-flat cells only -> mean_cos

``hru_aspect`` itself is NOT computed here. It is a ``derived_columns:`` entry
applied by ``run_merge`` and re-applied after the KNN fill sweep, so it is always
recomputed from the two means rather than concatenated or interpolated -- KNN on a
circular quantity would reintroduce exactly the defect this module exists to fix.

No CONUS sin/cos rasters are built. gdptools already subsets the source to the
batch's bounding box (``UserTiffData.prep_agg_data``) and batches are KD-tree
spatially compact (``gfv2_params.batching``), so the derived arrays are per-batch
and transient: gfv2's largest batch bbox is 0.80e9 cells (3.2 GB float32), median
0.16e9. The alternative -- ~90 GB of new CONUS tiles plus VRTs and overviews --
would also land the two means in SEPARATE merged CSVs that ``derived_columns``
cannot join.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import rioxarray
from gdptools import UserTiffData, ZonalGen

# RichDEM does not flag flat cells: rd.TerrainAttribute(dem, attrib="aspect")
# assigns them 270.0 (due west). Measured against the co-registered slope tile on
# a 3000x3000 window per VPU -- VPU 01: 2.79% of cells are slope == 0 and 99.05%
# of aspect == 270 cells are among them; VPU 07: 0.34%; VPU 12: ~0%. No cell
# carries -1 or any other sentinel. ArcGIS, the implementation TM 6-B9 §603
# cites, writes -1 for flats instead, so this is an artifact of the move off
# ArcPy. A flat cell has no down slope direction -- which is what §603 says
# hru_aspect is the mean of -- so flats are excluded rather than voting for west.
#
# The test is exact equality on the SLOPE raster, never `aspect == 270`: a
# genuinely west-facing sloped cell is also 270 and must be kept.
FLAT_SLOPE = 0.0

# Pass 1's columns, in the order gdptools' exactextract engine returns them. This
# is exactly what `run_zonal_batch` emits for a continuous raster; keeping the
# order and the population identical is what makes the retained `mean` an honest
# record of the pre-fix product rather than a new statistic wearing the old name.
_STAT_COLUMNS = ["count", "mean", "std", "min", "25%", "50%", "75%", "max", "sum"]

# Cells of slop added to the batch bounds before clipping. gdptools' own subset
# buffers by `2 * max(res)` (`_get_shp_bounds_w_buffer`, gdptools/utils.py:718)
# -- so 2 is the exact minimum, not a margin. Tightening it to 1 would silently
# clip the edge cells gdptools itself requests for boundary-touching HRUs,
# biasing every HRU on a batch seam -- the precise failure this constant exists
# to prevent.
_BOUNDS_BUFFER_CELLS = 2


def _buffered_bounds(gdf, da):
    """Batch bounds in the raster's CRS, grown by `_BOUNDS_BUFFER_CELLS`.

    Both axes are padded by the LARGER of the two resolutions, matching gdptools'
    `bbox.buffer(2 * max(...))` exactly rather than padding each axis by its own
    resolution. The two agree on square pixels (today's VRTs are 30x30 m) and
    would not on anisotropic ones, where the per-axis form would under-pad the
    short axis relative to what gdptools itself requests.
    """
    minx, miny, maxx, maxy = gdf.total_bounds
    pad = _BOUNDS_BUFFER_CELLS * max(abs(v) for v in da.rio.resolution())
    return (minx - pad, miny - pad, maxx + pad, maxy + pad)


def _assert_co_registered(aspect_da, slope_da, aspect_name: str, slope_name: str) -> None:
    """Both clips must land on the same grid, or the flat mask masks the wrong cells.

    aspect.vrt and slope.vrt both derive from the same per-VPU NEDSnapshot DEM, so
    this holds by construction today. It is checked anyway because the failure is
    silent: a half-cell offset would exclude the neighbours of the flat cells and
    keep the flats, and every downstream number would still look plausible.

    The CRS is part of the check, not decoration. Shape and transform are bare
    numbers with no datum attached, so two rasters in DIFFERENT projections can
    match on both -- and the caller computes the clip bounds in the ASPECT CRS
    only, reusing them verbatim to `clip_box` the slope raster. Two different CRSs
    would then mask a different patch of ground, which is exactly the silent
    wrong-cells failure this function exists to prevent.
    """
    if (
        aspect_da.shape != slope_da.shape
        or aspect_da.rio.transform() != slope_da.rio.transform()
        or aspect_da.rio.crs != slope_da.rio.crs
    ):
        raise ValueError(
            f"aspect and slope clips are not co-registered: {aspect_name} is "
            f"{aspect_da.shape} at {aspect_da.rio.transform()} in CRS "
            f"{aspect_da.rio.crs}, {slope_name} is {slope_da.shape} at "
            f"{slope_da.rio.transform()} in CRS {slope_da.rio.crs}. Both must come "
            f"from the same DEM lattice."
        )


def _zonal_means(da, nhru_gdf, id_feature: str, var_name: str, out_dir: Path):
    """One exactextract pass over an already-clipped DataArray.

    `zonal_writer=None`: gdptools writes a CSV only when it equals "csv"
    (`ZonalGen.calculate_zonal`) and returns the frame either way. Three passes
    must not leave three files in `out_dir` -- `run_merge` globs it with
    `base_nhm_aspect_{fabric}_batch_*_param.csv` and would concat any stray that
    happened to match. `tests/test_aspect_zonal.py` asserts exactly one CSV is
    written, so a gdptools change here fails loudly.
    """
    data = UserTiffData(
        source_var=var_name,
        source_ds=da,
        source_crs=da.rio.crs,
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
        zonal_writer=None,
        out_path=str(out_dir),
        file_prefix=var_name,
        jobs=4,
    )
    return zonal_gen.calculate_zonal(categorical=False)


def run_aspect_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of circular-mean aspect stats.

    Writes a single CSV named to the same pattern the generic runner uses, so
    `run_merge` needs no special case.
    """
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    aspect_path = Path(config["source_raster"])
    # A plain config read, like every other runner's param-entry keys (lulc.py's
    # crosswalk_file/canopy_raster, soils.py's source_raster). NOT
    # `require_config_key`: that helper's message says "Expected from fabric profile
    # '<fabric>' in configs/base_config.yml", and `slope_raster` lives in the aspect
    # entry of configs/zonal/zonal_params.yml -- an operator who dropped the key
    # would be sent to the wrong file to put it back.
    if "slope_raster" not in config:
        raise KeyError(
            "Required key 'slope_raster' missing from merged config for "
            "run_aspect_batch. It is a param-entry key: add it to the `aspect` entry "
            "in configs/zonal/zonal_params.yml (it names the slope VRT read ONLY to "
            "build the flat mask)."
        )
    slope_path = Path(config["slope_raster"])
    batch_gpkg = Path(config["batch_dir"]) / f"batch_{batch_id:04d}.gpkg"
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    for path, label in ((aspect_path, "aspect"), (slope_path, "slope")):
        if not path.exists():
            raise FileNotFoundError(f"Input {label} raster not found: {path}")
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")

    logger.info("Aspect raster: %s", aspect_path)
    logger.info("Slope raster (flat mask): %s", slope_path)
    logger.info("Batch GPKG: %s", batch_gpkg)

    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)",
                target_layer, len(nhru_gdf), batch_id)

    aspect_full = rioxarray.open_rasterio(aspect_path, masked=True)
    slope_full = rioxarray.open_rasterio(slope_path, masked=True)

    bounds = _buffered_bounds(nhru_gdf.to_crs(aspect_full.rio.crs), aspect_full)
    aspect_da = aspect_full.rio.clip_box(*bounds)
    slope_da = slope_full.rio.clip_box(*bounds)
    _assert_co_registered(aspect_da, slope_da, aspect_path.name, slope_path.name)
    logger.info("Clipped both rasters to batch bounds: shape=%s", aspect_da.shape)

    # `slope == 0` is False where slope is NaN, so nodata never counts as flat.
    flat = slope_da == FLAT_SLOPE
    sloped_aspect = aspect_da.where(~flat)
    radians = np.deg2rad(sloped_aspect)
    # `spatial_ref` (and therefore `.rio.crs`) survives `.where` and these numpy
    # ufuncs unchanged -- measured directly, so write_crs isn't repairing a lost
    # CRS. Kept anyway to assert the requirement UserTiffData actually depends on
    # (`source_ds.rio.crs` resolving to a real CRS) explicitly at the point of
    # use, rather than relying on an rioxarray propagation behaviour we don't
    # want this module's correctness to depend on staying that way.
    sin_da = np.sin(radians).rio.write_crs(aspect_da.rio.crs)
    cos_da = np.cos(radians).rio.write_crs(aspect_da.rio.crs)
    # Per-cell budget while all seven names above are live: aspect_da + slope_da
    # + sloped_aspect + radians + sin_da + cos_da at 4 B (float32) each, plus
    # flat at 1 B (bool) = 25 B/cell. Only aspect_da (pass 1, below), sin_da and
    # cos_da are needed past this point -- slope_da/flat/sloped_aspect/radians
    # are dead the moment sin_da/cos_da exist. On gfv2's largest measured batch
    # (0.797e9 cells) that is 19.9 GB of arrays, 10.4 GB of it dead weight this
    # drop avoids carrying through all three exactextract passes below.
    del slope_da, flat, sloped_aspect, radians

    raw = _zonal_means(aspect_da, nhru_gdf, id_feature, source_type, output_dir)
    sin_stats = _zonal_means(sin_da, nhru_gdf, id_feature, f"{source_type}_sin", output_dir)
    cos_stats = _zonal_means(cos_da, nhru_gdf, id_feature, f"{source_type}_cos", output_dir)

    # All three frames are indexed by id_feature, so these align by HRU, not by
    # row order.
    out = raw[_STAT_COLUMNS].copy()
    out["n_aspect_cells"] = sin_stats["count"]
    out["mean_sin"] = sin_stats["mean"]
    out["mean_cos"] = cos_stats["mean"]
    # `count` is exactextract's COVERAGE-WEIGHTED cell count, so this is an area
    # fraction, not a cell tally. NaN rather than inf/0 for an HRU with no covered
    # cells at all -- "no data" is not "no flats".
    out["flat_frac"] = np.where(
        out["count"] > 0, 1.0 - out["n_aspect_cells"] / out["count"], np.nan
    )

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"
    out_path = output_dir / f"{file_prefix}.csv"
    out.to_csv(out_path)
    logger.info("Aspect zonal statistics complete. Shape: %s -> %s", out.shape, out_path)
