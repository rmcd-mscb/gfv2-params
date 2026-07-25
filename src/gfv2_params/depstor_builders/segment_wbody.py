"""Emit the segment-derived on-stream COMID table consumed by `wbody_connectivity`.

Runs before `waterbody` (whose BurnAdd overlap guard reads this table) and before
`wbody_connectivity` (whose primary on-stream source it is). It has no dependency on
`endorheic` — it reads only the fabric's own `segments_gpkg` (the model routing
network) and `waterbody_gpkg`, and writes the COMIDs a segment intersects with
positive length — the PRIMARY on-stream source.

No raster inputs, so this is cheap: measured 42 s wall / 2.0 GB peak RSS at CONUS scale
(186,709 segments x 448,124 polygons), unlike the ~384 G full-grid `waterbody`/`dprst`
steps. No windowing needed.

Deliberately FTYPE-agnostic — the Playa/Ice Mass never-on-stream guardrail lives at the
`wbody_connectivity` chokepoint so it applies to the opt-in NHD comparison sources too.
"""

from __future__ import annotations

import geopandas as gpd
import rasterio
from shapely.geometry import box

from ..segment_wbody import (
    check_onstream_floor,
    load_segment_comids,
    segment_comid_frame,
    segment_waterbody_comids,
    segment_waterbody_pairs,
    write_segment_comids,
)
from .context import BuildContext


def _assert_overlaps_template(seg: gpd.GeoDataFrame, template_path, logger) -> None:
    """Fail loud if the segment layer and the template grid are disjoint.

    A `segments_gpkg` mis-wired to another fabric would match zero waterbodies, make
    every waterbody depression storage, and exit 0. The floor catches that on a fabric
    that declares one; this catches it everywhere and names the actual cause.
    """
    with rasterio.open(template_path) as src:
        tmpl = gpd.GeoSeries([box(*src.bounds)], crs=src.crs)
    if seg.crs is not None and tmpl.crs != seg.crs:
        tmpl = tmpl.to_crs(seg.crs)
    if not box(*seg.total_bounds).intersects(tmpl.iloc[0]):
        raise ValueError(
            f"the segment layer's extent {seg.total_bounds.tolist()} does not overlap "
            f"the template grid {tmpl.total_bounds.tolist()} (in the segment CRS) — the "
            f"on-stream classifier would match nothing and every waterbody would become "
            f"depression storage. Check `segments_gpkg` points at THIS fabric's segments."
        )
    logger.info("  segment layer overlaps the template grid")


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.segments_gpkg is None:
        raise KeyError(
            "segment_wbody step needs `segments_gpkg` in the fabric profile — it is the "
            "model routing network the on-stream classifier is built on."
        )
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "segment_wbody step needs `waterbody_gpkg` and `waterbody_layer`."
        )
    output_path = ctx.resolve_output(step_cfg["output"])

    logger.info("--- segment_wbody ---")
    logger.info("  Segments  : %s (layer=%s)", ctx.segments_gpkg, ctx.segments_layer)
    logger.info("  Waterbody : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Output    : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        # Honour the floor on the skip path too: a stale or collapsed table left by an
        # aborted run would otherwise sail straight through into wbody_connectivity.
        check_onstream_floor(
            len(load_segment_comids(output_path)),
            fabric=ctx.fabric, floor=ctx.min_onstream_comids, source=output_path,
        )
        return {"segment_wbody_comids": output_path}

    if not ctx.segments_gpkg.exists():
        raise FileNotFoundError(f"segments gpkg not found: {ctx.segments_gpkg}")
    if not ctx.waterbody_gpkg.exists():
        raise FileNotFoundError(f"waterbody gpkg not found: {ctx.waterbody_gpkg}")

    seg = gpd.read_file(ctx.segments_gpkg, layer=ctx.segments_layer, use_arrow=True)
    logger.info("  %d segments", len(seg))
    _assert_overlaps_template(seg, ctx.template_path, logger)

    wb = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer, use_arrow=True)
    logger.info("  %d waterbody polygons", len(wb))

    pairs = segment_waterbody_pairs(seg, wb, logger=logger)
    comids = segment_waterbody_comids(pairs)
    frame = segment_comid_frame(pairs)
    check_onstream_floor(
        len(comids), fabric=ctx.fabric, floor=ctx.min_onstream_comids,
        source=output_path,
    )
    write_segment_comids(frame, output_path)
    logger.info(
        "  %d on-stream COMIDs from %d positive-length pairs (of %d waterbody polygons)",
        len(comids), int((pairs["overlap_m"] > 0).sum()) if len(pairs) else 0, len(wb),
    )
    return {"segment_wbody_comids": output_path}
