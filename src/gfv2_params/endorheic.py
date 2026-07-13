"""Endorheic (closed-basin) depression-storage classifier.

The existing on-stream tests (WBAREACOMI, flow-through) are LOCAL: "does a Network
flowline enter and leave this waterbody?" NHD draws Network artificial paths between
the arms of a terminal lake, so the lake reads as through-flowing and is promoted
on-stream. No local test can see that a whole basin is endorheic -- which is why the
Great Salt Lake is currently classified on-stream.

Two signals fix that, both of which can only SUBTRACT from the on-stream set:

Signal A (primary) -- terminus-inside-itself, on the FDR grid.
    A waterbody is depression storage iff its water's terminus lies INSIDE ITSELF.
    GSL's water ends in GSL. A pond upstream in the same closed basin ends in GSL,
    not in the pond, so it stays on-stream. Lewis and Clark Lake's water ends in the
    Gulf of Mexico. (This is why the rule is terminus-INSIDE-ITSELF and not merely
    terminates-at-a-sink: the latter demotes every on-stream reservoir in a closed
    basin.)

Signal B (complement) -- majority-inside a WBD type-C (closed) HUC12. Earns its place
    because Walker Lake contains no FDR terminal cell, so Signal A misses it.

Signal A reads the same grid -- and runs the same kernel -- that `routing` reads, so
the classifier and the router agree BY CONSTRUCTION: d8_routing treats code 0 as a
terminus, so a waterbody the router dead-ends in IS a depression.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds
from shapely.geometry import Point

from .d8_routing import drains_to_dprst_kernel

# Minimum window pad around a waterbody, in metres, so a flow path that circles
# within the lake's own basin stays inside the window.
MIN_PAD_M = 20_000.0

# Share of a waterbody's area that must lie inside the closed-basin union (Signal B),
# and share of its cells that must reach its own terminus (Signal A). NOT a tuned
# knob: frac_own is bimodal (204 of 239 CONUS candidates at >= 0.95, only 3 in
# 0.45-0.55) and the demotion set moves ~3% across thresholds 0.3-0.7.
MIN_FRAC = 0.5


def closed_basin_comids(
    wb_gdf: gpd.GeoDataFrame,
    closed_gdf: gpd.GeoDataFrame,
    min_frac: float = MIN_FRAC,
) -> set[int]:
    """COMIDs of waterbodies majority-inside the DISSOLVED union of closed HUC12s.

    Dissolve first, then measure: a lake straddling two *adjacent* closed HUC12s is
    fully inside the closed system but majority-inside neither polygon on its own.

    A waterbody is a COMID, not a row: `conus_waterbodies.gpkg` stores multi-part
    waterbodies as multiple rows sharing one COMID (448,124 rows, strictly fewer
    unique COMIDs). Area and intersection-area are summed across all of a COMID's
    rows BEFORE dividing, so a COMID is never decided on the strength of one row
    that individually clears `min_frac` while the COMID's true combined fraction
    does not.

    Majority-area -- NOT `intersects`, NOT `within`:
      * `within` fails on Great Salt Lake, which spills 1.1% into a neighbouring
        HUC12 (frac_in = 0.989).
      * `intersects` returns True for a ZERO-interior-overlap boundary touch, which
        wrongly grabs lakes grazing a closed boundary at frac_in = 0.000 (Eagle Lake,
        Middle Alkali Lake). Do not "simplify" this predicate back to `intersects`.

    Degenerate geometry (null/empty rows, or a COMID whose rows sum to zero/
    negative area) raises `ValueError` instead of silently falling out via
    `NaN > min_frac == False` -- which would be indistinguishable from
    "legitimately not endorheic" and could hide an upstream CRS collapse across
    a whole batch. Mirrors the notna()/is_empty() guard in
    `depstor_builders/wbody_connectivity.py`.
    """
    if closed_gdf.empty:
        return set()

    n_before = len(wb_gdf)
    wb_gdf = wb_gdf[wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty]
    if n_before and wb_gdf.empty:
        raise ValueError(
            f"closed_basin_comids: all {n_before} waterbody geometries are "
            "null/empty -- this would silently classify a whole batch as "
            "not-endorheic instead of failing loud. Check the waterbody layer "
            "for an upstream CRS bug or bad extract."
        )
    if wb_gdf.empty:
        return set()

    closed = closed_gdf.to_crs(wb_gdf.crs) if closed_gdf.crs != wb_gdf.crs else closed_gdf
    union = closed.geometry.union_all()
    area = wb_gdf.geometry.area.groupby(wb_gdf["COMID"]).sum()
    inter = wb_gdf.geometry.intersection(union).area.groupby(wb_gdf["COMID"]).sum()
    if (area <= 0).any():
        bad = area.index[area <= 0].tolist()
        raise ValueError(
            f"closed_basin_comids: {len(bad)} COMID(s) have zero/negative total "
            f"area after summing all rows (e.g. {bad[:5]}) -- likely degenerate "
            "geometry or a CRS collapse upstream. Fail loud instead of letting "
            "them silently drop out of the closed-basin signal."
        )
    frac = inter / area
    return {int(c) for c in area.index[frac > min_frac]}


def terminal_cells(fdr_path: Path) -> gpd.GeoDataFrame:
    """Every FDR code-0 (terminal sink) cell, as a point on the FDR grid.

    These are the cells NHDPlus deliberately left UNFILLED in its HydroDEM, and they
    are what `d8_routing` already dead-ends at. The CONUS FDR has 15,262 of them.
    Scanned block-by-block: a full-grid array would be ~17 GB at CONUS scale.
    """
    xs, ys = [], []
    with rasterio.open(fdr_path) as src:
        crs = src.crs
        for _, win in src.block_windows(1):
            a = src.read(1, window=win)
            if not (a == 0).any():
                continue
            rows, cols = np.where(a == 0)
            x, y = rasterio.transform.xy(src.window_transform(win), rows, cols)
            xs.extend(np.atleast_1d(x))
            ys.extend(np.atleast_1d(y))
    return gpd.GeoDataFrame(
        geometry=[Point(x, y) for x, y in zip(xs, ys)], crs=crs
    )


def frac_own_for_window(
    fdr: np.ndarray, inside: np.ndarray, fdr_nodata: int
) -> float:
    """Share of the waterbody's cells whose D8 path reaches a terminus INSIDE it.

    `pour` is seeded from the terminal cells that fall inside the waterbody, then the
    same kernel `routing` uses resolves which cells reach one.
    """
    pour = np.zeros(fdr.shape, dtype=np.uint8)
    pour[inside & (fdr == 0)] = 1
    n_inside = int(inside.sum())
    if n_inside == 0 or pour.sum() == 0:
        return 0.0
    barrier = np.zeros(fdr.shape, dtype=np.uint8)
    # NOTE: drains_to_dprst_kernel returns a TUPLE (out, n_cycles).
    reach, _n_cycles = drains_to_dprst_kernel(fdr, pour, barrier, fdr_nodata=fdr_nodata)
    return float(((reach == 1) & inside).sum()) / n_inside


def terminus_own_fraction(
    wb_gdf: gpd.GeoDataFrame,
    fdr_path: Path,
    terminal: gpd.GeoDataFrame,
    logger=None,
) -> pd.DataFrame:
    """Per-COMID `frac_own` for every waterbody containing >= 1 terminal cell.

    Waterbodies with no terminal cell inside them cannot be endorheic under Signal A
    and are not evaluated (that is the cheap pre-filter — 6,429 of 448,124 CONUS
    waterbodies contain one).

    Returns columns: comid, n_terminal, frac_own.
    """
    hits = gpd.sjoin(
        terminal.to_crs(wb_gdf.crs), wb_gdf[["COMID", "geometry"]],
        how="inner", predicate="within",
    )
    counts = hits.groupby("COMID").size()
    cand = wb_gdf[wb_gdf["COMID"].isin(counts.index)].copy()
    # A multi-part waterbody appears as several rows sharing one COMID -- COMID is
    # NOT unique in the waterbody layer (measured on the real candidate set: 6,429
    # rows / 6,427 unique COMIDs). Dissolve so each COMID is evaluated as ONE
    # geometry: keeping only the largest row (e.g. `drop_duplicates` on area) would
    # silently discard the other rows' area -- and could throw away the very row
    # that holds the terminal cell that made the COMID a candidate in the first
    # place, wrongly zeroing frac_own. The largest dissolved bbox extent among real
    # multi-row candidates is 1.35 km, far below MIN_PAD_M, so this never grows the
    # window.
    cand = cand.dissolve(by="COMID", as_index=False)

    rows = []
    with rasterio.open(fdr_path) as src:
        nodata = int(src.nodata) if src.nodata is not None else 255
        for i, rec in enumerate(cand.itertuples()):
            b = rec.geometry.bounds
            pad = max(MIN_PAD_M, 0.5 * max(b[2] - b[0], b[3] - b[1]))
            win = from_bounds(
                b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad, transform=src.transform
            ).round_offsets().round_lengths()
            if win.width < 3 or win.height < 3:
                continue
            fdr = src.read(1, window=win, boundless=True, fill_value=nodata)
            inside = geometry_mask(
                [rec.geometry], out_shape=fdr.shape,
                transform=src.window_transform(win), invert=True,
            )
            rows.append({
                "comid": int(rec.COMID),
                "n_terminal": int(counts.loc[rec.COMID]),
                "frac_own": frac_own_for_window(fdr, inside, nodata),
            })
            if logger and (i + 1) % 500 == 0:
                logger.info("  terminus scan: %d/%d waterbodies", i + 1, len(cand))
    return pd.DataFrame(rows, columns=["comid", "n_terminal", "frac_own"])
