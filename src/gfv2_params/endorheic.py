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
# knob: frac_own is bimodal on the real CONUS run of this code -- 6,298 of the 6,427
# candidate COMIDs sit at >= 0.95 and only 10 fall in the whole 0.45-0.55 band -- and
# the demotion set moves ~3% across thresholds 0.3-0.7.
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

    The intersection is computed only for the waterbodies an STRtree/`sjoin` says
    touch the closed union AT ALL (a few thousand of the 448,124). Everything else
    has intersection area 0 by construction, so this changes no result -- but the
    unfiltered elementwise GEOS overlay measured ~17 s per 2,000 rows, i.e. hours of
    single-threaded work inside the 384 G depstor job.
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
    # A duplicated index would make the positional candidate lookup below ambiguous.
    wb_gdf = wb_gdf.reset_index(drop=True)

    closed = closed_gdf.to_crs(wb_gdf.crs) if closed_gdf.crs != wb_gdf.crs else closed_gdf
    area = wb_gdf.geometry.area.groupby(wb_gdf["COMID"]).sum()
    if (area <= 0).any():
        bad = area.index[area <= 0].tolist()
        raise ValueError(
            f"closed_basin_comids: {len(bad)} COMID(s) have zero/negative total "
            f"area after summing all rows (e.g. {bad[:5]}) -- likely degenerate "
            "geometry or a CRS collapse upstream. Fail loud instead of letting "
            "them silently drop out of the closed-basin signal."
        )

    hits = wb_gdf.sindex.query(closed.geometry, predicate="intersects")[1]
    cand = wb_gdf.iloc[np.unique(hits)]
    if cand.empty:
        return set()
    union = closed.geometry.union_all()
    # Summed per COMID, then reindexed onto EVERY COMID: a COMID with rows both in and
    # out of the candidate set still divides its inside-area by its FULL area (the
    # majority-area-by-COMID semantics), and a COMID with no candidate row gets 0.0.
    inter = (
        cand.geometry.intersection(union).area.groupby(cand["COMID"]).sum()
        .reindex(area.index, fill_value=0.0)
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


def endorheic_frame(
    wb_gdf: gpd.GeoDataFrame,
    fdr_path: Path,
    closed_gdf: gpd.GeoDataFrame | None = None,
    min_frac: float = MIN_FRAC,
    logger=None,
) -> pd.DataFrame:
    """Combine Signal A and Signal B into one provenance-carrying frame.

    Columns: comid, frac_own, by_terminus, by_closed_huc12.

    Every waterbody Signal A EVALUATED (>= 1 FDR terminal cell inside it -- the
    cheap Signal-A prefilter in `terminus_own_fraction`) appears here, whether or
    not it cleared `min_frac` -- plus any additional COMID Signal B flags. This is
    load-bearing for `scripts/diagnose/endorheic_fixtures.py`'s threshold sweep: if
    only the FLAGGED union were persisted (as this used to do), a candidate at
    frac_own = 0.40 would never be written, and a sweep at threshold 0.3 would
    structurally undercount -- unable to tell an inert threshold from a broken one.
    `by_terminus`/`by_closed_huc12` are both False on an evaluated-but-unflagged
    row, so `load_endorheic_comids`'s existing `by_terminus | by_closed_huc12`
    filter still excludes it from the demotion set -- persisting the full
    distribution changes nothing about which COMIDs get demoted.
    """
    terminal = terminal_cells(fdr_path)
    if logger:
        logger.info("  %d FDR terminal (code-0) cells", len(terminal))
    own = terminus_own_fraction(wb_gdf, fdr_path, terminal, logger=logger)
    a = set(own.loc[own["frac_own"] > min_frac, "comid"].astype(int))
    b = closed_basin_comids(wb_gdf, closed_gdf, min_frac) if closed_gdf is not None else set()
    if logger:
        logger.info(
            "  Signal A (terminus-inside-itself): %d; Signal B (closed basin): %d; "
            "union: %d", len(a), len(b), len(a | b),
        )
    frac = dict(zip(own["comid"].astype(int), own["frac_own"]))
    # Every Signal-A-evaluated candidate is persisted (flagged or not), unioned with
    # anything Signal B flags that Signal A never evaluated (no terminal cell, so it
    # gets frac_own = 0.0 below -- Signal A simply never ran against it).
    comids = sorted(set(own["comid"].astype(int)) | b)
    return pd.DataFrame({
        "comid": pd.Series(comids, dtype="int64"),
        "frac_own": [float(frac.get(c, 0.0)) for c in comids],
        "by_terminus": [c in a for c in comids],
        "by_closed_huc12": [c in b for c in comids],
    })


def write_endorheic_comids(df: pd.DataFrame, out_path: Path) -> None:
    """Write the endorheic COMID table (with per-signal provenance).

    Dtypes are pinned so a legitimately EMPTY table (a domain with no closed basin)
    round-trips with the same schema as a populated one, instead of writing all-null
    columns that `load_endorheic_comids` then has to guess at.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.astype(
        {
            "comid": "int64",
            "frac_own": "float64",
            "by_terminus": "bool",
            "by_closed_huc12": "bool",
        }
    ).to_parquet(out_path, index=False)


def load_endorheic_comids(path: Path) -> set[int]:
    """Load the endorheic COMID set. An EMPTY set is a legitimate result.

    A domain with no closed basin has no endorheic waterbody — `tjc` (Texas-Gulf) is
    exactly that, with 4 FDR code-0 cells and 0 flagged waterbodies against 15,262 /
    thousands on `gfv2`. Raising here (as this used to) conflated "the classifier is
    broken" with "this domain legitimately has none" and bricked the whole `tjc`
    depstor DAG. The demotion then subtracts the empty set: a correct no-op.

    The protection against a SILENTLY empty result on a fabric that should have
    demotions (where a no-op would leave the Great Salt Lake on-stream) lives at the
    producing end instead — `depstor_builders/endorheic.py`'s `min_endorheic_comids`
    floor, which `gfv2` declares.

    A row only counts as a demotion if at least one signal actually flagged it.
    `endorheic_frame` also persists every Signal-A-EVALUATED candidate that was NOT
    flagged (both columns false) — so a threshold sweep over the full `frac_own`
    distribution is possible (see `scripts/diagnose/endorheic_fixtures.py`) — and
    this filter is what keeps that from silently changing the demotion set.
    """
    df = pd.read_parquet(path, columns=["comid", "by_terminus", "by_closed_huc12"])
    if df.empty:
        return set()
    flagged = df[df["by_terminus"].astype(bool) | df["by_closed_huc12"].astype(bool)]
    return {int(c) for c in flagged["comid"].to_numpy()}
