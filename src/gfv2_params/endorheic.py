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

Two terms, used consistently throughout this module and the docs, because they name
different sets and are easy to confuse:

  FLAGGED  -- a signal called this COMID endorheic. 22,942 on the shipped CONUS table.
              This is what `min_endorheic_comids` floors and what `endorheic_wbody.tif`
              rasterizes.
  DEMOTED  -- a FLAGGED COMID that was also ON-STREAM, so the subtraction in
              `wbody_connectivity` actually removed it. 818 on CONUS. Every demoted
              COMID is flagged; most flagged COMIDs were never on-stream to begin with,
              so the subtraction is a no-op for them.

Signal B -- majority-inside a WBD type-C (closed) HUC12. Earns its place because
    Walker Lake contains no FDR terminal cell, so Signal A misses it -- but on the
    shipped CONUS tables it is not a minor complement: of the 818 DEMOTIONS, 543 are
    Signal-B-only, 112 Signal-A-only, and 163 both. BY COUNT, Signal B dominates.
    BY AREA it does not: Signal-B-only demotions are small (median ~0.09 km2,
    ~1,400 km2 total) -- ponds and playas sitting inside a closed basin, not large
    lakes -- while Signal A carries the overwhelming majority of the demoted area,
    including the Great Salt Lake itself (4,369 km2).

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

# The signal columns of the endorheic table. A row is a DEMOTION iff at least one of
# them is True; a row with all-False columns is a Signal-A-EVALUATED candidate that did
# not clear MIN_FRAC (persisted for the threshold sweep -- see `endorheic_frame`).
#
# Every consumer must agree on this predicate, which is why it lives here once rather
# than being spelled out at each site. The failure it prevents is asymmetric and silent:
# a new signal column that `check_endorheic_floor` counted but `load_endorheic_comids`
# did not would let a collapsed demotion clear the floor while demoting nothing.
SIGNAL_COLUMNS = ("by_terminus", "by_closed_huc12")


def flagged(df: pd.DataFrame) -> pd.Series:
    """Rows at least one signal actually flagged (a demotion), vs merely evaluated."""
    return df[list(SIGNAL_COLUMNS)].astype(bool).any(axis=1)


def signal_counts(df: pd.DataFrame) -> dict[str, int]:
    """Flagged-row counts: `total` (the union) plus one entry per signal column."""
    counts = {"total": int(flagged(df).sum())}
    counts.update({c: int(df[c].astype(bool).sum()) for c in SIGNAL_COLUMNS})
    return counts


def read_signal_counts(path: Path) -> dict[str, int]:
    """`signal_counts` for a persisted endorheic table."""
    return signal_counts(pd.read_parquet(path, columns=["comid", *SIGNAL_COLUMNS]))


def check_endorheic_floor(
    counts: dict[str, int],
    *,
    fabric: str,
    floor: int | None,
    signal_b_active: bool,
    source,
) -> None:
    """Raise if a fabric that declares `min_endorheic_comids` has a collapsed result.

    An EMPTY endorheic table is legitimate for a domain with no closed basin (`tjc`,
    Texas-Gulf), so this cannot be a blanket raise — it is opt-in per fabric. `gfv2`
    declares the floor, so a silently-empty or collapsed CONUS result can never slip
    through: the demotion would be a no-op and the Great Salt Lake would stay on-stream.

    Each SIGNAL is floored as well as the union, because the union alone cannot see one
    signal die. Signal B contributes 543 of the 818 CONUS demotions BY COUNT while
    Signal A carries almost all of the demoted AREA (the Great Salt Lake alone is
    4,369 km2 of it), so a TOTAL collapse of Signal A still leaves the union far above
    any count-based floor while ~75% of the demoted area silently disappears.

    Called from BOTH the producing end (`depstor_builders/endorheic.py`) and the
    consuming end (`depstor_builders/wbody_connectivity.py`). The consuming end is not
    redundant: `--from wbody_connectivity --force` — the documented cascade-rebuild
    recipe (slurm_batch/RUNME.md) — does not run the `endorheic` step at all. The
    orchestrator hydrates its table straight off disk with no validation, so a check
    that lives only in the producing builder never executes on that path.
    """
    if floor is None:
        return
    if counts["total"] < floor:
        raise ValueError(
            f"endorheic table for fabric '{fabric}' carries {counts['total']} flagged "
            f"COMIDs, below its declared `min_endorheic_comids` floor of {floor} "
            f"({source}). That is a collapsed or no-op demotion — the Great Salt Lake "
            f"would stay on-stream. Check that fdr_raster has code-0 cells, that the "
            f"waterbody layer overlaps it, and that wbd_huc12_table is staged; re-run "
            f"the `endorheic` step with --force. Lower/remove `min_endorheic_comids` in "
            f"the fabric profile ONLY if this domain genuinely has no closed basin."
        )
    if counts["by_terminus"] == 0:
        raise ValueError(
            f"endorheic table for fabric '{fabric}' has {counts['total']} flagged COMIDs "
            f"but NOT ONE from Signal A (terminus-inside-itself) ({source}). Signal A "
            f"always runs and carries almost all of the demoted area — the Great Salt "
            f"Lake is a Signal-A demotion — so a total Signal-A collapse leaves the "
            f"count-based `min_endorheic_comids` floor satisfied by Signal B alone while "
            f"the demoted AREA silently vanishes. Check that fdr_raster has code-0 cells "
            f"and shares a CRS with the waterbody layer, then re-run `endorheic --force`."
        )
    if signal_b_active and counts["by_closed_huc12"] == 0:
        raise ValueError(
            f"endorheic table for fabric '{fabric}' has {counts['total']} flagged COMIDs "
            f"but NOT ONE from Signal B (majority-inside a closed HUC12) ({source}), "
            f"even though `wbd_huc12_table` is configured — i.e. the operator asserted "
            f"Signal B should run. On CONUS it flags the majority of demotions by count "
            f"(543 of 818). A zero here means the staged WBD yielded no type-C rows "
            f"(check HU_12_TYPE) or the table is stale; re-stage with "
            f"`python -m gfv2_params.download.wbd_huc12` and re-run `endorheic --force`."
        )


def closed_basin_comids(
    wb_gdf: gpd.GeoDataFrame,
    closed_gdf: gpd.GeoDataFrame,
    min_frac: float = MIN_FRAC,
) -> set[int]:
    """COMIDs of waterbodies majority-inside the DISSOLVED union of closed HUC12s.

    Dissolve first, then measure: a lake straddling two *adjacent* closed HUC12s is
    fully inside the closed system but majority-inside neither polygon on its own.

    A waterbody is a COMID, not a row: the waterbody layer stores multi-part
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

    Everything below works in the FDR's CRS. The read window AND the `inside` mask are
    computed against the FDR's transform from bounds taken in the waterbody's CRS, so on
    a mismatch every window lands at arbitrary coordinates -- and `boundless=True`
    returns an all-nodata block rather than raising, which makes `frac_own` come out 0.0
    for EVERY candidate. Signal A would then flag nothing, silently, and be
    indistinguishable from a domain with no closed basin. `_assert_overlaps_fdr` cannot
    catch that (it reprojects the FDR bbox into the waterbody CRS before testing
    overlap, so a mismatch passes it cleanly), so reproject up front rather than trust
    the caller -- `depstor.rasterize_binary` guards the same way for the same reason.
    """
    with rasterio.open(fdr_path) as src:
        fdr_crs, fdr_nodata = src.crs, src.nodata
    if fdr_crs is None or wb_gdf.crs is None:
        raise ValueError(
            f"Signal A needs a CRS on both the FDR grid ({fdr_path}: {fdr_crs}) and the "
            f"waterbody layer ({wb_gdf.crs}) to place its read windows. Without one, "
            f"every window would land off-grid and the classifier would flag nothing, "
            f"silently."
        )
    if wb_gdf.crs != fdr_crs:
        if logger:
            logger.info(
                "  reprojecting waterbodies from %s to the FDR's %s for Signal A",
                wb_gdf.crs, fdr_crs,
            )
        wb_gdf = wb_gdf.to_crs(fdr_crs)

    hits = gpd.sjoin(
        terminal.to_crs(fdr_crs), wb_gdf[["COMID", "geometry"]],
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
    n_degenerate_window = 0
    n_no_raster_terminus = 0
    with rasterio.open(fdr_path) as src:
        nodata = int(fdr_nodata) if fdr_nodata is not None else 255
        for i, rec in enumerate(cand.itertuples()):
            b = rec.geometry.bounds
            pad = max(MIN_PAD_M, 0.5 * max(b[2] - b[0], b[3] - b[1]))
            win = from_bounds(
                b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad, transform=src.transform
            ).round_offsets().round_lengths()
            if win.width < 3 or win.height < 3:
                # Structurally impossible on a sane grid (MIN_PAD_M alone is 20 km), so
                # this means a degenerate transform or NaN bounds -- count it rather
                # than dropping the candidate out of the table unremarked.
                n_degenerate_window += 1
                continue
            fdr = src.read(1, window=win, boundless=True, fill_value=nodata)
            inside = geometry_mask(
                [rec.geometry], out_shape=fdr.shape,
                transform=src.window_transform(win), invert=True,
            )
            # `sjoin` put a terminal point INSIDE this polygon, so the rasterized view
            # of the same polygon disagreeing with the vector one is an inconsistency,
            # not a hydrologic result -- frac_own is forced to 0.0 either way, which is
            # indistinguishable from "legitimately not endorheic". A few are expected
            # (geometry_mask is cell-centre based, so a sub-cell polygon can rasterize
            # to nothing); a large share means the grid and the layer are misaligned.
            if not bool((inside & (fdr == 0)).any()):
                n_no_raster_terminus += 1
            rows.append({
                "comid": int(rec.COMID),
                "n_terminal": int(counts.loc[rec.COMID]),
                "frac_own": frac_own_for_window(fdr, inside, nodata),
            })
            if logger and (i + 1) % 500 == 0:
                logger.info("  terminus scan: %d/%d waterbodies", i + 1, len(cand))
    if logger and n_degenerate_window:
        logger.warning(
            "  %d of %d Signal-A candidates produced a sub-3-cell read window and were "
            "DROPPED from the table — expect 0; a degenerate transform or NaN bounds.",
            n_degenerate_window, len(cand),
        )
    if logger and n_no_raster_terminus:
        logger.warning(
            "  %d of %d Signal-A candidates contain a terminal cell by vector test but "
            "none once rasterized (frac_own forced to 0.0). A handful is expected for "
            "sub-cell polygons; a large share means the waterbody layer and the FDR "
            "grid are misaligned.", n_no_raster_terminus, len(cand),
        )
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

    Every waterbody Signal A EVALUATED (>= 1 FDR terminal cell inside it -- the cheap
    Signal-A prefilter in `terminus_own_fraction`) appears here, whether or not it
    cleared `min_frac`, plus any additional COMID Signal B flags. Persisting the
    unflagged candidates too is load-bearing for the threshold sweep in
    `scripts/diagnose/endorheic_fixtures.py`: with only the flagged union on disk, a
    candidate at frac_own = 0.40 would be absent, and a sweep at threshold 0.3 would
    structurally undercount -- unable to tell an inert threshold from a broken one.

    It costs nothing downstream: both signal columns are False on an
    evaluated-but-unflagged row, so `flagged()` -- the single predicate the loader, the
    floor and the sweep all share -- excludes it from the demotion set. Which COMIDs get
    demoted is unaffected by what else is persisted alongside them.
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
    thousands on `gfv2`. An empty set must therefore NOT raise here: it would conflate
    "the classifier is broken" with "this domain legitimately has none" and brick the
    whole `tjc` depstor DAG, which lives in the fabric-independent depstor config. The
    demotion simply subtracts the empty set — a correct no-op.

    The protection against a SILENTLY empty result on a fabric that SHOULD have
    demotions (where the no-op would leave the Great Salt Lake on-stream) is the
    `min_endorheic_comids` floor instead — see `check_endorheic_floor`, which is applied
    at both the producing and the consuming end.

    Only FLAGGED rows count (`flagged()`): `endorheic_frame` also persists every
    Signal-A-evaluated candidate that no signal flagged, and this filter is what keeps
    those out of the demotion set.
    """
    df = pd.read_parquet(path, columns=["comid", *SIGNAL_COLUMNS])
    if df.empty:
        return set()
    return {int(c) for c in df.loc[flagged(df), "comid"].to_numpy()}
