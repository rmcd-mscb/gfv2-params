"""Segment-driven on-stream waterbody classifier.

A waterbody is ON-STREAM iff a stream segment from the MODEL's own routing network
(the fabric's `segments_gpkg`) intersects it with POSITIVE LENGTH. This replaces the
NHD-flowline question ("is this waterbody on the NHD network?") with the one the
parameters actually need ("is it on the network the model routes?"): NHD's network is
far finer, so a waterbody NHD routes but the model does not had no representation at
all -- neither stream routing nor depression storage.

Positive length, not bare `intersects`: a segment grazing a shoreline at a single
point promotes the whole waterbody, the same failure mode `CLAUDE.md` warns about for
containment tests. On the retired `conus_waterbodies.gpkg` 19.0% of oregon's candidate
pairs were zero-length grazes; on the current `nhd_waterbodies.gpkg` it is 3.1% (2,300
of 73,343 CONUS pairs), so the rule is cheap insurance against shoreline-vintage
mis-registration rather than a large correction. WBAREACOMI corroborates it: of the 6
COMIDs it drops in oregon, 4 have no NHD signal at all and 2 are Playa.

Two consequences of "positive length" are DELIBERATE, verified against shapely, and
pinned by tests:

  * a segment COLLINEAR with the shoreline has positive length and IS promoted --
    a line the modeller drew along the lake edge is a routing decision;
  * a segment TERMINATING inside a waterbody IS promoted, whereas NHD's flow-through
    rule required both inflow and outflow. That discrimination is gone here, so the
    `endorheic` subtraction in `wbody_connectivity` is what still demotes genuinely
    closed terminal lakes.

This module is FTYPE-agnostic on purpose. The Playa/Ice Mass never-on-stream guardrail
lives in `wbody_connectivity`, at the chokepoint both this source and the opt-in NHD
comparison sources pass through.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

# Pair intersections are computed in chunks so ONE unrepairable geometry cannot abort a
# whole CONUS run (73,343 candidate pairs). Big enough that the vectorised GEOS call
# dominates, small enough to bound the per-row fallback.
CHUNK = 5000

# Floor on a "positive length" intersection, in metres. NUMERICAL HYGIENE, not a
# hydrologic threshold -- do NOT tune it.
#
# GEOS resolves a segment grazing a waterbody's boundary to a length that is
# mathematically zero but numerically a hair above it. Measured on the real oregon
# fabric: 8 of 770 kept COMIDs came in between 2.3e-10 m and 7.7e-9 m -- boundary
# touches, which is the EXACT case the positive-length rule exists to exclude (see
# the module docstring). A bare `> 0` test admits them on float noise alone; after the
# Playa guardrail, 4 waterbodies / 2.05 km2 were on-stream on the strength of a
# 0.2-nanometre "traversal".
#
# The populations are cleanly bimodal, which is what makes the constant safe rather than
# arbitrary: on oregon the artifacts top out at 7.7e-9 m and the smallest GENUINE
# traversal is 5.4 m — about nine orders of magnitude of empty space in between. This
# does not cut through a continuum of borderline cases; there are no borderline cases.
#
# 1 micron is ~11 orders of magnitude below the 30 m grid and below any real NHD
# vertex spacing, so it can only ever reject artifacts: it encodes no judgment about
# WHICH grazes matter hydrologically. That judgment WAS considered as a cell-scale
# threshold (cell_size * sqrt(2) = 42.4 m) and deliberately rejected as a tuning knob
# this classifier must not have -- this is a different thing, and the distinction is
# why it is safe.
MIN_OVERLAP_M = 1e-6

_PAIR_DTYPES = {
    "comid": "int64",
    "wb_index": "int64",
    "seg_index": "int64",
    "overlap_m": "float64",
}
_FRAME_DTYPES = {"comid": "int64", "n_segments": "int64", "overlap_m": "float64"}


def _empty_pairs() -> pd.DataFrame:
    return pd.DataFrame({k: pd.array([], dtype=v) for k, v in _PAIR_DTYPES.items()})


def repair_invalid(gdf: gpd.GeoDataFrame, *, name: str, logger=None):
    """Return `(gdf, n_repaired)` with `shapely.make_valid` applied to invalid rows.

    Found the hard way: the CONUS measurement run died with
    `GEOSException: TopologyException: side location conflict` after 73,343 candidate
    pairs. `sjoin` survives invalid polygons because prepared predicates are tolerant;
    `.intersection()` does not. 193 of the 448,124 CONUS waterbody polygons need this.
    Oregon exposes only 10, and a synthetic test none at all, so this is a CONUS-only
    failure -- the same family as the NHD measured-3D XYZM -> `Point()` crash (2a67d85).

    Raises if a geometry survives `make_valid` still invalid: proceeding would take the
    per-row fallback in `_overlap_lengths` for every pair it appears in, and a NaN
    overlap scored as zero-length would demote a waterbody to dprst on a geometry error.
    """
    geoms = np.asarray(gdf.geometry.values)
    invalid = ~shapely.is_valid(geoms)
    n_invalid = int(invalid.sum())
    if not n_invalid:
        return gdf, 0
    geoms = geoms.copy()
    geoms[invalid] = shapely.make_valid(geoms[invalid])
    n_still = int((~shapely.is_valid(geoms)).sum())
    if n_still:
        raise ValueError(
            f"{n_still} {name} geometr(ies) are still invalid after make_valid — the "
            f"intersection-length pass cannot measure them, and scoring an unmeasurable "
            f"pair as zero-length would silently demote a waterbody to depression "
            f"storage. Fix the source layer."
        )
    if logger:
        logger.info("  %s: repaired %d invalid geometr(ies) with make_valid", name, n_invalid)
    return gdf.set_geometry(
        gpd.GeoSeries(geoms, index=gdf.index, crs=gdf.crs)
    ), n_invalid


def _overlap_lengths(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    """Intersection length per pair, chunked, with a per-row fallback.

    A GEOS failure inside one chunk must not lose the other 4,999 pairs, so the chunk is
    retried row by row and only the genuinely unmeasurable pair is scored NaN. The caller
    RAISES on any NaN -- see `segment_waterbody_pairs`.
    """
    out = np.full(len(left), np.nan, dtype="float64")
    for start in range(0, len(left), CHUNK):
        stop = min(start + CHUNK, len(left))
        try:
            out[start:stop] = shapely.length(
                shapely.intersection(left[start:stop], right[start:stop])
            )
        except Exception:
            for i in range(start, stop):
                try:
                    out[i] = shapely.length(shapely.intersection(left[i], right[i]))
                except Exception:
                    out[i] = np.nan
    return out


def segment_waterbody_pairs(seg_gdf, wb_gdf, *, logger=None) -> pd.DataFrame:
    """One row per intersecting (segment, waterbody-ROW) pair, with `overlap_m`.

    Columns: comid, wb_index, seg_index, overlap_m.

    Keyed on the waterbody ROW index, never on COMID: the layer holds 448,124 rows for
    447,907 distinct COMIDs, so merging pair rows back on COMID duplicates them (it
    inflated the measurement script's CONUS pair count from 73,343 to 73,723). Harmless
    for a COMID set, but it would corrupt `n_segments` and `overlap_m`.
    """
    if "COMID" not in wb_gdf.columns:
        raise KeyError(
            "waterbody layer has no COMID column — the segment on-stream classifier "
            "emits a COMID table and cannot run. Use a fabric whose waterbody layer "
            "carries COMID (e.g. `gfv2`, `oregon`), not the NHM_01_draft `wbs` layer."
        )
    if seg_gdf.crs is None or wb_gdf.crs is None:
        raise ValueError(
            f"the segment on-stream classifier needs a CRS on both layers to measure an "
            f"intersection length (segments: {seg_gdf.crs}, waterbodies: {wb_gdf.crs}). "
            f"Without one the layers could be in different units and every pair would be "
            f"mis-measured, silently."
        )
    if not wb_gdf.crs.is_projected:
        raise ValueError(
            f"the waterbody layer is in the geographic CRS {wb_gdf.crs}; the "
            f"positive-length rule is a LENGTH and must be measured in a projected CRS "
            f"(the pipeline uses EPSG:5070). In degrees the comparison against zero would "
            f"still 'work' while the threshold meant nothing."
        )
    if seg_gdf.crs != wb_gdf.crs:
        if logger:
            logger.info(
                "  reprojecting segments from %s to the waterbody CRS %s",
                seg_gdf.crs, wb_gdf.crs,
            )
        seg_gdf = seg_gdf.to_crs(wb_gdf.crs)

    seg_ok = seg_gdf.geometry.notna() & ~seg_gdf.geometry.is_empty
    wb_ok = wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty
    seg = gpd.GeoDataFrame(
        geometry=seg_gdf.geometry[seg_ok].reset_index(drop=True), crs=wb_gdf.crs
    )
    wb = gpd.GeoDataFrame(
        {"COMID": pd.to_numeric(wb_gdf["COMID"][wb_ok], errors="coerce").to_numpy()},
        geometry=wb_gdf.geometry[wb_ok].reset_index(drop=True),
        crs=wb_gdf.crs,
    )
    if seg.empty or wb.empty:
        return _empty_pairs()

    seg, _ = repair_invalid(seg, name="segments", logger=logger)
    wb, _ = repair_invalid(wb, name="waterbodies", logger=logger)

    joined = gpd.sjoin(seg, wb[["geometry"]], how="inner", predicate="intersects")
    if joined.empty:
        return _empty_pairs()
    seg_index = joined.index.to_numpy()
    wb_index = joined["index_right"].to_numpy()
    if logger:
        logger.info("  %d candidate (segment, waterbody) pairs", len(joined))

    overlap = _overlap_lengths(
        np.asarray(seg.geometry.values)[seg_index],
        np.asarray(wb.geometry.values)[wb_index],
    )
    n_unmeasurable = int(np.isnan(overlap).sum())
    if n_unmeasurable:
        raise ValueError(
            f"{n_unmeasurable} of {len(overlap)} (segment, waterbody) pairs could not be "
            f"intersected even after make_valid. Refusing to score them: an unmeasurable "
            f"pair treated as zero-length would demote its waterbody to depression "
            f"storage on a geometry error rather than a hydrologic result."
        )

    comid = wb["COMID"].to_numpy()[wb_index]
    pairs = pd.DataFrame(
        {
            "comid": comid,
            "wb_index": wb_index.astype("int64"),
            "seg_index": seg_index.astype("int64"),
            "overlap_m": overlap,
        }
    )
    n_bad = int(pairs["comid"].isna().sum())
    if n_bad:
        pairs = pairs[pairs["comid"].notna()]
        if logger:
            logger.warning(
                "  dropped %d pair(s) whose waterbody COMID is non-numeric/NaN", n_bad
            )
    n_zero = int((pairs["overlap_m"] <= MIN_OVERLAP_M).sum())
    if logger:
        logger.info(
            "  %d of %d pairs are zero-length grazes (%.1f%%) and are dropped",
            n_zero, len(pairs), 100.0 * n_zero / max(len(pairs), 1),
        )
    return pairs.astype(_PAIR_DTYPES).reset_index(drop=True)


def segment_waterbody_comids(pairs: pd.DataFrame) -> set[int]:
    """COMIDs with at least one POSITIVE-LENGTH pair — the on-stream set.

    "Positive" means above `MIN_OVERLAP_M`, not above 0.0: see that constant for why a
    bare `> 0` test admits GEOS boundary-touch artifacts.
    """
    if pairs.empty:
        return set()
    return {
        int(c)
        for c in pairs.loc[pairs["overlap_m"] > MIN_OVERLAP_M, "comid"].unique()
    }


def segment_comid_frame(pairs: pd.DataFrame) -> pd.DataFrame:
    """Per-COMID aggregate over POSITIVE-LENGTH pairs: comid, n_segments, overlap_m.

    `n_segments` counts DISTINCT segments, so a multi-part waterbody (several rows
    sharing a COMID) crossed by one segment reports 1, not one per row.
    """
    positive = pairs[pairs["overlap_m"] > MIN_OVERLAP_M] if not pairs.empty else pairs
    if positive.empty:
        return pd.DataFrame({k: pd.array([], dtype=v) for k, v in _FRAME_DTYPES.items()})
    return (
        positive.groupby("comid", as_index=False)
        .agg(n_segments=("seg_index", "nunique"), overlap_m=("overlap_m", "sum"))
        .astype(_FRAME_DTYPES)
    )


def write_segment_comids(df: pd.DataFrame, out_path: Path) -> None:
    """Write the on-stream COMID table with pinned dtypes.

    Dtypes are pinned so an empty table round-trips with the same schema as a populated
    one, instead of writing all-null columns `load_segment_comids` has to guess at.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.astype(_FRAME_DTYPES).to_parquet(out_path, index=False)


def load_segment_comids(path: Path) -> set[int]:
    """Load the segment-derived on-stream COMID set."""
    df = pd.read_parquet(path, columns=["comid"])
    if df.empty:
        return set()
    return {int(c) for c in df["comid"].to_numpy()}


def check_onstream_floor(n: int, *, fabric: str, floor: int | None, source) -> None:
    """Raise if a fabric declaring `min_onstream_comids` has a collapsed on-stream set.

    Opt-in per fabric, exactly like `min_endorheic_comids`: `gfv2` measures 48,529 and
    `oregon` 770, while a small fabric may legitimately have few. Applied at BOTH the
    producing builder and the consuming `wbody_connectivity`, because `--from
    wbody_connectivity` (the documented cascade-rebuild recipe) leaves the producer out
    of the run list and the orchestrator hydrates its table off disk unvalidated — a
    guard living only in the producer never runs on the path operators actually use.
    """
    if floor is None:
        return
    if n < floor:
        raise ValueError(
            f"segment on-stream table for fabric '{fabric}' carries {n} COMIDs, below its "
            f"declared `min_onstream_comids` floor of {floor} ({source}). That is a "
            f"collapsed classifier: nearly every waterbody would become depression "
            f"storage. Check that `segments_gpkg` covers the fabric domain and shares a "
            f"projected CRS with `waterbody_gpkg`, then re-run the `segment_wbody` step "
            f"with --force. Lower or remove `min_onstream_comids` in the fabric profile "
            f"ONLY if this domain genuinely has that few on-stream waterbodies."
        )
