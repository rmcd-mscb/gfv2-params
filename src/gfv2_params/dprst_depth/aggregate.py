"""Per-HRU aggregation of `dprst_depth.tif` -> `dprst_depth_avg` (#173 Task 8).

`dprst_depth.tif` (Task 6/7's `burn.burn_depth`) burns each dprst polygon's
own V/A mean depth (metres) onto every dprst cell it covers. Because of that,
the AREA-WEIGHTED MEAN of `dprst_depth.tif` over an HRU's dprst cells reduces
exactly to that HRU's own aggregate Sum(V)/Sum(A) across every dprst polygon
it touches -- precisely the continuous zonal MEAN the existing
exactextract-based zonal runners already compute for elevation/slope/aspect
(`categorical=false` -> a `mean` column; see `zonal_runners/zonal.py` and
`scripts/derive_depstor_params.py`'s `run_mean_zonal`). This module owns the
two things that mean-zonal pass does NOT do:

  1. **Finalize** the raw exactextract output into the PRMS parameter:
     metres -> inches (`fill.M_TO_IN`, the same constant Task 5's fallback
     floor is expressed in), and fill every HRU with zero dprst cells
     (exactextract returns NaN -- no valid pixels intersect the HRU) with the
     constant floor, never a NaN. `finalize_depth_params` is a PURE function
     (no I/O), so it is unit-testable without invoking exactextract at all --
     the exactextract call itself is integration-tested against a real
     fabric in Task 10.

  2. **Provenance**: which fill `method` (`"measured"` /
     `"calibrated_hollister"` / `"regional_fill"` / `"constant_floor"` -- see
     `fill.fill_flat`) dominates an HRU's dprst area. `dprst_depth.tif` only
     carries the numeric depth, not the per-polygon method label, so this is
     NOT an exactextract stat -- it is a per-HRU AREA-WEIGHTED majority vote
     over the per-polygon method labels the builder already computed.
     `depstor_builders/dprst_depth.py` persists those labels as a companion
     GeoParquet (`dprst_depth_polygons.parquet`) precisely so this module can
     read them back without recomputing anything. `area_weighted_provenance`
     is a small geopandas vector overlay (intersection + per-(HRU, method)
     area sum + per-HRU argmax) -- standard vector GIS, not a second zonal
     engine.
"""
from __future__ import annotations

import logging

import geopandas as gpd
import numpy as np
import pandas as pd

from .fill import DEPTH_CAP_M, M_TO_IN

__all__ = [
    "NO_DPRST_CELLS",
    "UNKNOWN_PROVENANCE",
    "finalize_depth_params",
    "area_weighted_provenance",
]

logger = logging.getLogger(__name__)

# Provenance labels for the two cases area_weighted_provenance can't answer:
# an HRU with genuinely zero dprst area (nothing to vote over -- distinct
# from fill.py's per-polygon "constant_floor", which means a real dprst
# polygon's OWN depth had to be filled), and an HRU with a valid mean depth
# but no matching row in the caller-supplied provenance table (e.g.
# provenance_df wasn't computed/passed for this run).
NO_DPRST_CELLS = "no_dprst_cells"
UNKNOWN_PROVENANCE = "unknown"


def finalize_depth_params(
    zonal_df: pd.DataFrame,
    hru_ids,
    id_feature: str,
    floor_in: float = 49.0,
    provenance_df: pd.DataFrame | None = None,
    mean_column: str = "mean",
) -> pd.DataFrame:
    """Pure finalize: exactextract mean (metres) -> `dprst_depth_avg` (inches).

    Args:
        zonal_df: raw exactextract output, at least columns
            `[id_feature, mean_column]`. `mean_column` is NaN for an HRU with
            no valid (non-nodata) pixels -- i.e. `dprst_frac == 0`.
        hru_ids: the FULL universe of HRU ids for this fabric. Guarantees the
            output has exactly one row per HRU even if an HRU is entirely
            absent from `zonal_df` (e.g. a batch boundary edge case) --
            such an HRU is treated identically to a NaN-mean HRU.
        id_feature: the fabric's HRU id column name (e.g. `nat_hru_id`).
        floor_in: the constant fallback depth (inches) for HRUs with zero
            dprst cells. Matches `fill.fill_flat`'s per-polygon floor
            (`context.BuildContext.dprst_depth_floor_in`, NHM calibrated
            median = 49 in) -- same floor, applied at the HRU grain here
            instead of the polygon grain there.
        provenance_df: optional `[id_feature, "dprst_depth_provenance"]`
            (e.g. from `area_weighted_provenance`). HRUs missing from this
            table get `UNKNOWN_PROVENANCE`; HRUs with zero dprst cells always
            get `NO_DPRST_CELLS` regardless of what's in `provenance_df`.

    Returns:
        DataFrame `[id_feature, dprst_depth_avg, dprst_depth_provenance]`,
        one row per `hru_ids` entry, sorted by `id_feature`.
        `dprst_depth_avg` is NEVER NaN and always > 0.
    """
    if id_feature not in zonal_df.columns:
        raise KeyError(f"finalize_depth_params: zonal_df missing '{id_feature}'")
    if mean_column not in zonal_df.columns:
        raise KeyError(f"finalize_depth_params: zonal_df missing '{mean_column}'")
    if not np.isfinite(floor_in) or floor_in <= 0:
        raise ValueError(f"finalize_depth_params: floor_in must be > 0, got {floor_in}")

    hru_index = pd.Index(pd.unique(pd.Series(list(hru_ids))), name=id_feature)
    base = pd.DataFrame({id_feature: hru_index})

    merged = base.merge(
        zonal_df[[id_feature, mean_column]].rename(columns={mean_column: "_mean_m"}),
        on=id_feature,
        how="left",
    )

    no_dprst = merged["_mean_m"].isna()
    n_no_dprst = int(no_dprst.sum())

    mean_m = merged["_mean_m"].to_numpy(dtype=float)
    depth_in = np.where(no_dprst.to_numpy(), floor_in, mean_m * M_TO_IN)

    # Guard against a non-finite/non-positive value slipping through despite
    # a non-null mean (shouldn't happen -- burn_depth guarantees every burned
    # cell is > 0 -- but a PRMS parameter must never be NaN/<=0, so fail safe
    # to the floor rather than propagate a bad value).
    bad = ~np.isfinite(depth_in) | (depth_in <= 0)
    n_bad = int(bad.sum())
    if n_bad:
        logger.warning(
            "finalize_depth_params: %d HRU(s) resolved to a non-finite/non-positive "
            "depth despite a non-null mean -- forcing to the %.1f in floor "
            "(investigate upstream; burn_depth should guarantee positive depths)",
            n_bad, floor_in,
        )
        depth_in = np.where(bad, floor_in, depth_in)

    # #173 FIX 1 defensive backstop: an HRU's area-weighted mean cannot
    # exceed 300 in if every contributing polygon is already capped by
    # fill.fill_flat's DEPTH_CAP_M, but clamp + log defensively in case a
    # future upstream path bypasses that per-polygon cap (e.g. a raster
    # burned from an uncapped polygon set).
    depth_cap_in = DEPTH_CAP_M * M_TO_IN
    over_cap = depth_in > depth_cap_in
    n_over_cap = int(np.sum(over_cap))
    if n_over_cap:
        # Log the OVERAGE MAGNITUDE alongside the count (#173 PR#177 review
        # FIX 5) so float32-rounding noise just past the cap (e.g. 300.001
        # in) is distinguishable at a glance from a real bug (e.g. 400+ in,
        # which would mean the per-polygon cap was bypassed upstream).
        max_over_cap_in = float(np.max(depth_in[over_cap]))
        logger.warning(
            "finalize_depth_params: %d HRU(s) exceeded the %.1f in physical cap despite "
            "per-polygon capping upstream (max observed %.4f in, %.4f in over cap) -- "
            "clamping (investigate: fill.fill_flat's DEPTH_CAP_M should make this "
            "impossible)",
            n_over_cap, depth_cap_in, max_over_cap_in, max_over_cap_in - depth_cap_in,
        )
        depth_in = np.where(over_cap, depth_cap_in, depth_in)

    merged["dprst_depth_avg"] = depth_in

    if provenance_df is not None and len(provenance_df):
        missing = {id_feature, "dprst_depth_provenance"} - set(provenance_df.columns)
        if missing:
            raise KeyError(f"finalize_depth_params: provenance_df missing columns {sorted(missing)}")
        merged = merged.merge(
            provenance_df[[id_feature, "dprst_depth_provenance"]], on=id_feature, how="left",
        )
    else:
        merged["dprst_depth_provenance"] = pd.array([pd.NA] * len(merged), dtype=object)

    merged.loc[no_dprst, "dprst_depth_provenance"] = NO_DPRST_CELLS
    still_missing = merged["dprst_depth_provenance"].isna()
    merged.loc[still_missing, "dprst_depth_provenance"] = UNKNOWN_PROVENANCE

    out = (
        merged[[id_feature, "dprst_depth_avg", "dprst_depth_provenance"]]
        .sort_values(id_feature)
        .reset_index(drop=True)
    )

    logger.info(
        "finalize_depth_params: %d HRUs (%d with dprst cells, %d floored at %.1f in)",
        len(out), len(out) - n_no_dprst, n_no_dprst, floor_in,
    )
    return out


def area_weighted_provenance(
    polygons_gdf: gpd.GeoDataFrame, hru_gdf: gpd.GeoDataFrame, id_feature: str,
) -> pd.DataFrame:
    """Per-HRU dominant `dprst_depth` fill `method`, by intersected area.

    For every HRU, intersects it against every dprst polygon (in whatever
    CRS `hru_gdf` is in -- `polygons_gdf` is reprojected to match if needed),
    sums the intersected area per `(id_feature, method)`, and keeps the
    `method` with the largest summed area per HRU -- an area-weighted
    majority vote, consistent with `dprst_depth_avg` itself being an
    area-weighted mean.

    Args:
        polygons_gdf: dprst polygon set with a `method` column (e.g. loaded
            from `depstor_builders/dprst_depth.py`'s companion
            `dprst_depth_polygons.parquet`).
        hru_gdf: HRU polygons with an `id_feature` column.
        id_feature: the fabric's HRU id column name.

    Returns:
        DataFrame `[id_feature, dprst_depth_provenance]`, at most one row per
        HRU that has ANY dprst-polygon overlap (HRUs with none are simply
        absent -- `finalize_depth_params` maps those to `NO_DPRST_CELLS`).
    """
    if "method" not in polygons_gdf.columns:
        raise KeyError("area_weighted_provenance: polygons_gdf missing 'method'")
    if id_feature not in hru_gdf.columns:
        raise KeyError(f"area_weighted_provenance: hru_gdf missing '{id_feature}'")
    if polygons_gdf.crs is None or hru_gdf.crs is None:
        raise ValueError("area_weighted_provenance: both inputs must have a CRS")

    empty = pd.DataFrame(
        {
            id_feature: pd.Series([], dtype=hru_gdf[id_feature].dtype),
            "dprst_depth_provenance": pd.Series([], dtype=object),
        }
    )
    if len(polygons_gdf) == 0 or len(hru_gdf) == 0:
        return empty

    polys = polygons_gdf[["method", "geometry"]]
    if polys.crs != hru_gdf.crs:
        polys = polys.to_crs(hru_gdf.crs)

    overlay = gpd.overlay(
        polys, hru_gdf[[id_feature, "geometry"]], how="intersection", keep_geom_type=False,
    )
    overlay = overlay[overlay.geometry.notna() & ~overlay.geometry.is_empty]
    if len(overlay) == 0:
        return empty

    overlay = overlay.copy()
    overlay["_area"] = overlay.geometry.area
    grouped = overlay.groupby([id_feature, "method"], as_index=False)["_area"].sum()
    dominant_idx = grouped.groupby(id_feature)["_area"].idxmax()
    dominant = (
        grouped.loc[dominant_idx, [id_feature, "method"]]
        .rename(columns={"method": "dprst_depth_provenance"})
        .reset_index(drop=True)
    )
    return dominant
