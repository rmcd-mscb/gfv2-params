"""Diagnostic probe for Issue #173 Phase 0 spike: dprst_depth_avg from
best-available topography. Analysis-only; not a builder. See
docs/superpowers/specs/2026-07-10-dprst-depth-phase0-spike-design.md.
"""
from __future__ import annotations

import argparse
import gc
import subprocess
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
from rasterio.warp import transform_bounds
from scipy import stats as scipy_stats

from gfv2_params.depstor import load_connected_comids
from gfv2_params.dprst_depth.topo import (
    depth_to_spill,
    dprst_polygons,
    is_hydroflattened,
    lake_max_depth,
    max_to_mean,
    read_window,
    resolution_class,
    volume_mean_depth,
)

# --- CONUS coverage audit (--audit) -----------------------------------------
#
# WESM (Work Extent Spatial Metadata) is the authoritative 3DEP workunit
# footprint index (~3,258 workunits, ~3.6 GB GeoPackage). The task brief
# points at the S3 object via `/vsis3/prd-tnm/...`, but two GDAL access paths
# were tried and rejected on this cluster before falling back to a local
# download:
#   - `/vsis3/...` (anonymous request): GDAL's GeoPackage driver issues a
#     metadata-table probe query on open (`SELECT COUNT(*) FROM sqlite_master
#     WHERE name IN ('gpkg_metadata', ...)`) that raises "attempt to write a
#     readonly database", reproduced with both fiona and
#     `gdal.OpenEx(..., GA_ReadOnly)`.
#   - `/vsicurl/https://prd-tnm.s3.amazonaws.com/...` (plain HTTPS, sidesteps
#     the /vsis3/ driver bug above and opens fine): but a full-layer read
#     over this path raised "database disk image is malformed" partway
#     through — reproduced on both a full feature scan and a pushed-down SQL
#     GROUP BY. GeoPackage's SQLite b-tree access pattern issues many
#     scattered small-range HTTP requests across a 3.6 GB file, which is not
#     reliable on this network.
# A one-time plain HTTPS download to local disk (`ensure_wesm_local`) reads
# the same authoritative S3 object as an ordinary local SQLite file once
# fetched, avoiding both failure modes.
WESM_HTTPS_URL = (
    "https://prd-tnm.s3.amazonaws.com/"
    "StagedProducts/Elevation/metadata/WESM.gpkg"
)


def ensure_wesm_local(cache_dir: Path, logger, url: str = WESM_HTTPS_URL) -> Path:
    """Download (or reuse a cached copy of) WESM.gpkg and return its local path.

    See the module-level note above for why this reads over plain HTTPS to
    local disk rather than a GDAL `/vsis3/` or `/vsicurl/` virtual path.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = cache_dir / "WESM.gpkg"
    if local_path.exists():
        logger.info("Using cached WESM index: %s", local_path)
        return local_path
    tmp_path = cache_dir / "WESM.gpkg.part"
    logger.info("Downloading WESM workunit index (~3.6 GB) to %s ...", local_path)
    subprocess.run(["curl", "-fsS", "-o", str(tmp_path), url], check=True)
    tmp_path.rename(local_path)
    logger.info("  download complete: %s", local_path)
    return local_path


def load_wesm_1m_footprints(logger, path: Path, batch_size: int = 100) -> gpd.GeoDataFrame:
    """Read the WESM workunit index and keep only 1 m/QL1/QL2-qualifying footprints.

    WESM covers every 3DEP workunit regardless of quality (legacy 3-30 m
    LiDAR projects included). Its own `onemeter_category` field flags whether
    a workunit meets the 3DEP 1 m DEM spec; observed values on this download
    are {"Meets", "Meets with variance", "Does not meet", "Pending
    publication"}. "Meets"/"Meets with variance" are both effectively
    published 1 m/QL1/QL2 product footprints (variance = minor spec
    deviation, still 1 m data); "Does not meet" is legacy coarser LiDAR and
    "Pending publication" is not yet downloadable — both are excluded from
    the "1m" tier `resolution_class` keys on.

    Reads in two passes: a geometry-free attribute scan of all ~3,258
    workunits (cheap — logs the full category breakdown), then a batched
    geometry read filtered by a pushed-down SQL WHERE to *only* the
    qualifying ~1,790 rows. Each batch's geometries are immediately
    collapsed to their convex hull before the next batch is read.

    Two earlier, simpler versions both OOM-killed on the HPC login node's
    session memory cgroup (~11 GB observed ceiling): reading every
    workunit's full-precision geometry (including the ~1,468
    legacy/pending ones this function discards) cost ~9.5 GB; even after
    WHERE-pushdown to just the 1,790 qualifying rows it still cost ~7.6 GB.
    Some WESM workunit footprints are the un-dissolved union of every
    constituent LAS-tile rectangle (thousands of polygon parts each
    carrying GEOS/Shapely per-part object overhead) rather than a single
    simplified outline — `.simplify()` does not reduce part count, only
    per-ring vertex count, so it would not have helped. The convex hull
    collapses each workunit to one simple polygon. For a Phase-0 coverage
    *audit* (not a per-pixel data-availability claim), this is an
    acceptable, one-directional approximation: it can only ever tag a
    dprst polygon "1m" that the true (non-convex) footprint would have
    left "10m", never the reverse.
    """
    logger.info("Reading WESM workunit footprint index: %s", path)
    attrs = pyogrio.read_dataframe(
        str(path), columns=["onemeter_category"], read_geometry=False,
    )
    logger.info("  %d total WESM workunits", len(attrs))
    breakdown = attrs["onemeter_category"].value_counts().to_dict()
    logger.info("  onemeter_category breakdown: %s", breakdown)
    del attrs

    qualifying = ("Meets", "Meets with variance")
    where = "onemeter_category IN ({})".format(", ".join(f"'{v}'" for v in qualifying))
    parts = []
    offset = 0
    while True:
        batch = gpd.read_file(
            path, columns=["workunit", "onemeter_category"], where=where,
            skip_features=offset, max_features=batch_size,
        )
        if len(batch) == 0:
            break
        batch["geometry"] = batch.geometry.convex_hull
        parts.append(batch)
        offset += batch_size
    if not parts:
        raise ValueError(
            "0 WESM workunits meet the 1 m spec — onemeter_category values may "
            "have changed; refusing to silently report 0% 1m coverage."
        )
    onem = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=parts[0].crs)
    del parts
    logger.info(
        "  %d workunits qualify as 1 m/QL1/QL2 (onemeter_category in %s); "
        "footprints simplified to convex hulls",
        len(onem), qualifying,
    )
    return onem


def _read_vector(path, layer, columns, logger):
    """geopandas.read_file with the pyarrow-backed pyogrio engine, falling
    back to fiona if pyarrow is unavailable (mirrors wbody_connectivity.py)."""
    try:
        return gpd.read_file(path, layer=layer, columns=columns, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer, columns=columns)


def assign_vpu(
    dprst_gdf: gpd.GeoDataFrame, hru_gpkg, hru_layer: str, logger,
    batch_size: int = 50_000,
) -> pd.Series:
    """Tag each dprst polygon with the VPU of the HRU its centroid falls in.

    Pure-vector centroid-in-polygon join against the fabric's `vpu` attribute
    (no vpu_id raster — the fabric has no vector VPU-boundary layer of its
    own, so the HRU polygons stand in for one). Reads the 361k-polygon HRU
    fabric in sequential batches rather than one whole-fabric GeoDataFrame: a
    single full read OOM-killed on the HPC login node's per-session memory
    cgroup (~11 GB observed ceiling) once its geometries sat alongside the
    dprst frame and WESM index already in memory. Each batch's sjoin result
    is folded in and the batch discarded before the next read.
    """
    pts = dprst_gdf.set_geometry(dprst_gdf.geometry.centroid)
    assigned = pd.Series(pd.NA, index=dprst_gdf.index, dtype="object")
    crs = None
    offset = 0
    n_hru = 0
    while True:
        chunk = gpd.read_file(
            hru_gpkg, layer=hru_layer, columns=["vpu"],
            skip_features=offset, max_features=batch_size, use_arrow=True,
        )
        if len(chunk) == 0:
            break
        n_hru += len(chunk)
        if crs is None:
            crs = chunk.crs
            if crs != pts.crs:
                logger.info("  Reprojecting HRU batches from %s to %s", crs, pts.crs)
        if chunk.crs != pts.crs:
            chunk = chunk.to_crs(pts.crs)
        hit = gpd.sjoin(pts, chunk[["vpu", "geometry"]], how="inner", predicate="within")
        assigned.loc[hit.index] = hit["vpu"].to_numpy()
        offset += batch_size
        del chunk, hit
    logger.info("  %d HRU polygons scanned in batches of %d", n_hru, batch_size)
    return assigned


def load_conus_dprst(base: dict, logger) -> gpd.GeoDataFrame:
    """Reconstruct the shipped CONUS dprst polygon set (`wbody_connectivity`
    -> `dprst`) with an `area_km2` column.

    Shared by `run_audit` (Task 2) and `run_flatness` (Task 4) so both
    measure the exact same shipped classification — pulled out of `run_audit`
    verbatim (issue #173 Task 4), no behavioural change to the audit path.
    """
    from gfv2_params.config import require_config_key

    waterbody_gpkg = require_config_key(base, "waterbody_gpkg", "dprst_depth_probe")
    waterbody_layer = require_config_key(base, "waterbody_layer", "dprst_depth_probe")
    connected_table = Path(require_config_key(base, "connected_comids_table", "dprst_depth_probe"))
    flowthrough_table = base.get("flowthrough_comids_table")

    connected = load_connected_comids(connected_table)
    n_wbareacomi = len(connected)
    n_flowthrough = 0
    if flowthrough_table is not None:
        flowthrough_table = Path(flowthrough_table)
        flowthrough = load_connected_comids(flowthrough_table)
        n_flowthrough = len(flowthrough - connected)
        connected = connected | flowthrough
    logger.info(
        "connected COMIDs: %d WBAREACOMI + %d new flow-through = %d total",
        n_wbareacomi, n_flowthrough, len(connected),
    )

    logger.info("Reading waterbodies: %s (layer=%s)", waterbody_gpkg, waterbody_layer)
    wb_gdf = _read_vector(
        waterbody_gpkg, waterbody_layer,
        ["COMID", "FTYPE", "member_comid"],
        logger,
    )
    logger.info("  %d waterbody polygons", len(wb_gdf))

    dprst = dprst_polygons(wb_gdf, connected)
    del wb_gdf  # 448k full-CONUS waterbody polygons no longer needed
    gc.collect()
    dprst["area_km2"] = dprst.geometry.area / 1e6
    total_polys = len(dprst)
    total_km2 = float(dprst["area_km2"].sum())
    logger.info(
        "dprst polygons (reconstructed shipped set): %d, total area: %.1f km^2",
        total_polys, total_km2,
    )
    # Sanity check against the issue's measured figure (~285,998 polygons /
    # ~53,159 km^2). Never silently cap or truncate — just log loud if this
    # run is wildly off (more than 3x either direction), since that would
    # signal an upstream input drift, not something to paper over here.
    ref_polys, ref_km2 = 285_998, 53_159
    if not (ref_polys / 3 <= total_polys <= ref_polys * 3):
        logger.warning(
            "dprst polygon count %d is >3x off the issue's reference figure "
            "%d — check inputs before trusting this audit.", total_polys, ref_polys,
        )
    if not (ref_km2 / 3 <= total_km2 <= ref_km2 * 3):
        logger.warning(
            "dprst total area %.1f km^2 is >3x off the issue's reference figure "
            "%d km^2 — check inputs before trusting this audit.", total_km2, ref_km2,
        )
    return dprst


def run_audit(base: dict, logger, wesm_cache_dir: Path) -> pd.DataFrame:
    """Reconstruct the CONUS dprst polygon set, tag best-available topo
    resolution and VPU, log the national split, and return the per-VPU table.
    """
    from gfv2_params.config import require_config_key

    hru_gpkg = require_config_key(base, "hru_gpkg", "dprst_depth_probe")
    hru_layer = require_config_key(base, "hru_layer", "dprst_depth_probe")

    dprst = load_conus_dprst(base, logger)
    total_polys = len(dprst)
    total_km2 = float(dprst["area_km2"].sum())

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    wesm = load_wesm_1m_footprints(logger, wesm_path)
    dprst = resolution_class(dprst, wesm)
    del wesm
    gc.collect()

    logger.info("Assigning VPU via HRU fabric (batched): %s (layer=%s)", hru_gpkg, hru_layer)
    dprst["vpu"] = assign_vpu(dprst, hru_gpkg, hru_layer, logger).to_numpy()
    n_unassigned = int(dprst["vpu"].isna().sum())
    if n_unassigned:
        logger.warning(
            "%d/%d dprst polygon centroids fell outside every HRU (no VPU "
            "assigned) — likely coastal/edge slivers; kept as 'unassigned'.",
            n_unassigned, total_polys,
        )
        dprst["vpu"] = dprst["vpu"].fillna("unassigned")

    # National split.
    national = dprst.groupby("best_topo").agg(n=("COMID", "size"), area_km2=("area_km2", "sum"))
    national["pct_n"] = 100.0 * national["n"] / total_polys
    national["pct_area"] = 100.0 * national["area_km2"] / total_km2
    logger.warning(
        "1m%% figures below are a convex-hull UPPER BOUND: WESM multi-part "
        "workunit footprints are collapsed to their convex hull before the "
        "spatial join (see load_wesm_1m_footprints docstring), which can "
        "only OVERSTATE 1m coverage, never understate it. True 1m coverage "
        "may be lower.",
    )
    for topo in ("1m", "10m"):
        if topo in national.index:
            row = national.loc[topo]
            logger.info(
                "  national best_topo=%s: %d polys (%.1f%%), %.1f km^2 (%.1f%%)",
                topo, int(row["n"]), row["pct_n"], row["area_km2"], row["pct_area"],
            )

    # Per-VPU table: count/area at 1m vs 10m, plus the % split within VPU.
    per_vpu = (
        dprst.groupby(["vpu", "best_topo"])
        .agg(n=("COMID", "size"), area_km2=("area_km2", "sum"))
        .reset_index()
    )
    vpu_totals = per_vpu.groupby("vpu").agg(n_total=("n", "sum"), area_km2_total=("area_km2", "sum"))
    per_vpu = per_vpu.join(vpu_totals, on="vpu")
    per_vpu["pct_n"] = 100.0 * per_vpu["n"] / per_vpu["n_total"]
    per_vpu["pct_area"] = 100.0 * per_vpu["area_km2"] / per_vpu["area_km2_total"]
    per_vpu = per_vpu.sort_values(["vpu", "best_topo"]).reset_index(drop=True)
    return per_vpu


# --- Flatness detector: ND validation + SwampMarsh verdict (--flatness, Task 4) --

# North Dakota Prairie Pothole study area (issue #173 design doc: "Prairie
# Pothole Region, North Dakota ... also Hay and others 2018 PRMS
# depression-storage calibration site"). A generous state-level bbox in
# EPSG:4326 (minx, miny, maxx, maxy) — used only to restrict the CONUS dprst
# set and the WESM 1 m footprint index before picking a project, not as a
# precise study-area boundary.
ND_BBOX_4326 = (-104.05, 45.93, -96.55, 49.0)

# The four FTYPEs the spike must settle a per-FTYPE flatness verdict for
# (issue #173 design doc decision table). Order matters only for logging.
FLATNESS_FTYPES = ("SwampMarsh", "LakePond", "Playa", "Reservoir")


def select_nd_project(
    dprst_gdf: gpd.GeoDataFrame, wesm_path: Path, logger, top_n_log: int = 5,
) -> tuple[str, gpd.GeoDataFrame]:
    """Pick the North Dakota Prairie Pothole 1 m WESM project with the
    densest gfv2-dprst polygon overlap — NOT hardcoded (issue #173 design:
    "chosen programmatically ... not hardcoded").

    Restricts both the CONUS dprst polygon set and the WESM 1 m footprint
    index to `ND_BBOX_4326`, spatial-joins dprst polygon centroids against
    project footprints (real per-workunit geometry, not the audit path's
    convex-hull simplification — this bbox-restricted read is cheap enough
    to skip that optimization), and returns the project name with the most
    contained centroids plus the dprst subset that falls in it. Logs the top
    `top_n_log` candidates by overlap count so the choice is auditable, not
    just "first project matching a name filter" (the shortcut Task 3's smoke
    test used).
    """
    bbox_5070 = transform_bounds("EPSG:4326", "EPSG:5070", *ND_BBOX_4326, densify_pts=21)
    nd_dprst = dprst_gdf.cx[bbox_5070[0]:bbox_5070[2], bbox_5070[1]:bbox_5070[3]]
    logger.info(
        "  %d/%d dprst polygons fall in the ND Prairie Pothole bbox",
        len(nd_dprst), len(dprst_gdf),
    )

    bbox_4269 = transform_bounds("EPSG:4326", "EPSG:4269", *ND_BBOX_4326, densify_pts=21)
    wesm_nd = gpd.read_file(
        wesm_path, columns=["project", "onemeter_category"],
        where="onemeter_category IN ('Meets', 'Meets with variance')",
        bbox=bbox_4269,
    )
    logger.info("  %d 1m WESM workunit footprints intersect the ND bbox", len(wesm_nd))
    wesm_nd = wesm_nd.to_crs(nd_dprst.crs)

    pts = nd_dprst.set_geometry(nd_dprst.geometry.centroid)
    hit = gpd.sjoin(pts, wesm_nd[["project", "geometry"]], how="inner", predicate="within")
    hit = hit[~hit.index.duplicated(keep="first")]  # a centroid may land in >1 workunit footprint
    counts = hit.groupby("project").size().sort_values(ascending=False)
    if counts.empty:
        raise RuntimeError(
            "no 1m WESM project overlaps any dprst polygon centroid in the ND bbox "
            f"{ND_BBOX_4326} — cannot pick a study project."
        )
    logger.info(
        "  top ND 1m projects by dprst-polygon overlap:\n%s",
        counts.head(top_n_log).to_string(),
    )
    chosen = str(counts.index[0])
    logger.info(
        "  chosen project (densest dprst overlap): %s (%d dprst polygons)",
        chosen, int(counts.iloc[0]),
    )
    project_idx = hit.index[hit["project"] == chosen]
    return chosen, nd_dprst.loc[project_idx].copy()


def sample_per_ftype(
    gdf: gpd.GeoDataFrame, n_per_ftype: int, logger,
    ftypes: tuple[str, ...] = FLATNESS_FTYPES, seed: int = 173,
) -> dict[str, gpd.GeoDataFrame]:
    """Sample up to `n_per_ftype` dprst polygons per FTYPE, logging the exact
    per-FTYPE sample size actually used. NEVER silently caps below what's
    available — if a FTYPE has fewer than `n_per_ftype` polygons, every
    available polygon is used and that is logged explicitly.
    """
    rng = np.random.default_rng(seed)
    samples: dict[str, gpd.GeoDataFrame] = {}
    for ftype in ftypes:
        sub = gdf[gdf["FTYPE"] == ftype]
        n_avail = len(sub)
        if n_avail == 0:
            logger.warning(
                "  FTYPE=%s: 0 polygons available in the chosen project — skipped entirely",
                ftype,
            )
            samples[ftype] = sub.iloc[0:0]
            continue
        n = min(n_per_ftype, n_avail)
        if n_avail < n_per_ftype:
            logger.info(
                "  FTYPE=%s: only %d available (< target %d) — sampling all of them",
                ftype, n_avail, n_per_ftype,
            )
        else:
            logger.info("  FTYPE=%s: sampling %d of %d available", ftype, n, n_avail)
        idx = rng.choice(sub.index.to_numpy(), size=n, replace=False)
        samples[ftype] = sub.loc[idx]
    return samples


def _interior_mask(dem: np.ndarray, transform, geom, sentinel: float = -9999.0) -> np.ndarray:
    """Boolean mask of `dem` cells whose centre lies inside `geom` (the raw,
    unbuffered dprst polygon) and are not the nodata sentinel.

    `read_window`'s DEM window covers `geom.bounds` padded by `rim_buffer_m`
    on every side; rasterizing the *unbuffered* polygon geometry onto that
    same transform is exactly "exclude the rim buffer, keep only the
    polygon interior" — no separate erosion needed, the rim buffer only
    exists outside `geom` in the first place. Pulled out of `_interior_values`
    (issue #173 Task 5) so `run_freeboard` can reuse the identical interior
    definition Task 4's flatness detector uses, rather than re-deriving it —
    `_interior_values` (below) is now a thin wrapper over this mask.
    """
    from rasterio.features import geometry_mask

    if dem.size == 0:
        return np.zeros(dem.shape, dtype=bool)
    mask = geometry_mask([geom], out_shape=dem.shape, transform=transform, invert=True)
    mask &= dem != sentinel
    return mask


def _interior_values(dem: np.ndarray, transform, geom, sentinel: float = -9999.0) -> np.ndarray:
    """Interior (non-rim-buffer, non-nodata) `dem` values as a flat float64 array.

    See `_interior_mask` for the mask definition this wraps.
    """
    if dem.size == 0:
        return np.empty(0, dtype=np.float64)
    mask = _interior_mask(dem, transform, geom, sentinel)
    return dem[mask].astype(np.float64)


def analyze_flatness_sample(
    samples: dict[str, gpd.GeoDataFrame], project: str, logger, min_cells: int = 4,
) -> pd.DataFrame:
    """Run `read_window` -> interior mask -> `is_hydroflattened` over every
    sampled polygon and return one row per polygon (per-FTYPE aggregation is
    done by the caller from this table).
    """
    rows = []
    n_total = sum(len(v) for v in samples.values())
    n_done = 0
    for ftype, sub in samples.items():
        for _, row in sub.iterrows():
            geom = row.geometry
            comid = row["COMID"] if "COMID" in sub.columns else row.name
            n_done += 1
            try:
                dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
            except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
                logger.warning("  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                                n_done, n_total, ftype, comid, exc)
                continue
            vals = _interior_values(dem, transform, geom)
            if vals.size < min_cells:
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: only %d interior cells (< %d) — skipped",
                    n_done, n_total, ftype, comid, vals.size, min_cells,
                )
                continue
            stats = is_hydroflattened(vals)
            stats.update({
                "COMID": comid, "FTYPE": ftype, "n_cells": int(vals.size),
                "resolution": source["resolution"],
            })
            rows.append(stats)
            if n_done % 25 == 0 or n_done == n_total:
                logger.info("  [%d/%d] sampled so far (last: FTYPE=%s COMID=%s flat=%s range=%.4f)",
                            n_done, n_total, ftype, comid, stats["flat"], stats["range"])
    return pd.DataFrame(rows)


def summarize_by_ftype(per_polygon: pd.DataFrame, logger) -> pd.DataFrame:
    """Per-FTYPE flattened fraction + range/std/n_unique distributions —
    the decision table input for the SwampMarsh verdict."""
    if per_polygon.empty:
        raise RuntimeError("flatness sample produced 0 usable polygons — cannot summarize")
    agg = per_polygon.groupby("FTYPE").agg(
        n_sampled=("COMID", "size"),
        n_flat=("flat", "sum"),
        pct_flat=("flat", "mean"),
        range_median=("range", "median"),
        range_p90=("range", lambda s: float(np.percentile(s, 90))),
        std_median=("std", "median"),
        n_unique_median=("n_unique", "median"),
        pct_1m=("resolution", lambda s: float((s == "1m").mean())),
    ).reset_index()
    agg["pct_flat"] = 100.0 * agg["pct_flat"]
    agg["pct_1m"] = 100.0 * agg["pct_1m"]
    agg = agg.sort_values("FTYPE").reset_index(drop=True)
    for _, r in agg.iterrows():
        logger.info(
            "  FTYPE=%-10s n=%4d flat=%5.1f%% (range median=%.4f m, p90=%.4f m) 1m-read=%.1f%%",
            r["FTYPE"], int(r["n_sampled"]), r["pct_flat"], r["range_median"], r["range_p90"], r["pct_1m"],
        )
    return agg


def _write_separability_histogram(per_polygon: pd.DataFrame, out_png: Path, logger) -> None:
    """Log-scale histogram of interior elevation range, split flat vs
    natural — the visual evidence that the detector cleanly separates
    hydro-flattened (range ~ 0) from bare-earth (range >> tol_m) surfaces."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    flat_ranges = per_polygon.loc[per_polygon["flat"], "range"].clip(lower=1e-4)
    natural_ranges = per_polygon.loc[~per_polygon["flat"], "range"].clip(lower=1e-4)
    bins = np.logspace(-4, np.log10(max(per_polygon["range"].max(), 1.0) + 1e-6), 60)
    ax.hist(flat_ranges, bins=bins, alpha=0.7, label=f"flat (n={len(flat_ranges)})", color="#1f77b4")
    ax.hist(natural_ranges, bins=bins, alpha=0.7, label=f"natural (n={len(natural_ranges)})", color="#d62728")
    ax.axvline(0.01, color="black", linestyle="--", linewidth=1, label="tol_m = 0.01 m")
    ax.set_xscale("log")
    ax.set_xlabel("interior elevation range (m, log scale)")
    ax.set_ylabel("polygon count")
    ax.set_title("Hydro-flattening detector separability (ND dprst sample)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote separability histogram: %s", out_png)


def run_flatness(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path, n_per_ftype: int = 300,
) -> pd.DataFrame:
    """Issue #173 tasks 2+3: validate the flatness detector on a real ND
    sample and settle the SwampMarsh hydro-flattening question.

    Reconstructs the CONUS dprst set, restricts to the ND Prairie Pothole
    bbox, programmatically picks the 1 m project with the densest overlap
    (`select_nd_project`), samples up to `n_per_ftype` polygons per FTYPE
    (`sample_per_ftype`), reads each at 1 m and tests interior flatness
    (`analyze_flatness_sample`), and writes `flatness_by_ftype.csv` +
    `flatness_separability.png` to `out_dir`. Returns the per-FTYPE summary.
    """
    dprst = load_conus_dprst(base, logger)

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info("Selecting ND 1m project with densest dprst overlap ...")
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    logger.info("Sampling up to %d dprst polygons per FTYPE from project=%s ...", n_per_ftype, project)
    samples = sample_per_ftype(nd_dprst, n_per_ftype, logger)
    n_total = sum(len(v) for v in samples.values())
    logger.info("Total sample size across all FTYPEs: %d", n_total)

    logger.info("Reading each sampled polygon at 1m + testing interior flatness ...")
    per_polygon = analyze_flatness_sample(samples, project, logger)
    logger.info(
        "Flatness reads completed: %d/%d polygons produced a usable interior sample",
        len(per_polygon), n_total,
    )

    summary = summarize_by_ftype(per_polygon, logger)

    if "SwampMarsh" in summary["FTYPE"].to_numpy():
        sm = summary.loc[summary["FTYPE"] == "SwampMarsh"].iloc[0]
        logger.info(
            "*** SwampMarsh verdict: %.1f%% of sampled polygons are hydro-flattened "
            "(n=%d) — decides whether at-risk dprst area is ~89%% or ~38.5%% ***",
            sm["pct_flat"], int(sm["n_sampled"]),
        )
    else:
        logger.warning(
            "*** SwampMarsh verdict: UNDETERMINED — 0 SwampMarsh polygons in the "
            "chosen project's sample; cannot settle the headline question from this "
            "ND project alone ***",
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "flatness_by_ftype.csv"
    caveat = (
        f"# ND Prairie Pothole flatness validation (issue #173 Task 4). "
        f"Project={project}. Per-polygon sample: "
        + ", ".join(f"{k}={len(v)}" for k, v in samples.items())
        + ". flat = interior elevation range < 0.01 m (is_hydroflattened tol_m).\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        summary.to_csv(f, index=False)
    logger.info("Wrote per-FTYPE flatness summary: %s", out_csv)

    per_polygon_csv = out_dir / "flatness_per_polygon.csv"
    per_polygon.to_csv(per_polygon_csv, index=False)
    logger.info("Wrote per-polygon flatness detail: %s", per_polygon_csv)

    _write_separability_histogram(per_polygon, out_dir / "flatness_separability.png", logger)

    return summary


# --- Freeboard quantification over Task 4's detected-flat sample (--freeboard, Task 5) --


def analyze_freeboard_sample(
    flat_df: pd.DataFrame, dprst_lookup: gpd.GeoDataFrame, project: str, logger,
) -> pd.DataFrame:
    """Run `read_window` -> `depth_to_spill` -> interior mask -> `volume_mean_depth`
    over every Task-4-detected-flat polygon and return one row per polygon.

    `flat_df` is the `flat == True` slice of Task 4's `flatness_per_polygon.csv`
    (COMID + FTYPE only — the cached CSV has no geometry); `dprst_lookup` is the
    Task 4 ND dprst subset (`select_nd_project`'s return), indexed by COMID, used
    only to recover each flat polygon's geometry. Mean freeboard = filled - raw
    averaged over the polygon interior (`_interior_mask`, Task 4's helper) — the
    above-water spill storage a hydro-flattened breakline surface hides above its
    constant water-surface elevation.
    """
    rows = []
    n_total = len(flat_df)
    n_done = 0
    for row in flat_df.itertuples(index=False):
        comid = row.COMID
        ftype = row.FTYPE
        n_done += 1
        if comid not in dprst_lookup.index:
            logger.warning(
                "  [%d/%d] COMID=%s: not found in the ND dprst subset — skipped",
                n_done, n_total, comid,
            )
            continue
        geom = dprst_lookup.loc[comid, "geometry"]
        try:
            dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
        except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
            logger.warning(
                "  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                n_done, n_total, ftype, comid, exc,
            )
            continue
        mask = _interior_mask(dem, transform, geom)
        n_interior = int(mask.sum())
        if n_interior == 0:
            logger.warning(
                "  [%d/%d] FTYPE=%s COMID=%s: 0 interior cells after masking — skipped",
                n_done, n_total, ftype, comid,
            )
            continue
        depth = depth_to_spill(dem)
        cell_area_m2 = abs(transform.a * transform.e)
        v, a, mean_d = volume_mean_depth(depth, mask, cell_area_m2)
        frac_wet = float((depth[mask] > 0).mean())
        rows.append({
            "COMID": comid,
            "FTYPE": ftype,
            "area_m2": a,
            "mean_freeboard_m": mean_d,
            "mean_freeboard_in": mean_d * 39.3701,
            "frac_interior_wet": frac_wet,
            "n_interior_cells": n_interior,
            "resolution": source["resolution"],
        })
        if n_done % 25 == 0 or n_done == n_total:
            logger.info(
                "  [%d/%d] FTYPE=%s COMID=%s freeboard=%.3f in (frac_wet=%.2f)",
                n_done, n_total, ftype, comid, mean_d * 39.3701, frac_wet,
            )
    return pd.DataFrame(rows)


def _write_freeboard_cdf(result: pd.DataFrame, out_png: Path, logger) -> None:
    """CDF of mean freeboard (inches) across detected-flat polygons — the
    visual evidence for the Task 5 finding (routinely non-trivial vs ~0)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    vals = np.sort(result["mean_freeboard_in"].to_numpy())
    cdf = np.arange(1, len(vals) + 1) / len(vals)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(vals, cdf, drawstyle="steps-post", color="#1f77b4")
    ax.axvline(0.0, color="black", linestyle="--", linewidth=1, label="0 in (no freeboard)")
    ax.set_xlabel("mean freeboard (in)")
    ax.set_ylabel("cumulative fraction of detected-flat polygons")
    ax.set_title(f"Freeboard CDF over hydro-flattened dprst polygons (n={len(vals)})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote freeboard CDF: %s", out_png)


def run_freeboard(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path, limit: int | None = None,
) -> pd.DataFrame:
    """Issue #173 Task 5: quantify freeboard (filled - raw) over Task 4's
    detected-flat (hydro-flattened) dprst polygon sample.

    Reads the cached `flatness_per_polygon.csv` Task 4 wrote to `out_dir`
    rather than recomputing the flatness sample from scratch — FAILS LOUD
    if it is absent (that sample costs ~731 live /vsicurl/ reads, ~20 min;
    `--freeboard` is a downstream analysis over its cached result, not a
    standalone rerun). Filters to `flat == True` (~105/731 on the ND
    project, issue #173 Task 4), reconstructs the identical ND dprst subset
    + project (`select_nd_project`, same as Task 4) purely to recover each
    flat COMID's geometry (the per-polygon CSV is COMID/FTYPE/stats only —
    CSVs don't carry geometry), then runs `analyze_freeboard_sample` and
    writes `freeboard_dist.csv` + `freeboard_cdf.png`.

    `limit`, if given, caps the number of flat polygons analyzed (head of
    the flat slice) — for smoke-testing the read/compute path cheaply
    without paying for the full ~105-polygon read.
    """
    flat_csv = out_dir / "flatness_per_polygon.csv"
    if not flat_csv.exists():
        raise RuntimeError(
            f"{flat_csv} not found — --freeboard reads Task 4's flatness sample "
            "and does NOT recompute it (that sample costs ~731 live /vsicurl/ "
            f"reads, ~20 min). Run `--flatness --out-dir {out_dir}` first, then "
            "re-run --freeboard."
        )
    per_polygon = pd.read_csv(flat_csv)
    if "flat" not in per_polygon.columns or "COMID" not in per_polygon.columns:
        raise KeyError(
            f"{flat_csv} is missing 'flat'/'COMID' columns — not a Task 4 "
            "flatness_per_polygon.csv?"
        )
    flat_df = per_polygon[per_polygon["flat"].astype(bool)].copy()
    if limit is not None:
        flat_df = flat_df.head(limit)
    n_flat = len(flat_df)
    logger.info(
        "Loaded %d detected-flat polygons from %s (of %d total sampled)%s",
        n_flat, flat_csv, len(per_polygon),
        f" [--limit {limit}]" if limit is not None else "",
    )
    if n_flat == 0:
        raise RuntimeError(f"0 flat=True polygons in {flat_csv} — nothing to analyze")

    dprst = load_conus_dprst(base, logger)
    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info(
        "Reselecting the ND 1m project + dprst subset (Task 4's select_nd_project) "
        "to recover flat-polygon geometries ..."
    )
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    if "COMID" not in nd_dprst.columns:
        raise KeyError("ND dprst subset has no COMID column; cannot match Task 4's flat-polygon list")
    lookup = nd_dprst.drop_duplicates(subset="COMID").set_index("COMID")

    logger.info("Reading each detected-flat polygon at 1m + computing freeboard ...")
    result = analyze_freeboard_sample(flat_df, lookup, project, logger)
    n_ok = len(result)
    logger.info("Freeboard reads completed: %d/%d flat polygons produced a usable result", n_ok, n_flat)
    if result.empty:
        raise RuntimeError("freeboard analysis produced 0 usable polygons")

    med = float(result["mean_freeboard_in"].median())
    p10 = float(np.percentile(result["mean_freeboard_in"], 10))
    p90 = float(np.percentile(result["mean_freeboard_in"], 90))
    logger.info(
        "Freeboard distribution (inches) over detected-flat polygons: median=%.2f, "
        "p10=%.2f, p90=%.2f (n=%d)",
        med, p10, p90, n_ok,
    )
    if med < 1.0:
        logger.info(
            "*** Freeboard finding: median ~%.2f in is essentially ZERO — flattened "
            "ponds are outlet-controlled; baseline depth_to_spill on the RAW rim does "
            "NOT capture their storage, the terrain model must carry the submerged "
            "volume ***",
            med,
        )
    else:
        logger.info(
            "*** Freeboard finding: median ~%.2f in is NON-TRIVIAL — baseline "
            "depth_to_spill already captures meaningful above-water spill storage for "
            "flattened ponds ***",
            med,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "freeboard_dist.csv"
    caveat = (
        f"# Freeboard (filled - raw) over Task 4 detected-flat dprst polygons "
        f"(issue #173 Task 5). Project={project}. n_flat_input={n_flat}"
        + (f" (--limit {limit})" if limit is not None else "")
        + f", n_usable={n_ok}. mean_freeboard = volume_mean_depth(depth_to_spill(dem)) "
        "over the polygon interior (rim_buffer_m terrain and nodata excluded via "
        "_interior_mask).\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        result.to_csv(f, index=False)
    logger.info("Wrote per-polygon freeboard distribution: %s", out_csv)

    _write_freeboard_cdf(result, out_dir / "freeboard_cdf.png", logger)

    return result


# --- Hollister terrain-slope max-depth prototype (--hollister, Task 6) -----
#
# `lake_max_depth`/`max_to_mean` are geometry, not raster I/O — this section
# is the plumbing that runs them over a real ND sample using the same
# `select_nd_project`/`sample_per_ftype`/`read_window`/`_interior_mask`
# helpers Tasks 4-5 already validated, rather than re-deriving sampling or
# masking logic. Restricted to FTYPE=LakePond (issue #173 Task 6: "a
# LakePond sample from the ND project") — SwampMarsh/Playa/Reservoir are out
# of scope here.
HOLLISTER_FTYPES = ("LakePond",)


def analyze_hollister_sample(
    samples: dict[str, gpd.GeoDataFrame], project: str, logger,
) -> pd.DataFrame:
    """Run `read_window` -> interior polygon mask -> `lake_max_depth` ->
    `max_to_mean` over every sampled polygon and return one row per polygon.

    `max_depth_m` is the Hollister/lakeMorpho terrain-slope-extension
    estimate (mean shoreline-ring slope x max in-polygon distance-to-shore);
    `mean_depth_m` is the conical V/A conversion (`max_to_mean(..., "cone")`,
    factor 1/3) documented in the module docstring above `lake_max_depth`.
    Polygon area is taken from the sampled GeoDataFrame's own `area_km2`
    column (computed once, CONUS-wide, in `load_conus_dprst`) rather than
    re-derived from the raster window, so it matches the shipped dprst area
    exactly regardless of read/mask resolution.
    """
    rows = []
    n_total = sum(len(v) for v in samples.values())
    n_done = 0
    for ftype, sub in samples.items():
        for _, row in sub.iterrows():
            geom = row.geometry
            comid = row["COMID"] if "COMID" in sub.columns else row.name
            area_km2 = float(row["area_km2"]) if "area_km2" in sub.columns else geom.area / 1e6
            n_done += 1
            try:
                dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
            except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                    n_done, n_total, ftype, comid, exc,
                )
                continue
            mask = _interior_mask(dem, transform, geom)
            n_interior = int(mask.sum())
            if n_interior == 0:
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: 0 interior cells after masking — skipped",
                    n_done, n_total, ftype, comid,
                )
                continue
            max_depth = lake_max_depth(dem, mask, transform)
            mean_depth = max_to_mean(max_depth, shape="cone")
            rows.append({
                "COMID": comid,
                "FTYPE": ftype,
                "area_km2": area_km2,
                "max_depth_m": max_depth,
                "mean_depth_m": mean_depth,
                "mean_depth_in": mean_depth * 39.3701,
                "n_interior_cells": n_interior,
                "resolution": source["resolution"],
            })
            if n_done % 5 == 0 or n_done == n_total:
                logger.info(
                    "  [%d/%d] FTYPE=%s COMID=%s max_depth=%.2f m mean_depth=%.1f in",
                    n_done, n_total, ftype, comid, max_depth, mean_depth * 39.3701,
                )
    return pd.DataFrame(rows)


def _write_hollister_scatter(result: pd.DataFrame, out_png: Path, logger) -> None:
    """Scatter of max-depth (m) vs polygon area (km^2, log-x) — the visual
    check that Hollister depth scales with lake size in a physically
    plausible way (not e.g. clustered at one degenerate value)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(result["area_km2"], result["max_depth_m"], alpha=0.6, color="#1f77b4", s=20)
    ax.set_xscale("log")
    ax.set_xlabel("polygon area (km^2, log scale)")
    ax.set_ylabel("Hollister max depth (m)")
    ax.set_title(f"Hollister max depth vs polygon area (ND LakePond sample, n={len(result)})")
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote max-depth-vs-area scatter: %s", out_png)


def run_hollister(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path,
    n_per_ftype: int = 300, limit: int | None = None,
) -> pd.DataFrame:
    """Issue #173 Task 6: Hollister/lakeMorpho terrain-slope max-depth
    prototype over an ND LakePond sample, plus the max->mean conversion.

    Reconstructs the CONUS dprst set, picks the same ND 1m project Tasks
    4-5 use (`select_nd_project`), samples up to `n_per_ftype` LakePond
    polygons (`sample_per_ftype`, `HOLLISTER_FTYPES`), reads each at 1 m
    and computes `lake_max_depth` -> `max_to_mean` over the polygon
    interior, and writes `hollister_sample.csv` +
    `hollister_maxdepth_vs_area.png` to `out_dir`.

    max->mean conversion: this spike has no field bathymetry survey for
    these ND potholes to calibrate against, so the conical V/A relation
    (mean = max/3, `max_to_mean(..., "cone")`) is used as the documented,
    best-available assumption — the same simplifying geometry lakeMorpho
    itself uses when no bathymetry is supplied (a cone is a reasonable
    first-order shape for a glacially-scoured prairie pothole: steep sides,
    a deep central low point, no flat bottom shelf). Both the max and the
    derived mean distributions are reported (in inches) so a future task
    with real bathymetry can recalibrate the factor without rerunning this
    read.

    `limit`, if given, caps the number of sampled LakePond polygons actually
    read (head of the per-FTYPE sample) — mirrors `run_freeboard`'s `--limit`,
    for cheap smoke-testing of the read/compute path.
    """
    dprst = load_conus_dprst(base, logger)

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info("Selecting ND 1m project with densest dprst overlap ...")
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    logger.info(
        "Sampling up to %d LakePond dprst polygons from project=%s ...", n_per_ftype, project,
    )
    samples = sample_per_ftype(nd_dprst, n_per_ftype, logger, ftypes=HOLLISTER_FTYPES)
    if limit is not None:
        samples = {ftype: sub.head(limit) for ftype, sub in samples.items()}
    n_total = sum(len(v) for v in samples.values())
    logger.info(
        "Total LakePond sample size: %d%s", n_total,
        f" [--limit {limit}]" if limit is not None else "",
    )
    if n_total == 0:
        raise RuntimeError("0 LakePond polygons in the chosen ND project — nothing to analyze")

    logger.info("Reading each sampled LakePond polygon at 1m + computing Hollister max depth ...")
    result = analyze_hollister_sample(samples, project, logger)
    n_ok = len(result)
    logger.info("Hollister reads completed: %d/%d polygons produced a usable result", n_ok, n_total)
    if result.empty:
        raise RuntimeError("Hollister analysis produced 0 usable polygons")

    max_med = float(result["max_depth_m"].median())
    max_p10 = float(np.percentile(result["max_depth_m"], 10))
    max_p90 = float(np.percentile(result["max_depth_m"], 90))
    mean_med_in = float(result["mean_depth_in"].median())
    logger.info(
        "Hollister max-depth distribution (m) over LakePond sample: median=%.2f, "
        "p10=%.2f, p90=%.2f (n=%d)",
        max_med, max_p10, max_p90, n_ok,
    )
    logger.info(
        "Derived mean-depth (max/3 cone conversion) median: %.1f in (max-depth median "
        "%.2f m = %.1f in)",
        mean_med_in, max_med, max_med * 39.3701,
    )
    plausible = 0.1 <= max_med <= 10.0 and (result["max_depth_m"] >= 0).all()
    if plausible:
        logger.info(
            "*** Hollister finding: max-depth magnitudes are PLAUSIBLE (a few metres, "
            "non-negative) — consistent with prairie-pothole/lake depths, not degenerate ***",
        )
    else:
        logger.warning(
            "*** Hollister finding: max-depth magnitudes look IMPLAUSIBLE (median %.2f m, "
            "or a negative value present) — inspect before trusting this factor ***",
            max_med,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "hollister_sample.csv"
    caveat = (
        f"# Hollister/lakeMorpho terrain-slope max-depth prototype over an ND LakePond "
        f"sample (issue #173 Task 6). Project={project}. n_sampled={n_total}"
        + (f" (--limit {limit})" if limit is not None else "")
        + f", n_usable={n_ok}. max_depth_m = lake_max_depth() (mean shoreline-ring slope "
        "x max in-polygon distance-to-shore, over the polygon interior via _interior_mask). "
        "mean_depth_m = max_to_mean(max_depth_m, shape='cone') = max_depth_m / 3 -- "
        "documented assumption (V/A of a cone), NOT field-calibrated in this spike; no "
        "bathymetry survey was available to fit the factor empirically. Cross-check: NHM's "
        "calibrated dprst_depth_avg median is ~49 in (docs/superpowers/... nhm_dprst_params_"
        "are_calibrated) -- compare against mean_depth_in below, but this spike does not "
        "block on matching it.\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        result.to_csv(f, index=False)
    logger.info("Wrote per-polygon Hollister sample: %s", out_csv)

    _write_hollister_scatter(result, out_dir / "hollister_maxdepth_vs_area.png", logger)

    return result


# --- Depth-area regression: ND non-flat V/A depths (--regression, Task 7) --
#
# The plan's Task 7 assumed a PLAYA-anchored large-area donor set (dry playas
# expose bare bed, so their raw DEM is a genuine bathymetry reading at large
# area). The ND Prairie Pothole study project has exactly ONE Playa polygon
# (`select_nd_project`'s chosen project; see `flatness_by_ftype.csv`,
# FTYPE=Playa n_sampled=1) — playas are an arid-West landform, not a Prairie
# Pothole one. One point cannot fit or validate a regression, so this mode
# does NOT attempt a playa-anchored fit. Instead it fits log(depth)~log(area)
# on the ND sample's NON-FLAT polygons across all FLATNESS_FTYPES: Task 4
# already showed hydro-flattening is a minority (SwampMarsh 21.7%, LakePond
# 11.0%, Reservoir 5.4% flat — `flatness_by_ftype.csv`), so most polygons in
# the existing `sample_per_ftype` sample carry a genuine bare-earth
# `depth_to_spill` reading (unlike the flat majority, whose measured depth is
# ~freeboard, i.e. near-zero and NOT a real bed depth — Task 5's finding).
# The playa-anchored arid-West large-area extrapolation is DEFERRED to a
# follow-up 1 m project with real playa coverage; this is a documented spike
# scoping limitation, not a failed analysis.
REGRESSION_MIN_N_FIT = 3

# Recipient dprst polygon area range this regression would ultimately need to
# extrapolate into (issue #173 Task 7 scoping note, full CONUS dprst
# population) — used only to characterize extrapolation risk in the log/plot
# below, never as fit data.
RECIPIENT_AREA_M2_MEDIAN = 35_000.0
RECIPIENT_AREA_M2_MAX = 2.8e9


def analyze_regression_sample(
    samples: dict[str, gpd.GeoDataFrame], project: str, logger,
    flat_lookup: dict | None = None,
) -> pd.DataFrame:
    """Run `read_window` -> interior mask -> `depth_to_spill` ->
    `volume_mean_depth` over every sampled polygon and return one row per
    polygon, tagged flat/non-flat.

    `flat_lookup` (COMID -> bool), if given, is Task 4's cached
    `flatness_per_polygon.csv` classification — preferred over recomputing
    `is_hydroflattened` so the flat/non-flat split matches the one already
    validated and reported by `--flatness`. Falls back to computing
    `is_hydroflattened` directly on this read's interior values (no extra
    read cost — the DEM is already in hand) for any COMID not found in the
    lookup (or when no lookup is supplied at all, e.g. a `--regression` run
    with no prior `--flatness` run in `--out-dir`).
    """
    rows = []
    n_total = sum(len(v) for v in samples.values())
    n_done = 0
    n_from_cache = 0
    for ftype, sub in samples.items():
        for _, row in sub.iterrows():
            geom = row.geometry
            comid = row["COMID"] if "COMID" in sub.columns else row.name
            area_m2 = (
                float(row["area_km2"]) * 1e6 if "area_km2" in sub.columns else geom.area
            )
            n_done += 1
            try:
                dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
            except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                    n_done, n_total, ftype, comid, exc,
                )
                continue
            mask = _interior_mask(dem, transform, geom)
            n_interior = int(mask.sum())
            if n_interior == 0:
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: 0 interior cells after masking — skipped",
                    n_done, n_total, ftype, comid,
                )
                continue
            depth = depth_to_spill(dem)
            cell_area_m2 = abs(transform.a * transform.e)
            _, _, mean_d = volume_mean_depth(depth, mask, cell_area_m2)

            if flat_lookup is not None and comid in flat_lookup:
                flat = bool(flat_lookup[comid])
                n_from_cache += 1
            else:
                flat = bool(is_hydroflattened(dem[mask])["flat"])

            rows.append({
                "COMID": comid,
                "FTYPE": ftype,
                "area_m2": area_m2,
                "mean_depth_m": mean_d,
                "flat": flat,
                "n_interior_cells": n_interior,
                "resolution": source["resolution"],
            })
            if n_done % 25 == 0 or n_done == n_total:
                logger.info(
                    "  [%d/%d] FTYPE=%s COMID=%s mean_depth=%.3f m area=%.1f m^2 flat=%s",
                    n_done, n_total, ftype, comid, mean_d, area_m2, flat,
                )
    if flat_lookup is not None:
        logger.info(
            "  flat/non-flat classification reused from cached flatness_per_polygon.csv "
            "for %d/%d polygons (remainder computed fresh)",
            n_from_cache, len(rows),
        )
    return pd.DataFrame(rows)


def _fit_loglog(df: pd.DataFrame, logger, label: str) -> dict | None:
    """Least-squares fit of log10(mean_depth_m) ~ log10(area_m2).

    Drops non-positive depth/area (log-undefined) before fitting. Returns
    None (and logs a warning, never raises — this is a spike, an
    under-populated stratum must not abort the run) if fewer than
    `REGRESSION_MIN_N_FIT` usable rows remain.
    """
    d = df[(df["area_m2"] > 0) & (df["mean_depth_m"] > 0)]
    if len(d) < REGRESSION_MIN_N_FIT:
        logger.warning(
            "  %s: only %d usable (positive-depth, positive-area) non-flat polygons "
            "(< %d) — skipping this fit",
            label, len(d), REGRESSION_MIN_N_FIT,
        )
        return None
    x = np.log10(d["area_m2"].to_numpy())
    y = np.log10(d["mean_depth_m"].to_numpy())
    fit = scipy_stats.linregress(x, y)
    result = {
        "label": label,
        "n": len(d),
        "slope": float(fit.slope),
        "intercept": float(fit.intercept),
        "r2": float(fit.rvalue ** 2),
        "area_m2_min": float(d["area_m2"].min()),
        "area_m2_max": float(d["area_m2"].max()),
    }
    logger.info(
        "  fit[%s]: n=%d slope=%.3f intercept=%.3f R^2=%.3f (area range %.1f-%.1e m^2)",
        label, result["n"], result["slope"], result["intercept"], result["r2"],
        result["area_m2_min"], result["area_m2_max"],
    )
    return result


def _write_regression_scatter(
    per_polygon: pd.DataFrame, fits: list[dict], out_png: Path, logger,
) -> None:
    """Log-log scatter of mean_depth_m vs area_m2 (flat vs non-flat markers)
    with the fitted line(s) overlaid, plus the recipient area range marked
    so the extrapolation gap from donor to recipient sizes is visible."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 6))
    nonflat = per_polygon[~per_polygon["flat"]]
    flat = per_polygon[per_polygon["flat"]]
    ax.scatter(
        nonflat["area_m2"], nonflat["mean_depth_m"].clip(lower=1e-3),
        alpha=0.5, s=14, color="#1f77b4", label=f"non-flat (n={len(nonflat)}, real bed depth)",
    )
    ax.scatter(
        flat["area_m2"], flat["mean_depth_m"].clip(lower=1e-3),
        alpha=0.4, s=14, color="#d62728", marker="x",
        label=f"flat (n={len(flat)}, ~freeboard, NOT fit)",
    )
    colors = ["black", "#2ca02c", "#ff7f0e", "#9467bd", "#8c564b", "#17becf"]
    for i, f in enumerate(fits):
        xr = np.linspace(np.log10(f["area_m2_min"]), np.log10(f["area_m2_max"]), 50)
        yr = f["slope"] * xr + f["intercept"]
        ax.plot(
            10 ** xr, 10 ** yr, color=colors[i % len(colors)], linewidth=2,
            label=f"{f['label']}: slope={f['slope']:.2f} R^2={f['r2']:.2f} (n={f['n']})",
        )
    ax.axvline(RECIPIENT_AREA_M2_MEDIAN, color="gray", linestyle=":", linewidth=1)
    ax.axvline(RECIPIENT_AREA_M2_MAX, color="gray", linestyle="--", linewidth=1)
    ax.text(
        RECIPIENT_AREA_M2_MEDIAN, 1e-3,
        " recipient median", rotation=90, va="bottom", fontsize=7, color="gray",
    )
    ax.text(
        RECIPIENT_AREA_M2_MAX, 1e-3, " recipient max", rotation=90, va="bottom",
        fontsize=7, color="gray",
    )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("polygon area (m^2, log scale)")
    ax.set_ylabel("mean depth V/A (m, log scale)")
    ax.set_title("ND dprst depth-area regression (non-flat polygons)")
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote depth-area regression scatter: %s", out_png)


def run_regression(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path,
    n_per_ftype: int = 300, limit: int | None = None,
) -> pd.DataFrame:
    """Issue #173 Task 7: depth-area regression over the ND sample's
    non-flat (real bed-depth) dprst polygons.

    See the module-level note above `REGRESSION_MIN_N_FIT` for why this is
    NOT the plan's playa-anchored large-area fit (the ND project has only 1
    Playa polygon) — that extrapolation is deferred to an arid-West project.

    Reconstructs the CONUS dprst set, picks the same ND 1m project Tasks
    4-6 use (`select_nd_project`), samples up to `n_per_ftype` polygons per
    FTYPE across all of `FLATNESS_FTYPES` (`sample_per_ftype` — the same
    default sample Task 4 validated flatness on, so if `flatness_per_polygon
    .csv` already exists in `out_dir` with matching FTYPEs/n_per_ftype/seed
    its flat/non-flat labels are reused by COMID), measures V/A mean depth
    per polygon (`depth_to_spill` -> `volume_mean_depth`), fits
    log10(depth) ~ log10(area) via least squares on the non-flat subset
    (overall AND per-FTYPE), and writes `depth_area_regression.csv` +
    `depth_area_regression.png` to `out_dir`.
    """
    dprst = load_conus_dprst(base, logger)

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info("Selecting ND 1m project with densest dprst overlap ...")
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    logger.info("Sampling up to %d dprst polygons per FTYPE from project=%s ...", n_per_ftype, project)
    samples = sample_per_ftype(nd_dprst, n_per_ftype, logger)
    if limit is not None:
        samples = {ftype: sub.head(limit) for ftype, sub in samples.items()}
    n_total = sum(len(v) for v in samples.values())
    logger.info(
        "Total sample size across all FTYPEs: %d%s", n_total,
        f" [--limit {limit}]" if limit is not None else "",
    )
    if n_total == 0:
        raise RuntimeError("0 dprst polygons in the chosen ND project — nothing to analyze")

    flat_lookup: dict | None = None
    flat_csv = out_dir / "flatness_per_polygon.csv"
    if flat_csv.exists():
        cached = pd.read_csv(flat_csv)
        if "COMID" in cached.columns and "flat" in cached.columns:
            flat_lookup = dict(zip(cached["COMID"], cached["flat"].astype(bool)))
            logger.info(
                "Loaded cached flat/non-flat classification for %d polygons from %s "
                "(Task 4's --flatness run) — reused by COMID where available",
                len(flat_lookup), flat_csv,
            )
        else:
            logger.warning(
                "%s exists but is missing COMID/flat columns — ignoring, will compute "
                "flatness fresh for every polygon", flat_csv,
            )
    else:
        logger.info(
            "No cached %s found — computing flat/non-flat classification fresh for "
            "every sampled polygon (run --flatness first to reuse it instead)", flat_csv,
        )

    logger.info("Reading each sampled polygon at 1m + measuring V/A mean depth ...")
    per_polygon = analyze_regression_sample(samples, project, logger, flat_lookup)
    n_ok = len(per_polygon)
    logger.info("Depth reads completed: %d/%d polygons produced a usable result", n_ok, n_total)
    if per_polygon.empty:
        raise RuntimeError("depth-area regression produced 0 usable polygons")

    n_flat = int(per_polygon["flat"].sum())
    n_nonflat = n_ok - n_flat
    logger.info(
        "%d/%d polygons non-flat (real bed depth, used for the fit); %d/%d flat "
        "(~freeboard, excluded from the fit per Task 5's finding)",
        n_nonflat, n_ok, n_flat, n_ok,
    )
    non_flat = per_polygon[~per_polygon["flat"]]

    fits: list[dict] = []
    overall = _fit_loglog(non_flat, logger, "overall (all FTYPEs)")
    if overall is not None:
        fits.append(overall)
    per_ftype_fits: list[dict] = []
    for ftype in sorted(non_flat["FTYPE"].unique()):
        f = _fit_loglog(non_flat[non_flat["FTYPE"] == ftype], logger, f"FTYPE={ftype}")
        if f is not None:
            per_ftype_fits.append(f)
    fits.extend(per_ftype_fits)

    if not fits:
        logger.warning(
            "*** Depth-area regression verdict: UNDETERMINED — every stratum had "
            "fewer than %d usable non-flat polygons; no fit could be produced ***",
            REGRESSION_MIN_N_FIT,
        )
    else:
        slopes = [f["slope"] for f in per_ftype_fits] or [fits[0]["slope"]]
        slope_spread = max(slopes) - min(slopes)
        r2s = [f["r2"] for f in per_ftype_fits]
        single_law_plausible = (
            len(per_ftype_fits) < 2
            or (slope_spread <= 0.3 and (overall is None or overall["r2"] >= 0.2))
        )
        if single_law_plausible:
            logger.info(
                "*** Depth-area regression verdict: a SINGLE power law across FTYPEs is a "
                "reasonable approximation (per-FTYPE slope spread=%.3f%s) — but see the "
                "extrapolation caveat below before applying it CONUS-wide ***",
                slope_spread,
                f", overall R^2={overall['r2']:.2f}" if overall is not None else "",
            )
        else:
            logger.warning(
                "*** Depth-area regression verdict: STRATIFY by FTYPE — per-FTYPE slopes "
                "diverge (spread=%.3f) and/or fits vary in quality (R^2 range %.2f-%.2f); "
                "a single overall power law is NOT a good fit across FTYPEs ***",
                slope_spread, min(r2s) if r2s else float("nan"), max(r2s) if r2s else float("nan"),
            )
        donor_min = min(f["area_m2_min"] for f in fits)
        donor_max = max(f["area_m2_max"] for f in fits)
        logger.warning(
            "*** Extrapolation risk: donor (measured) area range is %.1f-%.1e m^2; the "
            "recipient dprst population spans median~%.0f m^2 to max~%.1e m^2 — the "
            "recipient MAX is ~%.0fx beyond the donor MAX. This regression is fit on "
            "small ND Prairie Pothole depressions only; extrapolating its slope out to "
            "km^2-scale depressions is UNVALIDATED. The plan's playa-anchored large-area "
            "extrapolation (dry playas expose bare bed at large area) is DEFERRED to an "
            "arid-West 1m project — the ND study area has only 1 Playa polygon, too few "
            "to fit or validate that anchor here. ***",
            donor_min, donor_max, RECIPIENT_AREA_M2_MEDIAN, RECIPIENT_AREA_M2_MAX,
            RECIPIENT_AREA_M2_MAX / donor_max,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "depth_area_regression.csv"
    caveat = (
        f"# Depth-area regression over ND dprst sample (issue #173 Task 7). "
        f"Project={project}. n_sampled={n_total}"
        + (f" (--limit {limit})" if limit is not None else "")
        + f", n_usable={n_ok} ({n_nonflat} non-flat / {n_flat} flat). "
        "mean_depth_m = volume_mean_depth(depth_to_spill(dem)) over the polygon interior "
        "(_interior_mask). flat = is_hydroflattened verdict (cached from --flatness when "
        "available, else computed fresh) -- flat polygons are EXCLUDED from all fits "
        "(their measured depth is ~freeboard, not real bed depth; see Task 5). "
        "PLAYA-ANCHORED large-area extrapolation is DEFERRED (only 1 Playa polygon in "
        "the ND study area) -- see the run log for the full extrapolation-risk caveat.\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        per_polygon.to_csv(f, index=False)
    logger.info("Wrote per-polygon depth-area regression table: %s", out_csv)

    _write_regression_scatter(per_polygon, fits, out_dir / "depth_area_regression.png", logger)

    return per_polygon


# --- Hollister validation: score vs raw-DEM bathymetry + empirical max->mean --
# (--hollister-validation, issue #173 high-value follow-up)
#
# On a NON-hydro-flattened dprst polygon the raw 3DEP DEM captures the real
# bed (Task 5's freeboard finding: a flattened surface is breakline-enforced
# and constant, NOT the true bathymetry; a non-flat surface is unmodified
# bare-earth). `depth_to_spill()` over such a polygon's interior is therefore
# FULL measured bathymetry, not just above-water freeboard -- so on the SAME
# polygons we can compute both Hollister's PREDICTED max depth
# (`lake_max_depth`, Task 6) and the raw-DEM MEASURED max/mean depth
# (`depth_to_spill`/`volume_mean_depth`, Task 7's helpers) and compare them
# head-to-head. Tasks 6 and 7 never did this: Task 6 sampled LakePond only
# and never measured a ground-truth max; Task 7 sampled all FTYPEs and
# measured mean depth but never ran Hollister. This mode reuses every
# read/mask/depth helper from both tasks -- no new sampling, masking, or
# depth-computation logic.
HOLLISTER_VALIDATION_MIN_N_FIT = 3


def analyze_hollister_validation_sample(
    samples: dict[str, gpd.GeoDataFrame], project: str, logger,
    flat_lookup: dict | None = None,
) -> pd.DataFrame:
    """Run `read_window` -> interior mask -> `depth_to_spill` -> {measured
    mean/max depth, Hollister predicted max depth} over every NON-FLAT
    sampled polygon; return one row per usable polygon.

    Flat/non-flat classification reuses Task 4's cached
    `flatness_per_polygon.csv` by COMID where available (same reuse pattern
    as `analyze_regression_sample`), else computes `is_hydroflattened` fresh
    on this read's interior values. Flat polygons are skipped outright
    (counted, not raised) -- their raw DEM is a breakline-enforced water
    surface, not real bathymetry (Task 5), so neither side of this
    comparison means anything on them. Polygons with `measured_max_m <= 0`
    (no real bowl in the interior -- e.g. a hydrologically flat non-flagged
    edge case) are also skipped, since both the mean/max ratio and any
    Hollister comparison would be degenerate (division by zero / comparing
    against a non-existent bed).
    """
    rows = []
    n_total = sum(len(v) for v in samples.values())
    n_done = 0
    n_flat_skipped = 0
    n_degenerate_skipped = 0
    for ftype, sub in samples.items():
        for _, row in sub.iterrows():
            geom = row.geometry
            comid = row["COMID"] if "COMID" in sub.columns else row.name
            area_km2 = float(row["area_km2"]) if "area_km2" in sub.columns else geom.area / 1e6
            n_done += 1
            try:
                dem, transform, crs, source = read_window(geom, "1m", wesm_row={"project": project})
            except Exception as exc:  # noqa: BLE001 - log and skip, never abort the sample
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: read_window failed (%s) — skipped",
                    n_done, n_total, ftype, comid, exc,
                )
                continue
            mask = _interior_mask(dem, transform, geom)
            n_interior = int(mask.sum())
            if n_interior == 0:
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: 0 interior cells after masking — skipped",
                    n_done, n_total, ftype, comid,
                )
                continue

            if flat_lookup is not None and comid in flat_lookup:
                flat = bool(flat_lookup[comid])
            else:
                flat = bool(is_hydroflattened(dem[mask])["flat"])
            if flat:
                n_flat_skipped += 1
                continue

            depth = depth_to_spill(dem)
            cell_area_m2 = abs(transform.a * transform.e)
            _, _, measured_mean_m = volume_mean_depth(depth, mask, cell_area_m2)
            measured_max_m = float(depth[mask].max())
            if measured_max_m <= 0:
                n_degenerate_skipped += 1
                logger.warning(
                    "  [%d/%d] FTYPE=%s COMID=%s: measured_max_m=%.4f <= 0 (no real "
                    "bowl) — skipped", n_done, n_total, ftype, comid, measured_max_m,
                )
                continue
            hollister_max_m = lake_max_depth(dem, mask, transform)

            rows.append({
                "COMID": comid,
                "FTYPE": ftype,
                "area_km2": area_km2,
                "measured_mean_m": measured_mean_m,
                "measured_max_m": measured_max_m,
                "hollister_max_m": hollister_max_m,
                "measured_mean_over_max": measured_mean_m / measured_max_m,
                "n_interior_cells": n_interior,
                "resolution": source["resolution"],
            })
            if n_done % 25 == 0 or n_done == n_total:
                logger.info(
                    "  [%d/%d] FTYPE=%s COMID=%s measured_max=%.2f m hollister_max="
                    "%.2f m measured_mean=%.2f m",
                    n_done, n_total, ftype, comid, measured_max_m, hollister_max_m,
                    measured_mean_m,
                )
    logger.info(
        "  %d/%d polygons flat (skipped, no real bed depth); %d degenerate "
        "(measured_max_m<=0, skipped); %d usable non-flat polygons",
        n_flat_skipped, n_total, n_degenerate_skipped, len(rows),
    )
    return pd.DataFrame(rows)


def _fit_skill(pred: np.ndarray, measured: np.ndarray) -> dict:
    """Correlation/R^2, RMSE, and median signed bias of `pred` vs `measured`.

    `linregress` here is used only for its Pearson `rvalue` (symmetric in
    its two arguments, so which is x/y doesn't matter) -- this is a
    predicted-vs-truth skill score, not a fitted conversion line to apply
    elsewhere.
    """
    fit = scipy_stats.linregress(measured, pred)
    rmse = float(np.sqrt(np.mean((pred - measured) ** 2)))
    bias = float(np.median(pred - measured))
    return {
        "n": len(pred),
        "r": float(fit.rvalue),
        "r2": float(fit.rvalue ** 2),
        "rmse": rmse,
        "median_bias": bias,
    }


def _hollister_skill_stats(df: pd.DataFrame, logger) -> dict:
    """Finding 1: is Hollister's predicted max depth a good predictor of the
    raw-DEM measured max depth? Linear skill (r/R^2/RMSE/median bias) plus a
    log10-log10 fit R^2, since a terrain-slope-x-distance estimate is a
    multiplicative (not additive) model of depth."""
    pred = df["hollister_max_m"].to_numpy()
    measured = df["measured_max_m"].to_numpy()
    stats = _fit_skill(pred, measured)
    logger.info(
        "  Hollister max-depth skill (n=%d): r=%.3f R^2=%.3f RMSE=%.3f m "
        "median_bias=%+.3f m", stats["n"], stats["r"], stats["r2"], stats["rmse"],
        stats["median_bias"],
    )
    pos = (pred > 0) & (measured > 0)
    if int(pos.sum()) >= HOLLISTER_VALIDATION_MIN_N_FIT:
        log_fit = scipy_stats.linregress(np.log10(measured[pos]), np.log10(pred[pos]))
        stats["log10_r2"] = float(log_fit.rvalue ** 2)
        stats["log10_n"] = int(pos.sum())
        logger.info(
            "  Hollister max-depth log10-log10 fit: R^2=%.3f (n=%d)",
            stats["log10_r2"], stats["log10_n"],
        )
    else:
        stats["log10_r2"] = float("nan")
        stats["log10_n"] = int(pos.sum())
        logger.warning(
            "  Hollister max-depth log10-log10 fit: too few positive pairs (n=%d) "
            "to fit", stats["log10_n"],
        )

    if stats["r2"] >= 0.5:
        verdict = "Hollister IS a useful predictor of real max depth (R^2>=0.5)"
    elif stats["r2"] >= 0.2:
        verdict = "Hollister is a WEAK predictor of real max depth (0.2<=R^2<0.5)"
    else:
        verdict = "Hollister is a POOR predictor of real max depth (R^2<0.2)"
    logger.info(
        "*** Hollister skill verdict: %s (R^2=%.3f, median bias=%+.3f m, n=%d) ***",
        verdict, stats["r2"], stats["median_bias"], stats["n"],
    )
    return stats


def _maxmean_factor_stats(df: pd.DataFrame, logger) -> dict:
    """Finding 2: the REAL (measured, Hollister-independent) mean/max shape
    factor distribution -- to check against the assumed cone factor 1/3."""
    ratio = df["measured_mean_over_max"].to_numpy()
    cone_assumed = 1.0 / 3.0
    median = float(np.median(ratio))
    q1, q3 = float(np.percentile(ratio, 25)), float(np.percentile(ratio, 75))
    stats = {"n": len(ratio), "median": median, "q1": q1, "q3": q3, "cone_assumed": cone_assumed}
    logger.info(
        "  Empirical mean/max depth factor (n=%d): median=%.3f IQR=[%.3f, %.3f] "
        "(assumed cone factor = 1/3 = %.3f)", stats["n"], median, q1, q3, cone_assumed,
    )
    if q1 <= cone_assumed <= q3:
        verdict = "1/3 cone assumption falls WITHIN the empirical IQR — reasonable default"
    else:
        verdict = (
            f"1/3 cone assumption falls OUTSIDE the empirical IQR — Phase 1 should use "
            f"the empirical median {median:.3f} instead"
        )
    logger.info("*** Max->mean factor verdict: %s ***", verdict)
    return stats


def _end_to_end_stats(df: pd.DataFrame, logger) -> dict:
    """Finding 3: how well does the FULL Hollister->mean(cone) pipeline --
    what Phase 1 would actually apply to flat polygons -- predict the real
    (measured) mean depth?"""
    predicted_mean = np.array([max_to_mean(m, "cone") for m in df["hollister_max_m"]])
    measured_mean = df["measured_mean_m"].to_numpy()
    stats = _fit_skill(predicted_mean, measured_mean)
    logger.info(
        "  End-to-end Hollister->mean(cone) pipeline skill (n=%d): r=%.3f R^2=%.3f "
        "RMSE=%.3f m median_bias=%+.3f m", stats["n"], stats["r"], stats["r2"],
        stats["rmse"], stats["median_bias"],
    )
    if stats["r2"] >= 0.5:
        verdict = "the FULL Hollister->mean(cone) pipeline predicts real mean depth WELL"
    elif stats["r2"] >= 0.2:
        verdict = "the FULL Hollister->mean(cone) pipeline is a WEAK predictor of real mean depth"
    else:
        verdict = "the FULL Hollister->mean(cone) pipeline is a POOR predictor of real mean depth"
    logger.info(
        "*** End-to-end verdict: %s (R^2=%.3f, RMSE=%.3f m, n=%d) ***",
        verdict, stats["r2"], stats["rmse"], stats["n"],
    )
    return stats


def _write_hollister_validation_scatter(df: pd.DataFrame, out_png: Path, logger) -> None:
    """Predicted (Hollister) vs measured (raw-DEM) max depth, with a 1:1
    reference line -- the visual evidence for Finding 1."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(
        df["measured_max_m"], df["hollister_max_m"], alpha=0.5, s=16, color="#1f77b4",
    )
    lim = float(max(df["measured_max_m"].max(), df["hollister_max_m"].max())) * 1.05
    lim = max(lim, 1e-3)
    ax.plot([0, lim], [0, lim], color="black", linestyle="--", linewidth=1, label="1:1")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_aspect("equal")
    ax.set_xlabel("measured max depth (m, raw-DEM depth_to_spill)")
    ax.set_ylabel("Hollister predicted max depth (m)")
    ax.set_title(f"Hollister predicted vs raw-DEM measured max depth (n={len(df)})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote Hollister pred-vs-measured max-depth scatter: %s", out_png)


def _write_maxmean_factor_hist(df: pd.DataFrame, out_png: Path, logger) -> None:
    """Histogram of measured_mean/measured_max, with the assumed 1/3 cone
    factor and the empirical median marked -- the visual evidence for
    Finding 2."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    med = float(df["measured_mean_over_max"].median())
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(df["measured_mean_over_max"], bins=40, color="#1f77b4", alpha=0.8)
    ax.axvline(1.0 / 3.0, color="black", linestyle="--", linewidth=1.5, label="assumed cone factor = 1/3")
    ax.axvline(med, color="#d62728", linestyle="-", linewidth=1.5, label=f"empirical median = {med:.3f}")
    ax.set_xlabel("measured mean / measured max depth")
    ax.set_ylabel("polygon count")
    ax.set_title(f"Empirical max->mean shape factor (n={len(df)})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    logger.info("  wrote max/mean factor histogram: %s", out_png)


def run_hollister_validation(
    base: dict, logger, out_dir: Path, wesm_cache_dir: Path,
    n_per_ftype: int = 300, limit: int | None = None,
) -> pd.DataFrame:
    """Issue #173 follow-up: validate Hollister's terrain-slope max-depth
    prediction against raw-DEM measured bathymetry on non-flat dprst
    polygons, and empirically calibrate the max->mean shape factor.

    See the module-level note above `HOLLISTER_VALIDATION_MIN_N_FIT` for why
    this works: on a non-flat polygon the raw 3DEP DEM IS the real bed, so
    the SAME polygon yields both a measured ground truth and a Hollister
    prediction, letting the two be scored head-to-head for the first time.

    Samples up to `n_per_ftype` polygons per FTYPE across all of
    `FLATNESS_FTYPES` (LakePond is Hollister's target method, per Task 6,
    but SwampMarsh/Playa/Reservoir are included and reported separately —
    the caller-requested "focus on LakePond, include + report the rest").
    Reuses Task 4's cached `flatness_per_polygon.csv` (from `out_dir`) to
    keep only non-flat polygons where available, falls back to computing
    `is_hydroflattened` fresh otherwise (mirrors `run_regression`'s reuse
    pattern). Reports three findings via the logger: (1) Hollister max-depth
    skill (linear + log10-log10 R^2, RMSE, median bias vs measured max), (2)
    the empirical measured_mean/measured_max shape-factor distribution vs
    the assumed 1/3 cone, and (3) the end-to-end Hollister->mean(cone)
    pipeline skill against measured mean depth — what Phase 1 would actually
    apply to flat polygons. Writes `hollister_validation.csv` +
    `hollister_pred_vs_measured_max.png` + `maxmean_factor_hist.png`.
    """
    dprst = load_conus_dprst(base, logger)

    wesm_path = ensure_wesm_local(wesm_cache_dir, logger)
    logger.info("Selecting ND 1m project with densest dprst overlap ...")
    project, nd_dprst = select_nd_project(dprst, wesm_path, logger)
    del dprst
    gc.collect()

    logger.info("Sampling up to %d dprst polygons per FTYPE from project=%s ...", n_per_ftype, project)
    samples = sample_per_ftype(nd_dprst, n_per_ftype, logger)
    if limit is not None:
        samples = {ftype: sub.head(limit) for ftype, sub in samples.items()}
    n_total = sum(len(v) for v in samples.values())
    logger.info(
        "Total sample size across all FTYPEs: %d%s", n_total,
        f" [--limit {limit}]" if limit is not None else "",
    )
    if n_total == 0:
        raise RuntimeError("0 dprst polygons in the chosen ND project — nothing to analyze")

    flat_lookup: dict | None = None
    flat_csv = out_dir / "flatness_per_polygon.csv"
    if flat_csv.exists():
        cached = pd.read_csv(flat_csv)
        if "COMID" in cached.columns and "flat" in cached.columns:
            flat_lookup = dict(zip(cached["COMID"], cached["flat"].astype(bool)))
            logger.info(
                "Loaded cached flat/non-flat classification for %d polygons from %s "
                "(Task 4's --flatness run) — reused by COMID where available",
                len(flat_lookup), flat_csv,
            )
        else:
            logger.warning(
                "%s exists but is missing COMID/flat columns — ignoring, will compute "
                "flatness fresh for every polygon", flat_csv,
            )
    else:
        logger.info(
            "No cached %s found — computing flat/non-flat classification fresh for "
            "every sampled polygon (run --flatness first to reuse it instead)", flat_csv,
        )

    logger.info(
        "Reading each sampled non-flat polygon at 1m + computing measured mean/max "
        "depth + Hollister predicted max ..."
    )
    per_polygon = analyze_hollister_validation_sample(samples, project, logger, flat_lookup)
    n_ok = len(per_polygon)
    logger.info(
        "Hollister-validation reads completed: %d/%d sampled polygons usable "
        "(non-flat, non-degenerate)", n_ok, n_total,
    )
    if per_polygon.empty:
        raise RuntimeError("Hollister-validation analysis produced 0 usable polygons")

    logger.info("--- Finding 1: Hollister max-depth skill (predicted vs measured) ---")
    skill = _hollister_skill_stats(per_polygon, logger)

    logger.info("--- Finding 2: empirical measured_mean/measured_max shape factor ---")
    factor = _maxmean_factor_stats(per_polygon, logger)

    logger.info("--- Finding 3: end-to-end Hollister->mean(cone) pipeline skill ---")
    end_to_end = _end_to_end_stats(per_polygon, logger)

    logger.info("--- Per-FTYPE breakdown ---")
    for ftype in sorted(per_polygon["FTYPE"].unique()):
        sub = per_polygon[per_polygon["FTYPE"] == ftype]
        ratio_med = float(sub["measured_mean_over_max"].median())
        if len(sub) >= HOLLISTER_VALIDATION_MIN_N_FIT:
            sub_skill = _fit_skill(sub["hollister_max_m"].to_numpy(), sub["measured_max_m"].to_numpy())
            logger.info(
                "  FTYPE=%-10s n=%4d Hollister R^2=%.3f RMSE=%.3f m mean/max median=%.3f",
                ftype, len(sub), sub_skill["r2"], sub_skill["rmse"], ratio_med,
            )
        else:
            logger.info(
                "  FTYPE=%-10s n=%4d (< %d, too few to fit) mean/max median=%.3f",
                ftype, len(sub), HOLLISTER_VALIDATION_MIN_N_FIT, ratio_med,
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "hollister_validation.csv"
    caveat = (
        f"# Hollister-vs-raw-DEM-bathymetry validation + empirical max->mean factor "
        f"(issue #173 follow-up). Project={project}. n_sampled={n_total}"
        + (f" (--limit {limit})" if limit is not None else "")
        + f", n_usable={n_ok} (non-flat, measured_max_m>0). measured_mean_m/"
        "measured_max_m = volume_mean_depth/max(depth_to_spill) over the polygon "
        "interior (real bathymetry -- only valid on non-flat polygons, Task 5). "
        "hollister_max_m = lake_max_depth() predicted max depth (Task 6). Hollister "
        f"skill: R^2={skill['r2']:.3f} RMSE={skill['rmse']:.3f} m "
        f"median_bias={skill['median_bias']:+.3f} m (log10-log10 R^2="
        f"{skill['log10_r2']:.3f}, n={skill['log10_n']}). Empirical mean/max factor: "
        f"median={factor['median']:.3f} IQR=[{factor['q1']:.3f}, {factor['q3']:.3f}] "
        f"vs assumed cone=1/3={factor['cone_assumed']:.3f}. End-to-end "
        f"Hollister->mean(cone) pipeline: R^2={end_to_end['r2']:.3f} "
        f"RMSE={end_to_end['rmse']:.3f} m median_bias={end_to_end['median_bias']:+.3f} m.\n"
    )
    with open(out_csv, "w") as f:
        f.write(caveat)
        per_polygon.to_csv(f, index=False)
    logger.info("Wrote per-polygon Hollister-validation table: %s", out_csv)

    _write_hollister_validation_scatter(
        per_polygon, out_dir / "hollister_pred_vs_measured_max.png", logger,
    )
    _write_maxmean_factor_hist(per_polygon, out_dir / "maxmean_factor_hist.png", logger)

    return per_polygon


def main() -> None:
    from gfv2_params.config import load_base_config
    from gfv2_params.log import configure_logging

    parser = argparse.ArgumentParser(description=__doc__)
    # Mutually-exclusive MODE group: exactly one investigation task per run.
    # Task 7 (issue #173) adds --regression here as another
    # mutually_exclusive_group() member — do not go back to a single
    # `required=True` flag (that breaks the moment a second mode is added;
    # see Task 2 review carry-forward note).
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--audit", action="store_true",
                       help="Run the CONUS coverage audit (Task 2, issue #173).")
    mode.add_argument("--flatness", action="store_true",
                       help="Run the ND flatness-detector validation + SwampMarsh "
                            "verdict (Task 4, issue #173).")
    mode.add_argument("--freeboard", action="store_true",
                       help="Quantify freeboard (filled - raw) over Task 4's "
                            "detected-flat dprst polygon sample (Task 5, issue #173). "
                            "Requires flatness_per_polygon.csv already in --out-dir "
                            "(run --flatness first).")
    mode.add_argument("--hollister", action="store_true",
                       help="Hollister/lakeMorpho terrain-slope max-depth prototype + "
                            "max->mean conversion over an ND LakePond sample (Task 6, "
                            "issue #173).")
    mode.add_argument("--regression", action="store_true",
                       help="Depth-area regression (log10 depth ~ log10 area) over the "
                            "ND sample's non-flat dprst polygons, overall + per-FTYPE "
                            "(Task 7, issue #173). Playa-anchored large-area "
                            "extrapolation is DEFERRED (only 1 Playa polygon in the ND "
                            "study area) -- see the run log's extrapolation-risk caveat. "
                            "Reuses flatness_per_polygon.csv from --out-dir if present.")
    mode.add_argument("--hollister-validation", action="store_true",
                       help="Score Hollister's predicted max depth against raw-DEM "
                            "measured bathymetry on non-flat dprst polygons, and "
                            "empirically calibrate the max->mean shape factor (issue "
                            "#173 follow-up). Samples all of FLATNESS_FTYPES (focus on "
                            "LakePond, reports others too). Reuses "
                            "flatness_per_polygon.csv from --out-dir if present.")
    parser.add_argument("--fabric", default=None,
                         help="Fabric name (overrides FABRIC env / default_fabric).")
    parser.add_argument(
        "--out-dir", type=Path, required=True,
        help="Output directory (coverage_audit.csv for --audit; "
             "flatness_by_ftype.csv/flatness_separability.png for --flatness; "
             "hollister_sample.csv/hollister_maxdepth_vs_area.png for --hollister; "
             "depth_area_regression.csv/depth_area_regression.png for --regression; "
             "hollister_validation.csv/hollister_pred_vs_measured_max.png/"
             "maxmean_factor_hist.png for --hollister-validation). "
             "Also doubles as the WESM.gpkg download cache dir (multi-GB "
             "one-time download, shared by all modes), so pick a path with "
             "enough free space; there is no default.",
    )
    parser.add_argument(
        "--n-per-ftype", type=int, default=300,
        help="--flatness/--hollister/--regression/--hollister-validation only: target "
             "sample size per FTYPE (default 300, per issue #173 Task 4; --hollister "
             "samples LakePond only, --regression/--hollister-validation sample all of "
             "FLATNESS_FTYPES). Uses fewer if a FTYPE has fewer polygons in the chosen "
             "project — never silently capped, always logged.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="--freeboard/--hollister/--regression/--hollister-validation only: cap "
             "the number of polygons analyzed (head of the sample) — for cheap "
             "smoke-testing; default: analyze the full sample (every flat=True "
             "polygon for --freeboard, every sampled polygon per FTYPE for the other "
             "modes).",
    )
    args = parser.parse_args()

    logger = configure_logging("dprst_depth_probe")
    base = load_base_config(fabric=args.fabric)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.audit:
        per_vpu = run_audit(base, logger, wesm_cache_dir=args.out_dir)

        out_csv = args.out_dir / "coverage_audit.csv"
        caveat = (
            "# CAVEAT: 1m%/1m-count figures are a convex-hull UPPER BOUND — WESM "
            "multi-part workunit footprints are collapsed to their convex hull "
            "before the best_topo spatial join, which can only OVERSTATE 1m "
            "coverage, never understate it. True 1m coverage may be lower. See "
            "load_wesm_1m_footprints() in dprst_depth_probe.py.\n"
        )
        with open(out_csv, "w") as f:
            f.write(caveat)
            per_vpu.to_csv(f, index=False)
        logger.info("Wrote per-VPU coverage audit: %s (%d rows)", out_csv, len(per_vpu))
    elif args.flatness:
        run_flatness(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            n_per_ftype=args.n_per_ftype,
        )
    elif args.freeboard:
        run_freeboard(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            limit=args.limit,
        )
    elif args.hollister:
        run_hollister(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            n_per_ftype=args.n_per_ftype, limit=args.limit,
        )
    elif args.regression:
        run_regression(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            n_per_ftype=args.n_per_ftype, limit=args.limit,
        )
    elif args.hollister_validation:
        run_hollister_validation(
            base, logger, out_dir=args.out_dir, wesm_cache_dir=args.out_dir,
            n_per_ftype=args.n_per_ftype, limit=args.limit,
        )


if __name__ == "__main__":
    main()
