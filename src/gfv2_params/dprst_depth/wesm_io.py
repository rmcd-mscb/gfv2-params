"""WESM (Work Extent Spatial Metadata) 3DEP workunit-footprint I/O.

Promoted verbatim from the Phase 0 diagnostic probe
(`scripts/diagnose/dprst_depth_probe.py`) so both the diagnostic and the
`gfv2_params.download.wesm` staging module (issue #173 Task 7b) import a
single validated copy — no duplicated download/filter logic.

WESM is the authoritative 3DEP workunit footprint index (~3,258 workunits,
~3.6 GB GeoPackage). The task brief points at the S3 object via
`/vsis3/prd-tnm/...`, but two GDAL access paths were tried and rejected on
this cluster before falling back to a local download:
  - `/vsis3/...` (anonymous request): GDAL's GeoPackage driver issues a
    metadata-table probe query on open (`SELECT COUNT(*) FROM sqlite_master
    WHERE name IN ('gpkg_metadata', ...)`) that raises "attempt to write a
    readonly database", reproduced with both fiona and
    `gdal.OpenEx(..., GA_ReadOnly)`.
  - `/vsicurl/https://prd-tnm.s3.amazonaws.com/...` (plain HTTPS, sidesteps
    the /vsis3/ driver bug above and opens fine): but a full-layer read
    over this path raised "database disk image is malformed" partway
    through — reproduced on both a full feature scan and a pushed-down SQL
    GROUP BY. GeoPackage's SQLite b-tree access pattern issues many
    scattered small-range HTTP requests across a 3.6 GB file, which is not
    reliable on this network.
A one-time plain HTTPS download to local disk (`ensure_wesm_local`) reads
the same authoritative S3 object as an ordinary local SQLite file once
fetched, avoiding both failure modes.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio

WESM_HTTPS_URL = (
    "https://prd-tnm.s3.amazonaws.com/"
    "StagedProducts/Elevation/metadata/WESM.gpkg"
)

# WESM's own `onemeter_category` field flags whether a workunit meets the
# 3DEP 1 m DEM spec; observed values on a real download are {"Meets", "Meets
# with variance", "Does not meet", "Pending publication"}. "Meets"/"Meets
# with variance" are both effectively published 1 m/QL1/QL2 product
# footprints (variance = minor spec deviation, still 1 m data); "Does not
# meet" is legacy coarser LiDAR and "Pending publication" is not yet
# downloadable — both are excluded.
QUALIFYING_1M_CATEGORIES = ("Meets", "Meets with variance")


def is_1m_qualifying(category: str) -> bool:
    """True if a WESM `onemeter_category` value qualifies as 1 m/QL1/QL2."""
    return category in QUALIFYING_1M_CATEGORIES


def _qualifying_where_clause(
    categories: tuple[str, ...] = QUALIFYING_1M_CATEGORIES,
) -> str:
    """Pure SQL WHERE-clause builder for `categories`, pushed down to the
    GeoPackage read so only qualifying rows are ever pulled off disk.
    Factored out so the exact clause syntax is unit-testable offline.
    """
    return "onemeter_category IN ({})".format(", ".join(f"'{v}'" for v in categories))


def ensure_wesm_local(cache_dir: Path, logger, url: str = WESM_HTTPS_URL) -> Path:
    """Download (or reuse a cached copy of) WESM.gpkg and return its local path.

    See the module-level note above for why this reads over plain HTTPS to
    local disk rather than a GDAL `/vsis3/` or `/vsicurl/` virtual path.
    """
    cache_dir = Path(cache_dir)
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
    LiDAR projects included); see `QUALIFYING_1M_CATEGORIES` for the
    qualifying `onemeter_category` values.

    Reads in two passes: a geometry-free attribute scan of all ~3,258
    workunits (cheap — logs the full category breakdown), then a batched
    geometry read filtered by a pushed-down SQL WHERE to *only* the
    qualifying ~1,790 rows, keeping `workunit`, `project`, and
    `onemeter_category`. `project` is the field
    `dprst_depth.topo.read_window`'s 1 m tile URL template keys on
    (`TILE1M_HTTPS_TEMPLATE`), so it must survive to any staged output the
    Phase 1 builder consumes. Each batch's geometries are immediately
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

    where = _qualifying_where_clause()
    parts = []
    offset = 0
    while True:
        batch = gpd.read_file(
            path, columns=["workunit", "project", "onemeter_category"], where=where,
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
        len(onem), QUALIFYING_1M_CATEGORIES,
    )
    return onem
