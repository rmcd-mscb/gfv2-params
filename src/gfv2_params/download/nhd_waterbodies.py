"""Stage NHDPlusV2 NHDWaterbody polygons -> `input/nhd/nhd_waterbodies.parquet`.

This is the last unverified input in the NHD/WBD reproducibility chain: every
other hand-made file in `input/nhd/` has already been replaced by a
source-staged module (`nhd_flowlines.py`, `nhd_topology.py`,
`nhd_burn_components.py`, `wbd_huc12.py`) except the waterbody polygons
themselves, which have lived only as a hand-made `input/nhd/conus_waterbodies.gpkg`
(782 MB, owned by another user, observed to change size mid-rebuild). This
module reproduces that layer from source.

NHDWaterbody.shp lives in the SAME per-VPU `NHDSnapshot` archive that
`nhd_flowlines` already downloads (e.g.
`NHDPlusNE/NHDPlus01/NHDSnapshot/Hydrography/NHDWaterbody.shp`), so
`download_waterbody_snapshot` reuses `nhd_flowlines.download_snapshot` for the
actual download/extract mechanics (`.part` sidecar + Content-Length check +
atomic rename, unconditional re-extract) and only re-derives the sibling
shapefile path.

## What `member_comid` actually is

`member_comid` is NOT a native NHDWaterbody field — NHDWaterbody.shp carries no
such column, and nothing in NHDPlus's waterbody/reach linkage tables produces it
either. Reverse-engineered from the existing `conus_waterbodies.gpkg` (measured
on the real archives): for 447,844 of 448,124 rows it is a plain copy of
`COMID` (a bare `str(COMID)`, no comma). For the other 280 rows it is a
**comma-separated, ascending-numeric-order list of raw NHDWaterbody COMIDs**
that were dissolved into one output polygon within a single VPU — provenance
for a many-to-one merge, not a join key.

**The merge rule is: within one VPU, every NHDWaterbody row sharing the same
non-null, non-empty GNIS_ID is dissolved into ONE output row — unconditionally,
with no spatial-adjacency test.** This was verified, not assumed: NHD's own
`touches`/`intersects` is NOT the criterion. Real CONUS examples:

* Mono Lake (VPU 18, GNIS_ID 263749): COMID 120053921 (`member_comid`
  "20286504,120053921") unions raw COMIDs 120053921 (118.745 km2) and 20286504
  (42.345 km2), which do happen to touch.
* Clear Lake, CA (VPU 18, GNIS_ID 1664234): COMID 8005399 (6-member
  `member_comid`) unions 6 touching parts.
* **Lake Conroe (VPU 12, GNIS_ID 1380953): COMIDs 1466730 and 120053033 are
  merged despite being 662.8 m apart — measured `touches=False`,
  `intersects=False`.** This is the case that disproves a spatial-adjacency
  requirement: NHD's own GNIS_ID is the entire criterion.
* Lake Oahe (VPU 10U, GNIS_ID 1266878, COMIDs 19247123/19247131/19251179)
  additionally shows FTYPE need not be uniform within a merge: two small parts
  are LakePond, the dominant 1,254.6 km2 part is Reservoir.

For any merge, the retained `COMID`/`GNIS_NAME`/`FTYPE` come from the
**largest-area member** (recomputed geometric area in EPSG:5070, not the raw
`AREASQKM` attribute, so the same computation drives both the tie-break and
the output `area_sqkm`); `member_comid` is the sorted (ascending, by COMID)
comma-joined member list; `area_sqkm`/geometry come from the unioned polygon.
A mismatched FTYPE within a group is logged (for auditability), not raised.

This means `member_comid` is functionally almost inert downstream:
`depstor.select_connected_waterbodies` does
`pd.to_numeric(member_comid, errors="coerce")`, which turns every comma-list
into NaN — so a merged row can only be matched by its (single) `COMID`, never
by an individual raw member id. The column exists for provenance/schema
compatibility (and so `burn_add_to_waterbody_frame`'s BurnAddWaterbody rows,
whose `member_comid` mirrors their negative `COMID`, need no special case), not
as a distinct connectivity join key.

## Why the dissolve runs PER-VPU, never across VPU archives

The GNIS_ID grouping above runs **within each VPU's own frame** — polygons from
different VPU archives are never compared against each other, even when they
share a GNIS_ID and are geometrically identical. This is not a simplification;
it is required to reproduce the existing layer's own residual duplicates.
NHDPlus ships some boundary-straddling waterbodies as a bit-for-bit identical
polygon in BOTH adjacent drainage-area archives (218 COMIDs measured, at the
VPU 04/07 and 12/13 seams — see `dedupe_cross_vpu_duplicates`), and the
existing `conus_waterbodies.gpkg` carries every one of them as **two separate
rows with the same COMID** — measured even for a case with a shared, non-null
GNIS_ID (GNIS_ID 178159 "Saint Vrain Glaciers", COMID 16000340, duplicated
across VPU 10L/14): if GNIS_ID-matching ran across the whole CONUS frame, that
duplicate pair would incorrectly dissolve into one row (same GNIS_ID, and
trivially "touching" since it's the same geometry twice). Fourteen such
same-GNIS_ID, cross-VPU pairs survive unmerged in the existing 448,124; every
one checked is either an exact cross-VPU duplicate COMID or a named feature
split into two different COMIDs at a VPU boundary (e.g. GNIS_ID 1564644 "Empire
Swamp", COMIDs 120054138 in VPU 04 and 937030254 in VPU 07 — a real feature
straddling the Great Lakes/Upper Mississippi height-of-land). `main()` dissolves
each VPU's frame independently (via `dissolve_named_parts`) and only
concatenates afterward, reproducing this exactly.

## Cross-VPU duplicate COMIDs (NOT collapsed by `main()`)

NHDPlus deliberately ships a boundary-straddling waterbody into BOTH adjacent
drainage-area archives, so each DA's own snapshot is topologically complete on
its own. Measured on the real CONUS archives: 218 raw COMIDs are duplicated
across VPU pairs, overwhelmingly the VPU 04/VPU 07 seam (80 COMIDs, near the
Chicago Sanitary and Ship Canal diversion between the Great Lakes and Upper
Mississippi basins) plus one VPU 12/VPU 13 pair (Rio Grande). Every duplicate
checked is a bit-for-bit identical copy (same FTYPE/GNIS_ID/area/geometry).
`dedupe_cross_vpu_duplicates` collapses these to one row per COMID and raises
if a duplicate group is ever NOT an exact copy (a genuine conflict, not this
benign case) — but **`main()` deliberately does not call it**, because the
existing layer being reproduced does not dedupe them either (the 217 residual
duplicate-COMID rows above). It is kept as a tested, documented utility for a
future intentionally-cleaned variant, not part of the default reproduction.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio

from gfv2_params.config import load_base_config
from gfv2_params.download.nhd_flowlines import download_snapshot as _download_flowline_snapshot
from gfv2_params.download.nhd_flowlines import vpu_index
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_waterbodies")

# Exact schema the `waterbody` builder and `depstor.select_connected_waterbodies`
# consume (module docstring for `member_comid`'s provenance).
OUTPUT_COLUMNS = [
    "GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm", "geometry",
]

_REQUIRED_FIELDS = ("COMID", "GNIS_ID", "GNIS_NAME", "FTYPE")


def read_waterbody_attrs(shp_path: Path) -> gpd.GeoDataFrame:
    """Read COMID/GNIS_ID/GNIS_NAME/FTYPE + geometry from an NHDWaterbody source.

    NHD field-name casing is inconsistent across VPU snapshots (mirrors
    `nhd_flowlines.read_flowline_attrs`: VPU 12 ships COMID, VPU 13 ships
    ComID), so fields are resolved case-insensitively and normalised to
    canonical upper-case names. Requesting the exact upper-case names would
    make pyogrio silently drop a mismatched-case column, KeyError'ing later.
    """
    available = list(pyogrio.read_info(shp_path)["fields"])
    by_upper = {name.upper(): name for name in available}
    rename = {}
    for canon in _REQUIRED_FIELDS:
        actual = by_upper.get(canon)
        if actual is None:
            raise KeyError(
                f"{shp_path}: NHDWaterbody has no '{canon}' field (case-insensitive)."
                f" Available fields: {available}"
            )
        rename[actual] = canon
    gdf = pyogrio.read_dataframe(shp_path, columns=list(rename))
    return gdf.rename(columns=rename)


def download_waterbody_snapshot(dd: str, vpu: str, download_dir: Path, extract_dir: Path) -> Path | None:
    """Download + extract a VPU's NHDSnapshot; return the NHDWaterbody.shp path.

    NHDWaterbody.shp lives in the same NHDSnapshot archive/Hydrography folder as
    NHDFlowline.shp, so the download/extract itself (incl. the `.part` sidecar +
    Content-Length check + atomic rename, and the unconditional re-extract that
    protects against a partially-extracted dir from a killed job) is delegated to
    `nhd_flowlines.download_snapshot`, not reimplemented here — this just derives
    the sibling shapefile path from the same extraction.
    """
    flowline = _download_flowline_snapshot(dd, vpu, download_dir, extract_dir)
    if flowline is None:
        return None
    shps = list(flowline.parent.glob("NHDWaterbody.shp"))
    if not shps:
        logger.error(
            f"NHDWaterbody.shp not found in extracted snapshot for VPU {vpu} "
            f"(dir: {flowline.parent})"
        )
        return None
    return shps[0]


def _is_named(gdf: gpd.GeoDataFrame) -> pd.Series:
    gnis = gdf["GNIS_ID"]
    return gnis.notna() & (gnis.astype(str).str.strip() != "")


def _passthrough_row(row: pd.Series) -> dict:
    return {
        "GNIS_ID": row["GNIS_ID"],
        "GNIS_NAME": row["GNIS_NAME"],
        "COMID": int(row["COMID"]),
        "FTYPE": row["FTYPE"],
        "member_comid": str(int(row["COMID"])),
        "area_sqkm": float(row["_area_sqkm"]),
        "geometry": row.geometry,
    }


def _merge_group(members: gpd.GeoDataFrame) -> dict:
    """Dissolve every same-GNIS_ID NHDWaterbody row (within one VPU) into one
    output row (module docstring for the rule — no spatial-adjacency test).

    FTYPE is NOT required to be uniform across the group: the real CONUS data
    has a genuine case (Lake Oahe, GNIS_ID 1266878, COMIDs 19247123/19247131/
    19251179) where two small parts are tagged LakePond and the dominant
    1,254.6 km2 part is tagged Reservoir. The existing hand-made layer resolves
    this the same way it resolves COMID/GNIS_NAME: the largest-area member
    wins. A mismatch is logged (not silently invisible) but does not raise.
    """
    ftypes = sorted(set(members["FTYPE"]))
    if len(ftypes) > 1:
        logger.info(
            f"  GNIS_ID {members['GNIS_ID'].iloc[0]!r} "
            f"(COMIDs {sorted(int(c) for c in members['COMID'])}) mixes FTYPEs "
            f"{ftypes} -- resolving to the largest-area member's FTYPE (mirrors "
            f"COMID/GNIS_NAME resolution)."
        )
    rep = members.loc[members["_area_sqkm"].idxmax()]
    member_comid = ",".join(str(c) for c in sorted(int(c) for c in members["COMID"]))
    geom = members.geometry.union_all()
    return {
        "GNIS_ID": rep["GNIS_ID"],
        "GNIS_NAME": rep["GNIS_NAME"],
        "COMID": int(rep["COMID"]),
        "FTYPE": rep["FTYPE"],
        "member_comid": member_comid,
        "area_sqkm": geom.area / 1e6,
        "geometry": geom,
    }


def dedupe_cross_vpu_duplicates(combined: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Collapse NHDWaterbody rows NHDPlus ships in BOTH adjacent VPU/DA
    archives at a boundary seam down to one row per COMID.

    NHDPlus deliberately duplicates a boundary-straddling feature into both
    archives so each drainage-area's own snapshot is topologically complete on
    its own -- this is NOT a data error. Measured on the real CONUS archives:
    218 raw COMIDs are duplicated across VPU pairs (mostly the VPU 04/VPU 07
    seam near the Chicago Sanitary and Ship Canal diversion between the Great
    Lakes and Upper Mississippi basins, 80 COMIDs; one VPU 12/VPU 13 pair along
    the Rio Grande), and every one checked is a bit-for-bit identical copy
    (same FTYPE, GNIS_ID, area, and geometry to floating-point noise).

    Raises if any duplicate group is NOT an exact copy (differing FTYPE,
    GNIS_ID, area beyond 1e-6 km2, or geometry beyond a 1e-6 tolerance) --
    that would be a genuine conflict, not the benign boundary-seam case this
    function exists to collapse, and silently keeping one copy could drop
    real depression area or paper over attribute drift.

    NOT called by `main()` (module docstring: "Cross-VPU duplicate COMIDs") --
    kept as a tested, documented utility for a future intentionally-cleaned
    variant, since the existing layer this module reproduces does not dedupe
    these either.
    """
    dup_mask = combined["COMID"].duplicated(keep=False)
    if not dup_mask.any():
        return combined

    bad = []
    for comid, group in combined[dup_mask].groupby("COMID"):
        ftypes = set(group["FTYPE"])
        gnis_ids = set(group["GNIS_ID"].fillna("__NULL__").astype(str))
        areas_km2 = group.geometry.area / 1e6
        area_ok = (areas_km2.max() - areas_km2.min()) <= 1e-6
        geoms = list(group.geometry)
        geom_ok = all(geoms[0].equals_exact(g, 1e-6) for g in geoms[1:])
        if len(ftypes) > 1 or len(gnis_ids) > 1 or not area_ok or not geom_ok:
            bad.append(int(comid))
    if bad:
        raise ValueError(
            f"{len(bad)} raw NHDWaterbody COMID(s) are duplicated across VPU "
            f"archives but are NOT identical copies (e.g. {sorted(bad)[:10]}) -- "
            f"NHDPlus is known to duplicate boundary-straddling waterbodies into "
            f"both adjacent DA archives (VPU 04/07, VPU 12/13 measured), but always "
            f"as an exact copy; a mismatched copy is a genuine conflict, not that "
            f"benign case, and dropping one silently could lose real depression "
            f"area or paper over attribute drift. Investigate: {sorted(bad)}"
        )

    n_dup_comids = int(combined.loc[dup_mask, "COMID"].nunique())
    n_dropped = int(dup_mask.sum()) - n_dup_comids
    logger.info(
        f"Collapsed {n_dropped} duplicate NHDWaterbody row(s) shipped across a "
        f"VPU/DA archive boundary seam ({n_dup_comids} distinct COMIDs, each "
        f"verified as an exact copy)"
    )
    return combined.drop_duplicates(subset="COMID", keep="first")


def dissolve_named_parts(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproduce the existing layer's many-to-one waterbody merge, WITHIN one
    VPU's frame (see module docstring). `gdf` must already be in an equal-area
    CRS (EPSG:5070) — area is used both for the merge tie-break and the output
    `area_sqkm`. Must be called separately per VPU (module docstring: "Why the
    dissolve runs PER-VPU") — never on a multi-VPU concatenated frame.

    Unnamed waterbodies (null/empty GNIS_ID) pass through unchanged. Every row
    sharing a non-null, non-empty GNIS_ID is dissolved into one row via
    `_merge_group` — unconditionally, with no spatial-adjacency test (verified
    real case: Lake Conroe's two parts are 662.8 m apart and still merge).
    """
    gdf = gdf.reset_index(drop=True)
    gdf = gdf.assign(_area_sqkm=gdf.geometry.area / 1e6)

    named = _is_named(gdf)
    rows: list[dict] = [_passthrough_row(row) for _, row in gdf[~named].iterrows()]

    for _, group in gdf[named].groupby("GNIS_ID", sort=False):
        if len(group) == 1:
            rows.append(_passthrough_row(group.iloc[0]))
        else:
            rows.append(_merge_group(group))

    out = gpd.GeoDataFrame(rows, crs=gdf.crs)
    return out[OUTPUT_COLUMNS]


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    out_dir = data_root / "input/nhd"
    for d in (download_dir, extract_dir, out_dir):
        d.mkdir(parents=True, exist_ok=True)

    # Dissolve runs PER-VPU (module docstring "Why the dissolve runs PER-VPU"):
    # the existing layer being reproduced never compares one VPU archive's
    # polygons against another's, so a boundary-straddling duplicate is never
    # merged or deduped there, even when it carries a non-null GNIS_ID and is
    # geometrically self-identical (measured: GNIS_ID 178159 "Saint Vrain
    # Glaciers", COMID 16000340, survives as two rows in the existing 448,124).
    outputs = []
    failures = []
    n_raw_total = 0
    for vpu, dd in vpu_index.items():
        shp = download_waterbody_snapshot(dd, vpu, download_dir, extract_dir)
        if shp is None:
            failures.append(vpu)
            continue
        gdf = read_waterbody_attrs(shp)
        gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
        # Reproject before dissolving: the tie-break/output `area_sqkm` need an
        # equal-area CRS, and reprojecting each VPU individually (rather than
        # labelling a later concatenated frame with one VPU's CRS) avoids
        # silently mis-projecting every other VPU's polygons if their source
        # CRS differs (mirrors nhd_burn_components).
        gdf = gdf.to_crs(5070)
        n_raw_total += len(gdf)
        logger.info(f"VPU {vpu}: {len(gdf)} NHDWaterbody polygons")
        outputs.append(dissolve_named_parts(gdf))

    if failures:
        # A silently dropped VPU under-stages the waterbody set there — fail loud.
        raise RuntimeError(f"NHDSnapshot download/read failed for VPU(s): {failures}")

    if not outputs:
        raise ValueError("0 NHDWaterbody polygons staged across all VPUs.")

    out = gpd.GeoDataFrame(pd.concat(outputs, ignore_index=True), crs=5070)
    n_merged = int(out["member_comid"].str.contains(",").sum())
    logger.info(
        f"Dissolved same-GNIS_ID parts per VPU: {n_raw_total} raw polygons "
        f"across {len(outputs)} VPUs -> {len(out)} waterbodies "
        f"({n_merged} are multi-part merges)"
    )

    out_path = out_dir / "nhd_waterbodies.parquet"
    out.to_parquet(out_path)
    logger.info(
        f"Wrote {len(out)} waterbodies ({out['area_sqkm'].sum():,.1f} km2) -> {out_path}"
    )


if __name__ == "__main__":
    main()
