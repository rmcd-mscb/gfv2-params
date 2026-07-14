"""Stage NHDPlusV2 `NHDPlusBurnComponents`: Sink.shp + BurnAddWaterbody.shp.

Two products, one archive per VPU:

* `sink_points.parquet` — NHDPlus's authoritative sink list. Kept for PROVENANCE
  (`PURPCODE`/`PURPDESC`) and for the BurnAddWaterbody linkage
  (`SOURCEFC`/`FEATUREID`). It is **not** a classifier signal: the endorheic
  classifier reads the FDR grid the router reads (see gfv2_params.endorheic).

  The pre-made `input/nhd/NHD_sink_points.gpkg` is a STRICT SUBSET of this — 537
  sinks in VPU 16 against 3,222 here — because it omits `PURPCODE 1`
  ("BurnLineEvent network end") entirely, which is precisely the class NHDPlus uses
  to mark where a burned flowline's network terminates. It therefore contains 0 sinks
  inside Great Salt Lake, where NHDPlus has 29. Do not use it.

* `burn_add_waterbodies.parquet` — the SINK-PURPOSE subset of BurnAddWaterbody.shp:
  waterbody polygons NHDPlus added for the burn that are absent from NHDWaterbody.
  These are genuinely new depression AREA (VPU 16 alone: 23 polygons, 374.5 km²,
  largest a 136.8 km² playa; 0 of 23 overlap an existing waterbody). `waterbody.py`
  unions them into the waterbody layer, after which they flow through waterbody ->
  dprst -> routing untouched and become dprst pour-points.

  BurnAddWaterbody is NOT a sink layer. It is the general "waterbodies added to the
  DEM burn" layer, and only the rows carrying a sink `PurpCode` are sinks (see
  PURPCODE_IS_SINK). Measured on the real archives: VPU 16's rows all have PurpCode
  populated and every one is referenced by a sink, but VPU 01 ships 702 rows whose
  PurpCode/PurpDesc are entirely NULL, VPU 01's own Sink.shp holds ZERO sinks, 503 of
  those 702 are ON-network (OnOffNet = 1), and their FCodes include 12 × 46006
  (StreamRiver) and 1 × 33600 (CanalDitch). Merging them would classify canals and
  river reaches as depression storage. So `burn_add_to_waterbody_frame` keeps only the
  sink-purpose rows and drops the rest.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import geopandas as gpd
import pandas as pd
import py7zr
import requests

from gfv2_params.config import load_base_config
from gfv2_params.download.nhd_flowlines import (
    _S3_HOST,
    _S3_NS,
    _base_url,
    vpu_index,
)
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_burn_components")

# Is this BurnAddWaterbody PurpCode a SINK (depression-storage) purpose? Measured
# across all 21 CONUS VPU archives (PurpCode is a STRING domain — it carries "NT"):
#
#   PurpCode  PurpDesc                                      rows  FCODEs
#   4         BurnAddWaterbody Playa                          86  36100
#   5         NHD Waterbody closed lake                       21  36100 (17), 46600 (4)
#   8         BurnAddWaterbody closed lake                  1551  39000/39001/39004/39009 (1550), 46600 (1)
#   NT        Canada National Topographic Data Base (NTDB)     3  39004 (2), 46006 (1)
#   (NULL)    —                                              704  39004/39000/46006/44500/33600
#
# 4/5/8 are the closed-lake / playa purposes → depression storage. PurpCode 5's rows
# are NOT already in `conus_waterbodies.gpkg` despite the "NHD Waterbody" wording
# (measured: 0.000 area overlap with the existing layer for all 21) — they are 17 real
# Rio Grande playas and 4 swamp/marsh polygons, i.e. genuinely new depression area.
# `NT` is a PROVENANCE tag on Canadian fill polygons, not a sink purpose (one of the 3
# is a 26.7 km² StreamRiver), and NULL means "added to the DEM burn, not a sink".
# Both are dropped: merging them would turn river reaches and canals into depression
# storage. A POPULATED code absent from this table RAISES rather than defaulting.
PURPCODE_IS_SINK = {"4": True, "5": True, "8": True, "NT": False}

# NHD FCODE (first three digits) -> FTYPE. FCODE, not PurpCode, is the authoritative
# FTYPE source here: it is populated on every BurnAddWaterbody row, and PurpCode 5
# spans BOTH Playa (36100) and SwampMarsh (46600), so a PurpCode->FTYPE map would
# mislabel 17 real playas as lakes. FTYPE drives NEVER_ONSTREAM_FTYPES,
# EXCLUDE_WATERBODY_FTYPES and the dprst_depth donor grouping, so an unrecognised
# FCODE raises rather than defaulting.
FTYPE_BY_FCODE_PREFIX = {
    336: "CanalDitch",
    361: "Playa",
    378: "Ice Mass",
    390: "LakePond",
    436: "Reservoir",
    445: "SeaOcean",
    460: "StreamRiver",
    466: "SwampMarsh",
    493: "Estuary",
}

# A sink-purpose polygon that is a CONVEYANCE is a contradiction in terms — this is
# precisely the failure mode the NULL/NT drop exists to prevent, so assert it on the
# rows we KEEP rather than trusting the PurpCode domain to stay clean forever.
CONVEYANCE_FTYPES = {"StreamRiver", "CanalDitch", "ArtificialPath"}


def pick_component_key(keys: list[str], vpu: str) -> str | None:
    """Highest-version NHDPlusBurnComponents 7z S3 key for a VPU, or None.

    Mirrors nhd_flowlines._pick_snapshot_key. Version numbers are not uniform across
    VPUs, so the version is discovered from the bucket listing, not hardcoded.
    """
    pat = re.compile(rf"_{re.escape(vpu)}_NHDPlusBurnComponents_(\d+)\.7z$")
    matches = sorted((m.group(1), k) for k in keys for m in [pat.search(k)] if m)
    return matches[-1][1] if matches else None


def _component_url(dd: str, vpu: str) -> str | None:
    prefix = _base_url(dd, vpu).split(".amazonaws.com/", 1)[1]
    r = requests.get(f"{_S3_HOST}/?list-type=2&prefix={prefix}/", timeout=60)
    r.raise_for_status()
    keys = [e.text for e in ET.fromstring(r.text).iter(f"{_S3_NS}Key")]
    key = pick_component_key(keys, vpu)
    return f"{_S3_HOST}/{key}" if key else None


def download_burn_components(
    dd: str, vpu: str, download_dir: Path, extract_dir: Path
) -> tuple[Path | None, Path | None]:
    """Download + extract a VPU's NHDPlusBurnComponents.

    Returns (Sink.shp, BurnAddWaterbody.shp); either may be None if the archive
    genuinely lacks it (VPU 16 has both; some VPUs have no BurnAddWaterbody).
    """
    url = _component_url(dd, vpu)
    if url is None:
        logger.error(f"NHDPlusBurnComponents not found in S3 listing for VPU {vpu}")
        return None, None
    filename = url.rsplit("/", 1)[1]
    archive = download_dir / filename

    if archive.exists():
        logger.info(f"Already downloaded: {filename}")
    else:
        logger.info(f"Downloading {filename} ...")
        # Download to a .part sidecar and atomically rename only after a
        # size-verified, complete write, so an interrupted download (node
        # preemption, walltime) can't leave a truncated archive that the next
        # run silently reuses via the archive.exists() short-circuit above.
        tmp = archive.with_suffix(archive.suffix + ".part")
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            expected = int(r.headers.get("Content-Length", 0))
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        got = tmp.stat().st_size
        if expected and got != expected:
            tmp.unlink(missing_ok=True)
            raise OSError(f"{filename}: downloaded {got} bytes, expected {expected}")
        tmp.rename(archive)

    # Always re-extract rather than trusting a possibly-partial target dir: a
    # walltime kill mid-extraction can leave Sink.shp written but
    # BurnAddWaterbody.shp not yet extracted, and a bare `if not
    # target.exists()` skip would then treat that partial extraction as done
    # forever, silently dropping BurnAddWaterbody for that VPU on every future run.
    target = extract_dir / f"burncomponents_{vpu}"
    target.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(archive, mode="r") as a:
        a.extractall(path=target)

    sink = next(iter(target.rglob("Sink.shp")), None)
    burn = next(iter(target.rglob("BurnAddWaterbody.shp")), None)
    if sink is None:
        logger.error(f"Sink.shp not found in extracted burn components for VPU {vpu}")
    if burn is None:
        logger.info(
            f"BurnAddWaterbody.shp not found in extracted burn components for VPU "
            f"{vpu} (this is normal for some VPUs)"
        )
    return sink, burn


def _resolve_field(gdf: gpd.GeoDataFrame, canon: str) -> str:
    """Case-insensitive column lookup for a raw NHDPlus shapefile field.

    Sink.shp/BurnAddWaterbody.shp are the same class of raw per-VPU NHDPlus
    shapefile as NHDFlowline/PlusFlowlineVAA, where field-name casing is known
    to vary across VPUs (e.g. VPU 12 ships COMID/WBAREACOMI, VPU 13 ships
    ComID/WBAreaComI). Raises a descriptive KeyError (not a bare
    ``KeyError: 'PurpCode'``) if the field is genuinely absent, so a real
    schema problem is actionable mid-CONUS-run rather than a mystery 15 VPUs in.
    """
    by_upper = {c.upper(): c for c in gdf.columns}
    actual = by_upper.get(canon.upper())
    if actual is None:
        raise KeyError(
            f"BurnAddWaterbody has no '{canon}' field (case-insensitive). "
            f"Available fields: {list(gdf.columns)}"
        )
    return actual


def normalize_purpcode(value) -> str | None:
    """Canonical string form of a raw PurpCode cell, or None when it is NULL.

    The domain is mixed: numeric codes arrive as int, float or str across VPUs
    ("4", 4, 4.0 all mean PurpCode 4), and "NT" is a genuine non-numeric code. NULL
    (None/NaN/blank) means "not a sink-purpose polygon", not "unknown code".
    """
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    try:
        return str(int(float(s)))
    except ValueError:
        return s.upper()


def ftype_for_fcode(fcode) -> str:
    """NHD FTYPE for an FCODE (its first three digits). Raises on an unknown FTYPE."""
    prefix = int(fcode) // 100
    ftype = FTYPE_BY_FCODE_PREFIX.get(prefix)
    if ftype is None:
        raise ValueError(
            f"BurnAddWaterbody row carries FCODE {fcode} (FTYPE code {prefix}), which "
            f"is not in FTYPE_BY_FCODE_PREFIX; refusing to guess a FTYPE. FTYPE drives "
            f"NEVER_ONSTREAM_FTYPES / EXCLUDE_WATERBODY_FTYPES, so a wrong default "
            f"would misclassify this polygon. Extend FTYPE_BY_FCODE_PREFIX "
            f"deliberately. Known: {sorted(FTYPE_BY_FCODE_PREFIX)}"
        )
    return ftype


def burn_add_to_waterbody_frame(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reshape the SINK-PURPOSE rows of BurnAddWaterbody.shp into the waterbody schema.

    Rows whose `PurpCode` is not a sink purpose (PURPCODE_IS_SINK) are DROPPED, because
    BurnAddWaterbody is not a sink layer (module docstring): VPU 01's 702 rows carry a
    NULL PurpCode, 503 of them are on-network, and they include StreamRiver and
    CanalDitch FCodes. Merging those would turn canals and river reaches into
    depression storage. A NULL PurpCode means "not a sink-purpose polygon" and is
    dropped; a POPULATED but unrecognised PurpCode still raises (FTYPE drives
    NEVER_ONSTREAM_FTYPES, so it must never be guessed).

    FTYPE comes from FCODE (see FTYPE_BY_FCODE_PREFIX), and every retained row is
    asserted to be a non-conveyance FTYPE.

    `PolyID` (always negative) becomes both COMID and member_comid, so
    depstor.select_connected_waterbodies can join without a KeyError — and so these
    polygons can never match a WBAREACOMI / flow-through COMID (all positive). That
    makes them structurally incapable of on-stream promotion, which is correct:
    every retained row is a sink-purpose polygon.

    Returns an EMPTY frame (not an error) when a VPU has no sink-purpose row at all —
    VPU 01 (702 rows, none sink-purpose) is exactly that case.
    """
    purpcode_col = _resolve_field(gdf, "PurpCode")
    polyid_col = _resolve_field(gdf, "PolyID")
    fcode_col = _resolve_field(gdf, "FCode")

    code = gdf[purpcode_col].map(normalize_purpcode)
    unknown = sorted(set(code.dropna().unique()) - set(PURPCODE_IS_SINK))
    if unknown:
        raise ValueError(
            f"BurnAddWaterbody carries unrecognised PurpCode(s) {unknown}; refusing "
            f"to guess whether they are sinks. A wrong default would either drop real "
            f"depression area or turn a river reach into depression storage. Known "
            f"codes: {sorted(PURPCODE_IS_SINK)} — extend PURPCODE_IS_SINK "
            f"deliberately, from the layer's own PurpDesc/FCode."
        )
    sink_codes = [c for c, is_sink in PURPCODE_IS_SINK.items() if is_sink]
    keep = code.isin(sink_codes)
    n_dropped = int((~keep).sum())
    if n_dropped:
        dropped = code[~keep].fillna("(NULL)").value_counts().to_dict()
        logger.info(
            f"  dropped {n_dropped} of {len(gdf)} BurnAddWaterbody polygons with no "
            f"sink PurpCode {dropped} — NULL = added to the DEM burn but not a sink, "
            f"NT = Canadian NTDB fill polygon; keeping them would classify on-network "
            f"canals/river reaches as depression storage"
        )
    gdf = gdf[keep]

    ftype = gdf[fcode_col].map(ftype_for_fcode)
    conveyance = sorted(set(ftype) & CONVEYANCE_FTYPES)
    if conveyance:
        raise ValueError(
            f"sink-purpose BurnAddWaterbody rows carry conveyance FTYPE(s) "
            f"{conveyance} — a canal/river reach is not depression storage. Refusing "
            f"to merge them into the waterbody layer; check the archive's "
            f"PurpCode/FCode domain."
        )
    out = gpd.GeoDataFrame(
        {
            "GNIS_ID": pd.Series([None] * len(gdf), dtype="object"),
            "GNIS_NAME": pd.Series([None] * len(gdf), dtype="object"),
            "COMID": gdf[polyid_col].astype("int64").to_numpy(),
            "FTYPE": ftype.to_numpy(),
            "member_comid": gdf[polyid_col].astype("int64").to_numpy(),
            "area_sqkm": (gdf.to_crs(5070).geometry.area / 1e6).to_numpy(),
        },
        geometry=gdf.geometry.to_numpy(),
        crs=gdf.crs,
    )
    if (out["COMID"] >= 0).any():
        raise ValueError(
            "BurnAddWaterbody PolyID is expected to be negative (that is what makes "
            "these polygons unable to match a positive WBAREACOMI/flow-through COMID). "
            "A non-negative PolyID would silently become on-stream-promotable."
        )
    return out


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    out_dir = data_root / "input/nhd"
    for d in (download_dir, extract_dir, out_dir):
        d.mkdir(parents=True, exist_ok=True)

    sinks, burns, failures = [], [], []
    for vpu, dd in vpu_index.items():
        sink_shp, burn_shp = download_burn_components(dd, vpu, download_dir, extract_dir)
        if sink_shp is None:
            failures.append(vpu)
            continue
        s = gpd.read_file(sink_shp)
        s["vpu"] = vpu
        logger.info(f"VPU {vpu}: {len(s)} sinks")
        # Reproject each VPU's frame individually before concatenating —
        # mirrors the `burns` handling below. Labelling the concatenated
        # multi-VPU frame with a single VPU's CRS (as this used to do) silently
        # mis-projects every other VPU's sink points if their source CRS differs.
        sinks.append(s.to_crs(5070))
        if burn_shp is None:
            # Already logged (with reason) inside download_burn_components.
            continue
        b = gpd.read_file(burn_shp)
        if len(b) == 0:
            continue
        logger.info(f"VPU {vpu}: {len(b)} BurnAddWaterbody polygons")
        sink_purpose = burn_add_to_waterbody_frame(b)
        if len(sink_purpose) == 0:
            # Legitimate: a VPU can ship BurnAddWaterbody rows with no sink PurpCode
            # at all (VPU 01 ships 702 such rows and zero sinks). They add no
            # depression area — see burn_add_to_waterbody_frame.
            logger.info(f"VPU {vpu}: 0 of {len(b)} are sink-purpose (PurpCode 4/5/8)")
            continue
        logger.info(f"VPU {vpu}: {len(sink_purpose)} sink-purpose BurnAddWaterbody kept")
        burns.append(sink_purpose.to_crs(5070))

    if failures:
        raise RuntimeError(
            f"NHDPlusBurnComponents staging failed for VPU(s): {failures}. A silently "
            f"dropped VPU under-stages the sink/BurnAdd set there — fix, do not skip."
        )

    sink_out = out_dir / "sink_points.parquet"
    combined_sinks = gpd.GeoDataFrame(pd.concat(sinks, ignore_index=True), crs=5070)
    combined_sinks.to_parquet(sink_out)
    logger.info(f"Wrote {sink_out} ({len(combined_sinks)} sinks)")

    if not burns:
        raise ValueError(
            "0 BurnAddWaterbody polygons staged across all VPUs — that would add no "
            "depression area at all. Expected >= 23 in VPU 16 alone; investigate."
        )
    burn_out = out_dir / "burn_add_waterbodies.parquet"
    combined = gpd.GeoDataFrame(pd.concat(burns, ignore_index=True), crs=5070)
    combined.to_parquet(burn_out)
    logger.info(
        f"Wrote {burn_out} ({len(combined)} polygons, "
        f"{combined.geometry.area.sum() / 1e6:,.1f} km2)"
    )


if __name__ == "__main__":
    main()
