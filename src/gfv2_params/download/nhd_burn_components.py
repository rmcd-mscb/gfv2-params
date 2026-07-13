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

* `burn_add_waterbodies.parquet` — waterbody polygons NHDPlus added for the burn that
  are absent from NHDWaterbody. These are genuinely new depression AREA (VPU 16 alone:
  23 polygons, 374.5 km², largest a 136.8 km² playa; 0 of 23 overlap an existing
  waterbody). `waterbody.py` unions them into the waterbody layer, after which they
  flow through waterbody -> dprst -> routing untouched and become dprst pour-points.
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

# BurnAddWaterbody PurpCode -> NHD FTYPE. Deliberately exhaustive: an unrecognised
# code raises rather than defaulting, because FTYPE drives NEVER_ONSTREAM_FTYPES
# (a mis-defaulted Playa would become promotable on-stream).
PURPCODE_TO_FTYPE = {4: "Playa", 8: "LakePond"}


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


def burn_add_to_waterbody_frame(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reshape BurnAddWaterbody.shp into the waterbody-layer schema.

    `PolyID` (always negative) becomes both COMID and member_comid, so
    depstor.select_connected_waterbodies can join without a KeyError — and so these
    polygons can never match a WBAREACOMI / flow-through COMID (all positive). That
    makes them structurally incapable of on-stream promotion, which is correct:
    NHDPlus flagged every one of them as a sink.
    """
    purpcode_col = _resolve_field(gdf, "PurpCode")
    polyid_col = _resolve_field(gdf, "PolyID")

    unknown = sorted(set(gdf[purpcode_col].astype(int)) - set(PURPCODE_TO_FTYPE))
    if unknown:
        raise ValueError(
            f"BurnAddWaterbody carries unrecognised PurpCode(s) {unknown}; refusing "
            f"to guess a FTYPE. FTYPE drives NEVER_ONSTREAM_FTYPES, so a wrong "
            f"default would let a Playa be promoted on-stream. Known codes: "
            f"{sorted(PURPCODE_TO_FTYPE)} — extend PURPCODE_TO_FTYPE deliberately."
        )
    out = gpd.GeoDataFrame(
        {
            "GNIS_ID": pd.Series([None] * len(gdf), dtype="object"),
            "GNIS_NAME": pd.Series([None] * len(gdf), dtype="object"),
            "COMID": gdf[polyid_col].astype("int64").to_numpy(),
            "FTYPE": gdf[purpcode_col].astype(int).map(PURPCODE_TO_FTYPE).to_numpy(),
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
        sinks.append(s)
        if burn_shp is None:
            # Already logged (with reason) inside download_burn_components.
            continue
        b = gpd.read_file(burn_shp)
        if len(b) == 0:
            continue
        logger.info(f"VPU {vpu}: {len(b)} BurnAddWaterbody polygons")
        burns.append(burn_add_to_waterbody_frame(b).to_crs(5070))

    if failures:
        raise RuntimeError(
            f"NHDPlusBurnComponents staging failed for VPU(s): {failures}. A silently "
            f"dropped VPU under-stages the sink/BurnAdd set there — fix, do not skip."
        )

    sink_out = out_dir / "sink_points.parquet"
    gpd.GeoDataFrame(pd.concat(sinks, ignore_index=True), crs=sinks[0].crs).to_crs(
        5070
    ).to_parquet(sink_out)
    logger.info(f"Wrote {sink_out} ({sum(len(s) for s in sinks)} sinks)")

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
