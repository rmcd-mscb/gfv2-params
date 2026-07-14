"""Stage the full WBD HUC12 layer from NHDPlusV2's per-VPU `WBDSnapshot`.

`HU_12_TYPE == 'C'` marks a closed basin (a HUC12 that contributes flow to nothing).
It is Signal B of the endorheic classifier (see gfv2_params.endorheic).

Why the FULL WBD and not `input/nhd/closed_huc12.gpkg`: that pre-made extract has
23 type-C HUC12s in the Great Basin against 141 here, and resolves only 1 of the 10
classic terminal lakes (the full WBD resolves 5 — it adds Pyramid, Lake Abert, Walker
and Summer, each of which the extract reports at frac_in = 0.000).
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

logger = configure_logging("download_wbd_huc12")


def pick_wbd_key(keys: list[str], vpu: str) -> str | None:
    """Highest-version WBDSnapshot 7z S3 key for a VPU, or None."""
    pat = re.compile(rf"_{re.escape(vpu)}_WBDSnapshot_(\d+)\.7z$")
    matches = sorted((m.group(1), k) for k in keys for m in [pat.search(k)] if m)
    return matches[-1][1] if matches else None


def _wbd_url(dd: str, vpu: str) -> str | None:
    prefix = _base_url(dd, vpu).split(".amazonaws.com/", 1)[1]
    r = requests.get(f"{_S3_HOST}/?list-type=2&prefix={prefix}/", timeout=60)
    r.raise_for_status()
    keys = [e.text for e in ET.fromstring(r.text).iter(f"{_S3_NS}Key")]
    key = pick_wbd_key(keys, vpu)
    return f"{_S3_HOST}/{key}" if key else None


def download_wbd(dd: str, vpu: str, download_dir: Path, extract_dir: Path) -> Path | None:
    """Download + extract a VPU's WBDSnapshot; return WBD_Subwatershed.shp."""
    url = _wbd_url(dd, vpu)
    if url is None:
        logger.error(f"WBDSnapshot not found in S3 listing for VPU {vpu}")
        return None
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
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    fh.write(chunk)
        got = tmp.stat().st_size
        if expected and got != expected:
            tmp.unlink(missing_ok=True)
            raise OSError(f"{filename}: downloaded {got} bytes, expected {expected}")
        tmp.rename(archive)

    # Always re-extract rather than trusting a possibly-partial target dir: a
    # walltime kill mid-extraction can leave the archive half-unpacked, and a
    # bare `if not target.exists()` skip would then treat that partial
    # extraction as done forever, silently dropping data for that VPU on every
    # future run.
    target = extract_dir / f"wbd_{vpu}"
    target.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(archive, mode="r") as a:
        a.extractall(path=target)

    shp = next(iter(target.rglob("WBD_Subwatershed.shp")), None)
    if shp is None:
        logger.error(
            f"WBD_Subwatershed.shp not found in extracted WBDSnapshot for VPU {vpu}"
        )
    return shp


def _resolve_field(gdf: gpd.GeoDataFrame, canon: str) -> str:
    """Case-insensitive column lookup for a raw WBD shapefile field.

    WBD_Subwatershed.shp is the same class of raw per-VPU NHDPlus shapefile as
    NHDFlowline/PlusFlowlineVAA/BurnAddWaterbody, where field-name casing is known
    to vary across VPUs (e.g. VPU 12 ships COMID, VPU 13 ships ComID). Raises a
    descriptive KeyError (not a bare ``KeyError: 'HU_12_TYPE'``) if the field is
    genuinely absent, so a real schema problem is actionable mid-CONUS-run rather
    than a mystery 15 VPUs in.
    """
    by_upper = {c.upper(): c for c in gdf.columns}
    actual = by_upper.get(canon.upper())
    if actual is None:
        raise KeyError(
            f"WBD layer has no '{canon}' field (case-insensitive). "
            f"Available fields: {list(gdf.columns)}"
        )
    return actual


def closed_basin_frame(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Rows whose HU_12_TYPE is 'C' (closed basin), trimmed to the keep columns.

    We apply the type filter ourselves rather than trusting an upstream selection:
    the pre-made closed_huc12.gpkg carried 219 non-C rows, 212 of them fully
    CONTRIBUTING HUC12s that merely drain into closed ones. Demoting lakes on
    their internal stream network would be wrong. A layer with no HU_12_TYPE
    field raises (via _resolve_field) rather than silently staging an empty
    table, which would make Signal B of the endorheic classifier a no-op.
    """
    huc_col = _resolve_field(gdf, "HUC_12")
    type_col = _resolve_field(gdf, "HU_12_TYPE")
    closed = gdf[gdf[type_col] == "C"]
    return gpd.GeoDataFrame(
        {
            "HUC_12": closed[huc_col].to_numpy(),
            "HU_12_TYPE": closed[type_col].to_numpy(),
        },
        geometry=closed.geometry.to_numpy(),
        crs=gdf.crs,
    )


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    out_dir = data_root / "input/wbd"
    for d in (download_dir, extract_dir, out_dir):
        d.mkdir(parents=True, exist_ok=True)

    frames, failures = [], []
    for vpu, dd in vpu_index.items():
        shp = download_wbd(dd, vpu, download_dir, extract_dir)
        if shp is None:
            failures.append(vpu)
            continue
        g = gpd.read_file(shp).to_crs(5070)
        closed = closed_basin_frame(g)
        logger.info(f"VPU {vpu}: {len(g)} HUC12s, {len(closed)} type-C (closed)")
        frames.append(closed)

    if failures:
        raise RuntimeError(f"WBDSnapshot staging failed for VPU(s): {failures}")

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=5070)
    combined = combined.drop_duplicates(subset="HUC_12")
    if combined.empty:
        raise ValueError(
            "0 closed (type-C) HUC12s staged across all VPUs → Signal B would demote "
            "nothing. Expected >= 141 in VPU 16 alone; investigate."
        )
    out = out_dir / "wbd_huc12.parquet"
    combined.to_parquet(out)
    logger.info(f"Wrote {out} ({len(combined)} closed HUC12s)")


if __name__ == "__main__":
    main()
