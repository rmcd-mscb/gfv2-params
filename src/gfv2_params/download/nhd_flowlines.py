"""Stage NHDPlusV2 NHDFlowline attributes and distill connected-waterbody COMIDs.

NHD encodes waterbody connectivity directly: an artificial-path NHDFlowline that
runs through a waterbody carries WBAREACOMI = that waterbody's COMID. The distinct
set of populated WBAREACOMI values is the set of on-stream (connected) waterbodies.
This module downloads the per-VPU NHDSnapshot archives, reads NHDFlowline, and
writes a flat parquet of connected COMIDs consumed by the depstor
`wbody_connectivity` builder.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import py7zr
import pyogrio
import requests

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_flowlines")

# A real waterbody COMID is a positive integer; non-positive WBAREACOMI (0, or
# the -9999 nodata sentinel some VPUs use) means the flowline does not pass
# through a waterbody.

# VPU -> drainage-area code (DD). NHDSnapshot is per-VPU (no RPU split).
vpu_index = {
    "01": "NE", "02": "MA", "03N": "SA", "03S": "SA", "03W": "SA",
    "04": "GL", "05": "MS", "06": "MS", "07": "MS", "08": "MS",
    "09": "SR", "10L": "MS", "10U": "MS", "11": "MS", "12": "TX",
    "13": "RG", "14": "CO", "15": "CO", "16": "GB", "17": "PN", "18": "CA",
}

_S3_HOST = "https://dmap-data-commons-ow.s3.amazonaws.com"
_S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def connected_comids_from_flowlines(df: pd.DataFrame) -> set[int]:
    """Distinct positive WBAREACOMI values (connected waterbody COMIDs) as ints.

    Only positive values are kept: 0 and the -9999 nodata sentinel both mean the
    flowline does not pass through a waterbody.

    Raises ValueError if any non-null WBAREACOMI fails to parse as a number.
    WBAREACOMI is a numeric COMID field, so a parse failure means a column-format
    change in the snapshot; silently coercing those to NaN would drop a VPU to an
    empty connected set and misclassify its waterbodies as depression storage.
    """
    raw = df["WBAREACOMI"]
    col = pd.to_numeric(raw, errors="coerce")
    n_lost = int(raw.notna().sum() - col.notna().sum())
    if n_lost:
        raise ValueError(
            f"{n_lost} non-null WBAREACOMI value(s) failed numeric parse — "
            "likely a column-format change in this NHDFlowline snapshot."
        )
    vals = col[col > 0]
    return {int(v) for v in vals.unique()}


def write_connected_comids(comids: set[int], out_path: Path) -> None:
    """Write the connected COMIDs to a single-column int64 parquet, sorted."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"comid": sorted(int(c) for c in comids)}).astype({"comid": "int64"})
    df.to_parquet(out_path, index=False)


def read_flowline_attrs(flowline_path: Path) -> pd.DataFrame:
    """Read COMID/WBAREACOMI from an NHDFlowline source (no geometry).

    NHD field-name casing is inconsistent across VPU snapshots (e.g. VPU 12
    ships COMID/WBAREACOMI, VPU 13 ships ComID/WBAreaComI), so the columns are
    resolved case-insensitively and normalised to canonical upper-case names.
    Requesting the exact upper-case names would make pyogrio silently drop the
    mismatched-case column, leaving the connectivity distiller to KeyError.

    Connectivity is keyed purely off non-zero WBAREACOMI (which NHD populates
    only on artificial paths), so FTYPE is not read.
    """
    available = list(pyogrio.read_info(flowline_path)["fields"])
    by_upper = {name.upper(): name for name in available}
    rename = {}
    for canon in ("COMID", "WBAREACOMI"):
        actual = by_upper.get(canon)
        if actual is None:
            raise KeyError(
                f"{flowline_path}: NHDFlowline has no '{canon}' field "
                f"(case-insensitive). Available fields: {available}"
            )
        rename[actual] = canon
    df = pyogrio.read_dataframe(
        flowline_path, columns=list(rename), read_geometry=False
    )
    return df.rename(columns=rename)


def _base_url(dd: str, vpu: str) -> str:
    nested = {"03", "10", "05", "06", "07", "08", "11", "14", "15"}
    if any(code in vpu for code in nested):
        return f"{_S3_HOST}/NHDPlusV21/Data/NHDPlus{dd}/NHDPlus{vpu}"
    return f"{_S3_HOST}/NHDPlusV21/Data/NHDPlus{dd}"


def _pick_snapshot_key(keys: list[str], vpu: str) -> str | None:
    """Highest-version NHDSnapshot (non-FGDB) S3 key for a VPU, or None.

    NHDSnapshot version numbers are not uniform across VPUs (observed 04-09), so
    the version is discovered from the bucket listing rather than probed from a
    hardcoded list. The `NHDSnapshot_<digits>` shape excludes the parallel
    `NHDSnapshotFGDB` archive and other components.
    """
    pat = re.compile(rf"_{re.escape(vpu)}_NHDSnapshot_(\d+)\.7z$")
    matches = sorted((m.group(1), k) for k in keys for m in [pat.search(k)] if m)
    return matches[-1][1] if matches else None


def _snapshot_url(dd: str, vpu: str) -> str | None:
    """Discover the NHDSnapshot archive URL for a VPU via the S3 listing."""
    prefix = _base_url(dd, vpu).split(".amazonaws.com/", 1)[1]
    r = requests.get(f"{_S3_HOST}/?list-type=2&prefix={prefix}/", timeout=60)
    r.raise_for_status()
    keys = [e.text for e in ET.fromstring(r.text).iter(f"{_S3_NS}Key")]
    key = _pick_snapshot_key(keys, vpu)
    return f"{_S3_HOST}/{key}" if key else None


def download_snapshot(dd: str, vpu: str, download_dir: Path, extract_dir: Path) -> Path | None:
    """Download + extract a VPU's NHDSnapshot; return the NHDFlowline.shp path."""
    url = _snapshot_url(dd, vpu)
    if url is None:
        logger.error(f"NHDSnapshot not found in S3 listing for VPU {vpu}")
        return None
    filename = url.rsplit("/", 1)[1]
    candidate = download_dir / filename

    if candidate.exists():
        logger.info(f"Already downloaded: {filename}")
    else:
        logger.info(f"Downloading {filename} ...")
        # Download to a .part sidecar and atomically rename only after a
        # size-verified, complete write, so an interrupted download (node
        # preemption, walltime) can't leave a truncated archive that the next
        # run silently reuses via the candidate.exists() short-circuit above.
        tmp = candidate.with_suffix(candidate.suffix + ".part")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            expected = int(r.headers.get("Content-Length", 0))
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        got = tmp.stat().st_size
        if expected and got != expected:
            tmp.unlink(missing_ok=True)
            raise OSError(f"{filename}: downloaded {got} bytes, expected {expected}")
        tmp.rename(candidate)

    out_dir = extract_dir / vpu / "NHDSnapshot"
    out_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(candidate, mode="r") as archive:
        archive.extractall(path=out_dir)

    shps = list(out_dir.glob("**/NHDFlowline.shp"))
    if not shps:
        logger.error(f"NHDFlowline.shp not found in extracted snapshot for VPU {vpu}")
        return None
    return shps[0]


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    connected: set[int] = set()
    failures = []
    for vpu, dd in vpu_index.items():
        flowline = download_snapshot(dd, vpu, download_dir, extract_dir)
        if flowline is None:
            failures.append(vpu)
            continue
        df = read_flowline_attrs(flowline)
        vpu_connected = connected_comids_from_flowlines(df)
        logger.info(f"VPU {vpu}: {len(vpu_connected)} connected waterbody COMIDs")
        connected |= vpu_connected

    if failures:
        # A silently dropped VPU under-flags connectivity there — make it loud.
        raise RuntimeError(f"NHDSnapshot download/read failed for VPU(s): {failures}")

    out_path = data_root / "input/nhd/connected_waterbody_comids.parquet"
    write_connected_comids(connected, out_path)
    logger.info(f"Wrote {len(connected)} connected COMIDs -> {out_path}")


if __name__ == "__main__":
    main()
