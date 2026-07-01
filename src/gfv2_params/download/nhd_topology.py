"""Stage NHDPlusV2 PlusFlowlineVAA topology -> a flat per-flowline parquet.

NHDPlus carries authoritative network direction and membership for every
flowline (FromNode/ToNode, Hydroseq/DnHydroseq) independent of the NHDFlowline
FLOWDIR field. The depstor flow-through classifier consumes the distilled
`flowline_topology.parquet` to determine which waterbodies discharge to the
routed network (rule D1 in download/nhd_flowthrough.py).
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
from gfv2_params.download.nhd_flowlines import (
    _S3_HOST,
    _S3_NS,
    _base_url,
    vpu_index,
)
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_topology")

# VAA fields distilled (canonical upper-case). DnHydroseq drives the routed-
# network test; the rest are carried for diagnostics / follow-up sink detection.
_VAA_FIELDS = [
    "COMID", "DNHYDROSEQ", "HYDROSEQ", "TERMINALFL",
    "STARTFLAG", "STREAMORDE", "FROMNODE", "TONODE",
]


def _pick_attributes_key(keys: list[str], vpu: str) -> str | None:
    """Highest-version NHDPlusAttributes 7z S3 key for a VPU, or None.

    Mirrors nhd_flowlines._pick_snapshot_key but matches the Attributes
    component. Version numbers are not uniform across VPUs, so the version is
    discovered from the bucket listing rather than hardcoded.
    """
    pat = re.compile(rf"_{re.escape(vpu)}_NHDPlusAttributes_(\d+)\.7z$")
    matches = sorted((m.group(1), k) for k in keys for m in [pat.search(k)] if m)
    return matches[-1][1] if matches else None


def _attributes_url(dd: str, vpu: str) -> str | None:
    """Discover the NHDPlusAttributes archive URL for a VPU via the S3 listing."""
    prefix = _base_url(dd, vpu).split(".amazonaws.com/", 1)[1]
    r = requests.get(f"{_S3_HOST}/?list-type=2&prefix={prefix}/", timeout=60)
    r.raise_for_status()
    keys = [e.text for e in ET.fromstring(r.text).iter(f"{_S3_NS}Key")]
    key = _pick_attributes_key(keys, vpu)
    return f"{_S3_HOST}/{key}" if key else None


def download_attributes(
    dd: str, vpu: str, download_dir: Path, extract_dir: Path
) -> Path | None:
    """Download + extract a VPU's NHDPlusAttributes; return PlusFlowlineVAA.dbf."""
    url = _attributes_url(dd, vpu)
    if url is None:
        logger.error(f"NHDPlusAttributes not found in S3 listing for VPU {vpu}")
        return None
    filename = url.rsplit("/", 1)[1]
    candidate = download_dir / filename
    if candidate.exists():
        logger.info(f"Already downloaded: {filename}")
    else:
        logger.info(f"Downloading {filename} ...")
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

    out_dir = extract_dir / vpu / "NHDPlusAttributes"
    out_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(candidate, mode="r") as archive:
        targets = [n for n in archive.getnames()
                   if Path(n).name.lower() == "plusflowlinevaa.dbf"]
        archive.extract(path=out_dir, targets=targets)
    vaas = list(out_dir.glob("**/PlusFlowlineVAA.dbf"))
    if not vaas:
        logger.error(f"PlusFlowlineVAA.dbf not found in attributes for VPU {vpu}")
        return None
    return vaas[0]


def read_vaa(path: Path) -> pd.DataFrame:
    """Read the VAA fields (case-insensitive) normalised to upper-case names."""
    available = list(pyogrio.read_info(path)["fields"])
    by_upper = {name.upper(): name for name in available}
    rename = {}
    for canon in _VAA_FIELDS:
        actual = by_upper.get(canon)
        if actual is None:
            raise KeyError(
                f"{path}: PlusFlowlineVAA has no '{canon}' field "
                f"(case-insensitive). Available: {available}"
            )
        rename[actual] = canon
    df = pyogrio.read_dataframe(path, columns=list(rename), read_geometry=False)
    return df.rename(columns=rename)[_VAA_FIELDS]


def write_topology(df: pd.DataFrame, out_path: Path) -> None:
    """Write the distilled topology to a parquet, lower-case columns, comid int64."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out = df.rename(columns={c: c.lower() for c in df.columns}).copy()
    out["comid"] = out["comid"].astype("int64")
    out.to_parquet(out_path, index=False)


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    failures = []
    for vpu, dd in vpu_index.items():
        vaa_path = download_attributes(dd, vpu, download_dir, extract_dir)
        if vaa_path is None:
            failures.append(vpu)
            continue
        df = read_vaa(vaa_path)
        logger.info(f"VPU {vpu}: {len(df)} VAA flowline records")
        frames.append(df)

    if failures:
        raise RuntimeError(f"NHDPlusAttributes staging failed for VPU(s): {failures}")

    combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["COMID"])
    if combined.empty:
        raise ValueError("distilled 0 VAA records across all VPUs — check inputs")

    out_path = data_root / "input/nhd/flowline_topology.parquet"
    write_topology(combined, out_path)
    logger.info(f"Wrote {len(combined)} topology records -> {out_path}")


if __name__ == "__main__":
    main()
