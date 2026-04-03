"""Download NHM v1.1 pre-derived LULC rasters from ScienceBase.

Source: U.S. Geological Survey (USGS) ScienceBase
Item:   5ebb182b82ce25b5136181cf
        Data Layers for the National Hydrologic Model, version 1.1
        (Wieczorek and Bock, 2021 — https://doi.org/10.5066/P971JAGF)

Downloads and extracts three zip files:
  LULC.zip  -> pre-derived 5-class LULC raster (0=bare, 1=grass, 2=shrub,
               3=deciduous forest, 4=evergreen forest) — NHM cov_type codes
  keep.zip  -> per-pixel winter leaf retention (0-100%)
  CNPY.zip  -> per-pixel canopy cover (0-100%)

These are NHM v1.1 parameterisation products, not raw FORE-SCE scenario
rasters.  Use configs/lulc_nhm_v11_param.yml and crosswalks/nhm_v11_nhm.csv
to run the pipeline with these inputs.

Extracts to: {data_root}/input/lulc_veg/nhm_v11/
"""

from pathlib import Path
from zipfile import ZipFile

import requests
import urllib3

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

# HPC clusters often have SSL inspection proxies with self-signed certificates.
# Disable verification and suppress the resulting InsecureRequestWarning.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_BASE_URL = (
    "https://www.sciencebase.gov/catalog/file/get/5ebb182b82ce25b5136181cf"
)

# (zip filename, download URL, sentinel TIF to test whether already extracted,
#  optional rename: {extracted_name -> desired_name})
_DOWNLOADS = [
    (
        "LULC.zip",
        f"{_BASE_URL}?f=__disk__dc%2Fd1%2F4d%2Fdcd14d4fa5682cff1ccdf8fee0173dfe966f4291",
        "LULC.tif",
        {},
    ),
    (
        "keep.zip",
        f"{_BASE_URL}?f=__disk__12%2F7a%2F21%2F127a21988c0b2fb432ccc8be49b9555665dc30cb",
        "keep.tif",
        {},
    ),
    (
        "CNPY.zip",
        f"{_BASE_URL}?f=__disk__c5%2F3a%2F09%2Fc53a09eb54669e1fafcf9bd5d18a1c4ecb1c7cc4",
        "CNPY.tif",
        {},
    ),
]

logger = configure_logging("download_nhm_v11_lulc")


def _download(url: str, dest: Path) -> None:
    """Stream *url* to *dest*."""
    logger.info("Downloading %s ...", dest.name)
    with requests.get(url, stream=True, timeout=600, verify=False) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
    logger.info("Downloaded: %s", dest)


def _extract_tifs(
    zip_path: Path,
    out_dir: Path,
    renames: dict[str, str] | None = None,
) -> list[Path]:
    """Extract all .tif entries from *zip_path* flat into *out_dir*.

    *renames* maps original basename → desired basename.  Applied after
    extraction so the sentinel check in ``download_and_extract`` passes.
    Returns the list of final paths.
    """
    renames = renames or {}
    extracted = []
    with ZipFile(zip_path, "r") as zf:
        tif_names = [n for n in zf.namelist() if n.lower().endswith(".tif")]
        for name in tif_names:
            src_name = Path(name).name
            final_name = renames.get(src_name, src_name)
            dest = out_dir / final_name
            if dest.exists():
                logger.info("Already extracted: %s — skipping", final_name)
                extracted.append(dest)
                continue
            logger.info("Extracting %s ...", src_name)
            data = zf.read(name)
            if final_name != src_name:
                logger.info("Renaming %s -> %s", src_name, final_name)
            dest.write_bytes(data)
            extracted.append(dest)
    return extracted


def download_and_extract(
    zip_name: str,
    url: str,
    sentinel: str,
    out_dir: Path,
    renames: dict[str, str] | None = None,
) -> Path:
    """Download and extract one zip if the sentinel TIF is not yet present.

    Returns the path to the sentinel TIF.
    """
    sentinel_path = out_dir / sentinel
    if sentinel_path.exists():
        logger.info("Already extracted: %s — skipping download", sentinel)
        return sentinel_path

    local_zip = out_dir / zip_name
    if not local_zip.exists():
        _download(url, local_zip)
    else:
        logger.info("Already downloaded: %s — skipping download", zip_name)

    extracted = _extract_tifs(local_zip, out_dir, renames)
    logger.info("Extracted %d TIF(s) from %s", len(extracted), zip_name)

    if not sentinel_path.exists():
        raise FileNotFoundError(
            f"Expected '{sentinel}' not found after extracting {zip_name}. "
            f"Contents of {out_dir}: {[p.name for p in out_dir.iterdir()]}"
        )
    return sentinel_path


def main():
    base = load_base_config()
    data_root = Path(base["data_root"])
    out_dir = data_root / "input" / "lulc_veg" / "nhm_v11"
    out_dir.mkdir(parents=True, exist_ok=True)

    for zip_name, url, sentinel, renames in _DOWNLOADS:
        path = download_and_extract(zip_name, url, sentinel, out_dir, renames)
        logger.info("Ready: %s", path)

    logger.info("FORE-SCE LULC rasters ready at: %s", out_dir)


if __name__ == "__main__":
    main()
