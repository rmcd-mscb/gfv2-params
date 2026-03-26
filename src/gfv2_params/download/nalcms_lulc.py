"""Download and extract the NALCMS 2020 North America land cover raster (30 m).

Source: Commission for Environmental Cooperation (CEC)
URL:    https://www.cec.org/files/atlas_layers/1_terrestrial_ecosystems/
        1_01_0_land_cover_2020_30m/land_cover_2020v2_30m_tif.zip

Extracts to: {data_root}/input/lulc/nalcms_2020/
Expected raster: NA_NALCMS_landcover_2020v2_30m.tif
"""

from pathlib import Path
from zipfile import ZipFile

import requests

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

URL = (
    "https://www.cec.org/files/atlas_layers/1_terrestrial_ecosystems/"
    "1_01_0_land_cover_2020_30m/land_cover_2020v2_30m_tif.zip"
)
EXPECTED_TIF = "NA_NALCMS_landcover_2020v2_30m.tif"

logger = configure_logging("download_nalcms_lulc")


def download_and_extract(url: str, out_dir: Path) -> Path:
    """Download the NALCMS zip and extract to *out_dir*.

    Skips download if the zip already exists.  Skips extraction if the
    expected TIF already exists.  Returns the path to the extracted TIF.
    """
    local_zip = out_dir / Path(url).name
    tif_path = out_dir / EXPECTED_TIF

    if tif_path.exists():
        logger.info("Already extracted: %s — skipping download and extraction", tif_path)
        return tif_path

    if not local_zip.exists():
        logger.info("Downloading %s ...", url)
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(local_zip, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
        logger.info("Downloaded: %s", local_zip)
    else:
        logger.info("Already downloaded: %s — skipping download", local_zip)

    logger.info("Extracting %s ...", local_zip)
    with ZipFile(local_zip, "r") as zf:
        zf.extractall(out_dir)
    logger.info("Extracted to: %s", out_dir)

    if not tif_path.exists():
        # The zip may place the tif in a subdirectory; surface it.
        matches = list(out_dir.rglob(EXPECTED_TIF))
        if not matches:
            raise FileNotFoundError(
                f"Expected {EXPECTED_TIF} not found after extraction in {out_dir}. "
                f"Contents: {list(out_dir.iterdir())}"
            )
        if matches[0] != tif_path:
            matches[0].rename(tif_path)
            logger.info("Moved %s → %s", matches[0], tif_path)

    return tif_path


def main():
    base = load_base_config()
    data_root = Path(base["data_root"])
    out_dir = data_root / "input/lulc/nalcms_2020"
    out_dir.mkdir(parents=True, exist_ok=True)

    tif_path = download_and_extract(URL, out_dir)
    logger.info("NALCMS 2020 raster ready at: %s", tif_path)


if __name__ == "__main__":
    main()
