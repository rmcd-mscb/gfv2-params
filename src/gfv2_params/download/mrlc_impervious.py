from pathlib import Path
from zipfile import ZipFile

import requests

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

# List of MRLC NLCD data URLs
urls = [
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_1985-1994_CU_C1V0.zip",
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_1995-2004_CU_C1V0.zip",
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_2005-2014_CU_C1V0.zip",
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_2015-2023_CU_C1V0.zip"
]

logger = configure_logging("download_mrlc_impervious")


def download_and_unzip(url, out_dir):
    local_zip = out_dir / Path(url).name
    logger.info(f"Downloading {url} ...")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    logger.info(f"Downloaded to {local_zip}")

    logger.info(f"Unzipping {local_zip} ...")
    with ZipFile(local_zip, 'r') as zip_ref:
        zip_ref.extractall(out_dir)
    logger.info(f"Extracted to {out_dir}")

    # Optionally, remove the zip file after extraction
    # local_zip.unlink()


def main():
    base = load_base_config()
    data_root = Path(base["data_root"])
    output_dir = data_root / "input/mrlc_impervious"
    output_dir.mkdir(parents=True, exist_ok=True)

    for url in urls:
        download_and_unzip(url, output_dir)


if __name__ == "__main__":
    main()
