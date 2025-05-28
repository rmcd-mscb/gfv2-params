from pathlib import Path
from zipfile import ZipFile

import requests

# List of MRLC NLCD data URLs
urls = [
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_1985-1994_CU_C1V0.zip",
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_1995-2004_CU_C1V0.zip",
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_2005-2014_CU_C1V0.zip",
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_FctImp_2015-2023_CU_C1V0.zip"
]

# Define output directory for downloads and extraction
output_dir = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/source_data/mrlc_nlcd_fract_impervious")
output_dir.mkdir(parents=True, exist_ok=True)

def download_and_unzip(url, out_dir):
    local_zip = out_dir / Path(url).name
    print(f"Downloading {url} ...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded to {local_zip}")

    print(f"Unzipping {local_zip} ...")
    with ZipFile(local_zip, 'r') as zip_ref:
        zip_ref.extractall(out_dir)
    print(f"Extracted to {out_dir}")

    # Optionally, remove the zip file after extraction
    # local_zip.unlink()

if __name__ == "__main__":
    for url in urls:
        download_and_unzip(url, output_dir)
