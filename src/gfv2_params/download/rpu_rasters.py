from pathlib import Path

import py7zr
import requests

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

# Define RPU metadata dictionary
rpu_index = {
    "01": {
        "dd": "NE",
        "rpu_ids": ["01a"],
    },
    "02": {
        "dd": "MA",
        "rpu_ids": ["02a", "02b"],
    },
    "03N": {
        "dd": "SA",
        "rpu_ids": ["03a", "03b"],
    },
    "03S": {
        "dd": "SA",
        "rpu_ids": ["03c", "03d"],
    },
    "03W": {
        "dd": "SA",
        "rpu_ids": ["03e", "03f"],
    },
    "04": {
        "dd": "GL",
        "rpu_ids": ["04a", "04b", "04c", "04d"],
    },
    "05": {
        "dd": "MS",
        "rpu_ids": ["05a", "05b", "05c", "05d"],
    },
    "06": {
        "dd": "MS",
        "rpu_ids": ["06a"],
    },
    "07": {
        "dd": "MS",
        "rpu_ids": ["07a", "07b", "07c"],
    },
    "08": {
        "dd": "MS",
        "rpu_ids": ["08a", "08b", "03g"],
    },
    "09": {
        "dd": "SR",
        "rpu_ids": ["09a"],
    },
    "10L": {
        "dd": "MS",
        "rpu_ids": ["10a", "10b", "10c", "10d"],
    },
    "10U": {
        "dd": "MS",
        "rpu_ids": ["10e", "10f", "10g", "10h", "10i"],
    },
    "11": {
        "dd": "MS",
        "rpu_ids": ["11a", "11b", "11c", "11d"],
    },
    "12": {
        "dd": "TX",
        "rpu_ids": ["12a", "12b", "12c", "12d"],
    },
    "13": {
        "dd": "RG",
        "rpu_ids": ["13a", "13b", "13c", "13d"],
    },
    "14": {
        "dd": "CO",
        "rpu_ids": ["14a", "14b"],
    },
    "15": {
        "dd": "CO",
        "rpu_ids": ["15a", "15b"],
    },
    "16": {
        "dd": "GB",
        "rpu_ids": ["16a", "16b"],
    },
    "17": {
        "dd": "PN",
        "rpu_ids": ["17a", "17b", "17c", "17d"],
    },
    "18": {
        "dd": "CA",
        "rpu_ids": ["18a", "18b", "18c"],
    },
    "20": {
        "dd": "HI",
        "rpu_ids": ["20a", "20b", "20c", "20d", "20e", "20f", "20g", "20h"],
    },
}


components = ["NEDSnapshot", "FdrFac", "Hydrodem"]
vv = "01"  # version

logger = configure_logging("download_rpu_rasters")


def download_and_extract_old(dd, vpu, rpu, component, download_dir, extract_dir):
    if any(code in vpu for code in {"03", "10", "05", "06", "07", "08", "11", "14", "15"}):
        base_url = f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlus{dd}/NHDPlus{vpu}"
    else:
        base_url = f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlus{dd}"

    filename = f"NHDPlusV21_{dd}_{vpu}_{rpu}_{component}_{vv}.7z"
    url = f"{base_url}/{filename}"
    local_path = download_dir / filename

    logger.info(f"Checking: {url}")
    head = requests.head(url, timeout=60)
    if head.status_code != 200:
        logger.info(f"Not found: {filename}")
        return False

    if not local_path.exists():
        logger.info(f"Downloading {filename} ...")
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info(f"Downloaded: {filename}")
    else:
        logger.info(f"Already downloaded: {filename}")

    extract_path = extract_dir / vpu / rpu / component
    extract_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Extracting {filename} to {extract_path}")
    try:
        with py7zr.SevenZipFile(local_path, mode="r") as archive:
            archive.extractall(path=extract_path)
        logger.info(f"Extracted: {filename}")
    except (py7zr.Bad7zFile, py7zr.DecompressionError) as e:
        logger.error(f"Failed to extract {filename} (archive corruption): {e}")
        local_path.unlink(missing_ok=True)
        raise
    except Exception as e:
        logger.error(f"Failed to extract {filename}: {e}")
        raise

    return True


def download_and_extract(dd, vpu, rpu, component, download_dir, extract_dir):
    # Determine base URL structure
    if any(code in vpu for code in {"03", "10", "05", "06", "07", "08", "11", "14", "15"}):
        base_url = f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlus{dd}/NHDPlus{vpu}"
    else:
        base_url = f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlus{dd}"

    # If component is hydrodem, try multiple capitalizations
    component_variants = [component]
    if component.lower() == "hydrodem":
        component_variants = ["Hydrodem", "HydroDem"]

    # Try each component variant with each version, from newest to oldest
    version_candidates = ["05", "04", "03", "02", "01"]
    found = False

    for comp_variant in component_variants:
        for version in version_candidates:
            filename = f"NHDPlusV21_{dd}_{vpu}_{rpu}_{comp_variant}_{version}.7z"
            url = f"{base_url}/{filename}"
            local_path = download_dir / filename

            # Skip if already downloaded
            if local_path.exists():
                logger.info(f"Already downloaded: {filename}, skipping download & extraction")
                return True

            logger.info(f"Checking: {url}")
            head = requests.head(url, timeout=60)
            if head.status_code == 200:
                found = True
                break
        if found:
            break

    if not found:
        logger.error(f"File not found for any variant of: {component} in {vpu}-{rpu}")
        return False

    # Download
    logger.info(f"Downloading {filename} ...")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    logger.info(f"Downloaded: {filename}")

    # Extract (keep logical component name for path)
    extract_path = extract_dir / vpu / rpu / component
    extract_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Extracting {filename} to {extract_path}")
    try:
        with py7zr.SevenZipFile(local_path, mode="r") as archive:
            archive.extractall(path=extract_path)
        logger.info(f"Extracted: {filename}")
    except (py7zr.Bad7zFile, py7zr.DecompressionError) as e:
        logger.error(f"Failed to extract {filename} (archive corruption): {e}")
        local_path.unlink(missing_ok=True)
        raise
    except Exception as e:
        logger.error(f"Failed to extract {filename}: {e}")
        raise

    return True


def main():
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "work/nhd_extracted"
    download_dir.mkdir(exist_ok=True)
    extract_dir.mkdir(exist_ok=True)

    failures = []

    # Main loop
    for vpu, vpu_data in rpu_index.items():
        dd = vpu_data["dd"]
        for rpu in vpu_data["rpu_ids"]:
            for component in components:
                success = download_and_extract(dd, vpu, rpu, component, download_dir, extract_dir)
                if not success:
                    failures.append(f"{vpu}-{rpu}-{component}")

    if failures:
        logger.warning(
            "Download/extract failed for %d component(s): %s",
            len(failures),
            ", ".join(failures),
        )


if __name__ == "__main__":
    main()
