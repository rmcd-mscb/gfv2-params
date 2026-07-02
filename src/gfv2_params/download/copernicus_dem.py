"""Download Copernicus GLO-30 DEM tiles from AWS S3 for border gap fill.

Copernicus GLO-30 provides near-global 30m elevation data as 1-degree
Cloud-Optimized GeoTIFFs on a public S3 bucket (no credentials needed).

Tile naming convention:
    Copernicus_DSM_COG_10_{lat_label}_00_{lon_label}_00_DEM.tif
    - lat_label: N{dd} or S{dd}  (south edge of tile, zero-padded 2 digits)
    - lon_label: W{ddd} or E{ddd} (west edge of tile, zero-padded 3 digits)
"""

import logging
import math
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

S3_BASE_URL = "https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com"


def tile_label(lat: int, lon: int) -> str:
    """Return the Copernicus tile label for the cell whose SW corner is (lat, lon).

    Parameters
    ----------
    lat : int
        South edge latitude (e.g. 48 for the 48N-49N band).
    lon : int
        West edge longitude (e.g. -123 for the 123W-122W band).
    """
    lat_part = f"N{abs(lat):02d}" if lat >= 0 else f"S{abs(lat):02d}"
    lon_part = f"E{abs(lon):03d}" if lon >= 0 else f"W{abs(lon):03d}"
    return f"Copernicus_DSM_COG_10_{lat_part}_00_{lon_part}_00_DEM"


def tiles_for_bbox(
    south: float, north: float, west: float, east: float,
) -> list[str]:
    """Return Copernicus tile labels covering a WGS84 bounding box.

    Each tile covers 1x1 degrees.  The label encodes the SW corner.
    """
    lat_min = math.floor(south)
    lat_max = math.floor(north)
    lon_min = math.floor(west)
    lon_max = math.floor(east)
    labels = []
    for lat in range(lat_min, lat_max + 1):
        for lon in range(lon_min, lon_max + 1):
            labels.append(tile_label(lat, lon))
    return labels


def download_tiles(
    tile_labels: list[str],
    out_dir: Path,
    timeout: int = 120,
) -> tuple[list[Path], list[str]]:
    """Download Copernicus GLO-30 tiles to *out_dir*.

    Idempotent: skips files that already exist. Tiles that return HTTP 404 are
    the *expected* open-ocean/polar case (the border bbox is deliberately
    generous, see BORDER_ZONES) and are silently skipped. Tiles that fail for
    any *other* reason (timeout, 5xx, connection reset, DNS) are real download
    failures and are collected separately so the caller can distinguish
    "no land here" from "the download broke" — a count-based shortfall check
    cannot tell them apart.

    Returns
    -------
    (paths, failed) : tuple[list[Path], list[str]]
        ``paths`` — .tif files now available (freshly downloaded or pre-existing).
        ``failed`` — labels that failed for a non-404 reason (empty on a clean
        run); the caller should treat any entries as a hard error.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    failed = []
    skipped = 0

    for i, label in enumerate(tile_labels, start=1):
        tif_name = f"{label}.tif"
        local_path = out_dir / tif_name

        if local_path.exists():
            paths.append(local_path)
            skipped += 1
            continue

        url = f"{S3_BASE_URL}/{label}/{tif_name}"
        partial = local_path.with_suffix(".tif.partial")

        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                if r.status_code == 404:
                    logger.debug("Tile not available (ocean/polar): %s", label)
                    continue
                r.raise_for_status()
                with open(partial, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
            partial.rename(local_path)
            paths.append(local_path)

            if i % 50 == 0 or i == len(tile_labels):
                logger.info(
                    "Downloaded %d/%d tiles (%d skipped existing)",
                    i, len(tile_labels), skipped,
                )
        except requests.RequestException as e:
            partial.unlink(missing_ok=True)
            failed.append(label)
            logger.warning("Failed to download %s: %s", label, e)

    logger.info(
        "Download complete: %d tiles available, %d skipped (existing), "
        "%d failed (non-404), %d total requested",
        len(paths), skipped, len(failed), len(tile_labels),
    )
    return paths, failed
