"""Stage EPA Level III/IV Ecoregions of the conterminous US as a shared input.

Reusable across parameterizations; not fabric-specific. Ecoregions provide the
geoclimatic stratification the `dprst_depth` builder (issue #173) uses to join
depression-storage polygons to a physiographic region by centroid, feeding the
per-ecoregion regional-fill of dprst depth (median null / CV-selected
calibrated-Hollister — see dprst_depth.fill).

Source: EPA's public S3-hosted ecoregion archive (reachable over HTTPS from
this HPC; the epa.gov host itself is not verified reachable):
    https://dmap-prod-oms-edc.s3.us-east-1.amazonaws.com/ORD/Ecoregions/us/us_eco_l3.zip
    https://dmap-prod-oms-edc.s3.us-east-1.amazonaws.com/ORD/Ecoregions/us/us_eco_l4.zip

Level III (≈77-85 CONUS regions, field US_L3CODE; exact count varies by
dataset vintage) is the default; Level IV is a finer alternative (field
US_L4CODE) for sensitivity testing only.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import requests

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

logger = configure_logging("download_epa_ecoregions")

ECO_ID_FIELD = "US_L3CODE"  # Level III; L4 available via US_L4CODE

_S3_BASE = "https://dmap-prod-oms-edc.s3.us-east-1.amazonaws.com/ORD/Ecoregions/us"
_TARGET_CRS = "EPSG:5070"


def ecoregion_of(
    points_gdf: gpd.GeoDataFrame,
    eco_gdf: gpd.GeoDataFrame,
    id_field: str = ECO_ID_FIELD,
) -> pd.Series:
    """Assign each polygon its ecoregion by centroid-in-polygon join.

    `points_gdf` is typically a polygon layer (e.g. dprst clumps); only its
    centroid is used. Returns a Series aligned to `points_gdf`'s index.
    """
    pts = points_gdf.set_geometry(points_gdf.geometry.centroid)
    eco = eco_gdf.to_crs(points_gdf.crs)[[id_field, "geometry"]]
    hit = gpd.sjoin(pts, eco, how="left", predicate="within")
    return hit.groupby(level=0)[id_field].first()


def _zip_url(level: int) -> str:
    if level not in (3, 4):
        raise ValueError(f"level must be 3 or 4, got {level}")
    return f"{_S3_BASE}/us_eco_l{level}.zip"


def _download(url: str, dest: Path, timeout: int = 120) -> None:
    """Stream *url* to *dest*, failing loud on a non-200 response."""
    logger.info("Downloading %s ...", url)
    with requests.get(url, stream=True, timeout=timeout) as r:
        if r.status_code != 200:
            raise RuntimeError(
                f"Failed to download {url}: HTTP {r.status_code}"
            )
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
    logger.info("Downloaded to %s (%d bytes)", dest, dest.stat().st_size)


def stage_ecoregions(dest_dir: Path, level: int = 3, logger=logger) -> Path:
    """Download EPA Level III/IV ecoregions to dest_dir/us_eco_l{level}.gpkg.

    Reprojects to EPSG:5070. Idempotent: returns the existing gpkg without
    re-downloading if already staged. Fails loud (raises) on a non-200
    download response or a missing shapefile inside the zip — never silently
    skips staging.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / f"us_eco_l{level}.gpkg"
    if out.exists():
        logger.info("Ecoregion layer already staged: %s", out)
        return out

    id_field = "US_L3CODE" if level == 3 else "US_L4CODE"
    shp_name = f"us_eco_l{level}.shp"
    url = _zip_url(level)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        zip_path = tmp_path / f"us_eco_l{level}.zip"
        _download(url, zip_path)

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)

        shp_matches = list(extract_dir.rglob(shp_name))
        if not shp_matches:
            available = sorted(p.name for p in extract_dir.rglob("*.shp"))
            raise FileNotFoundError(
                f"{shp_name} not found inside {url} — available shapefiles: "
                f"{available}"
            )
        shp_path = shp_matches[0]

        gdf = gpd.read_file(shp_path)
        if id_field not in gdf.columns:
            raise KeyError(
                f"{shp_path} has no '{id_field}' field. Available: "
                f"{list(gdf.columns)}"
            )
        gdf = gdf.to_crs(_TARGET_CRS)

        n_invalid = int((~gdf.geometry.is_valid).sum())
        if n_invalid:
            logger.info(
                "Repairing %d invalid geometr%s (e.g. ring self-intersection, "
                "a known artifact of the EPA source shapefile) via make_valid",
                n_invalid, "y" if n_invalid == 1 else "ies",
            )
            gdf["geometry"] = gdf.geometry.make_valid()

        tmp_out = tmp_path / out.name
        gdf.to_file(tmp_out, driver="GPKG")
        shutil.move(str(tmp_out), str(out))

    logger.info(
        "Staged %d ecoregion features (%s) -> %s", len(gdf), id_field, out
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage EPA Level III/IV Ecoregions as a shared input."
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=None,
        help="Destination directory (default: {data_root}/input/ecoregions "
        "from base_config.yml).",
    )
    parser.add_argument(
        "--level",
        type=int,
        choices=(3, 4),
        default=3,
        help="EPA Ecoregion level to stage (default: 3).",
    )
    args = parser.parse_args()

    if args.dest is not None:
        dest_dir = args.dest
    else:
        base = load_base_config()
        dest_dir = Path(base["data_root"]) / "input/ecoregions"

    out = stage_ecoregions(dest_dir, level=args.level, logger=logger)
    logger.info("Done: %s", out)


if __name__ == "__main__":
    main()
