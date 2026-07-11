"""Stage the WESM 1 m/QL1/QL2 3DEP workunit-footprint index as a shared input.

Reusable across parameterizations; not fabric-specific. The `dprst_depth`
builder (issue #173 Task 7) reads this staged GeoPackage to tag each dprst
polygon with its best-available topography source
(`gfv2_params.dprst_depth.topo.resolution_class`) and to look up the
1 m-project name each polygon's 1 m tiles live under
(`gfv2_params.dprst_depth.compute._project_lookup`,
`gfv2_params.dprst_depth.topo.read_window`'s `TILE1M_HTTPS_TEMPLATE`).

Reuses the download/filter logic validated by the Phase 0 diagnostic probe
(`scripts/diagnose/dprst_depth_probe.py`), now promoted to
`gfv2_params.dprst_depth.wesm_io` (Task 7b) so this staging module and the
diagnostic import a single copy — see that module's docstring for the
/vsis3/ vs /vsicurl/ GDAL access-path investigation notes on why WESM.gpkg
(~3.6 GB) is downloaded to local disk over plain HTTPS rather than read
in-place.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config
from gfv2_params.dprst_depth.wesm_io import ensure_wesm_local, load_wesm_1m_footprints
from gfv2_params.log import configure_logging

logger = configure_logging("download_wesm")

_OUTPUT_NAME = "wesm_1m_footprints.gpkg"
_TARGET_CRS = "EPSG:5070"


def stage_wesm(dest_dir: Path, logger=logger, cache_dir: Path | None = None) -> Path:
    """Download (or reuse) WESM.gpkg, filter to 1 m/QL1/QL2 footprints, and
    write `dest_dir/wesm_1m_footprints.gpkg` in EPSG:5070.

    Idempotent: returns the existing output gpkg without re-downloading or
    re-filtering if already staged. `cache_dir` (default: `dest_dir`) is
    where the ~3.6 GB raw `WESM.gpkg` download is cached/reused — pass an
    existing cache directory (e.g. one populated by a prior diagnostic run)
    to avoid re-downloading. Fails loud (raises) on a download failure or on
    0 qualifying workunits — never silently skips staging or ships an empty
    index.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / _OUTPUT_NAME
    if out.exists():
        logger.info("WESM 1m footprint index already staged: %s", out)
        return out

    cache_dir = Path(cache_dir) if cache_dir is not None else dest_dir
    wesm_path = ensure_wesm_local(cache_dir, logger)
    onem = load_wesm_1m_footprints(logger, wesm_path)
    onem = onem.to_crs(_TARGET_CRS)

    # Keep the ".gpkg" suffix on the staging path too -- pyogrio/GDAL infer
    # the output driver from the extension and warn on a bare ".tmp".
    tmp_out = dest_dir / f"_staging_{_OUTPUT_NAME}"
    onem.to_file(tmp_out, driver="GPKG")
    tmp_out.rename(out)

    logger.info(
        "Staged %d 1m/QL1/QL2 WESM footprints (%s) -> %s", len(onem), _TARGET_CRS, out
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage the WESM 1m/QL1/QL2 3DEP workunit-footprint index "
        "as a shared input."
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=None,
        help="Destination directory (default: {data_root}/input/wesm from "
        "base_config.yml).",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Directory to cache/reuse the raw ~3.6 GB WESM.gpkg download "
        "(default: --dest). Point this at an existing cache to skip "
        "re-downloading.",
    )
    args = parser.parse_args()

    if args.dest is not None:
        dest_dir = args.dest
    else:
        base = load_base_config()
        dest_dir = Path(base["data_root"]) / "input/wesm"

    out = stage_wesm(dest_dir, logger=logger, cache_dir=args.cache_dir)
    logger.info("Done: %s", out)


if __name__ == "__main__":
    main()
