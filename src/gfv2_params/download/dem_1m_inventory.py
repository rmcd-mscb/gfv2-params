"""Stage the real 3DEP 1 m tile inventory + WESM project attributes (issue #223).

Shared, fabric-independent input. Writes:
  {data_root}/input/3dep/dem_1m_tile_inventory.parquet
  {data_root}/input/wesm/wesm_project_attrs.parquet
Both are a SNAPSHOT of what USGS publishes on the day they are staged;
re-stage deliberately (--force). Every fabric's dprst_depth must then be
re-run, the same obligation a shared_rasters rebuild carries.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pyogrio

from gfv2_params.config import load_base_config
from gfv2_params.dprst_depth import inventory as inv
from gfv2_params.log import configure_logging

logger = configure_logging("stage_dem_1m_inventory")

WESM_VSICURL = "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg"
INVENTORY_NAME = "dem_1m_tile_inventory.parquet"
ATTRS_NAME = "wesm_project_attrs.parquet"
MIN_WESM_MATCH_FRAC = 0.90


def stage(data_root: Path, *, n_threads: int, force: bool, logger) -> tuple[Path, Path]:
    inv_path = data_root / "input" / "3dep" / INVENTORY_NAME
    attrs_path = data_root / "input" / "wesm" / ATTRS_NAME
    if inv_path.exists() and attrs_path.exists() and not force:
        logger.info("already staged (use --force to refresh): %s, %s", inv_path, attrs_path)
        return inv_path, attrs_path

    wesm = pyogrio.read_dataframe(
        WESM_VSICURL, columns=["workunit", "project", "ql", "collect_end", "sourcedem_link", "lpc_link"],
        read_geometry=False,
    )
    projects = inv.list_projects()
    attrs = inv.project_attrs(wesm, projects)
    frac = len(attrs) / len(projects) if projects else 0.0
    logger.info("  %d S3 1m project dir(s); %d matched to WESM (%.1f%%; by %s); unmatched rank last",
                len(projects), len(attrs), 100 * frac, attrs["matched_by"].value_counts().to_dict())
    if frac < MIN_WESM_MATCH_FRAC:
        raise RuntimeError(
            f"only {frac:.1%} of S3 1m project dirs matched a WESM row (floor {MIN_WESM_MATCH_FRAC:.0%}; "
            f"96.4% measured 2026-09-19) -- the WESM schema or S3 naming has changed; investigate"
        )
    df = inv.build_inventory(projects, n_threads=n_threads, logger=logger)

    # Write BOTH temp files first, then rename BOTH (#223 review round 3, also-fix
    # 2). The previous write-then-rename-per-file loop wrote and renamed inv_path
    # to its final name BEFORE `attrs.to_parquet` (the second frame's write) ever
    # ran -- a crash in that window leaves a FRESH inventory beside STALE attrs on
    # disk, undetectable afterwards (both files exist, both parse fine, nothing
    # flags that they were staged on different runs). Getting both frames onto
    # disk as temps before either rename narrows that window to back-to-back
    # `Path.rename` calls instead of spanning a full `to_parquet` write.
    pending = []
    for path, frame in ((inv_path, df), (attrs_path, attrs)):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"_staging_{path.name}")
        frame.to_parquet(tmp)
        pending.append((tmp, path, frame))
    for tmp, path, frame in pending:
        tmp.rename(path)
        logger.info("  wrote %s (%d rows)", path, len(frame))
    return inv_path, attrs_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--threads", type=int, default=32)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    base = load_base_config()
    data_root = Path(base["data_root"])
    stage(data_root, n_threads=args.threads, force=args.force, logger=logger)


if __name__ == "__main__":
    main()
