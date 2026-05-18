"""Migrate a data_root from the legacy `work/` layout to the new `shared/` layout.

Companion to the layout reorganisation that introduces an explicit shared-vs-fabric
boundary in the on-disk store. Renames directories in place via `os.rename` (atomic
on the same filesystem) and regenerates GDAL VRTs at the end because they encode
absolute paths to their per-VPU sources.

Old -> new mapping::

    {data_root}/work/                              -> {data_root}/shared/
    work/nhd_merged/{01..18,03N,03S,03W,10L,10U}/  -> shared/per_vpu/{vpu}/
    work/nhd_merged/copernicus_fill/               -> shared/conus/borders/
    work/nhd_merged/*.vrt                          -> shared/conus/vrt/*.vrt    (regenerated)
    work/derived_rasters/                          -> shared/conus/derived/
    work/weights/                                  -> shared/conus/weights/
    work/nhd_extracted/                            -> shared/source/

Usage::

    pixi run python scripts/migrate_to_shared_layout.py --data-root /path --dry-run
    pixi run python scripts/migrate_to_shared_layout.py --data-root /path --execute

`--dry-run` (default) prints every planned move without touching anything. `--execute`
actually performs the renames. After `--execute`, the script re-runs the build_vrt
step of the shared-raster orchestrator to rewrite VRT internal paths against the new
locations.

Idempotent: re-running after a successful migration is a no-op (every source
directory is missing because it already moved).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from gfv2_params.log import configure_logging


def _per_vpu_subdirs(nhd_merged: Path) -> list[Path]:
    """Find every per-VPU subdirectory under work/nhd_merged/ (excludes copernicus_fill)."""
    if not nhd_merged.exists():
        return []
    return sorted(
        p for p in nhd_merged.iterdir()
        if p.is_dir() and p.name != "copernicus_fill"
    )


def _vrt_files(nhd_merged: Path) -> list[Path]:
    """Find every CONUS VRT (siblings of per-VPU subdirs under work/nhd_merged/)."""
    if not nhd_merged.exists():
        return []
    return sorted(nhd_merged.glob("*.vrt"))


def _plan_moves(data_root: Path) -> list[tuple[Path, Path, str]]:
    """Compute the (src, dst, kind) tuples for every planned rename."""
    work = data_root / "work"
    shared = data_root / "shared"
    nhd_merged = work / "nhd_merged"
    moves: list[tuple[Path, Path, str]] = []

    # Per-VPU subdirs (work/nhd_merged/{vpu}/ -> shared/per_vpu/{vpu}/)
    for vpu_dir in _per_vpu_subdirs(nhd_merged):
        moves.append((vpu_dir, shared / "per_vpu" / vpu_dir.name, "per-VPU directory"))

    # copernicus_fill -> shared/conus/borders/
    borders_src = nhd_merged / "copernicus_fill"
    if borders_src.exists():
        moves.append((borders_src, shared / "conus" / "borders", "border DEM fill"))

    # VRTs -> shared/conus/vrt/  (will be regenerated, but we move existing ones
    # so they exist somewhere downstream code can find them during the gap)
    for vrt in _vrt_files(nhd_merged):
        moves.append((vrt, shared / "conus" / "vrt" / vrt.name, "CONUS VRT (will regenerate)"))

    # derived_rasters -> shared/conus/derived/
    derived_src = work / "derived_rasters"
    if derived_src.exists():
        moves.append((derived_src, shared / "conus" / "derived", "derived rasters dir"))

    # weights -> shared/conus/weights/
    weights_src = work / "weights"
    if weights_src.exists():
        moves.append((weights_src, shared / "conus" / "weights", "weights dir"))

    # nhd_extracted -> shared/source/
    source_src = work / "nhd_extracted"
    if source_src.exists():
        moves.append((source_src, shared / "source", "raw NHDPlus extract"))

    return moves


def _ensure_dst_parents(moves: list[tuple[Path, Path, str]], logger) -> None:
    """Create destination parent dirs (no-op if they exist)."""
    parents = {dst.parent for _, dst, _ in moves}
    for parent in sorted(parents):
        if not parent.exists():
            logger.info("  mkdir -p %s", parent)
            parent.mkdir(parents=True, exist_ok=True)


def _execute_moves(moves: list[tuple[Path, Path, str]], logger) -> int:
    """Run the moves; return count actually executed (skips conflicts)."""
    n_moved = 0
    for src, dst, kind in moves:
        if not src.exists():
            logger.info("  SKIP (already moved or never existed): %s", src)
            continue
        if dst.exists():
            logger.warning(
                "  CONFLICT: destination already exists, refusing to overwrite: %s "
                "(src: %s). Resolve manually.", dst, src,
            )
            continue
        logger.info("  mv [%s] %s -> %s", kind, src, dst)
        os.rename(src, dst)
        n_moved += 1
    return n_moved


def _cleanup_empty_work(data_root: Path, logger) -> None:
    """Remove now-empty work/nhd_merged/ and work/ if they have no files left."""
    work = data_root / "work"
    nhd_merged = work / "nhd_merged"
    for d in (nhd_merged, work):
        if d.exists() and d.is_dir():
            try:
                d.rmdir()
                logger.info("  rmdir (empty): %s", d)
            except OSError:
                logger.warning("  Not removing %s — directory still has contents", d)


def _regenerate_vrts(logger) -> None:
    """Re-run build_vrt step so the moved VRTs encode the new per-VPU paths."""
    repo_root = Path(__file__).resolve().parent.parent
    orchestrator = repo_root / "scripts" / "build_shared_rasters.py"
    config = repo_root / "configs" / "shared_rasters" / "shared_rasters.yml"
    cmd = [
        sys.executable, str(orchestrator),
        "--config", str(config),
        "--step", "build_vrt",
        "--force",
    ]
    logger.info("Regenerating VRTs (their absolute internal paths need updating)...")
    logger.info("  %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("VRT regeneration FAILED. stdout:\n%s\nstderr:\n%s",
                     result.stdout, result.stderr)
        raise RuntimeError("build_vrt step failed during VRT regeneration")
    logger.info("VRT regeneration complete.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Migrate a data_root from the legacy work/ layout to the new shared/ layout. "
            "Atomic per-directory renames + post-move VRT regeneration."
        ),
    )
    parser.add_argument("--data-root", required=True, type=Path,
                        help="Path to the data_root (the directory containing input/, work/, etc.)")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true",
                      help="Preview every planned move without touching the filesystem")
    mode.add_argument("--execute", action="store_true",
                      help="Actually perform the renames and regenerate VRTs")
    args = parser.parse_args()

    logger = configure_logging("migrate_to_shared_layout")

    data_root = args.data_root.resolve()
    if not data_root.exists():
        parser.error(f"data_root does not exist: {data_root}")
    if not (data_root / "work").exists() and not (data_root / "shared").exists():
        logger.warning("Neither work/ nor shared/ found under %s — nothing to do.", data_root)
        return

    logger.info("=== migrate_to_shared_layout ===")
    logger.info("data_root: %s", data_root)
    logger.info("mode     : %s", "EXECUTE" if args.execute else "DRY RUN")

    moves = _plan_moves(data_root)
    if not moves:
        logger.info("No moves to perform. Migration already complete (or work/ is empty).")
        return

    logger.info("Planned moves (%d):", len(moves))
    for src, dst, kind in moves:
        marker = "would mv" if args.dry_run else "    mv  "
        logger.info("  %s [%s] %s -> %s", marker, kind, src, dst)

    if args.dry_run:
        logger.info("DRY RUN complete — no filesystem changes made. Re-run with --execute to apply.")
        return

    _ensure_dst_parents(moves, logger)
    n_moved = _execute_moves(moves, logger)
    _cleanup_empty_work(data_root, logger)

    logger.info("Moves complete: %d of %d performed.", n_moved, len(moves))
    if n_moved > 0:
        _regenerate_vrts(logger)
    logger.info("=== migrate_to_shared_layout complete ===")


if __name__ == "__main__":
    main()
