"""A/B the segment-derived on-stream set against NHD flowline topology.

Reads the `segment_wbody` step's output for a fabric and compares it to the two NHD
COMID tables, so the classifier switch can be quantified without wiring the NHD tables
back into a production profile. This replaces the throwaway measurement script that
produced the design spec's CONUS numbers (48,529 on-stream COMIDs; 16,939 segment-only
vs WBAREACOMI, 6,265 vs flow-through).

  pixi run --as-is python scripts/diagnose/ab_segment_vs_nhd_onstream.py --fabric gfv2

Paths come from the fabric profile, never hardcoded. The NHD tables are read from
`{data_root}/input/nhd/` because the profile keys are commented out by design.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import load_connected_comids
from gfv2_params.log import configure_logging
from gfv2_params.segment_wbody import load_segment_comids


def compare_onstream_sets(
    segment: set[int], wbareacomi: set[int], flowthrough: set[int]
) -> dict[str, int]:
    """Counts in each direction between the segment set and the NHD union."""
    nhd = wbareacomi | flowthrough
    return {
        "n_segment": len(segment),
        "n_wbareacomi": len(wbareacomi),
        "n_flowthrough": len(flowthrough),
        "n_nhd_union": len(nhd),
        "n_shared": len(segment & nhd),
        "n_segment_only": len(segment - nhd),
        "n_nhd_only": len(nhd - segment),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/depstor/depstor_rasters.yml")
    parser.add_argument("--base_config", default=None)
    parser.add_argument("--fabric", default=None)
    args = parser.parse_args()

    logger = configure_logging("ab_segment_vs_nhd_onstream")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    # load_config() already resolves {data_root}/{fabric} placeholders in every
    # top-level scalar value (including output_dir, a step-config scalar) --
    # see scripts/build_depstor_rasters.py's `_build_context`, which reads
    # config["output_dir"] the same way with no further substitution.
    data_root = Path(config["data_root"])
    output_dir = Path(require_config_key(config, "output_dir", "ab_segment_vs_nhd_onstream"))
    segment_table = output_dir / "segment_waterbody_comids.parquet"
    if not segment_table.exists():
        raise FileNotFoundError(
            f"{segment_table} not found — run the `segment_wbody` depstor step for "
            f"fabric '{config['fabric']}' first."
        )

    segment = load_segment_comids(segment_table)
    nhd_dir = data_root / "input" / "nhd"
    wbareacomi = _maybe_load(nhd_dir / "connected_waterbody_comids.parquet", logger)
    flowthrough = _maybe_load(nhd_dir / "flowthrough_waterbody_comids.parquet", logger)

    logger.info("=== segment vs NHD on-stream, fabric=%s ===", config["fabric"])
    for key, value in compare_onstream_sets(segment, wbareacomi, flowthrough).items():
        logger.info("  %-16s %d", key, value)


def _maybe_load(path: Path, logger) -> set[int]:
    if not path.exists():
        logger.warning("  %s not staged — treating as empty", path)
        return set()
    return load_connected_comids(path)


if __name__ == "__main__":
    main()
