"""Export one file per PRMS parameter from the gap-filled `merged/` products.

Reads `{data_root}/{fabric}/params/merged/<entry>.csv` (the FILLED product) and writes
`{data_root}/{fabric}/params/prms/<prms_name>.csv`, named for the parameter in
docs/HI_paramDB_DataDictionary.csv. See `gfv2_params.prms_export` for the rules.

Run AFTER the fill stage:

    FABRIC=gfv2r2 sbatch slurm_batch/export_prms_params.batch

or by hand (about 30 columns x one fabric's HRUs -- small, but still not for the login node):

    srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=2 --mem=16G \\
      pixi run --as-is python scripts/export_prms_params.py --fabric <fabric>

`--dry-run` prints the plan (which file comes from which entry/columns) and touches
nothing; it needs no data on disk.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.params_index import SNAREA_LIBRARY_CONFIG
from gfv2_params.prms_export import PRMS_SUBDIR, export_fabric, plan_exports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan; write nothing.")
    args = parser.parse_args(argv)

    logger = configure_logging("export_prms_params")

    if args.dry_run:
        for item in plan_exports():
            logger.info(
                "%-22s <- %s : %s [%s %s]",
                item.prms_name, item.merged_file, ",".join(item.source_cols), item.dimension, item.dtype,
            )
        return 0

    base = load_base_config(Path(args.base_config) if args.base_config else None, fabric=args.fabric)
    params_dir = Path(base["data_root"]) / base["fabric"] / "params"
    merged_dir = params_dir / "merged"
    id_feature = require_config_key(base, "id_feature", "export_prms_params")
    expected = require_config_key(base, "expected_max_hru_id", "export_prms_params")

    library_file = yaml.safe_load(SNAREA_LIBRARY_CONFIG.read_text())["library_file"]
    try:
        written = export_fabric(
            merged_dir,
            params_dir / PRMS_SUBDIR,
            id_feature,
            expected,
            library_path=merged_dir / library_file,
            logger=logger,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error("export failed, nothing written: %s", exc)
        return 1
    logger.info("Exported %d PRMS parameter files to %s", len(written), params_dir / PRMS_SUBDIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
