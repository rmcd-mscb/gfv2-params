"""Merge per-batch parameter CSVs into a single file, sorted by id_feature.

Validates completeness: raises on duplicate IDs, warns on gaps.
"""

import argparse
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def process_files(config, logger):
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    merged_file = config["merged_file"]
    fabric = config["fabric"]
    expected_max = config.get("expected_max_hru_id")

    input_dir = Path(config["output_dir"]) / source_type
    final_output_dir = Path(config["output_dir"]) / "merged"
    final_output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    file_pattern = f"base_nhm_{source_type}_{fabric}_batch_*_param.csv"
    files = sorted(input_dir.glob(file_pattern))

    if not files:
        raise FileNotFoundError(f"No batch files found matching: {input_dir / file_pattern}")

    logger.info("Found %d batch files for %s", len(files), source_type)

    dfs = []
    for file in files:
        logger.debug("Reading: %s", file)
        df = pd.read_csv(file)
        if id_feature not in df.columns:
            raise ValueError(f"'{id_feature}' column not found in file: {file}")
        dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df = merged_df.sort_values(id_feature).reset_index(drop=True)

    # Validate: check for duplicates
    dupes = merged_df[merged_df[id_feature].duplicated(keep=False)]
    if len(dupes) > 0:
        dupe_ids = sorted(dupes[id_feature].unique())
        raise ValueError(
            f"Duplicate {id_feature} values found ({len(dupe_ids)} IDs). "
            f"First 10: {dupe_ids[:10]}. This indicates overlapping batches."
        )

    # Validate: check for gaps (optional, based on expected_max_hru_id)
    if expected_max is not None:
        existing_ids = set(merged_df[id_feature])
        expected_ids = set(range(1, expected_max + 1))
        gaps = sorted(expected_ids - existing_ids)
        if gaps:
            logger.warning(
                "%d missing %s values (expected 1-%d, got %d). First 10: %s",
                len(gaps), id_feature, expected_max, len(existing_ids), gaps[:10],
            )

    output_path = final_output_dir / merged_file
    merged_df.to_csv(output_path, index=False)
    logger.info("Merged %d rows -> %s", len(merged_df), output_path)


def main():
    parser = argparse.ArgumentParser(description="Merge per-batch parameter CSVs.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    args = parser.parse_args()

    logger = configure_logging("merge_params")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )
    process_files(config, logger)


if __name__ == "__main__":
    main()
