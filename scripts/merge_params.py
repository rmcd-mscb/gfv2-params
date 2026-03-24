"""Merge per-VPU parameter CSVs into a single file, sorted by nat_hru_id."""

import argparse
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def process_files(config, logger):
    input_dir = Path(config["output_dir"]) / config["source_type"]
    source_type = config["source_type"]
    merged_file = Path(config["merged_file"])
    final_output_dir = Path(config["output_dir"]) / "nhm_params_merged"
    final_output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    file_pattern = f"base_nhm_{source_type}_*_param.csv"
    files = sorted(input_dir.glob(file_pattern), key=lambda f: f.stem.split("_")[3])

    merged_df = pd.DataFrame()

    for file in files:
        logger.info("Processing file: %s", file)
        df = pd.read_csv(file)

        if "nat_hru_id" not in df.columns:
            raise ValueError(f"'nat_hru_id' column not found in file: {file}")

        df = df.sort_values("nat_hru_id")
        vpu = file.stem.split("_")[3]
        df["vpu"] = vpu

        logger.info("vpu: %s, num_hru: %d", vpu, len(df))
        merged_df = pd.concat([merged_df, df], ignore_index=True)
        merged_df = merged_df.sort_values("nat_hru_id")

    merged_df.to_csv(final_output_dir / merged_file, index=False)
    logger.info("Merged file saved to: %s", final_output_dir / merged_file)


def main():
    parser = argparse.ArgumentParser(description="Merge per-VPU parameter CSVs.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    logger = configure_logging("merge_params")
    config = load_config(Path(args.config))
    process_files(config, logger)


if __name__ == "__main__":
    main()
