"""Merge NHM default parameter tables to nat_hru_id."""

import argparse
import re
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def load_param_dict(dict_file, logger):
    param_df = pd.read_csv(dict_file)
    nhru_params = param_df[param_df["Dimensions"] == "nhru"]["Parameter"].tolist()
    logger.info("Found %d parameters with 'nhru' dimension", len(nhru_params))
    return nhru_params


def find_param_files(param_name, base_dir):
    all_dirs = [d for d in base_dir.glob("*") if d.is_dir() and d.name != "rOR"]
    matching_files = []
    for directory in all_dirs:
        matches = list(directory.glob(f"{param_name}*.csv"))
        matching_files.extend(matches)

    def vpu_sort_key(filepath):
        folder = filepath.parent.name
        match = re.match(r"r(\d+)([a-zA-Z]*)", folder)
        if match:
            num = int(match.group(1))
            suffix = match.group(2).lower() if match.group(2) else ""
            return (num, suffix)
        return folder

    return sorted(matching_files, key=vpu_sort_key)


def merge_param_files(param_name, files, output_dir, logger):
    if not files:
        logger.warning("No files found for parameter: %s", param_name)
        return None

    logger.info("Merging %d files for parameter: %s", len(files), param_name)

    cumulative_offset = 0
    merged_df = pd.DataFrame()

    for file in files:
        logger.debug("Processing file: %s", file)
        df = pd.read_csv(file)

        if "hru_id" not in df.columns and "$id" in df.columns:
            df = df.rename(columns={"$id": "hru_id"})

        if "hru_id" not in df.columns:
            logger.warning("'hru_id' column not found in file: %s", file)
            continue

        folder = file.parent.name
        vpu_match = re.match(r"r(\d+)([a-zA-Z]*)", folder)
        if vpu_match:
            num = vpu_match.group(1)
            suffix = vpu_match.group(2).lower() if vpu_match.group(2) else ""
            vpu = f"{num}{suffix}"
        else:
            vpu = "unknown"

        df["nat_hru_id"] = df["hru_id"] + cumulative_offset
        df["vpu"] = vpu
        cumulative_offset += len(df["hru_id"])

        merged_df = pd.concat([merged_df, df], ignore_index=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    merged_file = output_dir / f"{param_name}_merged.csv"
    merged_df.to_csv(merged_file, index=False)
    logger.info("Merged file saved to: %s", merged_file)
    return merged_file


def main():
    parser = argparse.ArgumentParser(description="Merge default parameter files by nat_hru_id.")
    parser.add_argument("--dict", required=True, help="Path to parameter dictionary CSV file")
    parser.add_argument("--base_dir", default=None, help="Base directory containing parameter files")
    parser.add_argument("--output_dir", default=None, help="Output directory for merged files")
    args = parser.parse_args()

    logger = configure_logging("merge_default_params")

    # Load base config after argparse so --help works without config file
    base = load_base_config()
    data_root = base["data_root"]

    if args.base_dir is None:
        args.base_dir = f"{data_root}/nhm_params/default"
    if args.output_dir is None:
        args.output_dir = f"{data_root}/nhm_params/merged"

    dict_file = Path(args.dict)
    base_dir = Path(args.base_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    nhru_params = load_param_dict(dict_file, logger)

    for param_name in nhru_params:
        logger.info("Processing parameter: %s", param_name)
        param_files = find_param_files(param_name, base_dir)

        if param_files:
            logger.info("Found %d files for %s", len(param_files), param_name)
            merge_param_files(param_name, param_files, output_dir, logger)
        else:
            logger.warning("No files found for parameter: %s", param_name)


if __name__ == "__main__":
    main()
