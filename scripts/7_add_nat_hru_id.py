from pathlib import Path

import pandas as pd
import yaml


def load_config(config_path):
    """
    Load the YAML configuration file.

    Args:
        config_path (str): Path to the YAML config file.

    Returns:
        dict: Parsed configuration as a dictionary.
    """
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def process_files(config):
    """
    Process CSV files in the input directory, sorted by VPU, add nat_hru_id and vpu columns,
    and merge them into a single file.

    Args:
        config (dict): Configuration dictionary containing input_dir, source_type, and merged_file.

    Returns:
        None: Updates the files in place with the new columns and saves the merged file.
    """
    input_dir = Path(config["output_dir"]) / config["source_type"]
    source_type = config["source_type"]
    merged_file = Path(config["merged_file"])
    final_output_dir = Path(config["output_dir"]) / "nhm_params_merged"
    final_output_dir.mkdir(parents=True, exist_ok=True)  # Ensure output directory exists

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    # Glob all files matching the pattern dynamically based on the source_type
    file_pattern = f"base_nhm_{source_type}_*_param.csv"
    files = sorted(input_dir.glob(file_pattern), key=lambda f: f.stem.split("_")[3])

    # cumulative_offset = 0  # Tracks the cumulative hru_id offset across VPUs
    merged_df = pd.DataFrame()  # DataFrame to hold the merged result

    for file in files:
        print(f"Processing file: {file}")
        df = pd.read_csv(file)

        if "nat_hru_id" not in df.columns:
            raise ValueError(f"'nat_hru_id' column not found in file: {file}")

        # Sort by 'nat_hru_id'
        df = df.sort_values("nat_hru_id")

        # Extract VPU from the filename
        vpu = file.stem.split("_")[3]

        # # Add nat_hru_id and vpu columns
        # df["nat_hru_id"] = df["hru_id"] + cumulative_offset
        df["vpu"] = vpu

        # # Update cumulative offset
        # cumulative_offset += len(df["hru_id"])

        # # Save the updated file
        # df.to_csv(file, index=False)
        # print(f"Updated file saved: {file}")

        print(f"vpu: {vpu}, num_hru: {len(df)}")

        # Append to the merged DataFrame
        merged_df = pd.concat([merged_df, df], ignore_index=True)

        # Sort by "nat_hru_id"
        merged_df = merged_df.sort_values("nat_hru_id")

    # Save the merged DataFrame to the specified file
    merged_df.to_csv(final_output_dir / merged_file, index=False)
    print(f"Merged file saved to: {merged_file}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Add nat_hru_id and vpu columns to source_type files and merge them.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    # Load the configuration
    config = load_config(args.config)

    # Process files based on the configuration
    process_files(config)
