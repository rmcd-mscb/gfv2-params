"""
Script to read the parameter dictionary, filter for parameters with 'nhru' dimension,
and merge the corresponding parameter files from the default directory.
"""

import argparse
import re
from pathlib import Path

import pandas as pd


def load_param_dict(dict_file):
    """
    Load and filter parameter dictionary for 'nhru' dimensions.

    Args:
        dict_file (str): Path to the parameter dictionary CSV file.

    Returns:
        list: List of parameter names with 'nhru' dimension.
    """
    # Read the parameter dictionary
    param_df = pd.read_csv(dict_file)

    # Filter for parameters with 'nhru' dimension
    nhru_params = param_df[param_df['Dimensions'] == 'nhru']['Parameter'].tolist()
    print(f"Found {len(nhru_params)} parameters with 'nhru' dimension")

    return nhru_params

def find_param_files(param_name, base_dir):
    """
    Find all files in base_dir that start with the parameter name.

    Args:
        param_name (str): Parameter name to search for.
        base_dir (Path): Base directory to search in.

    Returns:
        list: List of file paths matching the parameter name pattern.
    """
    # Get all subdirectories (excluding rOR)
    all_dirs = [d for d in base_dir.glob("*") if d.is_dir() and d.name != "rOR"]

    # Find files matching the pattern in all directories
    matching_files = []
    for directory in all_dirs:
        pattern = f"{param_name}*.csv"
        matches = list(directory.glob(pattern))
        matching_files.extend(matches)

    # Custom sorting function for VPU folders
    def vpu_sort_key(filepath):
        # Extract folder name (e.g., "r01", "r03n")
        folder = filepath.parent.name

        # Extract numeric part and any suffix
        match = re.match(r'r(\d+)([a-zA-Z]*)', folder)
        if match:
            num = int(match.group(1))
            suffix = match.group(2).lower() if match.group(2) else ""
            return (num, suffix)
        return folder  # Fallback

    # Sort files by their VPU folder names
    return sorted(matching_files, key=vpu_sort_key)

def merge_param_files(param_name, files, output_dir):
    """
    Merge parameter files, adding nat_hru_id and vpu columns.

    Args:
        param_name (str): Parameter name.
        files (list): List of file paths to merge.
        output_dir (Path): Directory to save the merged file.

    Returns:
        Path: Path to the merged file.
    """
    if not files:
        print(f"No files found for parameter: {param_name}")
        return None

    print(f"Merging {len(files)} files for parameter: {param_name}")

    cumulative_offset = 0  # Tracks the cumulative hru_id offset across VPUs
    merged_df = pd.DataFrame()  # DataFrame to hold the merged result

    for file in files:
        print(f"  Processing file: {file}")
        df = pd.read_csv(file)

        if "hru_id" not in df.columns and "$id" in df.columns:
            df = df.rename(columns={"$id": "hru_id"})

        if "hru_id" not in df.columns:
            print(f"  Warning: 'hru_id' column not found in file: {file}")
            continue

        # Extract VPU from the parent directory name (e.g., "r01", "r03n")
        folder = file.parent.name
        vpu_match = re.match(r'r(\d+)([a-zA-Z]*)', folder)
        if vpu_match:
            num = vpu_match.group(1)
            suffix = vpu_match.group(2).lower() if vpu_match.group(2) else ""
            vpu = f"{num}{suffix}"
        else:
            vpu = "unknown"

        # Add nat_hru_id and vpu columns
        df["nat_hru_id"] = df["hru_id"] + cumulative_offset
        df["vpu"] = vpu

        # Update cumulative offset
        cumulative_offset += len(df["hru_id"])

        # Append to the merged DataFrame
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save the merged DataFrame
    merged_file = output_dir / f"{param_name}_merged.csv"
    merged_df.to_csv(merged_file, index=False)
    print(f"Merged file saved to: {merged_file}")

    return merged_file

def main():
    parser = argparse.ArgumentParser(description="Merge parameter files based on parameter dictionary.")
    parser.add_argument("--dict", required=True, help="Path to parameter dictionary CSV file")
    parser.add_argument("--base_dir", default="/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/nhm_params/default",
                       help="Base directory containing parameter files")
    parser.add_argument("--output_dir", default="/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/nhm_params/merged",
                       help="Output directory for merged files")
    args = parser.parse_args()

    # Convert paths to Path objects
    dict_file = Path(args.dict)
    base_dir = Path(args.base_dir)
    output_dir = Path(args.output_dir)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get nhru parameters
    nhru_params = load_param_dict(dict_file)

    # Process each parameter
    for param_name in nhru_params:
        print(f"\nProcessing parameter: {param_name}")
        param_files = find_param_files(param_name, base_dir)

        if param_files:
            print(f"Found {len(param_files)} files for {param_name}")
            for f in param_files:
                print(f"  {f}")

            merge_param_files(param_name, param_files, output_dir)
        else:
            print(f"No files found for parameter: {param_name}")

if __name__ == "__main__":
    main()
