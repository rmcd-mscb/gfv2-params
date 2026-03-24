"""Find missing nat_hru_id values in a parameter CSV file."""

import argparse
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def find_missing_nat_hru_ids(csv_file, expected_max, output_file, logger):
    df = pd.read_csv(csv_file)
    nat_hru_ids = df["nat_hru_id"].sort_values()

    expected_range = set(range(1, expected_max + 1))
    actual_values = set(nat_hru_ids)
    missing_values = sorted(expected_range - actual_values)

    output_lines = []
    output_lines.append(f"Analysis of missing nat_hru_id values in: {csv_file}")
    output_lines.append("=" * 60)
    output_lines.append(f"Total expected values: {len(expected_range)}")
    output_lines.append(f"Total actual values: {len(actual_values)}")
    output_lines.append(f"Missing values count: {len(missing_values)}")
    output_lines.append(f"Minimum nat_hru_id: {nat_hru_ids.min()}")
    output_lines.append(f"Maximum nat_hru_id: {nat_hru_ids.max()}")
    output_lines.append("")

    if missing_values:
        output_lines.append(f"First 20 missing values: {missing_values[:20]}")
        output_lines.append(f"Last 20 missing values: {missing_values[-20:]}")
        output_lines.append("")

        gaps = []
        gap_start = missing_values[0]
        gap_end = missing_values[0]

        for i in range(1, len(missing_values)):
            if missing_values[i] == missing_values[i - 1] + 1:
                gap_end = missing_values[i]
            else:
                if gap_end > gap_start:
                    gaps.append((gap_start, gap_end))
                else:
                    gaps.append((gap_start,))
                gap_start = missing_values[i]
                gap_end = missing_values[i]

        if gap_end > gap_start:
            gaps.append((gap_start, gap_end))
        else:
            gaps.append((gap_start,))

        output_lines.append(f"Number of gaps: {len(gaps)}")
        if gaps:
            output_lines.append("First 10 gaps:")
            for i, gap in enumerate(gaps[:10]):
                if len(gap) == 2:
                    output_lines.append(f"  Gap {i+1}: {gap[0]} to {gap[1]} (size: {gap[1] - gap[0] + 1})")
                else:
                    output_lines.append(f"  Gap {i+1}: {gap[0]} (single missing value)")

        output_lines.append("")
        output_lines.append("ALL MISSING nat_hru_id VALUES:")
        output_lines.append("-" * 40)

        for i in range(0, len(missing_values), 10):
            chunk = missing_values[i : i + 10]
            output_lines.append(", ".join(map(str, chunk)))
    else:
        output_lines.append("No missing values found!")

    for line in output_lines:
        logger.info(line)

    if output_file is None:
        csv_path = Path(csv_file)
        output_file = csv_path.parent / f"missing_hru_ids_{csv_path.stem}.txt"

    with open(output_file, "w") as f:
        for line in output_lines:
            f.write(line + "\n")

    logger.info("Results saved to: %s", output_file)


def main():
    parser = argparse.ArgumentParser(description="Find missing nat_hru_id values in CSV file")
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("--output", "-o", help="Path to output text file (optional)")
    args = parser.parse_args()

    logger = configure_logging("find_missing_hru_ids")

    base = load_base_config()
    expected_max = base["expected_max_hru_id"]

    find_missing_nat_hru_ids(args.csv_file, expected_max, args.output, logger)


if __name__ == "__main__":
    main()
