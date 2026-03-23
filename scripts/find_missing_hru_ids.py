from pathlib import Path

import pandas as pd


def find_missing_nat_hru_ids(csv_file, output_file=None):
    """
    Find missing nat_hru_id values in a CSV file and save results to a text file.

    Args:
        csv_file (str): Path to the CSV file
        output_file (str): Path to output text file (optional)
    """
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Get the nat_hru_id column
    nat_hru_ids = df['nat_hru_id'].sort_values()

    # Create the expected range
    expected_min = 1
    expected_max = 361471
    expected_range = set(range(expected_min, expected_max + 1))

    # Get actual values
    actual_values = set(nat_hru_ids)

    # Find missing values
    missing_values = sorted(expected_range - actual_values)

    # Prepare output
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

        # Check for gaps (consecutive missing values)
        gaps = []
        if missing_values:
            gap_start = missing_values[0]
            gap_end = missing_values[0]

            for i in range(1, len(missing_values)):
                if missing_values[i] == missing_values[i-1] + 1:
                    gap_end = missing_values[i]
                else:
                    if gap_end > gap_start:
                        gaps.append((gap_start, gap_end))
                    else:
                        gaps.append((gap_start,))
                    gap_start = missing_values[i]
                    gap_end = missing_values[i]

            # Add the last gap
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

        # Write all missing values in chunks of 10 per line for readability
        for i in range(0, len(missing_values), 10):
            chunk = missing_values[i:i+10]
            output_lines.append(", ".join(map(str, chunk)))

    else:
        output_lines.append("No missing values found!")

    # Print to console
    for line in output_lines:
        print(line)

    # Save to file
    if output_file is None:
        # Create output filename based on input filename
        csv_path = Path(csv_file)
        output_file = csv_path.parent / f"missing_hru_ids_{csv_path.stem}.txt"

    with open(output_file, 'w') as f:
        for line in output_lines:
            f.write(line + '\n')

    print(f"\nResults saved to: {output_file}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Find missing nat_hru_id values in CSV file")
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("--output", "-o", help="Path to output text file (optional)")
    args = parser.parse_args()

    find_missing_nat_hru_ids(args.csv_file, args.output)
