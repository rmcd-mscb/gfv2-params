"""
Script to merge VPU geopackages and fill missing parameter values using nearest neighbor interpolation.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors


def merge_vpu_geopackages(targets_dir, vpus, output_file, simplify_tolerance=100):
    """
    Merge VPU geopackages into a single file, sorted by nat_hru_id.

    Args:
        targets_dir (Path): Directory containing VPU geopackages
        vpus (list): List of VPU codes
        output_file (Path): Output merged geopackage file
        simplify_tolerance (float): Tolerance for geometry simplification in meters
    """
    print("Step 1: Merging VPU geopackages...")

    merged_gdfs = []

    for vpu in vpus:
        gpkg_file = targets_dir / f"NHM_{vpu}_draft.gpkg"

        if not gpkg_file.exists():
            print(f"Warning: {gpkg_file} not found, skipping...")
            continue

        print(f"Processing VPU: {vpu}")

        # Read the nhru layer
        gdf = gpd.read_file(gpkg_file, layer="nhru")

        # # Add nat_hru_id and vpu columns
        # gdf["nat_hru_id"] = gdf["hru_id"] + cumulative_offset
        # gdf["vpu"] = vpu

        # # Update cumulative offset
        # cumulative_offset += len(gdf["hru_id"])

        # Simplify geometries to reduce file size
        if simplify_tolerance > 0:
            gdf["geometry"] = gdf.apply(
                lambda row: row["geometry"].simplify(tolerance=simplify_tolerance, preserve_topology=True)
                if row["geometry"].area > simplify_tolerance * 10
                else row["geometry"],
                axis=1
            )

        merged_gdfs.append(gdf)
        print(f"  Added {len(gdf)} features")

    # Combine all GeoDataFrames
    merged_gdf = pd.concat(merged_gdfs, ignore_index=True)

    # Sort by nat_hru_id
    merged_gdf = merged_gdf.sort_values("nat_hru_id").reset_index(drop=True)

    # Save merged geopackage
    print(f"Saving merged geopackage with {len(merged_gdf)} features to: {output_file}")
    merged_gdf.to_file(output_file, driver="GPKG", layer="nhru")

    return merged_gdf

def find_missing_ids(param_file, expected_max=361471):
    """
    Find missing nat_hru_id values in parameter file.

    Args:
        param_file (Path): Path to parameter CSV file
        expected_max (int): Maximum expected nat_hru_id value

    Returns:
        tuple: (DataFrame with parameter data, list of missing IDs)
    """
    print("Step 2: Finding missing nat_hru_id values...")

    # Read parameter file
    param_df = pd.read_csv(param_file)

    # Get existing nat_hru_ids
    existing_ids = set(param_df["nat_hru_id"])
    expected_ids = set(range(1, expected_max + 1))
    missing_ids = sorted(expected_ids - existing_ids)

    print(f"Found {len(missing_ids)} missing nat_hru_id values out of {expected_max}")

    return param_df, missing_ids

def fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_column, k=5):
    """
    Fill missing parameter values using k-nearest neighbors based on centroid locations.

    Args:
        param_df (DataFrame): Parameter data with existing values
        missing_ids (list): List of missing nat_hru_id values
        merged_gdf (GeoDataFrame): Merged geopackage with all HRUs
        param_column (str): Name of the parameter column to fill
        k (int): Number of nearest neighbors to use

    Returns:
        DataFrame: Complete parameter DataFrame with filled values
    """
    print("Step 3: Filling missing values using nearest neighbor interpolation...")

    if not missing_ids:
        print("No missing values to fill!")
        return param_df

    # Create centroids for all geometries
    print("Calculating centroids...")
    merged_gdf["centroid"] = merged_gdf["geometry"].centroid
    merged_gdf["x"] = merged_gdf["centroid"].x
    merged_gdf["y"] = merged_gdf["centroid"].y

    # Get existing parameter values with coordinates
    existing_df = param_df.merge(
        merged_gdf[["nat_hru_id", "x", "y"]],
        on="nat_hru_id",
        how="left"
    )

    # Get missing HRUs with coordinates
    missing_df = merged_gdf[merged_gdf["nat_hru_id"].isin(missing_ids)][["nat_hru_id", "x", "y", "vpu"]]

    # Prepare coordinates for KNN
    existing_coords = existing_df[["x", "y"]].values
    missing_coords = missing_df[["x", "y"]].values
    existing_values = existing_df[param_column].values

    # Fit KNN model
    print(f"Fitting KNN model with k={k}...")
    knn = NearestNeighbors(n_neighbors=k)
    knn.fit(existing_coords)

    # Find nearest neighbors for missing points
    distances, indices = knn.kneighbors(missing_coords)

    # Calculate interpolated values (mean of k nearest neighbors)
    interpolated_values = []
    for i, neighbor_indices in enumerate(indices):
        neighbor_values = existing_values[neighbor_indices]
        # Use mean of k nearest neighbors
        interpolated_value = np.mean(neighbor_values)
        interpolated_values.append(interpolated_value)

    # Create DataFrame for missing values
    missing_filled = pd.DataFrame({
        "nat_hru_id": missing_df["nat_hru_id"].values,
        param_column: interpolated_values,
        "vpu": missing_df["vpu"].values
    })

    # Add hru_id column (assuming it matches the pattern from existing data)
    # For simplicity, we'll calculate hru_id based on vpu and position
    missing_filled["hru_id"] = missing_filled["nat_hru_id"]  # Placeholder - adjust as needed

    # Combine existing and filled data
    complete_df = pd.concat([param_df, missing_filled], ignore_index=True)
    complete_df = complete_df.sort_values("nat_hru_id").reset_index(drop=True)

    print(f"Filled {len(missing_ids)} missing values")

    return complete_df

def main():
    parser = argparse.ArgumentParser(description="Merge VPU geopackages and fill missing parameter values")
    parser.add_argument("--targets_dir", default="/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/targets",
                       help="Directory containing VPU geopackages")
    parser.add_argument("--param_file", default="/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/nhm_params/nhm_params_merged/nhm_ssflux_params.csv",
                       help="Parameter file to process")
    parser.add_argument("--output_dir", default="/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/nhm_params/nhm_params_merged",
                       help="Output directory")
    parser.add_argument("--simplify_tolerance", type=float, default=100,
                       help="Geometry simplification tolerance in meters")
    parser.add_argument("--k_neighbors", type=int, default=1,
                       help="Number of nearest neighbors for interpolation")
    parser.add_argument("--force_rebuild", action="store_true",
                       help="Force rebuild of merged geopackage even if it exists")
    args = parser.parse_args()

    # Define VPUs
    vpus = ["01", "02", "03N", "03S", "03W", "04", "05", "06", "07", "08", "09",
            "10L", "10U", "11", "12", "13", "14", "15", "16", "17", "18"]

    # Convert paths
    targets_dir = Path(args.targets_dir)
    param_file = Path(args.param_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define merged geopackage path in targets directory
    merged_gpkg = targets_dir / "gfv2_merged_simplified.gpkg"
    filled_param_file = output_dir / f"filled_{param_file.name}"

    # Step 1: Check if merged geopackage exists, create if not
    if merged_gpkg.exists() and not args.force_rebuild:
        print(f"Merged geopackage already exists: {merged_gpkg}")
        print("Loading existing merged geopackage...")
        merged_gdf = gpd.read_file(merged_gpkg, layer="nhru")
        print(f"Loaded {len(merged_gdf)} features from existing file")
    else:
        if args.force_rebuild:
            print("Force rebuild requested - creating new merged geopackage...")
        else:
            print("Merged geopackage not found - creating new one...")
        merged_gdf = merge_vpu_geopackages(targets_dir, vpus, merged_gpkg, args.simplify_tolerance)

    # Step 2: Find missing parameter values
    param_df, missing_ids = find_missing_ids(param_file)

    if missing_ids:
        # Extract parameter column name (assuming it's not hru_id, nat_hru_id, or vpu)
        param_columns = [col for col in param_df.columns if col not in ["hru_id", "nat_hru_id", "vpu"]]
        if not param_columns:
            print("Error: No parameter column found in the data")
            return

        param_column = param_columns[0]  # Use the first parameter column
        print(f"Using parameter column: {param_column}")

        # Step 3: Fill missing values
        complete_df = fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_column, args.k_neighbors)

        # Save filled parameter file
        complete_df.to_csv(filled_param_file, index=False)
        print(f"Filled parameter file saved to: {filled_param_file}")

        # Verify completeness
        final_ids = set(complete_df["nat_hru_id"])
        expected_ids = set(range(1, 361472))
        still_missing = expected_ids - final_ids

        if still_missing:
            print(f"Warning: {len(still_missing)} IDs are still missing")
        else:
            print("All missing values have been filled successfully!")

    else:
        print("No missing values found in the parameter file")

if __name__ == "__main__":
    main()
