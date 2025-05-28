import argparse
from pathlib import Path

import rioxarray as rxr
import yaml
from rioxarray.merge import merge_arrays


def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description="Merge NHD rasters by VPU and dataset type.")
    parser.add_argument("--config", required=True, help="Path to config_merge_rpu_by_vpu.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    args = parser.parse_args()

    base_path = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/")
    config = load_config(args.config)
    vpu_config = config.get(args.vpu)
    if vpu_config is None:
        raise ValueError(f"VPU {args.vpu} not found in config.")

    for dataset_name, values in vpu_config.items():
        print(f"🔹 Dataset: {dataset_name}")
        rpus = values.get("rpus", [])
        output_file = values.get("output")
        output = base_path / output_file.lstrip("/")

        if output.exists():
            print(f"✅ Output already exists, skipping: {output}")
            continue

        datasets = []
        for d in rpus:
            d = base_path / d.lstrip("/")
            print(f"Reading raster from: {d}")
            if not d.exists():
                raise FileNotFoundError(f"Input raster folder not found: {d}")
            if not (d / "hdr.adf").exists():
                raise ValueError(f"Folder {d} does not appear to be a valid ESRI Grid raster")
            ds = rxr.open_rasterio(str(d), masked=True).squeeze()
            datasets.append(ds)

        print(f"Merging {len(datasets)} datasets")
        if len(datasets) == 1:
            merged = datasets[0]
        else:
            # Use union of all extents for DEMs and Hydrodem
            if dataset_name in ("NEDSnapshot", "Hydrodem"):
                merged = merge_arrays(datasets, method="min")
            else:
                merged = merge_arrays(datasets, method="first")

        # Optional: check all CRSs are the same
        crs_set = {ds.rio.crs.to_string() for ds in datasets}
        if len(crs_set) > 1:
            raise ValueError(f"Inconsistent CRS among inputs: {crs_set}")

        # Set nodata and cast based on dataset type
        match dataset_name:
            case "NEDSnapshot":
                nodata_val = -9999
                merged = merged.astype("float32")
                # Convert from centimeters to meters, preserve nodata
                merged = merged.where(~merged.isnull(), nodata_val)
                merged = merged / 100.0
                merged.rio.write_nodata(nodata_val, inplace=True)
                print("Converted NEDSnapshot from centimeters to meters.")

            case "Hydrodem":
                nodata_val = -9999
                merged = merged.astype("float32")
                merged = merged.where(~merged.isnull(), nodata_val)
                merged = merged / 100.0
                print("Converted Hydrodem from centimeters to meters.")

            case "FdrFac_Fdr":
                nodata_val = 255  # for D8-coded FDR
                merged = merged.fillna(nodata_val).astype("uint8")

            case "FdrFac_Fac":
                nodata_val = -9999
                merged = merged.fillna(nodata_val).astype("int32")

            case _:
                raise ValueError(f"Unknown dataset_name: {dataset_name}")

        print(f"Writing raster: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)

        merged.rio.write_crs(datasets[0].rio.crs, inplace=True)
        merged.rio.write_nodata(nodata_val, inplace=True)

        match dataset_name:
            case "NEDSnapshot" | "Hydrodem":
                merged.rio.to_raster(
                    output,
                    compress="lzw",
                    predictor=2,
                    tiled=True,
                    blockxsize=512,
                    blockysize=512
                )
            case "FdrFac_Fdr" | "FdrFac_Fac":
                merged.rio.to_raster(
                    output,
                    compress="lzw",
                    tiled=True,
                    blockxsize=512,
                    blockysize=512
                )

        print(f"✅ Wrote raster: {output}")

if __name__ == "__main__":
    main()
