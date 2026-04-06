"""Merge Regional Processing Unit (RPU) rasters by VPU and dataset type."""

import argparse
from pathlib import Path

import rioxarray as rxr
import yaml
from rioxarray.merge import merge_arrays

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Merge NHD rasters by VPU and dataset type.")
    parser.add_argument("--config", required=True, help="Path to merge_rpu_by_vpu.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing output files")
    args = parser.parse_args()

    logger = configure_logging("merge_rpu_by_vpu")

    # Load base config for data_root (this config has a unique nested structure,
    # so we load it separately via yaml rather than through load_config)
    base = load_base_config()
    base_path = Path(base["data_root"])

    # Load the RPU merge config (VPU-keyed nested structure)
    with open(args.config, "r") as f:
        rpu_config = yaml.safe_load(f)

    vpu_config = rpu_config.get(args.vpu)
    if vpu_config is None:
        raise ValueError(f"VPU {args.vpu} not found in config.")

    for dataset_name, values in vpu_config.items():
        logger.info("Dataset: %s", dataset_name)
        rpus = values.get("rpus", [])
        output_file = values.get("output")
        output = base_path / output_file.lstrip("/")

        if output.exists() and not args.force:
            logger.info("Output already exists, skipping: %s", output)
            continue

        datasets = []
        for d in rpus:
            d = base_path / d.lstrip("/")
            logger.info("Reading raster from: %s", d)
            if not d.exists():
                raise FileNotFoundError(f"Input raster folder not found: {d}")
            if not (d / "hdr.adf").exists():
                raise ValueError(f"Folder {d} does not appear to be a valid ESRI Grid raster")
            ds = rxr.open_rasterio(str(d), masked=True).squeeze()
            datasets.append(ds)

        logger.info("Merging %d datasets", len(datasets))
        if len(datasets) == 1:
            merged = datasets[0]
        else:
            if dataset_name in ("NEDSnapshot", "Hydrodem"):
                merged = merge_arrays(datasets, method="min")
            else:
                merged = merge_arrays(datasets, method="first")

        crs_set = {ds.rio.crs.to_string() for ds in datasets}
        if len(crs_set) > 1:
            raise ValueError(f"Inconsistent CRS among inputs: {crs_set}")

        match dataset_name:
            case "NEDSnapshot":
                nodata_val = -9999
                merged = merged.astype("float32")
                merged = merged.where(~merged.isnull(), nodata_val)
                merged = merged / 100.0
                merged.rio.write_nodata(nodata_val, inplace=True)
                logger.info("Converted NEDSnapshot from centimeters to meters.")

            case "Hydrodem":
                nodata_val = -9999
                merged = merged.astype("float32")
                merged = merged.where(~merged.isnull(), nodata_val)
                merged = merged / 100.0
                logger.info("Converted Hydrodem from centimeters to meters.")

            case "FdrFac_Fdr":
                nodata_val = 255
                merged = merged.fillna(nodata_val).astype("uint8")

            case "FdrFac_Fac":
                nodata_val = -9999
                merged = merged.fillna(nodata_val).astype("int32")

            case _:
                raise ValueError(f"Unknown dataset_name: {dataset_name}")

        logger.info("Writing raster: %s", output)
        output.parent.mkdir(parents=True, exist_ok=True)

        merged.rio.write_crs(datasets[0].rio.crs, inplace=True)
        merged.rio.write_nodata(nodata_val, inplace=True)

        match dataset_name:
            case "NEDSnapshot" | "Hydrodem":
                merged.rio.to_raster(output, compress="lzw", predictor=2, tiled=True, blockxsize=512, blockysize=512)
            case "FdrFac_Fdr" | "FdrFac_Fac":
                merged.rio.to_raster(output, compress="lzw", tiled=True, blockxsize=512, blockysize=512)

        logger.info("Wrote raster: %s", output)


if __name__ == "__main__":
    main()
