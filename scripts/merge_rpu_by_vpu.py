"""Merge Regional Processing Unit (RPU) rasters by VPU and dataset type."""

import argparse
from pathlib import Path

import numpy as np
import rioxarray as rxr
import yaml
from rioxarray.merge import merge_arrays

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.depstor import read_land_mask_for_grid
from gfv2_params.log import configure_logging


def main():
    parser = argparse.ArgumentParser(description="Merge NHD rasters by VPU and dataset type.")
    parser.add_argument("--config", required=True, help="Path to merge_rpu_by_vpu.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing output files")
    args = parser.parse_args()

    logger = configure_logging("merge_rpu_by_vpu")

    # Load base config for data_root (this config has a unique nested structure,
    # so we load it separately via yaml rather than through load_config)
    base = load_base_config(fabric=args.fabric)
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
                raise FileNotFoundError(f"Input raster not found: {d}")
            # ESRI Grid directories (NHD source datasets) need an hdr.adf;
            # single-file rasters (e.g. TWI .tif) are read directly by rasterio.
            if d.is_dir() and not (d / "hdr.adf").exists():
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
                # After ÷100 the fill pixels are -99.99, not -9999.
                # Declare the actual fill value so downstream consumers
                # (build_vrt.py, compute_slope_aspect.py) can trust the metadata.
                nodata_val = nodata_val / 100.0  # -99.99
                merged.rio.write_nodata(nodata_val, inplace=True)
                logger.info("Converted NEDSnapshot from centimeters to meters (nodata=%.2f).", nodata_val)

            case "Hydrodem":
                nodata_val = -9999
                merged = merged.astype("float32")
                merged = merged.where(~merged.isnull(), nodata_val)
                merged = merged / 100.0
                nodata_val = nodata_val / 100.0  # -99.99
                logger.info("Converted Hydrodem from centimeters to meters (nodata=%.2f).", nodata_val)

            case "FdrFac_Fdr":
                nodata_val = 255
                merged = merged.fillna(nodata_val).astype("uint8")

            case "FdrFac_Fac":
                nodata_val = -9999
                merged = merged.fillna(nodata_val).astype("int32")

            case "TWI":
                # TWI is a unitless float (log of upslope contributing area / slope).
                # Source rasters declare nodata=-FLT_MAX (~-3.4e38); remap to -9999
                # to match NEDSnapshot/Hydrodem conventions for downstream consumers.
                # No unit conversion (TWI is dimensionless — do NOT divide by 100).
                nodata_val = -9999
                merged = merged.astype("float32")
                merged = merged.where(merged > -1e30, nodata_val)

                # Mask to the HRU-fabric land_mask.tif (PR #69 convention). The
                # per-RPU TWI tiles cover the source-DEM footprint, which bulges
                # into the ocean on coastal RPUs; without this step the merged
                # TWI carries those bulges into downstream zonal aggregation.
                landmask_path = Path(require_config_key(
                    base, "landmask_raster", "merge_rpu_by_vpu (TWI)",
                ))
                if not landmask_path.exists():
                    raise FileNotFoundError(
                        f"Land mask not found (run build_depstor_landmask first): {landmask_path}"
                    )
                logger.info("Masking merged TWI to HRU-fabric land mask: %s", landmask_path)
                merged_transform = merged.rio.transform()
                merged_h, merged_w = merged.shape[-2], merged.shape[-1]
                land_valid = read_land_mask_for_grid(
                    landmask_path, merged_transform, merged_h, merged_w,
                )
                merged_arr = np.asarray(merged.values)
                n_off_land = int((~land_valid & (merged_arr != nodata_val)).sum())
                merged_arr = np.where(land_valid, merged_arr, np.float32(nodata_val))
                merged = merged.copy(data=merged_arr)
                logger.info(
                    "  Land mask dropped %d off-fabric cells (set to nodata=%s)",
                    n_off_land, nodata_val,
                )

            case _:
                raise ValueError(f"Unknown dataset_name: {dataset_name}")

        logger.info("Writing raster: %s", output)
        output.parent.mkdir(parents=True, exist_ok=True)

        merged.rio.write_crs(datasets[0].rio.crs, inplace=True)
        merged.rio.write_nodata(nodata_val, inplace=True)

        # BIGTIFF=YES: several CONUS VPUs land in the 3-4 GB range and VPU 10
        # exceeds the classic 4 GB TIFF cap. Force BigTIFF for all merges to
        # avoid CPLE_AppDefinedError on the heaviest VPUs; the format overhead
        # for smaller VPUs is a few bytes (8-byte vs 4-byte offsets).
        match dataset_name:
            case "NEDSnapshot" | "Hydrodem" | "TWI":
                merged.rio.to_raster(output, compress="lzw", predictor=2, tiled=True, blockxsize=512, blockysize=512, BIGTIFF="YES")
            case "FdrFac_Fdr" | "FdrFac_Fac":
                merged.rio.to_raster(output, compress="lzw", tiled=True, blockxsize=512, blockysize=512, BIGTIFF="YES")

        logger.info("Wrote raster: %s", output)


if __name__ == "__main__":
    main()
