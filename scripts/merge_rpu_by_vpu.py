"""Merge Regional Processing Unit (RPU) rasters by VPU and dataset type.

Two processing paths based on RPU count:

Single-RPU: Uses rasterio windowed read/write for minimal memory (~MB).
Multi-RPU:  Pairwise sequential merge — loads 2 RPUs at a time, merges,
            frees inputs, repeats. Peak memory is ~2 RPUs + 1 output
            instead of N RPUs + 1 output.
"""

import argparse
import gc
from pathlib import Path

import numpy as np
import rasterio
import rioxarray as rxr
import yaml
from rioxarray.merge import merge_arrays

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging


def _process_single_rpu_elevation(input_path, output_path, nodata_val, logger):
    """Windowed read/write for single-RPU elevation (cm → m). Minimal memory."""
    with rasterio.open(input_path) as src:
        profile = src.profile.copy()
        src_nodata = src.nodata
        profile.update(
            dtype="float32",
            compress="lzw",
            predictor=2,
            tiled=True,
            blockxsize=512,
            blockysize=512,
        )

        output_nodata = nodata_val / 100.0  # -99.99
        profile["nodata"] = output_nodata

        with rasterio.open(output_path, "w", **profile) as dst:
            for _, window in src.block_windows(1):
                data = src.read(1, window=window).astype(np.float32)

                # Mask source nodata
                if src_nodata is not None:
                    data[data == src_nodata] = output_nodata * 100.0  # mark for divide

                # Convert cm → m
                data = data / 100.0
                dst.write(data, 1, window=window)

    logger.info("Single-RPU windowed write complete: %s", output_path)
    return output_nodata


def _process_single_rpu_copy(input_path, output_path, nodata_val, dtype, logger):
    """Windowed read/write for single-RPU non-elevation (Fdr, Fac). Minimal memory."""
    with rasterio.open(input_path) as src:
        profile = src.profile.copy()
        np_dtype = np.dtype(dtype)
        profile.update(
            dtype=dtype,
            compress="lzw",
            tiled=True,
            blockxsize=512,
            blockysize=512,
            nodata=nodata_val,
        )

        with rasterio.open(output_path, "w", **profile) as dst:
            for _, window in src.block_windows(1):
                data = src.read(1, window=window).astype(np_dtype)
                # Fill any NaN-like values (source nodata) with target nodata
                if src.nodata is not None:
                    data[data == src.nodata] = nodata_val
                dst.write(data, 1, window=window)

    logger.info("Single-RPU windowed write complete: %s", output_path)


def _pairwise_merge(rpu_paths, base_path, method, logger):
    """Merge N RPUs pairwise to limit peak memory to ~2 RPUs + 1 output."""
    logger.info("Pairwise merge of %d RPUs (method=%s)", len(rpu_paths), method)

    # Load first RPU
    first_path = base_path / rpu_paths[0].lstrip("/")
    result = rxr.open_rasterio(str(first_path), masked=True).squeeze()
    logger.info("  Loaded RPU 1/%d: %s", len(rpu_paths), first_path.name)

    for i, rpu_rel in enumerate(rpu_paths[1:], start=2):
        rpu_path = base_path / rpu_rel.lstrip("/")
        logger.info("  Loading RPU %d/%d: %s", i, len(rpu_paths), rpu_path.name)
        next_rpu = rxr.open_rasterio(str(rpu_path), masked=True).squeeze()

        logger.info("  Merging RPU %d into result...", i)
        result = merge_arrays([result, next_rpu], method=method)

        # Explicitly free the consumed RPU
        del next_rpu
        gc.collect()

    return result


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

        output.parent.mkdir(parents=True, exist_ok=True)
        n_rpus = len(rpus)
        logger.info("Processing %d RPU(s) for %s", n_rpus, dataset_name)

        # --- Single-RPU fast path: windowed read/write, minimal memory ---
        if n_rpus == 1:
            rpu_path = base_path / rpus[0].lstrip("/")
            if not rpu_path.exists():
                raise FileNotFoundError(f"Input raster folder not found: {rpu_path}")
            if not (rpu_path / "hdr.adf").exists():
                raise ValueError(f"Folder {rpu_path} does not appear to be a valid ESRI Grid raster")

            logger.info("Single-RPU path (windowed): %s", rpu_path)

            match dataset_name:
                case "NEDSnapshot" | "Hydrodem":
                    nodata_val = -9999
                    actual_nodata = _process_single_rpu_elevation(
                        str(rpu_path), str(output), nodata_val, logger,
                    )
                    logger.info("Converted %s from centimeters to meters (nodata=%.2f).", dataset_name, actual_nodata)

                case "FdrFac_Fdr":
                    _process_single_rpu_copy(str(rpu_path), str(output), nodata_val=255, dtype="uint8", logger=logger)

                case "FdrFac_Fac":
                    _process_single_rpu_copy(str(rpu_path), str(output), nodata_val=-9999, dtype="int32", logger=logger)

                case _:
                    raise ValueError(f"Unknown dataset_name: {dataset_name}")

            logger.info("Wrote raster: %s", output)
            continue

        # --- Multi-RPU path: pairwise merge to limit peak memory ---
        for rpu_rel in rpus:
            rpu_path = base_path / rpu_rel.lstrip("/")
            if not rpu_path.exists():
                raise FileNotFoundError(f"Input raster folder not found: {rpu_path}")
            if not (rpu_path / "hdr.adf").exists():
                raise ValueError(f"Folder {rpu_path} does not appear to be a valid ESRI Grid raster")

        match dataset_name:
            case "NEDSnapshot" | "Hydrodem":
                method = "min"
            case "FdrFac_Fdr" | "FdrFac_Fac":
                method = "first"
            case _:
                raise ValueError(f"Unknown dataset_name: {dataset_name}")

        merged = _pairwise_merge(rpus, base_path, method, logger)

        crs = rxr.open_rasterio(
            str(base_path / rpus[0].lstrip("/")), masked=True,
        ).squeeze().rio.crs

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

        logger.info("Writing raster: %s", output)

        merged.rio.write_crs(crs, inplace=True)
        merged.rio.write_nodata(nodata_val, inplace=True)

        match dataset_name:
            case "NEDSnapshot" | "Hydrodem":
                merged.rio.to_raster(output, compress="lzw", predictor=2, tiled=True, blockxsize=512, blockysize=512)
            case "FdrFac_Fdr" | "FdrFac_Fac":
                merged.rio.to_raster(output, compress="lzw", tiled=True, blockxsize=512, blockysize=512)

        logger.info("Wrote raster: %s", output)

        # Free merged array before next dataset type
        del merged
        gc.collect()


if __name__ == "__main__":
    main()
