import marimo

__generated_with = "0.13.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import yaml
    import rioxarray as rxr
    from pathlib import Path
    from rioxarray.merge import merge_arrays
    import matplotlib.pyplot as plt
    import sys
    import marimo as mo
    import pandas as pd
    import numpy as np
    # Add the src directory to the Python path
    src_path = Path(__file__).resolve().parent.parent / "src"
    sys.path.append(str(src_path))

    # Now you can import helpers
    from helpers import load_config
    import simple_parsing
    from dataclasses import dataclass
    from simple_parsing import ArgumentParser
    from typing import Literal
    return (
        ArgumentParser,
        Literal,
        Path,
        dataclass,
        load_config,
        merge_arrays,
        rxr,
    )


@app.cell
def _(ArgumentParser, Literal, dataclass):

    @dataclass
    class Config:
        config_file: str = "gfv2-params/configs/config_merge_rpu_by_vpu.yml"
        vpu: Literal["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "20"] = "01"

    parser = ArgumentParser()
    parser.add_arguments(Config, dest="cfg")
    args = parser.parse_args()
    cfg = args.cfg

    print(cfg.config_file, cfg.vpu)

    return (cfg,)


@app.cell
def _(Path, cfg, load_config):
    # cell: load a config file using your helper
    base_path = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/")
    config = load_config(base_path / cfg.config_file)
    config = config.get(cfg.vpu)
    config.items()
    return base_path, config


@app.cell
def _(base_path, config, merge_arrays, rxr):
    # for vpu_id, data_sets in config.items():
    #     print(f"\n📦 VPU: {vpu_id}")
    for dataset_name, values in config.items():
        print(f"  🔹 Dataset: {dataset_name}")
        rpus = values.get("rpus", [])
        output_file = values.get("output")
        output = base_path / output_file.lstrip("/")

        if output.exists():
            print(f"✅ Output already exists, skipping: {output}")
            continue

        print(f"  🔹 Dataset: {dataset_name}")
        datasets = []
        for d in rpus:
            d = base_path / d.lstrip("/")
            print(f"rpu: {d}")

            if not d.exists():
                raise FileNotFoundError(f"Input raster folder not found: {d}")

            if not (d / "hdr.adf").exists():
                raise ValueError(f"Folder {d} does not appear to be a valid ESRI Grid raster")

            print(f"Reading raster from: {d}")
            ds = rxr.open_rasterio(str(d), masked=True).squeeze()
            datasets.append(ds)

        print(f"Merging {len(datasets)} datasets")
        if len(datasets) == 1:
            merged = datasets[0]
        else:
            merged = merge_arrays(datasets, method="first", res=None)

        # Optional: check all CRSs are the same
        crs_set = {ds.rio.crs.to_string() for ds in datasets}
        if len(crs_set) > 1:
            raise ValueError(f"Inconsistent CRS among inputs: {crs_set}")

        # Set nodata and cast based on dataset type
        match dataset_name:
            case "NEDSnapshot" | "Hydrodem":
                nodata_val = datasets[0].rio.nodata or -9999
                merged = merged.astype("float32")

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
    return


if __name__ == "__main__":
    app.run()
