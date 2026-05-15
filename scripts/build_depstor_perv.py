"""Build the pervious-area binary raster from imperv and dprst inputs.

A cell is pervious where it is inside the modeling domain and neither
impervious nor depression-storage. Implements the PRMS-standard cell-wise
interpretation of depstor workflow Level Three step `GetPervAreaTotal`
(docs/depstor_workflow.md) — NOT depstor's `remove_all_overlap=True`
all-or-nothing exclusion, which diverges from the documented design intent.

The land mask is essential: this builder defaults every cell to pervious and
only excludes imperv/dprst cells, so without masking against land_mask.tif
(the rasterised HRU fabric) the entire ocean inside the grid is marked pervious.

Inputs (all on the same template grid):
- land_mask.tif     (uint8 1/255) [from build_depstor_landmask.py]
- imperv_binary.tif (uint8 1/255) [from build_depstor_imperv.py]
- dprst_binary.tif  (uint8 1/255) [from build_depstor_dprst.py]

Output:
- perv_binary.tif : 1 where land AND (imperv != 1) AND (dprst != 1), else 255.
"""

import argparse
import time
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import RasterInfo
from gfv2_params.log import configure_logging

# Multiple of the 256-row output tile size — strip writes align with tile
# boundaries so LZW compression sees full tiles.
STRIP_ROWS = 1024


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def compute_perv_binary(
    imperv: np.ndarray, dprst: np.ndarray, land_valid: np.ndarray
) -> np.ndarray:
    """Cell is pervious where it is valid land AND NOT impervious AND NOT
    depression-storage.

    `land_valid` is the HRU-fabric land mask (boolean, True = inside the
    modeling domain — `land_mask.tif == 1`). It is required, not optional: this
    function defaults every cell to pervious, so omitting the mask would
    classify the whole ocean as pervious.
    """
    exclude = imperv == 1
    exclude |= dprst == 1
    exclude |= ~land_valid
    out = np.full_like(imperv, 1)
    out[exclude] = 255
    return out


def _uint8_binary_profile(info: RasterInfo) -> dict:
    return {
        "driver": "GTiff",
        "height": info.height,
        "width": info.width,
        "count": 1,
        "dtype": "uint8",
        "crs": info.crs,
        "transform": info.transform,
        "nodata": 255,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "BIGTIFF": "YES",
    }


def _assert_aligned(src, info: RasterInfo, name: str) -> None:
    if (src.width, src.height) != (info.width, info.height):
        raise ValueError(
            f"{name} shape ({src.width}x{src.height}) != template "
            f"({info.width}x{info.height})"
        )
    if src.crs != info.crs:
        raise ValueError(f"{name} CRS {src.crs} != template CRS {info.crs}")
    if src.transform != info.transform:
        raise ValueError(f"{name} transform mismatch with template")


def main():
    parser = argparse.ArgumentParser(description="Build depstor perv_binary.tif.")
    parser.add_argument("--config", required=True, help="Path to depstor_perv_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_perv")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    template_path = Path(require_config_key(config, "template_raster", "build_depstor_perv"))
    landmask_path = Path(config["landmask_raster"])
    imperv_path = Path(config["imperv_raster"])
    dprst_path = Path(config["dprst_raster"])
    perv_path = Path(config["perv_raster"])

    for p in (template_path, landmask_path, imperv_path, dprst_path):
        if not p.exists():
            raise FileNotFoundError(f"Required input not found: {p}")

    logger.info("=== build_depstor_perv ===")
    logger.info("Template      : %s", template_path)
    logger.info("Land mask     : %s", landmask_path)
    logger.info("Imperv binary : %s", imperv_path)
    logger.info("Dprst binary  : %s", dprst_path)
    logger.info("Perv out      : %s", perv_path)

    if perv_path.exists() and not args.force:
        logger.info("Output exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    perv_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("--- Streaming perv_binary over %d-row strips ---", STRIP_ROWS)
    t1 = time.time()
    n_perv = 0
    profile = _uint8_binary_profile(info)

    with rasterio.open(landmask_path) as landmask_src, \
         rasterio.open(imperv_path) as imperv_src, \
         rasterio.open(dprst_path) as dprst_src, \
         rasterio.open(perv_path, "w", **profile) as dst:
        _assert_aligned(landmask_src, info, "land_mask")
        _assert_aligned(imperv_src, info, "imperv")
        _assert_aligned(dprst_src, info, "dprst")

        for row_off in range(0, info.height, STRIP_ROWS):
            h = min(STRIP_ROWS, info.height - row_off)
            window = Window(0, row_off, info.width, h)
            land_valid = landmask_src.read(1, window=window) == 1
            imperv = imperv_src.read(1, window=window)
            dprst = dprst_src.read(1, window=window)
            perv = compute_perv_binary(imperv, dprst, land_valid)
            dst.write(perv, 1, window=window)
            n_perv += int((perv == 1).sum())

    total = info.height * info.width
    logger.info(
        "  %d cells marked pervious (%.4f%% of grid). Built in %s",
        n_perv, 100 * n_perv / total, _elapsed(t1),
    )

    logger.info("=== build_depstor_perv complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
