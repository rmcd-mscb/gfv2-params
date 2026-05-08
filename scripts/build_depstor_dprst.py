"""Combine wbody + stream-buffer + imperv to produce dprst and on-stream rasters.

Region-level mask: a water-body region is "true depression storage" only if NO
cell in that region intersects either the stream-buffer mask or the imperv mask.
Regions that touch any stream/imperv cell are excluded entirely (preserves
depstor's `remove_all_overlap=True` semantics — DepStor.py:382-407, 689-696).

Inputs (all on the same template grid):
- wbody_regions.tif (int32, 0 = nodata)  [from build_depstor_waterbody.py]
- wbody_binary.tif  (uint8 1/255)         [from build_depstor_waterbody.py]
- stream_buffer.tif (uint8 1/255)         [from build_depstor_streambuffer.py]
- imperv_binary.tif (uint8 1/255)         [from build_depstor_imperv.py]

Outputs:
- dprst_binary.tif    : wbody cells whose region touches NO stream and NO imperv.
- onstream_binary.tif : wbody cells that are NOT depression storage
                        (i.e. wbody_binary AND NOT dprst_binary).

Logic source: depstor/scripts/DepStor.py:666-701 (getDprst) and 768-791
(onStreamStor non-imperv-wbody construction).
"""

import argparse
import time
from pathlib import Path

import numpy as np
import rasterio

from gfv2_params.config import load_config
from gfv2_params.depstor import (
    RasterInfo,
    read_aligned_uint8,
    regions_to_binary,
    regions_touching_mask,
    write_uint8_binary,
)
from gfv2_params.log import configure_logging


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def main():
    parser = argparse.ArgumentParser(description="Build depstor dprst_binary.tif and onstream_binary.tif.")
    parser.add_argument("--config", required=True, help="Path to depstor_dprst_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_dprst")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )

    template_path = Path(config["template_raster"])
    wbody_binary_path = Path(config["wbody_binary_raster"])
    wbody_regions_path = Path(config["wbody_regions_raster"])
    stream_buffer_path = Path(config["stream_buffer_raster"])
    imperv_path = Path(config["imperv_raster"])
    dprst_path = Path(config["dprst_raster"])
    onstream_path = Path(config["onstream_raster"])

    for p in (template_path, wbody_binary_path, wbody_regions_path, stream_buffer_path, imperv_path):
        if not p.exists():
            raise FileNotFoundError(f"Required input not found: {p}")

    logger.info("=== build_depstor_dprst ===")
    logger.info("Template      : %s", template_path)
    logger.info("Wbody binary  : %s", wbody_binary_path)
    logger.info("Wbody regions : %s", wbody_regions_path)
    logger.info("Stream buffer : %s", stream_buffer_path)
    logger.info("Imperv binary : %s", imperv_path)
    logger.info("Dprst out     : %s", dprst_path)
    logger.info("On-stream out : %s", onstream_path)

    if dprst_path.exists() and onstream_path.exists() and not args.force:
        logger.info("Both outputs exist — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)

    logger.info("--- Step 1/4: Read aligned inputs ---")
    t1 = time.time()
    wbody_binary = read_aligned_uint8(wbody_binary_path, info)
    stream_binary = read_aligned_uint8(stream_buffer_path, info)
    imperv_binary = read_aligned_uint8(imperv_path, info)
    with rasterio.open(wbody_regions_path) as src:
        regions = src.read(1)
    logger.info("  Inputs read in %s", _elapsed(t1))

    logger.info("--- Step 2/4: Identify regions touching stream or imperv ---")
    t2 = time.time()
    stream_regions = regions_touching_mask(regions, stream_binary)
    imperv_regions = regions_touching_mask(regions, imperv_binary)
    excluded = stream_regions | imperv_regions
    n_total = int(regions.max())
    logger.info(
        "  %d total wbody regions; %d touch stream, %d touch imperv, %d excluded (%s)",
        n_total, len(stream_regions), len(imperv_regions), len(excluded), _elapsed(t2),
    )

    logger.info("--- Step 3/4: Build dprst_binary (kept regions only) ---")
    t3 = time.time()
    all_ids = set(int(v) for v in np.unique(regions) if v != 0)
    kept_ids = all_ids - excluded
    dprst_binary = regions_to_binary(regions, kept_ids)
    write_uint8_binary(dprst_binary, info, dprst_path)
    n_dprst = int((dprst_binary == 1).sum())
    logger.info(
        "  %d regions kept; %d cells in dprst (%.4f%% of grid). Written in %s",
        len(kept_ids), n_dprst, 100 * n_dprst / dprst_binary.size, _elapsed(t3),
    )

    logger.info("--- Step 4/4: Build onstream_binary (wbody AND NOT dprst) ---")
    t4 = time.time()
    onstream = np.where((wbody_binary == 1) & (dprst_binary != 1), np.uint8(1), np.uint8(255))
    write_uint8_binary(onstream, info, onstream_path)
    n_on = int((onstream == 1).sum())
    logger.info(
        "  %d cells in on-stream storage (%.4f%% of grid). Written in %s",
        n_on, 100 * n_on / onstream.size, _elapsed(t4),
    )

    logger.info("=== build_depstor_dprst complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
