"""Build the stream-segment buffer binary raster.

Reads the staged stream-segment vector layer, buffers each line by
buffer_distance metres (default 60 m — 2 cell widths at 30 m), and burns the
buffered polygons onto the elevation-VRT template grid as a uint8 binary mask
(1 = inside any stream buffer, 255 = nodata).

Output: {fabric}/depstor_rasters/stream_buffer.tif
Logic source: depstor/scripts/DepStor.py:521-577 — minus the HRU-tagging step.
"""

import argparse
import time
from pathlib import Path

import geopandas as gpd

from gfv2_params.config import load_config, require_profile_key
from gfv2_params.depstor import RasterInfo, rasterize_binary, write_uint8_binary
from gfv2_params.log import configure_logging


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _load_segments(path: Path, layer: str | None, logger):
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except Exception:
        logger.warning(
            "PyArrow unavailable for vector load; falling back to fiona. "
            "Install pyarrow for faster reads."
        )
        return gpd.read_file(path, layer=layer)


def main():
    parser = argparse.ArgumentParser(description="Build depstor stream_buffer.tif.")
    parser.add_argument("--config", required=True, help="Path to depstor_streambuffer_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_streambuffer")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    template_path = Path(require_profile_key(config, "template_raster", "build_depstor_streambuffer"))
    segments_gpkg = Path(require_profile_key(config, "segments_gpkg", "build_depstor_streambuffer"))
    segments_layer = config.get("segments_layer", "nsegment")
    output_path = Path(config["output_raster"])
    buffer_distance = float(config.get("buffer_distance", 60))

    if not template_path.exists():
        raise FileNotFoundError(f"Template raster not found: {template_path}")
    if not segments_gpkg.exists():
        raise FileNotFoundError(f"Segments gpkg not found: {segments_gpkg}")

    logger.info("=== build_depstor_streambuffer ===")
    logger.info("Template     : %s", template_path)
    logger.info("Segments gpkg: %s (layer=%s)", segments_gpkg, segments_layer)
    logger.info("Output       : %s", output_path)
    logger.info("Buffer       : %s metres", buffer_distance)

    if output_path.exists() and not args.force:
        logger.info("Output already exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    logger.info("Template grid: %dx%d, CRS=%s", info.width, info.height, info.crs)

    logger.info("--- Step 1/3: Load and validate segments ---")
    t1 = time.time()
    seg_gdf = _load_segments(segments_gpkg, segments_layer, logger)
    seg_gdf = seg_gdf[seg_gdf.geometry.notna() & ~seg_gdf.geometry.is_empty].copy()
    logger.info("  Loaded %d valid segments in %s", len(seg_gdf), _elapsed(t1))

    logger.info("--- Step 2/3: Reproject (if needed) and buffer ---")
    t2 = time.time()
    if seg_gdf.crs != info.crs:
        logger.info("  Reprojecting segments from %s to %s", seg_gdf.crs, info.crs)
        seg_gdf = seg_gdf.to_crs(info.crs)
    seg_gdf["geometry"] = seg_gdf.geometry.buffer(buffer_distance)
    logger.info("  Buffered in %s", _elapsed(t2))

    logger.info("--- Step 3/3: Rasterize buffer onto template grid ---")
    t3 = time.time()
    binary = rasterize_binary(seg_gdf, info, all_touched=True)
    write_uint8_binary(binary, info, output_path)
    n_in = int((binary == 1).sum())
    logger.info(
        "  Rasterize + write done in %s | %d cells inside stream buffer (%.4f%%)",
        _elapsed(t3), n_in, 100 * n_in / binary.size,
    )

    logger.info("=== build_depstor_streambuffer complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
