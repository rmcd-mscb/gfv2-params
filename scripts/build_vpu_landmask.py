"""Build the per-VPU HRU-fabric land mask consumed by the TWI pipeline.

Rasterises HRUs filtered to ``vpu == <vpu>`` onto the per-VPU Hydrodem grid
(``Hydrodem_merged_<vpu>.tif``), producing
``work/nhd_merged/<vpu>/land_mask_<vpu>.tif`` — a uint8 1/255 binary raster
where 1 = inside one of this VPU's HRUs, 255 = outside.

Why a per-VPU mask instead of the CONUS ``land_mask.tif`` from PR #69: the
per-VPU Hydrodem is a roughly rectangular tile that bulges past its VPU's
HRU boundary on inland flanks (the western boundary against adjacent VPUs,
and the northern boundary against the international line). The CONUS
``land_mask.tif`` covers every VPU's HRUs, so it reports "land" in those
bulges wherever an adjacent VPU happens to drape coverage. That left the
open-source TWI raster (``Twi_hydrodem_<vpu>.tif``) and the canonical
``Twi_merged_<vpu>.tif`` with values along the priority-flood-to-nodata
boundary — striped patterns inside the Hydrodem footprint but outside
this VPU's HRU fabric.

This mask constrains each per-VPU TWI product to just the HRUs that
belong to that VPU. The CONUS depstor ``land_mask.tif`` is left alone —
depstor's per-HRU zonal stats don't care about cross-VPU mask bleed.

Fabric-independent by design: the open-source TWI rasters this mask
constrains are global products (one per VPU under ``work/nhd_merged/``),
so the masks are also global. The HRU source is the canonical CONUS
fabric (``gfv2_nhru_merged.gpkg``); per-fabric HRU subsets are subsets
of that gpkg and filtering by the ``vpu`` column is sufficient. The
script takes no ``--fabric`` argument for this reason.

Output: ``{data_root}/work/nhd_merged/<vpu>/land_mask_<vpu>.tif``
"""

import argparse
import time
from pathlib import Path

import geopandas as gpd

from gfv2_params.config import load_config, require_config_key
from gfv2_params.depstor import RasterInfo, rasterize_binary, write_uint8_binary
from gfv2_params.log import configure_logging


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _load_hru(path: Path, layer: str, logger):
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer)


def build_vpu_landmask(
    template_path: Path,
    hru_gpkg: Path,
    hru_layer: str,
    vpu: str,
    output_path: Path,
    logger,
    vpu_column: str = "vpu",
):
    """Rasterise HRUs filtered to this VPU onto the per-VPU template grid.

    Pulled out of ``main`` so unit tests can exercise the build without going
    through argument parsing and config-file resolution. Comparison is
    string-equality after zero-padding to two characters, so values stored
    as int (``1``) or zero-padded string (``"01"``) both match.
    """
    info = RasterInfo.from_path(template_path)
    hru_gdf = _load_hru(hru_gpkg, hru_layer, logger)

    if vpu_column not in hru_gdf.columns:
        raise KeyError(
            f"HRU gpkg {hru_gpkg} layer '{hru_layer}' has no '{vpu_column}' "
            f"column; cannot filter by VPU. Available columns: {sorted(hru_gdf.columns)}"
        )

    vpu_key = str(vpu).zfill(2)
    matches = hru_gdf[vpu_column].astype(str).str.zfill(2) == vpu_key
    hru_gdf = hru_gdf[matches]
    hru_gdf = hru_gdf[hru_gdf.geometry.notna() & ~hru_gdf.geometry.is_empty]
    if hru_gdf.empty:
        raise ValueError(
            f"No HRUs matched {vpu_column}=={vpu} in {hru_gpkg}:{hru_layer}. "
            f"Verify the VPU code and the fabric gpkg."
        )

    # all_touched=True for the same reason as build_depstor_landmask: stay
    # inclusive at thin HRU edges; create_zonal_params remains the precise
    # arbiter downstream.
    binary = rasterize_binary(hru_gdf, info, all_touched=True)
    write_uint8_binary(binary, info, output_path)
    return binary, len(hru_gdf)


def main():
    parser = argparse.ArgumentParser(description="Build per-VPU HRU-fabric land mask for the TWI pipeline.")
    parser.add_argument("--config", required=True, help="Path to vpu_landmask_raster.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_vpu_landmask")
    t_start = time.time()

    # No --fabric: the per-VPU land mask is a fabric-independent product. The
    # config pins the HRU source to the canonical CONUS `gfv2_nhru_merged.gpkg`
    # and the output to work/nhd_merged/<vpu>/ — both fabric-free locations.
    # `load_config` still goes through fabric resolution to set `{data_root}`,
    # but the step-config's `hru_gpkg` / `hru_layer` win over any fabric-profile
    # defaults via the standard step-overrides-base merge.
    config = load_config(
        Path(args.config),
        vpu=args.vpu,
        base_config_path=Path(args.base_config) if args.base_config else None,
    )

    template_path = Path(require_config_key(config, "template_raster", "build_vpu_landmask"))
    hru_gpkg = Path(require_config_key(config, "hru_gpkg", "build_vpu_landmask"))
    hru_layer = require_config_key(config, "hru_layer", "build_vpu_landmask")
    output_path = Path(config["output_raster"])

    if not template_path.exists():
        raise FileNotFoundError(f"Per-VPU Hydrodem template not found: {template_path}")
    if not hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {hru_gpkg}")

    logger.info("=== build_vpu_landmask (VPU %s) ===", args.vpu)
    logger.info("Template  : %s", template_path)
    logger.info("HRU fabric: %s (layer=%s, filtered to vpu=%s)", hru_gpkg, hru_layer, args.vpu)
    logger.info("Output    : %s", output_path)

    if output_path.exists() and not args.force:
        logger.info("Output already exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    logger.info("Template grid: %dx%d, CRS=%s", info.width, info.height, info.crs)

    t1 = time.time()
    binary, n_polys = build_vpu_landmask(
        template_path, hru_gpkg, hru_layer, args.vpu, output_path, logger,
    )
    n_land = int((binary == 1).sum())
    logger.info(
        "  Rasterised %d HRU polygons in %s | %d land cells (%.2f%% of grid)",
        n_polys, _elapsed(t1), n_land, 100 * n_land / binary.size,
    )

    logger.info("=== build_vpu_landmask complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
