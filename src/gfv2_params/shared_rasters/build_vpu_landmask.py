"""Per-VPU HRU-fabric land mask consumed by the TWI pipeline.

Library entrypoint for the shared-raster orchestrator. The thin CLI shell at
scripts/build_vpu_landmask.py delegates here so existing sbatch jobs keep
working unchanged.

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
boundary — striped patterns inside the Hydrodem footprint but outside this
VPU's HRU fabric.

Fabric-independent by design: the open-source TWI rasters this mask
constrains are global products (one per VPU under ``work/nhd_merged/``),
so the masks are also global. The HRU source is the canonical CONUS fabric
(``gfv2_nhru_merged.gpkg``); per-fabric HRU subsets are subsets of that
gpkg and filtering by the ``vpu`` column is sufficient.
"""

from __future__ import annotations

import time
from pathlib import Path

import geopandas as gpd

from gfv2_params.config import VPU_RASTER_MAP
from gfv2_params.depstor import RasterInfo, rasterize_binary, write_uint8_binary

from .context import SharedRastersContext


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

    Public for unit tests so they can exercise the build without going
    through orchestrator-style config resolution. Comparison is string-
    equality after zero-padding to two characters, so values stored as int
    (``1``) or zero-padded string (``"01"``) both match.
    """
    info = RasterInfo.from_path(template_path)
    hru_gdf = _load_hru(hru_gpkg, hru_layer, logger)

    if vpu_column not in hru_gdf.columns:
        raise KeyError(
            f"HRU gpkg {hru_gpkg} layer '{hru_layer}' has no '{vpu_column}' "
            f"column; cannot filter by VPU. Available columns: {sorted(hru_gdf.columns)}"
        )

    # Per-VPU rasters use simple two-character codes (`03`, `10`), but the HRU
    # fabric stores sub-region codes (`03N`/`03S`/`03W`, `10L`/`10U`) — there
    # is no plain `03` or `10` row in gfv2_nhru_merged.gpkg. VPU_RASTER_MAP
    # encodes the sub-region -> raster-VPU mapping, so we invert it here to
    # build the set of acceptable HRU vpu values for this raster VPU.
    vpu_key = str(vpu).zfill(2)
    sub_vpus = {sub for sub, parent in VPU_RASTER_MAP.items() if parent == vpu_key}
    acceptable_vpus = {vpu_key} | sub_vpus
    logger.info("  Filtering HRUs by %s in %s", vpu_column, sorted(acceptable_vpus))

    matches = hru_gdf[vpu_column].astype(str).isin(acceptable_vpus)
    hru_gdf = hru_gdf[matches]
    hru_gdf = hru_gdf[hru_gdf.geometry.notna() & ~hru_gdf.geometry.is_empty]
    if hru_gdf.empty:
        raise ValueError(
            f"No HRUs matched {vpu_column} in {sorted(acceptable_vpus)} in "
            f"{hru_gpkg}:{hru_layer}. Verify the VPU code and the fabric gpkg."
        )

    # all_touched=True for the same reason as build_depstor_landmask: stay
    # inclusive at thin HRU edges; create_zonal_params remains the precise
    # arbiter downstream.
    binary = rasterize_binary(hru_gdf, info, all_touched=True)
    write_uint8_binary(binary, info, output_path)
    return binary, len(hru_gdf)


def _process_vpu(
    vpu: str,
    template_pattern: str,
    hru_gpkg: Path,
    hru_layer: str,
    output_pattern: str,
    force: bool,
    logger,
) -> None:
    template_path = Path(template_pattern.replace("{vpu}", vpu))
    output_path = Path(output_pattern.replace("{vpu}", vpu))

    if not template_path.exists():
        raise FileNotFoundError(f"[VPU {vpu}] template Hydrodem not found: {template_path}")

    if output_path.exists() and not force:
        logger.info("[VPU %s] land mask exists — skipping (use --force to rebuild): %s",
                    vpu, output_path)
        return

    info = RasterInfo.from_path(template_path)
    logger.info("[VPU %s] template grid: %dx%d, CRS=%s", vpu, info.width, info.height, info.crs)

    t1 = time.time()
    binary, n_polys = build_vpu_landmask(
        template_path, hru_gpkg, hru_layer, vpu, output_path, logger,
    )
    n_land = int((binary == 1).sum())
    logger.info(
        "[VPU %s] rasterised %d HRU polygons in %s | %d land cells (%.2f%% of grid)",
        vpu, n_polys, _elapsed(t1), n_land, 100 * n_land / binary.size,
    )


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Build per-VPU HRU land masks for every VPU in ``ctx.vpus``.

    step_cfg keys:
      template_raster — per-VPU Hydrodem path pattern with ``{vpu}`` placeholder
      hru_gpkg        — canonical CONUS HRU geopackage
      hru_layer       — layer name inside the gpkg (typically ``nhru``)
      output_raster   — per-VPU output path pattern with ``{vpu}`` placeholder

    Returns an empty dict — per-VPU outputs are not registered in ctx.paths.
    """
    template_pattern = step_cfg["template_raster"]
    hru_gpkg = Path(step_cfg["hru_gpkg"])
    hru_layer = step_cfg["hru_layer"]
    output_pattern = step_cfg["output_raster"]

    if not hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {hru_gpkg}")

    if not ctx.vpus:
        logger.warning("build_vpu_landmask: ctx.vpus is empty, nothing to do")
        return {}

    for vpu in ctx.vpus:
        _process_vpu(
            vpu, template_pattern, hru_gpkg, hru_layer, output_pattern,
            ctx.force, logger,
        )

    return {}
