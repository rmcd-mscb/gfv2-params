"""Per-VPU merge of NHDPlus Regional Processing Unit rasters.

Library entrypoint for the ``merge_rpu_by_vpu`` step in the shared-raster
orchestrator (``scripts/build_shared_rasters.py``). Registered via the
BUILDERS dict in ``shared_rasters/__init__.py`` and called by the
orchestrator's STEP_ORDER walk.

One builder serves two step invocations because the production pipeline runs
this twice — once for the standard datasets (NEDSnapshot, Hydrodem, FdrFac_Fdr,
FdrFac_Fac) and again, after build_vpu_landmask, for the TWI dataset which
must be masked against the per-VPU HRU land mask (issue #70). The two
invocations are distinguished only by which `manifest` YAML is referenced in
the step block; the per-dataset case-logic below handles both.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import rioxarray as rxr
import yaml
from rioxarray.merge import merge_arrays

from gfv2_params.depstor import read_land_mask_for_grid

from .context import SharedRastersContext


def _resolve_manifest(manifest: str | Path, ctx: SharedRastersContext) -> Path:
    """Resolve manifest path relative to data_root if not already absolute."""
    p = Path(manifest)
    if p.is_absolute():
        return p
    # Prefer repo-relative resolution (configs live alongside the orchestrator).
    repo_root = Path(__file__).resolve().parents[3]
    candidate = repo_root / p
    if candidate.exists():
        return candidate
    return Path.cwd() / p


def _process_dataset(
    dataset_name: str,
    values: dict,
    vpu: str,
    base_path: Path,
    force: bool,
    logger,
) -> None:
    rpus = values.get("rpus", [])
    output_file = values.get("output")
    output = base_path / output_file.lstrip("/")

    if output.exists() and not force:
        logger.info("[VPU %s/%s] output exists, skipping: %s", vpu, dataset_name, output)
        return

    datasets = []
    for d in rpus:
        d = base_path / d.lstrip("/")
        logger.info("[VPU %s/%s] reading raster: %s", vpu, dataset_name, d)
        if not d.exists():
            raise FileNotFoundError(f"Input raster not found: {d}")
        # ESRI Grid directories (NHD source datasets) need an hdr.adf;
        # single-file rasters (e.g. TWI .tif) are read directly by rasterio.
        if d.is_dir() and not (d / "hdr.adf").exists():
            raise ValueError(f"Folder {d} does not appear to be a valid ESRI Grid raster")
        ds = rxr.open_rasterio(str(d), masked=True).squeeze()
        datasets.append(ds)

    logger.info("[VPU %s/%s] merging %d datasets", vpu, dataset_name, len(datasets))
    if len(datasets) == 1:
        merged = datasets[0]
    elif dataset_name in ("NEDSnapshot", "Hydrodem"):
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
            # After ÷100 the fill pixels are -99.99, not -9999. Declare the
            # actual fill value so downstream consumers (build_vrt,
            # compute_slope_aspect) can trust the metadata.
            nodata_val = nodata_val / 100.0  # -99.99
            merged.rio.write_nodata(nodata_val, inplace=True)
            logger.info("[VPU %s/%s] converted from cm to m (nodata=%.2f)",
                        vpu, dataset_name, nodata_val)

        case "Hydrodem":
            nodata_val = -9999
            merged = merged.astype("float32")
            merged = merged.where(~merged.isnull(), nodata_val)
            merged = merged / 100.0
            nodata_val = nodata_val / 100.0  # -99.99
            logger.info("[VPU %s/%s] converted from cm to m (nodata=%.2f)",
                        vpu, dataset_name, nodata_val)

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

            # Mask to the per-VPU HRU land mask (issue #70). The per-RPU TWI
            # tiles cover the source-DEM footprint, which bulges past this
            # VPU's HRU boundary on both the coastal flank (ocean) and the
            # inland flank (adjacent VPUs / Canadian border). The per-VPU
            # mask is strict: only HRUs whose `vpu` attribute matches this
            # VPU are rasterised, so adjacent-VPU drape doesn't survive into
            # the merged TWI footprint.
            vpu_landmask_path = base_path / "shared" / "per_vpu" / vpu / f"land_mask_{vpu}.tif"
            if not vpu_landmask_path.exists():
                raise FileNotFoundError(
                    f"Per-VPU land mask not found (run build_vpu_landmask first): "
                    f"{vpu_landmask_path}"
                )
            logger.info("[VPU %s/TWI] masking merged TWI to per-VPU HRU land mask: %s",
                        vpu, vpu_landmask_path)
            merged_transform = merged.rio.transform()
            merged_h, merged_w = merged.shape[-2], merged.shape[-1]
            land_valid = read_land_mask_for_grid(
                vpu_landmask_path, merged_transform, merged_h, merged_w,
            )
            merged_arr = np.asarray(merged.values)
            n_off_land = int((~land_valid & (merged_arr != nodata_val)).sum())
            merged_arr = np.where(land_valid, merged_arr, np.float32(nodata_val))
            merged = merged.copy(data=merged_arr)
            logger.info(
                "[VPU %s/TWI] per-VPU land mask dropped %d off-fabric cells (set to nodata=%s)",
                vpu, n_off_land, nodata_val,
            )

        case _:
            raise ValueError(f"Unknown dataset_name: {dataset_name}")

    logger.info("[VPU %s/%s] writing raster: %s", vpu, dataset_name, output)
    output.parent.mkdir(parents=True, exist_ok=True)

    merged.rio.write_crs(datasets[0].rio.crs, inplace=True)
    merged.rio.write_nodata(nodata_val, inplace=True)

    # BIGTIFF=YES: several CONUS VPUs land in the 3-4 GB range and VPU 10
    # exceeds the classic 4 GB TIFF cap. Force BigTIFF for all merges to
    # avoid CPLE_AppDefinedError on the heaviest VPUs; the format overhead
    # for smaller VPUs is a few bytes (8-byte vs 4-byte offsets).
    match dataset_name:
        case "NEDSnapshot" | "Hydrodem" | "TWI":
            merged.rio.to_raster(
                output, compress="lzw", predictor=2, tiled=True,
                blockxsize=512, blockysize=512, BIGTIFF="YES",
            )
        case "FdrFac_Fdr" | "FdrFac_Fac":
            merged.rio.to_raster(
                output, compress="lzw", tiled=True,
                blockxsize=512, blockysize=512, BIGTIFF="YES",
            )

    logger.info("[VPU %s/%s] wrote: %s", vpu, dataset_name, output)


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Merge per-RPU NHDPlus rasters into per-VPU GeoTIFFs for every VPU in ``ctx.vpus``.

    step_cfg keys:
      manifest — path to the VPU-keyed YAML manifest (e.g.,
                 configs/shared_rasters/merge_rpu_by_vpu.yml for non-TWI datasets, or
                 configs/shared_rasters/merge_rpu_by_vpu_twi.yml for TWI). Path is resolved
                 relative to repo root if not absolute.

    Returns an empty dict — per-VPU outputs are not registered in ctx.paths
    (downstream consumers re-template per-VPU paths off conventional patterns).
    """
    manifest_path = _resolve_manifest(step_cfg["manifest"], ctx)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest config not found: {manifest_path}")
    logger.info("merge_rpu_by_vpu: manifest = %s", manifest_path)

    with open(manifest_path) as f:
        rpu_config = yaml.safe_load(f)

    if not ctx.vpus:
        logger.warning("merge_rpu_by_vpu: ctx.vpus is empty, nothing to do")
        return {}

    base_path = ctx.data_root
    for vpu in ctx.vpus:
        vpu_config = rpu_config.get(vpu)
        if vpu_config is None:
            logger.warning("merge_rpu_by_vpu: VPU %s not in manifest, skipping", vpu)
            continue
        for dataset_name, values in vpu_config.items():
            _process_dataset(dataset_name, values, vpu, base_path, ctx.force, logger)

    return {}
