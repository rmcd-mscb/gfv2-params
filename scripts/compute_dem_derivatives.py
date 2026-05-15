"""Open-source TWI/slope/aspect from per-VPU merged Hydrodem.

**Status: parallel-artifact pipeline, not the canonical TWI source.** The
canonical CONUS TWI for downstream PRMS parameter extraction
(carea_max, smidx_coef) remains `Twi_merged_<vpu>.tif` from
merge_rpu_by_vpu.py, because the threshold values used in those parameters
(8.0 and 15.6) were calibrated against the original ArcPy TWI distribution
shape — swapping the source would invalidate those thresholds. This script
exists as a self-contained open-source recipe that can be re-run on any DEM
(future cross-border work, #53; future PRMS-threshold recalibration, etc.).

Pipeline (all richdem; no WhiteboxTools subprocess on this path):
  Hydrodem nodata fix → richdem FillDepressions+epsilon (Barnes 2014
  priority-flood) → richdem FlowAccumulation method=D8 (O'Callaghan-Mark
  1984) → richdem slope_degrees/slope_percentage/aspect on the FIXED
  (pre-fill) DEM → TWI = log(fac * 10 / (tan(slope_rad) + 0.01)) → mask
  against the per-VPU HRU land mask (`land_mask_<vpu>.tif`, built by
  `build_vpu_landmask.py`). The mask is per-VPU rather than the CONUS
  depstor land_mask because the per-VPU Hydrodem footprint bulges past
  this VPU's HRU boundary into adjacent VPUs; only the per-VPU mask
  clips the TWI to just this VPU's HRUs. Characterization vs ArcPy
  ground truth lives in notebooks/diff_twi_hydrodem_vs_merged.py.

Why richdem D8 (and not WhiteboxTools D8Pointer + D8FlowAccumulation): the
WBT subprocess pair adds GeoTIFF round-trips and walltime that scaled poorly
on CONUS VPUs (per #70). richdem's D8 FlowAccumulation runs in-process on
the same rdarray we already have from the fill step, drops two intermediate
writes (Fdr_hydrodem, Fac_hydrodem), and uses the same widely-cited
O'Callaghan & Mark (1984) D8 routing — defensible for an open-source TWI
recipe. FAC convention differs: richdem counts the cell itself (headwater =
1), so the TWI formula uses `fac` directly where the ArcPy/WBT version used
`fac + 1`.

Why richdem for fill: whitebox FillDepressions --fix_flats iterates a flat-
resolution pass that scales poorly with the size of contiguous flat regions
— VPU 18 (California's Central Valley + Mojave) didn't finish in 4 hours,
while richdem's Barnes 2014 priority-flood-with-epsilon finishes the same
VPU in ~6 minutes. Float64 precision is required during fill so the
cumulative epsilon increment doesn't push deep depressions above their rim
(richdem's "negligible gradients ... rose above" warning).

Outputs (per VPU, written to {data_root}/work/nhd_merged/<vpu>/):
- Hydrodem_merged_fixed_<vpu>.tif  (intermediate, nodata=-9999)
- Hydrodem_filled_<vpu>.tif        (richdem FillDepressions+epsilon, float64)
- Slope_hydrodem_<vpu>.tif         (richdem slope_degrees, on FIXED DEM)
- Slope_pct_hydrodem_<vpu>.tif     (richdem slope_percentage, on FIXED DEM)
- Aspect_hydrodem_<vpu>.tif        (richdem aspect, on FIXED DEM)
- Twi_hydrodem_<vpu>.tif           (log(fac*10 / (tan(slope_rad)+0.01)),
                                    per-VPU HRU land mask applied)
"""

import argparse
from pathlib import Path

import numpy as np
import rasterio
import richdem as rd
import rioxarray  # noqa: F401  (registers .rio accessor)

from gfv2_params.config import load_config
from gfv2_params.depstor import read_land_mask
from gfv2_params.log import configure_logging

# Hydrodem_merged_<vpu>.tif declares nodata=-99.99 (centimeters/100, same as
# NEDSnapshot). Re-encode to nodata=-9999 so richdem picks up an unambiguous
# value (a float -99.99 comparison is risky) and the downstream open-source
# pipeline shares one nodata convention.
DEM_SRC_NODATA = -99.99
DEM_NODATA = -9999.0


def _fix_dem_nodata(dem_src: Path, dem_fixed: Path, logger) -> None:
    """Re-encode Hydrodem nodata from the source's -99.99 to -9999.

    A float -99.99 comparison is risky for downstream consumers (richdem reads
    nodata with floating-point equality), so we remap to an unambiguous
    sentinel. LZW + predictor=2 is intentionally NOT used here: the original
    motivation (WhiteboxTools subprocess input) is gone, but predictor=2 buys
    little on a -9999-padded ocean and we keep the simpler encoding.
    """
    logger.info(
        "Re-encoding Hydrodem nodata: %s -> %s (nodata %s -> %s)",
        dem_src, dem_fixed, DEM_SRC_NODATA, DEM_NODATA,
    )
    da = rioxarray.open_rasterio(dem_src, masked=True).squeeze()
    da_fixed = da.fillna(DEM_NODATA)
    da_fixed.rio.write_nodata(DEM_NODATA, inplace=True)
    da_fixed.rio.to_raster(
        dem_fixed,
        compress="lzw",
        tiled=True,
        blockxsize=512,
        blockysize=512,
        BIGTIFF="YES",
    )


def _fill_depressions_richdem(dem_fixed: Path, dem_filled: Path, logger) -> rd.rdarray:
    """Fill depressions with richdem priority-flood + epsilon (Barnes 2014).

    Float64 throughout (in-memory AND on-disk) is required: richdem's epsilon
    increments are 1 ULP per cell. At 1000 m elevation, ULP_float32 ~ 6e-5 m
    while ULP_float64 ~ 1e-13 m — the float64 increment is small enough to be
    invisibly thin, but float32 ULPs accumulate fast enough to push deep
    closed basins above their bounding rim on continent-scale flats. So we
    fill in float64 and save in float64 so the D8 FlowAccumulation step sees
    the same per-cell ULP gradient that broke under float32 in earlier
    revisions.

    Returns the filled rdarray so the caller can pass it straight to
    FlowAccumulation without round-tripping through disk.
    """
    logger.info("Loading fixed DEM into richdem (no_data=%s)...", DEM_NODATA)
    dem_in = rd.LoadGDAL(str(dem_fixed), no_data=DEM_NODATA)
    logger.info("  shape=%s, dtype=%s -> upcasting to float64", dem_in.shape, dem_in.dtype)
    dem64 = rd.rdarray(dem_in.astype(np.float64), no_data=DEM_NODATA)
    dem64.geotransform = dem_in.geotransform
    dem64.projection = dem_in.projection
    del dem_in

    logger.info("Running FillDepressions(epsilon=True)...")
    rd.FillDepressions(dem64, in_place=True, epsilon=True)

    logger.info("Saving filled DEM as LZW float64 (preserves ULP gradient): %s", dem_filled)
    with rasterio.open(dem_fixed) as tmpl:
        profile = tmpl.profile.copy()
    profile.update({
        "dtype": "float64",
        "nodata": DEM_NODATA,
        "compress": "lzw",
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "BIGTIFF": "YES",
    })
    profile.pop("predictor", None)
    with rasterio.open(dem_filled, "w", **profile) as dst:
        dst.write(np.asarray(dem64, dtype=np.float64), 1)
    return dem64


def _flow_accumulation_d8(dem_filled: rd.rdarray, logger) -> rd.rdarray:
    """D8 flow accumulation via richdem (O'Callaghan & Mark 1984).

    Returns cell-count flow accumulation including the cell itself (headwater
    = 1). The previous WBT D8FlowAccumulation produced exclusive counts
    (headwater = 0); the TWI formula is adjusted accordingly downstream.
    """
    logger.info("Running richdem FlowAccumulation(method='D8')...")
    return rd.FlowAccumulation(dem_filled, method="D8")


def _compute_twi(
    fac: rd.rdarray,
    slope_deg: rd.rdarray,
    template_path: Path,
    land_valid: np.ndarray,
    twi_out: Path,
    logger,
) -> None:
    """Write TWI = log(fac * 10 / (tan(slope_rad) + 0.01)), masked to land.

    Matches the scale of the ArcPy reference (the constant 10 = the 100/10
    factor in the original Twi = ln(((FAC+1)*100) / (tan(slope_rad)+0.01)/10)
    expression). richdem's FAC counts the cell itself, so the historical
    `fac+1` adjustment is rolled into the FAC values themselves — no
    arithmetic change at the cell level.

    `land_valid` is the boolean HRU-fabric land mask aligned to the TWI grid
    (True = inside the fabric). Off-land cells write nodata, mirroring the
    PR #69 convention so coastal-bulge artefacts never leak into downstream
    zonal aggregation.
    """
    fac_arr = np.asarray(fac, dtype=np.float64)
    fac_nd_raw = getattr(fac, "no_data", None)
    fac_nd = float(fac_nd_raw) if fac_nd_raw is not None else None

    slope_arr = np.asarray(slope_deg, dtype=np.float64)
    slope_nd_raw = getattr(slope_deg, "no_data", None)
    slope_nd = float(DEM_NODATA if slope_nd_raw is None else slope_nd_raw)

    valid = np.isfinite(fac_arr) & np.isfinite(slope_arr)
    if fac_nd is not None and np.isfinite(fac_nd):
        valid &= fac_arr != fac_nd
    valid &= slope_arr != slope_nd
    # Filter near-vertical slopes — physically impossible (>89°) and arise from
    # closed-basin fill artifacts (a handful of cells per VPU; ~0.0001%).
    valid &= slope_arr < 89.0
    valid &= land_valid

    twi = np.full(fac_arr.shape, DEM_NODATA, dtype=np.float32)
    slope_rad = np.deg2rad(slope_arr[valid])
    twi_valid = np.log((fac_arr[valid] * 10.0) / (np.tan(slope_rad) + 0.01))
    twi[valid] = twi_valid.astype(np.float32)

    # predictor=2 is safe here because the TWI output is consumed by
    # GDAL-based tools only (build_vrt.py, marimo notebooks, QGIS) — never
    # passed to a WhiteboxTools subprocess.
    with rasterio.open(template_path) as tmpl:
        twi_profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "nodata": DEM_NODATA,
            "width": tmpl.width,
            "height": tmpl.height,
            "count": 1,
            "crs": tmpl.crs,
            "transform": tmpl.transform,
            "compress": "lzw",
            "predictor": 2,
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
            "BIGTIFF": "YES",
        }

    twi_out.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(twi_out, "w", **twi_profile) as dst:
        dst.write(twi, 1)
    logger.info(
        "Wrote: %s (%d valid pixels of %d; %d cells dropped by land mask)",
        twi_out, int(valid.sum()), valid.size, int((~land_valid).sum()),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Compute open-source DEM derivatives (filled DEM, slope, aspect, TWI) from Hydrodem.",
    )
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 06")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    logger = configure_logging("compute_dem_derivatives")
    config = load_config(Path(args.config), vpu=args.vpu, fabric=args.fabric)

    input_dir = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])

    vpu_dir = output_dir / args.vpu
    vpu_dir.mkdir(parents=True, exist_ok=True)

    dem_src = input_dir / args.vpu / f"Hydrodem_merged_{args.vpu}.tif"
    dem_fixed = vpu_dir / f"Hydrodem_merged_fixed_{args.vpu}.tif"
    dem_filled = vpu_dir / f"Hydrodem_filled_{args.vpu}.tif"
    slope_out = vpu_dir / f"Slope_hydrodem_{args.vpu}.tif"
    slope_pct_out = vpu_dir / f"Slope_pct_hydrodem_{args.vpu}.tif"
    aspect_out = vpu_dir / f"Aspect_hydrodem_{args.vpu}.tif"
    twi_out = vpu_dir / f"Twi_hydrodem_{args.vpu}.tif"
    # Per-VPU HRU land mask aligned to the per-VPU Hydrodem grid (built by
    # scripts/build_vpu_landmask.py). The CONUS depstor land_mask.tif is the
    # union of every VPU's HRUs — it leaves cells unmasked where adjacent-VPU
    # HRUs drape into this VPU's Hydrodem bulge. The per-VPU mask is strict.
    vpu_landmask_path = vpu_dir / f"land_mask_{args.vpu}.tif"

    if not dem_src.exists():
        raise FileNotFoundError(f"Hydrodem source not found: {dem_src}")
    if not vpu_landmask_path.exists():
        raise FileNotFoundError(
            f"Per-VPU land mask not found (run build_vpu_landmask first): {vpu_landmask_path}"
        )

    if not args.force and twi_out.exists():
        logger.info("Final TWI output exists (use --force to rebuild): %s", twi_out)
        return

    if args.force or not dem_fixed.exists():
        _fix_dem_nodata(dem_src, dem_fixed, logger)
    else:
        logger.info("Reusing fixed DEM (exists): %s", dem_fixed)

    if args.force or not dem_filled.exists():
        logger.info("--- richdem FillDepressions+epsilon (float64) ---")
        dem64 = _fill_depressions_richdem(dem_fixed, dem_filled, logger)
    else:
        logger.info("Reusing filled DEM (exists): %s", dem_filled)
        dem_in = rd.LoadGDAL(str(dem_filled), no_data=DEM_NODATA)
        dem64 = rd.rdarray(dem_in.astype(np.float64), no_data=DEM_NODATA)
        dem64.geotransform = dem_in.geotransform
        dem64.projection = dem_in.projection
        del dem_in

    logger.info("--- richdem FlowAccumulation method=D8 (in-memory) ---")
    fac = _flow_accumulation_d8(dem64, logger)
    del dem64

    # Compute slope/aspect from the FIXED (pre-fill) DEM, not the filled DEM.
    # richdem's epsilon=True fill imprints a per-cell ULP increment (~6e-5 m at
    # 1000 m elevation), which on flat-bottomed streams creates a non-trivial
    # synthetic slope and dims TWI on those cells. ArcPy's `Fill` doesn't
    # imprint such a gradient, so its `Slope(DEM_filled)` matches `Slope(unfilled)`
    # on stream cells (which aren't depressions and aren't modified by fill).
    # The fill is still required for proper FAC routing — this just decouples
    # slope from the epsilon artifact.
    logger.info("Loading fixed DEM into richdem for slope/aspect (no_data=%s)...", DEM_NODATA)
    dem_rd = rd.LoadGDAL(str(dem_fixed), no_data=DEM_NODATA)

    logger.info("--- richdem slope_degrees ---")
    slope_deg = rd.TerrainAttribute(dem_rd, attrib="slope_degrees")
    rd.SaveGDAL(str(slope_out), slope_deg)
    logger.info("Wrote: %s", slope_out)

    logger.info("--- richdem slope_percentage ---")
    slope_pct = rd.TerrainAttribute(dem_rd, attrib="slope_percentage")
    rd.SaveGDAL(str(slope_pct_out), slope_pct)
    logger.info("Wrote: %s", slope_pct_out)
    del slope_pct

    logger.info("--- richdem aspect ---")
    aspect = rd.TerrainAttribute(dem_rd, attrib="aspect")
    rd.SaveGDAL(str(aspect_out), aspect)
    logger.info("Wrote: %s", aspect_out)
    del aspect, dem_rd

    # Per-VPU mask and Hydrodem share the same grid by construction (the
    # mask was rasterised onto Hydrodem_merged_<vpu>.tif), so a direct
    # full-array read is correct and avoids a windowed lookup.
    logger.info("Reading per-VPU HRU land mask: %s", vpu_landmask_path)
    land_valid = read_land_mask(vpu_landmask_path)

    logger.info("--- TWI = log(fac*10 / (tan(slope_rad) + 0.01)), land-masked ---")
    _compute_twi(fac, slope_deg, dem_fixed, land_valid, twi_out, logger)

    logger.info("compute_dem_derivatives complete for VPU %s", args.vpu)


if __name__ == "__main__":
    main()
