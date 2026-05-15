"""Open-source TWI/FDR/FAC/slope/aspect from per-VPU merged Hydrodem.

**Status: parallel-artifact pipeline, not the canonical TWI source.** The
canonical CONUS TWI for downstream PRMS parameter extraction
(carea_max, smidx_coef) remains `Twi_merged_<vpu>.tif` from
merge_rpu_by_vpu.py, because the threshold values used in those parameters
(8.0 and 15.6) were calibrated against the original ArcPy TWI distribution
shape — swapping the source would invalidate those thresholds. This script
exists as a self-contained open-source recipe that can be re-run on any DEM
(future cross-border work, #53; future PRMS-threshold recalibration, etc.).

Pipeline (hybrid richdem + WhiteboxTools):
  Hydrodem nodata fix → richdem FillDepressions+epsilon (Barnes 2014
  priority-flood) → WhiteboxTools D8Pointer (Esri encoding) → WhiteboxTools
  D8FlowAccumulation (cells, exclusive of the cell itself) → richdem
  slope_degrees/slope_percentage/aspect on the FIXED (pre-fill) DEM → TWI
  = log((fac + 1) * 10 / (tan(slope_rad) + 0.01)) → mask against the
  per-VPU HRU land mask (`land_mask_<vpu>.tif`, built by
  `build_vpu_landmask.py`). The mask is per-VPU rather than the CONUS
  depstor land_mask because the per-VPU Hydrodem footprint bulges past
  this VPU's HRU boundary into adjacent VPUs; only the per-VPU mask clips
  the TWI to just this VPU's HRUs. Characterization vs ArcPy ground truth
  lives in notebooks/diff_twi_hydrodem_vs_merged.py.

Why hybrid (richdem fill + WBT D8): a previous all-richdem revision used
`rd.FlowAccumulation(method='D8')` for routing. That worked on small/mid
VPUs but segfaulted on the giants — VPU 03 (2.31B cells) at
"Creating dependencies array" with 122 GB RAM allocated, and VPU 10 (3.60B
cells) never made it past 1% fill. richdem's internal C++ uses int32 for
cell linear indices, which overflows at 2^31 - 1 = 2.147B cells.
WhiteboxTools' D8FlowAccumulation uses `size_t` and routes over arbitrarily
large grids — slower per cell but stable. We still use richdem for the
*fill* step because WBT's `FillDepressions --fix_flats` doesn't terminate
on continent-scale flats (VPU 18, California's Central Valley + Mojave,
didn't finish in 4 hours), where richdem's Barnes 2014 priority-flood
finishes in ~6 minutes. FAC convention reverts to WBT's: headwater = 0
(exclusive of self), so the TWI formula uses `(fac + 1) * 10` — matching
the ArcPy reference.

Why float64 throughout the fill: richdem's epsilon increments are 1 ULP
per cell. At 1000 m elevation, ULP_float32 ~ 6e-5 m while ULP_float64 ~
1e-13 m — the float64 increment is small enough to be invisibly thin, but
float32 ULPs accumulate fast enough to push deep closed basins above their
bounding rim on continent-scale flats. So we fill in float64 and save in
float64 so WBT D8Pointer / D8FlowAccumulation see the same per-cell ULP
gradient that broke under float32 in earlier revisions.

Outputs (per VPU, written to {data_root}/work/nhd_merged/<vpu>/):
- Hydrodem_merged_fixed_<vpu>.tif  (intermediate, nodata=-9999)
- Hydrodem_filled_<vpu>.tif        (richdem FillDepressions+epsilon, float64)
- Fdr_hydrodem_<vpu>.tif           (WBT D8 pointer, Esri encoding)
- Fac_hydrodem_<vpu>.tif           (WBT D8 flow accumulation, cells)
- Slope_hydrodem_<vpu>.tif         (richdem slope_degrees, on FIXED DEM)
- Slope_pct_hydrodem_<vpu>.tif     (richdem slope_percentage, on FIXED DEM)
- Aspect_hydrodem_<vpu>.tif        (richdem aspect, on FIXED DEM)
- Twi_hydrodem_<vpu>.tif           (log((fac+1)*10 / (tan(slope_rad)+0.01)),
                                    per-VPU HRU land mask applied)
"""

import argparse
import os
import subprocess
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


def _find_whitebox_tools_binary() -> str:
    """Locate the WhiteboxTools executable inside the bundled `whitebox` package.

    Mirrors the helper in scripts/build_depstor_routing.py. Instantiates
    WhiteboxTools() first to trigger the auto-download of the rust binary on a
    fresh env (idempotent on subsequent calls).
    """
    import whitebox  # local import — keep optional unless this step runs
    from whitebox import WhiteboxTools

    WhiteboxTools()  # auto-downloads the binary on first use

    pkg_dir = os.path.dirname(whitebox.__file__)
    candidates = [
        os.path.join(pkg_dir, "whitebox_tools.exe"),
        os.path.join(pkg_dir, "whitebox_tools"),
        os.path.join(pkg_dir, "bin", "whitebox_tools.exe"),
        os.path.join(pkg_dir, "bin", "whitebox_tools"),
    ]
    runner = next((c for c in candidates if os.path.isfile(c)), None)
    if runner is None:
        raise FileNotFoundError(
            "WhiteboxTools binary not found inside `whitebox` package. "
            "Reinstall the `whitebox` pip package."
        )
    return runner


def _run_wbt(runner: str, tool: str, args: list[str], logger) -> None:
    cmd = [runner, f"--wd={os.getcwd()}", "--max_procs=-1", f"-r={tool}", *args, "-v"]
    logger.info("  Running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.stdout:
        logger.debug("WBT stdout:\n%s", proc.stdout)
    if proc.returncode != 0:
        logger.error("WBT stderr:\n%s", proc.stderr)
        raise RuntimeError(
            f"WhiteboxTools {tool} failed (exit code {proc.returncode}). See stderr above."
        )


def _fix_dem_nodata(dem_src: Path, dem_fixed: Path, logger) -> None:
    """Re-encode Hydrodem nodata from the source's -99.99 to -9999.

    A float -99.99 comparison is risky for downstream consumers (richdem reads
    nodata with floating-point equality), so we remap to an unambiguous
    sentinel. The output is written with LZW *without* predictor=2:
    WhiteboxTools' built-in TIFF reader silently produces garbage on
    horizontal-differencing predictor input (the file reads fine in
    GDAL/rasterio but every WBT downstream step propagates corrupt elevations).
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


def _fill_depressions_richdem(dem_fixed: Path, dem_filled: Path, logger) -> None:
    """Fill depressions with richdem priority-flood + epsilon (Barnes 2014).

    Float64 throughout (in-memory AND on-disk) is required: richdem's epsilon
    increments are 1 ULP per cell. At 1000 m elevation, ULP_float32 ~ 6e-5 m
    while ULP_float64 ~ 1e-13 m — the float64 increment is small enough to be
    invisibly thin, but float32 ULPs accumulate fast enough to push deep
    closed basins above their bounding rim on continent-scale flats. So we
    fill in float64 and save in float64 so the downstream WBT D8 step sees
    the same per-cell ULP gradient (a float32 cast collapses adjacent ULP
    values to identical float32 and WBT then assigns FDR = 0 across filled
    flats, breaking flow accumulation through every filled depression).
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


def _compute_twi(
    fac_path: Path,
    slope_deg: rd.rdarray,
    land_valid: np.ndarray,
    twi_out: Path,
    logger,
) -> None:
    """Write TWI = log((fac + 1) * 10 / (tan(slope_rad) + 0.01)), masked to land.

    Matches the ArcPy reference exactly: fac is FlowAccumulation+1 (WBT's
    `--out_type=cells` counts upslope cells exclusive of self, so headwater
    = 0; the +1 includes the cell itself), the 100/10 factor simplifies to
    *10, and the +0.01 constant prevents div-by-zero on flats.

    `land_valid` is the boolean per-VPU HRU mask aligned to the FAC grid
    (True = inside the fabric). Off-land cells write nodata.
    """
    with rasterio.open(fac_path) as fac_ds:
        fac = fac_ds.read(1).astype(np.float64)
        fac_nd = fac_ds.nodata
        twi_profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "nodata": DEM_NODATA,
            "width": fac_ds.width,
            "height": fac_ds.height,
            "count": 1,
            "crs": fac_ds.crs,
            "transform": fac_ds.transform,
            "compress": "lzw",
            # predictor=2 is safe here because the TWI output is consumed by
            # GDAL-based tools only (build_vrt.py, marimo notebooks, QGIS) —
            # never passed to a WhiteboxTools subprocess.
            "predictor": 2,
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
            "BIGTIFF": "YES",
        }

    slope_arr = np.asarray(slope_deg, dtype=np.float64)
    slope_nd_raw = getattr(slope_deg, "no_data", None)
    slope_nd = float(DEM_NODATA if slope_nd_raw is None else slope_nd_raw)

    valid = np.isfinite(fac) & np.isfinite(slope_arr)
    if fac_nd is not None and np.isfinite(fac_nd):
        valid &= fac != fac_nd
    valid &= slope_arr != slope_nd
    # Filter near-vertical slopes — physically impossible (>89°) and arise from
    # closed-basin fill artifacts (a handful of cells per VPU; ~0.0001%).
    valid &= slope_arr < 89.0
    valid &= land_valid

    twi = np.full(fac.shape, DEM_NODATA, dtype=np.float32)
    slope_rad = np.deg2rad(slope_arr[valid])
    twi_valid = np.log(((fac[valid] + 1.0) * 10.0) / (np.tan(slope_rad) + 0.01))
    twi[valid] = twi_valid.astype(np.float32)

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
    fdr_out = vpu_dir / f"Fdr_hydrodem_{args.vpu}.tif"
    fac_out = vpu_dir / f"Fac_hydrodem_{args.vpu}.tif"
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

    runner = _find_whitebox_tools_binary()
    logger.info("WhiteboxTools binary: %s", runner)

    if args.force or not dem_fixed.exists():
        _fix_dem_nodata(dem_src, dem_fixed, logger)
    else:
        logger.info("Reusing fixed DEM (exists): %s", dem_fixed)

    if args.force or not dem_filled.exists():
        logger.info("--- richdem FillDepressions+epsilon (float64) ---")
        _fill_depressions_richdem(dem_fixed, dem_filled, logger)
    else:
        logger.info("Reusing filled DEM (exists): %s", dem_filled)

    if args.force or not fdr_out.exists():
        logger.info("--- WBT D8Pointer (Esri encoding) ---")
        _run_wbt(
            runner, "D8Pointer",
            [f"--dem={dem_filled}", f"--output={fdr_out}", "--esri_pntr"],
            logger,
        )
    else:
        logger.info("Reusing FDR (exists): %s", fdr_out)

    if args.force or not fac_out.exists():
        logger.info("--- WBT D8FlowAccumulation (cells, esri_pntr) ---")
        _run_wbt(
            runner, "D8FlowAccumulation",
            [f"--input={fdr_out}", f"--output={fac_out}",
             "--pntr", "--esri_pntr", "--out_type=cells"],
            logger,
        )
    else:
        logger.info("Reusing FAC (exists): %s", fac_out)

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
    # full-array read is correct and avoids a windowed lookup. The WBT FAC
    # output also lives on this grid, so the mask aligns to it too.
    logger.info("Reading per-VPU HRU land mask: %s", vpu_landmask_path)
    land_valid = read_land_mask(vpu_landmask_path)

    logger.info("--- TWI = log((fac+1)*10 / (tan(slope_rad) + 0.01)), land-masked ---")
    _compute_twi(fac_out, slope_deg, land_valid, twi_out, logger)

    logger.info("compute_dem_derivatives complete for VPU %s", args.vpu)


if __name__ == "__main__":
    main()
