"""Open-source TWI/FDR/FAC/slope/aspect from per-VPU merged Hydrodem.

Library entrypoint for the ``compute_dem_derivatives`` step in the
shared-raster orchestrator (``scripts/build_shared_rasters.py``).
Registered via the BUILDERS dict in ``shared_rasters/__init__.py``;
opt-in (not in the default ``steps:`` list of ``shared_rasters.yml``).

**Status: parallel-artifact pipeline, not the canonical TWI source.** The
canonical CONUS TWI for downstream PRMS parameter extraction (carea_max,
smidx_coef) remains ``Twi_merged_<vpu>.tif`` from the merge_rpu_by_vpu_twi
builder, because the threshold values used in those parameters (8.0 and
15.6) were calibrated against the original ArcPy TWI distribution shape —
swapping the source would invalidate those thresholds. This step exists as
a self-contained open-source recipe that can be re-run on any DEM (future
cross-border work, #53; future PRMS-threshold recalibration, etc.). It is
intentionally **not** in the default ``steps:`` list of
configs/shared_rasters/shared_rasters.yml; users add it explicitly to opt in.

Pipeline (hybrid richdem + WhiteboxTools):
  Hydrodem nodata fix -> richdem FillDepressions+epsilon (Barnes 2014
  priority-flood) -> WhiteboxTools D8Pointer (Esri encoding) -> WhiteboxTools
  D8FlowAccumulation (cells, exclusive of the cell itself) -> richdem
  slope_degrees/slope_percentage/aspect on the FIXED (pre-fill) DEM -> TWI
  = log((fac + 1) * 10 / (tan(min(slope, 60deg)) + 0.01)) -> mask against the
  per-VPU HRU land mask (``land_mask_<vpu>.tif``, built by the
  build_vpu_landmask step). The mask is per-VPU rather than the CONUS
  depstor land_mask because the per-VPU Hydrodem footprint bulges past
  this VPU's HRU boundary into adjacent VPUs; only the per-VPU mask clips
  the TWI to just this VPU's HRUs.

Why cap slope at 60deg: the NHDPlus Hydrodem carries occasional spurious
high-elevation cells (5530 m points in the Adirondacks, etc. — uncleaned
stream-burn sentinels or bridge artefacts). Adjacent to those, computed
slope spikes to ~89deg+. The previous behaviour was a hard ``slope < 89deg``
filter that dropped those cells to nodata, producing a 1-pixel "snake" of
holes along HRU/stream boundaries where the bad cells cluster. The cap
keeps every valid cell and guarantees TWI >= log(10/(tan(60deg)+0.01))
approx 1.75 for fac=0 cells, eliminating negative TWI as a numerical
artefact of the formula at near-vertical slopes.

Why hybrid (richdem fill + WBT D8): a previous all-richdem revision used
``rd.FlowAccumulation(method='D8')`` for routing. That worked on small/mid
VPUs but segfaulted on the giants — VPU 03 (2.31B cells) at "Creating
dependencies array" with 122 GB RAM allocated, and VPU 10 (3.60B cells)
never made it past 1% fill. richdem's internal C++ uses int32 for cell
linear indices, which overflows at 2^31 - 1 = 2.147B cells. WhiteboxTools'
D8FlowAccumulation uses ``size_t`` and routes over arbitrarily large grids
— slower per cell but stable. We still use richdem for the *fill* step
because WBT's ``FillDepressions --fix_flats`` doesn't terminate on
continent-scale flats (VPU 18, California's Central Valley + Mojave, didn't
finish in 4 hours), where richdem's Barnes 2014 priority-flood finishes in
~6 minutes. FAC convention reverts to WBT's: headwater = 0 (exclusive of
self), so the TWI formula uses ``(fac + 1) * 10`` — matching the ArcPy
reference.

Why float64 throughout the fill: richdem's epsilon increments are 1 ULP
per cell. At 1000 m elevation, ULP_float32 ~ 6e-5 m while ULP_float64 ~
1e-13 m — the float64 increment is small enough to be invisibly thin, but
float32 ULPs accumulate fast enough to push deep closed basins above their
bounding rim on continent-scale flats. So we fill in float64 and save in
float64 so WBT D8Pointer / D8FlowAccumulation see the same per-cell ULP
gradient that broke under float32 in earlier revisions.

Outputs (per VPU, written to {data_root}/shared/per_vpu/<vpu>/):
- Hydrodem_merged_fixed_<vpu>.tif  (intermediate, nodata=-9999)
- Hydrodem_filled_<vpu>.tif        (richdem FillDepressions+epsilon, float64)
- Fdr_hydrodem_<vpu>.tif           (WBT D8 pointer, Esri encoding)
- Fac_hydrodem_<vpu>.tif           (WBT D8 flow accumulation, cells)
- Slope_hydrodem_<vpu>.tif         (richdem slope_degrees, on FIXED DEM)
- Slope_pct_hydrodem_<vpu>.tif     (richdem slope_percentage, on FIXED DEM)
- Aspect_hydrodem_<vpu>.tif        (richdem aspect, on FIXED DEM)
- Twi_hydrodem_<vpu>.tif           (log((fac+1)*10/(tan(min(slope,60deg))+0.01)),
                                    per-VPU HRU land mask applied)
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import numpy as np
import rasterio
import richdem as rd
import rioxarray  # noqa: F401  (registers .rio accessor)

from gfv2_params.depstor import read_land_mask

from .context import SharedRastersContext

# Hydrodem_merged_<vpu>.tif declares nodata=-99.99 (centimeters/100, same as
# NEDSnapshot). Re-encode to nodata=-9999 so richdem picks up an unambiguous
# value (a float -99.99 comparison is risky) and the downstream open-source
# pipeline shares one nodata convention.
DEM_SRC_NODATA = -99.99
DEM_NODATA = -9999.0

# Cap slope at 60deg before computing TWI. The Hydrodem carries occasional
# spurious-elevation cells (e.g., uncleaned stream-burn sentinels, bridge
# artefacts) — 5530 m points in the Adirondacks where Mt. Marcy tops out at
# 1629 m — that drive computed slopes to ~89deg+ in adjacent cells. Without
# a cap, those cells produce negative or near-zero TWI clustered along HRU/
# stream boundaries (where the bad cells live), visible as a 1-pixel "snake"
# pattern in QGIS. Capping at 60deg keeps every cell valid and guarantees TWI
# >= log(10/(tan(60deg)+0.01)) approx 1.75 for fac=0 cells. Real terrain rarely
# exceeds 60deg at 30 m resolution (Half Dome ~80deg+ but on a tiny pixel
# fraction). Less defensible than fixing the Hydrodem outliers upstream,
# but tractable.
SLOPE_CAP_DEG = 60.0


def _find_whitebox_tools_binary() -> str:
    """Locate the WhiteboxTools executable inside the bundled `whitebox` package.

    Mirrors the helper in gfv2_params.depstor_builders.routing. Instantiates
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
    """Write TWI = log((fac + 1) * 10 / (tan(min(slope, 60deg)) + 0.01)), masked to land.

    Matches the ArcPy reference scale: fac is FlowAccumulation+1 (WBT's
    ``--out_type=cells`` counts upslope cells exclusive of self, so headwater
    = 0; the +1 includes the cell itself), the 100/10 factor simplifies to
    *10, and the +0.01 constant prevents div-by-zero on flats.

    Slope is clipped to SLOPE_CAP_DEG before taking tan() — see module-level
    constant for rationale (Hydrodem outliers driving near-vertical computed
    slopes; cap eliminates negative TWI as a numerical artefact).

    ``land_valid`` is the boolean per-VPU HRU mask aligned to the FAC grid
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
            # GDAL-based tools only (build_vrt, marimo notebooks, QGIS) —
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
    valid &= land_valid

    # Cap slope at SLOPE_CAP_DEG before the tan() — see module-level constant
    # for rationale. Counts the cap-clipping for the log.
    n_capped = int((valid & (slope_arr > SLOPE_CAP_DEG)).sum())
    slope_capped = np.minimum(slope_arr, SLOPE_CAP_DEG)

    twi = np.full(fac.shape, DEM_NODATA, dtype=np.float32)
    slope_rad = np.deg2rad(slope_capped[valid])
    twi_valid = np.log(((fac[valid] + 1.0) * 10.0) / (np.tan(slope_rad) + 0.01))
    twi[valid] = twi_valid.astype(np.float32)

    twi_out.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(twi_out, "w", **twi_profile) as dst:
        dst.write(twi, 1)
    logger.info(
        "Wrote: %s (%d valid pixels of %d; %d cells dropped by land mask; "
        "%d cells slope-capped at %.0fdeg)",
        twi_out, int(valid.sum()), valid.size, int((~land_valid).sum()),
        n_capped, SLOPE_CAP_DEG,
    )


def _process_vpu(
    vpu: str,
    input_dir: Path,
    output_dir: Path,
    runner: str,
    force: bool,
    logger,
) -> None:
    vpu_dir = output_dir / vpu
    vpu_dir.mkdir(parents=True, exist_ok=True)

    dem_src = input_dir / vpu / f"Hydrodem_merged_{vpu}.tif"
    dem_fixed = vpu_dir / f"Hydrodem_merged_fixed_{vpu}.tif"
    dem_filled = vpu_dir / f"Hydrodem_filled_{vpu}.tif"
    fdr_out = vpu_dir / f"Fdr_hydrodem_{vpu}.tif"
    fac_out = vpu_dir / f"Fac_hydrodem_{vpu}.tif"
    slope_out = vpu_dir / f"Slope_hydrodem_{vpu}.tif"
    slope_pct_out = vpu_dir / f"Slope_pct_hydrodem_{vpu}.tif"
    aspect_out = vpu_dir / f"Aspect_hydrodem_{vpu}.tif"
    twi_out = vpu_dir / f"Twi_hydrodem_{vpu}.tif"
    # Per-VPU HRU land mask aligned to the per-VPU Hydrodem grid (built by
    # the build_vpu_landmask step). The CONUS depstor land_mask.tif is the
    # union of every VPU's HRUs — it leaves cells unmasked where adjacent-VPU
    # HRUs drape into this VPU's Hydrodem bulge. The per-VPU mask is strict.
    vpu_landmask_path = vpu_dir / f"land_mask_{vpu}.tif"

    if not dem_src.exists():
        raise FileNotFoundError(f"Hydrodem source not found: {dem_src}")
    if not vpu_landmask_path.exists():
        raise FileNotFoundError(
            f"Per-VPU land mask not found (run build_vpu_landmask first): {vpu_landmask_path}"
        )

    if not force and twi_out.exists():
        logger.info("[VPU %s] final TWI output exists (use --force to rebuild): %s",
                    vpu, twi_out)
        return

    if force or not dem_fixed.exists():
        _fix_dem_nodata(dem_src, dem_fixed, logger)
    else:
        logger.info("[VPU %s] reusing fixed DEM (exists): %s", vpu, dem_fixed)

    if force or not dem_filled.exists():
        logger.info("[VPU %s] --- richdem FillDepressions+epsilon (float64) ---", vpu)
        _fill_depressions_richdem(dem_fixed, dem_filled, logger)
    else:
        logger.info("[VPU %s] reusing filled DEM (exists): %s", vpu, dem_filled)

    if force or not fdr_out.exists():
        logger.info("[VPU %s] --- WBT D8Pointer (Esri encoding) ---", vpu)
        _run_wbt(
            runner, "D8Pointer",
            [f"--dem={dem_filled}", f"--output={fdr_out}", "--esri_pntr"],
            logger,
        )
    else:
        logger.info("[VPU %s] reusing FDR (exists): %s", vpu, fdr_out)

    if force or not fac_out.exists():
        logger.info("[VPU %s] --- WBT D8FlowAccumulation (cells, esri_pntr) ---", vpu)
        _run_wbt(
            runner, "D8FlowAccumulation",
            [f"--input={fdr_out}", f"--output={fac_out}",
             "--pntr", "--esri_pntr", "--out_type=cells"],
            logger,
        )
    else:
        logger.info("[VPU %s] reusing FAC (exists): %s", vpu, fac_out)

    # Compute slope/aspect from the FIXED (pre-fill) DEM, not the filled DEM.
    # richdem's epsilon=True fill imprints a per-cell ULP increment (~6e-5 m at
    # 1000 m elevation), which on flat-bottomed streams creates a non-trivial
    # synthetic slope and dims TWI on those cells. ArcPy's `Fill` doesn't
    # imprint such a gradient, so its `Slope(DEM_filled)` matches `Slope(unfilled)`
    # on stream cells (which aren't depressions and aren't modified by fill).
    # The fill is still required for proper FAC routing — this just decouples
    # slope from the epsilon artifact.
    logger.info("[VPU %s] loading fixed DEM into richdem for slope/aspect (no_data=%s)...",
                vpu, DEM_NODATA)
    dem_rd = rd.LoadGDAL(str(dem_fixed), no_data=DEM_NODATA)

    logger.info("[VPU %s] --- richdem slope_degrees ---", vpu)
    slope_deg = rd.TerrainAttribute(dem_rd, attrib="slope_degrees")
    rd.SaveGDAL(str(slope_out), slope_deg)
    logger.info("[VPU %s] wrote: %s", vpu, slope_out)

    logger.info("[VPU %s] --- richdem slope_percentage ---", vpu)
    slope_pct = rd.TerrainAttribute(dem_rd, attrib="slope_percentage")
    rd.SaveGDAL(str(slope_pct_out), slope_pct)
    logger.info("[VPU %s] wrote: %s", vpu, slope_pct_out)
    del slope_pct

    logger.info("[VPU %s] --- richdem aspect ---", vpu)
    aspect = rd.TerrainAttribute(dem_rd, attrib="aspect")
    rd.SaveGDAL(str(aspect_out), aspect)
    logger.info("[VPU %s] wrote: %s", vpu, aspect_out)
    del aspect, dem_rd

    # Per-VPU mask and Hydrodem share the same grid by construction (the
    # mask was rasterised onto Hydrodem_merged_<vpu>.tif), so a direct
    # full-array read is correct and avoids a windowed lookup. The WBT FAC
    # output also lives on this grid, so the mask aligns to it too.
    logger.info("[VPU %s] reading per-VPU HRU land mask: %s", vpu, vpu_landmask_path)
    land_valid = read_land_mask(vpu_landmask_path)

    logger.info("[VPU %s] --- TWI = log((fac+1)*10 / (tan(slope_rad) + 0.01)), land-masked ---", vpu)
    _compute_twi(fac_out, slope_deg, land_valid, twi_out, logger)

    logger.info("[VPU %s] compute_dem_derivatives complete", vpu)


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Compute open-source DEM derivatives for every VPU in ``ctx.vpus``.

    Optional/parallel pipeline — not in the default ``steps:`` list of
    configs/shared_rasters/shared_rasters.yml. Users add it explicitly when they want the
    open-source TWI alongside the canonical ArcPy-derived one.

    step_cfg keys (both optional; default to ``ctx.per_vpu_dir``):
      input_dir  — per-VPU Hydrodem source directory
      output_dir — per-VPU derived raster output directory

    Depends on the per-VPU HRU land mask at
    ``{output_dir}/<vpu>/land_mask_<vpu>.tif`` (build_vpu_landmask step).

    Returns an empty dict — per-VPU outputs are not registered in ctx.paths.
    """
    input_dir = Path(step_cfg.get("input_dir", ctx.per_vpu_dir))
    output_dir = Path(step_cfg.get("output_dir", ctx.per_vpu_dir))

    if not ctx.vpus:
        logger.warning("compute_dem_derivatives: ctx.vpus is empty, nothing to do")
        return {}

    runner = _find_whitebox_tools_binary()
    logger.info("WhiteboxTools binary: %s", runner)

    for vpu in ctx.vpus:
        _process_vpu(vpu, input_dir, output_dir, runner, ctx.force, logger)

    return {}
