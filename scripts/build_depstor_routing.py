"""Build the drains-to-depression binary raster via WhiteboxTools Watershed.

Uses the staged D8 flow-direction raster (Esri pointer encoding) and the
dprst_binary.tif as pour points to delineate, for every cell, which depression
(if any) it drains into. The resulting per-depression labels are then collapsed
to a uint8 binary raster where 1 = drains to ANY depression, 255 = does not /
nodata.

Outputs:
- {fabric}/depstor_rasters/drains_to_dprst.tif (uint8 binary)

Logic source: depstor/scripts/DepStor.py:413-449 (whitebox_run) and 704-739
(getHruSro_to_dprst).
"""

import argparse
import os
import subprocess
import time
from pathlib import Path

import numpy as np
import rasterio
import rioxarray  # noqa: F401  (registers .rio accessor)
import xarray as xr

from gfv2_params.config import load_config
from gfv2_params.depstor import RasterInfo, write_uint8_binary
from gfv2_params.log import configure_logging


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _find_whitebox_tools_binary() -> str:
    """Locate the WhiteboxTools executable inside the bundled `whitebox` package."""
    import whitebox  # local import — keep optional unless this step runs

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


def _reproject_fdr_with_rioxarray(fdr_path: Path, dprst_path: Path, out_path: Path, logger) -> None:
    """Reproject FDR onto the dprst grid using rioxarray.reproject_match."""
    logger.info("  Reprojecting FDR to match dprst grid (rioxarray.reproject_match)...")
    fdr_da = xr.open_dataarray(fdr_path, engine="rasterio").squeeze("band", drop=True)
    dprst_da = xr.open_dataarray(dprst_path, engine="rasterio").squeeze("band", drop=True)
    fdr_aligned = fdr_da.rio.reproject_match(dprst_da)
    fdr_aligned = fdr_aligned.rio.write_nodata(np.uint8(255))
    # xarray >= 2023 refuses to encode if _FillValue is in both attrs and
    # encoding. reproject_match preserves it in attrs from the source raster.
    fdr_aligned.attrs.pop("_FillValue", None)
    fdr_aligned.rio.to_raster(
        out_path,
        driver="GTiff",
        compress="LZW",
        tiled=True,
        blockxsize=256,
        blockysize=256,
        dtype="uint8",
        nodata=np.uint8(255),
        BIGTIFF="YES",
    )


def _prepare_pour_points(dprst_path: Path, out_path: Path, logger) -> None:
    """Convert dprst_binary.tif (uint8: 1=pour, 255=nodata) into a 0/1 raster
    with nodata=0 for WhiteboxTools.

    WBT's Watershed tool reads the raw raster values and treats every non-zero
    value as a pour-point — it does not consult the GeoTIFF NoData tag for the
    pour-points input. The 255 (nodata) cells in dprst_binary.tif therefore
    leak in as 76,878 watershed-1 cells + 811M watershed-255 cells, which
    collapses the resulting binary to 100% coverage.
    """
    logger.info("  Converting dprst_binary.tif (1/255) -> 0/1 pour-points (nodata=0)...")
    with rasterio.open(dprst_path) as src:
        data = src.read(1)
        profile = src.profile.copy()
    pour = np.where(data == 1, np.uint8(1), np.uint8(0))
    profile.update(nodata=0, compress="LZW")
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(pour, 1)


def _run_whitebox_watershed(fdr_path: Path, pour_pts_path: Path, output_path: Path, logger) -> None:
    runner = _find_whitebox_tools_binary()
    logger.info("  WhiteboxTools binary: %s", runner)
    cmd = [
        runner,
        f"--wd={os.getcwd()}",
        "--max_procs=-1",
        "-r=Watershed",
        f"--d8_pntr={fdr_path}",
        f"--pour_pts={pour_pts_path}",
        f"--output={output_path}",
        "--esri_pntr",
        "-v",
    ]
    logger.info("  Running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.stdout:
        logger.debug("WBT stdout:\n%s", proc.stdout)
    if proc.returncode != 0:
        logger.error("WBT stderr:\n%s", proc.stderr)
        raise RuntimeError(
            f"WhiteboxTools Watershed failed (exit code {proc.returncode}). See stderr above."
        )


def _watershed_to_binary(watershed_path: Path, info: RasterInfo, out_path: Path, logger) -> None:
    """Collapse the per-depression watershed labels into a uint8 binary mask.

    Rule: any cell with a value not equal to the source nodata is "drains to a
    depression" (1); all others become 255 (nodata).
    """
    with rasterio.open(watershed_path) as src:
        data = src.read(1)
        src_nodata = src.nodata
    if src_nodata is None:
        # WhiteboxTools typically writes a recognised nodata; if missing,
        # treat <= 0 as nodata.
        valid = data > 0
    elif isinstance(src_nodata, float) and np.isnan(src_nodata):
        valid = ~np.isnan(data)
    else:
        valid = data != src_nodata
    binary = np.where(valid, np.uint8(1), np.uint8(255))
    n_in = int((binary == 1).sum())
    pct_drains = 100 * n_in / binary.size
    # Sanity check: most real drainage networks route <1% of cells into
    # depressions (most flow paths terminate at streams or the basin outlet).
    # Coverage > 50% almost certainly means pour-points were mis-encoded —
    # see the WBT pour-points nodata bug fixed in PR #56 (the symptom was
    # 100% coverage with `ExitCode=0` and an otherwise-valid GeoTIFF).
    if pct_drains > 50:
        logger.warning(
            "Drains-to-dprst coverage is %.2f%% of the grid — unusually high. "
            "Check that the pour-points raster uses nodata=0 (not 255) and "
            "that the FDR is correctly aligned to the dprst grid.",
            pct_drains,
        )
    write_uint8_binary(binary, info, out_path)
    logger.info(
        "  Drains-to-dprst mask written: %s (%d cells, %.4f%% of grid)",
        out_path, n_in, pct_drains,
    )


def main():
    parser = argparse.ArgumentParser(description="Build depstor drains_to_dprst.tif via WhiteboxTools Watershed.")
    parser.add_argument("--config", required=True, help="Path to depstor_routing_raster.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    logger = configure_logging("build_depstor_routing")
    t_start = time.time()

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )

    template_path = Path(config["template_raster"])
    fdr_path = Path(config["fdr_raster"])
    dprst_path = Path(config["dprst_raster"])
    output_path = Path(config["output_raster"])
    keep_intermediates = bool(config.get("keep_intermediates", False))

    for p in (template_path, fdr_path, dprst_path):
        if not p.exists():
            raise FileNotFoundError(f"Required input not found: {p}")

    logger.info("=== build_depstor_routing ===")
    logger.info("Template : %s", template_path)
    logger.info("FDR      : %s", fdr_path)
    logger.info("Dprst    : %s", dprst_path)
    logger.info("Output   : %s", output_path)

    if output_path.exists() and not args.force:
        logger.info("Output exists — skipping (pass --force to rebuild)")
        return

    info = RasterInfo.from_path(template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fdr_aligned = output_path.parent / "fdr_aligned.tif"
    pour_pts = output_path.parent / "dprst_pourpts.tif"
    watershed_raw = output_path.parent / "hru_to_dprst_labels.tif"

    try:
        logger.info("--- Step 1/4: Align FDR to template grid ---")
        t1 = time.time()
        _reproject_fdr_with_rioxarray(fdr_path, dprst_path, fdr_aligned, logger)
        logger.info("  FDR aligned in %s: %s", _elapsed(t1), fdr_aligned)

        logger.info("--- Step 2/4: Prepare pour-points for WhiteboxTools ---")
        _prepare_pour_points(dprst_path, pour_pts, logger)

        logger.info("--- Step 3/4: Run WhiteboxTools Watershed ---")
        t2 = time.time()
        _run_whitebox_watershed(fdr_aligned, pour_pts, watershed_raw, logger)
        logger.info("  Watershed done in %s: %s", _elapsed(t2), watershed_raw)

        logger.info("--- Step 4/4: Collapse labels to uint8 binary ---")
        t3 = time.time()
        _watershed_to_binary(watershed_raw, info, output_path, logger)
        logger.info("  Binary mask written in %s", _elapsed(t3))
    finally:
        if not keep_intermediates:
            for p in (fdr_aligned, pour_pts, watershed_raw):
                if p.exists():
                    p.unlink()
                    logger.debug("  Cleaned up: %s", p)

    logger.info("=== build_depstor_routing complete in %s ===", _elapsed(t_start))


if __name__ == "__main__":
    main()
