"""A/B drains_to_dprst on one VPU across three FDR conditionings (#147).

For a single test VPU, warp a chosen flow-direction raster onto the dprst grid
(reusing the routing builder's streaming gdal.Warp) and run the in-process D8
kernel, writing a per-VPU drains_to_dprst raster. With --labels (a labeled
depression raster, e.g. wbody_regions masked to dprst) it also writes per-
depression contributing-area counts using the labeled kernel.

FDR sources (--fdr):
  production : the fabric fdr.vrt (NHDPlus FdrFac, stream-burned + filled)
  fill       : Fdr_hydrodem_<vpu>.tif (richdem fill-all on the same Hydrodem)
  breach     : Fdr_breached_<vpu>.tif (depression-respecting, this work)

The fill source shares its DEM with breach, so production-vs-fill isolates the
DEM/stream-burn difference and fill-vs-breach isolates the conditioning.

Analysis tool, not a pipeline builder (cf. diagnose_drains_to_dprst.py): paths
are passed on the CLI; nothing is registered in the DAG.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window

from gfv2_params.d8_routing import (
    drains_to_dprst_kernel,
    drains_to_dprst_labeled_kernel,
)
from gfv2_params.depstor import (
    RasterInfo,
    mask_fdr_to_vpu,
    read_aligned_uint8,
    vpu_bbox,
    vpu_pour_points,
)
from gfv2_params.depstor_builders.routing import _align_fdr_to_dprst_grid

_FDR_CHOICES = ("production", "fill", "breach")


def resolve_fdr_path(which: str, vpu: str, *, fdr_vrt: Path,
                     per_vpu_dir: Path) -> Path:
    """Map an FDR choice to its on-disk raster for this VPU."""
    if which == "production":
        return fdr_vrt
    if which == "fill":
        return per_vpu_dir / vpu / f"Fdr_hydrodem_{vpu}.tif"
    if which == "breach":
        return per_vpu_dir / vpu / f"Fdr_breached_{vpu}.tif"
    raise ValueError(f"unknown FDR choice {which!r}; expected one of {_FDR_CHOICES}")


def per_depression_counts(labeled: np.ndarray) -> dict[int, int]:
    """Label -> contributing-area cell count (background label 0 dropped)."""
    counts = np.bincount(labeled.ravel().astype(np.int64))
    return {int(lab): int(n) for lab, n in enumerate(counts) if lab > 0 and n > 0}


def _run_one_vpu(fdr_path, dprst_path, vpu_id_path, template_path, vpu_code,
                 labels_path, out_tif, out_csv, logger):
    info = RasterInfo.from_path(template_path)
    fdr_aligned = out_tif.parent / f"_fdr_aligned_{vpu_code}.tif"
    _align_fdr_to_dprst_grid(fdr_path, dprst_path, fdr_aligned, logger)
    try:
        vpu_id = read_aligned_uint8(vpu_id_path, info)
        code = int(vpu_code)
        bbox = vpu_bbox(vpu_id, code)
        if bbox is None:
            raise SystemExit(f"VPU {code} not present in {vpu_id_path}")
        r0, r1, c0, c1 = bbox
        window = Window(c0, r0, c1 - c0, r1 - r0)
        vpu_win = vpu_id[r0:r1, c0:c1]
        with rasterio.open(fdr_aligned) as fsrc, rasterio.open(dprst_path) as dsrc:
            fdr_win = fsrc.read(1, window=window)
            dprst_win = dsrc.read(1, window=window)
        fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
        pour = vpu_pour_points(dprst_win, vpu_win, code)
        # This A/B diagnostic compares FDR variants only; it has no on-stream
        # barrier concept, so pass an all-zero barrier (keeps the binary result
        # comparable to the barrier-free labeled kernel below for the
        # n_labeled == n_drain self-check).
        no_barrier = np.zeros_like(pour)
        drains, n_cycles = drains_to_dprst_kernel(fdr_masked, pour, no_barrier, fdr_nodata=255)
        n_land = int((vpu_win == code).sum())
        n_drain = int((drains[vpu_win == code] == 1).sum())
        logger.info("VPU %d [%s]: %d/%d land cells drain (%.4f); %d cycles",
                    code, out_tif.stem, n_drain, n_land,
                    (n_drain / n_land if n_land else 0.0), n_cycles)
        _write_window_uint8(drains, info, bbox, out_tif)

        if labels_path is not None:
            with rasterio.open(labels_path) as lsrc:
                label_win = lsrc.read(1, window=window).astype(np.int32)
            # Restrict the per-depression attribution to the SAME pour-point set
            # as the binary metric: dprst cells in this VPU. The labels raster
            # (wbody_regions) ids every waterbody incl. on-stream ones; without
            # the dprst mask the labeled kernel would seed on-stream waterbodies
            # too and attribute whole river-corridor catchments to them, so the
            # per-depression sum would not match the dprst-seeded binary drains.
            label_win[(vpu_win != code) | (dprst_win != 1)] = 0
            # This A/B diagnostic has no on-stream barrier concept (see the
            # no_barrier comment above for the binary kernel); pass an
            # all-zero barrier so labeled coverage stays comparable to the
            # barrier-free binary drains count in the n_labeled == n_drain
            # self-check below.
            labeled, _ = drains_to_dprst_labeled_kernel(
                fdr_masked, label_win, np.zeros_like(label_win, dtype=np.uint8),
                fdr_nodata=255)
            counts = per_depression_counts(labeled)
            # Consistency self-check: same pour-points => labeled coverage must
            # equal the binary dprst-drains count. A mismatch means the label
            # mask diverged from the dprst pour-points (the bug this guards).
            n_labeled = int(sum(counts.values()))
            if n_labeled != n_drain:
                logger.warning(
                    "  per-depression sum (%d) != binary drains (%d) for VPU %d "
                    "[%s] — label/dprst pour-point mismatch.",
                    n_labeled, n_drain, code, out_tif.stem,
                )
            with open(out_csv, "w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["depression_label", "contributing_cells"])
                for lab in sorted(counts):
                    w.writerow([lab, counts[lab]])
            logger.info("Wrote per-depression areas (%d depressions): %s",
                        len(counts), out_csv)
    finally:
        if fdr_aligned.exists():
            fdr_aligned.unlink()


def _write_window_uint8(arr, info, bbox, out_tif):
    """Write the per-VPU drains window as a standalone GeoTIFF (nodata=255).

    ``arr`` must already be the VPU-window-sized array (shape ``(r1-r0, c1-c0)``);
    ``bbox`` is used only for the output dimensions and transform offset.
    """
    from rasterio.transform import Affine

    r0, r1, c0, c1 = bbox
    assert arr.shape == (r1 - r0, c1 - c0), (
        f"window array {arr.shape} does not match bbox {(r1 - r0, c1 - c0)}"
    )
    win = np.ascontiguousarray(arr)
    # Offset the template transform to the window's top-left (col0,row0).
    transform = info.transform * Affine.translation(c0, r0)
    profile = {
        "driver": "GTiff", "dtype": "uint8", "nodata": 255,
        "width": c1 - c0, "height": r1 - r0, "count": 1,
        "crs": info.crs, "transform": transform,
        "compress": "lzw", "tiled": True, "blockxsize": 256, "blockysize": 256,
        "BIGTIFF": "YES",
    }
    out_tif.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_tif, "w", **profile) as dst:
        dst.write(win, 1)


def main() -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger("ab_drains_to_dprst")

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--vpu", required=True)
    ap.add_argument("--fdr", required=True, choices=_FDR_CHOICES)
    ap.add_argument("--fdr-vrt", required=True, type=Path,
                    help="production fdr.vrt (also the dprst-grid template)")
    ap.add_argument("--per-vpu-dir", required=True, type=Path)
    ap.add_argument("--dprst", required=True, type=Path)
    ap.add_argument("--vpu-id", required=True, type=Path)
    ap.add_argument("--template", required=True, type=Path,
                    help="dprst-grid template (the fabric fdr.vrt clip)")
    ap.add_argument("--labels", type=Path, default=None,
                    help="optional labeled depression raster for per-depression areas")
    ap.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args()

    fdr_path = resolve_fdr_path(args.fdr, args.vpu, fdr_vrt=args.fdr_vrt,
                                per_vpu_dir=args.per_vpu_dir)
    if not fdr_path.exists():
        raise SystemExit(f"FDR not found: {fdr_path}")
    out_tif = args.out_dir / f"drains_to_dprst_{args.vpu}_{args.fdr}.tif"
    out_csv = args.out_dir / f"per_depression_area_{args.vpu}_{args.fdr}.csv"
    _run_one_vpu(fdr_path, args.dprst, args.vpu_id, args.template, args.vpu,
                 args.labels, out_tif, out_csv, logger)


if __name__ == "__main__":
    main()
