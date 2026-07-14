"""Measure what a GLOBAL per-cell on-stream carve would recover, over the shipped
endorheic exemption — i.e. put a number on the design `dprst.py` deliberately rejected.

THE DESIGN QUESTION THIS ANSWERS. `dprst.py` exempts a waterbody's cells from the
region-level on-stream exclusion only where it has DIRECT hydrologic evidence:

    endorheic_wbody == 1  AND  connected_wbody != 1  AND  wbody_binary == 1

The obvious "simplification" is to drop the first term, exempting every not-on-stream
waterbody cell whose clump merely happens to touch an on-stream feature:

    connected_wbody != 1  AND  wbody_binary == 1          <-- the GLOBAL carve

That was considered and REJECTED: it recovers waterbodies for which no signal ever
produced evidence, on the strength of the clump proxy alone, and those must keep the
unexempted clump behaviour exactly (see `drains_to_dprst` over-extension #145/#158/#161).
This script measures the delta so the rejection rests on a reproducible number rather
than a remembered one. The figure it prints is the "~8,471 km²" quoted in CLAUDE.md,
docs/ARCHITECTURE.md, slurm_batch/HPC_REFERENCE.md and `dprst.py`.

Streams in row strips: the CONUS grid is ~16.9e9 cells, so a full-grid array of any of
these rasters is ~17 GB and four of them will not co-reside.

    pixi run --as-is python scripts/diagnose/measure_global_carve.py --fabric gfv2
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window

from gfv2_params.config import load_config

STRIP_ROWS = 2048


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fabric", required=True)
    args = ap.parse_args()

    cfg = load_config("configs/depstor/depstor_rasters.yml", fabric=args.fabric)
    out = Path(cfg["output_dir"])
    paths = {
        n: out / f"{n}.tif"
        for n in (
            "wbody_binary", "connected_wbody", "endorheic_wbody", "dprst_binary",
            # The exemption runs BEFORE the impervious carve and the land mask, so a
            # cell a global carve "recovers" is still dropped if it is impervious or
            # off-land. Without these two terms the delta is overcounted (by ~2,000 km2
            # on CONUS) with cells no carve would ever have kept.
            "imperv_binary", "land_mask",
        )
    }
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        print("ERROR: missing raster(s):\n  " + "\n  ".join(missing))
        return 1

    srcs = {n: rasterio.open(p) for n, p in paths.items()}
    ref = srcs["dprst_binary"]
    cell_km2 = abs(ref.transform.a * ref.transform.e) / 1e6

    n_exempted = 0     # cells the SHIPPED exemption recovered (endorheic evidence)
    n_global_only = 0  # cells ONLY a global carve would recover (no endorheic evidence)

    for row in range(0, ref.height, STRIP_ROWS):
        h = min(STRIP_ROWS, ref.height - row)
        win = Window(0, row, ref.width, h)
        wb = srcs["wbody_binary"].read(1, window=win)
        con = srcs["connected_wbody"].read(1, window=win)
        end = srcs["endorheic_wbody"].read(1, window=win)
        dpr = srcs["dprst_binary"].read(1, window=win)
        imp = srcs["imperv_binary"].read(1, window=win)
        land = srcs["land_mask"].read(1, window=win)

        # The cells a global carve targets: a waterbody cell that is not itself
        # on-stream, and that would SURVIVE the impervious carve and land mask that run
        # after the exemption. Split by whether the classifier produced evidence for it.
        carve = (wb == 1) & (con != 1) & (imp != 1) & (land == 1)
        n_exempted += int(np.count_nonzero(carve & (end == 1) & (dpr == 1)))
        # No endorheic evidence, and NOT currently dprst -> only the clump veto is
        # keeping it out, so only a global carve would recover it.
        n_global_only += int(np.count_nonzero(carve & (end != 1) & (dpr != 1)))

    for s in srcs.values():
        s.close()

    print(f"fabric: {args.fabric}   cell: {cell_km2 * 1e6:.0f} m2")
    print(
        f"\n  recovered by the SHIPPED exemption (direct endorheic evidence):\n"
        f"    {n_exempted:>14,} cells   {n_exempted * cell_km2:>12,.0f} km2"
    )
    print(
        f"\n  additionally recovered by a GLOBAL per-cell carve (NO evidence —\n"
        f"  clump merely abuts an on-stream feature; these must stay excluded):\n"
        f"    {n_global_only:>14,} cells   {n_global_only * cell_km2:>12,.0f} km2"
    )
    print(
        "\nThe second number is the cost of dropping `endorheic_wbody == 1` from the\n"
        "exemption. It is the figure quoted in CLAUDE.md / ARCHITECTURE.md / dprst.py."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
