"""Per-VPU drains_to_dprst coverage diagnostic.

Reports, per VPU, the fraction of land cells flagged drains_to_dprst==1. Run
before and after a dprst rebuild: humid open-drainage VPUs (Lower Miss,
S. Atl-Gulf, Great Lakes) should drop sharply while endorheic VPUs (Great Basin,
Rio Grande) stay ~flat. Windowed read to stay within memory at CONUS scale.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window


def vpu_coverage(
    drains: np.ndarray, vpu_id: np.ndarray, land: np.ndarray
) -> dict[int, float]:
    """Fraction of land cells where drains==1, per VPU code (excludes code 0)."""
    out: dict[int, float] = {}
    for code in np.unique(vpu_id):
        if code == 0:
            continue
        sel = (vpu_id == code) & land
        n = int(sel.sum())
        if n == 0:
            continue
        out[int(code)] = float((drains[sel] == 1).sum()) / n
    return out


def _accumulate(drains_path: Path, vpu_path: Path, land_path: Path,
                strip: int = 2048) -> dict[int, float]:
    """Stream the CONUS rasters in row-strips; sum land + draining cells per VPU."""
    num: dict[int, int] = {}  # land cells with drains==1
    den: dict[int, int] = {}  # land cells total
    with rasterio.open(drains_path) as d, rasterio.open(vpu_path) as v, \
         rasterio.open(land_path) as lm:
        h, w = d.height, d.width
        for r0 in range(0, h, strip):
            hh = min(strip, h - r0)
            win = Window(0, r0, w, hh)
            dr = d.read(1, window=win)
            vp = v.read(1, window=win)
            ld = lm.read(1, window=win) == 1
            for code in np.unique(vp):
                if code == 0:
                    continue
                sel = (vp == code) & ld
                den[int(code)] = den.get(int(code), 0) + int(sel.sum())
                num[int(code)] = num.get(int(code), 0) + int((dr[sel] == 1).sum())
    return {c: num[c] / den[c] for c in den if den[c]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--drains", required=True, type=Path)
    ap.add_argument("--vpu-id", required=True, type=Path)
    ap.add_argument("--land", required=True, type=Path)
    args = ap.parse_args()
    cov = _accumulate(args.drains, args.vpu_id, args.land)
    print("VPU  drains_to_dprst coverage (fraction of land)")
    for code in sorted(cov):
        print(f"{code:>3}  {cov[code]:.4f}")


if __name__ == "__main__":
    main()
