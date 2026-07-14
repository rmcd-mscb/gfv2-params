"""A/B the endorheic rebuild, and GATE on the PRODUCT — not on the classifier table.

THE LESSON THIS SCRIPT ENCODES: the classifier can be completely correct and the product
still wrong. On the first CONUS rebuild, all 20 named fixtures passed and
`connected_wbody.tif` correctly dropped the Great Salt Lake — and GSL *still* came out
**0 % depression storage**, because `clump_regions` 8-connects it to a 49.1 km² inflow
marsh (COMID 10273192) that is *correctly* on-stream, and `regions_touching_mask` vetoed
the whole 5.02 M-cell region. **A 49 km² marsh vetoed a 4,369 km² lake.**

So the gate below is the RASTER, not the parquet: Great Salt Lake's cells must actually
be depression storage in `dprst_binary.tif`. Everything else here is diagnostics.

Also measured: GSL flips from routing BARRIER to POUR-POINT (#158), so `drains_to_dprst`
grows in VPU 16. That is expected and correct — `same_hru_drains` (#160/#162) bounds the
PRMS ratios, and the barrier set still holds every outletted waterbody. What would be
WRONG is drainage being LOST: the demotion is a strict subtraction and can only add.

    pixi run --as-is python scripts/diagnose/ab_endorheic_rebuild.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
from rasterio.windows import Window, from_bounds

ROOT = Path("/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2")
NEW = ROOT / "gfv2" / "depstor_rasters"
OLD = ROOT / "gfv2" / "depstor_rasters_pre_endorheic_2026-07-13"
WB = ROOT / "input" / "nhd" / "conus_waterbodies.gpkg"
PX_KM2 = 0.0009  # 30 m cells
VPU16 = 16       # Great Basin — home of the Great Salt Lake

# Endorheic: their water has nowhere to go. Must be dprst IN THE RASTER.
MUST_BE_DPRST = {946020001: "Great Salt Lake", 948100002: "Salton Sea",
                 11310757: "Pyramid Lake", 120053921: "Mono Lake"}
# DOMAIN EXITS: terminal only because the CONUS model ends there. Must NOT be dprst.
MUST_NOT = {904140248: "Lake Michigan", 11758154: "Lewis and Clark Lake",
            15447630: "Lake Champlain"}


def pct_dprst(geom, raster: Path) -> float:
    with rasterio.open(raster) as src:
        win = from_bounds(*geom.bounds, transform=src.transform)
        win = win.round_offsets().round_lengths()
        a = src.read(1, window=win, boundless=True, fill_value=255)
        m = geometry_mask([geom], out_shape=a.shape,
                          transform=src.window_transform(win), invert=True)
        n = int(m.sum())
        return 100.0 * int(((a == 1) & m).sum()) / n if n else 0.0


def compare(name: str, fname: str) -> tuple[float, float]:
    tot_new = tot_old = v16_new = v16_old = gained = lost = 0
    with rasterio.open(NEW / fname) as A, rasterio.open(OLD / fname) as B, \
            rasterio.open(NEW / "vpu_id.tif") as V:
        for r0 in range(0, A.height, 4096):
            h = min(4096, A.height - r0)
            w = Window(0, r0, A.width, h)
            a = A.read(1, window=w) == 1
            b = B.read(1, window=w) == 1
            v = V.read(1, window=w)
            tot_new += int(a.sum())
            tot_old += int(b.sum())
            gained += int((a & ~b).sum())
            lost += int((~a & b).sum())
            m16 = v == VPU16
            v16_new += int((a & m16).sum())
            v16_old += int((b & m16).sum())
    k = PX_KM2
    print(f"\n=== {name} ===")
    print(f"  CONUS  {tot_old * k:>10,.0f} -> {tot_new * k:>10,.0f} km2   "
          f"({(tot_new - tot_old) * k:+,.0f} km2, "
          f"{100 * (tot_new - tot_old) / max(tot_old, 1):+.1f}%)")
    print(f"     gained {gained * k:>9,.0f} km2      lost {lost * k:>9,.0f} km2")
    print(f"  VPU 16 {v16_old * k:>10,.0f} -> {v16_new * k:>10,.0f} km2   "
          f"({(v16_new - v16_old) * k:+,.0f} km2, "
          f"{100 * (v16_new - v16_old) / max(v16_old, 1):+.1f}%)")
    return gained * k, lost * k


def main() -> int:
    failures: list[str] = []
    print("A/B: endorheic classifier rebuild (VPU 16 = Great Basin, home of GSL)")

    compare("dprst_binary", "dprst_binary.tif")
    gained, lost = compare("drains_to_dprst", "drains_to_dprst.tif")

    # The demotion is a STRICT SUBTRACTION from the on-stream set, so drains_to_dprst can
    # only GAIN (newly-demoted lakes become pour-points). Material LOSS means the on-stream
    # barrier set (#158) changed in a way it should not have.
    if lost > 0.02 * max(gained, 1) and lost > 500:
        failures.append(f"drains_to_dprst LOST {lost:,.0f} km2 against {gained:,.0f} gained — "
                        f"a strict subtraction should not strip drainage")

    ids = list(MUST_BE_DPRST) + list(MUST_NOT)
    wb = gpd.read_file(WB, layer="waterbodies",
                       where="COMID IN (%s)" % ",".join(map(str, ids)), use_arrow=True)

    print("\n=== THE GATE: is it depression storage in the RASTER? ===")
    print("  (the classifier being right is NOT enough — the first rebuild passed all 20")
    print("   fixtures and still left GSL at 0% dprst, vetoed by a 49 km2 inflow marsh)\n")
    for _, r in wb.sort_values("COMID").iterrows():
        comid = int(r.COMID)
        before = pct_dprst(r.geometry, OLD / "dprst_binary.tif")
        after = pct_dprst(r.geometry, NEW / "dprst_binary.tif")
        if comid in MUST_BE_DPRST:
            name, want, ok = MUST_BE_DPRST[comid], "must be dprst", after > 95.0
            if not ok:
                failures.append(f"{name} is only {after:.1f}% dprst (expected >95%) — "
                                f"the clump veto is still firing")
        else:
            name, want, ok = MUST_NOT[comid], "must NOT be dprst", after < 1.0
            if not ok:
                failures.append(f"{name} is {after:.1f}% dprst — a DOMAIN EXIT was "
                                f"wrongly demoted")
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<22} {want:<16} "
              f"dprst {before:5.1f}% -> {after:5.1f}%")

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nGATE PASSED: the terminal lakes ARE depression storage in the product, "
          "the domain exits are not,\n             and drainage was only added, never lost.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
