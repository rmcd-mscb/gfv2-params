"""Compare the source-staged `nhd_waterbodies.parquet` against the hand-made
`conus_waterbodies.gpkg` (layer `waterbodies`) it is meant to reproduce.

This is the acceptance check for `gfv2_params.download.nhd_waterbodies`: the
CONUS product is validated against the existing 448,124-row hand-made layer, so
before `waterbody_gpkg`/`waterbody_layer` can ever be repointed at the staged
parquet, the two must be shown to (essentially) agree. Reports, on both sides:

* row count and unique-COMID count
* COMIDs present in only one side
* FTYPE distribution (the hand-made layer has exactly 66,488 SwampMarsh rows)
* total area (km2)
* for COMIDs present on both sides: max abs `area_sqkm` difference, and how many
  geometries differ beyond a small area-based tolerance

Prints a report; does not raise or gate anything (an analysis tool, not a
pipeline builder or a test). Any material difference is a FINDING to report,
not a bug to silently reconcile.

    pixi run --as-is python scripts/diagnose/verify_nhd_waterbodies.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

DEFAULT_STAGED = Path(
    "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/input/nhd/nhd_waterbodies.parquet"
)
DEFAULT_EXISTING = Path(
    "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2/input/nhd/conus_waterbodies.gpkg"
)
EXISTING_LAYER = "waterbodies"


def _summary(name: str, gdf: gpd.GeoDataFrame) -> None:
    n = len(gdf)
    n_unique = gdf["COMID"].nunique()
    total_area = gdf["area_sqkm"].sum()
    print(f"\n=== {name} ===")
    print(f"  rows           : {n:,}")
    print(f"  unique COMIDs  : {n_unique:,}")
    if n != n_unique:
        print(f"  ** {n - n_unique:,} duplicate COMID row(s) **")
    print(f"  total area     : {total_area:,.1f} km2")
    print("  FTYPE distribution:")
    for ftype, count in gdf["FTYPE"].value_counts().items():
        print(f"    {ftype:<20} {count:>8,}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--staged", type=Path, default=DEFAULT_STAGED)
    p.add_argument("--existing-gpkg", type=Path, default=DEFAULT_EXISTING)
    p.add_argument("--existing-layer", default=EXISTING_LAYER)
    args = p.parse_args()

    print(f"Staged  : {args.staged}")
    print(f"Existing: {args.existing_gpkg} (layer={args.existing_layer})")

    staged = gpd.read_parquet(args.staged)
    existing = gpd.read_file(args.existing_gpkg, layer=args.existing_layer, use_arrow=True)
    existing["COMID"] = existing["COMID"].astype("int64")
    staged["COMID"] = staged["COMID"].astype("int64")

    _summary("STAGED (source-derived)", staged)
    _summary("EXISTING (hand-made conus_waterbodies.gpkg)", existing)

    staged_comids = set(staged["COMID"])
    existing_comids = set(existing["COMID"])
    only_staged = staged_comids - existing_comids
    only_existing = existing_comids - staged_comids
    shared = staged_comids & existing_comids

    print("\n=== COMID set comparison ===")
    print(f"  shared COMIDs        : {len(shared):,}")
    print(f"  only in STAGED       : {len(only_staged):,}")
    print(f"  only in EXISTING     : {len(only_existing):,}")
    if only_staged:
        print(f"    e.g. {sorted(only_staged)[:10]}")
    if only_existing:
        print(f"    e.g. {sorted(only_existing)[:10]}")

    if shared:
        # Both sides carry the same 217 duplicate-COMID rows (NHDPlus ships a
        # boundary-straddling waterbody identically into two adjacent VPU/DA
        # archives, and neither this module nor the existing hand-made layer
        # dedupes them -- see nhd_waterbodies.py's module docstring). Drop to
        # one row per COMID before the join: a duplicate's two rows are
        # identical copies, so keeping either is equivalent for this
        # comparison, and set_index on a non-unique key would otherwise
        # silently scramble the merge below.
        s_df = staged.drop_duplicates(subset="COMID", keep="first")
        e_df = existing.drop_duplicates(subset="COMID", keep="first")
        s = s_df.set_index("COMID").loc[list(shared), ["area_sqkm", "geometry"]]
        e = e_df.set_index("COMID").loc[list(shared), ["area_sqkm", "geometry"]]
        diff = (s["area_sqkm"] - e["area_sqkm"]).abs()
        print("\n=== shared-COMID area_sqkm comparison ===")
        print(f"  max abs difference   : {diff.max():.6f} km2")
        print(f"  mean abs difference  : {diff.mean():.6f} km2")
        n_over_tol = int((diff > 0.001).sum())
        print(f"  rows differing > 0.001 km2: {n_over_tol:,}")

        print("\n=== shared-COMID geometry comparison (symmetric difference) ===")
        # Symmetric-difference area as a fraction of the staged polygon's own
        # area -- robust to floating point / vertex-order noise, sensitive to a
        # genuinely different polygon. `make_valid` first: a handful of source
        # polygons are self-intersecting (GEOS "side location conflict"), which
        # would otherwise crash the whole (vectorized) comparison over one bad
        # row. If it still fails after repair, fall back to a per-row pass so
        # one bad geometry can't blank out this whole section.
        s_geom = s.geometry.make_valid()
        e_geom = e.geometry.make_valid()
        try:
            sym_diff_area = s_geom.symmetric_difference(e_geom).area
            n_failed = 0
        except Exception:
            sym_diff_area = pd.Series(index=s.index, dtype="float64")
            n_failed = 0
            for comid in s.index:
                try:
                    sym_diff_area[comid] = s_geom[comid].symmetric_difference(e_geom[comid]).area
                except Exception:
                    n_failed += 1
                    sym_diff_area[comid] = float("nan")
        frac = sym_diff_area / s.geometry.area.replace(0, pd.NA)
        n_differ = int((frac.fillna(0) > 0.01).sum())
        print(f"  geometries differing by >1% of area: {n_differ:,} of {len(shared):,}")
        if n_failed:
            print(f"  ({n_failed:,} row(s) failed the symmetric-difference op entirely -- skipped)")
        if n_differ:
            worst = frac.fillna(0).sort_values(ascending=False).head(10)
            print(f"    worst offenders (COMID: frac): {worst.to_dict()}")

    print("\n=== done ===")


if __name__ == "__main__":
    main()
