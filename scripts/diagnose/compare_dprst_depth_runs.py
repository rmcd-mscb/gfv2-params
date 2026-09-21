"""Compare a baseline and a re-run `dprst_depth_polygons.parquet` (issue #223 re-run gate).

usage: compare_dprst_depth_runs.py BASELINE.parquet NEW.parquet [--expect-identical COMIDS.txt]

`--expect-identical` lists COMIDs whose source did not change and whose window
sat wholly inside its tile in the baseline: their depth must match bit for bit
(the output-identity guarantee). Any mismatch there is a violation and exits 1.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd


def compare(old: pd.DataFrame, new: pd.DataFrame, expect_identical: set[int]) -> dict:
    """Classify every COMID in the outer join of `old`/`new` by depth: `identical`
    (same value, or NaN on both sides), `dropped` (was measured in `old`, absent
    from `new` entirely), `newly_measured` (absent from `old`, present in `new` --
    see the note below), or `changed` (present, differing, on both sides).

    `dropped` used to be silently folded into `changed`: a `dropped` row has
    `b = NaN`, so `(b - a)` there is NaN too, and `np.nanmedian` drops it from
    `abs_change_p50_m` -- so a re-run that LOSES polygons reported a falsely SMALL
    median change while inflating the `changed` count with rows that were never
    actually a same-COMID depth comparison. `changed` now excludes `dropped` rows,
    so the median reflects only genuine value changes, and callers can tell
    "changed" apart from "gone" (`--expect-identical` below does).

    `newly_measured` is effectively dead against a `dprst_depth_polygons.parquet`
    pair specifically (both are POST-FILL products -- `fill.fill_flat` guarantees
    no NaN depth survives -- so `a.isna()` for a COMID that exists in `old` never
    happens): it only fires for a COMID that is not in `old` AT ALL, i.e. a
    genuinely new polygon in the re-run, not a flat->measured transition. Kept
    under its original name for backward compatibility with callers of `compare`;
    this docstring is the "comment" half of issue #223 review round 2's finding 7
    (re-bucket or comment -- renaming risked breaking an existing caller for no
    behavioural gain).
    """
    m = old.merge(new, on="COMID", suffixes=("_old", "_new"), how="outer")
    a, b = m["dprst_depth_m_old"], m["dprst_depth_m_new"]
    same = (a == b) | (a.isna() & b.isna())
    dropped = a.notna() & b.isna()
    newly = a.isna() & b.notna()
    changed = ~same & ~dropped & ~newly
    is_expected = m["COMID"].isin(expect_identical)
    # Distinguish "changed but expected identical" from "gone but expected
    # identical" -- both are violations of the output-identity guarantee, but a
    # re-run operator needs to know which failure mode they're looking at.
    violations_changed = sorted(int(c) for c in m.loc[changed & is_expected, "COMID"])
    violations_dropped = sorted(int(c) for c in m.loc[dropped & is_expected, "COMID"])
    return {
        "n": len(m), "identical": int(same.sum()), "changed": int(changed.sum()),
        "dropped": int(dropped.sum()), "newly_measured": int(newly.sum()),
        "violations": violations_changed + violations_dropped,
        "violations_changed": violations_changed, "violations_dropped": violations_dropped,
        "abs_change_p50_m": float(np.nanmedian((b - a)[changed].abs())) if changed.any() else 0.0,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("baseline")
    p.add_argument("new")
    p.add_argument("--expect-identical")
    a = p.parse_args()
    ids = set(int(x) for x in open(a.expect_identical).read().split()) if a.expect_identical else set()
    r = compare(pd.read_parquet(a.baseline), pd.read_parquet(a.new), ids)
    for k, v in r.items():
        if k in ("violations", "violations_changed", "violations_dropped"):
            print(f"{k}: {len(v)}")
        else:
            print(f"{k}: {v}")
    if r["violations_changed"]:
        print("first violations (changed):", r["violations_changed"][:20])
    if r["violations_dropped"]:
        print("first violations (dropped from the new run):", r["violations_dropped"][:20])
    if r["violations"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
