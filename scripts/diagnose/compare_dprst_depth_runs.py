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
    m = old.merge(new, on="COMID", suffixes=("_old", "_new"), how="outer")
    a, b = m["dprst_depth_m_old"], m["dprst_depth_m_new"]
    same = (a == b) | (a.isna() & b.isna())
    newly = a.isna() & b.notna()
    changed = ~same & ~newly
    violations = sorted(int(c) for c in m.loc[~same & m["COMID"].isin(expect_identical), "COMID"])
    return {"n": len(m), "identical": int(same.sum()), "changed": int(changed.sum()),
            "newly_measured": int(newly.sum()), "violations": violations,
            "abs_change_p50_m": float(np.nanmedian((b - a)[changed].abs())) if changed.any() else 0.0}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("baseline")
    p.add_argument("new")
    p.add_argument("--expect-identical")
    a = p.parse_args()
    ids = set(int(x) for x in open(a.expect_identical).read().split()) if a.expect_identical else set()
    r = compare(pd.read_parquet(a.baseline), pd.read_parquet(a.new), ids)
    for k, v in r.items():
        print(f"{k}: {v if k != 'violations' else len(v)}")
    if r["violations"]:
        print("first violations:", r["violations"][:20])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
