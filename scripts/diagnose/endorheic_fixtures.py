"""Assert the 20 named fixtures the endorheic classifier exists to get right.

Ten waterbodies must become depression storage. Ten must stay on-stream — and every
one of those ten is a DOMAIN EXIT: terminal only because the CONUS model ends there,
not because its basin is closed. They are what broke every attribute-based guard tried
during design (a fabric-network "segments enter but none leave" rule flagged Lake
Michigan; NHDPlus `LandSea` was useless; the WBD's frontal type missed Lake Champlain).

The hydrology-first rule gets all twenty right with no guard at all, because it asks
the only question that matters: does this waterbody's water terminate INSIDE ITSELF?

    Great Salt Lake's water ends in Great Salt Lake      -> frac_own = 1.000 -> dprst
    Lewis and Clark Lake's water ends in the Gulf        -> frac_own = 0.007 -> on-stream
    A pond upstream in GSL's basin ends in GSL, not itself              -> on-stream

Run AFTER the `endorheic` step:
    pixi run --as-is python scripts/diagnose/endorheic_fixtures.py --fabric gfv2
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from gfv2_params.config import load_config

# Genuinely endorheic — their water has nowhere to go.
MUST_BE_DPRST = {
    946020001: "Great Salt Lake",
    948100002: "Salton Sea",
    11310757: "Pyramid Lake",
    120053921: "Mono Lake",
    10734232: "Walker Lake",
    24040174: "Lake Abert",
    24032426: "Summer Lake",
    20296729: "Honey Lake",
    120052284: "Goose Lake",
    120052521: "Devils Lake",
}

# DOMAIN EXITS — terminal only because the CONUS model ends there. Demoting any of
# these would be a catastrophe (Lake Michigan is not a pothole).
MUST_STAY_ONSTREAM = {
    904140248: "Lake Michigan",
    904140243: "Lake Superior",
    904140244: "Lake Huron",
    904140245: "Lake Erie",
    904140246: "Lake Ontario",
    15447630: "Lake Champlain",       # drains north to the St. Lawrence
    120052195: "Lake of the Woods",   # drains north to Hudson Bay
    22762810: "Lake Borgne",          # coastal Louisiana
    120055431: "Everglades SwampMarsh",
    11758154: "Lewis and Clark Lake",  # Missouri mainstem reservoir (Gavins Point Dam)
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fabric", required=True)
    args = ap.parse_args()

    cfg = load_config("configs/depstor/depstor_rasters.yml", fabric=args.fabric)
    path = Path(cfg["output_dir"]) / "endorheic_waterbody_comids.parquet"
    if not path.exists():
        print(f"ERROR: {path} not found — run the `endorheic` step first.")
        return 1

    df = pd.read_parquet(path)
    # `df` carries every Signal-A-EVALUATED candidate (flagged or not), not just the
    # demotions -- see gfv2_params.endorheic.endorheic_frame. A demotion is a row
    # flagged by at least one signal; `demoted` must apply that filter rather than
    # treat every persisted row as endorheic.
    flagged = df["by_terminus"] | df["by_closed_huc12"]
    demoted = set(df.loc[flagged, "comid"].astype(int))
    print(f"{path}")
    print(f"  {len(demoted):,} endorheic COMIDs "
          f"({int(df.by_terminus.sum()):,} by terminus, "
          f"{int(df.by_closed_huc12.sum()):,} by closed basin)")
    print(f"  {len(df):,} total Signal-A-evaluated candidates persisted "
          f"(flagged + unflagged) — this is the population the threshold sweep "
          f"below runs over\n")

    failures: list[str] = []
    for comid, name in MUST_BE_DPRST.items():
        ok = comid in demoted
        frac = df.loc[df.comid == comid, "frac_own"]
        f = f"{frac.iloc[0]:.3f}" if len(frac) else "  -  "
        print(f"  {'PASS' if ok else 'FAIL'}  dprst      frac_own={f}  {name} ({comid})")
        if not ok:
            failures.append(f"{name} is endorheic but was NOT demoted")

    print()
    for comid, name in MUST_STAY_ONSTREAM.items():
        ok = comid not in demoted
        print(f"  {'PASS' if ok else 'FAIL'}  on-stream         {name} ({comid})")
        if not ok:
            failures.append(f"{name} is a DOMAIN EXIT and must NOT be demoted")

    # The 0.5 threshold must remain inert — that is what makes it a physical fact
    # rather than a tuned knob. Design measured a ~3% swing across 0.3-0.7, over the
    # FULL evaluated-candidate population (6,427 candidates; 6,298 at frac_own >=
    # 0.95; only 10 in the whole 0.45-0.55 band) — NOT just the rows already flagged
    # at 0.5, which would structurally hide any candidate sitting between 0.3 and 0.5
    # (never written under the old flagged-only table) and make this gate unable to
    # detect the one failure mode it exists to catch.
    counts = {t: int((df["frac_own"] > t).sum()) for t in (0.3, 0.5, 0.7)}
    swing = (counts[0.3] - counts[0.7]) / max(counts[0.5], 1)
    print(f"\nthreshold sweep (Signal A): {counts}   swing = {swing:.1%}")
    if swing > 0.10:
        failures.append(
            f"frac_own is no longer bimodal (swing {swing:.1%} across 0.3-0.7; design "
            f"measured ~3%). The 0.5 threshold has become load-bearing — re-examine the rule."
        )

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll 20 named fixtures pass; threshold is inert.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
