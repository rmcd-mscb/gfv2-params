"""Report which `merged/` products predate the things that produced them (#215).

WHY TWO AXES. A parameter goes stale for two independent reasons, and checking only
one of them misses real drift:

  1. its BUILDER CODE changed since the product was written, or
  2. its SOURCE RASTER changed since the product was written.

Issue #215 originally proposed axis 1 alone. The `aspect` rebuild (#201) showed that is
not enough: `nhm_aspect_params.csv` was stale on 413 of 361,471 gfv2 HRUs by up to 89
degrees, and NEITHER `zonal_runners/zonal.py` nor the `aspect:` config entry had been
touched. What changed was the input -- every aspect source tile was rebuilt on
2026-06-30/07-01 by the COG mosaic work (#149/#151), a month after the product. Axis 1
would have classified that product verified-current.

The same check then found `nhm_elevation_params.csv` (2026-05-29) predating
`elevation.vrt`, and a single-batch diff confirmed drift on ~99.9% of HRUs in both
border and interior batches (median ~0.1 m, max 2.7 m).

WHAT THIS IS NOT. Both axes are mtime/commit-date heuristics that OVER-SELECT, and
axis 1 over-selects badly: a comment-only docstring commit touching a builder trips it
just as hard as a rewrite. In the run that motivated this script, 3 of 6 axis-1 flags
were false positives from a docs commit. So this tool produces a CANDIDATE LIST for a
human to triage by reading what actually changed -- it is not a verdict, and it never
rebuilds anything.

The cheap way to settle a candidate is to re-run its builder over ONE batch against
today's inputs and diff that batch against the on-disk product. That is minutes of
compute and it is decisive.

KEEP THE PRE-REBUILD COPY when you do rebuild. Without it, "we rebuilt it" and "it was
already correct" are indistinguishable afterwards -- which is exactly the ambiguity the
`soil_moist_max` bit-identical result (#211) resolved, and the reason the `aspect`
drift was measurable at all.

    pixi run --as-is python scripts/diagnose/audit_product_staleness.py --fabric gfv2
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import yaml

from gfv2_params import params_index as pi
from gfv2_params.config import load_config

_REPO_ROOT = Path(__file__).resolve().parents[2]

# `prms.builder` is prose, not a path list -- e.g.
# "depstor_builders/dprst_depth.py + dprst_depth/aggregate.py". Pull the module paths
# out of it rather than requiring a second, drift-prone declaration.
_PY_PATH = re.compile(r"[\w/]+\.py")


def builder_paths(builder_field: str | None) -> list[str]:
    """Repo-relative module paths named in a `prms.builder` string.

    Returns them under `src/gfv2_params/`, which is where every builder lives.
    """
    return [f"src/gfv2_params/{m}" for m in _PY_PATH.findall(builder_field or "")]


def parse_vrt_sources(xml_text: str, base: Path) -> list[Path]:
    """Every file a VRT references, resolved against the VRT's own directory.

    A VRT is an XML POINTER, so its own mtime is not evidence about its data in either
    direction: rewriting the XML bumps it without the pixels changing, and replacing the
    tiles underneath leaves it untouched. Resolving through to the referenced files is
    the only way to date what a VRT actually serves.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    out: list[Path] = []
    for el in root.iter():
        if not el.tag.endswith("SourceFilename") or not el.text:
            continue
        # relativeToVRT="1" means relative to the VRT's directory, not the CWD.
        out.append(base / el.text if el.get("relativeToVRT") == "1" else Path(el.text))
    return out


def classify(product, builder, source) -> list[str]:
    """Which axes flag this product. Empty list means no candidate.

    A missing date on either axis is NOT a flag: some params (the depstor ratios,
    ssflux) have no single `source_raster`, and that is a legitimate shape, not an
    unknown. Treating absent as stale would bury the real candidates in noise.
    """
    flags = []
    if builder is not None and product < builder:
        flags.append("CODE newer")
    if source is not None and product < source:
        flags.append("SOURCE newer")
    return flags


def _mdate(path) -> _dt.date | None:
    p = Path(path)
    return _dt.date.fromtimestamp(p.stat().st_mtime) if p.exists() else None


def _last_commit_date(path: str) -> _dt.date | None:
    """Author date of the last commit touching `path`, or None if git has no record."""
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", path],
            capture_output=True, text=True, check=True, cwd=_REPO_ROOT,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    return _dt.date.fromisoformat(out[:10]) if out else None


def _source_date(source_raster: str | None) -> _dt.date | None:
    if not source_raster:
        return None
    p = Path(source_raster)
    if not p.exists():
        return None
    if p.suffix != ".vrt":
        return _mdate(p)
    dates = [d for d in (_mdate(s) for s in parse_vrt_sources(p.read_text(), p.parent)) if d]
    return max(dates) if dates else _mdate(p)


def audit(fabric: str) -> list[dict]:
    cfg = load_config("configs/zonal/zonal_params.yml", fabric=fabric)
    data_root = cfg["data_root"]
    zc = yaml.safe_load((_REPO_ROOT / "configs/zonal/zonal_params.yml").read_text())
    sources = {p["name"]: p.get("source_raster") for p in zc.get("params", [])}

    rows = []
    for d in sorted(pi.load_declared_params(), key=lambda x: x.name):
        product = Path(data_root) / fabric / "params" / "merged" / d.merged_file
        pdate = _mdate(product)
        if pdate is None:
            continue  # not built for this fabric; #215 scopes those out
        bdates = [x for x in (_last_commit_date(b) for b in builder_paths(d.prms.get("builder"))) if x]
        src = sources.get(d.name)
        if src:
            src = src.replace("{data_root}", str(data_root)).replace("{fabric}", fabric)
        rows.append({
            "param": d.name,
            "product": pdate,
            "builder": max(bdates) if bdates else None,
            "source": _source_date(src),
        })
    for r in rows:
        r["flags"] = classify(r["product"], r["builder"], r["source"])
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fabric", required=True)
    args = ap.parse_args()

    rows = audit(args.fabric)
    if not rows:
        print(f"No built products found for fabric '{args.fabric}'.")
        return 0

    print(f"{'param':22s} {'product':>10s} {'builder':>10s} {'source':>10s}  verdict")
    print("-" * 86)
    for r in rows:
        v = " + ".join(r["flags"]) if r["flags"] else "current"
        print(f"{r['param']:22s} {str(r['product']):>10s} {str(r['builder']):>10s} "
              f"{str(r['source']):>10s}  {v}")

    cands = [r for r in rows if r["flags"]]
    print(f"\n{len(cands)} candidate(s) of {len(rows)} product(s):")
    for r in cands:
        print(f"  {r['param']}: {' + '.join(r['flags'])}")
    if cands:
        print("\nThese are CANDIDATES, not verdicts. A comment-only commit touching a")
        print("builder trips the CODE axis exactly as hard as a rewrite. Triage by")
        print("reading what changed, then settle it by re-running one batch and diffing")
        print("against the on-disk product. Keep the pre-rebuild copy.")
    # Always 0: this is a report, not a gate. Exiting non-zero would make it
    # unusable in the ad-hoc way it is meant to be run.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
