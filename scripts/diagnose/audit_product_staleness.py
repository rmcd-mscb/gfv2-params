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
`elevation.vrt`. The full rebuild confirmed it on every fabric -- gfv2 99.9% of HRUs
(median 0.14 m, max 22.6 m), oregon 99.9% (median 0.46 m), tjc 99.8%.

Note what oregon proves: its product was built 2026-07-25, AFTER every elevation tile
(<= 2026-07-01), and it was still drifted. A VRT rewrite alone moved it. `_source_date`
therefore takes the LATER of a VRT's own mtime and its newest tile -- see its docstring
for why taking only the tiles was a false-negative bug.

DATES ARE DERIVATION DATES, NOT FILE MTIMES. `merged/<name>.csv` is rewritten in
place by the gap-fill sweep, so its mtime is the last WRITE and is too recent for
anything filled since it was built. The audit uses the per-batch CSVs under
`{output_dir}/{param}/` instead -- see `derivation_date`, and the oregon lulc case
that made it necessary.

THIRD AXIS: CASCADES. A product is also stale when something it CONSUMES is stale,
even if the product itself is far newer -- `ssflux` was derived 2026-08-10 from a
`slope` product whose own derivation was 2026-05-29. The dependency is derived from
the config (`ssflux.merged_slope_file` names slope's merged_file), not separately
declared, so it cannot drift from what the builder reads. Raster-DAG cascades
(the depstor chain) are NOT covered -- see #217.

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

    Needed because replacing the tiles under a VRT leaves the VRT's own mtime
    untouched, so the XML alone under-reports. See `_source_date` for why the
    converse also holds and both dates are taken.
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


def derivation_date(per_batch_dir: Path, merged_date):
    """When the product was DERIVED, not when it was last written.

    `merge_and_fill_params.write_filled_in_place` rewrites `merged/<name>.csv` IN
    PLACE, so a gap-fill bumps the product's mtime without re-running the builder.
    Using the merged file's mtime therefore reports a too-recent date for anything
    that has been filled since it was built -- a false negative, in the direction
    that lets drift ship.

    Measured, on the run that motivated this: oregon's `nhm_lulc_nhm_v11_params.csv`
    read 2026-07-25, comfortably after the #135/#136 lulc rewrite (2026-06-08), so
    the audit called it CURRENT. Its per-batch CSVs are dated 2026-05-20 and it still
    carries the pre-rewrite `retention` column -- the 2026-07-25 stamp was a fill
    sweep. The derivation predates the builder by nearly three weeks.

    The per-batch CSVs under `{output_dir}/{param}/` are what `run_merge` actually
    consumed, and nothing rewrites them after the fact, so their newest mtime is the
    honest derivation date. Params with no per-batch stage (depstor constants, snarea)
    fall back to the merged mtime, which is the best available.
    """
    if not per_batch_dir.is_dir():
        return merged_date
    dates = [d for d in (_mdate(f) for f in per_batch_dir.glob("*.csv")) if d]
    return max(dates) if dates else merged_date


def consumes_map(entries: list[dict]) -> dict[str, set[str]]:
    """param -> the params whose `merged_file` it references in its own config entry.

    DERIVED, not declared. `ssflux` names `merged_slope_file:
    {data_root}/{fabric}/params/merged/nhm_slope_params.csv`, so the dependency is
    already stated in the key the builder actually reads. A hand-maintained
    `consumes:` block would be a second declaration of the same fact, free to drift
    from the first -- and the drift would be silent, which is the failure mode this
    whole tool exists to catch.

    Only ZONAL cross-param file references are visible here. The depstor cascade
    (`wbody_connectivity -> dprst -> routing -> drains_*`) is a RASTER DAG in
    depstor_rasters.yml, not a merged-CSV reference, and is out of scope -- see #217.
    """
    by_file = {e["merged_file"]: e["name"] for e in entries if e.get("merged_file")}
    out: dict[str, set[str]] = {}
    for e in entries:
        deps = set()
        for key, val in e.items():
            if key == "merged_file" or not isinstance(val, str):
                continue
            dep = by_file.get(Path(val).name)
            if dep and dep != e["name"]:
                deps.add(dep)
        if deps:
            out[e["name"]] = deps
    return out


def propagate_upstream(rows: list[dict], consumes: dict[str, set[str]]) -> list[dict]:
    """Add an UPSTREAM flag to any product whose dependency is stale or newer than it.

    Two distinct conditions, and BOTH are needed:

      * the dependency is itself a candidate -- the consumer inherits its staleness
        even though the consumer may be far newer. This is the ssflux case: ssflux
        was derived 2026-08-10 from a slope product whose own derivation was
        2026-05-29 and predates the 2026-07-01 slope.vrt rebuild. A "is my input
        newer than me?" test alone reports ssflux current, because it is.
      * the dependency is NEWER than the consumer -- the consumer read an older
        version of it.

    Propagated to a fixed point so a chain of any length converges; the loop is
    bounded by the row count, so a cyclic config cannot hang it.
    """
    by_name = {r["param"]: r for r in rows}
    for _ in range(len(rows)):
        changed = False
        for name, deps in consumes.items():
            row = by_name.get(name)
            if row is None:
                continue
            for dep in deps:
                drow = by_name.get(dep)
                if drow is None:
                    continue
                inherits = bool(drow["flags"])
                newer = drow["product"] > row["product"]
                if (inherits or newer) and not any(
                    f.startswith(f"UPSTREAM {dep}") for f in row["flags"]
                ):
                    why = "stale" if inherits else "newer"
                    row["flags"].append(f"UPSTREAM {dep} {why}")
                    changed = True
        if not changed:
            break
    return rows


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
    """Newest date among a raster's inputs. For a VRT: the LATER of its own mtime
    and its newest referenced tile.

    Both terms are load-bearing, in opposite directions:

      * tiles alone under-report -- a VRT's mtime does not move when the tiles
        beneath it are replaced;
      * the VRT alone under-reports -- rewriting the XML does not touch the tiles.

    Taking the max was NOT the original design. This function first resolved to the
    tiles only, on the reasoning that a VRT is "just a pointer" whose mtime moves
    independently of its data. That reasoning is true and the conclusion drawn from
    it was wrong: a VRT also defines the COMPOSITING -- which sources, in what order,
    with what nodata and band mapping -- so a rewrite can change served pixels with
    every tile byte-identical.

    Measured, on the run that motivated this fix: oregon's `nhm_elevation_params.csv`
    was built 2026-07-25, later than every `elevation.vrt` tile (<= 2026-07-01), so
    the tiles-only form reported it CURRENT. Rebuilding it moved 99.9% of HRUs
    (median 0.46 m, max 8.9 m). The only input that had changed was the VRT itself,
    rewritten 2026-08-05.

    The asymmetry decides it: taking the max costs a false positive when a VRT is
    rewritten inertly, and a false positive is triaged away in minutes. Missing it
    lets drift ship silently, which is the failure this whole tool exists to prevent.
    """
    if not source_raster:
        return None
    p = Path(source_raster)
    if not p.exists():
        return None
    own = _mdate(p)
    if p.suffix != ".vrt":
        return own
    dates = [d for d in (_mdate(s) for s in parse_vrt_sources(p.read_text(), p.parent)) if d]
    dates.append(own)
    return max(d for d in dates if d)


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
        # The merged file's mtime is the last WRITE, which a fill sweep bumps.
        # Prefer the per-batch CSVs, which are what run_merge consumed.
        pdate = derivation_date(Path(data_root) / fabric / "params" / d.name, pdate)
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
    return propagate_upstream(rows, consumes_map(zc.get("params", [])))


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
