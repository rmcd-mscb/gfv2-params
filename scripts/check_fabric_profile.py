"""Validate a fabric profile against the geopackage it names, before any SLURM run.

Run this after filling the four TODO values `init-data-root --add-fabric`
leaves in `configs/base_config.yml`, and before `prepare_fabric` or the
`submit_fabric_rerun.sh` chain:

    pixi run --as-is python scripts/check_fabric_profile.py --fabric <name>

It prints one PASS/FAIL line per check and exits 1 if any check failed. Every
check runs even after an earlier one fails, so the whole punch-list comes out
of one run. The checks are exactly the mistakes that corrupt a new fabric
SILENTLY rather than loudly:

- `id_feature` must be unique and contiguous 1..N, and `expected_max_hru_id`
  must equal N. Gap-fill synthesizes a row for every id in 1..N that is missing
  from a merged CSV, so a national id with gaps manufactures thousands of
  phantom HRUs, and a wrong max either does the same or hides real gaps.
- `hru_layer` / `segments_layer` must exist in their gpkgs. A segments layer
  mis-wired to the wrong file makes nearly every waterbody depression storage
  and the run still exits 0.
- `vpu` must resolve the way `depstor_builders/vpu_id.py` will resolve it: a
  valid profile scalar, else a per-HRU `vpu` column. Otherwise `vpu_id` raises
  hours into the depstor stack.
- Every path-valued input the profile declares must exist, at the resolved path.

Only the profile's own values are read; the rules come from the same functions
the pipeline uses (`resolve_vpu_source`, `vpu_to_code`), not a second copy.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pyogrio

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.depstor_builders.vpu_id import resolve_vpu_source, vpu_to_code

# Path-valued profile keys, checked only when the profile declares them. An
# undeclared key is a documented omission (tjc has no burn_add_waterbody_table),
# not a missing file.
_PATH_KEYS = (
    "template_raster",
    "fdr_raster",
    "twi_raster",
    "waterbody_gpkg",
    "wbd_huc12_table",
    "burn_add_waterbody_table",
    "sink_points_table",
    "ecoregions_gpkg",
    "dem_1m_inventory",
    "wesm_project_attrs",
)

_SCRIPT = "check_fabric_profile"


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


def _layer_names(path: Path) -> list[str]:
    return [str(row[0]) for row in pyogrio.list_layers(path)]


def _check_layer(results: list[CheckResult], label: str, gpkg: Path, layer: str) -> bool:
    """Append `<label> exists` and `<label_layer> present`; return True if both hold."""
    if not gpkg.exists():
        results.append(CheckResult(f"{label} exists", False, f"not found: {gpkg}"))
        return False
    results.append(CheckResult(f"{label} exists", True, str(gpkg)))
    layers = _layer_names(gpkg)
    layer_key = label.replace("_gpkg", "_layer")
    if layer not in layers:
        results.append(CheckResult(
            f"{layer_key} present", False,
            f"no layer '{layer}' in {gpkg.name}; it has: {layers}",
        ))
        return False
    results.append(CheckResult(f"{layer_key} present", True, layer))
    return True


def _check_ids(results: list[CheckResult], config: dict, gpkg: Path, layer: str, fields: list[str]) -> None:
    id_col = require_config_key(config, "id_feature", _SCRIPT)
    if id_col not in fields:
        results.append(CheckResult(
            "id column present", False,
            f"no column '{id_col}' in layer {layer}; columns: {fields}",
        ))
        return
    results.append(CheckResult("id column present", True, id_col))

    ids = pyogrio.read_dataframe(gpkg, layer=layer, columns=[id_col], read_geometry=False)[id_col]
    n_null = int(ids.isna().sum())
    results.append(CheckResult(
        "id column has no nulls", n_null == 0,
        f"{n_null} null id(s)" if n_null else "no nulls",
    ))
    ids = ids.dropna().astype("int64")
    n = len(ids)

    dup = sorted(ids[ids.duplicated()].unique().tolist())
    results.append(CheckResult(
        "id column unique", not dup,
        f"duplicated id(s): {dup[:10]}" if dup else f"{n} unique ids",
    ))

    lo, hi = int(ids.min()), int(ids.max())
    missing = sorted(set(range(1, n + 1)) - set(ids.tolist()))
    contiguous = lo == 1 and hi == n and not missing
    results.append(CheckResult(
        "id column contiguous 1..N", contiguous,
        f"ids run 1..{n} with no gaps" if contiguous
        else f"{n} rows but ids run {lo}..{hi}; missing from 1..{n}: {missing[:10]}",
    ))

    expected = config.get("expected_max_hru_id")
    results.append(CheckResult(
        "expected_max_hru_id matches", expected == hi,
        f"profile says {expected}, the gpkg's highest id is {hi}",
    ))


def run_checks(config: dict) -> list[CheckResult]:
    """Run every check against the resolved profile; never stop at the first failure."""
    results: list[CheckResult] = []

    hru_gpkg = Path(require_config_key(config, "hru_gpkg", _SCRIPT))
    hru_layer = config.get("hru_layer", "nhru")
    fields: list[str] | None = None
    if _check_layer(results, "hru_gpkg", hru_gpkg, hru_layer):
        fields = list(pyogrio.read_info(hru_gpkg, layer=hru_layer)["fields"])

    if config.get("segments_gpkg"):
        _check_layer(results, "segments_gpkg", Path(config["segments_gpkg"]),
                     config.get("segments_layer", "nsegment"))

    if fields is not None:
        _check_ids(results, config, hru_gpkg, hru_layer, fields)

    try:
        kind, value = resolve_vpu_source(config.get("vpu"), fields is not None and "vpu" in fields)
        if kind == "scalar":
            vpu_to_code(value)
        results.append(CheckResult("vpu resolves", True, f"{kind}: {value}"))
    except ValueError as exc:
        results.append(CheckResult("vpu resolves", False, str(exc)))

    for key in _PATH_KEYS:
        if key in config:
            p = Path(config[key])
            results.append(CheckResult(
                f"{key} exists", p.exists(), str(p) if p.exists() else f"not found: {p}",
            ))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fabric", required=True, help="Fabric name (a profile in base_config.yml).")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml (default: the repo's).")
    args = parser.parse_args(argv)

    config = load_base_config(Path(args.base_config) if args.base_config else None, fabric=args.fabric)
    results = run_checks(config)

    width = max(len(r.name) for r in results)
    for r in results:
        print(f"{'PASS' if r.ok else 'FAIL'}  {r.name.ljust(width)}  {r.detail}")
    n_fail = sum(not r.ok for r in results)
    if n_fail:
        print(f"\n{n_fail} check(s) FAILED for fabric '{args.fabric}'. Fix the profile or the gpkg before submitting anything.")
        return 1
    print(f"\nAll {len(results)} checks passed for fabric '{args.fabric}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
