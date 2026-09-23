"""Validate a fabric profile against the geopackage it names, before any SLURM run.

Run this after filling the four TODO values `init-data-root --add-fabric`
leaves in `configs/base_config.yml`, and before `prepare_fabric` or the
`submit_fabric_rerun.sh` chain:

    pixi run --as-is python scripts/check_fabric_profile.py --fabric <name>

It prints one PASS/FAIL line per check and exits 1 if any check failed. Every
check runs even after an earlier one fails, so the whole punch-list comes out
of one run; a problem the checker cannot get past (an unreadable gpkg, a
missing key) is itself a FAIL line, never a traceback. The checks are the
mistakes that corrupt a new fabric SILENTLY rather than loudly:

- `id_feature` must be an integer column, unique and contiguous 1..N, and
  `expected_max_hru_id` must be the plain integer N. Gap-fill synthesizes a
  row for every id in 1..N that is missing from a merged CSV, so a national id
  with gaps manufactures thousands of phantom HRUs, and a wrong max either
  does the same or hides real gaps. (These rules restate what
  `merge_and_fill_params.find_missing_ids` assumes; the pipeline itself only
  warns on gaps.)
- `hru_layer` / `segments_layer` must exist in their gpkgs and hold features.
  A typo fails at the first depstor step, hours in. This does NOT detect a
  valid segments layer from the wrong fabric; `min_onstream_comids` is for that.
- `vpu` must resolve the way `depstor_builders/vpu_id.py` will resolve it
  (its own `resolve_vpu_source` / `vpu_to_code`): a valid profile scalar, else
  a per-HRU `vpu` column with no null and no non-VPU value. Otherwise `vpu_id`
  raises at step 11 of the depstor stack.
- Every path in `_PATH_KEYS` (the fabric FDR clip plus the shared CONUS inputs)
  that the profile declares must exist at the resolved path. Opt-in
  comparison tables are not checked.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyogrio

from gfv2_params.config import SHARED_DEPSTOR_INPUT_KEYS, load_base_config
from gfv2_params.depstor_builders.vpu_id import resolve_vpu_source, vpu_to_code

# Path-valued profile keys, checked only when the profile declares them. An
# undeclared key is a legitimate omission (see SHARED_DEPSTOR_INPUT_KEYS), not a
# missing file. The fabric-owned FDR clip plus the shared CONUS inputs
# `init-data-root --check` also verifies (one list, in config.py).
_PATH_KEYS = ("template_raster", "fdr_raster") + SHARED_DEPSTOR_INPUT_KEYS


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


def _declared(results: list[CheckResult], config: dict, key: str) -> bool:
    """Append `<key> declared` only when it FAILS; a missing/null key is a FAIL line, not a raise."""
    if config.get(key) is None:
        results.append(CheckResult(f"{key} declared", False, f"`{key}` is missing or empty in the profile"))
        return False
    return True


def _check_layer(results: list[CheckResult], label: str, gpkg: Path, layer: str) -> bool:
    """Append `<label> exists/readable`, `<label_layer> present/has features`.

    Returns True when the layer is present (readable and named), so callers can
    go on to inspect it; an EMPTY layer still returns True, with its own FAIL
    line, so the id checks can report "no ids" rather than being skipped.
    """
    if not gpkg.exists():
        results.append(CheckResult(f"{label} exists", False, f"not found: {gpkg}"))
        return False
    results.append(CheckResult(f"{label} exists", True, str(gpkg)))
    try:
        layers = [str(row[0]) for row in pyogrio.list_layers(gpkg)]
    except pyogrio.errors.DataSourceError as exc:
        results.append(CheckResult(f"{label} readable", False, f"not a readable geopackage: {exc}"))
        return False
    layer_key = label.replace("_gpkg", "_layer")
    if layer not in layers:
        results.append(CheckResult(
            f"{layer_key} present", False,
            f"no layer '{layer}' in {gpkg.name}; it has: {layers}",
        ))
        return False
    results.append(CheckResult(f"{layer_key} present", True, layer))
    n = int(pyogrio.read_info(gpkg, layer=layer)["features"])
    results.append(CheckResult(
        f"{layer_key} has features", n > 0,
        f"{n} features" if n else f"layer '{layer}' is empty",
    ))
    return True


def _check_ids(results: list[CheckResult], config: dict, gpkg: Path, layer: str, fields: list[str]) -> None:
    if not _declared(results, config, "id_feature"):
        return
    id_col = config["id_feature"]
    if id_col not in fields:
        results.append(CheckResult(
            "id column present", False,
            f"no column '{id_col}' in layer {layer}; columns: {fields}",
        ))
        return
    results.append(CheckResult("id column present", True, id_col))

    raw = pyogrio.read_dataframe(gpkg, layer=layer, columns=[id_col], read_geometry=False)[id_col]
    n_null = int(raw.isna().sum())
    results.append(CheckResult(
        "id column has no nulls", n_null == 0,
        f"{n_null} null id(s)" if n_null else "no nulls",
    ))
    raw = raw.dropna()

    # A text column, or a float column with fractional values, is not an id
    # column. Report it rather than let the cast raise a traceback.
    numeric = pd.to_numeric(raw, errors="coerce")
    bad = raw[numeric.isna() | (numeric % 1 != 0)]
    if len(bad):
        results.append(CheckResult(
            "id column is integer", False,
            f"non-integer value(s): {bad.astype(str).unique()[:5].tolist()}",
        ))
        return
    results.append(CheckResult("id column is integer", True, f"{len(raw)} values, dtype {raw.dtype}"))
    ids = numeric.astype("int64")
    n = len(ids)

    # The stub ships 0; a float or a quoted number break range()/== downstream.
    expected = config.get("expected_max_hru_id")
    is_int = isinstance(expected, int) and not isinstance(expected, bool)
    results.append(CheckResult(
        "expected_max_hru_id is an integer", is_int,
        str(expected) if is_int else f"must be a plain integer, got {type(expected).__name__} {expected!r}",
    ))

    if n == 0:
        results.append(CheckResult("id column contiguous 1..N", False, "no ids at all (empty layer, or every id null)"))
        results.append(CheckResult("expected_max_hru_id matches", False, f"profile says {expected}, the gpkg has no ids"))
        return

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

    results.append(CheckResult(
        "expected_max_hru_id matches", is_int and expected == hi,
        f"profile says {expected!r}, the gpkg's highest id is {hi}",
    ))


def _check_vpu(config: dict, gpkg: Path, layer: str, fields: list[str] | None) -> CheckResult:
    """Resolve `vpu` the way vpu_id.build() will, and validate the VALUES it will see.

    On the scalar path that is the one profile value. On the attribute path
    (a multi-VPU fabric, the gfv2 pattern) `vpu_id` calls `vpu_to_code` on
    every row while rasterizing, nulls included, so one bad or null value in
    the column raises at step 11 of the depstor stack. Check every value now.
    """
    try:
        kind, value = resolve_vpu_source(config.get("vpu"), fields is not None and "vpu" in fields)
    except ValueError as exc:
        return CheckResult("vpu resolves", False, str(exc))
    if kind == "scalar":
        try:
            vpu_to_code(value)
        except ValueError as exc:
            return CheckResult("vpu resolves", False, str(exc))
        return CheckResult("vpu resolves", True, f"scalar: {value}")

    values = pyogrio.read_dataframe(gpkg, layer=layer, columns=["vpu"], read_geometry=False)["vpu"]
    n_null = int(values.isna().sum())
    if n_null:
        return CheckResult(
            "vpu resolves", False,
            f"{n_null} HRU(s) have a null `vpu`; vpu_id calls vpu_to_code on every row",
        )
    bad = []
    for v in values.unique():
        try:
            vpu_to_code(v)
        except ValueError:
            bad.append(str(v))
    if bad:
        return CheckResult("vpu resolves", False, f"`vpu` attribute has value(s) that are not a VPU: {bad[:10]}")
    return CheckResult("vpu resolves", True, f"attribute: {sorted(values.astype(str).unique().tolist())}")


def run_checks(config: dict) -> list[CheckResult]:
    """Run every check against the resolved profile; never stop at the first failure."""
    results: list[CheckResult] = []

    fields: list[str] | None = None
    hru_gpkg = Path(config["hru_gpkg"]) if config.get("hru_gpkg") else None
    hru_layer = config.get("hru_layer")
    if _declared(results, config, "hru_gpkg") and _declared(results, config, "hru_layer"):
        if _check_layer(results, "hru_gpkg", hru_gpkg, hru_layer):
            fields = list(pyogrio.read_info(hru_gpkg, layer=hru_layer)["fields"])

    if _declared(results, config, "segments_gpkg") and _declared(results, config, "segments_layer"):
        _check_layer(results, "segments_gpkg", Path(config["segments_gpkg"]), config["segments_layer"])

    if fields is not None:
        _check_ids(results, config, hru_gpkg, hru_layer, fields)

    results.append(_check_vpu(config, hru_gpkg, hru_layer, fields))

    for key in _PATH_KEYS:
        if key not in config:
            continue
        if not _declared(results, config, key):
            continue
        p = Path(config[key])
        results.append(CheckResult(
            f"{key} exists", p.exists(), str(p) if p.exists() else f"not found: {p}",
        ))
    return results


def _report(results: list[CheckResult], fabric: str) -> int:
    width = max(len(r.name) for r in results)
    for r in results:
        print(f"{'PASS' if r.ok else 'FAIL'}  {r.name.ljust(width)}  {r.detail}")
    n_fail = sum(not r.ok for r in results)
    if n_fail:
        print(f"\n{n_fail} check(s) FAILED for fabric '{fabric}'. Fix the profile or the gpkg before submitting anything.")
        return 1
    print(f"\nAll {len(results)} checks passed for fabric '{fabric}'.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fabric", required=True, help="Fabric name (a profile in base_config.yml).")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml (default: the repo's).")
    args = parser.parse_args(argv)

    try:
        config = load_base_config(Path(args.base_config) if args.base_config else None, fabric=args.fabric)
    except (ValueError, FileNotFoundError) as exc:
        return _report([CheckResult("profile loads", False, str(exc))], args.fabric)
    return _report(run_checks(config), args.fabric)


if __name__ == "__main__":
    raise SystemExit(main())
