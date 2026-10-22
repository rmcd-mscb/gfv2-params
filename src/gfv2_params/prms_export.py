"""One file per PRMS parameter: `merged/<entry>.csv` -> `params/prms/<prms_name>.csv`.

`merged/` is shaped by config ENTRY: one file per entry, mixing PRMS parameters with raw
zonal statistics, derivation inputs and provenance. This module is the post-fill export
that reads those FILLED files and writes one file per PRMS parameter, named for the
parameter in `docs/HI_paramDB_DataDictionary.csv` (`soil_moist_max.csv`, `hru_elev.csv`).
The mapping is the existing `prms.columns.<emitted col>.prms` declaration, so no new
declaration is needed beyond the `prms.export: false` opt-out.

Pure pandas/yaml and import-safe like `params_index.py`: no geo imports, no data root
needed to PLAN. Only `export_fabric` touches disk.

Rules this module enforces (each is a regression someone has already hit elsewhere):

- It reads ONLY the filled `merged/<entry>.csv`. NaN in an exported column raises --
  the fill stage's job is to have removed them, and `merged/_unfilled/` is never read.
- Only `prms.columns` is walked. `defects` and `provenance` are never exported, so a
  column that is supposed to be a PRMS parameter and currently is not can never leave
  under a correct-looking name.
- A PRMS name produced by more than one exportable entry raises. The four LULC
  sources all emit `cov_type`, so the three alternatives carry `prms.export: false`
  and a fourth source that forgets it fails in CI rather than clobbering a file.
- A missing merged file for an exportable entry raises. This is deliberately stricter
  than the fill sweep, which skips unbuilt params: here a missing file means a stage
  was not run, and a silently partial `params/prms/` directory is the worse outcome.
- Everything is built and validated in memory before anything is written, and each
  write is an atomic replace, so a failure never leaves a half-written product.
"""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd

from gfv2_params.params_index import DeclaredParam, load_declared_params
from gfv2_params.snarea.library import flatten_prms_curve

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DICTIONARY_PATH = _REPO_ROOT / "docs" / "HI_paramDB_DataDictionary.csv"

# Dictionary `Dimensions` values this module knows how to write. Anything else on a
# covered column raises at PLAN time (a CI failure), never at export time.
DIM_NHRU = "nhru"
DIM_NDEPLVAL = "ndeplval"
SNAREA_CURVE_POINTS = 11
SNAREA_CURVE_NAME = "snarea_curve"

PRMS_SUBDIR = "prms"


class ExportItem(NamedTuple):
    """One output file: which PRMS parameter, from which entry and column(s)."""

    prms_name: str
    entry: str  # DeclaredParam.name
    merged_file: str
    source_cols: tuple  # alias group (nhru) or the snarea_curve_N columns
    dimension: str  # dictionary `Dimensions`: 'nhru' | 'ndeplval'
    dtype: str  # dictionary `Type`: 'I' | 'F'


def load_dictionary(path: Path | None = None) -> dict[str, dict[str, str]]:
    """The PRMS data dictionary keyed by parameter name. Parsed with `csv` -- the
    Units and Description cells contain commas and quotes."""
    with open(path or DICTIONARY_PATH, newline="") as f:
        return {row["Parameter"]: row for row in csv.DictReader(f)}


def plan_exports(
    declared: list[DeclaredParam] | None = None,
    dictionary: dict[str, dict[str, str]] | None = None,
) -> list[ExportItem]:
    """Every file the export writes, derived from the declarations. Needs no data root."""
    declared = declared if declared is not None else load_declared_params()
    dictionary = dictionary if dictionary is not None else load_dictionary()

    items: list[ExportItem] = []
    producers: dict[str, list[str]] = {}
    for d in declared:
        if d.prms.get("export") is False:
            continue
        by_name: dict[str, list[str]] = {}
        for col, spec in (d.prms.get("columns") or {}).items():
            by_name.setdefault(spec["prms"], []).append(col)
        for prms_name, cols in by_name.items():
            if prms_name not in dictionary:
                raise ValueError(
                    f"entry {d.name!r} declares PRMS parameter {prms_name!r} "
                    f"(columns {cols}) which is not in the data dictionary "
                    f"{DICTIONARY_PATH.name} -- a typo, or a parameter the dictionary lacks"
                )
            row = dictionary[prms_name]
            dim = row["Dimensions"].strip()
            if dim not in (DIM_NHRU, DIM_NDEPLVAL):
                raise NotImplementedError(
                    f"{prms_name!r} has dimension {dim!r}; the export writes only "
                    f"{DIM_NHRU!r} and {DIM_NDEPLVAL!r}"
                )
            producers.setdefault(prms_name, []).append(d.name)
            items.append(
                ExportItem(prms_name, d.name, d.merged_file, tuple(cols), dim, row["Type"].strip())
            )

    dupes = {n: e for n, e in producers.items() if len(e) > 1}
    if dupes:
        raise ValueError(
            "PRMS parameter(s) produced by more than one exportable entry: "
            + "; ".join(f"{n} <- {sorted(e)}" for n, e in sorted(dupes.items()))
            + ". Set `prms.export: false` on the alternatives (see the LULC entries)."
        )
    return items


def _require_finite(name: str, source: str, values: pd.Series) -> None:
    arr = pd.to_numeric(values, errors="raise").to_numpy(float)
    nan = int(np.isnan(arr).sum())
    if nan:
        raise ValueError(
            f"{name}: {nan} NaN in {source} -- the export reads the FILLED product; "
            f"run the fill stage (merged/<name>.csv is the filled file)"
        )
    if not np.isfinite(arr).all():
        raise ValueError(f"{name}: non-finite value in {source}")


def _coerce(name: str, source: str, values: pd.Series, dtype: str) -> pd.Series:
    _require_finite(name, source, values)
    if dtype == "I":
        arr = values.to_numpy(float)
        if not np.array_equal(arr, np.round(arr)):
            raise ValueError(f"{name}: dictionary type is integer but {source} has non-integral values")
        return pd.Series(arr.astype(np.int64), index=values.index, name=name)
    return pd.Series(values.to_numpy(float), index=values.index, name=name)


def _warn_out_of_range(name: str, series: pd.Series, row: dict, logger) -> None:
    """Warn only: the dictionary min/max are the original NHM ranges, wrong for a new fabric."""
    try:
        lo, hi = float(row["minimum"]), float(row["maximum"])
    except (KeyError, ValueError):
        return
    n = int(((series < lo) | (series > hi)).sum())
    if n:
        logger.warning("%s: %d value(s) outside the dictionary range [%s, %s]", name, n, lo, hi)


def export_fabric(
    merged_dir: Path,
    out_dir: Path,
    id_feature: str,
    expected_rows: int,
    library_path: Path | None = None,
    declared: list[DeclaredParam] | None = None,
    dictionary: dict[str, dict[str, str]] | None = None,
    logger: logging.Logger | None = None,
) -> list[Path]:
    """Write `out_dir/<prms_name>.csv` for every planned parameter. Returns the paths.

    `expected_rows` is the fabric's `expected_max_hru_id`: the fill stage guarantees ids
    1..N are all present, so row-count equality is the right test.
    `library_path` is the snarea curve library CSV; required iff `snarea_curve` is planned.
    """
    logger = logger or logging.getLogger("prms_export")
    dictionary = dictionary if dictionary is not None else load_dictionary()
    items = plan_exports(declared, dictionary)
    merged_dir, out_dir = Path(merged_dir), Path(out_dir)

    frames: dict[str, pd.DataFrame] = {}
    id_sets: dict[str, set] = {}
    cache: dict[str, pd.DataFrame] = {}

    def merged_frame(item: ExportItem) -> tuple[pd.DataFrame, str]:
        path = merged_dir / item.merged_file
        if not path.is_file():
            raise FileNotFoundError(
                f"{item.prms_name}: {path} is missing -- entry {item.entry!r} has not been "
                f"built/filled on this fabric (run its stage, then the fill stage)"
            )
        if item.merged_file not in cache:
            df = pd.read_csv(path)
            if id_feature not in df.columns:
                raise ValueError(f"{path}: id column {id_feature!r} not in header")
            if df[id_feature].duplicated().any():
                raise ValueError(f"{path}: duplicate {id_feature} values")
            if len(df) != expected_rows:
                raise ValueError(f"{path}: {len(df)} rows, expected {expected_rows}")
            cache[item.merged_file] = df
            id_sets[item.merged_file] = set(df[id_feature])
        return cache[item.merged_file], item.merged_file

    for item in items:
        if item.dimension == DIM_NDEPLVAL:
            if item.prms_name != SNAREA_CURVE_NAME:
                raise NotImplementedError(f"no writer for {item.dimension} parameter {item.prms_name!r}")
            if library_path is None or not Path(library_path).is_file():
                raise FileNotFoundError(
                    f"snarea_curve needs the curve library CSV; {library_path} is missing"
                )
            flat = flatten_prms_curve(pd.read_csv(library_path))
            frames[item.prms_name] = pd.DataFrame(
                {"ndeplval_index": np.arange(1, len(flat) + 1), SNAREA_CURVE_NAME: flat}
            )
            continue

        df, key = merged_frame(item)
        present = [c for c in item.source_cols if c in df.columns]
        if len(present) != 1:
            raise ValueError(
                f"{item.prms_name}: expected exactly one of {list(item.source_cols)} in "
                f"{item.merged_file}, found {present} (a fabric vintage with both or neither?)"
            )
        col = present[0]
        values = _coerce(item.prms_name, f"{item.merged_file}:{col}", df[col], item.dtype)
        out = pd.DataFrame({id_feature: df[id_feature].to_numpy(), item.prms_name: values.to_numpy()})
        frames[item.prms_name] = out.sort_values(id_feature).reset_index(drop=True)
        _warn_out_of_range(item.prms_name, values, dictionary[item.prms_name], logger)

    if len({frozenset(s) for s in id_sets.values()}) > 1:
        raise ValueError("merged files disagree on the HRU id set: " + ", ".join(sorted(id_sets)))

    if "hru_deplcrv" in frames and SNAREA_CURVE_NAME in frames:
        ndepl = len(frames[SNAREA_CURVE_NAME]) // SNAREA_CURVE_POINTS
        idx = frames["hru_deplcrv"]["hru_deplcrv"]
        if idx.min() < 1 or idx.max() > ndepl:
            raise ValueError(
                f"hru_deplcrv spans {idx.min()}..{idx.max()} but snarea_curve holds {ndepl} curves"
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for name, frame in sorted(frames.items()):
        path = out_dir / f"{name}.csv"
        tmp = path.with_suffix(".csv.tmp")
        frame.to_csv(tmp, index=False)
        os.replace(tmp, path)
        written.append(path)
        col = frame.iloc[:, 1]
        logger.info("%s: %d rows, min %s, max %s -> %s", name, len(frame), col.min(), col.max(), path)

    stale = sorted(p.name for p in out_dir.glob("*.csv") if p.stem not in frames)
    if stale:
        logger.warning("%s holds files outside the current plan (not deleted): %s", out_dir, stale)
    return written
