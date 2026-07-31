"""Fill missing parameter values using KNN interpolation against the fabric geopackage.

The fabric geopackage is read from the active base_config.yml profile's
hru_gpkg/hru_layer (the same gpkg prepare_fabric.py batched) — the single
source of truth, not a {fabric}_nhru_merged.gpkg naming convention. For
VPU-based fabrics that gpkg is produced by notebooks/merge_vpu_targets.py; for
single-file fabrics (e.g. oregon) it is a pre-existing gpkg declared in the
profile.
"""

import argparse
import fnmatch
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import geopandas as gpd
import numpy as np
import pandas as pd
import yaml
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging


def find_missing_ids(param_file, expected_max, id_feature, logger):
    logger.info("Finding missing %s values...", id_feature)
    param_df = pd.read_csv(param_file)
    existing_ids = set(param_df[id_feature])
    expected_ids = set(range(1, expected_max + 1))
    missing_ids = sorted(expected_ids - existing_ids)
    logger.info("Found %d missing %s values out of %d", len(missing_ids), id_feature, expected_max)
    return param_df, missing_ids


@dataclass
class FillPlan:
    """What a param's fill run may touch, and what it found but must not touch.

    `fill_columns` is exactly the caller's declared list, intersected with nothing —
    a declared column missing from the frame is an error, not a silent skip.
    `fabric_columns` maps CSV column name -> (gdf_column, scale) for columns whose
    value is a known geometric fact (e.g. hru_area = geometry.area) and must be
    copied from the fabric GDF rather than KNN-interpolated.
    `undeclared_with_nan` is the caller's warning material: columns carrying NaN that
    nobody declared fillable.
    """

    fill_columns: list[str] = field(default_factory=list)
    fabric_columns: dict[str, tuple[str, float]] = field(default_factory=dict)
    undeclared_with_nan: dict[str, int] = field(default_factory=dict)


# Columns that identify an HRU rather than parameterise it. Never fillable, and never
# reported as an undeclared gap.
ID_COLUMNS = {"hru_id", "nat_hru_id", "model_hru_idx", "vpu"}


def resolve_fill_plan(param_df, declared, missing_ids, id_feature, param_name, fabric_col_spec=None) -> FillPlan:
    """Decide what to fill for one param, and what to surface without filling.

    The selection is DECLARATION-driven, not gap-driven, because "has a NaN" does not
    mean "is missing". `nhm_snarea_curve_params.csv` carries 7,891 rows with NaN, every
    one in a provenance column -- `cv_empirical` is derivable for only ~42% of HRUs BY
    DESIGN, and `cv_subgrid` exists to rescue the rest. Filling it would overwrite the
    record of how each curve was derived with interpolated noise. Before this function
    existed the column list was "everything except the id columns", which would have
    done exactly that.

    Each entry in `declared` is normally a plain column name, but MAY instead be a
    list/tuple of alias alternatives -- e.g. `[retention, rad_trncf]` for
    `lulc_nhm_v11`, whose `lulc_prederived` builder renamed the same computed quantity
    from `retention` to `rad_trncf` ("There is no `retention` column: it was only ever
    a stand-in for rad_trncf", zonal_runners/lulc_prederived.py). Fabrics built before
    the rename (gfv2, oregon) still carry `retention` on disk; fabrics built after it
    (tjc) carry `rad_trncf`. The first alternative present in `param_df` wins; this is
    NOT a generic "optional column" mechanism -- if NONE of the alternatives are
    present, that still raises exactly like a plain missing column would, so a
    genuinely absent column cannot silently skip filling.

    Two asymmetric guards, because the two kinds of gap differ in what they can mean:

      * an undeclared column with NaN CELLS is ambiguous -- it may be a legitimate
        "not derivable" -- so it is reported for the caller to WARN about;
      * an absent HRU ROW is not ambiguous. The HRU is in the file or it is not, and no
        provenance reading makes a missing HRU correct. With nothing declared fillable,
        that is a configuration error and RAISES.
    """
    declared = list(declared or [])

    resolved: list[str] = []
    unresolved: list[str] = []
    for item in declared:
        if isinstance(item, (list, tuple)):
            alternatives = list(item)
            match = next((alt for alt in alternatives if alt in param_df.columns), None)
            if match is None:
                unresolved.append(" or ".join(alternatives))
            else:
                resolved.append(match)
        elif item not in param_df.columns:
            unresolved.append(item)
        else:
            resolved.append(item)

    if unresolved:
        raise ValueError(
            f"`fill_columns` for '{param_name}' names {unresolved}, which are not present in "
            f"the parameter file (columns: {sorted(param_df.columns)}). Fix the config -- "
            f"a typo here would silently fill nothing."
        )

    if missing_ids and not declared:
        raise ValueError(
            f"'{param_name}' is missing {len(missing_ids)} HRU row(s) but declares no "
            f"`fill_columns`, so nothing would be filled and the gap would persist "
            f"unnoticed. An absent HRU row is unambiguous -- unlike a NaN cell, it cannot "
            f"be a legitimate 'not derivable' result. Add `fill_columns` for this param in "
            f"its config entry."
        )

    undeclared_with_nan = {}
    for col in param_df.columns:
        if col in resolved or col in ID_COLUMNS or col == id_feature:
            continue
        n_nan = int(param_df[col].isna().sum())
        if n_nan:
            undeclared_with_nan[col] = n_nan

    # fabric_columns: {csv_col: {source: gdf_col_or_"geometry", scale: float}}
    # Validate that declared fabric columns are present in the frame (same contract
    # as fill_columns — a typo must raise, not silently copy nothing).
    fabric_columns: dict[str, tuple[str, float]] = {}
    for csv_col, spec in (fabric_col_spec or {}).items():
        if csv_col not in param_df.columns:
            raise ValueError(
                f"`fabric_columns` for '{param_name}' names '{csv_col}', which is not "
                f"present in the parameter file. Fix the config."
            )
        fabric_columns[csv_col] = (spec["source"], float(spec.get("scale", 1.0)))
        # Exclude from undeclared_with_nan — it will be filled from the fabric.
        undeclared_with_nan.pop(csv_col, None)

    return FillPlan(fill_columns=resolved, fabric_columns=fabric_columns, undeclared_with_nan=undeclared_with_nan)


def fill_missing_values_knn(param_df, missing_ids, merged_gdf, param_columns, k, id_feature, logger):
    """KNN-interpolate absent HRU rows AND present-but-NaN parameter cells.

    For absent ids: appends a new all-NaN row so every expected id exists in
    the frame before per-column filling begins.

    For NaN cells: per column, the fit set is rows where that column has a
    valid (non-NaN) value AND valid coordinates; the fill set is rows where
    that column is NaN AND coords are valid. NaN-valued rows are excluded from
    the fit set so they cannot be chosen as a fill source.

    Exactly one row per id is guaranteed in the output; no duplicate ids.
    """
    logger.info("Filling missing values using KNN interpolation (k=%d)...", k)

    # Normalise to a list so a single column name still works.
    if isinstance(param_columns, str):
        param_columns = [param_columns]

    # Check whether there is anything to do before loading centroids.
    has_nan_cells = param_df[param_columns].isna().to_numpy().any() if param_columns else False
    if not missing_ids and not has_nan_cells:
        logger.info("No missing values to fill!")
        return param_df

    # Capture NaN count from the *original* frame before absent rows are appended
    # so the log line reflects only genuinely missing parameter cells, not the
    # all-NaN rows we are about to synthesise for absent ids.
    n_input_nan = param_df[param_columns].isna().to_numpy().sum() if param_columns else 0

    # Compute centroids once (avoid mutating caller's GDF).
    merged_gdf = merged_gdf.copy()
    merged_gdf["centroid"] = merged_gdf["geometry"].centroid
    merged_gdf["x"] = merged_gdf["centroid"].x
    merged_gdf["y"] = merged_gdf["centroid"].y
    coords_df = merged_gdf[[id_feature, "x", "y"]].copy()

    # Step 1: append all-NaN rows for absent ids so every expected id is present.
    if missing_ids:
        absent_rows = pd.DataFrame({id_feature: missing_ids})
        # Some fabrics' CSVs carry a secondary local hru_id column; populate it for
        # appended rows ONLY if the frame already has one. gfv2's id_feature is
        # nat_hru_id with NO hru_id column at all -- unconditionally adding one here
        # would introduce a spurious, all-empty trailing hru_id column into the
        # canonical output for every param with an absent row (lulc_nhm_v11,
        # lulc_nalcms, ssflux on gfv2). When id_feature is already hru_id (e.g.
        # oregon) the column already exists, so this is a harmless self-assignment.
        if "hru_id" in param_df.columns:
            absent_rows["hru_id"] = absent_rows[id_feature]
        param_df = pd.concat([param_df, absent_rows], ignore_index=True)

    # Step 2: attach centroid x,y to every row (left join preserves all ids).
    full_df = param_df.merge(coords_df, on=id_feature, how="left")

    # Step 3: per-column KNN fill.
    for param_column in tqdm(param_columns, desc="Filling param columns"):
        col_vals = full_df[param_column].values
        x_vals = full_df["x"].values
        y_vals = full_df["y"].values

        has_valid_coord = ~(np.isnan(x_vals) | np.isnan(y_vals))
        has_valid_value = ~np.isnan(col_vals)

        fill_mask = (~has_valid_value) & has_valid_coord
        fit_mask = has_valid_value & has_valid_coord

        # Null-geometry guard: any fill row with NaN coords cannot be KNN-filled.
        null_geom_fill = (~has_valid_value) & (~has_valid_coord)
        if null_geom_fill.any():
            raise ValueError(
                f"{null_geom_fill.sum()} features needing fill for '{param_column}' "
                "have null geometry and cannot be filled via KNN interpolation."
            )

        if not fill_mask.any():
            continue

        if not fit_mask.any():
            raise ValueError(
                f"Column '{param_column}' has no valid (non-NaN) values to fit from; "
                "cannot KNN-fill with an empty training set."
            )

        fit_coords = np.column_stack([x_vals[fit_mask], y_vals[fit_mask]])
        fit_values = col_vals[fit_mask]
        fill_coords = np.column_stack([x_vals[fill_mask], y_vals[fill_mask]])

        knn = NearestNeighbors(n_neighbors=k)
        knn.fit(fit_coords)
        _, indices = knn.kneighbors(fill_coords)

        filled_values = np.array([np.mean(fit_values[neighbor_idx]) for neighbor_idx in indices])
        full_df.loc[fill_mask, param_column] = filled_values

    # Step 4: drop helper columns, sort, reset index.
    full_df = full_df.drop(columns=["x", "y"], errors="ignore")
    full_df = full_df.sort_values(id_feature).reset_index(drop=True)

    n_absent = len(missing_ids)
    logger.info(
        "Filled %d absent id(s) and %d NaN cell(s) across %d column(s)",
        n_absent, n_input_nan, len(param_columns),
    )
    return full_df


UNFILLED_DIRNAME = "_unfilled"


def write_filled_in_place(complete_df, param_file, original_df, dtypes, logger=None):
    """Write the filled frame OVER `param_file`, preserving the pre-fill copy.

    `merged/<name>.csv` is the single canonical per-HRU parameter file -- always
    gap-filled -- so a consumer's rule is one line: read `merged/*.csv`. The retired
    `filled_` prefix required a consumer to know, per param AND per fabric, which of two
    files was authoritative; the set differed between gfv2 (2 filled) and oregon (4), and
    only `viz.py` encoded the rule.

    The pre-fill copy goes to `merged/_unfilled/`, mirroring `merged/_intermediates/`.

    IDEMPOTENCY, load-bearing: `_unfilled/<name>.csv` is written ONLY if it does not
    already exist. On a re-run the on-disk file is ALREADY filled, so overwriting the
    preserved copy with it would destroy the true raw version -- irreversibly, on a
    shared filesystem with no version control.

    `dtypes` restores each column's pre-fill dtype. KNN returns float64 even at k=1, which
    silently turned `cov_type` (an integer class 0-3) into 0.0/1.0/2.0/3.0. No averaging
    occurs at k=1, so the classes are correct -- but the dtype change is visible to any
    consumer that does not cast.
    """
    unfilled_dir = param_file.parent / UNFILLED_DIRNAME
    unfilled_dir.mkdir(parents=True, exist_ok=True)
    raw_copy = unfilled_dir / param_file.name

    if raw_copy.exists():
        if logger:
            logger.info(
                "  %s already preserved at %s — not overwriting (a re-run's input is "
                "already filled)", param_file.name, raw_copy,
            )
    else:
        original_df.to_csv(raw_copy, index=False)
        if logger:
            logger.info("  pre-fill copy preserved -> %s", raw_copy)

    out = complete_df.copy()
    for col, dt in (dtypes or {}).items():
        if col in out.columns and dt.kind in "iu" and out[col].notna().all():
            out[col] = out[col].round().astype(dt)
    out.to_csv(param_file, index=False)
    if logger:
        logger.info("  canonical parameter file written -> %s", param_file)
    return param_file


# Repo-relative config paths consumed by `iter_declared_params`. Only the
# static `name`/`merged_file`/`output_file`/`fill_columns` fields are read
# here (none are {data_root}/{fabric}-templated), so a bare yaml.safe_load is
# enough -- no need for gfv2_params.config.load_config's fabric-profile
# resolution, which keeps this read safe with no data root (the CI contract:
# tests may parse these files but must not touch a real data root).
_REPO_ROOT = Path(__file__).resolve().parent.parent
ZONAL_PARAMS_CONFIG = _REPO_ROOT / "configs" / "zonal" / "zonal_params.yml"
DEPSTOR_PARAMS_CONFIG = _REPO_ROOT / "configs" / "depstor" / "depstor_params.yml"
SNAREA_LIBRARY_CONFIG = _REPO_ROOT / "configs" / "snarea" / "snarea_library.yml"


def _load_yaml_doc(path: Path) -> dict:
    """Bare `yaml.safe_load` of a config file, no placeholder resolution."""
    doc = yaml.safe_load(path.read_text())
    return doc or {}


class DeclaredParam(NamedTuple):
    """One config entry's fill contract: what file, and what may be written into it.

    A NamedTuple rather than a bare tuple because this record has now been widened
    twice (`fill_columns`, then `fabric_columns`) and each widening has to reach FOUR
    consumption sites. The `fabric_columns` widening missed one of them --
    `warn_undeclared_merged_files`, whose `for _, merged_file, _ in ...` then raised
    `ValueError: too many values to unpack` in all-params mode, the DEFAULT and the
    only mode `slurm_batch/merge_and_fill_params.batch` runs. It aborted `main()`
    outside `run_fill_sweep`'s per-param guard, so nothing was filled at all, while
    the test suite stayed green because its fixtures hand-built the OLD 3-tuple shape
    -- test and implementation wrong in the same direction, agreeing with each other.

    Attribute access cannot drift that way: a fifth field is invisible to every
    consumer that does not ask for it.
    """

    name: str
    merged_file: str
    fill_columns: list[str]
    fabric_columns: dict


def iter_declared_params(
    zonal_cfg: dict, depstor_cfg: dict, snarea_cfg: dict | None = None
) -> list[DeclaredParam]:
    """Every param with a canonical `merged/` output, and what it declares fillable.

    Returns `DeclaredParam(name, merged_file, fill_columns, fabric_columns)` records.
    `fill_columns` is KNN-interpolated; `fabric_columns` is copied verbatim from the
    fabric GDF (see `apply_fabric_columns`). Both default to empty for the params that
    declare neither -- today every param but `ssflux` has an empty `fabric_columns`.

    `zonal_cfg["params"]` and `depstor_cfg["means"]` name their canonical file
    `merged_file`; `depstor_cfg["ratios"]` names it `output_file` instead (see
    `derive_depstor_params.py`'s `run_mean_finalize` / `run_ratios`).
    `depstor_cfg["fractions"]` is DELIBERATELY EXCLUDED: every fraction entry
    also happens to carry a `merged_file` key, but that key names the
    per-fraction COUNT csv written to `merged/_intermediates/` (`run_merge`),
    never a `merged/` output -- fractions are intermediates, not
    consumer-facing params, and are never filled.

    `snarea_cfg` is optional and does NOT share either config's
    list-of-entries shape. `snarea_curve` is not a `zonal_params.yml` entry at
    all -- it comes from the separate 3-stage SNODAS pipeline
    (`configs/snarea/snarea_library.yml`, Stage 3), a flat single-param config
    whose `params_file` names the real `nhm_snarea_curve_params.csv`. A
    synthetic `snarea_curve` entry could not live in `zonal_params.yml`
    instead: `slurm_batch/submit_zonal_params.sh` loops that exact `params:`
    list to submit one SLURM array job per param, and a phantom entry with no
    `source_raster`/`script:` would break that orchestration. Hence the
    separate optional argument rather than a fourth zonal-shaped list.
    """
    def _record(name: str, merged_file: str, entry: dict) -> DeclaredParam:
        return DeclaredParam(
            name=name,
            merged_file=merged_file,
            fill_columns=list(entry.get("fill_columns") or []),
            fabric_columns=dict(entry.get("fabric_columns") or {}),
        )

    declared: list[DeclaredParam] = []

    for entry in zonal_cfg.get("params", []) or []:
        merged_file = entry.get("merged_file")
        if merged_file:
            declared.append(_record(entry["name"], merged_file, entry))

    for entry in depstor_cfg.get("means", []) or []:
        merged_file = entry.get("merged_file")
        if merged_file:
            declared.append(_record(entry["name"], merged_file, entry))

    for entry in depstor_cfg.get("ratios", []) or []:
        merged_file = entry.get("output_file")
        if merged_file:
            declared.append(_record(entry["name"], merged_file, entry))

    if snarea_cfg:
        merged_file = snarea_cfg.get("params_file")
        if merged_file:
            declared.append(_record("snarea_curve", merged_file, snarea_cfg))

    return declared


def _load_declared_params() -> list[DeclaredParam]:
    """`iter_declared_params` over the real in-repo configs."""
    zonal_cfg = _load_yaml_doc(ZONAL_PARAMS_CONFIG)
    depstor_cfg = _load_yaml_doc(DEPSTOR_PARAMS_CONFIG)
    snarea_cfg = _load_yaml_doc(SNAREA_LIBRARY_CONFIG)
    return iter_declared_params(zonal_cfg, depstor_cfg, snarea_cfg)


def check_param_file_in_fabric(param_file: Path, merged_dir: Path) -> None:
    """Refuse a `--param_file` that does not live under the active fabric's
    own `merged/` directory.

    `--fabric gfv2 --param_file <gfv2_vpu01 path>` would otherwise write in
    place into a DIFFERENT fabric's canonical parameter file -- the CLI takes
    `--fabric` and `--param_file` as two independent flags with nothing tying
    them together, so nothing else catches a mismatched pair.
    """
    if param_file.parent.resolve() != merged_dir.resolve():
        raise ValueError(
            f"--param_file {param_file} is not under the active fabric's merged "
            f"directory ({merged_dir}). Refusing to write in place into what looks "
            f"like a different fabric's canonical parameter file -- pass a path "
            f"under {merged_dir}, or check --fabric."
        )


# Allowlist for warn_undeclared_merged_files: on-disk artifacts under merged/
# that legitimately have no fill_columns declaration because they are not a
# per-HRU consumer param (e.g. a *_library.csv reference table).
_UNDECLARED_ALLOW_PATTERNS = ("*_library*", "*_validation*")


def warn_undeclared_merged_files(merged_dir: Path, declared_params, logger) -> list[str]:
    """Warn about any `merged/nhm_*_params.csv` that no config entry declares.

    All-params mode iterates the DECLARATION, so a merged file with no config
    entry is otherwise never filled and never flagged -- there is nothing
    enforcing the reverse direction (declaration -> disk is covered by the
    per-target skip warning; disk -> declaration was not, until now).

    Returns the sorted list of undeclared filenames warned about (for tests).
    """
    # Read by NAME, not by position: this set is the only thing this function needs
    # from the record, and a positional unpack here is exactly what broke when
    # `fabric_columns` widened it (see DeclaredParam).
    declared_filenames = {d.merged_file for d in declared_params}
    undeclared = []
    for on_disk in sorted(merged_dir.glob("nhm_*_params.csv")):
        if on_disk.name in declared_filenames:
            continue
        if any(fnmatch.fnmatch(on_disk.name, pat) for pat in _UNDECLARED_ALLOW_PATTERNS):
            continue
        undeclared.append(on_disk.name)
        logger.warning(
            "%s exists under %s but is not declared in any fill_columns config "
            "(configs/zonal/zonal_params.yml, configs/depstor/depstor_params.yml, or "
            "configs/snarea/snarea_library.yml) -- it will never be gap-filled by this "
            "step.", on_disk.name, merged_dir,
        )
    return undeclared


def apply_fabric_columns(complete_df, missing_ids, merged_gdf, fabric_columns, id_feature, logger):
    """Copy exact values from the fabric GDF into synthesized rows for declared fabric_columns.

    For each column in `fabric_columns`, the value is read from the GDF column named
    `source` (or computed as `geometry.area` when source == "geometry") and multiplied
    by `scale`. Only synthesized (previously absent) rows are updated — existing rows
    already carry the builder-computed value and must not be overwritten.
    """
    if not missing_ids:
        return complete_df

    gdf_indexed = merged_gdf.set_index(id_feature)
    df = complete_df.copy()

    for csv_col, (source, scale) in fabric_columns.items():
        for hru_id in missing_ids:
            if hru_id not in gdf_indexed.index:
                logger.warning("  fabric_columns: %s not found in GDF for id %s", csv_col, hru_id)
                continue
            row = gdf_indexed.loc[hru_id]
            val = row.geometry.area if source == "geometry" else row[source]
            df.loc[df[id_feature] == hru_id, csv_col] = val * scale

    logger.info(
        "  fabric_columns: copied exact values for %s into %d synthesized row(s)",
        list(fabric_columns), len(missing_ids),
    )
    return df


def run_fill_sweep(targets, merged_gdf, expected_max, id_feature, k_neighbors, logger) -> list[str]:
    """Fill every `(name, param_file, fill_columns, fabric_col_spec)` in `targets`,
    isolating per-param failures.

    One param's config drift (e.g. a column rename that resolve_fill_plan can't
    resolve) must not starve every OTHER declared param of its fill -- a
    partially-applied canonical set from an aborted sweep is worse than a
    param that stays unfilled with a named, logged cause. Catches per-param,
    logs the full traceback, records the failure, and continues; the caller
    decides the process exit code from the returned failure list.
    """
    failed_params: list[str] = []
    for name, param_file, declared_columns, fabric_col_spec in targets:
        logger.info("=== %s (%s) ===", name, param_file.name)
        try:
            param_df, missing_ids = find_missing_ids(param_file, expected_max, id_feature, logger)
            plan = resolve_fill_plan(param_df, declared_columns, missing_ids, id_feature, name, fabric_col_spec)

            for col, n in plan.undeclared_with_nan.items():
                logger.warning(
                    "  %s: column '%s' has %d NaN cell(s) but is NOT declared in "
                    "`fill_columns` — left untouched. If it is a PRMS parameter rather than "
                    "provenance, add it to the config; if it is provenance, this is correct.",
                    param_file.name, col, n,
                )

            if not plan.fill_columns:
                logger.info("  No fill_columns declared for %s; nothing to fill", name)
                continue

            # Capture dtypes on the pristine pre-fill frame; fill_missing_values_knn
            # does not mutate param_df in place (it rebinds through pd.concat/merge),
            # so it also doubles as the "original" frame write_filled_in_place needs.
            dtypes = param_df.dtypes.to_dict()

            complete_df = fill_missing_values_knn(
                param_df, missing_ids, merged_gdf, plan.fill_columns, k_neighbors, id_feature, logger,
            )

            if plan.fabric_columns:
                complete_df = apply_fabric_columns(
                    complete_df, missing_ids, merged_gdf, plan.fabric_columns, id_feature, logger,
                )

            write_filled_in_place(complete_df, param_file, param_df, dtypes, logger=logger)

            final_ids = set(complete_df[id_feature])
            expected_ids = set(range(1, expected_max + 1))
            still_missing = expected_ids - final_ids
            if still_missing:
                logger.warning("  %d IDs are still missing", len(still_missing))
            else:
                logger.info("  All missing values have been filled successfully!")
        except Exception:
            # See docstring: isolate this param's failure, keep going.
            logger.exception("  %s: FAILED to fill -- continuing with remaining params", name)
            failed_params.append(name)

    return failed_params


def main():
    parser = argparse.ArgumentParser(description="Fill missing parameter values using KNN interpolation.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--merged_gpkg", default=None, help="Path to merged nhru geopackage")
    parser.add_argument(
        "--param_file", default=None,
        help="Path to ONE merged parameter CSV to fill (single-param mode). Its "
             "filename must be declared (merged_file/output_file/params_file) in "
             "configs/zonal/zonal_params.yml, configs/depstor/depstor_params.yml, or "
             "configs/snarea/snarea_library.yml. Omit to fill every declared param "
             "for the active fabric whose merged file already exists (default).",
    )
    parser.add_argument(
        "--output_dir", default=None,
        help="Unused. Filling now writes in place at merged/<name>.csv (see "
             "write_filled_in_place) -- kept only so a caller still passing this "
             "flag does not break.",
    )
    parser.add_argument("--k_neighbors", type=int, default=1)
    args = parser.parse_args()

    logger = configure_logging("merge_and_fill_params")

    base = load_base_config(
        Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    data_root = base["data_root"]
    fabric = base["fabric"]
    expected_max = base["expected_max_hru_id"]
    id_feature = require_config_key(base, "id_feature", "merge_and_fill_params")

    # The merged fabric gpkg is authoritative in the active base_config.yml
    # profile (hru_gpkg/hru_layer) — read it from there, not a
    # {fabric}_nhru_merged.gpkg naming convention. --merged_gpkg is an override.
    hru_layer = base.get("hru_layer", "nhru")
    if args.merged_gpkg is None:
        args.merged_gpkg = require_config_key(base, "hru_gpkg", "merge_and_fill_params")

    merged_gpkg = Path(args.merged_gpkg)
    merged_dir = Path(data_root) / fabric / "params" / "merged"

    if not merged_gpkg.exists():
        raise FileNotFoundError(
            f"Fabric geopackage not found: {merged_gpkg}\n"
            "Check the active fabric profile's hru_gpkg in configs/base_config.yml. "
            "For VPU-based fabrics, run notebooks/merge_vpu_targets.py to produce it; "
            "for single-file fabrics, place the gpkg at the hru_gpkg path."
        )

    logger.info("Loading merged geopackage: %s (layer=%s)", merged_gpkg, hru_layer)
    try:
        merged_gdf = gpd.read_file(merged_gpkg, layer=hru_layer)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read merged geopackage: {merged_gpkg}\n"
            "The file may be corrupt."
        ) from exc
    logger.info("Loaded %d features", len(merged_gdf))

    declared_params = _load_declared_params()

    if args.param_file is not None:
        # Single-param mode: fill exactly the file named on the CLI, routed
        # through the same resolve_fill_plan / fill_missing_values_knn /
        # write_filled_in_place path as the default all-params mode below.
        param_file = Path(args.param_file)
        check_param_file_in_fabric(param_file, merged_dir)
        if not param_file.exists():
            raise FileNotFoundError(
                f"Parameter file not found: {param_file}\n"
                "Run scripts/derive_zonal_params.py --mode merge --param <name> (or the "
                "matching depstor/snarea step) for this parameter type first."
            )
        match = next((d for d in declared_params if d.merged_file == param_file.name), None)
        if match is None:
            raise ValueError(
                f"'{param_file.name}' is not declared in configs/zonal/zonal_params.yml, "
                "configs/depstor/depstor_params.yml, or configs/snarea/snarea_library.yml "
                "-- add a `fill_columns` entry for it before filling."
            )
        targets = [(match.name, param_file, match.fill_columns, match.fabric_columns)]
    else:
        # All-params mode (default): every declared param whose merged file has
        # already been produced for this fabric. A param not yet produced
        # (e.g. lulc_nlcd/lulc_foresce -- inputs unstaged) is skipped with a
        # WARNING (not silently at INFO -- an operator scanning the log for
        # problems should see it).
        targets = []
        for d in declared_params:
            pf = merged_dir / d.merged_file
            if not pf.exists():
                logger.warning("Skipping %s: %s not found (not yet produced for this fabric)", d.name, pf)
                continue
            targets.append((d.name, pf, d.fill_columns, d.fabric_columns))
        if not targets:
            logger.warning("No declared params' merged files were found under %s", merged_dir)
            return 0

        # The reverse direction: warn about a merged/nhm_*_params.csv on disk that
        # no config entry declares at all -- see warn_undeclared_merged_files.
        warn_undeclared_merged_files(merged_dir, declared_params, logger)

    failed_params = run_fill_sweep(
        targets, merged_gdf, expected_max, id_feature, args.k_neighbors, logger,
    )

    if failed_params:
        logger.error(
            "%d of %d param(s) FAILED to fill: %s. The canonical set is only PARTIALLY "
            "applied -- fix the cause(s) above and re-run.",
            len(failed_params), len(targets), ", ".join(failed_params),
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
