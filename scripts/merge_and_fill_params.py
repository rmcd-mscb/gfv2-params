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
import math
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.log import configure_logging

# The declaration this sweep is driven by lives in the package, not here, so the
# index generator and the two guards can import it without importing this script's
# geo stack. Re-exported by this import: tests/test_merge_and_fill_params.py
# reaches them as `maf.DeclaredParam` / `maf.iter_declared_params`.
from gfv2_params.params_index import (  # noqa: F401  (DeclaredParam re-exported)
    DeclaredParam,
    iter_declared_params,
    load_declared_params,
)


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
    `fabric_columns` maps CSV column name -> (source, scale), where `source` is either
    a fabric-GDF column name or the SENTINEL "geometry", meaning `geometry.area` -- a
    derived scalar, not a column read. These are columns whose value is an exact known
    fact (`hru_area`), copied from the fabric rather than KNN-interpolated; see
    `apply_fabric_columns`. Note the two lists are NOT peers in one respect: a param
    may declare either, both, or neither, but `fabric_columns` alone only ever writes
    the rows in `missing_ids`.
    `undeclared_with_nan` is the caller's warning material: columns carrying NaN that
    nobody declared fillable. A `fabric_columns` column is NOT exempt from it -- see
    the census comment in `resolve_fill_plan`.
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

    # fabric_columns: {csv_col: {source: gdf_col_or_"geometry", scale: float}}
    # Validated eagerly and completely, like fill_columns above, because the whole
    # point of the mechanism is that a typo must RAISE rather than silently copy
    # nothing. Narrower than fill_columns in one respect: there is no alias-list
    # support here (fill_columns has it for the retention/rad_trncf rename), so a
    # key is always a plain column name.
    fabric_columns: dict[str, tuple[str, float]] = {}
    bad_specs: list[str] = []
    for csv_col, spec in (fabric_col_spec or {}).items():
        if csv_col not in param_df.columns:
            bad_specs.append(f"{csv_col!r} (not a column in the parameter file)")
            continue
        if not isinstance(spec, dict):
            bad_specs.append(
                f"{csv_col!r} (value must be a mapping with a `source` key, got {spec!r})"
            )
            continue
        if "source" not in spec:
            bad_specs.append(f"{csv_col!r} (mapping has no `source` key)")
            continue
        try:
            scale = float(spec.get("scale", 1.0))
        except (TypeError, ValueError):
            bad_specs.append(f"{csv_col!r} (`scale` is not a number: {spec.get('scale')!r})")
            continue
        if not math.isfinite(scale) or scale == 0:
            bad_specs.append(f"{csv_col!r} (`scale` must be finite and non-zero, got {scale!r})")
            continue
        fabric_columns[csv_col] = (spec["source"], scale)

    if bad_specs:
        raise ValueError(
            f"`fabric_columns` for '{param_name}' is malformed: {bad_specs}. Expected "
            f"`{{<csv column>: {{source: <gdf column> | geometry, scale: <number>}}}}`, "
            f"where the csv column is one of {sorted(param_df.columns)}. Fix the config -- "
            f"a typo here would silently copy nothing."
        )

    if missing_ids and not declared and not fabric_columns:
        raise ValueError(
            f"'{param_name}' is missing {len(missing_ids)} HRU row(s) but declares no "
            f"`fill_columns` or `fabric_columns`, so nothing would be filled and the gap "
            f"would persist unnoticed. An absent HRU row is unambiguous -- unlike a NaN "
            f"cell, it cannot be a legitimate 'not derivable' result. Add `fill_columns` "
            f"for this param in its config entry."
        )

    # NaN census. A fabric column is NOT exempted here, even though
    # `apply_fabric_columns` will write into it: this census runs on the frame as
    # read from disk, BEFORE the absent rows are appended, so every NaN it counts is
    # in a row that already exists -- and `apply_fabric_columns` only ever writes
    # rows in `missing_ids`. Exempting them (as this originally did, on the reasoning
    # that "it will be filled from the fabric") suppressed the warning for exactly
    # the population the mechanism cannot reach.
    undeclared_with_nan = {}
    for col in param_df.columns:
        if col in resolved or col in ID_COLUMNS or col == id_feature:
            continue
        n_nan = int(param_df[col].isna().sum())
        if n_nan:
            undeclared_with_nan[col] = n_nan

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


def validate_fabric_sources(fabric_columns, merged_gdf, param_name) -> None:
    """Check every `fabric_columns` source against the fabric GDF, before any fill runs.

    The config half of the spec is validated in `resolve_fill_plan`, which has the
    parameter frame but not the GDF. This is the other half, and it must be EAGER: a
    typo'd `source` would otherwise surface only as a bare `KeyError` deep inside
    `apply_fabric_columns`, and only on the runs where a row happens to be absent --
    so a broken config could validate as fine indefinitely and fail on the exact day
    the mechanism was needed.
    """
    bad = []
    for csv_col, (source, _scale) in fabric_columns.items():
        if source == "geometry":
            if "geometry" not in merged_gdf.columns:
                bad.append(f"{csv_col!r} -> geometry (the fabric GDF has no geometry column)")
        elif source not in merged_gdf.columns:
            bad.append(f"{csv_col!r} -> {source!r} (not a column in the fabric GDF)")
    if bad:
        raise ValueError(
            f"`fabric_columns` for '{param_name}' names sources absent from the fabric "
            f"geopackage: {bad}. Available columns: {sorted(merged_gdf.columns)}. Fix the "
            f"config, or check that the profile's hru_gpkg is the fabric you meant."
        )


def apply_fabric_columns(complete_df, missing_ids, merged_gdf, fabric_columns, id_feature, logger):
    """Copy exact values from the fabric GDF into synthesized rows for declared fabric_columns.

    WHY this exists: `hru_area` is `geometry.area` -- an exact fact already on disk in
    the fabric gpkg, not a field to interpolate. Under the pre-`resolve_fill_plan`
    regime ("fill everything except the id columns") the 77 HRUs absent from gfv2's
    `nhm_ssflux_params.csv` had it KNN(k=1)-copied from whichever neighbour was
    nearest: median 2.45x the true area, worst case 11,109x. Under the current
    declaration-driven regime the same rows would instead land silently NaN, because
    `hru_area` is a litho/slope INPUT that nobody declares fillable and the
    `undeclared_with_nan` census runs before the absent rows are appended. Neither
    outcome is acceptable when the true value is one attribute lookup away.

    For each column in `fabric_columns`, the value is read from the GDF column named
    `source` (or computed as `geometry.area` when `source` is the sentinel "geometry")
    and multiplied by `scale`.

    Only synthesized (previously absent) rows are updated. For `hru_area` the existing
    rows' values are `geometry.area` too (`zonal_runners/ssflux.py`), so this is about
    blast radius rather than divergence: a mechanism that rewrote all 361,471 rows
    from a second code path could silently shift the canonical product, and there is
    no reason to take that risk to fix 77 rows.

    RAISES rather than warns when an id cannot be served -- absent from the GDF, or
    absent from the frame. Every other unrecoverable gap in this module raises (see
    `fill_missing_values_knn`'s null-geometry guard and `resolve_fill_plan`), and the
    failure mode of a warn-and-continue here is silent: the cell keeps its NaN, the
    param is written to the canonical file, and `main()` exits 0.
    """
    if not missing_ids:
        return complete_df

    # A duplicated id makes `.loc[id]` return a DataFrame, `row.geometry.area` a
    # Series, and the assignment below index-align into NaN instead of raising.
    gdf_indexed = merged_gdf.set_index(id_feature, verify_integrity=True)
    df = complete_df.copy()

    absent_from_gdf = [i for i in missing_ids if i not in gdf_indexed.index]
    if absent_from_gdf:
        raise ValueError(
            f"fabric_columns: {len(absent_from_gdf)} id(s) needing fill are absent from "
            f"the fabric geopackage (first few: {absent_from_gdf[:10]}). Every id in "
            f"`missing_ids` comes from range(1, expected_max_hru_id + 1), so this means "
            f"the fabric gpkg and expected_max_hru_id disagree -- a configuration error, "
            f"not a data condition."
        )

    n_written = 0
    for csv_col, (source, scale) in fabric_columns.items():
        for hru_id in missing_ids:
            row = gdf_indexed.loc[hru_id]
            val = row.geometry.area if source == "geometry" else row[source]
            target = df[id_feature] == hru_id
            if not target.any():
                raise ValueError(
                    f"fabric_columns: id {hru_id} has no row in '{csv_col}'s frame to "
                    f"write into. Synthesized rows are appended by fill_missing_values_knn "
                    f"-- this means it did not run, or ran on a different id set."
                )
            df.loc[target, csv_col] = val * scale
            n_written += 1

    # Count actual writes, not len(missing_ids): a summary that reports the ATTEMPT
    # count reads as an unqualified success in a SLURM log even when nothing landed.
    logger.info(
        "  fabric_columns: copied exact values for %s into %d synthesized row(s) (%d cell(s))",
        list(fabric_columns), len(missing_ids), n_written,
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

            validate_fabric_sources(plan.fabric_columns, merged_gdf, name)

            # `fabric_columns` counts as work: a param declaring ONLY fabric columns
            # must not be short-circuited here (this guard tested `fill_columns` alone
            # and made `apply_fabric_columns` below unreachable for such a param).
            if not plan.fill_columns and not plan.fabric_columns:
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
                # Post-condition, checked BEFORE the write: a fabric column that came
                # out NaN on a synthesized row means the copy silently did not land,
                # and nothing downstream would ever report it -- `merged/<name>.csv` is
                # the always-filled canonical artifact consumers read unconditionally.
                if missing_ids:
                    synthesized = complete_df[id_feature].isin(missing_ids)
                    for csv_col in plan.fabric_columns:
                        n_nan = int(complete_df.loc[synthesized, csv_col].isna().sum())
                        if n_nan:
                            raise ValueError(
                                f"fabric_columns: '{csv_col}' is still NaN on {n_nan} of "
                                f"{len(missing_ids)} synthesized row(s) after copying from "
                                f"the fabric. Refusing to write a canonical parameter file "
                                f"with an unfilled gap."
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

    declared_params = load_declared_params()

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
