"""Fill missing parameter values using KNN interpolation against the fabric geopackage.

The fabric geopackage is read from the active base_config.yml profile's
hru_gpkg/hru_layer (the same gpkg prepare_fabric.py batched) — the single
source of truth, not a {fabric}_nhru_merged.gpkg naming convention. For
VPU-based fabrics that gpkg is produced by notebooks/merge_vpu_targets.py; for
single-file fabrics (e.g. oregon) it is a pre-existing gpkg declared in the
profile.
"""

import argparse
from dataclasses import dataclass, field
from pathlib import Path

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
    `undeclared_with_nan` is the caller's warning material: columns carrying NaN that
    nobody declared fillable.
    """

    fill_columns: list[str] = field(default_factory=list)
    undeclared_with_nan: dict[str, int] = field(default_factory=dict)


# Columns that identify an HRU rather than parameterise it. Never fillable, and never
# reported as an undeclared gap.
ID_COLUMNS = {"hru_id", "nat_hru_id", "model_hru_idx", "vpu"}


def resolve_fill_plan(param_df, declared, missing_ids, id_feature, param_name) -> FillPlan:
    """Decide what to fill for one param, and what to surface without filling.

    The selection is DECLARATION-driven, not gap-driven, because "has a NaN" does not
    mean "is missing". `nhm_snarea_curve_params.csv` carries 7,891 rows with NaN, every
    one in a provenance column -- `cv_empirical` is derivable for only ~42% of HRUs BY
    DESIGN, and `cv_subgrid` exists to rescue the rest. Filling it would overwrite the
    record of how each curve was derived with interpolated noise. Before this function
    existed the column list was "everything except the id columns", which would have
    done exactly that.

    Two asymmetric guards, because the two kinds of gap differ in what they can mean:

      * an undeclared column with NaN CELLS is ambiguous -- it may be a legitimate
        "not derivable" -- so it is reported for the caller to WARN about;
      * an absent HRU ROW is not ambiguous. The HRU is in the file or it is not, and no
        provenance reading makes a missing HRU correct. With nothing declared fillable,
        that is a configuration error and RAISES.
    """
    declared = list(declared or [])

    absent = [c for c in declared if c not in param_df.columns]
    if absent:
        raise ValueError(
            f"`fill_columns` for '{param_name}' names {absent}, which are not present in "
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
        if col in declared or col in ID_COLUMNS or col == id_feature:
            continue
        n_nan = int(param_df[col].isna().sum())
        if n_nan:
            undeclared_with_nan[col] = n_nan

    return FillPlan(fill_columns=declared, undeclared_with_nan=undeclared_with_nan)


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
        # gfv2 CSVs carry a secondary local hru_id; populate it for appended rows.
        # When id_feature is already hru_id (e.g. oregon) this is a harmless self-assignment.
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


def iter_declared_params(
    zonal_cfg: dict, depstor_cfg: dict, snarea_cfg: dict | None = None
) -> list[tuple[str, str, list[str]]]:
    """Every param with a canonical `merged/` output and its declared `fill_columns`.

    Returns `(param_name, merged_filename, fill_columns)` tuples.

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
    declared: list[tuple[str, str, list[str]]] = []

    for entry in zonal_cfg.get("params", []) or []:
        merged_file = entry.get("merged_file")
        if merged_file:
            declared.append((entry["name"], merged_file, list(entry.get("fill_columns") or [])))

    for entry in depstor_cfg.get("means", []) or []:
        merged_file = entry.get("merged_file")
        if merged_file:
            declared.append((entry["name"], merged_file, list(entry.get("fill_columns") or [])))

    for entry in depstor_cfg.get("ratios", []) or []:
        merged_file = entry.get("output_file")
        if merged_file:
            declared.append((entry["name"], merged_file, list(entry.get("fill_columns") or [])))

    if snarea_cfg:
        merged_file = snarea_cfg.get("params_file")
        if merged_file:
            declared.append(("snarea_curve", merged_file, list(snarea_cfg.get("fill_columns") or [])))

    return declared


def _load_declared_params() -> list[tuple[str, str, list[str]]]:
    """`iter_declared_params` over the real in-repo configs."""
    zonal_cfg = _load_yaml_doc(ZONAL_PARAMS_CONFIG)
    depstor_cfg = _load_yaml_doc(DEPSTOR_PARAMS_CONFIG)
    snarea_cfg = _load_yaml_doc(SNAREA_LIBRARY_CONFIG)
    return iter_declared_params(zonal_cfg, depstor_cfg, snarea_cfg)


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
        if not param_file.exists():
            raise FileNotFoundError(
                f"Parameter file not found: {param_file}\n"
                "Run scripts/derive_zonal_params.py --mode merge --param <name> (or the "
                "matching depstor/snarea step) for this parameter type first."
            )
        match = next((d for d in declared_params if d[1] == param_file.name), None)
        if match is None:
            raise ValueError(
                f"'{param_file.name}' is not declared in configs/zonal/zonal_params.yml, "
                "configs/depstor/depstor_params.yml, or configs/snarea/snarea_library.yml "
                "-- add a `fill_columns` entry for it before filling."
            )
        name, _merged_file, fill_columns = match
        targets = [(name, param_file, fill_columns)]
    else:
        # All-params mode (default): every declared param whose merged file has
        # already been produced for this fabric. A param not yet produced
        # (e.g. lulc_nlcd/lulc_foresce -- inputs unstaged) is skipped with an
        # INFO line, not an error.
        targets = []
        for name, merged_file, fill_columns in declared_params:
            pf = merged_dir / merged_file
            if not pf.exists():
                logger.info("Skipping %s: %s not found (not yet produced for this fabric)", name, pf)
                continue
            targets.append((name, pf, fill_columns))
        if not targets:
            logger.warning("No declared params' merged files were found under %s", merged_dir)
            return

    for name, param_file, declared_columns in targets:
        logger.info("=== %s (%s) ===", name, param_file.name)

        param_df, missing_ids = find_missing_ids(param_file, expected_max, id_feature, logger)
        plan = resolve_fill_plan(param_df, declared_columns, missing_ids, id_feature, name)

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
            param_df, missing_ids, merged_gdf, plan.fill_columns, args.k_neighbors, id_feature, logger,
        )
        write_filled_in_place(complete_df, param_file, param_df, dtypes, logger=logger)

        final_ids = set(complete_df[id_feature])
        expected_ids = set(range(1, expected_max + 1))
        still_missing = expected_ids - final_ids
        if still_missing:
            logger.warning("  %d IDs are still missing", len(still_missing))
        else:
            logger.info("  All missing values have been filled successfully!")


if __name__ == "__main__":
    main()
