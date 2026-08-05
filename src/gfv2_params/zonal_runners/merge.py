"""Concat per-batch CSVs for one param into its merged output.

Same function used by both the unified orchestrator (--mode merge) and the
legacy ``scripts/merge_params.py`` (retired in PR #85; the library function
stayed). Sorted by HRU id; writes to ``{output_dir}/{merged_subdir}/{merged_file}``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from gfv2_params import raster_ops

# Transforms a `derived_columns:` entry may name. Deliberately a whitelist rather
# than getattr(raster_ops, name): a typo must raise, not silently resolve to some
# other module-level function that happens to share the name.
_TRANSFORMS = {
    "deg_to_fraction": raster_ops.deg_to_fraction,
}


def apply_derived_columns(df, derived_columns: dict | None):
    """Add each declared derived column to a merged frame.

    Applied AFTER concat so it runs once per param rather than once per batch, and
    at merge time rather than zonal time -- which is why adding one needs no zonal
    re-run, only `--mode merge`.

    The source column is KEPT: it is declared provenance, not a temporary. For
    `slope` that matters, since `mean` (degrees) is what `hru_slope` is derived
    from and what a reader needs to check the derivation.
    """
    for out_col, spec in (derived_columns or {}).items():
        src, tname = spec["from"], spec["transform"]
        if tname not in _TRANSFORMS:
            raise ValueError(
                f"`{tname}` is not a known transform for derived column '{out_col}'. "
                f"Known: {sorted(_TRANSFORMS)}."
            )
        if src not in df.columns:
            raise ValueError(
                f"derived column '{out_col}' reads '{src}', which is not in the merged "
                f"frame (columns: {sorted(df.columns)})."
            )
        df[out_col] = df[src].astype(float).apply(_TRANSFORMS[tname])
    return df


def run_merge(config: dict, logger) -> None:
    """Concat per-batch CSVs for one param into the merged output CSV.

    Originally extracted from the now-retired scripts/merge_params.py:process_files()
    (see PR #85). Validates no
    duplicates, warns on gaps (if expected_max_hru_id is set in config).
    """
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    merged_file = config["merged_file"]
    fabric = config["fabric"]
    expected_max = config.get("expected_max_hru_id")

    input_dir = Path(config["output_dir"]) / source_type
    final_output_dir = Path(config["output_dir"]) / config.get("merged_subdir", "merged")
    final_output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    file_pattern = f"base_nhm_{source_type}_{fabric}_batch_*_param.csv"
    files = sorted(input_dir.glob(file_pattern))

    if not files:
        raise FileNotFoundError(f"No batch files found matching: {input_dir / file_pattern}")

    logger.info("Found %d batch files for %s", len(files), source_type)

    dfs = []
    for f in files:
        logger.debug("Reading: %s", f)
        df = pd.read_csv(f)
        if id_feature not in df.columns:
            raise ValueError(f"'{id_feature}' column not found in file: {f}")
        dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df = merged_df.sort_values(id_feature).reset_index(drop=True)

    dupes = merged_df[merged_df[id_feature].duplicated(keep=False)]
    if len(dupes) > 0:
        dupe_ids = sorted(dupes[id_feature].unique())
        raise ValueError(
            f"Duplicate {id_feature} values found ({len(dupe_ids)} IDs). "
            f"First 10: {dupe_ids[:10]}. This indicates overlapping batches."
        )

    if expected_max is not None:
        existing_ids = set(merged_df[id_feature])
        expected_ids = set(range(1, int(expected_max) + 1))
        gaps = sorted(expected_ids - existing_ids)
        if gaps:
            logger.warning(
                "%d missing %s values (expected 1-%d, got %d). First 10: %s. "
                "If this is expected, run merge_and_fill_params.py to fill gaps via KNN.",
                len(gaps), id_feature, expected_max, len(existing_ids), gaps[:10],
            )

    derived = config.get("derived_columns")
    if derived:
        merged_df = apply_derived_columns(merged_df, derived)
        logger.info("Applied derived columns: %s", sorted(derived))

    output_path = final_output_dir / merged_file
    merged_df.to_csv(output_path, index=False)
    logger.info("Merged %d rows -> %s", len(merged_df), output_path)
