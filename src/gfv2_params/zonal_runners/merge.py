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
    "atan2_deg": raster_ops.atan2_deg,
}


def apply_derived_columns(df, derived_columns: dict | None):
    """Add each declared derived column to a merged frame.

    Applied AFTER concat so it runs once per param rather than once per batch, and
    at merge time rather than zonal time -- which is why adding one needs no zonal
    re-run, only `--mode merge`.

    The source column is KEPT: it is declared provenance, not a temporary. For
    `slope` that matters, since `mean` (degrees) is what `hru_slope` is derived
    from and what a reader needs to check the derivation.

    `from:` accepts a single column name or a list of them; the transform's arity
    must match. `hru_slope` reads one column, `hru_aspect` reads two (`mean_sin`,
    `mean_cos`) because a circular mean is not a function of any single statistic.
    """
    for out_col, spec in (derived_columns or {}).items():
        src, tname = spec["from"], spec["transform"]
        if tname not in _TRANSFORMS:
            raise ValueError(
                f"`{tname}` is not a known transform for derived column '{out_col}'. "
                f"Known: {sorted(_TRANSFORMS)}."
            )
        # `from:` is one column name (deg_to_fraction) or a list of them
        # (atan2_deg needs mean_sin AND mean_cos). Normalised to a list here so
        # the missing-column check below covers both shapes with one code path --
        # a list form that skipped validation would address the wrong columns and
        # return plausible bearings.
        srcs = [src] if isinstance(src, str) else list(src)
        missing = [s for s in srcs if s not in df.columns]
        if missing:
            raise ValueError(
                f"derived column '{out_col}' reads {missing}, which are not in the merged "
                f"frame (columns: {sorted(df.columns)})."
            )
        # The transforms vectorise, so hand them whole Series rather than calling
        # them 361k times per param.
        df[out_col] = _TRANSFORMS[tname](*(df[s].astype(float) for s in srcs))
    return df


def _schema_mismatch_message(schemas: dict, source_type: str) -> str:
    """Name the odd-one-out batches and the columns they differ by.

    Reports the MINORITY groups against the largest one rather than dumping all
    64 filenames: on the failure this exists to catch, the majority is what the
    current code writes and the handful named is the set to re-run.
    """
    groups = sorted(schemas.items(), key=lambda kv: len(kv[1]), reverse=True)
    majority_cols, majority_files = groups[0]
    n_files = sum(len(v) for v in schemas.values())
    lines = [
        f"Per-batch CSVs for '{source_type}' do not all have the same columns: "
        f"{len(schemas)} distinct schemas across {n_files} files. pandas.concat "
        f"would UNION them and pad the difference with NaN, so the odd batches' "
        f"HRUs would silently reach the merged product with an empty cell in every "
        f"column they lack -- which the KNN fill sweep then interpolates from "
        f"neighbours, shipping guessed values at exit 0.",
        f"  {len(majority_files)} file(s) carry the majority schema "
        f"({len(majority_cols)} columns).",
    ]
    for cols, names in groups[1:]:
        shown = ", ".join(names[:5])
        if len(names) > 5:
            shown += f", ... (+{len(names) - 5} more)"
        lines.append(
            f"  {len(names)} file(s) differ by {sorted(majority_cols ^ cols)}: {shown}"
        )
    lines.append(
        "This normally means a SUBSET of batches predates a schema change -- a "
        "partially-failed zonal array leaves the previous run's CSVs in place. "
        "Re-run the zonal pass for the files named above "
        f"(`derive_zonal_params.py --mode zonal --param {source_type} --batch_id <N>`) "
        "and merge again."
    )
    return "\n".join(lines)


def run_merge(config: dict, logger, *, reducer=None) -> None:
    """Concat per-batch CSVs for one param into the merged output CSV.

    Originally extracted from the now-retired scripts/merge_params.py:process_files()
    (see PR #85). Validates no
    duplicates, warns on gaps (if expected_max_hru_id is set in config).

    ``reducer`` is an optional ``(df, config, logger) -> df`` callable applied
    once to the CONCATENATED frame, before the file is written. It exists for
    params whose final values need a statistic over the whole population and so
    cannot be computed per batch -- ssflux's min/max interpolation. The
    orchestrator resolves it from the config's ``reducer:`` tag via
    MERGE_REDUCERS; params without that tag keep today's behaviour exactly.
    """
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    merged_file = config["merged_file"]
    fabric = config["fabric"]
    expected_max = config.get("expected_max_hru_id")
    # pandas infers dtypes PER FILE, not across the whole param. A categorical
    # column that is all numeric-looking-with-leading-zeros in some batches and
    # alphanumeric in others (ssflux's `vpu`: "01".."09" vs "03N"/"10L") gets
    # inferred as int64 in the numeric-only batches and str in the mixed ones --
    # int64 silently strips the leading zero ("01" -> 1). Measured on a real
    # gfv2_dev rebuild: 12/20 ssflux batches inferred `vpu` int64, 8/20 str,
    # producing 28 distinct vpu labels in the merged product where the fabric
    # has only 21, 8 of which (1,2,4,5,6,7,8,9) don't exist in the fabric at
    # all -- the same VPU split across two labels depending on which batch an
    # HRU landed in. `read_dtypes:` lets a param's config pin the dtype for any
    # such column so every batch is read the same way; it's a property of any
    # categorical string column emitted per batch, not a vpu special case.
    read_dtypes = config.get("read_dtypes")

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

    # Every batch must carry the SAME columns, and `id_feature` alone is not that
    # check. `pd.concat` unions differing column sets and pads the gap with NaN, so
    # a PARTIAL re-run merges silently: 61 of 64 array tasks land the new schema, 3
    # fail (a PROJ-firewall storm once killed 11 of 64 snarea batches) and leave the
    # previous run's CSVs on disk. Those batches' HRUs then reach `merged/` with NaN
    # in every new column -- `resolve_fill_plan` checks column PRESENCE only, the fit
    # set is non-empty so KNN interpolates them from neighbours, and any derived
    # column is re-derived from the interpolated sources into a plausible-looking
    # value. Guard 2 sees the header; Guard 3 sees no NaN; the operator sees exit 0.
    # The ALL-stale case fails loudly on its own (the merged frame simply lacks the
    # column); it is precisely the partial case that is silent, and a partial case is
    # what a 64-task array produces when it goes wrong. Applies to every param.
    dfs = []
    schemas: dict[frozenset, list[str]] = {}
    for f in files:
        logger.debug("Reading: %s", f)
        df = pd.read_csv(f, dtype=read_dtypes) if read_dtypes else pd.read_csv(f)
        if id_feature not in df.columns:
            raise ValueError(f"'{id_feature}' column not found in file: {f}")
        schemas.setdefault(frozenset(df.columns), []).append(f.name)
        dfs.append(df)

    if len(schemas) > 1:
        raise ValueError(_schema_mismatch_message(schemas, source_type))

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

    if reducer is not None:
        reducer_name = config.get("reducer") or getattr(reducer, "__name__", repr(reducer))
        pre_n = len(merged_df)
        pre_ids = set(merged_df[id_feature])

        merged_df = reducer(merged_df, config, logger)
        if not isinstance(merged_df, pd.DataFrame):
            raise TypeError(
                f"reducer must return a pandas DataFrame, got "
                f"{type(merged_df).__name__}. It is applied to the concatenated "
                "frame and its return value is what gets written."
            )

        # MERGE_REDUCERS is explicitly designed for extension beyond ssflux
        # (see zonal_runners/__init__.py), so this invariant protects every
        # future reducer, not just today's one: pre-reducer validation above
        # (duplicate check, gap warning) is worthless if the reducer itself is
        # free to silently drop or relabel rows on the way out.
        post_n = len(merged_df)
        if post_n != pre_n:
            raise ValueError(
                f"reducer '{reducer_name}' changed the row count from {pre_n} "
                f"to {post_n}. A reducer must transform columns only -- it may "
                "not add or drop rows."
            )
        if id_feature not in merged_df.columns:
            raise ValueError(
                f"reducer '{reducer_name}' dropped the id column '{id_feature}' "
                "from its output."
            )
        post_ids = set(merged_df[id_feature])
        if post_ids != pre_ids:
            raise ValueError(
                f"reducer '{reducer_name}' changed the set of {id_feature} "
                f"values ({len(pre_ids)} -> {len(post_ids)} distinct ids). A "
                "reducer must preserve row identity, not just row count."
            )

        logger.info("Applied merge reducer: %s", config.get("reducer"))

    output_path = final_output_dir / merged_file
    merged_df.to_csv(output_path, index=False)
    logger.info("Merged %d rows -> %s", len(merged_df), output_path)
