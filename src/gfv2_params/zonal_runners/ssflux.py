"""Per-batch ssflux parameter derivation.

Uses the CONUS-wide P2P weight matrix produced by ``run_build_weights`` (in
``weights.py``); chains in the merged slope CSV (from a prior ``merge`` of the
``slope`` zonal output) to compute ssflux family params (soil2gw_max,
ssr2gw_rate, fastcoef_lin, slowcoef_lin, gwflow_coef, dprst_seep_rate_open,
dprst_flow_coef).
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from ..raster_ops import deg_to_fraction
from .ssflux_math import (
    aggregate_k_perm_log,
    derive_log_params,
    interpolate_to_range,
    validate_weight_coverage,
)

# validate_weight_coverage's keyword-only thresholds, individually overridable
# from the ssflux entry in configs/zonal/zonal_params.yml. Absent from config
# means the function's own default is used -- _coverage_kwargs only forwards
# keys that are actually present, so an operator touching zero of them gets
# today's behaviour exactly.
_COVERAGE_THRESHOLD_KEYS = ("median_tol", "max_bad_fraction", "hard_bad_fraction")


def _coverage_kwargs(config: dict) -> dict:
    """Pull whichever weight-coverage thresholds the config overrides.

    Keeps validate_weight_coverage's own defaults for anything the config
    doesn't set, rather than re-stating them here and risking the two
    drifting apart.
    """
    return {k: config[k] for k in _COVERAGE_THRESHOLD_KEYS if k in config}


def _canonical_vpu_label(value) -> str:
    """Canonical string form of a VPU label for norm_scope: vpu grouping.

    Independent per-batch-file ``read_csv`` dtype inference can turn the same
    VPU into "01" (str) in one file and 1 (int) in another; a bare
    ``astype(str)`` would keep those as the distinct strings "01" and "1" and
    silently split one VPU's HRUs into two normalisation groups. Numeric-
    looking labels are canonicalised through ``int()`` so both collapse to
    "1"; NHDPlus's non-numeric labels ("10L", "10U") pass through unchanged
    (after stripping whitespace) since ``int()`` rejects them.
    """
    s = str(value).strip()
    try:
        return str(int(s))
    except (TypeError, ValueError):
        return s


def run_ssflux_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of subsurface flux parameter derivation.

    Originally extracted from the now-retired scripts/create_ssflux_params.py
    (see PR #85). Requires pre-computed
    CONUS weights (from run_build_weights) and merged slope CSV (from
    run_merge applied to the slope param). Output writes to
    {output_dir}/ssflux/ (subdir name is hardcoded to 'ssflux' to match
    today's create_ssflux_params.py behaviour).
    """
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    output_dir = Path(config["output_dir"])
    weight_dir = Path(config["weight_dir"])
    fabric = config["fabric"]

    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    target_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    batch_ids = set(target_gdf[id_feature].values)
    logger.info("Loaded %d features (batch %d)", len(target_gdf), batch_id)

    weight_file = weight_dir / f"lith_weights_{fabric}.csv"
    if not weight_file.exists():
        raise FileNotFoundError(
            f"Weight file not found: {weight_file}\n"
            "Run --mode build_weights first."
        )
    all_weights = pd.read_csv(weight_file)
    weights = all_weights[all_weights[id_feature].isin(batch_ids)].copy()
    logger.info("Loaded weights: %d rows (from %d total)", len(weights), len(all_weights))

    merged_slope_file = Path(config["merged_slope_file"])
    if not merged_slope_file.exists():
        raise FileNotFoundError(
            f"Merged slope file not found: {merged_slope_file}\n"
            "Run merge for the slope param first."
        )
    all_slope = pd.read_csv(merged_slope_file)
    slope_df = all_slope[all_slope[id_feature].isin(batch_ids)].copy()
    slope_df["mean_slope_fraction"] = slope_df["mean"].astype(float).apply(deg_to_fraction)
    logger.info("Loaded slope for %d features", len(slope_df))

    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))

    weights["flux_id"] = weights["flux_id"].astype(str)
    source_gdf["flux_id"] = source_gdf["flux_id"].astype(str)
    w = weights.merge(source_gdf[["flux_id", "k_perm"]], on="flux_id")

    # Permeability is INTENSIVE: area-weighted mean, not an area-prorated sum.
    # k_perm == 0 is Gleeson's no-data flag and is excluded, not floored to
    # k_perm_min -- see ssflux_math.aggregate_k_perm_log and issue #175.
    validate_weight_coverage(w, id_feature, logger, **_coverage_kwargs(config))
    agg = aggregate_k_perm_log(w, id_feature)
    agg[id_feature] = agg[id_feature].astype(int)
    k_perm_agg = agg.sort_values(by=id_feature).reset_index(drop=True)

    slope_merge = slope_df[[id_feature, "mean_slope_fraction"]].copy()
    try:
        slope_merge[id_feature] = slope_merge[id_feature].astype("int64")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Non-numeric {id_feature} values in slope data") from exc

    target_gdf["hru_area"] = target_gdf.geometry.area
    area_df = target_gdf[[id_feature, "hru_area"]].copy()
    try:
        area_df[id_feature] = area_df[id_feature].astype("int64")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Non-numeric {id_feature} values in target fabric") from exc

    # Carried so the reducer can honour `norm_scope: vpu`. gfv2 is multi-VPU and
    # keys this off the per-HRU `vpu` attribute on the batch gpkg. Single-VPU
    # fabrics (oregon, tjc) declare a `vpu` scalar in their base_config
    # profile instead; scripts/derive_zonal_params.py's `_build_param_cfg`
    # threads that scalar from the resolved fabric profile into `config["vpu"]`
    # when present, which is what the fallback below reads.
    if "vpu" in target_gdf.columns:
        vpu_df = target_gdf[[id_feature, "vpu"]].copy()
    else:
        vpu_df = pd.DataFrame(
            {id_feature: target_gdf[id_feature], "vpu": config.get("vpu", pd.NA)}
        )
    vpu_df[id_feature] = vpu_df[id_feature].astype("int64")

    df = k_perm_agg.merge(slope_merge, on=id_feature, how="left").copy()
    df = df.merge(area_df, on=id_feature, how="left")
    df = df.merge(vpu_df, on=id_feature, how="left")

    null_slope = df["mean_slope_fraction"].isna().sum()
    null_area = df["hru_area"].isna().sum()
    if null_slope > 0 or null_area > 0:
        raise ValueError(
            f"Merge produced missing values: {null_slope} features missing slope, "
            f"{null_area} features missing area. Check that slope and batch data "
            f"use consistent {id_feature} values."
        )

    # Raw values stay in log10 space; normalisation happens in the reduce step
    # (run_ssflux_reduce) because min/max must be taken over the whole fabric,
    # not over an arbitrary SLURM batch.
    log_params = derive_log_params(
        df["k_perm_log_wtd"].to_numpy(),
        df["mean_slope_fraction"].to_numpy(),
        df["hru_area"].to_numpy(),
    )
    for name, values in log_params.items():
        df[f"L_{name}"] = values

    ssflux_dir = output_dir / "ssflux"
    ssflux_dir.mkdir(parents=True, exist_ok=True)
    file_prefix = f"base_nhm_ssflux_{fabric}_batch_{batch_id:04d}_param"
    df.to_csv(ssflux_dir / f"{file_prefix}.csv", index=False)
    logger.info("SSFlux parameters saved (batch %d)", batch_id)


def run_ssflux_reduce(df, config: dict, logger):
    """Normalise the log-space L_* columns onto each parameter's target range.

    Runs once on the CONCATENATED frame (see run_merge's `reducer` hook), never
    per batch: TM 6-B9 interpolates over "all HRUs in a GF region", and doing it
    per SLURM batch made two HRUs with identical geology and slope receive
    different values depending on how the fabric was chunked (#175 item 3).

    `norm_scope: fabric` (default) takes one min/max over every HRU, which makes
    the output invariant to batch partitioning by construction. `norm_scope: vpu`
    reproduces TM 6-B9's per-region wording, at the cost of discontinuities at
    VPU boundaries.
    """
    id_feature = config["id_feature"]
    flux_params = config["flux_params"]
    scope = config.get("norm_scope", "fabric")
    if scope not in ("fabric", "vpu"):
        raise ValueError(f"Unknown norm_scope '{scope}'; expected 'fabric' or 'vpu'.")

    missing = [f"L_{fp['name']}" for fp in flux_params if f"L_{fp['name']}" not in df.columns]
    if missing:
        raise ValueError(
            f"Merged frame is missing {missing}. The ssflux batch runner must emit "
            "L_* columns; a frame without them predates the #175 map/reduce split, "
            "so re-run --mode zonal before merging."
        )

    # A pre-#175 batch CSV left behind by a failed/unrun array task carries
    # NEITHER k_perm_log_wtd NOR L_*/fflux -- its real (pre-#175) schema is
    # <id>, k_perm_wtd, mean_slope_fraction, hru_area, <7 param columns>.
    # pd.concat's column union backfills every column that file lacks with
    # NaN for the rows it contributed, which is why a check keyed on
    # "k_perm_log_wtd populated but L_* NaN" can never fire for a real stale
    # file: such a row has k_perm_log_wtd == NaN too (it never had that
    # column at all), so `has_k` is False and the AND short-circuits to
    # False. That check only ever caught a hand-built test frame that forced
    # k_perm_log_wtd populated on the "stale" rows -- an impossible state for
    # an actual pre-#175 CSV.
    #
    # Two checks that actually detect a real stale/foreign batch file:
    output_dir = config.get("output_dir", "<output_dir>")

    # (a) Retired-column check: k_perm_wtd was RENAMED to k_perm_log_wtd by
    # #175 (root cause #1, extensive -> intensive aggregation). Its presence
    # in the merged frame at all is conclusive proof some batch predates the
    # rewrite.
    if "k_perm_wtd" in df.columns:
        raise ValueError(
            "Merged frame contains the retired 'k_perm_wtd' column -- at least "
            "one batch CSV predates the #175 aggregation rewrite (k_perm_wtd, "
            "the extensive-form column, was replaced by k_perm_log_wtd, the "
            f"intensive-form one). Clear stale files from {output_dir}/ssflux/ "
            "and re-run the affected --mode zonal batch(es), then --mode merge."
        )

    # (b) Coverage-column check: the map phase (run_ssflux_batch via
    # aggregate_k_perm_log) always emits a populated `fflux` -- either a real
    # coverage fraction or the FFLUX_NO_OVERLAP (-1.0) sentinel, NEVER NaN.
    # This is what separates a stale row from a legitimate no-lithology HRU:
    # both have NaN k_perm_log_wtd/L_*, but only the legitimate HRU has a
    # populated fflux. A NaN fflux can therefore only mean the contributing
    # file didn't produce this column at all -- stale or foreign. (If `fflux`
    # is missing from EVERY batch, the frame predates #175 entirely and the
    # `missing` L_* check above already raised -- this check only needs to
    # catch the mixed case where at least one fresh batch contributed it.)
    if "fflux" in df.columns:
        n_nan_fflux = int(df["fflux"].isna().sum())
        if n_nan_fflux > 0:
            raise ValueError(
                f"{n_nan_fflux} row(s) have a NaN 'fflux'. The map phase always "
                "emits fflux as either a real coverage fraction or the "
                "FFLUX_NO_OVERLAP (-1.0) sentinel -- never NaN -- so these rows "
                "came from a batch CSV that did not produce this column, i.e. a "
                f"stale pre-#175 or foreign file. Clear stale files from "
                f"{output_dir}/ssflux/ and re-run the failed --mode zonal "
                "batch(es), then --mode merge."
            )

    if scope == "vpu":
        if "vpu" not in df.columns or df["vpu"].isna().all():
            raise ValueError(
                "norm_scope: vpu needs a populated 'vpu' column in the merged frame. "
                "Multi-VPU fabrics carry it per HRU; single-VPU fabrics must declare "
                "a `vpu` scalar in their base_config profile."
            )
        n_null = int(df["vpu"].isna().sum())
        if n_null > 0:
            raise ValueError(
                f"norm_scope: vpu found {n_null} of {len(df)} HRU(s) with a null "
                "'vpu' value. A null VPU cannot be normalised per-region -- "
                "groupby(dropna=True) would silently drop those rows from every "
                "group, leaving them NaN and indistinguishable from the "
                "legitimate no-lithology NaN, so they'd be silently KNN gap-filled "
                "downstream. Fix the source data so every HRU carries a VPU label."
            )
        # Per-batch CSVs are read back independently (see run_merge), so
        # pandas' per-file dtype inference can disagree on the same VPU label
        # ("01" -> int 1 in one file, str "01" in another, both meaning the
        # same VPU); NHDPlus VPU labels also include non-numeric ones ("10L",
        # "10U"). A bare astype(str) is not enough -- it turns "01" into the
        # string "01" and int 1 into the string "1", which are DIFFERENT
        # groupby keys, silently splitting one VPU's HRUs into two
        # normalisation groups. Canonicalise numeric-looking labels through
        # int() first so "01" and 1 collapse to the same "1"; non-numeric
        # labels ("10L"/"10U") fall through unchanged.
        df = df.assign(vpu=df["vpu"].map(_canonical_vpu_label))
        groups = list(df.groupby("vpu").groups.items())
    else:
        groups = [("__fabric__", df.index)]

    out = df.copy()
    for fp in flux_params:
        name, lo, hi = fp["name"], float(fp["min"]), float(fp["max"])
        col = np.full(len(out), np.nan)
        for label, idx in groups:
            pos = out.index.get_indexer(idx)
            try:
                col[pos] = interpolate_to_range(out.loc[idx, f"L_{name}"].to_numpy(), lo, hi)
            except ValueError as exc:
                raise ValueError(f"{name} (scope={scope}, group={label}): {exc}") from exc
        out[name] = col

    out = out.drop(columns=[f"L_{fp['name']}" for fp in flux_params])
    logger.info(
        "Normalised %d ssflux params over scope=%s (%d group(s))",
        len(flux_params), scope, len(groups),
    )
    return out.sort_values(id_feature).reset_index(drop=True)
