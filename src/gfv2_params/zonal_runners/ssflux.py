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
    validate_weight_coverage(w, id_feature, logger)
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
    # keys this off the per-HRU `vpu` attribute; single-VPU fabrics declare a
    # `vpu` scalar in their base_config profile instead.
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

    if scope == "vpu":
        if "vpu" not in df.columns or df["vpu"].isna().all():
            raise ValueError(
                "norm_scope: vpu needs a populated 'vpu' column in the merged frame. "
                "Multi-VPU fabrics carry it per HRU; single-VPU fabrics must declare "
                "a `vpu` scalar in their base_config profile."
            )
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
