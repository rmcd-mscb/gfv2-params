"""Library functions for the Part 2 zonal-pass parameter pipeline.

Each `run_*` function performs the per-batch (or CONUS-once) work for one
parameter type. Both the legacy per-script CLIs under ``scripts/`` and the
unified ``scripts/derive_zonal_params.py`` orchestrator delegate here so the
work logic lives in exactly one place.

The `config` dict each function receives is a flat mapping containing the
keys the existing per-param configs already provide (source_type,
source_raster, batch_dir, target_layer, id_feature, output_dir, merged_file,
categorical, fabric, the fabric-profile hru_gpkg/hru_layer that run_build_weights
reads, plus per-script extras like canopy_raster, crosswalk_file,
keep_raster, source_shapefile, merged_slope_file, weight_dir, k_perm_min,
flux_params). The orchestrator builds this dict by flattening the active
param entry in ``configs/zonal/zonal_params.yml`` onto the top-level ``defaults:``
block (plus the resolved fabric profile). The legacy CLIs build the same
shape via ``gfv2_params.config.load_config`` against a per-param YAML.

Refactor invariant: the existing per-script behaviour is preserved verbatim
— these functions are the prior ``main()`` bodies extracted unchanged
modulo argument plumbing.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Pre-import heartbeats so a future hang in the geo-library import chain
# (rasterio/GDAL/PROJ/pyogrio init under shared-FS metadata contention,
# observed once on VPU01 issue-#61 array run with zero stdout from one task)
# can be localised. Printed unconditionally with flush=True so SLURM's
# stdout/stderr line buffering doesn't swallow them.
print(
    f"[startup pid={os.getpid()} host={os.uname().nodename} "
    f"task={os.environ.get('SLURM_ARRAY_TASK_ID', '-')}] "
    f"python {sys.version.split()[0]} interpreter up, importing geo libs...",
    flush=True,
)
_t_imports = time.time()

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray
from gdptools import UserTiffData, WeightGenP2P, ZonalGen
from osgeo import gdal, osr

print(f"[startup] geo-library imports complete in {time.time() - _t_imports:.1f}s", flush=True)

from gfv2_params.lulc import (
    assign_cov_type,
    class_percentages_from_histogram,
    compute_covden,
    compute_interception,
    compute_retention,
    load_crosswalk,
)
from gfv2_params.raster_ops import deg_to_fraction

# Opt into the GDAL 4.0 default of raising Python exceptions instead of C-style
# error codes. Silences the FutureWarning that osgeo emits when neither
# UseExceptions/DontUseExceptions is set. NB: GDAL state is process-global —
# importing this module from a notebook or test harness will flip exception
# handling on for the whole process. That is the desired behaviour (GDAL 4.0's
# default) and what the slurm batches expect, but worth knowing if anyone
# embeds this module elsewhere.
gdal.UseExceptions()
osr.UseExceptions()


# ---------------------------------------------------------------------------
# Per-batch zonal workers
# ---------------------------------------------------------------------------

def run_zonal_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of continuous-zonal stats from a single raster.

    Drives the elevation/slope/aspect param types. Originally extracted from the now-retired scripts/create_zonal_params.py
    (see PR #85). Uses the gdptools NEW API
    (source_var/source_ds/source_crs/target_gdf/target_id).
    """
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    raster_path = Path(config["source_raster"])
    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{batch_id:04d}.gpkg"
    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")

    logger.info("Raster: %s", raster_path)
    logger.info("Batch GPKG: %s", batch_gpkg)

    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), batch_id)

    ned_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", ned_da.shape, ned_da.rio.crs)

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"

    data = UserTiffData(
        source_var=source_type,
        source_ds=ned_da,
        source_crs=ned_da.rio.crs,
        source_x_coord="x",
        source_y_coord="y",
        band=1,
        bname="band",
        target_gdf=nhru_gdf,
        target_id=id_feature,
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics complete. Shape: %s", stats.shape)


def run_soils_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of soils (categorical) or soil_moist_max (continuous).

    Originally extracted from the now-retired scripts/create_soils_params.py
    (see PR #85). Uses the gdptools OLD API (var/ds/proj_ds/f_feature/id_feature) — preserved verbatim because
    that's what the per-batch helper functions below also use.
    """
    source_type = config["source_type"]
    categorical = config.get("categorical", False)
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), batch_id)

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"

    raster_path = Path(config["source_raster"])
    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    source_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Loaded raster: shape=%s, crs=%s", source_da.shape, source_da.rio.crs)

    if source_type == "soils":
        _process_soils(source_da, nhru_gdf, output_dir, file_prefix, categorical, id_feature, logger)
    elif source_type == "soil_moist_max":
        _process_soil_moist_max(source_da, nhru_gdf, output_dir, source_type, file_prefix, categorical, id_feature, logger)
    else:
        raise ValueError(f"Unknown source_type for soils dispatch: {source_type}")


def _process_soils(source_da, nhru_gdf, output_path, file_prefix, categorical, id_feature, logger):
    """Categorical soils: zonal histogram -> dominant category -> CSV."""
    data = UserTiffData(
        var="soils", ds=source_da, proj_ds=source_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    zonal_gen = ZonalGen(
        user_data=data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_path, file_prefix=f"{file_prefix}_temp", jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed")

    zg_file = output_path / f"{file_prefix}_temp.csv"
    if zg_file.exists():
        zg_file.unlink()

    category_cols = [col for col in stats.columns if str(col) not in ("count",)]
    top_stats = stats.copy()
    top_stats["max_category"] = top_stats[category_cols].idxmax(axis=1)
    result = top_stats[["max_category"]].rename(columns={"max_category": "soils"})
    result.sort_index(inplace=True)

    result_csv = output_path / f"{file_prefix}.csv"
    result.to_csv(result_csv)
    logger.info("Soils parameters saved to: %s", result_csv)


def _process_soil_moist_max(source_da, nhru_gdf, output_path, source_type, file_prefix, categorical, id_feature, logger):
    """Continuous soil_moist_max: zonal mean from the pre-built raster."""
    data = UserTiffData(
        var=source_type, ds=source_da, proj_ds=source_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    zonal_gen = ZonalGen(
        user_data=data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_path, file_prefix=f"{file_prefix}_temp", jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=categorical)
    logger.info("Zonal statistics computed for soil_moist_max")

    mean_stats = stats[["mean"]].rename(columns={"mean": "soil_moist_max"})
    result_csv = output_path / f"{file_prefix}.csv"
    mean_stats.to_csv(result_csv)
    logger.info("soil_moist_max parameters saved to: %s", result_csv)


def run_lulc_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of LULC parameter derivation.

    Originally extracted from the now-retired scripts/create_lulc_params.py
    (see PR #85). Five steps:
      1. categorical zonal stats on LULC raster -> class percentages
      2. continuous zonal stats on canopy raster -> canopy_mean per HRU
      3. retention: either zonal mean from keep raster (FORE-SCE / NHM v1.1)
         or crosswalk evergreen_retention (NLCD / NALCMS)
      4. compute cov_type / interception / covden via gfv2_params.lulc helpers
      5. merge + write CSV
    """
    source_type = config["source_type"]
    id_feature = config["id_feature"]
    target_layer = config["target_layer"]
    fabric = config["fabric"]

    output_dir = Path(config["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_dir = Path(config["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{batch_id:04d}.gpkg"
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")
    nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
    logger.info("Loaded %s layer: %d features (batch %d)", target_layer, len(nhru_gdf), batch_id)

    crosswalk_path = Path(config["crosswalk_file"])
    if not crosswalk_path.is_absolute():
        # Relative crosswalk paths resolve against the repo root (this module
        # is src/gfv2_params/zonal_runners.py -> parents[2] = repo root).
        crosswalk_path = Path(__file__).resolve().parents[2] / crosswalk_path
    crosswalk = load_crosswalk(crosswalk_path)
    logger.info("Loaded crosswalk: %d classes", len(crosswalk))

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"

    # --- Step 1: Categorical zonal stats on LULC raster ---
    lulc_path = Path(config["source_raster"])
    if not lulc_path.exists():
        raise FileNotFoundError(f"LULC raster not found: {lulc_path}")
    lulc_da = rioxarray.open_rasterio(lulc_path, masked=True)
    logger.info("Loaded LULC raster: shape=%s", lulc_da.shape)

    lulc_data = UserTiffData(
        var="lulc", ds=lulc_da, proj_ds=lulc_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    lulc_zonal = ZonalGen(
        user_data=lulc_data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_dir, file_prefix=f"{file_prefix}_lulc_temp", jobs=4,
    )
    histogram = lulc_zonal.calculate_zonal(categorical=True)
    logger.info("LULC categorical zonal stats computed")

    temp_csv = output_dir / f"{file_prefix}_lulc_temp.csv"
    if temp_csv.exists():
        temp_csv.unlink()

    class_perc = class_percentages_from_histogram(histogram)
    id_name = histogram.index.name or "id"
    if id_name != id_feature:
        class_perc = class_perc.rename(columns={id_name: id_feature})
    logger.info("Class percentages computed for %d HRUs", class_perc[id_feature].nunique())

    # --- Step 2: Continuous zonal stats on canopy raster ---
    cnpy_path = Path(config["canopy_raster"])
    if not cnpy_path.exists():
        raise FileNotFoundError(f"Canopy raster not found: {cnpy_path}")
    cnpy_da = rioxarray.open_rasterio(cnpy_path, masked=True)
    logger.info("Loaded canopy raster: shape=%s", cnpy_da.shape)

    cnpy_data = UserTiffData(
        var="canopy", ds=cnpy_da, proj_ds=cnpy_da.rio.crs,
        x_coord="x", y_coord="y", band=1, bname="band",
        f_feature=nhru_gdf, id_feature=id_feature,
    )
    cnpy_zonal = ZonalGen(
        user_data=cnpy_data, zonal_engine="exactextract", zonal_writer="csv",
        out_path=output_dir, file_prefix=f"{file_prefix}_cnpy_temp", jobs=4,
    )
    cnpy_stats = cnpy_zonal.calculate_zonal(categorical=False)
    logger.info("Canopy continuous zonal stats computed")

    temp_csv = output_dir / f"{file_prefix}_cnpy_temp.csv"
    if temp_csv.exists():
        temp_csv.unlink()

    canopy_mean_df = cnpy_stats[["mean"]].rename(columns={"mean": "canopy_mean"})
    canopy_mean_df.index.name = id_feature
    canopy_mean_df = canopy_mean_df.reset_index()

    # --- Step 3: Retention (raster-based or crosswalk-based) ---
    keep_raster_str = config.get("keep_raster")
    if keep_raster_str:
        keep_path = Path(keep_raster_str)
        if not keep_path.exists():
            raise FileNotFoundError(f"Keep raster not found: {keep_path}")
        keep_da = rioxarray.open_rasterio(keep_path, masked=True)
        logger.info("Loaded keep raster: shape=%s", keep_da.shape)

        keep_data = UserTiffData(
            var="keep", ds=keep_da, proj_ds=keep_da.rio.crs,
            x_coord="x", y_coord="y", band=1, bname="band",
            f_feature=nhru_gdf, id_feature=id_feature,
        )
        keep_zonal = ZonalGen(
            user_data=keep_data, zonal_engine="exactextract", zonal_writer="csv",
            out_path=output_dir, file_prefix=f"{file_prefix}_keep_temp", jobs=4,
        )
        keep_stats = keep_zonal.calculate_zonal(categorical=False)
        logger.info("Keep raster zonal stats computed")

        temp_csv = output_dir / f"{file_prefix}_keep_temp.csv"
        if temp_csv.exists():
            temp_csv.unlink()

        # Keep raster values are 0-100; normalise to 0-1
        retention_df = keep_stats[["mean"]].rename(columns={"mean": "retention"})
        retention_df["retention"] = retention_df["retention"] * 0.01
        retention_df.index.name = id_feature
        retention_df = retention_df.reset_index()
        logger.info("Retention computed from keep raster (raster-based)")
    else:
        retention_df = compute_retention(class_perc, crosswalk, id_col=id_feature)
        logger.info("Retention computed from crosswalk evergreen_retention (crosswalk-based)")

    # --- Step 4: Compute parameters ---
    cov_type_df = assign_cov_type(class_perc, crosswalk, id_col=id_feature)
    logger.info("Cover types assigned")

    intcp_df = compute_interception(class_perc, crosswalk, id_col=id_feature)
    logger.info("Interception parameters computed")

    covden_df = compute_covden(class_perc, crosswalk, canopy_mean_df, id_col=id_feature)
    logger.info("Cover density parameters computed")

    # --- Step 5: Merge and write ---
    expected_hrus = class_perc[id_feature].nunique()
    result = (
        cov_type_df
        .merge(intcp_df, on=id_feature)
        .merge(covden_df, on=id_feature)
        .merge(retention_df, on=id_feature)
    )
    if len(result) != expected_hrus:
        logger.warning(
            "Row count mismatch after merge: expected %d HRUs, got %d. "
            "Some HRUs may have been dropped.",
            expected_hrus,
            len(result),
        )
    result = result.sort_values(id_feature).set_index(id_feature)

    result_csv = output_dir / f"{file_prefix}.csv"
    result.to_csv(result_csv)
    logger.info("LULC parameters saved to: %s (%d HRUs)", result_csv, len(result))


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

    k_perm_min = config["k_perm_min"]
    w["k_perm"] = w["k_perm"].replace(0, k_perm_min)
    w["k_perm_actual"] = 10 ** w["k_perm"]
    w["k_perm_wtd_sum"] = w["k_perm_actual"] * (w["area_weight"] / w["flux_id_area"])

    extensive_agg = (
        w.groupby(id_feature)
        .agg(k_perm_wtd=("k_perm_wtd_sum", "sum"))
        .reset_index()
    )
    extensive_agg[id_feature] = extensive_agg[id_feature].astype(int)
    extensive_sorted = extensive_agg.sort_values(by=id_feature).reset_index(drop=True)

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

    df = extensive_sorted.merge(slope_merge, on=id_feature, how="left").copy()
    df = df.merge(area_df, on=id_feature, how="left")

    null_slope = df["mean_slope_fraction"].isna().sum()
    null_area = df["hru_area"].isna().sum()
    if null_slope > 0 or null_area > 0:
        raise ValueError(
            f"Merge produced missing values: {null_slope} features missing slope, "
            f"{null_area} features missing area. Check that slope and batch data "
            f"use consistent {id_feature} values."
        )

    df["r_soil2gw_max"] = df["k_perm_wtd"] ** 3
    df["r_ssr2gw_rate"] = df["k_perm_wtd"] * (1 - df["mean_slope_fraction"])
    df["r_slowcoef_lin"] = (df["k_perm_wtd"] * df["mean_slope_fraction"]) / df["hru_area"]
    df["r_fastcoef_lin"] = 2 * df["r_slowcoef_lin"]
    df["r_gwflow_coef"] = df["r_slowcoef_lin"]
    df["r_dprst_seep_rate_open"] = df["r_ssr2gw_rate"]
    df["r_dprst_flow_coef"] = df["r_fastcoef_lin"]

    # Normalisation is per-batch (not CONUS-wide), matching prior per-VPU
    # behaviour. The same raw value may map to slightly different normalised
    # values across batches because per-batch min/max ranges differ.
    flux_params = config["flux_params"]
    param_names = [fp["name"] for fp in flux_params]
    param_maxes = [fp["max"] for fp in flux_params]
    param_mins = [fp["min"] for fp in flux_params]

    df_r = df[[f"r_{p}" for p in param_names]].agg(["min", "max"])
    df_r.loc["range"] = df_r.loc["max"] - df_r.loc["min"]

    for i, p in enumerate(param_names):
        rcol = f"r_{p}"
        min_in, rng_in = df_r.at["min", rcol], df_r.at["range", rcol]
        min_out, max_out = param_mins[i], param_maxes[i]
        rng_out = max_out - min_out
        if rng_in == 0:
            logger.warning("Range is zero for %s; using midpoint of output range", p)
            df[p] = (min_out + max_out) / 2.0
        else:
            norm = (df[rcol] - min_in) / rng_in
            df[p] = norm * rng_out + min_out

    df.drop(columns=[f"r_{p}" for p in param_names], inplace=True)

    ssflux_dir = output_dir / "ssflux"
    ssflux_dir.mkdir(parents=True, exist_ok=True)
    file_prefix = f"base_nhm_ssflux_{fabric}_batch_{batch_id:04d}_param"
    df.to_csv(ssflux_dir / f"{file_prefix}.csv", index=False)
    logger.info("SSFlux parameters saved (batch %d)", batch_id)


# ---------------------------------------------------------------------------
# CONUS-once worker (ssflux prereq)
# ---------------------------------------------------------------------------

def run_build_weights(config: dict, logger, force: bool = False) -> None:
    """Pre-compute the CONUS-wide P2P weight matrix that ssflux consumes.

    Originally extracted from the now-retired scripts/build_weights.py
    (see PR #85). One CSV per fabric, written
    to ``config['weight_dir']/lith_weights_<fabric>.csv``. Idempotent: skips
    if the file exists unless force=True.

    The target fabric is read from ``config['hru_gpkg']``/``hru_layer`` (the
    active base_config.yml profile, threaded in via _build_param_cfg) — the
    single source of truth, not a {fabric}_nhru_merged.gpkg naming convention.
    """
    fabric = config["fabric"]
    id_feature = config["id_feature"]
    hru_gpkg = Path(config["hru_gpkg"])
    hru_layer = config.get("hru_layer", "nhru")

    weight_dir = Path(config["weight_dir"])
    weight_dir.mkdir(parents=True, exist_ok=True)
    weight_file = weight_dir / f"lith_weights_{fabric}.csv"

    if weight_file.exists() and not force:
        logger.info("Weight file already exists: %s (use --force to overwrite)", weight_file)
        return

    if not hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {hru_gpkg}")
    target_gdf = gpd.read_file(hru_gpkg, layer=hru_layer)
    logger.info("Loaded target fabric: %d features", len(target_gdf))

    source_gdf = gpd.read_file(Path(config["source_shapefile"]))
    source_gdf["flux_id"] = np.arange(len(source_gdf))
    logger.info("Loaded lithology: %d features", len(source_gdf))

    logger.info("Computing P2P weights (this may take a while)...")
    weight_gen = WeightGenP2P(
        target_poly=target_gdf,
        target_poly_idx=id_feature,
        source_poly=source_gdf,
        source_poly_idx="flux_id",
        method="serial",
        weight_gen_crs="5070",
        output_file=weight_file,
    )
    weights = weight_gen.calculate_weights()
    if weights is None or len(weights) == 0:
        raise RuntimeError(
            "WeightGenP2P returned no weights. Check that target and source "
            "polygons overlap spatially."
        )
    if not weight_file.exists():
        raise RuntimeError(f"WeightGenP2P did not write output file: {weight_file}")
    logger.info("Weights computed: %d rows -> %s", len(weights), weight_file)


# ---------------------------------------------------------------------------
# Merge worker (any param type)
# ---------------------------------------------------------------------------

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

    output_path = final_output_dir / merged_file
    merged_df.to_csv(output_path, index=False)
    logger.info("Merged %d rows -> %s", len(merged_df), output_path)
