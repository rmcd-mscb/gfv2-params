"""Per-batch LULC parameters from pre-derived rasters (faithful NHM v1.1).

Not to be confused with ``gfv2_params.zonal_runners.lulc`` (the crosswalk-driven
runner). This module reproduces the NHM v1.1 ArcPy ``3_coverDen.py`` (Wieczorek
& Bock, 2021): each parameter is a direct zonal statistic of a pre-derived
raster shipped in the ScienceBase P971JAGF release, so it carries none of the
5-class crosswalk's collapse error or the keep-vs-loss covden_win mistake.

Per HRU:
  - cov_type     categorical zonal stats on LULC.tif -> 5-class decision tree
                 (``assign_cov_type``; the crosswalk supplies only the
                 lu_code -> nhm_cov_type mapping)
  - covden_sum   zonal mean of CNPY.tif / 100, zeroed where cov_type == 0
  - covden_win   covden_sum * (1 - zonal_mean(loss.tif)/100)
  - srain_intcp  zonal mean of SRain.tif / 100   (hundredths of inch -> inch)
  - wrain_intcp  zonal mean of WRain.tif / 100
  - snow_intcp   zonal mean of Snow.tif  / 100
  - rad_trncf    Beer's-law transform of zonal_mean(radtrn raster) (compute_rad_trncf)

There is no ``retention`` column: it was only ever a stand-in for rad_trncf,
which this path computes directly.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from ..lulc import (
    assign_cov_type,
    class_percentages_from_histogram,
    compute_rad_trncf,
    covden_win_from_loss,
    load_crosswalk,
)


def _zonal(raster_path, nhru_gdf, id_feature, output_dir, prefix, var, *, categorical):
    """Run one zonal pass over ``raster_path`` and return the result frame.

    Cleans up the temp CSV ZonalGen writes. Returns the raw ZonalGen frame
    (indexed by HRU id); callers pull ``mean`` (continuous) or the category
    histogram columns (categorical).
    """
    raster_path = Path(raster_path)
    if not raster_path.exists():
        raise FileNotFoundError(f"{var} raster not found: {raster_path}")
    # NB on nodata: masked=True relies on each GeoTIFF's declared nodata tag.
    # The staged interception/loss rasters carry one (Snow/SRain/WRain=15,
    # loss/keep Int8=-128), so sentinels are masked. The derived radtrn raster
    # legitimately has no nodata tag — its non-tree 0s are valid data that must
    # count in the mean — so we must NOT blanket-require a nodata tag here.
    da = rioxarray.open_rasterio(raster_path, masked=True)
    data = UserTiffData(
        var=var,
        ds=da,
        proj_ds=da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )
    zonal = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=f"{prefix}_{var}_temp",
        jobs=4,
    )
    result = zonal.calculate_zonal(categorical=categorical)
    temp_csv = output_dir / f"{prefix}_{var}_temp.csv"
    if temp_csv.exists():
        temp_csv.unlink()
    return result


def _zonal_mean_col(raster_path, nhru_gdf, id_feature, output_dir, prefix, var, scale=1.0):
    """Per-HRU zonal mean of a continuous raster as a 2-col frame [id, var].

    ``scale`` multiplies the mean (e.g. 0.01 to convert hundredths-of-inch
    interception rasters or 0-100 canopy percent to a 0-1 fraction). HRUs with
    no valid (non-nodata) pixels get NaN here; the caller decides fill policy.
    """
    stats = _zonal(raster_path, nhru_gdf, id_feature, output_dir, prefix, var, categorical=False)
    out = stats[["mean"]].rename(columns={"mean": var})
    out[var] = out[var] * scale
    out.index.name = id_feature
    return out.reset_index()


def run_lulc_prederived_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of faithful (pre-derived-raster) LULC parameters.

    Config keys: source_raster (LULC), canopy_raster (CNPY), loss_raster,
    snow_raster, srain_raster, wrain_raster, radtrn_raster, crosswalk_file
    (cov_type mapping only), plus the usual batch_dir/output_dir/target_layer/
    id_feature/fabric/source_type.
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
        crosswalk_path = Path(__file__).resolve().parents[3] / crosswalk_path
    crosswalk = load_crosswalk(crosswalk_path)

    prefix = f"base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param"

    # --- cov_type: categorical zonal on LULC -> 5-class decision tree ---
    histogram = _zonal(config["source_raster"], nhru_gdf, id_feature, output_dir, prefix, "lulc", categorical=True)
    class_perc = class_percentages_from_histogram(histogram)
    id_name = histogram.index.name or "id"
    if id_name != id_feature:
        class_perc = class_perc.rename(columns={id_name: id_feature})
    cov_type_df = assign_cov_type(class_perc, crosswalk, id_col=id_feature)
    logger.info("cov_type assigned for %d HRUs", len(cov_type_df))

    # --- covden_sum: zonal mean of canopy / 100, zeroed where cov_type == 0 ---
    covden_sum_df = _zonal_mean_col(
        config["canopy_raster"], nhru_gdf, id_feature, output_dir, prefix, "covden_sum", scale=0.01
    )
    covden_sum_df = covden_sum_df.merge(cov_type_df, on=id_feature, how="left")
    covden_sum_df.loc[covden_sum_df["cov_type"] == 0, "covden_sum"] = 0.0
    covden_sum_df = covden_sum_df[[id_feature, "covden_sum"]]

    # --- covden_win: covden_sum * (1 - loss/100) ---
    # NB: do NOT zero-fill a missing loss mean. Unlike interception (where the
    # legacy sets missing -> 0), loss=0 means "no leaf loss" -> covden_win =
    # covden_sum, i.e. FULL winter canopy — the max-impact wrong value. A NaN
    # loss mean signals an HRU with no loss-raster overlap (CRS/extent bug, or
    # an edge HRU), so leave covden_win NaN and let the downstream KNN gap-fill
    # (PR #134) handle it, matching the legacy null -> CL_HRU fill.
    loss_df = _zonal_mean_col(config["loss_raster"], nhru_gdf, id_feature, output_dir, prefix, "loss")
    covden_win_df = covden_sum_df.merge(loss_df, on=id_feature, how="left")
    no_loss = covden_win_df["loss"].isna() & (covden_win_df["covden_sum"] > 0)
    if no_loss.any():
        logger.warning(
            "%d HRUs have canopy but no loss-raster overlap; covden_win left NaN for gap-fill",
            int(no_loss.sum()),
        )
    covden_win_df["covden_win"] = covden_win_from_loss(covden_win_df["covden_sum"], covden_win_df["loss"])
    covden_win_df = covden_win_df[[id_feature, "covden_win"]]
    logger.info("covden_sum / covden_win computed")

    # --- interception: zonal mean / 100 (hundredths of inch -> inch) ---
    intcp_specs = [
        ("snow_raster", "snow_intcp"),
        ("srain_raster", "srain_intcp"),
        ("wrain_raster", "wrain_intcp"),
    ]
    intcp_dfs = []
    for cfg_key, col in intcp_specs:
        df = _zonal_mean_col(config[cfg_key], nhru_gdf, id_feature, output_dir, prefix, col, scale=0.01)
        df[col] = df[col].fillna(0.0)  # HRUs with no valid pixels -> 0 (legacy)
        intcp_dfs.append(df)
    logger.info("interception parameters computed")

    # --- rad_trncf: Beer's-law transform of the radtrn zonal mean ---
    rad_trncf_df = _zonal_mean_col(config["radtrn_raster"], nhru_gdf, id_feature, output_dir, prefix, "rad_trncf")
    rad_trncf_df["rad_trncf"] = compute_rad_trncf(rad_trncf_df["rad_trncf"])
    logger.info("rad_trncf computed")

    # --- merge + write ---
    result = cov_type_df.merge(covden_sum_df, on=id_feature).merge(covden_win_df, on=id_feature)
    for df in intcp_dfs:
        result = result.merge(df, on=id_feature)
    result = result.merge(rad_trncf_df, on=id_feature)

    # Diagnose dropped HRUs against the batch geometry (the true baseline), not
    # cov_type_df: an HRU with no LULC pixels is absent from cov_type_df and
    # would silently vanish from the param table. The inner merges can also drop
    # an HRU missing from any per-param frame. Report ids so a CRS/extent bug is
    # visible rather than swallowed (the downstream gap-fill backfills absent
    # rows, but only if we know they were dropped here vs. genuinely missing).
    dropped = set(nhru_gdf[id_feature]) - set(result[id_feature])
    if dropped:
        logger.warning(
            "%d/%d HRUs absent from LULC output (no LULC pixels or merge loss): %s%s",
            len(dropped),
            len(nhru_gdf),
            sorted(dropped)[:20],
            " ..." if len(dropped) > 20 else "",
        )
    result = result.sort_values(id_feature).set_index(id_feature)

    result_csv = output_dir / f"{prefix}.csv"
    result.to_csv(result_csv)
    logger.info("LULC (pre-derived) parameters saved to: %s (%d HRUs)", result_csv, len(result))
