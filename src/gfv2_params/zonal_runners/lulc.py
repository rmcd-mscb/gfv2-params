"""Per-batch LULC parameter derivation.

Not to be confused with ``gfv2_params.lulc`` (the crosswalk/computation helpers
that this module imports from).

One ``run_lulc_batch`` covers all four LULC source types (nhm_v11, nalcms,
nlcd, foresce) — the orchestrator normalises ``source_type`` to ``lulc_<source>``
so each source writes per-batch CSVs to its own subdir. Pipeline: categorical
zonal stats on the LULC raster, continuous zonal stats on the canopy raster,
optional zonal stats on a ``keep`` raster, optional zonal stats on a ``radtrn``
raster (-> rad_trncf), crosswalk lookup + cov_type assignment,
interception/covden/retention computation.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import rioxarray
from gdptools import UserTiffData, ZonalGen

from ..lulc import (
    assign_cov_type,
    class_percentages_from_histogram,
    compute_covden,
    compute_interception,
    compute_rad_trncf,
    compute_retention,
    load_crosswalk,
)


def run_lulc_batch(config: dict, batch_id: int, logger) -> None:
    """One HRU batch of LULC parameter derivation.

    Originally extracted from the now-retired scripts/create_lulc_params.py
    (see PR #85). Steps:
      1. categorical zonal stats on LULC raster -> class percentages
      2. continuous zonal stats on canopy raster -> canopy_mean per HRU
      3. retention: either zonal mean from keep raster (FORE-SCE / NHM v1.1)
         or crosswalk evergreen_retention (NLCD / NALCMS)
      3b. rad_trncf: when a radtrn_raster is configured, zonal-mean it and
         apply the Beer's-law transform (compute_rad_trncf)
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
        # is src/gfv2_params/zonal_runners/lulc.py -> parents[3] = repo root).
        crosswalk_path = Path(__file__).resolve().parents[3] / crosswalk_path
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
        var="lulc",
        ds=lulc_da,
        proj_ds=lulc_da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )
    lulc_zonal = ZonalGen(
        user_data=lulc_data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=f"{file_prefix}_lulc_temp",
        jobs=4,
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
        var="canopy",
        ds=cnpy_da,
        proj_ds=cnpy_da.rio.crs,
        x_coord="x",
        y_coord="y",
        band=1,
        bname="band",
        f_feature=nhru_gdf,
        id_feature=id_feature,
    )
    cnpy_zonal = ZonalGen(
        user_data=cnpy_data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=f"{file_prefix}_cnpy_temp",
        jobs=4,
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
            var="keep",
            ds=keep_da,
            proj_ds=keep_da.rio.crs,
            x_coord="x",
            y_coord="y",
            band=1,
            bname="band",
            f_feature=nhru_gdf,
            id_feature=id_feature,
        )
        keep_zonal = ZonalGen(
            user_data=keep_data,
            zonal_engine="exactextract",
            zonal_writer="csv",
            out_path=output_dir,
            file_prefix=f"{file_prefix}_keep_temp",
            jobs=4,
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

    # --- Step 3b: rad_trncf (winter-canopy radiation transmission coef) ---
    # When a radtrn raster is configured (cnpy*keep/100 over tree pixels, built
    # by the shared-rasters build_lulc_rasters step), zonal-mean it per HRU and
    # apply the NHM v1.1 Beer's-law transform (compute_rad_trncf). Sources with
    # no keep/radtrn raster (NLCD/NALCMS, pending a synthesized keep raster)
    # skip this column.
    rad_trncf_df = None
    radtrn_raster_str = config.get("radtrn_raster")
    if radtrn_raster_str:
        radtrn_path = Path(radtrn_raster_str)
        if not radtrn_path.exists():
            raise FileNotFoundError(f"radtrn raster not found: {radtrn_path}")
        radtrn_da = rioxarray.open_rasterio(radtrn_path, masked=True)
        logger.info("Loaded radtrn raster: shape=%s", radtrn_da.shape)

        radtrn_data = UserTiffData(
            var="radtrn",
            ds=radtrn_da,
            proj_ds=radtrn_da.rio.crs,
            x_coord="x",
            y_coord="y",
            band=1,
            bname="band",
            f_feature=nhru_gdf,
            id_feature=id_feature,
        )
        radtrn_zonal = ZonalGen(
            user_data=radtrn_data,
            zonal_engine="exactextract",
            zonal_writer="csv",
            out_path=output_dir,
            file_prefix=f"{file_prefix}_radtrn_temp",
            jobs=4,
        )
        radtrn_stats = radtrn_zonal.calculate_zonal(categorical=False)
        logger.info("radtrn raster zonal stats computed")

        temp_csv = output_dir / f"{file_prefix}_radtrn_temp.csv"
        if temp_csv.exists():
            temp_csv.unlink()

        # radtrn zonal mean is the per-HRU winter-canopy density (0-100).
        rad_trncf_df = radtrn_stats[["mean"]].rename(columns={"mean": "rad_trncf"})
        rad_trncf_df["rad_trncf"] = compute_rad_trncf(rad_trncf_df["rad_trncf"])
        rad_trncf_df.index.name = id_feature
        rad_trncf_df = rad_trncf_df.reset_index()
        logger.info("rad_trncf computed from radtrn raster (Beer's-law transform)")

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
        cov_type_df.merge(intcp_df, on=id_feature).merge(covden_df, on=id_feature).merge(retention_df, on=id_feature)
    )
    if rad_trncf_df is not None:
        result = result.merge(rad_trncf_df, on=id_feature)
    if len(result) != expected_hrus:
        logger.warning(
            "Row count mismatch after merge: expected %d HRUs, got %d. Some HRUs may have been dropped.",
            expected_hrus,
            len(result),
        )
    result = result.sort_values(id_feature).set_index(id_feature)

    result_csv = output_dir / f"{file_prefix}.csv"
    result.to_csv(result_csv)
    logger.info("LULC parameters saved to: %s (%d HRUs)", result_csv, len(result))
