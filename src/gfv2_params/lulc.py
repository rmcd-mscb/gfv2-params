"""LULC parameterization: crosswalk-mediated cover type and interception parameters."""

from pathlib import Path

import numpy as np
import pandas as pd

REQUIRED_CROSSWALK_COLUMNS = {
    "lu_code",
    "lu_desc",
    "nhm_cov_type",
    "srain_intcp",
    "wrain_intcp",
    "snow_intcp",
    "nhm_covden_win",
    "evergreen_retention",
}


def load_crosswalk(crosswalk_path: Path) -> pd.DataFrame:
    """Load and validate a crosswalk CSV.

    Parameters
    ----------
    crosswalk_path : Path
        Path to crosswalk CSV with columns: lu_code, lu_desc, nhm_cov_type,
        srain_intcp, wrain_intcp, snow_intcp, nhm_covden_win, evergreen_retention.

    Returns
    -------
    pd.DataFrame
        Validated crosswalk with lu_code as index.

    Raises
    ------
    ValueError
        If required columns are missing or lu_code values are not unique.
    """
    df = pd.read_csv(crosswalk_path)
    missing = REQUIRED_CROSSWALK_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Crosswalk missing required columns: {sorted(missing)}")
    if df["lu_code"].duplicated().any():
        raise ValueError("Crosswalk contains duplicate lu_code values")
    return df.set_index("lu_code")


def class_percentages_from_histogram(histogram_df: pd.DataFrame) -> pd.DataFrame:
    """Convert categorical zonal histogram to per-class percentage per HRU.

    Parameters
    ----------
    histogram_df : pd.DataFrame
        Output from ZonalGen.calculate_zonal(categorical=True).
        Columns are category codes (as strings); index is HRU id.
        Values are pixel counts per category per HRU.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with columns: [id, lu_code, perc].
        ``id`` matches the original index name. ``perc`` is 0-100.
    """
    id_name = histogram_df.index.name or "id"
    # Drop non-category columns (e.g. "count") if present
    category_cols = [c for c in histogram_df.columns if c != "count"]
    counts = histogram_df[category_cols].copy()

    # Normalise to percentages per HRU
    row_totals = counts.sum(axis=1)
    pct = counts.div(row_totals, axis=0) * 100.0

    # Pivot to long format
    pct.index.name = id_name
    long = pct.reset_index().melt(id_vars=id_name, var_name="lu_code", value_name="perc")
    long["lu_code"] = long["lu_code"].astype(int)
    # Drop zero-percentage rows
    long = long[long["perc"] > 0].reset_index(drop=True)
    return long


def assign_cov_type(class_perc_df: pd.DataFrame, crosswalk: pd.DataFrame, id_col: str = "nat_hru_id") -> pd.DataFrame:
    """Assign dominant NHM cover type per HRU via decision tree.

    Decision tree (applied per HRU, in priority order):
      1. bare/developed (nhm_cov_type=0) >= 90%  ->  cov_type = 0
      2. trees          (nhm_cov_type=3) >= 20%  ->  cov_type = 3
      3. shrub          (nhm_cov_type=2) >= 20%  ->  cov_type = 2
      4. shrub + trees combined          >= 35%  ->  whichever is greater
      5. grass          (nhm_cov_type=1) >= 50%  ->  cov_type = 1
      6. else  ->  nhm_cov_type with highest total percentage

    Parameters
    ----------
    class_perc_df : pd.DataFrame
        Long-format with columns [<id_col>, lu_code, perc].
    crosswalk : pd.DataFrame
        Crosswalk with lu_code as index and nhm_cov_type column.
    id_col : str
        Name of the HRU identifier column.

    Returns
    -------
    pd.DataFrame
        Columns: [<id_col>, cov_type]. One row per HRU.
    """
    # Merge class percentages with cover types
    merged = class_perc_df.merge(
        crosswalk[["nhm_cov_type"]], left_on="lu_code", right_index=True, how="left",
    )
    # Aggregate: total percentage per (HRU, nhm_cov_type)
    agg = merged.groupby([id_col, "nhm_cov_type"])["perc"].sum().reset_index()

    all_hrus = agg[id_col].unique()
    assigned = {}

    for hru_id in all_hrus:
        hru = agg[agg[id_col] == hru_id].set_index("nhm_cov_type")["perc"]

        bare = hru.get(0, 0.0)
        grass = hru.get(1, 0.0)
        shrub = hru.get(2, 0.0)
        tree = hru.get(3, 0.0)

        if bare >= 90.0:
            assigned[hru_id] = 0
        elif tree >= 20.0:
            assigned[hru_id] = 3
        elif shrub >= 20.0:
            assigned[hru_id] = 2
        elif (shrub + tree) >= 35.0:
            assigned[hru_id] = 3 if tree >= shrub else 2
        elif grass >= 50.0:
            assigned[hru_id] = 1
        else:
            assigned[hru_id] = int(hru.idxmax())

    result = pd.DataFrame(
        {id_col: list(assigned.keys()), "cov_type": list(assigned.values())}
    )
    return result


def compute_interception(
    class_perc_df: pd.DataFrame,
    crosswalk: pd.DataFrame,
    id_col: str = "nat_hru_id",
) -> pd.DataFrame:
    """Compute weighted interception parameters per HRU.

    For each HRU: param = sum(perc/100 * coefficient) across all LULC classes.
    Classes with nhm_cov_type=0 contribute zero.

    Parameters
    ----------
    class_perc_df : pd.DataFrame
        Long-format with columns [<id_col>, lu_code, perc].
    crosswalk : pd.DataFrame
        Crosswalk with srain_intcp, wrain_intcp, snow_intcp columns.
    id_col : str
        Name of the HRU identifier column.

    Returns
    -------
    pd.DataFrame
        Columns: [<id_col>, srain_intcp, wrain_intcp, snow_intcp].
    """
    intcp_cols = ["srain_intcp", "wrain_intcp", "snow_intcp"]
    merged = class_perc_df.merge(
        crosswalk[["nhm_cov_type"] + intcp_cols],
        left_on="lu_code",
        right_index=True,
        how="left",
    )

    # Zero out bare/developed classes
    bare_mask = merged["nhm_cov_type"] == 0
    for col in intcp_cols:
        merged.loc[bare_mask, col] = 0.0

    # Weighted sum
    for col in intcp_cols:
        merged[col] = merged["perc"] * 0.01 * merged[col]

    result = merged.groupby(id_col)[intcp_cols].sum().reset_index()
    return result


def compute_covden(
    class_perc_df: pd.DataFrame,
    crosswalk: pd.DataFrame,
    canopy_mean_df: pd.DataFrame,
    id_col: str = "nat_hru_id",
) -> pd.DataFrame:
    """Compute summer and winter canopy density per HRU.

    covden_sum = sum(perc/100 * canopy_mean/100) for non-bare classes.
    covden_win = sum(perc/100 * canopy_mean/100 * (1 - nhm_covden_win)).

    Parameters
    ----------
    class_perc_df : pd.DataFrame
        Long-format with columns [<id_col>, lu_code, perc].
    crosswalk : pd.DataFrame
        Crosswalk with nhm_cov_type and nhm_covden_win columns.
    canopy_mean_df : pd.DataFrame
        Columns: [<id_col>, canopy_mean]. Zonal mean of canopy raster per HRU.
    id_col : str
        Name of the HRU identifier column.

    Returns
    -------
    pd.DataFrame
        Columns: [<id_col>, covden_sum, covden_win].
    """
    merged = class_perc_df.merge(
        crosswalk[["nhm_cov_type", "nhm_covden_win"]],
        left_on="lu_code",
        right_index=True,
        how="left",
    )
    merged = merged.merge(canopy_mean_df, on=id_col, how="left")

    # Zero out bare/developed classes
    bare_mask = merged["nhm_cov_type"] == 0
    merged.loc[bare_mask, "canopy_mean"] = 0.0

    merged["covden_sum"] = merged["perc"] * 0.01 * merged["canopy_mean"] * 0.01
    merged["covden_win"] = merged["covden_sum"] * (1.0 - merged["nhm_covden_win"])

    result = merged.groupby(id_col)[["covden_sum", "covden_win"]].sum().reset_index()
    return result


def compute_retention(
    class_perc_df: pd.DataFrame,
    crosswalk: pd.DataFrame,
    id_col: str = "nat_hru_id",
) -> pd.DataFrame:
    """Compute per-HRU evergreen retention from crosswalk class values.

    This function synthesises a per-HRU retention metric when no ``keep``
    raster is available (e.g. NLCD, NALCMS).  The result is the
    area-weighted average of the crosswalk's ``evergreen_retention``
    column across all non-bare LULC classes present in each HRU:

        retention = sum(perc / 100 * evergreen_retention)

    where the sum runs over LULC classes with ``nhm_cov_type != 0``.
    Bare / developed classes contribute zero.

    The value ranges from 0.0 (fully deciduous HRU) to 1.0 (fully
    evergreen HRU) and can be used as a surrogate for the raster-derived
    ``keep`` mean when computing radiation transmission or other
    retention-dependent parameters.

    Parameters
    ----------
    class_perc_df : pd.DataFrame
        Long-format with columns [<id_col>, lu_code, perc].
    crosswalk : pd.DataFrame
        Crosswalk with ``nhm_cov_type`` and ``evergreen_retention``
        columns.  ``evergreen_retention`` values should be in [0, 1].
    id_col : str
        Name of the HRU identifier column.

    Returns
    -------
    pd.DataFrame
        Columns: [<id_col>, retention].  One row per HRU.
    """
    merged = class_perc_df.merge(
        crosswalk[["nhm_cov_type", "evergreen_retention"]],
        left_on="lu_code",
        right_index=True,
        how="left",
    )

    # Bare / developed classes contribute zero retention
    bare_mask = merged["nhm_cov_type"] == 0
    merged.loc[bare_mask, "evergreen_retention"] = 0.0

    merged["retention"] = merged["perc"] * 0.01 * merged["evergreen_retention"]

    result = merged.groupby(id_col)[["retention"]].sum().reset_index()
    return result
