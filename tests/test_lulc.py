import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from gfv2_params.lulc import (
    REQUIRED_CROSSWALK_COLUMNS,
    assign_cov_type,
    class_percentages_from_histogram,
    compute_covden,
    compute_interception,
    compute_retention,
    load_crosswalk,
)


@pytest.fixture
def crosswalk_path(tmp_path):
    """Write a minimal valid crosswalk CSV and return its path."""
    csv = tmp_path / "cw.csv"
    csv.write_text(
        "lu_code,lu_desc,nhm_cov_type,srain_intcp,wrain_intcp,snow_intcp,nhm_covden_win,evergreen_retention\n"
        "0,NoData,0,0.0,0.0,0.0,0.0,-1\n"
        "1,Grass,1,0.02,0.02,0.02,0.0,-1\n"
        "2,Shrub,2,0.03,0.03,0.03,0.3,-1\n"
        "3,Deciduous,3,0.05,0.02,0.05,0.5,-1\n"
        "4,Evergreen,3,0.05,0.05,0.05,0.0,-1\n"
    )
    return csv


@pytest.fixture
def crosswalk(crosswalk_path):
    return load_crosswalk(crosswalk_path)


# --- load_crosswalk ---


def test_load_crosswalk_valid(crosswalk_path):
    cw = load_crosswalk(crosswalk_path)
    assert cw.index.name == "lu_code"
    assert len(cw) == 5
    assert set(REQUIRED_CROSSWALK_COLUMNS) - {"lu_code"} <= set(cw.columns)


def test_load_crosswalk_missing_column(tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("lu_code,lu_desc\n0,NoData\n")
    with pytest.raises(ValueError, match="missing required columns"):
        load_crosswalk(csv)


def test_load_crosswalk_duplicate_lu_code(tmp_path):
    csv = tmp_path / "dup.csv"
    csv.write_text(
        "lu_code,lu_desc,nhm_cov_type,srain_intcp,wrain_intcp,snow_intcp,nhm_covden_win,evergreen_retention\n"
        "0,A,0,0,0,0,0,-1\n"
        "0,B,1,0,0,0,0,-1\n"
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_crosswalk(csv)


# --- class_percentages_from_histogram ---


def test_class_percentages_from_histogram():
    hist = pd.DataFrame(
        {"0": [10, 0], "1": [30, 50], "3": [60, 50]},
        index=pd.Index([101, 102], name="nat_hru_id"),
    )
    result = class_percentages_from_histogram(hist)
    assert set(result.columns) == {"nat_hru_id", "lu_code", "perc"}

    hru101 = result[result["nat_hru_id"] == 101].set_index("lu_code")["perc"]
    assert hru101[0] == pytest.approx(10.0)
    assert hru101[1] == pytest.approx(30.0)
    assert hru101[3] == pytest.approx(60.0)

    hru102 = result[result["nat_hru_id"] == 102].set_index("lu_code")["perc"]
    assert 0 not in hru102.index  # zero-count classes excluded
    assert hru102[1] == pytest.approx(50.0)
    assert hru102[3] == pytest.approx(50.0)


def test_class_percentages_zero_pixels():
    hist = pd.DataFrame(
        {"0": [0], "1": [0]},
        index=pd.Index([101], name="nat_hru_id"),
    )
    result = class_percentages_from_histogram(hist)
    assert len(result) == 0


# --- assign_cov_type ---


def test_assign_cov_type_bare_dominant(crosswalk):
    # 95% bare, 5% grass
    perc = pd.DataFrame({"nat_hru_id": [1, 1], "lu_code": [0, 1], "perc": [95.0, 5.0]})
    result = assign_cov_type(perc, crosswalk)
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 0


def test_assign_cov_type_tree_20pct(crosswalk):
    # 25% deciduous(3), 15% shrub(2), 60% grass(1) -> tree wins at 20% threshold
    perc = pd.DataFrame({"nat_hru_id": [1, 1, 1], "lu_code": [3, 2, 1], "perc": [25.0, 15.0, 60.0]})
    result = assign_cov_type(perc, crosswalk)
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 3


def test_assign_cov_type_shrub_20pct(crosswalk):
    # 5% deciduous(3), 25% shrub(2), 70% grass(1) -> shrub wins
    perc = pd.DataFrame({"nat_hru_id": [1, 1, 1], "lu_code": [3, 2, 1], "perc": [5.0, 25.0, 70.0]})
    result = assign_cov_type(perc, crosswalk)
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 2


def test_assign_cov_type_shrub_tree_combined(crosswalk):
    # 18% deciduous(3) + 18% shrub(2) = 36% >= 35%, shrub higher -> cov_type=2
    perc = pd.DataFrame(
        {"nat_hru_id": [1, 1, 1], "lu_code": [3, 2, 1], "perc": [17.0, 19.0, 64.0]}
    )
    result = assign_cov_type(perc, crosswalk)
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 2


def test_assign_cov_type_shrub_tree_combined_tree_higher(crosswalk):
    # 19% deciduous(3) + 17% shrub(2) = 36% >= 35%, tree higher -> cov_type=3
    perc = pd.DataFrame(
        {"nat_hru_id": [1, 1, 1], "lu_code": [3, 2, 1], "perc": [19.0, 17.0, 64.0]}
    )
    result = assign_cov_type(perc, crosswalk)
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 3


def test_assign_cov_type_grass_50pct(crosswalk):
    # 5% bare(0), 10% shrub(2), 5% tree(3), 80% grass(1) -> grass wins at 50%
    perc = pd.DataFrame(
        {"nat_hru_id": [1, 1, 1, 1], "lu_code": [0, 2, 3, 1], "perc": [5.0, 10.0, 5.0, 80.0]}
    )
    result = assign_cov_type(perc, crosswalk)
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 1


def test_assign_cov_type_fallback_max(crosswalk):
    # 40% bare(0), 10% tree(3), 10% shrub(2), 40% grass(1)
    # No rule triggers: bare < 90, tree < 20, shrub < 20, tree+shrub < 35, grass < 50
    # Fallback: bare and grass tied at 40%, picks whichever idxmax returns
    perc = pd.DataFrame(
        {"nat_hru_id": [1, 1, 1, 1], "lu_code": [0, 3, 2, 1], "perc": [35.0, 10.0, 10.0, 45.0]}
    )
    result = assign_cov_type(perc, crosswalk)
    # grass has 45% which is highest -> cov_type=1
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 1


def test_assign_cov_type_multiple_hrus(crosswalk):
    perc = pd.DataFrame({
        "nat_hru_id": [1, 1, 2, 2],
        "lu_code": [0, 1, 3, 1],
        "perc": [95.0, 5.0, 30.0, 70.0],
    })
    result = assign_cov_type(perc, crosswalk)
    assert len(result) == 2
    assert result.loc[result["nat_hru_id"] == 1, "cov_type"].iloc[0] == 0
    assert result.loc[result["nat_hru_id"] == 2, "cov_type"].iloc[0] == 3


# --- compute_interception ---


def test_compute_interception(crosswalk):
    perc = pd.DataFrame({
        "nat_hru_id": [1, 1],
        "lu_code": [1, 3],  # grass(srain=0.02) and deciduous(srain=0.05)
        "perc": [60.0, 40.0],
    })
    result = compute_interception(perc, crosswalk)
    # srain = 0.60*0.02 + 0.40*0.05 = 0.012 + 0.020 = 0.032
    assert result.loc[0, "srain_intcp"] == pytest.approx(0.032)
    # wrain = 0.60*0.02 + 0.40*0.02 = 0.012 + 0.008 = 0.020
    assert result.loc[0, "wrain_intcp"] == pytest.approx(0.020)
    # snow = 0.60*0.02 + 0.40*0.05 = 0.012 + 0.020 = 0.032
    assert result.loc[0, "snow_intcp"] == pytest.approx(0.032)


def test_compute_interception_bare_zero(crosswalk):
    perc = pd.DataFrame({
        "nat_hru_id": [1, 1],
        "lu_code": [0, 1],  # bare + grass
        "perc": [50.0, 50.0],
    })
    result = compute_interception(perc, crosswalk)
    # Only grass contributes: 0.50*0.02 = 0.01
    assert result.loc[0, "srain_intcp"] == pytest.approx(0.01)


# --- compute_covden ---


def test_compute_covden(crosswalk):
    perc = pd.DataFrame({
        "nat_hru_id": [1, 1],
        "lu_code": [3, 4],  # deciduous(covden_win=0.5) + evergreen(covden_win=0.0)
        "perc": [50.0, 50.0],
    })
    canopy = pd.DataFrame({"nat_hru_id": [1], "canopy_mean": [80.0]})
    result = compute_covden(perc, crosswalk, canopy)

    # covden_sum = 0.50 * 0.80 + 0.50 * 0.80 = 0.40 + 0.40 = 0.80
    assert result.loc[0, "covden_sum"] == pytest.approx(0.80)
    # covden_win:
    #   deciduous: 0.40 * (1 - 0.5) = 0.20
    #   evergreen: 0.40 * (1 - 0.0) = 0.40
    #   total = 0.60
    assert result.loc[0, "covden_win"] == pytest.approx(0.60)


def test_compute_covden_bare_zero(crosswalk):
    perc = pd.DataFrame({
        "nat_hru_id": [1],
        "lu_code": [0],  # bare
        "perc": [100.0],
    })
    canopy = pd.DataFrame({"nat_hru_id": [1], "canopy_mean": [50.0]})
    result = compute_covden(perc, crosswalk, canopy)
    assert result.loc[0, "covden_sum"] == pytest.approx(0.0)
    assert result.loc[0, "covden_win"] == pytest.approx(0.0)


# --- compute_retention ---


def test_compute_retention_mixed():
    cw = pd.DataFrame({
        "lu_code": [1, 4],
        "lu_desc": ["Grass", "Evergreen"],
        "nhm_cov_type": [1, 3],
        "srain_intcp": [0.02, 0.05],
        "wrain_intcp": [0.02, 0.05],
        "snow_intcp": [0.02, 0.05],
        "nhm_covden_win": [0.0, 0.0],
        "evergreen_retention": [0.0, 1.0],
    }).set_index("lu_code")

    perc = pd.DataFrame({
        "nat_hru_id": [1, 1],
        "lu_code": [1, 4],
        "perc": [60.0, 40.0],
    })
    result = compute_retention(perc, cw)
    # retention = 0.60*0.0 + 0.40*1.0 = 0.40
    assert result.loc[0, "retention"] == pytest.approx(0.40)


def test_compute_retention_all_evergreen():
    cw = pd.DataFrame({
        "lu_code": [42],
        "lu_desc": ["Evergreen_Forest"],
        "nhm_cov_type": [3],
        "srain_intcp": [0.05],
        "wrain_intcp": [0.05],
        "snow_intcp": [0.05],
        "nhm_covden_win": [0.0],
        "evergreen_retention": [1.0],
    }).set_index("lu_code")

    perc = pd.DataFrame({"nat_hru_id": [1], "lu_code": [42], "perc": [100.0]})
    result = compute_retention(perc, cw)
    assert result.loc[0, "retention"] == pytest.approx(1.0)


def test_compute_retention_bare_contributes_zero():
    cw = pd.DataFrame({
        "lu_code": [0, 42],
        "lu_desc": ["Bare", "Evergreen"],
        "nhm_cov_type": [0, 3],
        "srain_intcp": [0.0, 0.05],
        "wrain_intcp": [0.0, 0.05],
        "snow_intcp": [0.0, 0.05],
        "nhm_covden_win": [0.0, 0.0],
        "evergreen_retention": [0.5, 1.0],  # bare has 0.5 but should be zeroed out
    }).set_index("lu_code")

    perc = pd.DataFrame({
        "nat_hru_id": [1, 1],
        "lu_code": [0, 42],
        "perc": [50.0, 50.0],
    })
    result = compute_retention(perc, cw)
    # bare zeroed out: 0.50*0.0 + 0.50*1.0 = 0.50
    assert result.loc[0, "retention"] == pytest.approx(0.50)


def test_compute_retention_mixed_forest():
    """NLCD-style: deciduous=0.0, evergreen=1.0, mixed=0.5."""
    cw = pd.DataFrame({
        "lu_code": [41, 42, 43],
        "lu_desc": ["Deciduous", "Evergreen", "Mixed"],
        "nhm_cov_type": [3, 3, 3],
        "srain_intcp": [0.05, 0.05, 0.05],
        "wrain_intcp": [0.02, 0.05, 0.035],
        "snow_intcp": [0.05, 0.05, 0.05],
        "nhm_covden_win": [0.5, 0.0, 0.25],
        "evergreen_retention": [0.0, 1.0, 0.5],
    }).set_index("lu_code")

    perc = pd.DataFrame({
        "nat_hru_id": [1, 1, 1],
        "lu_code": [41, 42, 43],
        "perc": [30.0, 40.0, 30.0],
    })
    result = compute_retention(perc, cw)
    # retention = 0.30*0.0 + 0.40*1.0 + 0.30*0.5 = 0.0 + 0.40 + 0.15 = 0.55
    assert result.loc[0, "retention"] == pytest.approx(0.55)
