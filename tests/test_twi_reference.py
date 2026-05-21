"""Unit tests for the pure percentile / CDF-inversion helpers used to derive
TWI threshold cutoffs (issue #55 Stage 1)."""

import numpy as np
import pytest

from gfv2_params.shared_rasters.twi_reference import (
    assemble_reference_table,
    percentile_of_values,
    rank_of_value,
    raster_vpus,
)


def test_percentile_of_values_basic():
    vals = np.arange(1, 101, dtype="float64")  # 1..100
    p = percentile_of_values(vals, [75.0, 95.0])
    assert p[0] == pytest.approx(75.25, abs=0.5)
    assert p[1] == pytest.approx(95.05, abs=0.5)


def test_percentile_drops_nan_and_nodata():
    vals = np.array([1.0, 2.0, 3.0, 4.0, np.nan, -9999.0], dtype="float64")
    p = percentile_of_values(vals, [50.0], nodata=-9999.0)
    # median of {1,2,3,4} == 2.5
    assert p[0] == pytest.approx(2.5)


def test_rank_of_value_is_inverse_of_percentile():
    vals = np.arange(1, 101, dtype="float64")
    # value 75 sits at ~the 75th percentile
    assert rank_of_value(vals, 75.0) == pytest.approx(75.0, abs=1.0)


def test_rank_of_value_handles_nodata():
    vals = np.array([1.0, 2.0, 3.0, 4.0, -9999.0], dtype="float64")
    # 3.0 is >= 3 of 4 valid values -> 75%
    assert rank_of_value(vals, 3.0, nodata=-9999.0) == pytest.approx(75.0)


def test_percentile_empty_raises():
    with pytest.raises(ValueError, match="no valid"):
        percentile_of_values(np.array([-9999.0, np.nan]), [50.0], nodata=-9999.0)


def test_assemble_reference_table_uses_inverted_defaults():
    # Fake per-VPU valid-land TWI samples for two VPUs of one source.
    samples = {
        "01": np.arange(1, 101, dtype="float64"),       # 1..100
        "17": np.arange(1, 201, dtype="float64") / 2.0,  # 0.5..100
    }

    def sampler(vpu):
        return samples[vpu]

    rows = assemble_reference_table(
        source="hydrodem",
        vpus=["01", "17"],
        sampler=sampler,
        # invert 8.0/15.6 against this ArcPy VPU01 sample to get the percentiles
        arcpy_vpu01_sample=np.arange(1, 101, dtype="float64"),
        legacy_carea=8.0,
        legacy_smidx=15.6,
    )
    by = {(r["scope"], r["vpu"]): r for r in rows}
    # Inversion: 8.0 -> ~8th pct, 15.6 -> ~16th pct of 1..100
    assert by[("vpu", "01")]["p_carea"] == pytest.approx(8.0, abs=1.0)
    assert by[("vpu", "01")]["p_smidx"] == pytest.approx(15.6, abs=1.0)
    # CONUS row pools all VPUs and uses the same percentiles
    assert ("conus", "CONUS") in by
    # t_carea is the p_carea-th percentile of that scope's sample
    assert by[("vpu", "17")]["t_carea"] < by[("vpu", "01")]["t_carea"]


def test_assemble_reference_table_explicit_percentiles_skip_inversion():
    samples = {"01": np.arange(1, 101, dtype="float64")}
    rows = assemble_reference_table(
        source="arcpy", vpus=["01"], sampler=lambda v: samples[v],
        p_carea=75.0, p_smidx=95.0,
    )
    r = next(x for x in rows if x["scope"] == "vpu")
    assert r["p_carea"] == 75.0 and r["p_smidx"] == 95.0
    assert r["t_carea"] == pytest.approx(75.25, abs=0.5)


def test_raster_vpus_dedup_subregions():
    assert raster_vpus(["01", "02", "03N", "03S", "03W", "10L", "10U", "17"]) == [
        "01", "02", "03", "10", "17",
    ]
    assert raster_vpus(["01", "01", "18"]) == ["01", "18"]
