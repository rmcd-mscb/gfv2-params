"""Unit tests for build_vrt's per-type SRS override (twi_hydrodem needs a
named EPSG:5070 because the source tiles report an 'unnamed' Albers CRS)."""

import importlib

build_vrt = importlib.import_module("gfv2_params.shared_rasters.build_vrt")


def test_twi_hydrodem_registered_with_nodata():
    assert "twi_hydrodem" in build_vrt.RASTER_TYPES
    pattern, src_nodata = build_vrt.RASTER_TYPES["twi_hydrodem"][:2]
    assert pattern == "Twi_hydrodem_*.tif"
    assert src_nodata == "-9999"


def test_srs_override_only_for_twi_hydrodem():
    assert build_vrt._srs_override("twi_hydrodem") == "EPSG:5070"
    assert build_vrt._srs_override("twi") is None
    assert build_vrt._srs_override("fdr") is None
