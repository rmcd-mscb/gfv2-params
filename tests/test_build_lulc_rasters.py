"""Behavioral tests for src/gfv2_params/shared_rasters/build_lulc_rasters.py.

Covers the warn-and-skip path for missing inputs (the failure mode in
job 20516163, where an unstaged NLCD source took down the orchestrator
mid-walk after nalcms had already completed a 17-minute resample).
"""

import logging
from pathlib import Path

import numpy as np
import rasterio
import yaml
from rasterio.crs import CRS
from rasterio.transform import from_bounds

from gfv2_params.shared_rasters.build_lulc_rasters import _build_one_source
from gfv2_params.shared_rasters.context import SharedRastersContext


def _write_tiny_tiff(path: Path) -> None:
    """Write a 2x2 GeoTIFF so rasterio.open() succeeds. _build_one_source
    calls _raster_info(...) for logging on every input that exists, so the
    "existing" inputs in these tests have to be readable rasters, not
    zero-byte placeholders."""
    with rasterio.open(
        str(path), "w", driver="GTiff", height=2, width=2, count=1,
        dtype="float32", crs=CRS.from_string("EPSG:4326"),
        transform=from_bounds(0, 0, 2, 2, 2, 2),
    ) as dst:
        dst.write(np.ones((2, 2), dtype=np.float32), 1)


def _write_config(path: Path, cfg: dict) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(cfg, f)


def _make_ctx(data_root: Path) -> SharedRastersContext:
    return SharedRastersContext(
        data_root=data_root,
        vpus=[],
        output_dir=data_root / "work",
    )


def test_missing_lulc_source_raster_warns_and_skips(tmp_path, caplog):
    """A configured but missing source_raster yields a warning + empty dict,
    not a FileNotFoundError. The orchestrator must be able to continue to
    the next source in the `sources:` list."""
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    cfg_path = tmp_path / "lulc_missing.yml"
    _write_config(cfg_path, {
        "lulc_source": "fake_source",
        "source_raster": str(data_root / "input" / "lulc_veg" / "fake_source" / "missing.tif"),
        "canopy_raster": str(data_root / "input" / "lulc_veg" / "CNPY.tif"),
    })

    logger = logging.getLogger("test_lulc_missing_source")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        produced = _build_one_source(cfg_path, _make_ctx(data_root), logger)

    assert produced == {}
    assert any("LULC raster not found" in r.message for r in caplog.records)
    assert any("fake_source" in r.message for r in caplog.records)


def test_missing_canopy_raster_warns_and_skips(tmp_path, caplog):
    """Same warn-and-skip behavior when source_raster exists but canopy is missing.

    source_raster must be a real GeoTIFF (not a zero-byte placeholder) because
    _build_one_source calls _raster_info(lulc_raster) for logging before it
    gets to check whether canopy_raster exists.
    """
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    fake_lulc = data_root / "lulc.tif"
    _write_tiny_tiff(fake_lulc)
    cfg_path = tmp_path / "lulc_missing_cnpy.yml"
    _write_config(cfg_path, {
        "lulc_source": "fake_source",
        "source_raster": str(fake_lulc),
        "canopy_raster": str(data_root / "missing_cnpy.tif"),
    })

    logger = logging.getLogger("test_lulc_missing_cnpy")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        produced = _build_one_source(cfg_path, _make_ctx(data_root), logger)

    assert produced == {}
    assert any("Canopy raster not found" in r.message for r in caplog.records)


def test_missing_keep_raster_warns_and_retains_cnpy_output(tmp_path, caplog):
    """A configured but missing keep_raster yields a warning, skips Steps 2-3,
    but retains the cnpy_resampled output from Step 1 in the produced dict."""
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    derived_dir = data_root / "work" / "derived_rasters"
    derived_dir.mkdir(parents=True)

    # source_raster, canopy_raster, AND cnpy_resampled all need to be real
    # readable rasters: _build_one_source logs raster info for the first two
    # (via _raster_info -> rasterio.open), and _is_valid_raster opens the
    # third to decide whether to skip the Step 1 resample.
    fake_lulc = data_root / "lulc.tif"
    fake_cnpy = data_root / "cnpy.tif"
    cnpy_resampled = derived_dir / "cnpy_resampled_fake_source.tif"
    for p in (fake_lulc, fake_cnpy, cnpy_resampled):
        _write_tiny_tiff(p)

    cfg_path = tmp_path / "lulc_missing_keep.yml"
    _write_config(cfg_path, {
        "lulc_source": "fake_source",
        "source_raster": str(fake_lulc),
        "canopy_raster": str(fake_cnpy),
        "keep_raster": str(data_root / "missing_keep.tif"),
        "radtrn_raster": str(derived_dir / "radtrn_fake_source.tif"),
    })

    logger = logging.getLogger("test_lulc_missing_keep")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        produced = _build_one_source(cfg_path, _make_ctx(data_root), logger)

    assert "cnpy_resampled_fake_source" in produced
    assert "keep_resampled_fake_source" not in produced
    assert "radtrn_fake_source" not in produced
    assert any("Keep raster not found" in r.message for r in caplog.records)
    assert any("Cnpy resample retained" in r.message for r in caplog.records)
