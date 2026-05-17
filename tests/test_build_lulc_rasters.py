"""Behavioral tests for src/gfv2_params/shared_rasters/build_lulc_rasters.py.

Covers the warn-and-skip path for missing inputs (the failure mode in
job 20516163, where an unstaged NLCD source took down the orchestrator
mid-walk after nalcms had already completed a 17-minute resample).
"""

import logging
import tempfile
from pathlib import Path

import yaml

from gfv2_params.shared_rasters.build_lulc_rasters import _build_one_source
from gfv2_params.shared_rasters.context import SharedRastersContext


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
    """Same warn-and-skip behavior when source_raster exists but canopy is missing."""
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    fake_lulc = data_root / "lulc.tif"
    fake_lulc.write_bytes(b"")  # existence check only; we never read it
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


def test_missing_keep_raster_warns_and_retains_cnpy_output(tmp_path, caplog, monkeypatch):
    """A configured but missing keep_raster yields a warning, skips Steps 2-3,
    but retains the cnpy_resampled output from Step 1 in the produced dict.

    Stubs resample() so we don't actually do CONUS-scale GDAL work in a test.
    """
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    derived_dir = data_root / "work" / "derived_rasters"
    derived_dir.mkdir(parents=True)

    fake_lulc = data_root / "lulc.tif"
    fake_lulc.write_bytes(b"")
    fake_cnpy = data_root / "cnpy.tif"
    fake_cnpy.write_bytes(b"")

    # Pre-create a fake cnpy_resampled output so _is_valid_raster's path
    # check passes and Step 1 takes the "already exists — skipping" branch.
    # Use a real (tiny) GeoTIFF so rasterio.open() inside _is_valid_raster
    # actually succeeds.
    import numpy as np
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    cnpy_resampled = derived_dir / "cnpy_resampled_fake_source.tif"
    with rasterio.open(
        str(cnpy_resampled), "w", driver="GTiff", height=2, width=2, count=1,
        dtype="float32", crs=CRS.from_string("EPSG:4326"),
        transform=from_bounds(0, 0, 2, 2, 2, 2),
    ) as dst:
        dst.write(np.ones((2, 2), dtype=np.float32), 1)

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
