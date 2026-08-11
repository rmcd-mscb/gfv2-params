"""End-to-end tests for the circular-mean aspect runner (issue #201).

These build real (tiny) GeoTIFFs and a real batch gpkg and run the real
gdptools/exactextract path, rather than mocking it. That is deliberate: the
design leans on exactextract treating NaN as nodata (so masked flat cells drop
out of `mean` and out of `count`), and a mock would assert our belief about
gdptools instead of gdptools' behaviour.
"""

import logging
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from gfv2_params.raster_ops import atan2_deg
from gfv2_params.zonal_runners.aspect import run_aspect_batch

_CELL = 30.0
_ORIGIN_X, _ORIGIN_Y = 0.0, 60.0          # a 2-row grid: y from 60 down to 0
_TRANSFORM = from_origin(_ORIGIN_X, _ORIGIN_Y, _CELL, _CELL)
_CRS = "EPSG:5070"
_LOG = logging.getLogger("test_aspect")


def _write_raster(path: Path, arr: np.ndarray, transform=_TRANSFORM) -> None:
    height, width = arr.shape
    with rasterio.open(
        path, "w", driver="GTiff", height=height, width=width, count=1,
        dtype="float32", crs=_CRS, transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(arr.astype("float32"), 1)


def _write_batch_gpkg(path: Path, id_feature="nat_hru_id", hru_ids=(1,), boxes=None):
    """One polygon per HRU. Default: a single polygon covering the whole grid."""
    if boxes is None:
        boxes = [box(_ORIGIN_X, _ORIGIN_Y - 2 * _CELL, _ORIGIN_X + 4 * _CELL, _ORIGIN_Y)]
    gdf = gpd.GeoDataFrame({id_feature: list(hru_ids)}, geometry=list(boxes), crs=_CRS)
    gdf.to_file(path, layer="nhru", driver="GPKG")


def _make_config(tmp_path, aspect_arr, slope_arr, slope_transform=_TRANSFORM):
    batch_dir = tmp_path / "batches"
    batch_dir.mkdir()
    output_dir = tmp_path / "params"
    output_dir.mkdir()

    _write_raster(tmp_path / "aspect.tif", aspect_arr)
    _write_raster(tmp_path / "slope.tif", slope_arr, transform=slope_transform)
    _write_batch_gpkg(batch_dir / "batch_0000.gpkg")

    return {
        "source_type": "aspect",
        "id_feature": "nat_hru_id",
        "target_layer": "nhru",
        "fabric": "testfab",
        "source_raster": str(tmp_path / "aspect.tif"),
        "slope_raster": str(tmp_path / "slope.tif"),
        "batch_dir": str(batch_dir),
        "output_dir": str(output_dir),
    }


def _read_output(config) -> pd.DataFrame:
    path = (Path(config["output_dir"]) / "aspect"
            / "base_nhm_aspect_testfab_batch_0000_param.csv")
    return pd.read_csv(path)


def test_circular_mean_survives_the_wrap_where_the_arithmetic_mean_does_not(tmp_path):
    """The whole of issue #201, in one HRU.

    Four cells alternate 350 deg and 10 deg -- all essentially north-facing. The
    arithmetic `mean` says 180 (due SOUTH). atan2(mean_sin, mean_cos) says 0.
    Both numbers are in the same file, which is what makes the old product
    auditable after the fix lands.
    """
    aspect = np.array([[350.0, 10.0, 350.0, 10.0],
                       [350.0, 10.0, 350.0, 10.0]])
    slope = np.full((2, 4), 5.0)
    config = _make_config(tmp_path, aspect, slope)

    run_aspect_batch(config, 0, _LOG)
    out = _read_output(config)

    assert math.isclose(out["mean"][0], 180.0, abs_tol=1e-3)          # the defect
    assert math.isclose(atan2_deg(out["mean_sin"], out["mean_cos"])[0], 0.0, abs_tol=1e-3)
    assert math.isclose(out["flat_frac"][0], 0.0, abs_tol=1e-9)


def test_flat_cells_are_excluded_and_counted(tmp_path):
    """RichDEM writes 270 for slope == 0; a flat cell has no down slope direction.

    Half the grid is flat-at-270, half is genuinely 90 (east). The circular mean
    must be 90, not the 180 that including the flats would give.
    """
    aspect = np.array([[90.0, 90.0, 270.0, 270.0],
                       [90.0, 90.0, 270.0, 270.0]])
    slope = np.array([[5.0, 5.0, 0.0, 0.0],
                      [5.0, 5.0, 0.0, 0.0]])
    config = _make_config(tmp_path, aspect, slope)

    run_aspect_batch(config, 0, _LOG)
    out = _read_output(config)

    assert math.isclose(atan2_deg(out["mean_sin"], out["mean_cos"])[0], 90.0, abs_tol=1e-3)
    assert math.isclose(out["flat_frac"][0], 0.5, abs_tol=1e-6)
    assert math.isclose(out["n_aspect_cells"][0], 4.0, abs_tol=1e-6)
    assert math.isclose(out["count"][0], 8.0, abs_tol=1e-6)
    # Pass 1 is UNMASKED: `mean` still averages all eight cells, flats included,
    # so it stays byte-comparable with the pre-fix product.
    assert math.isclose(out["mean"][0], 180.0, abs_tol=1e-3)


def test_an_all_flat_hru_reports_nan_means_not_west(tmp_path):
    """flat_frac == 1 and NaN means, rather than a confident 270 deg."""
    aspect = np.full((2, 4), 270.0)
    slope = np.zeros((2, 4))
    config = _make_config(tmp_path, aspect, slope)

    run_aspect_batch(config, 0, _LOG)
    out = _read_output(config)

    assert math.isnan(out["mean_sin"][0])
    assert math.isnan(out["mean_cos"][0])
    assert math.isclose(out["n_aspect_cells"][0], 0.0, abs_tol=1e-9)
    assert math.isclose(out["flat_frac"][0], 1.0, abs_tol=1e-9)


def test_legacy_columns_match_the_generic_zonal_runner(tmp_path):
    """Pass 1 must reproduce `run_zonal_batch` exactly, flats included.

    If it did not, the retained `mean` column would be a NEW statistic wearing
    the old name and the old-vs-new comparison at rollout would be meaningless.
    """
    from gfv2_params.zonal_runners.zonal import run_zonal_batch

    aspect = np.array([[10.0, 100.0, 200.0, 300.0],
                       [20.0, 110.0, 210.0, 310.0]])
    slope = np.array([[5.0, 5.0, 0.0, 5.0],
                      [5.0, 0.0, 5.0, 5.0]])
    config = _make_config(tmp_path, aspect, slope)
    run_aspect_batch(config, 0, _LOG)
    ours = _read_output(config)

    ref_config = dict(config, source_type="aspect_ref", categorical=False)
    (Path(config["output_dir"]) / "aspect_ref").mkdir(parents=True, exist_ok=True)
    run_zonal_batch(ref_config, 0, _LOG)
    ref = pd.read_csv(
        Path(config["output_dir"]) / "aspect_ref"
        / "base_nhm_aspect_ref_testfab_batch_0000_param.csv"
    )

    for col in ["count", "mean", "std", "min", "25%", "50%", "75%", "max", "sum"]:
        assert math.isclose(ours[col][0], ref[col][0], rel_tol=1e-9, abs_tol=1e-9), col


def test_misaligned_slope_raster_raises(tmp_path):
    """A half-cell offset would mask the flat test against the wrong aspect cells.

    Nothing downstream would ever report it, so it must raise here.
    """
    shifted = from_origin(_ORIGIN_X + _CELL / 2, _ORIGIN_Y, _CELL, _CELL)
    config = _make_config(
        tmp_path, np.full((2, 4), 90.0), np.full((2, 4), 5.0), slope_transform=shifted
    )
    with pytest.raises(ValueError, match="not co-registered"):
        run_aspect_batch(config, 0, _LOG)


def test_writes_exactly_one_csv(tmp_path):
    """gdptools writes a CSV per ZonalGen when zonal_writer == "csv".

    Three passes must not leave three files: `run_merge` globs this directory
    with `base_nhm_aspect_{fabric}_batch_*_param.csv` and would concat strays.
    """
    config = _make_config(tmp_path, np.full((2, 4), 90.0), np.full((2, 4), 5.0))
    run_aspect_batch(config, 0, _LOG)
    written = sorted((Path(config["output_dir"]) / "aspect").glob("*.csv"))
    assert [p.name for p in written] == ["base_nhm_aspect_testfab_batch_0000_param.csv"]
