import logging
from contextlib import contextmanager

import geopandas as gpd
import numpy as np
import pandas as pd
from affine import Affine
from rasterio.errors import RasterioIOError
from shapely.geometry import box

import gfv2_params.dprst_depth.compute as compute_mod
from gfv2_params.dprst_depth.compute import _polygon_depth_from_dem, run_batch
from gfv2_params.dprst_depth.sources import TileSet, encode


def _L(name="test_dprst_depth_compute"):
    return logging.getLogger(name)


def test_polygon_depth_from_dem_bowl_and_flat():
    # 20x20, 1 m cells; 8x8 pit in the centre; rest flat rim at 10.0.
    # The pit floor is NOT perfectly constant (a real, non-hydro-flattened
    # depression's bed has genuine relief) — it grades from 8.5 down to
    # 8.0 (a 0.5 m interior range, well above is_hydroflattened's 0.01 m
    # tolerance), so the interior-only flatness gate correctly reads
    # flat=False and still measures a sensible V/A mean depth.
    dem = np.full((20, 20), 10.0, np.float64)
    pit = np.linspace(8.5, 8.0, num=8)
    dem[6:14, 6:14] = np.tile(pit, (8, 1))
    mask = np.zeros((20, 20), bool)
    mask[6:14, 6:14] = True  # interior = the pit
    r = _polygon_depth_from_dem(dem, mask, Affine.scale(1, -1), nodata=-9999.0)
    assert not r["flat"]
    expected_mean_depth = float(np.mean(10.0 - pit))
    assert np.isclose(r["dprst_depth_m"], expected_mean_depth)
    assert np.isclose(r["measured_max_m"], 2.0)  # deepest cell: 10.0 - 8.0
    assert np.isfinite(r["hollister_max_m"])

    # Hydro-flattened case: the INTERIOR is exactly constant (the
    # breakline-enforced water surface), even though the surrounding rim
    # carries real relief (sloped terrain, not a flat whole-window read —
    # this is exactly the case the window-wide gate got wrong: it would
    # have seen the rim/interior contrast as "not flat" and tried to
    # measure a depth off the flat water surface instead of correctly
    # detecting hydro-flattening from the interior alone).
    sloped = np.full((20, 20), 10.0, np.float64)
    sloped += np.arange(20).reshape(-1, 1) * 0.5  # rim relief, not constant
    sloped[6:14, 6:14] = 8.0  # hydro-flattened interior: exactly constant
    rf = _polygon_depth_from_dem(sloped, mask, Affine.scale(1, -1), nodata=-9999.0)
    assert rf["flat"] and np.isnan(rf["dprst_depth_m"])
    assert np.isnan(rf["measured_max_m"])
    assert np.isfinite(rf["hollister_max_m"])


def test_polygon_depth_from_dem_all_nodata_interior_is_flat_with_nan_depth():
    """(PR#177 review edge case) An interior mask that selects only nodata
    cells (e.g. a polygon whose window fell entirely outside the DEM's real
    coverage) has `interior_valid.size == 0` -> the degenerate branch of the
    `flat` test, distinct from a genuinely hydro-flattened (non-empty,
    exactly-constant) interior -- both read `flat=True`/NaN depth, but for
    different reasons, and this path (empty interior) was untested."""
    dem = np.full((10, 10), -9999.0, np.float64)
    mask = np.ones((10, 10), bool)  # whole window "inside the polygon"
    r = _polygon_depth_from_dem(dem, mask, Affine.scale(1, -1), nodata=-9999.0)
    assert r["flat"] is True
    assert np.isnan(r["dprst_depth_m"])
    assert np.isnan(r["measured_max_m"])
    # hollister_max_m is ALWAYS computed (see _polygon_depth_from_dem's
    # docstring) -- with no valid ring cells to project a slope from,
    # lake_max_depth's early-return gives 0.0, not NaN/inf.
    assert np.isfinite(r["hollister_max_m"])


def test_read_tile_window_rim_buffer_bounds(monkeypatch):
    """(#223 review M-4) `_read_tile_window`'s four +=/-= rim-buffer
    statements were re-transcribed into one inline bounds tuple -- a sign
    flip would be silent (a 400 m-shifted window still returns plausible
    depths), and every other test in this file monkeypatches
    `_read_tile_window` itself, never exercising this arithmetic. Monkeypatch
    `read_padded` (its only caller) and check the exact bounds tuple it
    receives against `geom.bounds +/- rim_buffer_m`, in (minx, miny, maxx,
    maxy) order."""
    captured = {}

    def _fake_read_padded(vrt, bounds, sentinel=-9999.0):
        captured["vrt"] = vrt
        captured["bounds"] = bounds
        return np.zeros((2, 2), np.float32), Affine.identity()

    monkeypatch.setattr(compute_mod, "read_padded", _fake_read_padded)
    sentinel_vrt = object()
    geom = box(1000.0, 2000.0, 1100.0, 2100.0)
    compute_mod._read_tile_window(sentinel_vrt, geom)
    assert captured["vrt"] is sentinel_vrt
    assert captured["bounds"] == (
        1000.0 - 200.0, 2000.0 - 200.0, 1100.0 + 200.0, 2100.0 + 200.0,
    )


# ---------------------------------------------------------------------------
# Tile-SET run_batch (issue #223 part 2): open each tile set once, run sets
# concurrently, recover a polygon whose primary set misses via its remaining
# ranked `candidates`. All tests below monkeypatch the two seams `run_batch`
# actually calls -- `open_tile_set` and `_compute_one` -- so NO S3/network
# access happens; `run_batch`'s own grouping/threading/recovery/counting
# logic is what's under test.
# ---------------------------------------------------------------------------

P1 = encode(TileSet("P1", ("k1",), True))
P2 = encode(TileSet("P2", ("k2",), True))
TEN = encode(TileSet("10m", ("k10",), True))


def _gdf(rows):
    return gpd.GeoDataFrame(
        {"COMID": [r[0] for r in rows], "source_tiles": [r[1] for r in rows], "candidates": [r[2] for r in rows]},
        geometry=[box(0, 0, 1, 1)] * len(rows), crs="EPSG:5070",
    )


def _fake_open(void_projects=frozenset(), bad_projects=frozenset()):
    @contextmanager
    def _open(ts):
        if ts.project in bad_projects:
            raise RasterioIOError(f"synthetic 404 {ts.project}")
        yield ts.project  # the "vrt" is just the project name
    return _open


def _fake_compute(void_projects):
    def _one(vrt, geom):
        if vrt in void_projects:
            return None  # interior had 0 valid cells
        return {"dprst_depth_m": 1.0, "measured_max_m": 2.0, "hollister_max_m": 3.0, "flat": False,
                "resolution": "10m" if vrt == "10m" else "1m"}
    return _one


def test_run_batch_recovers_void_primary_from_next_candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute({"P1"}))
    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert df.loc[0, "source"] == "P2" and df.loc[0, "method"] == "measured"


def test_run_batch_falls_to_10m_when_every_1m_set_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open(bad_projects={"P1", "P2"}))
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert df.loc[0, "source"] == "10m" and df.loc[0, "resolution"] == "10m"


def test_run_batch_skips_polygon_with_no_usable_source(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute({"P1", "10m"}))
    caplog.set_level(logging.INFO)
    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert len(df) == 0
    assert "n_no_source=1" in caplog.text


def test_run_batch_threads_give_identical_output_to_serial(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    rows = [(i, P1 if i % 2 else P2, [P1 if i % 2 else P2, TEN]) for i in range(40)]
    serial = run_batch(_gdf(rows), [P1, P2], tmp_path / "s.parquet", _L(), n_threads=1)
    threaded = run_batch(_gdf(rows), [P1, P2], tmp_path / "t.parquet", _L(), n_threads=8)
    pd.testing.assert_frame_equal(serial, threaded)
    assert serial["COMID"].is_monotonic_increasing


def test_run_batch_counts_unexpected_error_as_compute_error(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())

    def _boom(vrt, geom):
        raise TypeError("synthetic bug")

    monkeypatch.setattr(compute_mod, "_compute_one", _boom)
    caplog.set_level(logging.INFO)
    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert len(df) == 0 and "n_compute_error=2" in caplog.text  # primary + 10m both raised
