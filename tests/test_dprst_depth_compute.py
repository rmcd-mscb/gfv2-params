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


# ---------------------------------------------------------------------------
# PR#177 review gap: run_batch's multi-tile-polygon dedup + per-tile/
# per-polygon failure isolation (#173). All tests below monkeypatch the I/O
# seams (`_open_tile_vrt`, `_read_tile_window`, `compute_polygon`,
# `group_by_tile`) so NO S3/network access happens -- run_batch's own
# tile-grouping / bin-packing / bookkeeping logic is what's under test.
# ---------------------------------------------------------------------------


def _fake_open_tile_vrt_factory(bad_tiles=frozenset()):
    """Returns a fake `_open_tile_vrt`: raises RasterioIOError on entry for
    any tile key in `bad_tiles`, otherwise yields a harmless sentinel object
    in place of a real WarpedVRT (never touched directly -- only passed
    through to the also-monkeypatched `_read_tile_window`)."""

    def _factory(tile_key):
        @contextmanager
        def _cm():
            if tile_key in bad_tiles:
                raise RasterioIOError(f"synthetic: tile absent ({tile_key})")
            yield f"FAKE_VRT[{tile_key}]"

        return _cm()

    return _factory


def _dummy_dem_transform():
    return np.full((5, 5), 10.0, dtype=np.float32), Affine.identity()


def test_run_batch_skips_failed_tile_without_aborting_batch(tmp_path, monkeypatch, caplog):
    """One tile fails to open (RasterioIOError); a second tile succeeds. The
    good tile's polygon must still be written; the batch must not raise; the
    bad-tile failure must land in n_read_failure, not n_compute_error (both
    only observable via the summary log line, since run_batch returns only
    the DataFrame -- see the module's run_batch docstring on the two
    separate failure counters)."""
    dprst_gdf = gpd.GeoDataFrame(
        {"COMID": [100, 200], "best_topo": ["10m", "10m"]},
        geometry=[box(0, 0, 1, 1), box(50, 50, 51, 51)],
        crs="EPSG:5070",
    )
    # idx 0 -> COMID 100 lives ONLY on the bad tile; idx 1 -> COMID 200 lives
    # ONLY on the good tile.
    monkeypatch.setattr(
        compute_mod, "group_by_tile",
        lambda dprst, wesm: {"bad_tile": [0], "good_tile": [1]},
    )
    monkeypatch.setattr(
        compute_mod, "_open_tile_vrt", _fake_open_tile_vrt_factory(bad_tiles={"bad_tile"}),
    )
    monkeypatch.setattr(
        compute_mod, "_read_tile_window",
        lambda vrt, geom, rim_buffer_m=200.0: _dummy_dem_transform(),
    )

    def _boom_fallback(geom, best_topo, wesm_row=None):
        # idx 0's only tile failed to open, so it falls through to the
        # multi-tile fallback path too -- simulate "genuinely no data
        # anywhere" (a routine read-gap, not a code bug) rather than let it
        # hit real S3.
        raise RasterioIOError("synthetic: no fallback data either")

    monkeypatch.setattr(compute_mod, "compute_polygon", _boom_fallback)

    out_parquet = tmp_path / "batch.parquet"
    caplog.set_level(logging.INFO, logger="skip_failed_tile")

    out_df = run_batch(dprst_gdf, ["bad_tile", "good_tile"], wesm_gdf=None,
                        out_parquet=out_parquet, logger=_L("skip_failed_tile"))

    assert len(out_df) == 1
    assert out_df.iloc[0]["COMID"] == 200

    written = pd.read_parquet(out_parquet)
    assert sorted(written["COMID"].tolist()) == [200]

    # Both failures (tile-open + fallback) are the EXPECTED "no data here"
    # signal (RasterioIOError) -> n_read_failure, never n_compute_error --
    # the FIX-2 distinction the summary line makes visible.
    summaries = [r.getMessage() for r in caplog.records if "run_batch:" in r.getMessage()]
    assert summaries, "expected a run_batch summary log line"
    assert "n_read_failure=2" in summaries[-1]
    assert "n_compute_error=0" in summaries[-1]
    assert not any(r.levelno == logging.ERROR for r in caplog.records)


def test_run_batch_counts_compute_error_separately(tmp_path, monkeypatch, caplog):
    """A polygon's compute raises a NON-io error (ValueError -- a real code
    bug, not a routine tile-absent read gap). It must be counted as
    `n_compute_error` (the FIX-A/FIX-2 distinction from `n_read_failure`),
    logged at ERROR, and the batch must still complete with the OTHER
    polygon on the same tile written."""
    dprst_gdf = gpd.GeoDataFrame(
        {"COMID": [100, 200], "best_topo": ["10m", "10m"]},
        # idx 0's geometry is the sentinel the fake _read_tile_window keys
        # its ValueError off of; idx 1 is any other geometry.
        geometry=[box(0, 0, 1, 1), box(50, 50, 51, 51)],
        crs="EPSG:5070",
    )
    monkeypatch.setattr(
        compute_mod, "group_by_tile", lambda dprst, wesm: {"tile1": [0, 1]},
    )
    monkeypatch.setattr(compute_mod, "_open_tile_vrt", _fake_open_tile_vrt_factory())

    def _fake_read_tile_window(vrt, geom, rim_buffer_m=200.0):
        if tuple(geom.bounds) == (0.0, 0.0, 1.0, 1.0):
            raise ValueError("synthetic: real code bug, not a read gap")
        return _dummy_dem_transform()

    monkeypatch.setattr(compute_mod, "_read_tile_window", _fake_read_tile_window)

    def _boom_fallback(geom, best_topo, wesm_row=None):
        # idx 0 never gets marked `done` (the inner except `continue`s
        # without emitting), so it also reaches the multi-tile fallback --
        # keep it a ValueError there too so it stays a compute_error, not a
        # read_failure, and never touches real S3.
        raise ValueError("synthetic: fallback also hits the same code bug")

    monkeypatch.setattr(compute_mod, "compute_polygon", _boom_fallback)

    out_parquet = tmp_path / "batch.parquet"
    caplog.set_level(logging.INFO, logger="compute_error_isolation")

    out_df = run_batch(dprst_gdf, ["tile1"], wesm_gdf=None,
                        out_parquet=out_parquet, logger=_L("compute_error_isolation"))

    # The batch did not raise, and the surviving (idx 1 / COMID 200) polygon
    # is still written.
    assert len(out_df) == 1
    assert out_df.iloc[0]["COMID"] == 200

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records, "expected at least one ERROR-level log for the compute bug"
    assert any("UNEXPECTED compute error" in r.getMessage() for r in error_records)

    summaries = [r.getMessage() for r in caplog.records if "run_batch:" in r.getMessage()]
    assert summaries, "expected a run_batch summary log line"
    # Escalated to ERROR (FIX 3's completeness gate: n_compute_error > 0).
    assert "n_compute_error=2" in summaries[-1]  # tile-path + fallback, both idx 0
    assert "n_read_failure=0" in summaries[-1]
    summary_records = [r for r in caplog.records if "run_batch:" in r.getMessage()]
    assert summary_records[-1].levelno == logging.ERROR


def test_run_batch_dedupes_multi_tile_polygon(tmp_path, monkeypatch):
    """A polygon (idx 1 / COMID 200) whose covering-tile set spans TWO tile
    keys within this batch must be computed exactly once -- via the
    multi-tile fallback (`compute_polygon`), never via the single-tile
    tile-cache path (which run_batch restricts to `len(tiles_per_polygon[
    idx]) == 1` precisely to avoid this) -- and appear exactly once in the
    output parquet."""
    dprst_gdf = gpd.GeoDataFrame(
        {"COMID": [100, 200], "best_topo": ["10m", "10m"]},
        geometry=[box(0, 0, 1, 1), box(50, 50, 51, 51)],
        crs="EPSG:5070",
    )
    # idx 0 -> single-tile (tileA only); idx 1 -> multi-tile (both tileA and
    # tileB) -- the shape run_batch's own docstring says triggers the
    # multi-tile fallback dedup.
    monkeypatch.setattr(
        compute_mod, "group_by_tile",
        lambda dprst, wesm: {"tileA": [0, 1], "tileB": [1]},
    )
    monkeypatch.setattr(compute_mod, "_open_tile_vrt", _fake_open_tile_vrt_factory())
    monkeypatch.setattr(
        compute_mod, "_read_tile_window",
        lambda vrt, geom, rim_buffer_m=200.0: _dummy_dem_transform(),
    )

    call_counts: dict[int, int] = {}

    def _counting_compute_polygon(geom, best_topo, wesm_row=None):
        call_counts[200] = call_counts.get(200, 0) + 1
        return {
            "dprst_depth_m": 1.0, "measured_max_m": 1.0, "hollister_max_m": 1.0,
            "flat": False, "resolution": "10m", "method": "measured",
        }

    monkeypatch.setattr(compute_mod, "compute_polygon", _counting_compute_polygon)

    out_parquet = tmp_path / "batch.parquet"
    out_df = run_batch(dprst_gdf, ["tileA", "tileB"], wesm_gdf=None,
                        out_parquet=out_parquet, logger=_L("dedup"))

    # Computed exactly once for the multi-tile polygon (COMID 200).
    assert call_counts[200] == 1

    assert sorted(out_df["COMID"].tolist()) == [100, 200]
    assert (out_df["COMID"] == 200).sum() == 1  # appears once, not once per tile

    written = pd.read_parquet(out_parquet)
    assert (written["COMID"] == 200).sum() == 1
