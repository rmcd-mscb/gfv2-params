import logging
from contextlib import contextmanager

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from affine import Affine
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.io import MemoryFile
from rasterio.vrt import WarpedVRT
from shapely.geometry import box

import gfv2_params.dprst_depth.compute as compute_mod
from gfv2_params.dprst_depth.compute import _polygon_depth_from_dem, open_tile_set, run_batch
from gfv2_params.dprst_depth.sources import TileSet, encode


def _L(name="test_dprst_depth_compute"):
    return logging.getLogger(name)


def _raise_buildvrt_heterogeneous(*_args, **_kwargs):
    """A stand-in for `gdal.BuildVRT` raising under `gdal.UseExceptions()`
    -- used only to isolate `open_tile_set`'s fallback branch (see
    `test_open_tile_set_raises_when_buildvrt_fails_and_the_per_key_probe_finds_nothing`);
    the message is a plain PERMANENT-shaped string, deliberately not
    claiming to reproduce any specific real GDAL wording."""
    raise RuntimeError("mock GDAL: heterogeneous mosaic")


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
# The REAL open_tile_set (#223 review round 3, CRITICAL): every test above and
# below this block monkeypatches open_tile_set away entirely, so the real
# function -- specifically its `len(ts.keys) > 1` BuildVRT branch -- had ZERO
# coverage. Direct tests of it only, no monkeypatching of open_tile_set itself.
# ---------------------------------------------------------------------------


def _write_small_tif(path, transform, value, width=10, height=10, crs="EPSG:5070", nodata=-9999.0):
    with rasterio.open(
        path, "w", driver="GTiff", width=width, height=height, count=1,
        dtype="float32", crs=crs, transform=transform, nodata=nodata,
    ) as ds:
        ds.write(np.full((height, width), value, dtype="float32"), 1)


def test_open_tile_set_mosaics_a_real_multi_key_set(tmp_path):
    """(#223 review round 3, CRITICAL) `open_tile_set`'s multi-key branch builds
    an in-memory VRT via `gdal.BuildVRT`, but GDAL only serialises a VRT to its
    target when the dataset handle is FLUSHED/RELEASED -- not just because
    BuildVRT returned a Dataset. The shipped code kept `vrt_ds` bound for the
    whole `with rasterio.open(path)` block below it, so `/vsimem/...vrt` did not
    exist yet when rasterio tried to open it, and EVERY multi-tile set failed
    with RasterioIOError -- `_attempt`'s outer handler treats that as a routine
    404, so every polygon whose window touched 2+ tiles of its primary project
    silently fell all the way to the 10 m last resort. `topo.read_window` has
    always discarded BuildVRT's return value outright (never bound it), which is
    why that path worked and this one didn't. Two adjacent real GeoTIFFs, same
    CRS, mosaicked and read back -- this is the test whose absence let the bug
    through."""
    tile1 = tmp_path / "tile1.tif"
    tile2 = tmp_path / "tile2.tif"
    _write_small_tif(tile1, Affine(1.0, 0, 0.0, 0, -1.0, 10.0), value=1.0)
    _write_small_tif(tile2, Affine(1.0, 0, 10.0, 0, -1.0, 10.0), value=2.0)

    ts = TileSet(project="P", keys=(str(tile1), str(tile2)), covers=True)
    with open_tile_set(ts) as vrt:
        arr = vrt.read(1)
        assert vrt.width == 20 and vrt.height == 10
        assert vrt.bounds.left == pytest.approx(0.0)
        assert vrt.bounds.right == pytest.approx(20.0)
        assert np.allclose(arr[:, :10], 1.0) and np.allclose(arr[:, 10:], 2.0)


def test_open_tile_set_unlinks_the_vsimem_vrt_even_if_the_body_raises(tmp_path, monkeypatch):
    """The `finally: gdal.Unlink(vsimem)` cleanup must fire on an exception
    raised INSIDE the `with open_tile_set(ts):` body too, not only on a clean
    exit -- a leaked /vsimem/ entry per failed polygon set would otherwise
    accumulate for the life of the process."""
    tile1 = tmp_path / "tile1.tif"
    tile2 = tmp_path / "tile2.tif"
    _write_small_tif(tile1, Affine(1.0, 0, 0.0, 0, -1.0, 10.0), value=1.0)
    _write_small_tif(tile2, Affine(1.0, 0, 10.0, 0, -1.0, 10.0), value=2.0)
    ts = TileSet(project="P", keys=(str(tile1), str(tile2)), covers=True)

    unlinked = []
    real_unlink = compute_mod.gdal.Unlink

    def _spy_unlink(path):
        unlinked.append(path)
        return real_unlink(path)

    monkeypatch.setattr(compute_mod.gdal, "Unlink", _spy_unlink)

    with pytest.raises(RuntimeError, match="boom"), open_tile_set(ts):
        raise RuntimeError("boom")

    assert len(unlinked) == 1
    assert unlinked[0].startswith("/vsimem/dprst_depth_set_")


def test_open_tile_set_raises_when_buildvrt_fails_and_the_per_key_probe_finds_nothing(monkeypatch):
    """`open_tile_set`'s `BuildVRT`-failure fallback path: if
    `_probe_keys_for_real_cause` finds every key opens FINE on its own (the
    probe doesn't reproduce what's wrong -- e.g. a genuinely heterogeneous
    CRS that `strict=True` rejects even though each individual key is
    readable on its own), `open_tile_set` must still raise, chaining
    `BuildVRT`'s own exception as `__cause__` rather than silently
    swallowing the failure.

    (#223 round 2 review) Originally asserted a bare `RuntimeError` from a
    `None`-returning `BuildVRT` (the pre-round-1 bug: that type landed in
    `_attempt`'s `n_compute_error` bug bucket instead of the routine-open-
    failure branch) and separately mocked `gdal.GetLastErrorMsg` to return
    a fabricated CRS-mismatch string -- but real GDAL's `GetLastErrorMsg()`
    is EMPTY at this point even under real GDAL 3.12.3 (verified for both
    an all-DNS and an all-404 key list, round 2 review), so that mock
    asserted a message real GDAL never actually returns there. This test
    does not touch `GetLastErrorMsg` at all (whatever the real one returns
    is embedded in the message but never asserted); `_probe_keys_for_real_
    cause` is mocked instead, to isolate this ONE fallback branch without
    live network I/O -- the two real-GDAL-probe tests below cover the
    branch where the probe DOES find the real cause."""
    monkeypatch.setattr(compute_mod.gdal, "BuildVRT", _raise_buildvrt_heterogeneous)
    monkeypatch.setattr(compute_mod, "_probe_keys_for_real_cause", lambda ts: None)
    ts = TileSet(project="P", keys=("/vsicurl/a.tif", "/vsicurl/b.tif"), covers=True)
    with pytest.raises(compute_mod.TileSetOpenError, match="cannot build the mosaic") as excinfo, open_tile_set(ts):
        pass
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert compute_mod._is_transient_error_chain(excinfo.value) is False


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
                "resolution": "10m" if vrt == "10m" else "1m", "interior_coverage": 1.0}
    return _one


def test_run_batch_recovers_void_primary_from_next_candidate(tmp_path, monkeypatch, caplog):
    """(#223 fix round 1, finding 1) The predecessor PR's
    `test_run_batch_defers_empty_interior_to_multi_tile_fallback` asserted
    BOTH the WARNING text and `n_read_failure=1` in the summary; it was
    deleted (correctly -- it patched dead seams) with nothing replacing
    those two assertions, so this test would still pass if the
    `_bump("n_read_failure")`/`logger.warning(...)` pair inside the empty-
    interior branch were deleted and only the retry itself survived. That
    guard is the only operator-visible signal that a set's window missed,
    and issue #223's own acceptance gate sums `n_read_failure`."""
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute({"P1"}))
    caplog.set_level(logging.INFO)
    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert df.loc[0, "source"] == "P2" and df.loc[0, "method"] == "measured"
    assert any(
        r.levelno == logging.WARNING and "no valid interior" in r.getMessage()
        for r in caplog.records
    )
    summaries = [r.getMessage() for r in caplog.records if "run_batch:" in r.getMessage()]
    assert summaries and "n_read_failure=1" in summaries[-1]


def test_recover_skips_the_primary_by_value_not_position(tmp_path, monkeypatch):
    """(#223 review round 2, finding 5) `_recover` used to slice off `candidates[0]`,
    assuming it always equals `source_tiles` -- true today by construction
    (`sources.assign_sources` always writes `candidates[0]` as `source_tiles`), but
    an unchecked positional coupling across two modules. Build a candidates list
    where the primary is NOT first (`[P2, P1, TEN]` with `source_tiles=P1`) to prove
    the fix compares by VALUE: it skips P1 (already tried above, and fails again
    here) and still walks P2 -- the old slicing implementation would have sliced off
    P2 instead (assuming position 0 was the already-tried primary), retried P1 for
    no reason, and fallen through to the 10m last resort without ever trying P2."""
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open(bad_projects={"P1"}))
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    df = run_batch(_gdf([(7, P1, [P2, P1, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert df.loc[0, "source"] == "P2"


def test_run_batch_warns_on_a_manifest_tile_set_with_no_member_polygon(tmp_path, monkeypatch, caplog):
    """(#223 review round 2, finding 4) A manifest tile set with zero member
    polygons in `dprst_gdf` (a planner/tagged-parquet generation mismatch) used to
    drop out of `members` with no signal at all -- `run_batch` must WARN."""
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    caplog.set_level(logging.INFO)
    # P2 is in the manifest's tile_sets but no polygon's source_tiles is P2.
    run_batch(_gdf([(7, P1, [P1, TEN])]), [P1, P2], tmp_path / "b.parquet", _L())
    assert any(
        r.levelno == logging.WARNING and "NO member polygon" in r.getMessage() and P2 in r.getMessage()
        for r in caplog.records
    )


def test_run_batch_falls_to_10m_when_every_1m_set_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open(bad_projects={"P1", "P2"}))
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert df.loc[0, "source"] == "10m" and df.loc[0, "resolution"] == "10m"


def test_run_batch_warns_when_recovery_rate_is_material_even_at_full_success(tmp_path, monkeypatch, caplog):
    """(#223 review round 3, IMPORTANT) `success_fraction` alone stays at 1.0 when
    every polygon still ships via the fallback ladder -- exactly what "every 1 m set
    failing and every polygon recovering to 10 m" looks like (the docstring's own
    named scenario, and the CRITICAL open_tile_set/BuildVRT-flush bug's actual
    failure mode: it never lost a polygon, it silently downgraded every one of
    them). The summary must WARN on the recovered-fraction term even though every
    polygon was written and `success_fraction == 1.0`."""
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open(bad_projects={"P1"}))
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    caplog.set_level(logging.INFO)
    gdf = _gdf([(i, P1, [P1, TEN]) for i in range(5)])
    out = run_batch(gdf, [P1], tmp_path / "b.parquet", _L())
    assert len(out) == 5 and (out["source"] == "10m").all()  # every polygon still shipped
    summary = [r for r in caplog.records if r.msg.startswith("run_batch:")]
    assert summary and summary[-1].levelno == logging.WARNING


def test_run_batch_skips_polygon_with_no_usable_source(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute({"P1", "10m"}))
    caplog.set_level(logging.INFO)
    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert len(df) == 0
    assert "n_no_source=1" in caplog.text
    # (#223 review round 2, finding 9) this run already exercises the summary
    # line's success_fraction < 0.90 WARNING branch (0/1 written here) but never
    # asserted the LEVEL -- close the only untested half of that escalation.
    summary = [r for r in caplog.records if r.msg.startswith("run_batch:")]
    assert summary and summary[0].levelno == logging.WARNING


def test_run_batch_threads_give_identical_output_to_serial(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    rows = [(i, P1 if i % 2 else P2, [P1 if i % 2 else P2, TEN]) for i in range(40)]
    serial = run_batch(_gdf(rows), [P1, P2], tmp_path / "s.parquet", _L(), n_threads=1)
    threaded = run_batch(_gdf(rows), [P1, P2], tmp_path / "t.parquet", _L(), n_threads=8)
    pd.testing.assert_frame_equal(serial, threaded)
    assert serial["COMID"].is_monotonic_increasing
    # (#223 fix round 1, finding 6) interior_coverage must actually reach the
    # parquet, not just live in the in-memory frame this test already checks.
    written = pd.read_parquet(tmp_path / "t.parquet")
    assert "interior_coverage" in written.columns
    assert (written["interior_coverage"] == 1.0).all()


def test_run_batch_counts_unexpected_error_as_compute_error(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())

    def _boom(vrt, geom):
        raise TypeError("synthetic bug")

    monkeypatch.setattr(compute_mod, "_compute_one", _boom)
    caplog.set_level(logging.INFO)
    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert len(df) == 0 and "n_compute_error=2" in caplog.text  # primary + 10m both raised


def test_run_batch_survives_one_tile_set_erroring_at_open(tmp_path, monkeypatch, caplog):
    """(#223 fix round 1, finding 2) `_attempt`'s outer handler caught only
    `RasterioIOError` -- a corrupt COG, a bad/missing CRS
    (`_native_resolution`'s `CRSError`, `WarpedVRT`'s own `CRSError`/
    `WarpedVRTError`, neither a `RasterioIOError` subclass), a mixed-CRS
    `BuildVRT` failure, or a `MemoryError` at open time propagated out of
    `_attempt`, out of `ex.map`, out of `run_batch` entirely -- one bad tile
    set in a real batch would mean NO batch_XXXX.parquet is written and
    every other already-computed set's work is discarded. Here `BAD`'s open
    raises a plain `ValueError`; `P1`'s set is unaffected, and `BAD`'s
    polygon still recovers via its next candidate (10m)."""
    BAD = encode(TileSet("BAD", ("kbad",), True))

    @contextmanager
    def _fake_open_with_one_bad(ts):
        if ts.project == "BAD":
            raise ValueError("synthetic corrupt COG")
        yield ts.project

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_with_one_bad)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    caplog.set_level(logging.INFO)

    df = run_batch(
        _gdf([(7, BAD, [BAD, TEN]), (8, P1, [P1, TEN])]),
        [BAD, P1], tmp_path / "b.parquet", _L(),
    )
    assert sorted(df["COMID"].tolist()) == [7, 8]
    assert df.loc[df["COMID"] == 7, "source"].iloc[0] == "10m"  # recovered off BAD
    assert df.loc[df["COMID"] == 8, "source"].iloc[0] == "P1"
    assert "n_compute_error=1" in caplog.text
    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_run_batch_double_compute_guard_excludes_already_done_polygons(tmp_path, monkeypatch):
    """(#223 fix round 1, "double compute" finding) An exception raised while
    CLOSING a tile set (not opening it -- e.g. a `WarpedVRT`/`open_tile_set`
    cleanup failure) surfaces AFTER the for-loop over this set's polygons
    already recorded some of them in `done`. `pending` must exclude those --
    setting it to the whole `idxs` list unconditionally would re-attempt an
    already-resolved polygon against its next candidate and inflate
    `n_recovered` with a phantom retry."""

    @contextmanager
    def _fake_open_raises_on_close(ts):
        yield ts.project
        raise RuntimeError("synthetic cleanup failure")

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_raises_on_close)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))

    df = run_batch(
        _gdf([(7, P1, [P1, TEN]), (8, P1, [P1, TEN])]),
        [P1], tmp_path / "b.parquet", _L(),
    )
    assert sorted(df["COMID"].tolist()) == [7, 8]
    assert (df["source"] == "P1").all()  # resolved on the primary set, never re-attempted


def test_compute_one_reports_interior_coverage_fraction(monkeypatch):
    """(#223 fix round 1, finding 6) `interior_coverage` must be measured
    against the polygon's TRUE interior footprint, not the whole window or
    nothing at all. `_read_tile_window` is patched to return a window whose
    polygon interior (here, the whole window) is exactly HALF real values
    and HALF the sentinel -- an implementation computing the fraction
    against the whole window (real + rim), or not computing it at all,
    would not produce this exact 0.5."""
    dem = np.full((10, 10), 5.0, dtype=np.float32)
    dem[:, :5] = -9999.0  # left half of the window is sentinel/void
    transform = Affine.identity()
    geom = box(0, 0, 10, 10)  # interior == the whole 10x10 window

    monkeypatch.setattr(compute_mod, "_read_tile_window", lambda vrt, g: (dem, transform))
    result = compute_mod._compute_one(object(), geom)

    assert result is not None
    assert result["interior_coverage"] == pytest.approx(0.5)


def _ramp_dataset(memfile, width=40, height=30, nodata=-999999.0):
    """Local copy of `test_dprst_depth_topo.py`'s helper (#223 fix round 1,
    finding 5): a small in-memory EPSG:5070 raster whose value encodes its
    own (row, col), opened exactly like a real tile set so the REAL numeric
    stack below runs against it rather than a hand-fed dict. Duplicated
    rather than imported across test modules -- this repo has no existing
    precedent for one test file importing a private helper from another,
    and the helper is 6 lines."""
    data = (np.arange(height)[:, None] * 1000 + np.arange(width)[None, :]).astype("float32")
    transform = Affine(1.0, 0, 5000.0, 0, -1.0, 8000.0)  # origin (5000, 8000)
    ds = memfile.open(driver="GTiff", width=width, height=height, count=1, dtype="float32",
                      crs="EPSG:5070", transform=transform, nodata=nodata)
    ds.write(data, 1)
    return ds


def test_run_batch_runs_the_real_numeric_stack_correctly_under_threads(tmp_path, monkeypatch):
    """(#223 fix round 1, findings 5+6) Every other threading test in this
    file patches BOTH `open_tile_set` and `_compute_one`, so `read_padded`,
    `geometry_mask`, and richdem's `FillDepressions` (a C++ extension whose
    thread behaviour here was never verified) never actually run
    concurrently anywhere. Patch ONLY `open_tile_set` -- to hand out a fresh
    in-memory ramp raster per call instead of touching S3 (never a SHARED
    dataset handle across threads: `open_tile_set`'s real contract is one
    open per call, and this fake mirrors it) -- and let the REAL
    `_compute_one` run under `n_threads=1` and `n_threads=8`, comparing full
    frames. Each polygon sits well inside the raster's real 40x30 m extent,
    so this also exercises `interior_coverage` end to end through the real
    pipeline (not a hand-fed dict): a fully-interior polygon must read back
    as fully covered."""

    @contextmanager
    def _fake_open(ts):
        with MemoryFile() as mf, _ramp_dataset(mf) as ds:
            yield ds

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open)

    sets = [encode(TileSet(f"RP{i}", (f"k{i}",), True)) for i in range(8)]
    rows = [(i, sets[i % 8], [sets[i % 8], TEN]) for i in range(16)]
    # Small (2x2 m) boxes fully inside the ramp dataset's real extent
    # (origin (5000, 8000), 1 m cells, 40x30) -- their interior never
    # touches the rim buffer's sentinel padding, whatever the buffer pulls
    # in around them.
    geoms = [box(5010.0 + i, 7980.0, 5012.0 + i, 7982.0) for i in range(16)]
    gdf = gpd.GeoDataFrame(
        {"COMID": [r[0] for r in rows], "source_tiles": [r[1] for r in rows], "candidates": [r[2] for r in rows]},
        geometry=geoms, crs="EPSG:5070",
    )

    serial = run_batch(gdf, sets, tmp_path / "rs.parquet", _L(), n_threads=1)
    threaded = run_batch(gdf, sets, tmp_path / "rt.parquet", _L(), n_threads=8)

    pd.testing.assert_frame_equal(serial, threaded)
    assert len(serial) == 16
    assert (serial["interior_coverage"] == 1.0).all()


def test_run_batch_reports_partial_coverage_as_measured_not_flat(tmp_path, monkeypatch):
    """(#223 review round 3, test gap 6a) A partial-coverage, NON-FLAT polygon
    driven through `run_batch` with the REAL numeric stack, asserting
    `interior_coverage` strictly between 0 and 1 AND `method == "measured"` --
    the documented "still ships" invariant (`compute.py`'s `_OUTPUT_COLUMNS`
    comment: "a low-but-nonzero coverage still ships as method='measured' with
    no gate at write time") was proven NOWHERE: the closest existing test
    (`_compute_one_reports_interior_coverage_fraction`) hand-feeds a constant
    DEM, which reads flat, and the real-numeric-stack test above keeps every
    polygon fully inside real coverage on purpose (`interior_coverage == 1.0`
    there BY DESIGN). Straddle the ramp dataset's real x-extent edge (x=5040,
    origin (5000,8000), 40x30): the polygon's own interior is half inside real
    coverage, half beyond it (padded to the sentinel by `read_padded`), so the
    covered half's genuine ramp relief must read non-flat/measured with a
    partial coverage fraction, not fall back to flat/nodata handling."""

    @contextmanager
    def _fake_open(ts):
        with MemoryFile() as mf, _ramp_dataset(mf) as ds:
            yield ds

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open)

    ts = encode(TileSet("RP", ("k",), True))
    geom = box(5035.0, 7975.0, 5045.0, 7985.0)  # straddles the real x=5040 edge
    gdf = gpd.GeoDataFrame(
        {"COMID": [1], "source_tiles": [ts], "candidates": [[ts, TEN]]},
        geometry=[geom], crs="EPSG:5070",
    )

    out = run_batch(gdf, [ts], tmp_path / "partial.parquet", _L())

    assert len(out) == 1
    row = out.iloc[0]
    assert row["method"] == "measured"
    assert not row["flat"]
    assert 0.0 < row["interior_coverage"] < 1.0


# ---------------------------------------------------------------------------
# Transient-vs-permanent retry (issue #223 real-incident follow-on,
# 2026-09-20): a cluster-wide DNS failure ("CURL error: Could not resolve
# host: prd-tnm.s3.amazonaws.com", 532 times across 5 nodes within the
# single second 21:08:10) must be RETRIED on the same source, never treated
# as grounds to advance `_recover`'s candidate walk -- that's what silently
# demoted 4,822 of 5,535 tjc polygons (87%) off their correctly-ranked
# primary source on the real rerun. Every test below patches `_sleep` so
# the backoff never actually waits.
# ---------------------------------------------------------------------------

_DNS_ERROR = "CURL error: Could not resolve host: prd-tnm.s3.amazonaws.com"


def test_is_transient_error_classifies_the_measured_dns_incident_as_transient():
    assert compute_mod._is_transient_error(_DNS_ERROR) is True


def test_is_transient_error_classifies_404_and_unsupported_format_as_permanent():
    assert compute_mod._is_transient_error("HTTP response code: 404") is False
    assert compute_mod._is_transient_error(
        "USGS_1M_x.tif: not recognized as being in a supported file format"
    ) is False


def test_run_batch_retries_transient_open_failure_then_succeeds_on_primary(tmp_path, monkeypatch, caplog):
    """(test 1/5 from the transient-retry spec) An open that fails
    transiently twice and then succeeds must resolve the polygon against
    its PRIMARY source, count exactly 2 retries, and inflate neither
    `n_recovered` nor `n_read_failure` -- a transient failure that
    eventually succeeds was never a failure of this source at all."""
    monkeypatch.setattr(compute_mod, "_sleep", lambda s: None)
    caplog.set_level(logging.INFO)
    calls = {"n": 0}

    @contextmanager
    def _fake_open_flaky_then_ok(ts):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RasterioIOError(_DNS_ERROR)
        yield ts.project

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_flaky_then_ok)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))

    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())

    assert df.loc[0, "source"] == "P1" and df.loc[0, "method"] == "measured"
    summaries = [r.getMessage() for r in caplog.records if "run_batch:" in r.getMessage()]
    assert summaries and "n_transient_retry=2" in summaries[-1]
    assert "n_recovered=0" in summaries[-1]
    assert "n_read_failure=0" in summaries[-1]


def test_run_batch_raises_when_transient_open_failure_never_clears(tmp_path, monkeypatch):
    """(test 2/5) Every retry attempt also failing transiently must FAIL THE
    WHOLE TASK -- raise, name the affected set, and write NO parquet at
    all -- rather than let `_recover` silently demote the polygon to a
    lower-ranked candidate."""
    monkeypatch.setattr(compute_mod, "_sleep", lambda s: None)

    @contextmanager
    def _fake_open_always_flaky(ts):
        raise RasterioIOError(_DNS_ERROR)
        yield  # pragma: no cover - unreachable; required so this stays a generator function

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_always_flaky)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))

    out_parquet = tmp_path / "b.parquet"
    with pytest.raises(RuntimeError, match="P1"):
        run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], out_parquet, _L())

    assert not out_parquet.exists()


def test_run_batch_does_not_retry_a_permanent_open_failure(tmp_path, monkeypatch):
    """(test 3/5) A PERMANENT failure (here: GDAL's "not recognized..."
    message for a genuinely unreadable object) must NOT be retried at
    all -- `_sleep` is never called -- and the candidate walk must proceed
    exactly as it did before this fix."""
    sleep_calls = []
    monkeypatch.setattr(compute_mod, "_sleep", lambda s: sleep_calls.append(s))

    @contextmanager
    def _fake_open_unreadable(ts):
        if ts.project == "P1":
            raise RasterioIOError(
                "USGS_1M_x.tif: not recognized as being in a supported file format"
            )
        yield ts.project

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_unreadable)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))

    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())

    assert df.loc[0, "source"] == "P2"  # candidate walk proceeded, unaffected by this fix
    assert sleep_calls == []  # never retried


# ---------------------------------------------------------------------------
# Round 2 (#223 review): REAL GDAL/rasterio probes, no mocks of GDAL's own
# error machinery. A round-2 review found that round 1's `GetLastErrorMsg`
# mocks (just deleted above) asserted messages real GDAL never emits at
# that call site -- the exact trap that hid the pre-round-1 BuildVRT-flush
# bug (every test back then mocked `open_tile_set` itself, so the real
# function had zero coverage). `.invalid` is a reserved TLD (RFC 2606) that
# fails DNS resolution deterministically, even with no network access at
# all, so these are CI-safe; `GDAL_HTTP_MAX_RETRY=0` + a short timeout keep
# them fast regardless (a DNS failure never reaches curl's own HTTP retry
# layer anyway -- that's the whole premise of the original incident).
# ---------------------------------------------------------------------------

_INVALID_HOST_ENV = dict(GDAL_HTTP_MAX_RETRY="0", GDAL_HTTP_TIMEOUT="2", GDAL_HTTP_CONNECTTIMEOUT="2")
_INVALID_URL_A = "/vsicurl/https://this-host-does-not-exist.invalid/a.tif"
_INVALID_URL_B = "/vsicurl/https://this-host-does-not-exist.invalid/b.tif"


def test_real_gdal_path1_single_key_open_dns_failure_is_transient():
    """PATH 1 of the incident: a single-key `open_tile_set` (`rasterio.open`
    directly) against a REAL DNS failure. Fixed in round 1; kept here as
    the real-GDAL baseline the other two paths are compared against."""
    ts = TileSet(project="P", keys=(_INVALID_URL_A,), covers=True)
    with rasterio.Env(**_INVALID_HOST_ENV):
        with pytest.raises(RasterioIOError) as excinfo:
            with open_tile_set(ts):
                pass
    assert compute_mod._is_transient_error_chain(excinfo.value) is True


def test_real_gdal_path2_multikey_buildvrt_open_dns_failure_is_transient():
    """PATH 2 of the incident: a multi-key `open_tile_set` (the
    `gdal.BuildVRT` mosaic branch) against a REAL DNS failure on every key
    -- #223 round 2 CRITICAL C1. `gdal.GetLastErrorMsg()` is empty here
    even under real GDAL (verified by the reviewer), so this exercises the
    per-key `rasterio.open` probe (`_probe_keys_for_real_cause`) that
    recovers the real classifiable cause -- without it, this path
    classified PERMANENT (the bug C1 fixes)."""
    ts = TileSet(project="P", keys=(_INVALID_URL_A, _INVALID_URL_B), covers=True)
    with rasterio.Env(**_INVALID_HOST_ENV):
        with pytest.raises(compute_mod.TileSetOpenError) as excinfo:
            with open_tile_set(ts):
                pass
    assert compute_mod._is_transient_error_chain(excinfo.value) is True


def _make_dns_failing_vrt(tmp_path, real_tif_path):
    """A real local GDAL VRT over `real_tif_path`, then its
    `<SourceFilename>` XML-rewritten (via `ElementTree`, NOT a naive string
    replace -- a string replace produced an unreadable VRT when tried,
    #223 round 2 review) to a `.invalid` DNS-failing URL. Reproduces a
    per-polygon READ hitting a real network failure through a `WarpedVRT`
    over a local VRT -- the exact shape `open_tile_set`'s multi-key branch
    produces in production."""
    import xml.etree.ElementTree as ET

    vrt_path = tmp_path / "dns_fail.vrt"
    # Discard BuildVRT's return value outright (never bind it) -- see
    # `open_tile_set`'s own comment on why holding the reference would
    # leave the VRT unflushed to disk.
    gdal.BuildVRT(str(vrt_path), [str(real_tif_path)])
    tree = ET.parse(vrt_path)
    for elem in tree.iter("SourceFilename"):
        elem.text = _INVALID_URL_A
        elem.set("relativeToVRT", "0")
    tree.write(vrt_path)
    return vrt_path


def test_real_gdal_path3_per_polygon_read_through_warpedvrt_dns_failure_is_transient(tmp_path):
    """PATH 3 of the incident: a per-polygon READ (not an open) against a
    REAL DNS failure reached through a `WarpedVRT` over a local VRT --
    #223 round 2 CRITICAL C2. Real GDAL wraps this as a generic
    `RasterioIOError('Read failed. See previous exception for details.')`;
    the real cause (a GDAL `CPLE_HttpResponseError`/`CPLE_AppDefinedError`
    carrying the actual CURL/DNS text) is only reachable via
    `__cause__`/`__context__`, which is exactly what
    `_is_transient_error_chain` walks (a plain `_is_transient_error(str(exc))`
    check on the outer message alone would miss it and classify
    PERMANENT)."""
    real_tif = tmp_path / "real.tif"
    _write_small_tif(real_tif, Affine(1.0, 0, 0.0, 0, -1.0, 10.0), value=1.0)
    vrt_path = _make_dns_failing_vrt(tmp_path, real_tif)

    with rasterio.Env(**_INVALID_HOST_ENV):
        with rasterio.open(vrt_path) as src, WarpedVRT(
            src, crs="EPSG:5070", resampling=Resampling.nearest
        ) as vrt:
            with pytest.raises(RasterioIOError) as excinfo:
                vrt.read(1)

    assert compute_mod._is_transient_error_chain(excinfo.value) is True


def test_run_batch_retries_a_transient_tile_set_open_error(tmp_path, monkeypatch, caplog):
    """(test 4/5, end to end) `TileSetOpenError` -- what `open_tile_set` now
    raises when `gdal.BuildVRT` returns `None` for a transient GDAL cause --
    IS a `RasterioIOError` subclass, so `_attempt` must retry it exactly
    like a plain single-key open failure."""
    monkeypatch.setattr(compute_mod, "_sleep", lambda s: None)
    caplog.set_level(logging.INFO)
    calls = {"n": 0}

    @contextmanager
    def _fake_open_buildvrt_flaky(ts):
        calls["n"] += 1
        if calls["n"] <= 1:
            raise compute_mod.TileSetOpenError(
                f"gdal.BuildVRT returned None for tile set project={ts.project!r} "
                f"keys={ts.keys!r} -- cannot build the mosaic (GDAL: {_DNS_ERROR})"
            )
        yield ts.project

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_buildvrt_flaky)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))

    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())

    assert df.loc[0, "source"] == "P1"
    summaries = [r.getMessage() for r in caplog.records if "run_batch:" in r.getMessage()]
    assert summaries and "n_transient_retry=1" in summaries[-1]


def test_run_batch_retries_a_transient_per_polygon_read_failure(tmp_path, monkeypatch, caplog):
    """(test 5/5) A per-polygon `_compute_one` call that fails transiently
    (the SET opened fine; only THIS polygon's read hit the blip) must be
    retried on the SAME already-open set, ending up read from the
    PRIMARY -- never dropped to `_recover`'s candidate walk."""
    monkeypatch.setattr(compute_mod, "_sleep", lambda s: None)
    caplog.set_level(logging.INFO)
    calls = {"n": 0}

    def _flaky_compute_one(vrt, geom):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RasterioIOError(_DNS_ERROR)
        return {
            "dprst_depth_m": 1.0, "measured_max_m": 2.0, "hollister_max_m": 3.0,
            "flat": False, "resolution": "1m", "interior_coverage": 1.0,
        }

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _flaky_compute_one)

    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())

    assert df.loc[0, "source"] == "P1"
    summaries = [r.getMessage() for r in caplog.records if "run_batch:" in r.getMessage()]
    assert summaries and "n_transient_retry=2" in summaries[-1]
    assert "n_recovered=0" in summaries[-1]


def test_run_batch_raises_when_per_polygon_transient_failure_never_clears(tmp_path, monkeypatch):
    """A per-polygon read that fails transiently on every attempt, through
    BOTH the in-place budget and the one deferred retry, is the same
    doctrine as a persistently-failing OPEN (test 2/5 above): fail the
    whole task, write no parquet, rather than fall through to a
    lower-ranked candidate."""
    monkeypatch.setattr(compute_mod, "_sleep", lambda s: None)

    def _always_flaky_compute_one(vrt, geom):
        raise RasterioIOError(_DNS_ERROR)

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _always_flaky_compute_one)

    out_parquet = tmp_path / "b.parquet"
    with pytest.raises(RuntimeError, match="P1"):
        run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], out_parquet, _L())

    assert not out_parquet.exists()


def test_run_batch_recovers_via_the_deferred_pass_when_a_transient_failure_outlasts_the_inplace_budget(
    tmp_path, monkeypatch, caplog
):
    """(#223 round 2, I4) The real second 2026-09-20 smoke failure (job
    4532393) was NOT a persistent host failure -- a ~30s DNS outage
    outlasted the then-5-attempt/~20s in-place retry budget by 8s, so the
    set was wrongly declared persistent moments before it would have
    succeeded. A set whose open fails transiently through its ENTIRE
    in-place budget (`_RETRY_ATTEMPTS` attempts) must NOT immediately fail
    the task -- it is deferred to a second pass, and if THAT succeeds (as
    here: the fake clears on its `_RETRY_ATTEMPTS + 1`-th call, i.e. the
    deferred pass's very first attempt), the polygon resolves against its
    PRIMARY source with NO raise at all."""
    monkeypatch.setattr(compute_mod, "_sleep", lambda s: None)
    caplog.set_level(logging.INFO)
    calls = {"n": 0}

    @contextmanager
    def _fake_open_outlasts_inplace_budget(ts):
        calls["n"] += 1
        if calls["n"] <= compute_mod._RETRY_ATTEMPTS:
            raise RasterioIOError(_DNS_ERROR)
        yield ts.project

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_outlasts_inplace_budget)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))

    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())

    assert df.loc[0, "source"] == "P1" and df.loc[0, "method"] == "measured"
    summaries = [r.getMessage() for r in caplog.records if r.getMessage().startswith("run_batch:")]
    assert summaries
    # in-place attempts 1..(_RETRY_ATTEMPTS - 1) each bump the counter (the
    # final in-place attempt exhausts without bumping -- it's a give-up,
    # not a retry); the deferred pass's own first attempt succeeds outright
    # with no further retry needed.
    expected_retries = compute_mod._RETRY_ATTEMPTS - 1
    assert f"n_transient_retry={expected_retries}" in summaries[-1]
    assert "n_recovered=0" in summaries[-1]
