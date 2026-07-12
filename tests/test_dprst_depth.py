"""Integration test for the dprst_depth builder (issue #173 Task 7).

`test_dprst_depth_build_end_to_end` drives `build()` on a tiny synthetic
fabric: a local 1000x1000 m / 1 m-cell DEM (in place of a real 3DEP tile),
two dprst polygons (one real, non-flat depression; one hydro-flattened —
`fill.py`'s fallback ladder must supply its depth), a tiny HRU/ecoregion/WESM
footprint set, and no per-batch parquet dir (exercises the in-process
`tiling.group_by_tile` + `compute.run_batch` path).

No live S3 read: `rasterio.open` is monkeypatched to redirect any
`/vsicurl/`-or-`/vsis3/`-prefixed path to the local synthetic DEM. This is
deliberately a broader interception point than the task brief's suggested
`topo.read_window` patch: `compute.run_batch`'s single-tile fast path
(`_open_tile_vrt`) calls `rasterio.open(tile_key)` directly and never goes
through `read_window` at all, so patching only `read_window` would miss it.
Patching `rasterio.open` (auto-reverted by `monkeypatch`) covers both the
fast tile-cache path and the multi-tile `compute_polygon` fallback path
uniformly, whichever one `group_by_tile`'s real geometry math happens to
route each polygon through.
"""
from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
import yaml
from rasterio.transform import from_origin
from shapely.geometry import box

from gfv2_params.depstor_builders import BUILDERS, dprst_depth
from gfv2_params.depstor_builders.context import BuildContext
from gfv2_params.dprst_depth.aggregate import (
    NO_DPRST_CELLS,
    UNKNOWN_PROVENANCE,
    area_weighted_provenance,
    finalize_depth_params,
)
from gfv2_params.dprst_depth.fill import M_TO_IN

_CRS = "EPSG:5070"
_REPO_ROOT = Path(__file__).resolve().parent.parent


def test_provenance_filename_matches_config():
    """`dprst_depth.POLYGON_PROVENANCE_FILENAME` (the constant the builder
    actually writes the companion parquet as) and
    `configs/depstor/depstor_params.yml`'s `means[dprst_depth_avg].
    provenance_source` literal must name the same file, or `mean_finalize`
    (scripts/derive_depstor_params.py) silently reads a stale/missing
    provenance parquet. Guards against the two drifting independently."""
    config_path = _REPO_ROOT / "configs" / "depstor" / "depstor_params.yml"
    config = yaml.safe_load(config_path.read_text())
    means = {m["name"]: m for m in config["means"]}
    provenance_source = means["dprst_depth_avg"]["provenance_source"]

    assert Path(provenance_source).name == dprst_depth.POLYGON_PROVENANCE_FILENAME


def _L():
    return logging.getLogger("test_dprst_depth")


def test_dprst_depth_registered():
    # BUILDERS values are the bound `build` functions themselves (see
    # depstor_builders/__init__.py: `"dprst_depth": dprst_depth.build`), not
    # the module -- every other entry follows the same convention.
    assert "dprst_depth" in BUILDERS
    assert callable(BUILDERS["dprst_depth"])
    assert BUILDERS["dprst_depth"] is dprst_depth.build


def _write_dem(path):
    """1000x1000 m, 1 m cells. Baseline 100.0 everywhere; two 40x40 m
    features far enough apart that each one's 200 m rim-buffered read
    window stays inside the raster (avoids an out-of-bounds windowed read).

    Polygon A footprint (x,y in [280,320]): a real, non-flat depression —
    a gradient pit (95.0 -> 90.0), interior range 5 m >> the 0.01 m
    hydro-flattening tolerance -> `flat=False`, a genuine measured depth.
    Polygon B footprint (x,y in [680,720]): a hydro-flattened surface — an
    EXACTLY constant 95.0 -> interior range 0 -> `flat=True`, must go
    through fill.py's fallback ladder.
    """
    n = 1000
    dem = np.full((n, n), 100.0, dtype=np.float32)
    transform = from_origin(0, n, 1, 1)  # row = n - y ; col = x

    # A: rows 680:720 (y 280..320), cols 280:320 (x 280..320)
    pit = np.linspace(95.0, 90.0, num=40)
    dem[680:720, 280:320] = np.tile(pit, (40, 1))

    # B: rows 680:720 (y 280..320), cols 680:720 (x 680..720)
    dem[680:720, 680:720] = 95.0

    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1,
        dtype="float32", crs=_CRS, transform=transform, nodata=-999999.0,
    ) as dst:
        dst.write(dem, 1)


def _write_template_and_landmask(tmp_path):
    # Coarser 10 m grid over the same 0..1000 extent — burn_depth rasterizes
    # dynamically onto whatever template grid is given, independent of the
    # DEM tile source's resolution/extent.
    n = 100
    transform = from_origin(0, 1000, 10, 10)
    tmpl = tmp_path / "template.tif"
    with rasterio.open(
        tmpl, "w", driver="GTiff", height=n, width=n, count=1,
        dtype="float32", crs=_CRS, transform=transform, nodata=-9999.0,
    ) as d:
        d.write(np.full((n, n), 100.0, np.float32), 1)
    lm = tmp_path / "land_mask.tif"
    with rasterio.open(
        lm, "w", driver="GTiff", height=n, width=n, count=1,
        dtype="uint8", crs=_CRS, transform=transform, nodata=0,
    ) as d:
        d.write(np.ones((n, n), np.uint8), 1)
    # dprst_binary.tif convention (depstor_builders/dprst.py): 1 = dprst.
    # All-dprst here so the FIX-1 mask gate is a no-op for these builder
    # end-to-end tests (which only exercise the fill/compute/burn pipeline,
    # not the dprst-mask carve itself — that's covered directly in
    # test_dprst_depth_burn.py).
    dm = tmp_path / "dprst_binary.tif"
    with rasterio.open(
        dm, "w", driver="GTiff", height=n, width=n, count=1,
        dtype="uint8", crs=_CRS, transform=transform, nodata=255,
    ) as d:
        d.write(np.ones((n, n), np.uint8), 1)
    return tmpl, lm, dm


def _write_waterbody_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {
            "COMID": [101, 102],
            "member_comid": ["101", "102"],
            "FTYPE": ["LakePond", "LakePond"],
            "geometry": [box(280, 280, 320, 320), box(680, 280, 720, 320)],
        },
        crs=_CRS,
    )
    gdf.to_file(path, layer="waterbodies", driver="GPKG")


def _write_connected_parquet(path):
    # Empty connected set: neither polygon is on-stream -> both stay dprst.
    pd.DataFrame({"comid": pd.Series([], dtype="int64")}).to_parquet(path)


def _write_ecoregions_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {"US_L3CODE": ["17"], "geometry": [box(-1000, -1000, 2000, 2000)]}, crs=_CRS,
    )
    gdf.to_file(path, layer="ecoregions", driver="GPKG")


def _write_wesm_gpkg(path):
    # A footprint far from both dprst polygons -> resolution_class tags
    # everything "10m" (no covering 1m project), keeping the compute path
    # on the simple seamless-tile branch.
    gdf = gpd.GeoDataFrame(
        {"project": ["unused"], "geometry": [box(10_000_000, 10_000_000, 10_000_100, 10_000_100)]},
        crs=_CRS,
    )
    gdf.to_file(path, layer="wesm", driver="GPKG")


def _write_hru_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {
            "hru_id": [1, 2],
            "geometry": [box(250, 250, 350, 350), box(650, 250, 750, 350)],
        },
        crs=_CRS,
    )
    gdf.to_file(path, layer="nhru", driver="GPKG")


def test_dprst_depth_build_end_to_end(tmp_path, monkeypatch):
    dem_path = tmp_path / "local_dem.tif"
    _write_dem(dem_path)

    real_open = rasterio.open

    def _fake_open(path, *args, **kwargs):
        if isinstance(path, str) and ("/vsicurl/" in path or "/vsis3/" in path):
            return real_open(str(dem_path), *args, **kwargs)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(rasterio, "open", _fake_open)

    tmpl, lm, dm = _write_template_and_landmask(tmp_path)

    waterbody_gpkg = tmp_path / "waterbodies.gpkg"
    _write_waterbody_gpkg(waterbody_gpkg)
    connected_table = tmp_path / "connected.parquet"
    _write_connected_parquet(connected_table)
    ecoregions_gpkg = tmp_path / "ecoregions.gpkg"
    _write_ecoregions_gpkg(ecoregions_gpkg)
    wesm_index = tmp_path / "wesm.gpkg"
    _write_wesm_gpkg(wesm_index)
    hru_gpkg = tmp_path / "hru.gpkg"
    _write_hru_gpkg(hru_gpkg)

    ctx = BuildContext(
        fabric="t", template_path=tmpl, output_dir=tmp_path,
        hru_gpkg=hru_gpkg, hru_layer="nhru", id_feature="hru_id",
        waterbody_gpkg=waterbody_gpkg, waterbody_layer="waterbodies",
        connected_comids_table=connected_table,
        wesm_index=wesm_index, ecoregions_gpkg=ecoregions_gpkg,
    )
    ctx.paths["landmask"] = lm
    ctx.paths["dprst"] = dm

    step_cfg = {"outputs": {"dprst_depth": "dprst_depth.tif", "op_flow_thres": "op_flow_thres_params.csv"}}
    produced = dprst_depth.build(step_cfg, ctx, _L())

    assert produced["dprst_depth"].exists()
    assert produced["op_flow_thres"].exists()

    # op_flow_thres: constant 1.0 for every HRU.
    op_flow = pd.read_csv(produced["op_flow_thres"])
    assert set(op_flow["hru_id"]) == {1, 2}
    assert (op_flow["op_flow_thres"] == 1.0).all()

    # HRU 1 (covers polygon A) has a positive burned depth via a manual
    # zonal read (Task 8 owns the real gdptools aggregation).
    with rasterio.open(produced["dprst_depth"]) as src:
        from rasterio.mask import mask as rio_mask
        hru1_geom = [box(250, 250, 350, 350)]
        out_arr, _ = rio_mask(src, hru1_geom, crop=True)
        nodata = src.nodata
        valid = out_arr[0][out_arr[0] != nodata]
        assert valid.size > 0
        assert valid.mean() > 0

        hru2_geom = [box(650, 250, 750, 350)]
        out_arr2, _ = rio_mask(src, hru2_geom, crop=True)
        valid2 = out_arr2[0][out_arr2[0] != nodata]
        assert valid2.size > 0
        assert valid2.mean() > 0

    # Task 8 (#173): the companion per-polygon provenance parquet must exist
    # alongside dprst_depth.tif, carrying the fill `method` burn_depth itself
    # discards. Polygon A (COMID 101, the real gradient pit) is non-flat ->
    # "measured"; polygon B (COMID 102, hydro-flattened) falls back to its
    # own (sparse, n_donors=1) ecoregion/FTYPE median -> "regional_fill".
    prov_path = produced["dprst_depth"].parent / "dprst_depth_polygons.parquet"
    assert prov_path.exists()
    prov_gdf = gpd.read_parquet(prov_path)
    assert set(prov_gdf["COMID"]) == {101, 102}
    methods = prov_gdf.set_index("COMID")["method"]
    assert methods.loc[101] == "measured"
    assert methods.loc[102] == "regional_fill"
    assert prov_gdf["dprst_depth_m"].notna().all()
    assert (prov_gdf["dprst_depth_m"] > 0).all()


# ---------------------------------------------------------------------------
# Task 8 (#173): per-HRU aggregation -- finalize_depth_params (pure) +
# area_weighted_provenance
# ---------------------------------------------------------------------------


def test_finalize_depth_params_converts_metres_to_inches_and_floors_missing(tmp_path):
    """A small stand-in for a real exactextract `mean` column: HRU 1 has a
    real area-weighted mean depth (metres); HRU 2 is entirely absent from
    the zonal output (as a dprst_frac==0 HRU would be -- exactextract finds
    no valid pixels and either omits the row or reports NaN, both handled
    identically here since HRU 2 is passed in `hru_ids` but not `zonal_df`).
    """
    zonal_df = pd.DataFrame({"hru_id": [1], "mean": [2.0]})
    provenance_df = pd.DataFrame({"hru_id": [1], "dprst_depth_provenance": ["measured"]})

    out = finalize_depth_params(
        zonal_df, hru_ids=[1, 2], id_feature="hru_id", floor_in=49.0, provenance_df=provenance_df,
    )

    # Round-trip through an actual CSV, per the task's "assert the CSV
    # dprst_depth_avg equals..." validation shape.
    out_csv = tmp_path / "nhm_dprst_depth_avg_params.csv"
    out.to_csv(out_csv, index=False)
    read_back = pd.read_csv(out_csv).set_index("hru_id")

    assert not read_back["dprst_depth_avg"].isna().any()
    assert read_back.loc[1, "dprst_depth_avg"] == pytest.approx(2.0 * M_TO_IN)
    assert read_back.loc[1, "dprst_depth_provenance"] == "measured"

    assert read_back.loc[2, "dprst_depth_avg"] == pytest.approx(49.0)
    assert read_back.loc[2, "dprst_depth_provenance"] == NO_DPRST_CELLS


def test_finalize_depth_params_nan_mean_also_gets_floor():
    """An HRU explicitly present with mean=NaN (the more literal
    dprst_frac==0 shape exactextract actually returns) must floor the same
    way as one missing from zonal_df entirely."""
    zonal_df = pd.DataFrame({"hru_id": [1, 2], "mean": [1.0, np.nan]})

    out = finalize_depth_params(zonal_df, hru_ids=[1, 2], id_feature="hru_id", floor_in=49.0)

    out = out.set_index("hru_id")
    assert not out["dprst_depth_avg"].isna().any()
    assert out.loc[1, "dprst_depth_avg"] == pytest.approx(1.0 * M_TO_IN)
    assert out.loc[2, "dprst_depth_avg"] == pytest.approx(49.0)
    assert out.loc[2, "dprst_depth_provenance"] == NO_DPRST_CELLS


def test_finalize_depth_params_missing_provenance_marked_unknown():
    """A valid mean with no matching provenance_df row (or no provenance_df
    at all) must not be silently mislabeled as 'no_dprst_cells' -- it has
    dprst cells, we just don't know the dominant method."""
    zonal_df = pd.DataFrame({"hru_id": [1], "mean": [0.5]})

    out = finalize_depth_params(zonal_df, hru_ids=[1], id_feature="hru_id", floor_in=49.0)

    assert out.loc[0, "dprst_depth_provenance"] == UNKNOWN_PROVENANCE
    assert out.loc[0, "dprst_depth_avg"] == pytest.approx(0.5 * M_TO_IN)


def test_finalize_depth_params_clamps_over_cap_mean():
    """(#173 FIX 1 defensive backstop, PR#177 review gap) An HRU whose
    area-weighted mean depth exceeds the 300 in physical cap — e.g. a future
    upstream path that bypasses `fill.fill_flat`'s per-polygon
    `DEPTH_CAP_M` clamp — must be clamped here too, not shipped as a
    300+ in `dprst_depth_avg`. 10 m = 393.7 in, well past the 300 in cap.
    This backstop fired on 11 HRUs in the real Oregon run."""
    zonal_df = pd.DataFrame({"hru_id": [1], "mean": [10.0]})  # 10 m = 393.7 in > 300 cap
    out = finalize_depth_params(zonal_df, hru_ids=[1], id_feature="hru_id", floor_in=49.0)
    assert out.loc[0, "dprst_depth_avg"] == pytest.approx(300.0)


def test_finalize_depth_params_bad_nonpositive_mean_floored():
    """A present (non-null) but non-positive mean — the `bad` branch, which
    guards against a value slipping through despite a non-null mean (should
    never happen upstream, but a PRMS parameter must never be <= 0) — must
    be forced to the floor, not left as a negative/zero
    `dprst_depth_avg`. This is a DIFFERENT code path than the missing/NaN
    mean case (`no_dprst`), which is already covered by
    `test_finalize_depth_params_nan_mean_also_gets_floor`."""
    zonal_df = pd.DataFrame({"hru_id": [1], "mean": [-2.0]})
    out = finalize_depth_params(zonal_df, hru_ids=[1], id_feature="hru_id", floor_in=49.0)
    assert out.loc[0, "dprst_depth_avg"] == pytest.approx(49.0)
    # Distinct from NO_DPRST_CELLS: this HRU DID have a (bad) mean value, so
    # it should not be mislabeled as having zero dprst cells.
    assert out.loc[0, "dprst_depth_provenance"] != NO_DPRST_CELLS


def test_finalize_depth_params_requires_positive_floor():
    zonal_df = pd.DataFrame({"hru_id": [1], "mean": [0.5]})
    with pytest.raises(ValueError):
        finalize_depth_params(zonal_df, hru_ids=[1], id_feature="hru_id", floor_in=0.0)


def test_finalize_depth_params_missing_columns_raise():
    with pytest.raises(KeyError):
        finalize_depth_params(pd.DataFrame({"hru_id": [1]}), hru_ids=[1], id_feature="hru_id")
    with pytest.raises(KeyError):
        finalize_depth_params(pd.DataFrame({"mean": [1.0]}), hru_ids=[1], id_feature="hru_id")


def test_area_weighted_provenance_dominant_by_area():
    """Two dprst polygons intersect HRU 1 with different `method` labels and
    different areas; the larger-area method wins. HRU 2 has no dprst overlap
    at all and must be absent from the result (finalize_depth_params maps a
    missing HRU to NO_DPRST_CELLS, not this function)."""
    polygons_gdf = gpd.GeoDataFrame(
        {
            "method": ["measured", "constant_floor"],
            # 40x40 = 1600 m^2 (bigger) vs 10x10 = 100 m^2 (smaller), both
            # fully inside HRU 1.
            "geometry": [box(0, 0, 40, 40), box(60, 60, 70, 70)],
        },
        crs=_CRS,
    )
    hru_gdf = gpd.GeoDataFrame(
        {"hru_id": [1, 2], "geometry": [box(-10, -10, 110, 110), box(1000, 1000, 1010, 1010)]},
        crs=_CRS,
    )

    out = area_weighted_provenance(polygons_gdf, hru_gdf, "hru_id")

    out = out.set_index("hru_id")
    assert list(out.index) == [1]
    assert out.loc[1, "dprst_depth_provenance"] == "measured"


def test_area_weighted_provenance_missing_method_column_raises():
    polygons_gdf = gpd.GeoDataFrame({"geometry": [box(0, 0, 1, 1)]}, crs=_CRS)
    hru_gdf = gpd.GeoDataFrame({"hru_id": [1], "geometry": [box(0, 0, 1, 1)]}, crs=_CRS)
    with pytest.raises(KeyError):
        area_weighted_provenance(polygons_gdf, hru_gdf, "hru_id")


def test_area_weighted_provenance_empty_polygons_returns_empty():
    polygons_gdf = gpd.GeoDataFrame({"method": [], "geometry": []}, crs=_CRS)
    hru_gdf = gpd.GeoDataFrame({"hru_id": [1], "geometry": [box(0, 0, 1, 1)]}, crs=_CRS)
    out = area_weighted_provenance(polygons_gdf, hru_gdf, "hru_id")
    assert len(out) == 0
    assert list(out.columns) == ["hru_id", "dprst_depth_provenance"]


def test_derive_depstor_params_mean_modes_wired():
    """Light import-check + CLI-wiring check for the mean_zonal/mean_finalize
    modes added to scripts/derive_depstor_params.py (#173 Task 8) -- mirrors
    tests/test_dprst_depth_probe.py's importlib pattern for loading a
    scripts/*.py module directly."""
    spec = importlib.util.spec_from_file_location(
        "derive_depstor_params",
        Path(__file__).resolve().parent.parent / "scripts" / "derive_depstor_params.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert callable(module._find_mean)
    assert callable(module.run_mean_zonal)
    assert callable(module.run_mean_finalize)

    config = {"means": [{"name": "dprst_depth_avg", "merged_file": "nhm_dprst_depth_avg_params.csv"}]}
    assert module._find_mean(config, "dprst_depth_avg")["merged_file"] == "nhm_dprst_depth_avg_params.csv"
    with pytest.raises(ValueError):
        module._find_mean(config, "not_a_real_mean")


def test_dprst_depth_skips_when_outputs_exist(tmp_path, monkeypatch):
    tmpl, lm, dm = _write_template_and_landmask(tmp_path)
    depth_out = tmp_path / "dprst_depth.tif"
    op_flow_out = tmp_path / "op_flow_thres_params.csv"
    depth_out.write_bytes(b"placeholder")
    op_flow_out.write_text("hru_id,op_flow_thres\n1,1.0\n")

    def _boom(*a, **k):
        raise AssertionError("build() should have skipped — outputs already exist")

    monkeypatch.setattr(dprst_depth, "_load_dprst_polygons", _boom)

    ctx = BuildContext(
        fabric="t", template_path=tmpl, output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru", id_feature="hru_id",
    )
    ctx.paths["landmask"] = lm
    ctx.paths["dprst"] = dm

    step_cfg = {"outputs": {"dprst_depth": "dprst_depth.tif", "op_flow_thres": "op_flow_thres_params.csv"}}
    produced = dprst_depth.build(step_cfg, ctx, _L())
    assert produced == {"dprst_depth": depth_out, "op_flow_thres": op_flow_out}


# ---------------------------------------------------------------------------
# PR#177 review gap: _compute_depths' CONUS parquet-ingestion branch (concat
# pre-existing batch_dir/*.parquet + drop_duplicates(subset="COMID",
# keep="first")) -- the real production path once the SLURM array
# (tiling.py --plan) populates batch_dir, previously exercised only by the
# in-process branch (test_dprst_depth_build_end_to_end, no batch_dir).
# ---------------------------------------------------------------------------


def test_compute_depths_ingests_and_dedupes_batch_parquets(tmp_path):
    """Two per-batch parquets (Task 9's SLURM array output shape), one
    COMID (555) present in both with DIFFERENT depths. `_compute_depths`
    must concat + `drop_duplicates(subset='COMID', keep='first')`, so the
    surviving row is whichever file sorts first (`sorted(batch_dir.glob(...))`
    -- batch_000 before batch_001), and every other COMID passes through
    untouched."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()

    pd.DataFrame({
        "COMID": [555, 600],
        "dprst_depth_m": [1.0, 2.0],
        "measured_max_m": [1.5, 2.5],
        "hollister_max_m": [1.2, 2.2],
        "flat": [False, False],
        "resolution": ["10m", "10m"],
        "method": ["measured", "measured"],
    }).to_parquet(batch_dir / "batch_000.parquet", index=False)

    pd.DataFrame({
        "COMID": [555, 700],
        "dprst_depth_m": [99.0, 3.0],  # 555's depth here must NOT win
        "measured_max_m": [99.5, 3.5],
        "hollister_max_m": [99.2, 3.2],
        "flat": [False, False],
        "resolution": ["10m", "10m"],
        "method": ["measured", "measured"],
    }).to_parquet(batch_dir / "batch_001.parquet", index=False)

    ctx = BuildContext(
        fabric="t", template_path=tmp_path / "unused_template.tif", output_dir=tmp_path,
        hru_gpkg=tmp_path / "unused_hru.gpkg", hru_layer="nhru",
    )
    step_cfg = {"batch_dir": str(batch_dir)}
    empty_dprst = gpd.GeoDataFrame({"COMID": []}, geometry=[], crs="EPSG:5070")

    out = dprst_depth._compute_depths(empty_dprst, wesm_gdf=None, ctx=ctx, step_cfg=step_cfg, logger=_L())

    assert sorted(out["COMID"].tolist()) == [555, 600, 700]
    by_comid = out.set_index("COMID")
    assert by_comid.loc[555, "dprst_depth_m"] == pytest.approx(1.0)  # batch_000 kept (keep="first")
    assert by_comid.loc[600, "dprst_depth_m"] == pytest.approx(2.0)
    assert by_comid.loc[700, "dprst_depth_m"] == pytest.approx(3.0)
