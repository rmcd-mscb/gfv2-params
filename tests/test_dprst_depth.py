"""Integration test for the dprst_depth builder (issue #173 Task 7; real
tile-inventory wiring, issue #223).

`test_dprst_depth_build_end_to_end` drives `build()` on a tiny synthetic
fabric: two dprst polygons (one real, non-flat depression; one
hydro-flattened — `fill.py`'s fallback ladder must supply its depth), a tiny
HRU/ecoregion/3DEP-inventory/WESM-attrs set, and no per-batch parquet dir
(exercises the in-process `tiling.tile_set_groups` + `compute.run_batch`
path, sourced from `sources.tag_and_assign`'s real tile-set assignment).

No live S3 read: `compute.open_tile_set`/`compute._compute_one` — the two
seams `run_batch` actually calls — are monkeypatched, exactly as
`tests/test_dprst_depth_compute.py` does for `run_batch`'s own unit tests.
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

import gfv2_params.dprst_depth.compute as compute_mod
from gfv2_params.depstor_builders import BUILDERS, dprst_depth
from gfv2_params.depstor_builders.context import BuildContext
from gfv2_params.dprst_depth.aggregate import (
    NO_DPRST_CELLS,
    UNKNOWN_PROVENANCE,
    area_weighted_provenance,
    finalize_depth_params,
)
from gfv2_params.dprst_depth.fill import M_TO_IN
from gfv2_params.dprst_depth.inventory import INVENTORY_COLUMNS
from gfv2_params.dprst_depth.tiling import _load_and_tag_for_plan

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


def _fake_open_tile_set_and_compute_one(monkeypatch):
    """Stub `compute.run_batch`'s two actual seams (`open_tile_set`/
    `_compute_one`) so `test_dprst_depth_build_end_to_end` never opens a real
    (or even locally-fake) raster — mirrors `tests/test_dprst_depth_compute.py`'s
    own `run_batch` unit tests.

    Discriminates by geometry, not by tile set, matching the retired local-DEM
    fixture's two cases: polygon A (COMID 101, bounds minx=280) is a real,
    non-flat depression; polygon B (COMID 102, bounds minx=680) is
    hydro-flattened and must fall through `fill.py`'s ladder.

    `interior_coverage` is DELIBERATELY distinctive per polygon (0.83 / 0.55,
    neither of which is 1.0 or any other value a silently-dropped/defaulted
    column could plausibly produce) so a test asserting on it cannot pass by
    accident — see `test_dprst_depth_build_end_to_end`'s provenance-parquet
    assertions.
    """
    from contextlib import contextmanager

    @contextmanager
    def _fake_open_tile_set(ts):
        yield ts.project

    def _fake_compute_one(vrt, geom):
        if geom.bounds[0] < 500:  # polygon A
            return {
                "dprst_depth_m": 2.5, "measured_max_m": 5.0, "hollister_max_m": 3.0,
                "flat": False, "resolution": "10m", "interior_coverage": 0.83,
            }
        return {  # polygon B: hydro-flattened
            "dprst_depth_m": float("nan"), "measured_max_m": float("nan"),
            "hollister_max_m": 3.0, "flat": True, "resolution": "10m", "interior_coverage": 0.55,
        }

    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open_tile_set)
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute_one)


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


def _write_segment_comids(tmp_path, comids, name="segment_waterbody_comids.parquet"):
    """A `segment_wbody` output table, in the shape `load_segment_comids` reads
    (mirrors `test_wbody_connectivity.py`'s `_write_segment_table`)."""
    comids = list(comids)
    path = tmp_path / name
    pd.DataFrame({
        "comid": pd.array(comids, dtype="int64"),
        "n_segments": pd.array([1] * len(comids), dtype="int64"),
        "overlap_m": pd.array([100.0] * len(comids), dtype="float64"),
    }).to_parquet(path, index=False)
    return path


def _write_endorheic_comids(tmp_path, comids=(), name="endorheic_waterbody_comids.parquet"):
    """An `endorheic` output table, in the shape `load_endorheic_comids` reads.
    Empty by default -- the no-closed-basin no-op case (mirrors
    `test_wbody_connectivity.py`'s `_write_empty_endorheic`)."""
    comids = list(comids)
    n = len(comids)
    path = tmp_path / name
    pd.DataFrame({
        "comid": pd.array(comids, dtype="int64"),
        "frac_own": pd.array([1.0] * n, dtype="float64"),
        "by_terminus": pd.array([True] * n, dtype="bool"),
        "by_closed_huc12": pd.array([False] * n, dtype="bool"),
    }).to_parquet(path, index=False)
    return path


def _write_ecoregions_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {"US_L3CODE": ["17"], "geometry": [box(-1000, -1000, 2000, 2000)]}, crs=_CRS,
    )
    gdf.to_file(path, layer="ecoregions", driver="GPKG")


def _write_inventory(tmp_path):
    """Real 3DEP tile inventory + WESM project attrs fixtures (issue #223) --
    one project, one tile, covering both dprst polygon fixtures (x,y in
    [0, 1000]) -- so `sources.tag_and_assign` tags both polygons `"1m"` and
    assigns this tile as their primary `source_tiles`. `open_tile_set`/
    `_compute_one` are monkeypatched in every test that actually computes a
    depth, so this tile's `key` is never opened."""
    dem_1m_inventory = tmp_path / "dem_1m_tile_inventory.parquet"
    pd.DataFrame(
        [["test_project", "/vsicurl/https://example/tile.tif", 13, "EPSG:26913",
          1000, 1000, -500.0, -500.0, 1500.0, 1500.0]],
        columns=INVENTORY_COLUMNS,
    ).to_parquet(dem_1m_inventory)

    wesm_project_attrs = tmp_path / "wesm_project_attrs.parquet"
    pd.DataFrame(
        {"ql_rank": [1], "collect_end": [pd.Timestamp("2020-01-01")], "matched_by": ["project"]},
        index=pd.Index(["test_project"], name="project"),
    ).to_parquet(wesm_project_attrs)
    return dem_1m_inventory, wesm_project_attrs


def _write_hru_gpkg(path):
    gdf = gpd.GeoDataFrame(
        {
            "hru_id": [1, 2],
            "geometry": [box(250, 250, 350, 350), box(650, 250, 750, 350)],
        },
        crs=_CRS,
    )
    gdf.to_file(path, layer="nhru", driver="GPKG")


def test_load_dprst_polygons_raises_when_segment_wbody_comids_missing(tmp_path):
    """`_load_dprst_polygons` must raise a KeyError naming `segment_wbody` when
    `segment_wbody_comids` is absent from `ctx.paths` -- the `segment_wbody`
    step hasn't run for this fabric, and without its table the reconstructed
    dprst polygon set would diverge from `dprst_binary.tif`."""
    waterbody_gpkg = tmp_path / "waterbodies.gpkg"
    _write_waterbody_gpkg(waterbody_gpkg)

    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru",
        waterbody_gpkg=waterbody_gpkg, waterbody_layer="waterbodies",
    )
    # endorheic_comids present -- isolates the missing segment_wbody_comids check.
    ctx.paths["endorheic_comids"] = _write_endorheic_comids(tmp_path)

    with pytest.raises(KeyError, match="segment_wbody"):
        dprst_depth._load_dprst_polygons(ctx, _L())


def test_load_dprst_polygons_raises_when_endorheic_comids_missing(tmp_path):
    """Same guard, the other table: without the endorheic subtraction the
    reconstructed dprst polygon set would exclude terminal lakes (Great Salt
    Lake among them) that `dprst_binary.tif` includes."""
    waterbody_gpkg = tmp_path / "waterbodies.gpkg"
    _write_waterbody_gpkg(waterbody_gpkg)

    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru",
        waterbody_gpkg=waterbody_gpkg, waterbody_layer="waterbodies",
    )
    # segment_wbody_comids present -- isolates the missing endorheic_comids check.
    ctx.paths["segment_wbody_comids"] = _write_segment_comids(tmp_path, [101])

    with pytest.raises(KeyError, match="endorheic"):
        dprst_depth._load_dprst_polygons(ctx, _L())


def test_builder_and_plan_paths_resolve_the_same_onstream_set(tmp_path):
    """The builder (`_load_dprst_polygons`, reads `ctx.paths`) and the SLURM
    `--plan` hook (`tiling._load_and_tag_for_plan`, reads `config["output_dir"]`)
    must reconstruct the IDENTICAL dprst polygon set from the same segment +
    endorheic tables -- the whole point of `load_fabric_dprst_polygons` taking
    a resolved `onstream_comids` set instead of two provenance-specific table
    paths (see `topo.load_fabric_dprst_polygons`'s docstring).

    COMID 101 is on-stream per the segment classifier but ALSO endorheic, so
    it must survive the subtraction and end up classified dprst (the Great
    Salt Lake shape); COMID 102 is genuinely on-stream and must stay excluded
    on BOTH paths.
    """
    waterbody_gpkg = tmp_path / "waterbodies.gpkg"
    _write_waterbody_gpkg(waterbody_gpkg)
    ecoregions_gpkg = tmp_path / "ecoregions.gpkg"
    _write_ecoregions_gpkg(ecoregions_gpkg)
    dem_1m_inventory, wesm_project_attrs = _write_inventory(tmp_path)
    hru_gpkg = tmp_path / "hru.gpkg"
    _write_hru_gpkg(hru_gpkg)

    segment_table = _write_segment_comids(tmp_path, [101, 102])
    endorheic_table = _write_endorheic_comids(tmp_path, [101])
    assert segment_table.parent == tmp_path  # both land directly under output_dir

    # --- builder path: ctx.paths (orchestrator-tracked) ---
    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=tmp_path,
        hru_gpkg=hru_gpkg, hru_layer="nhru",
        waterbody_gpkg=waterbody_gpkg, waterbody_layer="waterbodies",
    )
    ctx.paths["segment_wbody_comids"] = segment_table
    ctx.paths["endorheic_comids"] = endorheic_table
    builder_dprst = dprst_depth._load_dprst_polygons(ctx, _L())

    # --- --plan path: config["output_dir"] (same tmp_path, same filenames) ---
    config = {
        "waterbody_gpkg": str(waterbody_gpkg), "waterbody_layer": "waterbodies",
        "output_dir": str(tmp_path),
        "dem_1m_inventory": str(dem_1m_inventory), "wesm_project_attrs": str(wesm_project_attrs),
        # _write_inventory's fixture is a single tile/project, well under
        # sources.tag_and_assign's default floors (issue #223 review round 2) --
        # disabled here since this test is about the builder/plan on-stream
        # reconstruction agreeing, not the inventory floor.
        "min_dem_1m_tiles": 0, "min_dem_1m_projects": 0, "min_wesm_project_attrs_rows": 0,
        "ecoregions_gpkg": str(ecoregions_gpkg),
        "hru_gpkg": str(hru_gpkg), "hru_layer": "nhru",
    }
    plan_dprst = _load_and_tag_for_plan(config, _L())

    assert set(builder_dprst["COMID"]) == set(plan_dprst["COMID"])
    assert set(builder_dprst["COMID"]) == {101}
    assert 102 not in set(builder_dprst["COMID"])


def test_dprst_depth_build_end_to_end(tmp_path, monkeypatch):
    _fake_open_tile_set_and_compute_one(monkeypatch)

    tmpl, lm, dm = _write_template_and_landmask(tmp_path)

    waterbody_gpkg = tmp_path / "waterbodies.gpkg"
    _write_waterbody_gpkg(waterbody_gpkg)
    ecoregions_gpkg = tmp_path / "ecoregions.gpkg"
    _write_ecoregions_gpkg(ecoregions_gpkg)
    dem_1m_inventory, wesm_project_attrs = _write_inventory(tmp_path)
    hru_gpkg = tmp_path / "hru.gpkg"
    _write_hru_gpkg(hru_gpkg)

    ctx = BuildContext(
        fabric="t", template_path=tmpl, output_dir=tmp_path,
        hru_gpkg=hru_gpkg, hru_layer="nhru", id_feature="hru_id",
        waterbody_gpkg=waterbody_gpkg, waterbody_layer="waterbodies",
        dem_1m_inventory=dem_1m_inventory, wesm_project_attrs=wesm_project_attrs,
        ecoregions_gpkg=ecoregions_gpkg,
        # _write_inventory's fixture is a single tile/project, well under
        # sources.tag_and_assign's default floors (issue #223 review round 2) --
        # disabled here since this test is an end-to-end build, not the floor.
        min_dem_1m_tiles=0, min_dem_1m_projects=0, min_wesm_project_attrs_rows=0,
    )
    ctx.paths["landmask"] = lm
    ctx.paths["dprst"] = dm
    # Empty on-stream set (segment - endorheic, both empty): neither polygon
    # is on-stream -> both stay dprst, matching the retired _write_connected_parquet
    # fixture's "empty connected set" comment.
    ctx.paths["segment_wbody_comids"] = _write_segment_comids(tmp_path, [])
    ctx.paths["endorheic_comids"] = _write_endorheic_comids(tmp_path)

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

    # issue #223 fix round 1: the static column-membership check
    # (test_provenance_carries_the_winning_source below) would still pass if
    # `_fill_and_join`'s keep_cols filter -- or a future name collision --
    # silently dropped `source`/`interior_coverage` before the parquet write
    # (`_write_polygon_provenance` only WARNs on a missing diagnostic column,
    # it doesn't raise). Assert the columns actually round-trip on disk, with
    # the DISTINCTIVE values `_fake_open_tile_set_and_compute_one` produced --
    # both polygons resolve to the real "test_project" tile assigned by
    # `sources.tag_and_assign` (never the "10m" last-resort fallback, since
    # `_write_inventory`'s tile covers both fixtures), and their
    # `interior_coverage` values (0.83 / 0.55) cannot arise from a dropped
    # column silently defaulting to NaN or 1.0.
    assert "source" in prov_gdf.columns
    assert "interior_coverage" in prov_gdf.columns
    sources = prov_gdf.set_index("COMID")["source"]
    assert sources.loc[101] == "test_project"
    assert sources.loc[102] == "test_project"
    coverage = prov_gdf.set_index("COMID")["interior_coverage"]
    assert coverage.loc[101] == pytest.approx(0.83)
    assert coverage.loc[102] == pytest.approx(0.55)


def test_provenance_carries_the_winning_source(tmp_path, monkeypatch):
    """The re-run validation (Task 11) needs to know WHICH project each depth came from."""
    from gfv2_params.depstor_builders import dprst_depth as b
    assert "source" in b._DEPTH_COLUMNS
    assert "source" in b._PROVENANCE_DIAGNOSTIC_COLUMNS


def test_tag_polygons_requires_the_inventory_keys(tmp_path):
    from gfv2_params.depstor_builders import dprst_depth as b
    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru",
        dem_1m_inventory=None, wesm_project_attrs=tmp_path / "attrs.parquet",
    )
    dprst = _make_dprst_gdf([1, 2])  # existing helper in this file
    with pytest.raises(KeyError, match="dem_1m_inventory"):
        b._tag_polygons(dprst, ctx, _L())


def test_tag_polygons_requires_the_inventory_files_to_exist(tmp_path):
    """The mirrored branch of the guard above: the key IS set but the staged
    file is absent -- the path an operator actually hits when they haven't
    run `sbatch slurm_batch/stage_dem_1m_inventory.batch` yet."""
    from gfv2_params.depstor_builders import dprst_depth as b
    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru",
        dem_1m_inventory=tmp_path / "does_not_exist.parquet",
        wesm_project_attrs=tmp_path / "attrs.parquet",
    )
    dprst = _make_dprst_gdf([1, 2])
    with pytest.raises(FileNotFoundError, match="dem_1m_inventory"):
        b._tag_polygons(dprst, ctx, _L())


def test_tag_polygons_threads_the_min_dem_1m_tiles_override_through_to_the_raise(tmp_path):
    """(#223 review round 3, test gap 6d) Every existing test touching
    `min_dem_1m_tiles`/`min_dem_1m_projects`/`min_wesm_project_attrs_rows` sets
    them to 0 (disabling the floor) -- nothing proves the NON-zero override path,
    `ctx.min_dem_1m_tiles` actually reaching `sources.tag_and_assign` through
    `_tag_polygons`, works at all."""
    from gfv2_params.depstor_builders import dprst_depth as b
    dem_1m_inventory, wesm_project_attrs = _write_inventory(tmp_path)  # 1 tile/1 project
    ecoregions_gpkg = tmp_path / "ecoregions.gpkg"
    _write_ecoregions_gpkg(ecoregions_gpkg)
    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru",
        dem_1m_inventory=dem_1m_inventory, wesm_project_attrs=wesm_project_attrs,
        ecoregions_gpkg=ecoregions_gpkg,
        min_dem_1m_tiles=5,
    )
    dprst = _make_dprst_gdf([1, 2])
    with pytest.raises(RuntimeError, match=r"dem_1m_inventory carries 1 tiles, below its floor of 5"):
        b._tag_polygons(dprst, ctx, _L())


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


# ---------------------------------------------------------------------------
# Robustness guard 2: `run_mean_finalize`'s provenance_source resolution must
# RAISE FileNotFoundError when a *configured* provenance_source is missing on
# disk -- shipping dprst_depth_avg with every HRU silently mislabeled
# "unknown" is exactly the failure to stop. Extracted as `_resolve_provenance_df`
# (the tightest testable seam -- `run_mean_finalize` itself reads batch CSVs +
# the master HRU gpkg via `_load_resolved_config`, which needs a real fabric
# profile to exercise end-to-end).
# ---------------------------------------------------------------------------


def _load_derive_depstor_params_module():
    """Load scripts/derive_depstor_params.py as a module -- mirrors
    tests/test_dprst_depth_probe.py's importlib pattern (the script isn't a
    package, so pytest can't `import` it directly)."""
    spec = importlib.util.spec_from_file_location(
        "derive_depstor_params",
        Path(__file__).resolve().parent.parent / "scripts" / "derive_depstor_params.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resolve_provenance_df_raises_on_missing_path(tmp_path):
    module = _load_derive_depstor_params_module()
    hru_gdf = gpd.GeoDataFrame({"hru_id": [1], "geometry": [box(0, 0, 1, 1)]}, crs=_CRS)
    missing_path = tmp_path / "does_not_exist.parquet"

    with pytest.raises(FileNotFoundError, match=str(missing_path)):
        module._resolve_provenance_df(str(missing_path), hru_gdf, "hru_id")


def test_resolve_provenance_df_returns_none_when_unconfigured():
    module = _load_derive_depstor_params_module()
    hru_gdf = gpd.GeoDataFrame({"hru_id": [1], "geometry": [box(0, 0, 1, 1)]}, crs=_CRS)

    assert module._resolve_provenance_df(None, hru_gdf, "hru_id") is None
    assert module._resolve_provenance_df("", hru_gdf, "hru_id") is None


def test_resolve_provenance_df_reads_real_parquet(tmp_path):
    """Positive path: an existing provenance_source is read and aggregated
    via `area_weighted_provenance` (same shape as
    `test_area_weighted_provenance_dominant_by_area`)."""
    module = _load_derive_depstor_params_module()
    polygons_gdf = gpd.GeoDataFrame(
        {"method": ["measured"], "geometry": [box(0, 0, 40, 40)]}, crs=_CRS,
    )
    prov_path = tmp_path / "dprst_depth_polygons.parquet"
    polygons_gdf.to_parquet(prov_path)
    hru_gdf = gpd.GeoDataFrame({"hru_id": [1], "geometry": [box(-10, -10, 110, 110)]}, crs=_CRS)

    out = module._resolve_provenance_df(str(prov_path), hru_gdf, "hru_id")

    assert out is not None
    assert out.set_index("hru_id").loc[1, "dprst_depth_provenance"] == "measured"


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


# ---------------------------------------------------------------------------
# Robustness guard 1: `_fill_and_join`'s measured_fraction completeness gate
# must RAISE (not just warn) on a mass read-failure, with a configurable
# threshold (`ctx.dprst_depth_min_measured_frac`, default 0.5) and a `0`/
# negative escape hatch.
# ---------------------------------------------------------------------------


def _make_dprst_gdf(comids):
    """Minimal fabric-clipped dprst polygon set, post-`_tag_polygons` shape:
    carries `ecoregion`/`ftype` (which `_tag_polygons` normally sets from the
    EPA ecoregions gpkg / `FTYPE` before `_fill_and_join` ever runs)."""
    n = len(comids)
    return gpd.GeoDataFrame(
        {
            "COMID": comids,
            "ecoregion": ["17"] * n,
            "ftype": ["LakePond"] * n,
        },
        geometry=[box(i * 10, 0, i * 10 + 1, 1) for i in range(n)],
        crs=_CRS,
    )


def _make_depth_df(comids):
    """A `depth_df` row per COMID -- non-flat, genuinely measured."""
    return pd.DataFrame(
        {
            "COMID": comids,
            "dprst_depth_m": [1.0] * len(comids),
            "measured_max_m": [1.5] * len(comids),
            "hollister_max_m": [1.2] * len(comids),
            "flat": [False] * len(comids),
            "resolution": ["10m"] * len(comids),
            "method": ["measured"] * len(comids),
        }
    )


def test_fill_and_join_raises_on_mass_read_failure():
    """10 polygons, only 3 with a computed depth -> 30% measured, well below
    the default 0.5 threshold -- must RAISE, not warn."""
    dprst = _make_dprst_gdf(list(range(1, 11)))
    depth_df = _make_depth_df([1, 2, 3])  # only COMIDs 1-3 got a computed depth

    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=Path("unused"),
        hru_gpkg=Path("unused"), hru_layer="nhru",
    )

    with pytest.raises(RuntimeError, match=r"30\.0%|3/10|measured"):
        dprst_depth._fill_and_join(dprst, depth_df, ctx, _L())


def test_fill_and_join_does_not_raise_on_healthy_fraction():
    """10 polygons, 8 with a computed depth -> 80% measured, comfortably
    above the default 0.5 threshold -- must NOT raise."""
    dprst = _make_dprst_gdf(list(range(1, 11)))
    depth_df = _make_depth_df(list(range(1, 9)))  # COMIDs 1-8 computed

    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=Path("unused"),
        hru_gpkg=Path("unused"), hru_layer="nhru",
    )

    out = dprst_depth._fill_and_join(dprst, depth_df, ctx, _L())
    assert len(out) == 10
    assert out["dprst_depth_m"].notna().all()


def test_fill_and_join_escape_hatch_disables_guard():
    """Same mass-read-failure inputs as the raising test above, but
    `dprst_depth_min_measured_frac=0` (the documented escape hatch) must
    disable the guard entirely -- no raise."""
    dprst = _make_dprst_gdf(list(range(1, 11)))
    depth_df = _make_depth_df([1, 2, 3])

    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=Path("unused"),
        hru_gpkg=Path("unused"), hru_layer="nhru",
        dprst_depth_min_measured_frac=0.0,
    )

    out = dprst_depth._fill_and_join(dprst, depth_df, ctx, _L())
    assert len(out) == 10
    assert out["dprst_depth_m"].notna().all()


def test_build_context_dprst_depth_min_measured_frac_default():
    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=Path("unused"),
        hru_gpkg=Path("unused"), hru_layer="nhru",
    )
    assert ctx.dprst_depth_min_measured_frac == pytest.approx(0.5)


def _write_plan(batch_dir, comids, n_batches):
    """The `_plan/` the tiled planner writes: the polygon set the parquets were
    computed for, and how many batch files the array produces."""
    import json

    plan = batch_dir / "_plan"
    plan.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"COMID": list(comids)}).to_parquet(plan / "dprst_polygons_tagged.parquet")
    (plan / "batch_manifest.json").write_text(json.dumps({"n_batches": n_batches}))


def _write_batch(batch_dir, i, comids, depths):
    """Current-schema (post-#223) batch parquet -- carries `source`/`interior_coverage`.
    See `_write_legacy_batch` for the PRE-#223 shape (issue #223 review round 2,
    finding 3) some tests deliberately still need."""
    n = len(comids)
    pd.DataFrame({
        "COMID": comids,
        "dprst_depth_m": depths,
        "measured_max_m": [d + 0.5 for d in depths],
        "hollister_max_m": [d + 0.2 for d in depths],
        "flat": [False] * n,
        "resolution": ["10m"] * n,
        "method": ["measured"] * n,
        "source": ["10m"] * n,
        "interior_coverage": [1.0] * n,
    }).to_parquet(batch_dir / f"batch_{i:04d}.parquet", index=False)


def _write_legacy_batch(batch_dir, i, comids, depths):
    """PRE-#223 batch parquet shape -- no `source`/`interior_coverage` at all (those
    columns didn't exist yet). Reproduces exactly what a `--from`/`--force` re-run
    against an old, unregenerated batch dir looks like."""
    n = len(comids)
    pd.DataFrame({
        "COMID": comids,
        "dprst_depth_m": depths,
        "measured_max_m": [d + 0.5 for d in depths],
        "hollister_max_m": [d + 0.2 for d in depths],
        "flat": [False] * n,
        "resolution": ["10m"] * n,
        "method": ["measured"] * n,
    }).to_parquet(batch_dir / f"batch_{i:04d}.parquet", index=False)


def _parquet_ctx(tmp_path):
    return BuildContext(
        fabric="t", template_path=tmp_path / "unused_template.tif", output_dir=tmp_path,
        hru_gpkg=tmp_path / "unused_hru.gpkg", hru_layer="nhru",
    )


def test_compute_depths_ingests_and_dedupes_batch_parquets(tmp_path):
    """Two per-batch parquets (Task 9's SLURM array output shape), one
    COMID (555) present in both with DIFFERENT depths. `_compute_depths`
    must concat + `drop_duplicates(subset='COMID', keep='first')`, so the
    surviving row is whichever file sorts first (`sorted(batch_dir.glob(...))`
    -- batch_0000 before batch_0001), and every other COMID passes through
    untouched.

    #221: parquets are now only loaded alongside the `_plan/` that produced them,
    and only when that plan's polygon set is the current one -- so this fixture
    writes a matching plan and uses the real 4-digit batch names."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()
    _write_plan(batch_dir, [555, 600, 700], n_batches=2)
    _write_batch(batch_dir, 0, [555, 600], [1.0, 2.0])
    _write_batch(batch_dir, 1, [555, 700], [99.0, 3.0])  # 555's depth here must NOT win

    step_cfg = {"batch_dir": str(batch_dir)}
    dprst = _make_dprst_gdf([555, 600, 700])

    out = dprst_depth._compute_depths(dprst, ctx=_parquet_ctx(tmp_path), step_cfg=step_cfg, logger=_L())

    assert sorted(out["COMID"].tolist()) == [555, 600, 700]
    by_comid = out.set_index("COMID")
    assert by_comid.loc[555, "dprst_depth_m"] == pytest.approx(1.0)  # batch_000 kept (keep="first")
    assert by_comid.loc[600, "dprst_depth_m"] == pytest.approx(2.0)
    assert by_comid.loc[700, "dprst_depth_m"] == pytest.approx(3.0)


def test_output_columns_and_depth_columns_declare_the_same_set():
    """(#223 review round 3, also-fix 5) `_compute_depths`'s missing-column raise
    (round 2, finding 3) is scoped to the loaded-from-disk `parquet_files` branch
    ONLY, on the premise that the REAL `compute.run_batch`'s in-process output
    always carries every `dprst_depth._DEPTH_COLUMNS` member by construction --
    i.e. that `compute._OUTPUT_COLUMNS` and `dprst_depth._DEPTH_COLUMNS` name the
    SAME set. Nothing pinned that equality; if the two lists ever diverge (a
    column added to one but not the other), that premise silently breaks and the
    in-process path could ship a frame missing a declared column with no raise
    anywhere."""
    assert set(compute_mod._OUTPUT_COLUMNS) == set(dprst_depth._DEPTH_COLUMNS)


def test_compute_depths_raises_on_pre_223_batch_parquets_missing_source_columns(tmp_path):
    """`_fill_and_join`'s `keep_cols` used to silently drop whatever `_DEPTH_COLUMNS`
    member a loaded batch frame didn't have -- exactly what re-running against a
    PRE-#223 batch dir (no `source`/`interior_coverage` at all) looks like.
    `_verify_batches_match_plan` only checks `n_batches` and the COMID set, not the
    schema, so a stale batch dir passes that check silently; `_compute_depths` must
    catch it itself."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()
    _write_plan(batch_dir, [1, 2], n_batches=1)
    _write_legacy_batch(batch_dir, 0, [1, 2], [1.0, 2.0])

    with pytest.raises(RuntimeError, match=r"missing column\(s\).*source.*interior_coverage"):
        dprst_depth._compute_depths(
            _make_dprst_gdf([1, 2]), ctx=_parquet_ctx(tmp_path),
            step_cfg={"batch_dir": str(batch_dir)}, logger=_L(),
        )


def test_compute_depths_does_not_raise_on_a_legitimately_empty_batch_set(tmp_path):
    """`compute._empty_batch_frame` guarantees every `_DEPTH_COLUMNS` member for a
    legitimately empty batch (a SLURM array task with 0 assigned tile sets) -- that
    path must NOT trip the new missing-column raise, only a NON-empty frame missing
    a column should."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()
    _write_plan(batch_dir, [], n_batches=1)
    from gfv2_params.dprst_depth.compute import _empty_batch_frame
    _empty_batch_frame().to_parquet(batch_dir / "batch_0000.parquet", index=False)

    out = dprst_depth._compute_depths(
        _make_dprst_gdf([]), ctx=_parquet_ctx(tmp_path),
        step_cfg={"batch_dir": str(batch_dir)}, logger=_L(),
    )
    assert len(out) == 0


# ---------------------------------------------------------------------------
# #221 guard: the in-process fallback must refuse CONUS-scale work.
#
# With no tiled parquets on disk, `_compute_depths` used to compute every polygon
# serially and log it only at INFO. gfv2r2's first run took that path: 36,974 of
# 392,672 polygons (9.4%) in 11 h against an 18 h job limit. The fallback exists
# for small fabrics (oregon 3,717 polygons, tjc 6,113), so the guard is a polygon
# ceiling, not a ban.
# ---------------------------------------------------------------------------


def _no_batches_ctx(tmp_path):
    ctx = BuildContext(
        fabric="t", template_path=tmp_path / "unused.tif", output_dir=tmp_path,
        hru_gpkg=tmp_path / "unused.gpkg", hru_layer="nhru",
    )
    return ctx, str(tmp_path / "no_such_batch_dir")


def test_inprocess_fallback_refuses_more_polygons_than_the_ceiling(tmp_path, monkeypatch):
    """Over the ceiling -> RuntimeError naming the tiled wrapper, raised BEFORE any
    tile is grouped. Proven by making the first unit of work explode: if the guard
    were checked after starting, this would raise the wrong error."""
    ctx, missing = _no_batches_ctx(tmp_path)

    def _started(*a, **k):
        raise AssertionError("in-process compute STARTED despite exceeding the ceiling")

    monkeypatch.setattr(dprst_depth, "tile_set_groups", _started)
    dprst = _make_dprst_gdf(list(range(1, 7)))  # 6 polygons
    step_cfg = {"batch_dir": missing, "max_inprocess_polygons": 5}

    with pytest.raises(RuntimeError, match="submit_dprst_depth.sh"):
        dprst_depth._compute_depths(dprst, ctx=ctx, step_cfg=step_cfg, logger=_L())


def test_inprocess_fallback_runs_at_or_under_the_ceiling(tmp_path, monkeypatch):
    """At the ceiling exactly, the small-fabric path must still work."""
    ctx, missing = _no_batches_ctx(tmp_path)
    ran = {}

    monkeypatch.setattr(dprst_depth, "tile_set_groups", lambda d: {"k": list(d.index)})

    def _fake_run(dprst, tile_sets, out, logger, n_threads=1):
        ran["n"] = len(dprst)
        return _make_depth_df(dprst["COMID"].tolist())

    monkeypatch.setattr(dprst_depth, "run_batch", _fake_run)
    dprst = _make_dprst_gdf(list(range(1, 6)))  # exactly 5
    step_cfg = {"batch_dir": missing, "max_inprocess_polygons": 5}

    out = dprst_depth._compute_depths(dprst, ctx=ctx, step_cfg=step_cfg, logger=_L())
    assert ran["n"] == 5
    assert len(out) == 5


def test_inprocess_ceiling_zero_is_the_escape_hatch(tmp_path, monkeypatch):
    """Same convention as `dprst_depth_min_measured_frac=0`: 0 disables the guard, for
    an operator who has decided the long in-process run is what they want."""
    ctx, missing = _no_batches_ctx(tmp_path)
    monkeypatch.setattr(dprst_depth, "tile_set_groups", lambda d: {"k": list(d.index)})
    monkeypatch.setattr(
        dprst_depth, "run_batch", lambda d, t, o, lg, n_threads=1: _make_depth_df(d["COMID"].tolist())
    )
    dprst = _make_dprst_gdf(list(range(1, 7)))
    step_cfg = {"batch_dir": missing, "max_inprocess_polygons": 0}

    out = dprst_depth._compute_depths(dprst, ctx=ctx, step_cfg=step_cfg, logger=_L())
    assert len(out) == 6


def test_default_inprocess_ceiling_separates_small_fabrics_from_conus():
    """The default must let every small fabric through and stop every CONUS one.
    Measured from the tiled planners' own manifests (2026-09-18): oregon 3,717 and tjc
    6,113 polygons; gfv2 279,391 (July) and gfv2_dev 392,673. It must also fit the job:
    at the ~3,300 polygons/h gfv2r2 actually achieved, the ceiling has to finish well
    inside build_depstor_rasters.batch's 18 h limit."""
    ceiling = dprst_depth.DEFAULT_MAX_INPROCESS_POLYGONS
    assert max(3_717, 6_113) < ceiling < min(279_391, 392_673)
    assert ceiling / 3_300 < 18 * 0.6, "the ceiling would not comfortably finish in 18 h"


# ---------------------------------------------------------------------------
# #221: tiled parquets are loaded only when they belong to the CURRENT polygon set.
#
# Nothing ever cleared `dprst_depth_batches/`, and the builder loaded whatever it
# found. The documented cascade rebuild after a classifier change -- `--from
# wbody_connectivity --force` -- runs through dprst_depth and so picked up parquets
# planned for the OLD polygon set: consistent with their own plan, wrong for the
# current one. gfv2's parquets (279,391 polygons, July) predate the segment-driven
# classifier that now yields ~392k on the same extent. The final product was only
# protected by coincidence (left join onto the current set; fresh batch indices
# sorting before stale ones under keep="first"). These make it structural.
# ---------------------------------------------------------------------------


def test_parquets_planned_for_a_different_polygon_set_are_refused(tmp_path):
    """The classifier-changed case: plan and parquets agree with each other, and
    disagree with the polygons the builder is about to fill. A count check would pass
    this; only comparing the sets catches it."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()
    _write_plan(batch_dir, [1, 2, 3], n_batches=1)
    _write_batch(batch_dir, 0, [1, 2, 3], [1.0, 1.0, 1.0])

    current = _make_dprst_gdf([1, 2, 4, 5])  # 3 gone, 4 and 5 new
    with pytest.raises(RuntimeError, match="submit_dprst_depth.sh") as exc:
        dprst_depth._compute_depths(
            current, ctx=_parquet_ctx(tmp_path),
            step_cfg={"batch_dir": str(batch_dir)}, logger=_L(),
        )
    msg = str(exc.value)
    assert "2 new" in msg and "1 no longer" in msg, msg


def test_parquets_without_their_plan_are_refused(tmp_path):
    """No _plan/ means there is no way to tell which polygon set they were computed
    for. A stale output directory is never a legitimate configuration."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()
    _write_batch(batch_dir, 0, [1, 2], [1.0, 1.0])

    with pytest.raises(RuntimeError, match="_plan"):
        dprst_depth._compute_depths(
            _make_dprst_gdf([1, 2]), ctx=_parquet_ctx(tmp_path),
            step_cfg={"batch_dir": str(batch_dir)}, logger=_L(),
        )


def test_a_missing_batch_file_is_refused(tmp_path):
    """A partially-completed array leaves polygons without a depth. They would be
    quietly regionally filled instead of measured; the measured-fraction gate only
    catches a MASS failure, not a batch or two."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()
    _write_plan(batch_dir, [1, 2, 3], n_batches=3)
    _write_batch(batch_dir, 0, [1], [1.0])
    _write_batch(batch_dir, 2, [3], [1.0])  # batch_0001 never written

    with pytest.raises(RuntimeError, match="batch_0001"):
        dprst_depth._compute_depths(
            _make_dprst_gdf([1, 2, 3]), ctx=_parquet_ctx(tmp_path),
            step_cfg={"batch_dir": str(batch_dir)}, logger=_L(),
        )


def test_a_stale_extra_batch_file_is_refused(tmp_path):
    """A new plan with FEWER batches used to leave the old high-index files behind,
    and the builder globbed them in with the fresh ones."""
    batch_dir = tmp_path / "dprst_depth_batches"
    batch_dir.mkdir()
    _write_plan(batch_dir, [1, 2], n_batches=2)
    _write_batch(batch_dir, 0, [1], [1.0])
    _write_batch(batch_dir, 1, [2], [1.0])
    _write_batch(batch_dir, 7, [99], [5.0])  # left over from an older, larger plan

    with pytest.raises(RuntimeError, match="batch_0007"):
        dprst_depth._compute_depths(
            _make_dprst_gdf([1, 2]), ctx=_parquet_ctx(tmp_path),
            step_cfg={"batch_dir": str(batch_dir)}, logger=_L(),
        )
