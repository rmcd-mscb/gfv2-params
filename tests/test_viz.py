"""Tests for the results-viewer helper library `gfv2_params.viz`.

Synthetic data only — no real fabric files, so these run in CI. Plotting uses
the non-interactive Agg backend (selected globally in tests/conftest.py).
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
import rasterio
from affine import Affine
from rasterio.crs import CRS
from shapely.geometry import Polygon, box

from gfv2_params import viz

# --------------------------------------------------------------------------- #
# grid-snapping helpers (mirrored verbatim from clip_shared_to_fabric.py)
# --------------------------------------------------------------------------- #

def test_snap_bounds_to_grid_contains_input():
    # north-up: 30 m cells, origin (0, 1000)
    transform = Affine(30, 0, 0, 0, -30, 1000)
    bounds = (115.0, 217.0, 403.0, 661.0)
    snapped = viz.snap_bounds_to_grid(bounds, transform, buffer_cells=8)
    ulx, uly, lrx, lry = snapped
    # snapped window fully contains the (unbuffered) input bounds
    assert ulx <= bounds[0]
    assert lry <= bounds[1]
    assert lrx >= bounds[2]
    assert uly >= bounds[3]
    # corners lie on the pixel lattice
    for x in (ulx, lrx):
        assert abs(((x - transform.c) / transform.a) - round((x - transform.c) / transform.a)) < 1e-9
    for y in (uly, lry):
        assert abs(((transform.f - y) / -transform.e) - round((transform.f - y) / -transform.e)) < 1e-9


def test_whole_cell_offset_aligned_is_zero():
    ref = Affine(30, 0, 0, 0, -30, 1000)
    col_frac, row_frac = viz.whole_cell_offset(ref, ref)
    assert abs(col_frac) < 1e-9
    assert abs(row_frac) < 1e-9


# --------------------------------------------------------------------------- #
# raster reads
# --------------------------------------------------------------------------- #

def _write_tif(path, arr, nodata, transform=None, crs="EPSG:5070"):
    if transform is None:
        transform = Affine(30, 0, 0, 0, -30, 100 * 30)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype=arr.dtype,
        crs=CRS.from_string(crs),
        transform=transform,
        nodata=nodata,
    ) as dst:
        dst.write(arr, 1)


def test_read_overview_masks_nodata(tmp_path):
    arr = np.arange(100 * 100, dtype="float32").reshape(100, 100)
    arr[:10, :10] = -9999.0
    path = tmp_path / "ov.tif"
    _write_tif(path, arr, nodata=-9999.0)

    out = viz.read_overview(path, target_px=50)
    assert isinstance(out, np.ma.MaskedArray)
    assert out.shape[0] <= 50 and out.shape[1] <= 50
    # the nodata corner must be masked
    assert out.mask.any()
    assert -9999.0 not in out.compressed()


def test_clip_overview_shape_and_extent(tmp_path):
    arr = np.arange(100 * 100, dtype="float32").reshape(100, 100)
    transform = Affine(30, 0, 0, 0, -30, 3000)  # origin (0, 3000)
    path = tmp_path / "clip.tif"
    _write_tif(path, arr, nodata=None, transform=transform)

    full, full_extent = viz.clip_overview(path, (0, 0, 3000, 3000), target_px=200)
    # extent ordering is (minx, maxx, miny, maxy)
    assert full_extent[0] < full_extent[1]
    assert full_extent[2] < full_extent[3]

    # a sub-window returns fewer cells than the full read
    sub, sub_extent = viz.clip_overview(path, (0, 1500, 1500, 3000), target_px=200)
    assert isinstance(sub, np.ma.MaskedArray)
    assert sub.size < full.size
    assert sub_extent[1] <= full_extent[1] + 1e-6
    assert sub_extent[2] >= full_extent[2] - 1e-6


def test_clip_overview_pads_out_of_bounds(tmp_path):
    # raster covers (0,0)-(3000,3000); request a window extending past its
    # left/top edges. boundless=True must pad with nodata, and the pad masked.
    arr = np.ones((100, 100), dtype="float32")
    transform = Affine(30, 0, 0, 0, -30, 3000)
    path = tmp_path / "oob.tif"
    _write_tif(path, arr, nodata=-9999.0, transform=transform)

    out, _ = viz.clip_overview(path, (-1500, 1500, 1500, 4500), target_px=300)
    assert isinstance(out, np.ma.MaskedArray)
    assert out.mask.any()            # the out-of-raster pad is masked
    assert 1.0 in out.compressed()   # the in-raster cells survive


# --------------------------------------------------------------------------- #
# fabric bounds
# --------------------------------------------------------------------------- #

def test_fabric_bounds_reprojects(tmp_path):
    gpkg = tmp_path / "f.gpkg"
    _synthetic_fabric("hru_id").to_file(gpkg, layer="nhru", driver="GPKG")
    native = viz.fabric_bounds(gpkg, "nhru")
    reproj = viz.fabric_bounds(gpkg, "nhru", dst_crs="EPSG:4326")
    assert native != reproj  # reprojection actually changed the bounds


def test_fabric_bounds_raises_on_empty(tmp_path):
    gdf = gpd.GeoDataFrame({"hru_id": [1], "geometry": [Polygon()]}, crs="EPSG:5070")
    gpkg = tmp_path / "empty.gpkg"
    gdf.to_file(gpkg, layer="nhru", driver="GPKG")
    with pytest.raises(ValueError):
        viz.fabric_bounds(gpkg, "nhru")


# --------------------------------------------------------------------------- #
# param load / join
# --------------------------------------------------------------------------- #

def _synthetic_fabric(id_feature="nat_hru_id"):
    geoms = [box(i, 0, i + 1, 1) for i in range(3)]
    return gpd.GeoDataFrame(
        {id_feature: [1, 2, 3], "geometry": geoms},
        crs="EPSG:5070",
    )


def test_load_param_left_join_preserves_all_hrus(tmp_path):
    fabric = _synthetic_fabric()
    # CSV covers only HRUs 1 and 2
    df = pd.DataFrame({"nat_hru_id": [1, 2], "mean": [10.0, 20.0]})
    csv = tmp_path / "p.csv"
    df.to_csv(csv, index=False)

    gdf = viz.load_param(tmp_path, "p.csv", fabric, "nat_hru_id")
    assert len(gdf) == 3
    by_id = gdf.set_index("nat_hru_id")["mean"]
    assert by_id[1] == 10.0
    assert by_id[2] == 20.0
    assert pd.isna(by_id[3])


# --------------------------------------------------------------------------- #
# inventories
# --------------------------------------------------------------------------- #

def test_param_inventory_kinds():
    entries = viz.param_inventory()
    assert len(entries) == 26
    assert all(isinstance(e, viz.ParamEntry) for e in entries)
    by_name = {e.name: e for e in entries}
    assert by_name["soils"].kind == "categorical"
    assert by_name["cov_type"].kind == "categorical"
    # representative continuous params from each pipeline section
    assert by_name["elevation"].kind == "continuous"
    assert by_name["srain_intcp"].csv_name == "nhm_lulc_nhm_v11_params.csv"
    assert by_name["wrain_intcp"].csv_name == "nhm_lulc_nhm_v11_params.csv"
    # nhm_v11 winter-canopy term has both schema variants (crosswalk: retention,
    # faithful lulc_prederived: rad_trncf); the render loop skips the absent one.
    assert by_name["rad_trncf"].csv_name == "nhm_lulc_nhm_v11_params.csv"
    assert by_name["rad_trncf"].kind == "continuous"
    # ssflux PRMS params read from the gap-filled Stage 7 CSV
    for n in ("soil2gw_max", "ssr2gw_rate", "fastcoef_lin", "slowcoef_lin",
              "gwflow_coef", "dprst_seep_rate_open", "dprst_flow_coef"):
        assert by_name[n].csv_name == "filled_nhm_ssflux_params.csv"
        assert by_name[n].kind == "continuous"


def test_shared_raster_inventory_skips_missing(tmp_path):
    vrt_dir = tmp_path / "shared" / "conus" / "vrt"
    vrt_dir.mkdir(parents=True)
    for name in ("elevation.vrt", "slope.vrt", "aspect.vrt"):
        (vrt_dir / name).write_text("")  # existence-only
    twi = tmp_path / "twi.vrt"
    twi.write_text("")
    cfg = {
        "data_root": str(tmp_path),
        "twi_raster": str(twi),
        "fdr_raster": str(tmp_path / "missing_fdr.vrt"),  # absent -> skipped
    }
    with pytest.warns(UserWarning, match="fdr"):
        entries = viz.shared_raster_inventory(cfg)
    names = {e.name for e in entries}
    assert {"twi", "elevation", "slope", "aspect"} <= names
    assert "fdr" not in names  # skipped because the path is missing
    assert all(e.kind in ("continuous", "categorical") for e in entries)


def test_zonal_source_inventory_resolves_placeholders(tmp_path):
    # load_config leaves {data_root}/{fabric} unresolved inside the params list;
    # the inventory builder must resolve them itself.
    (tmp_path / "e.tif").write_text("")
    (tmp_path / "s.tif").write_text("")
    zonal_cfg = {
        "data_root": str(tmp_path),
        "fabric": "oregon",
        "params": [
            {"name": "elevation", "source_raster": "{data_root}/e.tif"},
            {"name": "soils", "source_raster": "{data_root}/s.tif"},
            {"name": "ssflux", "source_shapefile": "{data_root}/x.shp"},  # no raster
        ],
    }
    entries = viz.zonal_source_inventory(zonal_cfg)
    by_name = {e.name: e for e in entries}
    assert set(by_name) == {"elevation", "soils"}  # ssflux excluded (no source_raster)
    assert "{data_root}" not in str(by_name["elevation"].path)  # placeholder resolved
    assert by_name["soils"].kind == "categorical"
    assert by_name["elevation"].kind == "continuous"


def test_depstor_raster_inventory_skips_missing(tmp_path):
    base = tmp_path / "fab" / "depstor_rasters"
    base.mkdir(parents=True)
    (base / "land_mask.tif").write_text("")  # only one of the 14 present
    cfg = {"data_root": str(tmp_path), "fabric": "fab"}
    with pytest.warns(UserWarning):
        entries = viz.depstor_raster_inventory(cfg)
    assert [e.name for e in entries] == ["land_mask"]
    assert entries[0].kind == "categorical"


def test_entry_kind_validation():
    with pytest.raises(ValueError):
        viz.ParamEntry(name="x", csv_name="y.csv", column="z", kind="bogus")
    with pytest.raises(ValueError):
        viz.RasterEntry("x", "p.tif", "bogus")


def test_dedupe_raster_entries_preserves_first_and_order():
    e1 = viz.RasterEntry("twi", "/x/twi.vrt", "continuous")
    e2 = viz.RasterEntry("fdr", "/x/oregon_fdr.vrt", "categorical")
    e3 = viz.RasterEntry("template", "/x/oregon_fdr.vrt", "categorical")  # dup path
    e4 = viz.RasterEntry("elevation", "/x/elev.vrt", "continuous")
    e5 = viz.RasterEntry("elevation", "/x/elev.vrt", "continuous")        # dup path
    out = viz.dedupe_raster_entries([e1, e2, e3, e4, e5])
    assert [e.name for e in out] == ["twi", "fdr", "elevation"]  # first wins, order kept


# --------------------------------------------------------------------------- #
# plot_raster: categorical class legend (vs colorbar) + nodata grey
# --------------------------------------------------------------------------- #

def test_plot_raster_categorical_builds_class_legend():
    # 3 classes (1, 2, 3) — should render one legend handle per class
    data = np.array([[1, 2, 3], [3, 2, 1]], dtype="float32")
    arr = np.ma.array(data, mask=np.zeros_like(data, dtype=bool))
    fig, ax = plt.subplots()
    viz.plot_raster(ax, arr, categorical=True, title="soils", label="class")
    leg = ax.get_legend()
    assert leg is not None
    assert [t.get_text() for t in leg.get_texts()] == ["1", "2", "3"]
    plt.close(fig)


def test_plot_raster_categorical_falls_back_to_colorbar_above_max_legend():
    # 5 classes with max_legend=4 -> falls back to colorbar (no legend)
    data = np.array([[1, 2, 3, 4, 5]], dtype="float32")
    arr = np.ma.array(data, mask=np.zeros_like(data, dtype=bool))
    fig, ax = plt.subplots()
    viz.plot_raster(ax, arr, categorical=True, label="vpu", max_legend=4)
    assert ax.get_legend() is None
    plt.close(fig)


def test_plot_raster_masks_nodata_with_grey():
    # Continuous: cmap.set_bad("lightgrey") should be applied so masked cells
    # render distinguishably from valid data.
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    arr = np.ma.array(data, mask=[[True, False], [False, False]])
    fig, ax = plt.subplots()
    im = viz.plot_raster(ax, arr, categorical=False, cmap="viridis")
    bad_rgba = im.get_cmap().get_bad()
    # matplotlib "lightgrey" == (0.827, 0.827, 0.827, 1.0)
    assert bad_rgba[:3] == pytest.approx((0.8274509803921568,) * 3, abs=1e-3)
    plt.close(fig)


# --------------------------------------------------------------------------- #
# plotting helpers return Figures
# --------------------------------------------------------------------------- #

def test_map_continuous_returns_figure():
    fabric = _synthetic_fabric()
    fabric["mean"] = [1.0, 2.0, 3.0]
    fig = viz.map_continuous(fabric, "mean", "T", units="m")
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_map_categorical_returns_figure():
    fabric = _synthetic_fabric()
    fabric["soils"] = [1, 2, 1]
    fig = viz.map_categorical(fabric, "soils", "S")
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


# --------------------------------------------------------------------------- #
# save-figures workflow
# --------------------------------------------------------------------------- #

def test_save_figure_writes_when_enabled(tmp_path, monkeypatch):
    monkeypatch.setattr(viz, "FIGURES_DIR", tmp_path)
    monkeypatch.setattr(viz, "FABRIC", "testfab")
    monkeypatch.setattr(viz, "SAVE_FIGURES", True)
    fig, ax = plt.subplots()
    viz.save_figure(fig, "x")
    plt.close(fig)
    assert (tmp_path / "testfab" / "x.png").exists()


def test_save_figure_noop_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(viz, "FIGURES_DIR", tmp_path)
    monkeypatch.setattr(viz, "FABRIC", "testfab")
    monkeypatch.setattr(viz, "SAVE_FIGURES", False)
    fig, ax = plt.subplots()
    viz.save_figure(fig, "x")
    plt.close(fig)
    assert not (tmp_path / "testfab" / "x.png").exists()


def test_save_figure_warns_without_fabric(tmp_path, monkeypatch):
    monkeypatch.setattr(viz, "FIGURES_DIR", tmp_path)
    monkeypatch.setattr(viz, "FABRIC", None)
    monkeypatch.setattr(viz, "SAVE_FIGURES", True)
    fig, ax = plt.subplots()
    with pytest.warns(UserWarning):
        viz.save_figure(fig, "y")
    plt.close(fig)
    # un-namespaced: lands directly in the base dir, not a fabric subdir
    assert (tmp_path / "y.png").exists()


# --------------------------------------------------------------------------- #
# Interactive folium overlays (depstor "output-binary" rasters)
# --------------------------------------------------------------------------- #

def test_depstor_output_binary_inventory_filters_and_warns(tmp_path):
    # Stage 2 of the 7 curated binaries; the other 5 should be skipped+warned.
    base = tmp_path / "fab" / "depstor_rasters"
    base.mkdir(parents=True)
    (base / "perv_binary.tif").write_text("")
    (base / "carea_map_t8_binary.tif").write_text("")
    cfg = {"data_root": str(tmp_path), "fabric": "fab"}
    with pytest.warns(UserWarning):
        entries = viz.depstor_output_binary_inventory(cfg)
    names = [e.name for e in entries]
    assert names == ["perv_binary", "carea_map_t8_binary"]  # first-occurrence order
    assert all(isinstance(e, viz.OverlayEntry) for e in entries)
    assert all(e.color.startswith("#") for e in entries)
    assert "carea_max" in entries[1].feeds  # human-readable provenance


def test_build_overlay_image_reprojects_to_latlon_and_marks_on_cells(tmp_path):
    # Synthetic binary raster in EPSG:5070 over a small Albers window. The
    # function should reproject to EPSG:4326, return an (H,W,4) uint8 array,
    # paint on-cells (value > threshold) in the requested color, and leave
    # off-cells fully transparent.
    arr = np.array([[0, 0, 1, 1], [0, 1, 1, 1]], dtype="uint8")  # 6 on-cells out of 8
    transform = Affine(30, 0, 1_000_000.0, 0, -30, 2_000_000.0)  # arbitrary Albers
    path = tmp_path / "binary.tif"
    _write_tif(path, arr, nodata=255, transform=transform, crs="EPSG:5070")

    rgba, bounds = viz.build_overlay_image(
        path, color="#ff0000", alpha=0.5, target_px=8, threshold=0.5,
    )
    assert rgba.ndim == 3 and rgba.shape[-1] == 4
    assert rgba.dtype == np.uint8

    # On-cells: red @ 50% alpha = (255, 0, 0, 127)
    on_mask = rgba[..., 3] > 0
    assert on_mask.any()
    on_rgba = rgba[on_mask]
    assert (on_rgba[:, 0] == 255).all() and (on_rgba[:, 1] == 0).all() and (on_rgba[:, 2] == 0).all()
    assert (on_rgba[:, 3] == 127).all()
    # Off-cells fully transparent
    assert (rgba[~on_mask, 3] == 0).all()

    # Bounds are in lat/lon (CONUS Albers origin reprojects to roughly N hemisphere)
    w, s, e, n = bounds
    assert -180 <= w < e <= 180
    assert -90 <= s < n <= 90


def test_raster_to_image_overlay_returns_folium_overlay(tmp_path):
    folium = pytest.importorskip("folium")  # only in notebooks env
    arr = np.array([[0, 1], [1, 1]], dtype="uint8")
    transform = Affine(30, 0, 1_000_000.0, 0, -30, 2_000_000.0)
    path = tmp_path / "binary.tif"
    _write_tif(path, arr, nodata=255, transform=transform, crs="EPSG:5070")

    ov = viz.raster_to_image_overlay(path, name="perv", color="#00ff00", target_px=4)
    assert isinstance(ov, folium.raster_layers.ImageOverlay)
    assert ov.layer_name == "perv"
    # bounds shape: [[south, west], [north, east]]
    b = ov.bounds
    assert len(b) == 2 and len(b[0]) == 2 and len(b[1]) == 2
    assert b[0][0] < b[1][0]  # south < north
    assert b[0][1] < b[1][1]  # west < east
