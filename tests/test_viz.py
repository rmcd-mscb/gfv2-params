"""Tests for the results-viewer helper library `gfv2_params.viz`.

Synthetic data only — no real fabric files, so these run in CI. Plotting uses
the non-interactive Agg backend (selected globally in tests/conftest.py).
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from affine import Affine
from rasterio.crs import CRS
from shapely.geometry import box

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
    assert len(entries) == 16
    assert all(isinstance(e, viz.ParamEntry) for e in entries)
    by_name = {e.name: e for e in entries}
    assert by_name["soils"].kind == "categorical"
    assert by_name["cov_type"].kind == "categorical"
    # a representative continuous one
    assert by_name["elevation"].kind == "continuous"


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
