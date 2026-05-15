"""Smoke test for the build_landmask core function in build_depstor_landmask.

Builds a 10x10 template raster and a 2-polygon HRU fixture, rasterises them,
and asserts the resulting land mask uses the 1/255 convention with the right
cells marked land.
"""

import importlib.util
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon


_SCRIPT = Path(__file__).parent.parent / "scripts" / "build_depstor_landmask.py"
_spec = importlib.util.spec_from_file_location("build_depstor_landmask", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
build_landmask = _mod.build_landmask


def _write_template(path: Path, height: int = 10, width: int = 10) -> None:
    """Write a tiny float32 template raster at 30 m on an EPSG:5070-like grid."""
    transform = from_origin(0, height * 30, 30, 30)
    data = np.full((height, width), 100.0, dtype=np.float32)
    with rasterio.open(
        path, "w", driver="GTiff", height=height, width=width, count=1,
        dtype="float32", crs="EPSG:5070", transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(data, 1)


def _write_two_polygon_gpkg(path: Path) -> None:
    """Two squares occupying the top-left and bottom-right of a 10x10 30m grid.

    Top-left square covers rows 0-1, cols 0-1 (origin at top); bottom-right
    covers rows 8-9, cols 8-9. Everything else is "ocean" (outside the fabric).
    """
    # Grid is 10x10 cells at 30m, top at y=300, bottom at y=0.
    top_left = Polygon([(0, 240), (60, 240), (60, 300), (0, 300)])      # rows 0-1, cols 0-1
    bottom_right = Polygon([(240, 0), (300, 0), (300, 60), (240, 60)])  # rows 8-9, cols 8-9
    gdf = gpd.GeoDataFrame(
        {"nat_hru_id": [1, 2]}, geometry=[top_left, bottom_right], crs="EPSG:5070",
    )
    gdf.to_file(path, layer="nhru", driver="GPKG")


def test_build_landmask_two_polygons(tmp_path):
    """Smoke test: rasterising two HRU polygons produces a 1/255 land mask
    with exactly those cells marked land and everything else as nodata."""
    template_path = tmp_path / "template.tif"
    hru_gpkg = tmp_path / "fabric.gpkg"
    out_path = tmp_path / "land_mask.tif"

    _write_template(template_path)
    _write_two_polygon_gpkg(hru_gpkg)

    binary, n_polys = build_landmask(
        template_path, hru_gpkg, "nhru", out_path, logging.getLogger("test"),
    )

    assert n_polys == 2
    assert binary.dtype == np.uint8
    assert binary.shape == (10, 10)

    # Re-read what was written so we exercise the full IO path.
    with rasterio.open(out_path) as src:
        assert src.nodata == 255
        out = src.read(1)
    np.testing.assert_array_equal(out, binary)

    # Cells covered by either polygon are 1; everything else is 255.
    land_count = int((out == 1).sum())
    nodata_count = int((out == 255).sum())
    assert land_count == 8                       # 2 polygons * 4 cells each
    assert land_count + nodata_count == 100
    # Top-left corner is land
    assert out[0, 0] == 1
    # Bottom-right corner is land
    assert out[9, 9] == 1
    # Middle is ocean
    assert out[5, 5] == 255


def test_build_landmask_empty_fabric(tmp_path):
    """Empty fabric (no polygons after filtering) yields all-nodata mask."""
    template_path = tmp_path / "template.tif"
    hru_gpkg = tmp_path / "fabric.gpkg"
    out_path = tmp_path / "land_mask.tif"

    _write_template(template_path)
    gpd.GeoDataFrame(
        {"nat_hru_id": []}, geometry=[], crs="EPSG:5070",
    ).to_file(hru_gpkg, layer="nhru", driver="GPKG")

    binary, n_polys = build_landmask(
        template_path, hru_gpkg, "nhru", out_path, logging.getLogger("test"),
    )

    assert n_polys == 0
    assert (binary == 255).all()
