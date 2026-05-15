"""Smoke test for rasterize_land_mask in gfv2_params.depstor_builders.landmask.

Builds a 10x10 template raster and a 2-polygon HRU fixture, rasterises them,
and asserts the resulting land mask uses the 1/255 convention with the right
cells marked land.
"""

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon

from gfv2_params.depstor_builders.landmask import rasterize_land_mask


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
    """Two squares occupying the top-left and bottom-right of a 10x10 30m grid."""
    top_left = Polygon([(0, 240), (60, 240), (60, 300), (0, 300)])      # rows 0-1, cols 0-1
    bottom_right = Polygon([(240, 0), (300, 0), (300, 60), (240, 60)])  # rows 8-9, cols 8-9
    gdf = gpd.GeoDataFrame(
        {"nat_hru_id": [1, 2]}, geometry=[top_left, bottom_right], crs="EPSG:5070",
    )
    gdf.to_file(path, layer="nhru", driver="GPKG")


def test_rasterize_land_mask_two_polygons(tmp_path):
    template_path = tmp_path / "template.tif"
    hru_gpkg = tmp_path / "fabric.gpkg"
    out_path = tmp_path / "land_mask.tif"

    _write_template(template_path)
    _write_two_polygon_gpkg(hru_gpkg)

    binary, n_polys = rasterize_land_mask(
        template_path, hru_gpkg, "nhru", out_path, logging.getLogger("test"),
    )

    assert n_polys == 2
    assert binary.dtype == np.uint8
    assert binary.shape == (10, 10)

    with rasterio.open(out_path) as src:
        assert src.nodata == 255
        out = src.read(1)
    np.testing.assert_array_equal(out, binary)

    land_count = int((out == 1).sum())
    nodata_count = int((out == 255).sum())
    assert land_count == 8                       # 2 polygons * 4 cells each
    assert land_count + nodata_count == 100
    assert out[0, 0] == 1
    assert out[9, 9] == 1
    assert out[5, 5] == 255


def test_rasterize_land_mask_empty_fabric(tmp_path):
    """Empty fabric (no polygons after filtering) yields all-nodata mask."""
    template_path = tmp_path / "template.tif"
    hru_gpkg = tmp_path / "fabric.gpkg"
    out_path = tmp_path / "land_mask.tif"

    _write_template(template_path)
    gpd.GeoDataFrame(
        {"nat_hru_id": []}, geometry=[], crs="EPSG:5070",
    ).to_file(hru_gpkg, layer="nhru", driver="GPKG")

    binary, n_polys = rasterize_land_mask(
        template_path, hru_gpkg, "nhru", out_path, logging.getLogger("test"),
    )

    assert n_polys == 0
    assert (binary == 255).all()
