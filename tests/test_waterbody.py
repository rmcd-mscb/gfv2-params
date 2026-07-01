"""Tests for the `waterbody` depstor builder (clump rasterisation + FTYPE exclusion)."""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box


def _write_template(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="float32",
        crs="EPSG:5070", transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(np.full((n, n), 100.0, dtype=np.float32), 1)


def _write_landmask(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="uint8",
        crs="EPSG:5070", transform=transform, nodata=255,
    ) as dst:
        dst.write(np.ones((n, n), dtype=np.uint8), 1)  # all land


def test_waterbody_excludes_ice_mass_keeps_lakepond(tmp_path):
    """Ice Mass polygons must be absent from wbody_binary; LakePond must remain."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    # 2 waterbodies, both well above the default min_area (900 m^2):
    #   WB 10 — LakePond, top-left 2x2 block (3600 m^2) -> must be kept
    #   WB 20 — Ice Mass, bottom-right 2x2 block (3600 m^2) -> must be excluded
    gdf = gpd.GeoDataFrame(
        {"COMID": [10, 20], "FTYPE": ["LakePond", "Ice Mass"]},
        geometry=[
            box(0, 270, 60, 300),   # top-left 2x2: cells [0,0],[0,1],[1,0],[1,1]
            box(240, 0, 300, 30),   # bottom-right 2x2: cells [8,8],[8,9],[9,8],[9,9]
        ],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
    )
    ctx.paths["landmask"] = landmask

    produced = waterbody.build(
        {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["wbody_binary"]) as src:
        arr = src.read(1)

    assert arr[0, 0] == 1     # WB 10 (LakePond) present
    assert arr[9, 9] != 1     # WB 20 (Ice Mass) absent


def test_waterbody_missing_ftype_column_is_graceful(tmp_path):
    """A waterbody layer without FTYPE must still build (warn, don't crash)."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10]},
        geometry=[box(0, 270, 60, 300)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
    )
    ctx.paths["landmask"] = landmask

    produced = waterbody.build(
        {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["wbody_binary"]) as src:
        arr = src.read(1)
    assert arr[0, 0] == 1  # waterbody still rasterised despite missing FTYPE
