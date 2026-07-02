import logging

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from gfv2_params.depstor import RasterInfo, rasterize_ids


def test_rasterize_ids_burns_attribute(tmp_path):
    # 4x4 grid, 1.0 cell size, origin (0, 4) north-up
    tpl = tmp_path / "tpl.tif"
    transform = from_origin(0, 4, 1, 1)
    with rasterio.open(tpl, "w", driver="GTiff", height=4, width=4, count=1,
                       dtype="uint8", crs="EPSG:5070", transform=transform) as d:
        d.write(np.zeros((4, 4), np.uint8), 1)
    info = RasterInfo.from_path(tpl)
    gdf = gpd.GeoDataFrame(
        {"nat_hru_id": [11, 22]},
        geometry=[box(0, 0, 2, 4), box(2, 0, 4, 4)], crs="EPSG:5070",
    )
    out = rasterize_ids(gdf, "nat_hru_id", info)
    assert out.dtype == np.int32
    assert out[0, 0] == 11 and out[0, 3] == 22   # left half 11, right half 22


def _write_template(path, n: int = 4) -> None:
    transform = from_origin(0, n, 1, 1)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="uint8",
        crs="EPSG:5070", transform=transform,
    ) as dst:
        dst.write(np.zeros((n, n), dtype=np.uint8), 1)


def _write_hru_gpkg(path, ids) -> None:
    """Two squares (left half id=ids[0], right half id=ids[1]) on a 4x4 1m grid."""
    gdf = gpd.GeoDataFrame(
        {"nat_hru_id": list(ids)},
        geometry=[box(0, 0, 2, 4), box(2, 0, 4, 4)], crs="EPSG:5070",
    )
    gdf.to_file(path, layer="nhru", driver="GPKG")


def test_hru_id_build_writes_expected_int32_raster(tmp_path):
    from gfv2_params.depstor_builders import hru_id
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    hru_gpkg = tmp_path / "fabric.gpkg"
    _write_template(template)
    _write_hru_gpkg(hru_gpkg, ids=(11, 22))

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=hru_gpkg, hru_layer="nhru", id_feature="nat_hru_id",
    )

    produced = hru_id.build({"output": "hru_id.tif"}, ctx, logging.getLogger("test"))

    out_path = produced["hru_id"]
    with rasterio.open(out_path) as src:
        assert src.dtypes[0] == "int32"
        arr = src.read(1)
    assert arr[0, 0] == 11 and arr[0, 3] == 22   # left half 11, right half 22


def test_hru_id_build_raises_on_non_positive_id(tmp_path):
    from gfv2_params.depstor_builders import hru_id
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    hru_gpkg = tmp_path / "fabric.gpkg"
    _write_template(template)
    _write_hru_gpkg(hru_gpkg, ids=(0, 22))  # 0 is the invalid/no-HRU sentinel

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=hru_gpkg, hru_layer="nhru", id_feature="nat_hru_id",
    )

    with pytest.raises(ValueError):
        hru_id.build({"output": "hru_id.tif"}, ctx, logging.getLogger("test"))
