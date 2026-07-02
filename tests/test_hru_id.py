import geopandas as gpd
import numpy as np
from shapely.geometry import box

from gfv2_params.depstor import RasterInfo, rasterize_ids


def test_rasterize_ids_burns_attribute(tmp_path):
    # 4x4 grid, 1.0 cell size, origin (0, 4) north-up
    import rasterio
    from rasterio.transform import from_origin
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
