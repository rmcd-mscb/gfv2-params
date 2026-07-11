import geopandas as gpd
from shapely.geometry import Point, box

from gfv2_params.download.epa_ecoregions import ecoregion_of


def test_ecoregion_of_assigns_by_centroid():
    polys = gpd.GeoDataFrame(
        {"COMID": [1, 2]},
        geometry=[Point(0.5, 0.5).buffer(0.2), Point(9, 9).buffer(0.2)],
        crs="EPSG:5070",
    )
    eco = gpd.GeoDataFrame(
        {"US_L3CODE": ["17", "80"]},
        geometry=[box(0, 0, 1, 1), box(8, 8, 10, 10)],
        crs="EPSG:5070",
    )
    out = ecoregion_of(polys, eco)
    assert list(out) == ["17", "80"]
