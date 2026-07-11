import numpy as np
from affine import Affine

from gfv2_params.dprst_depth.compute import _polygon_depth_from_dem


def test_polygon_depth_from_dem_bowl_and_flat():
    # 20x20, 1 m cells; 8x8 pit depth 2 in the centre; rest flat rim
    dem = np.full((20, 20), 10.0, np.float64)
    dem[6:14, 6:14] = 8.0
    mask = np.zeros((20, 20), bool)
    mask[6:14, 6:14] = True  # interior = the pit
    r = _polygon_depth_from_dem(dem, mask, Affine.scale(1, -1), nodata=-9999.0)
    assert not r["flat"]
    assert np.isclose(r["dprst_depth_m"], 2.0)  # V/A over the pit
    assert np.isclose(r["measured_max_m"], 2.0)

    flat = np.full((20, 20), 5.0, np.float64)
    rf = _polygon_depth_from_dem(flat, mask, Affine.scale(1, -1), nodata=-9999.0)
    assert rf["flat"] and np.isnan(rf["dprst_depth_m"])
