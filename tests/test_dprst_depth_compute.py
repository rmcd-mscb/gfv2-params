import numpy as np
from affine import Affine

from gfv2_params.dprst_depth.compute import _polygon_depth_from_dem


def test_polygon_depth_from_dem_bowl_and_flat():
    # 20x20, 1 m cells; 8x8 pit in the centre; rest flat rim at 10.0.
    # The pit floor is NOT perfectly constant (a real, non-hydro-flattened
    # depression's bed has genuine relief) — it grades from 8.5 down to
    # 8.0 (a 0.5 m interior range, well above is_hydroflattened's 0.01 m
    # tolerance), so the interior-only flatness gate correctly reads
    # flat=False and still measures a sensible V/A mean depth.
    dem = np.full((20, 20), 10.0, np.float64)
    pit = np.linspace(8.5, 8.0, num=8)
    dem[6:14, 6:14] = np.tile(pit, (8, 1))
    mask = np.zeros((20, 20), bool)
    mask[6:14, 6:14] = True  # interior = the pit
    r = _polygon_depth_from_dem(dem, mask, Affine.scale(1, -1), nodata=-9999.0)
    assert not r["flat"]
    expected_mean_depth = float(np.mean(10.0 - pit))
    assert np.isclose(r["dprst_depth_m"], expected_mean_depth)
    assert np.isclose(r["measured_max_m"], 2.0)  # deepest cell: 10.0 - 8.0
    assert np.isfinite(r["hollister_max_m"])

    # Hydro-flattened case: the INTERIOR is exactly constant (the
    # breakline-enforced water surface), even though the surrounding rim
    # carries real relief (sloped terrain, not a flat whole-window read —
    # this is exactly the case the window-wide gate got wrong: it would
    # have seen the rim/interior contrast as "not flat" and tried to
    # measure a depth off the flat water surface instead of correctly
    # detecting hydro-flattening from the interior alone).
    sloped = np.full((20, 20), 10.0, np.float64)
    sloped += np.arange(20).reshape(-1, 1) * 0.5  # rim relief, not constant
    sloped[6:14, 6:14] = 8.0  # hydro-flattened interior: exactly constant
    rf = _polygon_depth_from_dem(sloped, mask, Affine.scale(1, -1), nodata=-9999.0)
    assert rf["flat"] and np.isnan(rf["dprst_depth_m"])
    assert np.isnan(rf["measured_max_m"])
    assert np.isfinite(rf["hollister_max_m"])
