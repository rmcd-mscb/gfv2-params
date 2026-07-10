import importlib.util
from pathlib import Path

import geopandas as gpd
import numpy as np
from shapely.geometry import Point

_spec = importlib.util.spec_from_file_location(
    "dprst_depth_probe",
    Path(__file__).resolve().parent.parent / "scripts" / "diagnose" / "dprst_depth_probe.py",
)
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)


def _wb(rows):
    # rows: list of (COMID, member_comid, FTYPE)
    return gpd.GeoDataFrame(
        {
            "COMID": [r[0] for r in rows],
            "member_comid": [r[1] for r in rows],
            "FTYPE": [r[2] for r in rows],
            "geometry": [Point(i, 0).buffer(1) for i in range(len(rows))],
        },
        crs="EPSG:5070",
    )


def test_dprst_polygons_classification():
    wb = _wb(
        [
            (10, 10, "LakePond"),    # on-stream (COMID in connected)  -> dropped
            (11, 11, "LakePond"),    # off-stream                       -> dprst
            (12, 12, "Playa"),       # in connected but Playa           -> dprst (forced)
            (13, 13, "Ice Mass"),    # off-stream Ice Mass              -> excluded
        ]
    )
    connected = {10, 12}
    out = probe.dprst_polygons(wb, connected)
    comids = set(out["COMID"])
    assert comids == {11, 12}          # 11 off-stream, 12 playa forced
    assert 10 not in comids            # genuine on-stream LakePond removed
    assert 13 not in comids            # Ice Mass excluded entirely


def test_resolution_class_assigns_1m_inside_footprint():
    import geopandas as gpd
    from shapely.geometry import Point, box

    dprst = gpd.GeoDataFrame(
        {"COMID": [1, 2], "geometry": [Point(0.5, 0.5).buffer(0.1), Point(9, 9).buffer(0.1)]},
        crs="EPSG:5070",
    )
    wesm = gpd.GeoDataFrame(
        {"workunit": ["A"], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:5070"
    )
    out = probe.resolution_class(dprst, wesm)
    assert list(out.sort_values("COMID")["best_topo"]) == ["1m", "10m"]


def test_depth_to_spill_and_mean_depth_on_synthetic_bowl():
    # 5x5 flat plateau at z=10 with a single 3x3 pit of depth 2 (z=8).
    dem = np.full((5, 5), 10.0, dtype=np.float64)
    dem[1:4, 1:4] = 8.0
    depth = probe.depth_to_spill(dem)
    # Filled restores the pit to the rim (10); depth = 2 in the pit, 0 on the rim.
    assert np.isclose(depth[2, 2], 2.0)
    assert np.isclose(depth[0, 0], 0.0)

    mask = depth > 0            # the 3x3 pit
    v, a, mean_d = probe.volume_mean_depth(depth, mask, cell_area_m2=1.0)
    assert np.isclose(a, 9.0)          # 9 cells * 1 m^2
    assert np.isclose(v, 18.0)         # 9 cells * depth 2 * 1 m^2
    assert np.isclose(mean_d, 2.0)     # V/A
