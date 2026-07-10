import importlib.util
from pathlib import Path

import geopandas as gpd
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
