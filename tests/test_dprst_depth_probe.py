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


def test_depth_to_spill_zeroes_nodata_void_no_spurious_depth():
    # Same 3x3 pit as above, but with a nodata void planted at a corner
    # (simulating a tile-edge / data-gap cell inside a real read_window
    # window). A 9x9 grid (not 5x5) keeps the pit well clear of a richdem
    # small-grid edge case where a border no_data cell can suppress the fill
    # entirely on a tiny array; real read_window windows are hundreds of
    # pixels wide, so this is representative of production shapes.
    n = 9
    dem = np.full((n, n), 10.0, dtype=np.float64)
    c = n // 2
    dem[c - 1 : c + 2, c - 1 : c + 2] = 8.0
    dem[0, 0] = -9999.0  # nodata void

    # Default nodata (None -> sentinel -9999.0), matching the realistic
    # read_window -> depth_to_spill(dem) call path (no explicit nodata arg).
    depth = probe.depth_to_spill(dem)

    assert np.isclose(depth[0, 0], 0.0)       # void cell: no spurious fill-to-rim depth
    assert np.isclose(depth[c, c], 2.0)       # real pit still computed correctly
    assert np.isclose(depth[n - 1, n - 1], 0.0)  # untouched rim cell stays 0


def test_normalize_nodata_maps_voids_to_sentinel():
    # Realistic read_window shape: a real numeric source nodata (3DEP's
    # -999999, not -9999) at one cell, a NaN void at another (e.g. a tile
    # mosaic seam), and otherwise-valid data -- this is the exact branch
    # flagged as untested (only reachable via a live S3 read before this
    # extraction), since all prior smoke tests used 0 nodata.
    n = 9
    src_nodata = -999999.0
    arr = np.full((n, n), 10.0, dtype=np.float64)
    c = n // 2
    arr[c - 1 : c + 2, c - 1 : c + 2] = 8.0  # a real 3x3 pit, depth 2
    arr[0, 0] = src_nodata                   # real numeric nodata void
    arr[0, 1] = np.nan                       # NaN void

    normalized = probe._normalize_nodata(arr, src_nodata)

    assert normalized.dtype == np.float32
    assert normalized[0, 0] == -9999.0
    assert normalized[0, 1] == -9999.0
    assert np.isclose(normalized[c, c], 8.0)          # pit cell unchanged
    assert np.isclose(normalized[n - 1, n - 1], 10.0)  # valid rim cell unchanged

    # Chain into depth_to_spill (no explicit nodata -> default sentinel
    # -9999.0) to prove the realistic read_window -> depth_to_spill path end
    # to end without a live S3 read: both void cells produce zero depth, and
    # the real pit is still filled correctly.
    depth = probe.depth_to_spill(normalized)
    assert np.isclose(depth[0, 0], 0.0)
    assert np.isclose(depth[0, 1], 0.0)
    assert np.isclose(depth[c, c], 2.0)
