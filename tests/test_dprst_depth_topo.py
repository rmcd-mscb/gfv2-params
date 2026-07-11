import geopandas as gpd
import numpy as np
from shapely.geometry import Point

from gfv2_params.dprst_depth import topo


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
    out = topo.dprst_polygons(wb, connected)
    comids = set(out["COMID"])
    assert comids == {11, 12}          # 11 off-stream, 12 playa forced
    assert 10 not in comids            # genuine on-stream LakePond removed
    assert 13 not in comids            # Ice Mass excluded entirely


def test_clip_dprst_to_fabric_drops_out_of_fabric_polygons(tmp_path):
    import logging

    from shapely.geometry import box

    # 3 dprst polygons: two inside the fabric HRU footprint, one far outside.
    dprst = gpd.GeoDataFrame(
        {"COMID": [1, 2, 3]},
        geometry=[box(0, 0, 1, 1), box(2, 2, 3, 3), box(100, 100, 101, 101)],
        crs="EPSG:5070",
    )
    # A single HRU polygon covering only the first two.
    hru = gpd.GeoDataFrame(
        {"nat_hru_id": [1]}, geometry=[box(-1, -1, 4, 4)], crs="EPSG:5070"
    )
    hru_path = tmp_path / "hru.gpkg"
    hru.to_file(hru_path, layer="nhru", driver="GPKG")

    out = topo._clip_dprst_to_fabric(dprst, hru_path, "nhru", logging.getLogger("t"))
    assert sorted(out["COMID"]) == [1, 2]  # the out-of-fabric COMID 3 is dropped


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
    out = topo.resolution_class(dprst, wesm)
    assert list(out.sort_values("COMID")["best_topo"]) == ["1m", "10m"]


def test_depth_to_spill_and_mean_depth_on_synthetic_bowl():
    # 5x5 flat plateau at z=10 with a single 3x3 pit of depth 2 (z=8).
    dem = np.full((5, 5), 10.0, dtype=np.float64)
    dem[1:4, 1:4] = 8.0
    depth = topo.depth_to_spill(dem)
    # Filled restores the pit to the rim (10); depth = 2 in the pit, 0 on the rim.
    assert np.isclose(depth[2, 2], 2.0)
    assert np.isclose(depth[0, 0], 0.0)

    mask = depth > 0            # the 3x3 pit
    v, a, mean_d = topo.volume_mean_depth(depth, mask, cell_area_m2=1.0)
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
    depth = topo.depth_to_spill(dem)

    assert np.isclose(depth[0, 0], 0.0)       # void cell: no spurious fill-to-rim depth
    assert np.isclose(depth[c, c], 2.0)       # real pit still computed correctly
    assert np.isclose(depth[n - 1, n - 1], 0.0)  # untouched rim cell stays 0


def test_is_hydroflattened_detects_constant_surface():
    flat = np.full((20, 20), 512.30, dtype=np.float32)
    natural = flat + np.linspace(0, 1.5, 400).reshape(20, 20).astype(np.float32)
    assert topo.is_hydroflattened(flat)["flat"] is True
    r = topo.is_hydroflattened(natural)
    assert r["flat"] is False
    assert r["range"] > 1.0


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

    normalized = topo._normalize_nodata(arr, src_nodata)

    assert normalized.dtype == np.float32
    assert normalized[0, 0] == -9999.0
    assert normalized[0, 1] == -9999.0
    assert np.isclose(normalized[c, c], 8.0)          # pit cell unchanged
    assert np.isclose(normalized[n - 1, n - 1], 10.0)  # valid rim cell unchanged

    # Chain into depth_to_spill (no explicit nodata -> default sentinel
    # -9999.0) to prove the realistic read_window -> depth_to_spill path end
    # to end without a live S3 read: both void cells produce zero depth, and
    # the real pit is still filled correctly.
    depth = topo.depth_to_spill(normalized)
    assert np.isclose(depth[0, 0], 0.0)
    assert np.isclose(depth[0, 1], 0.0)
    assert np.isclose(depth[c, c], 2.0)


def test_lake_max_depth_scales_with_surrounding_slope():
    import numpy as np
    from affine import Affine

    # 41x41 grid, 1 m cells; circular lake radius ~10 in the centre.
    n = 41
    yy, xx = np.mgrid[0:n, 0:n]
    r = np.hypot(xx - 20, yy - 20)
    mask = r <= 10
    # Terrain slopes 0.2 m/m toward the lake; lake cells flat (water surface).
    # Cap the slope well past the mask radius (20, not 10) -- capping exactly
    # at the mask radius makes dem[mask] = dem[mask].min() collapse the WHOLE
    # array to one constant elevation (verified: np.unique(dem) == [98.]),
    # since the region just outside the mask is then already flat too, and
    # lake_max_depth would spuriously measure zero slope everywhere.
    dem = 100 - 0.2 * np.minimum(r, 20)
    dem[mask] = dem[mask].min()
    d = topo.lake_max_depth(dem.astype(np.float64), mask, Affine.identity())
    # ~ slope(0.2) * radius(10) order of magnitude; positive, not absurd.
    assert 0.5 < d < 5.0
    assert np.isclose(topo.max_to_mean(3.0, shape="cone"), 1.0)


def test_lake_max_depth_ignores_nodata_ring():
    import numpy as np
    from affine import Affine

    # Same setup as test_lake_max_depth_scales_with_surrounding_slope, but
    # plant a -9999 nodata void (a tile-edge gap read_window would produce)
    # in the shoreline ring just outside the lake. Without excluding it,
    # np.gradient jumps from a real elevation (~96-98) to -9999 across one
    # pixel, producing a wildly inflated mean_slope and an absurd max_depth
    # (issue #173 T6 finding) instead of the same bounded result as the
    # void-free case.
    n = 41
    yy, xx = np.mgrid[0:n, 0:n]
    r = np.hypot(xx - 20, yy - 20)
    mask = r <= 10
    dem = 100 - 0.2 * np.minimum(r, 20)
    dem[mask] = dem[mask].min()
    dem = dem.astype(np.float64)
    # A ring cell (radius ~11, just outside mask) set to the nodata sentinel.
    dem[20, 31] = -9999.0
    d = topo.lake_max_depth(dem, mask, Affine.identity())
    assert np.isfinite(d)
    assert 0.5 < d < 5.0
