"""Unit tests for the pure merge contract in scripts/merge_vpu_segments.py.

``streambuffer`` buffers whatever geometry it gets, so the only thing the merge
must guarantee is: every valid per-VPU segment survives, null/empty geometries
are dropped, and mismatched CRSs are reprojected to one common CRS before the
concat (a silent CRS mix would put a VPU's segments in the wrong place). Those
invariants are pinned here at CI speed with synthetic line layers — no staged
geopackages or GDAL file I/O.

scripts/ is not an importable package, so the module is loaded by path.
"""

import importlib.util
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "merge_vpu_segments.py"
_spec = importlib.util.spec_from_file_location("merge_vpu_segments", _SCRIPT)
merge = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(merge)

CRS = "EPSG:5070"


def _lines(n, crs=CRS, start=0):
    geoms = [LineString([(i, i), (i + 1, i + 1)]) for i in range(start, start + n)]
    return gpd.GeoDataFrame({"seg_id": list(range(start, start + n))}, geometry=geoms, crs=crs)


def test_concat_sums_feature_counts():
    merged = merge.concat_segments([_lines(3), _lines(2, start=10)])
    assert len(merged) == 5
    assert merged.crs == _lines(1).crs


def test_drops_null_and_empty_geometries():
    g = gpd.GeoDataFrame(
        {"seg_id": [1, 2, 99, 100]},
        geometry=[
            LineString([(0, 0), (1, 1)]),
            LineString([(1, 1), (2, 2)]),
            None,            # null
            LineString(),    # empty
        ],
        crs=CRS,
    )
    merged = merge.concat_segments([g])
    assert len(merged) == 2
    assert merged.geometry.notna().all()
    assert (~merged.geometry.is_empty).all()


def test_reprojects_mismatched_crs_to_target():
    native = _lines(2)  # EPSG:5070, metric coords near the origin
    # a realistic CONUS point in lon/lat that has a well-defined 5070 image
    other = gpd.GeoDataFrame(
        {"seg_id": [5]},
        geometry=[LineString([(-100.0, 40.0), (-99.9, 40.1)])],
        crs="EPSG:4326",
    )
    merged = merge.concat_segments([native, other], target_crs=CRS)
    assert merged.crs == native.crs
    assert len(merged) == 3
    # the reprojected row must land in 5070's metric range (hundreds of
    # thousands of metres), not be left as a ~ -100 degree coordinate
    assert abs(merged.geometry.iloc[2].coords[0][0]) > 1000


def test_default_target_crs_is_first_available():
    merged = merge.concat_segments([_lines(1), _lines(1, start=2)])
    assert merged.crs == _lines(1).crs


def test_empty_input_raises():
    with pytest.raises(ValueError, match="No segment layers"):
        merge.concat_segments([])
