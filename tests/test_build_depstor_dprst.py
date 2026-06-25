"""Behavioural test for the dprst builder's connected-vs-depression split.

The feature's whole point: a waterbody region that the NHD-connected mask touches
is on-stream (excluded from depression storage and placed in onstream), while a
region touching neither the connected mask nor imperv is kept as depression
storage. This pins that contract at the dprst level on a tiny synthetic grid.
"""

import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from gfv2_params.depstor_builders import dprst
from gfv2_params.depstor_builders.context import BuildContext

_N = 10
_TRANSFORM = from_origin(0, _N * 30, 30, 30)


def _write(path: Path, arr: np.ndarray, dtype: str, nodata) -> None:
    with rasterio.open(
        path, "w", driver="GTiff", height=_N, width=_N, count=1, dtype=dtype,
        crs="EPSG:5070", transform=_TRANSFORM, nodata=nodata,
    ) as dst:
        dst.write(arr.astype(dtype), 1)


def test_dprst_excludes_connected_region_keeps_isolated(tmp_path):
    template = tmp_path / "template.tif"
    _write(template, np.full((_N, _N), 100.0), "float32", -9999.0)

    # Two waterbody regions: region 1 top-left (rows 0-1, cols 0-1),
    # region 2 bottom-right (rows 8-9, cols 8-9).
    regions = np.zeros((_N, _N), dtype=np.int32)
    regions[0:2, 0:2] = 1
    regions[8:10, 8:10] = 2
    _write(tmp_path / "wbody_regions.tif", regions, "int32", 0)

    wbody_binary = np.where(regions > 0, np.uint8(1), np.uint8(255))
    _write(tmp_path / "wbody_binary.tif", wbody_binary, "uint8", 255)

    # Connected mask overlaps region 1 only -> region 1 is on-stream.
    connected = np.full((_N, _N), 255, dtype=np.uint8)
    connected[0:2, 0:2] = 1
    _write(tmp_path / "connected_wbody.tif", connected, "uint8", 255)

    # Nothing impervious, all land.
    _write(tmp_path / "imperv.tif", np.full((_N, _N), 255, dtype=np.uint8), "uint8", 255)
    _write(tmp_path / "land_mask.tif", np.ones((_N, _N), dtype=np.uint8), "uint8", 255)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="nhru",
    )
    ctx.paths.update({
        "landmask": tmp_path / "land_mask.tif",
        "wbody_binary": tmp_path / "wbody_binary.tif",
        "wbody_regions": tmp_path / "wbody_regions.tif",
        "connected_wbody": tmp_path / "connected_wbody.tif",
        "imperv": tmp_path / "imperv.tif",
    })

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)
    with rasterio.open(produced["onstream"]) as src:
        onstream_arr = src.read(1)

    # Region 2 (isolated) is kept as depression storage; region 1 (connected) is not.
    assert dprst_arr[9, 9] == 1
    assert dprst_arr[0, 0] != 1
    # Region 1 lands in on-stream storage; region 2 does not.
    assert onstream_arr[0, 0] == 1
    assert onstream_arr[9, 9] != 1


def test_dprst_carves_imperv_cells_but_keeps_region(tmp_path):
    template = tmp_path / "template.tif"
    _write(template, np.full((_N, _N), 100.0), "float32", -9999.0)

    # One isolated waterbody region: rows 0-1, cols 0-3 (8 cells), not connected.
    regions = np.zeros((_N, _N), dtype=np.int32)
    regions[0:2, 0:4] = 1
    _write(tmp_path / "wbody_regions.tif", regions, "int32", 0)

    wbody_binary = np.where(regions > 0, np.uint8(1), np.uint8(255))
    _write(tmp_path / "wbody_binary.tif", wbody_binary, "uint8", 255)

    # Connected mask touches nothing -> nothing excluded for connectivity.
    _write(tmp_path / "connected_wbody.tif",
           np.full((_N, _N), 255, dtype=np.uint8), "uint8", 255)

    # Two impervious cells fall inside the region (e.g. a road across a playa).
    imperv = np.full((_N, _N), 255, dtype=np.uint8)
    imperv[0, 0] = 1
    imperv[0, 1] = 1
    _write(tmp_path / "imperv.tif", imperv, "uint8", 255)
    _write(tmp_path / "land_mask.tif", np.ones((_N, _N), dtype=np.uint8), "uint8", 255)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="nhru",
    )
    ctx.paths.update({
        "landmask": tmp_path / "land_mask.tif",
        "wbody_binary": tmp_path / "wbody_binary.tif",
        "wbody_regions": tmp_path / "wbody_regions.tif",
        "connected_wbody": tmp_path / "connected_wbody.tif",
        "imperv": tmp_path / "imperv.tif",
    })

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)
    with rasterio.open(produced["onstream"]) as src:
        onstream_arr = src.read(1)

    # The region is kept as depression storage at its non-impervious cells ...
    assert dprst_arr[1, 0] == 1
    assert dprst_arr[1, 3] == 1
    # ... the two impervious cells are carved out of dprst ...
    assert dprst_arr[0, 0] != 1
    assert dprst_arr[0, 1] != 1
    # ... and are NOT swept into on-stream storage.
    assert onstream_arr[0, 0] != 1
    assert onstream_arr[0, 1] != 1
    # Invariant: dprst and imperv never coincide (no double-count).
    assert int(((dprst_arr == 1) & (imperv == 1)).sum()) == 0
