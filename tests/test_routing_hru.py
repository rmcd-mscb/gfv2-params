import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from gfv2_params.d8_routing import drains_to_dprst_labeled_kernel
from gfv2_params.depstor import mask_fdr_to_vpu, vpu_pour_points
from gfv2_params.depstor_builders import routing_hru
from gfv2_params.depstor_builders.context import BuildContext


def test_labeled_trace_attributes_to_reached_hru_with_barrier():
    # 1x5 single VPU (code 1). land -> land -> [dprst in HRU 42] , and a second
    # chain where an on-stream barrier blocks the reach.
    #   col: 0    1        2(dprst,HRU42)   3(onstream)    4(dprst,HRU9)
    # flow all East; col4 pours to HRU9 but col3 is a barrier.
    vpu = np.ones((1, 5), dtype=np.uint8)
    fdr = np.array([[1, 1, 255, 1, 255]], dtype=np.uint8)
    dprst = np.array([[0, 0, 1, 0, 1]], dtype=np.uint8)
    onstream = np.array([[0, 0, 0, 1, 0]], dtype=np.uint8)
    hru = np.array([[7, 7, 42, 8, 9]], dtype=np.int32)

    fdr_m = mask_fdr_to_vpu(fdr, vpu, code=1)
    label = np.where((dprst == 1) & (vpu == 1), hru, 0).astype(np.int32)
    barrier = vpu_pour_points(onstream, vpu, code=1)
    out, _ = drains_to_dprst_labeled_kernel(fdr_m, label, barrier)
    # col0,col1 reach dprst HRU42; col2 is the dprst (label 42); col3 barrier=0;
    # col4 is its own dprst (label 9).
    assert out.tolist() == [[42, 42, 42, 0, 9]]


_N = (1, 5)
_TRANSFORM = from_origin(0, 30, 30, 30)


def _write(path: Path, arr: np.ndarray, dtype: str, nodata) -> None:
    height, width = arr.shape
    with rasterio.open(
        path, "w", driver="GTiff", height=height, width=width, count=1, dtype=dtype,
        crs="EPSG:5070", transform=_TRANSFORM, nodata=nodata,
    ) as dst:
        dst.write(arr.astype(dtype), 1)


def test_build_writes_labeled_hru_raster_with_barrier_and_land_mask(tmp_path):
    """End-to-end `build()` smoke test — exercises the w+ read-modify-write and
    the land-mask carve, not just the kernel helpers.

    Same 1x5 single-VPU layout as the helper-level test above, plus a
    land_mask that marks col1 off-land even though the D8 trace reaches HRU 42
    there — the write must drop it despite the kernel computing a label.
    #   col: 0(land)  1(off-land)  2(dprst,HRU42)  3(onstream)  4(dprst,HRU9)
    """
    template = tmp_path / "template.tif"
    _write(template, np.full((1, 5), 100.0), "float32", -9999.0)

    fdr = np.array([[1, 1, 255, 1, 255]], dtype=np.uint8)
    _write(tmp_path / "fdr.tif", fdr, "uint8", 255)

    vpu = np.ones((1, 5), dtype=np.uint8)
    _write(tmp_path / "vpu_id.tif", vpu, "uint8", 0)

    dprst = np.array([[0, 0, 1, 0, 1]], dtype=np.uint8)
    _write(tmp_path / "dprst_binary.tif", dprst, "uint8", 255)

    onstream = np.array([[0, 0, 0, 1, 0]], dtype=np.uint8)
    _write(tmp_path / "onstream_binary.tif", onstream, "uint8", 255)

    hru = np.array([[7, 7, 42, 8, 9]], dtype=np.int32)
    _write(tmp_path / "hru_id.tif", hru, "int32", 0)

    # col1 is off-land despite draining to HRU 42.
    land = np.array([[1, 0, 1, 1, 1]], dtype=np.uint8)
    _write(tmp_path / "land_mask.tif", land, "uint8", 255)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="nhru",
        fdr_raster=tmp_path / "fdr.tif",
    )
    ctx.paths.update({
        "landmask": tmp_path / "land_mask.tif",
        "dprst": tmp_path / "dprst_binary.tif",
        "onstream": tmp_path / "onstream_binary.tif",
        "vpu_id": tmp_path / "vpu_id.tif",
        "hru_id": tmp_path / "hru_id.tif",
    })

    produced = routing_hru.build(
        {"output": "drains_to_dprst_hru.tif"}, ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["drains_to_dprst_hru"]) as src:
        out = src.read(1)

    # col0 reaches HRU42 and is on land -> 42; col1 reaches HRU42 but is
    # off-land -> forced to 0 (nodata); col2 is the dprst cell itself -> 42;
    # col3 is the on-stream barrier -> 0; col4 is its own dprst (HRU9) -> 9.
    assert out.tolist() == [[42, 0, 42, 0, 9]]


def test_build_multi_vpu_overlapping_bboxes_do_not_clobber(tmp_path):
    """2-VPU windowed read-modify-write, with OVERLAPPING VPU bounding boxes.

    2x2 grid, vpu_id=[[1,2],[2,1]]: each code's cells sit on a diagonal, so
    `vpu_bbox` for BOTH code 1 and code 2 spans the whole grid (their windows
    fully overlap) — the layout the read-modify-write in routing_hru.build()
    must handle without one VPU's write erasing the other's.

    Layout (row, col):
        (0,0)=VPU1 land  --SE--> (1,1)=VPU1 dprst, HRU 42
        (0,1)=VPU2 land  --SW--> (1,0)=VPU2 dprst, HRU 99

    Both VPU1 (processed first, ascending code order) and VPU2 write into the
    identical full-grid window; the final raster must carry BOTH VPUs'
    labelled cells.
    """
    template = tmp_path / "template.tif"
    _write(template, np.full((2, 2), 100.0), "float32", -9999.0)

    # SE (2) from (0,0)->(1,1); SW (8) from (0,1)->(1,0); dprst cells are sinks.
    fdr = np.array([[2, 8], [255, 255]], dtype=np.uint8)
    _write(tmp_path / "fdr.tif", fdr, "uint8", 255)

    vpu = np.array([[1, 2], [2, 1]], dtype=np.uint8)
    _write(tmp_path / "vpu_id.tif", vpu, "uint8", 0)

    dprst = np.array([[0, 0], [1, 1]], dtype=np.uint8)
    _write(tmp_path / "dprst_binary.tif", dprst, "uint8", 255)

    onstream = np.zeros((2, 2), dtype=np.uint8)
    _write(tmp_path / "onstream_binary.tif", onstream, "uint8", 255)

    hru = np.array([[7, 8], [99, 42]], dtype=np.int32)
    _write(tmp_path / "hru_id.tif", hru, "int32", 0)

    land = np.ones((2, 2), dtype=np.uint8)
    _write(tmp_path / "land_mask.tif", land, "uint8", 255)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="nhru",
        fdr_raster=tmp_path / "fdr.tif",
    )
    ctx.paths.update({
        "landmask": tmp_path / "land_mask.tif",
        "dprst": tmp_path / "dprst_binary.tif",
        "onstream": tmp_path / "onstream_binary.tif",
        "vpu_id": tmp_path / "vpu_id.tif",
        "hru_id": tmp_path / "hru_id.tif",
    })

    produced = routing_hru.build(
        {"output": "drains_to_dprst_hru.tif"}, ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["drains_to_dprst_hru"]) as src:
        out = src.read(1)

    # VPU1's (0,0)->(1,1) trace (HRU 42) must survive VPU2's later write, and
    # VPU2's (0,1)->(1,0) trace (HRU 99) must be present too — neither VPU's
    # read-modify-write may zero the other's cells despite the full overlap.
    assert out.tolist() == [[42, 99], [99, 42]]
