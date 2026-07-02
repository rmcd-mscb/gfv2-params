import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from gfv2_params.depstor import same_hru_intersect
from gfv2_params.depstor_builders import same_hru_drains
from gfv2_params.depstor_builders.context import BuildContext


def test_same_hru_intersect_keeps_only_matching_hru():
    labeled = np.array([[42, 42, 9, 0]], dtype=np.int32)   # reached-HRU per cell
    hru_id = np.array([[42, 8, 9, 5]], dtype=np.int32)     # cell's own HRU
    land = np.array([[1, 1, 1, 1]], dtype=np.uint8)         # perv everywhere
    out = same_hru_intersect(labeled, hru_id, land)
    # col0: 42==42 & perv -> 1 ; col1: 42!=8 -> 255 ; col2: 9==9 -> 1 ; col3: 0!=5 -> 255
    assert out.tolist() == [[1, 255, 1, 255]]


_N = (1, 5)
_TRANSFORM = from_origin(0, 30, 30, 30)


def _write(path: Path, arr: np.ndarray, dtype: str, nodata) -> None:
    height, width = arr.shape
    with rasterio.open(
        path, "w", driver="GTiff", height=height, width=width, count=1, dtype=dtype,
        crs="EPSG:5070", transform=_TRANSFORM, nodata=nodata,
    ) as dst:
        dst.write(arr.astype(dtype), 1)


def test_build_excludes_cross_hru_cells_and_keeps_same_hru(tmp_path):
    """`build()`-level test — the PR deliverable, not just the pure helper.

    Same 1x5 layout style as test_routing_hru.py's build() test: col0/col1
    drain to HRU 42's depression but sit in HRU 7 (cross-HRU -> excluded);
    col2 is the depression cell itself, labeled 42 and own HRU 42 (same-HRU ->
    kept); col3 has labeled==0 (doesn't drain to any depression -> excluded);
    col4 drains to its own HRU 9's depression (same-HRU -> kept).
    """
    template = tmp_path / "template.tif"
    _write(template, np.full(_N, 100.0), "float32", -9999.0)

    labeled = np.array([[42, 42, 42, 0, 9]], dtype=np.int32)
    _write(tmp_path / "drains_to_dprst_hru.tif", labeled, "int32", 0)

    hru_id = np.array([[7, 7, 42, 8, 9]], dtype=np.int32)
    _write(tmp_path / "hru_id.tif", hru_id, "int32", 0)

    perv = np.array([[1, 1, 1, 1, 1]], dtype=np.uint8)
    _write(tmp_path / "perv_binary.tif", perv, "uint8", 255)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="nhru",
    )
    ctx.paths.update({
        "drains_to_dprst_hru": tmp_path / "drains_to_dprst_hru.tif",
        "hru_id": tmp_path / "hru_id.tif",
        "perv": tmp_path / "perv_binary.tif",
    })

    produced = same_hru_drains.build(
        {
            "name": "drains_perv",
            "inputs": ["drains_to_dprst_hru", "hru_id", "perv"],
            "output": "drains_perv_binary.tif",
            "output_key": "drains_perv",
        },
        ctx,
        logging.getLogger("test"),
    )

    with rasterio.open(produced["drains_perv"]) as src:
        out = src.read(1)

    assert out.tolist() == [[255, 255, 1, 255, 1]]
