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


# ---------------------------------------------------------------------------
# Endorheic exemption (the Great Salt Lake clump-veto fix)
#
# GSL scenario, synthetic: one wbody_regions clump ("region 1") holds BOTH the
# "lake" cells (rows 0-2, cols 0-2 -- endorheic, NOT themselves on-stream) and
# the "marsh" cells (rows 3-4, cols 3-4 -- genuinely on-stream), mirroring how
# clump_regions' 8-connectivity actually merged the Great Salt Lake with its
# 49.1 km2 inflow SwampMarsh (COMID 10273192) into one region. Without the
# exemption, the marsh's on-stream status vetoes the WHOLE region -- including
# the lake, which is the bug this fix addresses.
# ---------------------------------------------------------------------------


def _gsl_clump_ctx(tmp_path, *, with_endorheic: bool, imperv_lake_cell: bool = False):
    template = tmp_path / "template.tif"
    _write(template, np.full((_N, _N), 100.0), "float32", -9999.0)

    # One merged region: "lake" (rows 0-2, cols 0-2) + "marsh" (rows 3-4, cols 3-4).
    regions = np.zeros((_N, _N), dtype=np.int32)
    regions[0:3, 0:3] = 1  # lake
    regions[3:5, 3:5] = 1  # marsh -- same region id, already clump-merged upstream

    wbody_binary = np.where(regions > 0, np.uint8(1), np.uint8(255))
    _write(tmp_path / "wbody_regions.tif", regions, "int32", 0)
    _write(tmp_path / "wbody_binary.tif", wbody_binary, "uint8", 255)

    # Only the marsh cells are genuinely on-stream.
    connected = np.full((_N, _N), 255, dtype=np.uint8)
    connected[3:5, 3:5] = 1
    _write(tmp_path / "connected_wbody.tif", connected, "uint8", 255)

    # Only the lake cells carry positive endorheic evidence.
    endorheic = np.full((_N, _N), 255, dtype=np.uint8)
    endorheic[0:3, 0:3] = 1
    _write(tmp_path / "endorheic_wbody.tif", endorheic, "uint8", 255)

    imperv = np.full((_N, _N), 255, dtype=np.uint8)
    if imperv_lake_cell:
        imperv[0, 0] = 1  # one impervious pixel inside the lake
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
    if with_endorheic:
        ctx.paths["endorheic_wbody"] = tmp_path / "endorheic_wbody.tif"
    return ctx


def test_endorheic_exemption_recovers_gsl_lake_without_freeing_the_marsh(tmp_path):
    """With the exemption: the lake becomes dprst, the marsh stays on-stream."""
    ctx = _gsl_clump_ctx(tmp_path, with_endorheic=True)

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )
    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)
    with rasterio.open(produced["onstream"]) as src:
        onstream_arr = src.read(1)

    # Lake cells: recovered into dprst.
    assert dprst_arr[0, 0] == 1
    assert dprst_arr[2, 2] == 1
    assert onstream_arr[0, 0] != 1
    # Marsh cells: still genuinely on-stream, NOT swept into dprst.
    assert dprst_arr[3, 3] != 1
    assert onstream_arr[3, 3] == 1


def test_endorheic_absent_reproduces_todays_bug(tmp_path):
    """Without `endorheic_wbody` in the build context, behaviour is unchanged:
    the marsh's on-stream status still vetoes the whole clump, including the
    lake (this pins the pre-fix bug so a regression here is caught)."""
    ctx = _gsl_clump_ctx(tmp_path, with_endorheic=False)

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )
    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)
    with rasterio.open(produced["onstream"]) as src:
        onstream_arr = src.read(1)

    # Bug reproduced: the lake is wrongly excluded from dprst and swept into
    # on-stream storage, exactly like the marsh it's clumped with.
    assert dprst_arr[0, 0] != 1
    assert onstream_arr[0, 0] == 1


def test_endorheic_exemption_never_overrides_an_onstream_cell(tmp_path):
    """A cell that is BOTH endorheic-flagged AND itself on-stream must stay
    carved out -- the exemption only ever applies where `connected_binary != 1`.
    """
    ctx = _gsl_clump_ctx(tmp_path, with_endorheic=True)
    # Overwrite: mark one of the marsh cells as ALSO endorheic-flagged (e.g. a
    # provenance edge case) while it remains on-stream. It must still be
    # excluded -- self on-stream status always wins over endorheic evidence.
    endorheic = np.full((_N, _N), 255, dtype=np.uint8)
    endorheic[0:3, 0:3] = 1
    endorheic[3, 3] = 1  # marsh cell, also flagged endorheic
    _write(tmp_path / "endorheic_wbody.tif", endorheic, "uint8", 255)

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )
    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)
    with rasterio.open(produced["onstream"]) as src:
        onstream_arr = src.read(1)

    assert dprst_arr[3, 3] != 1, "on-stream cell must stay carved out despite endorheic flag"
    assert onstream_arr[3, 3] == 1


def test_endorheic_exemption_keeps_imperv_dprst_perv_disjoint(tmp_path):
    """An impervious cell inside the recovered lake must still be carved out of
    dprst -- the imperv/dprst/perv partition stays disjoint after the exemption
    runs, same invariant as the pre-existing imperv carve test above."""
    ctx = _gsl_clump_ctx(tmp_path, with_endorheic=True, imperv_lake_cell=True)

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )
    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)
    with rasterio.open(tmp_path / "imperv.tif") as src:
        imperv_arr = src.read(1)

    # The impervious lake cell is carved out of dprst despite being exempted...
    assert dprst_arr[0, 0] != 1
    # ... while the rest of the recovered lake stays dprst.
    assert dprst_arr[2, 2] == 1
    # Invariant: dprst and imperv never coincide (no double-count).
    assert int(((dprst_arr == 1) & (imperv_arr == 1)).sum()) == 0


def test_endorheic_exemption_never_recovers_a_non_wbody_cell(tmp_path):
    """`endorheic_wbody` is rasterized from a raw, unfiltered read of the
    waterbody gpkg (no EXCLUDE_WATERBODY_FTYPES / Ice Mass filter, no
    min_area_threshold), unlike `wbody_binary` which applies both. The real
    case: two Mt Shasta glacier COMIDs (Ice Mass, excluded from the waterbody
    classification entirely -- its cells fall back to land) are flagged
    endorheic by Signal B because the summit HUC12s are WBD type-C. Without
    gating the exemption on `wbody_binary == 1`, those glacier cells would be
    silently reinstated as dprst, violating `dprst ⊆ wbody_binary` and turning
    a glacier into a depression-storage pour-point.
    """
    ctx = _gsl_clump_ctx(tmp_path, with_endorheic=True)

    # Knock the lake's corner cell out of wbody_binary -- standing in for an
    # Ice Mass polygon that `waterbody.build()` excluded but the raw
    # endorheic-wbody rasterization (no FTYPE filter) still flags.
    with rasterio.open(tmp_path / "wbody_binary.tif") as src:
        wbody_binary = src.read(1)
    wbody_binary[0, 0] = 255
    with rasterio.open(
        tmp_path / "wbody_binary.tif", "w", driver="GTiff", height=_N, width=_N,
        count=1, dtype="uint8", crs="EPSG:5070", transform=_TRANSFORM, nodata=255,
    ) as dst:
        dst.write(wbody_binary, 1)

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )
    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)

    # The non-wbody "glacier" cell is endorheic-flagged and not on-stream, but
    # must NOT be recovered into dprst -- it isn't a waterbody cell at all.
    assert dprst_arr[0, 0] != 1
    # The rest of the recovered lake (still a genuine wbody_binary cell) is
    # unaffected by the gate.
    assert dprst_arr[2, 2] == 1
