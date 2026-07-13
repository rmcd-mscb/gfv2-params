"""Tests for the `waterbody` depstor builder (clump rasterisation + FTYPE exclusion)."""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon, box

from gfv2_params.depstor_builders.waterbody import merge_burn_add


def _write_template(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="float32",
        crs="EPSG:5070", transform=transform, nodata=-9999.0,
    ) as dst:
        dst.write(np.full((n, n), 100.0, dtype=np.float32), 1)


def _write_landmask(path: Path, n: int = 10) -> None:
    transform = from_origin(0, n * 30, 30, 30)
    with rasterio.open(
        path, "w", driver="GTiff", height=n, width=n, count=1, dtype="uint8",
        crs="EPSG:5070", transform=transform, nodata=255,
    ) as dst:
        dst.write(np.ones((n, n), dtype=np.uint8), 1)  # all land


def test_waterbody_excludes_ice_mass_keeps_lakepond(tmp_path):
    """Ice Mass polygons must be absent from wbody_binary; LakePond must remain."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    # 2 waterbodies, both well above the default min_area (900 m^2):
    #   WB 10 — LakePond, top-left 2x2 block (3600 m^2) -> must be kept
    #   WB 20 — Ice Mass, bottom-right 2x2 block (3600 m^2) -> must be excluded
    gdf = gpd.GeoDataFrame(
        {"COMID": [10, 20], "FTYPE": ["LakePond", "Ice Mass"]},
        geometry=[
            box(0, 270, 60, 300),   # top-left 2x2: cells [0,0],[0,1],[1,0],[1,1]
            box(240, 0, 300, 30),   # bottom-right 2x2: cells [8,8],[8,9],[9,8],[9,9]
        ],
        crs="EPSG:5070",
    )
    gdf.to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
    )
    ctx.paths["landmask"] = landmask

    produced = waterbody.build(
        {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["wbody_binary"]) as src:
        arr = src.read(1)

    assert arr[0, 0] == 1     # WB 10 (LakePond) present
    assert arr[9, 9] != 1     # WB 20 (Ice Mass) absent


def test_waterbody_missing_ftype_column_raises(tmp_path):
    """A waterbody layer without FTYPE must raise (refuse to silently miss Ice Mass)."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10]},
        geometry=[box(0, 270, 60, 300)],
        crs="EPSG:5070",
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(KeyError):
        waterbody.build(
            {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
            ctx, logging.getLogger("test"),
        )


CRS = "EPSG:5070"
WB_COLS = ["GNIS_ID", "GNIS_NAME", "COMID", "FTYPE", "member_comid", "area_sqkm"]


def _sq(x0, y0, s=100):
    return Polygon([(x0, y0), (x0 + s, y0), (x0 + s, y0 + s), (x0, y0 + s)])


def _frame(rows):
    return gpd.GeoDataFrame(rows, columns=WB_COLS + ["geometry"], crs=CRS)


def test_merge_burn_add_appends_the_polygons():
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(500, 500)]])
    out = merge_burn_add(base, burn)
    assert len(out) == 2
    assert set(out.COMID) == {111, -367111}


def test_merge_burn_add_rejects_a_non_negative_comid():
    # A positive BurnAdd COMID could match a WBAREACOMI / flow-through COMID and be
    # promoted on-stream -- but NHDPlus flagged every BurnAddWaterbody as a SINK.
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, 222, "Playa", 222, 0.01, _sq(500, 500)]])
    with pytest.raises(ValueError, match="negative"):
        merge_burn_add(base, burn)


def test_merge_burn_add_is_a_noop_when_not_configured():
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    assert merge_burn_add(base, None) is base


def _write_burn_add_parquet(path: Path, rows: list[list]) -> None:
    gpd.GeoDataFrame(rows, columns=WB_COLS + ["geometry"], crs=CRS).to_parquet(path)


def test_build_merges_burn_add_before_ftype_exclusion(tmp_path):
    """A BurnAdd Ice Mass polygon must still be excluded from wbody_binary.

    `merge_burn_add` must run BEFORE the `EXCLUDE_WATERBODY_FTYPES` filter inside
    `waterbody.build()`. A refactor that moved the merge to AFTER that filter would
    pass every other test in this file (they all call `merge_burn_add` directly) but
    would let a BurnAdd Ice Mass polygon leak into the raster unfiltered -- this test
    drives the real `build()` entry point to catch exactly that regression.
    """
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    burn_parquet = tmp_path / "burn_add.parquet"
    _write_template(template)
    _write_landmask(landmask)

    # Base layer: one ordinary LakePond, top-left 2x2 block (cells [0,0]-[1,1]).
    gpd.GeoDataFrame(
        {"COMID": [10], "FTYPE": ["LakePond"]},
        geometry=[box(0, 270, 60, 300)],
        crs=CRS,
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    # BurnAdd Ice Mass polygon, exactly cell [5,5] (900 m^2, at the min-area floor),
    # far from the base waterbody so it can't be clump-merged with it.
    _write_burn_add_parquet(
        burn_parquet,
        [[None, None, -900555, "Ice Mass", -900555, 0.0009, _sq(150, 120)]],
    )

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        burn_add_waterbody_table=burn_parquet,
    )
    ctx.paths["landmask"] = landmask

    produced = waterbody.build(
        {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["wbody_binary"]) as src:
        arr = src.read(1)

    assert arr[0, 0] == 1     # base LakePond present
    assert arr[5, 5] != 1     # BurnAdd Ice Mass excluded, even though it was merged in


def test_build_raises_on_configured_but_missing_burn_add_table(tmp_path):
    """A configured `burn_add_waterbody_table` that doesn't exist must fail loud."""
    from gfv2_params.depstor_builders import waterbody
    from gfv2_params.depstor_builders.context import BuildContext

    template = tmp_path / "template.tif"
    landmask = tmp_path / "land_mask.tif"
    wb_gpkg = tmp_path / "wb.gpkg"
    _write_template(template)
    _write_landmask(landmask)

    gpd.GeoDataFrame(
        {"COMID": [10], "FTYPE": ["LakePond"]},
        geometry=[box(0, 270, 60, 300)],
        crs=CRS,
    ).to_file(wb_gpkg, layer="waterbodies", driver="GPKG")

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=wb_gpkg, hru_layer="waterbodies",
        waterbody_gpkg=wb_gpkg, waterbody_layer="waterbodies",
        burn_add_waterbody_table=tmp_path / "does_not_exist.parquet",
    )
    ctx.paths["landmask"] = landmask

    with pytest.raises(FileNotFoundError, match="BurnAddWaterbody table not found"):
        waterbody.build(
            {"outputs": {"binary": "wbody_binary.tif", "regions": "wbody_regions.tif"}},
            ctx, logging.getLogger("test"),
        )


def test_merge_burn_add_rejects_an_overlapping_polygon():
    """A BurnAdd polygon overlapping an existing waterbody must FAIL LOUD.

    `clump_regions` labels 8-connected components, so an overlap merges the BurnAdd
    playa and the existing waterbody into ONE region. If that waterbody is on-stream,
    `regions_touching_mask` excludes the whole clump — silently DELETING the playa's
    depression area, the exact opposite of why we staged it, with nothing in the log
    to say so. The VPU 16 spike measured 0/23 overlaps; CONUS-wide is unverified, so
    this is checked at build time and not left to a diagnostic.
    """
    base = _frame([[None, None, 111, "LakePond", 111, 0.01, _sq(0, 0)]])
    burn = _frame([[None, None, -367111, "Playa", -367111, 0.01, _sq(50, 50)]])  # overlaps
    with pytest.raises(ValueError, match="overlap"):
        merge_burn_add(base, burn)
