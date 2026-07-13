"""Unit tests for the endorheic dprst classifier (synthetic geometry + FDR arrays)."""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon

import pandas as pd

from gfv2_params.endorheic import (
    closed_basin_comids,
    frac_own_for_window,
    load_endorheic_comids,
    terminal_cells,
    terminus_own_fraction,
    write_endorheic_comids,
)

CRS = "EPSG:5070"
# ESRI D8: 1=E 2=SE 4=S 8=SW 16=W 32=NW 64=N 128=NE. 0 and 255 are termini.
NOD = 255


def _box(x0, y0, x1, y1):
    return Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])


def _wb(rows):
    return gpd.GeoDataFrame(rows, columns=["COMID", "geometry"], crs=CRS)


def _closed(polys):
    return gpd.GeoDataFrame({"HUC_12": [str(i) for i in range(len(polys))]},
                            geometry=polys, crs=CRS)


def _write_fdr(path, arr, transform, nodata=NOD):
    with rasterio.open(
        path, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1],
        count=1, dtype=arr.dtype, crs=CRS, transform=transform, nodata=nodata,
    ) as dst:
        dst.write(arr, 1)


def test_closed_basin_keeps_a_waterbody_fully_inside():
    wb = _wb([[101, _box(1, 1, 2, 2)]])
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == {101}


def test_closed_basin_rejects_a_boundary_graze():
    # THE regression that matters. A polygon touching the closed-HUC12 boundary with
    # ZERO interior overlap returns True from `intersects` -- this artifact produced a
    # false "Cedar Lake routes out of its closed basin" reading during design, and in
    # the real data Eagle Lake / Middle Alkali Lake graze at frac_in = 0.000.
    # Majority-area must reject them; `intersects` must never be substituted back in.
    wb = _wb([[102, _box(10, 0, 12, 2)]])          # shares only the x=10 edge
    closed = _closed([_box(0, 0, 10, 10)])
    assert wb.geometry.iloc[0].intersects(closed.geometry.iloc[0])  # the trap
    assert closed_basin_comids(wb, closed) == set()


def test_closed_basin_keeps_a_majority_overlap():
    # Great Salt Lake sits at frac_in = 0.989 -- it spills ~1% into a neighbouring
    # HUC12, so a strict `within` predicate would drop it. Majority-area keeps it.
    wb = _wb([[103, _box(8, 0, 11, 2)]])           # 2/3 inside the closed box
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == {103}


def test_closed_basin_rejects_a_minority_overlap():
    wb = _wb([[104, _box(9, 0, 12, 2)]])           # 1/3 inside
    assert closed_basin_comids(wb, _closed([_box(0, 0, 10, 10)])) == set()


def test_closed_basin_dissolves_adjacent_huc12s():
    # A lake straddling two ADJACENT closed HUC12s is fully inside the closed system
    # but majority-inside neither polygon on its own. Dissolve first, then measure.
    wb = _wb([[105, _box(4, 1, 6, 2)]])            # half in each of two closed boxes
    closed = _closed([_box(0, 0, 5, 10), _box(5, 0, 10, 10)])
    assert closed_basin_comids(wb, closed) == {105}


def test_closed_basin_empty_closed_set_demotes_nothing():
    wb = _wb([[106, _box(1, 1, 2, 2)]])
    empty = gpd.GeoDataFrame({"HUC_12": []}, geometry=[], crs=CRS)
    assert closed_basin_comids(wb, empty) == set()


def test_closed_basin_aggregates_multi_row_comid_by_area():
    # A single COMID split across two rows -- multi-part waterbody geometry, as
    # in the real conus_waterbodies.gpkg layer (448,124 rows, strictly fewer
    # unique COMIDs). Row A is 100% inside the closed union but is only 10% of
    # the COMID's true total area; row B is the other 90% and sits entirely
    # outside. The true combined fraction is 0.1 (must NOT be endorheic), but
    # "any row individually clears min_frac" semantics wrongly grab COMID 107
    # off row A alone (frac_A = 1.0). A waterbody is a COMID, not a row.
    wb = _wb([
        [107, _box(1, 1, 2, 2)],        # area 1, fully inside closed box -> frac 1.0
        [107, _box(20, 0, 29, 1)],      # area 9, fully outside -> frac 0.0
    ])
    closed = _closed([_box(0, 0, 10, 10)])
    assert closed_basin_comids(wb, closed) == set()


def test_closed_basin_raises_on_all_empty_geometry():
    # If every incoming geometry is null/empty (e.g. an upstream CRS bug
    # collapsed a whole batch), silently returning set() would misclassify
    # every one of these waterbodies as not-endorheic with no signal that
    # anything went wrong. Fail loud instead -- mirrors the notna()/is_empty()
    # prefilter in depstor_builders/wbody_connectivity.py.
    wb = _wb([[108, Polygon()]])
    closed = _closed([_box(0, 0, 10, 10)])
    with pytest.raises(ValueError, match="null/empty"):
        closed_basin_comids(wb, closed)


def test_closed_basin_raises_on_zero_area_geometry():
    # A degenerate (collinear-point) polygon has area 0.0 without being
    # `is_empty` -- the notna()/is_empty() prefilter alone can't catch it.
    # `area.where(area > 0)` used to turn this into NaN and silently drop it
    # (NaN > min_frac is False, same as "not endorheic"). That must fail loud.
    degenerate = Polygon([(1, 1), (2, 1), (3, 1)])  # collinear -> area 0.0
    assert degenerate.area == 0.0
    assert not degenerate.is_empty
    wb = _wb([[109, degenerate]])
    closed = _closed([_box(0, 0, 10, 10)])
    with pytest.raises(ValueError, match="zero/negative total area"):
        closed_basin_comids(wb, closed)


def test_closed_basin_reprojects_mismatched_closed_gdf_crs():
    # closed_gdf's CRS can legitimately differ from wb_gdf's (e.g. WBD staged
    # natively in one CRS, the waterbody layer in another). Every other fixture
    # in this file hardcodes both frames to EPSG:5070, so without this test the
    # `to_crs` reprojection branch never actually runs under test. Reuses the
    # majority-overlap geometry (2/3 inside, frac_in = 0.667) so a reprojection
    # bug that shifted or rescaled the union would flip the answer.
    wb = _wb([[110, _box(8, 0, 11, 2)]])
    closed = _closed([_box(0, 0, 10, 10)]).to_crs("EPSG:4326")
    assert closed.crs != wb.crs
    assert closed_basin_comids(wb, closed) == {110}


def test_frac_own_endorheic_lake_drains_to_its_own_terminus():
    # A 3x3 lake (rows/cols 1..3) whose every cell flows to a code-0 cell at its
    # centre (2,2) -- the Great Salt Lake shape. All 9 cells reach their OWN
    # terminus, so frac_own == 1.0.
    fdr = np.full((5, 5), NOD, dtype=np.uint8)
    fdr[1, 1], fdr[1, 2], fdr[1, 3] = 2, 4, 8        # SE, S, SW -> (2,2)
    fdr[2, 1], fdr[2, 2], fdr[2, 3] = 1, 0, 16       # E, SINK, W -> (2,2)
    fdr[3, 1], fdr[3, 2], fdr[3, 3] = 128, 64, 32    # NE, N, NW -> (2,2)
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    assert frac_own_for_window(fdr, inside, NOD) == 1.0


def test_frac_own_through_flowing_lake_rejects():
    # The Lewis and Clark Lake shape: a through-flowing reservoir that happens to
    # contain ONE stray terminal cell. Every other cell flows E and leaves the lake,
    # so only the sink cell itself reaches an inside terminus -> 1/9 = 0.111.
    # Rule A ("contains a terminal cell") would wrongly demote this; Signal A does not.
    fdr = np.full((5, 5), 1, dtype=np.uint8)         # everything flows East
    fdr[1, 1] = 0                                     # the stray terminal cell
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    frac = frac_own_for_window(fdr, inside, NOD)
    assert frac == pytest.approx(1 / 9, abs=1e-6)
    assert frac < 0.5


def test_frac_own_is_zero_when_the_lake_has_no_terminal_cell():
    fdr = np.full((5, 5), 1, dtype=np.uint8)
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    assert frac_own_for_window(fdr, inside, NOD) == 0.0


def test_frac_own_ignores_a_terminal_cell_OUTSIDE_the_lake():
    # A pond upstream in a closed basin drains to the BASIN's terminus, not its own.
    # Signal A must not demote it. Here the lake's cells all flow E to a sink that
    # lies outside the lake.
    fdr = np.full((5, 5), 1, dtype=np.uint8)
    fdr[2, 4] = 0                                     # terminus OUTSIDE the lake
    inside = np.zeros((5, 5), dtype=bool)
    inside[1:4, 1:4] = True
    assert frac_own_for_window(fdr, inside, NOD) == 0.0


def test_terminal_cells_finds_the_one_code0_cell(tmp_path):
    # terminal_cells scans block-by-block (required at CONUS scale, see module
    # docstring) -- prove it finds exactly the sink cell and none of the flow cells.
    fdr = np.full((5, 5), 1, dtype=np.uint8)
    fdr[2, 2] = 0
    transform = from_origin(0.0, 5 * 2000.0, 2000.0, 2000.0)
    path = tmp_path / "fdr_single.tif"
    _write_fdr(path, fdr, transform)
    terminal = terminal_cells(path)
    assert len(terminal) == 1
    with rasterio.open(path) as src:
        assert terminal.crs == src.crs


def test_terminus_own_fraction_dissolves_a_two_row_comid(tmp_path):
    # `conus_waterbodies.gpkg` stores a multi-part waterbody as several rows sharing
    # one COMID (measured on the real candidate set: 6,429 rows / 6,427 unique
    # COMIDs). COMID 555 here is split into two rows that do NOT touch:
    #   * part_b (3x3, 9 cells, area 3.6e7 m^2) -- the Great-Salt-Lake "own terminus"
    #     pattern, holding the ONLY code-0 cell in the raster, and every one of its
    #     other 8 cells drains to it.
    #   * part_a (4x4, 16 cells, area 6.4e7 m^2 -- the LARGER part) -- flows due east
    #     and never reaches part_b's terminus.
    # Keeping only the largest row (`drop_duplicates(subset="COMID")` on area) would
    # throw part_b -- and the only terminal cell -- away entirely, silently zeroing
    # frac_own for the whole COMID. Dissolving keeps both parts, so the terminus is
    # still found and both parts' cells count in the denominator:
    #   n_inside = 9 (part_b) + 16 (part_a) = 25; reach = the 9 part_b cells only
    #   (part_a's east-flowing cells never reach it) -> frac_own = 9/25 = 0.36.
    ny = 14
    pixel = 2000.0
    fdr = np.full((ny, ny), NOD, dtype=np.uint8)
    fdr[1, 1], fdr[1, 2], fdr[1, 3] = 2, 4, 8
    fdr[2, 1], fdr[2, 2], fdr[2, 3] = 1, 0, 16
    fdr[3, 1], fdr[3, 2], fdr[3, 3] = 128, 64, 32
    fdr[7:11, 7:11] = 1

    transform = from_origin(0.0, ny * pixel, pixel, pixel)
    fdr_path = tmp_path / "fdr_two_row.tif"
    _write_fdr(fdr_path, fdr, transform)

    part_b = _box(1 * pixel, (ny - 4) * pixel, 4 * pixel, (ny - 1) * pixel)
    part_a = _box(7 * pixel, (ny - 11) * pixel, 11 * pixel, (ny - 7) * pixel)
    assert part_a.area > part_b.area  # part_a is the one "keep largest" would keep
    assert not part_a.intersects(part_b)
    wb = _wb([[555, part_a], [555, part_b]])

    terminal = terminal_cells(fdr_path)
    assert len(terminal) == 1

    out = terminus_own_fraction(wb, fdr_path, terminal)
    assert list(out["comid"]) == [555]
    row = out.iloc[0]
    assert row["n_terminal"] == 1
    assert row["frac_own"] == pytest.approx(9 / 25, abs=1e-6)


def test_endorheic_parquet_roundtrip(tmp_path):
    df = pd.DataFrame({
        "comid": [1, 2, 3],
        "frac_own": [1.0, 0.007, 0.0],
        "by_terminus": [True, False, False],
        "by_closed_huc12": [False, False, True],
    })
    p = tmp_path / "endorheic.parquet"
    write_endorheic_comids(df, p)
    # Only rows flagged by at least one signal are demotions.
    assert load_endorheic_comids(p) == {1, 3}


def test_load_endorheic_comids_rejects_an_empty_table(tmp_path):
    p = tmp_path / "endorheic.parquet"
    write_endorheic_comids(
        pd.DataFrame(columns=["comid", "frac_own", "by_terminus", "by_closed_huc12"]), p
    )
    with pytest.raises(ValueError, match="0 endorheic COMIDs"):
        load_endorheic_comids(p)
