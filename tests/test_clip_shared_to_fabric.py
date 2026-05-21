"""Unit tests for the pure grid-math helpers in scripts/clip_shared_to_fabric.py.

These functions decide the fabric-bounds template grid for the depstor pipeline.
A wrong snap silently produces a misaligned or under-covering template that only
fails after a long CONUS-scale run (or, worse, passes carea_map's alignment check
on the wrong lattice), so the math is worth pinning at CI speed with synthetic
transforms — no staged rasters or GDAL I/O required.

scripts/ is not an importable package, so the module is loaded by path.
"""

import importlib.util
from pathlib import Path

import pytest
from rasterio.transform import Affine

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "clip_shared_to_fabric.py"
_spec = importlib.util.spec_from_file_location("clip_shared_to_fabric", _SCRIPT)
clip = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(clip)

CELL = 30.0


def _on_lattice(value, origin, cell, tol=1e-6):
    """True if `value` sits on the {origin + k*cell} lattice."""
    k = (value - origin) / cell
    return abs(k - round(k)) < tol


class TestSnapBoundsToGrid:
    # A north-up transform whose origin is deliberately NOT a round multiple of
    # the cell size, to catch any code that forgets to subtract the origin.
    OX, OY = -2_356_109.7, 3_166_613.3
    TRANSFORM = Affine(CELL, 0.0, OX, 0.0, -CELL, OY)

    def test_contains_buffered_bounds(self):
        # bounds chosen to fall mid-cell on every side
        bounds = (self.OX + 100.7, self.OY - 9_000.3, self.OX + 5_000.4, self.OY - 100.9)
        buf = 8
        ulx, uly, lrx, lry = clip._snap_bounds_to_grid(bounds, self.TRANSFORM, buf)
        minx, miny, maxx, maxy = bounds
        assert ulx <= minx - buf * CELL
        assert lrx >= maxx + buf * CELL
        assert uly >= maxy + buf * CELL
        assert lry <= miny - buf * CELL

    def test_edges_land_on_source_lattice(self):
        bounds = (self.OX + 100.7, self.OY - 9_000.3, self.OX + 5_000.4, self.OY - 100.9)
        ulx, uly, lrx, lry = clip._snap_bounds_to_grid(bounds, self.TRANSFORM, 8)
        assert _on_lattice(ulx, self.OX, CELL)
        assert _on_lattice(lrx, self.OX, CELL)
        assert _on_lattice(uly, self.OY, CELL)
        assert _on_lattice(lry, self.OY, CELL)

    def test_width_height_are_whole_cells(self):
        bounds = (self.OX + 100.7, self.OY - 9_000.3, self.OX + 5_000.4, self.OY - 100.9)
        ulx, uly, lrx, lry = clip._snap_bounds_to_grid(bounds, self.TRANSFORM, 8)
        width = (lrx - ulx) / CELL
        height = (uly - lry) / CELL
        assert abs(width - round(width)) < 1e-6
        assert abs(height - round(height)) < 1e-6

    def test_zero_buffer_still_contains(self):
        bounds = (self.OX + 100.7, self.OY - 9_000.3, self.OX + 5_000.4, self.OY - 100.9)
        ulx, uly, lrx, lry = clip._snap_bounds_to_grid(bounds, self.TRANSFORM, 0)
        assert ulx <= bounds[0] and lry <= bounds[1]
        assert lrx >= bounds[2] and uly >= bounds[3]

    def test_bounds_already_on_lattice_no_spurious_expansion(self):
        # With zero buffer and edges exactly on the lattice, snapping must be a
        # no-op (no off-by-one full-cell expansion).
        minx = self.OX + 3 * CELL
        maxx = self.OX + 10 * CELL
        maxy = self.OY - 2 * CELL
        miny = self.OY - 7 * CELL
        ulx, uly, lrx, lry = clip._snap_bounds_to_grid((minx, miny, maxx, maxy), self.TRANSFORM, 0)
        assert (ulx, uly, lrx, lry) == pytest.approx((minx, maxy, maxx, miny))

    def test_rejects_non_north_up_transform(self):
        rotated = Affine(CELL, 0.5, self.OX, 0.5, -CELL, self.OY)  # nonzero b, d
        with pytest.raises(ValueError, match="north-up"):
            clip._snap_bounds_to_grid((0, 0, 100, 100), rotated, 4)

    def test_rejects_positive_e_transform(self):
        flipped = Affine(CELL, 0.0, self.OX, 0.0, CELL, self.OY)  # e > 0 (south-up)
        with pytest.raises(ValueError, match="north-up"):
            clip._snap_bounds_to_grid((0, 0, 100, 100), flipped, 4)


class TestWholeCellOffset:
    REF = Affine(CELL, 0.0, -2_356_125.0, 0.0, -CELL, 3_166_605.0)

    def test_aligned_when_shifted_by_integer_cells(self):
        clip_t = Affine(CELL, 0.0, self.REF.c + 469 * CELL, 0.0, -CELL, self.REF.f - 15159 * CELL)
        col_frac, row_frac = clip._whole_cell_offset(clip_t, self.REF)
        assert abs(col_frac) < 1e-6
        assert abs(row_frac) < 1e-6

    def test_detects_half_cell_offset(self):
        clip_t = Affine(CELL, 0.0, self.REF.c + 0.5 * CELL, 0.0, -CELL, self.REF.f - 0.5 * CELL)
        col_frac, row_frac = clip._whole_cell_offset(clip_t, self.REF)
        assert abs(abs(col_frac) - 0.5) < 1e-6
        assert abs(abs(row_frac) - 0.5) < 1e-6
