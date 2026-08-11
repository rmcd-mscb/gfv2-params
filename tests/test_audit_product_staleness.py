"""Tests for the two-axis product-staleness audit (#215).

The pure functions are what carry the logic worth pinning: which axes flag a product,
how a `prms.builder` prose string maps to module paths, and how a VRT resolves through
to the files it actually serves. The filesystem/git orchestration around them is thin
and data-root-gated, so it is exercised by running the script, not by a unit test.
"""

import datetime as dt
from pathlib import Path

from scripts.diagnose.audit_product_staleness import (
    _source_date,
    builder_paths,
    classify,
    parse_vrt_sources,
)

D = dt.date


class TestClassify:
    def test_no_flags_when_product_is_newest(self):
        assert classify(D(2026, 8, 11), D(2026, 7, 1), D(2026, 7, 1)) == []

    def test_code_axis_flags_a_newer_builder(self):
        assert classify(D(2026, 5, 29), D(2026, 6, 8), D(2026, 3, 26)) == ["CODE newer"]

    def test_source_axis_flags_a_newer_raster(self):
        """The aspect case (#201): the builder and config were untouched, and the
        SOURCE moved. An audit on the code axis alone reported it verified-current
        while 413 gfv2 HRUs were drifted by up to 89 degrees."""
        assert classify(D(2026, 5, 29), D(2026, 5, 23), D(2026, 7, 1)) == ["SOURCE newer"]

    def test_both_axes_can_flag_together(self):
        """The elevation case: product 2026-05-29, builder 2026-08-10, source 2026-07-01."""
        assert classify(D(2026, 5, 29), D(2026, 8, 10), D(2026, 7, 1)) == [
            "CODE newer", "SOURCE newer",
        ]

    def test_a_missing_date_is_not_a_flag(self):
        """Several params legitimately have no single `source_raster` -- the depstor
        ratios and ssflux among them. Treating absent as stale would bury the real
        candidates in noise."""
        assert classify(D(2026, 7, 15), None, None) == []
        assert classify(D(2026, 7, 15), None, D(2026, 7, 1)) == []

    def test_same_day_is_not_stale(self):
        """Strict `<`, so a product written the same day its builder changed is not
        flagged. Date granularity cannot resolve that ordering, and flagging it would
        make every freshly-rebuilt product a candidate."""
        assert classify(D(2026, 8, 10), D(2026, 8, 10), D(2026, 8, 10)) == []


class TestBuilderPaths:
    def test_single_module(self):
        assert builder_paths("zonal_runners/lulc.py") == ["src/gfv2_params/zonal_runners/lulc.py"]

    def test_multi_module_prose_string(self):
        """`prms.builder` is prose, not a path list -- real entries read like
        'depstor_builders/dprst_depth.py + dprst_depth/aggregate.py'."""
        assert builder_paths("depstor_builders/dprst_depth.py + dprst_depth/aggregate.py") == [
            "src/gfv2_params/depstor_builders/dprst_depth.py",
            "src/gfv2_params/dprst_depth/aggregate.py",
        ]

    def test_prose_around_the_paths_is_ignored(self):
        """The aspect entry reads
        'zonal_runners/aspect.py + zonal_runners/merge.py (derived_columns)'."""
        got = builder_paths("zonal_runners/aspect.py + zonal_runners/merge.py (derived_columns)")
        assert got == [
            "src/gfv2_params/zonal_runners/aspect.py",
            "src/gfv2_params/zonal_runners/merge.py",
        ]

    def test_empty_and_none(self):
        assert builder_paths(None) == []
        assert builder_paths("") == []


class TestParseVrtSources:
    def test_relative_sources_resolve_against_the_vrt_directory(self):
        """relativeToVRT="1" means relative to the VRT, not the CWD -- resolving it
        against the CWD would silently yield non-existent paths, and a missing path
        contributes no date, so the SOURCE axis would go quietly dark."""
        xml = """<VRTDataset rasterXSize="10" rasterYSize="10">
          <VRTRasterBand band="1"><SimpleSource>
            <SourceFilename relativeToVRT="1">../per_vpu/01/tile_01.tif</SourceFilename>
          </SimpleSource></VRTRasterBand></VRTDataset>"""
        got = parse_vrt_sources(xml, Path("/data/shared/conus/vrt"))
        assert got == [Path("/data/shared/conus/vrt/../per_vpu/01/tile_01.tif")]

    def test_absolute_sources_are_left_alone(self):
        xml = """<VRTDataset><VRTRasterBand band="1"><SimpleSource>
            <SourceFilename relativeToVRT="0">/abs/path/tile.tif</SourceFilename>
          </SimpleSource></VRTRasterBand></VRTDataset>"""
        assert parse_vrt_sources(xml, Path("/data/vrt")) == [Path("/abs/path/tile.tif")]

    def test_collects_every_source_not_just_the_first(self):
        """The date that matters is the NEWEST tile, so missing later sources would
        under-report staleness -- the failure direction that lets drift ship."""
        xml = """<VRTDataset><VRTRasterBand band="1">
            <SimpleSource><SourceFilename relativeToVRT="1">a.tif</SourceFilename></SimpleSource>
            <SimpleSource><SourceFilename relativeToVRT="1">b.tif</SourceFilename></SimpleSource>
            <SimpleSource><SourceFilename relativeToVRT="1">c.tif</SourceFilename></SimpleSource>
          </VRTRasterBand></VRTDataset>"""
        assert len(parse_vrt_sources(xml, Path("/v"))) == 3

    def test_malformed_xml_returns_empty_rather_than_raising(self):
        """An unreadable VRT must not abort the audit of every other param."""
        assert parse_vrt_sources("not xml at all <<<", Path("/v")) == []


class TestSourceDate:
    """A VRT's date must be the LATER of its own mtime and its newest tile.

    Taking only the tiles was a real false negative, not a hypothetical one: oregon's
    `nhm_elevation_params.csv` (2026-07-25) postdated every `elevation.vrt` tile
    (<= 2026-07-01) and the tiles-only form called it current -- but rebuilding moved
    99.9% of its HRUs. `elevation.vrt` had been rewritten 2026-08-05, and a VRT defines
    the compositing (sources, order, nodata, band mapping), so a rewrite changes served
    pixels with every tile byte-identical.
    """

    def _vrt(self, tmp_path, tile_names=("a.tif",)):
        for n in tile_names:
            (tmp_path / n).write_bytes(b"tile")
        srcs = "".join(
            f'<SimpleSource><SourceFilename relativeToVRT="1">{n}</SourceFilename>'
            f"</SimpleSource>" for n in tile_names
        )
        v = tmp_path / "x.vrt"
        v.write_text(f'<VRTDataset><VRTRasterBand band="1">{srcs}</VRTRasterBand></VRTDataset>')
        return v

    def _touch(self, path, day):
        import os
        ts = dt.datetime(2026, 8, day).timestamp()
        os.utime(path, (ts, ts))

    def test_vrt_newer_than_its_tiles_wins(self, tmp_path):
        """The oregon-elevation case. Tiles old, VRT rewritten later -> the VRT date."""
        v = self._vrt(tmp_path)
        self._touch(tmp_path / "a.tif", 1)
        self._touch(v, 5)
        assert _source_date(str(v)) == D(2026, 8, 5)

    def test_tile_newer_than_its_vrt_wins(self, tmp_path):
        """The converse: tiles replaced under a VRT leave its own mtime untouched."""
        v = self._vrt(tmp_path)
        self._touch(v, 1)
        self._touch(tmp_path / "a.tif", 9)
        assert _source_date(str(v)) == D(2026, 8, 9)

    def test_newest_of_several_tiles_wins(self, tmp_path):
        v = self._vrt(tmp_path, ("a.tif", "b.tif", "c.tif"))
        self._touch(v, 1)
        for n, day in (("a.tif", 2), ("b.tif", 7), ("c.tif", 3)):
            self._touch(tmp_path / n, day)
        assert _source_date(str(v)) == D(2026, 8, 7)

    def test_plain_raster_uses_its_own_mtime(self, tmp_path):
        tif = tmp_path / "plain.tif"
        tif.write_bytes(b"x")
        self._touch(tif, 4)
        assert _source_date(str(tif)) == D(2026, 8, 4)

    def test_missing_and_none_are_not_dates(self, tmp_path):
        assert _source_date(None) is None
        assert _source_date(str(tmp_path / "nope.vrt")) is None
