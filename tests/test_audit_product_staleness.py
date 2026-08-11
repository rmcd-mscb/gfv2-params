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
    consumes_map,
    derivation_date,
    parse_vrt_sources,
    propagate_upstream,
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


class TestDerivationDate:
    """The merged file's mtime is the last WRITE; a gap-fill bumps it in place.

    oregon's `nhm_lulc_nhm_v11_params.csv` read 2026-07-25 -- after the #135/#136 lulc
    rewrite (2026-06-08) -- so the audit called it current. Its per-batch CSVs are dated
    2026-05-20 and the product still carried the pre-rewrite `retention` column. The
    2026-07-25 stamp was a fill sweep, and the real derivation predated the builder by
    nearly three weeks.
    """

    def _touch(self, path, month, day):
        import os
        ts = dt.datetime(2026, month, day).timestamp()
        os.utime(path, (ts, ts))

    def test_per_batch_csvs_win_over_a_fill_bumped_merged_mtime(self, tmp_path):
        """The oregon lulc case, by name."""
        d = tmp_path / "lulc_nhm_v11"
        d.mkdir()
        for n in ("b0.csv", "b1.csv"):
            f = d / n
            f.write_text("x")
            self._touch(f, 5, 20)
        assert derivation_date(d, D(2026, 7, 25)) == D(2026, 5, 20)

    def test_newest_per_batch_file_wins(self, tmp_path):
        """A partial re-run leaves a mix; the newest is what run_merge consumed last."""
        d = tmp_path / "p"
        d.mkdir()
        for n, day in (("b0.csv", 20), ("b1.csv", 29), ("b2.csv", 22)):
            f = d / n
            f.write_text("x")
            self._touch(f, 5, day)
        assert derivation_date(d, D(2026, 7, 25)) == D(2026, 5, 29)

    def test_falls_back_to_merged_when_there_is_no_per_batch_stage(self, tmp_path):
        """depstor constants and snarea have no per-batch directory; the merged mtime
        is then the best available and must not be discarded."""
        assert derivation_date(tmp_path / "absent", D(2026, 7, 15)) == D(2026, 7, 15)

    def test_falls_back_when_the_directory_is_empty(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        assert derivation_date(d, D(2026, 7, 15)) == D(2026, 7, 15)


class TestConsumesMap:
    """Dependencies are DERIVED from the config keys builders actually read, so they
    cannot drift from the truth the way a second hand-maintained declaration would."""

    ENTRIES = [
        {"name": "slope", "merged_file": "nhm_slope_params.csv"},
        {"name": "ssflux", "merged_file": "nhm_ssflux_params.csv",
         "merged_slope_file": "{data_root}/{fabric}/params/merged/nhm_slope_params.csv"},
        {"name": "elevation", "merged_file": "nhm_elevation_params.csv"},
    ]

    def test_finds_the_real_ssflux_slope_edge(self):
        assert consumes_map(self.ENTRIES) == {"ssflux": {"slope"}}

    def test_a_params_own_merged_file_is_not_a_self_dependency(self):
        entries = [{"name": "x", "merged_file": "nhm_x_params.csv",
                    "other": "/some/path/nhm_x_params.csv"}]
        assert consumes_map(entries) == {}

    def test_unrelated_paths_are_ignored(self):
        entries = [{"name": "a", "merged_file": "nhm_a_params.csv",
                    "source_raster": "/data/shared/conus/vrt/elevation.vrt"}]
        assert consumes_map(entries) == {}


class TestPropagateUpstream:
    def _row(self, name, product, flags=None):
        return {"param": name, "product": product, "builder": None,
                "source": None, "flags": list(flags or [])}

    def test_consumer_inherits_a_stale_dependency_even_when_far_newer(self):
        """The ssflux case, and the reason an 'is my input newer than me?' test alone
        is not enough: ssflux (2026-08-10) IS newer than slope (2026-05-29), yet it was
        built from a slope product that predates the 2026-07-01 slope.vrt rebuild."""
        rows = [self._row("slope", D(2026, 5, 29), ["SOURCE newer"]),
                self._row("ssflux", D(2026, 8, 10))]
        out = {r["param"]: r["flags"] for r in propagate_upstream(rows, {"ssflux": {"slope"}})}
        assert out["ssflux"] == ["UPSTREAM slope stale"]

    def test_consumer_flagged_when_its_dependency_is_newer_than_it(self):
        rows = [self._row("slope", D(2026, 8, 10)), self._row("ssflux", D(2026, 5, 1))]
        out = {r["param"]: r["flags"] for r in propagate_upstream(rows, {"ssflux": {"slope"}})}
        assert out["ssflux"] == ["UPSTREAM slope newer"]

    def test_clean_dependency_leaves_the_consumer_alone(self):
        rows = [self._row("slope", D(2026, 5, 1)), self._row("ssflux", D(2026, 8, 10))]
        out = {r["param"]: r["flags"] for r in propagate_upstream(rows, {"ssflux": {"slope"}})}
        assert out["ssflux"] == []

    def test_staleness_propagates_along_a_chain(self):
        """a stale -> b inherits -> c inherits from b."""
        rows = [self._row("a", D(2026, 5, 1), ["SOURCE newer"]),
                self._row("b", D(2026, 8, 1)), self._row("c", D(2026, 8, 2))]
        out = {r["param"]: r["flags"] for r in
               propagate_upstream(rows, {"b": {"a"}, "c": {"b"}})}
        assert out["b"] == ["UPSTREAM a stale"]
        assert out["c"] == ["UPSTREAM b stale"]

    def test_a_cycle_terminates(self):
        """A cyclic config must not hang the audit; the loop is bounded by row count."""
        rows = [self._row("a", D(2026, 5, 1), ["CODE newer"]), self._row("b", D(2026, 5, 2))]
        out = propagate_upstream(rows, {"a": {"b"}, "b": {"a"}})
        assert all(isinstance(r["flags"], list) for r in out)

    def test_a_missing_dependency_row_is_skipped(self):
        """A param declared in config but not built for this fabric has no row."""
        rows = [self._row("ssflux", D(2026, 8, 10))]
        out = propagate_upstream(rows, {"ssflux": {"slope"}})
        assert out[0]["flags"] == []
