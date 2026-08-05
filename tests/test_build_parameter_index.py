"""Unit coverage for scripts/build_parameter_index.py's pure logic.

`test_generated_index_is_up_to_date` in tests/test_params_index.py proves only that
the committed doc is a FIXED POINT of today's generator against today's configs. A
bug in `_collapse`, `_rename_marker` or `_entry_locations` is baked into the
committed doc, so generator and golden file agree with each other and the golden
test passes forever -- the same "test and implementation wrong in the same
direction" failure the DeclaredParam docstring exists to warn about.

These test the logic directly instead. Pure text + YAML, CI-safe.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from gfv2_params.params_index import DeclaredParam

_REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "build_parameter_index", _REPO_ROOT / "scripts" / "build_parameter_index.py"
)
bpi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bpi)


def _declared(name="p", merged="f.csv", fill=None, prms=None):
    return DeclaredParam(name, merged, fill or [], {}, prms or {})


# --- staleness detection: the failing direction the golden test never exercises ---


def test_check_reports_stale_when_a_generated_region_drifts(tmp_path, monkeypatch):
    """A generator that always returned 0 would pass CI without this.

    test_generated_index_is_up_to_date only ever sees the passing direction.
    """
    doc = tmp_path / "parameter_index.md"
    doc.write_text(_REPO_ROOT.joinpath("docs/parameter_index.md").read_text())
    monkeypatch.setattr(bpi, "INDEX_PATH", doc)

    assert bpi.build(check=True) == 0  # baseline: the copy is in sync

    doc.write_text(doc.read_text().replace("`carea_max`", "`carea_maxx`", 1))
    assert bpi.build(check=True) == 1


def test_build_is_idempotent(tmp_path, monkeypatch):
    """A non-fixed-point generator makes the CI staleness test flap unreproducibly."""
    doc = tmp_path / "parameter_index.md"
    doc.write_text(_REPO_ROOT.joinpath("docs/parameter_index.md").read_text())
    monkeypatch.setattr(bpi, "INDEX_PATH", doc)

    assert bpi.build() == 0
    first = doc.read_text()
    assert bpi.build() == 0
    assert doc.read_text() == first


def test_missing_marker_raises_rather_than_guessing(tmp_path, monkeypatch):
    doc = tmp_path / "parameter_index.md"
    doc.write_text("# no markers here\n")
    monkeypatch.setattr(bpi, "INDEX_PATH", doc)
    with pytest.raises(SystemExit, match="by-process"):
        bpi.build()


# --- the line-number scanner, and the documented dprst_frac collision ---


def test_entry_locations_resolves_dprst_frac_to_the_ratios_entry():
    """`- name: dprst_frac` appears in BOTH `fractions:` and `ratios:`.

    The fractions one comes first, and it names a count CSV in
    merged/_intermediates/, not the PRMS parameter. This is why `fractions:` is
    excluded from _ENTRY_SECTIONS; adding it back would silently re-point the cite.
    """
    locations = bpi._entry_locations()
    cite = locations["dprst_frac"]
    lineno = int(cite.split(":")[1])
    line = bpi.DEPSTOR_PARAMS_CONFIG.read_text().splitlines()[lineno - 1]
    assert line.strip().startswith("- name: dprst_frac")

    # ...and it is the ratios entry, i.e. the one carrying output_file.
    tail = "\n".join(bpi.DEPSTOR_PARAMS_CONFIG.read_text().splitlines()[lineno:lineno + 6])
    assert "output_file:" in tail, f"cite points at a non-ratios dprst_frac: {cite}"


def test_entry_locations_covers_every_declared_entry():
    """A miss degrades the cite to a bare param name and sorts the row to the top."""
    from gfv2_params.params_index import load_declared_params

    missing = {d.name for d in load_declared_params()} - set(bpi._entry_locations())
    assert not missing, f"no computed cite for {sorted(missing)}"


def test_entry_sort_key_orders_line_numbers_numerically():
    """Regression: a string compare sorted `:43` after `:389`."""
    cites = ["z.yml:43", "z.yml:389", "z.yml:81", "z.yml:133"]
    assert sorted(cites, key=bpi._entry_sort_key) == [
        "z.yml:43", "z.yml:81", "z.yml:133", "z.yml:389",
    ]


def test_natural_key_orders_snarea_curve_10_after_9():
    cols = ["snarea_curve_0", "snarea_curve_10", "snarea_curve_9"]
    assert sorted(cols, key=bpi._natural_key)[-1] == "snarea_curve_10"


# --- the ⚠️ rename marker's four documented rules ---


@pytest.mark.parametrize(
    ("prms", "cols", "expect_marker"),
    [
        ("soil_type", ["soils"], True),          # genuine rename
        ("hru_elev", ["mean"], True),            # genuine rename
        ("carea_max", ["carea_max"], False),     # identical
        ("snarea_curve", [f"snarea_curve_{i}" for i in range(11)], False),  # dimensioned
        ("rad_trncf", ["retention", "rad_trncf"], True),  # alias: one member differs
    ],
)
def test_rename_marker_rules(prms, cols, expect_marker):
    """The alias rule is a subtle inversion worth pinning.

    The test is "is EVERY declared column the PRMS name?", not "is ANY of them?" --
    on gfv2 and oregon the on-disk column is `retention`, so a reader there DOES
    need to translate. Reads wrong at a glance and would be "simplified" away.
    """
    row = {"prms": prms, "cols": cols, "defective": False}
    assert bool(bpi._rename_marker(row)) is expect_marker


# --- row collapsing across alternative sources ---


def test_collapse_merges_alternative_sources_keeping_cells_row_aligned():
    """The four LULC sources are alternatives, not four parameter sets.

    Uncollapsed they turned PRMSCanopy's 6 parameters into 24 rows. The parallel
    `sources` array is what keeps the file / entry / builder cells aligned, so a
    reader can pair them positionally -- a collapse bug would mis-attribute a
    builder to a file in published docs.
    """
    locations = {"a": "zonal_params.yml:100", "b": "zonal_params.yml:200"}
    declared = [
        _declared("a", "nhm_a.csv", prms={
            "builder": "lulc_prederived.py",
            "columns": {"cov_type": {"prms": "cov_type", "processes": ["PRMSCanopy"]}},
        }),
        _declared("b", "nhm_b.csv", prms={
            "builder": "lulc.py",
            "columns": {"cov_type": {"prms": "cov_type", "processes": ["PRMSCanopy"]}},
        }),
    ]
    by_process, _ = bpi._process_rows(declared, locations)
    rows = by_process["PRMSCanopy"]
    assert len(rows) == 1, "alternative sources for one parameter must collapse"

    (row,) = rows.values()
    assert row["sources"] == [
        ("nhm_a.csv", "zonal_params.yml:100", "lulc_prederived.py"),
        ("nhm_b.csv", "zonal_params.yml:200", "lulc.py"),
    ], "sources must stay in config order and keep file/entry/builder together"


def test_defects_never_collapse_into_a_real_row():
    """Merging a defect with a working column would erase the DEFECTIVE warning."""
    declared = [
        _declared("aspect", "nhm_aspect.csv", prms={
            "builder": "zonal.py",
            "columns": {"mean": {"prms": "hru_aspect", "processes": ["PRMSAtmosphere"]}},
            "defects": {"mean": {"prms": "hru_aspect", "processes": ["PRMSAtmosphere"]}},
        }),
    ]
    by_process, _ = bpi._process_rows(declared, {"aspect": "z.yml:1"})
    verdicts = {r["defective"] for r in by_process["PRMSAtmosphere"].values()}
    assert verdicts == {True, False}, "a defect must stay a distinct row"


def test_defect_with_no_processes_still_appears():
    """Regression: the defects loop had no empty-processes fallback.

    A defect whose consuming process is not yet known vanished from the by-process
    view entirely -- the exact silent omission the defects bucket exists to prevent.
    """
    declared = [
        _declared("x", "nhm_x.csv", prms={
            "builder": "b.py",
            "defects": {"mean": {"prms": "hru_thing", "processes": []}},
        }),
    ]
    _, unconsumed = bpi._process_rows(declared, {"x": "z.yml:1"})
    assert [r["defective"] for r in unconsumed.values()] == [True]


def test_defective_row_renders_the_issue_link():
    row = {
        "prms": "hru_aspect", "cols": ["mean"], "defective": True, "issue": 201,
        "sources": [("nhm_aspect.csv", "z.yml:1", "zonal.py")],
    }
    md = bpi._row_markdown(row)
    assert "**DEFECTIVE**" in md
    assert "issues/201" in md
    assert "⚠️" not in md, "DEFECTIVE supersedes the rename marker"
