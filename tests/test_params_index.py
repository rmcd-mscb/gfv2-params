"""Unit coverage for gfv2_params.params_index.

Pure YAML parsing -- no geo imports, no data root. Safe on CI's ubuntu-latest.

The DISK-facing half of this contract lives in tests/test_params_index_ondisk.py,
which is data-root-gated and therefore SKIPS here. Nothing in this file can catch a
new column reaching disk with no PRMS decision recorded.
"""

from __future__ import annotations

import pytest

from gfv2_params import params_index as pi


def test_declared_param_has_prms_field_defaulting_to_empty():
    d = pi.DeclaredParam("elevation", "nhm_elevation_params.csv", ["mean"], {})
    assert d.prms == {}
    assert d.name == "elevation"


def test_iter_declared_params_unions_four_config_families():
    zonal = {
        "params": [
            {
                "name": "elevation",
                "merged_file": "nhm_elevation_params.csv",
                "fill_columns": ["mean"],
            }
        ]
    }
    depstor = {
        "fractions": [{"name": "perv_frac", "merged_file": "nhm_perv_frac_params.csv"}],
        "means": [
            {
                "name": "dprst_depth_avg",
                "merged_file": "nhm_dprst_depth_avg_params.csv",
                "fill_columns": ["dprst_depth_avg"],
            }
        ],
        "ratios": [
            {
                "name": "dprst_frac",
                "output_file": "nhm_dprst_frac_params.csv",
                "fill_columns": ["dprst_frac"],
            }
        ],
    }
    declared = pi.iter_declared_params(zonal, depstor)
    names = {d.name for d in declared}
    assert names == {"elevation", "dprst_depth_avg", "dprst_frac"}
    # fractions are intermediates -- excluded despite carrying a merged_file key
    assert "perv_frac" not in names


def test_iter_declared_params_includes_constants():
    """`constants:` are builder-written per-HRU params copied into merged/."""
    depstor = {
        "constants": [
            {
                "name": "op_flow_thres",
                "merged_file": "nhm_op_flow_thres_params.csv",
                "fill_columns": ["op_flow_thres"],
            }
        ]
    }
    declared = pi.iter_declared_params({}, depstor)
    assert [d.name for d in declared] == ["op_flow_thres"]
    assert declared[0].merged_file == "nhm_op_flow_thres_params.csv"


def test_iter_declared_params_carries_prms_block():
    zonal = {
        "params": [
            {
                "name": "slope",
                "merged_file": "nhm_slope_params.csv",
                "fill_columns": ["mean"],
                "prms": {
                    "columns": {
                        "hru_slope": {
                            "prms": "hru_slope",
                            "processes": ["PRMSSolarGeometry"],
                        }
                    }
                },
            }
        ]
    }
    declared = pi.iter_declared_params(zonal, {})
    assert declared[0].prms["columns"]["hru_slope"]["prms"] == "hru_slope"


# ---------------------------------------------------------------------------
# Guard 1: prms.columns | prms.defects | prms.provenance is the DECLARED
# COMPLETE column list for every entry.
# ---------------------------------------------------------------------------


def _flatten(fill_columns):
    """fill_columns entries may be a list of alias alternatives, not only a string.

    zonal_params.yml's lulc_nhm_v11 carries [retention, rad_trncf] because
    lulc_prederived.py renamed the same computed quantity; fabrics built before the
    rename carry `retention`, after carry `rad_trncf`. EVERY alternative must be
    declared, so a fabric of either vintage is covered. Without this flatten the
    guard raises `TypeError: unhashable type: 'list'` rather than failing usefully.
    """
    out = []
    for item in fill_columns:
        if isinstance(item, (list, tuple)):
            out.extend(item)
        else:
            out.append(item)
    return out


def _declared_columns(declared):
    """Every emitted column the entry has recorded a PRMS decision for.

    Three buckets, because there are three distinct decisions: `columns` is "this
    IS the PRMS parameter", `defects` is "this is SUPPOSED to be the PRMS parameter
    and currently is not", `provenance` is "this is not a PRMS parameter at all".
    """
    return (
        set(declared.prms.get("columns") or {})
        | set(declared.prms.get("defects") or {})
        | set(declared.prms.get("provenance") or {})
    )


@pytest.mark.parametrize("declared", pi.load_declared_params(), ids=lambda d: d.name)
def test_guard1_prms_declares_every_fillable_column(declared):
    """Guard 1 only proves the declaration is SELF-CONSISTENT.

    It runs declaration -> declaration and cannot see disk. On 16 of 19 entries
    fill_columns and the declared columns are the same set, so it is a literal
    tautology; on the other three the fill_columns cover only part of the on-disk
    header (ssflux 7 of 10, snarea 13 of 23, dprst_depth_avg 1 of 2), leaving the
    remainder unchecked HERE -- note `prms:` itself declares all of them, it is
    `fill_columns` that is the smaller set. Guard 2
    (tests/test_params_index_ondisk.py) is the one that catches a new column
    reaching disk with no PRMS decision recorded.
    """
    assert declared.prms, (
        f"{declared.name} has no `prms:` block. It is mandatory, not optional -- "
        f"without it this guard passes vacuously."
    )
    required = set(_flatten(declared.fill_columns)) | set(declared.fabric_columns or {})
    missing = required - _declared_columns(declared)
    assert not missing, (
        f"{declared.name}: {sorted(missing)} are declared fillable but appear in none "
        f"of prms.columns / prms.defects / prms.provenance. Every emitted column "
        f"needs a PRMS decision."
    )


@pytest.mark.parametrize("declared", pi.load_declared_params(), ids=lambda d: d.name)
def test_guard1_prms_declares_a_builder(declared):
    """`builder:` drives the index's by-builder view.

    Kept in the config rather than a lookup table in the generator, which would
    drift the moment a builder moves.
    """
    assert declared.prms.get("builder"), f"{declared.name} declares no prms.builder"


@pytest.mark.parametrize("declared", pi.load_declared_params(), ids=lambda d: d.name)
def test_guard1_no_column_is_declared_twice(declared):
    """columns / defects / provenance are mutually exclusive verdicts.

    A column in two buckets means two contradictory answers to "is this the PRMS
    parameter?", and `_declared_columns`'s union would hide it from Guard 1.
    """
    buckets = [
        set(declared.prms.get(k) or {}) for k in ("columns", "defects", "provenance")
    ]
    overlaps = (
        (buckets[0] & buckets[1]) | (buckets[0] & buckets[2]) | (buckets[1] & buckets[2])
    )
    assert not overlaps, f"{declared.name}: {sorted(overlaps)} declared in two buckets"


def test_op_flow_thres_is_declared_with_a_merged_file():
    """D1: op_flow_thres is a PRMSRunoff param that was written outside merged/.

    A depstor BUILDER wrote it straight to depstor_rasters/, so it was invisible to
    iter_declared_params AND to warn_undeclared_merged_files (which globs merged_dir).
    Anyone assembling a parameter file by globbing merged/nhm_*_params.csv dropped it
    silently, and the fill sweep never saw it either.
    """
    declared = {d.name: d for d in pi.load_declared_params()}
    assert "op_flow_thres" in declared
    assert declared["op_flow_thres"].merged_file == "nhm_op_flow_thres_params.csv"
    assert declared["op_flow_thres"].prms["columns"]["op_flow_thres"]["processes"] == [
        "PRMSRunoff"
    ]


def test_op_flow_thres_is_not_a_means_entry():
    """`constants:`, deliberately, not `means:`.

    run_mean_zonal does Path(spec["source_raster"]) unconditionally and _find_mean
    advertises every means[].name as a runnable --mean target, so a raster-less
    means entry is a KeyError waiting for the first operator who types
    `--mean op_flow_thres`. op_flow_thres is a constant 1.0 built from the fabric's
    id list, with no raster at all.
    """
    depstor = pi._load_yaml_doc(pi.DEPSTOR_PARAMS_CONFIG)
    assert "op_flow_thres" not in {e["name"] for e in depstor.get("means") or []}
    assert "op_flow_thres" in {e["name"] for e in depstor.get("constants") or []}
    for entry in depstor.get("constants") or []:
        assert "source_raster" not in entry, f"{entry['name']} is not a zonal target"


# ---------------------------------------------------------------------------
# params_for_process + the index generator.
# ---------------------------------------------------------------------------


def test_params_for_process_is_column_grained_not_entry_grained():
    """ssflux spans three processes; asking for PRMSRunoff must return only its
    two runoff columns, not the whole 10-column file."""
    hits = pi.params_for_process("PRMSRunoff")
    ssflux_cols = {col for col, d in hits if d.name == "ssflux"}
    assert ssflux_cols == {"dprst_seep_rate_open", "dprst_flow_coef"}
    assert "gwflow_coef" not in ssflux_cols  # PRMSGroundwater
    assert "soil2gw_max" not in ssflux_cols  # PRMSSoilzone


def test_prms_runoff_has_the_expected_parameter_count():
    hits = pi.params_for_process("PRMSRunoff")
    assert len({col for col, _ in hits}) == 11


def test_params_for_process_never_returns_a_defective_column():
    """aspect's `mean` names PRMSSolarGeometry/PRMSAtmosphere in prms.defects.

    Returning it here would hand a caller a broken column under a correct-looking
    parameter name -- the exact failure the defects/columns split exists to prevent.
    """
    for process in ("PRMSSolarGeometry", "PRMSAtmosphere"):
        hits = pi.params_for_process(process)
        assert all(d.name != "aspect" for _, d in hits), process
        assert {col for col, _ in hits} == {"hru_slope"}


def test_generated_index_is_up_to_date():
    """docs/parameter_index.md's generated regions must match the configs.

    Pure text + YAML, so it runs on CI. This is what stops the index drifting from
    the `prms:` blocks the moment someone edits a config -- the whole failure mode
    that made a hand-maintained index untrustworthy.
    """
    import importlib.util
    from pathlib import Path

    import pytest

    spec = importlib.util.spec_from_file_location(
        "build_parameter_index",
        Path(__file__).resolve().parent.parent / "scripts" / "build_parameter_index.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if module.build(check=True) != 0:
        pytest.fail(
            "docs/parameter_index.md is stale -- run "
            "`python scripts/build_parameter_index.py` and commit the result."
        )


# The 8 processes any declared column may name. pywatershed 2.0.4's Process
# classes plus the two PRMS-only consumers TM6B9 documents. Hardcoded rather than
# imported: pywatershed lives in the `reference` pixi env, not `dev`, so importing
# it would make this test env-dependent -- and the point is to catch a TYPO, which
# a frozen list does as well as a live import.
_KNOWN_PROCESSES = {
    "PRMSRunoff",
    "PRMSSoilzone",
    "PRMSSnow",
    "PRMSCanopy",
    "PRMSGroundwater",
    "PRMSEt",
    "PRMSSolarGeometry",
    "PRMSAtmosphere",
}


@pytest.mark.parametrize("declared", pi.load_declared_params(), ids=lambda d: d.name)
def test_guard1_column_specs_are_well_formed(declared):
    """Every columns/defects spec needs a `prms` name and a `processes` list.

    Guard 1 otherwise looks only at the spec KEYS, so a spec like
    `{processes: [PRMSRunoff]}` with the `prms:` key omitted passes Guard 1, passes
    Guard 2, makes params_for_process silently return nothing useful, and then dies
    inside scripts/build_parameter_index.py as a bare `KeyError: 'prms'` -- an error
    pointing at a documentation generator rather than at the typo'd YAML.
    """
    for bucket in ("columns", "defects"):
        for col, spec in (declared.prms.get(bucket) or {}).items():
            where = f"{declared.name}.prms.{bucket}.{col}"
            assert isinstance(spec, dict), f"{where} must be a mapping, got {spec!r}"
            assert isinstance(spec.get("prms"), str) and spec["prms"], (
                f"{where} has no `prms:` name. Without it the index generator "
                f"raises a bare KeyError far from the config that caused it."
            )
            assert isinstance(spec.get("processes"), list), (
                f"{where} has no `processes:` list. Use [] for a parameter no "
                f"pywatershed Process consumes (hru_elev), not a missing key."
            )


@pytest.mark.parametrize("declared", pi.load_declared_params(), ids=lambda d: d.name)
def test_guard1_process_names_are_known(declared):
    """A misspelt process name fails NOTHING without this.

    `PRMSSoilzon` silently drops that column from params_for_process("PRMSSoilzone")
    and invents a phantom `### PRMSSoilzon` section in the generated index -- and
    because the index is regenerated from the same typo, the staleness check still
    passes. The doc would confidently document a process that does not exist.
    """
    for bucket in ("columns", "defects"):
        for col, spec in (declared.prms.get(bucket) or {}).items():
            unknown = set(spec.get("processes") or []) - _KNOWN_PROCESSES
            assert not unknown, (
                f"{declared.name}.prms.{bucket}.{col} names unknown process(es) "
                f"{sorted(unknown)}. Known: {sorted(_KNOWN_PROCESSES)}. If this is a "
                f"genuinely new pywatershed Process, add it to _KNOWN_PROCESSES."
            )


def test_every_config_entry_reaches_the_declaration():
    """An entry with a typo'd `merged_file:` vanishes silently from the declaration.

    iter_declared_params skips any entry whose merged_file/output_file is falsy, and
    a skipped entry generates no parametrize case -- so the "prms: is mandatory" rule
    and both guards simply never run on it, and it never reaches the index. The
    backstop is a KeyError inside a SLURM-chained merge job, where it has to be
    noticed in sacct; this catches it in CI instead.
    """
    declared = {d.name for d in pi.load_declared_params()}
    for config, sections in (
        (pi.ZONAL_PARAMS_CONFIG, ("params",)),
        (pi.DEPSTOR_PARAMS_CONFIG, ("means", "ratios", "constants")),
    ):
        doc = pi._load_yaml_doc(config)
        for section in sections:
            for entry in doc.get(section) or []:
                assert entry["name"] in declared, (
                    f"{config.name}'s `{section}:` entry '{entry['name']}' never "
                    f"reached load_declared_params() -- check its "
                    f"merged_file:/output_file: key for a typo."
                )
