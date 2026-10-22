"""`export_fabric` -- the writer, on synthetic merged files in `tmp_path`.

Each failure test pins a way the export could otherwise ship a plausible-looking but
wrong `params/prms/` directory at exit code 0.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

from gfv2_params.params_index import DeclaredParam
from gfv2_params.prms_export import export_fabric, load_dictionary
from gfv2_params.snarea.library import CURVE_COLS

DICTIONARY = load_dictionary()
ID = "nat_hru_id"
N = 4


def _cols(mapping):
    return {c: {"prms": p, "processes": []} for c, p in mapping.items()}


DECLARED = [
    DeclaredParam("elevation", "nhm_elevation_params.csv", [], {},
                  {"columns": _cols({"mean": "hru_elev"}), "provenance": {"std": "stat"}}),
    DeclaredParam("soils", "nhm_soils_params.csv", [], {},
                  {"columns": _cols({"soils": "soil_type"})}),
    DeclaredParam("lulc_nhm_v11", "nhm_lulc_nhm_v11_params.csv", [], {},
                  {"columns": _cols({"retention": "rad_trncf", "rad_trncf": "rad_trncf"})}),
    DeclaredParam("lulc_nalcms", "nhm_lulc_nalcms_params.csv", [], {},
                  {"columns": _cols({"retention": "rad_trncf"}), "export": False}),
    DeclaredParam("snarea_curve", "nhm_snarea_curve_params.csv", [], {},
                  {"columns": _cols({"hru_deplcrv": "hru_deplcrv",
                                     **{c: "snarea_curve" for c in CURVE_COLS}})}),
]


def _ids():
    return np.arange(1, N + 1)


def _write_merged(d, **overrides):
    files = {
        "nhm_elevation_params.csv": pd.DataFrame({ID: _ids(), "mean": [10.0, 20.0, 30.0, 40.0], "std": 1.0}),
        "nhm_soils_params.csv": pd.DataFrame({ID: _ids(), "soils": [1.0, 2.0, 3.0, 2.0]}),
        "nhm_lulc_nhm_v11_params.csv": pd.DataFrame({ID: _ids(), "rad_trncf": [0.5, 0.6, 0.7, 0.8]}),
        "nhm_snarea_curve_params.csv": pd.DataFrame(
            {ID: _ids(), "hru_deplcrv": [1, 2, 1, 2], **{c: 0.0 for c in CURVE_COLS}}),
    }
    files.update(overrides)
    d.mkdir(parents=True, exist_ok=True)
    for name, df in files.items():
        df.to_csv(d / name, index=False)


def _library(d):
    # repo order is DESCENDING within a curve; the export must emit ASCENDING
    rows = []
    for cid, scale in ((1, 1.0), (2, 0.5)):
        curve = np.linspace(1.0, 0.0, 11) * scale
        rows.append({"deplcrv_id": cid, **dict(zip(CURVE_COLS, curve))})
    path = d / "lib.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


@pytest.fixture
def env(tmp_path):
    merged = tmp_path / "merged"
    _write_merged(merged)
    return merged, tmp_path / "prms", _library(tmp_path)


def _run(env, **kw):
    merged, out, lib = env
    return export_fabric(merged, out, ID, N, library_path=lib, declared=DECLARED,
                         dictionary=DICTIONARY, **kw)


def test_happy_path_writes_one_file_per_parameter_named_for_the_prms_parameter(env):
    paths = _run(env)
    assert sorted(p.name for p in paths) == [
        "hru_deplcrv.csv", "hru_elev.csv", "rad_trncf.csv", "snarea_curve.csv", "soil_type.csv"]
    elev = pd.read_csv(env[1] / "hru_elev.csv")
    assert list(elev.columns) == [ID, "hru_elev"]          # renamed from `mean`, id kept
    assert elev["hru_elev"].tolist() == [10.0, 20.0, 30.0, 40.0]
    assert "std" not in elev.columns                         # provenance never exported


def test_alternative_lulc_entry_is_not_read_even_when_its_file_is_absent(env):
    # nalcms is export:false and has no merged file here; the run must not need it
    assert not (env[0] / "nhm_lulc_nalcms_params.csv").exists()
    _run(env)


def test_integer_typed_parameters_are_written_as_integers(env):
    _run(env)
    text = (env[1] / "soil_type.csv").read_text().splitlines()
    assert text[1:] == ["1,1", "2,2", "3,3", "4,2"]          # not 1.0 / 2.0


def test_non_integral_value_in_an_integer_parameter_raises(env):
    _write_merged(env[0], **{"nhm_soils_params.csv": pd.DataFrame({ID: _ids(), "soils": [1.0, 2.5, 3.0, 2.0]})})
    with pytest.raises(ValueError, match="non-integral"):
        _run(env)


def test_nan_in_an_exported_column_raises_and_names_the_fill_stage(env):
    _write_merged(env[0], **{"nhm_elevation_params.csv": pd.DataFrame(
        {ID: _ids(), "mean": [10.0, np.nan, 30.0, 40.0], "std": 1.0})})
    with pytest.raises(ValueError, match="fill stage"):
        _run(env)


def test_short_row_count_raises(env):
    _write_merged(env[0], **{"nhm_soils_params.csv": pd.DataFrame({ID: _ids()[:3], "soils": [1.0, 2.0, 3.0]})})
    with pytest.raises(ValueError, match="rows, expected"):
        _run(env)


def test_duplicate_ids_raise(env):
    _write_merged(env[0], **{"nhm_soils_params.csv": pd.DataFrame(
        {ID: [1, 1, 3, 4], "soils": [1.0, 2.0, 3.0, 2.0]})})
    with pytest.raises(ValueError, match="duplicate"):
        _run(env)


def test_files_disagreeing_on_the_id_set_raise(env):
    _write_merged(env[0], **{"nhm_soils_params.csv": pd.DataFrame(
        {ID: [1, 2, 3, 9], "soils": [1.0, 2.0, 3.0, 2.0]})})
    with pytest.raises(ValueError, match="disagree"):
        _run(env)


def test_a_missing_merged_file_for_an_exportable_entry_raises_with_no_partial_output(env):
    (env[0] / "nhm_soils_params.csv").unlink()
    with pytest.raises(FileNotFoundError, match="soil_type"):
        _run(env)
    assert not env[1].exists() or not list(env[1].glob("*.csv"))   # nothing half-written


def test_alias_group_with_both_columns_present_raises(env):
    _write_merged(env[0], **{"nhm_lulc_nhm_v11_params.csv": pd.DataFrame(
        {ID: _ids(), "retention": 0.5, "rad_trncf": 0.5})})
    with pytest.raises(ValueError, match="exactly one of"):
        _run(env)


def test_alias_group_accepts_the_older_retention_vintage(env):
    _write_merged(env[0], **{"nhm_lulc_nhm_v11_params.csv": pd.DataFrame(
        {ID: _ids(), "retention": [0.1, 0.2, 0.3, 0.4]})})
    _run(env)
    assert pd.read_csv(env[1] / "rad_trncf.csv")["rad_trncf"].tolist() == [0.1, 0.2, 0.3, 0.4]


def test_snarea_curve_is_flat_ascending_in_deplcrv_order(env):
    _run(env)
    curve = pd.read_csv(env[1] / "snarea_curve.csv")
    assert list(curve.columns) == ["ndeplval_index", "snarea_curve"]
    assert curve["ndeplval_index"].tolist() == list(range(1, 23))
    expected = np.concatenate([np.linspace(0.0, 1.0, 11), np.linspace(0.0, 1.0, 11) * 0.5])
    np.testing.assert_allclose(curve["snarea_curve"].to_numpy(), expected)


def test_hru_deplcrv_beyond_the_library_raises(env):
    _write_merged(env[0], **{"nhm_snarea_curve_params.csv": pd.DataFrame(
        {ID: _ids(), "hru_deplcrv": [1, 2, 3, 2], **{c: 0.0 for c in CURVE_COLS}})})
    with pytest.raises(ValueError, match="hru_deplcrv"):
        _run(env)


def test_rerun_is_byte_identical(env):
    _run(env)
    first = {p.name: p.read_bytes() for p in env[1].glob("*.csv")}
    _run(env)
    assert {p.name: p.read_bytes() for p in env[1].glob("*.csv")} == first
    assert not list(env[1].glob("*.tmp"))


def test_stale_files_are_warned_about_not_deleted(env, caplog):
    env[1].mkdir()
    (env[1] / "old_param.csv").write_text("x\n")
    with caplog.at_level(logging.WARNING, logger="prms_export"):
        _run(env)
    assert (env[1] / "old_param.csv").exists()
    assert "old_param.csv" in caplog.text


def test_unfilled_directory_is_never_read(env):
    unfilled = env[0] / "_unfilled"
    unfilled.mkdir()
    pd.DataFrame({ID: _ids(), "soils": [np.nan] * N}).to_csv(unfilled / "nhm_soils_params.csv", index=False)
    _run(env)   # reads merged/ only; the NaN copy in _unfilled/ is irrelevant
