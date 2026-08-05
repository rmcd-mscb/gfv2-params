"""Unit coverage for gfv2_params.params_index.

Pure YAML parsing -- no geo imports, no data root. Safe on CI's ubuntu-latest.

The DISK-facing half of this contract lives in tests/test_params_index_ondisk.py,
which is data-root-gated and therefore SKIPS here. Nothing in this file can catch a
new column reaching disk with no PRMS decision recorded.
"""

from __future__ import annotations

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
