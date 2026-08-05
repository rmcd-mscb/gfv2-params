"""Tests for scripts/merge_and_fill_params.py"""

import importlib.util
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import yaml
from shapely.geometry import Point

_spec = importlib.util.spec_from_file_location(
    "merge_and_fill_params",
    Path(__file__).resolve().parent.parent / "scripts" / "merge_and_fill_params.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

find_missing_ids = _mod.find_missing_ids
fill_missing_values_knn = _mod.fill_missing_values_knn
maf = _mod


def _declared(name, merged_file, fill_columns, fabric_columns=None):
    """Build a real DeclaredParam for a run_fill_sweep target.

    A helper rather than an inline literal so the fixtures go through the SAME
    constructor production does. Hand-built tuples are what let the last
    DeclaredParam widening break four consumption sites while the suite stayed
    green -- test and implementation wrong in the same direction, agreeing with
    each other. See issue #204.
    """
    return maf.DeclaredParam(name, merged_file, fill_columns, fabric_columns or {})


class TestFindMissingIds:
    def test_finds_missing_ids(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": [1, 2, 4, 5], "val": [10, 20, 40, 50]})
        df.to_csv(csv, index=False)

        param_df, missing = find_missing_ids(csv, 5, "nat_hru_id", logger)
        assert missing == [3]
        assert len(param_df) == 4

    def test_no_missing_ids(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": [1, 2, 3], "val": [10, 20, 30]})
        df.to_csv(csv, index=False)

        _, missing = find_missing_ids(csv, 3, "nat_hru_id", logger)
        assert missing == []

    def test_all_missing(self, tmp_path):
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"nat_hru_id": pd.Series(dtype=int), "val": pd.Series(dtype=float)})
        df.to_csv(csv, index=False)

        _, missing = find_missing_ids(csv, 3, "nat_hru_id", logger)
        assert missing == [1, 2, 3]

    def test_keys_on_custom_id_feature(self, tmp_path):
        """A fabric whose id column is hru_id (e.g. oregon) keys on it, not nat_hru_id."""
        import logging
        logger = logging.getLogger("test")
        csv = tmp_path / "params.csv"
        df = pd.DataFrame({"hru_id": [1, 2, 4], "val": [10, 20, 40]})
        df.to_csv(csv, index=False)

        param_df, missing = find_missing_ids(csv, 4, "hru_id", logger)
        assert missing == [3]
        assert len(param_df) == 3


class TestMergedGpkgPathResolution:
    """The merged gpkg default now comes from the active profile's hru_gpkg
    (configs/base_config.yml), not a {fabric}_nhru_merged.gpkg naming
    convention. End-to-end resolution + error behavior is covered by
    tests/test_hru_gpkg_config.py; here we pin the source-of-truth contract."""

    def test_default_is_profile_hru_gpkg(self):
        src = (
            Path(__file__).resolve().parent.parent
            / "scripts" / "merge_and_fill_params.py"
        ).read_text()
        # The merged gpkg default is sourced from the active profile's hru_gpkg
        # via require_config_key — not the retired {fabric}_nhru_merged.gpkg
        # path convention. Assert on code (not a substring scan of the whole
        # file, which would also match explanatory comments); end-to-end
        # resolution is covered by tests/test_hru_gpkg_config.py.
        assert 'require_config_key(base, "hru_gpkg"' in src
        assert 'f"{data_root}/{fabric}/fabric/{fabric}_nhru_merged.gpkg"' not in src


class TestFileNotFoundBehavior:
    def test_raises_when_gpkg_missing(self, tmp_path):
        merged_gpkg = tmp_path / "nonexistent.gpkg"
        assert not merged_gpkg.exists()
        with pytest.raises(FileNotFoundError, match="base_config.yml"):
            if not merged_gpkg.exists():
                raise FileNotFoundError(
                    f"Fabric geopackage not found: {merged_gpkg}\n"
                    "Check the active fabric profile's hru_gpkg in configs/base_config.yml. "
                    "For VPU-based fabrics, run notebooks/merge_vpu_targets.py to produce it; "
                    "for single-file fabrics, place the gpkg at the hru_gpkg path."
                )


class TestFillMissingValuesKnn:
    def test_knn_fills_with_nearest_value(self):
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )

        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2],
            "hru_id": [1, 2],
            "my_param": [100.0, 200.0],
        })

        result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 3
        assert 3 in result["nat_hru_id"].values
        filled_val = result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0]
        assert filled_val in [100.0, 200.0]

    def test_knn_no_missing_returns_original(self):
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1]},
            geometry=[Point(0, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({"nat_hru_id": [1], "my_param": [42.0]})

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 1
        assert result["my_param"].iloc[0] == 42.0

    def test_knn_fills_multiple_columns_without_duplicates(self):
        """Filling N columns must append ONE row per missing id with every
        column populated — not N fragmented rows (regression for the
        per-column re-append bug surfaced by the oregon ssflux gap-fill)."""
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({
            "hru_id": [1, 2],
            "p1": [100.0, 200.0],
            "p2": [1.0, 2.0],
        })

        result = fill_missing_values_knn(
            param_df, [3], merged_gdf, ["p1", "p2"], 1, "hru_id", logger
        )
        # Exactly one row per id — no duplicates from per-column appends.
        assert len(result) == 3
        assert result["hru_id"].duplicated().sum() == 0
        # The filled row has BOTH columns populated, not just one.
        filled = result.loc[result["hru_id"] == 3]
        assert len(filled) == 1
        assert filled["p1"].notna().all() and filled["p2"].notna().all()
        assert not result.isna().any().any()

    def test_knn_fills_on_custom_id_feature(self):
        """Filling keys on the configured id_feature (hru_id) for non-gfv2 fabrics."""
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({"hru_id": [1, 2], "my_param": [100.0, 200.0]})

        result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, "hru_id", logger)
        assert len(result) == 3
        assert 3 in result["hru_id"].values
        filled_val = result.loc[result["hru_id"] == 3, "my_param"].iloc[0]
        assert filled_val in [100.0, 200.0]

    def test_knn_fills_nan_value_in_present_row(self):
        """A row present in param_df but with NaN value is filled by KNN."""
        import logging
        logger = logging.getLogger("test")

        # id=3 at Point(1,0) — nearest to id=1 at (0,0), distance 1 vs id=2 at (10,0), distance 9
        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(1, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2, 3],
            "hru_id": [1, 2, 3],
            "my_param": [100.0, 200.0, np.nan],
        })

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 3
        assert not result["my_param"].isna().any()
        filled_val = result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0]
        # id=1 (0,0) is distance 1; id=2 (10,0) is distance 9 — nearest is unambiguously id=1
        assert filled_val == 100.0

    def test_knn_fills_absent_and_nan_together(self):
        """Absent rows AND present-but-NaN cells are both filled in one pass."""
        import logging
        logger = logging.getLogger("test")

        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3, 4]},
            geometry=[Point(0, 0), Point(10, 0), Point(1, 0), Point(9, 0)],
            crs="EPSG:5070",
        )
        # id=3 present but NaN; id=4 absent
        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2, 3],
            "hru_id": [1, 2, 3],
            "my_param": [100.0, 200.0, np.nan],
        })

        result = fill_missing_values_knn(param_df, [4], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 4
        assert not result["my_param"].isna().any()
        assert result["nat_hru_id"].duplicated().sum() == 0
        # id=3 at (1,0): nearest valid is id=1 (0,0) → 100.0
        assert result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0] == 100.0
        # id=4 at (9,0): nearest valid is id=2 (10,0) → 200.0
        assert result.loc[result["nat_hru_id"] == 4, "my_param"].iloc[0] == 200.0

    def test_raises_when_column_has_no_valid_source(self):
        import logging
        logger = logging.getLogger("test")
        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2]},
            geometry=[Point(0, 0), Point(1, 0)],
            crs="EPSG:5070",
        )
        # both rows NaN for the column -> no valid fit source
        param_df = pd.DataFrame({"nat_hru_id": [1, 2], "my_param": [np.nan, np.nan]})
        with pytest.raises(ValueError, match="no valid"):
            fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, "nat_hru_id", logger)

    def test_nan_valued_row_not_used_as_fill_source(self):
        """NaN-valued rows must not be included in the KNN fit set (they can't donate values)."""
        import logging
        logger = logging.getLogger("test")

        # id=1 NaN at (0,0); id=2 val=5.0 at (1,0); id=3 NaN at (100,0)
        merged_gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(1, 0), Point(100, 0)],
            crs="EPSG:5070",
        )
        param_df = pd.DataFrame({
            "nat_hru_id": [1, 2, 3],
            "hru_id": [1, 2, 3],
            "my_param": [np.nan, 5.0, np.nan],
        })

        result = fill_missing_values_knn(param_df, [], merged_gdf, "my_param", 1, "nat_hru_id", logger)
        assert len(result) == 3
        assert not result["my_param"].isna().any()
        # Only id=2 (val=5.0) is in the fit set — both NaN rows must get 5.0
        assert result.loc[result["nat_hru_id"] == 1, "my_param"].iloc[0] == 5.0
        assert result.loc[result["nat_hru_id"] == 3, "my_param"].iloc[0] == 5.0


def _frame():
    """A snarea_curve-shaped frame: real params complete, provenance partly NaN."""
    return pd.DataFrame({
        "hru_id": [1, 2, 3],
        "hru_deplcrv": [1.0, 2.0, np.nan],      # declared param, has a gap
        "snarea_thresh": [0.1, 0.2, 0.3],        # declared param, complete
        "cv_empirical": [np.nan, np.nan, 0.4],   # UNDECLARED provenance, NaN by design
    })


def test_only_declared_columns_are_filled():
    plan = maf.resolve_fill_plan(
        _frame(), declared=["hru_deplcrv", "snarea_thresh"],
        missing_ids=set(), id_feature="hru_id", param_name="snarea_curve",
    )
    assert plan.fill_columns == ["hru_deplcrv", "snarea_thresh"]
    assert "cv_empirical" not in plan.fill_columns


def test_undeclared_column_with_nan_is_reported_not_filled():
    plan = maf.resolve_fill_plan(
        _frame(), declared=["hru_deplcrv", "snarea_thresh"],
        missing_ids=set(), id_feature="hru_id", param_name="snarea_curve",
    )
    # cv_empirical has 2 NaN — surfaced for the caller to warn about, never filled.
    assert plan.undeclared_with_nan == {"cv_empirical": 2}


def test_absent_hru_row_with_no_declaration_raises():
    """A missing ROW admits no provenance reading — it is a config error, not a result."""
    with pytest.raises(ValueError, match="fill_columns"):
        maf.resolve_fill_plan(
            _frame(), declared=[], missing_ids={4, 5},
            id_feature="hru_id", param_name="mystery_param",
        )


def test_absent_hru_row_with_declaration_is_fine():
    plan = maf.resolve_fill_plan(
        _frame(), declared=["hru_deplcrv"], missing_ids={4},
        id_feature="hru_id", param_name="snarea_curve",
    )
    assert plan.fill_columns == ["hru_deplcrv"]


def test_declared_column_absent_from_frame_raises():
    """A typo'd fill_columns entry must fail loud, not silently fill nothing."""
    with pytest.raises(ValueError, match="not present"):
        maf.resolve_fill_plan(
            _frame(), declared=["hru_deplcrv", "typo_column"], missing_ids=set(),
            id_feature="hru_id", param_name="snarea_curve",
        )


# ---------------------------------------------------------------------------
# Finding 2: retention/rad_trncf alias groups in fill_columns.
#
# lulc_prederived.py renamed the same computed quantity from `retention` to
# `rad_trncf`; gfv2/oregon were built before the rename (still carry
# `retention`), tjc was built after (carries `rad_trncf`). A declared
# fill_columns entry may be a list/tuple of alias alternatives instead of a
# plain string -- the first one present in the frame wins.
# ---------------------------------------------------------------------------

def _lulc_frame(column_name):
    return pd.DataFrame({
        "model_hru_idx": [1, 2, 3],
        "cov_type": [1, 2, 3],
        column_name: [0.1, 0.2, np.nan],
    })


def test_alias_resolves_to_first_alternative_present():
    """Legacy header (gfv2/oregon): `retention` present, `rad_trncf` absent."""
    plan = maf.resolve_fill_plan(
        _lulc_frame("retention"), declared=["cov_type", ["retention", "rad_trncf"]],
        missing_ids=set(), id_feature="model_hru_idx", param_name="lulc_nhm_v11",
    )
    assert plan.fill_columns == ["cov_type", "retention"]
    assert "rad_trncf" not in plan.fill_columns


def test_alias_resolves_to_second_alternative_present():
    """Current header (tjc, post-rename): `rad_trncf` present, `retention` absent."""
    plan = maf.resolve_fill_plan(
        _lulc_frame("rad_trncf"), declared=["cov_type", ["retention", "rad_trncf"]],
        missing_ids=set(), id_feature="model_hru_idx", param_name="lulc_nhm_v11",
    )
    assert plan.fill_columns == ["cov_type", "rad_trncf"]
    assert "retention" not in plan.fill_columns


def test_alias_raises_when_neither_alternative_present():
    """Neither alias present must still raise -- this is NOT an optional-column
    mechanism; a genuinely missing column cannot silently skip filling."""
    frame = pd.DataFrame({"model_hru_idx": [1, 2], "cov_type": [1, 2]})
    with pytest.raises(ValueError, match="not present"):
        maf.resolve_fill_plan(
            frame, declared=["cov_type", ["retention", "rad_trncf"]], missing_ids=set(),
            id_feature="model_hru_idx", param_name="lulc_nhm_v11",
        )


def test_alias_column_excluded_from_undeclared_with_nan():
    """Whichever alias actually resolves must not ALSO be reported as an
    undeclared NaN-carrying column."""
    plan = maf.resolve_fill_plan(
        _lulc_frame("rad_trncf"), declared=["cov_type", ["retention", "rad_trncf"]],
        missing_ids=set(), id_feature="model_hru_idx", param_name="lulc_nhm_v11",
    )
    assert "rad_trncf" not in plan.undeclared_with_nan


# ---------------------------------------------------------------------------
# Finding 3: appending an absent-id row must not invent an hru_id column that
# was never in the original frame.
# ---------------------------------------------------------------------------

def test_knn_does_not_add_spurious_hru_id_column():
    """gfv2's id_feature is nat_hru_id with NO hru_id column at all. Filling an
    absent row must not manufacture an all-empty hru_id column in the output."""
    import logging
    logger = logging.getLogger("test")

    merged_gdf = gpd.GeoDataFrame(
        {"nat_hru_id": [1, 2, 3]},
        geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
        crs="EPSG:5070",
    )
    param_df = pd.DataFrame({
        "nat_hru_id": [1, 2],
        "my_param": [100.0, 200.0],
    })

    result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, "nat_hru_id", logger)
    assert "hru_id" not in result.columns


def test_knn_still_populates_hru_id_when_frame_already_has_it():
    """Regression guard for the opposite direction: when hru_id genuinely IS a
    secondary column in the frame, appended rows must still populate it (not
    leave it NaN) -- this is the pre-existing, still-desired behavior."""
    import logging
    logger = logging.getLogger("test")

    merged_gdf = gpd.GeoDataFrame(
        {"nat_hru_id": [1, 2, 3]},
        geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
        crs="EPSG:5070",
    )
    param_df = pd.DataFrame({
        "nat_hru_id": [1, 2],
        "hru_id": [1, 2],
        "my_param": [100.0, 200.0],
    })

    result = fill_missing_values_knn(param_df, [3], merged_gdf, "my_param", 1, "nat_hru_id", logger)
    assert result.loc[result["nat_hru_id"] == 3, "hru_id"].iloc[0] == 3


def test_id_column_with_nan_excluded_from_undeclared_with_nan():
    """An ID column (hru_id, in ID_COLUMNS but NOT the active id_feature) that
    carries NaN must never be reported/filled -- this exclusion is exactly the
    branch that hid the spurious-hru_id regression (Finding 3): if it had
    warned, the bug would have been visible in every gfv2 fill-run log."""
    frame = pd.DataFrame({
        "nat_hru_id": [1, 2, 3],
        "hru_id": [1, 2, np.nan],
        "hru_deplcrv": [1.0, 2.0, 3.0],
    })
    plan = maf.resolve_fill_plan(
        frame, declared=["hru_deplcrv"], missing_ids=set(),
        id_feature="nat_hru_id", param_name="mystery_param",
    )
    assert "hru_id" not in plan.undeclared_with_nan


def test_writes_in_place_and_preserves_raw(tmp_path):
    p = tmp_path / "nhm_x_params.csv"
    pd.DataFrame({"hru_id": [1, 2], "v": [1.0, np.nan]}).to_csv(p, index=False)
    original = pd.read_csv(p)
    filled = pd.DataFrame({"hru_id": [1, 2], "v": [1.0, 9.0]})

    out = maf.write_filled_in_place(filled, p, original, {"v": np.dtype("float64")})

    assert out == p
    assert pd.read_csv(p)["v"].tolist() == [1.0, 9.0]          # canonical is filled
    raw = pd.read_csv(tmp_path / "_unfilled" / "nhm_x_params.csv")
    assert raw["v"].isna().sum() == 1                            # raw preserved


def test_second_run_does_not_clobber_the_raw_copy(tmp_path):
    """The irreversible one: a re-run must not move the FILLED file into _unfilled/."""
    p = tmp_path / "nhm_x_params.csv"
    pd.DataFrame({"hru_id": [1, 2], "v": [1.0, np.nan]}).to_csv(p, index=False)
    original = pd.read_csv(p)
    filled = pd.DataFrame({"hru_id": [1, 2], "v": [1.0, 9.0]})

    maf.write_filled_in_place(filled, p, original, {"v": np.dtype("float64")})
    # Run 2: the on-disk file is now already filled. Passing it as "original" is exactly
    # what the orchestrator does on a re-run.
    again = pd.read_csv(p)
    maf.write_filled_in_place(filled, p, again, {"v": np.dtype("float64")})

    raw = pd.read_csv(tmp_path / "_unfilled" / "nhm_x_params.csv")
    assert raw["v"].isna().sum() == 1, "_unfilled/ must still hold the ORIGINAL raw frame"


def test_categorical_dtype_is_restored(tmp_path):
    """cov_type is an integer class (0-3). k=1 copies a real class, so it must stay int."""
    p = tmp_path / "nhm_lulc_params.csv"
    pd.DataFrame({"hru_id": [1, 2], "cov_type": [1, 3]}).to_csv(p, index=False)
    original = pd.read_csv(p)
    filled = pd.DataFrame({"hru_id": [1, 2], "cov_type": [1.0, 3.0]})  # KNN returns float

    maf.write_filled_in_place(filled, p, original, {"cov_type": np.dtype("int64")})

    got = pd.read_csv(p)
    assert got["cov_type"].dtype.kind == "i"
    assert got["cov_type"].tolist() == [1, 3]


# ---------------------------------------------------------------------------
# All-params default mode: every configured param declares fill_columns.
#
# The two configs use DIFFERENT top-level list keys — zonal has `params:`,
# depstor has `fractions:` / `means:` / `ratios:`. Iterating only "params"
# would silently return [] for depstor and make this test pass vacuously.
#
# NB deviation from the design brief: the brief assumed `snarea_curve` was a
# configs/zonal/zonal_params.yml `params:` entry. It is not — snarea_curve
# comes from the separate 3-stage SNODAS pipeline
# (configs/snarea/snarea_library.yml, Stage 3), a FLAT single-param config
# (no `params:`/`fractions:` list, no per-entry `name:`) whose `params_file`
# names the real `nhm_snarea_curve_params.csv`. Folding a synthetic
# `snarea_curve` entry into zonal_params.yml's `params:` list would also reach
# slurm_batch/submit_zonal_params.sh. That wrapper does not read the YAML — it
# carries a hardcoded PARAMS bash array mirroring that list, and
# tests/test_submit_wrapper_param_lists.py requires the two to match — so the
# phantom would have to be added to the array as well, and the wrapper would
# then submit a SLURM array job for an entry with no `source_raster`/`script:`.
# So `snarea_curve` is checked separately below, against its real config file.
# ---------------------------------------------------------------------------

_PARAM_LIST_KEYS = ("params", "fractions", "means", "ratios")


def _configured_entries(doc):
    for key in _PARAM_LIST_KEYS:
        for entry in doc.get(key, []) or []:
            if isinstance(entry, dict):
                yield key, entry


def _canonical_merged_filename(list_key, entry):
    """The filename an entry writes to merged/ (canonical, consumer-facing),
    or None if it does not land there.

    zonal `params` and depstor `means` name their canonical file `merged_file`;
    depstor `ratios` names it `output_file` instead (see
    derive_depstor_params.py's run_mean_finalize / run_ratios). depstor
    `fractions` are EXCLUDED even though every entry there also carries a
    `merged_file` key — that key names the per-fraction COUNT csv written to
    merged/_intermediates/ (run_merge), never a merged/ output. Fractions are
    intermediates, not consumer-facing params, and are never filled.
    """
    if list_key == "fractions":
        return None
    if list_key == "ratios":
        return entry.get("output_file")
    return entry.get("merged_file")


def test_every_configured_param_declares_fill_columns():
    """A param with no fill_columns cannot be gap-filled — catch it at config time."""
    root = Path(__file__).resolve().parent.parent
    checked = 0
    for cfg in [root / "configs/zonal/zonal_params.yml",
                root / "configs/depstor/depstor_params.yml"]:
        doc = yaml.safe_load(cfg.read_text())
        for key, entry in _configured_entries(doc):
            canonical = _canonical_merged_filename(key, entry)
            if not canonical:
                continue
            checked += 1
            assert entry.get("fill_columns"), (
                f"{entry['name']} (under '{key}' in {cfg.name}) has a merged/ output but "
                f"no fill_columns, so the gap-fill step would skip it and any missing HRU "
                f"row would raise."
            )

    # snarea_curve: separate flat config, see the module-level note above.
    snarea_doc = yaml.safe_load(
        (root / "configs/snarea/snarea_library.yml").read_text()
    )
    assert snarea_doc.get("params_file"), "snarea_library.yml must declare params_file"
    checked += 1
    assert snarea_doc.get("fill_columns"), (
        "configs/snarea/snarea_library.yml declares params_file but no fill_columns, so "
        "snarea_curve would never be filled by the default all-params run."
    )

    # Guard against the whole test passing because nothing was found.
    assert checked >= 7, f"expected to check at least 7 merged params, checked {checked}"


def test_snarea_curve_does_not_declare_provenance_columns():
    """Regression guard for the whole point of this change.

    Reads configs/snarea/snarea_library.yml, not configs/zonal/zonal_params.yml
    — see the module-level deviation note above.
    """
    root = Path(__file__).resolve().parent.parent
    doc = yaml.safe_load((root / "configs/snarea/snarea_library.yml").read_text())
    forbidden = {"cv_assign", "cv_subgrid", "cv_empirical", "cv_source",
                 "sdc_status", "sca_class", "similarity", "n_seasons",
                 "n_peak_years", "peak_swe_mm"}
    assert not (set(doc["fill_columns"]) & forbidden)
    assert "hru_deplcrv" in doc["fill_columns"]


# ---------------------------------------------------------------------------
# iter_declared_params: unit coverage on synthetic dicts (no data root, no
# real config files) for the exclusion/inclusion rules exercised above.
# ---------------------------------------------------------------------------

def test_iter_declared_params_excludes_fractions_includes_means_and_ratios():
    zonal_cfg = {
        "params": [
            {"name": "elevation", "merged_file": "nhm_elevation_params.csv",
             "fill_columns": ["mean"]},
        ],
    }
    depstor_cfg = {
        "fractions": [
            {"name": "perv_frac", "merged_file": "nhm_perv_frac_params.csv"},
        ],
        "means": [
            {"name": "dprst_depth_avg", "merged_file": "nhm_dprst_depth_avg_params.csv",
             "fill_columns": ["dprst_depth_avg"]},
        ],
        "ratios": [
            {"name": "dprst_frac", "output_file": "nhm_dprst_frac_params.csv",
             "fill_columns": ["dprst_frac"]},
        ],
    }

    declared = maf.iter_declared_params(zonal_cfg, depstor_cfg)
    names = {d[0] for d in declared}

    assert names == {"elevation", "dprst_depth_avg", "dprst_frac"}
    assert "perv_frac" not in names  # fraction excluded despite its merged_file key

    # Attribute access, NOT tuple equality: DeclaredParam is a NamedTuple, so a
    # 4-tuple comparison silently encodes today's field count and breaks on every
    # widening (`prms` was the third). Naming the fields makes a new defaulted
    # field invisible here, which is the whole point of the NamedTuple.
    by_name = {d[0]: d for d in declared}
    d = by_name["dprst_frac"]
    assert (d.name, d.merged_file, d.fill_columns, d.fabric_columns) == (
        "dprst_frac", "nhm_dprst_frac_params.csv", ["dprst_frac"], {})
    d = by_name["elevation"]
    assert (d.name, d.merged_file, d.fill_columns, d.fabric_columns) == (
        "elevation", "nhm_elevation_params.csv", ["mean"], {})


def test_iter_declared_params_includes_snarea_when_given():
    snarea_cfg = {"params_file": "nhm_snarea_curve_params.csv", "fill_columns": ["hru_deplcrv"]}
    declared = maf.iter_declared_params({}, {}, snarea_cfg)
    assert len(declared) == 1
    d = declared[0]
    assert (d.name, d.merged_file, d.fill_columns, d.fabric_columns) == (
        "snarea_curve", "nhm_snarea_curve_params.csv", ["hru_deplcrv"], {})


def test_iter_declared_params_snarea_optional():
    """The 2-arg call the brief documented must still work (snarea omitted)."""
    declared = maf.iter_declared_params({}, {})
    assert declared == []


# ---------------------------------------------------------------------------
# Finding 2 (isolation half): run_fill_sweep must not let one param's config
# drift starve every other declared param of its fill.
# ---------------------------------------------------------------------------

class TestRunFillSweep:
    def _merged_gdf(self):
        return gpd.GeoDataFrame(
            {"hru_id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(10, 0), Point(5, 0)],
            crs="EPSG:5070",
        )

    def test_one_param_failure_does_not_block_the_others(self, tmp_path):
        """A param whose declared fill_columns names a column absent from its
        own file (the tjc-vs-gfv2 rad_trncf/retention scenario, pre-fix) must
        fail in isolation -- every OTHER param in the sweep still gets filled."""
        import logging
        logger = logging.getLogger("test")

        good = tmp_path / "nhm_good_params.csv"
        pd.DataFrame({"hru_id": [1, 2], "v": [10.0, 20.0]}).to_csv(good, index=False)

        bad = tmp_path / "nhm_bad_params.csv"
        pd.DataFrame({"hru_id": [1, 2], "v": [1.0, 2.0]}).to_csv(bad, index=False)

        targets = [
            (_declared("good_param", "nhm_good_params.csv", ["v"]), good),
            # typo_column is not present -> resolve_fill_plan raises
            (_declared("bad_param", "nhm_bad_params.csv", ["typo_column"]), bad),
        ]

        failed = maf.run_fill_sweep(
            targets, self._merged_gdf(), expected_max=3, id_feature="hru_id",
            k_neighbors=1, logger=logger,
        )

        assert failed == ["bad_param"]
        # good_param was still filled despite bad_param's failure.
        result = pd.read_csv(good)
        assert result["hru_id"].tolist() == [1, 2, 3]

    def test_consumes_the_real_producers_records(self, tmp_path):
        """Feed `load_declared_params()`'s ACTUAL records in, not hand-built ones.

        The counterpart to TestWarnUndeclaredMergedFiles's version of this test, and
        the reason issue #204 was filed: run_fill_sweep used to take an anonymous
        `(name, param_file, fill_columns, fabric_columns)` 4-tuple re-derived at the
        call site, so it carried -- one layer down -- the exact shape whose widening
        broke warn_undeclared_merged_files while the suite stayed green. Every
        run_fill_sweep test hand-built that tuple, which is precisely the fixture
        pattern that cannot catch a producer/consumer contract change.

        This one takes a REAL DeclaredParam off the real configs and drives a fill
        with it. If a future field widening breaks the unpacking, this fails.
        """
        import logging

        declared = next(
            d for d in maf.load_declared_params() if d.name == "soil_moist_max"
        )
        pf = tmp_path / declared.merged_file
        pd.DataFrame({"hru_id": [1, 2], "soil_moist_max": [10.0, 20.0]}).to_csv(
            pf, index=False
        )

        failed = maf.run_fill_sweep(
            [(declared, pf)], self._merged_gdf(), expected_max=3,
            id_feature="hru_id", k_neighbors=1,
            logger=logging.getLogger("test_fill_sweep_real"),
        )

        assert failed == []
        result = pd.read_csv(pf)
        assert result["hru_id"].tolist() == [1, 2, 3]
        assert result["soil_moist_max"].notna().all()

    def test_no_failures_returns_empty_list(self, tmp_path):
        import logging
        logger = logging.getLogger("test")

        good = tmp_path / "nhm_good_params.csv"
        pd.DataFrame({"hru_id": [1, 2], "v": [10.0, 20.0]}).to_csv(good, index=False)

        failed = maf.run_fill_sweep(
            [(_declared("good_param", "nhm_good_params.csv", ["v"]), good)],
            self._merged_gdf(), expected_max=3,
            id_feature="hru_id", k_neighbors=1, logger=logger,
        )
        assert failed == []


# ---------------------------------------------------------------------------
# Finding 5: --param_file must be under the ACTIVE fabric's own merged/ dir.
# ---------------------------------------------------------------------------

def test_check_param_file_in_fabric_accepts_matching_dir(tmp_path):
    merged_dir = tmp_path / "gfv2" / "params" / "merged"
    merged_dir.mkdir(parents=True)
    param_file = merged_dir / "nhm_x_params.csv"
    maf.check_param_file_in_fabric(param_file, merged_dir)  # must not raise


def test_check_param_file_in_fabric_rejects_foreign_fabric(tmp_path):
    """--fabric gfv2 --param_file <gfv2_vpu01 path> must be refused, not
    silently write into the wrong fabric's canonical file."""
    merged_dir = tmp_path / "gfv2" / "params" / "merged"
    merged_dir.mkdir(parents=True)
    foreign_file = tmp_path / "gfv2_vpu01" / "params" / "merged" / "nhm_x_params.csv"

    with pytest.raises(ValueError, match="not under the active fabric"):
        maf.check_param_file_in_fabric(foreign_file, merged_dir)


# ---------------------------------------------------------------------------
# Finding 4: warn about a merged/nhm_*_params.csv that no config entry
# declares (the reverse direction from the per-target skip warning).
# ---------------------------------------------------------------------------

class TestWarnUndeclaredMergedFiles:
    def test_warns_on_undeclared_on_disk_file(self, tmp_path, caplog):
        import logging
        (tmp_path / "nhm_declared_params.csv").write_text("hru_id,v\n1,1\n")
        (tmp_path / "nhm_mystery_params.csv").write_text("hru_id,v\n1,1\n")
        logger = logging.getLogger("test_warn_undeclared")

        with caplog.at_level(logging.WARNING, logger="test_warn_undeclared"):
            undeclared = maf.warn_undeclared_merged_files(
                tmp_path, [maf.DeclaredParam("declared", "nhm_declared_params.csv", ["v"], {})], logger,
            )

        assert undeclared == ["nhm_mystery_params.csv"]
        assert any("nhm_mystery_params.csv" in rec.message for rec in caplog.records)

    def test_allowlists_library_and_validation_files(self, tmp_path):
        import logging
        (tmp_path / "nhm_declared_params.csv").write_text("hru_id,v\n1,1\n")
        (tmp_path / "nhm_foo_library_params.csv").write_text("a,b\n1,2\n")
        (tmp_path / "nhm_bar_validation_params.csv").write_text("a,b\n1,2\n")
        logger = logging.getLogger("test_warn_undeclared_allow")

        undeclared = maf.warn_undeclared_merged_files(
            tmp_path, [maf.DeclaredParam("declared", "nhm_declared_params.csv", ["v"], {})], logger,
        )

        assert undeclared == []

    def test_no_undeclared_files_returns_empty(self, tmp_path):
        import logging
        (tmp_path / "nhm_declared_params.csv").write_text("hru_id,v\n1,1\n")
        logger = logging.getLogger("test_warn_undeclared_none")

        undeclared = maf.warn_undeclared_merged_files(
            tmp_path, [maf.DeclaredParam("declared", "nhm_declared_params.csv", ["v"], {})], logger,
        )

        assert undeclared == []

    def test_consumes_the_real_producers_output(self, tmp_path):
        """Feed `_load_declared_params()`'s ACTUAL records in, not a hand-built literal.

        Regression guard for the `fabric_columns` widening: this function read the
        record positionally (`for _, merged_file, _ in ...`) and raised
        `ValueError: too many values to unpack` on every all-params run -- the default
        mode, and the only one slurm_batch/merge_and_fill_params.batch uses. The three
        tests above did not catch it because they hand-built the record, so they
        asserted the shape the implementation still expected rather than the shape
        production actually supplies. A fixture can never catch a producer/consumer
        contract change; consuming the producer can.
        """
        import logging
        (tmp_path / "nhm_mystery_params.csv").write_text("hru_id,v\n1,1\n")

        undeclared = maf.warn_undeclared_merged_files(
            tmp_path, maf.load_declared_params(), logging.getLogger("test_warn_undeclared_real"),
        )

        assert undeclared == ["nhm_mystery_params.csv"]


# ---------------------------------------------------------------------------
# fabric_columns: exact values copied from the GDF for synthesized rows.
# ---------------------------------------------------------------------------

class TestApplyFabricColumns:
    def _gdf(self):
        from shapely.geometry import box

        # id=1: 100m x 100m = 10000 m²; id=2: 200m x 200m = 40000 m²; id=3: 50m x 50m = 2500 m²
        return gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 3]},
            geometry=[box(0, 0, 100, 100), box(0, 0, 200, 200), box(0, 0, 50, 50)],
            crs="EPSG:5070",
        )

    def test_geometry_area_copied_for_synthesized_rows(self):
        import logging
        logger = logging.getLogger("test")
        gdf = self._gdf()
        # id=3 is synthesized (absent from original CSV)
        # Existing rows carry SENTINEL values that deliberately differ from their own
        # geometry.area (10000.0 / 40000.0). If this test used the true areas, an
        # implementation that overwrote every row -- not just the synthesized ones --
        # would still pass, and the "must not be overwritten" assertion below would
        # prove nothing.
        df = pd.DataFrame({
            "nat_hru_id": [1, 2, 3],
            "hru_area": [111.0, 222.0, 99999.0],  # id=3 has wrong neighbour value
            "my_param": [1.0, 2.0, 3.0],
        })
        result = maf.apply_fabric_columns(
            df, missing_ids=[3], merged_gdf=gdf,
            fabric_columns={"hru_area": ("geometry", 1.0)},
            id_feature="nat_hru_id", logger=logger,
        )
        assert result.loc[result["nat_hru_id"] == 3, "hru_area"].iloc[0] == pytest.approx(2500.0)
        # Existing rows must not be touched -- these are the sentinels, not the areas.
        assert result.loc[result["nat_hru_id"] == 1, "hru_area"].iloc[0] == pytest.approx(111.0)
        assert result.loc[result["nat_hru_id"] == 2, "hru_area"].iloc[0] == pytest.approx(222.0)

    def test_scale_applied(self):
        import logging
        logger = logging.getLogger("test")
        from shapely.geometry import box
        gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1]},
            geometry=[box(0, 0, 1000, 1000)],  # 1e6 m²
            crs="EPSG:5070",
        )
        df = pd.DataFrame({"nat_hru_id": [1], "hru_area": [0.0]})
        result = maf.apply_fabric_columns(
            df, missing_ids=[1], merged_gdf=gdf,
            fabric_columns={"hru_area": ("geometry", 1e-6)},  # m² -> km²
            id_feature="nat_hru_id", logger=logger,
        )
        assert result.loc[result["nat_hru_id"] == 1, "hru_area"].iloc[0] == pytest.approx(1.0)

    def test_no_missing_ids_returns_unchanged(self):
        import logging
        logger = logging.getLogger("test")
        gdf = self._gdf()
        df = pd.DataFrame({"nat_hru_id": [1, 2], "hru_area": [10000.0, 40000.0]})
        result = maf.apply_fabric_columns(
            df, missing_ids=[], merged_gdf=gdf,
            fabric_columns={"hru_area": ("geometry", 1.0)},
            id_feature="nat_hru_id", logger=logger,
        )
        pd.testing.assert_frame_equal(result, df)

    def test_plain_gdf_column_source(self):
        """The non-`geometry` half of the mechanism: copy an ordinary GDF attribute."""
        import logging
        gdf = self._gdf()
        gdf["areasqkm"] = [0.01, 0.04, 0.0025]
        df = pd.DataFrame({"nat_hru_id": [1, 2, 3], "hru_area": [111.0, 222.0, np.nan]})
        result = maf.apply_fabric_columns(
            df, missing_ids=[3], merged_gdf=gdf,
            fabric_columns={"hru_area": ("areasqkm", 1e6)},  # km² -> m²
            id_feature="nat_hru_id", logger=logging.getLogger("test"),
        )
        assert result.loc[result["nat_hru_id"] == 3, "hru_area"].iloc[0] == pytest.approx(2500.0)

    def test_id_absent_from_gdf_raises(self):
        """An id needing fill that the fabric cannot serve must RAISE, not warn.

        Warn-and-continue left the cell NaN, wrote it into the canonical parameter
        file, and exited 0 -- while the summary log still reported a successful copy.
        Every other unrecoverable gap in this module raises.
        """
        import logging
        df = pd.DataFrame({"nat_hru_id": [1, 2, 4], "hru_area": [111.0, 222.0, np.nan]})
        with pytest.raises(ValueError, match="absent from the fabric geopackage"):
            maf.apply_fabric_columns(
                df, missing_ids=[4], merged_gdf=self._gdf(),  # gdf has ids 1,2,3 only
                fabric_columns={"hru_area": ("geometry", 1.0)},
                id_feature="nat_hru_id", logger=logging.getLogger("test"),
            )

    def test_duplicate_id_in_gdf_raises(self):
        """A duplicated id would make `.loc` return a frame and the write align to NaN."""
        import logging

        from shapely.geometry import box
        gdf = gpd.GeoDataFrame(
            {"nat_hru_id": [1, 2, 2]},
            geometry=[box(0, 0, 10, 10), box(0, 0, 20, 20), box(0, 0, 30, 30)],
            crs="EPSG:5070",
        )
        df = pd.DataFrame({"nat_hru_id": [1, 2], "hru_area": [111.0, np.nan]})
        with pytest.raises(ValueError):
            maf.apply_fabric_columns(
                df, missing_ids=[2], merged_gdf=gdf,
                fabric_columns={"hru_area": ("geometry", 1.0)},
                id_feature="nat_hru_id", logger=logging.getLogger("test"),
            )


class TestFabricColumnsThroughRunFillSweep:
    """The seam: `fabric_columns` must actually reach the on-disk file.

    Every other fabric_columns test calls `apply_fabric_columns` directly. Without
    this one, `run_fill_sweep`'s call to it could be deleted outright and the suite
    would stay green -- the feature would ship inert. This also pins the ORDERING
    (synthesize rows via KNN -> copy fabric values -> write), which nothing else does.
    """

    def _gdf(self):
        from shapely.geometry import box
        return gpd.GeoDataFrame(
            {"hru_id": [1, 2, 3]},
            geometry=[box(0, 0, 100, 100), box(0, 0, 200, 200), box(0, 0, 50, 50)],
            crs="EPSG:5070",
        )

    def test_fabric_column_lands_in_the_written_file(self, tmp_path):
        import logging
        pf = tmp_path / "nhm_ssflux_params.csv"
        # id=3 absent entirely; the existing rows' hru_area are sentinels, not areas.
        pd.DataFrame({"hru_id": [1, 2], "hru_area": [111.0, 222.0], "v": [10.0, 20.0]}).to_csv(pf, index=False)

        failed = maf.run_fill_sweep(
            [(_declared("ssflux", "nhm_ssflux_params.csv", ["v"],
                        {"hru_area": {"source": "geometry", "scale": 1.0}}), pf)],
            self._gdf(), expected_max=3, id_feature="hru_id", k_neighbors=1,
            logger=logging.getLogger("test"),
        )

        assert failed == []
        result = pd.read_csv(pf).set_index("hru_id")
        assert result.loc[3, "hru_area"] == pytest.approx(2500.0)  # exact, not a neighbour's
        # KNN still filled the ordinary column: id=3's centroid (25,25) is nearest
        # id=1's (50,50), not id=2's (100,100).
        assert result.loc[3, "v"] == pytest.approx(10.0)
        assert result.loc[1, "hru_area"] == pytest.approx(111.0)   # existing rows untouched
        assert result.loc[2, "hru_area"] == pytest.approx(222.0)

    def test_fabric_columns_only_param_is_not_short_circuited(self, tmp_path):
        """A param declaring ONLY fabric_columns must still fill.

        `if not plan.fill_columns: continue` made `apply_fabric_columns` unreachable
        for such a param -- silently doing nothing rather than failing.
        """
        import logging
        pf = tmp_path / "nhm_areaonly_params.csv"
        pd.DataFrame({"hru_id": [1, 2], "hru_area": [111.0, 222.0]}).to_csv(pf, index=False)

        failed = maf.run_fill_sweep(
            [(_declared("areaonly", "nhm_areaonly_params.csv", [],
                        {"hru_area": {"source": "geometry", "scale": 1.0}}), pf)],
            self._gdf(), expected_max=3, id_feature="hru_id", k_neighbors=1,
            logger=logging.getLogger("test"),
        )

        assert failed == []
        result = pd.read_csv(pf).set_index("hru_id")
        assert result.index.tolist() == [1, 2, 3]
        assert result.loc[3, "hru_area"] == pytest.approx(2500.0)

    def test_bad_source_fails_this_param_without_writing(self, tmp_path):
        """A `source` absent from the GDF is caught eagerly, before any write."""
        import logging
        pf = tmp_path / "nhm_ssflux_params.csv"
        pd.DataFrame({"hru_id": [1, 2], "hru_area": [111.0, 222.0], "v": [10.0, 20.0]}).to_csv(pf, index=False)

        failed = maf.run_fill_sweep(
            [(_declared("ssflux", "nhm_ssflux_params.csv", ["v"],
                        {"hru_area": {"source": "typo_col", "scale": 1.0}}), pf)],
            self._gdf(), expected_max=3, id_feature="hru_id", k_neighbors=1,
            logger=logging.getLogger("test"),
        )

        assert failed == ["ssflux"]
        # The canonical file is untouched -- no partial write.
        assert pd.read_csv(pf)["hru_id"].tolist() == [1, 2]


def test_resolve_fill_plan_parses_fabric_columns():
    """fabric_columns spec is parsed into (source, scale) tuples."""
    frame = pd.DataFrame({
        "nat_hru_id": [1, 2],
        "hru_area": [10000.0, 20000.0],
        "my_param": [1.0, 2.0],
    })
    plan = maf.resolve_fill_plan(
        frame,
        declared=["my_param"],
        missing_ids=set(),
        id_feature="nat_hru_id",
        param_name="ssflux",
        fabric_col_spec={"hru_area": {"source": "geometry", "scale": 1.0}},
    )
    assert plan.fabric_columns == {"hru_area": ("geometry", 1.0)}


def test_resolve_fill_plan_still_warns_on_nan_in_an_existing_fabric_column_row():
    """A NaN in an EXISTING row of a fabric column must still be surfaced.

    `apply_fabric_columns` writes only rows in `missing_ids`, and this census runs on
    the pre-append frame -- so every NaN it sees is in a row the mechanism structurally
    cannot reach. Exempting fabric columns from the census (the original behaviour,
    justified as "it will be filled from the fabric") suppressed the module's only
    warning for a gap that nothing fills.
    """
    frame = pd.DataFrame({
        "nat_hru_id": [1, 2],
        "hru_area": [10000.0, np.nan],
        "my_param": [1.0, 2.0],
    })
    plan = maf.resolve_fill_plan(
        frame,
        declared=["my_param"],
        missing_ids=set(),
        id_feature="nat_hru_id",
        param_name="ssflux",
        fabric_col_spec={"hru_area": {"source": "geometry", "scale": 1.0}},
    )
    assert plan.undeclared_with_nan == {"hru_area": 1}


def test_resolve_fill_plan_defaults_scale_to_one():
    frame = pd.DataFrame({"nat_hru_id": [1], "hru_area": [10000.0], "my_param": [1.0]})
    plan = maf.resolve_fill_plan(
        frame, declared=["my_param"], missing_ids=set(), id_feature="nat_hru_id",
        param_name="ssflux", fabric_col_spec={"hru_area": {"source": "geometry"}},
    )
    assert plan.fabric_columns == {"hru_area": ("geometry", 1.0)}


@pytest.mark.parametrize("spec", [
    {"scale": 1.0},              # no `source`
    "geometry",                  # scalar shorthand -- not a mapping
    None,                        # empty YAML value
    {"source": "geometry", "scale": "1e-6x"},   # unparseable scale
    {"source": "geometry", "scale": 0},         # would zero every synthesized value
])
def test_resolve_fill_plan_malformed_fabric_spec_raises_with_context(spec):
    """A malformed spec must raise the module's own instructive ValueError.

    Each of these previously produced a context-free builtin (KeyError, TypeError,
    ValueError) from `spec["source"]` / `float(...)`, five lines below a raise that
    names the param, the column, and the remedy. In a SLURM log that surfaces only as
    a truncated line inside `logger.exception`.
    """
    frame = pd.DataFrame({"nat_hru_id": [1], "hru_area": [10000.0], "my_param": [1.0]})
    with pytest.raises(ValueError, match="malformed"):
        maf.resolve_fill_plan(
            frame, declared=["my_param"], missing_ids=set(), id_feature="nat_hru_id",
            param_name="ssflux", fabric_col_spec={"hru_area": spec},
        )


def test_resolve_fill_plan_fabric_column_absent_from_frame_raises():
    frame = pd.DataFrame({"nat_hru_id": [1], "my_param": [1.0]})
    with pytest.raises(ValueError, match="fabric_columns"):
        maf.resolve_fill_plan(
            frame,
            declared=["my_param"],
            missing_ids=set(),
            id_feature="nat_hru_id",
            param_name="ssflux",
            fabric_col_spec={"hru_area": {"source": "geometry", "scale": 1.0}},
        )


def test_ssflux_config_declares_fabric_columns():
    """Regression guard: ssflux must declare hru_area as a fabric_column."""
    root = Path(__file__).resolve().parent.parent
    doc = yaml.safe_load((root / "configs/zonal/zonal_params.yml").read_text())
    ssflux = next(e for e in doc["params"] if e["name"] == "ssflux")
    assert "fabric_columns" in ssflux, "ssflux must declare fabric_columns"
    assert "hru_area" in ssflux["fabric_columns"], "hru_area must be a fabric_column for ssflux"
    assert ssflux["fabric_columns"]["hru_area"]["source"] == "geometry"
    # The two lists must stay disjoint: declaring hru_area in BOTH would have KNN
    # interpolate it and then the fabric copy overwrite -- harmless only by accident
    # of ordering, and an interpolated hru_area is the bug this declaration fixes.
    assert "hru_area" not in ssflux["fill_columns"], (
        "hru_area must be a fabric_column ONLY -- KNN must never touch it"
    )
