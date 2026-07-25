# Parameter Gap-Fill Convention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `{fabric}/params/merged/nhm_*_params.csv` the single canonical per-HRU parameter set — always gap-filled — retire the `filled_` prefix, and never overwrite provenance columns.

**Architecture:** `scripts/merge_and_fill_params.py` gains a config-driven `fill_columns` declaration per param, writes in place, and moves the pre-fill copy to `merged/_unfilled/`. Undeclared columns are never filled, but never silently: a NaN cell warns, an absent HRU row raises. A separate one-time migration script converts existing `filled_*` products on both fabrics.

**Tech Stack:** Python 3.12, pandas 3.0.2, geopandas 1.1.3, scikit-learn (KNN), pytest, pixi.

**Spec:** [`docs/superpowers/specs/2026-07-25-param-fill-convention-design.md`](../specs/2026-07-25-param-fill-convention-design.md)

## Global Constraints

- **NEVER run `pytest` on the HPC head node** — concurrent geo-library imports trigger shared-FS metadata storms that hang it. Every test run goes through `srun`. Non-negotiable.
- All tests must run in CI with **no data root** (no `/caldera` mount) — synthetic frames and `tmp_path` only.
- Imports at the top of the file — ruff's default `E4` includes E402.
- No new dependencies.
- Run `pixi run -e dev pre-commit run --files <changed files>` before committing.
- Branch `feat/param-fill-convention` is already checked out. Do NOT create or switch branches.
- KNN fill stays `k=1` nearest-neighbour. This plan changes *what it runs against, where it writes, and what it refuses to touch* — never the interpolation itself.
- **`_unfilled/<name>.csv` is written ONLY if it does not already exist.** Violating this destroys the true pre-fill copy on a second run, irreversibly, on a filesystem with no version control.

---

## File Structure

**Modify:**
- `scripts/merge_and_fill_params.py` — fill-column selection, the guard, in-place write, `_unfilled/`, dtype restore, all-params default mode
- `configs/zonal/zonal_params.yml` — `fill_columns` per param
- `configs/depstor/depstor_params.yml` — `fill_columns` per param
- `src/gfv2_params/viz.py` — 7 hardcoded `filled_nhm_ssflux_params.csv` references
- `slurm_batch/RUNME.md` §5, `docs/ARCHITECTURE.md`, `slurm_batch/HPC_REFERENCE.md`
- `tests/test_merge_and_fill_params.py`

**Create:**
- `scripts/migrate_filled_params.py` — one-time migration, `--dry-run` default
- `tests/test_migrate_filled_params.py`

---

## Task 1: Fill-column selection and the guard

**Files:**
- Modify: `scripts/merge_and_fill_params.py`
- Test: `tests/test_merge_and_fill_params.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `resolve_fill_plan(param_df, declared, missing_ids, id_feature, param_name) -> FillPlan`
  - `FillPlan` — a dataclass with `fill_columns: list[str]`, `undeclared_with_nan: dict[str, int]`

**Why this is separate:** it is pure decision logic over a DataFrame, testable with no filesystem at all, and it is where the provenance-destroying bug would live.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_merge_and_fill_params.py` (put any new imports at the TOP of the file with the existing ones):

```python
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
```

The existing test file loads the script via `importlib`; reuse that loader and bind it to `maf`. If the file does not already expose such a handle, add near the top:

```python
_SPEC = importlib.util.spec_from_file_location(
    "merge_and_fill_params",
    Path(__file__).resolve().parent.parent / "scripts" / "merge_and_fill_params.py",
)
maf = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(maf)
```

- [ ] **Step 2: Run to verify they fail**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_merge_and_fill_params.py -q -k "declared or absent_hru"
```

Expected: `AttributeError: module 'merge_and_fill_params' has no attribute 'resolve_fill_plan'`.

- [ ] **Step 3: Implement `resolve_fill_plan`**

Add to `scripts/merge_and_fill_params.py`, above `fill_missing_values_knn`. Add `from dataclasses import dataclass, field` to the imports at the top.

```python
@dataclass
class FillPlan:
    """What a param's fill run may touch, and what it found but must not touch.

    `fill_columns` is exactly the caller's declared list, intersected with nothing —
    a declared column missing from the frame is an error, not a silent skip.
    `undeclared_with_nan` is the caller's warning material: columns carrying NaN that
    nobody declared fillable.
    """

    fill_columns: list[str] = field(default_factory=list)
    undeclared_with_nan: dict[str, int] = field(default_factory=dict)


# Columns that identify an HRU rather than parameterise it. Never fillable, and never
# reported as an undeclared gap.
ID_COLUMNS = {"hru_id", "nat_hru_id", "model_hru_idx", "vpu"}


def resolve_fill_plan(param_df, declared, missing_ids, id_feature, param_name) -> FillPlan:
    """Decide what to fill for one param, and what to surface without filling.

    The selection is DECLARATION-driven, not gap-driven, because "has a NaN" does not
    mean "is missing". `nhm_snarea_curve_params.csv` carries 7,891 rows with NaN, every
    one in a provenance column -- `cv_empirical` is derivable for only ~42% of HRUs BY
    DESIGN, and `cv_subgrid` exists to rescue the rest. Filling it would overwrite the
    record of how each curve was derived with interpolated noise. Before this function
    existed the column list was "everything except the id columns", which would have
    done exactly that.

    Two asymmetric guards, because the two kinds of gap differ in what they can mean:

      * an undeclared column with NaN CELLS is ambiguous -- it may be a legitimate
        "not derivable" -- so it is reported for the caller to WARN about;
      * an absent HRU ROW is not ambiguous. The HRU is in the file or it is not, and no
        provenance reading makes a missing HRU correct. With nothing declared fillable,
        that is a configuration error and RAISES.
    """
    declared = list(declared or [])

    absent = [c for c in declared if c not in param_df.columns]
    if absent:
        raise ValueError(
            f"`fill_columns` for '{param_name}' names {absent}, which are not present in "
            f"the parameter file (columns: {sorted(param_df.columns)}). Fix the config -- "
            f"a typo here would silently fill nothing."
        )

    if missing_ids and not declared:
        raise ValueError(
            f"'{param_name}' is missing {len(missing_ids)} HRU row(s) but declares no "
            f"`fill_columns`, so nothing would be filled and the gap would persist "
            f"unnoticed. An absent HRU row is unambiguous -- unlike a NaN cell, it cannot "
            f"be a legitimate 'not derivable' result. Add `fill_columns` for this param in "
            f"its config entry."
        )

    undeclared_with_nan = {}
    for col in param_df.columns:
        if col in declared or col in ID_COLUMNS or col == id_feature:
            continue
        n_nan = int(param_df[col].isna().sum())
        if n_nan:
            undeclared_with_nan[col] = n_nan

    return FillPlan(fill_columns=declared, undeclared_with_nan=undeclared_with_nan)
```

- [ ] **Step 4: Run to verify they pass**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_merge_and_fill_params.py -q
```

Expected: all pass, including the file's pre-existing tests.

- [ ] **Step 5: Lint and commit**

```bash
pixi run -e dev pre-commit run --files scripts/merge_and_fill_params.py tests/test_merge_and_fill_params.py
git add scripts/merge_and_fill_params.py tests/test_merge_and_fill_params.py
git commit -m "feat(params): declaration-driven fill selection with asymmetric guards

Column selection was 'everything except the id columns', which would overwrite
snarea_curve's provenance -- cv_empirical is NaN for ~58% of HRUs BY DESIGN
(cv_subgrid rescues them), and filling it replaces the record of how each curve
was derived with interpolated noise.

Selection is now declaration-driven. Undeclared columns are never filled but
never silently: a NaN cell is reported for a warning, an absent HRU row RAISES.
A row is either present or not -- no provenance reading makes a missing HRU
correct -- so that one is a config error, not a result."
```

---

## Task 2: In-place write, `_unfilled/`, idempotency, dtype restore

**Files:**
- Modify: `scripts/merge_and_fill_params.py`
- Test: `tests/test_merge_and_fill_params.py`

**Interfaces:**
- Consumes: `resolve_fill_plan`, `FillPlan` from Task 1.
- Produces: `write_filled_in_place(complete_df, param_file, original_df, dtypes) -> Path` — writes the filled frame to `param_file`, preserving the pre-fill copy at `param_file.parent / "_unfilled" / param_file.name`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_merge_and_fill_params.py`:

```python
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
```

- [ ] **Step 2: Run to verify they fail**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_merge_and_fill_params.py -q -k "in_place or clobber or categorical"
```

Expected: `AttributeError: ... has no attribute 'write_filled_in_place'`.

- [ ] **Step 3: Implement `write_filled_in_place`**

Add to `scripts/merge_and_fill_params.py`:

```python
UNFILLED_DIRNAME = "_unfilled"


def write_filled_in_place(complete_df, param_file, original_df, dtypes, logger=None):
    """Write the filled frame OVER `param_file`, preserving the pre-fill copy.

    `merged/<name>.csv` is the single canonical per-HRU parameter file -- always
    gap-filled -- so a consumer's rule is one line: read `merged/*.csv`. The retired
    `filled_` prefix required a consumer to know, per param AND per fabric, which of two
    files was authoritative; the set differed between gfv2 (2 filled) and oregon (4), and
    only `viz.py` encoded the rule.

    The pre-fill copy goes to `merged/_unfilled/`, mirroring `merged/_intermediates/`.

    IDEMPOTENCY, load-bearing: `_unfilled/<name>.csv` is written ONLY if it does not
    already exist. On a re-run the on-disk file is ALREADY filled, so overwriting the
    preserved copy with it would destroy the true raw version -- irreversibly, on a
    shared filesystem with no version control.

    `dtypes` restores each column's pre-fill dtype. KNN returns float64 even at k=1, which
    silently turned `cov_type` (an integer class 0-3) into 0.0/1.0/2.0/3.0. No averaging
    occurs at k=1, so the classes are correct -- but the dtype change is visible to any
    consumer that does not cast.
    """
    unfilled_dir = param_file.parent / UNFILLED_DIRNAME
    unfilled_dir.mkdir(parents=True, exist_ok=True)
    raw_copy = unfilled_dir / param_file.name

    if raw_copy.exists():
        if logger:
            logger.info(
                "  %s already preserved at %s — not overwriting (a re-run's input is "
                "already filled)", param_file.name, raw_copy,
            )
    else:
        original_df.to_csv(raw_copy, index=False)
        if logger:
            logger.info("  pre-fill copy preserved -> %s", raw_copy)

    out = complete_df.copy()
    for col, dt in (dtypes or {}).items():
        if col in out.columns and dt.kind in "iu" and out[col].notna().all():
            out[col] = out[col].round().astype(dt)
    out.to_csv(param_file, index=False)
    if logger:
        logger.info("  canonical parameter file written -> %s", param_file)
    return param_file
```

- [ ] **Step 4: Run to verify they pass**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_merge_and_fill_params.py -q
```

- [ ] **Step 5: Lint and commit**

```bash
pixi run -e dev pre-commit run --files scripts/merge_and_fill_params.py tests/test_merge_and_fill_params.py
git add scripts/merge_and_fill_params.py tests/test_merge_and_fill_params.py
git commit -m "feat(params): write the filled frame in place, preserving raw in _unfilled/

merged/<name>.csv becomes the single canonical per-HRU file so a consumer's
rule is 'read merged/*.csv'. Pre-fill copies go to merged/_unfilled/, mirroring
merged/_intermediates/.

_unfilled/<name>.csv is written ONLY if absent: on a re-run the on-disk file is
already filled, and overwriting the preserved copy with it would destroy the
true raw version irreversibly. Dedicated test.

Restores pre-fill dtypes -- KNN returns float64 even at k=1, which turned
cov_type from an integer class into 0.0/1.0/2.0/3.0."
```

---

## Task 3: All-params default mode and `fill_columns` config

**Files:**
- Modify: `scripts/merge_and_fill_params.py`, `configs/zonal/zonal_params.yml`, `configs/depstor/depstor_params.yml`
- Test: `tests/test_merge_and_fill_params.py`

**Interfaces:**
- Consumes: `resolve_fill_plan`, `write_filled_in_place`.
- Produces: `iter_declared_params(zonal_cfg, depstor_cfg) -> list[tuple[str, str, list[str]]]` returning `(param_name, merged_file, fill_columns)`.

- [ ] **Step 1: Add `fill_columns` to the configs**

In `configs/zonal/zonal_params.yml`, add a `fill_columns:` list to each param entry:

| param | `fill_columns` |
| --- | --- |
| `elevation` | the param's own value column(s) as they appear in its `merged_file` |
| `slope` | same |
| `aspect` | same |
| `soils` | same |
| `soil_moist_max` | `[soil_moist_max]` |
| `lulc_nhm_v11` | `[cov_type, srain_intcp, wrain_intcp, snow_intcp, covden_sum, covden_win, retention]` |
| `lulc_nalcms` | same 7 as `lulc_nhm_v11` |
| `lulc_nlcd`, `lulc_foresce` | same 7 (configured but inputs unstaged) |
| `ssflux` | `[soil2gw_max, ssr2gw_rate, fastcoef_lin, slowcoef_lin, gwflow_coef, dprst_seep_rate_open, dprst_flow_coef]` |
| `snarea_curve` | `[hru_deplcrv, snarea_thresh, snarea_curve_0 … snarea_curve_10]` — **13 entries**, and NOT `cv_assign`, `cv_subgrid`, `cv_empirical`, `cv_source`, `sdc_status`, `sca_class`, `similarity`, `n_seasons`, `n_peak_years`, `peak_swe_mm` |

Read each param's `merged_file` header on disk to get its exact value-column names rather than guessing — e.g.
`head -1 {data_root}/oregon/params/merged/nhm_elevation_params.csv`. If a param's file is
absent for every fabric, declare the columns its builder writes and say so in your report.

Add a comment above `snarea_curve`'s list:

```yaml
    # cv_* / sdc_status / sca_class / similarity / n_seasons / n_peak_years / peak_swe_mm
    # are DELIBERATELY absent: NaN there means "not derivable" (cv_empirical is derivable
    # for only ~42% of HRUs by design; cv_subgrid rescues the rest), which is a RESULT.
    # Filling them would replace the record of how each curve was derived with noise.
```

In `configs/depstor/depstor_params.yml`, add `fill_columns` to each of the 6 ratios plus `dprst_depth_avg`, each naming its single value column (e.g. `fill_columns: [dprst_frac]`).

- [ ] **Step 2: Write the failing tests**

```python
# The two configs use DIFFERENT top-level list keys — zonal has `params:`, depstor has
# `fractions:` / `means:` / `ratios:`. Iterating only "params" would silently return []
# for depstor and make this test pass vacuously.
_PARAM_LIST_KEYS = ("params", "fractions", "means", "ratios")


def _configured_entries(doc):
    for key in _PARAM_LIST_KEYS:
        for entry in doc.get(key, []) or []:
            if isinstance(entry, dict):
                yield key, entry


def test_every_configured_param_declares_fill_columns():
    """A param with no fill_columns cannot be gap-filled — catch it at config time."""
    import yaml
    root = Path(__file__).resolve().parent.parent
    checked = 0
    for cfg in [root / "configs/zonal/zonal_params.yml",
                root / "configs/depstor/depstor_params.yml"]:
        doc = yaml.safe_load(cfg.read_text())
        for key, entry in _configured_entries(doc):
            # Only files landing in merged/ are canonical consumer-facing params;
            # depstor `fractions` go to merged/_intermediates/ and are not filled.
            if not entry.get("merged_file"):
                continue
            checked += 1
            assert entry.get("fill_columns"), (
                f"{entry['name']} (under '{key}' in {cfg.name}) has a merged_file but no "
                f"fill_columns, so the gap-fill step would skip it and any missing HRU "
                f"row would raise."
            )
    # Guard against the whole test passing because nothing was found.
    assert checked >= 7, f"expected to check at least 7 merged params, checked {checked}"


def test_snarea_curve_does_not_declare_provenance_columns():
    """Regression guard for the whole point of this change."""
    import yaml
    root = Path(__file__).resolve().parent.parent
    doc = yaml.safe_load((root / "configs/zonal/zonal_params.yml").read_text())
    entry = next(e for e in doc["params"] if e["name"] == "snarea_curve")
    forbidden = {"cv_assign", "cv_subgrid", "cv_empirical", "cv_source",
                 "sdc_status", "sca_class", "similarity", "n_seasons",
                 "n_peak_years", "peak_swe_mm"}
    assert not (set(entry["fill_columns"]) & forbidden)
    assert "hru_deplcrv" in entry["fill_columns"]
```

If either config uses a top-level key other than `params:` for its entry list, adjust the
accessor to match and note it in your report.

- [ ] **Step 3: Run to verify they fail**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 \
  pixi run -e dev pytest tests/test_merge_and_fill_params.py -q -k "fill_columns or provenance"
```

- [ ] **Step 4: Implement all-params mode**

In `main()`, when `--param_file` is NOT given, iterate every declared param instead of defaulting to `nhm_ssflux_params.csv`. For each: resolve its merged file under `{data_root}/{fabric}/params/merged/`, skip with an INFO line if absent, otherwise run `resolve_fill_plan` → `fill_missing_values_knn` (declared columns only) → `write_filled_in_place`.

Emit `logger.warning` for every entry in `plan.undeclared_with_nan`:

```python
        for col, n in plan.undeclared_with_nan.items():
            logger.warning(
                "  %s: column '%s' has %d NaN cell(s) but is NOT declared in "
                "`fill_columns` — left untouched. If it is a PRMS parameter rather than "
                "provenance, add it to the config; if it is provenance, this is correct.",
                merged_file.name, col, n,
            )
```

Delete the `filled_param_file = output_dir / f"filled_{param_file.name}"` line and every use of it. Keep `--param_file` working for single-param runs, routed through the same three functions.

- [ ] **Step 5: Run the full suite**

```bash
srun -p cpu -A impd --mem=16G --time=00:30:00 pixi run -e dev pytest tests/ -q
```

Expected: all pass. If `tests/test_viz.py` fails on a missing `filled_nhm_ssflux_params.csv`, that is Task 4's job — note it and continue.

- [ ] **Step 6: Lint and commit**

```bash
pixi run -e dev pre-commit run --files scripts/merge_and_fill_params.py \
  configs/zonal/zonal_params.yml configs/depstor/depstor_params.yml tests/test_merge_and_fill_params.py
git add scripts/merge_and_fill_params.py configs/zonal/zonal_params.yml \
        configs/depstor/depstor_params.yml tests/test_merge_and_fill_params.py
git commit -m "feat(params): fill every declared param by default, not just ssflux

The step filled ONE file per invocation and hardcoded nhm_ssflux_params.csv, so
a param was filled only if someone remembered to pass --param_file for it. The
canonical set therefore differed per fabric: gfv2 had 2 filled files, oregon 4,
with nothing recording which was authoritative.

Every param declaring a merged_file now declares fill_columns, and the default
run covers all of them. Undeclared columns carrying NaN are warned about by
name so a missing declaration cannot hide."
```

---

## Task 4: Migration script and `viz.py`

**Files:**
- Create: `scripts/migrate_filled_params.py`, `tests/test_migrate_filled_params.py`
- Modify: `src/gfv2_params/viz.py`

**Interfaces:**
- Consumes: `UNFILLED_DIRNAME` from Task 2.
- Produces: `plan_migration(merged_dir) -> list[tuple[Path, Path, Path]]` returning `(filled_file, canonical_target, raw_target)`.

- [ ] **Step 1: Write the failing tests**

```python
def test_plan_migration_pairs_filled_with_canonical(tmp_path):
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "nhm_ssflux_params.csv").write_text("hru_id,v\n1,\n")
    (tmp_path / "nhm_slope_params.csv").write_text("hru_id,v\n1,2\n")   # no filled_ pair

    plan = mig.plan_migration(tmp_path)

    assert len(plan) == 1
    filled, canonical, raw = plan[0]
    assert filled.name == "filled_nhm_ssflux_params.csv"
    assert canonical.name == "nhm_ssflux_params.csv"
    assert raw == tmp_path / "_unfilled" / "nhm_ssflux_params.csv"


def test_migration_is_idempotent(tmp_path):
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "nhm_ssflux_params.csv").write_text("hru_id,v\n1,\n")

    mig.apply_migration(mig.plan_migration(tmp_path))
    raw_after_first = (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text()
    mig.apply_migration(mig.plan_migration(tmp_path))   # second run: nothing left to do

    assert (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text() == raw_after_first
    assert not (tmp_path / "filled_nhm_ssflux_params.csv").exists()
    assert (tmp_path / "nhm_ssflux_params.csv").read_text() == "hru_id,v\n1,1\n"


def test_migration_refuses_when_raw_already_preserved(tmp_path):
    """Never overwrite an existing _unfilled/ copy — that is the irreversible mistake."""
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "nhm_ssflux_params.csv").write_text("hru_id,v\n1,\n")
    (tmp_path / "_unfilled").mkdir()
    (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").write_text("ORIGINAL\n")

    mig.apply_migration(mig.plan_migration(tmp_path))

    assert (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text() == "ORIGINAL\n"
```

Bind `mig` with the same `importlib` loader pattern used for `maf`.

- [ ] **Step 2: Run to verify they fail**

```bash
srun -p cpu -A impd --mem=8G --time=00:15:00 pixi run -e dev pytest tests/test_migrate_filled_params.py -q
```

- [ ] **Step 3: Write the migration script**

Create `scripts/migrate_filled_params.py`. It must default to `--dry-run` and require an explicit `--apply`, print every move before making it, and never overwrite an existing `_unfilled/` entry. Reuse `UNFILLED_DIRNAME` by importing it from `merge_and_fill_params` if convenient, or redefine the literal `"_unfilled"` with a comment naming the source of truth.

`plan_migration(merged_dir)` globs `filled_*.csv`, pairs each with its canonical name (strip the `filled_` prefix), and returns the triples. `apply_migration(plan)` performs, per triple: move canonical → `_unfilled/` (skip if the target exists), then move filled → canonical.

- [ ] **Step 4: Point `viz.py` at the canonical files**

Replace all 7 occurrences of `filled_nhm_ssflux_params.csv` with `nhm_ssflux_params.csv` in `src/gfv2_params/viz.py`.

**`tests/test_viz.py:186` asserts the old filename** (`assert by_name[n].csv_name == "filled_nhm_ssflux_params.csv"`) and must be updated to `nhm_ssflux_params.csv` in the same commit — otherwise the suite fails and it looks like the repoint broke something.

Verify with:

```bash
grep -c "filled_" src/gfv2_params/viz.py tests/test_viz.py   # expect 0 for both
```

- [ ] **Step 5: Run the tests**

```bash
srun -p cpu -A impd --mem=16G --time=00:30:00 \
  pixi run -e dev pytest tests/test_migrate_filled_params.py tests/test_viz.py tests/test_merge_and_fill_params.py -q
```

- [ ] **Step 6: Dry-run the migration against BOTH real fabrics, and report the output**

```bash
DR=/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2
for f in gfv2 oregon; do
  echo "=== $f ==="
  pixi run --as-is python scripts/migrate_filled_params.py --merged_dir "$DR/$f/params/merged"
done
```

Expected: `gfv2` lists 2 moves (`ssflux`, `soil_moist_max`); `oregon` lists 4 (those plus `lulc_nalcms`, `lulc_nhm_v11`). **Do NOT pass `--apply`** — the controller decides when to migrate real products. Paste the dry-run output verbatim into your report.

- [ ] **Step 7: Lint and commit**

```bash
pixi run -e dev pre-commit run --files scripts/migrate_filled_params.py \
  tests/test_migrate_filled_params.py src/gfv2_params/viz.py
git add scripts/migrate_filled_params.py tests/test_migrate_filled_params.py src/gfv2_params/viz.py
git commit -m "feat(params): migration to the canonical convention + repoint viz.py

One-time, dry-run by default, --apply required. Per filled_X.csv: move X.csv to
_unfilled/X.csv (skipped if already preserved), then filled_X.csv to X.csv.

viz.py was the ONLY consumer encoding the filled_ convention -- 7 hardcoded
references to filled_nhm_ssflux_params.csv -- so it moves to the canonical name."
```

---

## Task 5: Documentation

**Files:**
- Modify: `slurm_batch/RUNME.md`, `docs/ARCHITECTURE.md`, `slurm_batch/HPC_REFERENCE.md`

- [ ] **Step 1: Rewrite RUNME §5**

It is currently three lines that say "KNN-fills any missing per-HRU parameter values" and show one `sbatch`. Replace with content covering: the step now fills **every** param declaring `fill_columns` (not just ssflux); `merged/*.csv` is canonical and always gap-filled, so **consumers read `merged/`**; pre-fill copies live in `merged/_unfilled/`; the run logs per-param filled counts and **warns** for undeclared columns carrying NaN; and that an absent HRU row in a param with no `fill_columns` **raises** rather than passing silently.

State plainly that the `filled_` prefix is retired, since anyone with an older checkout will look for it.

- [ ] **Step 2: Update `docs/ARCHITECTURE.md`**

Add `fill_columns` to the per-key required-field table, and document `merged/` (canonical, gap-filled) vs `merged/_unfilled/` (pre-fill copies) alongside the existing `merged/_intermediates/` description.

- [ ] **Step 3: Update `slurm_batch/HPC_REFERENCE.md`**

Wherever the param-file inventory names outputs, state that `merged/*.csv` is the canonical set.

- [ ] **Step 4: Verify no stale reference survives**

```bash
grep -rn "filled_nhm\|filled_" --include=*.md --include=*.py --include=*.yml \
  docs/ slurm_batch/ src/ scripts/ configs/ | grep -v superpowers/specs | grep -v superpowers/plans
```

Expected: only `_unfilled` matches and `merge_and_fill_params.py`'s own docstrings. Anything else is a stale reference to fix.

- [ ] **Step 5: Full suite, lint, commit**

```bash
srun -p cpu -A impd --mem=16G --time=00:30:00 pixi run -e dev pytest tests/ -q
pixi run -e dev pre-commit run --all-files
git add docs/ slurm_batch/
git commit -m "docs(params): document the canonical merged/ convention and gap-filling

RUNME §5 described the step in three lines and mentioned neither that it filled
ONE file per run nor that it emitted a filled_ prefix -- which is why four
params on oregon went unfilled until someone audited them by hand."
```

---

## Self-Review

**Spec coverage:**

| spec section | task |
| --- | --- |
| Consumer contract (`merged/` canonical) | 2 |
| `fill_columns` declaration | 3 |
| Guard: undeclared NaN warns | 1 (detection) + 3 (warning emission) |
| Guard: absent row raises | 1 |
| All-params default mode | 3 |
| In-place write + `_unfilled/` | 2 |
| Idempotency rule | 2 (+ 4 for the migration) |
| dtype restore | 2 |
| `viz.py` repoint | 4 |
| Migration script | 4 |
| Docs (RUNME/ARCHITECTURE/HPC_REFERENCE) | 5 |

No spec section is unimplemented. The spec's "out of scope" items (KNN behaviour, the
non-per-HRU snarea files, choosing between the two LULC products) have no task, correctly.

**Placeholder scan:** no TBD/TODO. Two steps deliberately require the implementer to read
real data rather than transcribe: Task 3 Step 1's exact value-column names per param
(guessing them would produce a `fill_columns` typo that Task 1's guard then raises on),
and Task 5's grep sweep. Both say so explicitly and both have a verification command.

**Type consistency:** `resolve_fill_plan` / `FillPlan.fill_columns` / `FillPlan.undeclared_with_nan`
are used identically in Tasks 1 and 3. `write_filled_in_place(complete_df, param_file,
original_df, dtypes, logger=None)` matches between Tasks 2 and 3. `UNFILLED_DIRNAME` is
defined in Task 2 and consumed in Task 4. `plan_migration` / `apply_migration` are
consistent within Task 4.

**Risk worth naming:** Task 4 Step 6 runs the migration against real products in dry-run
only. Applying it is a controller decision, taken after the dry-run output has been read —
it is the one step in this plan that can lose data.
