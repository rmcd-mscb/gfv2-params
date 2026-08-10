"""Guard 3: the canonical merged/ product must actually be the gap-filled one.

DATA-ROOT-GATED, exactly like Guard 2 (tests/test_params_index_ondisk.py). CI has no
data root, so every case SKIPS there and CI reports green while proving nothing --
record this test's result by SLURM job id, never infer it from a CI badge.

PR #189 retired the `filled_` prefix and made `merged/<name>.csv` the single
canonical, always-gap-filled per-HRU file, with the pre-fill copy preserved at
`merged/_unfilled/<name>.csv`. Nothing enforced that on disk, and the convention
silently rotted on two fabrics: until issue #211, gfv2 and tjc still carried the
pre-fill product under the canonical name with the real values stranded in
`filled_nhm_soil_moist_max_params.csv`. A consumer following the documented
convention read 2,009 NaN (gfv2) / 23 NaN (tjc) and had no way to know.

That is the class of defect these two tests catch:

* `test_no_legacy_filled_prefix_remains` -- a `filled_*` file in merged/ means some
  param is split across two names again, and the canonical one is the WRONG half.
* `test_fill_columns_have_no_nan_on_disk` -- a column the config declares fillable
  must contain no NaN once the fill sweep has run. This catches both a never-run
  sweep and a sweep that silently skipped a column.

Pure pandas + yaml: no rasterio/GDAL/pyogrio, so it is head-node safe -- but per
CLAUDE.md, run it under srun on a compute node anyway, not on the login node.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from gfv2_params.config import load_base_config
from gfv2_params.params_index import load_declared_params

_DECLARED = load_declared_params()


def _merged_dir() -> Path:
    base = load_base_config(None, fabric=None)
    return Path(base["data_root"]) / base["fabric"] / "params" / "merged"


def _fill_column_names(declared) -> list[str]:
    """Flatten `fill_columns`, expanding alias groups.

    A list-valued entry (lulc_nhm_v11's `[retention, rad_trncf]`) names alternatives
    of one another; exactly one is on disk for a given fabric. Expanding them here is
    safe because the NaN check below only inspects columns the file actually has.
    """
    names: list[str] = []
    for item in declared.fill_columns or []:
        if isinstance(item, (list, tuple)):
            names.extend(item)
        else:
            names.append(item)
    return names


def test_no_legacy_filled_prefix_remains():
    """No `filled_*` file may survive in merged/ -- see issue #211."""
    merged_dir = _merged_dir()
    if not merged_dir.exists():
        pytest.skip(f"no data root on this host: {merged_dir}")

    stray = sorted(p.name for p in merged_dir.glob("filled_*.csv"))
    assert not stray, (
        f"{stray} still use the retired `filled_` prefix in {merged_dir}. "
        "PR #189 made merged/<name>.csv the canonical gap-filled file, so a "
        "surviving filled_<name>.csv means the canonical one is the PRE-FILL half "
        "and consumers are silently reading NaN. Run "
        "scripts/migrate_filled_params.py --merged_dir <dir> (dry-run first). "
        "Do NOT migrate a param whose merged/<name>.csv was rebuilt more recently "
        "than its filled_ copy -- that would overwrite the rebuild with stale data."
    )


@pytest.mark.parametrize("declared", _DECLARED, ids=lambda d: d.name)
def test_fill_columns_have_no_nan_on_disk(declared):
    """Every declared-fillable column must be NaN-free in the canonical product."""
    merged_dir = _merged_dir()
    if not merged_dir.exists():
        pytest.skip(f"no data root on this host: {merged_dir}")

    path = merged_dir / declared.merged_file
    if not path.exists():
        pytest.skip(f"not built for this fabric: {path}")

    fill_cols = _fill_column_names(declared)
    if not fill_cols:
        pytest.skip(f"{declared.name} declares no fill_columns")

    # low_memory=False: these files are read whole anyway, and chunked dtype
    # inference otherwise warns on categorical columns whose values look numeric
    # in some chunks and not others (ssflux's `vpu`, where "01" infers as int 1 in
    # a numeric-only chunk). Irrelevant to the NaN check, but a guard should not
    # emit noise that trains readers to ignore its output.
    df = pd.read_csv(path, low_memory=False)
    present = [c for c in fill_cols if c in df.columns]
    if not present:
        pytest.skip(f"{declared.name}: none of {fill_cols} on disk yet")

    offenders = {c: int(df[c].isna().sum()) for c in present if df[c].isna().any()}
    assert not offenders, (
        f"{declared.name}: {offenders} NaN in declared-fillable column(s) of "
        f"{path.name}. merged/<name>.csv is the canonical ALREADY-GAP-FILLED "
        "product (PR #189), so NaN here means the fill sweep never ran for this "
        "param, or ran and skipped the column. Run "
        "`FABRIC=<fabric> sbatch slurm_batch/merge_and_fill_params.batch "
        f"--param_file <path to {path.name}>`."
    )
