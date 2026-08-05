"""Guard 2: the declared column list must cover the on-disk header.

DATA-ROOT-GATED. CI (ubuntu-latest) has no data root, so every case SKIPS there and
CI reports green while proving nothing -- record this test's result by SLURM job id,
never infer it from a CI badge. See the companion spec's Testing section.

Guard 1 (tests/test_params_index.py) runs declaration -> declaration and is a
tautology on most entries; it cannot see disk, and therefore cannot catch the class
of defect it was written for. This is the guard that catches a new column reaching
disk with no PRMS decision recorded -- which is exactly how the hru_slope and
hru_aspect defects arose.

Direction matters: this asserts DISK is a subset of DECLARED, not equality. Alias
columns (lulc_nhm_v11's retention | rad_trncf) mean only one alternative is present
on any given fabric, so declared-but-absent is expected and fine. Absent-but-
undeclared is the failure.

Pure pandas + yaml: no rasterio/GDAL/pyogrio, so it is head-node safe -- but per
CLAUDE.md, run it under srun on a compute node anyway, not on the login node.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from gfv2_params.config import load_base_config, require_config_key
from gfv2_params.params_index import load_declared_params

_DECLARED = load_declared_params()

# Columns that identify an HRU rather than parameterise it. Mirrors
# scripts/merge_and_fill_params.py's ID_COLUMNS, duplicated rather than imported
# because that module pulls in geopandas/sklearn and this test must not. The
# active fabric's own id_feature is added on top: the four stages do not all agree
# on the id column name (gfv2's id_feature is nat_hru_id, but the depstor mean and
# snarea stages write hru_id), and an id column is never a PRMS parameter.
_ID_COLUMNS = {"hru_id", "nat_hru_id", "model_hru_idx", "vpu"}


def _merged_dir_and_id() -> tuple[Path, str]:
    base = load_base_config(None, fabric=None)
    merged_dir = Path(base["data_root"]) / base["fabric"] / "params" / "merged"
    id_feature = require_config_key(base, "id_feature", "test_params_index_ondisk")
    return merged_dir, str(id_feature)


@pytest.mark.parametrize("declared", _DECLARED, ids=lambda d: d.name)
def test_guard2_declared_columns_cover_disk(declared):
    merged_dir, id_feature = _merged_dir_and_id()
    if not merged_dir.exists():
        pytest.skip(f"no data root on this host: {merged_dir}")

    path = merged_dir / declared.merged_file
    if not path.exists():
        pytest.skip(f"not built for this fabric: {path}")

    header = set(pd.read_csv(path, nrows=0).columns)
    declared_cols = (
        set(declared.prms.get("columns") or {})
        | set(declared.prms.get("defects") or {})
        | set(declared.prms.get("provenance") or {})
    )

    undeclared = header - declared_cols - _ID_COLUMNS - {id_feature}
    assert not undeclared, (
        f"{declared.name}: {sorted(undeclared)} exist in {path.name} but are declared "
        f"in none of prms.columns / prms.defects / prms.provenance. A new column "
        f"reached disk with no PRMS decision recorded -- that is exactly how the "
        f"hru_slope and hru_aspect defects arose."
    )


def test_guard2_is_gated_not_silently_empty():
    """Fail loudly if the parametrize list is empty.

    Every case above is allowed to skip, so an empty _DECLARED would make this whole
    module report green while asserting nothing -- the same vacuous-pass failure mode
    the `prms:`-is-mandatory rule exists to prevent one level up.
    """
    assert _DECLARED, "load_declared_params() returned nothing -- configs unreadable?"
