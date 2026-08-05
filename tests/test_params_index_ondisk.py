"""Guard 2: the declared column list must cover the on-disk header.

DATA-ROOT-GATED. CI (ubuntu-latest) has no data root, so every case SKIPS there and
CI reports green while proving nothing -- record this test's result by SLURM job id,
never infer it from a CI badge. See the companion spec's Testing section.

Guard 1 (tests/test_params_index.py) runs declaration -> declaration and is a
tautology on most entries; it cannot see disk, and therefore cannot catch the class
of defect it was written for. This is the guard that catches a new column reaching
disk with no PRMS decision recorded -- which is exactly how the hru_slope and
hru_aspect defects arose.

BOTH directions are checked, by two separate tests:

* disk -> declared: a column reached disk with no PRMS decision recorded.
* declared -> disk: `prms.columns` claims a PRMS parameter the file does not
  actually carry. The generated index renders every `prms.columns` entry as a fact
  about what is on disk, so without this the doc can advertise a parameter nobody
  emits. This is not hypothetical -- `hru_slope` is this PR's headline deliverable
  and, checked only one way, nothing anywhere verified it reached disk.

Alias groups are the reason the second direction is not simple equality: only one
alternative of lulc_nhm_v11's `retention` | `rad_trncf` exists on any given fabric,
so alias members are exempted. They are derived from the list-valued entries of
`fill_columns`, not hardcoded.

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

def _alias_members(declared) -> set[str]:
    """Column names that are alternatives of one another, not independent columns.

    A list-valued `fill_columns` entry IS the alias declaration -- lulc_nhm_v11's
    `[retention, rad_trncf]` marks the same computed quantity under two names across
    fabric vintages. Exactly one member is on disk for a given fabric, so the
    declared -> disk direction must exempt them. Derived, not hardcoded, so a new
    alias group needs no edit here.
    """
    return {
        alt
        for item in declared.fill_columns
        if isinstance(item, (list, tuple))
        for alt in item
    }


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

    undeclared = header - declared_cols - {id_feature}
    assert not undeclared, (
        f"{declared.name}: {sorted(undeclared)} exist in {path.name} but are declared "
        f"in none of prms.columns / prms.defects / prms.provenance. A new column "
        f"reached disk with no PRMS decision recorded -- that is exactly how the "
        f"hru_slope and hru_aspect defects arose."
    )


@pytest.mark.parametrize("declared", _DECLARED, ids=lambda d: d.name)
def test_guard2_declared_columns_are_present_on_disk(declared):
    """The converse direction: prms.columns must not claim a column nobody emits.

    scripts/build_parameter_index.py renders every prms.columns entry into
    docs/parameter_index.md as a statement about what is in merged/, so a declared
    column absent from disk means the published index advertises a PRMS parameter
    that does not exist.

    `hru_slope` is exactly why this matters. It is emitted by a `derived_columns:`
    block applied at merge time, so if the operator never re-ran the merge, or a
    future edit dropped the block, the config would still claim the column, the doc
    would still advertise it, and the disk -> declared direction would still pass.
    Nothing would notice. (There IS a loud path today via resolve_fill_plan, but
    only because hru_slope was also added to fill_columns -- a derived column that
    is not declared fillable would have no backstop at all.)
    """
    merged_dir, _ = _merged_dir_and_id()
    if not merged_dir.exists():
        pytest.skip(f"no data root on this host: {merged_dir}")

    path = merged_dir / declared.merged_file
    if not path.exists():
        pytest.skip(f"not built for this fabric: {path}")

    header = set(pd.read_csv(path, nrows=0).columns)
    required = set(declared.prms.get("columns") or {}) - _alias_members(declared)
    missing = required - header
    assert not missing, (
        f"{declared.name}: {sorted(missing)} are declared in prms.columns as PRMS "
        f"parameters but are NOT in {path.name}. Either the file needs rebuilding "
        f"(for a derived_columns output, re-run --mode merge for this param), or "
        f"the declaration is wrong. docs/parameter_index.md is currently "
        f"advertising a parameter that is not on disk."
    )

    # An alias group must contribute at least one member, or the parameter really
    # is absent and the exemption above would hide it.
    aliases = _alias_members(declared) & set(declared.prms.get("columns") or {})
    if aliases:
        assert aliases & header, (
            f"{declared.name}: none of the alias alternatives {sorted(aliases)} is "
            f"present in {path.name}. Exactly one is expected per fabric vintage."
        )


def test_guard2_is_gated_not_silently_empty():
    """Fail loudly if the parametrize list is empty.

    Every case above is allowed to skip, so an empty _DECLARED would make this whole
    module report green while asserting nothing -- the same vacuous-pass failure mode
    the `prms:`-is-mandatory rule exists to prevent one level up.
    """
    assert _DECLARED, "load_declared_params() returned nothing -- configs unreadable?"
