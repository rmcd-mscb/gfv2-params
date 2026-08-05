"""Every parameter with a canonical ``merged/`` output, and what it declares.

Moved here from ``scripts/merge_and_fill_params.py`` so the declaration is
importable by the index generator (``scripts/build_parameter_index.py``) and by
the two guards, not only by the fill sweep.

Pure YAML: only the static ``name`` / ``merged_file`` / ``output_file`` /
``fill_columns`` / ``fabric_columns`` / ``prms`` fields are read, none of which
are ``{data_root}``/``{fabric}``-templated. That keeps this import safe with no
data root -- the CI contract recorded at ``scripts/merge_and_fill_params.py``'s
config-path block: tests may parse these files but must not touch a real data
root.

Import-safe by construction: ``src/gfv2_params/__init__.py`` holds only
``__version__``, so ``import gfv2_params.params_index`` pulls in no geo
libraries.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import yaml

# Repo-relative config paths consumed by `load_declared_params`. A bare
# yaml.safe_load is enough -- no need for gfv2_params.config.load_config's
# fabric-profile resolution, which is what keeps this read data-root-free.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ZONAL_PARAMS_CONFIG = _REPO_ROOT / "configs" / "zonal" / "zonal_params.yml"
DEPSTOR_PARAMS_CONFIG = _REPO_ROOT / "configs" / "depstor" / "depstor_params.yml"
SNAREA_LIBRARY_CONFIG = _REPO_ROOT / "configs" / "snarea" / "snarea_library.yml"


class DeclaredParam(NamedTuple):
    """One config entry's contract: what file, what may be filled, what PRMS calls it.

    A NamedTuple rather than a bare tuple because this record has now been widened
    THREE times (`fill_columns`, then `fabric_columns`, then `prms`) and each
    widening has to reach several consumption sites. The `fabric_columns` widening
    missed one of them -- `warn_undeclared_merged_files`, whose
    `for _, merged_file, _ in ...` then raised `ValueError: too many values to
    unpack` in all-params mode, the DEFAULT and the only mode
    `slurm_batch/merge_and_fill_params.batch` runs. It aborted `main()` outside
    `run_fill_sweep`'s per-param guard, so nothing was filled at all, while the test
    suite stayed green because its fixtures hand-built the OLD 3-tuple shape -- test
    and implementation wrong in the same direction, agreeing with each other.

    Attribute access cannot drift that way: a fifth field is invisible to every
    consumer that does not ask for it. `prms` carries a default so positional
    construction in existing tests keeps working; tuple-EQUALITY assertions do NOT
    survive a widening and must be rewritten to attribute access.

    `prms` shape (see configs/*/: every declared entry carries one)::

        prms:
          builder: zonal_runners/ssflux.py    # module path, for the by-builder view
          columns:                            # emitted column -> PRMS parameter
            gwflow_coef: {prms: gwflow_coef, processes: [PRMSGroundwater]}
          defects: {}                         # emitted column that does NOT represent
                                              # its PRMS parameter (see below)
          provenance:                         # emitted column that is not a PRMS param
            k_perm_wtd: litho-weighted permeability, flux-normalisation input

    `processes` is PER-COLUMN, not per-entry: `ssflux` alone spans PRMSSoilzone,
    PRMSGroundwater and PRMSRunoff across different columns, so an entry-level key
    would report 5 non-runoff parameters as feeding runoff.

    `defects` is deliberately separate from `columns`: `params_for_process` never
    reads it, so a column that does not currently represent its PRMS parameter can
    never be returned as feeding a process, while the index generator can still
    render it as a DEFECTIVE row rather than silently omitting the warning.
    """

    name: str
    merged_file: str
    fill_columns: list
    fabric_columns: dict
    prms: dict = {}


def _load_yaml_doc(path: Path) -> dict:
    """Bare `yaml.safe_load` of a config file, no placeholder resolution."""
    doc = yaml.safe_load(path.read_text())
    return doc or {}


def iter_declared_params(
    zonal_cfg: dict, depstor_cfg: dict, snarea_cfg: dict | None = None
) -> list[DeclaredParam]:
    """Every param with a canonical `merged/` output, and what it declares fillable.

    Returns `DeclaredParam(name, merged_file, fill_columns, fabric_columns, prms)`
    records. `fill_columns` is KNN-interpolated; `fabric_columns` is copied verbatim
    from the fabric GDF (see `apply_fabric_columns`). Both default to empty for the
    params that declare neither -- today every param but `ssflux` has an empty
    `fabric_columns`.

    `zonal_cfg["params"]`, `depstor_cfg["means"]` and `depstor_cfg["constants"]` name
    their canonical file `merged_file`; `depstor_cfg["ratios"]` names it `output_file`
    instead (see `derive_depstor_params.py`'s `run_mean_finalize` / `run_ratios`).
    `depstor_cfg["fractions"]` is DELIBERATELY EXCLUDED: every fraction entry also
    happens to carry a `merged_file` key, but that key names the per-fraction COUNT
    csv written to `merged/_intermediates/` (`run_merge`), never a `merged/` output --
    fractions are intermediates, not consumer-facing params, and are never filled.

    `constants` are per-HRU params a depstor BUILDER writes directly with no zonal
    pass; `--mode copy_constants` copies them into `merged/` so the "everything a
    consumer needs is in merged/" invariant holds.

    `snarea_cfg` is optional and does NOT share either config's list-of-entries shape
    -- the whole document IS the entry, so a `prms:` block there is a top-level key.
    `snarea_curve` is not a `zonal_params.yml` entry at all: it comes from the
    separate 3-stage SNODAS pipeline (`configs/snarea/snarea_library.yml`, Stage 3),
    a flat single-param config whose `params_file` names the real
    `nhm_snarea_curve_params.csv`. A synthetic `snarea_curve` entry could not live in
    `zonal_params.yml` instead: `slurm_batch/submit_zonal_params.sh` does not read the
    YAML -- it carries a hardcoded `PARAMS` bash array mirroring that `params:` list --
    and `tests/test_submit_wrapper_param_lists.py` requires the two to match. So a
    phantom entry would have to be added to the array too, and the wrapper would then
    submit a SLURM array job for an entry with no `source_raster`/`script:`. Hence the
    separate optional argument rather than a fourth zonal-shaped list.
    """

    def _record(name: str, merged_file: str, entry: dict) -> DeclaredParam:
        return DeclaredParam(
            name=name,
            merged_file=merged_file,
            fill_columns=list(entry.get("fill_columns") or []),
            fabric_columns=dict(entry.get("fabric_columns") or {}),
            prms=dict(entry.get("prms") or {}),
        )

    declared: list[DeclaredParam] = []

    for entry in zonal_cfg.get("params", []) or []:
        merged_file = entry.get("merged_file")
        if merged_file:
            declared.append(_record(entry["name"], merged_file, entry))

    for entry in depstor_cfg.get("means", []) or []:
        merged_file = entry.get("merged_file")
        if merged_file:
            declared.append(_record(entry["name"], merged_file, entry))

    for entry in depstor_cfg.get("ratios", []) or []:
        merged_file = entry.get("output_file")
        if merged_file:
            declared.append(_record(entry["name"], merged_file, entry))

    for entry in depstor_cfg.get("constants", []) or []:
        merged_file = entry.get("merged_file")
        if merged_file:
            declared.append(_record(entry["name"], merged_file, entry))

    if snarea_cfg:
        merged_file = snarea_cfg.get("params_file")
        if merged_file:
            declared.append(_record("snarea_curve", merged_file, snarea_cfg))

    return declared


def load_declared_params() -> list[DeclaredParam]:
    """`iter_declared_params` over the real in-repo configs."""
    return iter_declared_params(
        _load_yaml_doc(ZONAL_PARAMS_CONFIG),
        _load_yaml_doc(DEPSTOR_PARAMS_CONFIG),
        _load_yaml_doc(SNAREA_LIBRARY_CONFIG),
    )


def params_for_process(
    process: str, declared: list[DeclaredParam] | None = None
) -> list[tuple[str, DeclaredParam]]:
    """Every (emitted_column, DeclaredParam) whose column feeds `process`.

    Column-grained, not entry-grained: `ssflux` alone spans PRMSSoilzone,
    PRMSGroundwater and PRMSRunoff across different columns, so returning whole
    entries would report 5 non-runoff parameters as feeding runoff.

    Reads `prms.columns` ONLY -- never `prms.defects`. A defective column is one
    that is supposed to be the PRMS parameter and currently is not (aspect's `mean`
    is an arithmetic mean of a circular variable), so returning it here would hand a
    caller a broken column under a correct-looking name. The index generator reads
    `defects` separately and renders it as a DEFECTIVE row, which is the opposite of
    silently including it.
    """
    out: list[tuple[str, DeclaredParam]] = []
    for d in declared if declared is not None else load_declared_params():
        for col, spec in (d.prms.get("columns") or {}).items():
            if process in (spec.get("processes") or []):
                out.append((col, d))
    return out
