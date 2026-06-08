"""Package-level invariants for ``gfv2_params.zonal_runners``.

Cheap tests (no fixtures, no geo work) that catch refactor regressions in the
package's structural properties — re-export wiring, dispatch table shape, and
the ``__file__``-relative path depth that ``lulc.py``'s crosswalk-resolution
branch depends on.
"""

from pathlib import Path

from gfv2_params.zonal_runners import (
    BATCH_RUNNERS,
    run_build_weights,
    run_lulc_batch,
    run_lulc_prederived_batch,
    run_merge,
    run_soils_batch,
    run_ssflux_batch,
    run_zonal_batch,
)


def test_public_reexports_resolve():
    """All 7 public re-exports + BATCH_RUNNERS must be callable / dict."""
    assert callable(run_zonal_batch)
    assert callable(run_soils_batch)
    assert callable(run_lulc_batch)
    assert callable(run_lulc_prederived_batch)
    assert callable(run_ssflux_batch)
    assert callable(run_build_weights)
    assert callable(run_merge)
    assert isinstance(BATCH_RUNNERS, dict)


def test_batch_runners_keys_and_identity():
    """BATCH_RUNNERS dispatch must match the 5 script: tags and the re-exports.

    Function identity (not just name) — guarantees the orchestrator's
    BATCH_RUNNERS[tag](...) call reaches the same function the test suite
    can import directly.
    """
    assert sorted(BATCH_RUNNERS) == ["lulc", "lulc_prederived", "soils", "ssflux", "zonal"]
    assert BATCH_RUNNERS["zonal"] is run_zonal_batch
    assert BATCH_RUNNERS["soils"] is run_soils_batch
    assert BATCH_RUNNERS["lulc"] is run_lulc_batch
    assert BATCH_RUNNERS["lulc_prederived"] is run_lulc_prederived_batch
    assert BATCH_RUNNERS["ssflux"] is run_ssflux_batch


def test_lulc_module_parents3_resolves_to_repo_root():
    """`lulc.py` resolves relative crosswalk paths via `parents[3]`.

    From `src/gfv2_params/zonal_runners/lulc.py`:
        parents[0] = src/gfv2_params/zonal_runners/
        parents[1] = src/gfv2_params/
        parents[2] = src/
        parents[3] = repo root

    If the file is ever moved deeper or shallower in the tree, the
    ``Path(__file__).resolve().parents[3] / crosswalk_path`` line in
    ``run_lulc_batch`` would silently resolve to a wrong directory and
    relative-crosswalk configs (every entry in zonal_params.yml's lulc_*
    blocks) would fail at runtime with FileNotFoundError on the crosswalk.

    This test exists to fail loudly at refactor time, not in production.
    """
    # Inline import so isort doesn't fragment the module-level import block
    # by sorting `lulc as lulc_mod` between BATCH_RUNNERS and the run_* names.
    from gfv2_params.zonal_runners import lulc as lulc_mod

    repo_root = Path(lulc_mod.__file__).resolve().parents[3]
    assert (repo_root / "pyproject.toml").exists(), (
        f"parents[3] from {lulc_mod.__file__!r} resolves to {repo_root!r}, "
        "which is not the repo root — relative-crosswalk-path resolution in "
        "run_lulc_batch will silently break. Update parents[N] to match the "
        "new file depth."
    )
