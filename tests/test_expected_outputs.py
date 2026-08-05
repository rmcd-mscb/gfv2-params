"""Guards `build_depstor_rasters._expected_outputs` against a missing step.

`_hydrate_existing_outputs` calls `_expected_outputs` for every step NOT in
the current run (i.e. every --step / --from invocation), so an omitted single-
output step name raises an unhandled KeyError before the orchestrator's own
error handling can run (see `single_key` dict in `_expected_outputs`). This
test reproduces that failure mode generically: every step declared in
configs/depstor/depstor_rasters.yml must map to a non-empty output dict.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from gfv2_params.depstor_builders import BUILDERS, STEP_ORDER
from scripts.build_depstor_rasters import _expected_outputs

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "configs" / "depstor" / "depstor_rasters.yml"


def test_every_configured_step_has_expected_outputs():
    config = yaml.safe_load(CONFIG_PATH.read_text())
    steps = config["steps"]
    assert steps, "depstor_rasters.yml has no steps configured"

    for step in steps:
        produced = _expected_outputs(step)
        assert isinstance(produced, dict) and produced, (
            f"_expected_outputs() returned no output keys for step "
            f"'{step['name']}' — every step in depstor_rasters.yml must be "
            f"mapped so --step/--from resume works without a KeyError."
        )


def test_config_steps_and_step_order_agree():
    """Every STEP_ORDER step must have a config block, and vice versa.

    The orchestrator raises on both directions now (`build_depstor_rasters.main`), but
    this catches the drift in CI at the moment someone edits either file, rather than on
    a CONUS run hours later.

    The dangerous direction is STEP_ORDER -> config. Before the guard, `ordered_steps`
    was built with `if n in step_index`, so a registered-but-unconfigured step was
    silently dropped and `_hydrate_existing_outputs` then served the PREVIOUS run's
    artifact for its output key -- a `--from dprst` rebuild would re-emit stale CONUS
    product at exit 0.

    Set equality, not list: the YAML deliberately lists steps in a different order from
    STEP_ORDER (`waterbody` and `segment_wbody` are swapped), which is fine because the
    orchestrator re-sorts by STEP_ORDER. Only membership is a contract.

    NB this assertion is correct for depstor and would be WRONG for shared_rasters,
    whose config deliberately omits the opt-in `compute_dem_derivatives` and
    `compute_breached_fdr` steps (docs/ARCHITECTURE.md:299-305).
    """
    config = yaml.safe_load(CONFIG_PATH.read_text())
    configured = {s["name"] for s in config["steps"]}

    assert configured == set(STEP_ORDER), (
        f"depstor_rasters.yml and STEP_ORDER disagree.\n"
        f"  registered but not configured (SILENTLY SKIPPED before the guard): "
        f"{sorted(set(STEP_ORDER) - configured)}\n"
        f"  configured but not registered (raises at startup): "
        f"{sorted(configured - set(STEP_ORDER))}"
    )


def test_every_step_order_entry_has_a_builder():
    """STEP_ORDER and BUILDERS must cover the same steps.

    `build_depstor_rasters` does `BUILDERS[name]`, so a STEP_ORDER entry with no builder
    raises KeyError -- but only after every prior step has already run, which at CONUS
    scale is hours of wasted compute. `gfv2_params.shared_rasters` has had this
    assertion since its orchestrator landed (tests/test_shared_rasters_orchestrator.py);
    the depstor package never got one.
    """
    assert set(STEP_ORDER) == set(BUILDERS.keys()), (
        f"STEP_ORDER without a builder: {sorted(set(STEP_ORDER) - set(BUILDERS))}; "
        f"builders not in STEP_ORDER: {sorted(set(BUILDERS) - set(STEP_ORDER))}"
    )
