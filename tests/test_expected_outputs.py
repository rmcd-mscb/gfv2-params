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
