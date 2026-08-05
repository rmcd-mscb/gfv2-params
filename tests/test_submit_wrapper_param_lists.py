"""The submit wrappers retype the config's `name:` lists; nothing else checks them.

``slurm_batch/submit_zonal_params.sh`` does NOT read the YAML -- it carries a hardcoded
bash array (``PARAMS``) and exports ``PARAM=<name>`` per element, and
``scripts/derive_zonal_params.py`` then does a flat linear scan for a matching ``name:``.
``submit_depstor_params.sh`` has the same shape with ``FRACTIONS``.

So a param added to the YAML and forgotten in the array is silently NOT RUN, and the
wrapper still exits 0. This test is the only thing that catches that.

DELETE ME when the Snakemake migration retires the wrappers -- a Snakefile reads the YAML
directly, so the duplication (and this guard) disappear. See
``docs/superpowers/specs/2026-08-04-snakemake-migration-design.md``.

Pure text + ``yaml.safe_load``: no geo imports, no data root, so it is CI-safe and
head-node-safe.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _bash_array(script: Path, name: str) -> list[str]:
    """Extract a ``NAME=( ... )`` bash array, dropping comments and blank lines."""
    text = script.read_text()
    match = re.search(rf"^{name}=\((.*?)^\)", text, re.S | re.M)
    assert match, f"could not find a {name}=( ... ) array in {script}"
    return [
        line.split("#")[0].strip()
        for line in match.group(1).strip().splitlines()
        if line.split("#")[0].strip()
    ]


@pytest.mark.parametrize(
    ("script_rel", "array_name", "config_rel", "config_key"),
    [
        (
            "slurm_batch/submit_zonal_params.sh",
            "PARAMS",
            "configs/zonal/zonal_params.yml",
            "params",
        ),
        (
            "slurm_batch/submit_depstor_params.sh",
            "FRACTIONS",
            "configs/depstor/depstor_params.yml",
            "fractions",
        ),
    ],
)
def test_wrapper_array_matches_config_names(script_rel, array_name, config_rel, config_key):
    script = _REPO_ROOT / script_rel
    config = _REPO_ROOT / config_rel
    if not script.exists():
        pytest.skip(f"{script_rel} retired by the Snakemake migration -- delete this test")

    from_bash = _bash_array(script, array_name)
    from_yaml = [entry["name"] for entry in yaml.safe_load(config.read_text())[config_key]]

    assert from_bash == from_yaml, (
        f"{script_rel}'s {array_name} array has drifted from {config_rel}'s "
        f"`{config_key}:` names.\n"
        f"  bash: {from_bash}\n"
        f"  yaml: {from_yaml}\n"
        f"Order matters as well as membership: the wrapper submits in array order, and "
        f"ssflux must follow slope because it reads the merged slope CSV at zonal time."
    )
