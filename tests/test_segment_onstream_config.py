"""Invariants on the REAL configs/base_config.yml for the segment-driven classifier.

Follows the precedent of tests/test_expected_outputs.py (which asserts against the real
depstor config): these are cheap, data-root-free checks that catch a profile drifting
back to the retired waterbody layer or silently re-enabling the NHD comparison union in
production.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
BASE_CONFIG = REPO_ROOT / "configs" / "base_config.yml"

# gfv2_vpu01 reads a fabric-local `wbs` layer, not a CONUS waterbody layer, and its
# depstor DAG deliberately fail-fasts (no COMID column). It is out of scope here.
_CONUS_WATERBODY_FABRICS = ("gfv2", "gfv2_dev", "oregon", "tjc")


def _fabrics() -> dict:
    return yaml.safe_load(BASE_CONFIG.read_text())["fabrics"]


def test_no_fabric_reads_the_retired_conus_waterbodies_layer():
    for name, profile in _fabrics().items():
        assert "conus_waterbodies.gpkg" not in str(profile.get("waterbody_gpkg", "")), (
            f"fabric '{name}' still reads the retired conus_waterbodies.gpkg. Its "
            f"shoreline vintage is poorly registered against NHM segment geometry "
            f"(19.0% zero-length grazes vs 3.1% on nhd_waterbodies)."
        )


def test_conus_waterbody_fabrics_all_read_nhd_waterbodies():
    fabrics = _fabrics()
    for name in _CONUS_WATERBODY_FABRICS:
        assert fabrics[name]["waterbody_gpkg"].endswith("input/nhd/nhd_waterbodies.gpkg"), (
            f"fabric '{name}' must read the current nhd_waterbodies.gpkg so every "
            f"fabric classifies against one shoreline vintage."
        )


def test_no_fabric_enables_the_nhd_comparison_union_by_default():
    # Presence of either key unions NHD flowline topology back into the on-stream set.
    # That is a deliberate A/B mode, never a production profile.
    for name, profile in _fabrics().items():
        for key in ("connected_comids_table", "flowthrough_comids_table"):
            assert key not in profile, (
                f"fabric '{name}' has an ACTIVE `{key}`, which unions NHD flowline "
                f"topology into the segment-derived on-stream set. Comment it out — it "
                f"is an opt-in comparison mode, not the production classifier."
            )


def test_gfv2_and_oregon_declare_an_onstream_floor():
    fabrics = _fabrics()
    assert fabrics["gfv2"]["min_onstream_comids"] == 30000      # measured 48,529
    assert fabrics["gfv2_dev"]["min_onstream_comids"] == 30000
    assert fabrics["oregon"]["min_onstream_comids"] == 500      # measured 770


def test_every_depstor_fabric_declares_segments():
    # segments_gpkg is now a REQUIRED depstor input, not a legacy leftover.
    fabrics = _fabrics()
    for name in _CONUS_WATERBODY_FABRICS:
        assert fabrics[name].get("segments_gpkg"), (
            f"fabric '{name}' has no segments_gpkg — the on-stream classifier "
            f"cannot run without the model routing network."
        )
