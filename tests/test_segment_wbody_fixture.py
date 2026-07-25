"""The positive-length rule, pinned against REAL oregon NHM + NHD geometry.

The synthetic tests in test_segment_wbody.py fix the rule's semantics; this fixes it
against the geometry it actually runs on — genuine shoreline grazes, a multi-part
waterbody, and NHD polygons that need make_valid. Cut from
`oregon/fabric/model_layers 9.gpkg` (nsegment) and `input/nhd/nhd_waterbodies.gpkg`;
see tests/data/README.md for the exact recipe.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from gfv2_params.segment_wbody import (
    segment_comid_frame,
    segment_waterbody_comids,
    segment_waterbody_pairs,
)

DATA = Path(__file__).resolve().parent / "data"
FIXTURE = DATA / "segment_wbody_oregon_fixture.gpkg"
CROSSWALK = DATA / "OR_waterbody_nseg_intersection.csv"

# Waterbodies a segment traverses with POSITIVE length -> on-stream. Includes a Playa
# (24032198) and an Ice Mass (120050246) on purpose: this builder is FTYPE-agnostic,
# and `wbody_connectivity`'s never-on-stream guardrail is what drops them.
EXPECTED_ONSTREAM = {23794331, 24032198, 120050246, 120052284, 120055365}

# Waterbodies a segment only GRAZES (zero-length intersection) -> depression storage.
EXPECTED_GRAZE_ONLY = {24032328, 24051153, 24067917, 24079105, 24079145, 24083423}


@pytest.fixture(scope="module")
def layers():
    seg = gpd.read_file(FIXTURE, layer="nsegment")
    wb = gpd.read_file(FIXTURE, layer="waterbodies")
    return seg, wb


def test_positive_length_rule_on_real_geometry(layers):
    seg, wb = layers
    pairs = segment_waterbody_pairs(seg, wb)
    assert segment_waterbody_comids(pairs) == EXPECTED_ONSTREAM


def test_graze_only_waterbodies_are_dropped(layers):
    seg, wb = layers
    pairs = segment_waterbody_pairs(seg, wb)
    intersecting = set(pairs["comid"].unique())
    # Bare `intersects` would promote all of these; positive length must not.
    assert EXPECTED_GRAZE_ONLY <= intersecting, "fixture must contain the graze pairs"
    assert not (EXPECTED_GRAZE_ONLY & segment_waterbody_comids(pairs))


def test_every_graze_pair_measures_exactly_zero(layers):
    seg, wb = layers
    pairs = segment_waterbody_pairs(seg, wb)
    grazes = pairs[pairs["comid"].isin(EXPECTED_GRAZE_ONLY)]
    assert not grazes.empty
    assert (grazes["overlap_m"] == 0).all()


def test_multi_segment_waterbody_aggregates(layers):
    seg, wb = layers
    frame = segment_comid_frame(segment_waterbody_pairs(seg, wb))
    row = frame[frame["comid"] == 23794331].iloc[0]
    assert row["n_segments"] > 1, "COMID 23794331 is crossed by many segments"
    assert row["overlap_m"] > 1000.0


def test_crosswalk_provenance_is_committed():
    # The handoff crosswalk is a versioned audit trail, NOT a reproduction target: it
    # was built with bare `intersects` against the retired conus_waterbodies.gpkg
    # (1,629 pairs / 783 COMIDs). Against nhd_waterbodies the same join gives 1,453
    # pairs; the delta is the layer swap and is documented in the design spec.
    cw = pd.read_csv(CROSSWALK)
    assert list(cw.columns) == ["wb_comid", "nseg_id"]
    assert len(cw) == 1629
    assert cw["wb_comid"].nunique() == 783
    # Every fixture COMID that the CSV also found must be present in it.
    assert EXPECTED_GRAZE_ONLY <= set(cw["wb_comid"])
