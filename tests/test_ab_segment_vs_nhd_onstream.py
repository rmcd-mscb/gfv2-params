"""The A/B comparison arithmetic, isolated from any file or data root."""

from __future__ import annotations

from scripts.diagnose.ab_segment_vs_nhd_onstream import compare_onstream_sets


def test_compare_counts_each_direction():
    out = compare_onstream_sets(
        segment={1, 2, 3}, wbareacomi={2, 3, 4}, flowthrough={3, 5}
    )
    assert out["n_segment"] == 3
    assert out["n_wbareacomi"] == 3
    assert out["n_flowthrough"] == 2
    assert out["n_nhd_union"] == 4              # {2,3,4,5}
    assert out["n_shared"] == 2                 # {2,3}
    assert out["n_segment_only"] == 1           # {1}
    assert out["n_nhd_only"] == 2               # {4,5}


def test_compare_with_no_nhd_tables():
    out = compare_onstream_sets(segment={1, 2}, wbareacomi=set(), flowthrough=set())
    assert out["n_nhd_union"] == 0
    assert out["n_segment_only"] == 2
    assert out["n_shared"] == 0
