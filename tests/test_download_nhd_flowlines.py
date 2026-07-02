"""Unit tests for the NHD flowline -> connected-waterbody-COMID distillation."""

import pandas as pd
import pytest

from gfv2_params.download.nhd_flowlines import (
    _base_url,
    _pick_snapshot_key,
    connected_comids_from_flowlines,
    read_flowline_attrs,
    write_connected_comids,
)


def test_pick_snapshot_key_highest_version_excludes_fgdb():
    # NHD snapshot version numbers vary per VPU (observed 04-09); pick the
    # highest NHDSnapshot_<NN>.7z and never the parallel NHDSnapshotFGDB archive
    # or other components.
    pre = "NHDPlusV21/Data/NHDPlusMS/NHDPlus11/NHDPlusV21_MS_11_"
    keys = [
        f"{pre}NHDSnapshotFGDB_06.7z",   # FGDB variant — must be ignored
        f"{pre}NHDSnapshot_05.7z",
        f"{pre}NHDSnapshot_06.7z",       # highest non-FGDB -> winner
        f"{pre}FdrFac_01.7z",            # other component — must be ignored
    ]
    assert _pick_snapshot_key(keys, "11") == f"{pre}NHDSnapshot_06.7z"


def test_pick_snapshot_key_none_when_absent():
    assert _pick_snapshot_key([], "11") is None
    assert _pick_snapshot_key(["x/NHDPlusV21_MS_10L_NHDSnapshot_06.7z"], "11") is None


def test_read_flowline_attrs_normalises_nhd_field_casing(tmp_path):
    import geopandas as gpd
    from shapely.geometry import LineString

    # NHD field-name casing varies across VPU snapshots: VPU 12 ships
    # COMID/WBAREACOMI, VPU 13 ships ComID/WBAreaComI. read_flowline_attrs must
    # resolve them case-insensitively and normalise to canonical names so the
    # per-VPU loop doesn't crash on the lower-cased VPUs.
    gdf = gpd.GeoDataFrame(
        {"ComID": [1, 2], "WBAreaComI": [100, 0]},
        geometry=[LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])],
        crs="EPSG:4269",
    )
    src = tmp_path / "NHDFlowline.gpkg"
    gdf.to_file(src, driver="GPKG")

    df = read_flowline_attrs(src)
    assert set(df.columns) == {"COMID", "WBAREACOMI"}
    assert connected_comids_from_flowlines(df) == {100}


def test_base_url_nested_vs_flat():
    # VPUs whose code contains a "nested" drainage code get the extra
    # /NHDPlus{vpu} path segment; others sit directly under /NHDPlus{dd}.
    root = "https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data"
    assert _base_url("MS", "05") == f"{root}/NHDPlusMS/NHDPlus05"   # nested
    assert _base_url("SA", "03N") == f"{root}/NHDPlusSA/NHDPlus03N"  # nested
    assert _base_url("NE", "01") == f"{root}/NHDPlusNE"             # flat
    assert _base_url("GL", "04") == f"{root}/NHDPlusGL"             # flat


def test_connected_comids_distinct_nonzero():
    df = pd.DataFrame(
        {
            "COMID": [1, 2, 3, 4, 5],
            "FTYPE": ["ArtificialPath", "ArtificialPath", "StreamRiver",
                      "ArtificialPath", "ArtificialPath"],
            # 0 = not through a waterbody; duplicates collapse; None excluded.
            "WBAREACOMI": [100, 100, 0, 200, None],
        }
    )
    assert connected_comids_from_flowlines(df) == {100, 200}


def test_connected_comids_empty_when_all_zero():
    df = pd.DataFrame({"WBAREACOMI": [0, 0, 0]})
    assert connected_comids_from_flowlines(df) == set()


def test_connected_comids_numeric_strings_ok():
    # NHD shapefiles can deliver WBAREACOMI as a string field; numeric strings
    # must still parse so a VPU isn't silently dropped to the empty set.
    df = pd.DataFrame({"WBAREACOMI": ["100", "0", "200"]})
    assert connected_comids_from_flowlines(df) == {100, 200}


def test_connected_comids_excludes_nodata_sentinels():
    # Some VPUs use -9999 (not 0) as the WBAREACOMI nodata sentinel. A real
    # waterbody COMID is always positive, so only positive values are kept.
    # These are valid numbers (not a parse failure), so no ValueError is raised.
    df = pd.DataFrame({"WBAREACOMI": [100, -9999, 200, -1, 0]})
    assert connected_comids_from_flowlines(df) == {100, 200}


def test_connected_comids_raises_on_coercion_loss():
    # A populated-but-unparseable WBAREACOMI (column-format drift) would coerce
    # to NaN and silently contribute zero connected COMIDs for that VPU. That is
    # the catastrophic silent-failure path, so it must fail loud.
    df = pd.DataFrame({"WBAREACOMI": ["100", "bogus", "200"]})
    with pytest.raises(ValueError, match="failed numeric parse"):
        connected_comids_from_flowlines(df)


def test_connected_comids_network_gate_drops_non_network_wbareacomi():
    # Network gate: a WBAREACOMI carried by a Non-Network Flowline (absent from
    # the network set) must NOT promote its waterbody on-stream. NHD draws
    # Non-Network artificial paths through every closed-basin lake, so the raw
    # WBAREACOMI set over-promotes endorheic waterbodies (e.g. VPU 18 COMID
    # 2556875 via Non-Network ArtificialPath 2561885).
    df = pd.DataFrame(
        {"COMID": [10, 20], "WBAREACOMI": [100, 200]}
    )
    # only flowline 10 is a Network Flowline -> only its WBAREACOMI survives
    assert connected_comids_from_flowlines(df, network_comids={10}) == {100}


def test_connected_comids_network_gate_keeps_when_any_network_path():
    # A lake tagged by BOTH a Network and a Non-Network artificial path is a
    # genuine on-stream lake and stays promoted.
    df = pd.DataFrame(
        {"COMID": [10, 11, 20], "WBAREACOMI": [100, 100, 200]}
    )
    assert connected_comids_from_flowlines(df, network_comids={11}) == {100}


def test_connected_comids_network_gate_absent_keeps_all():
    # Backward-compat: with no network set supplied, every positive WBAREACOMI is
    # kept (the pre-gate contract; COMID column is not even required).
    df = pd.DataFrame({"WBAREACOMI": [100, 200]})
    assert connected_comids_from_flowlines(df) == {100, 200}


def test_connected_comids_empty_network_gate_drops_all():
    # An empty (not None) network set activates the gate and matches nothing, so
    # every WBAREACOMI is dropped — distinct from None (ungated). Guards against a
    # truthiness-vs-`is not None` regression in the gate condition.
    df = pd.DataFrame({"COMID": [10, 20], "WBAREACOMI": [100, 200]})
    assert connected_comids_from_flowlines(df, network_comids=set()) == set()


def test_connected_comids_network_gate_coerces_string_comid():
    # NHD ships COMID as strings in some VPU snapshots; the gate coerces so a
    # string COMID still matches the int network set.
    df = pd.DataFrame({"COMID": ["10", "20"], "WBAREACOMI": [100, 200]})
    assert connected_comids_from_flowlines(df, network_comids={10}) == {100}


def test_write_connected_comids_roundtrip(tmp_path):
    out = tmp_path / "nested" / "connected_waterbody_comids.parquet"
    write_connected_comids({300, 100, 200}, out)

    assert out.exists()
    got = pd.read_parquet(out)
    assert list(got.columns) == ["comid"]
    assert got["comid"].tolist() == [100, 200, 300]  # sorted ascending
    assert str(got["comid"].dtype) in ("int64", "Int64")
