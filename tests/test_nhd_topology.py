"""Unit tests for NHDPlus flowline-topology staging."""

from __future__ import annotations

import pandas as pd

from gfv2_params.download.nhd_topology import (
    _pick_attributes_key,
    read_vaa,
    write_topology,
)


def test_pick_attributes_key_selects_highest_version():
    keys = [
        "NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDPlusAttributes_08.7z",
        "NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDPlusAttributes_10.7z",
        "NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDSnapshot_07.7z",
    ]
    assert _pick_attributes_key(keys, "14").endswith("_14_NHDPlusAttributes_10.7z")


def test_pick_attributes_key_none_when_absent():
    keys = ["NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDSnapshot_07.7z"]
    assert _pick_attributes_key(keys, "14") is None


def test_read_vaa_normalises_casing(tmp_path):
    # NHDPlus ships mixed-case fields (ComID, DnHydroseq); read_vaa must
    # resolve them case-insensitively and return canonical upper-case names.
    import geopandas as gpd
    from shapely.geometry import Point

    p = tmp_path / "PlusFlowlineVAA.dbf"
    gpd.GeoDataFrame(
        {"ComID": [101], "DnHydroseq": [5.0], "Hydroseq": [9.0],
         "TerminalFl": [0], "StartFlag": [1], "StreamOrde": [1],
         "FromNode": [11.0], "ToNode": [12.0],
         "geometry": [Point(0, 0)]},
        crs="EPSG:4269",
    ).to_file(p)
    out = read_vaa(p)
    assert list(out.columns) == [
        "COMID", "DNHYDROSEQ", "HYDROSEQ", "TERMINALFL",
        "STARTFLAG", "STREAMORDE", "FROMNODE", "TONODE",
    ]
    assert int(out["COMID"].iloc[0]) == 101


def test_write_topology_roundtrip(tmp_path):
    df = pd.DataFrame({
        "COMID": [101, 102], "DNHYDROSEQ": [5.0, 0.0], "HYDROSEQ": [9.0, 4.0],
        "TERMINALFL": [0, 1], "STARTFLAG": [1, 0], "STREAMORDE": [1, 1],
        "FROMNODE": [11.0, 13.0], "TONODE": [12.0, 14.0],
    })
    out = tmp_path / "topo.parquet"
    write_topology(df, out)
    back = pd.read_parquet(out)
    assert list(back.columns) == [
        "comid", "dnhydroseq", "hydroseq", "terminalfl",
        "startflag", "streamorde", "fromnode", "tonode",
    ]
    assert back["comid"].dtype == "int64"
    routed = set(back[back["dnhydroseq"] != 0]["comid"])
    assert routed == {101}
