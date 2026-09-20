import logging

import numpy as np
import pandas as pd
import pytest

from gfv2_params.dprst_depth import inventory as inv

PAGE1 = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_15_x50y505_P1.tif</Key></Contents>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/readme.txt</Key></Contents>
<IsTruncated>true</IsTruncated><NextContinuationToken>tok/2+=</NextContinuationToken></ListBucketResult>"""
PAGE2 = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.tif</Key></Contents>
<IsTruncated>false</IsTruncated></ListBucketResult>"""
PREFIXES = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Prefix>StagedProducts/Elevation/1m/Projects/</Prefix>
<CommonPrefixes><Prefix>StagedProducts/Elevation/1m/Projects/P1/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>StagedProducts/Elevation/1m/Projects/P2_B22/</Prefix></CommonPrefixes>
<IsTruncated>false</IsTruncated></ListBucketResult>"""


def test_parse_list_response_reads_keys_prefixes_and_token():
    keys, prefixes, token = inv.parse_list_response(PAGE1)
    assert len(keys) == 2 and prefixes == [] and token == "tok/2+="
    assert inv.parse_list_response(PAGE2)[2] is None


def test_list_project_tiles_paginates_and_keeps_only_tile_tifs():
    calls = []

    def fetch(url):
        calls.append(url)
        return PAGE2 if "continuation-token=tok%2F2%2B%3D" in url else PAGE1

    keys = inv.list_project_tiles("P1", fetch=fetch)
    assert keys == [
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_15_x50y505_P1.tif",
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.tif",
    ]
    assert len(calls) == 2


def test_list_projects_returns_directory_names():
    assert inv.list_projects(fetch=lambda url: PREFIXES) == ["P1", "P2_B22"]


def test_tile_record_projects_real_bounds_to_5070():
    key = "/vsicurl/https://x/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.tif"
    # the real, CROPPED header measured 2026-09-19 for WI_12County_B22 x27y511
    header = {"crs": "EPSG:26916", "width": 10000, "height": 4289,
              "bounds": (269999.79, 5105711.0, 279999.79, 5110000.0)}
    rec = inv.tile_record(key, header)
    assert rec["project"] == "P1" and rec["zone"] == 16
    assert rec["height"] == 4289
    assert rec["maxx"] > rec["minx"] and rec["maxy"] > rec["miny"]
    # ~10 km x ~4.3 km survives the reprojection (densified bounds, so >=).
    # Measured directly via rasterio.warp.transform_bounds on this exact
    # header (UTM 16N -> EPSG:5070): (10394.8, 5264.5) -- the UTM-to-Albers
    # rotation/convergence at this latitude/zone inflates the N-S extent by
    # ~23% over the source height, more than the brief's original 5_000 m
    # ceiling anticipated. Bounds widened to bracket the real, verified
    # value rather than a guessed one; still tight enough to catch a wrong
    # source CRS or a dropped densify_pts.
    assert 9_900 < rec["maxx"] - rec["minx"] < 10_800
    assert 4_200 < rec["maxy"] - rec["miny"] < 5_500


def test_build_inventory_counts_failures_and_raises_above_threshold():
    keys = [f"/vsicurl/https://x/Projects/P1/TIFF/USGS_1M_15_x{i}y500_P1.tif" for i in range(10)]
    ok = {"crs": "EPSG:26915", "width": 10012, "height": 10012,
          "bounds": (499994.0, 4990006.0, 510006.0, 5000018.0)}

    def reader(key):
        if key.endswith("x3y500_P1.tif"):
            raise OSError("synthetic 503")
        return ok

    with pytest.raises(RuntimeError, match="1 of 10"):
        inv.build_inventory(["P1"], lister=lambda p: keys, header_reader=reader,
                            logger=logging.getLogger("t"), n_threads=4, max_fail_frac=0.05)
    df = inv.build_inventory(["P1"], lister=lambda p: keys, header_reader=reader,
                             logger=logging.getLogger("t"), n_threads=4, max_fail_frac=0.2)
    assert len(df) == 9 and list(df.columns) == inv.INVENTORY_COLUMNS
    assert df["key"].is_unique and df["key"].is_monotonic_increasing  # deterministic order


def test_build_inventory_raises_on_zero_keys_with_nonempty_projects():
    # A systemic listing bug (renamed TIFF/ subpath, changed directory layout,
    # anything that returns [] rather than raising) must not fall through to a
    # well-formed, silently EMPTY DataFrame -- see CLAUDE.md on the endorheic
    # table / min_onstream_comids floor for why an empty result must raise.
    with pytest.raises(RuntimeError, match="0 tile keys listed across 3 project"):
        inv.build_inventory(["P1", "P2", "P3"], lister=lambda p: [],
                            header_reader=lambda k: {}, logger=logging.getLogger("t"))


@pytest.mark.parametrize("value,rank", [("QL 0", 0), ("QL 1", 1), ("QL1", 1), ("QL 2", 2),
                                        ("QL 3", 3), ("Other", 9), (None, 9), (np.nan, 9)])
def test_ql_rank(value, rank):
    assert inv.ql_rank(value) == rank


def test_project_attrs_joins_s3_dirs_by_workunit_then_project_then_link():
    wesm = pd.DataFrame({
        "workunit": ["A_WU1", "A_WU2", "AL_25Co_B1_2017", "X_WU"],
        "project": ["A", "A", "AL_25Co_2017", "X_PROJ"],
        "ql": ["QL 2", "QL 1", "QL 2", "QL 1"],
        "collect_end": pd.to_datetime(["2019-01-01", "2018-06-01", "2017-05-01", "2020-01-01"]),
        "sourcedem_link": ["", "", "", "https://x/StagedProducts/Elevation/OPR/Projects/X_DIR/"],
        "lpc_link": ["", "", "", ""],
    })
    out = inv.project_attrs(wesm, ["A", "AL_25Co_B1_2017", "X_DIR", "NOPE"])
    assert out.loc["A", "ql_rank"] == 1 and out.loc["A", "matched_by"] == "project"
    assert out.loc["A", "collect_end"] == pd.Timestamp("2019-01-01")
    assert out.loc["AL_25Co_B1_2017", "matched_by"] == "workunit"  # the dominant real-world case
    assert out.loc["X_DIR", "matched_by"] == "link" and out.loc["X_DIR", "ql_rank"] == 1
    assert "NOPE" not in out.index  # unmatched -> ranks last in sources.rank_candidates
