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


def test_http_get_retries_a_transient_failure_then_succeeds(monkeypatch):
    # Every GDAL raster read already retries (GDAL_HTTP_MAX_RETRY=5); this plain
    # urllib listing request had none -- one transient failure among thousands of
    # list_s3 requests could abort a whole ~30-minute staging run for a routine blip.
    calls = {"n": 0}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"ok"

    def fake_urlopen(url, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("synthetic transient failure")
        return _Resp()

    monkeypatch.setattr(inv.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(inv.time, "sleep", lambda s: None)  # don't actually wait in tests
    assert inv.http_get("http://example", max_attempts=5, backoff_s=0.01) == "ok"
    assert calls["n"] == 3


def test_http_get_raises_after_exhausting_all_retries(monkeypatch):
    def fake_urlopen(url, timeout=None):
        raise OSError("synthetic permanent failure")

    monkeypatch.setattr(inv.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(inv.time, "sleep", lambda s: None)
    with pytest.raises(OSError, match="synthetic permanent failure"):
        inv.http_get("http://example", max_attempts=3, backoff_s=0.01)


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


LEGACY_PAGE = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/OR_DOGAMI_2017/TIFF/USGS_one_meter_x25y494_OR_DOGAMI_2017.tif</Key></Contents>
<Contents><Key>StagedProducts/Elevation/1m/Projects/OR_DOGAMI_2017/TIFF/readme.txt</Key></Contents>
<IsTruncated>false</IsTruncated></ListBucketResult>"""


def test_list_project_tiles_keeps_legacy_named_tiles_too():
    # Fix round 2 (job 4521387): 610 of 967 project directories publish ONLY
    # under this older, zone-less naming convention -- TILE_NAME_RE alone
    # silently dropped every one of them.
    keys = inv.list_project_tiles("OR_DOGAMI_2017", fetch=lambda url: LEGACY_PAGE)
    assert keys == [
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/"
        "Projects/OR_DOGAMI_2017/TIFF/USGS_one_meter_x25y494_OR_DOGAMI_2017.tif",
    ]


LOWERCASE_1M_PAGE = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/CA_NoCAL_Wildfires_PlumasNF_B1_2018/TIFF/USGS_1m_x58y445_CA_NoCAL_Wildfires_PlumasNF_B1_2018.tif</Key></Contents>
<IsTruncated>false</IsTruncated></ListBucketResult>"""


def test_list_project_tiles_keeps_lowercase_1m_named_tiles_too():
    # Fix round 3 (job 4521496): a THIRD convention, lowercase `1m`, no zone --
    # 92 more real project directories (e.g. CA_NoCAL_Wildfires_PlumasNF_B1_2018,
    # PR_PRVI_G_2018) contributed zero tiles under rounds 1/2's two patterns.
    keys = inv.list_project_tiles("CA_NoCAL_Wildfires_PlumasNF_B1_2018", fetch=lambda url: LOWERCASE_1M_PAGE)
    assert keys == [
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/"
        "CA_NoCAL_Wildfires_PlumasNF_B1_2018/TIFF/USGS_1m_x58y445_CA_NoCAL_Wildfires_PlumasNF_B1_2018.tif",
    ]


def test_tile_record_takes_zone_from_header_for_a_lowercase_1m_named_tile():
    key = ("/vsicurl/https://x/Projects/CA_NoCAL_Wildfires_PlumasNF_B1_2018/TIFF/"
           "USGS_1m_x58y445_CA_NoCAL_Wildfires_PlumasNF_B1_2018.tif")
    header = {"crs": "EPSG:26910", "width": 10012, "height": 10012,
              "bounds": (249994.0, 4939994.0, 260006.0, 4950006.0)}
    rec = inv.tile_record(key, header)
    assert rec["project"] == "CA_NoCAL_Wildfires_PlumasNF_B1_2018"
    assert rec["zone"] == 10  # no zone in the name at all; header CRS only


def test_list_project_tiles_excludes_tif_aux_xml_sidecars():
    # Real production example (S3-verified 2026-09-20): an `.aux.xml` sidecar
    # sits right next to its `.tif` in the same TIFF/ prefix and contains its
    # own "_x<digits>y<digits>_" token too -- e.g.
    # USGS_1M_10_x46y441_CA_NoCAL_3DEP_Supp_Funding_2018_D18.tif.aux.xml. The
    # generalised TILE_NAME_RE must still reject it: it doesn't END in `.tif`.
    page = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_10_x46y441_P1.tif</Key></Contents>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_10_x46y441_P1.tif.aux.xml</Key></Contents>
<IsTruncated>false</IsTruncated></ListBucketResult>"""
    keys = inv.list_project_tiles("P1", fetch=lambda url: page)
    assert keys == [
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_10_x46y441_P1.tif",
    ]


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


def test_tile_record_takes_zone_from_header_for_a_legacy_named_tile():
    # The legacy USGS_one_meter_* name has NO zone digits at all -- zone must
    # come entirely from the header CRS (fix round 2).
    key = "/vsicurl/https://x/Projects/OR_DOGAMI_2017/TIFF/USGS_one_meter_x25y494_OR_DOGAMI_2017.tif"
    header = {"crs": "EPSG:26910", "width": 10012, "height": 10012,
              "bounds": (249994.0, 4939994.0, 260006.0, 4950006.0)}
    rec = inv.tile_record(key, header)
    assert rec["project"] == "OR_DOGAMI_2017"
    assert rec["zone"] == 10  # EPSG:26910 - 26900


def test_tile_record_prefers_header_zone_and_warns_on_filename_mismatch(caplog):
    # Modern name says zone 15; header CRS says zone 16 -- header must win,
    # and the disagreement must be a loud WARNING (not a silent pick).
    key = "/vsicurl/https://x/Projects/P1/TIFF/USGS_1M_15_x50y505_P1.tif"
    header = {"crs": "EPSG:26916", "width": 10012, "height": 10012,
              "bounds": (499994.0, 4990006.0, 510006.0, 5000018.0)}
    with caplog.at_level(logging.WARNING, logger="gfv2_params.dprst_depth.inventory"):
        rec = inv.tile_record(key, header)
    assert rec["zone"] == 16
    assert "disagrees" in caplog.text and "15" in caplog.text and "16" in caplog.text


def test_tile_record_takes_project_from_directory_and_warns_on_filename_mismatch(caplog):
    # Fix round 4: the filename claims a different project than the
    # directory the key is actually filed under -- the directory must win
    # (it's what list_project_tiles was called with and what project_attrs
    # joins WESM against), and the disagreement must be a loud WARNING.
    key = "/vsicurl/https://x/Projects/REAL_DIR/TIFF/USGS_one_meter_x25y494_WRONG_NAME.tif"
    header = {"crs": "EPSG:26910", "width": 10012, "height": 10012,
              "bounds": (249994.0, 4939994.0, 260006.0, 4950006.0)}
    with caplog.at_level(logging.WARNING, logger="gfv2_params.dprst_depth.inventory"):
        rec = inv.tile_record(key, header)
    assert rec["project"] == "REAL_DIR"
    assert "disagrees" in caplog.text and "REAL_DIR" in caplog.text and "WRONG_NAME" in caplog.text


def test_tile_record_project_survives_an_adversarial_embedded_coordinate_token(caplog):
    # Adversarial case from design review: a project name that itself
    # contains a "_x<digits>y<digits>_"-shaped substring used to make a
    # greedy filename-only parse backtrack to the LAST such token, silently
    # truncating the parsed project (and coordinate) to the wrong value.
    # Taking `project` from the key's own directory component makes this
    # impossible regardless of what the regex captures.
    key = ("/vsicurl/https://x/Projects/PROJECT/TIFF/"
           "USGS_one_meter_x1y2_PROJECT_x99y88_SUFFIX.tif")
    header = {"crs": "EPSG:26910", "width": 10012, "height": 10012,
              "bounds": (249994.0, 4939994.0, 260006.0, 4950006.0)}
    with caplog.at_level(logging.WARNING, logger="gfv2_params.dprst_depth.inventory"):
        rec = inv.tile_record(key, header)
    # The directory ("PROJECT") wins, never the filename's own guess at its
    # project (whatever the regex happened to capture for the adversarial
    # name) -- and since they disagree here, a WARNING must also fire.
    assert rec["project"] == "PROJECT"
    assert "disagrees" in caplog.text


def test_list_project_tiles_keeps_an_uppercase_tif_extension():
    page = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.TIF</Key></Contents>
<IsTruncated>false</IsTruncated></ListBucketResult>"""
    keys = inv.list_project_tiles("P1", fetch=lambda url: page)
    assert keys == [
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.TIF",
    ]


def test_tile_record_matches_an_uppercase_tif_extension():
    # Pre-existing gap the design review flagged: \.tif$ alone is case
    # sensitive, so a real .TIF tile would be silently dropped by
    # list_project_tiles and crash tile_record's own TILE_NAME_RE.search
    # (None has no .group()) if it ever reached it.
    key = "/vsicurl/https://x/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.TIF"
    header = {"crs": "EPSG:26916", "width": 10012, "height": 10012,
              "bounds": (499994.0, 4990006.0, 510006.0, 5000018.0)}
    rec = inv.tile_record(key, header)
    assert rec["project"] == "P1" and rec["zone"] == 16


def test_zone_from_crs_raises_on_a_non_utm_crs():
    with pytest.raises(ValueError, match="EPSG:4326"):
        inv.zone_from_crs("EPSG:4326")


@pytest.mark.parametrize("crs", ["EPSG:26943", "EPSG:26929"])
def test_zone_from_crs_raises_on_a_nad83_state_plane_code(crs):
    # EPSG 26924+ is the NAD83 STATE PLANE family, not UTM -- 26943 is California
    # zone 3, 26929 is Michigan Central. An unbounded `code - 26900` in [1, 60]
    # would silently accept these and fabricate zone 43/29; two projects in
    # different CRSs that happen to map to the same zone number would then land
    # in one tile set and die in gdal.BuildVRT (mixed CRS).
    with pytest.raises(ValueError, match=crs):
        inv.zone_from_crs(crs)


def test_zone_from_crs_accepts_the_full_nad83_utm_range():
    assert inv.zone_from_crs("EPSG:26901") == 1
    assert inv.zone_from_crs("EPSG:26923") == 23


def test_zone_from_crs_accepts_the_full_wgs84_utm_range():
    assert inv.zone_from_crs("EPSG:32601") == 1
    assert inv.zone_from_crs("EPSG:32660") == 60
    assert inv.zone_from_crs("EPSG:32701") == 1
    assert inv.zone_from_crs("EPSG:32760") == 60


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


def test_build_inventory_logs_the_total_failure_count_and_worst_projects(caplog):
    # Only the first 20 per-failure lines are logged individually -- at real scale
    # (up to 1,256 tiles/project) a whole project can vanish under the 1% gate with
    # nothing but the arithmetic gap between two INFO lines as evidence. There must
    # be a WARNING that states the total and names the worst-hit project(s).
    keys = (
        [f"/vsicurl/https://x/Projects/BAD_PROJECT/TIFF/USGS_1M_15_x{i}y500_BAD_PROJECT.tif" for i in range(25)]
        + [f"/vsicurl/https://x/Projects/GOOD_PROJECT/TIFF/USGS_1M_15_x{i}y500_GOOD_PROJECT.tif" for i in range(75)]
    )
    ok = {"crs": "EPSG:26915", "width": 10012, "height": 10012,
          "bounds": (499994.0, 4990006.0, 510006.0, 5000018.0)}

    def reader(key):
        if "BAD_PROJECT" in key:
            raise OSError("synthetic 503")
        return ok

    with caplog.at_level(logging.WARNING, logger="t"):
        df = inv.build_inventory(
            ["BAD_PROJECT", "GOOD_PROJECT"], lister=lambda p: [k for k in keys if p in k],
            header_reader=reader, logger=logging.getLogger("t"),
            n_threads=4, max_fail_frac=0.3,
        )
    assert len(df) == 75
    assert "25/100 tile header read(s) failed in total" in caplog.text
    assert "BAD_PROJECT=25" in caplog.text


def test_build_inventory_counts_an_unrecognised_crs_as_a_header_failure():
    # zone_from_crs raises rather than guessing; build_inventory must count
    # that through the SAME fail-rate gate as any other header-read error,
    # not let it slip past uncounted.
    keys = ["/vsicurl/https://x/Projects/P1/TIFF/USGS_1M_15_x1y500_P1.tif"]
    bad = {"crs": "EPSG:4326", "width": 10, "height": 10, "bounds": (0, 0, 1, 1)}
    with pytest.raises(RuntimeError, match="1 of 1"):
        inv.build_inventory(["P1"], lister=lambda p: keys, header_reader=lambda k: bad,
                            logger=logging.getLogger("t"), n_threads=1, max_fail_frac=0.0)


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
