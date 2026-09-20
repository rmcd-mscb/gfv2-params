"""Real 3DEP 1 m tile inventory + WESM project attributes (issue #223).

Replaces the convex-hull WESM footprint index as the answer to "which 1 m
tiles exist here, and what do they really cover?". A hull claims ground a
project never flew; on gfv2r2, 61% of polygons that the hulls said were
covered by 2+ projects had exactly one real tile and 15% had none. The
inventory is built from the S3 listing (what exists) and each tile's own
COG header (its real, possibly cropped, extent), then reprojected to
EPSG:5070 so planning is pure geometry.

WESM is still the source of project QUALITY and DATE, read geometry-free
(the geometry is what made the old staging OOM). See `project_attrs` for
how S3 directory names are joined to WESM rows.

Fix round 2 (real staging run, job 4521387): the first stage wrote 88,403
tiles across only 357 of the 967 listed project directories. The other 610
publish under an OLDER naming convention with no UTM zone in the filename
(`USGS_one_meter_x<X>y<Y>_<project>.tif`, vs. the modern
`USGS_1M_<zone>_x<X>y<Y>_<project>.tif`), which the original `TILE_NAME_RE`
alone filtered out entirely -- exactly why the old hull-based code never
read these projects either. Since the legacy name carries no zone, `zone`
is now always derived from the tile's own header CRS (`zone_from_crs`),
authoritative either way and something `build_inventory` already reads
regardless of naming convention.

Fix round 3 (job 4521496): after round 2, 92 more directories still
contributed nothing -- a THIRD convention, `USGS_1m_x<X>y<Y>_<project>.tif`
(lowercase, also no zone). Two wrong guesses at "the" naming convention is
the signal to stop enumerating them: `TILE_NAME_RE` now matches on the part
that is actually invariant across all three -- and any future one USGS
invents -- a `USGS_` prefix and a `_x<digits>y<digits>_<project>.tif` tail,
with anything in between. `MODERN_ZONED_TILE_NAME_RE` (the original,
zone-specific pattern) is kept ONLY as a cross-check for the
filename-vs-header zone WARNING in `tile_record`, never for
matching/filtering -- `TILE_NAME_RE` alone decides what counts as a tile.

Fix round 4 (design review of round 3): round 3's generalised `.+` is
greedy, so an adversarial project name that happens to embed its OWN
`_x<digits>y<digits>_`-shaped substring -- e.g.
`USGS_one_meter_x1y2_PROJECT_x99y88_SUFFIX.tif` -- backtracks to the LAST
such token, silently truncating both the parsed coordinate and the parsed
project to the wrong values. Rounds 1-2's literal prefixes could not do
this; only one place in the string could ever match. `project` is now taken
from the tile's own `/Projects/<dir>/` key component (`_project_from_key`),
never the filename -- structurally impossible to get wrong regardless of
what the regex captures, and it's also the exact directory
`list_project_tiles` was called with and what `project_attrs` joins WESM
against, so it's the name that has to win on any disagreement anyway.
`TILE_NAME_RE`'s own `.+` is additionally made non-greedy (`.+?`) as
defence in depth for its `x`/`y` groups, which nothing downstream reads
today but which the pattern still captures. A filename-vs-directory project
mismatch, and an uppercase `.TIF` extension, are both handled the same way
the filename-vs-header zone mismatch already is: matched via
case-insensitive extension, cross-checked, and logged rather than silently
resolved one way or the other.
"""
from __future__ import annotations

import logging
import re
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.warp import transform_bounds
from shapely.geometry import box

from .topo import GDAL_HTTP_ENV

logger = logging.getLogger(__name__)

S3_BASE = "https://prd-tnm.s3.amazonaws.com"
PROJECTS_PREFIX = "StagedProducts/Elevation/1m/Projects/"
# The invariant across every 3DEP tile-name convention seen so far (issue
# #223, fix round 3): a `USGS_` prefix and a `_x<digits>y<digits>_<project>.tif`
# tail, with ANY middle token -- `1M_16`, `one_meter`, `1m`, or whatever comes
# next. Matching this instead of enumerating conventions is the point: rounds
# 1 and 2 each guessed wrong about how many conventions existed. Still
# specific enough to reject a non-tile file in the same `TIFF/` prefix (a
# `readme.txt`, an `.xml` sidecar, a browse image) -- none of those carry a
# `_x<digits>y<digits>_` token immediately before `.tif`. No `zone` group:
# `zone` always comes from the header CRS (`zone_from_crs`), never the name.
# `.+?` (non-greedy, fix round 4) rather than `.+`: a greedy match backtracks
# to the LAST `_x<digits>y<digits>_`-shaped substring in the string, which an
# adversarial (or just unlucky) project name containing its own such
# substring would silently hijack. `project`/`x`/`y` from this pattern are
# NOT authoritative regardless (see `_project_from_key`), but there's no
# reason to prefer the wrong split when the right one is free. `(?i:tif)`
# makes only the extension case-insensitive -- a real `.TIF` tile must not
# be silently dropped -- without loosening the rest of the pattern.
TILE_NAME_RE = re.compile(r"USGS_.+?_x(?P<x>\d+)y(?P<y>\d+)_(?P<project>.+)\.(?i:tif)$")
# The ORIGINAL, zone-specific modern pattern. Kept ONLY as a cross-check for
# the filename-vs-header zone WARNING in `tile_record` -- never used for
# matching/filtering (that's `TILE_NAME_RE` alone now).
MODERN_ZONED_TILE_NAME_RE = re.compile(r"USGS_1M_(?P<zone>\d{2})_x\d+y\d+_.+\.tif$")
INVENTORY_COLUMNS = ["project", "key", "zone", "crs", "width", "height", "minx", "miny", "maxx", "maxy"]
_KEY_RE = re.compile(r"<Key>([^<]*)</Key>")
_PREFIX_RE = re.compile(r"<Prefix>([^<]*)</Prefix>")
_TOKEN_RE = re.compile(r"<NextContinuationToken>([^<]*)</NextContinuationToken>")
# NAD83 UTM north is EPSG 269xx (zone = code - 26900); WGS84 UTM north/south
# is 326xx/327xx. Tried in this order in `zone_from_crs`.
_UTM_ZONE_EPSG_OFFSETS = (26900, 32600, 32700)


def zone_from_crs(crs: str) -> int:
    """Derive the UTM zone number from a tile's own CRS.

    Authoritative regardless of naming convention: the legacy
    `USGS_one_meter_*` filename carries no zone at all, and `build_inventory`
    already reads every tile's header, so there's no reason to trust a
    filename's zone digits over the CRS the tile is actually stored in. Raises
    on any CRS this repo doesn't recognise as a UTM zone rather than guessing
    -- `build_inventory` counts that like any other header-read failure
    against `max_fail_frac`, so a systemic CRS surprise fails loud instead of
    silently writing a fabricated `zone`.
    """
    m = re.fullmatch(r"EPSG:(\d+)", crs)
    if m:
        code = int(m.group(1))
        for offset in _UTM_ZONE_EPSG_OFFSETS:
            zone = code - offset
            if 1 <= zone <= 60:
                return zone
    raise ValueError(f"tile CRS {crs!r} is not a recognised UTM-zone EPSG code")


def http_get(url: str, timeout: float = 60.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def parse_list_response(xml: str) -> tuple[list[str], list[str], str | None]:
    prefixes = [p for p in _PREFIX_RE.findall(xml.split("<CommonPrefixes>", 1)[-1])] if "<CommonPrefixes>" in xml else []
    m = _TOKEN_RE.search(xml)
    return _KEY_RE.findall(xml), prefixes, (m.group(1) if m else None)


def list_s3(prefix: str, *, delimiter: str | None = None, fetch=http_get) -> tuple[list[str], list[str]]:
    keys, prefixes, token = [], [], None
    while True:
        q = {"list-type": "2", "prefix": prefix}
        if delimiter:
            q["delimiter"] = delimiter
        if token:
            q["continuation-token"] = token
        k, p, token = parse_list_response(fetch(f"{S3_BASE}/?{urllib.parse.urlencode(q)}"))
        keys += k
        prefixes += p
        if token is None:
            return keys, prefixes


def list_projects(fetch=http_get) -> list[str]:
    _, prefixes = list_s3(PROJECTS_PREFIX, delimiter="/", fetch=fetch)
    return sorted(p[len(PROJECTS_PREFIX):].strip("/") for p in prefixes)


def list_project_tiles(project: str, fetch=http_get) -> list[str]:
    keys, _ = list_s3(f"{PROJECTS_PREFIX}{project}/TIFF/", fetch=fetch)
    return sorted(f"/vsicurl/{S3_BASE}/{k}" for k in keys if TILE_NAME_RE.search(k))


def read_tile_header(key: str) -> dict:
    with rasterio.Env(**GDAL_HTTP_ENV), rasterio.open(key) as src:
        return {"crs": src.crs.to_string(), "width": src.width, "height": src.height,
                "bounds": tuple(src.bounds)}


_PROJECTS_ANCHOR = "/Projects/"


def _project_from_key(key: str) -> str:
    """The `/Projects/<dir>/` directory component of `key` -- AUTHORITATIVE
    for `project` (fix round 4), never re-derived from the filename. It's
    exactly the directory `list_project_tiles` was called with, and exactly
    what `project_attrs` joins WESM rows against, so it's the value that has
    to win on any disagreement. Structural, not a regex mitigation: a
    filename-only parse can never get this wrong regardless of how
    adversarial the project name is, because the directory isn't parsed out
    of the same string the coordinate is.

    Anchors on the bare `/Projects/` substring, not the full `PROJECTS_PREFIX`
    -- every real key contains `.../1m/Projects/<dir>/...`, and this is
    deliberately tolerant of whatever comes before it (a `/vsicurl/https://...`
    prefix, a shortened test fixture, or a future staging-path change), since
    the directory boundary itself is the only thing this needs to find.
    """
    try:
        return key.split(_PROJECTS_ANCHOR, 1)[1].split("/", 1)[0]
    except IndexError:
        raise ValueError(f"key {key!r} does not contain {_PROJECTS_ANCHOR!r}") from None


def tile_record(key: str, header: dict) -> dict:
    m = TILE_NAME_RE.search(key)
    project = _project_from_key(key)
    # Cross-check only: a filename whose OWN apparent project (per
    # TILE_NAME_RE, non-greedy or not) disagrees with the directory it's
    # actually filed under is worth a loud WARNING -- same shape as the
    # zone cross-check below -- but `project` itself always comes from the
    # directory, never this match.
    if m.group("project") != project:
        logger.warning(
            "tile name project %r disagrees with its own directory %r for %s (using the directory)",
            m.group("project"), project, key,
        )
    zone = zone_from_crs(header["crs"])
    # Cross-check only, and only for the modern zoned convention -- the
    # legacy/lowercase conventions have no zone digits to check at all. A
    # mismatch means the NAD83/WGS84 zone-derivation above is wrong somewhere
    # and is worth a loud WARNING, not a silent pick of one value over the
    # other. `zone` itself is never taken from the filename (see
    # `zone_from_crs`).
    modern = MODERN_ZONED_TILE_NAME_RE.search(key)
    if modern is not None and int(modern.group("zone")) != zone:
        logger.warning(
            "tile name zone %s disagrees with header CRS zone %d for %s (using the header)",
            modern.group("zone"), zone, key,
        )
    minx, miny, maxx, maxy = transform_bounds(header["crs"], "EPSG:5070", *header["bounds"], densify_pts=21)
    return {"project": project, "key": key, "zone": zone,
            "crs": header["crs"], "width": int(header["width"]), "height": int(header["height"]),
            "minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}


def build_inventory(projects, *, n_threads: int = 32, lister=list_project_tiles,
                    header_reader=read_tile_header, logger, max_fail_frac: float = 0.01) -> pd.DataFrame:
    with ThreadPoolExecutor(n_threads) as ex:
        keys = [k for ks in ex.map(lister, projects) for k in ks]
    logger.info("  listed %d tile(s) across %d project(s)", len(keys), len(projects))
    if not keys and projects:
        # `if keys and ...` below short-circuits to False on an empty list, so a
        # systemic listing bug (renamed TIFF/ subpath, changed layout -- anything
        # that returns [] rather than raising) would otherwise fall through to a
        # well-formed, EMPTY DataFrame and a silent 0-row inventory written to the
        # shared data root. Mirrors the `min_onstream_comids`/endorheic-floor
        # convention in CLAUDE.md: an empty result must raise, never masquerade as
        # success just because it type-checks.
        raise RuntimeError(
            f"0 tile keys listed across {len(projects)} project(s) -- refusing to "
            "stage an empty inventory"
        )

    def _one(key):
        try:
            return tile_record(key, header_reader(key)), None
        except Exception as exc:  # noqa: BLE001 - counted and gated below
            return None, f"{key}: {type(exc).__name__}: {exc}"

    with ThreadPoolExecutor(n_threads) as ex:
        results = list(ex.map(_one, keys))
    failures = [e for _, e in results if e]
    for e in failures[:20]:
        logger.warning("  header read failed: %s", e)
    if keys and len(failures) / len(keys) > max_fail_frac:
        raise RuntimeError(
            f"{len(failures)} of {len(keys)} tile header reads failed "
            f"(> {max_fail_frac:.1%}); refusing to stage a partial inventory"
        )
    df = pd.DataFrame([r for r, _ in results if r], columns=INVENTORY_COLUMNS)
    # Defensive: uniqueness should already follow from correct pagination, but
    # don't let a future continuation-token regression silently double-count a
    # tile -- de-dup before sorting rather than trusting `list_s3` forever.
    return df.drop_duplicates(subset="key").sort_values("key").reset_index(drop=True)


def load_inventory(path) -> gpd.GeoDataFrame:
    """Load the staged inventory parquet as a `box`-geometry GeoDataFrame in EPSG:5070.

    Read the `project` column as `frame["project"]`, never `frame.project` --
    `GeoDataFrame.project` is a bound method (CRS projection), so attribute
    access silently returns that method instead of raising, with no warning
    that anything went wrong.
    """
    df = pd.read_parquet(path)
    geom = [box(a, b, c, d) for a, b, c, d in df[["minx", "miny", "maxx", "maxy"]].itertuples(index=False)]
    return gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:5070")


def ql_rank(value) -> int:
    if not isinstance(value, str):
        return 9
    m = re.fullmatch(r"\s*QL\s*(\d)\s*", value)
    return int(m.group(1)) if m else 9


def project_attrs(wesm: pd.DataFrame, dirs) -> pd.DataFrame:
    """QL/date per S3 1 m project DIRECTORY. S3 directory names are mostly WESM
    WORKUNIT names, not project names: on 2026-09-19 exact `project` matched only
    366 of 967 dirs, `workunit` 572, and the link fields the rest of 932. No
    `onemeter_category` filter: a directory under 1m/Projects/ is itself proof
    the data qualifies for the 1 m product."""
    w = wesm.copy()
    w["ql_rank"] = w["ql"].map(ql_rank)
    links = w["sourcedem_link"].fillna("") + " " + w["lpc_link"].fillna("")
    rows = []
    for d in dirs:
        for how, hit in (("workunit", w["workunit"] == d), ("project", w["project"] == d),
                         ("link", links.str.contains(f"/Projects/{d}/", regex=False))):
            if hit.any():
                m = w[hit]
                rows.append({"project": d, "ql_rank": int(m["ql_rank"].min()),
                             "collect_end": m["collect_end"].max(), "matched_by": how})
                break
    return pd.DataFrame(rows, columns=["project", "ql_rank", "collect_end", "matched_by"]).set_index("project")


def load_project_attrs(path) -> pd.DataFrame:
    return pd.read_parquet(path)
