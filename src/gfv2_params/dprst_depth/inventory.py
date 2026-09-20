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
"""
from __future__ import annotations

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

S3_BASE = "https://prd-tnm.s3.amazonaws.com"
PROJECTS_PREFIX = "StagedProducts/Elevation/1m/Projects/"
TILE_NAME_RE = re.compile(r"USGS_1M_(?P<zone>\d{2})_x(?P<x>\d+)y(?P<y>\d+)_(?P<project>.+)\.tif$")
INVENTORY_COLUMNS = ["project", "key", "zone", "crs", "width", "height", "minx", "miny", "maxx", "maxy"]
_KEY_RE = re.compile(r"<Key>([^<]*)</Key>")
_PREFIX_RE = re.compile(r"<Prefix>([^<]*)</Prefix>")
_TOKEN_RE = re.compile(r"<NextContinuationToken>([^<]*)</NextContinuationToken>")


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


def tile_record(key: str, header: dict) -> dict:
    m = TILE_NAME_RE.search(key)
    minx, miny, maxx, maxy = transform_bounds(header["crs"], "EPSG:5070", *header["bounds"], densify_pts=21)
    return {"project": m.group("project"), "key": key, "zone": int(m.group("zone")),
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
