"""Per-tile-SET compute of per-polygon `dprst_depth_avg` (issue #173 Task 4;
tile-set batching + threading, issue #223 part 2).

This is the compute core the SLURM array fans out over: open each of this
batch's real 3DEP tile SETS (`sources.TileSet` -- one project, one UTM zone,
`sources.tag_and_assign`'s ranked candidate list) ONCE and run the depth math
for every polygon whose PRIMARY set that is, rather than opening a fresh
source per polygon. Measured on the gfv2r2 CONUS run: 51.6% of polygons
(202,680 of 392,672) took a per-polygon fallback that opened a fresh remote
source for each one -- 73% of that time was `vrt.read` (network I/O + warp),
only 24% arithmetic, and tasks ran at ~55% of one core. Opening each set once
and running sets CONCURRENTLY on threads is the fix: the work is I/O-bound
(waiting on remote reads), so Python's GIL release during `vrt.read` lets
`n_threads` windows actually overlap.

Four layers, ordered by how much I/O they touch:

- `_polygon_depth_from_dem` — pure numpy-in/dict-out core (unit-tested
  offline, no S3). Given a DEM window + the polygon's interior mask,
  decides hydro-flattened ("flat") vs a genuine, measurable depression and
  returns the depth stats either way.
- `compute_polygon` — the single-polygon, always-correct path: wraps
  `topo.read_window` (which resolves + mosaics whatever 1 m/10 m source(s)
  cover the polygon) and `topo._interior_mask`, then calls
  `_polygon_depth_from_dem`. Kept for a lone polygon (e.g. a live smoke
  test) — `run_batch` no longer calls it (a tile SET's `open_tile_set`
  already yields a single mosaicked/warped source, so there is no separate
  "spans more than one source" case left to fall back for).
- `open_tile_set`/`_compute_one` — the per-set primitives `run_batch` calls:
  `open_tile_set` opens ONE `TileSet` (a single tile, or an in-memory
  `gdal.BuildVRT` mosaic of one project's same-zone tiles) as a warped
  EPSG:5070 VRT; `_compute_one` windows one polygon against an already-open
  set and returns `None` (never raises) if the interior came back with no
  valid cells there, so the caller can try the polygon's next ranked
  candidate instead of shipping a false measurement.
- `run_batch` — the batch driver: groups this batch's polygons by their
  PRIMARY tile set (`source_tiles`), opens each set once, and runs sets
  CONCURRENTLY on `n_threads` threads. A polygon whose primary set yields no
  usable interior, or fails to open/read for a PERMANENT reason, walks its
  remaining ranked `candidates` in a second pass, ending at the
  always-present 10 m seamless tile — see `run_batch`'s own docstring for
  the counter/logging contract this candidate-recovery path preserves from
  the pre-#223 per-tile design. A TRANSIENT failure (a network blip -- see
  `_is_transient_error`) is retried on the SAME source instead, and if it
  never clears, fails the whole task rather than silently demoting a
  correctly-ranked primary to a worse candidate (the 2026-09-20 DNS
  incident this guards against).
"""
from __future__ import annotations

import logging
import random
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.features import geometry_mask
from rasterio.vrt import WarpedVRT

from .sources import TileSet, decode
from .topo import (
    GDAL_HTTP_ENV,
    _interior_mask,
    _native_resolution,
    depth_to_spill,
    is_hydroflattened,
    lake_max_depth,
    read_padded,
    read_window,
    volume_mean_depth,
)

__all__ = [
    "_polygon_depth_from_dem",
    "compute_polygon",
    "open_tile_set",
    "run_batch",
    "TileSetOpenError",
]

# GDAL/rasterio env for anonymous public-bucket HTTPS reads — a COPY of
# `topo.GDAL_HTTP_ENV` (see topo.py's module notes on /vsicurl/ vs /vsis3/
# and its HTTP timeout rationale). Copied, not aliased: `rasterio.Env(**...)`
# never mutates its kwargs, but sharing one mutable dict between two modules
# would make an in-place edit to either module's "own" copy silently change
# the other's env too.
_ENV_OPTS = dict(GDAL_HTTP_ENV)

# Output columns of `run_batch`'s parquet, fixed so an empty batch (a
# SLURM array task with 0 assigned tile sets) still writes a well-formed,
# concat-able parquet rather than a columnless one. `source` is the winning
# tile set's `project` (or `"10m"` on the seamless fallback); `interior_coverage`
# is the fraction of the polygon's TRUE interior footprint whose cells were
# NOT VOID -- covers both `read_padded`'s out-of-window sentinel padding and
# a genuine source-nodata gap inside real coverage (see `_compute_one`'s
# docstring) -- a diagnostic, never a gate at write time (toolkit review
# round 4, finding 6a/7, issue #223): a partially-covered polygon still
# ships as `method="measured"`, and this column is what lets a later
# donor-filter (next task) exclude it from regional calibration.
_OUTPUT_COLUMNS = [
    "COMID",
    "dprst_depth_m",
    "measured_max_m",
    "hollister_max_m",
    "flat",
    "resolution",
    "method",
    "source",
    "interior_coverage",
]


class TileSetOpenError(RasterioIOError):
    """Raised by `open_tile_set` when `gdal.BuildVRT` returns `None`, carrying
    GDAL's own `GetLastErrorMsg()` text so the real cause (network vs a
    genuinely bad mosaic, e.g. mixed CRS) is visible and classifiable by
    `_is_transient_error` -- rather than the bare, unclassifiable
    `RuntimeError` this used to be. A `RasterioIOError` subclass so it lands
    in `_attempt`'s existing "routine open failure" handling, not the
    `n_compute_error` bug bucket: the 2026-09-20 tjc smoke rerun hit a
    cluster-wide DNS failure ("CURL error: Could not resolve host:
    prd-tnm.s3.amazonaws.com", 532 times across 5 nodes within the single
    second 21:08:10) that surfaced HERE as a bare `RuntimeError` for every
    multi-tile-key primary set, while a single-key set's IDENTICAL DNS
    failure came through `rasterio.open` as a plain `RasterioIOError` and
    was (correctly) treated as a routine read failure. Making both paths
    raise the same family closes that gap.
    """


# Bounded exponential backoff + jitter for a TRANSIENT source open/read
# failure (see `_is_transient_error`) -- retried on the SAME source, never
# used as grounds to advance `_recover`'s candidate walk (CLAUDE.md's
# dprst_depth doctrine: a transient network failure is not evidence a
# source is unusable). 5 attempts starting at ~2s: the measured 2026-09-20
# incident was a DNS failure that hit 532 times within a single second
# (21:08:10) and had already cleared by the time the pre-fix code's
# candidate-recovery pass ran moments later -- so a handful of seconds of
# backoff comfortably rides out a blip of that duration without holding a
# thread for anywhere near the SLURM task's multi-hour budget. Jitter
# matters, not just backoff: every thread of every one of that run's 8
# tasks x 8 threads started within the same second, so synchronised
# retries (no jitter) would simply re-create the same stampede against
# DNS/S3 a few seconds later.
_RETRY_ATTEMPTS = 5
_RETRY_BASE_DELAY_S = 2.0
_RETRY_MAX_DELAY_S = 30.0

# Substrings (checked case-insensitively) that mark a source open/read
# failure message as TRANSIENT -- a network hiccup worth retrying on the
# SAME source, not evidence the source itself is unusable. Sourced from the
# measured 2026-09-20 incident (DNS: "Could not resolve host") plus the
# other curl/GDAL failure modes the same class of cluster-wide network
# blip produces. Kept as an explicit, documented list rather than a bare
# `except OSError` catch-all, because the failure modes in
# `_PERMANENT_ERROR_MARKERS` below (a genuinely unreadable object, a
# mixed-CRS mosaic) must NOT be retried -- retrying those would just delay
# the correct candidate-walk behaviour by `_RETRY_ATTEMPTS` rounds of
# backoff for no benefit.
_TRANSIENT_ERROR_MARKERS = (
    "could not resolve host",  # the measured 2026-09-20 DNS incident
    "couldn't resolve host",  # curl's own alternate wording
    "couldn't connect",
    "failed to connect",
    "timed out",
    "connection reset",
    "connection refused",
    "recv failure",
    "empty reply from server",
    "ssl connect error",
    "ssl handshake",
)
# HTTP 429 (throttling) and any 5xx (server-side) status, reported by
# GDAL/curl as e.g. "HTTP error code: 503" or "response code: 429" -- both
# transient. Matched only when the message also mentions "http"/"curl"
# (`_HTTP_CONTEXT_RE` below) so an incidental "500"/"429" substring inside a
# tile filename or COMID can't false-positive -- real vsicurl failures
# always carry one of those two words (GDAL's own "CURL error: ..." prefix,
# or an "HTTP ... code" body), so this stays lenient about the exact status
# phrasing while still gating on genuine transport context.
_TRANSIENT_HTTP_STATUS_RE = re.compile(r"\b(429|5\d\d)\b")

# Substrings/patterns that mark a failure as PERMANENT -- the source itself
# is unusable, so `_recover`'s candidate walk must proceed exactly as
# before this fix (no retry). "not recognized as being in a supported file
# format" is GDAL's message for a genuinely unreadable published object (2
# known in the real corpus); HTTP 404/403 are a missing/forbidden object.
_PERMANENT_ERROR_MARKERS = (
    "not recognized as being in a supported file format",
)
_PERMANENT_HTTP_STATUS_RE = re.compile(r"\b(404|403)\b")
_HTTP_CONTEXT_RE = re.compile(r"\b(?:http|curl)\b")


def _is_transient_error(message: str) -> bool:
    """Classify a source open/read failure message as TRANSIENT (retry the
    SAME source) vs PERMANENT (advance `_recover`'s candidate walk, exactly
    as pre-#223-transient-retry). PERMANENT markers are checked first so a
    message that happens to combine both (unseen in practice, but a
    mixed-CRS `BuildVRT` failure could in principle mention a transport
    detail) still fails safe toward NOT retrying -- retrying a genuinely
    unusable source only burns `_RETRY_ATTEMPTS` rounds of backoff before
    reaching the same correct outcome, whereas mis-retrying a truly
    transient failure as permanent would incorrectly demote a correctly-
    ranked primary source instantly, which is the whole bug this exists to
    fix. Anything matching neither list is treated as PERMANENT (the safe
    default -- unclassified failures keep today's behaviour rather than
    silently gaining an unbounded retry)."""
    text = str(message).lower()
    if any(marker in text for marker in _PERMANENT_ERROR_MARKERS):
        return False
    has_http_context = bool(_HTTP_CONTEXT_RE.search(text))
    if has_http_context and _PERMANENT_HTTP_STATUS_RE.search(text):
        return False
    if any(marker in text for marker in _TRANSIENT_ERROR_MARKERS):
        return True
    if has_http_context and _TRANSIENT_HTTP_STATUS_RE.search(text):
        return True
    return False


def _backoff_delay(attempt: int) -> float:
    """Delay (seconds) before retry attempt `attempt` (1-indexed): exponential
    backoff from `_RETRY_BASE_DELAY_S`, capped at `_RETRY_MAX_DELAY_S`, with
    +/-50% jitter -- see `_RETRY_ATTEMPTS`'s module comment for why jitter
    is load-bearing here (every thread of every task in the measured
    incident started within the same second)."""
    base = min(_RETRY_BASE_DELAY_S * (2 ** (attempt - 1)), _RETRY_MAX_DELAY_S)
    return base * random.uniform(0.5, 1.5)


def _sleep(seconds: float) -> None:
    """The retry backoff's sleep, as a module-level seam tests monkeypatch
    so a retry-exhaustion test doesn't actually wait ~30-90s."""
    time.sleep(seconds)


def _polygon_depth_from_dem(
    dem: np.ndarray, interior_mask: np.ndarray, transform, nodata: float = -9999.0
) -> dict:
    """Pure core: DEM window + interior mask -> depth stats for one polygon.

    Hydro-flattened water surfaces are published as an exactly-constant
    breakline elevation (USGS Lidar Base Spec): the Phase 0 spike validated
    that the flatness verdict must be read off the POLYGON INTERIOR alone
    (`interior_mask`-selected cells) — that's what produced the trustworthy
    SwampMarsh 21.7% / LakePond 11% flattened fractions (pre-#223
    measurement — the probe's reads predate `read_padded` and were subject
    to the same misregistration/empty-window bug it fixes, so re-measure
    rather than re-cite per this repo's rule) (a hydro-flattened lake's
    interior reads EXACTLY 0.000 m range). Running the gate over the
    rim-inclusive window instead is wrong: a real hydro-flattened lake
    sitting in terrain with any surrounding relief would have a
    window-range > tol and be misclassified non-flat, and we'd then
    "measure" a depth off its flat water surface rather than its bed. A
    perfectly-constant interior IS what hydro-flattening looks like — that
    correctly reads flat=True; a genuine, non-flattened depression's floor
    carries real (>1 cm) interior relief and correctly reads flat=False.

    `hollister_max_m` (Task 6's terrain-slope max-depth predictor) is
    ALWAYS computed, flat or not: Task 5 uses it both as calibration fit
    data (non-flat rows: compare predicted vs `measured_max_m`) and as the
    actual filled value for flat rows (`method="flat_pending"` today,
    resolved to `"calibrated_hollister"` et al. in Task 5).

    Returns ``{"dprst_depth_m", "measured_max_m", "hollister_max_m",
    "flat"}``. Flat (or degenerate — an all-nodata/empty-interior window):
    ``dprst_depth_m``/``measured_max_m`` are ``nan`` (Task 5 fills them);
    otherwise ``dprst_depth_m`` is the V/A mean depth and ``measured_max_m``
    the max cell depth, both over `interior_mask`, both metres.
    """
    dem = np.asarray(dem, dtype=np.float64)
    interior_mask = np.asarray(interior_mask, dtype=bool)

    hollister_max_m = float(lake_max_depth(dem, interior_mask, transform))

    interior_valid = dem[interior_mask & (dem != nodata)]
    flat = interior_valid.size == 0 or bool(is_hydroflattened(interior_valid)["flat"])

    if flat:
        return {
            "dprst_depth_m": float("nan"),
            "measured_max_m": float("nan"),
            "hollister_max_m": hollister_max_m,
            "flat": True,
        }

    depth = depth_to_spill(dem, nodata=nodata)
    cell_area_m2 = abs(transform.a * transform.e)
    _, _, mean_d = volume_mean_depth(depth, interior_mask, cell_area_m2)
    measured_max_m = float(depth[interior_mask].max())
    return {
        "dprst_depth_m": mean_d,
        "measured_max_m": measured_max_m,
        "hollister_max_m": hollister_max_m,
        "flat": False,
    }


def compute_polygon(geom, best_topo: str, wesm_row=None) -> dict:
    """Full single-polygon path: `read_window` + interior mask + the core.

    Always correct regardless of how many tiles `geom`'s buffered window
    touches (`read_window` mosaics 2-4 1 m tiles via `gdal.BuildVRT` when
    needed) — the price is a fresh `rasterio.open` per call, which is fine
    for a single polygon but is exactly what `run_batch`'s tile cache
    exists to avoid at CONUS scale.

    Returns `_polygon_depth_from_dem`'s dict plus `resolution` (the
    source actually read — may be `"10m"` on a documented 1m->10m
    fallback, see `read_window`) and `method` (`"measured"` if not flat,
    else `"flat_pending"` — Task 5 fills the real fill method in).
    """
    dem, transform, _crs, source = read_window(geom, best_topo, wesm_row)
    interior_mask = _interior_mask(dem, transform, geom)
    result = _polygon_depth_from_dem(dem, interior_mask, transform)
    result["resolution"] = source["resolution"]
    result["method"] = "flat_pending" if result["flat"] else "measured"
    return result


@contextmanager
def open_tile_set(ts: TileSet):
    """ONE open per tile set: a single tile, or an in-memory mosaic of one
    project's same-zone tiles (BuildVRT cannot mix CRSs, which is why a set
    is single-zone -- see sources.rank_candidates). Warped to EPSG:5070 at
    native GSD with nearest resampling, exactly as the per-tile path always
    did, so in-bounds single-tile reads stay bit-identical.

    Raises `TileSetOpenError` (a `RasterioIOError` subclass carrying GDAL's
    own error text) if the multi-key mosaic fails to build, or a plain
    `RasterioIOError` from `rasterio.open` for the single-key case -- the
    CALLER (`_attempt`/`_retry_compute_one` in `run_batch`) is responsible
    for classifying either via `_is_transient_error` and retrying
    accordingly; this function itself never retries."""
    vsimem = None
    path = ts.keys[0]
    if len(ts.keys) > 1:
        vsimem = f"/vsimem/dprst_depth_set_{uuid.uuid4().hex}.vrt"
        vrt_ds = gdal.BuildVRT(vsimem, list(ts.keys))
        if vrt_ds is None:
            # A mixed CRS/UTM zone slipping through sources.rank_candidates
            # (BuildVRT cannot mosaic mixed CRSs) is the expected PERMANENT
            # cause. Raise HERE, attributed to the set, rather than let a
            # `None` write silently fall through to a downstream
            # RasterioIOError at the read site that would misleadingly
            # blame the read (fix round 1, cheap finding). GDAL's own
            # `GetLastErrorMsg()` is captured and carried on the exception
            # -- the measured 2026-09-20 incident's ACTUAL cause here was a
            # DNS failure on every one of `ts.keys`' underlying
            # `rasterio.open`s, which BuildVRT surfaces only as `None`
            # unless the caller also reads GDAL's own error text; without
            # it, this used to raise a bare unclassifiable `RuntimeError`
            # that landed in the n_compute_error BUG bucket, hiding the
            # real network cause. `TileSetOpenError` (a `RasterioIOError`
            # subclass) makes `_is_transient_error` able to tell the two
            # causes apart from this message alone.
            gdal_msg = gdal.GetLastErrorMsg() or "<no GDAL error message>"
            raise TileSetOpenError(
                f"gdal.BuildVRT returned None for tile set project={ts.project!r} "
                f"keys={ts.keys!r} -- cannot build the mosaic (GDAL: {gdal_msg})"
            )
        # GDAL only serialises a VRT to its target (here, /vsimem/...) when the
        # dataset handle is flushed/released -- it does NOT happen just because
        # BuildVRT returned. Dropping the Python reference here (rather than
        # leaving it bound for the rest of this generator's body) is what
        # triggers that: `topo.read_window`'s BuildVRT call has always DISCARDED
        # its return value outright, which is why THAT path works and this one
        # didn't. Verified by reproduction: `rasterio.open(vsimem)` immediately
        # after `gdal.BuildVRT` (ds still bound) raises RasterioIOError "No such
        # file or directory"; releasing it first opens cleanly. Without this, any
        # polygon whose window touches 2+ tiles of its primary (multi-key) set
        # ALWAYS fails to open, is treated by `_attempt`'s outer handler as a
        # routine read failure, and falls all the way to the 10 m last resort --
        # exit code 0, `method="measured"`, from 10 m data with a correctly
        # ranked 1 m source sitting right there unread.
        vrt_ds = None
        path = vsimem
    try:
        with rasterio.open(path) as src:
            resolution = _native_resolution(src, "EPSG:5070")
            with WarpedVRT(
                src, crs="EPSG:5070", resampling=Resampling.nearest, resolution=resolution
            ) as vrt:
                yield vrt
    finally:
        if vsimem is not None:
            gdal.Unlink(vsimem)


def _read_tile_window(vrt, geom, rim_buffer_m: float = 200.0) -> tuple[np.ndarray, object]:
    """Windowed RAW-DEM read of `geom`'s buffered bbox against an ALREADY-OPEN VRT.

    The single-source counterpart of `read_window`'s inner read block —
    deliberately NOT calling `read_window` itself, which always opens its
    source fresh (that per-call open is exactly what `run_batch`'s tile
    cache avoids for polygons that don't straddle a tile boundary). `geom`
    must be in the VRT's CRS (EPSG:5070, matching `dprst_gdf`/
    `read_window`'s convention), so `rim_buffer_m` (metres) adds directly
    to `geom.bounds` with no reprojection, exactly as in `read_window`.
    Delegates to `topo.read_padded` (#223) so a buffered window that
    overhangs this tile's edge comes back correctly clipped-and-padded
    instead of silently misregistered or empty.
    """
    minx, miny, maxx, maxy = geom.bounds
    return read_padded(
        vrt, (minx - rim_buffer_m, miny - rim_buffer_m, maxx + rim_buffer_m, maxy + rim_buffer_m)
    )


def _compute_one(vrt, geom) -> dict | None:
    """Depth stats for one polygon against an open set, or `None` if its
    interior has no valid cells there (so the caller tries the next
    candidate).

    Also reports `interior_coverage`: the fraction of the polygon's TRUE
    interior footprint (before the `dem != sentinel` exclusion `_interior_mask`
    applies inline) whose cells are NOT VOID -- `_interior_mask` excludes
    `dem == sentinel`, and `read_padded`'s `_normalize_nodata` maps BOTH
    `read_padded`'s own out-of-window sentinel padding AND the source
    raster's own declared nodata (a genuine void inside real coverage, e.g.
    a data gap in a 3DEP tile) onto that same sentinel value -- so this
    figure covers both causes of "not real data here", which is the right
    single number for a donor filter (it doesn't matter to the filter WHY a
    cell was missing). `_interior_mask` returns only the post-exclusion mask
    and `topo.py` is out of scope for this task, so the footprint is
    rasterized a SECOND time here rather than splitting `_interior_mask`
    into two return values (toolkit review round 4, findings 6a/7, issue
    #223) -- `geom` is guaranteed non-degenerate here (`interior.any()`
    already returned True), so `footprint.sum()` is always >= 1. A
    low-but-nonzero coverage still ships as `method="measured"` with no gate
    at write time -- this figure is what lets a later donor-filter (next
    task) exclude it from regional calibration; see
    docs/dprst_depth_avg_reference.md's "coverage loss" paragraph.
    """
    dem, transform = _read_tile_window(vrt, geom)
    interior = _interior_mask(dem, transform, geom)
    if not interior.any():
        return None
    footprint = geometry_mask([geom], out_shape=dem.shape, transform=transform, invert=True)
    result = _polygon_depth_from_dem(dem, interior, transform)
    result["resolution"] = "1m" if abs(transform.a) < 5.0 else "10m"
    result["interior_coverage"] = float(interior.sum()) / max(int(footprint.sum()), 1)
    return result


def _retry_compute_one(vrt, ts: TileSet, idx, geom, logger: logging.Logger, bump) -> tuple[dict | None, bool]:
    """`_compute_one(vrt, geom)`, retrying a TRANSIENT `RasterioIOError` on
    the SAME already-open set/polygon (bounded backoff, see
    `_backoff_delay`) instead of dropping the polygon to `_recover`'s
    candidate walk over what may be nothing more than a network blip.

    Returns `(result, gave_up_transiently)`. `gave_up_transiently` is True
    only when every one of `_RETRY_ATTEMPTS` also failed transiently -- in
    that case `result` is always `None` and the CALLER must NOT advance
    this polygon to a lower-ranked candidate (the primary is still the
    correct source; see `run_batch`'s docstring / the 2026-09-20 DNS
    incident). A PERMANENT `RasterioIOError`, or any other exception, is
    re-raised immediately on the first attempt with no retry -- unchanged
    pre-existing behaviour, left for the caller's own try/except to handle
    exactly as before this fix.
    """
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            return _compute_one(vrt, geom), False
        except RasterioIOError as exc:
            if not _is_transient_error(str(exc)):
                raise
            if attempt >= _RETRY_ATTEMPTS:
                logger.error(
                    "  set=%s idx=%s: TRANSIENT read failure persisted after %d "
                    "attempt(s) (%s) -- NOT advancing to a lower-ranked candidate",
                    ts.project, idx, _RETRY_ATTEMPTS, exc,
                )
                return None, True
            bump("n_transient_retry")
            delay = _backoff_delay(attempt)
            logger.warning(
                "  set=%s idx=%s: TRANSIENT read failure (%s) -- retry %d/%d in %.1fs",
                ts.project, idx, exc, attempt, _RETRY_ATTEMPTS, delay,
            )
            _sleep(delay)
    raise AssertionError("unreachable: _retry_compute_one's loop always returns or raises")


def _empty_batch_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_OUTPUT_COLUMNS)


def run_batch(
    dprst_gdf: gpd.GeoDataFrame,
    tile_sets: list[str],
    out_parquet: str | Path,
    logger: logging.Logger,
    n_threads: int = 1,
) -> pd.DataFrame:
    """Compute every polygon whose PRIMARY tile set is in `tile_sets`.

    Each set is opened once (`open_tile_set`) and all its member polygons are
    windowed against it; sets run concurrently on `n_threads` threads because
    the work is remote-read bound -- on the OLD pre-#223-part-2 per-polygon
    path, which 51.6% of gfv2r2 polygons took, 73% of the FALLBACK time (not
    overall pipeline time) was `vrt.read`, not arithmetic. A polygon whose
    primary yields no valid interior, or hits a PERMANENT read/open failure
    (empty interior; HTTP 404/403; an unreadable object; a mixed-CRS
    `BuildVRT` failure -- see `_is_transient_error`), walks its remaining
    ranked `candidates`, ending at the 10 m seamless tile. Counters stay
    split (#173 PR#177 FIX 2, transient-retry fix follow-on): read failures
    (expected PERMANENT failures + empty interiors, WARNING) vs compute
    errors (bugs, ERROR) vs transient retries vs polygons with no usable
    source at all.

    A TRANSIENT failure (a network hiccup -- DNS, connect, timeout,
    connection reset, HTTP 429/5xx; see `_is_transient_error`'s module
    constants) is a DIFFERENT code path entirely, both at the set-open
    level and at the per-polygon `_compute_one` level: it is retried
    `_RETRY_ATTEMPTS` times with backoff+jitter (`_backoff_delay`) on the
    SAME source, and if every attempt also fails transiently, that
    source/polygon is NEVER handed to `_recover`'s candidate walk. The
    doctrine (CLAUDE.md's dprst_depth bullet) is that a transient failure
    is not evidence the primary is unusable -- the network is what's
    broken, not the source ranking -- so once every OTHER set in this batch
    has finished, `run_batch` FAILS THE WHOLE TASK LOUDLY (raises, and does
    NOT write `out_parquet`) rather than silently letting those polygons
    fall through to a lower-ranked candidate. This is what the measured
    2026-09-20 incident needed: a one-second cluster-wide DNS failure (532
    "Could not resolve host" errors across 5 nodes within the single
    second 21:08:10) hit while every polygon's PERMANENT-failure handling
    silently demoted 4,822 of 5,535 tjc polygons (87%) off their correctly-
    ranked primary source, with 578 single-candidate polygons losing their
    row outright -- the operator's remedy for a task that now fails this
    way is simply to resubmit that array index once the network recovers.

    The counters count ATTEMPTS, not polygons: a polygon that walks all of
    P1 -> P2 -> 10m before succeeding adds 2 to `n_read_failure` (one per
    PERMANENTLY-failed candidate) and 1 to `n_recovered`, not 1 total. An
    empty interior (`_compute_one` returns `None` without raising) is
    folded into `n_read_failure` alongside the PERMANENT exception-raising
    cases -- both mean "this candidate did not have the polygon's data",
    the same signal by a different mechanism. `n_transient_retry` counts
    RETRIES that happened (not give-ups, and never a candidate-walk step) --
    a transient failure that eventually succeeds inflates NEITHER
    `n_read_failure` nor `n_recovered`; it was never treated as a failure of
    that source at all.

    The final summary line is escalated (#173 FIX 3, restored in the #223
    fix-round-1 rewrite; degradation term added in review round 3) ONLY
    when `run_batch` reaches the point of writing `out_parquet` at all -- a
    persistent transient failure skips the summary/escalation logic
    entirely and raises instead (see above), which is a strictly LOUDER
    signal than any of these levels: ERROR if `n_compute_error > 0` (a real
    bug must never hide behind the expected read-failure rate); else
    WARNING if EITHER the written fraction of polygons drops below 90%
    (polygons LOST entirely -- a mass PERMANENT read failure) OR more than
    10% of planned polygons needed `_recover` at all (polygons that still
    SHIPPED, via the fallback ladder, but only because their primary set
    failed to open/read PERMANENTLY). The second term is the one that
    actually catches "every 1 m set failing and every polygon recovering to
    10 m": every recovered polygon is still in `out`, so `success_fraction`
    alone stays at 1.0 for that exact scenario and would otherwise log at
    INFO -- this is precisely the failure mode the round-3
    `open_tile_set`/BuildVRT-flush bug produced (every multi-tile-key
    primary open failed, recovering silently to the 10 m last resort). 10%
    is chosen symmetrically with the existing 90% success-fraction gate;
    genuine tile-boundary/hydro-flattening noise recovers a small, roughly
    constant fraction of polygons, so a jump to double digits is a systemic
    signal, not routine variance.
    """
    for col in ("source_tiles", "candidates", "COMID"):
        if col not in dprst_gdf.columns:
            raise KeyError(f"run_batch needs '{col}' (plan with sources.tag_and_assign first)")
    wanted = set(tile_sets)
    members = {s: list(g.index) for s, g in dprst_gdf.groupby("source_tiles") if s in wanted}
    empty_sets = wanted - set(members)
    if empty_sets:
        # A manifest tile set with no member polygons in THIS `dprst_gdf` is a
        # planner/tagged-parquet generation mismatch (the plan step's manifest and
        # `dprst_polygons_tagged.parquet` were written together, but nothing re-checks
        # them against each other here) -- it silently drops those polygons from this
        # batch with no signal at all, since the dict comprehension above just never
        # produces an entry for them.
        logger.warning(
            "  %d/%d tile set(s) in this batch's manifest have NO member polygon in "
            "dprst_gdf (planner/tagged-parquet mismatch?): %s",
            len(empty_sets), len(tile_sets), sorted(empty_sets)[:10],
        )
    counts = {
        "n_read_failure": 0,
        "n_compute_error": 0,
        "n_transient_retry": 0,
        "n_recovered": 0,
        "n_no_source": 0,
    }
    lock = threading.Lock()

    def _bump(key):
        with lock:
            counts[key] += 1

    def _attempt(ts_str, idxs):
        """Run `idxs` against one set.

        Returns `(done, pending, transient_fail)`:
        - `done`: {idx: result} -- resolved against THIS set.
        - `pending`: idx list whose failure against THIS set was PERMANENT
          (empty interior; 404/403; unreadable object; mixed-CRS -- see
          `_is_transient_error`). `_recover` walks `candidates` for these,
          exactly as before the transient-retry fix.
        - `transient_fail`: idx list whose open/read kept failing
          TRANSIENTLY even after `_RETRY_ATTEMPTS` retries with backoff.
          NEVER handed to `_recover` -- the primary is still the correct
          source, the network is what's broken (see `run_batch`'s
          docstring / the 2026-09-20 DNS incident: 532 "Could not resolve
          host" failures across 5 nodes within one second). `run_batch`
          fails the whole task loudly over these once every other set has
          finished, instead of silently advancing the candidate walk.
        """
        ts = decode(ts_str)
        done = {}
        pending, transient_fail = [], []
        open_exc = None
        for open_attempt in range(1, _RETRY_ATTEMPTS + 1):
            # Exclude already-`done` idx from a retried open (only possible
            # if a PRIOR attempt's for-loop ran to completion and then its
            # own CLOSE -- not open -- raised transiently; open-time
            # failures, the incident's actual shape, always occur before
            # any idx is processed, so `done` is empty on every retry in
            # that common case). A permanently-failed idx from a prior
            # attempt is deliberately NOT excluded here too -- that would
            # need tracking across attempts for a corner case (a
            # transient CLOSE failure immediately after a mixed
            # success/permanent-failure attempt) rare enough that
            # reprocessing it once more on the retried open is an
            # acceptable, bounded cost, not a correctness bug.
            remaining = [i for i in idxs if i not in done]
            if not remaining:
                open_exc = None
                break
            this_pending, this_transient = [], []
            try:
                with rasterio.Env(**_ENV_OPTS), open_tile_set(ts) as vrt:
                    for idx in remaining:
                        try:
                            r, gave_up = _retry_compute_one(
                                vrt, ts, idx, dprst_gdf.geometry.loc[idx], logger, _bump
                            )
                        except RasterioIOError as exc:
                            _bump("n_read_failure")
                            logger.warning("  set=%s idx=%s: read failure (%s)", ts.project, idx, exc)
                            r, gave_up = None, False
                        except Exception as exc:  # noqa: BLE001 - loud, isolated, never aborts the batch
                            _bump("n_compute_error")
                            logger.error("  set=%s idx=%s: UNEXPECTED compute error (%s: %s)",
                                         ts.project, idx, type(exc).__name__, exc)
                            r, gave_up = None, False
                        else:
                            if r is None and not gave_up:
                                # `_compute_one` returned None WITHOUT raising: an empty
                                # interior on this set, not an exception. This is the
                                # read-failure COUNT + WARNING from round-4's run_batch
                                # fix (#223) -- carry it forward here too, or the
                                # candidate-recovery rewrite silently drops the only
                                # operator-visible signal that a set's window missed.
                                _bump("n_read_failure")
                                logger.warning(
                                    "  set=%s idx=%s: window has no valid interior on "
                                    "this set — trying next candidate", ts.project, idx,
                                )
                        if gave_up:
                            this_transient.append(idx)
                        elif r is None:
                            this_pending.append(idx)
                        else:
                            r["source"] = ts.project
                            done[idx] = r
                open_exc = None
                pending, transient_fail = this_pending, this_transient
                break  # opened + processed successfully -- done retrying the open
            except RasterioIOError as exc:
                open_exc = exc
                pending, transient_fail = this_pending, this_transient
                if _is_transient_error(str(exc)) and open_attempt < _RETRY_ATTEMPTS:
                    _bump("n_transient_retry")
                    delay = _backoff_delay(open_attempt)
                    logger.warning(
                        "  set=%s: TRANSIENT open failure (%s) -- retry %d/%d in %.1fs",
                        ts.project, exc, open_attempt, _RETRY_ATTEMPTS, delay,
                    )
                    _sleep(delay)
                    continue
                break  # PERMANENT, or transient retries exhausted -- handled below
            except Exception as exc:  # noqa: BLE001 - one bad set (corrupt COG, bad/missing CRS, MemoryError, ...) must not abort the batch
                # Unexpected: a corrupt COG, a bad or missing CRS
                # (`_native_resolution`'s `CRSError`, `WarpedVRT`'s
                # `WarpedVRTError`/`CRSError` -- neither is a `RasterioIOError`
                # subclass), MemoryError, etc -- a real code/data bug, not a
                # routine read gap or a network blip. Without this handler one
                # bad tile set kills the whole array task: nothing after it
                # runs, no batch_XXXX.parquet is written, and every
                # already-computed set's work in this call is discarded
                # (fix round 1, finding 2).
                pending = [i for i in remaining if i not in done]
                _bump("n_compute_error")
                logger.error("  set=%s: UNEXPECTED error (%s: %s) — %d polygon(s) to recovery",
                             ts.project, type(exc).__name__, exc, len(pending))
                open_exc = None
                break

        if open_exc is not None:
            # `done` may be non-empty here (a `WarpedVRT`/`open_tile_set`
            # cleanup-time exception can surface AFTER the for loop already
            # emitted some results), so `unresolved` must exclude anything
            # already resolved -- re-including a resolved idx would
            # double-compute it and inflate n_recovered (fix round 1,
            # "double compute" finding).
            unresolved = [i for i in idxs if i not in done]
            if _is_transient_error(str(open_exc)):
                # Every open attempt failed TRANSIENTLY: the primary is
                # still the right source, the network is what's broken --
                # do NOT hand these to `_recover`'s candidate walk.
                transient_fail = unresolved
                logger.error(
                    "  set=%s: TRANSIENT open failure persisted after %d attempt(s) "
                    "(%s) -- %d polygon(s) unresolved, NOT advancing to a "
                    "lower-ranked candidate", ts.project, _RETRY_ATTEMPTS, open_exc,
                    len(unresolved),
                )
            else:
                # PERMANENT: the set doesn't exist / can't be opened -- a
                # routine 404/read gap, not a code bug. Unchanged
                # pre-existing behaviour.
                pending = unresolved
                _bump("n_read_failure")
                logger.warning("  set=%s: open failed (%s) — %d polygon(s) to recovery",
                               ts.project, open_exc, len(pending))
        return done, pending, transient_fail

    results, to_recover, transient_failures = {}, [], []
    items = sorted(members.items())
    with ThreadPoolExecutor(max(1, n_threads)) as ex:
        for i, ((ts_str, _), (done, pending, transient_fail)) in enumerate(
            zip(items, ex.map(lambda kv: _attempt(*kv), items)), 1
        ):
            results.update(done)
            to_recover += pending
            if transient_fail:
                transient_failures.append((decode(ts_str).project, transient_fail))
            if i % 25 == 0:
                logger.info("  [%d/%d tile sets] %d polygons done", i, len(members), len(results))

    def _recover(idx):
        # Skip the PRIMARY set (already tried above), not just the first list entry --
        # `candidates[0] == source_tiles` holds today by construction
        # (`sources.assign_sources` always takes `candidates[0]` as `source_tiles`),
        # but that's an unchecked positional coupling across two modules. Compare
        # against the polygon's own `source_tiles` instead of slicing, so a future
        # divergence between the two doesn't silently skip retrying (or silently never
        # retry) the wrong candidate.
        #
        # Only reached for a PERMANENT primary failure (`_attempt` never hands a
        # TRANSIENT one here) -- and the same doctrine applies to every candidate
        # tried below: if ts_str's open/read itself keeps failing TRANSIENTLY after
        # retries, that is NOT grounds to keep walking past it either, so the walk
        # stops there and reports a transient failure rather than falling through
        # to `n_no_source`.
        primary = dprst_gdf.at[idx, "source_tiles"]
        candidates = [c for c in dprst_gdf.at[idx, "candidates"] if c != primary]
        for ts_str in candidates:
            done, _, transient_fail = _attempt(ts_str, [idx])
            if idx in done:
                return idx, done[idx], None
            if transient_fail:
                return idx, None, decode(ts_str).project
        # Named individually, not just counted in the aggregate n_no_source
        # -- an operator working through a bad batch needs to find THIS
        # polygon, and the per-attempt WARNINGs above carry the DataFrame
        # index, not the COMID (fix round 1, cheap finding).
        logger.warning(
            "  idx=%s COMID=%s: no usable source after trying %d candidate(s)",
            idx, dprst_gdf.at[idx, "COMID"], len(candidates),
        )
        return idx, None, None

    with ThreadPoolExecutor(max(1, n_threads)) as ex:
        for idx, r, transient_project in ex.map(_recover, sorted(to_recover)):
            if transient_project is not None:
                transient_failures.append((transient_project, [idx]))
            elif r is None:
                counts["n_no_source"] += 1
            else:
                counts["n_recovered"] += 1
                results[idx] = r

    if transient_failures:
        # FAIL THE TASK LOUDLY instead of writing a partial/degraded batch --
        # every other set/candidate in this batch has already been processed
        # (both ThreadPoolExecutor passes above ran to completion) by the time
        # we get here. A missing batch_XXXX.parquet is already caught
        # downstream by PR #222's `_verify_batches_match_plan`, and this
        # non-zero exit stops the SLURM `afterok` build from starting on an
        # incomplete depstor stack. The operator's remedy is to resubmit this
        # array index once the network recovers (see CLAUDE.md's dprst_depth
        # transient-vs-permanent bullet).
        affected = sum(len(idxs) for _, idxs in transient_failures)
        named = ", ".join(f"{proj}={len(idxs)}" for proj, idxs in transient_failures)
        message = (
            f"run_batch: TRANSIENT network failure persisted after {_RETRY_ATTEMPTS} "
            f"retries on {len(transient_failures)} tile set(s) ({named}) -- "
            f"{affected} polygon(s) unresolved. NOT writing {out_parquet}. "
            f"Resubmit this array task once the network recovers."
        )
        logger.error(message)
        raise RuntimeError(message)

    rows = []
    for idx, r in results.items():
        r["COMID"] = dprst_gdf.at[idx, "COMID"]
        r["method"] = "flat_pending" if r["flat"] else "measured"
        rows.append(r)
    out = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
    out = out.sort_values("COMID").reset_index(drop=True) if len(out) else _empty_batch_frame()
    out.to_parquet(out_parquet, index=False)

    n_planned = sum(len(v) for v in members.values())
    success_fraction = len(out) / n_planned if n_planned else 1.0
    recovered_fraction = counts["n_recovered"] / n_planned if n_planned else 0.0
    summary_args = (
        len(out), n_planned, len(members),
        ", ".join(f"{k}={v}" for k, v in counts.items()), out_parquet,
    )
    summary_fmt = "run_batch: %d/%d polygons written (%d tile sets, %s) -> %s"
    # (#173 FIX 3, restored in the #223 fix-round-1 rewrite) Completeness
    # gate: a mass read-failure (S3 outage / HPC firewall regression) or ANY
    # unexpected compute error must not ship silently at INFO -- escalate
    # the whole summary line so it's visible in a normal log scan.
    #
    # (#223 review round 3) `success_fraction` alone does NOT catch "every 1 m
    # set failing and every polygon recovering to 10 m" -- a recovered polygon
    # is still written to `out`, so that scenario keeps success_fraction at 1.0
    # and would log at INFO despite every 1 m read having failed (exactly what
    # the round-3 open_tile_set/BuildVRT-flush bug produced). `recovered_
    # fraction > 0.10` (symmetric with the 90% success-fraction gate) catches
    # it: a material share of polygons needing the fallback ladder at all is a
    # systemic primary-open/read signal, not routine tile-boundary noise.
    if counts["n_compute_error"] > 0:
        logger.error(summary_fmt, *summary_args)
    elif success_fraction < 0.90 or recovered_fraction > 0.10:
        logger.warning(summary_fmt, *summary_args)
    else:
        logger.info(summary_fmt, *summary_args)
    return out
