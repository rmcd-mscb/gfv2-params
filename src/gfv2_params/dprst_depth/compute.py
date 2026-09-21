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
  `_classify_error_chain`, which classifies an exception's WHOLE cause
  chain) is retried on the SAME source instead; a failure that outlasts
  that in-place budget is DEFERRED to one more attempt after a pause
  rather than declared persistent immediately, and only fails the whole
  task if that deferred attempt also fails -- never silently demoting a
  correctly-ranked primary to a worse candidate (the three 2026-09-20 DNS
  incidents this guards against: two at the tile-set OPEN level, and a
  third at the per-polygon READ level on an already-open set, whose
  failure chain carries no HTTP/curl signal at all and is disambiguated by
  probe/reproduction instead -- see `_reclassify_unknown_read_failure`).
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

# `_ENV_OPTS` with GDAL's OWN retry layer turned off, for every call site
# that is itself inside OUR retry loop (`_attempt`'s open, `_retry_
# compute_one`'s reads, and the classification probes both call). #223
# round 3 review, M-D: `_ENV_OPTS`'s production GDAL_HTTP_MAX_RETRY=5 (with
# GDAL_HTTP_RETRY_DELAY=2) runs INSIDE a single attempt of our own loop --
# measured directly against a persistent 503 with `_ENV_OPTS` unchanged,
# one single read attempt was measured at ~188s (GDAL's own 5 retries,
# each preceded by a growing internal delay, all before OUR backoff/
# logging ever runs) -- so the documented "~2 minute in-place budget"
# (`_RETRY_ATTEMPTS` attempts) was really bounded by `_RETRY_ATTEMPTS * ~188s`
# (nearly 19 minutes, worst case), not ~2 minutes. Our own backoff+jitter
# (`_backoff_delay`) already
# provides the SAME resilience one layer up, with visible logging and
# counters GDAL's internal retries have neither -- so the retry-loop call
# sites disable GDAL's, making OUR loop the single source of truth for
# the RETRY COUNT. It does NOT make every attempt's own duration bounded
# to a fixed small number, though (#223 round 4 review, MINOR M2):
# `GDAL_HTTP_TIMEOUT`/`_CONNECTTIMEOUT`/`_LOW_SPEED_TIME` still apply PER
# REQUEST even with retries off, so a HANG-type failure (as opposed to a
# fast-failing one) still costs real per-attempt time -- see the
# `_RETRY_ATTEMPTS` module comment above for the corrected worst-case
# figure. `_ENV_OPTS` itself is untouched for every OTHER caller (e.g.
# `topo.read_window`'s single-shot reads, which are not wrapped in any
# retry loop and still want GDAL's own retry as their only safety net).
_IN_PROCESS_RETRY_ENV = dict(_ENV_OPTS, GDAL_HTTP_MAX_RETRY="0")

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
    """Raised by `open_tile_set` when `gdal.BuildVRT` fails to build a
    tile-set's mosaic (raises under `gdal.UseExceptions()` with
    `strict=True`, or -- defensively -- returns `None`).

    `gdal.GetLastErrorMsg()` is EMPTY at this point even under real GDAL
    3.12.3 (verified for both an all-DNS-failure and an all-404 key list,
    #223 round 2 review), so it carries no classifiable cause on its own.
    `open_tile_set` therefore probes each key individually
    (`_probe_keys_for_real_cause`) and chains THAT real exception as
    `__cause__` -- `_is_transient_error_chain` walks the chain, so the real
    "CURL error: Could not resolve host: ..."/"HTTP response code: 404"
    text is what actually gets classified, not this exception's own
    (necessarily vaguer) message.

    A `RasterioIOError` subclass so it lands in `_attempt`'s "routine open
    failure" handling, not the `n_compute_error` bug bucket: the 2026-09-20
    tjc smoke rerun hit a cluster-wide DNS failure ("CURL error: Could not
    resolve host: prd-tnm.s3.amazonaws.com", 532 times across 5 nodes
    within the single second 21:08:10) that surfaced HERE as a bare
    `RuntimeError` pre-round-1, and even post-round-1 (before this __cause__
    chaining existed) still classified PERMANENT -- QUIETER than the
    original bug, since it no longer even forced the ERROR-level
    `n_compute_error` summary that the bare `RuntimeError` did.
    """


# Bounded exponential backoff + jitter for a TRANSIENT source open/read
# failure (see `_is_transient_error_chain`) -- retried on the SAME source,
# never used as grounds to advance `_recover`'s candidate walk (CLAUDE.md's
# dprst_depth doctrine: a transient network failure is not evidence a
# source is unusable).
#
# Sized from TWO measured incidents, not one (#223 round 2 review, I4): the
# first (2026-09-20, job 4529029) was a DNS failure hitting 532 times within
# a single second (21:08:10); the second (job 4532393) measured the outage
# itself lasting ~30s (21:49:54-21:50:24) while the THEN-5-attempt/~20s
# in-place budget exhausted by 21:50:16 -- 8s before the network actually
# recovered -- and the same set opened fine again moments later. The budget
# was simply shorter than the outage it was meant to survive. 6 attempts
# from a 4s base, capped at 60s: the gaps between attempts are 4/8/16/32/60s
# (`_backoff_delay`), summing to ~120s (2 minutes) of BACKOFF SLEEP.
#
# That "~2 minutes" is the backoff alone, and is the REAL total budget
# only for a FAST-FAILING error -- DNS, connection-refused, an HTTP
# status response (#223 round 4 review, MINOR M2). `_IN_PROCESS_RETRY_ENV`
# disables GDAL's own retry layer, but `GDAL_HTTP_TIMEOUT` (60s),
# `GDAL_HTTP_CONNECTTIMEOUT` (30s), and `GDAL_HTTP_LOW_SPEED_TIME`/`_LIMIT`
# (30s under 1000 B/s) still apply PER REQUEST -- a HANG-type failure (a
# connection that trickles or never responds, rather than one that fails
# outright) can still cost up to ~60-90s per attempt on top of the
# backoff gap before it, and one attempt can involve more than one
# underlying HTTP request (several keys probed, or several block-range
# reads for one polygon). The realistic worst case for a hang-type
# failure is therefore TENS OF MINUTES per set/polygon, not ~2 minutes --
# see CLAUDE.md's dprst_depth bullet, `slurm_batch/HPC_REFERENCE.md`'s
# Recovery section, and `docs/dprst_depth_avg_reference.md` for the same
# correction. Either way, long
# enough to ride out a 30s-class blip with margin, still well under the
# SLURM task's multi-hour budget. A failure that ALSO outlasts this in-place
# budget is not immediately treated as fatal either: see `run_batch`'s
# DEFERRED SECOND PASS (`_DEFERRED_PASS_PAUSE_S`), which gives it one more
# full attempt after a pause before finally treating it as persistent.
# Jitter matters, not just backoff: every thread of every one of a run's
# tasks can start within the same second, so synchronised retries (no
# jitter) would simply re-create the same stampede against DNS/S3 moments
# later.
_RETRY_ATTEMPTS = 6
_RETRY_BASE_DELAY_S = 4.0
_RETRY_MAX_DELAY_S = 60.0

# After a set/polygon exhausts its IN-PLACE retry budget above, `run_batch`
# does not treat it as persistent yet -- it defers it to a SECOND pass that
# runs once, after every other set in the batch and the `_recover` walk have
# both finished, following a pause of at least this long (#223 round 2,
# I4). Only a set that ALSO fails transiently in that second pass is a
# truly persistent failure. 60s: comfortably longer than either measured
# incident's outage duration (~1s and ~30s), so a deferred retry lands well
# after the network has had a chance to recover, without adding an
# unbounded wait to the batch.
_DEFERRED_PASS_PAUSE_S = 60.0

# Substrings (checked case-insensitively) that mark a source open/read
# failure message as TRANSIENT -- a network hiccup worth retrying on the
# SAME source, not evidence the source itself is unusable. Sourced from the
# measured 2026-09-20 incident (DNS: "Could not resolve host") plus the
# other curl/GDAL failure modes the same class of cluster-wide network
# blip produces. Kept as an explicit, documented list rather than a bare
# `except OSError` catch-all, because the failure modes in
# `_PERMANENT_ERROR_MARKERS` below (a genuinely unreadable object, a
# heterogeneous mosaic) must NOT be retried -- retrying those would just
# delay the correct candidate-walk behaviour by `_RETRY_ATTEMPTS` rounds of
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
    # #223 round 4 review, IMPORTANT R3-I2: real curl/HTTP2 wordings for a
    # trickling/aborted transfer, none of which name an HTTP status at all.
    "operation too slow",  # curl's low-speed-abort (GDAL_HTTP_LOW_SPEED_TIME/_LIMIT)
    "failure when receiving data from the peer",
    "send failure",
    "was not closed cleanly",  # HTTP/2 stream abort, e.g. "... INTERNAL_ERROR"
    # `open_tile_set` uses this EXACT phrase (see its I-A comment) when
    # `BuildVRT` fails but the per-key probe finds every key healthy and
    # `BuildVRT`'s own message names no concrete permanent cause -- the
    # outage most likely ended BETWEEN the BuildVRT attempt and the probe
    # (#223 round 3 review, IMPORTANT I-A). A synthetic marker, not
    # anything GDAL itself emits, so it can't collide with a real message.
    "buildvrt race window",
)

# Substrings/patterns that mark a failure as PERMANENT -- the source itself
# is unusable, so `_recover`'s candidate walk must proceed exactly as
# before this fix (no retry). "not recognized as being in a supported file
# format" is GDAL's message for a genuinely unreadable published object (2
# known in the real corpus).
#
# "gdalbuildvrt does not support" (#223 round 5 review, audit finding) --
# the SAME marker text `_PERMANENT_BUILDVRT_MARKERS` already checks, but
# that list is consulted ONLY inside `open_tile_set` to decide whether to
# set `real_cause = build_exc` in the first place; nothing previously made
# `_classify_error_chain` itself recognize this text on the exception it
# later has to classify. Without it, `open_tile_set` correctly identifies a
# genuinely heterogeneous-projection/band-count/band-dtype `BuildVRT`
# failure and chains it as `real_cause` -- but `_classify_error_chain`
# still read the resulting `TileSetOpenError` as `"unknown"` (verified
# directly: neither the outer message nor "gdalbuildvrt does not support
# heterogeneous projection: ..." matches any OLD marker here), and
# `_resolve_open_transient` then routed it through
# `_reclassify_unknown_open_failure`, whose per-key probe finds every key
# healthy (heterogeneity is a MOSAIC-level property, invisible to a
# single-key `rasterio.open`) and returns `True` -- silently reclassifying
# a real, reproducible tile-set defect as a transient network race, to be
# retried/deferred and, worst case, to fail the whole array task instead
# of simply walking that polygon's candidate list. The existing tests for
# this path (`test_open_tile_set_still_treats_a_genuinely_heterogeneous_
# projection_as_permanent`) only asserted `_is_transient_error_chain(...)
# is False`, which is ALSO true for `"unknown"` and so never caught this.
#
# "buildvrt failure reproduced on healthy keys" (#223 round 5 review,
# IMPORTANT R4-I1) -- `open_tile_set`'s backstop same-inputs `BuildVRT`
# retry (see its own comment) wraps an IDENTICALLY-reproduced failure in an
# exception carrying this EXACT phrase, precisely BECAUSE `build_exc` itself
# (a bare, unmarked message like "Can't open /vsicurl/a.tif.") carries no
# signal here either -- without an explicit marker, that case also read
# `"unknown"`, and `_reclassify_unknown_open_failure` reclassified it
# TRANSIENT for the same reason as above (every key opens fine
# individually), so the task retried/deferred/failed forever blaming "the
# network" for what a same-inputs retry had already proven was a genuine,
# reproducible defect.
_PERMANENT_ERROR_MARKERS = (
    "not recognized as being in a supported file format",
    "gdalbuildvrt does not support",
    "buildvrt failure reproduced on healthy keys",
)

# HTTP status classification, shared by the string-level `_is_transient_
# error` and `_classify_error_chain`'s per-level walk below. #223 round 3
# review, IMPORTANT I-C: round 2 treated EVERY `CPLE_HttpResponseError` as
# transient unless it named 404/403, which wrongly retried (then deferred,
# then permanently failed the whole task over) a 400/401/410/416 -- genuine
# client-side/object-level errors, not transport hiccups. Only "0" (no
# real HTTP response at all -- a connection/DNS-level failure surfacing
# through GDAL's generic "HTTP response code ...: 0" wording, not a real
# status), 408 (request timeout), 429 (throttling), and any 5xx
# (server-side) are transient; every OTHER 4xx is permanent.
#
# Anchored on a status/response/error/code CUE within 12 characters of the
# digits, not a bare number anywhere in `text` -- `TileSetOpenError`'s own
# message embeds the full `ts.keys` URLs (port numbers, and "127.0.0.1"'s
# own "0" octets, both otherwise indistinguishable from a real status
# code), so an unanchored `\b0\b`/`\b4\d\d\b` false-positived on a LOOPBACK
# IP's "0" octet inside a URL during round 3's own real-GDAL test
# (verified directly: a good+404 mixed set misclassified transient via the
# "0" in "127.0.0.1"). The two regexes can't both match the same digit
# token, so checking transient first is sufficient -- no ordering
# ambiguity.
_TRANSIENT_HTTP_CODE_RE = re.compile(
    r"(?:response code|error code|status code|http error)\D{0,12}\b(0|408|429|5\d\d)\b"
)
_PERMANENT_HTTP_CODE_RE = re.compile(
    r"(?:response code|error code|status code|http error)\D{0,12}\b4\d\d\b"
)
# #223 round 4 review, MINOR M1: `_TRANSIENT_HTTP_CODE_RE`'s 12-character gap
# is far too short for GDAL's OTHER real wording, "HTTP response code on
# <url>: 0" -- the URL between "code on " and the trailing ": 0" is
# routinely 60-100+ characters. Anchored on "response code on <token>:"
# specifically (not a bare gap allowance, which would reopen the same
# false-positive risk `_TRANSIENT_HTTP_CODE_RE`'s own anchoring exists to
# avoid) -- `\S+` is greedy, so it matches up to the LAST `:` in the
# message, which is exactly the one immediately before the trailing code.
_TRANSIENT_HTTP_CODE_ON_URL_RE = re.compile(r"response code on \S+:\s*(0|408|429|5\d\d)\b")


def _http_status_is_transient(text: str) -> bool | None:
    """Classify an HTTP status embedded in `text` (already lower-cased):
    `True` (0/408/429/5xx), `False` (any other 4xx), or `None` if no
    recognizable status-code phrasing is present in `text` at all --
    distinct from `False`, since "no signal here" and "an explicit
    permanent signal here" must be told apart by `_classify_error_chain`'s
    UNKNOWN handling (#223 round 3, C-A)."""
    if _TRANSIENT_HTTP_CODE_RE.search(text) or _TRANSIENT_HTTP_CODE_ON_URL_RE.search(text):
        return True
    if _PERMANENT_HTTP_CODE_RE.search(text):
        return False
    return None


def _is_transient_error(message: str) -> bool:
    """Classify a source open/read failure MESSAGE STRING as TRANSIENT
    (retry the SAME source) vs PERMANENT (advance `_recover`'s candidate
    walk, exactly as pre-#223-transient-retry). A plain boolean, so
    "permanent" and "no signal at all" both read as `False` here -- most
    callers with an actual exception in hand should go through
    `_classify_error_chain`/`_is_transient_error_chain`/`_is_permanent_
    error_chain` instead, which distinguish those two and walk the whole
    cause chain, not just one message (#223 round 2, C2)."""
    text = str(message).lower()
    if any(marker in text for marker in _PERMANENT_ERROR_MARKERS):
        return False
    if any(marker in text for marker in _TRANSIENT_ERROR_MARKERS):
        return True
    verdict = _http_status_is_transient(text)
    if verdict is not None:
        return verdict
    return False


# Bounded depth for `_error_chain_messages`'s __cause__/__context__ walk --
# real GDAL/rasterio chains observed in practice are 2-3 deep; this is only
# a defensive backstop against an unbounded or (impossible, but cheap to
# guard) cyclic chain, not a tuned value.
_MAX_ERROR_CHAIN_DEPTH = 8


def _error_chain_messages(exc: BaseException) -> list[tuple[str, str]]:
    """Walk `exc`'s `__cause__`/`__context__` chain (bounded depth,
    de-duplicated by identity) and return each level's `(type_name,
    str(...))`. See `_classify_error_chain`, the actual classifier that
    consumes this."""
    levels: list[tuple[str, str]] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    depth = 0
    while current is not None and depth < _MAX_ERROR_CHAIN_DEPTH and id(current) not in seen:
        seen.add(id(current))
        levels.append((type(current).__name__, str(current)))
        current = current.__cause__ or current.__context__
        depth += 1
    return levels


def _classify_error_chain(exc: BaseException) -> str:
    """Classify a RAISED EXCEPTION's WHOLE `__cause__`/`__context__` chain
    (`_error_chain_messages`) as `"transient"`, `"permanent"`, or
    `"unknown"` -- the first level (outer to inner) carrying an EXPLICIT
    signal wins. A `CPLE_HttpResponseError`'s own `str()` is always exactly
    its status text (e.g. "HTTP response code: 404" -- verified real GDAL
    3.12.3), which `_http_status_is_transient`'s phrase-anchored regex
    already matches directly; no separate class-name special case is
    needed (nor, per #223 round 3's own false-positive finding, wanted --
    see `_TRANSIENT_HTTP_CODE_RE`'s docstring on why an UNANCHORED status
    check is unsafe against a message that also embeds a URL).

    `"unknown"` is the (#223 round 3 review, CRITICAL C-A) case this
    function exists to make explicit: a network blip DURING an active
    block fetch on an ALREADY-OPEN dataset -- as opposed to at OPEN time --
    surfaces through real GDAL/rasterio as a generic `RasterioIOError('Read
    failed. See previous exception for details.')` wrapping
    `CPLE_AppDefinedError`s like "IReadBlock failed .../TIFFReadEncodedTile
    Read error ...; got 0 bytes, expected N" -- verified (real GDAL
    3.12.3/rasterio 1.5.0) to carry NO HTTP/curl text at all, for a 503, a
    connection reset, AND a 404 alike. Every OPEN-time call site
    (`_probe_keys_for_real_cause`'s single-key `rasterio.open`, `open_tile_
    set`'s single-key branch, the multi-key `BuildVRT` probe) reliably gets
    an explicit `"transient"`/`"permanent"` verdict instead -- `"unknown"`
    is specific to a read on a dataset that already opened successfully,
    and its ONLY caller, `_retry_compute_one`, must NOT default it to
    permanent (a real, reproducible content gap normally comes back as
    nodata -- `_compute_one` returning `None` -- not as a raised exception;
    an exception at read time on an already-open set is itself evidence of
    a transport-level hiccup, not a data problem) -- see `_retry_compute_
    one`'s own re-classification logic for how `"unknown"` is resolved.

    `_is_transient_error_chain`/`_is_permanent_error_chain` below are
    boolean views for callers (every OPEN-time site) that never legitimately
    see `"unknown"` and don't need to handle it specially."""
    for _type_name, message in _error_chain_messages(exc):
        text = message.lower()
        if any(marker in text for marker in _PERMANENT_ERROR_MARKERS):
            return "permanent"
        if any(marker in text for marker in _TRANSIENT_ERROR_MARKERS):
            return "transient"
        verdict = _http_status_is_transient(text)
        if verdict is not None:
            return "transient" if verdict else "permanent"
    return "unknown"


def _is_transient_error_chain(exc: BaseException) -> bool:
    """Boolean view of `_classify_error_chain`: True iff EXPLICITLY
    transient. For an OPEN-time exception (the only kind this is meant
    for), "not transient" and "permanent" coincide -- see `_classify_
    error_chain`'s docstring for why a READ-time exception is different
    and must not use this function without first ruling out `"unknown"`."""
    return _classify_error_chain(exc) == "transient"


def _is_permanent_error_chain(exc: BaseException) -> bool:
    """Boolean view of `_classify_error_chain`: True iff EXPLICITLY
    permanent (an explicit marker/status matched) -- distinct from `not
    _is_transient_error_chain(exc)`, which is also True for `"unknown"`."""
    return _classify_error_chain(exc) == "permanent"


def _backoff_delay(attempt: int) -> float:
    """Delay (seconds) before retry attempt `attempt` (1-indexed): exponential
    backoff from `_RETRY_BASE_DELAY_S`, capped at `_RETRY_MAX_DELAY_S`, with
    +/-50% jitter -- see `_RETRY_ATTEMPTS`'s module comment for why jitter
    is load-bearing here (every thread of every task in the measured
    incident started within the same second)."""
    base = min(_RETRY_BASE_DELAY_S * (2 ** (attempt - 1)), _RETRY_MAX_DELAY_S)
    return base * random.uniform(0.5, 1.5)


def _sleep(seconds: float) -> None:
    """The retry backoff's (and the deferred-pass pause's) sleep, as a
    module-level seam tests monkeypatch so a retry-exhaustion or
    deferred-pass test doesn't actually wait."""
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


# Substrings naming a CONCRETE permanent condition in `gdal.BuildVRT`'s OWN
# raised exception (`build_exc`) -- verified real text: "gdalbuildvrt does
# not support heterogeneous projection: expected <CRS>, got <CRS>." Used
# ONLY as a fallback when `_probe_keys_for_real_cause` finds every key
# healthy (#223 round 3 review, IMPORTANT I-A) -- `build_exc`'s own generic
# "Can't open <url>." carries no such signal and must not be mistaken for
# one.
#
# Widened to the shared GDAL prefix, not just "heterogeneous projection"
# (#223 round 4 review, IMPORTANT R3-I1): under `strict=True`, `BuildVRT`
# ALSO deterministically rejects a heterogeneous band COUNT ("gdalbuildvrt
# does not support heterogeneous band numbers") and a heterogeneous band
# DATA TYPE ("... heterogeneous band data type") the same way it rejects a
# heterogeneous projection -- all three are real, reproducible defects in
# the tile set, not a network timing issue, but the narrower marker called
# them a "buildvrt race window" (transient), which retried, deferred, and
# then permanently failed the whole task over a condition that was never
# going to resolve on retry.
_PERMANENT_BUILDVRT_MARKERS = ("gdalbuildvrt does not support",)


def _build_exc_is_permanent(build_exc: Exception | None) -> bool:
    """True iff `gdal.BuildVRT`'s OWN raised exception names a CONCRETE
    permanent condition (see `_PERMANENT_BUILDVRT_MARKERS`) -- see
    `open_tile_set`'s I-A handling for why this is checked only as a
    fallback, and only when every key opens fine on its own."""
    if build_exc is None:
        return False
    text = str(build_exc).lower()
    return any(marker in text for marker in _PERMANENT_BUILDVRT_MARKERS)


def _clear_vsicurl_cache(ts: TileSet) -> None:
    """Clear GDAL's in-process `/vsicurl/` cache for every key in `ts.keys`
    before a disambiguation probe or a "fresh" reproduction read (#223
    round 4 review, CRITICAL R3-C1). Without this, `_probe_keys_for_real_
    cause`'s probe open and `_reclassify_unknown_read_failure`'s "fresh"
    open+re-read are served from GDAL's OWN cache of the file's
    headers/directory -- fetched the FIRST time the key was opened,
    BEFORE the outage started -- so both report "healthy" even while the
    real network is still down. Reproduced directly (probe13/14.py): an
    outage that starts during a read and PERSISTS through reclassification
    still resolved PERMANENT, because the probe's plain `rasterio.open`
    only needs the (cached) header, and the "fresh" open+read that
    follows a healthy probe reused the SAME cached header, so only the
    actual block read hit the (still-down) network, producing the SAME
    unclassifiable "unknown" shape a second time -- which the pre-fix
    code took as confirmation of a reproducible PERMANENT problem rather
    than a still-ongoing TRANSIENT one. `gdal.VSICurlPartialClearCache`
    forces the NEXT open/read of each key to hit the network for real."""
    for key in ts.keys:
        try:
            gdal.VSICurlPartialClearCache(key)
        except Exception as exc:  # noqa: BLE001 - best-effort; a key that isn't a real
            # vsicurl-style URL (e.g. a local path, or a synthetic test
            # key) raises "Missing url parameter" here rather than a
            # no-op -- this is a cache hygiene step, not a correctness
            # requirement for a key that was never cached in the first
            # place, so a failure here must never mask the REAL
            # classification work the caller is about to do. #223 round 5
            # review, MINOR M-1: silently swallowing this unconditionally
            # hid the one case worth an operator's attention -- a REAL
            # `/vsicurl/http(s)://...` key that failed to clear (the
            # cache-clear this whole function exists to guarantee, R3-C1)
            # -- so that case logs at WARNING. Verified directly: a plain
            # local path (no `/vsicurl/` prefix at all) does NOT even raise
            # here -- `VSICurlPartialClearCache` silently no-ops for it --
            # but a `/vsicurl/`-prefixed key with no real host (e.g. this
            # file's own pervasive synthetic `"/vsicurl/a.tif"` test keys)
            # DOES raise "Missing url parameter", and that is exactly the
            # "synthetic/non-URL test key" case meant to log at DEBUG only
            # -- checking the FULL `/vsicurl/http://` or `/vsicurl/https://`
            # prefix (not just `/vsicurl/`) is what actually distinguishes
            # the two, since a bare `/vsicurl/` prefix alone is common to
            # both.
            level = (
                logging.WARNING
                if key.startswith(("/vsicurl/http://", "/vsicurl/https://"))
                else logging.DEBUG
            )
            logging.getLogger(__name__).log(
                level, "  _clear_vsicurl_cache: could not clear cache for %s: %s", key, exc
            )


def _probe_keys_for_real_cause(ts: TileSet) -> Exception | None:
    """When `gdal.BuildVRT` fails to build `ts.keys`' mosaic,
    `gdal.GetLastErrorMsg()` is EMPTY at that point even under real GDAL
    (verified for both an all-DNS-failure and an all-404 key list, #223
    round 2 review) -- it carries no classifiable cause on its own. Clears
    each key's `/vsicurl/` cache first (`_clear_vsicurl_cache`, #223 round
    4, R3-C1) so this probe cannot be spuriously "healthy" from a
    pre-outage cached header, then probes EVERY key individually with
    `rasterio.open` (under the same reduced-retry env `_attempt`'s own
    open uses -- see `_IN_PROCESS_RETRY_ENV`) and returns ONE
    representative exception, chosen this way (#223 round 5 review,
    CRITICAL R4-C1, superseding round 3's M-B): PERMANENT only if EVERY
    failing key's own exception chain classifies EXPLICITLY permanent --
    any key whose chain is transient OR unknown makes the WHOLE probe
    return THAT key's exception immediately, as evidence the set is not
    (yet) confirmed permanent, rather than accumulating it as a candidate
    "permanent" cause. `_classify_error_chain`'s own doctrine (an unknown
    verdict is never itself permanent, #223 round 5) applies here just as
    much as it does to the caller's own handling of this function's
    return value -- the OLD check (`_is_transient_error_chain(exc)`, i.e.
    `_classify_error_chain(exc) == "transient"`) let an `"unknown"`-chain
    key fall through to `first_permanent` right alongside a genuinely
    permanent one, which is exactly what silently promoted a persistent
    low-speed brown-out (curl aborting a trickling header fetch --
    "TIFFReadDirectory: Failed to read directory at offset N", carrying no
    HTTP/curl text at all, hence unclassifiable on its own) to a
    confirmed-PERMANENT verdict at BOTH `_reclassify_unknown_read_failure`
    and `_reclassify_unknown_open_failure` (reproduced directly,
    `probe22.py`). Classifying from just the FIRST failing key (the round
    1/2 behaviour) could call a set permanent because its first key
    genuinely 404s while a later key is merely mid-outage, or miss a real
    404 sitting behind an earlier key's transient blip -- the ANY-non-
    permanent-short-circuits / ALL-must-be-permanent-to-accumulate shape
    here still avoids both, while also refusing to call a set permanent
    off the back of an unclassifiable key. Returns `None` if every key
    opens fine on its own -- `BuildVRT` failed for some other reason (see
    `open_tile_set`'s I-A handling of that case), and the caller falls
    back to classifying `BuildVRT`'s own raised exception/message
    instead."""
    _clear_vsicurl_cache(ts)
    first_permanent: Exception | None = None
    for key in ts.keys:
        try:
            with rasterio.Env(**_IN_PROCESS_RETRY_ENV), rasterio.open(key):
                pass
        except Exception as exc:  # noqa: BLE001 - returned to the caller, never raised here
            if _classify_error_chain(exc) != "permanent":
                return exc
            if first_permanent is None:
                first_permanent = exc
    return first_permanent


@contextmanager
def open_tile_set(ts: TileSet):
    """ONE open per tile set: a single tile, or an in-memory mosaic of one
    project's same-zone tiles. Warped to EPSG:5070 at native GSD with
    nearest resampling, exactly as the per-tile path always did, so
    in-bounds single-tile reads stay bit-identical.

    Raises `TileSetOpenError` (a `RasterioIOError` subclass whose
    `__cause__` is the real per-key exception from
    `_probe_keys_for_real_cause`) if the multi-key mosaic fails to build,
    or a plain `RasterioIOError` from `rasterio.open` for the single-key
    case -- the CALLER (`_attempt`/`_retry_compute_one` in `run_batch`) is
    responsible for classifying either via `_is_transient_error_chain` and
    retrying accordingly; this function itself never retries."""
    vsimem = None
    path = ts.keys[0]
    if len(ts.keys) > 1:
        vsimem = f"/vsimem/dprst_depth_set_{uuid.uuid4().hex}.vrt"
        # `strict=True` (#223 round 2 review, I1): the DEFAULT `BuildVRT`
        # silently SKIPS any key it cannot open, or whose properties (e.g.
        # CRS) differ from the rest, and returns a mosaic of whatever's
        # left -- with NO error at all (verified: a good key + a
        # DNS-failing key, or a good key + a 404 key, each produced a
        # working 1-file VRT covering only the good key; verified with a
        # real UTM 14N + UTM 15N pair that a genuinely heterogeneous-CRS
        # set does the SAME thing, NOT the previously-documented "BuildVRT
        # returns None for mixed CRS"). That silent partial mosaic is
        # worse than a hard failure: a polygon whose window touches the
        # DROPPED key reads void/nodata from its own PRIMARY set and either
        # walks to a lower-ranked candidate or ships a partial-interior
        # depth as `method="measured"`, with no signal anywhere that one
        # of the primary's own keys never made it into the mosaic.
        # `strict=True` makes GDAL treat any unopenable or heterogeneous
        # source as FATAL instead -- a set whose keys all open cleanly and
        # agree on CRS/etc. builds EXACTLY as before. **This IS a
        # trade-off, not "unaffected" (#223 round 3 review, MINOR M-A):**
        # for a multi-key set that DOES have one bad key, `strict=True`
        # demotes/retries the WHOLE set rather than silently mosaicking the
        # other, healthy keys -- a polygon whose window happens to fall
        # entirely on the healthy keys still pays for the one bad key. That
        # is deliberate: the alternative (accepting GDAL's silent partial
        # mosaic) is exactly the worse failure mode this fix replaces, and
        # a set-level retry/deferral/candidate-walk already exists to
        # recover from it -- there is no silent per-key partial acceptance
        # path, on purpose.
        try:
            vrt_ds = gdal.BuildVRT(vsimem, list(ts.keys), options=gdal.BuildVRTOptions(strict=True))
        except Exception as exc:  # noqa: BLE001 - GDAL raises under gdal.UseExceptions(); converted below
            vrt_ds = None
            build_exc: Exception | None = exc
        else:
            build_exc = None
        if vrt_ds is None:
            # `GetLastErrorMsg()` is empty here (see `_probe_keys_for_real_
            # cause`'s docstring) -- probe each key individually instead;
            # THAT'S what actually surfaces a classifiable real cause.
            # Without this, a multi-key set's DNS failure classified
            # PERMANENT -- QUIETER than the pre-round-1 bare `RuntimeError`
            # bug, which at least forced an ERROR-level `n_compute_error`
            # summary (#223 round 2, CRITICAL C1).
            probed = _probe_keys_for_real_cause(ts)
            if probed is not None:
                real_cause: Exception = probed
            elif _build_exc_is_permanent(build_exc):
                # Every key opens fine on its own, but `BuildVRT`'s OWN
                # message names a CONCRETE permanent condition (e.g. a
                # genuinely heterogeneous projection/CRS) -- a real defect
                # in this tile set, not a network timing issue.
                real_cause = build_exc  # type: ignore[assignment]
            else:
                # Every key opens fine on its own AND `BuildVRT` named no
                # concrete permanent cause -- classifying this PERMANENT
                # (the round 1/2 fallback: `probed or build_exc`, i.e.
                # `build_exc`'s own generic "Can't open <url>." text) is a
                # RACE: the outage most likely ended BETWEEN `BuildVRT`'s
                # attempt and this probe (#223 round 3 review, IMPORTANT
                # I-A) -- verified directly: forcing the outage to clear
                # right before the probe still left `build_exc` as a bare,
                # unclassifiable "Can't open ..." RuntimeError with no
                # network signal of its own, which the OLD fallback would
                # have taken as permanent.
                #
                # BACKSTOP (#223 round 4 review, IMPORTANT R3-I1): retry
                # `BuildVRT` ONCE more, purely for classification -- its
                # result is never reused even on success. `_PERMANENT_
                # BUILDVRT_MARKERS`'s prefix covers every heterogeneous
                # condition BuildVRT itself NAMES, but not every real
                # permanent condition necessarily gets named (untested
                # combinations could exist); an IDENTICAL failure message
                # on this second attempt, with every key still healthy, is
                # a REPRODUCIBLE condition, not a timing race that would
                # have cleared between two back-to-back attempts.
                retry_vsimem = f"/vsimem/dprst_depth_set_{uuid.uuid4().hex}.vrt"
                try:
                    retry_vrt_ds = gdal.BuildVRT(
                        retry_vsimem, list(ts.keys), options=gdal.BuildVRTOptions(strict=True)
                    )
                except Exception as retry_exc:  # noqa: BLE001 - classification-only
                    retry_message: str | None = str(retry_exc)
                else:
                    retry_message = None
                    if retry_vrt_ds is not None:
                        retry_vrt_ds = None  # flush -- see the module comment above on why
                        gdal.Unlink(retry_vsimem)  # never used -- this retry is classification-only
                if retry_message is not None and retry_message == str(build_exc):
                    # #223 round 5 review, IMPORTANT R4-I1: `build_exc` itself
                    # is a bare, unmarked message (e.g. "Can't open
                    # /vsicurl/a.tif.") -- setting `real_cause = build_exc`
                    # directly left `_classify_error_chain` reading this
                    # REPRODUCED, every-key-healthy failure as `"unknown"`,
                    # and `_resolve_open_transient` -> `_reclassify_unknown_
                    # open_failure` then reclassified it TRANSIENT (its own
                    # probe finds every key healthy too, for the same
                    # reason) -- so a genuine, reproducible defect retried,
                    # deferred, and ultimately failed the whole array task
                    # forever, blaming "the network" (reproduced directly,
                    # `probe25.py`). An identical failure on a same-inputs
                    # retry with every key healthy is PRECISELY the other
                    # half of this round's permanence doctrine (a failure
                    # that reproduces while every key's header opens cleanly
                    # on a cleared cache) -- so it gets an EXPLICIT permanent
                    # marker here, not `build_exc`'s own unmarked text.
                    # `build_exc` is preserved as `__cause__` for diagnostics
                    # (never reached by `_classify_error_chain`, since the
                    # marker matches at this, the outer, level first).
                    real_cause = RasterioIOError(
                        f"buildvrt failure reproduced on healthy keys: BuildVRT failed "
                        f"identically ({build_exc!r}) on a same-inputs retry for tile set "
                        f"project={ts.project!r} with every key probed healthy -- a "
                        f"genuine, reproducible defect, not a network timing race"
                    )
                    real_cause.__cause__ = build_exc
                else:
                    # Raise a synthetically-transient cause (see
                    # `_TRANSIENT_ERROR_MARKERS`'s "buildvrt race window"
                    # entry) so the caller's existing retry logic retries
                    # the WHOLE build, rather than demoting to a
                    # lower-ranked candidate over what was likely already
                    # fixed (or, if the retry itself now failed
                    # differently, is still genuinely ambiguous).
                    real_cause = RasterioIOError(
                        f"BuildVRT failed for tile set project={ts.project!r} but "
                        f"every key opened fine on a follow-up probe, BuildVRT "
                        f"named no concrete permanent cause, and a same-inputs "
                        f"retry did not reproduce an identical failure -- "
                        f"treating as a buildvrt race window (the outage likely "
                        f"ended between the first BuildVRT attempt and the "
                        f"probe/retry); build_exc={build_exc!r} retry={retry_message!r}"
                    )
            gdal_msg = gdal.GetLastErrorMsg() or "<no GDAL error message>"
            raise TileSetOpenError(
                f"gdal.BuildVRT failed for tile set project={ts.project!r} "
                f"keys={ts.keys!r} -- cannot build the mosaic (GDAL: {gdal_msg}; "
                f"probed cause: {real_cause!r})"
            ) from real_cause
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


def _reclassify_unknown_read_failure(ts: TileSet, geom, logger: logging.Logger) -> bool:
    """Disambiguate an UNKNOWN read-time `RasterioIOError` (#223 round 3
    review, CRITICAL C-A) -- one whose whole cause chain carries neither an
    explicit transient NOR an explicit permanent signal (`_classify_error_
    chain(exc) == "unknown"`), which is exactly what a real network blip
    DURING an active block fetch on an already-open dataset looks like
    (verified: a 503, a connection reset, and a 404 partway through a read
    all produce an identical generic `CPLE_AppDefinedError`/
    `TIFFReadEncodedTile` chain with no HTTP/curl text at all).

    Returns True (transient) UNLESS:
    - `_probe_keys_for_real_cause` finds a concrete cause on one of `ts`'s
      keys that itself classifies EXPLICITLY permanent, or
    - every key opens fine on its own AND an independent, completely FRESH
      `open_tile_set` + read of the SAME window (not reusing the caller's
      possibly-tainted already-open connection) reproduces either an
      explicit permanent signal, or the SAME kind of unclassifiable
      failure again -- a real, reproducible problem unrelated to a
      lingering network blip that merely hasn't cleared yet.
    A fresh attempt that instead reproduces an EXPLICIT transient signal
    (the network is still visibly down) stays transient -- it just hasn't
    recovered yet, which is not evidence of a data problem either.

    #223 round 5 review, CRITICAL R4-C1: when `probed is not None`, this
    used to `return _is_transient_error_chain(probed)` -- `False` for BOTH
    an explicitly permanent verdict AND an unknown one, wrongly treating
    "unknown" as grounds to stop retrying. `_probe_keys_for_real_cause`
    itself now only ever returns a non-`None` value that is either
    EXPLICITLY permanent (every failing key agreed) or evidence of a
    transient/unknown condition (see its own docstring) -- so the correct
    read here is `not _is_permanent_error_chain(probed)`: transient stays
    transient, unknown resolves transient too (the "unknown is never
    permanent by itself" doctrine), and ONLY an explicit permanent verdict
    resolves permanent. Reproduced directly (`probe22.py`): a persistent
    low-speed brown-out produced exactly this "unknown" probe result, and
    the OLD line here resolved it PERMANENT while the outage was ongoing.
    """
    probed = _probe_keys_for_real_cause(ts)  # clears each key's /vsicurl/ cache first, R3-C1
    if probed is not None:
        return not _is_permanent_error_chain(probed)
    # `_clear_vsicurl_cache` again, explicitly, right before this "fresh"
    # open -- #223 round 4, R3-C1's own reviewer-verified fix
    # (`cachefix.py`). Without it, the fresh `open_tile_set` below reuses
    # the cached header `_probe_keys_for_real_cause`'s own open just
    # populated, so only the SUBSEQUENT block read is a genuine network
    # round-trip -- if that read fails during a real, still-ongoing
    # outage, it reproduces the SAME unclassifiable shape, and this
    # function used to (wrongly) call that a reproducible PERMANENT
    # problem rather than the outage simply not having cleared yet.
    _clear_vsicurl_cache(ts)
    try:
        with rasterio.Env(**_IN_PROCESS_RETRY_ENV), open_tile_set(ts) as fresh_vrt:
            _compute_one(fresh_vrt, geom)
        return True  # reproduction succeeded (or found no valid interior) -- was transient
    except RasterioIOError as exc2:
        verdict = _classify_error_chain(exc2)
        if verdict == "transient":
            return True  # network still visibly down -- not yet recovered, not a data problem
        # `verdict` is "permanent" (an explicit signal appeared) or
        # "unknown" (the SAME unclassifiable symptom reproduced on a brand
        # new connection with healthy keys) -- either way, that is the
        # "reproduces while every key opens cleanly" case, and both count
        # as PERMANENT here.
        logger.warning(
            "  set=%s: unclassifiable read failure reproduced on a fresh "
            "connection (probe healthy, fresh-read verdict=%s: %s) -- "
            "treating as PERMANENT", ts.project, verdict, exc2,
        )
        return False


def _reclassify_unknown_open_failure(ts: TileSet, logger: logging.Logger) -> bool:
    """Disambiguate an UNKNOWN open-time `RasterioIOError`/`TileSetOpenError`
    (#223 round 4 review, IMPORTANT R3-I2) -- an open-time failure is
    USUALLY directly classifiable (round 3's own finding: a network
    failure at OPEN time carries a real "CURL error: ..."/"HTTP response
    code: ..." in its chain), but not always: a trickling header GET
    aborted mid-fetch by curl's low-speed-abort produces
    "TIFFReadDirectory: Failed to read directory at offset 8" -- another
    generic `CPLE_AppDefinedError`-family message with no HTTP/curl text,
    reproduced directly via `GDAL_HTTP_LOW_SPEED_TIME`. Same doctrine as
    `_reclassify_unknown_read_failure`, simplified for the open case:
    probe every key (which clears its own `/vsicurl/` cache first --
    `_clear_vsicurl_cache`, R3-C1); a key with its own concrete cause is
    classified from that. If every key now opens cleanly on a
    cache-cleared probe, that IS the fresh check (there is no separate
    "already open" connection to re-verify against, unlike the read
    case) -- treat it as a TRANSIENT timing race, the same doctrine I-A
    already established for the multi-key `BuildVRT`-probe case,
    generalized here to the single-key open path.

    #223 round 5 review, CRITICAL R4-C1 (open-time instance): same fix as
    `_reclassify_unknown_read_failure` -- `not _is_permanent_error_chain(
    probed)`, not `_is_transient_error_chain(probed)`, so a probed key
    that is itself unclassifiable ("unknown") resolves transient (retry),
    not permanent. Reproduced directly (`probe22.py`'s open-time variant):
    the SAME persistent low-speed brown-out demotes at open time too under
    the old check."""
    probed = _probe_keys_for_real_cause(ts)
    if probed is not None:
        return not _is_permanent_error_chain(probed)
    return True


def _retry_compute_one(
    vrt, ts: TileSet, idx, geom, logger: logging.Logger, bump, stop_event: threading.Event
) -> tuple[dict | None, bool]:
    """`_compute_one(vrt, geom)`, retrying a TRANSIENT `RasterioIOError` on
    the SAME already-open set/polygon (bounded backoff, see
    `_backoff_delay`; ~2 minute in-place budget) instead of dropping the
    polygon to `_recover`'s candidate walk over what may be nothing more
    than a network blip.

    Returns `(result, gave_up_transiently)`. `gave_up_transiently` is True
    when either every one of `_RETRY_ATTEMPTS` also failed transiently, or
    `stop_event` was already set by a SIBLING set/polygon in this SAME pass
    (#223 round 2, I3 -- don't ALSO burn this polygon's own ~2 minute
    budget confirming what a sibling already found). In that case `result`
    is always `None` and the CALLER must NOT advance this polygon to a
    lower-ranked candidate here -- the primary is still the correct
    source; `run_batch` gives it ONE deferred retry, after a pause, before
    treating it as truly persistent (#223 round 2, I4). A PERMANENT
    `RasterioIOError` is re-raised immediately with no retry -- unchanged
    pre-existing behaviour, left for the caller's own try/except to handle
    exactly as before this fix. An UNKNOWN one (#223 round 3 review,
    CRITICAL C-A -- see `_reclassify_unknown_read_failure`) is disambiguated
    ONCE per call (cached in `unknown_verdict`, not re-probed on every
    attempt -- the disambiguation itself does real network I/O) and then
    treated as whichever it resolves to for the rest of this call's
    attempts. Any OTHER exception is re-raised immediately, unchanged.
    """
    unknown_verdict: bool | None = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        if stop_event.is_set():
            logger.warning(
                "  set=%s idx=%s: TRANSIENT retry abandoned early -- a sibling "
                "set/polygon in this pass already confirmed the network is down",
                ts.project, idx,
            )
            return None, True
        try:
            return _compute_one(vrt, geom), False
        except RasterioIOError as exc:
            verdict = _classify_error_chain(exc)
            if verdict == "unknown":
                if unknown_verdict is None:
                    unknown_verdict = _reclassify_unknown_read_failure(ts, geom, logger)
                    logger.warning(
                        "  set=%s idx=%s: unclassifiable read failure (%s) -- "
                        "resolved to %s via probe/reproduction", ts.project, idx,
                        exc, "TRANSIENT" if unknown_verdict else "PERMANENT",
                    )
                if not unknown_verdict:
                    raise
            elif verdict == "permanent":
                raise
            if attempt >= _RETRY_ATTEMPTS:
                logger.warning(
                    "  set=%s idx=%s: TRANSIENT read failure exhausted this pass's "
                    "in-place retry budget (%d attempts, %s) -- deferring to a "
                    "second pass, NOT advancing to a lower-ranked candidate",
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


def _resolve_open_transient(ts: TileSet, exc: RasterioIOError, logger: logging.Logger) -> bool:
    """Classify an open-time exception as transient (`True`) or permanent
    (`False`) for `_attempt`'s open-retry loop, resolving an UNKNOWN
    verdict via `_reclassify_unknown_open_failure` (#223 round 4 review,
    IMPORTANT R3-I2) instead of the OLD implicit "not transient ->
    permanent" default that also caught `"unknown"`. `_attempt` calls this
    exactly ONCE per caught exception and reuses the result -- each
    UNKNOWN resolution does a real network probe, so classifying the same
    `exc` twice would double that cost for no benefit."""
    verdict = _classify_error_chain(exc)
    if verdict == "unknown":
        resolved = _reclassify_unknown_open_failure(ts, logger)
        logger.warning(
            "  set=%s: unclassifiable open failure (%s) -- resolved to %s "
            "via probe/reproduction", ts.project, exc,
            "TRANSIENT" if resolved else "PERMANENT",
        )
        return resolved
    return verdict == "transient"


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
    (empty interior; HTTP 404/401/etc. -- any 4xx except 408/429; an
    unreadable object; a heterogeneous `BuildVRT` mosaic -- see
    `_classify_error_chain`), walks its remaining ranked `candidates`,
    ending at the 10 m seamless tile. Counters stay split (#173 PR#177 FIX
    2, transient-retry fix follow-on): read failures (expected PERMANENT
    failures + empty interiors, WARNING) vs compute errors (bugs, ERROR)
    vs transient retries vs polygons with no usable source at all.

    A TRANSIENT failure (a network hiccup -- DNS, connect, timeout,
    connection reset, HTTP 0/408/429/5xx; see `_classify_error_chain`,
    which classifies the exception's WHOLE cause chain, not just its own
    message, into `"transient"`/`"permanent"`/`"unknown"`) is a DIFFERENT
    code path entirely, both at the set-open level and at the per-polygon
    `_compute_one` level: it is retried `_RETRY_ATTEMPTS` times with
    backoff+jitter (`_backoff_delay`, ~2 minute in-place budget, with
    GDAL's OWN retry layer disabled for these attempts so our loop -- not
    GDAL's -- owns that budget; see `_IN_PROCESS_RETRY_ENV`) on the SAME
    source, and NEVER handed to `_recover`'s candidate walk even if that
    in-place budget is exhausted. The doctrine (CLAUDE.md's dprst_depth
    bullet) is that a transient failure is not evidence the primary is
    unusable -- the network is what's broken, not the source ranking. A
    READ-time failure (on an already-open set) that classifies `"unknown"`
    -- carrying NEITHER signal, which real GDAL produces identically for a
    503, a connection reset, AND a 404 partway through a block fetch (#223
    round 3 review, CRITICAL C-A) -- is disambiguated via
    `_reclassify_unknown_read_failure` rather than defaulting to permanent;
    see that function's docstring.

    A set/polygon that exhausts its in-place budget is DEFERRED, not
    immediately failed: once every OTHER set in this batch and the
    `_recover` walk have both finished, `run_batch` pauses at least
    `_DEFERRED_PASS_PAUSE_S` and gives every deferred item ONE more full
    attempt. Only if THAT also fails transiently does `run_batch` FAIL THE
    WHOLE TASK LOUDLY (raise, and write NO `out_parquet`) rather than
    silently letting those polygons fall through to a lower-ranked
    candidate. The deferred pass exists because the in-place budget can be
    -- and in a real second incident WAS -- shorter than the outage it's
    meant to survive (job 4532393: a ~30s DNS outage measured
    21:49:54-21:50:24, while the then-5-attempt/~20s budget exhausted by
    21:50:16, 8s before the network actually recovered; the SAME set opened
    fine again moments later). A shared `threading.Event` lets a
    still-in-flight sibling in the SAME pass bail out of its own budget
    early once ANY set/polygon in that pass has confirmed the network is
    down, rather than every one of them independently burning a full ~2
    minutes to reach the same conclusion (#223 round 2, I3); it is reset
    before the deferred pass so THAT pass gets its own fair first attempt.

    This is what the measured 2026-09-20 incidents needed: the FIRST (job
    4529029, 532 "Could not resolve host" errors across 5 nodes within one
    second) is what every polygon's PERMANENT-failure handling used to
    treat identically to a genuine miss, silently demoting 4,822 of 5,535
    tjc polygons (87%) off their correctly-ranked primary source, with 578
    single-candidate polygons losing their row outright. The operator's
    remedy for a task that now genuinely fails is simply to resubmit that
    array index once the network recovers (see
    `slurm_batch/HPC_REFERENCE.md`'s dprst_depth Recovery section for the
    downstream-stage caveat).

    The counters count ATTEMPTS, not polygons: a polygon that walks all of
    P1 -> P2 -> 10m before succeeding adds 2 to `n_read_failure` (one per
    PERMANENTLY-failed candidate) and 1 to `n_recovered`, not 1 total. An
    empty interior (`_compute_one` returns `None` without raising) is
    folded into `n_read_failure` alongside the PERMANENT exception-raising
    cases -- both mean "this candidate did not have the polygon's data",
    the same signal by a different mechanism. `n_transient_retry` counts
    RETRIES that happened (not give-ups, and never a candidate-walk step) --
    a transient failure that eventually succeeds, whether in-place or on
    the deferred pass, inflates NEITHER `n_read_failure` nor `n_recovered`;
    it was never treated as a failure of that source at all. `n_recovered`
    DOES count a candidate that only resolved on the deferred pass (#223
    round 3 review, IMPORTANT I-B) -- it is a recovery (this idx's PRIMARY
    still failed permanently; a lower-ranked candidate is what shipped it)
    regardless of which pass finally got the candidate a clean read.

    The final summary line is escalated (#173 FIX 3, restored in the #223
    fix-round-1 rewrite; degradation term added in review round 3; retry
    term added in round 2) ONLY when `run_batch` reaches the point of
    writing `out_parquet` at all -- a failure that persists through the
    deferred pass skips the summary/escalation logic entirely and raises
    instead (see above), which is a strictly LOUDER signal than any of
    these levels: ERROR if `n_compute_error > 0` (a real bug must never
    hide behind the expected read-failure rate); else WARNING if ANY of
    (a) the written fraction of polygons drops below 90% (polygons LOST
    entirely -- a mass PERMANENT read failure), (b) more than 10% of
    planned polygons needed `_recover` at all (polygons that still
    SHIPPED, via the fallback ladder, but only because their primary set
    failed to open/read PERMANENTLY), or (c) `n_transient_retry > 0` (the
    batch hit at least one real network hiccup -- worth a human glance even
    when every affected polygon still resolved correctly on its primary).
    Term (b) is the one that actually catches "every 1 m set failing and
    every polygon recovering to 10 m": every recovered polygon is still in
    `out`, so `success_fraction` alone stays at 1.0 for that exact scenario
    and would otherwise log at INFO -- this is precisely the failure mode
    the round-3 `open_tile_set`/BuildVRT-flush bug produced (every
    multi-tile-key primary open failed, recovering silently to the 10 m
    last resort). 10% is chosen symmetrically with the existing 90%
    success-fraction gate; genuine tile-boundary/hydro-flattening noise
    recovers a small, roughly constant fraction of polygons, so a jump to
    double digits is a systemic signal, not routine variance.
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
    # Shared across every `_attempt`/`_retry_compute_one` call in a PASS
    # (rebound to a fresh `Event` before the deferred pass, below) -- once
    # ANY set/polygon in this pass confirms the network is down (exhausts
    # its own in-place retry budget), every OTHER set/polygon in the SAME
    # pass bails out of its own budget early instead of independently
    # re-confirming the same outage (#223 round 2, I3). `_attempt` is a
    # closure over this name (read-only, never reassigned from inside it),
    # so rebinding it here later is visible to every subsequent `_attempt`
    # call without threading it through as an explicit parameter.
    transient_stop = threading.Event()

    def _bump(key):
        with lock:
            counts[key] += 1

    def _attempt(ts_str, idxs):
        """Run `idxs` against one set.

        Returns `(done, pending, transient_fail)`:
        - `done`: {idx: result} -- resolved against THIS set.
        - `pending`: idx list whose failure against THIS set was PERMANENT
          (empty interior; 404/403; unreadable object; heterogeneous
          `BuildVRT` mosaic -- see `_is_transient_error_chain`). `_recover`
          walks `candidates` for these, exactly as before the
          transient-retry fix.
        - `transient_fail`: idx list whose open/read kept failing
          TRANSIENTLY through THIS call's own in-place retry budget
          (`_RETRY_ATTEMPTS` attempts, ~2 minutes), OR were never attempted
          at all because `transient_stop` was already set by a sibling in
          this pass. NEVER handed to `_recover` here -- the CALLER
          (`run_batch`) is responsible for giving these ONE deferred retry,
          after a pause, before treating them as truly persistent (#223
          round 2, I4).

        A permanently-failed idx from a PRIOR attempt of this same call's
        open-retry loop is deliberately not tracked separately from a
        transiently-failed one when computing what's still `remaining` on
        a later attempt -- both are excluded via `permanent_idx`/
        `transient_idx` below, so neither is ever double-attempted or
        double-counted (#223 round 2, I2: two branches used to compute
        `pending` from a stale, unfiltered idx list that could still
        include an idx already recorded as a transient give-up).
        """
        ts = decode(ts_str)
        done: dict = {}
        permanent_idx: set = set()
        transient_idx: set = set()

        if transient_stop.is_set():
            # A sibling set/polygon in THIS pass already exhausted its
            # in-place budget -- don't also burn this set's own ~2 minutes
            # reconfirming what's already known. Deferred, not failed: the
            # caller gives the whole set its one deferred retry below.
            logger.warning(
                "  set=%s: skipping in-place retry -- a sibling set/polygon "
                "in this pass already confirmed the network is down; "
                "deferring all %d polygon(s)", ts.project, len(idxs),
            )
            return done, [], list(idxs)

        open_exc = None
        open_exc_transient = False
        for open_attempt in range(1, _RETRY_ATTEMPTS + 1):
            if transient_stop.is_set():
                transient_idx.update(i for i in idxs if i not in done and i not in permanent_idx)
                open_exc = None
                break
            remaining = [i for i in idxs if i not in done and i not in permanent_idx and i not in transient_idx]
            if not remaining:
                open_exc = None
                break
            try:
                with rasterio.Env(**_IN_PROCESS_RETRY_ENV), open_tile_set(ts) as vrt:
                    for idx in remaining:
                        if transient_stop.is_set():
                            # (#223 round 3 review, MINOR M-E) Named
                            # individually, not just folded into the
                            # group-level "deferring all N polygon(s)"
                            # WARNING above -- an operator scanning this
                            # set's own log lines for why THIS polygon
                            # never got attempted needs it here too.
                            logger.warning(
                                "  set=%s idx=%s: deferred without an attempt -- a "
                                "sibling set/polygon in this pass already confirmed "
                                "the network is down", ts.project, idx,
                            )
                            transient_idx.add(idx)
                            continue
                        try:
                            r, gave_up = _retry_compute_one(
                                vrt, ts, idx, dprst_gdf.geometry.loc[idx], logger, _bump, transient_stop
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
                            transient_idx.add(idx)
                            transient_stop.set()
                        elif r is None:
                            permanent_idx.add(idx)
                        else:
                            r["source"] = ts.project
                            done[idx] = r
                open_exc = None
                break  # opened + processed this attempt's remaining -- stop retrying the open
            except RasterioIOError as exc:
                open_exc = exc
                # Resolved ONCE per exception (an UNKNOWN verdict does a
                # real network probe, #223 round 4 review, R3-I2) and
                # reused below at `if open_exc is not None:` instead of
                # reclassifying `open_exc` a second time there.
                open_exc_transient = _resolve_open_transient(ts, exc, logger)
                if (
                    open_exc_transient
                    and open_attempt < _RETRY_ATTEMPTS
                    and not transient_stop.is_set()
                ):
                    _bump("n_transient_retry")
                    delay = _backoff_delay(open_attempt)
                    logger.warning(
                        "  set=%s: TRANSIENT open failure (%s) -- retry %d/%d in %.1fs",
                        ts.project, exc, open_attempt, _RETRY_ATTEMPTS, delay,
                    )
                    _sleep(delay)
                    continue
                break  # PERMANENT, transient retries exhausted, or a sibling already
                       # confirmed it -- handled below via `unresolved`
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
                still_open = [i for i in remaining if i not in done and i not in transient_idx]
                permanent_idx.update(still_open)
                _bump("n_compute_error")
                logger.error("  set=%s: UNEXPECTED error (%s: %s) — %d polygon(s) to recovery",
                             ts.project, type(exc).__name__, exc, len(still_open))
                open_exc = None
                break

        if open_exc is not None:
            # `done` may be non-empty here (a `WarpedVRT`/`open_tile_set`
            # cleanup-time exception can surface AFTER the for loop already
            # emitted some results), so `unresolved` must exclude anything
            # already resolved OR already classified this attempt --
            # re-including a resolved/classified idx would double-compute
            # or double-count it (fix round 1, "double compute" finding;
            # #223 round 2, I2).
            unresolved = [i for i in idxs if i not in done and i not in permanent_idx and i not in transient_idx]
            if open_exc_transient:
                # Every open attempt failed TRANSIENTLY: the primary is
                # still the right source, the network is what's broken --
                # do NOT hand these to `_recover`'s candidate walk. Deferred
                # (not failed) -- see `run_batch`'s docstring.
                transient_idx.update(unresolved)
                transient_stop.set()
                logger.warning(
                    "  set=%s: TRANSIENT open failure exhausted this pass's "
                    "in-place retry budget (%d attempts, %s) -- %d polygon(s) "
                    "deferred to a second pass, NOT advancing to a "
                    "lower-ranked candidate", ts.project, _RETRY_ATTEMPTS, open_exc,
                    len(unresolved),
                )
            else:
                # PERMANENT: the set doesn't exist / can't be opened -- a
                # routine 404/read gap, not a code bug. Unchanged
                # pre-existing behaviour.
                permanent_idx.update(unresolved)
                _bump("n_read_failure")
                logger.warning("  set=%s: open failed (%s) — %d polygon(s) to recovery",
                               ts.project, open_exc, len(unresolved))

        pending = [i for i in idxs if i in permanent_idx]
        transient_fail = [i for i in idxs if i in transient_idx]
        return done, pending, transient_fail

    results, to_recover = {}, []
    # Each entry: (ts_str, idx, is_recovery). `is_recovery` distinguishes a
    # PRIMARY-pass deferral (this idx's own primary source, never yet
    # walked through `_recover`) from a `_recover`-CANDIDATE deferral (this
    # idx already used up its "no usable source" budget on earlier
    # candidates and is now waiting on ts_str specifically) -- needed so a
    # deferred idx that resolves successfully bumps `n_recovered` iff it
    # actually took the candidate-recovery path (#223 round 3 review,
    # IMPORTANT I-B: previously ungated, so `recovered_fraction` silently
    # missed every polygon that recovered ONLY via the deferred pass).
    deferred: list[tuple[str, object, bool]] = []
    items = sorted(members.items())
    with ThreadPoolExecutor(max(1, n_threads)) as ex:
        for i, ((ts_str, _), (done, pending, transient_fail)) in enumerate(
            zip(items, ex.map(lambda kv: _attempt(*kv), items)), 1
        ):
            results.update(done)
            to_recover += pending
            for idx in transient_fail:
                # DEFERRED, not final -- see `run_batch`'s docstring / I4.
                # `ts_str` (not just the decoded project name) is kept so
                # the deferred pass below can re-`_attempt` this EXACT set.
                deferred.append((ts_str, idx, False))
            if i % 25 == 0:
                logger.info("  [%d/%d tile sets] %d polygons done", i, len(members), len(results))

    def _recover(idx, resume_after: str | None = None):
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
        # stops there -- DEFERRING that candidate (returned as `ts_str`, not a
        # decoded project name, so the deferred pass can re-`_attempt` it directly)
        # rather than falling through to `n_no_source`.
        #
        # `resume_after` (#223 round 3 review, MINOR M-C): when the deferred
        # pass re-invokes `_recover` for an idx that already walked (and
        # permanently exhausted) some PRIOR candidates before deferring on
        # THIS one, resume the walk immediately AFTER `resume_after` rather
        # than restarting from the polygon's full candidate list -- restarting
        # would re-try, and re-COUNT (`n_read_failure`), candidates already
        # known permanently bad.
        primary = dprst_gdf.at[idx, "source_tiles"]
        candidates = [c for c in dprst_gdf.at[idx, "candidates"] if c != primary]
        if resume_after is not None and resume_after in candidates:
            candidates = candidates[candidates.index(resume_after) + 1 :]
        for ts_str in candidates:
            done, _, transient_fail = _attempt(ts_str, [idx])
            if idx in done:
                return idx, done[idx], None
            if transient_fail:
                return idx, None, ts_str
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
        for idx, r, transient_ts_str in ex.map(_recover, sorted(to_recover)):
            if transient_ts_str is not None:
                deferred.append((transient_ts_str, idx, True))
            elif r is None:
                counts["n_no_source"] += 1
            else:
                counts["n_recovered"] += 1
                results[idx] = r

    transient_failures = []
    if deferred:
        # DEFERRED SECOND PASS (#223 round 2, I4): the in-place retry budget
        # above can be -- and in a real second incident WAS -- shorter than
        # the outage it's meant to survive (see `run_batch`'s docstring).
        # Every set/candidate that exhausted its in-place budget (whether at
        # the primary-pass or the `_recover`-candidate level) gets exactly
        # ONE more full attempt here, after a pause, before being treated as
        # truly persistent. Merge by `ts_str` first so a set that produced
        # several small deferred groups is retried ONCE, not once per group;
        # `deferred_recovery_idx` (I-B) tracks, per idx, whether THIS
        # deferral is a candidate-recovery one, since a single `ts_str`
        # group can mix a primary-pass deferral for one idx with a
        # recovery-candidate deferral for another.
        deferred_groups: dict[str, list] = {}
        deferred_recovery_idx: set = set()
        for ts_str_d, idx_d, is_recovery in deferred:
            bucket = deferred_groups.setdefault(ts_str_d, [])
            if idx_d not in bucket:
                bucket.append(idx_d)
            if is_recovery:
                deferred_recovery_idx.add(idx_d)
        n_deferred_polygons = sum(len(v) for v in deferred_groups.values())
        logger.warning(
            "  %d tile set/candidate group(s), %d polygon(s) exhausted their "
            "in-place TRANSIENT-retry budget -- pausing %.0fs before ONE "
            "deferred retry pass", len(deferred_groups), n_deferred_polygons,
            _DEFERRED_PASS_PAUSE_S,
        )
        # Fresh Event for the deferred pass: it must get its own genuine
        # first attempt, not be short-circuited by the primary/recovery
        # pass's Event already being set.
        transient_stop = threading.Event()
        _sleep(_DEFERRED_PASS_PAUSE_S)
        deferred_items = sorted(deferred_groups.items())
        # `resume_points`: idx2 -> the candidate `_recover` should resume
        # AFTER, collected across every deferred group while the outer
        # executor below is running, then processed in ITS OWN separate
        # `ThreadPoolExecutor` pass AFTER that one has fully exited (#223
        # round 4 review, MINOR M3) -- nesting a second executor INSIDE
        # this loop (the round-3 shape) could reach 2x `n_threads`
        # concurrent open handles (this pass's own still-running `_attempt`
        # threads, plus the nested one's), for no benefit: nothing about
        # the `pending2` candidate walk needs to run WHILE this pass's
        # OTHER deferred groups are still being attempted.
        resume_points: dict = {}
        with ThreadPoolExecutor(max(1, n_threads)) as ex:
            for (ts_str_d, _), (done2, pending2, transient_fail2) in zip(
                deferred_items, ex.map(lambda kv: _attempt(*kv), deferred_items)
            ):
                for idx2, r2 in done2.items():
                    results[idx2] = r2
                    if idx2 in deferred_recovery_idx:
                        # Resolved on a deferred retry of a CANDIDATE (not
                        # this idx's own primary) -- this IS a recovery,
                        # exactly as if it had resolved on the very first
                        # `_recover` pass (#223 round 3 review, I-B).
                        counts["n_recovered"] += 1
                for idx2 in pending2:
                    # A PERMANENT condition (e.g. an empty interior) found
                    # only on the deferred retry -- give it the SAME
                    # candidate walk any other permanent failure gets,
                    # resuming AFTER `ts_str_d` for an idx that already
                    # walked candidates up to it (M-C).
                    resume_points[idx2] = (
                        ts_str_d if ts_str_d != dprst_gdf.at[idx2, "source_tiles"] else None
                    )
                if transient_fail2:
                    transient_failures.append((decode(ts_str_d).project, transient_fail2))

        if resume_points:
            # Run CONCURRENTLY rather than serially under the builtin `map`
            # (M-C), now that the outer pass's executor has exited (M3). A
            # further transient give-up during THIS walk is final
            # immediately (no third pass -- "ONE deferred retry" is the
            # contract), not deferred again.
            with ThreadPoolExecutor(max(1, n_threads)) as ex2:
                for idx2, r2, transient_ts_str2 in ex2.map(
                    lambda i: _recover(i, resume_after=resume_points[i]), resume_points
                ):
                    if transient_ts_str2 is not None:
                        transient_failures.append((decode(transient_ts_str2).project, [idx2]))
                    elif r2 is None:
                        counts["n_no_source"] += 1
                    else:
                        counts["n_recovered"] += 1
                        results[idx2] = r2

    if transient_failures:
        # FAIL THE TASK LOUDLY instead of writing a partial/degraded batch --
        # every other set/candidate in this batch, AND the one deferred
        # retry pass, have already been processed by the time we get here.
        # A missing batch_XXXX.parquet is already caught downstream by PR
        # #222's `_verify_batches_match_plan`, and this non-zero exit stops
        # the SLURM `afterok` build from starting on an incomplete depstor
        # stack. The operator's remedy is to resubmit this array index once
        # the network recovers (see `slurm_batch/HPC_REFERENCE.md`'s
        # dprst_depth Recovery section -- downstream stages need resubmitting
        # too, or a whole-DAG resubmit via `submit_dprst_depth.sh`, which is
        # idempotent).
        affected = sum(len(idxs) for _, idxs in transient_failures)
        named = ", ".join(f"{proj}={len(idxs)}" for proj, idxs in transient_failures)
        message = (
            f"run_batch: TRANSIENT network failure persisted through the deferred "
            f"retry pass on {len(transient_failures)} tile set(s)/candidate(s) "
            f"({named}) -- {affected} polygon(s) unresolved. NOT writing "
            f"{out_parquet}. Resubmit this array task once the network recovers."
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
    #
    # (#223 round 2 review, minor) A batch that hit at least one real
    # TRANSIENT retry (`n_transient_retry > 0`) used to log at INFO as long
    # as every affected polygon still resolved on its primary -- but "the
    # batch hit a real network hiccup" is worth a human glance even when
    # nothing was ultimately lost, so it escalates to WARNING too.
    if counts["n_compute_error"] > 0:
        logger.error(summary_fmt, *summary_args)
    elif success_fraction < 0.90 or recovered_fraction > 0.10 or counts["n_transient_retry"] > 0:
        logger.warning(summary_fmt, *summary_args)
    else:
        logger.info(summary_fmt, *summary_args)
    return out
