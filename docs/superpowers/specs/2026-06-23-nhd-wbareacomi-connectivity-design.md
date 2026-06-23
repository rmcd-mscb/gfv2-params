# NHD WBAREACOMI-driven waterbody connectivity (depression storage)

**Date:** 2026-06-23
**Status:** Design approved (integration option A); ready for implementation plan
**Author:** Rich McDonald (with Claude Code)

## Problem

The depression-storage (depstor) pipeline must decide which waterbodies are
**on-stream** (the modeled stream flows through them — handled by routing, not
depression storage) versus **depression storage** (off-stream ponds/closed
basins that contribute to `dprst_frac`).

Today this is a geometric heuristic. The `streambuffer` step buffers the fabric
stream segments by a flat **60 m**
([`streambuffer.py`](../../../src/gfv2_params/depstor_builders/streambuffer.py)),
rasterizes the buffer, and `dprst.py` excludes any waterbody clump-region that
shares **at least one pixel** with the buffer
([`dprst.py:46`](../../../src/gfv2_params/depstor_builders/dprst.py#L46)).

Weaknesses:

- A single overlapping pixel flips an entire waterbody from depression storage
  to on-stream.
- The 60 m distance is uncalibrated and applied uniformly.
- Proximity is not connectivity: a hydrologically isolated pond 50 m from a
  segment is flagged on-stream; a connected impoundment whose polygon edge sits
  70 m away is kept as depression storage.

## Approach

Replace the proximity heuristic with NHD's authoritative topology. In NHDPlusV2,
an **artificial-path** `NHDFlowline` drawn through a waterbody carries
`WBAREACOMI` = the COMID of that waterbody. The set of distinct populated
`WBAREACOMI` values is exactly the set of waterbodies the NHD stream network
flows through — i.e. the on-stream (connected) waterbodies.

Connectivity is therefore decided in NHD's own COMID space and joined to our
waterbody polygons by COMID. **Our fabric `nsegment` layer plays no role in the
classification.** This is the deliberate workaround for the fact that the fabric
segments are a subset of NHD and discretized differently — we never have to
reconcile `seg_id` to NHD COMID (the segments carry no COMID anyway).

### Decisions locked during brainstorming

- **Resolution: medium-res (NHDPlusV2).** Our `conus_waterbodies.gpkg` polygons
  are NHDPlusV2 COMIDs, so `WBAREACOMI` must come from the same resolution to
  share the COMID namespace. High-res (NHDPlus HR) uses a different identifier
  namespace and would require re-staging HR waterbodies — explicitly out of
  scope.
- **Connectivity scope: full NHD.** Any NHD artificial path through a waterbody
  marks it on-stream, regardless of whether that reach is in our fabric. This is
  physically cleanest and avoids segment reconciliation. Consequence: a pond on
  a dropped headwater reach becomes on-stream, not depression storage.
- **Data sourcing: build the download + staging step** (reproducible), mirroring
  the existing `download/rpu_rasters.py` pattern.
- **Integration: option A — connected-mask drop-in** (see below). Option B (pure
  attribute split, dropping pixel-clumping for the stream side) is deferred; we
  want an efficient path to first results and can refactor later.

### Grounding facts (verified during design)

- `conus_waterbodies.gpkg` (layer `waterbodies`, 448,124 features, EPSG:5070)
  carries `COMID` (Integer) and `member_comid` (String). `member_comid` is a
  **single COMID, not a delimited list**, and equals `COMID` in 99.94% of rows
  (280 multipart exceptions). The join is therefore
  `waterbody.COMID ∈ connected_set`, with `member_comid` unioned in to catch the
  280 multipart cases.
- `waterbody.py` currently rasterizes **geometry only** — it reads no attribute
  columns. The new connectivity step will read `COMID`/`member_comid`.
- The stream-connectivity test is a single line,
  [`dprst.py:46`](../../../src/gfv2_params/depstor_builders/dprst.py#L46):
  `stream_regions = regions_touching_mask(regions, stream_binary)`. The imperv
  branch (line 47) is independent.
- `stream_buffer.tif` is consumed **only** by `dprst.py`.

## Architecture

Integration **option A (connected-mask drop-in):** build a new
`connected_wbody.tif` raster — the waterbody polygons whose COMID is in the
connected set, rasterized and land-masked — and feed it into `dprst.py` in place
of `stream_buffer.tif`. The existing clump/region machinery and the entire
impervious branch are unchanged; only the *definition* of the connected mask
changes.

```
NHDPlusV2 NHDSnapshot (per VPU, S3)
  └─ NHDFlowline.dbf  (COMID, FTYPE, WBAREACOMI)
        │  distinct WBAREACOMI != 0
        ▼
  connected_waterbody_comids.parquet          [staging artifact]
        │  join on COMID (∪ member_comid)
        ▼
  conus_waterbodies.gpkg polygons ── flag connected ──► connected_wbody.tif
        │                                                      │
        ▼                                                      ▼
  wbody_binary / wbody_regions (unchanged)        dprst.py stream side:
                                                  stream_regions =
                                                  regions_touching_mask(
                                                    regions, connected_wbody)
                                                  imperv branch unchanged
                                                      │
                                                      ▼
                                            dprst_binary.tif / onstream_binary.tif
                                                      │
                                                      ▼
                                  dprst_frac, onstream_storage_frac (PRMS params)
```

### Components

1. **Download module — `src/gfv2_params/download/nhd_flowlines.py`**
   Mirrors [`download/rpu_rasters.py`](../../../src/gfv2_params/download/rpu_rasters.py).
   Pulls NHDPlusV2 **NHDSnapshot** archives per VPU from the
   `dmap-data-commons-ow` S3 bucket
   (`NHDPlusV21_{dd}_{vpu}_NHDSnapshot_{ver}.7z`), extracts only
   `NHDFlowline.dbf` (no geometry needed — only `COMID`, `FTYPE`, `WBAREACOMI`).
   Reads `data_root` via the base config; writes archives to
   `input/nhd_downloads/` and extracted DBFs under `shared/source/`. CLI:
   `python -m gfv2_params.download.nhd_flowlines`. New SLURM batch
   `slurm_batch/download_nhd_flowlines.batch` invoking `pixi run --as-is`.
   **Open item to confirm at implementation time:** the exact NHDSnapshot
   component filename and version-candidate list per VPU (the bucket layout is
   the same family as the rasters, but the `NHDSnapshot` archive name must be
   verified against an actual S3 listing before coding the path template).

2. **Connected-COMID table build**
   Reads every extracted `NHDFlowline.dbf`, collects distinct `WBAREACOMI` where
   `WBAREACOMI != 0`, writes a flat artifact
   `input/nhd/connected_waterbody_comids.parquet` (single column of connected
   waterbody COMIDs). Implemented as a function/entry in the download module so a
   single `python -m gfv2_params.download.nhd_flowlines` run downloads and
   produces the table. This is a one-time staging artifact alongside
   `conus_waterbodies.gpkg`.

3. **New depstor step — `wbody_connectivity` builder**
   `src/gfv2_params/depstor_builders/wbody_connectivity.py`. Reads
   `waterbody_gpkg`/`waterbody_layer` pulling `COMID` + `member_comid`, loads the
   connected-COMID parquet, flags polygons whose `COMID` (or `member_comid`) is
   in the connected set, rasterizes the flagged polygons land-masked, and
   registers output `connected_wbody` → `connected_wbody.tif` (uint8: 1 =
   connected, 255 = nodata/off-land). Registered in `BUILDERS` and `STEP_ORDER`
   in
   [`depstor_builders/__init__.py`](../../../src/gfv2_params/depstor_builders/__init__.py)
   (after `landmask`, before `dprst`; occupies the former `streambuffer` slot).
   Config block added to
   [`configs/depstor/depstor_rasters.yml`](../../../configs/depstor/depstor_rasters.yml)
   with keys for the connected-COMID table path and the output filename. Path
   inputs come from the fabric profile via `require_config_key`, never
   hardcoded. Ships with `tests/test_wbody_connectivity.py` (repo rule: builder +
   test together; match the synthetic tiny-raster + small-GeoDataFrame style of
   the existing depstor tests).

4. **Modify `dprst.py`**
   Consume `connected_wbody` (via `ctx.require("connected_wbody")`) instead of
   `stream_buffer`. The line becomes
   `stream_regions = regions_touching_mask(regions, connected_binary)`. No other
   logic changes; the imperv branch and outputs (`dprst`, `onstream`) are
   untouched. **Retire the `streambuffer` step**: remove it from `STEP_ORDER`,
   `BUILDERS`, and `depstor_rasters.yml`. Leave `streambuffer.py` on disk as
   reference but do not extend it. (Its only consumer was `dprst.py`.)

5. **Docs + memory**
   Audit and update on the same branch:
   [`docs/ARCHITECTURE.md`](../../../docs/ARCHITECTURE.md),
   [`slurm_batch/RUNME.md`](../../../slurm_batch/RUNME.md),
   [`slurm_batch/HPC_REFERENCE.md`](../../../slurm_batch/HPC_REFERENCE.md), and
   the depstor sections describing the streambuffer step. Update memory notes
   `dprst_connectivity_via_nhd_wbareacomi` and
   `depstor_segments_streambuffer_only` (segments no longer feed even the
   streambuffer step).

## Data flow

1. `download/nhd_flowlines.py` → per-VPU `NHDFlowline.dbf` under `shared/source/`.
2. Same module → `input/nhd/connected_waterbody_comids.parquet` (distinct
   `WBAREACOMI != 0`).
3. `wbody_connectivity` builder (depstor) → `connected_wbody.tif`.
4. `dprst` builder → `dprst_binary.tif` / `onstream_binary.tif` (stream side now
   driven by `connected_wbody.tif`; imperv side unchanged).
5. Zonal aggregation → `dprst_frac`, `onstream_storage_frac` PRMS params
   (unchanged downstream).

## Error handling

- **Missing connected-COMID table:** the `wbody_connectivity` builder fails fast
  with a clear message pointing at the `download/nhd_flowlines` step (do not
  silently fall back to an empty set, which would mark every waterbody as
  depression storage).
- **WBAREACOMI semantics:** `0` (and null) means "not through a waterbody" and
  is excluded from the connected set. Verify the sentinel against real DBF data
  during implementation; treat both `0` and null as not-connected.
- **Download robustness:** reuse `rpu_rasters.py`'s version-candidate fallback
  and archive-extraction handling; a missing VPU archive is a hard error, not a
  skip (a silently dropped VPU would under-flag connectivity there).
- **Join coverage:** log the count of waterbody polygons flagged connected and
  the count of connected COMIDs with no matching polygon, so coverage gaps are
  visible rather than silent.

## Testing

- `tests/test_wbody_connectivity.py`: synthetic tiny raster + small waterbody
  GeoDataFrame with known COMIDs; assert that only polygons whose COMID is in the
  connected set are rasterized to 1, that `member_comid` matches are honored, and
  that off-land cells are masked to 255.
- Download/table-build: a small unit test over a synthetic in-memory or fixture
  DBF asserting distinct `WBAREACOMI != 0` extraction (network download itself
  not exercised in CI).
- Existing `dprst` behavior: confirm the swap is mask-source-only — the imperv
  branch and output shapes are unchanged. Update any test that referenced
  `stream_buffer` as a `dprst` input.
- CI (`.github/workflows/ci.yml`) is the authoritative gate; do not run pytest on
  the HPC head node.

## Out of scope (YAGNI)

- Segment ↔ COMID reconciliation (the whole point is to avoid it).
- High-res (NHDPlus HR) staging.
- Downloading flowline geometry (only the DBF attribute table is needed).
- Option B (pure attribute split, removing pixel-clumping for the stream side) —
  deferred; revisit if the clump-merge edge case proves material.

## Validation after first run

- Compare `dprst_frac` / `onstream_storage_frac` against the current
  buffer-based outputs on a small fabric (e.g. VPU 01) to see how the
  classification shifts.
- Spot-check a handful of known on-stream impoundments and isolated ponds.
