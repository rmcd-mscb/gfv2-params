# Replace WBT `Watershed` with a cycle-safe numba D8 routing kernel

**Status:** Design — 2026-05-29

**Supersedes (in part):** the WhiteboxTools-memory framing of
[`2026-05-28-depstor-per-vpu-routing-design.md`](2026-05-28-depstor-per-vpu-routing-design.md).
That spec correctly introduced per-VPU tiling, but attributed the routing
failure to memory. The real blocker on CONUS is a **WBT `Watershed` hang**, not
OOM (see Diagnosis). The per-VPU tiling it introduced is retained; only the
per-tile compute (WBT subprocess) is replaced.

## Problem

The depstor `routing` step produces `drains_to_dprst.tif` — the binary upslope
contributing-area mask of the depression set, feeding `sro_to_dprst_perv` and
`sro_to_dprst_imperv` (2 of the 6 PRMS depstor parameters). It currently runs
WhiteboxTools (WBT) `Watershed` once per VPU tile against the ESRI-D8
flow-direction raster (FDR) plus the depression pour-points.

On the CONUS `gfv2` fabric the step **hangs**. In the cancelled run
(`logs/job_23453136.err`, 2026-05-28):

- VPU 1 (New England, 0.42 B-cell bbox) routed cleanly in ~1 minute
  (28,963,692 cells drain to dprst).
- VPU 2 (Mid-Atlantic, **only 0.73 B-cell bbox**) reached WBT `Progress: 48%`
  within ~4 seconds, then emitted **no further output for 3 h 20 m** until the
  job was cancelled.

## Diagnosis — a WBT hang, not OOM

The evidence rules out a scale/memory problem:

- VPU 2's bbox (0.73 B cells) is **smaller than seven other VPUs**; the largest,
  VPU 10 / Missouri, is 3.12 B cells — 4× larger — yet VPU 2 is where it stalls.
  A size-driven failure would hit VPU 10 first, not VPU 2.
- The job was **CANCELLED**, not OOM-killed; the node has ~503 GB and the per-VPU
  WBT working set is far below that.
- WBT `run_streamed` logs each line as it arrives (line-buffered). Complete
  silence after `Progress: 48%` means WBT emitted no new lines — it stalled on a
  single cell's downstream trace, not "ran slowly."

Mechanism: WBT `Watershed` traces each cell downstream to a pour point with **no
memoization and no cycle guard**. A flow cycle or pathological flat in the
open-source FDR makes one cell's trace non-terminating, and the tool spins
forever. Bumping `--mem` cannot fix this; it is a hard blocker.

## What the step actually computes

Stripped of WBT, the computation is a textbook linear-time graph traversal on a
**functional** flow graph (each cell has exactly one D8 downstream neighbour):

> `drains_to_dprst[cell] = 1` iff following the ESRI-D8 pointer downstream from
> that cell eventually reaches a `dprst_binary == 1` cell; else nodata.

The FDR is confirmed clean ESRI D8: `gfv2_fdr.vrt` is uint8, nodata 255, on the
exact template grid (153830×109901), with band values strictly in
`{1,2,4,8,16,32,64,128}` (no flats/zeros in sampled windows).

## Goal

A **durable** (cannot hang) and **accurate** (exact D8 upslope of the depression
set) replacement for the per-tile WBT call, dropped into the existing per-VPU
full-array mosaic with minimal change. No DAG change. No change to the
`drains_to_dprst.tif` schema or to any downstream step/parameter.

## Decision summary

Decisions reached during brainstorming (2026-05-29) with the hydrologist:

1. **Keep per-VPU tiling.** VPU boundaries are watershed-derived (drainage
   divides), so per-VPU truncation carries no hydrologic penalty — confirmed by
   the user and colleagues. Tiling also bounds per-tile working memory.
2. **Full-array mosaic now.** Keep today's in-RAM `vpu_id` read + full-CONUS
   `drains` accumulator (~34 GB peak). Smallest diff; fixes the hang; runs on the
   HPC and a 64 GB box. The ~6–9 GB workstation path (file-based mosaic) is
   documented below as a deferred enhancement with its own issue.
3. **New module for the kernel.** The traversal lives in a new
   `src/gfv2_params/d8_routing.py`, not in `depstor.py` — to quarantine the
   codebase's first `numba` import away from the common utility module that ~10
   builders import.

## Algorithm — memoized D8 traversal (cycle-safe, O(N))

New pure helper in `src/gfv2_params/d8_routing.py`:

```python
drains_to_dprst_kernel(fdr_win, pour_win, fdr_nodata=255) -> uint8[ny, nx]
```

- **State coloring** `st` (int8 per cell): `0`=unknown, `1`=drains, `2`=does-not,
  `3`=in-progress (currently on the active path).
- Seed `st = 1` at every `pour_win == 1` cell.
- For each cell still `0`, walk downstream — decode the ESRI-D8 code to a
  neighbour offset (table below), pushing each visited cell onto a reusable path
  stack and marking it `3` — until the next cell is:
  - `st == 1` → the whole path drains → resolve all stacked cells to `1`;
  - `st == 2`, or `fdr == fdr_nodata`, or the neighbour leaves the window →
    resolve all stacked cells to `2`;
  - `st == 3` → **cycle detected** (we re-entered the active path) → resolve all
    stacked cells to `2`.
- Backfill is path compression: each cell is resolved exactly once → **O(N)**;
  the stack is reused across cells, so peak extra memory is one `int8` state
  array plus a stack bounded by the longest flow path.
- Return `1` where `st == 1`, else `0`.

The `3`-coloring is the explicit fix for the WBT failure: a revisited
in-progress cell is recognised as a cycle and resolved immediately, so the
kernel **physically cannot loop forever**.

ESRI-D8 decode (`code → (drow, dcol)`):

| code | dir | drow | dcol |
|---|---|---|---|
| 1 | E | 0 | +1 |
| 2 | SE | +1 | +1 |
| 4 | S | +1 | 0 |
| 8 | SW | +1 | −1 |
| 16 | W | 0 | −1 |
| 32 | NW | −1 | −1 |
| 64 | N | −1 | 0 |
| 128 | NE | −1 | +1 |
| 255 (nodata) / other | sink/boundary | — | — |

Implementation note: use an iterative (explicit-stack) DFS, not recursion —
flow paths can be tens of thousands of cells long. `@njit` with a preallocated,
growable stack array.

## Integration into `routing.build`

Within the existing per-VPU loop
([`routing.py:139-169`](../../../src/gfv2_params/depstor_builders/routing.py)),
replace the *write-tile-files → WBT-subprocess → read-back* block with an
in-memory kernel call:

```python
fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code)    # existing helper
pour       = vpu_pour_points(dprst_win, vpu_win, code)   # existing helper
ws_win     = drains_to_dprst_kernel(fdr_masked, pour)    # NEW — replaces WBT
assign_vpu_drains(drains, vpu_id, code, bbox, ws_win, ws_nodata=0)  # existing
```

- **Delete from `routing.py`:** `_run_whitebox_watershed`, `_write_window_tif`,
  the per-tile `_fdr_vpu*/_pour_vpu*/_ws_vpu*` temp GeoTIFFs and their
  read-back, and the `from ..wbt import find_whitebox_tools_binary, run_streamed`
  line. The loop becomes pure in-process numpy/numba — no subprocess, no per-tile
  disk I/O.
- **Keep unchanged:** `_align_fdr_to_dprst_grid` (streaming `gdal.Warp` →
  `fdr_aligned.tif`, low-memory; reused as the source for windowed FDR reads),
  the whole-CONUS `vpu_id` read and `drains` allocation, `assign_vpu_drains`, the
  final land-mask, and `write_uint8_binary`. → **full-array mosaic, ~34 GB peak,
  semantics unchanged.**

`wbt.py` and the `whitebox` dependency are **not** removed — they remain in use
by `shared_rasters/compute_dem_derivatives.py`.

`assign_vpu_drains` is called with `ws_nodata=0`, so its "labelled" test
(`watershed_win != ws_nodata`) selects exactly the kernel's `1`-valued cells.

## Components & data flow

```
fdr_aligned.tif (gdal.Warp, CONUS, low-mem)      vpu_id.tif (per-cell VPU code)
dprst_binary.tif (pour candidates)                       │
        │                                                │
        └────────────────────┬───────────────────────────┘
                             ▼   for each VPU code c (full-array mosaic):
        window = bbox(vpu_id == c)                         [existing vpu_bbox]
        fdr_c  = mask_fdr_to_vpu(fdr[window], vpu==c)      [existing helper]
        pour_c = vpu_pour_points(dprst[window], vpu==c)    [existing helper]
        ws_c   = drains_to_dprst_kernel(fdr_c, pour_c)     [NEW numba kernel]
        assign_vpu_drains(drains, vpu_id, c, bbox, ws_c)   [existing helper]
                             ▼
        land-mask -> drains_to_dprst.tif  (CONUS, identical schema to today)
```

Isolation: `d8_routing.drains_to_dprst_kernel` is a pure array→array function
unit-tested in isolation; `routing.build` owns the loop + IO; the downstream
contract (`drains_to_dprst.tif`) is unchanged, so `drains_perv`/`drains_imperv`
and the params need no changes.

## Memory

Full-array mosaic peak ≈ `vpu_id` (16.9 GB) + `drains` (16.9 GB) + the largest
single tile's working arrays. Largest tile is VPU 10 (Missouri), 3.12 B-cell
bbox → FDR + state ≈ 6.2 GB held transiently. Total peak ≈ **~40 GB**; size the
batch `--mem` accordingly (64 GB is comfortable). This is the same order as the
existing full-grid steps (`waterbody` clump, `dprst` regions).

Per-VPU bbox sizes (decimated 1/32 scan of `vpu_id.tif`, 2026-05-29):

| VPU | bbox (B cells) | actual (B cells) | fill % |
|---|---|---|---|
| 10 | 3.12 | 1.50 | 48 |
| 3 | 2.02 | 0.75 | 37 |
| 13 | 1.88 | 0.63 | 33 |
| 17 | 1.74 | 0.90 | 52 |
| … | … | … | … |
| 2 | 0.73 | 0.31 | 42 |
| 1 | 0.42 | 0.19 | 45 |

## Testing

New `tests/test_drains_kernel.py` (pure kernel, no WBT, no IO):

1. **Straight chain into a pour point** → all upstream cells marked `1`.
2. **Chain draining away** from the only pour point → `0`.
3. **2-cell cycle** (two cells pointing at each other) with no reachable pour
   point → terminates, marks `0`. *Explicit regression for the WBT hang.*
4. **4-cell rotational cycle** likewise terminates and marks `0`.
5. **Cell upstream of a cycle** with no pour downstream → `0`.
6. **Cycle containing a pour point** → drains (seeding breaks the cycle).
7. **nodata / off-window** termination → `0`.
8. **Pour point itself** → `1`; cell whose immediate downstream is a pour → `1`.
9. **All eight ESRI directions** decoding into a central pour → all `1`.

The kernel also returns a per-window flow-cycle count, which `routing.build`
logs as a warning (a cycle is a hydro-conditioned-DEM defect worth surfacing).

**Regression anchor:** rerun routing on the `oregon` fabric (single VPU,
acyclic FDR) and diff `drains_to_dprst.tif` against its existing WBT-produced
output. Expect identical results (the kernel ≡ WBT on acyclic FDR with the same
pour set). If flats produce ≤1-cell edge differences, document the tolerance;
investigate anything larger.

The existing per-VPU isolation/mosaic helpers (`vpu_bbox`, `mask_fdr_to_vpu`,
`vpu_pour_points`, `assign_vpu_drains`) keep their current tests in
`tests/test_routing_tiling.py`.

## Docs to update (same branch, per CLAUDE.md doc-audit)

- `src/gfv2_params/depstor_builders/routing.py` module docstring — replace the
  WBT/OOM framing with the kernel + hang rationale.
- `docs/depstor_workflow.md` item 7 (`getHruSro_to_dprst`) — note the open-source
  port now uses the in-process D8 kernel, not WBT `Watershed`.
- `docs/depstor_port_summary.md` — the two `routing.py` rows referencing WBT
  `Watershed`, and bug #4 (WBT silently treats nodata as pour points) which no
  longer applies to this step.
- `docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md` — add a
  superseded-by pointer to this spec for the WBT-memory framing.
- `slurm_batch/build_depstor_rasters.batch` "sized for the WBT routing long-pole"
  comment and any `slurm_batch/RUNME.md` mention of WBT in routing.

## Future enhancement — workstation-scale memory (file-based mosaic)

**Deferred; tracked as [issue #129](https://github.com/rmcd-mscb/gfv2-params/issues/129).**
Captured here so the worked-out understanding is not lost.

The full-array mosaic is capped at ~34 GB regardless of tile size because it
holds two full-CONUS uint8 arrays the whole run: `vpu_id` (read whole) and the
`drains` accumulator. To reach a 16–32 GB workstation, make **the output GeoTIFF
on disk the accumulator** and never materialise a full-CONUS array:

1. **Streaming `vpu_id` bbox scan** — read `vpu_id.tif` in row-blocks once,
   recording each code's min/max row & col; discard each block. Replaces the
   whole-array `vpu_id` read; transient memory = one block.
2. **Windowed tile reads** — per VPU, read only that bbox from FDR / `vpu_id` /
   `dprst` (≤ ~3.1 GB each for VPU 10), mask, run the kernel.
3. **Read-modify-write mosaic** — open `drains_to_dprst.tif` in `r+` (update)
   mode, created up front filled with nodata. Per tile: read the window back, set
   cells to `1` where `vpu_id == code & drained`, write the window. RMW (not a
   blind write) because VPU bounding boxes overlap at the corners — a later VPU's
   nodata must not clobber an earlier VPU's already-written cells.
4. **Per-window land-mask** — apply the land-mask window during the RMW, so there
   is never a full-CONUS land-mask read either.

Peak RAM then = the largest single VPU window (Missouri ≈ 3.12 B cells; ~6–9 GB
with FDR + state held briefly, less if `dprst`/`vpu_id` windows are freed after
seeding). Cost: more windowing code (bbox scan, windowed reads, RMW writer,
windowed land-mask) and modest extra disk I/O (overlap corners read/written
twice). The pure kernel is unchanged between the two mosaics — only the IO
wrapper differs — so this enhancement is purely a `routing.build` rewrite with no
risk to the traversal logic.

## Non-goals

- Cross-VPU halos / exact cross-boundary contributing areas (accepted
  truncation; divides confirmed hydrologically appropriate).
- Removing the per-VPU tiling or the `gdal.Warp` FDR alignment.
- File-based mosaic (deferred — see above).
- Removing `wbt.py` / the `whitebox` dependency (still used by
  `compute_dem_derivatives`).
- SLURM-array parallelism across VPUs (sequential loop is in scope).

## Risks

- **First numba use in the repo.** Quarantined to `d8_routing.py`. `@njit` first
  call incurs JIT compilation (~seconds); negligible against CONUS routing wall
  time. CI imports numba cleanly (conda-forge dep).
- **Kernel ≢ WBT at flats/edges.** The oregon regression anchors equivalence on
  acyclic FDR; document any ≤1-cell differences, investigate larger ones.
- **Cycle resolution convention.** Cells trapped in a cycle with no pour-point
  exit resolve to `0` (does-not-drain). This is the correct behaviour for a
  contributing-area-to-depressions mask: a cell that never reaches a depression
  does not contribute to one.
