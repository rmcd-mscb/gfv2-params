# drains_to_dprst on-stream waterbody barrier — design

Date: 2026-07-01
Status: approved (brainstorming)
Branch: TBD (feature branch off `main`)

## Problem

`drains_to_dprst.tif` (built by the `routing` step,
`src/gfv2_params/depstor_builders/routing.py`, via the in-process D8 kernel in
`src/gfv2_params/d8_routing.py`) marks, for each cell, whether its ESRI-D8 flow
path eventually reaches a **depression-storage (dprst) pour-point cell**. The
pour-point set is exactly the dprst waterbody cells.

The FDR the traversal walks is the fully depression-filled NHDPlus HydroDEM, so
the traversal passes **straight through** any on-stream (non-dprst) waterbody as
if it were not there. As a result, a cell that first drains into an on-stream
waterbody — whose outflow then continues downstream and eventually reaches a
dprst waterbody — is incorrectly counted as draining to depression storage.
Observed in VPU 15.

`drains_to_dprst` feeds `drains_perv` (× `perv`) and `drains_imperv`
(× `imperv`) via `depstor_builders/intersect.py`, i.e. the fractions of an HRU's
pervious/impervious area that drain to depression storage. Over-attribution here
inflates those fractions.

## Hydrologic principle

By the classifier's definition, a depression-storage waterbody is an
**off-network / terminal** feature (inflow-only sink, outflow-only pothole, or
isolated depression) **plus playas** (force-classified dprst regardless of
apparent connectivity). Surface runoff captured by an on-stream waterbody has
entered the stream/lake routing network; its outflow is streamflow, not inflow
to a downstream depression's storage. So land upslope of an on-stream waterbody
must not be attributed to a downstream dprst.

An on-stream waterbody should therefore act as a **barrier**: any cell whose flow
path hits an on-stream waterbody *before* reaching a dprst is not
`drains_to_dprst`.

Notes settled during brainstorming:

- **Playas need no special handling.** Playas are classified `dprst`, never
  `onstream`, so they are never in the barrier set and their contributing area
  always counts. The classifier already carves them out.
- **Barrier set = all on-stream waterbodies**, including small on-stream ponds.
  No size/name filtering — treat every on-stream waterbody like any other.
- **Scope is on-stream waterbodies only, not the full stream-channel network.**
  With a correct dprst classifier, open-drainage land only reaches a "dprst" by
  routing through an on-network *waterbody*; barrier-ing those is minimal and
  sufficient. A pure river channel threading a terminal depression is a rare
  filled-FDR artifact and is out of scope (YAGNI).

## Approach (selected)

Add an explicit **barrier mask** to the D8 kernel rather than mutating the FDR.
Rejected alternative: burning on-stream cells into the FDR as nodata/sinks —
functionally similar but conflates classification with flow data and muddies the
"unexpected FDR code" diagnostics.

### Kernel — `src/gfv2_params/d8_routing.py`

- New signature: `drains_to_dprst_kernel(fdr_win, pour_win, barrier_win, fdr_nodata=255)`.
  `barrier_win` is uint8, `1` = on-stream waterbody cell, `0` = background.
- In `_resolve`, after seeding pour cells to `_DRAINS`, seed cells that are
  `barrier == 1` **and still `_UNKNOWN`** to `_NOT`. dprst wins any overlap
  (disjoint by construction; precedence made explicit for determinism).
- Traversal is otherwise unchanged. A path that reaches a barrier cell (state
  `_NOT`) resolves non-draining and stops; a barrier cell's own downstream walk
  is skipped because its state is no longer `_UNKNOWN`. Net semantics: **first
  waterbody on the flow path wins.**

### Builder — `src/gfv2_params/depstor_builders/routing.py`

- `onstream_path = ctx.require("onstream")` — the `onstream_binary.tif` the
  `dprst` step already emits. `routing` already depends on `dprst`, so it is on
  disk; no DAG reorder needed.
- Per VPU, read the on-stream window like `dprst_win`, mask it to the VPU code
  (mirroring `vpu_pour_points`), and pass it as `barrier_win`.
- Keep existing per-VPU "N cells drain to dprst" logging; optionally add a
  barrier-cell count for visibility.

### Semantics / invariants

`drains_to_dprst` becomes a **strict subtraction** from today's raster: a cell is
dropped iff its flow path hits an on-stream waterbody before any dprst. Coverage
can only decrease. The existing all-nodata guard and the >50% coverage warning
remain valid.

## Tests

- `tests/test_drains_kernel.py`:
  - linear chain `upslope → barrier → pour` ⇒ upslope **not** marked, pour marked;
  - `upslope → pour` (no barrier) ⇒ still marked (regression guard);
  - barrier placed **downstream** of a pour ⇒ upslope still marked (first-hit-wins).
- `tests/test_routing_tiling.py`: update kernel/builder call sites for the new
  argument; add a small "barrier blocks drainage" tiling case.

## Docs (same branch, per repo convention)

- Module docstrings in `d8_routing.py` and `routing.py`.
- The depstor section of `docs/ARCHITECTURE.md`.
- The `drains_to_dprst` note in `CLAUDE.md`.

## Validation

1. Rebuild `dprst` first — picks up the recent classifier changes, yielding the
   current set of depression-storage (and on-stream) waterbodies.
2. Run `routing` with the barrier.
3. On a test VPU (15): compare `drains_to_dprst` area before/after — expect a
   decrease concentrated below on-stream lakes.
4. Spot-check that no legitimate terminal-basin land was dropped by a mislabeled
   on-stream cell (data-quality check on the on-stream mask, not the logic).

## Out of scope

- Full stream-channel-network barrier (channels, not just waterbodies).
- Partial-capture / spill modeling for on-stream waterbodies — `drains_to_dprst`
  is an a-priori binary land attribution; PRMS routes the lake/stream network
  separately.
