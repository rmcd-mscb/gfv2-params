# Design: topology-based flow-through reclassification for dprst

**Date:** 2026-06-26
**Status:** approved (design); implementation plan to follow
**Related:** PR #139 (WBAREACOMI connectivity), PR #144 (per-cell imperv carve-out),
memory `drains_to_dprst_overextension`, `dprst_connectivity_via_nhd_wbareacomi`

## Problem

`drains_to_dprst` flags the entire D8 upslope watershed of every depression
pour-point, with no distance/area cap, on the hydro-conditioned NHD FDR
(`depstor_builders/routing.py` + `d8_routing.drains_to_dprst_kernel`). A
diagnostic run on 2026-06-25 showed this massively over-extends: ~26% of all
CONUS land "drains to dprst", and coverage ranks **highest in humid,
open-drainage basins** (Lower Mississippi 70%, S. Atlantic-Gulf 57%, Great
Lakes 39%) rather than the endorheic ones (Great Basin 22%, Rio Grande 7.5%).

Root cause: **28 of the 30 largest dprst waterbodies are through-flow
swamps/marshes** (both inflow and outflow, up to 2,845 km²) that are in dprst
only because NHD never tagged them with `WBAREACOMI`. Their whole upstream
watershed is then counted as draining to depression storage, which is wrong
for flow-through features.

The current on-stream/dprst split is keyed on a single signal — distinct
positive `WBAREACOMI` values from NHDFlowline (`download/nhd_flowlines.py` →
`connected_waterbody_comids.parquet`). A waterbody region that touches that
connected mask becomes on-stream; everything else is dprst. The signal is
incomplete: a marsh a river flows through, but for which NHD drew no artificial
path, carries no `WBAREACOMI` and is wrongly left in dprst.

## Goal

Fix the **classifier** (which waterbodies are dprst pour-points), so
`drains_to_dprst` becomes correct **by construction** — with **no cap and no
tuning knob** on the routing. A waterbody that conveys a stream (genuine
channel inflow **and** outflow) is a flow-through feature and belongs
on-stream; dprst is reserved for off-network, internally-drained, or
locally-spilling depressions. Genuine endorheic terminal sinks (Great Basin
playas, prairie potholes) must remain dprst.

### Why fix classification, not routing (and why no cap)

- Reclassifying a flow-through waterbody from dprst → on-stream automatically
  removes it as a routing pour-point: `dprst.py` already excludes on-stream
  regions from dprst, and `routing.py` only routes to dprst cells
  (`vpu_pour_points(dprst, ...)`). The `drains_to_dprst` correction falls out
  of the classification fix with **no kernel change**.
- A classification fix is strictly more complete than an outlet-aware routing
  change: it also moves the waterbody's **own** cells from `dprst_frac` to
  `onstream_storage_frac` — the right bucket for a through-flow feature.
- **No cap (distance/area/accumulation).** A cap cannot distinguish a
  legitimately huge contributing area (a real endorheic basin draining its
  whole basin to a playa — correct) from a spurious one (a through-flow swamp
  that should not be a sink). It would damage the correct cases to patch the
  wrong ones, and it adds an uncalibrated knob. The huge areas must fall out of
  a correct pour-point set, not a constraint layered on routing.

## Decision rule (the classifier)

For each NHDWaterbody polygon **W**, classify **ON-STREAM** (exclude from
dprst) if **any** of:

- **T1 — direction-free pass-through.** A single conveyance flowline
  (`FTYPE ∈ {StreamRiver 460, ArtificialPath 558, Connector 334, CanalDitch 336}`)
  crosses W's boundary **≥2 times** (the line clipped to W has both an entry and
  an exit). Unambiguous through-flow; needs no direction inference.
- **T2 — directional endpoint pairing.** W has **≥1 inflow** flowline
  (downstream endpoint inside W) **AND ≥1 outflow** flowline (upstream endpoint
  inside W), using NHD digitization order (downstream = last vertex), trusted
  **only where `FLOWDIR = "With Digitized"`**. Catches the common case where NHD
  breaks the flowline at the waterbody shore (a line terminates at the inflow
  shore and a separate line resumes at the outflow shore) — which T1 alone
  misses.
- **T3 — on-network coincidence (additive).** W overlaps an NHDArea conveyance
  polygon (the 2-D channel representation). Catches wide braided/anastomosing
  rivers digitized as polygons rather than single centerlines.

**Force-dprst override (beats T1/T2/T3):** `FTYPE ∈ {Playa 361, Ice Mass 378}`
always stays dprst. This is the endorheic guardrail and the graceful-degradation
path where flowline `FLOWDIR` is unreliable (the Great Basin
`FLOWDIR = "Uninitialized"` problem; ~15% of VPU-16 flowlines).

Otherwise → **DPRST** (unchanged).

### Why this rule preserves endorheic sinks

Requiring **both** inflow and outflow is what protects the cases dprst is built
for:

- A **terminal playa** has inflow only (no outflow) → stays dprst.
- A **locally-spilling pothole** has outflow only (no upstream stream entering)
  → stays dprst.
- An **isolated depression** has neither → stays dprst.
- Only a genuine **pass-through** feature has both → promoted to on-stream.

Inflow magnitude (QA_MA) and contributing-area size are **inflow** signals, not
**through-flow** signals: a large terminal sink fed by a big river (Great Salt
Lake, Salton Sea, a Great Basin playa draining a large closed basin) has high
QA_MA and a huge contributing-area ratio, so a QA_MA threshold or area-ratio
tie-breaker would **wrongly promote exactly the endorheic features we must
protect**. Those signals are therefore rejected (see Out of scope).

## Architecture

Mirrors the existing `WBAREACOMI` connectivity pattern exactly. The on-stream
set becomes the **union of two auditable COMID sources**; nothing currently
on-stream is ever removed (the change is strictly additive).

### New staging module: `download/nhd_flowthrough.py`

A sibling of `download/nhd_flowlines.py`. Per-VPU loop (same VPU index and S3
NHDSnapshot inputs already on disk):

1. Read `NHDFlowline` **with geometry** + `COMID` + `FTYPE` + `FLOWDIR`
   (today `nhd_flowlines.py` reads attributes only with `read_geometry=False`;
   this module needs geometry). Filter to conveyance FTYPEs.
2. Read `NHDArea` (conveyance FTYPEs) — present in all 21 VPU snapshots.
3. Read the VPU's `NHDWaterbody` polygons; drop force-dprst FTYPEs (Playa, Ice
   Mass) up front so they can never be promoted.
4. Run T1/T2/T3 via a spatial join (flowlines × waterbody polygons, NHDArea ×
   waterbody polygons), keyed by waterbody `COMID`.
5. Union the flow-through `COMID`s across VPUs and write
   `input/nhd/flowthrough_waterbody_comids.parquet` — a **second** parquet, kept
   separate from `connected_waterbody_comids.parquet` for provenance and
   before/after diffing.

This is a one-time per-VPU **vector** spatial join, sized like
`nhd_flowlines` (no CONUS-grid array, no 384G concern). It runs in the
NHD-staging stage, not in the depstor builders.

### Integration: `depstor_builders/wbody_connectivity.py`

A minimal change: load **both** parquets and union the COMID sets before
`select_connected_waterbodies` (which already matches on `COMID` **or**
`member_comid`, so merged-waterbody handling is free — the flow-through set is
keyed by original NHDWaterbody COMID, which equals `member_comid` in the merged
`conus_waterbodies.gpkg`).

Everything downstream (`connected_wbody.tif` → `dprst` → …) is **unchanged
code**; it simply sees a larger on-stream set.

## Rebuild cascade & water-balance consistency

Changing the on-stream set triggers the established dprst rebuild cascade:

```
wbody_connectivity → dprst → perv / routing / drains_perv / drains_imperv / carea_map
```

(`waterbody`, `vpu_id`, `imperv` unaffected.) `drains_to_dprst` shrinks **by
construction** — the reclassified swamps are no longer dprst pour-points — with
no cap and no kernel change. `dprst`+`waterbody` remain the memory ceiling
(~384G); `routing` ~80–96G; per the established cascade.

**Water-balance check (closed budget).** Every cell a waterbody loses from
`dprst` must be re-credited to the on-stream / carea branch, never silently
fall into `perv`. The existing pipeline already provides this path:

- `dprst.py` emits `onstream_binary.tif` alongside `dprst.tif`.
- `carea_map.py` consumes `onstream` (`carea_map.py:85,192-202`); the carea
  keep-rule is `land_valid & is_perv & (above_thresh | is_onstream)`
  (`depstor.py:209`).
- `onstream_storage_frac` (`configs/depstor/depstor_params.yml`) reads
  `onstream_binary.tif` directly.

A reclassified cell becomes `dprst=0, onstream=1, perv=1` (perv excludes dprst
but not onstream, so an on-stream land cell is pervious), and is picked up by
the `is_onstream` branch. The budget stays closed with **no `perv` builder
change**. This is asserted by a test rather than assumed.

## Validation

- **Fast regression (unit).** A small labeled fixture: a handful of the 28
  through-flow swamps that must flip to on-stream, plus confirmed Great Basin
  playas / prairie potholes that must **not** flip — asserted through the
  classifier. Built per the repo's "builder + test together" convention
  (`tests/test_nhd_flowthrough.py`).
- **CONUS gate.** Formalize the 2026-06-25 per-VPU `drains_to_dprst` coverage
  diagnostic into a repeatable script. Acceptance:
  - humid open-drainage VPUs (Lower Miss, S. Atl-Gulf, Great Lakes) drop
    sharply;
  - Great Basin / Rio Grande stay ~flat (endorheic preservation);
  - before/after on the 30 largest dprst waterbodies — the through-flow ones
    flip, the genuine sinks do not.

## Out of scope (YAGNI / follow-up)

- **QA_MA discharge threshold and contributing-area-ratio tie-breaker**
  (from the research note) — rejected: they reintroduce tuning knobs and
  actively mis-promote endorheic sinks with large inflows.
- **Open-vs-closed second screen** — a surviving dprst waterbody with *no*
  outflow flowline is a true closed depression and could get
  `dprst_frac_open < 1` (vs. the current uniform open/fill-and-spill
  assumption). A separate concern, recorded for a future follow-up if the
  prairie-pothole HRUs matter for SWE/recharge targets.
- **PlusFlowlineVAA node topology (`FromNode`/`ToNode`)** — the authoritative
  direction signal; requires staging the NHDPlusAttributes archive. Kept as a
  **documented fallback**, reached for only if validation shows residual
  over-extension that T1/T2/T3 cannot resolve.

## Docs to update (same branch)

Per repo convention, audit and update on the same branch:
`docs/ARCHITECTURE.md` (connectivity/dprst data flow), `slurm_batch/RUNME.md`
and `HPC_REFERENCE.md` (new `nhd_flowthrough` staging step + rebuild cascade),
`README.md` if user-facing, and the relevant `configs/depstor/*.yml` comments.
