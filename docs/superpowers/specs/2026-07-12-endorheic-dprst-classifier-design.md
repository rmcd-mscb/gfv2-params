# Endorheic depression-storage classifier — design

**Date:** 2026-07-12
**Status:** design, awaiting review
**Issue:** file on approval
**Strictly additive** to the dprst/on-stream classifier chain built by #145
(flow-through), #152 (topology / rule D1), #158 (on-stream barrier), #161
(Non-Network flowline gate). Supersedes nothing.

## Problem

The dprst/on-stream classifier decides whether an NHD waterbody is depression storage
or an on-stream waterbody. Today that rests on two tests that are both **local** and
both **attribute-based**: WBAREACOMI artificial-path topology (`nhd_flowlines`) and
geometric flow-through (`nhd_flowthrough`). Each asks "does a Network flowline enter
and leave *this* waterbody?"

No local test can see that a basin is endorheic. NHD draws Network artificial paths
between the arms of a terminal lake, so the lake reads as through-flowing and gets
promoted on-stream. The flagship casualty is the **Great Salt Lake**, currently
classified **on-stream** — which also makes it a traversal barrier in `routing`
(#158), so land draining into it is scored as *not* reaching a depression.

## The answer: ask the hydrology, not the attributes

**A waterbody is depression storage iff its water's terminus lies inside itself.**

Concretely, per waterbody `W`:

```
pour     = the FDR code-0 (terminal) cells lying inside W
frac_own = share of W's cells whose D8 path REACHES one of those cells
dprst   <=> frac_own > 0.5
```

* Great Salt Lake's water ends **in Great Salt Lake** → `frac_own = 1.000` → dprst.
* A pond upstream in the *same closed basin* ends in **GSL, not the pond** → stays
  on-stream. (This is why the rule is *terminus-inside-itself*, not merely
  *terminates-at-a-sink*: the latter would demote every on-stream reservoir in every
  closed basin.)
* Lewis and Clark Lake's water ends **in the Gulf of Mexico** → `frac_own = 0.007` →
  on-stream.

Two properties make this the right rule rather than just a better heuristic:

1. **It needs no new input.** `fdr_raster` is already a required profile key and
   `d8_routing.drains_to_dprst_kernel` already exists.
2. **The classifier and the router agree by construction.** `d8_routing` documents
   that "any other value (notably nodata 255, or 0) is treated as a sink/terminus"
   ([`d8_routing.py:29`](../../../src/gfv2_params/d8_routing.py)). The router already
   dead-ends at code-0 cells. Defining "depression" as "contains the cell where the
   router terminates" means a waterbody the router treats as a flow terminus *is* a
   depression — not by validation, but by definition.

It is the direct structural analogue of `same_hru_drains`
(`drains_to_dprst_hru == hru_id`): label the termini, run the labelled traversal,
compare reached-id against own-id.

## Evidence

All measured against the current CONUS `gfv2` product.

### The FDR is not fully depression-filled — and its terminal cells are the signal

Sampling `gfv2_fdr.vrt`:

* The CONUS FDR contains exactly **15,262 code-0 (terminal) cells**.
* **8,591 of the 8,611 NHD sink points (99.8 %) land on one.**

`CLAUDE.md` states the NHDPlus FdrFac is "stream-burned + walled + fully
depression-filled … with interior sinks removed". **That is false**, and it is the
load-bearing correction here: NHDPlus leaves these sinks unfilled *by design*, and
they are exactly what makes this classifier possible. The "benign FDR code-0
warnings" noted during #145 **are this dataset**.

### `input/nhd/NHD_sink_points.gpkg` is an incomplete extract — stage `Sink.shp` instead

Against NHDPlus's authoritative `NHDPlusBurnComponents/Sink.shp` for VPU 16:

| | `NHD_sink_points.gpkg` | **NHDPlus `Sink.shp`** |
| --- | --- | --- |
| VPU 16 sinks | 537 | **3,222** |
| SINKIDs unique to it | **0** (a strict subset) | 2,685 |
| **sinks inside Great Salt Lake** | **0** | **29** |
| VPU-16 terminal cells explained (≤100 m) | 13.8 % | **80.9 %** |

The cause is specific: NHDPlus sinks include **`PURPCODE 1 = "BurnLineEvent network
end"`** — a sink placed where a burned flowline's network *ends*, i.e. exactly the
terminus of a terminal lake. There are **29 of them inside Great Salt Lake**. The
staged extract carries purpose codes 3–10 and **omits PURPCODE 1 entirely**, so it
systematically drops precisely the class of sink that marks endorheic lakes.

**Do not use `NHD_sink_points.gpkg`.** `nhd_burn_components` stages `Sink.shp` from
source. Even so, the sink vectors are retained only for provenance
(`PURPCODE`/`PURPDESC`) and the BurnAddWaterbody linkage (`SOURCEFC`/`FEATUREID`) —
**not** as a classifier signal, because Signal A reads the grid the router reads.

> This is the third hand-made extract in `input/nhd/` found to be defective, alongside
> `closed_huc12.gpkg` (23 type-C HUC12s in the Great Basin vs 141 in the full WBD) and
> the `conus_waterbodies` layer (all 66,488 SwampMarsh removed). **Stage from the
> authoritative source; do not consume the pre-made extracts.**

### Rule validation

| | result |
| --- | --- |
| Demotions | **216 polygons, 7,877 km²** |
| Classic terminal lakes caught | **10 / 10** |
| Domain exits wrongly demoted | **0 / 10** |
| Lewis and Clark Lake (Missouri mainstem reservoir) | **rejected**, `frac_own = 0.007` |

Caught: Great Salt Lake (1.000), Salton Sea (1.000), Pyramid (1.000), Goose (1.000),
Honey (1.000), Mono (1.000), Lake Abert (0.990), Summer (1.000), Devils Lake (1.000),
Eagle Lake (1.000). Greaser Reservoir — a closed-basin impoundment in Lake County, OR
— comes through at 1.000, correctly.

Rejected: **Lake Michigan, Superior, Huron, Erie, Ontario, Lake Champlain, Lake of the
Woods, Lake Borgne, the Everglades, California Swamp** — all of them, *with no guard
at all*, because NHDPlus routes their water out and the grid says so.

**The 0.5 threshold does no work.** `frac_own` is bimodal — **204 of 239** candidates
sit at ≥ 0.95, and only **3** land in the 0.45–0.55 band:

| threshold | dprst | area |
| --- | --- | --- |
| 0.3 | 223 | 8,069 km² |
| **0.5** | **216** | **7,877 km²** |
| 0.7 | 208 | 7,833 km² |

A 3 % swing across 0.3–0.7. This is a physical bimodality, **not a tuned knob**.

### Why the WBD closed-basin signal still earns its place

The FDR has one miss: **Walker Lake** (144 km²) contains no code-0 cell, so the rule
never sees it. The full WBD types its HUC12 `C` and catches it at `frac = 1.000`. The
two signals are complementary, not redundant.

**Post-CONUS-rollout correction:** the "complement" framing above understates how much
work Signal B actually does. Measured on the shipped CONUS tables, of 818 total
demotions, 543 are Signal-B-only, 112 Signal-A-only, and 163 both — **by COUNT, Signal
B dominates**. **By AREA it does not**: Signal-B-only demotions are small (median
~0.09 km², ~1,400 km² total — ponds and playas sitting inside a closed basin, not large
lakes), while Signal A carries the overwhelming majority of the demoted area, including
the Great Salt Lake itself (4,369 km²). Read "earns its place because Walker Lake"
above as "earns its place, and turns out to matter far more broadly than Walker Lake
alone" — Signal B is not a rare-case patch, it is the majority-by-count contributor.

**`closed_huc12.gpkg` is an incomplete extract — use the full WBD.** It has 23 type-C
HUC12s in the Great Basin against **141** in NHDPlus's `WBDSnapshot`, and resolves
only **1 of the 10** classic terminal lakes (the full WBD resolves 5). abock's file is
retained as a cross-check only.

### Why the network-terminal signal is DROPPED

A fabric-network "segments enter but none leaves" rule was prototyped and measured. It
found all 10 terminal lakes — but **unguarded it also flags Lake Michigan
(57,905 km²)**, and every guard tested leaked:

| guard | Great Lakes | Lake Champlain | Gulf marshes |
| --- | --- | --- | --- |
| majority inside the HRU fabric | rejects | **passes** | **passes** |
| WBD type-F | — | **passes** (`fracF = 0.000`) | rejects |
| NHDPlus `LandSea` | **useless** (`ls_frac ≈ 0` even for Lake Michigan) | — | — |

Rule C catches the same 10 lakes **and** rejects Champlain, Lake of the Woods and the
Everglades. The network signal therefore buys nothing.

**Dropping it removes the classifier's only dependency on the fabric segment
network** — so no reliance on the draft network (whose `seg_id` is per-VPU, whose
`vpu_agg_id` has 82,609 duplicate rows, and whose `to_segment` never crosses a VPU),
and **no need for the fabric↔NHD crosswalk**. Nothing to re-validate when the network
is rebuilt.

### Orphan sinks and why BurnAddWaterbody is load-bearing

> ⚠️ **The figures in this section are provisional floors.** They were computed from
> `NHD_sink_points.gpkg` and `sink_cats.gpkg`, which share the same incomplete sink
> subset (`sink_cats` joins 8,476 of its 8,520 catchments to that file's SINKIDs).
> Recompute them at implementation against the authoritative `Sink.shp` and its
> catchments. The *direction* is robust — orphan sinks need an area, not a
> pour-point — but every number below will move.

2,402 terminal cells fall inside no waterbody. `sink_cats.gpkg` gives their
contributing area:

| | catchments | land | zero dprst area inside |
| --- | --- | --- | --- |
| **BurnAdd** (has a polygon in NHDPlus) | 1,419 | 22,684 km² | 1,379 (18,650 km²) |
| **not BurnAdd** (no polygon anywhere) | 983 | 51,946 km² | 773 (21,484 km²) |
| **total** | **2,402** | **74,630 km²** | **2,152 (40,135 km²)** |

Only 10.2 % of that land is currently `drains_to_dprst`, and total dprst area inside
all 2,402 catchments is **230 km²** — a contributing-to-depression ratio of 325:1
against a CONUS norm of ~12:1.

**Orphan sinks cannot become routing pour-points without an area.** PRMS ignores
`sro_to_dprst_*` where `dprst_frac == 0`, so pour-points alone would be inert for
2,152 catchments and incoherent for the rest. BurnAddWaterbody supplies area for 1,419
of them; the remaining 983 are the footprint spike.

### BurnAddWaterbody is real, usable area (spike, VPU 16)

* 23 polygons, **374.5 km²**, largest a **136.8 km² playa**.
* `PolyID` (negative) joins 1:1 to the sink `FEATUREID`; `PurpCode` matches the sink
  purpose codes (4 = Playa, 8 = closed lake).
* **All 20** BurnAdd sinks in RPU 16 fall inside a BurnAddWaterbody polygon.
* **0 of 23** overlap an existing waterbody → all genuinely new area.

## Design

Both signals feed the **existing** classifier as a strict subtraction from the
on-stream COMID set — structurally identical to the `NEVER_ONSTREAM_FTYPES` guardrail
already in `wbody_connectivity`. It can only *remove* waterbodies from on-stream, so
it can never inflate the on-stream mask.

### Signal A (primary) — FDR terminus-inside-itself

As specified above. Runs per-VPU-tiled like `routing`, reusing
`d8_routing.drains_to_dprst_kernel`. Emits `frac_own` per COMID so the demotion is
auditable.

### Signal B (complement) — WBD closed basin

A waterbody majority-inside (`> 0.5`) the **dissolved union** of the full WBD's
`HU_12_TYPE == 'C'` HUC12s.

**Dissolve first, then measure**: a lake straddling two *adjacent* closed HUC12s is
fully inside the closed system but majority-inside neither polygon individually.

**Majority-area — not `intersects`, not `within`.** `within` fails on GSL (it spills
1.1 % into a neighbouring HUC12). `intersects` wrongly grabs lakes that merely graze a
closed boundary at `frac_in = 0.000`.

> ⚠️ A geometry touching another's boundary with **zero interior overlap** returns
> `True` from `intersects`. This artifact produced a false "Cedar Lake routes out of
> its closed basin" reading during design. Do **not** "simplify" this predicate back
> to `intersects`.

**Filter `HU_12_TYPE == 'C'` ourselves** rather than trusting any upstream selection —
`closed_huc12.gpkg` contains 219 non-C rows, 212 of which are fully *contributing*
HUC12s that drain into closed ones.

### BurnAddWaterbody — new dprst area, and pour-points for free

Their `PolyID` is **negative**, so they can never match a WBAREACOMI or flow-through
COMID — structurally incapable of on-stream promotion, which is correct, since
NHDPlus flagged every one as a sink. **Asserted, not left to luck.**

Once they are waterbody polygons they flow through `waterbody → dprst → routing`
untouched and **become dprst pour-points automatically**. **`routing` needs no
change.**

## Architecture

**New — `src/gfv2_params/download/nhd_burn_components.py`**
Mirrors `nhd_flowlines.py`: S3-listing discovery, pattern
`_{vpu}_NHDPlusBurnComponents_(\d+)\.7z`, py7zr extract. Two products:
* `sink_points.parquet` — provenance + the BurnAdd linkage (**not** a classifier input)
* `burn_add_waterbodies.parquet` — `PurpCode` → FTYPE: **4 → `Playa`, 8 →
  `LakePond`**. Any other `PurpCode` **fails loud**: an unrecognised code must not
  default to a FTYPE, because FTYPE drives `NEVER_ONSTREAM_FTYPES`.

**New — `src/gfv2_params/download/wbd_huc12.py`**
Full WBD from NHDPlus's per-VPU `WBDSnapshot` (~25 MB/VPU) → `wbd_huc12.parquet`.

Both iterate `nhd_flowlines.vpu_index` — the **21 NHDPlus VPU archive codes**
(`03N`/`03S`/`03W`, `10L`/`10U`, …), which is what the S3 keys are named by. (The
18-vs-21 trap in `shared_rasters_vpu_scope_mismatch` / PR #150 concerns *raster tiles
on disk*, not NHDPlus archives — it does not apply here.)

**New — `src/gfv2_params/endorheic.py`**
Pure functions, no I/O: `terminus_own_fraction()` (Signal A),
`closed_basin_comids()` (Signal B), and a combiner emitting a COMID set with
`by_terminus` / `by_closed_huc12` provenance. Unit-testable on synthetic arrays and
GeoDataFrames.

**Changed — `depstor_builders/wbody_connectivity.py`**
One strict subtraction: `connected -= endorheic`.

**Changed — `depstor_builders/waterbody.py`**
Unions in `burn_add_waterbodies`, with the negative-COMID assertion.

**Changed — `configs/base_config.yml`**
New **optional** keys — `wbd_huc12_table`, `sink_points_table`,
`burn_add_waterbody_table`. Absent → today's behaviour. Signal A needs **no new key**
(`fdr_raster` already exists), so it works on every fabric immediately.

**Tests** — `tests/test_endorheic.py`: a synthetic terminus-inside-itself case, a
through-flowing case (the Lewis and Clark shape), the boundary-graze regression, and
the `PurpCode` fail-loud. Plus extensions to `test_wbody_connectivity.py` and
`test_waterbody.py`.

## Risk

**Great Salt Lake flips from routing *barrier* to *pour-point*.** Demoted, land will
trace *into* it rather than dead-ending at it. The GSL basin is ~55,000 km².

Bounded by two things, but **measured, not argued**:

* `same_hru_drains` (#160/#162) restricts `sro_to_dprst_*` to depressions in the
  cell's **own HRU**.
* The #158 barrier set is the *full* on-stream mask, so Utah Lake, Bear Lake and every
  other outletted waterbody remain barriers. Only paths reaching GSL *without* first
  hitting an on-stream waterbody newly count — which is the correct answer, not a
  regression.

`drains_to_dprst.tif` is HRU-agnostic and **will** grow in VPU 16.

## Validation (a gate, not a footnote)

1. **VPU 16 A/B** via `scripts/diagnose/ab_drains_to_dprst.py` (#147/#148). Confirm
   `drains_to_dprst` grows only where GSL is the same-HRU depression, and that the six
   PRMS ratios move sanely.
2. **Named fixtures.** Assert dprst: GSL, Salton Sea, Pyramid, Mono, Walker, Abert,
   Summer, Honey, Goose, Devils Lake. Assert on-stream: **Lake Michigan, Superior,
   Huron, Erie, Ontario, Lake Champlain, Lake of the Woods, Lake Borgne, the
   Everglades, Lewis and Clark Lake.** These twenty are the whole point of the rule.
3. **Threshold insensitivity** — re-report the 0.3/0.5/0.7 sweep; if it is no longer
   flat, the rule has drifted and must be re-examined.
4. **BurnAddWaterbody disjointness** — 0 overlap with existing waterbody polygons
   CONUS-wide (VPU 16 spike: 0/23); imperv/dprst/perv partition stays disjoint.
5. **Rebuild cascade** — `wbody_connectivity → dprst → routing → drains_perv/imperv`;
   `waterbody` also rebuilds. `--mem=384G` for `dprst`/`waterbody`, `96G` for
   `routing`.

## Docs

* **`CLAUDE.md`** — correct the FdrFac claim, and record that the FDR's 15,262 code-0
  cells are the endorheic classifier's primary signal.
* **`docs/ARCHITECTURE.md`** — the two new signals in the classifier chain.
* **`slurm_batch/RUNME.md` / `HPC_REFERENCE.md`** — the two staging steps.

## Follow-ups

### 1. Footprints for the 983 area-less sinks (spike)

51,946 km² of contributing area with no mapped depression. The NHDPlus HydroDEM is
depression-filled *everywhere except these sinks*, so `fill(hydrodem) − hydrodem`
isolates **exactly** them, yielding both area and depth (the latter feeding
`dprst_depth_avg`, #173). Mirror #173's Phase-0 spike.

### 2. Question for abock re `conus_waterbodies.gpkg`

Its newer `conus_waterbodies` layer is our `waterbodies` layer **minus all 66,488
SwampMarsh + 4 Estuary (149,468 km², including the Everglades)**, plus 221
`sink_waterbodies` — negative-COMID LakePonds totalling **5.3 km²**, ~90 % in
**northern Maine**, a median **1,257 km** from the nearest sink point, in a region with
**no sink coverage at all** (`InRPU` spans 03c–18c). We keep
`waterbody_layer: waterbodies` and do not consume that layer.
