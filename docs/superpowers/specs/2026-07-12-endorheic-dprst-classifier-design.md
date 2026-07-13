# Endorheic depression-storage classifier — design

**Date:** 2026-07-12
**Status:** design, awaiting review
**Issue:** file on approval
**Strictly additive** to the dprst/on-stream classifier chain built by #145
(flow-through), #152 (topology / rule D1), #158 (on-stream barrier), #161
(Non-Network flowline gate). Supersedes nothing.

## Problem

The dprst/on-stream classifier decides whether an NHD waterbody is depression
storage or an on-stream waterbody. Today that decision rests on two tests that are
both **local**: WBAREACOMI artificial-path topology (`nhd_flowlines`) and geometric
flow-through (`nhd_flowthrough`). Each asks "does a Network flowline enter and leave
*this* waterbody?"

No local test can see that an entire *basin* is endorheic. NHD draws Network
artificial paths between the arms of a terminal lake, so the lake reads as
through-flowing and is promoted on-stream. The flagship casualty is the **Great Salt
Lake**, currently classified **on-stream** — which also makes it a traversal barrier
in `routing` (#158), so land draining into it is scored as *not* reaching a
depression.

Two new datasets supply the basin-scale signal the local tests structurally cannot,
and a third supplies depression *area* that we are missing entirely.

## Scope

**In (this effort):** Signal 1 (NHDPlus sinks), Signal 2 (WBD closed basins, from
the **full** WBD), BurnAddWaterbody area, the `CLAUDE.md` FdrFac correction.

**Out (spikes, filed separately):** Signal 3 (network-terminal), sink footprints.
Both are specified in *Follow-ups* below with their evidence, because both are
strong ideas that need a real answer rather than a guard.

## Evidence

All numbers measured against the current CONUS `gfv2` product.

### The new datasets overwhelmingly confirm the existing classifier

Of the waterbodies each signal flags, **99.5–99.6 % are already classified dprst**.
These datasets validate the existing classifier far more than they correct it. That
is why the correction is small and surgical.

### The sink points are the FDR's terminal cells

Sampling `gfv2_fdr.vrt` at all 8,611 sink points:

* CONUS FDR contains exactly **15,262 code-0 (terminal sink) cells**.
* **8,591 of the 8,611 sink points (99.8 %) land on one.**

`CLAUDE.md` currently states the NHDPlus FdrFac is "stream-burned + walled + fully
depression-filled … with interior sinks removed". **That is false.** NHDPlus leaves
these 8,611 sinks unfilled *by design*; they are why the FDR has code-0 cells at
all. The "benign FDR code-0 warnings" noted during #145 **are this dataset**.

### `closed_huc12.gpkg` is an incomplete extract — use the full WBD

abock's file has **698** type-C HUC12s. Against NHDPlus's per-VPU `WBDSnapshot`:

| VPU | abock type-C | **full WBD type-C** | full WBD type-F |
| --- | --- | --- | --- |
| 16 Great Basin | 23 | **141** | 317 |
| 18 California | — | **136** | 340 |
| 17 Pacific NW | — | **152** | 423 |

It resolves only **1 of the 10 classic terminal lakes** (GSL). The full WBD resolves
**5** — adding Pyramid (466 km²), Lake Abert (164), Walker (144) and Summer (122),
each of which `closed_huc12.gpkg` reports at `frac_in = 0.000`.

**Signal 2 therefore consumes the full WBD, not `closed_huc12.gpkg`.** abock's file
is retained only as a cross-check.

### Demotion set

Against the *effective* on-stream set (COMID union **after** the existing
`NEVER_ONSTREAM_FTYPES` Playa/Ice-Mass guardrail — 160,878 polygons / 346,201 km²):

| signal | demotes |
| --- | --- |
| Signal 1 — sink point inside the waterbody | 31 polygons, 122 km² |
| Signal 2 — majority-inside a type-C HUC12 *(abock's file)* | 43 polygons, 4,413 km² |
| **union (11 overlap)** | **63 polygons, 4,503 km²** |

CONUS dprst area **53,155 → 57,657 km² (+8.5 %)**; 4,369 km² of it is the Great Salt
Lake.

> **These are floors.** Signal 2's figures are measured against abock's incomplete
> file. On the full WBD the set is strictly larger — at minimum adding Pyramid,
> Abert, Walker and Summer (+896 km² from those four alone). The exact CONUS figure
> is measured once the full WBD is staged, and is a deliverable of implementation.

### The NHM segment network independently corroborates the demotion

* **650 of the 698** type-C closed HUC12s contain **no NHM segment at all**.
* Of the 122 segments touching one, **93 are terminal**; 22 more route to another
  segment inside the closed set.
* Only **7** have a downstream link outside the closed set, and every one is a
  **boundary graze** — share of length actually inside: 3.4 %, 16.9 %, 19.8 %,
  **0.0 %**, 9.6 %, 3.5 %, 2.3 %. None drains a closed basin and carries water out.
* **Great Salt Lake: 47 segments intersect its HUC12 — 29 terminal, 18 routing
  onward, all 18 staying inside, zero exiting.** 30 carry the fabric's own hand-set
  `gsl == 1` flag.

NHM's routing network already treats GSL as a terminal sink. The depstor classifier
is the component out of step; demoting GSL brings it back into agreement with the
network the parameters are for.

### Orphan sinks and why BurnAddWaterbody is load-bearing

2,402 sinks fall inside no waterbody polygon. `sink_cats.gpkg` (NHDPlus's own
per-sink catchment) gives their contributing area:

| | catchments | land | zero dprst area inside |
| --- | --- | --- | --- |
| **BurnAdd** (has a polygon in NHDPlus) | 1,419 | 22,684 km² | 1,379 (18,650 km²) |
| **not BurnAdd** (no polygon anywhere) | 983 | 51,946 km² | 773 (21,484 km²) |
| **total** | **2,402** | **74,630 km²** | **2,152 (40,135 km²)** |

Only 10.2 % of that land is currently scored `drains_to_dprst`, and the total dprst
area inside all 2,402 catchments is **230 km²** — a contributing-to-depression ratio
of 325:1 against a CONUS norm of ~12:1.

**So orphan sinks cannot be made routing pour-points without also giving them an
area.** PRMS ignores `sro_to_dprst_*` where `dprst_frac == 0`, so pour-points alone
would be inert for 2,152 catchments and incoherent for the rest. BurnAddWaterbody
supplies that area for 1,419 of them; the remaining 983 are the sink-footprint spike.

### BurnAddWaterbody is real, usable area (spike, VPU 16)

Downloaded `NHDPlusGB/NHDPlus16/NHDPlusBurnComponents`:

* 23 `BurnAddWaterbody` polygons, **374.5 km²**, largest a **136.8 km² playa**.
* `PolyID` (negative) joins 1:1 to the sink `FEATUREID`; `PurpCode`/`PurpDesc` match
  the sink purpose codes exactly (4 = Playa, 8 = closed lake).
* **All 20** BurnAdd sinks in RPU 16 fall inside a BurnAddWaterbody polygon.
* **0 of 23** overlap any existing waterbody → all genuinely new area.

The same archive also contains `Sink.shp` (making `NHD_sink_points.gpkg` reproducible
from source).

## Design

Both signals feed the **existing** classifier as a strict subtraction from the
on-stream COMID set — structurally identical to the `NEVER_ONSTREAM_FTYPES` guardrail
already in `wbody_connectivity`. Because it can only *remove* waterbodies from
on-stream, it can never inflate the on-stream mask. No caps, no tuning knobs.

### Signal 1 — NHDPlus sink

A waterbody is endorheic if NHDPlus placed a sink in it:

* **COMID join** — `SOURCEFC == 'NHDWaterbody' AND FEATUREID > 0`; `FEATUREID` *is*
  the waterbody COMID. (Validated: 200/200 sampled sinks fall inside their own
  COMID's polygon.)
* **Spatial union** — any sink point *within* a waterbody polygon, catching BurnAdd
  and WBD sinks whose `FEATUREID` does not resolve to a COMID.

### Signal 2 — WBD closed basin

A waterbody is endorheic if it is **majority-inside** (`frac_in > 0.5`) the
**dissolved union** of the full WBD's `HU_12_TYPE == 'C'` HUC12s.

**Dissolve first, then measure.** The union matters: a lake straddling two *adjacent*
closed HUC12s is fully inside the closed system but majority-inside neither polygon
individually. Computing `frac_in` per-polygon would drop it.

**The predicate is majority-area — not `intersects`, not `within`.** `frac_in` is
sharply bimodal: median 1.000, remainder near-zero boundary grazes.

| threshold | polygons | area | GSL kept? |
| --- | --- | --- | --- |
| `intersects` (> 0) | 50 | 4,507 km² | yes |
| **majority (> 0.5)** | **43** | **4,413 km²** | **yes** |
| > 0.9 | 43 | 4,413 km² | yes |
| `within` (> 0.999) | 42 | 44 km² | **no** |

Nothing changes between 0.5 and 0.9 — majority-area is **insensitive, not tuned**.
`within` fails on GSL (it spills 1.1 % into a neighbouring HUC12). `intersects`
wrongly grabs seven lakes that merely graze a closed boundary; Eagle Lake and Middle
Alkali Lake come in at `frac_in = 0.000`. They may well be endorheic, but the WBD did
not flag their HUC12s as closed — **this dataset has no opinion on them, and we must
not invent one from a boundary touch.**

> ⚠️ A geometry touching another's boundary with **zero interior overlap** returns
> `True` from `intersects`. This artifact produced a false "Cedar Lake routes out of
> its closed basin" reading during design. Do **not** "simplify" this predicate back
> to `intersects`.

**We filter `HU_12_TYPE == 'C'` ourselves rather than trusting any upstream
selection.** `closed_huc12.gpkg` contains 219 non-C rows (S/F/M/U/W), 212 of which
have `NCONTRB_A == 0` — fully *contributing* HUC12s that drain into closed ones, not
closed themselves. Consuming them wholesale would demote lakes sitting on the
internal stream network of contributing basins.

### BurnAddWaterbody — new dprst area, and pour-points for free

A staging module emits the BurnAddWaterbody polygons; the `waterbody` builder unions
them into the waterbody layer.

Their `PolyID` is **negative**, so they can never match a WBAREACOMI or flow-through
COMID — structurally incapable of on-stream promotion, which is correct, since
NHDPlus flagged every one as a sink. **This is asserted, not left to luck.**

Once they are waterbody polygons they flow through `waterbody → dprst → routing`
untouched and **become dprst pour-points automatically**. Giving a sink a polygon is
the right way to make it a pour-point. **`routing` needs no change.**

## Architecture

Follows the existing pattern exactly — staging modules distil NHD/WBD into tables
that `wbody_connectivity` consumes.

**New — `src/gfv2_params/download/nhd_burn_components.py`**
Mirrors `nhd_flowlines.py`: S3-listing discovery, pattern
`_{vpu}_NHDPlusBurnComponents_(\d+)\.7z`, py7zr extract. One archive, two products:
* `sink_points.parquet` — reproduces `NHD_sink_points.gpkg` from source
* `burn_add_waterbodies.parquet` — new dprst polygons, `PurpCode` → FTYPE:
  **4 → `Playa`, 8 → `LakePond`**. Any *other* `PurpCode` **fails loud**: an
  unrecognised code must not silently default to a FTYPE, because FTYPE drives the
  `NEVER_ONSTREAM_FTYPES` policy (a mis-defaulted Playa would be promotable
  on-stream). VPU 16 carries only 4 and 8; the CONUS set is unverified.

**New — `src/gfv2_params/download/wbd_huc12.py`**
Stages the full WBD from NHDPlus's per-VPU `WBDSnapshot` (same S3 archive family,
~25 MB/VPU) → `wbd_huc12.parquet` with `HUC_12`, `HU_12_TYPE`, geometry.

Both modules iterate the **18 consolidated VPU tiles**, not the 21-entry RPU-split
list — see the `shared_rasters_vpu_scope_mismatch` trap (PR #150), which is exactly
this class of bug.

**New — `src/gfv2_params/endorheic.py`**
Pure functions, no I/O: `sink_comids()`, `closed_basin_comids()`, and a combiner
emitting a COMID set with `by_sink` / `by_closed_huc12` provenance columns. Trivially
unit-testable on synthetic GeoDataFrames.

**Changed — `depstor_builders/wbody_connectivity.py`**
One strict subtraction: `connected -= endorheic`. Three lines from the existing
`NEVER_ONSTREAM_FTYPES` guardrail.

**Changed — `depstor_builders/waterbody.py`**
Unions in `burn_add_waterbodies`, with the negative-COMID assertion.

**Changed — `configs/base_config.yml`**
New **optional** profile keys — `wbd_huc12_table`, `sink_points_table`,
`burn_add_waterbody_table`. Absent → today's behaviour, so `oregon` / `tjc` /
`gfv2_vpu01` are unaffected until opted in.

**Tests** — `tests/test_endorheic.py` (one per signal, plus a boundary-graze
regression asserting `intersects` and majority-area disagree), extensions to
`test_wbody_connectivity.py` and `test_waterbody.py`. Per repo convention, a builder
change ships with its test.

## Risk

**Great Salt Lake flips from routing *barrier* to *pour-point*.** Today GSL is
on-stream, so `routing` treats it as a traversal barrier (#158) and land upslope
dead-ends there. Demoted, it becomes a dprst pour-point and land will trace *into*
it. The GSL basin is ~55,000 km², and this is the same direction as the
over-extension bug #145/#158/#161 spent three PRs suppressing.

Two things bound it, but it **must be measured, not argued**:

* `same_hru_drains` (#160/#162) restricts `sro_to_dprst_*` to depressions in the
  cell's **own HRU**, so a Bear River headwater cell 300 km away cannot contribute.
* NHM already terminates the Bear/Weber/Jordan *at* GSL (29 terminal segments), so
  the demotion aligns depstor with the routing network rather than diverging from it.

`drains_to_dprst.tif` itself is HRU-agnostic and **will** grow in VPU 16.

## Validation (a gate, not a footnote)

1. **VPU 16 A/B** via `scripts/diagnose/ab_drains_to_dprst.py` (#147/#148). Confirm
   `drains_to_dprst` grows only where GSL is the same-HRU depression, and that the
   six PRMS ratios move sanely.
2. **Full-WBD demotion set** — report the CONUS count/area (≥ 63 polygons /
   ≥ 4,503 km²), the FTYPE split, and the delta versus `closed_huc12.gpkg`. Assert
   GSL, Pyramid, Abert, Walker and Summer are all present.
3. **Boundary-graze regression** — assert Eagle Lake and Middle Alkali Lake
   (`frac_in = 0.000`) are **not** demoted.
4. **BurnAddWaterbody disjointness** — assert 0 overlap with existing waterbody
   polygons CONUS-wide (VPU 16 spike: 0/23) and that the imperv/dprst/perv partition
   stays disjoint.
5. **Rebuild cascade** — `wbody_connectivity → dprst → routing → drains_perv/imperv`
   (see `depstor_rebuild_cascade`); `waterbody` also rebuilds because its input layer
   changed. `--mem=384G` for `dprst`/`waterbody`, `96G` for `routing`.

## Docs

* **`CLAUDE.md`** — correct the FdrFac claim. The FDR is depression-filled *except*
  at 8,611 NHDPlus sinks; those are its 15,262 code-0 cells.
* **`docs/ARCHITECTURE.md`** — the two new signals in the classifier chain.
* **`slurm_batch/RUNME.md` / `HPC_REFERENCE.md`** — the two new staging steps and
  their position (before `wbody_connectivity`).

## Follow-ups

### 1. Signal 3 — network-terminal classifier (spike)

**It is the strongest endorheic signal we have**, and worth doing properly:

| signal | of the 10 classic terminal lakes |
| --- | --- |
| `closed_huc12.gpkg` | **1 / 10** |
| full WBD type-C | **5 / 10** |
| **network-terminal** | **10 / 10** |

Mono Lake, Salton Sea, Goose Lake, Honey Lake and Devils Lake sit in HUC12s the WBD
types `S`, and NHDPlus puts **no sink** in them. Nothing in Phase 1 finds them.

Rule: `W` is terminal ⟺ segments with **genuine interior overlap** enter `W` and none
routes to a segment outside `W`. Unguarded on CONUS this flags 2,782 on-stream
waterbodies / 150,755 km², led by **Lake Michigan (57,905 km²)**, because
`to_segment == 0` conflates three things — endorheic basin, **domain exit**, and
network truncation.

Guards tested and their verdicts:

| guard | Great Lakes | Lake Champlain (728 km²) | Gulf marshes |
| --- | --- | --- | --- |
| majority inside the HRU fabric | ✅ rejects (0.1–0.7 % vs ≥ 99.9 %) | ❌ passes | ❌ passes |
| WBD type-F | — | ❌ passes (`fracF = 0.000`) | likely rejects |
| NHDPlus `LandSea` | ❌ useless (`ls_frac ≈ 0` even for Lake Michigan) | ❌ | ❌ |
| domain-edge adjacency | ✅ | ❌ passes | partly |

**No attribute lookup answers the real question — "does this water reach the
ocean?"** Stacking a fourth guard is the cap-and-knob antipattern `CLAUDE.md`
forbids. The spike should answer it physically: **trace D8 on the NHDPlus FDR from
the waterbody.** The flow either exits the domain (exorheic → reject) or dead-ends in
one of the 15,262 code-0 sink cells (endorheic → accept). `d8_routing.py` already
performs exactly this trace.

Open question the spike must resolve first: **GSL has no sink point**, so what does
the FDR do with its water? That determines whether the trace is viable.

Note also that the fabric network is a **draft**: `seg_id` has 18,461 unique values
across 186,709 rows, `vpu_agg_id` has 104,100 unique with **82,609 duplicated rows**,
and `to_segment` never crosses a `source_vpu` — 21 disconnected per-VPU graphs. Only
`(source_vpu, seg_id)` is a valid key. The spike assumes the *intended* contract (a
connected, NHD-crosswalked network) and must enforce it as a **fail-loud
precondition**.

### 2. Footprints for the 983 area-less sinks (spike)

51,946 km² of contributing area — WBD closed-HUC12 sinks, topographic depressions,
karst sinkholes — with no mapped depression anywhere. The NHDPlus HydroDEM is
depression-filled *everywhere except these sinks*, so `fill(hydrodem) − hydrodem`
isolates **exactly** them, yielding both area and depth (the latter feeding
`dprst_depth_avg`, #173). Mirror #173's Phase-0 spike.

### 3. Question for abock re `conus_waterbodies.gpkg`

Its newer `conus_waterbodies` layer is our `waterbodies` layer **minus all 66,488
SwampMarsh + 4 Estuary (149,468 km², including the Everglades)**, plus 221
`sink_waterbodies`. The 221 are negative-COMID LakePonds totalling **5.3 km²**, ~90 %
of them in **northern Maine** — a median of **1,257 km from the nearest sink point**,
in a region with **no sink coverage at all** (`InRPU` spans 03c–18c). We keep
`waterbody_layer: waterbodies` and do not consume that layer.
