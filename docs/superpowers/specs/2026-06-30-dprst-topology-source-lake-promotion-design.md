# Design: NHDPlus-topology source-lake promotion + force-dprst sink guardrail

**Date:** 2026-06-30
**Status:** approved (design); implementation plan to follow
**Related:** PR #145 (flow-through reclassification), PR #139 (WBAREACOMI
connectivity), spec `2026-06-26-dprst-flowthrough-reclassification-design.md`,
memory `drains_to_dprst_overextension`, `dprst_connectivity_via_nhd_wbareacomi`

> **Independent of this work:** a separate, already-implemented fix corrects the
> flow-through T1 crossing-counter for sinuous lines whose boundary intersection
> is a `GeometryCollection` (VPU 15 LakePond COMID 21744935). That change is
> isolated to `nhd_flowthrough.flowthrough_comids` + its unit test and should
> land on its own; it is **not** part of this spec.

## Problem

The on-stream / dprst classifier infers flow direction from **geometry**
(NHD digitization order) gated on the `FLOWDIR` field, and only promotes a
waterbody that has **both** channel inflow and outflow (or WBAREACOMI). Two
opposite defects follow, both visible in one closed-basin lake cluster in VPU 14
(Upper Colorado, NW New Mexico, ~−108.84°, 36.02°):

1. **Source/headwater lakes are wrongly left in dprst.** NHDWaterbody COMID
   **16969532** (LakePond) is a headwater lake: its single network connection is
   `16974320`, a `STARTFLAG=1` headwater flowline that *leaves* the lake and
   routes downstream (`DnHydroseq ≠ 0`). It has outflow but no inflow, so the
   inflow-AND-outflow rule leaves it dprst. Two other StreamRiver channels touch
   it but are **non-network** (absent from `PlusFlowlineVAA`), so the geometric
   classifier is also reasoning over noise. The `FLOWDIR=Uninitialized` value on
   those channels (the documented Great-Basin degradation) is why direction
   could not be trusted.

2. **Terminal sinks are wrongly promoted to on-stream via WBAREACOMI.** The
   WBAREACOMI path (`connected_waterbody_comids.parquet`) has **no** force-dprst
   guardrail, so NHD artificial paths drawn *into* a terminal lake tag it
   on-stream. A VPU-14 probe found **47** currently-on-stream waterbodies with no
   routed network outflow, **16 of them Playa (12) + Ice Mass (4)** — the exact
   FTYPEs the design says must never be promoted.

Both stem from the same missing capability: the pipeline stages **no NHDPlus
flow topology**. NHDPlus carries authoritative per-flowline direction
(`FromNode`/`ToNode`, `Hydroseq`/`DnHydroseq`) and network membership for every
flowline, independent of the `FLOWDIR` field.

## Goal

Stage NHDPlus flowline topology as a reusable shared input, and use it to fix the
classifier's **source** and **sink** tails:

- **Promote** waterbodies that discharge to the routed network (source lakes and
  pass-throughs), using authoritative direction so `FLOWDIR=Uninitialized` no
  longer demotes a real network node.
- **Guard** the force-dprst FTYPEs (Playa, Ice Mass) on **every** promotion path,
  closing the WBAREACOMI sink leak.

No cap, no tuning knob, no size/QA_MA threshold (those would damage real
endorheic features — see the 2026-06-26 spec's rejection of inflow-magnitude
signals).

## Decision rule (the classifier)

For each NHDWaterbody **W**, classify **ON-STREAM** (exclude from dprst) iff
`FTYPE(W) ∉ {Playa, Ice Mass}` **and any** of:

- **D1 — routed network outflow (NEW, topology-based).** A conveyance flowline
  **F** that is in the NHDPlus network and routes downstream
  (`F ∈ PlusFlowlineVAA` and `DnHydroseq(F) ≠ 0`) discharges out of W: F's
  **authoritative upstream end is inside W** (a stream leaves W). This single
  test captures **both** the pass-through lake (the split-at-shore outflow half
  has its upstream end inside W) **and** the source/headwater lake (a `STARTFLAG`
  origin inside W). It replaces the old **T2** (`FLOWDIR`-gated directional
  endpoint pairing).
- **T1 — geometric pass-through (unchanged).** A single conveyance flowline
  crosses W's boundary ≥2 times. Retained as a direction-free, topology-free
  fallback for an unsplit line through W and for flowlines absent from VAA.
- **T3 — NHDArea coincidence (unchanged).** W overlaps a conveyance NHDArea
  polygon.

**Why D1 is correct and preserves the endorheic guardrail.** Tabulating each
waterbody class by whether it has a routed network outflow:

| class | inflow | outflow | routed outflow → D1 | label |
|---|---|---|---|---|
| pass-through | ✓ | ✓ | yes | on-stream |
| source / headwater | ✗ | ✓ | yes | on-stream |
| terminal sink (playa, Great Salt Lake) | ✓ | ✗ | no | dprst |
| isolated | ✗ | ✗ | no | dprst |

A terminal sink's inflow flowline has its **downstream** end inside W (upstream
end outside) → D1 false → stays dprst. Great Salt Lake and Great Basin playas
have no routed outflow → preserved as dprst. The old rule required inflow only as
a proxy to exclude sinks; "has a routed outflow" excludes sinks directly while
correctly *including* source lakes.

**Authoritative upstream end.** NHDPlus network flowlines (those in
`PlusFlowlineVAA`) are digitized in the downstream direction, so the first
geometry vertex corresponds to `FromNode` (the upstream end). Validated on
`16974320` (a `STARTFLAG` headwater whose first vertex lies inside 16969532).
D1's "upstream end inside W" therefore uses the first vertex, gated on VAA
membership; it does **not** trust the `FLOWDIR` field.

## Architecture

Mirrors the existing two-parquet union pattern. The on-stream set stays an
auditable union of COMID sources; topology adds direction and a guardrail.

### New shared staging module: `download/nhd_topology.py`

Sibling of `download/nhd_flowlines.py` / `download/nhd_flowthrough.py`. Per-VPU
loop over the same `vpu_index`, downloading the **NHDPlusAttributes** 7z archive
(≈9 MB/VPU; e.g.
`NHDPlusV21/Data/NHDPlusCO/NHDPlus14/NHDPlusV21_CO_14_NHDPlusAttributes_10.7z`,
discovered from the S3 listing exactly like the NHDSnapshot URL, matching
`_<vpu>_NHDPlusAttributes_(\d+)\.7z$`). Reads `PlusFlowlineVAA.dbf`
(case-insensitive fields) and writes a flat parquet
`input/nhd/flowline_topology.parquet`:

| column | source | use |
|---|---|---|
| `comid` | ComID | join key to NHDFlowline |
| `dnhydroseq` | DnHydroseq | routes-downstream test (`≠ 0`) |
| `hydroseq` | Hydroseq | network membership / ordering |
| `terminalfl` | TerminalFl | terminal-sink detection (follow-up) |
| `startflag` | StartFlag | headwater-origin diagnostics |
| `streamorde` | StreamOrde | diagnostics / future use |

`fromnode`/`tonode` are carried for completeness/auditing. Empty distilled set →
hard error (same fail-loud contract as the other two stagers).

### Classifier integration: `download/nhd_flowthrough.py`

- Load `flowline_topology.parquet`; build the routed-network set
  `{comid : dnhydroseq ≠ 0}`.
- Restrict D1's flowline candidates to that set; apply the D1 outflow test
  (upstream end inside W) in place of the old T2.
- Keep T1 and T3 unchanged. Keep the up-front Playa/Ice Mass drop unchanged.
- Output is still `flowthrough_waterbody_comids.parquet`, now additionally
  including source lakes.
- The topology parquet path is read from the fabric profile via
  `require_config_key` (new key `topology_table`), consistent with
  `connected_comids_table` / `flowthrough_comids_table`.

### Sink guardrail chokepoint: `depstor_builders/wbody_connectivity.py`

After unioning the connected (WBAREACOMI) and flow-through COMID sets, **subtract
the COMIDs whose waterbody `FTYPE ∈ {Playa, Ice Mass}`** (read from the
waterbody layer already loaded in this builder). This applies the force-dprst
guardrail to *every* promotion path — closing the WBAREACOMI leak — at the single
point where both sources meet. Everything downstream (`connected_wbody.tif` →
`dprst` → routing → drains_*) is unchanged code seeing a corrected on-stream set.

## Scope / validation (VPU 14, 4,999 waterbodies)

- **146 waterbodies (2.9%) flip dprst → on-stream** under D1 — 142 LakePond,
  4 SwampMarsh. 16969532 is among them. ✓
- **16 force-dprst FTYPEs (12 Playa + 4 Ice Mass)** currently on-stream via
  WBAREACOMI are demoted by the guardrail. ✓
- CONUS extrapolation: order ~3% promotion + a smaller, FTYPE-bounded demotion.
  `drains_to_dprst` impact is one-directional (source-lake promotion only removes
  small-catchment pour points; it cannot re-introduce over-extension).

## Testing

Follow `tests/test_nhd_flowthrough.py` (synthetic geometry). Add:

- `test_source_lake_with_routed_outflow_is_onstream` — a `STARTFLAG`-style line
  whose upstream end is inside W, present in a synthetic topology table with
  `dnhydroseq ≠ 0` → on-stream (the 16969532 case).
- `test_terminal_sink_inflow_only_stays_dprst` (topology variant) — inflow line
  (downstream end inside, `dnhydroseq = 0`) → dprst.
- `test_non_network_outflow_ignored` — outflow line absent from the topology
  table → not promoted (the 16974354/16974376 noise case).
- `test_uninitialized_flowdir_promoted_via_topology` — outflow line with
  `FLOWDIR=Uninitialized` but present in VAA → on-stream (the core fix).
- `test_playa_force_dprst_via_wbareacomi_guardrail` — a Playa COMID present in
  the connected set is removed by the `wbody_connectivity` guardrail.
- A `nhd_topology` parse/normalisation test (mixed-case dbf fields, `DnHydroseq`
  filter) mirroring `test_read_flowline_attrs`-style coverage.

CI (`.github/workflows/ci.yml`) is the gate; no pytest on the HPC head node.

## Rebuild / rollout

1. `python -m gfv2_params.download.nhd_topology` → `flowline_topology.parquet`
   (one-time, ~9 MB × 21 VPU download + parse).
2. `python -m gfv2_params.download.nhd_flowthrough` → regenerate
   `flowthrough_waterbody_comids.parquet` (now source-lake-complete).
3. Rebuild the depstor dprst cascade: `wbody_connectivity → dprst → routing →
   drains_perv/imperv` (per memory `depstor_rebuild_cascade`; dprst+waterbody
   peak ~384G, routing ~80G).
4. Docs: update `docs/ARCHITECTURE.md` (third on-stream signal + guardrail) and
   the per-key required-field table for the new `topology_table` profile key.

## Out of scope (explicit follow-ups)

- **Demoting non-Playa terminal sinks.** The ~31 VPU-14 LakePond/SwampMarsh with
  no routed outflow are candidate terminal sinks but need per-case evidence
  (`TerminalFl=1` / `DnHydroseq=0`, validated) before demotion — a bulk demotion
  risks false negatives in outflow detection re-introducing the #145
  over-extension bug. Separate, evidence-driven pass once topology is staged.
- **Replacing T1/T3** or the whole geometric classifier with pure topology.
- **Lake morphology** (`PlusWaterbodyLakeMorphology`) and stream-order-based
  filtering.
- The case-1 `GeometryCollection` T1 fix (already implemented separately).
