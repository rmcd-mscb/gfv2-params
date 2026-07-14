# Depression-storage workflow deck (v2) — design

**Date:** 2026-07-14
**Branch:** `docs/depstor-deck-v2` (stacked on `feat/endorheic-dprst-classifier`, PR #178)
**Supersedes:** `docs/superpowers/specs/2026-07-07-depression-storage-presentation-design.md`

## Problem

`docs/presentations/2026-07-depression-storage-workflow.slides.md` is out of
date and, in one place, wrong. It presents the **Network-Flowline gate**
(#161/#163) as "the endorheic fix" — a claim PR #178 disproves: with the gate
in place, the Great Salt Lake still came out **0% depression storage**. The
deck's "Known open refinements" table is likewise stale.

Separately, the deck describes the classifier as a *conclusion* ("a waterbody
is dprst unless connected") without showing the rules firing on real
hydrography. A reviewer cannot check a rule they cannot see.

## Goal

A **technical design review** deck (~27 slides) that describes the
depression-storage workflow end to end, presents each classification rule as a
rule, and shows **what that rule looks like on real hydrography** — a map tile
at a named waterbody, with the evidence the rule actually reads drawn on top.

Audience: peers reviewing the method. Assumes PRMS/NHM familiarity, not this
pipeline's internals. Includes PR #178, which is open and unmerged.

## Non-goals

- Not a rewrite of the pipeline, the classifier, or any builder. Docs + figures only.
- Not a fabric-specific deck. Method-focused, spans fabrics (matches the
  existing naming convention in `docs/presentations/README.md`).
- Does **not** re-render or re-validate the CONUS product. It reads the two
  raster snapshots already on disk.

## Deck structure

Rewrite in place: `docs/presentations/2026-07-depression-storage-workflow.slides.md`.
Rule-first framing — each rule slide states the rule, shows the real-world tile,
then the number it changed. Before/after pairs appear only where a rule actually
moved something.

### Act 0 — Framing (4 slides)

1. Title.
2. What depression storage is in PRMS; the 6 spatial per-HRU parameters it feeds.
3. Why the classification is the whole ballgame — get the on-stream/dprst split
   wrong and a basin's runoff partition and timing are wrong before calibration
   starts.
4. The legacy ArcPy workflow (`docs/0b_TB_depr_stor.py`): 60 m Euclidean segment
   buffer, `Watershed()` with no barriers, single-machine per modeling unit.
   Its two weaknesses: geometric distance is not connectivity, and no test that
   is *local* to a waterbody can see that an entire basin is endorheic.

### Act 1 — The pipeline (3 slides)

5. Open-source stack + reproducibility: rasterio/numpy, richdem + WhiteboxTools,
   in-process D8 kernel, gdptools, pixi, SLURM.
6. The builder DAG (`pipeline_dag.png`, regenerated to include `nhd_topology`,
   `nhd_flowlines`, `nhd_flowthrough`, `endorheic`, `wbody_connectivity`,
   `dprst`, `routing`, `same_hru_drains`, `dprst_depth`).
7. **Input provenance.** PR #178 found three of the four hand-made `input/nhd/`
   files defective (`NHD_sink_points.gpkg` omits `PURPCODE 1` entirely → 0 sinks
   inside GSL where NHDPlus has 29; `closed_huc12.gpkg` has 23 Great Basin
   type-C HUC12s vs. 141; the waterbody layer's second layer was missing all
   66,488 SwampMarsh). Everything now stages from NHDPlus/WBD via
   `nhd_burn_components`, `wbd_huc12`, `nhd_waterbodies`.

### Act 2 — The rule ladder (12 slides: 1 overview + 11 rules) — the core

8. **The ladder, one slide.** `rule_ladder.png`: default dprst → on-stream
   evidence union → endorheic strict subtraction → guardrails → raster products.

Then one slide per rule, each with a real-world tile:

| # | Rule | Real-world example | Figure |
|---|---|---|---|
| 9 | What counts as a waterbody — NHDWaterbody + only the **sink-purpose** BurnAddWaterbody rows; `FTYPE` from `FCODE`, not `PurpCode` | VPU 01's 702 NULL-`PurpCode` rows against zero sinks in its own `Sink.shp` — 503 on-network, incl. StreamRiver and CanalDitch | `rule_burnadd_purpcode.png` |
| 10 | **Playa and Ice Mass are hard guardrails, and are NOT equivalent.** Playa → force-dprst, never promoted on-stream. Ice Mass → excluded from the classification entirely, falls back to land, perv/imperv via LULC. *As built: this slide absorbed the Playa half of row 18's planned "Guardrails" slide, so Playa and Ice Mass are contrasted on one tile rather than split across two.* | a playa vs. a Cascades ice mass | `rule_playa_guardrail.png` (supersedes the planned `rule_icemass.png`) |
| 11 | **Default: a waterbody IS depression storage** unless proven on-stream | *(statement slide, no tile)* | — |
| 12 | On-stream evidence A — **WBAREACOMI** artificial-path topology | a run-of-river reservoir with the artificial path threaded through it | `rule_wbareacomi.png` |
| 13 | On-stream evidence B — **geometric flow-through**: a Network flowline demonstrably enters **and** exits. Terminal sinks (inflow only) and locally-spilling potholes (outflow only) stay dprst. | Lewis and Clark Lake (in + out) vs. a terminal sink (in only) | `rule_flowthrough.png` |
| 14 | The **Network-Flowline gate** (#161/#163) — NHD draws Non-Network artificial paths through essentially every closed-basin lake; both COMID sources gate on membership in `flowline_topology.parquet` | a Great Basin lake threaded by a Non-Network cartographic path | `rule_network_gate.png` |
| 15 | **Endorheic Signal A — terminus-inside-itself** (#178). `frac_own` = share of the waterbody's cells whose D8 path reaches an FDR **code-0** cell *inside that same waterbody*. dprst iff `frac_own > 0.5`. The classifier and the router read the same grid, so they agree by construction. | Great Salt Lake `frac_own = 1.000`, code-0 cells visibly inside it; Lewis and Clark `frac_own = 0.007` — its terminus is the Gulf of Mexico | `rule_terminus_gsl.png` |
| 16 | `frac_own` is **bimodal** — 0.5 is not a tuned knob | 6,298 of 6,427 candidates at ≥ 0.95; 10 in the whole 0.45–0.55 band; threshold sweep moves the answer 0.5% across 0.3→0.7 | `frac_own_bimodal.png` |
| 17 | **Endorheic Signal B — majority-inside a WBD type-C closed HUC12.** Containment is **majority-area** — never `intersects` (a zero-interior boundary touch returns `True`: Eagle Lake and Middle Alkali graze closed basins at frac = 0.000), never `within` (it **drops GSL**, which spills 1.1% into a neighbouring HUC12 at frac = 0.989). | Walker Lake — contains no FDR terminal cell, so Signal A alone misses it | `rule_closed_huc12_walker.png` |
| 18 | **Domain-exit guardrail.** A waterbody that is terminal only because the CONUS model ends there is not endorheic; ten named fixtures must never be demoted. *As built: the Playa half of this planned slide moved to row 10, alongside Ice Mass — this slide covers domain exits only.* | Lake of the Woods, Champlain, the Everglades | `rule_domain_exits.png` |
| 19 | **The clump veto and its exemption** (#178's second bug). `clump_regions` 8-connects GSL to a 49.1 km² SwampMarsh that is *correctly* on-stream (its water drains **into** GSL, so its terminus is GSL, not itself), and `regions_touching_mask` excludes a whole region sharing ≥1 cell with the on-stream mask — all 4,854,156 GSL cells went with it. Fixed by exempting endorheic waterbodies' own not-on-stream cells: **evidence overrides proxy, but only where we have evidence.** The global per-cell carve was considered and rejected (it recovers a further ~8,471 km² with *no* endorheic evidence). | GSL + the marsh, before/after | `clump_veto_gsl.png` |

### Act 3 — Classification → parameters, in brief (4 slides)

20. **Impervious is carved per-cell, never whole-region** (#144). One impervious
    pixel must not drop a multi-km² waterbody. The imperv/dprst/perv partition
    stays disjoint. Land masking against `land_mask.tif`.
21. **D8 routing + the on-stream barrier** (#158/#159). Land upslope of an
    on-stream waterbody is captured by that waterbody's own routing and must not
    be attributed to a depression behind it. Strict subtraction — can only reduce
    `drains_to_dprst`, never increase it. Figure: `drains_great_basin_before_after.png`
    (the planned `lower_miss_before_after.png` is not reproducible — see "Figures"
    below).
22. **same-HRU restriction on `sro_to_dprst_*`** (#160/#162) — reproduces the
    legacy `Con(rSro == hru)`; a per-cell reached-HRU-vs-own-HRU test that
    gdptools' partial-pixel weighting cannot express.
23. **`dprst_depth_avg`** (#173) — freeboard + Hollister terrain-slope bathymetry
    + playa-anchored regression, because 99.9% of dprst area is hydro-flattened.

### Act 4 — Results & status (4 slides)

24. **CONUS results.** dprst 42,535 → **51,930 km²** (+22.1%); VPU 16 (Great
    Basin) 3,887 → **9,566 km²** (+146.1%); `drains_to_dprst` +21.0%; 725
    demotions / 8,735 km² plus 1,658 BurnAdd polygons / 722 km². Drainage gained
    116,630 km² and lost 916 — purely additive, as a strict subtraction must be.
    Figure: `conus_dprst_before_after.png`.
25. **Validation gates.** 20/20 named fixtures (`endorheic_fixtures.py`) and the
    product-level raster A/B (`ab_endorheic_rebuild.py`): GSL / Salton Sea /
    Pyramid / Mono go 0.0% → **100.0%** dprst; Lake Michigan / Champlain /
    Lewis and Clark stay 0.0%.
26. **Not in #178, and open issues.** The profile still points at the hand-made
    waterbody layer (`nhd_waterbodies.parquet` is staged and verified but its
    shoreline vintage differs by 2.2% in area — repointing would shift the
    validated product). `dprst_depth` must be regenerated. Open: #154 reservoir
    bucket, #155 permanence gate, #156 clump-merge sensitivity, #157 cross-VPU
    seams, #147 depression-respecting FDR.
27. Summary.

## Figures

Renderer: rewrite `scripts/render_depstor_figures.py` (currently 347 lines;
expect ~600–700). Output to `docs/figures/depstor/`.

### The workhorse: `tile(comid, ...)`

Given a COMID, resolve its bounds from `nhd_waterbodies.parquet`, then composite:

1. **Classification raster** — land / dprst / on-stream from `dprst_binary.tif` +
   `onstream_binary.tif`, read via `rasterio.windows.from_bounds` with an
   `out_shape` decimation. **Never a full-grid read** — the CONUS template is
   153,830 × 109,901 ≈ 16.9 B cells (CLAUDE.md's CONUS-memory rule).
2. **Waterbody outline** — the polygon the rule is deciding about.
3. **Flowlines colored by network membership** — per-VPU `NHDFlowline.shp` under
   `shared/source/{vpu}/NHDSnapshot/.../Hydrography/`, read bbox-filtered via
   pyogrio, joined to `flowline_topology.parquet`. Present in the topology =
   **Network**; absent = **Non-Network cartographic path**. This layer is what
   makes rules 12–14 legible.
4. **FDR code-0 terminal cells** — windowed read of `fdr.vrt`, `== 0`, drawn as
   markers. Makes "the terminus is *inside* itself" (rule 15) pointable-at.

Reprojection to the raster CRS (EPSG:5070) happens on the vector layers, which
are small after the bbox filter.

### Inventory (14)

As built, `rule_icemass.png` was folded into `rule_playa_guardrail.png` — Rule 2
("Playa and Ice Mass are hard guardrails, and are NOT equivalent") covers both
FTYPEs on one tile, since the point of the slide is contrasting them, not two
separate figures. `lower_miss_before_after.png` was replaced by
`drains_great_basin_before_after.png`: the isolating snapshot
(`pre_flowthrough_2026-06-26`) that a Lower-Mississippi-only figure would have
needed is deleted from disk (see the deck's "D8 + the on-stream barrier" slide),
so it can no longer be reproduced or verified; the Great Basin drains_to_dprst
before/after is derived from the two snapshots that do survive.

| Figure | Status |
|---|---|
| `pipeline_dag.png` | regenerate (add the #178 steps) |
| `rule_ladder.png` | **new** (replaces `decision_schematic.png`) |
| `rule_burnadd_purpcode.png` | **new** |
| `rule_playa_guardrail.png` | **new** (covers both Playa and Ice Mass; supersedes the planned `rule_icemass.png`) |
| `rule_wbareacomi.png` | **new** |
| `rule_flowthrough.png` | **new** |
| `rule_network_gate.png` | **new** |
| `rule_terminus_gsl.png` | **new** |
| `frac_own_bimodal.png` | **new** |
| `rule_closed_huc12_walker.png` | **new** |
| `rule_domain_exits.png` | **new** |
| `clump_veto_gsl.png` | **new** |
| `drains_great_basin_before_after.png` | **new** (replaces the planned `lower_miss_before_after.png` — see above) |
| `conus_dprst_before_after.png` | **new** |
| `decision_schematic.png` | **delete** (superseded by `rule_ladder.png`) |
| `great_basin_before_after.png` | **delete** — its caption credits the Network gate with the endorheic fix, the claim #178 disproves |
| `lower_miss_before_after.png` | **delete** — the isolating snapshot it depended on is gone; not reproducible (see above) |

## Data sources (all verified present on disk)

| Need | Path |
|---|---|
| before rasters | `{data_root}/gfv2/depstor_rasters_pre_endorheic_2026-07-13/` |
| after rasters | `{data_root}/gfv2/depstor_rasters/` |
| classifier table (`frac_own`, `by_terminus`, `by_closed_huc12`) | `{data_root}/gfv2/depstor_rasters/endorheic_waterbody_comids.parquet` (22,970 rows) |
| waterbody geometry (COMID → polygon, FTYPE) | **the profile's `waterbody_gpkg`** — `{data_root}/input/nhd/conus_waterbodies.gpkg`, layer `waterbodies` (448,124 rows, EPSG:5070) |
| flowline geometry | `{data_root}/shared/source/{vpu}/NHDSnapshot/**/Hydrography/NHDFlowline.shp` (all 21 VPUs staged, EPSG:4269) |
| Network membership | `{data_root}/input/nhd/flowline_topology.parquet` (2,691,339 rows) |
| BurnAdd rows | `{data_root}/input/nhd/burn_add_waterbodies.parquet` |
| closed HUC12s | `{data_root}/input/wbd/wbd_huc12.parquet` |
| FDR (code-0 cells) | `fdr_raster` profile key |
| named fixtures | `scripts/diagnose/endorheic_fixtures.py` (20 COMIDs) |

Paths resolve through `load_base_config()` / `require_config_key` against the
active fabric profile — never hardcoded (CLAUDE.md).

### Three data gotchas the renderer must respect

1. **Waterbody geometry comes from the profile's `waterbody_gpkg`
   (`conus_waterbodies.gpkg`), NOT `nhd_waterbodies.parquet`.** The rasters were
   built from the former; the latter is staged-from-source but not yet wired in
   (see "Not in #178"). Their shorelines differ — Great Salt Lake is 4,368.9 km²
   in the gpkg vs. 4,309.7 km² in the parquet, and the vetoing marsh is 49.1 vs.
   38.7 km². Drawing outlines from the parquet would misalign them with the
   pixels and contradict the PR's own numbers. Read with a `where=` clause on
   COMID (pyogrio pushes it down; no full-layer read).
2. **NHDFlowline field casing varies by VPU.** VPU 16 ships `ComID` /
   `WBAreaComI` / `FCode`; VPUs 01 and 08 ship `COMID` / `WBAREACOMI` / `FCODE`.
   The reader must upper-case field names before use — the same gotcha
   `download/nhd_flowlines.py` already handles (PR #140). Flowlines are EPSG:4269
   and must be reprojected to EPSG:5070.
3. **The endorheic table is a SET, not a demotion list.** It holds 22,970 COMIDs
   (6,364 by terminus, 21,503 by closed HUC12, 4,925 by both). Only the ones that
   were *also* in the on-stream union get demoted — hence 725 demotions, not
   22,970. The deck must say this plainly, or the numbers look inconsistent.
   Signal A's candidate population is the 6,427 waterbodies with a computed
   `frac_own`.

## Verification

Every number in the deck is **derived, not transcribed from the PR body**:

- `frac_own` values, the bimodality histogram, and the threshold sweep read
  `endorheic_waterbody_comids.parquet` directly.
- Fixture names/COMIDs come from `endorheic_fixtures.py`.
- CONUS area deltas are re-derived by the renderer from the two raster
  snapshots, so the results table and the maps cannot disagree.
- Every `file:line` cite in the deck is checked against the worktree.

**Where it runs:** windowed tiles are light, but `conus_dprst_before_after.png`
decimates a 16.9 B-cell raster. The render runs under `srun`, not on the HPC
login node.

## Docs check (CLAUDE.md requires one)

- `docs/presentations/README.md` — update the deck's description in the same commit.
- The renderer's module docstring — rewrite (it currently documents four figures).
- No `docs/ARCHITECTURE.md` change: this branch adds no pipeline behaviour.

## Risks

- **PR #178 is unmerged.** The deck describes code on `feat/endorheic-dprst-classifier`.
  If #178 changes in review, the deck's rules must follow. Mitigated by stacking
  this branch on #178 and re-verifying cites before merge.
- **Figure count.** 14 figures with vector overlays is the bulk of the work. If
  the renderer proves heavier than expected, tiles collapse into shared 2-panel
  figures (target floor: ~10) rather than dropping rules.
