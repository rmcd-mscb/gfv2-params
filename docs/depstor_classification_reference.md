# Depression-Storage Classification — Reference

**What this is.** A single, current-state map of how the depression-storage
(dprst) workflow turns raw geospatial inputs into PRMS/NHM parameters, organized
around three questions:

1. **[Data sources](#1-data-sources)** — what goes in, and which config key names it.
2. **[The gate ladder](#2-the-gate-ladder)** — the ordered tests that decide whether a
   waterbody is depression storage or on-stream.
3. **[Products → parameters](#3-products--parameters)** — the rasters produced and how they
   become the six spatial PRMS parameters.

Plus a **[staleness / maintenance](#4-staleness--maintenance)** section (which gates are
load-bearing, which are dead, what is in motion) and a **[one-page map](#5-one-page-map)**.

Verified against `main` (post-PR #178 endorheic classifier + PR #179 waterbody
repoint). Where a claim rests on code, it cites `file:function`; line numbers
drift, so function names are the durable reference. This supersedes the historical
planning transcription in [`depstor_workflow.md`](depstor_workflow.md) (the original
Bock/Russell design PDF) as the description of the *shipped* pipeline.

> **How to read this if you're lost.** The classifier grew one gate at a time
> across a dozen PRs, each fixing a real bug the previous one exposed, and no
> single source file states the whole sequence. Section 2 is that sequence. If
> you only read one thing, read the [gate ladder](#2-the-gate-ladder).

---

## 0. Where everything is declared — the three config files

The pipeline is config-driven; almost nothing is hardcoded. Three files, each
with a distinct job:

| File | Declares | Read by |
|---|---|---|
| [`configs/base_config.yml`](../configs/base_config.yml) | **Per-fabric inputs.** One profile per fabric (`gfv2`, `gfv2_dev`, `oregon`, `tjc`, …) under `fabrics:`. Every source path, plus fabric identity keys (`id_feature`, `hru_gpkg`, floors). Resolved by `load_config()` via `--fabric` → `FABRIC` env → `default_fabric`. | every builder + script |
| [`configs/depstor/depstor_rasters.yml`](../configs/depstor/depstor_rasters.yml) | **The raster stack.** An ordered `steps:` list; each step names a builder and its output raster(s). This is the classification + routing DAG. | `scripts/build_depstor_rasters.py` |
| [`configs/depstor/depstor_params.yml`](../configs/depstor/depstor_params.yml) | **The zonal aggregation.** `fractions:` (per-HRU cell counts from a raster), `ratios:` (final PRMS params = numerator ÷ denominator fraction), and `means:` (continuous-raster means, e.g. depth). | `scripts/derive_depstor_params.py` |

Placeholders (`{data_root}`, `{fabric}`, `{vpu}`) resolve at load time.
`{data_root}` is `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`
([base_config.yml:10](../configs/base_config.yml#L10)).

**Fabric caveat that bites:** the source paths below are the **`gfv2`** profile.
Other fabrics differ — most notably, `gfv2` now reads the source-derived
`nhd_waterbodies.gpkg` (PR #179), but `gfv2_dev`, `oregon`, and `tjc` still point
at the older hand-made `conus_waterbodies.gpkg`. "Which waterbody layer" is a
per-fabric answer. See [§4](#4-staleness--maintenance).

---

## 1. Data sources

Everything the classification consumes. Almost all of it is now **staged from
source** (NHDPlus V2 / WBD) by a module under `src/gfv2_params/download/`,
replacing an earlier set of hand-made files.

| Source | `gfv2` profile key | Resolves to | Staged by | Role |
|---|---|---|---|---|
| NHD waterbody polygons | `waterbody_gpkg` ([:49](../configs/base_config.yml#L49)) | `input/nhd/nhd_waterbodies.gpkg` | `download/nhd_waterbodies.py` | the base geometry every gate runs on |
| Flowline topology (PlusFlowlineVAA) | *(no key — staged by path)* | `input/nhd/flowline_topology.parquet` | `download/nhd_topology.py` | **Network-membership** truth; must stage first |
| WBAREACOMI connected COMIDs | `connected_comids_table` ([:48](../configs/base_config.yml#L48)) | `input/nhd/connected_waterbody_comids.parquet` | `download/nhd_flowlines.py` | on-stream promoter #1 |
| Flow-through COMIDs | `flowthrough_comids_table` ([:53](../configs/base_config.yml#L53)) | `input/nhd/flowthrough_waterbody_comids.parquet` | `download/nhd_flowthrough.py` | on-stream promoter #2 |
| Closed (type-C) HUC12s | `wbd_huc12_table` ([:61](../configs/base_config.yml#L61)) | `input/wbd/wbd_huc12.parquet` | `download/wbd_huc12.py` | endorheic Signal B |
| BurnAddWaterbody sink polygons | `burn_add_waterbody_table` ([:71](../configs/base_config.yml#L71)) | `input/nhd/burn_add_waterbodies.parquet` | `download/nhd_burn_components.py` | adds playa/closed-lake depression **area** |
| FDR grid (code-0 = NHDPlus sinks) | `fdr_raster` ([:27](../configs/base_config.yml#L27)) | `gfv2/shared/gfv2_fdr.vrt` | `scripts/clip_shared_to_fabric.py` | endorheic Signal A **and** all D8 routing |
| NLCD fractional impervious | `imperv_source` (in `depstor_rasters.yml`) | NLCD annual impervious tif | external | the impervious carve |
| HRU fabric (land/domain mask) | `hru_gpkg` ([:90](../configs/base_config.yml#L90)) | `gfv2/fabric/…nhru…gpkg` | rasterized by the `landmask` step | the domain mask + the per-HRU denominator |

**The one ordering rule that matters at staging time:** `nhd_topology` must run
**before** `nhd_flowlines` and `nhd_flowthrough`. Both COMID stagers gate their
output on Network-Flowline membership, which they read from
`flowline_topology.parquet` by path; both fail loud if it's missing. This is the
#161 fix — NHD draws Non-Network "cartographic" artificial paths through
essentially every closed-basin lake, and without the gate those paths promote
genuinely endorheic lakes on-stream.

**Provenance-only, not a gate:** `sink_points_table`
([base_config.yml:79](../configs/base_config.yml#L79), →
`input/nhd/sink_points.parquet`) is threaded through the build context but read
by **no classifier** — the endorheic test reads the FDR grid the router reads,
not a sink-point vector. The profile comment and the `BuildContext` docstring
both say so explicitly. It is kept deliberately for provenance; do not wire it to
a gate.

---

## 2. The gate ladder

A waterbody passes through **four builder stages in fixed order**
([`depstor_rasters.yml`](../configs/depstor/depstor_rasters.yml) `steps:`;
dispatch order in `depstor_builders/__init__.py` `STEP_ORDER`):

```
waterbody  →  endorheic  →  wbody_connectivity  →  dprst
```

Each stage's gates, in execution order. **Direction** = which way the gate pushes
a waterbody (→ dprst or → on-stream). **Kind** = hard override vs. weighed signal
vs. proxy.

### Stage 1 — `waterbody.build()` · what counts as a waterbody at all

| Gate | Test | Reads | Direction | Kind |
|---|---|---|---|---|
| **BurnAdd merge** | union NHDPlus BurnAddWaterbody sink polygons (playa / closed-lake depression *area*) into the layer; negative COMIDs, so they can never match a positive on-stream COMID; a clump-overlap guard raises if a BurnAdd clump transitively reaches an on-stream waterbody | `burn_add_waterbody_table` | adds dprst area | structural | `merge_burn_add()` |
| **Ice Mass exclude** | drop `FTYPE ∈ EXCLUDE_WATERBODY_FTYPES` (Ice Mass) from the layer **entirely** — it becomes land (perv/imperv via LULC), neither dprst nor on-stream | `FTYPE` | → land | **hard override** | `build()`, [waterbody.py:86](../src/gfv2_params/depstor_builders/waterbody.py#L86) |
| **min-area** | keep polygons ≥ `min_area_threshold` (default 900 m²) | geometry | drops slivers | threshold |
| *(rasterize)* | write `wbody_binary.tif`; label 8-connected clumps → `wbody_regions.tif` (`clump_regions`) | — | — | — |

### Stage 2 — `endorheic.build()` · is the basin closed? *(computes a COMID table; applied in Stage 3)*

Emits `endorheic_waterbody_comids.parquet` with **per-signal provenance columns**
(`by_terminus`, `by_closed_huc12`) so you can always tell which signal flagged a
lake.

| Signal | Test | Reads | Direction | Kind |
|---|---|---|---|---|
| **A — terminus inside itself** | for each waterbody containing ≥1 FDR **code-0** (terminal) cell, run the D8 kernel; flag if `frac_own > 0.5` — the share of the waterbody's *own* cells whose D8 path dead-ends at a terminus **inside the same polygon** | `fdr_raster` + the router's own D8 kernel | → dprst | weighed (`MIN_FRAC = 0.5`) | `terminus_own_fraction()`, [endorheic.py:281](../src/gfv2_params/endorheic.py#L281) |
| **B — closed HUC12** | flag if the waterbody sits **majority-area** (>0.5) inside the dissolved union of WBD type-C (closed) HUC12s — majority-area, never `intersects` (a boundary graze returns True) or `within` (drops GSL, which spills 1.1% out) | `wbd_huc12_table` (optional) | → dprst | weighed | `closed_basin_comids()` |
| *(floor guard)* | raise if the flagged total < `min_endorheic_comids`, or if either signal collapses to zero | signal counts | — | guard | `check_endorheic_floor()` |

> Signal A carries the closed-lake **area** (Great Salt Lake ends in itself,
> `frac_own = 1.000`). Signal B catches lakes with **no interior FDR sink** (Walker
> Lake, `frac_own = 0.000`). They are complementary, not redundant — see [§4](#4-staleness--maintenance).

### Stage 3 — `wbody_connectivity.build()` · on-stream = union of promoters, minus endorheic

| Gate | Test | Reads | Direction | Kind |
|---|---|---|---|---|
| **C1 — WBAREACOMI connected** | waterbody COMID is in the NHD artificial-path connected set — **Network-gated** (its flowline must be a Network Flowline) | `connected_comids_table` | → on-stream | promoter |
| **C2 — flow-through union** | union in the geometric/topology on-stream set: a Network line flows *through* (in **and** out), or is a routed-network source/outflow — also **Network-gated** | `flowthrough_comids_table` | → on-stream | promoter |
| **C3 — endorheic subtraction** | `on_stream = (C1 ∪ C2) − endorheic`. COMID-keyed; can only ever **remove** | `endorheic` COMID table | → dprst | strict subtraction |
| **C4 — NEVER_ONSTREAM guardrail** | drop `FTYPE ∈ {Playa, Ice Mass}` from the on-stream selection (Playa is force-dprst; Ice Mass is already gone) | `FTYPE` | → dprst / land | **hard override** |
| *(side output)* | rasterize the **full** endorheic set (regardless of on-stream) → `endorheic_wbody.tif`, for the Stage-4 exemption | endorheic table | evidence for D2 | — |

Outputs: `connected_wbody.tif` (on-stream cells) and `endorheic_wbody.tif`. Two
`_assert_*` guards (`_assert_no_endorheic_repromotion`,
`_assert_endorheic_selection_is_comid_faithful`) protect the strict-subtraction
invariant against a future layer whose COMID keys diverge.

### Stage 4 — `dprst.build()` · the final depression-storage raster

| Gate | Test | Reads | Direction | Kind |
|---|---|---|---|---|
| **D1 — region-level on-stream exclusion** | exclude a **whole** 8-connected clump if **any** cell touches `connected_wbody` | `connected_wbody.tif`, `wbody_regions.tif` | → on-stream | **proxy** |
| **D2 — endorheic clump-veto exemption** | recover cells where `endorheic_wbody == 1 AND connected != 1 AND wbody_binary == 1` back to dprst — direct terminus evidence overrides the clump proxy, but only for the waterbody's *own* not-on-stream cells | `endorheic_wbody.tif`, `connected_wbody.tif`, `wbody_binary.tif` | → dprst | evidence overrides proxy |
| **D3 — impervious carve** | `dprst[imperv == 1] = nodata`, **per cell**, never whole-region | `imperv_binary.tif` | removes imperv | per-cell |
| **D4 — land mask** | `dprst[~land] = nodata` | `land_mask.tif` | drops ocean | mask |

> **Why D2 exists.** `clump_regions` 8-connects Great Salt Lake to a 49 km²
> SwampMarsh whose water drains *into* the lake — so the marsh is correctly
> on-stream, but D1's whole-region veto would drop all ~4.85M GSL cells with it.
> D2 lets direct hydrologic evidence (terminus-inside-itself) override the clump
> proxy, without re-opening the over-extension the proxy prevents elsewhere.

Outputs: `dprst_binary.tif` (the depression product) and `onstream_binary.tif`.

---

## 3. Products → parameters

### 3a. The raster stack ([`depstor_rasters.yml`](../configs/depstor/depstor_rasters.yml) `steps:`)

| Raster | Represents | Built by (step) |
|---|---|---|
| `land_mask.tif` | 1 = land; the domain mask **and** the per-HRU pixel denominator | `landmask` |
| `imperv_binary.tif` | 1 = NLCD impervious cell (>50%) | `imperv` |
| `wbody_binary.tif` / `wbody_regions.tif` | waterbody cells / their 8-connected clump labels | `waterbody` |
| `endorheic_waterbody_comids.parquet` | COMIDs flagged endorheic (Signal A/B, with provenance columns) | `endorheic` |
| `connected_wbody.tif` | on-stream waterbody cells (after endorheic subtraction) | `wbody_connectivity` |
| `endorheic_wbody.tif` | the full endorheic set, on-stream or not (D2 evidence mask) | `wbody_connectivity` |
| **`dprst_binary.tif`** | **depression-storage cells** — the product | `dprst` |
| `onstream_binary.tif` | on-stream surface-storage cells | `dprst` |
| `perv_binary.tif` | pervious land = land − imperv − dprst (disjoint) | `perv` |
| `hru_id.tif` / `vpu_id.tif` | per-cell HRU id / VPU code | `hru_id` / `vpu_id` |
| `dprst_depth.tif` | per-cell dprst mean depth, masked to `dprst_binary` (#173) | `dprst_depth` |
| `drains_to_dprst.tif` | cells whose D8 path reaches a depression; on-stream cells are barriers (HRU-agnostic) | `routing` |
| `drains_to_dprst_hru.tif` | the HRU id of the depression each draining cell reaches (labeled) | `routing_hru` |
| `drains_perv_binary.tif` / `drains_imperv_binary.tif` | land draining to a depression **in its own HRU** (`drains_to_dprst_hru == hru_id`) | `same_hru_drains` |
| `carea_map_t8_binary.tif` / `carea_map_t156_binary.tif` | pervious cells above the two TWI thresholds | `carea_map` |

### 3b. The six spatial parameters ([`depstor_params.yml`](../configs/depstor/depstor_params.yml))

Every parameter is a **per-HRU ratio of two zonal cell-counts** (gdptools
exactextract; a `count` is the partial-pixel-weighted sum for the 1-valued cells,
*not* itself a fraction — the ratio makes it one). `fractions:` declares the
counts; `ratios:` declares the divisions.

| PRMS parameter | = count of… | ÷ count of… | Clamp |
|---|---|---|---|
| `dprst_frac` | `dprst_binary` | `land_mask` (HRU total) | — |
| `hru_percent_imperv` | `imperv_binary` | `land_mask` (HRU total) | — |
| `sro_to_dprst_perv` | `drains_perv_binary` | `perv_binary` | — |
| `sro_to_dprst_imperv` | `drains_imperv_binary` | `imperv_binary` | — |
| `carea_max` | `carea_map_t8_binary` | `perv_binary` | ≤ 1 |
| `smidx_coef` | `carea_map_t156_binary` | `perv_binary` | ≤ 1 |

Plus **`dprst_depth_avg`** — a `means:` entry, not a ratio: an exactextract
**mean** over `dprst_depth.tif` (metres → inches; HRUs with zero dprst floored at
49 in). Because `dprst_depth.tif` is itself masked to `dprst_binary`, depth stays
consistent with `dprst_frac` by construction.

**The subtle one — the same-HRU restriction.** `sro_to_dprst_perv/imperv`'s
numerators are **not** a plain zonal weight. `same_hru_drains.build()` does a
raster-space per-cell test — `drains_to_dprst_hru == hru_id` — to build
`drains_perv_binary.tif` / `drains_imperv_binary.tif` **before** gdptools runs,
counting a cell only if it drains to a depression in its *own* HRU. This
reproduces the legacy ArcPy `Con(rSro == hru)`
([docs/0b_TB_depr_stor.py:214](0b_TB_depr_stor.py)). Note it reads the *labeled*
`drains_to_dprst_hru.tif`, not the binary `drains_to_dprst.tif`.

**Non-spatial constants** (`dprst_flow_coef`, `dprst_seep_rate_*`, `smidx_exp`,
`op_flow_thres = 1.0`, …) are not part of this raster→param chain; see
[`pywatershed_depression_storage_requirements.md`](pywatershed_depression_storage_requirements.md).

---

## 4. Staleness / maintenance

The classifier accreted gate-by-gate; this section says which gates still earn
their place, which are dead, and what is in motion. Counts below are from the
**staged tables as currently built** (data-root artifacts; they drift as the
product is rebuilt).

### Confirmed load-bearing — *proven with data*, despite looking redundant

- **C1 (WBAREACOMI) vs C2 (flow-through)** are **not** redundant. Measured on the
  current tables: `connected − flowthrough` = **7,496 COMIDs** that flow-through
  misses (and `flowthrough − connected` = 35,500). Neither subsumes the other;
  keep both. *(The code computes only flow-through's new contribution, never the
  reverse, so it can't tell you this itself — this is the `set(connected) −
  set(flowthrough)` diff on the two parquets.)*
- **Signal A vs Signal B** are complementary. Measured: Signal-A-only = 1,436,
  Signal-B-only = 16,588, both = 4,916. Signal B carries the *count*; Signal A
  carries the *area* (GSL). The per-signal provenance columns + per-signal floor
  guard let you re-verify this from the shipped table anytime.
- **Signal A vs source-lake promotion (D1 in flow-through)** push in opposite
  directions (demote closed lakes vs. promote headwater/source lakes) and cannot
  cancel.
- The **"inert-today" `_assert_*` guards** in `wbody_connectivity` /
  `waterbody` are data-conditional invariants (they fire only if a future layer's
  COMID keys diverge or a BurnAdd clump reaches on-stream), not dead code. Keep.

### Confirmed dead — safe to remove *(the cleanup in this PR)*

- **`imperv_regions`** in `dprst.build()`
  ([dprst.py:82](../src/gfv2_params/depstor_builders/dprst.py#L82)) — a
  CONUS-scale full-grid `regions_touching_mask` pass whose result is, by its own
  comment, *"kept only for logging."* Impervious exclusion is the per-cell carve
  (D3); this region set changes no outcome. A decision-inert full-grid pass at
  16.9-billion-cell scale for one INFO line.
- **`depstor_builders/streambuffer.py`** — the pre-connectivity on-stream signal,
  retired when `wbody_connectivity` replaced it (documented at
  [base_config.yml:37](../configs/base_config.yml#L37)). Not in `STEP_ORDER`, not
  imported anywhere.
- **`depstor_builders/intersect.py`** — superseded by `same_hru_drains`. Not in
  `STEP_ORDER`, not imported. *(Distinct from the live `depstor.intersect_binaries`
  function — same word, different module.)*

### Intentionally retained — *not* dead, despite zero classifier reads

- **`sink_points_table`** — provenance + BurnAdd linkage; the profile comment and
  `BuildContext` docstring both say "not a classifier." Leave it.
- **Diagnostic fraction CSVs** — `onstream_storage_frac` and `drains_to_dprst_frac`
  are declared in `depstor_params.yml` `fractions:` but referenced by no `ratios:`
  entry; they're QA outputs, not parameter inputs. Harmless.

### In motion — decisions, not defects

- **Waterbody layer is now per-fabric.** `gfv2` reads the source-derived
  `nhd_waterbodies.gpkg` (PR #179); `gfv2_dev`
  ([:129](../configs/base_config.yml#L129)), `oregon`, and `tjc` still read
  `conus_waterbodies.gpkg`. Migrate them deliberately, or track the divergence.
- **Reproducibility gap.** `download/nhd_waterbodies.py` writes
  `nhd_waterbodies.parquet`, but the builders read `nhd_waterbodies.gpkg`, and **no
  script in the repo converts one to the other** — a manual `ogr2ogr`-type step in
  an otherwise fully-scripted staging chain. Worth closing.
- **Orphan hand-made files on the data root** (not in the repo, not deleted by this
  PR): `input/nhd/sink_cats.gpkg` (referenced nowhere), `input/nhd/closed_huc12.gpkg`
  and `input/nhd/NHD_sink_points.gpkg` (superseded; the config warns against using
  them). Safe to delete from the data root when convenient — done outside version
  control.

---

## 5. One-page map

```
 DATA SOURCES (base_config.yml gfv2 profile)          STAGED BY (download/)
 ─────────────────────────────────────────           ─────────────────────
 nhd_waterbodies.gpkg .......... waterbody_gpkg        nhd_waterbodies
 flowline_topology.parquet ..... (by path) ── gate ──▶ nhd_topology   (FIRST)
 connected_…comids.parquet ..... connected_comids ──┐  nhd_flowlines
 flowthrough_…comids.parquet ... flowthrough_comids ─┤  nhd_flowthrough
 wbd_huc12.parquet ............. wbd_huc12_table ────┤  wbd_huc12
 burn_add_waterbodies.parquet .. burn_add_waterbody ─┤  nhd_burn_components
 gfv2_fdr.vrt (code-0 = sinks) . fdr_raster ─────────┤  clip_shared_to_fabric
 NLCD impervious ............... imperv_source ──────┤  (external)
 HRU fabric .................... hru_gpkg ───────────┘  (landmask step)
                                                        │
        ┌───────────────────────────────────────────────┘
        ▼   THE GATE LADDER   (depstor_rasters.yml steps → depstor_builders/)
 ┌──────────────┐   ┌───────────────┐   ┌──────────────────────┐   ┌───────────┐
 │  waterbody   │──▶│   endorheic   │──▶│  wbody_connectivity  │──▶│   dprst   │
 ├──────────────┤   ├───────────────┤   ├──────────────────────┤   ├───────────┤
 │ + BurnAdd    │   │ Signal A:     │   │ C1 WBAREACOMI  ─┐     │   │ D1 clump  │
 │ − Ice Mass   │   │  terminus-in- │   │ C2 flow-through ┴ ∪   │   │  on-strm  │
 │   (→ land)   │   │  self >0.5    │   │        (Network-gated)│   │  exclude  │
 │ − slivers    │   │ Signal B:     │   │ C3 − endorheic (strict│   │ D2 endorh │
 │ → clumps     │   │  closed HUC12 │   │       subtraction)    │   │  EXEMPT   │
 │              │   │  majority-area│   │ C4 Playa/IceMass never│   │ D3 imperv │
 │              │   │               │   │ → connected_wbody.tif │   │  carve/cel│
 │              │   │ → COMID table │   │ → endorheic_wbody.tif │   │ D4 land   │
 └──────────────┘   └───────────────┘   └──────────────────────┘   └─────┬─────┘
   default = a waterbody is DEPRESSION STORAGE unless proven on-stream    │
                                                                          ▼
   PRODUCTS → PARAMETERS   (depstor_params.yml: fractions ÷ ratios)   dprst_binary.tif
   ────────────────────────────────────────────────────────────────
   dprst_binary        ÷ land_mask     → dprst_frac
   imperv_binary       ÷ land_mask     → hru_percent_imperv
   drains_perv_binary  ÷ perv_binary   → sro_to_dprst_perv    (same-HRU: drains_to_dprst_hru==hru_id)
   drains_imperv_binary÷ imperv_binary → sro_to_dprst_imperv  (same-HRU)
   carea_map_t8_binary ÷ perv_binary   → carea_max            (clamp ≤1)
   carea_map_t156_binary÷perv_binary   → smidx_coef           (clamp ≤1)
   dprst_depth.tif (mean, masked to dprst_binary) → dprst_depth_avg
```

---

*Maintenance: regenerate the measured counts in [§4](#4-staleness--maintenance)
from the staged parquets after any rebuild. If a gate's `file:function` cite goes
stale, the durable anchor is the function name, not the line.*
