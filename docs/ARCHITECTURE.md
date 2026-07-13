# Architecture

The single canonical source for the project's architecture. If anything here
contradicts CLAUDE.md or README.md, **this doc wins** — the others link to
here as the truth.

## Overview

`gfv2-params` generates PRMS/NHM hydrologic-model parameters by running zonal
statistics over CONUS source rasters (DEM, soils, lithology, LULC,
depression-storage) against a watershed fabric of HRU polygons. Production
runs are CONUS-scale on a USGS HPC cluster under SLURM; smaller fabrics
(e.g. `gfv2_vpu01`, `oregon`) are used for development and validation.

## Data-root layout (the key invariant)

A single on-disk `data_root` is set in `configs/base_config.yml`. Everything
the pipeline reads or writes lives under it, in three top-level subtrees:

```
data_root/
├── input/                      # Manually staged or downloaded external data
│   ├── fabric/                 # Per-VPU watershed fabric gpkgs
│   ├── soils_litho/            # TEXT_PRMS.tif, AWC.tif, Lithology_exp_Konly_Project.*
│   ├── lulc_veg/               # RootDepth.tif, CNPY.tif, Imperv.tif (+ per-source subdirs)
│   ├── lulc/                   # NLCD impervious, NALCMS land cover (downloadable)
│   ├── depstor/                # Per-fabric depression-storage inputs
│   ├── twi/<rpu>/              # Per-RPU TWI (staged via stage_twi.sh)
│   ├── nhm_default/            # NHM default parameter files
│   └── nhd_downloads/          # Raw NHDPlus zip archives
├── shared/                     # Fabric-INDEPENDENT intermediates (reused by every fabric)
│   ├── source/                 # Unzipped per-RPU NHDPlus rasters
│   ├── per_vpu/<vpu>/          # Per-VPU merged GeoTIFFs (NED, Hydrodem, Fdr, Fac, Twi, slope, aspect, landmask)
│   └── conus/
│       ├── vrt/                # CONUS GDAL virtual rasters (elevation/slope/aspect/fdr/twi/twi_hydrodem)
│       ├── derived/            # soil_moist_max.tif, radtrn, resampled CNPY/keep
│       ├── borders/            # Copernicus border-DEM fill (Canada/Mexico)
│       └── weights/            # P2P polygon weights for ssflux
└── {fabric}/                   # Per-fabric outputs (gfv2/, gfv2_vpu01/, oregon/, ...)
    ├── fabric/                 # Merged fabric gpkg
    ├── batches/                # Per-batch gpkgs + manifest.yml
    ├── depstor_rasters/        # Depression-storage intermediate rasters
    └── params/                 # Parameter outputs + merged/ + filled
```

**The invariant: every fabric reuses the same `shared/` rasters.** Per-VPU
iteration happens *inside* builders, not in per-VPU SLURM submissions. A new
fabric needs new `input/fabric/<gpkg>` + a new `{fabric}/` output tree; it
does NOT need new `shared/` content.

## Part 1 vs Part 2

The pipeline splits into two halves that share `data_root` but execute
independently:

- **Part 1 — fabric-independent.** Produces `shared/` content from `input/`.
  One run per CONUS, reused by every fabric. Driven by `build_shared_rasters.py`.
- **Part 2 — fabric-dependent.** Produces `{fabric}/` content by combining
  the fabric's HRU geometry with `shared/` rasters. Splits further into
  **2a (depstor)** and **2b (zonal)** which can run in parallel after Part 1
  finishes.

The natural parallelism boundary: Part 1 once per CONUS, Part 2 N times (one
per fabric). For most regional fabrics Part 1 can be scoped to the VPUs the
fabric overlaps (e.g. `VPUS=17` for `oregon`).

## Orchestrator + builder + unified-config pattern

Each pipeline stage is **one orchestrator script + one unified YAML config +
a package of per-step builder modules**. The orchestrators walk a step DAG
and dispatch into library functions; the SLURM `*.batch` wrappers are thin
shells around the same builders. The four stages:

| Stage | Orchestrator | Config | Builders |
|---|---|---|---|
| Part 1 shared rasters | `scripts/build_shared_rasters.py` | `configs/shared_rasters/shared_rasters.yml` | `src/gfv2_params/shared_rasters/` |
| Part 2a depstor rasters | `scripts/build_depstor_rasters.py` | `configs/depstor/depstor_rasters.yml` | `src/gfv2_params/depstor_builders/` |
| Part 2a depstor params | `scripts/derive_depstor_params.py` | `configs/depstor/depstor_params.yml` | `src/gfv2_params/depstor_ratios.py` |
| Part 2b zonal params | `scripts/derive_zonal_params.py` | `configs/zonal/zonal_params.yml` | `src/gfv2_params/zonal_runners/` |
| Part 2c snow-depletion aggregation | `scripts/derive_aggregate.py` | `configs/aggregate/aggregate_sources.yml` | `src/gfv2_params/aggregate/` |
| Part 2c snow-depletion curve build | `scripts/derive_snarea_curve.py` | `configs/snarea/snarea_curve.yml` | `src/gfv2_params/snarea/` |
| Part 2c snow-depletion curve library | `scripts/derive_snarea_library.py` | `configs/snarea/snarea_library.yml` | `src/gfv2_params/snarea/library.py` |

Orchestrators support `--step <name>` (one step), `--from <name>` (resume),
and `--force` (rebuild outputs that already exist). The zonal orchestrator
also supports `--mode zonal|merge|build_weights` for per-batch debugging.

SLURM submission wrappers (`slurm_batch/submit_*.sh`) chain array jobs →
merges → ratios via `afterok` dependencies.

### Per-package details

Each builders package has its own `__init__.py` documenting the per-step
contract:

- [`src/gfv2_params/shared_rasters/__init__.py`](../src/gfv2_params/shared_rasters/__init__.py) — Part 1 builders (10 modules)
- [`src/gfv2_params/depstor_builders/__init__.py`](../src/gfv2_params/depstor_builders/__init__.py) — Part 2a raster builders (11 modules)
- [`src/gfv2_params/zonal_runners/__init__.py`](../src/gfv2_params/zonal_runners/__init__.py) — Part 2b param runners (6 modules)
- [`src/gfv2_params/aggregate/`](../src/gfv2_params/aggregate/) — Part 2c
  Stage 1: a source-agnostic gridded-**time-series** → HRU aggregation
  harness — the time-series counterpart to `zonal_runners` (which handles
  static rasters). Wraps gdptools `UserCatData`/`WeightGen`/`AggGen` behind a
  declarative `SourceAdapter` (`adapter.py`); `driver.py`'s `aggregate_source`
  caches the per-fabric weight matrix once and loops `AggGen` per year. The
  current adapter, `snodas.py`, area-weights daily SNODAS SWE to `swe` (mean),
  derives `scov`/SCA (`masked_mean` of `swe > 0`, NaN-preserving over
  fill/nodata cells), and emits a `swe_std` sidecar (`std_variables=("swe",)`,
  per-cell SWE std dev within the HRU) — feeds Part 2c Stage 2 below, whose
  sub-grid CV needs `swe_std`. New gridded time-series
  sources (e.g. climate) plug in as a new `SourceAdapter`, not a new script.
- [`src/gfv2_params/snarea/`](../src/gfv2_params/snarea/) — Part 2c Stage 2:
  derives the empirical PRMS `snarea_curve` (11-point areal snow-depletion
  curve) and per-HRU sub-grid CV from the Stage 1 daily SWE/SCA/`swe_std`, per
  Driscoll, Hay & Bock (2017): per-calendar-year melt-season curve extraction
  (`season.py`), median/similarity/representative-curve selection
  (`representative.py`), six selection criteria + low/mid/high classification
  (`selection.py`), sub-grid CV from `swe_std` (`subgrid.py`), and final
  per-HRU assembly with default-curve fallback (`build.py`) — writes the
  intermediate derived CSV, not the terminal params. Design spec:
  [`docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md`](superpowers/specs/2026-07-04-snodas-snarea-curve-design.md);
  converted method paper: [`docs/Snow_Depletion_Curves.md`](Snow_Depletion_Curves.md).
- [`src/gfv2_params/snarea/library.py`](../src/gfv2_params/snarea/library.py) —
  Part 2c Stage 3 (`scripts/derive_snarea_library.py`): builds a physically-based
  CV/lognormal `snarea_curve` library from the Stage 2 derived CSV (Sexstone,
  Driscoll, Hay, Hammond & Barnhart 2020) — analytic curve-from-CV
  (`sdc_from_cv`), CV fit to each empirical curve (`fit_cv`), calibration of
  sub-grid CV against the empirical overlap (`validate_and_calibrate`),
  equal-population CV-bin library (`build_library`), nearest-CV assignment
  (`assign_deplcrv`), and the terminal params CSV + pyWatershed NetCDF writers.
  Design spec:
  [`docs/superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md`](superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md).

For a narrative/visual overview of the whole SNODAS → `snarea_curve` workflow
(what a depletion curve is, the Driscoll/Sexstone methods, per-stage figures, and
the pyWatershed products), see the Marp deck
[`docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md`](presentations/2026-07-snodas-snow-depletion-curves.slides.md).

Each `build(step_cfg, ctx, logger)` function produces named outputs that
downstream steps can reach via the shared context. The orchestrator/builder
pattern, the `BUILDERS` dispatch dict, and the `BuildContext` dataclass are
explained for non-Python-fluent readers in
[`docs/python-patterns.md`](python-patterns.md).

## Fabric profiles — the single source of truth

`configs/base_config.yml` holds the `data_root` and a `fabrics:` mapping of
profiles. **Every shared, required per-fabric input lives in its profile** —
never as a required CLI arg, never inferred from a naming convention.
Scripts read keys via `require_config_key(config, key, script_name)` from
`src/gfv2_params/config.py`, which also resolves placeholder substitution
(`{data_root}`, `{fabric}`, `{vpu}`, `{raster_vpu}`). Per-step configs are
fabric-agnostic templates resolved at runtime.

### Active fabric resolution (highest precedence first)

1. `--fabric <name>` CLI flag on any script
2. `FABRIC` env var (typical for `sbatch --export=ALL,FABRIC=...`)
3. `default_fabric` in `configs/base_config.yml` (currently `gfv2`)

### Required profile keys

Register a new fabric with `pixi run init-data-root --add-fabric <name>` to
append a profile stub; fill the stub's TODOs. Required keys depend on
whether the depstor pipeline will be run for the fabric:

| Key | Always required | Depstor only | Notes |
|---|:-:|:-:|---|
| `hru_gpkg` | ✓ | — | Path to the fabric geopackage (post-merge for VPU-based fabrics) |
| `hru_layer` | ✓ | — | Layer name inside `hru_gpkg` (typically `nhru`) |
| `id_feature` | ✓ | — | The HRU id column in the fabric (e.g. `nat_hru_id` for gfv2, `hru_id` for oregon); flows through to merged parameter CSVs |
| `expected_max_hru_id` | ✓ | — | Used by `merge_and_fill_params` to detect gaps in the merged output |
| `batch_size` | ✓ | — | Target features per spatial batch in `prepare_fabric` |
| `template_raster` | — | ✓ | Fabric-bounds clip of `fdr.vrt`; produced by `clip_shared_to_fabric.py` |
| `fdr_raster` | — | ✓ | Same fabric-bounds clip (typically points at the same file as `template_raster`) |
| `twi_raster` | — | ✓ | CONUS `twi.vrt` (ArcPy, calibrated) or `twi_hydrodem.vrt` (open-source, CONUS-complete) |
| `segments_gpkg` | — | ✓ | Stream-segment gpkg (no longer feeds any depstor step — the `streambuffer` step is retired). A VPU-based fabric (gfv2) merges per-VPU `nsegment` layers via `scripts/merge_vpu_segments.py` for other potential uses. |
| `segments_layer` | — | ✓ | Layer name inside `segments_gpkg` (typically `nsegment`) |
| `connected_comids_table` | — | ✓ | Path to `input/nhd/connected_waterbody_comids.parquet` — the set of NHDPlusV2 waterbody COMIDs that a **Network** NHD artificial path flows through (i.e. on-stream via `WBAREACOMI`). Produced by `download/nhd_flowlines.py`, which keeps a WBAREACOMI only if the flowline carrying it is a Network Flowline (in `flowline_topology.parquet`), so Non-Network artificial paths NHD draws through closed-basin lakes don't promote endorheic waterbodies on-stream (issue #161); consumed by the depstor `wbody_connectivity` builder. Required only for fabrics whose waterbody layer is COMID-keyed (`gfv2`, `oregon`, `tjc`); the `gfv2_vpu01` profile omits it (its `wbs` layer has no COMID), so the depstor DAG fail-fasts there — at the `endorheic` step first (it raises on a waterbody layer with no COMID column), and at `wbody_connectivity`/`dprst` after that. Use `gfv2` for depstor validation. |
| `flowthrough_comids_table` | — | — | Path to `input/nhd/flowthrough_waterbody_comids.parquet` — a second on-stream COMID set from flow-through topology: waterbodies that a **Network** conveyance flowline demonstrably enters AND exits (T1), or whose upstream end is inside the waterbody per authoritative NHDPlus routed-network direction (D1 — source/headwater lakes and split pass-through outflows), or that overlap an NHDArea conveyance polygon (T3). T1/D1 candidate flowlines are gated to Network Flowlines (in `flowline_topology.parquet`) so Non-Network closed-basin lines can't promote endorheic lakes (issue #161). Playa/Ice Mass waterbodies are dropped up front and never promoted onto the on-stream set (Playa because it's force-dprst; Ice Mass because it's excluded from the waterbody classification entirely — see the `waterbody` row below). Produced by `download/nhd_flowthrough.py`; unioned with `connected_comids_table` by `wbody_connectivity` before rasterizing (which also re-applies the `NEVER_ONSTREAM_FTYPES` guardrail to the unioned set, so it covers the WBAREACOMI path too). Optional (omitting it uses `connected_comids_table` only). |
| `waterbody_gpkg` | — | ✓ | NHDPlus waterbodies; depstor's `waterbody` step **raises** if unset. If the layer has an `FTYPE` column, `waterbody` drops `EXCLUDE_WATERBODY_FTYPES` (`{"Ice Mass"}`) before rasterizing: a glacier/permanent ice mass is not depression storage, so its cells are left out of `wbody_binary`/`wbody_regions` entirely and fall back to land (perv/imperv via LULC), not dprst and not on-stream. Playa is unaffected here — it stays a normal waterbody clump and is force-dprst downstream by the `NEVER_ONSTREAM_FTYPES` guardrail in `wbody_connectivity`/`nhd_flowthrough`. |
| `waterbody_layer` | — | ✓ | Layer name inside `waterbody_gpkg` |
| `wesm_index` | — | ✓ | Path to `input/wesm/wesm_1m_footprints.gpkg` — pre-staged, 1m/QL1/QL2-qualifying USGS 3DEP WESM workunit footprints (a `project` column + geometry). Produced by `pixi run python -m gfv2_params.download.wesm` (issue #173). Consumed by the `dprst_depth` step's `topo.resolution_class` (best-available-topo tagging) and `tiling.group_by_tile` (1 m tile-key resolution); required for `dprst_depth`, not for any other depstor step. |
| `ecoregions_gpkg` | — | ✓ | Path to `input/ecoregions/us_eco_l3.gpkg` — EPA Level III Ecoregions (see `gfv2_params.download.epa_ecoregions`). Used by the `dprst_depth` step's per-ecoregion regional-fill donor pool (`dprst_depth.fill.fit_ecoregion_models`); every fabric profile with a depstor-configured `dprst_depth` step already stages it (also listed as a shared, reusable input in `README.md`'s Stage 0). |
| `wbd_huc12_table` | — | — | Path to `input/wbd/wbd_huc12.parquet` — the full WBD HUC12 layer. Both ends filter `HU_12_TYPE == 'C'` (closed basin): `download/wbd_huc12.py` stages only type-C rows, **and** the `endorheic` depstor builder re-applies the filter itself (a table with no `HU_12_TYPE` column raises), so pointing this at a genuine full WBD layer cannot flag every waterbody endorheic and empty the on-stream set. Optional: absent turns off Signal B (majority-inside-closed-HUC12) and the `endorheic` step still runs Signal A (FDR terminus-inside-itself) alone. Do **not** point this at `input/nhd/closed_huc12.gpkg` — that is an incomplete extract (23 type-C HUC12s in the Great Basin vs 141 in the full WBD). |
| `burn_add_waterbody_table` | — | — | Path to `input/nhd/burn_add_waterbodies.parquet` — the **sink-purpose subset** of NHDPlus's BurnAddWaterbody polygons (new depression AREA; 1,658 polygons / 721.9 km² CONUS-wide), unioned into the waterbody layer by the `waterbody` builder's `merge_burn_add`, **before** the `EXCLUDE_WATERBODY_FTYPES` (Ice Mass) filter runs, so a BurnAdd Ice Mass polygon is still excluded. Configured-but-missing fails loud (`FileNotFoundError`), never silently skipped. BurnAdd rows are never on-stream-promotable, but not because `NEVER_ONSTREAM_FTYPES` is applied to them — `wbody_connectivity`/`nhd_flowthrough` re-read the raw `waterbody_gpkg` from disk, never the merged frame this builder produces, so that guardrail is never evaluated against a BurnAdd row at all. Safety is structural instead: `merge_burn_add` asserts every BurnAdd COMID (NHDPlus `PolyID`) is negative, so it can never match a positive WBAREACOMI/flow-through COMID, and asserts no BurnAdd polygon lies within one rasterized cell diagonal (`cell_size * sqrt(2)`, passed in from the template raster) of an existing **on-stream** waterbody — a buffered spatial join, not plain vector intersection, because `clump_regions`' 8-connectivity can merge cells that never touch in vector space. The guard is restricted to on-stream neighbours (via `_load_onstream_comids`, the same pre-endorheic WBAREACOMI ∪ flow-through union `wbody_connectivity` computes, minus `NEVER_ONSTREAM_FTYPES`) because merging with an already-dprst neighbour is harmless — the clump simply stays dprst — whereas an on-stream neighbour would silently drag the BurnAdd depression out of dprst; measured against real CONUS data, 112 of 1,658 BurnAdd polygons genuinely overlap an existing waterbody, all 112 neighbouring an already-dprst waterbody and none on-stream, so the original unconditional guard aborted the whole CONUS build over a failure mode that doesn't occur. If the on-stream COMID table(s) aren't configured or not yet staged, `merge_burn_add` falls back to the old broad guard (raises on ANY overlap) rather than silently skipping the check. Optional, staged by `gfv2_params.download.nhd_burn_components` — which keeps only the rows whose `PurpCode` is a sink purpose (4 Playa / 5 closed lake / 8 closed lake) and drops the rest: **BurnAddWaterbody is not a sink layer**, it is every waterbody NHDPlus added to the DEM burn, and VPU 01 alone ships 702 NULL-`PurpCode` rows (503 on-network, including StreamRiver and CanalDitch FCodes) against **zero** sinks in its own `Sink.shp`. FTYPE comes from `FCODE`, not `PurpCode` (`PurpCode` 5 spans both Playa and SwampMarsh). |
| `sink_points_table` | — | — | Path to `input/nhd/sink_points.parquet` — NHDPlus `Sink.shp` (15,728 sinks CONUS-wide). **Intentionally unread: no builder consumes it.** It is threaded through the profile and `BuildContext` for provenance and for the BurnAddWaterbody linkage (`SOURCEFC`/`FEATUREID`), so the sink layer that explains those polygons is staged and discoverable alongside them. It is **not** a classifier signal and must not be wired up as one: the `endorheic` builder's Signal A deliberately reads the FDR grid (the same grid `routing` reads), not this lossy point shadow of it. Optional. |
| `min_endorheic_comids` | — | — | Integer floor on the number of endorheic COMIDs the `endorheic` builder must produce on this fabric (`gfv2`/`gfv2_dev`: 100). Below it, the builder **raises** — on the fresh-build path and on the output-exists skip path — because a collapsed or empty result makes the demotion a silent no-op and leaves the Great Salt Lake on-stream. Optional, and deliberately absent on fabrics that legitimately have no closed basin (`tjc`, Texas-Gulf: 4 FDR code-0 cells, 0 endorheic waterbodies) — there an empty table is the correct result. |

For `template_raster`/`fdr_raster`, stage the clip with:

```bash
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric <name>
# writes {data_root}/<name>/shared/<name>_fdr.vrt
```

Every depstor builder sizes its arrays to the `template_raster` grid, so the
clip scopes compute to the fabric extent while staying VPU-agnostic (works
for fabrics that straddle VPU boundaries).

`snodas_dir` is a similar profile-overridable path, but optional: it points
the Part 2c snow-depletion aggregation (Stage 1) at a fabric's raw daily
SNODAS SWE NetCDFs. It defaults to the shared datastore path
(`{data_root}/../nhf-datastore/snodas/daily`) in
`configs/aggregate/aggregate_sources.yml` and only needs a profile entry if a
fabric's SNODAS source differs from that default.

### Common fabrics

- **`gfv2`** — CONUS production fabric (~361k HRUs).
- **`gfv2_vpu01`** — small-scale validation overlay (~11k HRUs in VPU 01).
- **`oregon`** — current regional test fabric (~17k HRUs incidental to VPU 17).

## Non-obvious conventions & gotchas

These are hard-won; violating them silently corrupts outputs.

- **Depstor template/fdr come from a fabric-bounds clip** of `fdr.vrt`
  ([`scripts/clip_shared_to_fabric.py`](../scripts/clip_shared_to_fabric.py)),
  not from CONUS VRTs or per-VPU tiles. The clip must come from the
  hydrology lattice (`fdr.vrt` / `twi.vrt`); `elevation.vrt` is on the
  offset DEM lattice and `carea_map` requires `template ≡ twi` alignment.
- **FDR provenance: `fdr.vrt` is the official NHDPlus V2 `FdrFac` flow
  direction** — merged from the per-RPU `FdrFac` component
  (`download/rpu_rasters.py`) into `Fdr_merged_*.tif` and VRT'd by
  `shared_rasters/build_vrt.py` (`"fdr": ("Fdr_merged_*.tif", "255")`; Byte,
  D8 codes 1–128). It is computed on the NHDPlus **HydroDEM**, which is
  **stream-burned, walled, and depression-filled (fully drainage-enforced)** —
  i.e. interior depressions are removed. `routing`/`drains_to_dprst` traces
  upslope on this FDR, so a depression low in the network captures a large
  contributing area *because the conditioning forces flow through former
  sinks*. (The legacy ArcPy parameterization used a different but also
  fully-filled FDR: SRTM → `arcpy.sa.Fill` → `FlowDirection`, no stream-burn;
  Bock et al. 2020, DOI 10.5066/P971JAGF.) The repo's
  `shared_rasters/compute_dem_derivatives.py` (richdem `FillDepressions`+epsilon
  → WBT D8) is an **opt-in parallel** product (`Fdr_hydrodem`), **not** what
  depstor routes on. Whether a *depression-respecting* FDR (breach, or
  depth/area-thresholded fill) would give more local depression-storage
  contributing areas is an open investigation — see issue #147.
  A second opt-in step, `compute_breached_fdr`
  (`shared_rasters/compute_breached_fdr.py`), produces
  `Fdr_breached_<vpu>.tif` per VPU and is registered into `fdr_breached.vrt`
  by `build_vrt`. This is **additional** infrastructure only — it never
  replaces `fdr.vrt`. Custom fabrics investigating issue #147 may clip
  `fdr_breached.vrt` and point `fdr_raster` at the result to route depstor
  on the depression-respecting FDR. See the design spec
  [`docs/superpowers/specs/2026-06-29-depression-respecting-fdr-design.md`](superpowers/specs/2026-06-29-depression-respecting-fdr-design.md)
  and the A/B runbook in `slurm_batch/HPC_REFERENCE.md`
  ("§ #147 depression-respecting FDR A/B").
- **On-stream waterbodies are traversal barriers in `routing`.** The `routing`
  step also consumes `onstream_binary.tif` (emitted by the `dprst` step): a
  cell is `drains_to_dprst` only if its D8 flow path reaches a
  depression-storage pour-point **before** it reaches any on-stream waterbody
  cell — traversal stops at the first waterbody on the path. This makes
  `drains_to_dprst` a strict subtraction from the pre-barrier behavior
  (coverage can only decrease, never increase): land upslope of an on-stream
  lake or reservoir is captured by that waterbody's stream/lake routing, not
  a downstream depression. Playas need no special handling — they are
  classified `dprst`, never `onstream`, so they are never barriers.
- **Same-HRU restriction on `sro_to_dprst_perv`/`sro_to_dprst_imperv` is a
  raster-space intersection, not a gdptools operation.** The chain is
  `hru_id` (rasterises `nat_hru_id` onto the template via `rasterize_ids`,
  `all_touched=True` → `hru_id.tif`, int32) → `routing_hru` (a labeled, barrier-aware D8 trace —
  same per-VPU tiling and on-stream barriers as `routing`, but each depression
  cell is labelled with its own HRU id and the kernel propagates that label to
  every cell that drains to it → `drains_to_dprst_hru.tif`, int32, per-cell
  reached-HRU) → `same_hru_drains` (replaces the old plain `intersect` step
  for `drains_perv`/`drains_imperv`, same output filenames/keys). It computes
  `drains_to_dprst_hru == hru_id` cell-by-cell (`same_hru_intersect` in
  `depstor.py`) **before** aggregation — deliberately **not** expressed as a
  gdptools zonal operation, because it is a per-cell test (does this cell's
  reached depression belong to *this same cell's* HRU?) that gdptools'
  partial-pixel weighting cannot express; a fractional-overlap weight has no
  way to encode "same HRU or not." The per-HRU **count** aggregation
  downstream is unaffected and still uses gdptools as normal. This reproduces
  the legacy `Con(rSro == hru)` (`docs/0b_TB_depr_stor.py:214`). `hru_id.tif`
  is rasterised `all_touched=True` to match `land_mask.tif`/`perv_binary.tif`'s
  footprint (`landmask.py`); a stricter (default) footprint would leave
  HRU-boundary land cells at `hru_id==0`, and `same_hru_intersect` (which
  requires `labeled==hru_id & labeled>0`) would silently drop them —
  undercounting `drains_perv`/`drains_imperv` at every HRU edge. The tradeoff
  is a 1-pixel HRU-boundary approximation (a cell rasterised into HRU A that
  geometrically straddles into HRU B), which is immaterial against the
  basin-scale `sro_to_dprst_*` signal. `drains_to_dprst.tif` (from `routing`)
  and the `drains_to_dprst_frac` param stay HRU-agnostic — only the
  `sro_to_dprst_*` ratios get the same-HRU restriction; `depstor_params.yml`
  is unchanged.
- **Land masking.** Every depstor raster is masked against `land_mask.tif`
  (the HRU fabric rasterised by the `landmask` step). Never use hydro-DEM
  nodata or FDR as a land mask.
- **WhiteboxTools cannot read LZW + `predictor=2` GeoTIFFs** — it silently
  corrupts them. Never pass `predictor=2` rasters to WBT subprocesses.
- **The continuous-float mosaic rasters are Cloud-Optimized.** Every CONUS-VRT
  source that is a continuous float surface — `elevation`/`slope`/`aspect`
  (`compute_slope_aspect` + the Copernicus border fill in `build_border_dem`),
  `twi` (`merge_rpu_by_vpu`), and `twi_hydrodem` (`compute_dem_derivatives`) —
  is written as a COG (tiled 512, internal overviews, ZSTD + `PREDICTOR=3`) via
  the shared `shared_rasters/cog.py` helper, and `build_vrt` adds an external
  `.vrt.ovr` overview pyramid to each CONUS VRT. This serves both consumers —
  fast continental QGIS pan/zoom and fast windowed reads for zonal
  stats/resampling (exactextract/gdptools/rioxarray). Aspect uses **nearest**
  overview resampling (circular 0/360 field); continuous surfaces use bilinear.
- **WBT-safety boundary for `to_cog`.** `to_cog` (ZSTD + predictor) is only for
  the GDAL/rasterio/QGIS-consumed float rasters above. WBT-fed rasters — the
  `Hydrodem` fixed/filled DEMs in `compute_dem_derivatives`, the per-VPU
  `NEDSnapshot`/`Hydrodem` merge tiles, and the `FDR`/`FAC` tiles — must stay
  LZW-without-predictor (WBT only reads PACKBITS/LZW/DEFLATE and silently
  corrupts predictor input, see the gotcha above) and are deliberately left on
  their existing write paths. The `fdr` VRT still gets a nearest-resampled
  `.vrt.ovr` for rendering, but its **source tiles** are not COG-converted.
- **CONUS-scale memory: stream/window, never hold a full-grid array.** The
  CONUS template is ~16.9 B cells (~17 GB uint8, ~68 GB int32, ~135 GB
  float64); whole-grid ops OOM the 503 GB node ceiling. `routing` tiles the
  in-process D8 routing pass per VPU (it runs after `vpu_id`, routes each VPU in
  isolation, and mosaics); reproject with streaming `gdal.Warp`, not in-memory
  `rioxarray.reproject_match`; window per `STRIP_ROWS` like `carea_map`. See
  CLAUDE.md for the full gotcha.
- **CONUS-scale COMPUTE (not memory): `dprst_depth` is per-polygon, not
  per-cell — budget core-hours, not GB.** Every other depstor step's cost
  scales with the CONUS grid (cells); `dprst_depth`'s cost scales with the
  dprst polygon count (~286k) times one windowed DEM read each, ~250-500
  core-hours run serially — small individually, but with no per-cell ceiling
  to hit an OOM guard on, so nothing stops it from silently running for
  weeks inside a single job unless it's fanned out. Its SLURM array bins by
  elevation TILE (`tiling.group_by_tile`/`component_tile_batches`), not HRU
  batch, and MUST run via `slurm_batch/submit_dprst_depth.sh` (or the
  equivalent plan → array → build chain) before the ordinary
  `build_depstor_rasters.batch` walk reaches the `dprst_depth` step — see the
  "How to add a new pipeline step" exception above and
  `slurm_batch/HPC_REFERENCE.md`'s "Stage 2d'".
- **On-stream classification is the union of two COMID sources.** The
  `wbody_connectivity` builder loads both `connected_waterbody_comids.parquet`
  (WBAREACOMI artificial-path topology, staged by `download/nhd_flowlines.py`)
  and `flowthrough_waterbody_comids.parquet` (flow-through topology, staged by
  `download/nhd_flowthrough.py`) and unions them before rasterizing. **Both
  staging steps gate on-stream promotion on Network-Flowline membership** — a
  COMID present in `flowline_topology.parquet` (NHDPlus PlusFlowlineVAA). NHD
  draws Non-Network artificial paths through essentially every closed-basin
  lake, so the ungated WBAREACOMI set and the ungated geometric T1 test both
  wrongly promoted genuinely endorheic waterbodies on-stream (issue #161); the
  gate keeps them in depression storage. This makes `nhd_topology` a
  prerequisite of **both** `nhd_flowlines` and `nhd_flowthrough` (each fails
  loud if the topology parquet is missing). A
  waterbody is flow-through if a **Network** conveyance flowline enters AND exits it (T1),
  or if a routed-network conveyance flowline's upstream end is inside it (D1 —
  authoritative NHDPlus direction from `flowline_topology.parquet`, staged by
  `download/nhd_topology.py`; this catches source/headwater lakes and
  split-pass-through outflows and replaced the old `FLOWDIR`-gated T2), or if
  it overlaps an NHDArea conveyance polygon (T3). `nhd_flowthrough` defines
  `FORCE_DPRST_FTYPES = {"Playa"}` (always depression storage, never promoted
  on-stream) and `EXCLUDE_WATERBODY_FTYPES = {"Ice Mass"}` (not depression
  storage either — a glacier is excluded from the depstor waterbody
  classification entirely and falls back to land/LULC), unioned into
  `NEVER_ONSTREAM_FTYPES`. Both are dropped up front in `flowthrough_comids`
  and never promoted; `wbody_connectivity` re-applies `NEVER_ONSTREAM_FTYPES`
  to the unioned set so a Playa/Ice Mass waterbody promoted via WBAREACOMI is
  also excluded (Ice Mass is belt-and-suspenders here — it's already removed
  upstream at the `waterbody` builder; see the `waterbody_gpkg` row above).
  The `dprst` and downstream builders are unchanged consumers — they see a larger
  on-stream set with no code change.
- **`flowline_topology.parquet`** — distilled NHDPlus PlusFlowlineVAA (COMID,
  DnHydroseq, Hydroseq, TerminalFl, StartFlag, StreamOrde, FromNode, ToNode). Staged by
  `download/nhd_topology.py`; consumed by **both** `download/nhd_flowlines.py`
  (the Network-Flowline gate on WBAREACOMI) and `download/nhd_flowthrough.py`
  (the Network-Flowline gate on T1/D1 candidates + the D1 routed-network outflow
  rule). Hardcoded data_root-relative, no config key — `nhd_topology.py` must
  run before **both** `nhd_flowlines.py` and `nhd_flowthrough.py` (each fails
  loud if `input/nhd/flowline_topology.parquet` is missing).
- **`endorheic` step (runs between `waterbody` and `wbody_connectivity`).**
  Emits `endorheic_waterbody_comids.parquet` (comid, frac_own, by_terminus,
  by_closed_huc12) via `endorheic_frame` (`src/gfv2_params/endorheic.py`):
  Signal A is a waterbody whose D8 terminus (on the same FDR grid `routing`
  reads) lies INSIDE itself; Signal B is a waterbody majority-inside a closed
  (type-C) WBD HUC12, needed because some closed-basin waterbodies (e.g.
  Walker Lake) contain no FDR terminal cell. Signal A needs only `fdr_raster`
  (already required on every fabric) and runs everywhere; Signal B activates
  only when `wbd_huc12_table` is configured (and the builder re-applies the
  type-C filter itself). An EMPTY result is legitimate — a domain with no closed
  basin has no endorheic waterbody (`tjc`, Texas-Gulf: 4 FDR code-0 cells, 0
  flagged; against 15,262 / thousands on `gfv2` and 1,438 / 680 on `oregon`) —
  so the builder writes an empty table and `wbody_connectivity` subtracts the
  empty set, a correct no-op. What fails loud is BREAKAGE: a waterbody layer that
  doesn't overlap the FDR grid, an all-null geometry set, and a result below the
  fabric's optional `min_endorheic_comids` floor (declared by `gfv2`, and checked
  on the output-exists skip path too) — so a silently-empty CONUS result, which
  would leave the Great Salt Lake on-stream, is still impossible to miss.
  `wbody_connectivity` subtracts this COMID set from the unioned on-stream set
  — a STRICT SUBTRACTION, so it can only remove COMIDs, never add one — which
  is what finally takes the Great Salt Lake off-stream (both WBAREACOMI and
  flow-through otherwise promote it, because NHD draws Network artificial paths
  between its arms). If `endorheic_comids` is absent from the build context
  (the `endorheic` step hasn't run for this fabric), `wbody_connectivity` warns
  loudly and proceeds without the demotion rather than failing — terminal/
  closed-basin lakes stay on-stream until the step is run. See
  [`docs/superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md`](superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md).
- **`carea_max`/`smidx_coef` threshold mode.** The legacy `absolute`
  thresholds (8.0/15.6) are only calibrated against VPU 01's ArcPy TWI
  distribution. For any other fabric, use `threshold_mode: percentile` (the
  default in `configs/depstor/depstor_rasters.yml`) with `twi_raster`
  pointing at `twi_hydrodem.vrt` and run the `twi_reference` shared-raster
  step first. See [`docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md`](superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md).

## How to add a new pipeline step

Same recipe for every stage (new shared raster, new depstor builder, new
zonal param family):

1. **Write the builder module** under the appropriate package
   (`src/gfv2_params/shared_rasters/`, `src/gfv2_params/depstor_builders/`,
   or `src/gfv2_params/zonal_runners/`). Export a single
   `build(step_cfg, ctx, logger) -> dict[str, Path]` (raster builders) or
   `run_<name>_batch(config, batch_id, logger) -> None` (zonal runners).
2. **Register in the package's `__init__.py`** — add to the `BUILDERS` /
   `STEP_ORDER` / `BATCH_RUNNERS` registries as appropriate.
3. **Add a config block** in the matching unified config under `configs/`.
4. **Add a test** under `tests/test_<name>.py`. CI (`.github/workflows/ci.yml`)
   gates the merge; the head-node-pytest prohibition (see CLAUDE.md) does
   not apply to PR-driven CI.

Do NOT add a new standalone script or a new YAML file. The
orchestrator + builder + unified-config pattern is the only way new steps
land.

**Exception: a step whose in-process compute cost exceeds one SLURM job's
wall-clock.** `dprst_depth` (issue #173) is the first depstor step whose
`build()` cost — a windowed DEM read per dprst polygon, ~286k polygons
CONUS-wide — is itself ~250-500 core-hours, too large to run serially inside
`build_depstor_rasters.py`'s single job. The step is still a normal builder
module + `BUILDERS`/`STEP_ORDER` registration + config block (Task 7), but
its CONUS-scale compute is fanned out over its OWN SLURM array *ahead of*
that job, keyed on the elevation TILE rather than the HRU batch every other
array in this repo uses (`src/gfv2_params/dprst_depth/tiling.py`'s
`group_by_tile`/`component_tile_batches`, `scripts/run_dprst_depth_batch.py`,
`slurm_batch/submit_dprst_depth.sh`). `depstor_builders/dprst_depth.py`'s
`build()` stays a normal, always-correct in-process fallback (small/test
fabrics take that path automatically — no `batch_dir` populated); the array
just pre-populates `{output_dir}/dprst_depth_batches/*.parquet` so the SAME
`build()` call, when it runs as part of the ordinary
`build_depstor_rasters.batch` walk, finds the work already done and
concatenates instead of recomputing. A future step with a similar per-feature
(not per-cell) compute-budget problem should follow this precedent — a
dedicated plan/array/finalize SLURM DAG feeding the same builder's
`build_dir`/`batch_dir`-style detection, not a change to the orchestrator's
core sequential walk. See `slurm_batch/submit_dprst_depth.sh`'s header for
the sizing arithmetic and `slurm_batch/HPC_REFERENCE.md`'s "Stage 2d'" for
the full DAG + recovery.

For a concrete trace of an existing parameter end-to-end, see
[docs/ADDING_A_PARAMETER.md](ADDING_A_PARAMETER.md) — walks `--param elevation`
through all 5 hops with file:line pointers and the shape of the `config`
dict at each step.

## Related docs

- [`README.md`](../README.md) — user-facing setup + usage
- [`CLAUDE.md`](../CLAUDE.md) — project rules for Claude (atomic commits, doc audit, etc.)
- [`slurm_batch/RUNME.md`](../slurm_batch/RUNME.md) — the step-by-step runbook (CONUS-gfv2 happy path)
- [`slurm_batch/HPC_REFERENCE.md`](../slurm_batch/HPC_REFERENCE.md) — per-stage detail, alternate paths, recovery, script→config map
- [`docs/superpowers/INDEX.md`](superpowers/INDEX.md) — index of design specs, implementation plans, and reviews
- [`docs/depstor_workflow.md`](depstor_workflow.md), [`docs/depstor_port_summary.md`](depstor_port_summary.md), [`docs/depstor_vpu01_validation_results.md`](depstor_vpu01_validation_results.md) — depstor pipeline reference (historical and current)

> **Narrative overview:** see the slide deck
> [`docs/presentations/2026-07-depression-storage-workflow.slides.md`](presentations/2026-07-depression-storage-workflow.slides.md)
> and the pyWatershed parameter contract
> [`docs/pywatershed_depression_storage_requirements.md`](pywatershed_depression_storage_requirements.md).
