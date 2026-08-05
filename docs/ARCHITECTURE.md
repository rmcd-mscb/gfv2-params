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
    └── params/                 # Parameter outputs; merged/ (canonical,
                                 #   gap-filled) + merged/_unfilled/ (pre-fill
                                 #   copies) + merged/_intermediates/
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
- [`src/gfv2_params/depstor_builders/__init__.py`](../src/gfv2_params/depstor_builders/__init__.py) — Part 2a raster builders (15 modules)
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
| `segments_gpkg` | — | ✓ | Stream-segment (`nsegment`) gpkg — the **model routing network** the on-stream classifier is built on. Required by the `segment_wbody` step: a waterbody is on-stream iff a segment intersects it with positive length. A VPU-based fabric (gfv2) merges per-VPU `nsegment` layers via `scripts/merge_vpu_segments.py` first. |
| `segments_layer` | — | ✓ | Layer name inside `segments_gpkg` (typically `nsegment`) |
| `min_onstream_comids` | — | — | Integer floor on the `segment_wbody` COMID count (gfv2/gfv2_dev: 30000 against 48,529 measured; oregon: 500 against 770). Below it the pipeline **raises** — a `segments_gpkg` mis-wired to another fabric would otherwise match ~0 waterbodies, make every waterbody depression storage, and exit 0. Enforced at BOTH the producing `segment_wbody` builder (fresh-build and output-exists skip paths) and the consuming `wbody_connectivity` (which is what covers `--from wbody_connectivity`, a recipe that skips `segment_wbody` entirely — same both-ends contract as `min_endorheic_comids` below). Optional; `tjc` omits it. |
| `connected_comids_table` | — | — | Path to `input/nhd/connected_waterbody_comids.parquet` — the set of NHDPlusV2 waterbody COMIDs that a **Network** NHD artificial path flows through (i.e. on-stream via `WBAREACOMI`). Produced by `download/nhd_flowlines.py`, which keeps a WBAREACOMI only if the flowline carrying it is a Network Flowline (in `flowline_topology.parquet`), so Non-Network artificial paths NHD draws through closed-basin lakes don't promote endorheic waterbodies on-stream (issue #161). **Opt-in comparison only** — commented out on every fabric profile. `wbody_connectivity` no longer needs it (the `segment_wbody` COMID table is the required primary on-stream source); if present, it is UNIONED into the segment-derived set for an A/B and logged as a `COMPARISON MODE` warning, because that union is not the production classifier. |
| `flowthrough_comids_table` | — | — | Path to `input/nhd/flowthrough_waterbody_comids.parquet` — a second on-stream COMID set from flow-through topology: waterbodies that a **Network** conveyance flowline demonstrably enters AND exits (T1), or whose upstream end is inside the waterbody per authoritative NHDPlus routed-network direction (D1 — source/headwater lakes and split pass-through outflows), or that overlap an NHDArea conveyance polygon (T3). T1/D1 candidate flowlines are gated to Network Flowlines (in `flowline_topology.parquet`) so Non-Network closed-basin lines can't promote endorheic lakes (issue #161). Playa/Ice Mass waterbodies are dropped up front and never promoted onto the on-stream set (Playa because it's force-dprst; Ice Mass because it's excluded from the waterbody classification entirely — see the `waterbody` row below). Produced by `download/nhd_flowthrough.py`. **Opt-in comparison only**, same as `connected_comids_table` — commented out on every fabric profile; unioned into the segment-derived set (which also re-applies the `NEVER_ONSTREAM_FTYPES` guardrail to the unioned set) only when an operator deliberately configures it for an A/B, logging the same `COMPARISON MODE` warning. |
| `waterbody_gpkg` | — | ✓ | NHDPlus waterbodies; depstor's `waterbody` step **raises** if unset. If the layer has an `FTYPE` column, `waterbody` drops `EXCLUDE_WATERBODY_FTYPES` (`{"Ice Mass"}`) before rasterizing: a glacier/permanent ice mass is not depression storage, so its cells are left out of `wbody_binary`/`wbody_regions` entirely and fall back to land (perv/imperv via LULC), not dprst and not on-stream. Playa is unaffected here — it stays a normal waterbody clump and is force-dprst downstream by the `NEVER_ONSTREAM_FTYPES` guardrail in `wbody_connectivity`/`nhd_flowthrough`. Every CONUS fabric (`gfv2`, `gfv2_dev`, `oregon`, `tjc`) points at the source-derived `input/nhd/nhd_waterbodies.gpkg` (layer `waterbodies`), staged by `gfv2_params.download.nhd_waterbodies` and converted from its verified `nhd_waterbodies.parquet` (the builders read an OGR gpkg layer; the geoparquet needs libduckdb, absent from the env). Same 448,124 COMIDs and schema (`GNIS_ID, GNIS_NAME, COMID, FTYPE, member_comid, area_sqkm, geometry`) as the retired hand-made `conus_waterbodies.gpkg`, but a fresher shoreline vintage (~2.2% total-area difference); `scripts/diagnose/verify_nhd_waterbodies.py` is the row-count/COMID-set/FTYPE/area diff against the retired layer, which stays on disk for A/B reference only (no profile points at it any more). |
| `waterbody_layer` | — | ✓ | Layer name inside `waterbody_gpkg` |
| `wesm_index` | — | ✓ | Path to `input/wesm/wesm_1m_footprints.gpkg` — pre-staged, 1m/QL1/QL2-qualifying USGS 3DEP WESM workunit footprints (a `project` column + geometry). Produced by `pixi run python -m gfv2_params.download.wesm` (issue #173). Consumed by the `dprst_depth` step's `topo.resolution_class` (best-available-topo tagging) and `tiling.group_by_tile` (1 m tile-key resolution); required for `dprst_depth`, not for any other depstor step. |
| `ecoregions_gpkg` | — | ✓ | Path to `input/ecoregions/us_eco_l3.gpkg` — EPA Level III Ecoregions (see `gfv2_params.download.epa_ecoregions`). Used by the `dprst_depth` step's per-ecoregion regional-fill donor pool (`dprst_depth.fill.fit_ecoregion_models`); every fabric profile with a depstor-configured `dprst_depth` step already stages it (also listed as a shared, reusable input in `README.md`'s Stage 0). |
| `wbd_huc12_table` | — | — | Path to `input/wbd/wbd_huc12.parquet` — the full WBD HUC12 layer. Both ends filter `HU_12_TYPE == 'C'` (closed basin): `download/wbd_huc12.py` stages only type-C rows, **and** the `endorheic` depstor builder re-applies the filter itself (a table with no `HU_12_TYPE` column raises), so pointing this at a genuine full WBD layer cannot flag every waterbody endorheic and empty the on-stream set. Optional: absent turns off Signal B (majority-inside-closed-HUC12) and the `endorheic` step still runs Signal A (FDR terminus-inside-itself) alone. Do **not** point this at `input/nhd/closed_huc12.gpkg` — that is an incomplete extract (23 type-C HUC12s in the Great Basin vs 141 in the full WBD). |
| `burn_add_waterbody_table` | — | — | Path to `input/nhd/burn_add_waterbodies.parquet` — the **sink-purpose subset** of NHDPlus's BurnAddWaterbody polygons (new depression AREA; 1,658 polygons / 721.9 km² CONUS-wide), unioned into the waterbody layer by the `waterbody` builder's `merge_burn_add`, **before** the `EXCLUDE_WATERBODY_FTYPES` (Ice Mass) filter runs, so a BurnAdd Ice Mass polygon is still excluded. Configured-but-missing fails loud (`FileNotFoundError`), never silently skipped. BurnAdd rows are never on-stream-promotable, but not because `NEVER_ONSTREAM_FTYPES` is applied to them — `wbody_connectivity`/`nhd_flowthrough` re-read the raw `waterbody_gpkg` from disk, never the merged frame this builder produces, so that guardrail is never evaluated against a BurnAdd row at all. Safety is structural instead: `merge_burn_add` asserts every BurnAdd COMID (NHDPlus `PolyID`) is negative, so it can never match a positive segment-derived/WBAREACOMI/flow-through COMID, and asserts no BurnAdd polygon lies within one rasterized cell diagonal (`cell_size * sqrt(2)`, passed in from the template raster) of an existing **on-stream** waterbody — a buffered spatial join, not plain vector intersection, because `clump_regions`' 8-connectivity can merge cells that never touch in vector space. The guard is restricted to on-stream neighbours (via `_load_onstream_comids`, the raw **pre-endorheic** `segment_wbody_comids` table `segment_wbody` writes — no `NEVER_ONSTREAM_FTYPES` subtraction, no endorheic subtraction, so it is a conservative superset of the FINAL on-stream mask `dprst` reads; that can only make the guard fire MORE, never less) because merging with an already-dprst neighbour is harmless — the clump simply stays dprst — whereas an on-stream neighbour would silently drag the BurnAdd depression out of dprst; measured against real CONUS data, 112 of 1,658 BurnAdd polygons genuinely overlap an existing waterbody, all 112 neighbouring an already-dprst waterbody and none on-stream, so the original unconditional guard aborted the whole CONUS build over a failure mode that doesn't occur. If the on-stream COMID table(s) aren't configured or not yet staged, `merge_burn_add` falls back to the old broad guard (raises on ANY overlap) rather than silently skipping the check. Optional, staged by `gfv2_params.download.nhd_burn_components` — which keeps only the rows whose `PurpCode` is a sink purpose (4 Playa / 5 closed lake / 8 closed lake) and drops the rest: **BurnAddWaterbody is not a sink layer**, it is every waterbody NHDPlus added to the DEM burn, and VPU 01 alone ships 702 NULL-`PurpCode` rows (503 on-network, including StreamRiver and CanalDitch FCodes) against **zero** sinks in its own `Sink.shp`. FTYPE comes from `FCODE`, not `PurpCode` (`PurpCode` 5 spans both Playa and SwampMarsh). |
| `sink_points_table` | — | — | Path to `input/nhd/sink_points.parquet` — NHDPlus `Sink.shp` (15,728 sinks CONUS-wide). **Intentionally unread: no builder consumes it.** It is threaded through the profile and `BuildContext` for provenance and for the BurnAddWaterbody linkage (`SOURCEFC`/`FEATUREID`), so the sink layer that explains those polygons is staged and discoverable alongside them. It is **not** a classifier signal and must not be wired up as one: the `endorheic` builder's Signal A deliberately reads the FDR grid (the same grid `routing` reads), not this lossy point shadow of it. Optional. |
| `min_endorheic_comids` | — | — | Integer floor on the number of FLAGGED endorheic COMIDs on this fabric (`gfv2`/`gfv2_dev`: 100). Below it — or if either signal flags nothing at all — the pipeline **raises**, because a collapsed or empty result makes the demotion a silent no-op and leaves the Great Salt Lake on-stream. Enforced in three places: the `endorheic` builder's fresh-build path, its output-exists skip path, and `wbody_connectivity` (the consuming end, which is what covers `--from wbody_connectivity`, a recipe that skips the `endorheic` step entirely). Optional, and deliberately absent on fabrics that legitimately have no closed basin (`tjc`, Texas-Gulf: 4 FDR code-0 cells, 0 endorheic waterbodies) — there an empty table is the correct result. |

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

### The `merged/` gap-fill convention (`fill_columns`)

`fill_columns` is **not** a fabric-profile key — it is declared per param
entry in `configs/zonal/zonal_params.yml` (`params:`),
`configs/depstor/depstor_params.yml` (`means:`/`ratios:`), and the flat
`configs/snarea/snarea_library.yml` (`snarea_curve`). `scripts/merge_and_fill_params.py`
(Stage 7 / RUNME Step 5) reads that declaration to decide, per param, which
columns it may KNN-fill. Two asymmetric guards, both in
`resolve_fill_plan`: a declared column absent from the CSV raises (a typo
would otherwise silently fill nothing); a param with no `fill_columns` that
is nonetheless missing an HRU row also raises (a missing row admits no
"not derivable" reading). A column that is present but **not** declared and
carries NaN cells only warns, naming the column and the NaN count — a NaN
cell can be a legitimate "not derivable" result (`cv_empirical` is derivable
for only ~42% of HRUs by design; `cv_subgrid` exists to rescue the rest).

#### `fabric_columns` — exact values, not interpolated ones

A param entry may also declare `fabric_columns`, a sibling of `fill_columns`
for columns whose value is an exact fact already on disk in the fabric gpkg
rather than something to interpolate:

```yaml
fabric_columns:
  hru_area:
    source: geometry   # or a fabric-GDF column name; `geometry` means geometry.area
    scale: 1.0         # multiplier, in the fabric CRS's units
```

`apply_fabric_columns` copies these into **synthesized (previously absent)
rows only** — existing rows keep their builder-computed value, so the
mechanism cannot shift a canonical product it was not meant to touch. Today
only `ssflux` declares one: `hru_area` is `geometry.area`, and the 77 gfv2
HRUs absent from `nhm_ssflux_params.csv` would otherwise land NaN (it is a
litho/slope *input*, so nobody declares it fillable) — or, under the retired
"fill everything but the ids" regime, be KNN-copied from a neighbour at up to
11,109× the true area.

Both halves of the spec are validated eagerly and raise: the CSV column in
`resolve_fill_plan`, the GDF `source` in `validate_fabric_sources` (before
any fill runs, so a typo cannot lie dormant until the day a row goes
missing). An id that the fabric cannot serve raises rather than warning, and
a fabric column still NaN on a synthesized row raises *before* the write —
`merged/<name>.csv` is read unconditionally by consumers, so a silent gap in
it has no downstream reader to catch it. A `fabric_columns` column is **not**
exempt from the undeclared-NaN warning: that census runs on the pre-append
frame, so every NaN it counts is in an existing row, which this mechanism
does not touch.

#### `derived_columns` — PRMS quantities computed at merge time

A zonal param entry may also declare `derived_columns`, which adds a column to
the merged frame by transforming one that is already there:

```yaml
derived_columns:
  hru_slope: { from: mean, transform: deg_to_fraction }
```

`transform` names a function in a **whitelist** in
`zonal_runners/merge.py` (`_TRANSFORMS`), not `getattr(gfv2_params.raster_ops,
name)`: a typo must raise, not silently resolve to some other module-level
function. `apply_derived_columns` runs in `run_merge`, after the per-batch
concat and before the CSV write, so **adding one needs no zonal re-run** — only
`--mode merge --param <name>`. The source column is kept, not consumed: it is
declared `prms.provenance`, and a reader needs it to check the derivation.

Today only `slope` declares one. `slope.vrt` is
`rd.TerrainAttribute(dem, "slope_degrees")`, so `mean` is degrees while PRMS
`hru_slope` is a decimal fraction rise/run (~57× for small angles); rather than
ship a footgun, the pipeline emits the PRMS quantity directly, reusing the same
`raster_ops.deg_to_fraction` that `ssflux.py:63` already applies. See
[Parameter index](parameter_index.md).

Note the ordering constraint with `fill_columns`: a derived column that is also
declared fillable (as `hru_slope` is) makes a **re-merge a prerequisite of the
next fill sweep** on any fabric whose CSV predates the declaration —
`resolve_fill_plan` raises on a declared column the file does not have. That is
deliberate: the alternative is a silent NaN in a PRMS parameter for exactly the
HRUs that were missing.

`merged/<name>.csv` is the single canonical, always-gap-filled per-HRU file
for every param that declares `fill_columns` — **consumers read
`merged/*.csv`**. The retired `filled_` prefix required a consumer to know,
per param *and* per fabric, which of two files was authoritative (the
canonical set differed between `gfv2`, 2 files, and `oregon`, 4); only
`viz.py` encoded that rule, and it is why four `oregon` params went unfilled
until someone audited them by hand. The pre-fill (raw) copy is preserved
once at `merged/_unfilled/<name>.csv` (`write_filled_in_place`, never
overwritten on a re-run — the on-disk `merged/<name>.csv` is by then already
filled), alongside the existing `merged/_intermediates/` per-fraction/derived
CSVs. A one-time migration off an existing `filled_`-prefixed product is
`scripts/migrate_filled_params.py` (dry-run by default, `--apply` required).

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
- **On-stream classification comes from the MODEL's own segment network, not
  NHD.** `segment_wbody` (`STEP_ORDER` position 3, before `waterbody`) promotes
  a waterbody to on-stream iff an `nsegment` from `segments_gpkg` intersects it
  with **positive length** — a zero-length shoreline graze does not count (3.1%
  of candidate pairs CONUS-wide). It writes
  `segment_waterbody_comids.parquet` (registered key `segment_wbody_comids`,
  schema `comid`/`n_segments`/`overlap_m`) and is cheap (42 s wall / 2.0 GB peak
  RSS at CONUS — 186,709 segments × 448,124 polygons — unlike the ~384 GB
  full-grid `waterbody`/`dprst` steps). This asks "is it on the network the
  model routes?", not "is it on the NHD network": NHD's network is far finer
  than the model's own segment network, so under the old NHD-driven classifier
  a waterbody that NHD routed but no model segment touched had no
  representation in the on-stream test at all. Deliberate consequences: a segment collinear
  with a shoreline promotes, and a segment **terminating inside** a waterbody
  promotes, so NHD's inflow-AND-outflow discrimination is gone and the
  `endorheic` subtraction (below) is what still demotes terminal lakes.
  `segment_wbody` is deliberately FTYPE-agnostic — the Playa/Ice Mass
  never-on-stream guardrail lives at the `wbody_connectivity` chokepoint so it
  applies to the opt-in NHD comparison sources too (see below). A
  `segments_gpkg` mis-wired to another fabric's segments would match ~0
  waterbodies and silently promote every waterbody to depression storage; two
  guards catch that: `_assert_overlaps_template` (extent check against the
  template grid) and the `min_onstream_comids` floor (see the per-key table).

  **The on-stream set has three consumers**, all using
  `segment_wbody_comids − endorheic_comids` (except where noted):
  1. `wbody_connectivity` — the **required primary** on-stream source; applies
     the endorheic subtraction and the Playa/Ice Mass guardrail.
  2. `waterbody`'s BurnAdd overlap guard (`_load_onstream_comids`) — reads the
     raw, **pre-endorheic** `segment_wbody_comids` (it runs before `endorheic`
     in `STEP_ORDER` and can't see the demotion). This is a deliberately
     conservative superset of the final on-stream mask, so the guard can only
     fire MORE often than necessary, never less — the safe direction.
  3. `dprst_depth` — reconstructs the dprst polygon set via
     `topo.load_fabric_dprst_polygons(onstream_comids=segment_wbody_comids −
     endorheic_comids)`, on both the in-process `build()` path and the SLURM
     `--plan` path (`tiling.py`), so it computes depths for the SAME polygon
     set `dprst_binary.tif` uses rather than a divergent, independently
     NHD-derived one (a ~769-waterbody divergence measured on `oregon` before
     this fix). This also closes a pre-existing gap: `topo.py` never
     subtracted the endorheic set either, so the Great Salt Lake used to be
     excluded from `dprst_depth`'s polygon set while `dprst_binary.tif`
     included it.

  **NHD flowline topology is retained as an opt-in comparison union**, not the
  production classifier: `connected_comids_table` (WBAREACOMI artificial-path
  topology, staged by `download/nhd_flowlines.py`) and `flowthrough_comids_table`
  (flow-through topology, staged by `download/nhd_flowthrough.py`) are commented
  out of every fabric profile. If either is configured, `wbody_connectivity`
  unions it into the segment-derived set and logs a `COMPARISON MODE` warning,
  because that union is not the production definition of on-stream. **Both
  staging steps still gate on-stream promotion on Network-Flowline
  membership** — a COMID present in `flowline_topology.parquet` (NHDPlus
  PlusFlowlineVAA) — because NHD draws Non-Network artificial paths through
  essentially every closed-basin lake, so the ungated WBAREACOMI set and the
  ungated geometric T1 test both wrongly promoted genuinely endorheic
  waterbodies on-stream (issue #161). This makes `nhd_topology` a prerequisite
  of **both** `nhd_flowlines` and `nhd_flowthrough` (each fails loud if the
  topology parquet is missing) — but, because the two are opt-in, that ordering
  constraint now applies only to the comparison path; a normal depstor run
  stages none of the three. A waterbody is flow-through if a **Network**
  conveyance flowline enters AND exits it (T1), or if a routed-network
  conveyance flowline's upstream end is inside it (D1 — authoritative NHDPlus
  direction from `flowline_topology.parquet`, staged by `download/nhd_topology.py`;
  this catches source/headwater lakes and split-pass-through outflows and
  replaced the old `FLOWDIR`-gated T2), or if it overlaps an NHDArea conveyance
  polygon (T3). `nhd_flowthrough` defines `FORCE_DPRST_FTYPES = {"Playa"}`
  (always depression storage, never promoted on-stream) and
  `EXCLUDE_WATERBODY_FTYPES = {"Ice Mass"}` (not depression storage either — a
  glacier is excluded from the depstor waterbody classification entirely and
  falls back to land/LULC), unioned into `NEVER_ONSTREAM_FTYPES`. Both are
  dropped up front in `flowthrough_comids` and never promoted;
  `wbody_connectivity` re-applies `NEVER_ONSTREAM_FTYPES` to the FINAL unioned
  set regardless of source, so a Playa/Ice Mass waterbody promoted via a segment
  or via WBAREACOMI is excluded either way (Ice Mass is belt-and-suspenders here
  — it's already removed upstream at the `waterbody` builder; see the
  `waterbody_gpkg` row above). The `dprst` and downstream builders are unchanged
  consumers of `wbody_connectivity`'s output either way — they see whichever
  on-stream set was computed with no code change on their side.
- **`flowline_topology.parquet`** — distilled NHDPlus PlusFlowlineVAA (COMID,
  DnHydroseq, Hydroseq, TerminalFl, StartFlag, StreamOrde, FromNode, ToNode). Staged by
  `download/nhd_topology.py`; consumed by **both** `download/nhd_flowlines.py`
  (the Network-Flowline gate on WBAREACOMI) and `download/nhd_flowthrough.py`
  (the Network-Flowline gate on T1/D1 candidates + the D1 routed-network outflow
  rule). Hardcoded data_root-relative, no config key — `nhd_topology.py` must
  run before **both** `nhd_flowlines.py` and `nhd_flowthrough.py` (each fails
  loud if `input/nhd/flowline_topology.parquet` is missing).
- **`nhd_waterbodies` (the CONUS waterbody source, wired everywhere) and the
  `member_comid` provenance.** `download/nhd_waterbodies.py` stages NHDWaterbody
  polygons from the same per-VPU `NHDSnapshot` archive `nhd_flowlines` already
  downloads, reproducing the retired hand-made `input/nhd/conus_waterbodies.gpkg`.
  Every CONUS-scale fabric's `waterbody_gpkg` now points at the source-derived
  `input/nhd/nhd_waterbodies.gpkg` instead (see the `waterbody_gpkg` row above);
  `conus_waterbodies.gpkg` stays on disk only for A/B reference via the verify
  script below, no profile reads it. Verified via
  `scripts/diagnose/verify_nhd_waterbodies.py` against the real CONUS layer:
  **exact match** on row count (448,124), unique-COMID count (447,907, incl.
  the same 217 residual duplicate-COMID rows), the full COMID set (0 only on
  either side), and the FTYPE distribution (incl. 66,488 SwampMarsh). Total
  area differs by ~2.2% (393,635 km2 staged vs 402,554 km2 existing),
  concentrated in ~1,000 of the largest waterbodies (Lake Michigan/Superior/
  Huron alone account for ~834 km2 of the gap) — consistent with the hand-made
  layer having been built from an older NHDSnapshot vintage than the highest
  version `_pick_snapshot_key` resolves today (NHD periodically revises
  shoreline vertices for a stable COMID), not a code defect.

  `member_comid` is NOT a native NHDWaterbody field: reverse-engineered from
  the hand-made layer, it is `str(COMID)` for 447,844 of 448,124 rows, and for
  the other 280 a **sorted comma list of raw COMIDs** dissolved into one output
  polygon. **The merge rule is: every NHDWaterbody row sharing a non-null
  GNIS_ID, WITHIN ONE VPU, is always dissolved into one row — there is no
  spatial-adjacency test.** This was verified, not assumed: Lake Conroe (VPU
  12, GNIS_ID 1380953, COMIDs 1466730/120053033) merges despite its two parts
  being 662.8 m apart (`touches=False`, `intersects=False`), disproving a
  touching/intersecting requirement. The retained `COMID`/`GNIS_NAME`/`FTYPE`
  come from the **largest-area member** (also true when `FTYPE` disagrees —
  Lake Oahe, GNIS_ID 1266878, two small LakePond parts + a 1,254.6 km2
  Reservoir part, resolves to `Reservoir`, matching the existing layer).
  `member_comid` is functionally near-inert downstream:
  `select_connected_waterbodies` calls `pd.to_numeric(member_comid,
  errors="coerce")`, turning every comma-list to NaN, so a merged row is only
  ever matched via its single `COMID`.

  **Why per-VPU, never cross-VPU:** the existing layer's 14 residual
  same-GNIS_ID pairs that stay unmerged are — every one checked — a case that
  spans TWO different VPU archives (either the exact same COMID, cross-VPU
  duplicated, e.g. GNIS_ID 178159 "Saint Vrain Glaciers" at the VPU 10L/14
  seam; or a named feature split into two different COMIDs at a VPU boundary,
  e.g. GNIS_ID 1564644 "Empire Swamp" at the VPU 04/07 seam). `main()`
  reproduces this by dissolving each VPU's frame independently before
  concatenating, never comparing polygons across archives — which also means
  it does not collapse NHDPlus's 218 known cross-VPU-duplicate COMIDs (VPU
  04/07 and 12/13 seams) into one row; `dedupe_cross_vpu_duplicates` exists,
  tested, to do that, but `main()` deliberately does not call it, since the
  layer being reproduced doesn't either. See the module docstring for the full
  derivation.
- **`endorheic` step (runs between `waterbody` and `wbody_connectivity`).**
  Emits `endorheic_waterbody_comids.parquet` (comid, frac_own, by_terminus,
  by_closed_huc12) via `endorheic_frame` (`src/gfv2_params/endorheic.py`):
  Signal A is a waterbody whose D8 terminus (on the same FDR grid `routing`
  reads) lies INSIDE itself; Signal B is a waterbody majority-inside a closed
  (type-C) WBD HUC12, needed because some closed-basin waterbodies (e.g.
  Walker Lake) contain no FDR terminal cell.

  Two counts, easy to confuse: a COMID is **flagged** when a signal calls it
  endorheic (**22,942** on CONUS — this is what `min_endorheic_comids` floors
  and what `endorheic_wbody.tif` rasterizes), and **demoted** when a flagged
  COMID was *also* on-stream, so `wbody_connectivity`'s subtraction actually
  removed it (**818** on CONUS). Most flagged COMIDs were never on-stream, so
  the subtraction is a no-op for them.

  On the shipped CONUS tables Signal
  B is not a minor complement: of the 818 demotions, 543 are Signal-B-only,
  112 Signal-A-only, 163 both — BY COUNT Signal B dominates. BY AREA it does
  not: Signal-B-only demotions are small (median ~0.09 km², ~1,400 km² total,
  mostly ponds/playas inside a closed basin), while Signal A carries the
  overwhelming majority of the demoted area, including the Great Salt Lake
  itself (4,369 km²). Signal A needs only `fdr_raster` (already required on
  every fabric) and runs everywhere; Signal B activates only when
  `wbd_huc12_table` is configured (and the builder re-applies the type-C
  filter itself). The emitted table also carries every Signal-A-EVALUATED
  candidate that was NOT flagged (both `by_terminus`/`by_closed_huc12` false),
  not only demotions, so a threshold sweep over `frac_own` (see
  `scripts/diagnose/endorheic_fixtures.py`) measures the real candidate
  distribution instead of only the rows a 0.5 threshold already flagged;
  `load_endorheic_comids` still filters to flagged rows only, so this does not
  change which COMIDs get demoted. An EMPTY (zero-FLAGGED) result is
  legitimate — a domain with no closed basin has no endorheic waterbody
  (`tjc`, Texas-Gulf: 4 FDR code-0 cells, 0 flagged; against 15,262 /
  thousands on `gfv2` and 1,438 / 680 on `oregon`) — so `wbody_connectivity`
  subtracts the empty (flagged) set, a correct no-op. What fails loud is
  BREAKAGE: a waterbody layer that doesn't overlap the FDR grid, an all-null
  geometry set, a staged WBD with zero type-C rows when Signal B is configured,
  and a flagged-row count below the fabric's optional `min_endorheic_comids`
  floor — so a silently-empty CONUS result, which would leave the Great Salt
  Lake on-stream, is still impossible to miss. That floor is checked in **three**
  places, and all three are load-bearing: on the `endorheic` builder's fresh-build
  path, on its output-exists skip path, and again at the CONSUMING end in
  `wbody_connectivity`. The last is not redundant — `--from wbody_connectivity`
  (the documented cascade-rebuild recipe) does not run the `endorheic` step at
  all, so the orchestrator hydrates its table straight off disk and the producing
  builder's checks never execute. The floor is applied per SIGNAL as well as to
  the union, because the union alone cannot see one signal die: Signal B dominates
  by COUNT (543 of 818 CONUS demotions) while Signal A carries almost all the
  demoted AREA, so a total Signal-A collapse would still clear a count-based floor
  while ~75% of the demoted area silently vanished.
  `wbody_connectivity` subtracts this COMID set from the on-stream set — a
  STRICT SUBTRACTION, so it can only remove COMIDs, never add one — which is
  what finally takes the Great Salt Lake off-stream: a model `nsegment`
  intersects it (so `segment_wbody` promotes it, same as the old WBAREACOMI and
  flow-through sources under the NHD comparison path, which also promote it
  because NHD draws Network artificial paths between its arms), and the
  segment-driven rule promotes on intersection alone with no inflow/outflow
  test — so the endorheic subtraction is the ONLY thing that still demotes it.
  If `endorheic_comids` is absent from the build context
  (the `endorheic` step hasn't run for this fabric), `wbody_connectivity`
  **raises** rather than proceeding without the demotion — every fabric that
  can reach `wbody_connectivity` has both a COMID-keyed waterbody layer and
  `fdr_raster`, so it can always run the `endorheic` step first; a *present but
  empty* endorheic table (the `tjc` case above) is unaffected and stays a
  legitimate no-op. See
  [`docs/superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md`](superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md).
- **`wbody_connectivity` emits a SECOND raster, `endorheic_wbody.tif`** — every
  waterbody the `endorheic` classifier flagged (Signal A and/or B), rasterized
  from the FULL endorheic set regardless of on-stream status (not just the ones
  the subtraction above demoted). `dprst.py` needs this because the demotion
  alone doesn't fix the CONUS product: `clump_regions` labels 8-connected
  waterbody components, and `regions_touching_mask` excludes a WHOLE region if
  any one cell touches the on-stream mask. The Great Salt Lake (4,369 km²,
  correctly demoted to dprst by `endorheic`) is 8-connected to a 49.1 km²
  SwampMarsh (COMID 10273192) whose water flows INTO the lake and is correctly
  left on-stream — so the region-level exclusion vetoed the entire merged
  region, silently excluding all 4,854,156 Great Salt Lake cells from
  depression storage even though `connected_wbody.tif` no longer contains it.
  The fix, in `dprst.py`, exempts a waterbody's own cells from the
  region-level exclusion wherever `endorheic_wbody == 1 AND connected_wbody !=
  1 AND wbody_binary == 1` — i.e. direct hydrologic evidence
  (terminus-inside-itself) overrides the clump proxy, but only for the
  waterbody's own (not itself on-stream) cells (the marsh's own cells stay
  excluded because they ARE on-stream), and only where `waterbody` already calls
  the cell a waterbody. That third term matters: `endorheic_wbody` is rasterized
  from a raw, unfiltered read of the gpkg, so without it the 2 Mt Shasta Ice Mass
  COMIDs that `waterbody` deliberately excluded would be reinstated as depression
  storage, breaking `dprst ⊆ wbody_binary`. This runs **before** the impervious
  carve and land mask so both still apply to recovered cells (the
  imperv/dprst/perv partition stays disjoint). It is intentionally narrower than
  a global per-cell on-stream carve — dropping the `endorheic_wbody` term alone
  *is* that carve, and it would recover a further ~7,403 km² (gfv2, measured 2026-08-04; the figure moves with each dprst cascade rebuild — 9,177 km² on the segment-driven gfv2_dev — so re-measure rather than trusting a quoted value) of non-endorheic
  waterbodies whose clump merely abuts an on-stream feature (reproduce with
  [`scripts/diagnose/measure_global_carve.py`](../scripts/diagnose/measure_global_carve.py));
  those must keep the
  unexempted clump behaviour exactly, which is what the `drains_to_dprst`
  over-extension #145/#158/#161 fixed. `endorheic_wbody` is **required** by
  `dprst`, not optional: `wbody_connectivity` always writes it alongside
  `connected_wbody`, so a build context with one and not the other is always a
  stale output directory, never a legitimate configuration. Treating it as
  "exemption off" is how a `--from dprst --force` rebuild against a pre-classifier
  output directory would silently re-emit the old product; `dprst` raises instead.
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
   For **depstor** this is mandatory and enforced: `build_depstor_rasters.py`
   raises if a `STEP_ORDER` step has no block in `depstor_rasters.yml`, and
   `tests/test_expected_outputs.py` asserts the two agree. (It used to be
   silent — a registered-but-unconfigured step was skipped, and
   `_hydrate_existing_outputs` then served the previous run's artifact for its
   output key, so a `--from` rebuild re-emitted stale CONUS product at exit 0.)
   **`shared_rasters` is deliberately different**: its config omits the opt-in
   `compute_dem_derivatives` and `compute_breached_fdr` steps, so its
   orchestrator tolerates registered-but-unconfigured steps by design. Don't
   copy the depstor guard there.
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
