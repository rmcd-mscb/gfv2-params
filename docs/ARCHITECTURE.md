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
| `connected_comids_table` | — | ✓ | Path to `input/nhd/connected_waterbody_comids.parquet` — the set of NHDPlusV2 waterbody COMIDs that an NHD artificial path flows through (i.e. on-stream via `WBAREACOMI`). Produced by `download/nhd_flowlines.py`; consumed by the depstor `wbody_connectivity` builder. Required only for fabrics whose waterbody layer is COMID-keyed (`gfv2`, `oregon`, `tjc`); the `gfv2_vpu01` profile omits it (its `wbs` layer has no COMID), so `wbody_connectivity`/`dprst` fail-fast there — use `gfv2` for depstor validation. |
| `flowthrough_comids_table` | — | — | Path to `input/nhd/flowthrough_waterbody_comids.parquet` — a second on-stream COMID set from geometric flow-through topology: waterbodies that a conveyance flowline demonstrably enters AND exits (T1/T2) or that overlap an NHDArea conveyance polygon (T3). Playa/Ice Mass waterbodies are never promoted. Produced by `download/nhd_flowthrough.py`; unioned with `connected_comids_table` by `wbody_connectivity` before rasterizing. Optional (omitting it uses `connected_comids_table` only). |
| `waterbody_gpkg` | — | ✓ | NHDPlus waterbodies; depstor's `waterbody` step **raises** if unset |
| `waterbody_layer` | — | ✓ | Layer name inside `waterbody_gpkg` |

For `template_raster`/`fdr_raster`, stage the clip with:

```bash
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric <name>
# writes {data_root}/<name>/shared/<name>_fdr.vrt
```

Every depstor builder sizes its arrays to the `template_raster` grid, so the
clip scopes compute to the fabric extent while staying VPU-agnostic (works
for fabrics that straddle VPU boundaries).

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
- **Land masking.** Every depstor raster is masked against `land_mask.tif`
  (the HRU fabric rasterised by the `landmask` step). Never use hydro-DEM
  nodata or FDR as a land mask.
- **WhiteboxTools cannot read LZW + `predictor=2` GeoTIFFs** — it silently
  corrupts them. Never pass `predictor=2` rasters to WBT subprocesses.
- **The elevation mosaic (`elevation`/`slope`/`aspect`) is Cloud-Optimized.**
  `compute_slope_aspect` writes the `_fixed_` elevation, slope, and aspect
  tiles as COGs (tiled 512, internal overviews, ZSTD + `PREDICTOR=3`) via the
  shared `shared_rasters/cog.py` helper, and `build_vrt` adds an external
  `.vrt.ovr` overview pyramid to each CONUS VRT. This serves both consumers —
  fast continental QGIS pan/zoom and fast windowed reads for zonal
  stats/resampling (exactextract/gdptools/rioxarray). **WBT-safety boundary:**
  `to_cog` (ZSTD + predictor) is only for these GDAL/rasterio/QGIS-consumed
  rasters. WBT-fed rasters — the `Hydrodem` fixed/filled DEMs in
  `compute_dem_derivatives` and the depstor routing FDR — must stay
  LZW-without-predictor and are deliberately left on their existing write paths
  (see the predictor gotcha above). Aspect uses **nearest** overview resampling
  (circular 0/360 field); FDR's VRT overview is nearest (categorical D8 codes);
  continuous surfaces use bilinear.
- **CONUS-scale memory: stream/window, never hold a full-grid array.** The
  CONUS template is ~16.9 B cells (~17 GB uint8, ~68 GB int32, ~135 GB
  float64); whole-grid ops OOM the 503 GB node ceiling. `routing` tiles the
  in-process D8 routing pass per VPU (it runs after `vpu_id`, routes each VPU in
  isolation, and mosaics); reproject with streaming `gdal.Warp`, not in-memory
  `rioxarray.reproject_match`; window per `STRIP_ROWS` like `carea_map`. See
  CLAUDE.md for the full gotcha.
- **On-stream classification is the union of two COMID sources.** The
  `wbody_connectivity` builder loads both `connected_waterbody_comids.parquet`
  (WBAREACOMI artificial-path topology, staged by `download/nhd_flowlines.py`)
  and `flowthrough_waterbody_comids.parquet` (geometric flow-through topology,
  staged by `download/nhd_flowthrough.py`) and unions them before rasterizing.
  A waterbody is flow-through if a conveyance flowline enters AND exits it
  (T1/T2 tests) or if it overlaps an NHDArea conveyance polygon (T3). Playa
  and Ice Mass waterbodies are force-excluded from flow-through promotion and
  remain dprst regardless. The `dprst` and downstream builders are unchanged
  consumers — they see a larger on-stream set with no code change.
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
