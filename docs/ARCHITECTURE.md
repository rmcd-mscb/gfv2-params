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
| `segments_gpkg` | — | ✓ | Stream segments for the depstor `streambuffer` step. A pre-merged fabric can point at `hru_gpkg`; a VPU-based fabric (gfv2) merges the per-VPU `nsegment` layers into one CONUS gpkg via `scripts/merge_vpu_segments.py` |
| `segments_layer` | — | ✓ | Layer name inside `segments_gpkg` (typically `nsegment`) |
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
- **Land masking.** Every depstor raster is masked against `land_mask.tif`
  (the HRU fabric rasterised by the `landmask` step). Never use hydro-DEM
  nodata or FDR as a land mask.
- **WhiteboxTools cannot read LZW + `predictor=2` GeoTIFFs** — it silently
  corrupts them. Never pass `predictor=2` rasters to WBT subprocesses.
- **CONUS-scale memory: stream/window, never hold a full-grid array.** The
  CONUS template is ~16.9 B cells (~17 GB uint8, ~68 GB int32, ~135 GB
  float64); whole-grid ops OOM the 503 GB node ceiling. `routing` tiles WBT
  Watershed per VPU (it runs after `vpu_id`, routes each VPU in isolation, and
  mosaics); reproject with streaming `gdal.Warp`, not in-memory
  `rioxarray.reproject_match`; window per `STRIP_ROWS` like `carea_map`. See
  CLAUDE.md for the full gotcha.
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
- [`slurm_batch/RUNME.md`](../slurm_batch/RUNME.md) — authoritative HPC workflow walkthrough
- [`docs/superpowers/INDEX.md`](superpowers/INDEX.md) — index of design specs, implementation plans, and reviews
- [`docs/depstor_workflow.md`](depstor_workflow.md), [`docs/depstor_port_summary.md`](depstor_port_summary.md), [`docs/depstor_vpu01_validation_results.md`](depstor_vpu01_validation_results.md) — depstor pipeline reference (historical and current)
