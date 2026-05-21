# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PRMS/NHM hydrologic-model parameter generation. Given a watershed fabric of HRU
polygons, the pipeline computes parameters by running zonal statistics against
CONUS source rasters (DEM, soils, lithology, LULC, depression-storage). It runs
on a USGS HPC cluster under SLURM; production runs are CONUS-scale.

## Environment & commands

Environment is managed by **pixi** (config in `pyproject.toml` `[tool.pixi.*]`,
pinned in `pixi.lock`). The legacy `environment.yml`/`geoenv` conda env is a
deprecated fallback — do not add to it.

```bash
pixi install                     # materialise .pixi/envs/default from pixi.lock
pixi shell -e dev                # default + pytest, ruff, pre-commit
pixi run python scripts/foo.py   # one-off command in the default env
pixi run -e dev pytest tests/ -v # run the test suite
pixi run -e dev pytest tests/test_config.py::test_resolve_vpu_standard -v  # single test
```

After editing `pyproject.toml` or `pixi.lock`, re-run `pixi install`.

**SLURM batches invoke `pixi run --as-is`** (= `--no-install --frozen`): the
already-installed env is used verbatim with no lock check or env mutation, so
concurrent array tasks don't race on `.pixi/envs/.../conda-meta`. Never change
batches to a flow that mutates the env per task. The `pixi` binary must be on
`PATH` at submit time (SLURM inherits the submitting shell), so always `sbatch`
from a shell where `~/.pixi/bin` is on `PATH`.

Lint/format runs via pre-commit: `pixi run -e dev pre-commit run --all-files`.

### Testing on the HPC head node

**Do not run `pytest` on the HPC login/head node.** Concurrent geo-library
imports (rasterio/GDAL/PROJ/pyogrio) trigger shared-FS metadata import storms
that can hang. The authoritative test gate is CI (`.github/workflows/ci.yml`),
which runs `pytest tests/` on push to `main` and every PR. Quick `py_compile`
or import checks on the head node are fine.

## Architecture

The pipeline splits into **Part 1 (fabric-independent)** and **Part 2
(fabric-dependent)**, both reading from a single on-disk `data_root` set in
`configs/base_config.yml`. `slurm_batch/RUNME.md` is the authoritative,
step-by-step HPC workflow; `README.md` covers the design notes.

**Layout of `data_root`** (the key invariant): `input/` (manually staged or
downloaded external data) → `shared/` (fabric-INDEPENDENT CONUS intermediates:
per-VPU merged GeoTIFFs, CONUS VRTs, derived rasters — reused by every fabric) →
`{fabric}/` (per-fabric outputs: batches, depstor_rasters, params). Every fabric
reuses the same `shared/` rasters; per-VPU iteration happens inside builders, not
in per-VPU sbatch launches.

### Orchestrator + builder-module pattern

Each pipeline stage is **one orchestrator script + one unified YAML config +
a package of per-step builder modules**. The orchestrators walk a step DAG and
dispatch into library functions; the SLURM `*.batch` wrappers and any
per-script CLIs are thin shells around the same builders. The four stages:

| Stage | Orchestrator | Config | Builders |
|---|---|---|---|
| Part 1 shared rasters | `scripts/build_shared_rasters.py` | `configs/shared_rasters/shared_rasters.yml` | `src/gfv2_params/shared_rasters/` |
| Part 2a depstor rasters | `scripts/build_depstor_rasters.py` | `configs/depstor/depstor_rasters.yml` | `src/gfv2_params/depstor_builders/` |
| Part 2a depstor params | `scripts/derive_depstor_params.py` | `configs/depstor/depstor_params.yml` | `src/gfv2_params/depstor_ratios.py` |
| Part 2b zonal params | `scripts/derive_zonal_params.py` | `configs/zonal/zonal_params.yml` | `src/gfv2_params/zonal_runners.py` |

Orchestrators support `--step <name>` (one step) / `--from <name>` (resume) /
`--force`, plus per-batch debugging modes (`--mode zonal|merge|build_weights`).
SLURM submission wrappers (`slurm_batch/submit_*.sh`) chain array jobs → merges →
ratios via `afterok`.

When adding a pipeline step, follow this pattern: add a builder module, register
it in the orchestrator's step DAG, and add its config block — don't add a new
standalone script or a new YAML file.

### Fabric profiles — the single source of truth

`configs/base_config.yml` holds the `data_root` and a `fabrics:` mapping of
profiles. **Every shared, required per-fabric input lives in its profile** —
never a required CLI arg and never inferred from a naming convention. Scripts
read them via `require_config_key(config, key, script_name)` from
`src/gfv2_params/config.py` (which also does fabric-profile resolution and
`{data_root}`/`{fabric}`/`{vpu}`/`{raster_vpu}` placeholder substitution).
Per-step configs are fabric-agnostic templates resolved at runtime.

Active fabric resolution (highest precedence first): `--fabric <name>` CLI flag
→ `FABRIC` env var (via sbatch) → `default_fabric` in base_config (currently
`gfv2`). Register a new fabric with `pixi run init-data-root --add-fabric <name>`
then fill the stub's TODO keys. Required keys: `hru_gpkg`/`hru_layer`,
`id_feature`, `expected_max_hru_id`, `batch_size`; depstor fabrics also need
`template_raster`, `fdr_raster`, `twi_raster`, `segments_gpkg`/`segments_layer`,
and `waterbody_gpkg`/`waterbody_layer` (waterbody is required — the step raises
if unset).

`gfv2_vpu01` is the standard small-scale validation target; `oregon` is the
current regional test fabric.

## Non-obvious conventions & gotchas

These are hard-won; violating them silently corrupts outputs.

- **Depstor template/fdr come from a fabric-bounds clip** of `fdr.vrt`
  (`scripts/clip_shared_to_fabric.py`), not CONUS VRTs or per-VPU tiles. The
  clip must come from the hydrology lattice (`fdr.vrt`/`twi.vrt`); `elevation.vrt`
  is on the offset DEM lattice and `carea_map` requires `template ≡ twi`
  alignment.
- **Land masking.** Every depstor raster is masked against `land_mask.tif` (HRU
  fabric rasterised by the `landmask` step). Never use hydro-DEM nodata or FDR
  as a land mask.
- **WhiteboxTools cannot read LZW + `predictor=2` GeoTIFFs** — it silently
  corrupts them. Never pass `predictor=2` rasters to WBT subprocesses.

## Working in this repo

- **Every code change needs a docs check.** Audit `docs/`, `README.md`, and
  `slurm_batch/RUNME.md`; update them on the same branch and surface findings
  before merge.
- **Atomic commits.** Split combined fixes into separate commits before pushing.
  If source changes exceed the original plan, lead the PR description with a
  scope-expansion callout.
- `_gfv2_params_legacy/`, `_create_lulc_params_legacy.py`, and `*_legacy` paths
  are retained for reference only — don't extend them.

### Code conventions

Repo-specific rules — uphold these when writing or reviewing code here:

- **Add a builder + a test together.** A new pipeline step is a builder module
  plus its DAG registration plus a config block, *and* a `tests/test_<builder>.py`
  (most builders have one — match the nearest existing test for style). Don't add
  a standalone script or YAML.
- **Paths and fabric inputs come from the profile, never hardcoded.** Read them
  with `require_config_key(...)` against the active fabric profile in
  `configs/base_config.yml`; use the `{data_root}`/`{fabric}`/`{vpu}`
  placeholders rather than literal paths.
- **Run `pixi run -e dev pre-commit run --all-files` before pushing.** CI is the
  test gate (not the head node) — open the PR and let it run `pytest`.
- **Add deps via `pyproject.toml`** (see its comment block for the conda-forge
  vs. pypi split), then `pixi install` — not the deprecated `environment.yml`.
