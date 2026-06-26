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

Local docs preview: `pixi run -e docs docs-serve` (live-reload on
`localhost:8000`); `pixi run -e docs docs-build` renders the static site
to `./site/`. Configuration: [`mkdocs.yml`](mkdocs.yml).

### Testing on the HPC head node

**Do not run `pytest` on the HPC login/head node.** Concurrent geo-library
imports (rasterio/GDAL/PROJ/pyogrio) trigger shared-FS metadata import storms
that can hang. The authoritative test gate is CI (`.github/workflows/ci.yml`),
which runs `pytest tests/` on push to `main` and every PR. Quick `py_compile`
or import checks on the head node are fine.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the canonical source
covering: data-root layout (`input/` → `shared/` → `{fabric}/`), Part 1 vs
Part 2 split, the **orchestrator + builder + unified-config pattern** for the
4 pipeline stages, fabric profiles as the single source of truth (with the
per-key required-field table), and how to add a new pipeline step.

`slurm_batch/RUNME.md` is the step-by-step runbook (the CONUS-gfv2 happy path); `slurm_batch/HPC_REFERENCE.md` holds the per-stage detail, alternate paths, and recovery;
`README.md` covers user-facing setup and usage.

## Non-obvious conventions & gotchas

These are hard-won; violating them silently corrupts outputs.

- **The dprst/on-stream split is driven by the UNION of two COMID sources.**
  `wbody_connectivity` unions `connected_waterbody_comids.parquet` (WBAREACOMI
  artificial-path topology, from `nhd_flowlines`) with
  `flowthrough_waterbody_comids.parquet` (geometric flow-through topology, from
  `nhd_flowthrough`). A waterbody promoted by the flow-through test must have
  both inflow and outflow — terminal sinks (inflow only), locally-spilling
  potholes (outflow only), and isolated depressions stay dprst. Playa and Ice
  Mass FTYPEs are a hard guardrail and are never promoted regardless. If
  `drains_to_dprst` over-extends into humid open-drainage basins, fix the
  **classifier** (which waterbodies are on-stream) — never add a cap or tuning
  knob to routing. A cap cannot distinguish a legitimately large endorheic basin
  from a spurious one and damages the correct cases.
- **Depstor template/fdr come from a fabric-bounds clip** of `fdr.vrt`
  (`scripts/clip_shared_to_fabric.py`), not CONUS VRTs or per-VPU tiles. The
  clip must come from the hydrology lattice (`fdr.vrt`/`twi.vrt`); `elevation.vrt`
  is on the offset DEM lattice and `carea_map` requires `template ≡ twi`
  alignment.
- **Land masking.** Every depstor raster is masked against `land_mask.tif` (HRU
  fabric rasterised by the `landmask` step). Never use hydro-DEM nodata or FDR
  as a land mask.
- **Impervious is carved from dprst per-cell, never whole-region.** A waterbody
  clump is depression storage unless it is *on-stream* (touches the NHD-connected
  mask); impervious cells are masked out of `dprst` cell-by-cell in
  `depstor_builders/dprst.py` (restoring the ArcPy `getDprst` "outside of
  impervious zones" behavior). Do NOT restore an imperv `regions_touching_mask`
  exclusion — one impervious pixel would then drop a whole multi-km² waterbody
  (a regression that falsely excluded ~16,800 km² CONUS-wide). The
  imperv/dprst/perv cell partition must stay disjoint (no double-count). The
  `imperv` 50% threshold (`VALUE > 50`) is a land-classification lever (which
  NLCD cells are impervious), decoupled from dprst exclusion by the per-cell
  carve — it is **not** a knob for limiting over-exclusion.
- **WhiteboxTools cannot read LZW + `predictor=2` GeoTIFFs** — it silently
  corrupts them. Never pass `predictor=2` rasters to WBT subprocesses.
- **CONUS-scale memory: stream/window, never hold a full-grid array.** The CONUS
  template is 153830×109901 ≈ 16.9 B cells — ~17 GB as uint8, ~68 GB as int32,
  ~135 GB as float64. Oregon (~0.56 B cells) hides this; CONUS OOMs any depstor
  builder that materializes a full-grid array (or a redundant copy of one).
  Follow `carea_map`'s windowed-strip pattern (`STRIP_ROWS`); reproject with
  streaming `gdal.Warp`, never in-memory `rioxarray.reproject_match` (it blew a
  17 GB uint8 FDR to ~400 GB and OOM-killed routing); use
  `astype(np.int32, copy=False)` so label arrays aren't duplicated. The
  remaining full-grid steps (`waterbody` clump, `dprst` regions) fit at
  `--mem=384G` but are the memory ceiling; `routing` is now per-VPU tiled with
  an in-process D8 kernel (~80 GB peak measured for CONUS — the whole-CONUS
  `vpu_id` + `drains` arrays plus the largest VPU window's working set; run at
  `--mem=96G`, not 64G), so it's no longer one of them.
- **`carea_max`/`smidx_coef` threshold mode.** The legacy `absolute` thresholds
  (8.0/15.6) are only calibrated against VPU 01's ArcPy TWI distribution. For
  any other fabric use `threshold_mode: percentile` in
  `configs/depstor/depstor_rasters.yml` with `twi_raster` pointing at
  `twi_hydrodem.vrt` and run the `twi_reference` shared-raster step first.

## Working in this repo

- **Every code change needs a docs check.** Audit `docs/`, `README.md`, and
  `slurm_batch/RUNME.md` (and `HPC_REFERENCE.md`); update them on the same branch and surface findings
  before merge.
- **Atomic commits.** Split combined fixes into separate commits before pushing.
  If source changes exceed the original plan, lead the PR description with a
  scope-expansion callout.
- `*_legacy` paths (if any reappear) are retained for reference only — don't
  extend them. The two pre-PR-#37 stale working copies at the repo root
  (`_gfv2_params_legacy/`, `_create_lulc_params_legacy.py`) were deleted in
  PR closing #46.

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
- **Unfamiliar Python idiom?** See [`docs/python-patterns.md`](docs/python-patterns.md)
  for the 10 patterns this codebase uses repeatedly (future-annotations,
  placeholder strings, `require_config_key`, the `BUILDERS` dispatch table, etc.).
