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
  `nhd_flowthrough`). **Both sources gate on-stream promotion on Network-Flowline
  membership** (a COMID present in `flowline_topology.parquet` / NHDPlus
  PlusFlowlineVAA): NHD draws Non-Network artificial paths through essentially
  every closed-basin lake, so the ungated WBAREACOMI set and the ungated
  geometric T1 test both wrongly promoted genuinely endorheic waterbodies
  on-stream (issue #161). Because of this gate, **`nhd_topology` must run before
  both `nhd_flowlines` and `nhd_flowthrough`** (both fail loud if the topology
  parquet is missing). A waterbody promoted by the flow-through test must have
  both inflow and outflow — terminal sinks (inflow only), locally-spilling
  potholes (outflow only), and isolated depressions stay dprst. Playa and Ice
  Mass FTYPEs are a hard guardrail and are never promoted on-stream regardless
  — but they are not equivalent: Playa IS depression storage (force-dprst);
  Ice Mass is NOT depression storage and is excluded from the waterbody
  classification entirely (its cells fall back to land, perv/imperv via LULC)
  at the `waterbody` builder, upstream of this union. If
  `drains_to_dprst` over-extends into humid open-drainage basins, fix the
  **classifier** (which waterbodies are on-stream) — never add a cap or tuning
  knob to routing. A cap cannot distinguish a legitimately large endorheic basin
  from a spurious one and damages the correct cases.
- **Depstor template/fdr come from a fabric-bounds clip** of `fdr.vrt`
  (`scripts/clip_shared_to_fabric.py`), not CONUS VRTs or per-VPU tiles. The
  clip must come from the hydrology lattice (`fdr.vrt`/`twi.vrt`); `elevation.vrt`
  is on the offset DEM lattice and `carea_map` requires `template ≡ twi`
  alignment. `fdr.vrt` is the **official NHDPlus V2 `FdrFac`** (NHDPlus HydroDEM:
  stream-burned + walled + depression-filled **everywhere except at NHDPlus's own
  sinks**). It is **not** fully drainage-enforced: it contains exactly **15,262
  code-0 (terminal) cells**, and 8,591 of the 8,611 NHD sink points land on one.
  NHDPlus leaves those sinks unfilled *by design* — that is what a sink is. The
  "FDR code-0 warnings" noted during issue #145 **are that sink set**, and they
  are now the primary signal of the endorheic dprst classifier: a waterbody is
  depression storage iff its water's terminus lies inside itself
  (`gfv2_params.endorheic`, Signal A) — *not* the opt-in richdem `Fdr_hydrodem`
  from `compute_dem_derivatives.py`. `d8_routing` already treats code 0 as a
  terminus, so the classifier and the router read the same grid and agree by
  construction. See `docs/ARCHITECTURE.md`,
  `docs/superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md`, and
  issue #147 (depression-respecting FDR investigation) for the provenance/tradeoff.
- **Endorheic demotion is a STRICT SUBTRACTION, and its input is the FDR — not a
  vector sink file.** `wbody_connectivity` subtracts an endorheic COMID set from
  the on-stream union; the subtraction can only ever remove COMIDs, never add
  one. Signal A ("terminus-inside-itself") reads the FDR's code-0 cells and runs
  `d8_routing`'s own kernel, so the classifier and the router agree by
  construction. **Do not** substitute `input/nhd/NHD_sink_points.gpkg` — it is a
  strict subset of NHDPlus's own `Sink.shp` (537 sinks vs 3,222 in VPU 16) that
  omits `PURPCODE 1` ("BurnLineEvent network end") entirely — precisely the
  class that marks terminal lakes — and therefore contains **0 sinks inside
  Great Salt Lake**, where NHDPlus's file has **29**. Stage from source via
  `gfv2_params.download.nhd_burn_components`. Likewise, do not substitute
  `input/nhd/closed_huc12.gpkg` — an incomplete extract (**23** type-C HUC12s in
  the Great Basin vs **141** in the full WBD; it resolves 1 of the 10 classic
  terminal lakes where the full WBD resolves 5). Stage via
  `gfv2_params.download.wbd_huc12`. Containment tests use **majority-area**,
  never `intersects` (a zero-interior-overlap boundary touch returns `True` —
  Eagle Lake and Middle Alkali Lake graze closed basins at frac = 0.000) and
  never `within` (it **drops Great Salt Lake**, which spills 1.1% into a
  neighbouring HUC12 at frac = 0.989). Separately, the BurnAddWaterbody overlap
  guard in `waterbody`'s `merge_burn_add` models **8-connected raster adjacency,
  not vector intersection** — `clump_regions` merges 8-connected cells, so two
  polygons ~42 m apart on a 30 m grid can clump-merge without vector-intersecting.
  If a BurnAdd polygon lands in the same clump as an on-stream waterbody,
  `regions_touching_mask` would delete the whole clump, silently destroying the
  BurnAdd playa's depression area — so the guard buffers by `cell_size *
  sqrt(2)` and **raises** instead of silently dropping it.
- **Endorheic demotion alone does not fix the CONUS dprst product — the
  region-level on-stream exclusion still vetoes it.** `clump_regions` labels
  8-connected waterbody components, and `regions_touching_mask` excludes a
  WHOLE region from `dprst` if any one cell touches the on-stream mask. The
  Great Salt Lake (4,369 km², correctly demoted to dprst by `endorheic`) is
  8-connected to a 49.1 km² SwampMarsh (COMID 10273192) whose water flows INTO
  the lake and is correctly left on-stream — so without a fix, that one
  marsh's on-stream status vetoed the entire merged region, silently
  excluding all 4,854,156 Great Salt Lake cells from depression storage even
  though `connected_wbody.tif` no longer contains it. The fix:
  `wbody_connectivity` rasterizes a SECOND mask, `endorheic_wbody.tif` (the
  FULL endorheic set, regardless of on-stream status), and `dprst.py` exempts a
  waterbody's own cells from the region-level exclusion wherever
  `endorheic_wbody == 1 AND connected_wbody != 1` — direct hydrologic evidence
  (terminus-inside-itself) overrides the clump proxy, but ONLY for the
  waterbody's own not-on-stream cells; a cell that is itself on-stream (the
  marsh) always stays excluded. Runs before the impervious carve and land
  mask so both still apply to recovered cells. `endorheic_wbody` is optional
  — absent (a fabric that hasn't run `endorheic`) is a pure no-op, so this
  cannot re-open the `drains_to_dprst` over-extension #145/#158/#161 fixed.
  Deliberately narrower than a global per-cell on-stream carve, which was
  considered and rejected — it would recover a further ~8,471 km² of
  non-endorheic waterbodies whose clump merely abuts an on-stream feature,
  and those must keep today's clump behaviour exactly.
- **`BurnAddWaterbody` is NOT a sink layer.** It is every waterbody NHDPlus added
  to the DEM burn; only the rows with a sink `PurpCode` (4 Playa, 5/8 closed
  lake) are sinks and become depression area. VPU 01 ships **702 NULL-`PurpCode`
  rows against ZERO sinks in its own `Sink.shp`** — 503 of them on-network,
  including StreamRiver and CanalDitch FCodes — so merging the layer wholesale
  turns canals and river reaches into depression storage.
  `download/nhd_burn_components.py` keeps only the sink-purpose rows and takes
  `FTYPE` from **`FCODE`**, not `PurpCode` (`PurpCode` 5 spans both Playa and
  SwampMarsh, and a Playa mislabelled LakePond loses force-dprst). A populated
  unrecognised `PurpCode`, or a retained conveyance `FTYPE`, raises.
- **An empty endorheic table is a legitimate result, not a failure.** A domain
  with no closed basin (`tjc`, Texas-Gulf) has no endorheic waterbody, and the
  `endorheic` step lives in the fabric-independent depstor config, so raising on
  a zero-row result bricks that fabric's whole DAG. Protection against a
  *silently* empty result lives in the optional per-fabric
  `min_endorheic_comids` profile floor (gfv2: 100), not in a blanket raise.
- **On-stream waterbodies are traversal barriers in `routing`.** Land upslope
  of an on-stream (non-dprst) waterbody is captured by that waterbody's
  stream/lake routing and must not be attributed to a downstream depression —
  `routing` stops a cell's D8 trace at the first on-stream waterbody cell it
  hits, before it can reach a dprst pour-point. The barrier set is the full
  `onstream` mask (no size filtering); the fix is a strict subtraction that
  can only reduce `drains_to_dprst` coverage, never increase it.
- **`sro_to_dprst_perv`/`sro_to_dprst_imperv` are same-HRU-restricted via a
  raster intersection, not gdptools.** `same_hru_drains` computes
  `drains_to_dprst_hru == hru_id` (both int32, per-cell) to build
  `drains_perv_binary.tif`/`drains_imperv_binary.tif`, replacing the old plain
  `intersect` — because it's a per-cell reached-HRU-vs-own-HRU test that
  gdptools' partial-pixel zonal weighting cannot express. The per-HRU COUNT
  aggregation downstream still uses gdptools as normal. This reproduces the
  legacy `Con(rSro == hru)` (`docs/0b_TB_depr_stor.py:214`). `drains_to_dprst`
  itself (from `routing`) stays HRU-agnostic — only the `sro_to_dprst_*`
  ratios get the same-HRU restriction.
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
