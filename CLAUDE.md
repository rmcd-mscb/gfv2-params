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
pixi run data-root               # print data_root from base_config.yml (use in scripts/RUNME)
pixi run init-data-root          # scaffold the data directory tree
```

After editing `pyproject.toml` or `pixi.lock`, re-run `pixi install`.

**SLURM batches invoke `pixi run --as-is`** (= `--no-install --frozen`): the
already-installed env is used verbatim with no lock check or env mutation, so
concurrent array tasks don't race on `.pixi/envs/.../conda-meta`. Never change
batches to a flow that mutates the env per task. The `pixi` binary must be on
`PATH` at submit time (SLURM inherits the submitting shell), so always `sbatch`
from a shell where `~/.pixi/bin` is on `PATH`.

Lint/format runs via pre-commit: `pixi run -e dev pre-commit run --all-files`.

**Run the `--all-files` sweep under `srun`, not on the login node** — the
`prettier` hook (mirrors-prettier `v4.0.0-alpha.8` on node v24.1.0) needs
between 16 GB and 64 GB to lint 17 small YAML files. On the login node it is
SIGKILLed (`exit code -9`) and at `--mem=16G` SLURM reports `oom_kill`; at 64 GB
it passes. The memory demand is a broken-tool artifact, not file size — the
largest tracked YAML is 27 KB, and any single file passes in isolation.

```bash
srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=4 --mem=64G \
  pixi run -e dev --as-is pre-commit run --all-files
```

Targeted runs (`--files a b c`) are fine on the login node — prettier is scoped
to `\.(yml|yaml)$`, so it no-ops unless you touched YAML.

**Anything launched through `srun`/`sbatch` takes `--as-is`, tests included** —
the same rule as the SLURM batches above, for the same reason: without it each
task re-checks the lock and can mutate `.pixi/envs/.../conda-meta`, which
concurrent jobs then race on. It is also markedly faster, since the lock check
is skipped — a full `pytest tests/` run measured 56.6 s with `--as-is` versus
174.2 s without.

```bash
srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G \
  pixi run -e dev --as-is pytest tests/ -q
```

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

`slurm_batch/RUNME.md` is the step-by-step runbook (the CONUS-gfv2 happy path) — it opens
with a **Quick Start for Scientists** section (condensed copy-paste commands), then the full
per-step reference. Optional steps are marked with `> **Optional:**` blockquotes; Step 4
leads with the wholesale `submit_zonal_params.sh` / `submit_depstor_params.sh` wrappers
(manual per-param commands are in a collapsed `<details>` block).
`slurm_batch/HPC_REFERENCE.md` holds the per-stage detail, alternate paths, and recovery;
`README.md` covers user-facing setup and usage.

## Non-obvious conventions & gotchas

These are hard-won; violating them silently corrupts outputs.

- **The dprst/on-stream split is driven by the MODEL's own segment network.**
  `segment_wbody` promotes a waterbody to on-stream iff an `nsegment` from the
  fabric's `segments_gpkg` intersects it with **positive length** (a zero-length
  shoreline graze does not count — 3.1% of candidate pairs CONUS-wide). This asks
  "is it on the network the model routes?", not "is it on the NHD network": NHD's
  network is far finer, and a waterbody NHD routes but the model does not had no
  representation at all. `segment_wbody` runs at **STEP_ORDER position 3**, ahead
  of `waterbody` (whose BurnAdd overlap guard consumes its output) and
  `wbody_connectivity`, and writes `segment_waterbody_comids.parquet` (registered
  key `segment_wbody_comids`). Consequences that are deliberate: a segment
  collinear with a shoreline promotes, and a segment **terminating inside** a
  waterbody promotes, so NHD's inflow-AND-outflow discrimination is gone and the
  **endorheic subtraction is what still demotes terminal lakes**.
  There are **three consumers** of `segment_wbody_comids − endorheic_comids`:
  `wbody_connectivity` (required primary on-stream source, applying the two
  unchanged subtractions — `endorheic` and the Playa/Ice Mass guardrail),
  `waterbody` (the BurnAdd overlap guard — reads the raw pre-endorheic
  `segment_wbody_comids`, a deliberately conservative superset, since it runs
  before `endorheic` and can't see the demotion), and `dprst_depth` (reconstructs
  the dprst polygon set via `topo.load_fabric_dprst_polygons(onstream_comids=...)`
  on both the in-process and SLURM `--plan` paths, so it computes depths for the
  SAME polygon set `dprst_binary.tif` uses, not a divergent NHD-derived one).
  NHD flowline topology (`nhd_flowlines` WBAREACOMI, `nhd_flowthrough`,
  `nhd_topology`) is retained as an **opt-in comparison union**: the profile keys
  are commented out on every fabric, and `wbody_connectivity` logs a
  `COMPARISON MODE` warning if it ever sees one present. Because they are opt-in,
  the "`nhd_topology` must run before both `nhd_flowlines` and `nhd_flowthrough`"
  ordering constraint now applies only to that comparison path — a normal depstor
  run stages none of the three. If `drains_to_dprst` over-extends into humid
  open-drainage basins, fix the **classifier** (which waterbodies are on-stream)
  — never add a cap or tuning knob to routing. A cap cannot distinguish a
  legitimately large endorheic basin from a spurious one and damages the correct
  cases.
- **Depstor template/fdr come from a fabric-bounds clip** of `fdr.vrt`
  (`scripts/clip_shared_to_fabric.py`), not CONUS VRTs or per-VPU tiles. The
  clip must come from the hydrology lattice (`fdr.vrt`/`twi.vrt`); `elevation.vrt`
  is on the offset DEM lattice and `carea_map` requires `template ≡ twi`
  alignment. `fdr.vrt` is the **official NHDPlus V2 `FdrFac`** (NHDPlus HydroDEM:
  stream-burned + walled + depression-filled **everywhere except at NHDPlus's own
  sinks**). It is **not** fully drainage-enforced: it contains exactly **15,262
  code-0 (terminal) cells**, and **15,693 of the 15,728** sinks in NHDPlus's own
  `Sink.shp` (99.8%) land on one — i.e. the FDR's code-0 cells *are* the NHD sink
  set. NHDPlus leaves those sinks unfilled *by design* — that is what a sink is. The
  "FDR code-0 warnings" noted during issue #145 **are that sink set**, and they
  are now the primary signal of the endorheic dprst classifier: a waterbody is
  depression storage iff its water's terminus lies inside itself
  (`gfv2_params.endorheic`, Signal A) — *not* the opt-in richdem `Fdr_hydrodem`
  from `compute_dem_derivatives.py`. `d8_routing` already treats code 0 as a
  terminus, so the classifier and the router read the same grid and agree by
  construction. See `docs/ARCHITECTURE.md`,
  `docs/superpowers/specs/2026-07-12-endorheic-dprst-classifier-design.md`, and
  issue #147 (depression-respecting FDR investigation) for the provenance/tradeoff.
  **The #147 A/B has now been run** (VPU 16, `slurm_batch/ab_drains_to_dprst.batch`):
  `drains_to_dprst` land coverage is 0.4401 under the production FdrFac vs 0.4388
  under a breached DEM — a 0.3% difference — while a richdem **fill-all** on the same
  DEM gives 0.8079, nearly double, inflating 1,009 of 2,605 depressions by >2x and 411
  by >10x. So the fill pathology #147 feared is real but **we are not exposed to it**:
  stream-burning and walling already give water defined exits, so the FdrFac behaves
  like a depression-respecting DEM. Do not swap the FDR chasing contributing-area
  magnitude — there is nothing to win. (Open sub-question: production and breach agree
  on the total but disagree on *attribution*, reshuffling 31% of contributing area
  between depressions, 415 of them by >2x. Which is correct is not decided by that
  A/B.) Note also that `Fdr_breached` sets `BREACH_FILL=True` and therefore has **no
  interior code-0 cells**, so it can never serve as an endorheic-classifier input —
  Signal A would go silently dark.
- **Endorheic demotion is a STRICT SUBTRACTION, and its input is the FDR — not a
  vector sink file.** `wbody_connectivity` subtracts an endorheic COMID set from
  the on-stream union; the subtraction can only ever remove COMIDs, never add
  one. Signal A ("terminus-inside-itself") reads the FDR's code-0 cells and runs
  `d8_routing`'s own kernel, so the classifier and the router agree by
  construction. **Do not** substitute a vector sink file for the FDR grid — not
  `input/nhd/NHD_sink_points.gpkg`, not any other point layer. A point layer is a
  lossy *shadow* of the grid the router actually reads: Signal A must agree with
  `d8_routing` by construction, and it can only do that by reading the same FDR.
  (A hand-curated `NHD_sink_points.gpkg` was rejected during design for dropping
  `PURPCODE 1`, "BurnLineEvent network end" — exactly the class that marks terminal
  lakes — and so containing **0 sinks inside Great Salt Lake**. That specific file
  has since been replaced with a near-complete copy of `Sink.shp`, so **do not
  re-derive the prohibition from its current contents**; the architectural reason
  above is what stands.) Stage the authoritative sinks from source via
  `gfv2_params.download.nhd_burn_components` — they are provenance for
  BurnAddWaterbody, deliberately unread by any classifier. Likewise, do not substitute
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
  sqrt(2)` and **raises** instead of silently dropping it. That clump membership
  is **transitive**, and the guard must walk it: BurnAdd → already-dprst waterbody
  → on-stream waterbody is ONE region, so testing only a BurnAdd's *direct*
  neighbours misses the chain. Do **not** "simplify" it back to a direct
  neighbour test on the reasoning that merging into an already-dprst neighbour is
  harmless — being dprst *by COMID* does not mean a waterbody's *region* survives
  the on-stream exclusion, which is exactly what the Great Salt Lake / COMID
  10273192 marsh case demonstrates. (Inert on today's CONUS layer: the 1,658
  BurnAdd polygons pull in 113 waterbodies, the walk closes after one hop, and
  none of them is on-stream.)
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
  `endorheic_wbody == 1 AND connected_wbody != 1 AND wbody_binary == 1`. All
  three terms are load-bearing: direct hydrologic evidence (terminus-inside-itself)
  overrides the clump proxy, but ONLY for the waterbody's own not-on-stream cells
  — a cell that is itself on-stream (the marsh) always stays excluded — and only
  where `waterbody` already calls it a waterbody, since `endorheic_wbody` is
  rasterized from a raw, unfiltered read of the gpkg and would otherwise reinstate
  the 2 Mt Shasta Ice Mass COMIDs that `waterbody` deliberately dropped (a glacier
  is not depression storage; this preserves `dprst ⊆ wbody_binary`). Runs before
  the impervious carve and land mask so both still apply to recovered cells.
  Deliberately narrower than a global per-cell on-stream carve, which was
  considered and rejected — dropping the `endorheic_wbody` term alone *is* that
  carve, and it would recover a further ~7,403 km² (gfv2, measured 2026-08-04; the figure moves with each dprst cascade rebuild — 9,177 km² on the segment-driven gfv2_dev — so re-measure rather than trusting a quoted value) of non-endorheic waterbodies
  whose clump merely abuts an on-stream feature (reproduce with
  `scripts/diagnose/measure_global_carve.py`); those must keep the unexempted
  clump behaviour exactly, which is what `drains_to_dprst` over-extension
  #145/#158/#161 fixed. `endorheic_wbody` is **required**, not optional:
  `wbody_connectivity` always writes it alongside `connected_wbody`, so having one
  without the other is always a *stale output directory*, never a legitimate
  configuration — treating it as "exemption off" is exactly how a `--from dprst`
  rebuild would silently re-emit the pre-fix product (all 4,854,156 Great Salt Lake
  cells back out of dprst, exit code 0). `dprst` fails loud instead.
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
  **The floor must be enforced at the CONSUMING end (`wbody_connectivity`), not
  only in the `endorheic` builder that writes the table** — `--from
  wbody_connectivity` (the documented cascade-rebuild recipe) leaves `endorheic`
  out of the run list, and `_hydrate_existing_outputs` then pulls its table off
  disk with no validation at all. A guard that lives only in the producing
  builder never executes on the exact path operators actually use. The floor also
  applies **per signal**, not just to the union: Signal B dominates by COUNT (543
  of 818 CONUS demotions) while Signal A carries almost all the demoted AREA, so a
  total Signal-A collapse still clears a count-based floor while ~75% of the
  demoted area silently vanishes. The same both-ends contract applies to
  `min_onstream_comids`, the floor on the `segment_wbody` COMID count (gfv2
  30000 against 48,529 measured; oregon 500 against 770).
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
- **gdptools' `masked_mean` returns `0.0`, NOT NaN, for a polygon whose source
  cells are all fill** — so "outside the source's domain" is indistinguishable
  from "genuinely zero" in the aggregated output. For SNODAS this means an HRU
  off the edge of the domain reports `swe = 0` every day and lands in
  `default_no_snow` next to Florida: **1,087 CONUS HRUs (0.30%)**, plus **35,315
  (9.8%)** aggregating over only part of their area. The fix is the `coverage`
  diagnostic (`gfv2_params.aggregate.coverage`, `derive_aggregate.py --mode
  coverage`), joined into Stage 2's CSV alongside `n_years_total` /
  `n_years_dropped`. It is a **diagnostic, never a gate** — a `coverage >= 0.999`
  selection criterion was measured and rejected because it flips ~13.3k CONUS
  HRUs off their empirical curve onto the default. A missing coverage table
  leaves the column NaN ("not measured"), deliberately distinct from a measured
  `0.0`. Note the source footprint is **not static**: SNODAS's valid domain
  expands across the record (31 of the 32 CONUS HRUs reading zero-coverage in
  2004 carry real snow by 2015), so coverage is averaged over one sampled day
  per year — a single-year probe would call a late-covered HRU permanently
  absent. Relatedly, an all-fill *year* is silently consumed as a snow-free year
  and dropped by `annual_sdc`, which is why `n_years_dropped` exists.
- **A consolidated gdptools weight file mixes per-batch index spaces — its
  `(i, j)` are NOT global.** Batched aggregation clips the source grid to each
  batch's own bounds (`driver.subset_to_gdf_bounds`), so every batch's weight
  `(i, j)` index that batch's subset. `derive_aggregate.py --mode merge`
  row-concats the per-batch tables into `{source}_weights_{fabric}.csv`, which is
  therefore safe ONLY for index-agnostic use — `cells_from_weights` does a
  `groupby().size()`, a count, which is why it has always been fine. Anything
  positional (the coverage diagnostic, any future per-cell join) must read the
  per-batch CSVs and pair each with its own `batch_{NNNN}.gpkg`. Mismatched
  indices do not error on their own — they silently address the wrong cells and
  return plausible numbers — so `coverage_from_weights` bounds-checks and raises.
- **`k_perm` is INTENSIVE — aggregate it as an area-weighted mean, never an
  area-prorated sum.** `ssflux.py` originally used gdptools' *extensive* form
  (`Σ Vᵢ·aᵢ/Aᵢ`, dividing by the SOURCE polygon area, the column gdptools labels
  "for extensive variables"), which is correct for population or volume but not
  for a property of the medium. Per-HRU weight sums spanned 3.6e13× instead of
  1.0, inflating spread from a physical 5.6 to an artifactual 15.4 orders of
  magnitude and pinning ~90% of HRUs at the range minimum (#175). Use
  `normalized_area_weight` (gdptools' `wght`) and renormalise by its per-HRU sum
  — that also corrects the 1,828 coverage-gap and 98 overlapping-source HRUs.
  Related traps: `k_perm == 0` is a **no-data flag**, not a measurement, and must
  be excluded rather than floored to `k_perm_min` (-16.48 is simultaneously the
  genuine least-permeable lithology class, 26,441 polygons); `k_perm` has only
  **8 distinct values**, so ~5% of HRUs sitting at the range minimum is real
  geology, not a defect. Normalisation is a **reduce step** — never per batch;
  and never fix the per-batch scope before fixing the log-space aggregation, or
  degeneracy goes from ~90% to 97-100%.

## Working in this repo

- **AI assistant memory sync.** `.amazonq/rules/workflow.md` is the single
  source of truth for the issue+PR workflow, completed/upcoming issues, and
  repo facts (pixi tasks, CI, branch conventions). When either file is updated,
  sync the other: completed issues and new pixi tasks belong in both;
  deep architectural gotchas and code conventions belong only in `CLAUDE.md`.

- **Triage a filed issue before working it** — verify its PREMISE, not just its
  `file:line` cites. The depstor classifier was rewritten repeatedly
  (#145 → #152 → #158 → #161 → #187), so issues filed against it go stale fast:
  3 of 3 triaged on 2026-08-04 were premise-void or resolved-by-design, and one
  proposed verbatim a design already rejected. The rejected designs are recorded
  in the bullets above — check them before implementing any "candidate fix", and
  re-measure any number a decision rests on rather than quoting it. Full checklist
  in `.amazonq/rules/workflow.md`.

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
- **Every declared config entry needs a `prms:` block, and it is not optional.**
  `builder:`, plus every emitted column sorted into exactly one of three buckets:
  `columns:` (this IS the PRMS parameter), `defects:` (this is *supposed* to be it
  and currently is not — carries `reason`/`recovery`/`issue`), `provenance:` (not a
  PRMS parameter at all). `processes:` is **per column, not per entry** — `ssflux`
  alone spans PRMSSoilzone, PRMSGroundwater and PRMSRunoff across different columns,
  so an entry-level key would report 5 non-runoff parameters as feeding runoff. Use
  `processes: []` for a parameter no pywatershed Process consumes (`hru_elev`), never
  a made-up process name — that would put a phantom heading in the generated index.
  `params_for_process` reads `columns:` ONLY, so a defective column can never be
  returned under a correct-looking parameter name. Guard 1
  (`tests/test_params_index.py`) fails without the block; Guard 2
  (`tests/test_params_index_ondisk.py`) checks the declaration against the on-disk
  header in **both** directions but is data-root-gated, so it SKIPS in CI — record
  its result by SLURM job id, never infer it from a green badge. After editing any
  `prms:` block run `python scripts/build_parameter_index.py`; CI fails if the
  generated tables in `docs/parameter_index.md` are stale.
- **`merged/<name>.csv` IS the gap-filled product — Guard 3 enforces it on disk.**
  PR #189 retired the `filled_` prefix: `merged/<name>.csv` is the single canonical,
  always-gap-filled per-HRU file, with the pre-fill copy preserved at
  `merged/_unfilled/<name>.csv`. Nothing checked that, and the convention silently
  rotted — until issue #211, gfv2 and tjc served the PRE-FILL half under the
  canonical name (2,009 and 23 NaN respectively) with the real values stranded in
  `filled_nhm_soil_moist_max_params.csv`. Guard 3
  (`tests/test_merged_products_ondisk.py`) fails on any surviving `filled_*` and on
  any NaN in a declared-fillable column; like Guard 2 it is data-root-gated and
  SKIPS in CI, so record it by SLURM job id. **Never run
  `scripts/migrate_filled_params.py` against a param whose `merged/<name>.csv` was
  rebuilt more recently than its `filled_` copy** — migration moves `filled_` OVER
  the canonical name, so on a freshly rebuilt param it overwrites the rebuild with
  stale data. That is why ssflux's legacy `filled_` copy was deleted rather than
  migrated after the #175 rebuild. Always dry-run first (the script defaults to it)
  and confirm the plan lists only the params you intend.
- **`prms.provenance` is orthogonal to `fill_columns`, not a restatement of it.**
  `fill_columns` asks "is this KNN-interpolable?"; `prms.provenance` asks "is this a
  PRMS parameter?". All four quadrants exist — the elevation/slope/aspect stats are
  filled *and* not PRMS parameters, `op_flow_thres` is a PRMS parameter that was not
  filled. Do not collapse the two.
- **A `derived_columns:` output that is also in `fill_columns` makes a re-merge a
  prerequisite of the next fill sweep.** `slope`'s `hru_slope` and `aspect`'s
  `hru_aspect` are the two instances: `resolve_fill_plan` raises on a declared column
  the file does not have, so any fabric whose CSV predates the declaration needs a
  re-merge (`derive_zonal_params.py --mode merge --param <slope|aspect>`) first. Do
  not "fix" it by dropping the column from `fill_columns` — the rationale is not a
  silent-NaN risk. `run_fill_sweep` RE-DERIVES both columns from their declared
  sources (`mean` for `hru_slope`; `mean_sin`/`mean_cos` for `hru_aspect`) AFTER the
  KNN pass, overwriting whatever KNN wrote, so the shipped value is always a function
  of its (possibly interpolated) sources on every row, never an
  independently-interpolated one. The declaration's job is the raise itself:
  `resolve_fill_plan`'s raise is the only LOUD, OPERATOR-VISIBLE tripwire — at
  fill-sweep time, against a real merged CSV on a data root — for a fabric CSV that
  predates a `derived_columns:` block. It is not a CI-visible tripwire: Guard 2
  (`tests/test_params_index_ondisk.py`) is data-root-gated and SKIPS in CI, so this
  raise is the only backstop for THAT failure mode that is not itself gated into
  skipping. It does **not** cover the block being DELETED from the config — the
  column is still declared fillable and still on disk, so nothing raises, the
  re-derivation is silently skipped, and KNN becomes what *determines* a circular
  quantity (issue #201 reappearing on exactly the gap-filled HRUs). That second case
  is pinned in CI by
  `tests/test_params_index.py::test_the_two_derived_columns_are_still_declared`,
  which is pure YAML and does not skip.
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
