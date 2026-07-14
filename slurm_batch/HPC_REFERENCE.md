# GFv2 HPC Pipeline — Reference

Reference detail for the GFv2 HPC pipeline. The step-by-step happy path is in
[RUNME.md](RUNME.md); this file holds the environment internals, per-stage
detail, alternate execution paths, per-fabric instructions, recovery, and the
script→config map.

---

## Environment internals

The pipeline uses [pixi](https://pixi.sh) for environment management. Install
pixi once per user (`~/.pixi/bin` must be on your `PATH`). From the repo root
run `pixi install` to materialise `.pixi/envs/default/` from `pixi.lock`
(configuration lives in `pyproject.toml` under `[tool.pixi.*]`).

SLURM batches invoke the env with `pixi run --as-is` (= `--no-install --frozen`):
the already-installed env is used verbatim — no lock check, no env mutation, no
PyPI/conda sync — so concurrent array tasks never race on
`.pixi/envs/default/conda-meta/`. Because the batches use `--as-is`, the `pixi`
binary must be on `PATH` on the compute node. SLURM jobs inherit the submitting
shell's environment, so always submit (`sbatch ...`, `submit_*.sh`) from a shell
where `~/.pixi/bin` is on your `PATH`. If a batch fails immediately with
`pixi: command not found`, that PATH was missing at submit time. Re-run
`pixi install` any time `pyproject.toml` or `pixi.lock` change.

Interactive environments:

```bash
pixi shell                       # default env
pixi shell -e notebooks          # default + marimo, plotly, hvplot, ...
pixi shell -e dev                # default + pytest, ruff, pre-commit
```

> **Migrating from `geoenv`?** The legacy `environment.yml` / `geoenv` conda
> env is retained as a deprecated fallback only. New work uses pixi.

---

## Array concurrency throttle

`submit_jobs.sh` accepts an optional 5th positional argument (or the
`SUBMIT_JOBS_MAX_CONCURRENT` env var) capping how many array tasks run
simultaneously — defaults to 4. The cap exists because concurrent geo-library
imports (rasterio / GDAL / PROJ / pyogrio) can deadlock under shared-FS
metadata contention when many tasks start simultaneously. Set to `0` (or `off`)
to disable the cap. For CONUS the default of 4 trades roughly one wave of
wall-clock time for reliability. The same throttle (`%N`) is used as the modulo
in every `--array=0-$((N-1))%$THROTTLE` invocation throughout Stage 4.

---

## Data directory layout

All data lives under `data_root` (set in `configs/base_config.yml`):

```
gfv2_param/
├── input/                  # External data (manually staged or downloaded)
│   ├── fabric/             # Per-VPU watershed fabric gpkgs
│   ├── soils_litho/        # TEXT_PRMS.tif, AWC.tif, Lithology_exp_Konly_Project.*
│   ├── lulc_veg/           # RootDepth.tif, CNPY.tif, Imperv.tif
│   │   └── nhm_v11/        # NHM v1.1 pre-derived LULC rasters (downloadable)
│   ├── lulc/
│   │   ├── nlcd_annual_imperv/   # NLCD fractional imperviousness (downloadable)
│   │   └── nalcms_2020/    # NALCMS 2020 land cover (downloadable)
│   ├── nhd/                # conus_waterbodies.gpkg (shared NHDPlusV2 waterbodies)
│   ├── twi/<rpu>/          # Per-RPU TWI (twi.tif + sidecars; staged via stage_twi.sh)
│   ├── nhm_default/        # NHM default parameter files
│   └── nhd_downloads/      # Raw NHDPlus zip archives (downloadable)
├── shared/                 # Fabric-independent intermediates (reused by every fabric)
│   ├── source/             # Unzipped per-RPU NHDPlus rasters
│   ├── per_vpu/<vpu>/      # Per-VPU merged GeoTIFFs (NED/Hydrodem/Fdr/Fac/Twi/slope/aspect/landmask)
│   └── conus/
│       ├── vrt/            # CONUS-wide GDAL virtual rasters (elevation/slope/aspect/fdr/twi)
│       ├── derived/        # soil_moist_max.tif, radtrn, resampled CNPY/keep
│       ├── borders/        # Copernicus border-DEM fill (Canada/Mexico)
│       └── weights/        # P2P polygon weights for ssflux
└── {fabric}/               # Per-fabric outputs (e.g., gfv2/, gfv2_vpu01/, oregon/)
    ├── fabric/             # Merged fabric gpkg
    ├── batches/            # Per-batch gpkgs + manifest
    ├── depstor_rasters/    # Depression-storage intermediate rasters (per fabric)
    └── params/             # Parameter outputs + merged + filled
```

> **Upgrading an existing `data_root` from the legacy `work/` layout?** Run
> `pixi run python scripts/migrate_to_shared_layout.py --data-root <path> --dry-run`
> to preview the 27 directory renames, then `--execute` to apply them.
> Atomic `os.rename` on the same filesystem (metadata-only, near-instant);
> regenerates CONUS VRTs at the end since they encode absolute source paths.
> Idempotent — re-running after success is a no-op.

---

## Selecting / running other fabrics

Fabric identities and all shared, required per-fabric inputs live as profiles in
`configs/base_config.yml` under a `fabrics:` mapping. Every profile carries
`hru_gpkg`/`hru_layer`, `id_feature`, `expected_max_hru_id`, and `batch_size`;
depstor fabrics add `template_raster`, `fdr_raster`, `twi_raster`,
`connected_comids_table`, `segments_gpkg`/`segments_layer`, and
`waterbody_gpkg`/`waterbody_layer`. The
active profile is selected via:

1. `--fabric <name>` CLI flag on any script, OR
2. `FABRIC` env var forwarded through `sbatch`, OR
3. `default_fabric` in `configs/base_config.yml` (currently `gfv2`).

SLURM batches default to `gfv2`. To run the same batch against a different fabric,
set `FABRIC` and optionally override resource asks at submission:

```bash
# CONUS gfv2 — default
sbatch slurm_batch/build_depstor_rasters.batch

# VPU01 validation overlay — smaller; override resources
FABRIC=gfv2_vpu01 sbatch --time=02:00:00 --mem=48G slurm_batch/build_depstor_rasters.batch

# Oregon (pre-merged single-domain fabric)
FABRIC=oregon sbatch slurm_batch/build_depstor_rasters.batch
```

`submit_jobs.sh` accepts fabric as its 4th positional argument and forwards it
via `--export=ALL,FABRIC=...` to the array job.

---

## Part 1 stage detail

All commands assume the repo root as working directory. The entire Part 1 DAG
can be driven by one batch:

```bash
sbatch slurm_batch/build_shared_rasters.batch
```

Env knobs the batch honours:

- `FORCE=1` — pass `--force` to rebuild outputs that already exist
- `VPUS=01,02` — restrict per-VPU steps to a subset

For interactive use or finer-grained flags, invoke the orchestrator directly:

```bash
pixi run python scripts/build_shared_rasters.py \
    --config configs/shared_rasters/shared_rasters.yml
```

Use `--step <name>` for a single step or `--from <name>` to resume mid-DAG.
Step names match keys in `configs/shared_rasters/shared_rasters.yml`. Heavy
single-step rebuilds should be submitted via the batch
(`sbatch slurm_batch/build_shared_rasters.batch --step <name>`), not run
directly on the login node. `build_vrt` writes VRT XML cheaply but now also
builds an external `.vrt.ovr` overview pyramid per VRT, which reads each
CONUS mosaic at full resolution once — submit it via the batch, not the login
node.

### Stage 0 — Initialize data root and stage inputs

Scaffold the full directory tree under `data_root`:

```bash
pixi run init-data-root
pixi run init-data-root --check    # verify manually-staged inputs are present
```

Manually-staged files required before `--check`:

| Destination | Required files |
|---|---|
| `input/fabric/` | `NHM_<VPU>_draft.gpkg` for each of the 21 VPUs |
| `input/soils_litho/` | `TEXT_PRMS.tif`, `AWC.tif`, `Lithology_exp_Konly_Project.shp` (+ `.dbf`, `.prj`, `.shx`) |
| `input/lulc_veg/` | `RootDepth.tif`, `CNPY.tif`, `Imperv.tif` |
| `input/nhm_default/` | NHM default parameter files |
| `input/nhd/` | `conus_waterbodies.gpkg` (layer `waterbodies`); `connected_waterbody_comids.parquet` (produced by `download_nhd_flowlines.batch`); `flowline_topology.parquet` (produced by `python -m gfv2_params.download.nhd_topology`; must run before **both** COMID staging steps below — they gate on-stream promotion on Network-Flowline membership); `flowthrough_waterbody_comids.parquet` (produced by `stage_nhd_flowthrough.batch`) |
| `input/twi/<rpu>/` | Per-RPU `twi.tif` + sidecars (staged via `stage_twi.sh`) |

Download jobs (idempotent — already-downloaded files are skipped):

```bash
mkdir -p logs
sbatch slurm_batch/download_rpu_rasters.batch    # NHDPlus RPU rasters (~112 GB)
sbatch slurm_batch/download_nalcms.batch         # NALCMS 2020 land cover (~2 GB)
sbatch slurm_batch/download_nhm_v11.batch        # NHM v1.1 LULC rasters
pixi run --as-is python -m gfv2_params.download.nhd_topology  # flowline topology (one-time, CONUS; run FIRST — gates both COMID steps below)
sbatch slurm_batch/download_nhd_flowlines.batch  # NHD-connected waterbody COMIDs (one-time, CONUS; needs topology)
sbatch slurm_batch/stage_nhd_flowthrough.batch   # flow-through waterbody COMIDs (one-time, CONUS; needs topology)
```

Stage per-RPU TWI rasters (reads from the impd-group mirror by default):

```bash
sbatch slurm_batch/stage_twi.batch
# or with an alternate source:
SRC=/alt/path/to/data_bins sbatch slurm_batch/stage_twi.batch
```

The staging script normalises HRU06a's uppercase `TWI.*` source filenames to
lowercase in the destination. Idempotent — re-running skips files already
present and newer than the source.

### Stage 1 — `merge_rpu_by_vpu` + `compute_slope_aspect`

Merge per-RPU NHDPlus rasters into per-VPU GeoTIFFs (NED, Hydrodem, FDR, FAC),
then derive slope/aspect on the fixed-nodata NEDSnapshot. The `_fixed_`
elevation, slope, and aspect tiles are written as **Cloud-Optimized GeoTIFFs**
(tiled 512 + internal overviews + ZSTD/`PREDICTOR=3`) — they feed the
elevation/slope/aspect VRTs and are consumed only by GDAL/rasterio/QGIS, never
WBT (the WBT-fed `Hydrodem` chain stays LZW-without-predictor). Driven by the
orchestrator batch; single-VPU rebuild:

```bash
VPUS=17 FORCE=1 sbatch --mem=384G slurm_batch/build_shared_rasters.batch --step merge_rpu_by_vpu
```

### Stage 1b — `build_border_dem`

Download Copernicus GLO-30 tiles and build elevation/slope/aspect fill rasters
for HRUs that extend into Canada or Mexico beyond NHDPlus coverage. Creates fill
rasters in `shared/conus/borders/`; the subsequent `build_vrt` step composites
these behind NHDPlus (NHDPlus takes priority where valid; Copernicus fills the
border gaps). Depends on Stage 1.

### Stage 1c1 — `build_vpu_landmask`

Build the per-VPU HRU-fabric land mask consumed by both TWI pipelines. Produces
`shared/per_vpu/<vpu>/land_mask_<vpu>.tif` — a uint8 1/255 raster where 1 =
inside an HRU whose `vpu` attribute matches this VPU, 255 = outside. Rasterised
onto the per-VPU `Hydrodem_merged_<vpu>.tif` grid. Depends only on Stage 1.

### Stage 1c2 — `merge_rpu_by_vpu_twi`

Merge per-RPU TWI rasters (staged in Stage 0) into per-VPU GeoTIFFs
(`Twi_merged_<vpu>.tif`, written as **COGs** — tiled 512 + overviews +
ZSTD/`PREDICTOR=3`, since TWI is GDAL-consumed only, never WBT). Clips its
output to the per-VPU HRU mask from Stage 1c1. Depends on Stage 1c1.
Single-VPU rebuild:

```bash
VPUS=17 FORCE=1 sbatch --mem=384G slurm_batch/build_shared_rasters.batch --step merge_rpu_by_vpu_twi
```

### Stage 2a — `build_vrt`

Combine per-VPU rasters and optional Copernicus fill into CONUS-wide GDAL
virtual rasters (elevation/slope/aspect/fdr/twi). Also builds
`twi_hydrodem.vrt` (open-source WhiteboxTools TWI, CONUS-complete) if
`Twi_hydrodem_*.tif` tiles are present in `per_vpu/`. Each VRT also gets an
external `.vrt.ovr` overview pyramid (bilinear for continuous surfaces; nearest
for fdr and aspect) so full-extent QGIS rendering reads a coarse level instead
of decimating the full-resolution CONUS mosaic (e.g. 231026×128331 for
elevation; the twi lattice differs). Re-running this step alone refreshes the
`.vrt.ovr` files even if the source tiles are unchanged. Rebuild:

```bash
FORCE=1 sbatch slurm_batch/build_shared_rasters.batch --step build_vrt
```

Recovery wrapper for finishing ArcPy TWI tiles + both VRTs in one shot:

```bash
bash slurm_batch/submit_twi_completion.sh
```

This submits three chained jobs: `merge_rpu_by_vpu_twi --force` (all 18 VPUs)
→ `build_vrt --force` (writes `twi.vrt` and `twi_hydrodem.vrt`) →
`twi_reference --force` (writes per-VPU percentile CSVs). Verify afterwards
with `gdalinfo` on both VRTs.

### Stage 2a' — `twi_reference`

Compute valid-land TWI percentile cutoffs per VPU (and CONUS) for each TWI
source (`arcpy`, `hydrodem`). Outputs
`shared/conus/twi_reference_percentiles.arcpy.csv` and
`shared/conus/twi_reference_percentiles.hydrodem.csv`. These tables are the
input to `carea_map threshold_mode: percentile` (Stage 2d). Runs automatically
after `build_vrt` in the orchestrator DAG; run on its own with:

```bash
FORCE=1 sbatch slurm_batch/build_shared_rasters.batch --step twi_reference
```

### Stage 2b — `build_derived_rasters`

Pre-compute `rd_250_raw.tif` and `soil_moist_max.tif`.

### Stage 2c — `build_lulc_rasters`

Pre-compute canopy-resampled + keep-resampled + radiation-transmission rasters
for every LULC source listed in
`configs/shared_rasters/shared_rasters.yml`'s `sources:` block (currently 4
sources: nhm_v11, nalcms, nlcd, foresce).

### Stage 2c' — `compute_breached_fdr` (#147 depression-respecting FDR A/B)

Opt-in step that runs WhiteboxTools least-cost breach on the per-VPU HydroDEM
and writes `Fdr_breached_<vpu>.tif` for each requested VPU.  Not in the
default step list; must be named explicitly.  After staging, run `build_vrt` to
register the tiles into `fdr_breached.vrt`.

**Stage the breached FDR for the two test VPUs (09, 16):**

```bash
sbatch slurm_batch/stage_breached_fdr.batch
```

To rebuild VRTs afterwards (so `fdr_breached.vrt` picks up the new tiles):

```bash
FORCE=1 sbatch slurm_batch/build_shared_rasters.batch --step build_vrt
```

**A/B runs — compare three FDR conditionings on a single VPU:**

For each of the six combinations (`--vpu {09,16}` × `--fdr {production,fill,breach}`):

```bash
pixi run --as-is python scripts/diagnose/ab_drains_to_dprst.py \
    --vpu 09 \
    --fdr production \
    --fdr-vrt "$DR/shared/conus/vrt/fdr.vrt" \
    --per-vpu-dir "$DR/shared/per_vpu" \
    --dprst "$FABRIC_DIR/depstor_rasters/dprst.tif" \
    --vpu-id "$FABRIC_DIR/depstor_rasters/vpu_id.tif" \
    --template "$FABRIC_DIR/shared/${FABRIC}_fdr.vrt" \
    --out-dir "$FABRIC_DIR/depstor_rasters/ab_fdr"
```

Replace `--fdr production` with `--fdr fill` or `--fdr breach` for the other
two runs.  Optionally add `--labels <labeled_dprst_raster>` to get per-depression
contributing-area CSVs alongside the drains raster.

**Coverage summary for a run** (per-VPU `drains_to_dprst==1` land fraction;
`diagnose_drains_to_dprst.py` takes the three rasters explicitly — run once per
A/B output raster):

```bash
pixi run --as-is python scripts/diagnose/diagnose_drains_to_dprst.py \
    --drains "$FABRIC_DIR/depstor_rasters/ab_fdr/drains_to_dprst_09_breach.tif" \
    --vpu-id "$FABRIC_DIR/depstor_rasters/vpu_id.tif" \
    --land   "$FABRIC_DIR/depstor_rasters/land_mask.tif"
```

FDR sources: `production` = `fdr.vrt` (NHDPlus FdrFac, fully filled);
`fill` = `Fdr_hydrodem_<vpu>.tif` (richdem fill-all on HydroDEM);
`breach` = `Fdr_breached_<vpu>.tif` (depression-respecting, this work).
`fill` vs `breach` isolates the conditioning; `production` vs `fill` isolates
the DEM/stream-burn difference.  See
[`docs/superpowers/specs/2026-06-29-depression-respecting-fdr-design.md`](../docs/superpowers/specs/2026-06-29-depression-respecting-fdr-design.md).

---

### Stage 2d depstor detail

Build the full depression-storage raster stack on a fabric-bounds template grid.
Outputs go to `{fabric}/depstor_rasters/` and feed the Stage 4 depstor
zonal-stats orchestrator.

**Template / FDR clip.** `template_raster` and `fdr_raster` are a fabric-bounds
clip of the CONUS FDR, staged with:

```bash
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric <name>
```

This writes `{data_root}/<name>/shared/<name>_fdr.vrt` — a zero-copy VRT clip
on the hydrology lattice. The clip scopes compute to the fabric extent and is
required because `carea_map` requires the template to share the hydrology
lattice with `twi.vrt`. The `elevation.vrt` is on the offset DEM lattice and is
rejected; never substitute it.

**Other inputs (per-fabric profile):**
- `twi_raster` — CONUS `shared/conus/vrt/twi.vrt`; warp-windowed onto the template.
- `hru_gpkg`, `segments_gpkg`/`segments_layer`, `waterbody_gpkg`/`waterbody_layer` (waterbody is required; the step raises if unset).
- `imperv_source` in `configs/depstor/depstor_rasters.yml` — NLCD fractional-impervious raster.

**DAG order:** landmask → imperv / waterbody → endorheic → wbody_connectivity →
dprst → perv → hru_id → dprst_depth → vpu_id → routing → routing_hru →
drains_perv / drains_imperv → carea_map. `endorheic` emits
`endorheic_waterbody_comids.parquet` (Signal A: FDR terminus-inside-itself;
Signal B: majority-inside a closed WBD HUC12, when `wbd_huc12_table` is
configured); `wbody_connectivity` subtracts this set from the unioned
on-stream COMIDs — a STRICT SUBTRACTION, never additive — and also rasterizes
the full endorheic set (regardless of on-stream status) to a second output,
`endorheic_wbody.tif`, which `dprst` consumes (see "Endorheic clump-veto
exemption" below). If `endorheic` hasn't been run for a fabric (no
`endorheic_comids` in the build context), `wbody_connectivity` **raises**
rather than proceeding without the demotion. A fabric whose domain has no closed basin (e.g.
`tjc`, Texas-Gulf) legitimately produces an EMPTY endorheic table and carries on
— the demotion is then a no-op. On a fabric that DOES expect demotions, set
`min_endorheic_comids` in its profile (gfv2: 100): the `endorheic` step then
fails loud if the classifier collapses below that floor. Selective re-runs via `--step <name>` or
`--from <name>` passed through to the Python script. `dprst_depth` (issue
#173) is a CONUS-scale compute outlier in this DAG — see "Stage 2d'" below; it
needs its own SLURM array run **before** a full unfiltered
`build_depstor_rasters.batch`, or that job will attempt an unbounded
in-process fallback compute at the `dprst_depth` step.

**Endorheic classifier inputs.** Signal A needs only `fdr_raster` (already
required on every fabric) and needs no extra staging. Signal B and the
BurnAddWaterbody union into `waterbody` are optional and staged once,
CONUS-wide, fabric-independent:

```bash
pixi run --as-is python -m gfv2_params.download.nhd_burn_components   # Sink.shp + BurnAddWaterbody
pixi run --as-is python -m gfv2_params.download.wbd_huc12             # full WBD (type-C closed basins)
```

Do **not** substitute the pre-made `input/nhd/NHD_sink_points.gpkg` (a strict
subset of NHDPlus's `Sink.shp` — 537 sinks vs 3,222 in VPU 16 — that omits
`PURPCODE 1` entirely and has 0 sinks inside Great Salt Lake, where NHDPlus has
29) or `input/nhd/closed_huc12.gpkg` (23 type-C HUC12s in the Great Basin vs
141 in the full WBD). Both are incomplete extracts; stage from source via the
two commands above.

**On-stream staging.** The `wbody_connectivity` builder unions two COMID
parquets: `connected_waterbody_comids.parquet` (WBAREACOMI, from
`download_nhd_flowlines.batch`) and `flowthrough_waterbody_comids.parquet`
(flow-through topology, from
`python -m gfv2_params.download.nhd_flowthrough`). **Both** staging steps now
require `input/nhd/flowline_topology.parquet` (distilled NHDPlus
PlusFlowlineVAA, staged by `python -m gfv2_params.download.nhd_topology` —
**run it first**; both fail loud if the parquet is missing): on-stream
promotion is gated on **Network-Flowline membership** (a COMID present in the
VAA), so a waterbody NHD tagged only via Non-Network flowlines — the artificial
paths NHD draws through closed-basin lakes — stays depression storage instead
of being wrongly promoted on-stream (issue #161). Topology also drives the
flow-through D1 rule, which uses authoritative routed-network direction to
promote source/headwater lakes and split-pass-through outflows. Both staging
steps are per-VPU vector operations — sized like `nhd_flowlines` with no CONUS-grid
array and no 384G concern — and run on the login node or in a lightweight
SLURM job. Updating either COMID parquet after an initial build requires
rebuilding from `wbody_connectivity`:

```bash
sbatch slurm_batch/build_depstor_rasters.batch --from wbody_connectivity --force
```

**Endorheic clump-veto exemption.** Demoting the Great Salt Lake's COMIDs out
of the on-stream set (above) is not sufficient by itself: `clump_regions`
labels 8-connected waterbody components, and `regions_touching_mask` excludes
a WHOLE region from `dprst` if any one cell touches the on-stream mask. The
Great Salt Lake is 8-connected to a 49.1 km² inflow SwampMarsh (COMID
10273192) that is correctly left on-stream, so the region-level exclusion
vetoed the entire merged region — silently excluding all 4,854,156 Great Salt
Lake cells from depression storage even after the COMID demotion.
`dprst.py` fixes this using `endorheic_wbody.tif`: it exempts a waterbody's
own cells from the region-level exclusion wherever `endorheic_wbody == 1 AND
connected_wbody != 1` — direct hydrologic evidence (terminus-inside-itself)
overrides the clump proxy, but only for the waterbody's own not-on-stream
cells; the marsh's own cells stay excluded because they ARE on-stream. Runs
before the impervious carve and land mask, so both still apply to recovered
cells. `endorheic_wbody` is optional in the build context — a fabric that
hasn't run `endorheic` gets no exemption, a pure no-op, matching pre-fix
behaviour exactly. This is deliberately narrower than a global per-cell
on-stream carve (which would additionally recover ~8,471 km² of
non-endorheic waterbodies whose clump merely abuts an on-stream feature) —
those keep today's clump behaviour.

**waterbody/endorheic rebuild cascade.** Changing the waterbody layer (e.g. a
new BurnAddWaterbody union) or the on-stream COMID set re-runs
`waterbody → endorheic → wbody_connectivity → dprst → routing →
drains_perv/drains_imperv` — `--mem=384G` for `waterbody`/`dprst`, `96G` for
`routing`. Rebuild with `--from waterbody --force` (or `--from endorheic
--force` if only the endorheic signals/inputs changed and the waterbody layer
itself did not).

**dprst rebuild cascade.** Changing `dprst` membership (e.g. the per-cell
impervious carve-out, or the on-stream COMID set) invalidates everything
downstream in the DAG: `perv`, `routing` → `drains_perv`/`drains_imperv`,
and `carea_map` (it consumes `onstream` + `perv`). Rebuild with
`--from dprst` and `FORCE=1`, then re-run the depstor zonal + merge for the
affected fractions (`dprst_frac`, `perv_frac`, `drains_*_frac`,
`onstream_storage_frac`, `carea_*`, `sro_to_dprst_*`).
`waterbody`, `wbody_connectivity`, `vpu_id`, and `landmask` are upstream and
unaffected.

**`vpu_id` step.** Rasterises the HRU fabric's `vpu` attribute onto the template
grid. Required by `carea_map` when `threshold_mode: percentile` and the fabric
spans multiple VPUs. Single-VPU fabrics set `vpu: "<id>"` in their profile and
skip this step.

**Memory ceiling (`waterbody` + `dprst`).** These two full-grid region steps
are the CONUS memory ceiling: `waterbody`'s 8-connectivity clump and `dprst`'s
`regions_to_binary` over the whole 153830×109901 grid peak around **384 GB** and
**OOM at 192 GB**. The `build_depstor_rasters.batch` default is therefore
`--mem=384G`; do not lower it for a full-DAG CONUS build (a single `--step
dprst`/`--step waterbody` rerun needs it too).

**`routing` memory.** Tiles the in-process D8 routing pass per VPU — each VPU
is routed in isolation (FDR masked to the VPU) and the results are mosaicked.
Peak memory ~80 GB measured for CONUS. A routing-only rerun can drop to
`--mem=96G` (`sbatch --mem=96G slurm_batch/build_depstor_rasters.batch --step
routing`), but the full build stays at the 384 GB default above.

**`carea_map` threshold modes** (configured in `configs/depstor/depstor_rasters.yml`):

- `threshold_mode: absolute` — uses the 8.0/15.6 thresholds. Requires ArcPy
  `twi.vrt` as `twi_raster`. Only calibrated for VPU 01 / `gfv2_vpu01`.
- `threshold_mode: percentile` — derives the TWI cutoff from the per-VPU
  reference table produced by Stage 2a'. Source-agnostic; use with `twi.vrt`
  (ArcPy) or `twi_hydrodem.vrt` (open-source). The `reference_table` key in
  `depstor_rasters.yml` and the profile's `twi_raster` must be set to the same
  source together — there is no auto-selection or guard for the pairing.

For multi-VPU or non-VPU-01 fabrics (e.g. `oregon`), use
`threshold_mode: percentile` with `twi_raster` pointing at `twi_hydrodem.vrt`;
the percentile cutoffs come from Stage 2a'.

```bash
sbatch slurm_batch/build_depstor_rasters.batch

# VPU01 validation overlay — override resources:
FABRIC=gfv2_vpu01 sbatch --time=02:00:00 --mem=48G \
    slurm_batch/build_depstor_rasters.batch

# Resume from a specific step (e.g. after a routing crash):
sbatch slurm_batch/build_depstor_rasters.batch --from routing --force
```

---

### Stage 2d' — `dprst_depth` (issue #173): its own SLURM array

`dprst_depth` produces `dprst_depth.tif` + `op_flow_thres_params.csv`
(`{fabric}/depstor_rasters/`) and, via a second driver, the per-HRU
`nhm_dprst_depth_avg_params.csv` (`{fabric}/params/merged/`). Unlike every
other depstor step, its cost scales with the ~286k CONUS dprst **polygons**
(one windowed DEM read each, ~250-500 core-hours run serially), not the
CONUS grid — see `docs/ARCHITECTURE.md`'s "CONUS-scale COMPUTE" gotcha.
Run **before** `build_depstor_rasters.batch`'s unfiltered walk reaches
`dprst_depth` in the DAG order (Stage 2d above), or that job attempts the
in-process fallback, which for CONUS is effectively unbounded wall-clock on
one core.

**Inputs (per-fabric profile, in addition to Stage 2d's):** `wesm_index`
(pre-staged 1m/QL1/QL2 WESM workunit footprints — `pixi run python -m
gfv2_params.download.wesm`) and `ecoregions_gpkg` (EPA L3 Ecoregions —
`pixi run python -m gfv2_params.download.epa_ecoregions`, Stage 0). Both are
already staged for `gfv2`/`gfv2_dev`/`oregon`/`tjc` in
`configs/base_config.yml`; `gfv2_vpu01` has neither (same reason it lacks
`connected_comids_table` — its `wbs` waterbody layer has no COMID).

**The 4-stage DAG (`slurm_batch/submit_dprst_depth.sh`):**

1. **Plan** (`plan_dprst_depth_batches.batch`, 1 task) — reconstructs the
   CONUS dprst polygon set, **clips it to the fabric's HRU extent** (same
   shared `topo.load_fabric_dprst_polygons` the builder uses — see the
   "Fabric clip" note below), tags it (`best_topo` via WESM, `ecoregion`
   via EPA L3), builds the tile → polygon work-list
   (`tiling.group_by_tile`), and bins it into `N_TILE_BATCHES`
   SLURM-array batches (`tiling.component_tile_batches` — greedy
   longest-processing-time bin-packing by estimated DEM-window-read COST
   (`tiling.polygon_window_cost`/`MAX_1M_WINDOW_CELLS`, ~100x weight for 1m
   vs 10m polygons), not raw polygon count, connected-component safe so a
   polygon spanning >1 tile is never split across batches).
   Writes `{fabric}/depstor_rasters/dprst_depth_batches/_plan/
   {dprst_polygons_tagged.parquet, batch_manifest.json}`. Pure geometry +
   local vector reads — no live S3/vsicurl.
2. **Array** (`run_dprst_depth_batch.batch`, `0..N_TILE_BATCHES-1`, afterok
   the plan) — one array task computes `dprst_depth` for ONE tile-batch
   (`scripts/run_dprst_depth_batch.py` → `dprst_depth.compute.run_batch`),
   writing `dprst_depth_batches/batch_XXXX.parquet`. **Deliberately not
   concurrency-throttled** — the ≤5 hr CONUS target only holds if
   `N_TILE_BATCHES` tasks actually run concurrently (see sizing below).
3. **Build** (reuses `build_depstor_rasters.batch --step dprst_depth`,
   afterok the array, `--mem=64G --time=02:00:00` override — dprst_depth's
   own compute is vector-scale + a streamed row-strip burn, not a
   full-CONUS-grid materialization, so it doesn't need the 384G/18h whole-DAG
   default) — finds the populated `dprst_depth_batches/*.parquet`, fills
   every flat/degenerate polygon (`fit_ecoregion_models`/`fill_flat`), burns
   `dprst_depth.tif`, and writes `op_flow_thres_params.csv` +
   `dprst_depth_polygons.parquet` (the per-polygon fill-method provenance
   companion, Task 8).
4. **Mean zonal + finalize** (`mean_zonal_dprst_depth.batch` array over the
   fabric's HRU batches, afterok the build; then
   `mean_finalize_dprst_depth.batch`, afterok the array) —
   `derive_depstor_params.py --mode mean_zonal`/`mean_finalize --mean
   dprst_depth_avg`: per-HRU exactextract MEAN of `dprst_depth.tif`, metres
   → inches, floor every zero-dprst-cell HRU at the NHM calibrated median
   (49 in, never NaN), join area-weighted dominant fill-method provenance →
   `{fabric}/params/merged/nhm_dprst_depth_avg_params.csv`. No ratio step
   (a mean needs no numerator/denominator, unlike the fraction params).

```bash
BATCHES={data_root}/{fabric}/batches   # HRU batch manifest — drives stage 4
slurm_batch/submit_dprst_depth.sh "$BATCHES" gfv2 configs/base_config.yml 150
```

**Sizing (≤5 hr CONUS target, stages 1-2):** `N_TILE_BATCHES` (4th positional
arg, default 150) trades array width for wall-clock:
`wall-clock ≈ (250-500 core-hours) / N_TILE_BATCHES` — 150 batches ≈
1.7-3.3 hr, comfortable margin under 5 hr even with imperfect load balance.
Dry-run the exact per-batch balance and projected wall-clock for a candidate
`N_TILE_BATCHES` before submitting (pure geometry — no live S3; safe on the
head node for a small fabric, but see the note below about `oregon`):

```bash
pixi run python -m gfv2_params.dprst_depth.tiling --plan \
    --fabric gfv2 --n-batches 150
```

> **Fabric clip.** Every fabric profile's `waterbody_gpkg` points at the
> shared CONUS `conus_waterbodies.gpkg`, so the dprst polygon set is
> reconstructed CONUS-wide (~321k polygons) and then **clipped to the
> fabric's HRU extent** (`topo.load_fabric_dprst_polygons` →
> `_clip_dprst_to_fabric`: HRU-bbox prefilter + `sjoin(predicate="intersects")`
> against the HRU polygons) — the SAME shared helper both the plan hook and
> the builder call, so the two can't diverge. A regional fabric therefore
> plans/processes only its own dprst polygons (e.g. `oregon` → ~3,100, not
> 321k; the plan runs in ~8-15 s), while `gfv2` (whose HRU extent IS CONUS)
> correctly keeps essentially all of them. The plan's projected wall-clock
> scales the CONUS core-hour estimate by the fabric's actual polygon count,
> so a regional dry run reports a sane sub-hour projection rather than
> inheriting the CONUS figure.

**Recovery:**

- A failed/timed-out array task (stage 2) only owns its own
  `batch_XXXX.parquet` — resubmit just that index:
  `sbatch --array=<idx> --export=ALL,BASE_CONFIG=...,FABRIC=... slurm_batch/run_dprst_depth_batch.batch`.
- Re-running the plan step (stage 1) is idempotent and cheap relative to the
  array — it only rewrites `_plan/*`, never touches the per-batch parquets.
- If `dprst_depth.tif`/`op_flow_thres_params.csv` need a full rebuild (e.g.
  after a WESM/ecoregion re-stage), pass `--force`:
  `sbatch slurm_batch/build_depstor_rasters.batch --step dprst_depth --force`
  (re-run stages 1-2 first if the polygon set itself changed).
- `mean_zonal`/`mean_finalize` (stage 4) only depend on `dprst_depth.tif`
  already existing — safe to re-run standalone any time after stage 3
  completes, without re-running stages 1-3.

---

## Stage 3 fabric prep detail

### Stage 3a — `merge_vpu_targets` (compute-node only)

Merge per-VPU fabric geopackages (`nhru` layer) into a single CONUS fabric gpkg.
Loads the full CONUS fabric into memory. **Run on a compute node only** — use
JupyterHub on a compute node, or `salloc` for an interactive session. Never run
on the login node.

```bash
# In JupyterHub on a compute node, or in an salloc session:
pixi run -e notebooks marimo run notebooks/merge_vpu_targets.py
```

### Stage 3b — `merge_vpu_segments`

Merge the per-VPU `nsegment` layers into a single CONUS stream-segments gpkg
(VPU-based fabrics only). Outputs
`{data_root}/gfv2/fabric/gfv2_nsegment_merged.gpkg` (layer `nsegment`). Submit
via the batch:

```bash
sbatch slurm_batch/merge_vpu_segments.batch
# Other fabrics:
FABRIC=<name> sbatch slurm_batch/merge_vpu_segments.batch
```

Idempotent; pass `--force` to rebuild. The `segments_gpkg` is no longer consumed
by any depstor step — the `streambuffer` step is retired; depstor connectivity
is now NHD-WBAREACOMI-driven (see `wbody_connectivity` builder). Routing
connectivity comes from the FDR raster, so merged-segment graph topology is not
required here.

### Stage 3c — `prepare_fabric`

Spatially batch the merged fabric into per-batch geopackages (KD-tree recursive
bisection) and write a manifest. Loads the whole CONUS fabric into memory; runs
as a SLURM job via the batch wrapper:

```bash
sbatch slurm_batch/prepare_fabric.batch
# Other fabrics:
FABRIC=<name> sbatch slurm_batch/prepare_fabric.batch
```

`hru_gpkg`/`hru_layer` and `batch_size` are read from the active profile in
`configs/base_config.yml`; no `--fabric_gpkg` is needed. `--fabric_gpkg` /
`--layer` remain as optional overrides for one-off runs.

---

## Stage 4A — Incremental per-parameter runs

> The runbook ([RUNME.md](RUNME.md) Step 4) now walks this per-parameter batch sequence explicitly; this section keeps the finer points — the array throttle, single-parameter reruns, and the ssflux→slope dependency mechanics.

Each parameter is a two-step unit: an array job over every HRU batch, then a
merge that runs `afterok` it.

Common preamble:

```bash
BATCHES=/path/to/gfv2/batches            # holds manifest.yml
FABRIC=gfv2
BASE_CONFIG=configs/base_config.yml
N=$(grep '^n_batches:' "$BATCHES/manifest.yml" | awk '{print $2}')
THROTTLE=4
```

**Zonal params** — run in this order (`slope` must be merged before `ssflux`):

| # | parameter | | # | parameter |
|--|--|--|--|--|
| 1 | elevation | | 6 | lulc_nhm_v11 |
| 2 | slope | | 7 | lulc_nalcms |
| 3 | aspect | | 8 | lulc_nlcd |
| 4 | soils | | 9 | lulc_foresce |
| 5 | soil_moist_max | | 10 | ssflux *(special — see below)* |

```bash
P=elevation                              # repeat for each parameter in the table
AID=$(sbatch --parsable --array=0-$((N-1))%$THROTTLE \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,PARAM=$P \
      slurm_batch/derive_zonal_params.batch)
sbatch --dependency=afterok:$AID \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,PARAM=$P \
      slurm_batch/merge_zonal_param.batch
```

`ssflux` needs the CONUS-wide P2P weight matrix and the merged `slope` CSV
first. Build the weight matrix (idempotent; export `FORCE=1` to rebuild), then
submit ssflux like any other parameter:

```bash
sbatch --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC \
    slurm_batch/build_zonal_weights.batch
# after weights finish, submit ssflux's array + merge as above with P=ssflux
```

**Depstor params** — same two-step unit per fraction, then one ratios job after
all fractions have merged:

Fractions: `perv_frac`, `imperv_frac`, `dprst_frac`, `drains_perv_frac`,
`drains_imperv_frac`, `onstream_storage_frac`, `drains_to_dprst_frac`,
`carea_t8_frac`, `carea_t156_frac`, `hru_total`

```bash
F=perv_frac                              # repeat for each fraction
AID=$(sbatch --parsable --array=0-$((N-1))%$THROTTLE \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,FRACTION=$F \
      slurm_batch/create_depstor_zonal.batch)
sbatch --dependency=afterok:$AID \
      --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC,FRACTION=$F \
      slurm_batch/merge_depstor_fraction.batch

# Once all 10 fraction merge jobs have COMPLETED (check `squeue -u $USER`),
# derive the 6 PRMS ratios:
sbatch --export=ALL,BASE_CONFIG=$BASE_CONFIG,FABRIC=$FABRIC \
      slurm_batch/derive_depstor_ratios.batch
```

For a quick single-batch sanity check without SLURM:

```bash
pixi run python scripts/derive_zonal_params.py --mode zonal --param elevation --batch_id 42 \
    --config configs/zonal/zonal_params.yml --base_config configs/base_config.yml
```

`--mode merge --param <name>` runs just the merge; `--mode build_weights` builds
the ssflux prereq.

---

## Stage 4B — Wholesale wrappers

Each wrapper loops the per-parameter steps from Stage 4A — the same array +
merge (+ ratios) jobs, chained via `afterok`:

```bash
slurm_batch/submit_zonal_params.sh   $BATCHES $FABRIC $BASE_CONFIG   # all 10 zonal params
slurm_batch/submit_depstor_params.sh $BATCHES $FABRIC $BASE_CONFIG   # 10 fractions + ratios
slurm_batch/submit_dprst_depth.sh    $BATCHES $FABRIC $BASE_CONFIG   # dprst_depth_avg (its own array — see Stage 2d')
```

`nhm_dprst_depth_avg_params.csv` is **not** produced by `submit_depstor_params.sh`
— it has its own tile-batch SLURM array (a `means:` aggregation, not a
`fractions:`/`ratios:` one) and lives entirely in `submit_dprst_depth.sh`;
see "Stage 2d'" above for its 4-stage DAG and sizing.

`submit_zonal_params.sh` auto-detects ssflux's `depends_on: build_weights`:
submits `build_zonal_weights.batch` first and chains the ssflux array on its
`afterok` (and on the merged slope CSV). `submit_depstor_params.sh` chains the
single ratios job on every fraction's merge.

Env knobs (both wrappers):

- `FABRIC=gfv2_vpu01` — non-default fabric (or pass as the 2nd positional arg)
- `SUBMIT_JOBS_MAX_CONCURRENT=4` — array concurrency cap (or the 4th positional arg to `submit_zonal_params.sh` / `submit_depstor_params.sh`)
- `FORCE=1` — rebuild the ssflux weight matrix
- `ZONAL_PARAMS="elevation slope ..."` (submit_zonal_params.sh only) — run just
  the listed subset instead of all 10, for fabrics whose inputs are unstaged
  (e.g. omit `lulc_nlcd`/`lulc_foresce` when their CONUS rasters aren't present).
  Keep `slope` before `ssflux` in the list.

**Depstor outputs** land in two subdirectories under `{fabric}/params/merged/`:

- `{fabric}/params/merged/` — **6 final PRMS-ready ratio CSVs**, dimensionless
  and bounded in [0, 1]: `sro_to_dprst_perv`, `sro_to_dprst_imperv`,
  `carea_max`, `smidx_coef`, `hru_percent_imperv`, `dprst_frac`.
- `{fabric}/params/merged/_intermediates/` — **10 per-fraction count CSVs**
  (`nhm_<name>_frac_params.csv` and `nhm_hru_total_count_params.csv`). Each
  row's `count` column is the partial-pixel-weighted sum of `1`-valued cells
  per HRU — **not** a [0, 1] fraction. Inputs to ratio derivation; not direct
  PRMS parameters. To get a true area fraction divide by the HRU pixel count
  (e.g. `areasqkm * 1e6 / 900` for the 30 m template grid; the `hru_total`
  fraction aggregates `land_mask.tif` to give that denominator).

---

## Stages 5–9

### Stage 5 — Merge and validate

Merging is part of Stage 4: the by-parameter path (4A) submits each merge as
the second command of every unit; the wholesale wrappers (4B) chain it via
`afterok`. To re-run a single param's merge after manually fixing a batch CSV:

```bash
pixi run python scripts/derive_zonal_params.py --mode merge --param elevation \
    --config configs/zonal/zonal_params.yml --base_config configs/base_config.yml
```

### Stage 6 — SSFlux (depends on merged slope)

Handled automatically by `submit_zonal_params.sh` — the dispatcher submits
`build_zonal_weights.batch` first and chains the ssflux array + merge on its
`afterok` and on the merged slope CSV. To build the CONUS-once weight matrix on
its own:

```bash
sbatch slurm_batch/build_zonal_weights.batch
```

Idempotent — skips if the matrix already exists; pass `FORCE=1` to overwrite.

### Stage 7 — KNN gap-fill

Fits sklearn NearestNeighbors over every HRU and fills all param columns. Loads
the full fabric into memory; submit via the batch:

```bash
sbatch slurm_batch/merge_and_fill_params.batch
# Other fabrics:
FABRIC=<name> sbatch slurm_batch/merge_and_fill_params.batch
```

### Stage 8 — Merge NHM defaults

```bash
sbatch slurm_batch/merge_default_output_params.batch
# Other fabrics:
FABRIC=<name> sbatch slurm_batch/merge_default_output_params.batch
```

### Stage 9 — View results

The notebooks in `notebooks/fabric_results/` view a fabric's full
parameterization — input rasters, depstor rasters, and per-HRU param maps.
They are parameterised by the `FABRIC` env var. **Run them in JupyterHub on a
compute node** (not the login node — a full CONUS `gfv2` render loads ~361k HRU
polygons).

To regenerate the figure set headlessly:

```bash
sbatch slurm_batch/render_figures.batch
# Other fabrics:
FABRIC=<name> sbatch slurm_batch/render_figures.batch
```

Outputs: `docs/figures/<name>/{input_raster_*,depstor_*,param_*}.png`.
JupyterHub is the interactive alternative for exploratory inspection.

---

### Stage 10 — Snow depletion curves (SNODAS → snarea_curve)

Three fabric-independent stages deriving the PRMS `snarea_curve`/`hru_deplcrv`
parameters from daily SNODAS SWE. Stage 1/2 use the empirical Driscoll, Hay &
Bock (2017) method (design spec
[`docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md`](../docs/superpowers/specs/2026-07-04-snodas-snarea-curve-design.md));
Stage 3 builds a CV/lognormal curve library from Stage 2's output (Sexstone,
Driscoll, Hay, Hammond & Barnhart 2020; design spec
[`docs/superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md`](../docs/superpowers/specs/2026-07-06-snodas-snarea-curve-library-design.md)).
Stage 1 is a SLURM array over the fabric's spatial batches (same pattern as
the depstor/zonal parameter jobs — see "Stage 4A" above), then a merge job,
then Stage 2 (derive) and Stage 3 (library). The one-command recipe
`submit_snarea_pipeline.sh` submits all four chained `--dependency=afterok`:

```bash
# Recommended: whole pipeline, one afterok chain, IDs printed. Sizes the
# Stage-1 array from the manifest and picks Stage-2 --mem by fabric
# (64G oregon / 384G CONUS default). DRYRUN=1 echoes the sbatch commands.
DRYRUN=1 ./slurm_batch/submit_snarea_pipeline.sh <fabric>   # inspect
./slurm_batch/submit_snarea_pipeline.sh <fabric>            # submit
# Env: STAGE2_MEM / STAGE2_TIME override Stage 2 sizing; MAX_CONCURRENT sets
# the Stage-1 array %K throttle (default 8); CLEAR_BATCHES=1 wipes
# {fabric}/snodas/_batches/ first. Trailing args forward to every sbatch.
```

Manual per-stage commands (when you want to inspect between stages):

```bash
# Stage 1 — aggregate daily SNODAS SWE to the HRU fabric, per spatial batch
BATCHES=/path/to/<fabric>/batches            # holds manifest.yml
FABRIC=<name>
N=$(grep '^n_batches:' "$BATCHES/manifest.yml" | awk '{print $2}')
AID=$(sbatch --parsable --array=0-$((N-1)) --export=ALL,FABRIC=$FABRIC \
    slurm_batch/derive_snodas_aggregate.batch)
MID=$(sbatch --parsable --dependency=afterok:$AID --export=ALL,FABRIC=$FABRIC \
    slurm_batch/merge_snodas_aggregate.batch)
# Stage 2 — derive per-HRU empirical snarea_curve + sub-grid CV from the aggregated SWE/SCA
#   (--mem=384G CONUS default; override down for small fabrics, e.g. oregon:
#    FABRIC=oregon sbatch --mem=64G --time=02:00:00 slurm_batch/derive_snarea_curve.batch)
sbatch --dependency=afterok:$MID --export=ALL,FABRIC=$FABRIC \
    slurm_batch/derive_snarea_curve.batch
# Stage 3 — build the CV/lognormal curve library from the Stage 2 derived CSV
FABRIC=<name> sbatch slurm_batch/derive_snarea_library.batch
```

**Re-run Stage 1 (`swe_std`):** Stage 1 emits a per-HRU `swe_std` sidecar that
Stage 3's CV needs. NetCDFs aggregated before it was added lack `swe_std`, so
Stage 2 raises `ValueError("...missing swe_std... Re-run Stage 1...")`. The
recipe always re-runs Stage 1; gdptools weights are cached (`weights_agg/`) so
the re-run is a cheap extra `masked_std` AggGen pass, not a weight recompute.

**Stage 1** (`src/gfv2_params/aggregate/`, config
`configs/aggregate/aggregate_sources.yml`) wraps gdptools `UserCatData` /
`WeightGen` / `AggGen` behind a declarative `SourceAdapter` — the time-series
counterpart to the static-raster `zonal_runners`. `derive_snodas_aggregate.batch`
runs `scripts/derive_aggregate.py --mode aggregate --batch_id "$SLURM_ARRAY_TASK_ID"`,
one array task per spatial batch (`{data_root}/{fabric}/batches/batch_<N>.gpkg`);
each task reads raw daily SNODAS NetCDFs (default path
`{data_root}/../nhf-datastore/snodas/daily`, overridable per-fabric via the
profile's `snodas_dir` key), clips the source grid to that batch's extent
(`aggregate_source`/`subset_to_gdf_bounds` in
`src/gfv2_params/aggregate/driver.py`), and writes
`{data_root}/{fabric}/snodas/_batches/snodas_batch<NNNN>_agg_<year>.nc` plus a
per-batch cached gdptools weight CSV under `{data_root}/{fabric}/weights_agg/`.
`merge_snodas_aggregate.batch` (`--mode merge`, `run_merge()` in
`scripts/derive_aggregate.py`) then concatenates every batch's per-year
NetCDFs on the id_feature dim (sorted) into
`{data_root}/{fabric}/snodas/snodas_agg_<year>.nc` — the file Stage 2 globs
for. Per-batch parts stay in the `_batches/` subdir so Stage 2's top-level
`*_agg_*.nc` glob only ever sees the merged files. Omitting `--batch_id`
(direct `pixi run`, not via `sbatch`) still runs the old whole-fabric path
against `hru_gpkg` for ad hoc/small-fabric use. The SNODAS adapter
(`src/gfv2_params/aggregate/snodas.py`) sets `std_variables=("swe",)`, so every
aggregated NC also carries a `swe_std` sidecar (per-cell SWE std dev within the
HRU, masked/aggregated the same way as `swe`) — the input Stage 2's sub-grid CV
needs. Re-run Stage 1 once against a fabric aggregated before `swe_std` was
added; the per-batch gdptools weight CSVs are cached, so the re-run is cheap.

**Stage 2** (`src/gfv2_params/snarea/`, config
`configs/snarea/snarea_curve.yml`) reads the Stage 1 merged NetCDFs, builds
per-HRU melt-season depletion curves, applies the six Driscoll selection
criteria (HRUs that fail fall back to a configured default curve, flagged in
`sdc_status`), computes each HRU's sub-grid SWE coefficient of variation
(`cv_subgrid`, from the `swe_std`/`swe` ratio at peak SWE —
`src/gfv2_params/snarea/subgrid.py`), and writes the **intermediate**
`{data_root}/{fabric}/params/merged/_intermediates/nhm_snarea_curve_derived.csv`
(not the terminal params — that is now Stage 3's job).

**Stage 3** (`src/gfv2_params/snarea/library.py`, driver
`scripts/derive_snarea_library.py`, config `configs/snarea/snarea_library.yml`,
batch `derive_snarea_library.batch`) reads the Stage 2 derived CSV and builds a
CV/lognormal curve library: fits each derived HRU's empirical curve to a
lognormal CV (`fit_cv`), validates/calibrates `cv_subgrid` against that
empirical CV on the overlap (monotone quantile-map, gated by median bias —
`validate_and_calibrate`), bins the calibrated population into `ndepl_cv`
equal-population CV bins plus one reserved default curve, and assigns each HRU
its nearest-CV library curve. It is pure tabular pandas/numpy — no daily-SWE
reload — so it is cheap (`--mem=16G --time=00:30:00`) and safely re-runnable at
any `ndepl_cv` without redoing Stage 1/2. Outputs, all under
`{data_root}/{fabric}/params/merged/`: `nhm_snarea_curve_library.csv` (one row
per `deplcrv_id`), `nhm_snarea_curve_params.csv` (one row per HRU:
`hru_deplcrv`, `snarea_thresh`, CV diagnostics, the assigned curve),
`nhm_snarea_curve_validation.csv` (calibration report: reconstruction error
before/after, bias, `n_estimable`), and `nhm_snarea_curve.nc` (the pyWatershed
parameter file — `snarea_curve` flat ascending, `hru_deplcrv`,
`snarea_thresh`).

**Cost profile:** Stage 1's gdptools weight-generation (`WeightGen`, one
polygon/cell intersection pass per batch) is the expensive step; each batch's
weight CSV is cached to `weight_dir` and reused on subsequent runs (including
across years) unless removed. The per-year `AggGen` aggregation itself is
cheap once weights exist. Stage 2 is a pure per-HRU pandas/numpy computation
over the already-aggregated daily series — cheap per HRU, but its whole-fabric
load is not: `read_daily_by_hru`'s `to_dataframe()` materializes ~21 years ×
`n_hru` × daily rows at once (CONUS gfv2 ≈ 2.8 B rows, ~344 GB peak, ~31 min just
to load). `derive_snarea_curve.batch` therefore **defaults to `--mem=384G`**
(64 GB OOMs on CONUS); Oregon and other small fabrics fit easily and can override
down (`sbatch --mem=64G --time=02:00:00 …`). The load-phase and per-HRU
derive-loop emit progress logging so the long silent stretch is traceable.

**PROJ behind the HPC firewall.** conda-forge `proj` defaults `PROJ_NETWORK=ON`
unless `proj-data` is installed; the firewall blocks `cdn.proj.org`, so gdptools'
centroid `to_crs(4326)` returns `inf` and crashes whole Stage 1 batches (11/64 on
the first CONUS gfv2 run — valid geometries, coordinates transform to `inf`).
Two guards: `derive_aggregate.py` forces PROJ's grid-free transform
(`pyproj.network.set_network_enabled(False)`; even the grid-free NAD83↔WGS84
fallback is ≤~1–2 m — irrelevant for cosmetic CF centroids, and all SWE/SCA math
is in equal-area 5070), and `proj-data` is a pinned pixi dep (grids local, so the
transform is actually sub-meter) so `proj4-activate.sh` sets
`PROJ_NETWORK=OFF` env-wide. Any other pipeline reprojecting to 4326 relies on
the latter.

The Stage 1 SWE aggregation can optionally be cross-checked against the
sibling `gfv2-spatial-targets` repo's `snodas_<year>_agg.nc` oracle.

---

## Adding a new fabric

A new fabric is added by appending a profile to `configs/base_config.yml` —
one file edit, no new YAMLs. Two cases:

### Case A: Pre-merged fabric (single gpkg — e.g., oregon)

1. Register the fabric and scaffold output directories:
   ```bash
   pixi run init-data-root --add-fabric oregon
   ```
   Fill the stub's TODO placeholders. Required fields: `expected_max_hru_id`,
   `batch_size`, `id_feature`, `hru_gpkg`/`hru_layer`. For depstor, also set
   `template_raster`, `fdr_raster`, `twi_raster`,
   `segments_gpkg`/`segments_layer`, `waterbody_gpkg`/`waterbody_layer`.
   Stage the `template_raster`/`fdr_raster` clip:
   ```bash
   pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric <name>
   ```
   For a single-file fabric like `oregon`, `segments_gpkg` can point at the
   same gpkg as `hru_gpkg` with `segments_layer: nsegment`.

2. Place the fabric gpkg at the `hru_gpkg` path under
   `{data_root}/oregon/fabric/` (NOT in `input/fabric/`).

3. Prepare batches:
   ```bash
   sbatch slurm_batch/prepare_fabric.batch   # FABRIC= override if needed
   ```

4. Submit parameter jobs:
   ```bash
   BATCHES={data_root}/oregon/batches
   slurm_batch/submit_zonal_params.sh $BATCHES oregon configs/base_config.yml
   slurm_batch/submit_depstor_params.sh $BATCHES oregon configs/base_config.yml
   ```

> **TWI source pairing for non-VPU-01 fabrics:** the 8.0/15.6 absolute
> thresholds are only calibrated for VPU 01. For multi-VPU or non-VPU-01
> fabrics, use `threshold_mode: percentile` in
> `configs/depstor/depstor_rasters.yml` with `twi_raster` pointing at
> `twi_hydrodem.vrt`; the percentile cutoffs come from Stage 2a'.

### Case B: VPU-based fabric (per-VPU gpkgs — e.g., gfv2)

1. Register the fabric + scaffold dirs:
   ```bash
   pixi run init-data-root --add-fabric <name>
   ```
   Fill the stub's TODO placeholders.

2. Place per-VPU gpkgs in `input/fabric/`.

3. Merge `nhru` (and, for depstor, `nsegment`):
   ```bash
   # In JupyterHub on a compute node or salloc session:
   pixi run -e notebooks marimo run notebooks/merge_vpu_targets.py
   # depstor only:
   sbatch slurm_batch/merge_vpu_segments.batch   # FABRIC=<name> if needed
   ```

4. Continue from Stage 3c (`prepare_fabric`) with `FABRIC=<name>`.

---

## Partial reruns & recovery

### Single-batch rerun

To rerun a single failed batch within an array job:

```bash
sbatch --array=37 \
    --export=ALL,PARAM=elevation,FABRIC=gfv2,BASE_CONFIG=configs/base_config.yml \
    slurm_batch/derive_zonal_params.batch
```

For Part 1 raster prep, re-run a single step via:

```bash
sbatch slurm_batch/build_shared_rasters.batch --step <name>
```

### Refill a VPU after a merge-manifest / source fix

When a per-VPU source gap is fixed, re-merge just that VPU and rebuild
dependent products. Run the steps in order, waiting for each to finish (they
are not auto-chained). Example for VPU 17:

```bash
V=17
# 1. re-merge NED / Hydrodem / Fdr / Fac for the VPU
VPUS=$V FORCE=1 sbatch --mem=384G slurm_batch/build_shared_rasters.batch --step merge_rpu_by_vpu
# 2. regenerate open-source hydrology derivatives (Fdr_hydrodem, Twi_hydrodem)
VPUS=$V FORCE=1 sbatch --mem=192G slurm_batch/build_shared_rasters.batch --step compute_dem_derivatives
# 3. re-merge the ArcPy TWI (masked) for the VPU
VPUS=$V FORCE=1 sbatch --mem=384G slurm_batch/build_shared_rasters.batch --step merge_rpu_by_vpu_twi
# 4. rebuild CONUS VRTs
FORCE=1 sbatch slurm_batch/build_shared_rasters.batch --step build_vrt
# 5. recompute TWI percentile reference tables
FORCE=1 sbatch slurm_batch/build_shared_rasters.batch --step twi_reference
```

Then re-clip and re-run depstor for the affected fabric:

```bash
# re-clip the fabric template/fdr from the rebuilt fdr.vrt
pixi run --as-is python scripts/clip_shared_to_fabric.py --fabric oregon
# re-derive depstor rasters and params
FABRIC=oregon sbatch slurm_batch/build_depstor_rasters.batch --force
DR=$(grep '^data_root:' configs/base_config.yml | awk '{print $2}' | tr -d '"')
slurm_batch/submit_depstor_params.sh "$DR/oregon/batches" oregon configs/base_config.yml
```

Zonal params (elevation/slope/aspect/soils/LULC/ssflux) are on the gap-free DEM
lattice and do not need re-running after a VPU FDR/TWI fix.

Verify with `notebooks/fabric_results/01_input_rasters.ipynb` (the `fdr` /
`twi_hydrodem` panels should have no nodata void inside the fabric).

---

## Monitoring

```bash
squeue -u "$USER"
tail -n 200 logs/job_*.out
sacct -j <JOBID> -o JobID,State,Elapsed,MaxRSS
```

---

## Script → config → entry-point map

### Orchestrators (primary surface)

| Batch / shell | Config | Script |
|---|---|---|
| `slurm_batch/build_shared_rasters.batch` | `configs/shared_rasters/shared_rasters.yml` | `scripts/build_shared_rasters.py` |
| `slurm_batch/build_depstor_rasters.batch` | `configs/depstor/depstor_rasters.yml` | `scripts/build_depstor_rasters.py` |
| `slurm_batch/submit_zonal_params.sh` | `configs/zonal/zonal_params.yml` | `scripts/derive_zonal_params.py` (dispatches 10 params × zonal+merge, with ssflux's `build_weights` prereq chained automatically) |
| `slurm_batch/submit_depstor_params.sh` | `configs/depstor/depstor_params.yml` | `scripts/derive_depstor_params.py` (dispatches 10 fractions × zonal+merge, then ratios via afterok) |
| `slurm_batch/submit_dprst_depth.sh` | `configs/depstor/depstor_rasters.yml` + `configs/depstor/depstor_params.yml` | plan → array → build → mean_zonal → mean_finalize, 5 jobs chained afterok (issue #173; see Stage 2d'). `python -m gfv2_params.dprst_depth.tiling --plan` is the dry-run/sizing hook for stage 1 |
| `slurm_batch/submit_snarea_pipeline.sh` | (chains the 4 batches below) | one-command recipe: Stage-1 array → merge → Stage-2 derive → Stage-3 library, all `--dependency=afterok`; sizes the array from the manifest, picks Stage-2 `--mem` by fabric. `DRYRUN=1` to echo the sbatch chain |
| `slurm_batch/derive_snodas_aggregate.batch` + `merge_snodas_aggregate.batch` | `configs/aggregate/aggregate_sources.yml` | `scripts/derive_aggregate.py --source snodas` (Stage 1: gridded-time-series → HRU aggregation via `src/gfv2_params/aggregate/`; array over spatial batches + merge) |
| `slurm_batch/derive_snarea_curve.batch` (or run directly with `pixi run`) | `configs/snarea/snarea_curve.yml` | `scripts/derive_snarea_curve.py` (Stage 2: per-HRU empirical curve + sub-grid CV → `_intermediates/nhm_snarea_curve_derived.csv` via `src/gfv2_params/snarea/`) |
| `slurm_batch/derive_snarea_library.batch` | `configs/snarea/snarea_library.yml` | `scripts/derive_snarea_library.py` (Stage 3: CV/lognormal curve library + per-HRU `snarea_curve`/`hru_deplcrv`/`snarea_thresh` + pyWatershed `.nc` via `src/gfv2_params/snarea/library.py`) |

### Part-2 workers (looped by the wrappers; also runnable by hand per Stage 4A)

| Batch | Used by | Config | Script |
|---|---|---|---|
| `slurm_batch/derive_zonal_params.batch` | `submit_zonal_params.sh` | `configs/zonal/zonal_params.yml` | `scripts/derive_zonal_params.py --mode zonal --param $PARAM` |
| `slurm_batch/merge_zonal_param.batch` | `submit_zonal_params.sh` | `configs/zonal/zonal_params.yml` | `scripts/derive_zonal_params.py --mode merge --param $PARAM` |
| `slurm_batch/build_zonal_weights.batch` | `submit_zonal_params.sh` (ssflux prereq) | `configs/zonal/zonal_params.yml` | `scripts/derive_zonal_params.py --mode build_weights` |
| `slurm_batch/create_depstor_zonal.batch` | `submit_depstor_params.sh` | `configs/depstor/depstor_params.yml` | `scripts/derive_depstor_params.py --mode zonal --fraction $FRACTION` |
| `slurm_batch/merge_depstor_fraction.batch` | `submit_depstor_params.sh` | `configs/depstor/depstor_params.yml` | `scripts/derive_depstor_params.py --mode merge --fraction $FRACTION` |
| `slurm_batch/derive_depstor_ratios.batch` | `submit_depstor_params.sh` | `configs/depstor/depstor_params.yml` | `scripts/derive_depstor_params.py --mode ratios` |
| `slurm_batch/plan_dprst_depth_batches.batch` | `submit_dprst_depth.sh` | `configs/depstor/depstor_rasters.yml` | `python -m gfv2_params.dprst_depth.tiling --plan` |
| `slurm_batch/run_dprst_depth_batch.batch` | `submit_dprst_depth.sh` | `configs/depstor/depstor_rasters.yml` | `scripts/run_dprst_depth_batch.py --batch_id $SLURM_ARRAY_TASK_ID` |
| `slurm_batch/mean_zonal_dprst_depth.batch` | `submit_dprst_depth.sh` | `configs/depstor/depstor_params.yml` | `scripts/derive_depstor_params.py --mode mean_zonal --mean dprst_depth_avg` |
| `slurm_batch/mean_finalize_dprst_depth.batch` | `submit_dprst_depth.sh` | `configs/depstor/depstor_params.yml` | `scripts/derive_depstor_params.py --mode mean_finalize --mean dprst_depth_avg` |

### Stage 3 / fabric-prep batches

| Batch | Config | Script |
|---|---|---|
| `slurm_batch/merge_vpu_segments.batch` | `configs/base_config.yml` | `scripts/merge_vpu_segments.py` (merges per-VPU `nsegment` → `{fabric}/fabric/{fabric}_nsegment_merged.gpkg`; VPU-based fabrics only) |
| `slurm_batch/prepare_fabric.batch` | `configs/base_config.yml` | `scripts/prepare_fabric.py` (spatial batching via KD-tree bisection + manifest) |

### Post-processing batches

| Batch | Config | Script |
|---|---|---|
| `slurm_batch/merge_and_fill_params.batch` | `configs/base_config.yml` | `scripts/merge_and_fill_params.py` (KNN gap-fill of missing parameter values) |
| `slurm_batch/merge_default_output_params.batch` | `configs/base_config.yml` | `scripts/merge_default_params.py` |
| `slurm_batch/render_figures.batch` | (none; fabric from `FABRIC` env) | `scripts/render_figures.py` (headless figure generation via nbconvert) |

### Standalone / setup

| Batch / shell | Config | Script |
|---|---|---|
| `slurm_batch/stage_twi.batch` | `configs/base_config.yml` (indirectly) | `scripts/stage_twi.sh` |
| `slurm_batch/submit_twi_completion.sh` | `configs/shared_rasters/shared_rasters.yml` | `scripts/build_shared_rasters.py` — three chained jobs: `--step merge_rpu_by_vpu_twi --force` → `--step build_vrt --force` → `--step twi_reference --force` |
| `slurm_batch/download_rpu_rasters.batch` | `configs/base_config.yml` | `gfv2_params.download.rpu_rasters` |
| `slurm_batch/download_nalcms.batch` | `configs/base_config.yml` | `gfv2_params.download.nalcms_lulc` |
| `slurm_batch/download_nhm_v11.batch` | `configs/base_config.yml` | `gfv2_params.download.nhm_v11_lulc` |
| `slurm_batch/download_nhd_flowlines.batch` | `configs/base_config.yml` | `gfv2_params.download.nhd_flowlines` — downloads per-VPU NHDPlusV2 `NHDFlowline` attributes and distills distinct non-zero `WBAREACOMI` values to `input/nhd/connected_waterbody_comids.parquet`, keeping only WBAREACOMI carried by a **Network Flowline** (requires `flowline_topology.parquet`; issue #161) (one-time, CONUS) |
| (run directly) | `configs/base_config.yml` | `gfv2_params.download.nhd_topology` — downloads per-VPU NHDPlusV2 `PlusFlowlineVAA` attributes and distills COMID/DnHydroseq/Hydroseq/TerminalFl/StartFlag/StreamOrde/FromNode/ToNode to `input/nhd/flowline_topology.parquet` (one-time, CONUS); must run before **both** `download_nhd_flowlines.batch` and `stage_nhd_flowthrough.batch` |
| `stage_nhd_flowthrough.batch` | `configs/base_config.yml` | `gfv2_params.download.nhd_flowthrough` — per-VPU vector spatial join; classifies flow-through NHDWaterbody polygons (T1: boundary crossings ≥2; D1: routed-network upstream endpoint inside the waterbody, from `flowline_topology.parquet`; T3: NHDArea overlap) — T1/D1 candidates are gated to **Network Flowlines** (in `flowline_topology.parquet`) so Non-Network closed-basin lines can't promote endorheic lakes (issue #161); Playa/Ice Mass dropped up front; writes `input/nhd/flowthrough_waterbody_comids.parquet` (one-time, CONUS; no CONUS-grid array) |
| `slurm_batch/submit_jobs.sh` | (caller-provided) | generic per-VPU array dispatcher |
| (run directly) | `configs/base_config.yml` | `scripts/migrate_to_shared_layout.py --data-root <path>` (legacy `work/` layout upgrade) |
| (run directly) | `configs/base_config.yml` | `scripts/clip_shared_to_fabric.py --fabric <name>` (fabric-bounds FDR/template clip) |
