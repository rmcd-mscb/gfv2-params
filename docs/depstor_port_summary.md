# What `gfv2-params` used from `../depstor`

**Audience:** Andy Bock, Cory Russell, and the wider NHM team — a report on
how the depstor depression-storage workflow was ported into `gfv2-params`,
which functions we kept vs. dropped, and which issues in the original
`DepStor.py` we had to work around.

**Status:** Through PR #72 — `feat/depstor-consolidate`. Levels 1-5 complete.
The pipeline now produces 6 PRMS-ready depstor parameters: `sro_to_dprst_perv`,
`sro_to_dprst_imperv`, `carea_max`, `smidx_coef`, `hru_percent_imperv`, and
`dprst_frac`. The per-step scripts and configs that earlier rows in this
document reference have been consolidated into 2 orchestrators + 2 unified
configs — see [§v2 architecture](#v2-consolidated-architecture-pr-72) at the
bottom. The function-by-function port-mapping table below is preserved as a
historical record of which depstor function landed where during the port;
read it together with the v2 section. Validation history for VPU01 is in
[`docs/depstor_vpu01_validation_results.md`](depstor_vpu01_validation_results.md).

The depstor design doc that guided the port is transcribed at
[`docs/depstor_workflow.md`](depstor_workflow.md); the source PDF is at
[`docs/DepStor_workflow.pdf`](DepStor_workflow.pdf).

---

## What we ported

The depstor `scripts/DepStor.py` is one 851-line script with a
`RasterPipeline` class and a chain of `get*` functions. We split it into a
reusable utility module plus one config-driven script per workflow step.

Post-consolidation (PR #72), the per-step scripts are library modules under
[`src/gfv2_params/depstor_builders/`](../src/gfv2_params/depstor_builders/);
the table below maps depstor functions to those modules. The orchestrator
that walks them in dependency order is
[`scripts/build_depstor_rasters.py`](../scripts/build_depstor_rasters.py).

| depstor function (`DepStor.py` line range) | `gfv2-params` artefact | What it does |
|---|---|---|
| `RasterPipeline.{rasterize, raster_create, vector_raster_mask, raster_raster_mask, open_raster, set_template}` (42-410) | [`src/gfv2_params/depstor.py`](../src/gfv2_params/depstor.py) — `RasterInfo`, `rasterize_binary`, `threshold_above`, `clump_regions`, `regions_touching_mask`, `regions_to_binary`, `write_uint8_binary`, `write_int32_regions`, `read_aligned_uint8`, `read_land_mask_for_grid` | Raster I/O + binary/region helpers |
| `whitebox_run` (412-449) | [`src/gfv2_params/depstor_builders/routing.py`](../src/gfv2_params/depstor_builders/routing.py) | Subprocess wrapper around WhiteboxTools `Watershed` |
| `getHruImperv` (452-518) | [`src/gfv2_params/depstor_builders/imperv.py`](../src/gfv2_params/depstor_builders/imperv.py) | Threshold the impervious raster to a binary mask |
| `getSegBuff` (521-577) | [`src/gfv2_params/depstor_builders/streambuffer.py`](../src/gfv2_params/depstor_builders/streambuffer.py) | Buffer NHD segments and rasterize |
| `getWBinHRUs` (580-663) | [`src/gfv2_params/depstor_builders/waterbody.py`](../src/gfv2_params/depstor_builders/waterbody.py) | Filter wbody polys by area, rasterize, then label connected components |
| `getDprst` (666-701) | [`src/gfv2_params/depstor_builders/dprst.py`](../src/gfv2_params/depstor_builders/dprst.py) | Region-level intersection logic (depression = wbody region with zero stream/imperv overlap) |
| `getHruSro_to_dprst` (704-739) | [`src/gfv2_params/depstor_builders/routing.py`](../src/gfv2_params/depstor_builders/routing.py) | Run WBT `Watershed` against FDR + dprst pour points |
| `GetPervAreaTotal` (741-765) | [`src/gfv2_params/depstor_builders/perv.py`](../src/gfv2_params/depstor_builders/perv.py) | Cell-wise `NOT imperv AND NOT dprst` (re-implemented; see bug #2 below) |
| `onStreamStor` (768-791) | folded into [`depstor_builders/dprst.py`](../src/gfv2_params/depstor_builders/dprst.py) | Wbody cells outside dprst — collapsed to a 2-line boolean |
| `getCarea_map` | [`src/gfv2_params/depstor_builders/carea_map.py`](../src/gfv2_params/depstor_builders/carea_map.py) | Build the two PRMS TWI-threshold binary rasters in one pass |
| `getSro_to_dprst_perv` / `getSro_to_dprst_imperv` (per-cell intersect step) | [`src/gfv2_params/depstor_builders/intersect.py`](../src/gfv2_params/depstor_builders/intersect.py) | `drains_to_dprst ∩ perv_binary` / `∩ imperv_binary` — cell-wise binary intersection |
| `getZonecount` / `getCarea` / `getSmidx` / `getCarea_max` / `getHRU_percent_imperv` / `getDprst_frac` | [`src/gfv2_params/depstor_ratios.py`](../src/gfv2_params/depstor_ratios.py) + [`scripts/derive_depstor_params.py`](../scripts/derive_depstor_params.py) `--mode ratios` | Per-HRU ratio CSVs — see the `ratios:` block of [`configs/depstor/depstor_params.yml`](../configs/depstor/depstor_params.yml) for the full set |

## What we intentionally dropped

- **The "rasterize HRU polygons and tag each cell with its HRU ID" pattern.**
  Every depstor output is a HRU-ID-labeled raster, and downstream parameter
  computation is per-HRU zone counts on those rasters. We replaced this with
  **gdptools `exactextract` zonal aggregation against the fabric polygons
  directly**. We keep depstor's binary masks (`imperv_binary`, `dprst_binary`,
  etc.), but never rasterize the HRU layer. Saves writing an `nhrug.tif`
  (~10 GB on CONUS) and lets us aggregate fractional coverage with sub-pixel
  accuracy instead of integer cell counts.
- **`getDprst_frac`** (~line 798 in the old code). Bock & Russell's design doc
  itself marks this "Possibly Deprecated / not used."
- **All the `main()` hard-coded relative paths** (`./../data/...`,
  `../output/...`). Replaced by YAML configs under `configs/depstor_*.yml`.

## Bugs in the original `DepStor.py` we had to work around

These all surfaced when running end-to-end against per-VPU NHDPlus inputs.
None were a fault of the design — they're the kinds of things you find on
first integration. Listed in case the depstor authors want to fix upstream.

### 1. `wbe` is referenced but never imported (`DepStor.py:657-660`) — crash bug

```python
WBHRUraster = wbe.read_raster(binary_path)        # line 657
WBregion    = wbe.clump(WBHRUraster, diag=True)   # line 658
wbe.write_raster(WBregion, wbg_region_path)       # line 660
```

`wbe` is not in any `import` and never assigned. The function `getWBinHRUs`
would `NameError` on the first call. The expected pattern is presumably
`from whitebox_workflows import WbEnvironment; wbe = WbEnvironment()`.

**Our workaround:** dropped `whitebox-workflows` for this step entirely. Use
`scipy.ndimage.label` with a 3×3 structure
([`gfv2_params/depstor.py:clump_regions`](../src/gfv2_params/depstor.py)).
Same 8-connectivity, smaller dependency footprint, and a couple of seconds
faster on the VPU01 grid.

### 2. `GetPervAreaTotal` uses `remove_all_overlap=True` — contradicts the design doc

The doc (item 9) says `pervAreaTotal` should be "areas that are not impervious
or depressions … basically a `con` operation." That's a **cell-wise** rule.

The Python code uses `raster_raster_mask(..., remove_all_overlap=True)`
twice. Reading `raster_raster_mask` lines 382-407, `remove_all_overlap=True`
collects the unique HRU IDs that overlap with any mask cell and zeroes out
**every cell of those HRUs**. So an HRU with even one impervious or
depression cell is entirely excluded from `pervAreaTotal`. Aggregated, this
gives a binary "is the HRU fully pervious" — not a pervious-area fraction.

**Our workaround:**
[`src/gfv2_params/depstor_builders/perv.py`](../src/gfv2_params/depstor_builders/perv.py)
implements the doc's cell-wise version:
`compute_perv_binary(imperv, dprst, land_valid) = np.where(land AND NOT imperv AND NOT dprst, 1, 255)`.
With the cell-wise rule, `imperv_frac + dprst_frac + perv_frac ≈ 1` per HRU,
which matches PRMS expectations. (The land-mask term is added in PR #69; see
[`docs/depstor_workflow.md`](depstor_workflow.md) gfv2-params land mask note.)

### 3. ZSTD compression breaks WhiteboxTools input

depstor writes intermediates as ZSTD-compressed GeoTIFFs
(`DepStor.py:59`, `compress: str = "ZSTD"`). WhiteboxTools' GeoTIFF decoder
only accepts PACKBITS / LZW / DEFLATE; when the routing step feeds
`dprst_binary.tif` to WBT, it fails to load with `The WhiteboxTools GeoTIFF
decoder currently only supports PACKBITS, LZW, and DEFLATE compression`.

**Our workaround:** all depstor intermediates write LZW now
(`gfv2_params/depstor.py:write_uint8_binary` and `write_int32_regions`).
Size impact is negligible — these rasters are mostly nodata.

### 4. WhiteboxTools `Watershed` silently treats nodata as pour points

The subtlest of the bunch. WBT `Watershed` reads the raw pour-points raster
values and treats every **non-zero** value as a pour point. It **does not
consult the GeoTIFF `NoData` tag**. depstor's binary convention uses
`1 = present, 255 = nodata`, so when `dprst_binary.tif` is handed to WBT,
the 811M nodata-255 cells are each treated as their own pour point. The
output looks plausible (`ExitCode=0`, valid GeoTIFF), but every cell ends up
marked "drains to somewhere."

**Our workaround:**
[`src/gfv2_params/depstor_builders/routing.py`](../src/gfv2_params/depstor_builders/routing.py)
added a `_prepare_pour_points` helper that rewrites the input as
`1 = pour point, 0 = nodata` before passing to WBT. This is a WBT quirk
rather than a depstor bug per se, but depstor's binary convention triggers
it.

### 5. xarray `_FillValue` collision in `rioxarray.to_raster`

After `reproject_match`, the source raster's `_FillValue` is in `attrs`;
`write_nodata` then adds it to `encoding`. xarray 2023+ refuses to encode
when `_FillValue` is in both: `ValueError: Key '_FillValue' already exists
in attrs, and will not be overwritten`. Hit this in our routing step on
rioxarray ≥ 0.15.

**Our workaround:** `attrs.pop("_FillValue", None)` before `to_raster`.
Library-version-sensitive; would also affect upstream depstor if they
upgrade xarray.

## Adaptations (not bugs — design changes for our pipeline)

These were intentional changes for the `gfv2-params` context, called out
separately so they don't get conflated with bug fixes.

1. **Config-driven instead of `main()`-hard-coded.** Every script reads its
   inputs from a YAML config + the active fabric profile in
   `configs/base_config.yml`. Lets us run the same scripts against CONUS,
   VPU01-overlay, or any future fabric without code edits.
2. **Per-batch fabric overlay via gdptools** (replaces depstor's HRU-tag
   pattern) so we can fan out the per-HRU aggregation as a SLURM array job.
   Critical at CONUS scale (~110k HRUs).
3. **Hard-coded thresholds parameterized.** depstor inlined
   `imperv_threshold=50`, `buffer_zone=60m`, `min_area=900m²`. All three are
   now config keys.
4. **Tiled / `BIGTIFF=YES` output.** depstor's `tiled=True/blockxsize=256`
   was fine but didn't set `BIGTIFF`; we hit the 4 GB GeoTIFF limit on CONUS
   region-label outputs without it.

## Inherited semantic concerns we should flag separately

These are **not** bugs in the port — depstor faithfully implements them —
but they're worth a conversation with the depstor authors:

1. **The 50% impervious threshold under-counts roads at 30 m resolution.**
   Verified on VPU01: of 10.96M cells with any impervious surface (1–100%
   in the NLCD fractional raster), only **16.14% (1.77M cells) clear the
   50% threshold**. A 2-lane road (~7 m) crossing a 30 m pixel orthogonally
   covers ~23% — well below the cutoff. Result: `hru_percent_imperv`
   systematically under-counts. Suggested fixes documented in
   [`docs/depstor_vpu01_validation_results.md:109`](depstor_vpu01_validation_results.md#L109).
2. **`onStreamStor` is documented as "Not quite sure of the point of this
   one"** (workflow doc item 10). We compute it because the doc implies
   Level 4 `getCarea_map` consumes it, but its role and downstream effect
   on PRMS parameters could use clarification.
3. **`getHruSro_to_dprst` produces HRU-ID-labeled rasters** in depstor; per
   the doc items 12-13, `sro_to_dprst_perv/imperv` are per-HRU zone counts
   of the intersection with perv/imperv. Our port produces a binary
   `drains_to_dprst.tif` and computes the intersections via gdptools —
   equivalent in outcome but a different intermediate shape. Worth a sanity
   check vs. a depstor reference run if one exists.

---

## v2: consolidated architecture (PR #72)

The function-by-function table above shows the *initial* port: 10
single-purpose `scripts/build_depstor_*.py` + 9 `configs/depstor_*_raster.yml`
on the generation side, plus 9 `configs/*_frac_param.yml` driving
`scripts/create_zonal_params.py` and a wedged `derive_depstor_ratios.py`
call inside `merge_output_params.batch` on the aggregation side. ~40 small
files, no top-level orchestrator, ordering documented only in `RUNME.md`.

PR #72 collapses that to **2 configs + 2 orchestrators**, deleting 49 files
and reducing the per-step boilerplate to one entry per step in a unified
config.

### Generation side

[`configs/depstor/depstor_rasters.yml`](../configs/depstor/depstor_rasters.yml) lists all 10
build steps in dependency order. Each `name` maps to a module under
[`src/gfv2_params/depstor_builders/`](../src/gfv2_params/depstor_builders/)
that exposes `build(step_cfg, ctx, logger) -> {output_key: Path}`. The
orchestrator [`scripts/build_depstor_rasters.py`](../scripts/build_depstor_rasters.py)
walks the canonical `STEP_ORDER`
(landmask → imperv / streambuffer / waterbody → dprst → perv / routing →
drains_perv / drains_imperv → carea_map), wires outputs through a
`BuildContext`, supports `--step <name>` / `--from <name>` for selective
re-runs, and runs under a single sbatch
([`slurm_batch/build_depstor_rasters.batch`](../slurm_batch/build_depstor_rasters.batch))
sized for the WhiteboxTools `routing` long-pole.

### Aggregation side

[`configs/depstor/depstor_params.yml`](../configs/depstor/depstor_params.yml) carries shared
defaults, **10 fractions** (each a zonal-stat target), and **6 ratios** that
divide pairs of fractions. The driver
[`scripts/derive_depstor_params.py`](../scripts/derive_depstor_params.py) has
three modes (`zonal` / `merge` / `ratios`); the slurm wrapper
[`slurm_batch/submit_depstor_params.sh`](../slurm_batch/submit_depstor_params.sh)
chains every fraction's array zonal job → merge (afterok) → one final ratios
job depending on every merge.

The 6 ratios are the actual PRMS-ready depstor parameters: `sro_to_dprst_perv`,
`sro_to_dprst_imperv`, `carea_max`, `smidx_coef`, plus the two new in PR #72:
`hru_percent_imperv` and `dprst_frac` (the PRMS area-fraction). The latter
two cover items 18 and the (previously deprecated, now needed) Level-5
`hru_percent_imperv` / `dprst_frac` outputs from the design doc.

### Output layout

The merged outputs split into two subdirs to disambiguate count CSVs from
PRMS-ready ratio CSVs (the name `dprst_frac` is used by both):

```
{fabric}/params/merged/
├── nhm_carea_max_params.csv               # PRMS [0, 1] ratio
├── nhm_smidx_coef_params.csv              # PRMS [0, 1] ratio
├── nhm_sro_to_dprst_perv_params.csv       # PRMS [0, 1] ratio
├── nhm_sro_to_dprst_imperv_params.csv     # PRMS [0, 1] ratio
├── nhm_hru_percent_imperv_params.csv      # PRMS [0, 1] ratio (NEW)
├── nhm_dprst_frac_params.csv              # PRMS [0, 1] ratio (NEW)
└── _intermediates/
    ├── nhm_perv_frac_params.csv           # partial-pixel-weighted cell count
    ├── nhm_imperv_frac_params.csv         # (NOT a fraction; see note below)
    ├── nhm_dprst_frac_params.csv          # collides with PRMS name above — different subdir!
    ├── ... (7 more fraction count CSVs)
    └── nhm_hru_total_count_params.csv     # aggregates land_mask.tif (NEW)
```

The `_intermediates/` CSVs are gdptools exactextract outputs with
`categorical=false` on uint8 1/255 binaries. The `count` column is the
partial-pixel-weighted count of `1`-valued cells per HRU — **not** a
[0, 1] fraction. The new `hru_total` count divides cleanly into the imperv
and dprst counts to give the area-fraction PRMS params.

### Why the consolidation

| Before PR #72 | After PR #72 |
|---|---|
| 10 single-purpose `build_depstor_*.py` scripts | 1 orchestrator + 10 library modules under `depstor_builders/` |
| 10 `depstor_*_raster.yml` configs | 1 `depstor_rasters.yml` config |
| 10 `build_depstor_*.batch` sbatch files | 1 `build_depstor_rasters.batch` |
| 9 `*_frac_param.yml` aggregation configs | 1 `depstor_params.yml` config |
| 9 `create_*_frac_params.batch` sbatches + ratio call wedged into `merge_output_params.batch` | 3 sbatches + 1 submit script with afterok DAG |
| 4 PRMS ratio outputs | 6 PRMS ratio outputs (adds `hru_percent_imperv`, `dprst_frac`) |
| Step ordering documented only in `RUNME.md` | Step ordering encoded in `STEP_ORDER`; `--step` / `--from` resume support |

Functional logic is unchanged — every `compute_*` helper from
`src/gfv2_params/depstor.py` and `depstor_ratios.py` is the same as before;
only the entry points moved. PR #72's diff is 49 deletions + 12 additions
+ 8 modifications.
