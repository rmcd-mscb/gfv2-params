# What `gfv2-params` used from `../depstor`

**Audience:** Andy Bock, Cory Russell, and the wider NHM team — a report on
how the depstor depression-storage workflow was ported into `gfv2-params`,
which functions we kept vs. dropped, and which issues in the original
`DepStor.py` we had to work around.

**Status:** Through PR #62 — `feat/depstor-closeout` (Levels 1-3 complete +
`perv_frac`). Issue #61 / branch `feat/depstor-levels-4-5-issue-61` is
in progress and covers Levels 4-5. Validation history for VPU01 is in
[`docs/depstor_vpu01_validation_results.md`](depstor_vpu01_validation_results.md).

The depstor design doc that guided the port is transcribed at
[`docs/depstor_workflow.md`](depstor_workflow.md); the source PDF is at
[`docs/DepStor_workflow.pdf`](DepStor_workflow.pdf).

---

## What we ported

The depstor `scripts/DepStor.py` is one 851-line script with a
`RasterPipeline` class and a chain of `get*` functions. We split it into a
reusable utility module plus one config-driven script per workflow step.

| depstor function (`DepStor.py` line range) | `gfv2-params` artefact | What it does |
|---|---|---|
| `RasterPipeline.{rasterize, raster_create, vector_raster_mask, raster_raster_mask, open_raster, set_template}` (42-410) | [`src/gfv2_params/depstor.py`](../src/gfv2_params/depstor.py) — `RasterInfo`, `rasterize_binary`, `threshold_above`, `clump_regions`, `regions_touching_mask`, `regions_to_binary`, `write_uint8_binary`, `write_int32_regions`, `read_aligned_uint8` | Raster I/O + binary/region helpers |
| `whitebox_run` (412-449) | [`scripts/build_depstor_routing.py`](../scripts/build_depstor_routing.py) | Subprocess wrapper around WhiteboxTools `Watershed` |
| `getHruImperv` (452-518) | [`scripts/build_depstor_imperv.py`](../scripts/build_depstor_imperv.py) | Threshold the impervious raster to a binary mask |
| `getSegBuff` (521-577) | [`scripts/build_depstor_streambuffer.py`](../scripts/build_depstor_streambuffer.py) | Buffer NHD segments and rasterize |
| `getWBinHRUs` (580-663) | [`scripts/build_depstor_waterbody.py`](../scripts/build_depstor_waterbody.py) | Filter wbody polys by area, rasterize, then label connected components |
| `getDprst` (666-701) | [`scripts/build_depstor_dprst.py`](../scripts/build_depstor_dprst.py) | Region-level intersection logic (depression = wbody region with zero stream/imperv overlap) |
| `getHruSro_to_dprst` (704-739) | [`scripts/build_depstor_routing.py`](../scripts/build_depstor_routing.py) | Run WBT `Watershed` against FDR + dprst pour points |
| `GetPervAreaTotal` (741-765) | [`scripts/build_depstor_perv.py`](../scripts/build_depstor_perv.py) | Cell-wise `NOT imperv AND NOT dprst` (re-implemented; see bug #2 below) |
| `onStreamStor` (768-791) | folded into [`build_depstor_dprst.py:123-127`](../scripts/build_depstor_dprst.py#L123-L127) | Wbody cells outside dprst — collapsed to a 2-line boolean |

Plus the `getCarea_map` / `getSro_to_dprst_*` / `getSmidx` / `getCarea_max`
functions at Levels 4-5 — in progress on
`feat/depstor-levels-4-5-issue-61` (issue #61).

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
[`scripts/build_depstor_perv.py`](../scripts/build_depstor_perv.py)
implements the doc's cell-wise version:
`compute_perv_binary(imperv, dprst) = np.where((imperv != 1) & (dprst != 1), 1, 255)`.
With the cell-wise rule, `imperv_frac + dprst_frac + perv_frac ≈ 1` per HRU,
which matches PRMS expectations.

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
[`scripts/build_depstor_routing.py`](../scripts/build_depstor_routing.py)
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
