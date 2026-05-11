# Depression-storage VPU 01 validation results (issue #38)

Branch: `feat/depstor-validation-issue-38`
Run 1: 2026-05-11 15:39–15:45 UTC (initial submission, 8/9 jobs COMPLETED — routing failed)
Run 2: 2026-05-11 16:25 UTC (resubmit after `_FillValue` fix — routing surfaced WBT/ZSTD bug)
Run 3: 2026-05-11 16:34 UTC (resubmit after ZSTD→LZW fix — routing surfaced WBT pour-points bug)
Run 4: 2026-05-11 17:00 UTC (resubmit after pour-points fix — **all 9 jobs COMPLETED with sensible outputs**)

**Final outcome:** all 9 depstor jobs COMPLETED, all 7 rasters and all 4 zonal-stat CSV groups produced and sanity-checked. Three independent script bugs were surfaced and fixed in commits on this branch (see "Bugs fixed during validation" below). Validation also surfaced one documentation/normalization improvement (zonal stat `mean` vs `count`) and confirmed the workflow design choices (layer naming, fabric overlay pattern, per-VPU FDR sidestepping issue #41).

## Configuration

- Fabric: `gfv2_vpu01`
- Template grid: `work/nhd_merged/01/Hydrodem_merged_01.tif` (25,845 × 31,405, unnamed Albers ≈ EPSG:5070)
- FDR: `work/nhd_merged/01/Fdr_merged_01.tif` (per-VPU; sidesteps the CONUS `fdr.vrt` memory issue tracked in issue #41)
- Fabric polygons: `input/fabric/NHM_01_draft.gpkg` layers `nhru` (11,278 features, `nat_hru_id` 1..11,278), `nsegment`, `wbs` (note: NOT `v2_wb`)
- Batch size: 2000 → KD-tree bisection produced **8** batches in `gfv2_vpu01/batches/`

## Job submission record

8 batches in `gfv2_vpu01/batches/` (KD-tree bisection from `batch_size: 2000` over 11,278 features → array range `0-7`).

### Run 1 — initial validation

| Step | Batch script | Job ID | Dependency | Status | Elapsed |
|---|---|---|---|---|---|
| imperv | `build_depstor_imperv_vpu01.batch` | 17021333 | — | COMPLETED | 1m10s |
| streambuffer | `build_depstor_streambuffer_vpu01.batch` | 17021334 | — | COMPLETED | 0m59s |
| waterbody | `build_depstor_waterbody_vpu01.batch` | 17021335 | — | COMPLETED | 1m07s |
| dprst | `build_depstor_dprst_vpu01.batch` | 17021336 | afterok: 17021333, 17021334, 17021335 | COMPLETED | 0m22s |
| routing | `build_depstor_routing_vpu01.batch` | 17021337 | afterok: 17021336 | **FAILED** (`_FillValue`) | 0m56s |
| dprst_frac (array 0-7) | `create_dprst_frac_params_vpu01.batch` | 17021338 | afterok: 17021336 | COMPLETED (all 8) | 0:31–0:59 per task |
| imperv_frac (array 0-7) | `create_imperv_frac_params_vpu01.batch` | 17021339 | afterok: 17021333 | COMPLETED (all 8) | 0:53–1:25 per task |
| onstream_frac (array 0-7) | `create_onstream_storage_frac_params_vpu01.batch` | 17021340 | afterok: 17021336 | COMPLETED (all 8) | 0:57–0:59 per task |
| drains_to_dprst_frac (array 0-7) | `create_drains_to_dprst_frac_params_vpu01.batch` | 17021341 | afterok: 17021337 | CANCELLED (dep never satisfied) | n/a |

### Run 4 — final, after all three bug fixes

| Step | Job ID | Status | Elapsed |
|---|---|---|---|
| imperv | 17026711 | COMPLETED | 0m49s |
| streambuffer | 17026712 | COMPLETED | 0m19s |
| waterbody | 17026713 | COMPLETED | 0m27s |
| dprst | 17026714 | COMPLETED | 0m25s |
| routing (re-run with intermediates + pour-points fix) | 17029832 → 17031363 prep | COMPLETED | ~3m |
| drains_to_dprst_frac array (final) | 17031363 | COMPLETED (all 8) | 0:14–1:21 per task |

## Raster output summary

All rasters share shape `31,405 × 25,845` (snapped to `work/nhd_merged/01/Hydrodem_merged_01.tif`) and `nodata = 255` (uint8) or `nodata = 0` (int32 regions). Coverage column = `(cells == 1) / (total cells)`.

| Raster | Dtype | Valid % (≠ nodata) | Cells == 1 | Coverage % | Notes |
|---|---|---|---|---|---|
| `imperv_binary.tif` | uint8 | 0.27 | 2,187,698 | 0.270 | NE urban density |
| `stream_buffer.tif` | uint8 | 0.83 | 6,742,060 | 0.831 | 60m buffer around 5,962 nsegment lines |
| `wbody_binary.tif` | uint8 | 0.63 | 5,114,053 | 0.630 | All waterbodies (`wbs` layer, 835 polygons, min area 900 m²) |
| `wbody_regions.tif` | int32 | 0.63 | n/a (864 unique IDs, max 864) | n/a | scipy.ndimage 8-conn labels |
| `dprst_binary.tif` | uint8 | 0.01 | 76,878 | 0.009 | Waterbody regions NOT touching streams or impervious |
| `onstream_binary.tif` | uint8 | 0.62 | 5,037,175 | 0.621 | Complement of dprst within wbody universe |
| `drains_to_dprst.tif` | uint8 | 0.07 | 554,688 | 0.068 | Cells whose D8 flow path terminates at any dprst pour-point. Includes the 76,878 dprst cells themselves + ~478k upstream contributing cells. |

**Plausibility check:** dprst (0.009%) << onstream (0.621%) is consistent with most New England waterbodies being on-stream (lakes connected to the river network). The wbody_binary ≈ dprst + onstream identity holds approximately (0.630 ≈ 0.009 + 0.621 + tiny boundary loss).

## Zonal CSV summary

Each `*_frac` group has 8 batch CSV files (one per `gfv2_vpu01/batches/batch_NNNN.gpkg`). CSV columns are the gdptools / exactextract defaults: `nat_hru_id, count, mean, std, min, 25%, 50%, 75%, max, sum`.

| Source | Files | Rows | Unique HRUs | Count match (11,278)? | NaNs in `count` | `count` range | `count` mean |
|---|---|---|---|---|---|---|---|
| `dprst_frac` | 8 | 11,278 | 11,278 | ✓ | 0 | 0.0 – 7,146.0 | 6.82 |
| `imperv_frac` | 8 | 11,278 | 11,278 | ✓ | 0 | 0.0 – 24,921.67 | 130.73 |
| `onstream_storage_frac` | 8 | 11,278 | 11,278 | ✓ | 0 | 0.0 – 65,182.21 | 446.67 |
| `drains_to_dprst_frac` | 8 | 11,278 | 11,278 | ✓ | 0 | 0.0 – 84,641.89 | 49.18 |

**Cross-check on `drains_to_dprst_frac`:** 98.4% of HRUs have `count == 0` (no flow path reaches any depression), versus 99.5% for `dprst_frac` (HRUs that *are* the depression). The 1.6% non-zero HRUs include the dprst-containing HRUs themselves plus their immediate upstream-neighbour HRUs — physically reasonable for VPU01's mostly through-flowing river network where most HRUs drain to streams, not to depressions. The max-count `84,641` (~76 km² of contributing area) exceeds any individual dprst max (`7,146`, ~6.4 km²), as upstream contributing area is expected to be larger than the depression itself.

**Important semantic note about the CSV format.** The binary source rasters use `1 / nodata=255` (NOT `1 / 0 / 255`). When gdptools+exactextract is invoked in `categorical: false` mode against this encoding, the resulting `mean` is `1.0` for every HRU with non-zero overlap (since every *valid* pixel is `1`), and `NaN` for HRUs with zero overlap. The actually-useful column is **`count`** — the (sub-pixel-weighted) number of `1`-valued cells per HRU. To convert this to a true fraction of HRU area, a downstream step must divide by the total HRU area in pixels (or use an HRU-area lookup). This appears to be intentional given the workflow's downstream consumers, but it contradicts the inline comment in `configs/dprst_frac_param.yml` lines 3-4 that claims "per-HRU mean is the fraction" — flagging as a documentation follow-up below.

## Bugs fixed during validation

Three latent bugs in the depstor port (introduced in PR #37) were exposed when running end-to-end against per-VPU NHDPlus inputs. The plan originally said "no script changes in this PR," but per user decision these were fixed on the branch to actually produce `drains_to_dprst.tif`. Each is a tightly scoped, principled fix:

1. **xarray `_FillValue` conflict** — commit `77fd4dc`. `scripts/build_depstor_routing.py:65` raised `ValueError: Key '_FillValue' already exists in attrs, and will not be overwritten` inside `rioxarray.to_raster` → `xarray.conventions.encode_cf_variable`. xarray 2023+ refuses to encode when `_FillValue` is in both `attrs` and `encoding`. `reproject_match` preserves the source raster's `_FillValue` in attrs, and `write_nodata` adds it to encoding. Fix: `attrs.pop("_FillValue", None)` after `write_nodata`.

2. **ZSTD compression incompatible with WhiteboxTools** — same commit. `src/gfv2_params/depstor.py` wrote all depstor rasters as ZSTD-compressed GeoTIFFs. WhiteboxTools' GeoTIFF decoder only accepts PACKBITS, LZW, and DEFLATE. The routing step feeds `dprst_binary.tif` to WBT as pour points, so the whole routing path failed at WBT load with `The WhiteboxTools GeoTIFF decoder currently only supports PACKBITS, LZW, and DEFLATE compression`. Fix: switched both `write_uint8_binary` (line 145) and `write_int32_regions` (line 168) to LZW. Negligible size impact for these sparse rasters (uint8 and int32 with mostly nodata).

3. **WhiteboxTools Watershed pour-points nodata bug** — same commit. The most subtle issue. WBT's Watershed reads the pour-points raster's raw values and treats every non-zero value as a pour point — it **does not consult the GeoTIFF NoData tag** for the pour-points input. `dprst_binary.tif` uses `1=pour, nodata=255`, so WBT was treating the 811M nodata-255 cells as 811M additional pour points, each becoming its own single-cell watershed. The collapse-to-binary then marked every cell as "drains to somewhere," producing the all-black 100%-coverage `drains_to_dprst.tif` that initially looked correct (`COMPLETED, ExitCode=0`) but was useless. Fix: added `_prepare_pour_points` helper that rewrites `dprst_binary.tif` as uint8 0/1 with nodata=0 before passing to WBT. With this fix, `hru_to_dprst_labels.tif` now contains only two values (`1` for the 554,688 cells that drain to a depression, `-32768` for the 811M cells that don't), and `drains_to_dprst.tif` is a sparse, meaningful signal at 0.068% coverage.

Separately, commit `ae3f8d3` migrated `scripts/create_zonal_params.py` to gdptools 0.3.13's new kwarg names (`source_ds`, `source_crs`, etc.) and enabled `gdal.UseExceptions()` / `osr.UseExceptions()` — silencing eight DeprecationWarnings / FutureWarnings per zonal-stats invocation.

## Observations

1. **Wall-time was minutes, not hours.** The plan estimated 1–2h end-to-end at VPU01 scale; actual single-pass was ~3 minutes. The per-VPU scope is small enough that the reduced `_vpu01.batch` resource asks (16-48G mem, 1-2h time) were more than sufficient — routing in particular peaked at <3 min and well under 8 GB RAM. The CONUS-scale routing batch (192G/12h) was sized for the failed-design unchunked CONUS `fdr.vrt` case; once issue #41 lands or a per-VPU pattern is adopted, those numbers can come down substantially.

2. **Layer name `wbs` vs `v2_wb`.** Verified at Phase 0; the VPU01 config correctly uses `wbs`. No surprises.

3. **CRS handling worked.** `Hydrodem_merged_01.tif` advertises itself as unnamed Albers (NAD83 + GRS80 + Albers Equal Area params); `NHM_01_draft.gpkg` is EPSG:5070 (NAD83/Conus Albers). The build scripts' `to_crs(info.crs)` handled this transparently — no projection warnings in logs.

4. **Zonal CSV semantics gotcha.** See the note in the Zonal CSV summary above. `count` is the parameter to use, not `mean`. The CONUS-scale runs will hit the same surprise; a docstring clarification on `configs/*_frac_param.yml` would save someone an afternoon of debugging.

5. **The pour-points bug (#3 above) would have been invisible without `keep_intermediates: true`.** WBT exited with `ExitCode=0`, the script wrote a valid GeoTIFF, and per-HRU zonal stats produced non-zero counts. It was only when looking at the *value distribution* of the output raster (100% == 1) that it was clear something was wrong. Recommendation: add a `STATISTICS_VALID_PERCENT < some_threshold` sanity-check assertion at the end of `build_depstor_routing.py`, or a notebook smoke-test step. The notebook in this PR (`notebooks/check_depstor_vpu01.ipynb`) does include `pct_of_total` per raster, which would have caught this.

## Follow-ups

The three routing bugs are fixed in this branch (split across commits during the final review). Remaining follow-ups, ordered roughly by priority:

**Highest priority — surfaced during VPU01 visual review:**

0. **The 50% imperv threshold under-counts roads.** Verified against `input/lulc_veg/Imperv.tif` over the VPU01 extent: of 10.96M cells with any impervious surface (1–100%), only **16.14% (1.77M cells) clear the 50% threshold**. The other 84% — most of which is roads at 30m resolution — is dropped. A 2-lane road (~7m) crossing a 30m pixel orthogonally covers ~23% (in the 20-30% band); only urban arterials with shoulders + medians + adjacent parking exceed 50%. This is faithful to the legacy `0b_TB_depr_stor.py:getImpervBin` ("VALUE > 50"), so the port did not introduce it — but it propagates to a systematically under-counted `hru_percent_imperv`. Options: (a) lower the threshold to ~20-25%, (b) compute `hru_percent_imperv` from the raw fractional `Imperv.tif` (0-100) via gdptools `mean / 100` and keep the 50% binary only for dprst exclusion, (c) write two separate rasters (binary for dprst, fractional for hru_percent_imperv). Option (b) or (c) is the architecturally correct fix.



1. **Document or normalize depstor zonal-stat outputs.** The inline comment in `configs/dprst_frac_param.yml` (and the parallel three configs) claims "per-HRU mean is the fraction." That's only true when the source raster is `1/0` — the depstor port writes `1/nodata=255`, so `count` (not `mean`) is the actually-useful column. Either update the comments, or change `categorical: false` callsites to emit a normalized fraction (count / hru_pixel_area). Worth filing as a separate issue.

2. **Add a sanity assertion to `build_depstor_routing.py`.** The all-black 100%-coverage bug shipped silently as `ExitCode=0`. A post-condition like "drains_to_dprst.tif coverage must be < 50% of the total grid" (it's typically < 1%) would have caught it. Same idea for the other depstor rasters (e.g., `imperv_binary.tif` should not exceed 5% in most fabrics).

3. **Layer naming inconsistency.** `NHM_01_draft.gpkg` uses `wbs`; the CONUS `{fabric}_segments_wbodies.gpkg` uses `v2_wb`. Worth standardizing during the CONUS fabric prep.

4. **Issue #41 sidestepped here.** Routing in this run used the per-VPU `Fdr_merged_01.tif` directly. The CONUS run still needs the chunked rioxarray reproject (or full replacement with a `gdal.Warp` cutline path).

5. **No parity comparison.** Per the issue, parity vs. the standalone `depstor` reference repo is deferred. Worth doing now that the routing pipeline produces meaningful outputs.

6. **CONUS scale-up checklist** (separate work):
  - Resolve follow-up #4.
  - Right-size CONUS resource asks. Current values (routing 192G/12h, waterbody 128G/8h) were placeholders — VPU01 used 48G/2h and finished in <3min for the heaviest step. CONUS is ~20× the area of VPU01; budgeting 8h on routing and 4h on waterbody is probably overkill but safe.
  - Stage `{fabric}_segments_wbodies.gpkg` with the chosen layer naming convention (per follow-up #3).
  - Add the sanity assertion from follow-up #2 before the CONUS run, since CONUS reruns are expensive.
