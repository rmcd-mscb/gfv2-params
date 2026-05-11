# Depression-storage VPU 01 validation results (issue #38)

Branch: `feat/depstor-validation-issue-38`
Run submitted: 2026-05-11 15:39 UTC
Run completed: 2026-05-11 15:45 UTC (~3 min, end-to-end for the 8 jobs that ran to completion)

**Outcome:** 8 of 9 jobs `COMPLETED`. Routing (`build_depstor_routing_vpu01.batch`, job 17021337) `FAILED` on an xarray/rioxarray `_FillValue` attribute conflict in `scripts/build_depstor_routing.py:65`. The dependent `drains_to_dprst_frac` array (17021341) was `CANCELLED` once routing failed. **6 of 7 expected rasters and 3 of 4 zonal-stat CSV groups were produced; sanity checks on all of these pass** (see Raster / Zonal sections below). The routing bug is a real source-code issue surfaced by validation — filing as a separate follow-up per the plan's "no script changes in this PR" guidance.

## Configuration

- Fabric: `gfv2_vpu01`
- Template grid: `work/nhd_merged/01/Hydrodem_merged_01.tif` (25,845 × 31,405, unnamed Albers ≈ EPSG:5070)
- FDR: `work/nhd_merged/01/Fdr_merged_01.tif` (per-VPU; sidesteps the CONUS `fdr.vrt` memory issue tracked in issue #41)
- Fabric polygons: `input/fabric/NHM_01_draft.gpkg` layers `nhru` (11,278 features, `nat_hru_id` 1..11,278), `nsegment`, `wbs` (note: NOT `v2_wb`)
- Batch size: 2000 → KD-tree bisection produced **8** batches in `gfv2_vpu01/batches/`

## Job submission record

8 batches in `gfv2_vpu01/batches/` (KD-tree bisection from `batch_size: 2000` over 11,278 features → array range `0-7`).

| Step | Batch script | Job ID | Dependency | Status | Elapsed |
|---|---|---|---|---|---|
| imperv | `build_depstor_imperv_vpu01.batch` | 17021333 | — | COMPLETED | 1m10s |
| streambuffer | `build_depstor_streambuffer_vpu01.batch` | 17021334 | — | COMPLETED | 0m59s |
| waterbody | `build_depstor_waterbody_vpu01.batch` | 17021335 | — | COMPLETED | 1m07s |
| dprst | `build_depstor_dprst_vpu01.batch` | 17021336 | afterok: 17021333, 17021334, 17021335 | COMPLETED | 0m22s |
| routing | `build_depstor_routing_vpu01.batch` | 17021337 | afterok: 17021336 | **FAILED** (exit 1) | 0m56s |
| dprst_frac (array 0-7) | `create_dprst_frac_params_vpu01.batch` | 17021338 | afterok: 17021336 | COMPLETED (all 8) | 0:31–0:59 per task |
| imperv_frac (array 0-7) | `create_imperv_frac_params_vpu01.batch` | 17021339 | afterok: 17021333 | COMPLETED (all 8) | 0:53–1:25 per task |
| onstream_frac (array 0-7) | `create_onstream_storage_frac_params_vpu01.batch` | 17021340 | afterok: 17021336 | COMPLETED (all 8) | 0:57–0:59 per task |
| drains_to_dprst_frac (array 0-7) | `create_drains_to_dprst_frac_params_vpu01.batch` | 17021341 | afterok: 17021337 | CANCELLED (dep never satisfied) | n/a |

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
| `drains_to_dprst.tif` | — | — | — | — | **MISSING — routing job failed (job 17021337)** |

**Plausibility check:** dprst (0.009%) << onstream (0.621%) is consistent with most New England waterbodies being on-stream (lakes connected to the river network). The wbody_binary ≈ dprst + onstream identity holds approximately (0.630 ≈ 0.009 + 0.621 + tiny boundary loss).

## Zonal CSV summary

Each `*_frac` group has 8 batch CSV files (one per `gfv2_vpu01/batches/batch_NNNN.gpkg`). CSV columns are the gdptools / exactextract defaults: `nat_hru_id, count, mean, std, min, 25%, 50%, 75%, max, sum`.

| Source | Files | Rows | Unique HRUs | Count match (11,278)? | NaNs in `count` | `count` range | `count` mean |
|---|---|---|---|---|---|---|---|
| `dprst_frac` | 8 | 11,278 | 11,278 | ✓ | 0 | 0.0 – 7,146.0 | 6.82 |
| `imperv_frac` | 8 | 11,278 | 11,278 | ✓ | 0 | 0.0 – 24,921.67 | 130.73 |
| `onstream_storage_frac` | 8 | 11,278 | 11,278 | ✓ | 0 | 0.0 – 65,182.21 | 446.67 |
| `drains_to_dprst_frac` | — | — | — | — | — | — | **MISSING — array cancelled (routing dep never satisfied)** |

**Important semantic note about the CSV format.** The binary source rasters use `1 / nodata=255` (NOT `1 / 0 / 255`). When gdptools+exactextract is invoked in `categorical: false` mode against this encoding, the resulting `mean` is `1.0` for every HRU with non-zero overlap (since every *valid* pixel is `1`), and `NaN` for HRUs with zero overlap. The actually-useful column is **`count`** — the (sub-pixel-weighted) number of `1`-valued cells per HRU. To convert this to a true fraction of HRU area, a downstream step must divide by the total HRU area in pixels (or use an HRU-area lookup). This appears to be intentional given the workflow's downstream consumers, but it contradicts the inline comment in `configs/dprst_frac_param.yml` lines 3-4 that claims "per-HRU mean is the fraction" — flagging as a documentation follow-up below.

## Observations

1. **Wall-time was minutes, not hours.** The plan estimated 1–2h end-to-end at VPU01 scale; actual was ~3 minutes. The per-VPU scope is small enough that the default CONUS-scale resource asks (192G/12h on routing) are wildly conservative. The reduced asks in the `_vpu01.batch` files (16-48G mem, 1-2h time) were more than sufficient; routing's 48G/2h would still be roughly 4x the actual needs once the script bug is fixed.

2. **Routing failure** — `scripts/build_depstor_routing.py:65` raises `ValueError: Key '_FillValue' already exists in attrs, and will not be overwritten` from inside `rioxarray.raster_array.to_raster` → `xarray.conventions.encode_cf_variable`. This is an xarray-2023+ stricter behavior triggered when the reprojected DataArray retains a `_FillValue` in `attrs` that conflicts with its encoding. The CONUS `input/depstor/{fabric}_fdr.tif` may have been pre-processed to avoid this (or the original test used a different xarray version). Workarounds:
   - One-line fix: `del fdr_aligned.attrs['_FillValue']` (or `.attrs.pop('_FillValue', None)`) before `to_raster` at line 65.
   - Cleaner fix: pass `nodata=...` to `to_raster` explicitly and drop the conflicting attr.
   - Best fix (aligns with issue #41): replace the rioxarray reproject_match path entirely with a `gdal.Warp` cutline (matches the pattern in `build_depstor_imperv.py:_warp_to_template`).

3. **Layer name `wbs` vs `v2_wb`.** Verified at Phase 0; the VPU01 config correctly uses `wbs`. No surprises.

4. **CRS handling worked.** Hydrodem_merged_01.tif advertises itself as unnamed Albers (NAD83 + GRS80 + Albers Equal Area params); NHM_01_draft.gpkg is EPSG:5070 (NAD83/Conus Albers). The build scripts' `to_crs(info.crs)` handled this transparently — no projection warnings in logs.

5. **Zonal CSV semantics gotcha.** See the note in the Zonal CSV summary above. `count` is the parameter to use, not `mean`. The CONUS-scale runs will hit the same surprise; a docstring clarification on `configs/*_frac_param.yml` would save someone an afternoon of debugging.

## Follow-ups (file as separate issues)

1. **NEW — `_FillValue` conflict in `build_depstor_routing.py`.** Job 17021337 failed on this. See Observations #2 for the exact stack trace and three candidate fixes. Highest priority — this blocks both the VPU01 `drains_to_dprst_frac` and the eventual CONUS routing run. Issue title suggestion: *"Fix `_FillValue` conflict in `build_depstor_routing.py` rioxarray to_raster call"*.

2. **NEW — clarify zonal-stat output semantics.** The inline comment in `configs/dprst_frac_param.yml` (and the parallel three configs) claims "per-HRU mean is the fraction." That's only true when the source raster is `1/0` — these rasters are `1/nodata=255`, so `count` (not `mean`) is the actually-useful column. Either update the comments, or change `categorical: false` callsites to emit a normalized fraction (count / hru_pixel_area). Issue title suggestion: *"Document or normalize depstor zonal-stat outputs: `count` is the parameter, not `mean`"*.

3. **Layer naming inconsistency.** `NHM_01_draft.gpkg` uses `wbs`; the CONUS `{fabric}_segments_wbodies.gpkg` uses `v2_wb`. Worth standardizing during the CONUS fabric prep.

4. **Issue #41 sidestepped here.** Routing in this run used the per-VPU `Fdr_merged_01.tif` directly. The CONUS run still needs the chunked rioxarray reproject (or — even better, see Observations #2 — full replacement with a `gdal.Warp` cutline path, which would also fix the `_FillValue` bug from follow-up #1 in one shot).

5. **No parity comparison.** Per the issue, parity vs. the standalone `depstor` reference repo is deferred. Worth doing once follow-ups #1 and #2 are resolved.

6. **CONUS scale-up checklist** (separate work):
  - Resolve follow-up #1 (and ideally #4 too).
  - Right-size CONUS resource asks. Current values (routing 192G/12h, waterbody 128G/8h) were placeholders — VPU01 used 48G/2h and finished in <1min for routing's parent steps. CONUS is ~20× the area of VPU01; budgeting 8h on routing and 4h on waterbody is probably overkill but safe.
  - Stage `{fabric}_segments_wbodies.gpkg` with the chosen layer naming convention (per follow-up #3).
  - Decide whether `drains_to_dprst_frac` array depends on routing-COMPLETED or routing-COMPLETED-OR-SKIPPED, so a routing failure doesn't cascade-cancel the array.
