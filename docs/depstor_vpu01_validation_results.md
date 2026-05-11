# Depression-storage VPU 01 validation results (issue #38)

Branch: `feat/depstor-validation-issue-38`
Run window: _TBD — populate after submission_

## Configuration

- Fabric: `gfv2_vpu01`
- Template grid: `work/nhd_merged/01/Hydrodem_merged_01.tif` (25,845 × 31,405, unnamed Albers ≈ EPSG:5070)
- FDR: `work/nhd_merged/01/Fdr_merged_01.tif` (per-VPU; sidesteps the CONUS `fdr.vrt` memory issue tracked in issue #41)
- Fabric polygons: `input/fabric/NHM_01_draft.gpkg` layers `nhru` (11,278 features, `nat_hru_id` 1..11,278), `nsegment`, `wbs` (note: NOT `v2_wb`)
- Batch size: 2000 → expect ~6 batches in `gfv2_vpu01/batches/`

## Job submission record

| Step | Batch script | Job ID | Status | Elapsed |
|---|---|---|---|---|
| imperv | `build_depstor_imperv_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| streambuffer | `build_depstor_streambuffer_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| waterbody | `build_depstor_waterbody_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| dprst | `build_depstor_dprst_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| routing | `build_depstor_routing_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| dprst_frac (array) | `create_dprst_frac_params_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| imperv_frac (array) | `create_imperv_frac_params_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| onstream_frac (array) | `create_onstream_storage_frac_params_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |
| drains_to_dprst_frac (array) | `create_drains_to_dprst_frac_params_vpu01.batch` | _TBD_ | _TBD_ | _TBD_ |

## Raster output summary

_Populate from `notebooks/check_depstor_vpu01.ipynb` raster summary table._

| Raster | Shape | Dtype | Valid % | % == 1 | Notes |
|---|---|---|---|---|---|
| `imperv_binary.tif` | | uint8 | | | |
| `stream_buffer.tif` | | uint8 | | | |
| `wbody_binary.tif` | | uint8 | | | |
| `wbody_regions.tif` | | int32 | | N/A (n_regions=) | |
| `dprst_binary.tif` | | uint8 | | | |
| `onstream_binary.tif` | | uint8 | | | |
| `drains_to_dprst.tif` | | uint8 | | | |

## Zonal CSV summary

_Populate from `notebooks/check_depstor_vpu01.ipynb` zonal table._

| Source | Batch files | Rows | Unique HRUs | NaNs | min/max | mean | In [0,1]? | Count match (11,278)? |
|---|---|---|---|---|---|---|---|---|
| dprst_frac | | | | | | | | |
| imperv_frac | | | | | | | | |
| onstream_storage_frac | | | | | | | | |
| drains_to_dprst_frac | | | | | | | | |

## Observations

_TBD — fill in after running. Document anything surprising vs. the expectations in the plan._

## Known scope limits / follow-ups

- **Issue #41 sidestepped.** Routing here uses the per-VPU `Fdr_merged_01.tif` directly. The CONUS run still needs the chunked rioxarray reproject (or `gdal.Warp` cutline) before it can consume `work/nhd_merged/fdr.vrt`.
- **Layer naming inconsistency.** `NHM_01_draft.gpkg` uses `wbs`; the CONUS `{fabric}_segments_wbodies.gpkg` uses `v2_wb`. Worth standardizing during the CONUS fabric prep — file a follow-up if a convention is chosen.
- **No parity comparison.** Per the issue, parity vs. the standalone `depstor` reference repo is deferred. Once the smoke test passes, schedule a parity comparison against the reference outputs.
- **CONUS scale-up checklist** (separate work):
  - Resolve issue #41 (chunked `fdr.vrt` reproject)
  - Verify resource asks at CONUS scale (existing CONUS batches: routing 192G/12h, waterbody 128G/8h)
  - Stage `{fabric}_segments_wbodies.gpkg` with the `v2_wb` layer naming OR update configs to read from a per-VPU pattern.
