# PRMS parameter index

Every parameter this pipeline emits to `{fabric}/params/merged/`, mapped to the PRMS
process that consumes it, the config entry that declares it, and the builder that computes
it.

Process membership is from `pywatershed.<Process>.get_parameters()` (pywatershed 2.0.4, the
`reference` pixi env), not inference. Column lists are observed on-disk headers from
`gfv2/params/merged/`.

**19 config entries** are declared, of which **17** are built for gfv2 — `lulc_nlcd` and
`lulc_foresce` have never been built on any fabric (see
[Not built on any fabric](#not-built-on-any-fabric)). That is the complete set. Note
`gfv2/params/merged/` also still holds two `filled_nhm_*.csv` files from the retired
`filled_` prefix convention (PR #189) — they are superseded, not parameters, and are not
listed here.

> **The three tables below are GENERATED** from the `prms:` blocks in `configs/` by
> `scripts/build_parameter_index.py` — do not hand-edit them; edit the config and re-run.
> Everything else on this page (this preamble, the notes under each view, Known gaps) is
> hand-maintained and the generator never touches it. Run
> `python scripts/build_parameter_index.py --check` to detect staleness.

## Reading this index

- ⚠️ marks a column whose **emitted name is not the PRMS parameter name**. You must
  translate it before feeding PRMS.
- **DEFECTIVE** marks a column that does not currently represent the PRMS parameter at all.
  Do not use it.
- *provenance* marks an emitted column that is not a PRMS parameter — a diagnostic, an
  intermediate, or a raw statistic.

**Three columns are renames** (⚠️): `mean`→`hru_elev`, `soils`→`soil_type` and
`retention`→`rad_trncf`. Feed any of them to PRMS under its emitted name and PRMS will not
recognise it. All three are safe once you know them; the per-process tables below mark every
one with ⚠️.

**One column is actively dangerous** rather than merely misnamed — it produces wrong numbers:

| Emitted | Reality |
| --- | --- |
| `nhm_aspect_params.csv:mean` | **DEFECTIVE** — arithmetic mean of a circular variable ([#201](https://github.com/rmcd-mscb/gfv2-params/issues/201)) |

Two further dangers this index originally flagged are **now fixed**:
`hru_slope` is emitted directly in rise/run rather than left as degrees under the name `mean`
(see [Known gaps](#hru_slope-was-degrees-on-disk-fixed)), and `op_flow_thres` is copied into
`merged/` rather than living only in `depstor_rasters/` (see
[Known gaps](#op_flow_thres-was-not-in-merged-fixed)).

---

## By PRMS process

<!-- BEGIN GENERATED: by-process -->
### PRMSRunoff — 11 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `carea_max` | `nhm_carea_max_params.csv` | `carea_max` | `depstor_params.yml:172` | `depstor_builders/carea_map.py` |
| `dprst_depth_avg` | `nhm_dprst_depth_avg_params.csv` | `dprst_depth_avg` | `depstor_params.yml:107` | `depstor_builders/dprst_depth.py + dprst_depth/aggregate.py` |
| `dprst_flow_coef` | `nhm_ssflux_params.csv` | `dprst_flow_coef` | `zonal_params.yml:451` | `zonal_runners/ssflux.py` |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | `depstor_params.yml:224` | `depstor_builders/dprst.py + landmask.py` |
| `dprst_seep_rate_open` | `nhm_ssflux_params.csv` | `dprst_seep_rate_open` | `zonal_params.yml:451` | `zonal_runners/ssflux.py` |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | `depstor_params.yml:202` | `depstor_builders/imperv.py + landmask.py` |
| `op_flow_thres` | `nhm_op_flow_thres_params.csv` | `op_flow_thres` | `depstor_params.yml:252` | `depstor_builders/dprst_depth.py` |
| `smidx_coef` | `nhm_smidx_coef_params.csv` | `smidx_coef` | `depstor_params.yml:186` | `depstor_builders/carea_map.py` |
| `soil_moist_max` | `nhm_soil_moist_max_params.csv` | `soil_moist_max` | `zonal_params.yml:187` | `zonal_runners/soils.py` |
| `sro_to_dprst_imperv` | `nhm_sro_to_dprst_imperv_params.csv` | `sro_to_dprst_imperv` | `depstor_params.yml:158` | `depstor_builders/same_hru_drains.py + imperv.py` |
| `sro_to_dprst_perv` | `nhm_sro_to_dprst_perv_params.csv` | `sro_to_dprst_perv` | `depstor_params.yml:144` | `depstor_builders/same_hru_drains.py + perv.py` |

### PRMSSoilzone — 9 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `cov_type` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `cov_type` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | `depstor_params.yml:224` | `depstor_builders/dprst.py + landmask.py` |
| `fastcoef_lin` | `nhm_ssflux_params.csv` | `fastcoef_lin` | `zonal_params.yml:451` | `zonal_runners/ssflux.py` |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | `depstor_params.yml:202` | `depstor_builders/imperv.py + landmask.py` |
| `slowcoef_lin` | `nhm_ssflux_params.csv` | `slowcoef_lin` | `zonal_params.yml:451` | `zonal_runners/ssflux.py` |
| `soil2gw_max` | `nhm_ssflux_params.csv` | `soil2gw_max` | `zonal_params.yml:451` | `zonal_runners/ssflux.py` |
| `soil_moist_max` | `nhm_soil_moist_max_params.csv` | `soil_moist_max` | `zonal_params.yml:187` | `zonal_runners/soils.py` |
| `soil_type` ⚠️ | `nhm_soils_params.csv` | `soils` | `zonal_params.yml:169` | `zonal_runners/soils.py` |
| `ssr2gw_rate` | `nhm_ssflux_params.csv` | `ssr2gw_rate` | `zonal_params.yml:451` | `zonal_runners/ssflux.py` |

### PRMSSnow — 7 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `cov_type` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `cov_type` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `covden_sum` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `covden_sum` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `covden_win` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `covden_win` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `hru_deplcrv` | `nhm_snarea_curve_params.csv` | `hru_deplcrv` | `snarea_library.yml` | `snarea/library.py` |
| `rad_trncf` ⚠️ | `nhm_lulc_nhm_v11_params.csv` | `retention` \| `rad_trncf` | `zonal_params.yml:209` | `zonal_runners/lulc_prederived.py` |
| `snarea_curve` | `nhm_snarea_curve_params.csv` | `snarea_curve_0` … `snarea_curve_10` | `snarea_library.yml` | `snarea/library.py` |
| `snarea_thresh` | `nhm_snarea_curve_params.csv` | `snarea_thresh` | `snarea_library.yml` | `snarea/library.py` |

### PRMSCanopy — 6 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `cov_type` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `cov_type` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `covden_sum` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `covden_sum` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `covden_win` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `covden_win` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `snow_intcp` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `snow_intcp` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `srain_intcp` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `srain_intcp` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |
| `wrain_intcp` | `nhm_lulc_nhm_v11_params.csv` · `nhm_lulc_nalcms_params.csv` · `nhm_lulc_nlcd_params.csv` · `nhm_lulc_foresce_params.csv` | `wrain_intcp` | `zonal_params.yml:209` · `zonal_params.yml:277` · `zonal_params.yml:331` · `zonal_params.yml:389` | `zonal_runners/lulc_prederived.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` · `zonal_runners/lulc.py` |

### PRMSEt — 2 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | `depstor_params.yml:224` | `depstor_builders/dprst.py + landmask.py` |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | `depstor_params.yml:202` | `depstor_builders/imperv.py + landmask.py` |

### PRMSAtmosphere — 1 parameter (+1 defective)

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `hru_slope` | `nhm_slope_params.csv` | `hru_slope` | `zonal_params.yml:81` | `zonal_runners/zonal.py + zonal_runners/merge.py (derived_columns)` |
| `hru_aspect` | `nhm_aspect_params.csv` | **DEFECTIVE** — `mean` is not this parameter ([#201](https://github.com/rmcd-mscb/gfv2-params/issues/201)) | `zonal_params.yml:133` | `zonal_runners/zonal.py` |

### PRMSGroundwater — 1 parameter

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `gwflow_coef` | `nhm_ssflux_params.csv` | `gwflow_coef` | `zonal_params.yml:451` | `zonal_runners/ssflux.py` |

### PRMSSolarGeometry — 1 parameter (+1 defective)

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `hru_slope` | `nhm_slope_params.csv` | `hru_slope` | `zonal_params.yml:81` | `zonal_runners/zonal.py + zonal_runners/merge.py (derived_columns)` |
| `hru_aspect` | `nhm_aspect_params.csv` | **DEFECTIVE** — `mean` is not this parameter ([#201](https://github.com/rmcd-mscb/gfv2-params/issues/201)) | `zonal_params.yml:133` | `zonal_runners/zonal.py` |

### Not consumed by any pywatershed process — 1 parameter

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `hru_elev` ⚠️ | `nhm_elevation_params.csv` | `mean` | `zonal_params.yml:43` | `zonal_runners/zonal.py` |
<!-- END GENERATED: by-process -->

**Notes on the tables above**

- **Cells separated by `·` are alternative sources for the same parameter**, and the file,
  config-entry and builder cells are **row-aligned**: the *n*-th file comes from the *n*-th
  config entry via the *n*-th builder.
- **The four LULC entries are alternatives, not four separate parameter sets.** That is the
  only place `·` appears today. `lulc_nhm_v11`, `lulc_nalcms`, `lulc_nlcd` and
  `lulc_foresce` each emit the same `cov_type` / `covden_*` / `*_intcp` set. Pick **one**
  source per model run; `nhm_v11` and `nalcms` are both on disk for gfv2, and only `nhm_v11`
  also derives `rad_trncf`. `lulc_nlcd` and `lulc_foresce` have never been built (see
  [Not built on any fabric](#not-built-on-any-fabric)).
- `dprst_frac` also appears in `merged/_intermediates/` under the same filename — that copy
  is a partial-pixel **count**, not a `[0, 1]` fraction. The PRMS parameter is the one in
  `merged/`.
- `hru_elev` appears in no pywatershed `Process.get_parameters()` list. It is used by PRMS's
  temperature/precipitation distribution modules — which pywatershed handles by reading CBH
  files — and by the `cov_type` reset rule at
  [TM6B9:707](NHM_description_Regan_2018_TM6B9.md) (HRUs above 11,500 ft). That is why its
  `processes:` is empty rather than naming a module pywatershed does not have.
- `snarea_curve` is one PRMS parameter of extent `ndeplval`, emitted as the 11 columns
  `snarea_curve_0` … `snarea_curve_10`.
- `soils` → `soil_type` is an **identity** mapping (1=sand, 2=loam, 3=clay) under a different
  name; the source metadata says otherwise and is wrong — see
  [Known gaps](#soils-soil_type-is-an-identity-mapping-and-the-source-metadata-says-otherwise).

---

## By config entry

<!-- BEGIN GENERATED: by-entry -->
| Config entry | Merged file | PRMS parameters | Provenance columns |
| --- | --- | --- | --- |
| `dprst_depth_avg` | `nhm_dprst_depth_avg_params.csv` | `dprst_depth_avg` | `dprst_depth_provenance` |
| `sro_to_dprst_perv` | `nhm_sro_to_dprst_perv_params.csv` | `sro_to_dprst_perv` | — |
| `sro_to_dprst_imperv` | `nhm_sro_to_dprst_imperv_params.csv` | `sro_to_dprst_imperv` | — |
| `carea_max` | `nhm_carea_max_params.csv` | `carea_max` | — |
| `smidx_coef` | `nhm_smidx_coef_params.csv` | `smidx_coef` | — |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | — |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | — |
| `op_flow_thres` | `nhm_op_flow_thres_params.csv` | `op_flow_thres` | — |
| `snarea_curve` | `nhm_snarea_curve_params.csv` | `hru_deplcrv`, `snarea_curve`, `snarea_thresh` | `cv_assign`, `cv_empirical`, `cv_source`, `cv_subgrid`, `n_peak_years`, `n_seasons`, `peak_swe_mm`, `sca_class`, `sdc_status`, `similarity` |
| `elevation` | `nhm_elevation_params.csv` | `hru_elev` | `25%`, `50%`, `75%`, `count`, `max`, `min`, `std`, `sum` |
| `slope` | `nhm_slope_params.csv` | `hru_slope` | `25%`, `50%`, `75%`, `count`, `max`, `mean`, `min`, `std`, `sum` |
| `aspect` | `nhm_aspect_params.csv` | `hru_aspect` — **DEFECTIVE** (emitted as `mean`) | `25%`, `50%`, `75%`, `count`, `max`, `min`, `std`, `sum` |
| `soils` | `nhm_soils_params.csv` | `soil_type` | — |
| `soil_moist_max` | `nhm_soil_moist_max_params.csv` | `soil_moist_max` | — |
| `lulc_nhm_v11` | `nhm_lulc_nhm_v11_params.csv` | `cov_type`, `covden_sum`, `covden_win`, `rad_trncf`, `snow_intcp`, `srain_intcp`, `wrain_intcp` | — |
| `lulc_nalcms` | `nhm_lulc_nalcms_params.csv` | `cov_type`, `covden_sum`, `covden_win`, `snow_intcp`, `srain_intcp`, `wrain_intcp` | `retention` |
| `lulc_nlcd` | `nhm_lulc_nlcd_params.csv` | `cov_type`, `covden_sum`, `covden_win`, `snow_intcp`, `srain_intcp`, `wrain_intcp` | `retention` |
| `lulc_foresce` | `nhm_lulc_foresce_params.csv` | `cov_type`, `covden_sum`, `covden_win`, `snow_intcp`, `srain_intcp`, `wrain_intcp` | `retention` |
| `ssflux` | `nhm_ssflux_params.csv` | `dprst_flow_coef`, `dprst_seep_rate_open`, `fastcoef_lin`, `gwflow_coef`, `slowcoef_lin`, `soil2gw_max`, `ssr2gw_rate` | `hru_area`, `k_perm_wtd`, `mean_slope_fraction` |
<!-- END GENERATED: by-entry -->

**Notes on the table above**

- The 10 `fractions:` entries in `depstor_params.yml` are **intermediates**, not parameters.
  They write partial-pixel-weighted count CSVs to `merged/_intermediates/` and feed the
  `ratios:` above. `iter_declared_params` excludes them, so they never appear here.
- `constants:` holds params a depstor **builder** writes directly, with no zonal pass — their
  source lives in `{fabric}/depstor_rasters/`, and
  `scripts/derive_depstor_params.py --mode copy_constants` copies each into `merged/`.
- `snarea_curve`'s entry is the whole of `configs/snarea/snarea_library.yml`, not a list
  element — it comes from the separate 3-stage SNODAS pipeline, so its `prms:` block is a
  top-level key.
- snarea's `cv_*` provenance columns are **derivable for only ~42% of HRUs by design** —
  `cv_subgrid` rescues the rest. A NaN there is a result, not a gap, which is why they are
  excluded from `fill_columns`.
- `slope`'s `hru_slope` is not a raw zonal statistic: it is declared via `derived_columns:`
  and computed at merge time from `mean`. See [Architecture](ARCHITECTURE.md).

---

## By builder

<!-- BEGIN GENERATED: by-builder -->
| Builder module | PRMS parameters produced |
| --- | --- |
| `depstor_builders/carea_map.py` | `carea_max`, `smidx_coef` |
| `depstor_builders/dprst.py + landmask.py` | `dprst_frac` |
| `depstor_builders/dprst_depth.py` | `op_flow_thres` |
| `depstor_builders/dprst_depth.py + dprst_depth/aggregate.py` | `dprst_depth_avg` |
| `depstor_builders/imperv.py + landmask.py` | `hru_percent_imperv` |
| `depstor_builders/same_hru_drains.py + imperv.py` | `sro_to_dprst_imperv` |
| `depstor_builders/same_hru_drains.py + perv.py` | `sro_to_dprst_perv` |
| `snarea/library.py` | `hru_deplcrv`, `snarea_curve`, `snarea_thresh` |
| `zonal_runners/lulc.py` | `cov_type`, `covden_sum`, `covden_win`, `snow_intcp`, `srain_intcp`, `wrain_intcp` |
| `zonal_runners/lulc_prederived.py` | `cov_type`, `covden_sum`, `covden_win`, `rad_trncf`, `snow_intcp`, `srain_intcp`, `wrain_intcp` |
| `zonal_runners/soils.py` | `soil_moist_max`, `soil_type` |
| `zonal_runners/ssflux.py` | `dprst_flow_coef`, `dprst_seep_rate_open`, `fastcoef_lin`, `gwflow_coef`, `slowcoef_lin`, `soil2gw_max`, `ssr2gw_rate` |
| `zonal_runners/zonal.py` | `hru_elev`, `hru_aspect` **(DEFECTIVE)** |
| `zonal_runners/zonal.py + zonal_runners/merge.py (derived_columns)` | `hru_slope` |
<!-- END GENERATED: by-builder -->

The `builder` string comes from each entry's `prms.builder` key, so it cannot drift from the
configs the way a lookup table in the generator would. The depstor ratios are finalised by
`gfv2_params/depstor_ratios.py` (a top-level module, not part of `depstor_builders/`), driven
by `scripts/derive_depstor_params.py --mode ratios`.

---

## Known gaps

### `hru_slope` was degrees on disk — fixed

`slope.vrt` is built as `rd.TerrainAttribute(dem, attrib="slope_degrees")`
(`shared_rasters/compute_slope_aspect.py:72`), so `nhm_slope_params.csv:mean` is **degrees**,
while PRMS `hru_slope` is a decimal fraction rise/run
([TM6B9:536](NHM_description_Regan_2018_TM6B9.md)).

For small angles the discrepancy is 180/π ≈ **57×**. gfv2 HRU 1: `mean = 4.4252` →
`hru_slope = 0.0774`. Copied verbatim, that declares a 77° cliff instead of a 4.4° hillslope.

**The pipeline was never wrong internally** — `zonal_runners/ssflux.py:63` applies
`raster_ops.deg_to_fraction` before deriving `ssr2gw_rate`/`slowcoef_lin`, so the flux
parameters have always been correct. What was missing is that nothing *emitted* `hru_slope`.

`nhm_slope_params.csv` now carries an `hru_slope` column, declared as
`derived_columns: {hru_slope: {from: mean, transform: deg_to_fraction}}` on the `slope` entry
and applied by `zonal_runners/merge.py` at merge time. `mean` is kept alongside it as
declared provenance, so the derivation stays checkable. Because it is applied in `run_merge`,
**no zonal re-run is needed** — but an existing fabric does need one re-merge:

```bash
python scripts/derive_zonal_params.py --config configs/zonal/zonal_params.yml \
  --base_config configs/base_config.yml --fabric <fabric> --mode merge --param slope
```

`hru_slope` is declared in `fill_columns`, so a slope CSV merged before this change will make
the next fill sweep **raise** until it is re-merged. That failure is loud and one command to
fix; the alternative — a silent NaN `hru_slope` for exactly the HRUs that were missing — is
not. (Run on gfv2 2026-08-05; oregon and tjc still need it.)

Known approximation: `tan(mean θ) ≠ mean(tan θ)`, and `tan` is convex, so the conversion
systematically underestimates. Estimated (second-order Taylor from the on-disk `mean`/`std` —
`mean(tan θ)` is not recoverable from summary statistics) over all 361,471 gfv2 HRUs: median
**0.2%**, p90 2.4%, p99 5.6%. Too small to justify building a CONUS fractional-slope VRT;
none exists, and `ssflux` has carried the same approximation since it was written.

### `hru_aspect` is DEFECTIVE — do not use `nhm_aspect_params.csv:mean`

It is an **arithmetic** mean of a **circular** variable.
[TM6B9:603](NHM_description_Regan_2018_TM6B9.md) requires
`atan2(mean(sin(aspect)), mean(cos(aspect)))`; the `aspect` entry runs the generic `zonal`
script instead.

Across all 361,471 gfv2 HRUs: median `mean` = **179.4°** (due south), IQR 151.7°–207.5°,
median within-HRU `std` 91.3° (uniform-circular ≈ 104°). Half of CONUS reporting a mean
orientation between 152° and 208° is not terrain — it is what arithmetic-averaging wrapped
directions produces.

The circularity rule *was* applied at every raster boundary
(`compute_slope_aspect.py:68`, `build_vrt.py:74`, `build_border_dem.py:224`, `cog.py:84`,
with tests). The zonal-side limitation was recorded at the time as a known simplification;
what was never done is measure it. Unlike `hru_slope`, this **cannot be repaired from what
is on disk** — a circular mean is not recoverable from an arithmetic one.

Tracked as [#201](https://github.com/rmcd-mscb/gfv2-params/issues/201).

### `op_flow_thres` was not in `merged/` — fixed

A PRMSRunoff parameter written by a depstor builder to
`{fabric}/depstor_rasters/op_flow_thres_params.csv`. Because it never reached `merged/`, the
gap-fill sweep never saw it and the "undeclared merged file" guard could not flag it — anyone
assembling a parameter file by globbing `merged/nhm_*_params.csv` silently dropped it.

It is now declared as a `constants:` entry in `depstor_params.yml` and copied into
`merged/nhm_op_flow_thres_params.csv` by
`scripts/derive_depstor_params.py --mode copy_constants` (step 4 of
`slurm_batch/RUNME.md`). It is a constant 1.0 for every HRU.

`constants:`, not `means:`, deliberately: `run_mean_zonal` reads
`spec["source_raster"]` unconditionally and `_find_mean` advertises every `means[].name`
as a runnable `--mean` target, so a raster-less `means` entry is a `KeyError` waiting for
the first operator who types `--mean op_flow_thres`.

### `retention` means two different things

On `lulc_nhm_v11` it is PRMS `rad_trncf` — `lulc_prederived.py:176-181` computes a Beer's-law
transform, and the column was renamed to `rad_trncf` after gfv2/oregon were built.

On `lulc_nalcms` / `lulc_nlcd` / `lulc_foresce` it is **not** `rad_trncf`.
`lulc.py:186-193` computes `zonal_mean(keep)/100` or the crosswalk's `evergreen_retention`
column. The module *does* carry a Beer's-law `rad_trncf` path (`lulc.py:194-239`), but it is
gated on `radtrn_raster`, which is configured for none of these three entries
(`zonal_params.yml:163-165` says so) — so it never runs for them. What
PRMS parameter it corresponds to, if any, is unverified.

### `soils` → `soil_type` is an identity mapping — and the source metadata says otherwise

**Verified 2026-08-05.** `TEXT_PRMS.tif` is already in PRMS `soil_type` encoding
(**1 = sand, 2 = loam, 3 = clay**), so `nhm_soils_params.csv:soils` can be used as
`soil_type` unchanged. Only the *name* differs.

⚠️ **The ScienceBase metadata for `TEXT_PRMS.tif` contradicts this, and it is wrong.** Its
`edom` block describes value 1 as "Percent clay greater than 40", value 2 as sand, value 3
as loam — i.e. clay and sand transposed relative to PRMS. Anyone who follows it and remaps
1↔3 will **silently invert sand and clay for every HRU**.

The raster's own values settle it. Observed cell counts (`rasterio`, full CONUS grid,
27317×15906 uint8):

| Value | Cells | % of valid | metadata `edom` claims | actual |
| --- | --- | --- | --- | --- |
| 1 | 38,489,559 | 29.4% | clay > 40% | **sand** |
| 2 | 91,238,514 | 69.6% | sand | **loam** |
| 3 | 1,288,556 | 1.0% | loam | **clay** |

Three independent reasons the data, not the metadata, is right:

1. The metadata's own `Count` range — min **1,288,556**, max **91,238,514** — matches values
   3 and 2 exactly, so this is the raster the metadata describes; only its class
   *descriptions* are transposed.
2. 29.4% of CONUS exceeding 40% clay is not credible; 1.0% is. Loam at 1.0% is absurd;
   69.6% is right.
3. [TM6B9:786](NHM_description_Regan_2018_TM6B9.md) assigns the **residual** to class 2
   ("*or 2 for the remaining cells*"), and a residual category is naturally the majority —
   which class 2 is.

Cross-check against the delivered product: `nhm_soils_params.csv` gives 30.5% / 69.0% / 0.4%
across 361,471 HRUs — the same shape, as expected for per-HRU dominant class.

### Unverified mappings

- **`mean` → `hru_elev`** — inferred from `viz.py:505-507` plus
  [TM6B9:601](NHM_description_Regan_2018_TM6B9.md). Elevation is neither circular nor
  transformed, so the arithmetic zonal mean is the right statistic; only the *name* is
  undocumented.
- **pywatershed's process lists are pywatershed's view of PRMS**, not TM6B9's NHM module set.
  They agreed everywhere spot-checked, but `hru_elev` appears in no pywatershed process at
  all, so the two are not identical.

### Not built on any fabric

`lulc_nlcd` and `lulc_foresce` are declared in `zonal_params.yml` but their source
directories (`input/lulc_veg/nlcd/`, `input/lulc_veg/foresce/`) do not exist. A CONUS run
submits them and they fail; the other eight params merge normally.

---

## See also

- [Adding a parameter](ADDING_A_PARAMETER.md) — end-to-end trace of `--param elevation`.
- [Architecture](ARCHITECTURE.md) — the orchestrator + builder + unified-config pattern.
- [`docs/nhm_source_crosscheck_2026-07.md`](nhm_source_crosscheck_2026-07.md) — TM 6-B9 and
  Driscoll 2020 vs. this pipeline, including where `ssflux` deviates from TM 6-B9.
- [NHM description (Regan 2018, TM 6-B9)](NHM_description_Regan_2018_TM6B9.md) — the
  authoritative parameter definitions.
