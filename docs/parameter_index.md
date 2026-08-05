# PRMS parameter index

Every parameter this pipeline emits to `{fabric}/params/merged/`, mapped to the PRMS
process that consumes it, the config entry that declares it, and the builder that computes
it.

Process membership is from `pywatershed.<Process>.get_parameters()` (pywatershed 2.0.4, the
`reference` pixi env), not inference. Column lists are observed on-disk headers from
`gfv2/params/merged/`.

The 16 `nhm_*_params.csv` files below are the complete set. Note `gfv2/params/merged/` also
still holds two `filled_nhm_*.csv` files from the retired `filled_` prefix convention
(PR #189) — they are superseded, not parameters, and are not listed here.

> **Hand-maintained as of 2026-08-04.** Generated from `configs/` once
> `scripts/build_parameter_index.py` lands — see
> `docs/superpowers/specs/2026-08-04-prms-parameter-index-design.md` in the repo
> (the `superpowers/` tree is excluded from this site).

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

### PRMSRunoff (`srunoff_smidx`) — 11 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `soil_moist_max` | `nhm_soil_moist_max_params.csv` | `soil_moist_max` | `zonal_params.yml:81` | `zonal_runners/soils.py` |
| `dprst_seep_rate_open` | `nhm_ssflux_params.csv` | `dprst_seep_rate_open` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `dprst_flow_coef` | `nhm_ssflux_params.csv` | `dprst_flow_coef` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `sro_to_dprst_perv` | `nhm_sro_to_dprst_perv_params.csv` | `sro_to_dprst_perv` | `depstor_params.yml:134` | `depstor_builders/same_hru_drains.py` + `perv.py` → `depstor_ratios.py` |
| `sro_to_dprst_imperv` | `nhm_sro_to_dprst_imperv_params.csv` | `sro_to_dprst_imperv` | `depstor_params.yml:141` | `depstor_builders/same_hru_drains.py` + `imperv.py` → `depstor_ratios.py` |
| `carea_max` | `nhm_carea_max_params.csv` | `carea_max` | `depstor_params.yml:148` | `depstor_builders/carea_map.py` |
| `smidx_coef` | `nhm_smidx_coef_params.csv` | `smidx_coef` | `depstor_params.yml:155` | `depstor_builders/carea_map.py` |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | `depstor_params.yml:164` | `depstor_builders/imperv.py` + `landmask.py` |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | `depstor_params.yml:177` | `depstor_builders/dprst.py` + `landmask.py` |
| `dprst_depth_avg` | `nhm_dprst_depth_avg_params.csv` | `dprst_depth_avg` | `depstor_params.yml:107` (`means:`) | `depstor_builders/dprst_depth.py` + `dprst_depth/aggregate.py` |
| `op_flow_thres` | `nhm_op_flow_thres_params.csv` | `op_flow_thres` | `depstor_params.yml` (`constants:`) | `depstor_builders/dprst_depth.py:361` |

`dprst_frac` also appears in `merged/_intermediates/` under the same filename — that copy is
a partial-pixel **count**, not a `[0, 1]` fraction. The PRMS parameter is the one in
`merged/`.

### PRMSSoilzone — 9 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `soil_type` ⚠️ | `nhm_soils_params.csv` | `soils` — identity, **1=sand, 2=loam, 3=clay**; the source metadata is wrong, see [Known gaps](#soils-soil_type-is-an-identity-mapping-and-the-source-metadata-says-otherwise) | `zonal_params.yml:74` | `zonal_runners/soils.py:78` |
| `soil_moist_max` | `nhm_soil_moist_max_params.csv` | `soil_moist_max` | `zonal_params.yml:81` | `zonal_runners/soils.py` |
| `cov_type` | `nhm_lulc_nhm_v11_params.csv` | `cov_type` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `soil2gw_max` | `nhm_ssflux_params.csv` | `soil2gw_max` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `ssr2gw_rate` | `nhm_ssflux_params.csv` | `ssr2gw_rate` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `fastcoef_lin` | `nhm_ssflux_params.csv` | `fastcoef_lin` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `slowcoef_lin` | `nhm_ssflux_params.csv` | `slowcoef_lin` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | `depstor_params.yml:164` | `depstor_builders/imperv.py` + `landmask.py` |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | `depstor_params.yml:177` | `depstor_builders/dprst.py` + `landmask.py` |

### PRMSSnow — 7 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `cov_type` | `nhm_lulc_nhm_v11_params.csv` | `cov_type` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `covden_sum` | `nhm_lulc_nhm_v11_params.csv` | `covden_sum` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `covden_win` | `nhm_lulc_nhm_v11_params.csv` | `covden_win` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `rad_trncf` ⚠️ | `nhm_lulc_nhm_v11_params.csv` | `retention` (gfv2, oregon) \| `rad_trncf` (tjc) | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `hru_deplcrv` | `nhm_snarea_curve_params.csv` | `hru_deplcrv` | `snarea_library.yml` | `snarea/library.py` |
| `snarea_thresh` | `nhm_snarea_curve_params.csv` | `snarea_thresh` | `snarea_library.yml` | `snarea/library.py` |
| `snarea_curve` | `nhm_snarea_curve_params.csv` | `snarea_curve_0` … `snarea_curve_10` | `snarea_library.yml` | `snarea/library.py` |

### PRMSCanopy — 6 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `cov_type` | `nhm_lulc_nhm_v11_params.csv` | `cov_type` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `covden_sum` | `nhm_lulc_nhm_v11_params.csv` | `covden_sum` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `covden_win` | `nhm_lulc_nhm_v11_params.csv` | `covden_win` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `srain_intcp` | `nhm_lulc_nhm_v11_params.csv` | `srain_intcp` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `wrain_intcp` | `nhm_lulc_nhm_v11_params.csv` | `wrain_intcp` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |
| `snow_intcp` | `nhm_lulc_nhm_v11_params.csv` | `snow_intcp` | `zonal_params.yml:96` | `zonal_runners/lulc_prederived.py` |

### PRMSGroundwater — 1 parameter

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `gwflow_coef` | `nhm_ssflux_params.csv` | `gwflow_coef` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |

### PRMSEt — 2 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `hru_percent_imperv` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | `depstor_params.yml:164` | `depstor_builders/imperv.py` + `landmask.py` |
| `dprst_frac` | `nhm_dprst_frac_params.csv` | `dprst_frac` | `depstor_params.yml:177` | `depstor_builders/dprst.py` + `landmask.py` |

### PRMSSolarGeometry / PRMSAtmosphere — 2 parameters

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `hru_slope` | `nhm_slope_params.csv` | `hru_slope` — decimal fraction rise/run. `mean` in the same file is *provenance*, in **degrees** | `zonal_params.yml` (`slope`, `derived_columns:`) | `zonal_runners/zonal.py` + `zonal_runners/merge.py` |
| `hru_aspect` | `nhm_aspect_params.csv` | **DEFECTIVE** — see [#201](https://github.com/rmcd-mscb/gfv2-params/issues/201) | `zonal_params.yml:64` | `zonal_runners/zonal.py` |

### Not consumed by any pywatershed process — 1 parameter

| PRMS parameter | Emitted file | Column | Config entry | Builder |
| --- | --- | --- | --- | --- |
| `hru_elev` ⚠️ | `nhm_elevation_params.csv` | `mean` (metres) | `zonal_params.yml:43` | `zonal_runners/zonal.py` |

`hru_elev` appears in no pywatershed `Process.get_parameters()` list. It is used by PRMS's
temperature/precipitation distribution modules — which pywatershed handles by reading CBH
files — and by the `cov_type` reset rule at
[TM6B9:707](NHM_description_Regan_2018_TM6B9.md) (HRUs above 11,500 ft).

---

## By config entry

### `configs/zonal/zonal_params.yml`

| Entry | Merged file | PRMS parameters | Provenance columns |
| --- | --- | --- | --- |
| `elevation` `:43` | `nhm_elevation_params.csv` | `hru_elev` ⚠️ (from `mean`) | `count`, `std`, `min`, `25%`, `50%`, `75%`, `max`, `sum` |
| `slope` `:56` | `nhm_slope_params.csv` | `hru_slope` (emitted, via `derived_columns:`) | same 8 stats, plus `mean` itself (degrees) |
| `aspect` `:64` | `nhm_aspect_params.csv` | **none — DEFECTIVE** | all 9 columns |
| `soils` `:74` | `nhm_soils_params.csv` | `soil_type` ⚠️ (from `soils`) | — |
| `soil_moist_max` `:81` | `nhm_soil_moist_max_params.csv` | `soil_moist_max` | — |
| `lulc_nhm_v11` `:96` | `nhm_lulc_nhm_v11_params.csv` | `cov_type`, `covden_sum`, `covden_win`, `srain_intcp`, `wrain_intcp`, `snow_intcp`, `rad_trncf` ⚠️ | — |
| `lulc_nalcms` `:133` | `nhm_lulc_nalcms_params.csv` | `cov_type`, `covden_sum`, `covden_win`, `srain_intcp`, `wrain_intcp`, `snow_intcp` | `retention` — **unverified**, not `rad_trncf` |
| `lulc_nlcd` `:154` | *never built* — `input/lulc_veg/nlcd/` absent | as `lulc_nalcms` | `retention` |
| `lulc_foresce` `:179` | *never built* — `input/lulc_veg/foresce/` absent | as `lulc_nalcms` | `retention` |
| `ssflux` `:208` | `nhm_ssflux_params.csv` | `soil2gw_max`, `ssr2gw_rate`, `fastcoef_lin`, `slowcoef_lin`, `gwflow_coef`, `dprst_seep_rate_open`, `dprst_flow_coef` | `k_perm_wtd`, `mean_slope_fraction`, `hru_area` |

### `configs/depstor/depstor_params.yml`

| Entry | Merged file | PRMS parameter | Provenance |
| --- | --- | --- | --- |
| `dprst_depth_avg` `:107` (`means:`) | `nhm_dprst_depth_avg_params.csv` | `dprst_depth_avg` | `dprst_depth_provenance` |
| `sro_to_dprst_perv` `:134` (`ratios:`) | `nhm_sro_to_dprst_perv_params.csv` | `sro_to_dprst_perv` | — |
| `sro_to_dprst_imperv` `:141` | `nhm_sro_to_dprst_imperv_params.csv` | `sro_to_dprst_imperv` | — |
| `carea_max` `:148` | `nhm_carea_max_params.csv` | `carea_max` | — |
| `smidx_coef` `:155` | `nhm_smidx_coef_params.csv` | `smidx_coef` | — |
| `hru_percent_imperv` `:164` | `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | — |
| `dprst_frac` `:177` | `nhm_dprst_frac_params.csv` | `dprst_frac` | — |

The 10 `fractions:` entries are **intermediates**, not parameters. They write
partial-pixel-weighted count CSVs to `merged/_intermediates/` and feed the ratios above.

`constants:` holds params a depstor **builder** writes directly, with no zonal pass —
their source lives in `{fabric}/depstor_rasters/`, and
`scripts/derive_depstor_params.py --mode copy_constants` copies each into `merged/` under
its canonical name.

| Entry | Merged file | PRMS parameter | Source |
| --- | --- | --- | --- |
| `op_flow_thres` (`constants:`) | `nhm_op_flow_thres_params.csv` | `op_flow_thres` | `depstor_rasters/op_flow_thres_params.csv` (`depstor_rasters.yml:77`) |

### `configs/snarea/snarea_library.yml`

| Merged file | PRMS parameters | Provenance |
| --- | --- | --- |
| `nhm_snarea_curve_params.csv` | `hru_deplcrv`, `snarea_thresh`, `snarea_curve` (11 columns) | `cv_assign`, `cv_subgrid`, `cv_empirical`, `cv_source`, `sdc_status`, `sca_class`, `similarity`, `n_seasons`, `n_peak_years`, `peak_swe_mm` |

The `cv_*` columns are **derivable for only ~42% of HRUs by design** — `cv_subgrid` rescues
the rest. A NaN there is a result, not a gap, which is why they are excluded from
`fill_columns`.

---

## By builder

| Builder module | PRMS parameters produced |
| --- | --- |
| `zonal_runners/zonal.py` | `hru_elev` ⚠️, `hru_slope` (via `merge.py`'s `derived_columns:`), `hru_aspect` (DEFECTIVE) |
| `zonal_runners/soils.py` | `soil_type` ⚠️, `soil_moist_max` |
| `zonal_runners/lulc_prederived.py` | `cov_type`, `covden_sum`, `covden_win`, `srain_intcp`, `wrain_intcp`, `snow_intcp`, `rad_trncf` ⚠️ |
| `zonal_runners/lulc.py` | `cov_type`, `covden_sum`, `covden_win`, `srain_intcp`, `wrain_intcp`, `snow_intcp` (+ unverified `retention`) |
| `zonal_runners/ssflux.py` | `soil2gw_max`, `ssr2gw_rate`, `fastcoef_lin`, `slowcoef_lin`, `gwflow_coef`, `dprst_seep_rate_open`, `dprst_flow_coef` |
| `depstor_builders/carea_map.py` | `carea_max`, `smidx_coef` |
| `depstor_builders/imperv.py` + `landmask.py` | `hru_percent_imperv` |
| `depstor_builders/dprst.py` + `landmask.py` | `dprst_frac` |
| `depstor_builders/same_hru_drains.py` (+ `perv.py` / `imperv.py`) | `sro_to_dprst_perv`, `sro_to_dprst_imperv` |
| `depstor_builders/dprst_depth.py` + `dprst_depth/aggregate.py` | `dprst_depth_avg`, `op_flow_thres` |
| `snarea/library.py` (← `snarea/build.py` ← `aggregate/`) | `hru_deplcrv`, `snarea_thresh`, `snarea_curve` |

The depstor ratios are finalised by `gfv2_params/depstor_ratios.py` (a top-level module, not
part of `depstor_builders/`), driven by `scripts/derive_depstor_params.py --mode ratios`.

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
