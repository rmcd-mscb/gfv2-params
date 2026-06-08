# LULC follow-through: rad_trncf, faithful nhm_v11, corrected NALCMS

> Continuation of the 2026-04-02 multi-source LULC work (#24/#25, plan
> `2026-04-02-lulc-parameterization.md`). Driven by the crosswalk validation
> against the authoritative source (see below). Tracker: issue #26.

## Source of truth (validation result)

The authoritative NHM v1.1 LULC crosswalk is the **Viger & Leavesley (2007)**
remap table reproduced verbatim in the **ScienceBase P971JAGF metadata**
("Remapped Data" process step; shipped as `CrossWalk.xlsx`). It maps each
**NALCMS** class (1-19) -> PRMS `lulc`(cov_type), `Keep`, `Loss`,
`Rooting_Depth`, `Snow`, `Wrain`, `Srain`. The legacy ArcPy `3_coverDen.py`
zonal-means the rasters this table built.

Key facts established:
- **Units**: Snow/SRain/WRain are *hundredths of inches* (Snow 0-10, S/WRain
  0-5); legacy divides by 100 -> inches. Crosswalk `snow_intcp=0.10` == Snow 10.
- **NHM canopy = MODIS VCF** (MOD44B, 250 m), not NLCD TCC. The generic
  `input/lulc_veg/CNPY.tif` used by the CONUS NALCMS run is therefore
  NHM-consistent (confirm `CNPY.tif` is that product).
- **rad_trncf** = `exp(-2.7557 * density/100) * 0.9917`, density = zonal-mean of
  `radtrn = Con(LULC>=3, cnpy) * keep/100`. The new pipeline builds the `radtrn`
  raster but never zonal-means it or applies Beer's law -> **rad_trncf is absent
  from every LULC output**; it emits a `retention` surrogate instead.

### Per-crosswalk verdict
- `nhm_v11_nhm.csv`: keep/cov_type **exact**; 5-class interception is a lossy
  collapse. Quantified over CONUS (decimated NALCMS-2020 sample, ~2.9 M px):
  area-weighted mean |delta| ~0.005 in, but **24.6% of CONUS land** has an
  interception param shifted >0.005 in, with local errors up to 0.05 in
  (mixed-forest snow, wetland snow, grassland srain/wrain). -> fix by staging
  the real rasters.
- `nalcms_nhm.csv`: **drifted** from authoritative (needleleaf snow 0.05->0.10,
  mixed-forest snow ->0.07, forest keep ->0.60, class-2 taiga, etc.). ->
  regenerate from the table.
- `nlcd_nhm.csv`: no USGS NLCD->PRMS authority exists (NHM v1.1 used NALCMS).
  -> derive by analogy to NALCMS-equivalent classes.

## Scope decisions
- Sources: **nhm_v11 + NLCD + corrected NALCMS**. **FORE-SCE dropped.**
- nhm_v11: **stage real Snow/SRain/WRain/loss rasters + direct zonal-mean**
  (reproduce NHM v1.1 exactly; the crosswalk path is unnecessary for nhm_v11).
- NALCMS: regenerate crosswalk from the authoritative table, then re-run CONUS.

## Workstreams

### A. rad_trncf (start here; mostly independent)
- [ ] Pure helper `compute_rad_trncf(density_mean_series)` in `lulc.py`:
      `exp(-2.7557 * density/100) * 0.9917`; density 0 -> ~0.9917, density 100
      -> ~0.063. Unit tests (`tests/test_lulc.py` or `test_rad_trncf.py`).
- [ ] Wire into `run_lulc_batch`: when a `radtrn_raster` is configured, zonal-mean
      it (continuous) -> density -> `compute_rad_trncf` -> `rad_trncf` column.
- [ ] Config: add `radtrn_raster` to the nhm_v11 zonal entry; DAG/merge picks up
      the new column. Docs: ARCHITECTURE + RUNME/HPC_REFERENCE param table.

### B. Faithful nhm_v11 (pre-derived raster source mode)
- [x] Stage `Snow/SRain/WRain/loss` from P971JAGF (keep, CNPY, LULC already on
      disk) into `input/lulc_veg/nhm_v11/` — all co-registered to the 30 m LULC
      grid; Snow/SRain/WRain Byte (nodata 15), loss/keep Int8 (0-100).
- [x] New `lulc_prederived` runner (`zonal_runners/lulc_prederived.py`): each
      param = direct zonal stat (snow/wrain/srain = raster/100; covden_sum from
      CNPY/100 zeroed on bare; covden_win = covden_sum*(1-loss/100); cov_type
      from 5-class decision tree; rad_trncf from radtrn). Ports `3_coverDen.py`;
      outputs rad_trncf, not retention. Registered in BATCH_RUNNERS.
- [x] `covden_win_from_loss` helper + tests (the keep-vs-loss correction);
      dispatch-registration test; config switched to `script: lulc_prederived`
      with per-param raster keys; docs (ADDING_A_PARAMETER).
- [ ] Re-run CONUS nhm_v11 (supersedes the on-disk CSV, which has the keep-based
      covden_win error). radtrn_nhm_v11.tif prerequisite confirmed present.

### C. Corrected NALCMS
- [ ] Regenerate `crosswalks/nalcms_nhm.csv` from the authoritative table (exact
      19-class cov_type/keep/snow/wrain/srain; covden_win = keep/100).
- [ ] Synthesize a per-pixel keep raster from the crosswalk keep column applied
      to the NALCMS raster so the existing `compute_radtrn` + A's helper produce
      rad_trncf without a native keep raster. (Same approach for NLCD.)
- [ ] Update crosswalk test. Re-run CONUS NALCMS (supersedes the drifted CSV).

### D. NLCD (last)
- [ ] Build `nlcd_nhm.csv` by analogy to NALCMS-equivalent classes.
- [ ] Stage NLCD 2021 land cover + canopy (MODIS VCF vs NLCD TCC — decide).
- [ ] Run CONUS.

## Notes
- Single rad_trncf code path for all sources via the synthesized-keep approach
  (C) + the radtrn builder that already exists.
- Per repo convention each builder change ships with its test + config + a docs
  audit on the same branch.
