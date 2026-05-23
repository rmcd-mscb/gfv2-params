# Archived notebooks

One-shot exploration, QA/QC, and pipeline-validation notebooks that earned
their keep during the work that produced them but are no longer part of the
ongoing workflow. Kept on disk for provenance, not for routine use.

**Audience-facing notebooks are at the top level of `notebooks/`:**
- `notebooks/fabric_results/{01,02,03,04}_*.ipynb` — the curated fabric
  results viewers (inputs, depstor rasters, params, depstor overlay).
- `notebooks/carea_threshold_sweep.py` — the marimo sweep tool for tuning
  `carea_max`/`smidx_coef` thresholds (PR #96).
- `notebooks/merge_vpu_targets.py` — the marimo tool for merging per-VPU
  fabric gpkgs (still part of the prepare-fabric workflow for VPU-based
  fabrics like `gfv2`).

Everything in this directory is **historical**. For full context on any
file, see `git log notebooks/_archive/<name>` (the renames preserve the
original `notebooks/` history).

## Inventory

### Visual QA / pipeline inspection (`check_*`, `qaqc_*`)

Used during pipeline development to eyeball raster outputs, validate per-VPU
merges, and spot-check parameter results before committing changes. Most
were superseded by [`notebooks/fabric_results/`](../fabric_results/) once
the audience-facing notebook set crystallised.

| File | Purpose |
|---|---|
| `check_and_plot_merged_rasters.py` | Visualise per-VPU merged GeoTIFFs after `merge_rpu_by_vpu` |
| `check_border_dem.py` | Visual inspection of Copernicus border-DEM fill (PR #36) |
| `check_depstor_vpu01.ipynb` | VPU01 depression-storage validation (issue #38) |
| `check_derived_rasters.{py,ipynb}` | Visual QA of `soil_moist_max`, `radtrn`, CNPY/keep resamples |
| `check_lulc_veg_inputs.ipynb` | LULC / vegetation source-raster spot-check |
| `check_params.ipynb` | Per-HRU parameter result visualisation (superseded by `fabric_results/03_param_results.ipynb`) |
| `check_twi_merge.{py,ipynb}` | TWI per-VPU merge QA (open-source pipeline, around PRs #95/#99) |
| `check_vrts.{py,ipynb}` | VRT quick-look: elevation / slope / aspect / fdr / twi (the windowed-read pattern was lifted into `src/gfv2_params/viz.py:clip_overview`) |
| `qaqc_depstor_vpu01.ipynb` | VPU01 depstor QA/QC — PR #62 closeout + PR #61 anticipations |

### Comparison studies (`diff_*`)

| File | Purpose |
|---|---|
| `diff_twi_hydrodem_vs_merged.{py,ipynb}` | Side-by-side characterisation of open-source `twi_hydrodem.vrt` vs ArcPy `twi.vrt` (issue #52 / PR #95) |

### One-shot exploration (`experiment_*`, `climate_forcing_comp.py`)

Early-phase exploration that informed the pipeline design. Kept for the
provenance trail; the working logic landed in the production builders.

| File | Purpose |
|---|---|
| `climate_forcing_comp.py` | Early comparison of climate-forcing variants (March 2026, abock-era) |
| `experiment_merge_rpu_by_vpu.py` | Prototype for what became `src/gfv2_params/shared_rasters/merge_rpu_by_vpu.py` |
| `experiment_process_NHD_by_vpu.py` | Prototype for NHDPlus per-VPU processing |
| `experiment_soilmoistmax.py` | `soil_moist_max` algorithm exploration (March 2026, abock-era) |
| `experiment_soils.py` | Soils zonal-stats algorithm exploration (March 2026, abock-era) |
| `experiment_ssflux.py` | ssflux algorithm exploration; landed in `src/gfv2_params/zonal_runners/ssflux.py` |

## Running an archived notebook

These were last-known-good when they were committed but may not run cleanly
against current code (function signatures, config paths, and data layouts
have all evolved). If you need to revive one, expect to update its imports
and config-resolution paths first.

```bash
pixi run -e notebooks marimo run notebooks/_archive/<name>.py
pixi run -e notebooks jupyter nbconvert --to notebook --execute notebooks/_archive/<name>.ipynb
```

## Why these aren't deleted

Two reasons:
1. **Provenance.** Several encode the original algorithm exploration that
   informed the production builders. They're easier to read in their
   marimo/Jupyter form than to reconstruct from `git log`.
2. **Reproducibility of past results.** If a question comes up about a
   historical decision (e.g. "why did we pick the 50% imperv threshold?"),
   the validation notebook that justified it lives on disk.

Periodically prune: if a notebook hasn't been opened in a year and its
informational content has been distilled into the production code or a
proper doc, it can go.
