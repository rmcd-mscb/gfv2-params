# Design: `carea_max` / `smidx_coef` threshold-iteration sweep tool

**Date:** 2026-05-22
**Status:** design — pending implementation plan
**Branch:** `feat/carea-threshold-sweep` (stacked on `feat/twi-percentile-carea-smidx` / PR #95)
**Related:** #55 Stage 1 (PR #95) — this is the calibration tool for choosing the percentile/threshold that #95's production path consumes.

---

## 1. Problem & motivation

The legacy `carea_max`/`smidx_coef` thresholds (TWI `8.0`/`15.6`) were **eyeballed** by the
original developer, and PR #95 made the cutoff a data-derived percentile (default:
the inversion of 8.0/15.6 through VPU 01's valid-land TWI CDF — empirically P87.9 /
P99.7). Choosing a *final* value is inherently iterative: try a threshold, look at
the resulting per-HRU parameter distribution, adjust, repeat.

Doing that through the production pipeline is slow — each candidate re-runs
`twi_reference → carea_map → zonal → ratios` (several cluster steps). We want a
**fast in-memory loop**: change a threshold and see the resulting parameters in
milliseconds, with no cluster reruns, then bake the chosen value into production.

### Key insight (what makes it cheap)

`carea_max` and `smidx_coef` are the **same per-HRU function** evaluated at two
thresholds:

```
f_hru(t) = ( n_perv_onstream + #(pervious, non-onstream cells with TWI > t) ) / n_perv     (capped at 1)
carea_max = f_hru(t_carea),   smidx_coef = f_hru(t_smidx)
```

So if we store, per HRU, `n_perv`, `n_perv_onstream`, and the **TWI distribution of
its pervious non-onstream cells**, we can evaluate `f_hru(t)` for any `t` instantly,
vectorised across all HRUs. Extract that once; sweep thresholds for free.

---

## 2. Scope

- **In:** a per-fabric extraction artifact, pure sweep math (tested), and a notebook
  with four inspection views + a sweep curve. **Oregon first** (`--fabric` generalises).
- **Out:** automated calibration/optimisation (the human eyeballs); changing the
  production parameter contract (PR #95 already owns `threshold_mode`); multi-VPU
  per-VPU sweeping (artifact carries `vpu` per HRU for later, but the oregon tool
  treats the fabric as single-VPU = VPU 17).

---

## 3. Architecture — three units

### 3.1 Extraction — `src/gfv2_params/threshold_sweep.py` (lib) + `scripts/build_carea_twi_artifact.py` (thin CLI)

A single pass over a fabric's depstor stack producing a compact per-HRU artifact.
Run once per fabric (heavy: full template grid). The CLI is a calibration utility in
the mould of `scripts/clip_shared_to_fabric.py` — **not** a production pipeline step,
so it is intentionally a standalone script rather than an orchestrator builder.

**Inputs** (all from the active fabric profile in `base_config.yml`, read via
`require_config_key`): `twi_raster`, `template_raster`, `hru_gpkg`/`hru_layer`,
`id_feature`; plus the fabric's depstor rasters `perv_binary.tif`,
`onstream_binary.tif`, `land_mask.tif` under `{data_root}/{fabric}/depstor_rasters/`.

**Process** — mirror `compute_carea_map_binary` (`src/gfv2_params/depstor.py`) exactly,
so a swept value reproduces a real production run:
- Rasterise HRU `id_feature` onto the template grid (an `hru_id` raster, same
  technique as the `vpu_id` builder).
- Strip-read `land_mask`, `perv`, `onstream`, and TWI (via `WarpedVRT` onto the
  template, nearest, whole-cell-aligned — identical to `carea_map`).
- For cells where `land & perv`: accumulate per HRU
  - `n_perv` += count
  - `n_perv_onstream` += count where also `onstream`
  - for `perv & ~onstream & twi_valid`: increment the per-HRU TWI histogram bin.
  (Pervious non-onstream cells with *nodata* TWI count toward `n_perv` only — never
  rescued by any threshold — matching production.)

**Reference distribution:** reuse `_sample_land_masked_twi`
(`shared_rasters/twi_reference.py`) on the fabric's VPU TWI tile + per-VPU land mask
to get the valid-land TWI sample, and store a fine **percentile grid** (e.g.
percentiles `0, 0.1, …, 100` → TWI values) for absolute↔percentile conversion.
This matches the population PR #95's `twi_reference` percentiles are defined over.

**Output:** one `.npz` at `{data_root}/{fabric}/params/carea_twi_artifact.npz`
containing: `id_feature` (ids), `vpu` (per HRU), `n_perv`, `n_perv_onstream`,
`hist` (n_hru × n_bins int), `bin_edges`, `ref_pctl` (grid percentiles),
`ref_value` (grid TWI values), and metadata (`fabric`, `twi_source`, bin range).

**Binning:** fixed bins ~0.05 TWI wide over a configurable range (default 0..30,
covering observed ~3–25). Resolution 0.05 is far finer than anyone eyeballs.

### 3.2 Sweep math — pure functions in `threshold_sweep.py` (CI-tested)

- `evaluate_threshold(artifact, t) -> np.ndarray` (per-HRU param in [0,1]):
  `clip((n_perv_onstream + hist[:, bin_centers > t].sum(axis=1)) / n_perv, 0, 1)`,
  with `n_perv == 0 → 0`. One call yields `carea_max` at `t_carea`; call again at
  `t_smidx` for `smidx_coef`.
- `value_to_percentile(artifact, t) -> float` and `percentile_to_value(artifact, p)
  -> float`: linear interpolation on the reference grid.
- `sweep(artifact, t_grid) -> pandas.DataFrame`: per-threshold summary
  (`mean`, `median`, `frac_zero`, `frac_one`) for the sensitivity curve.

### 3.3 Notebook — `notebooks/carea_threshold_sweep.ipynb`

Thin UI over the library. Loads the artifact for a fabric; a candidate is entered as
an absolute TWI value **or** a percentile, with a live two-way readout. Renders:
1. **Distribution:** histogram of per-HRU param + summary stats.
2. **Spatial map:** param joined to the fabric geometry (`hru_gpkg`).
3. **Legacy 8.0/15.6 diff:** present-but-inactive for oregon; activates when a legacy
   parameter CSV is supplied (VPU 01 / gfv2), showing per-HRU scatter + Δ stats.
4. **Gauge-tuned diff:** optional NHM CSV (`id_feature → value`) the user supplies.
Plus a **sweep curve** (`sweep()` output) of a summary metric vs threshold.

### 3.4 Persistence — handoff to production

The notebook prints the exact config snippet for the chosen value, for one of the two
paths PR #95 already supports:
- percentile: `percentiles: {carea_max: P, smidx: P}` in the `twi_reference` step, or
- eyeball-absolute: `threshold_mode: absolute` + `thresholds: {carea_max: t, smidx: t}`
  in the `carea_map` step.
No new production wiring — the chosen value flows through the existing #95 path.

---

## 4. Testing

`tests/test_threshold_sweep.py` (pure, CI):
- `evaluate_threshold` matches a brute-force count on a synthetic
  `n_perv`/`n_perv_onstream`/`hist` set at several thresholds (incl. below-all,
  above-all, mid-bin), and respects the clamp and the `n_perv==0 → 0` rule.
- `percentile_to_value`/`value_to_percentile` round-trip on a known grid.
- `sweep` monotonicity: mean param is non-increasing as `t` increases.
- artifact save/load round-trip (`.npz`).

**Faithfulness** (operational, not CI): on oregon, after PR #95's D1 builds
`carea_map` at the default percentile threshold `t*`, confirm
`evaluate_threshold(artifact, t*)` matches the production `carea_max` CSV per HRU
**within histogram-bin resolution**. Extraction mirrors `compute_carea_map_binary`
exactly, so the only expected difference is the ≤1-bin quantization of the threshold
(cells in the bin straddling `t*`); agreement of `mean`/`frac_one` to ~1e-2 confirms
fidelity. (To make a specific `t*` exact, snap it to a bin edge.)

---

## 5. File structure

**Created:**
- `src/gfv2_params/threshold_sweep.py` — extraction (`build_artifact`), pure sweep
  math (`evaluate_threshold`, `value_to_percentile`, `percentile_to_value`, `sweep`),
  artifact `save`/`load`.
- `scripts/build_carea_twi_artifact.py` — thin `--fabric` CLI around `build_artifact`
  (sbatch-able for large fabrics).
- `notebooks/carea_threshold_sweep.ipynb` — interactive UI.
- `tests/test_threshold_sweep.py` — unit tests.

**Reused (no change):** `depstor.compute_carea_map_binary` semantics,
`twi_reference._sample_land_masked_twi`, the fabric profile + `require_config_key`,
the depstor raster outputs.

---

## 6. Open questions / follow-ups

- **Multi-VPU sweeping (gfv2):** the artifact stores `vpu` per HRU; a future revision
  evaluates a per-VPU threshold map (one `t` per VPU) rather than a single scalar.
  Oregon is single-VPU, so deferred.
- **Notebook scope trim:** start with all four views; the group prunes to what is most
  useful (per the user).
