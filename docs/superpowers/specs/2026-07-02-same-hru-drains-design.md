# Same-HRU restriction for `sro_to_dprst_*` — design

Date: 2026-07-02
Status: approved (brainstorming)
Issue: #160
Branch: feat/same-hru-drains

## Problem

The open-source `drains_to_dprst` method credits a pervious/impervious cell to
its **own** HRU's `sro_to_dprst_perv/imperv` if it drains to **any** depression,
including one in a *different* (downstream) HRU. The legacy ArcPy method
restricted this to same-HRU depressions via `Con(rSro_to_dprst == hru,
rSro_to_dprst)` (`docs/0b_TB_depr_stor.py:214`). We dropped that restriction.

Evidence (VPU 15, rebuilt rasters, 2026-07-02; labeled trace matched production
`drains_to_dprst.tif` at 100.00%): 19.4% of `drains_perv` cells drain to a
different HRU's depression; VPU-wide area-weighted `sro_to_dprst_perv` is 1.24×
legacy; the effect is concentrated (median per-HRU diff 0) in a tail of ~70–120
HRUs, some saturating to 1.0 with `dprst_frac ≈ 0` (a pairing PRMS can't honor);
945 HRUs flip 0 → >0. Endorheic/playa terrain drives it.

## Goal

Restore the legacy same-HRU restriction on `sro_to_dprst_perv/imperv` only,
**additively** — leaving the merged `routing`/`intersect` binary
`drains_to_dprst` path (#159) untouched — and preserve every current raster
product and the entire parameter-aggregation config.

## Approach (selected: additive)

Rejected alternative — **integrated**: fold HRU labeling into `routing`, derive
the binary `drains_to_dprst.tif` as `labeled > 0`, compare in `intersect`. No
duplicate trace, but rewrites the just-merged/reviewed routing+intersect code.
Chosen additive path costs one extra per-VPU D8 pass but touches no working
step (matches the "finish the product, don't refactor working steps"
guidance).

### New / changed components

1. **`hru_id` raster builder** (new step). Rasterizes the fabric's `nat_hru_id`
   (the profile `id_feature`) onto the depstor **template grid** → `hru_id.tif`
   (int32, `0` = no HRU). Reads `hru_gpkg` / `hru_layer` / `id_feature` from the
   active fabric profile via `require_config_key` (never hardcoded). This is the
   open-source equivalent of the legacy `nhrug`. Depends only on the template +
   fabric; placed early (after `landmask`).

2. **Barrier support in `drains_to_dprst_labeled_kernel`** (`d8_routing.py`).
   Add the `barrier` argument (parity with `drains_to_dprst_kernel`; the
   follow-up deferred from #159). Barrier cells (on-stream waterbodies) seed
   non-draining and terminate any path that reaches them as label `0`.

3. **`routing_hru` builder** (new step, additive). Mirrors `routing`'s
   FDR-alignment + per-VPU tiling, reusing the existing alignment /
   `mask_fdr_to_vpu` / `vpu_pour_points` helpers. The FDR-alignment helper is
   promoted to a shared location (`depstor.py`) so neither routing builder
   imports the other's privates — a behavior-preserving move; `routing`'s binary
   path is unchanged and still covered by its tests. Per VPU: `label = hru_id
   where dprst == 1` (VPU-scoped), `barrier = onstream` (VPU-scoped), run the
   barrier-aware **labeled** kernel → `drains_to_dprst_hru.tif` (int32; each
   draining cell carries the `nat_hru_id` of the depression it reaches, `0`
   elsewhere).

4. **`same_hru_drains` builder** (new step; **replaces the two `intersect`
   steps** for these outputs). Streaming strips (STRIP_ROWS like `intersect`):
   `drains_perv_binary = (drains_to_dprst_hru == hru_id) & (perv == 1)`;
   `drains_imperv_binary = (drains_to_dprst_hru == hru_id) & (imperv == 1)`;
   `255` background. **Writes the same filenames / output-keys as today**
   (`drains_perv_binary.tif`, `drains_imperv_binary.tif`). Assert all inputs are
   template-aligned (as `intersect`/`carea_map` do).

### Data flow (aggregation UNCHANGED)

```
dprst, onstream, hru_id, FDR ─► routing_hru ─► drains_to_dprst_hru.tif
hru_id, perv/imperv ─────────► same_hru_drains ─► drains_perv/imperv_binary.tif
                                          │  gdptools zonal count (UNCHANGED)
                                 drains_perv_frac, drains_imperv_frac
                                          │  ratio (UNCHANGED)
                                 sro_to_dprst_perv/imperv  ◄── corrected
```

`drains_to_dprst.tif` and `drains_to_dprst_frac` stay HRU-agnostic and unchanged.
Because `same_hru_drains` writes the same filenames the `intersect` step did,
**`depstor_params.yml` needs zero changes** — the fraction zonal count and the
ratio division consume a corrected `drains_perv/imperv_binary.tif` transparently.

### DAG / config (`depstor_rasters.yml`)

Insert `hru_id` (after `landmask`), `routing_hru` (after `routing`), and
`same_hru_drains` **in place of** the `drains_perv` / `drains_imperv`
`intersect` steps. Existing `routing` → `drains_to_dprst.tif` untouched.
Register the new builders in the `BUILDERS` dispatch table and `STEP_ORDER`.

### Documentation (first-class requirement)

`docs/ARCHITECTURE.md` and `CLAUDE.md` MUST state plainly:

- The same-HRU restriction is a **raster-space intersection** — the labeled
  drains raster compared cell-by-cell against a **hard-rasterized `hru_id`
  grid** — applied **before** aggregation, **not** a gdptools operation.
- **Why:** the test is a per-cell comparison (a cell's reached-HRU vs. its own
  HRU) that gdptools' partial-pixel area-weighting cannot express; it
  reproduces the legacy `nhrug` / `Con(rSro == hru)` behavior
  (`0b_TB_depr_stor.py:214`).
- The per-HRU **count still uses gdptools** (the fraction zonal step is
  unchanged); only the same-HRU *selection* is raster-based.
- **Tradeoff:** a 1-pixel-wide HRU-boundary approximation (a cell may be
  area-credited to HRU A by gdptools weights but labeled HRU B by the hard
  rasterization) — immaterial against the basin-scale cross-HRU signal, and
  consistent with legacy.

### Testing

- `tests/test_hru_id.py` — rasterizes a tiny fabric, asserts per-cell ids +
  template alignment.
- `tests/test_drains_kernel.py` — labeled-kernel barrier cases (blocked upslope,
  first-waterbody-wins, no-barrier equivalence).
- `tests/test_routing_tiling.py` (or a new `test_routing_hru.py`) — per-VPU
  labeled trace attributes cells to the reached depression's HRU; on-stream
  barrier respected.
- `tests/test_same_hru_drains.py` — same-HRU equality × perv/imperv; a cell
  draining cross-HRU is excluded, same-HRU included.

### CONUS-scale notes

- `hru_id.tif` is a full int32 CONUS raster (~68 GB uncompressed; LZW-tiled on
  disk, windowed in memory — never held whole). Rasterize streams to disk.
- `routing_hru` is a second per-VPU trace with int32 label/output (heavier than
  the binary kernel but per-VPU windowed); run at similar mem to `routing`.
- All new builders follow the existing streaming/tiling discipline; no new
  full-grid in-memory array.

## Out of scope

- Fabric-level depression-aware HRU delineation (root cause; a next-fabric
  input, not this pipeline).
- Runtime re-aggregation of `sro_to_dprst_*` on the fabric (HPC step after
  merge; rasters are rebuilt but params not yet re-aggregated).
- Exact (weighted) same-HRU test inside the zonal stat — deferred; the
  raster-space approximation matches legacy and is sufficient.
