# Design: per-cell impervious carve-out in dprst

**Date:** 2026-06-25
**Status:** Approved (design), pending implementation plan
**Component:** `src/gfv2_params/depstor_builders/dprst.py` (depression-storage classification)

## Problem

The `dprst` builder classifies waterbody clumps as depression storage. A clump is
currently **excluded** from dprst if its region touches **either** the
NHD-connected (WBAREACOMI) mask **or** the impervious mask, via
`regions_touching_mask` — a region is flagged if it shares **≥1 cell** with the
mask ([depstor.py](../../../src/gfv2_params/depstor.py) `regions_touching_mask`;
[dprst.py:46-48](../../../src/gfv2_params/depstor_builders/dprst.py)).

For the impervious mask this any-touch rule is far too aggressive: a **single**
≥50%-impervious 30 m pixel (a dirt road across a dry lakebed, an outbuilding, or
NLCD misclassifying a bright playa/water surface) removes an **entire** multi-km²
natural waterbody from depression storage.

### Evidence (verified)

Three SE-Oregon (Great Basin) features, sampled from the on-disk
`gfv2/depstor_rasters` outputs:

| COMID | FTYPE | area | actual label | cause |
|---|---|---|---|---|
| 24079189 (Catlow) | Playa | 1.6 km² | **dprst** ✓ | touches no imperv / connected |
| 24068219 | Playa | 2.54 km² | on-stream | **3 imperv cells of 2813** |
| 24152941 | SwampMarsh | 3.9 km² | on-stream | **1 imperv cell of 4334** |

### Blast radius (CONUS scan, `scratchpad/blast_radius.py`, streaming row-strips over the int32 regions grid)

- 418,915 waterbody regions / 255,392 km²; current dprst = 268,432 regions / 60,141 km².
- **Excluded by imperv *only* (touch imperv, not connected): 14,898 regions / 16,800 km² ≈ +28% potential dprst area.**
- Of that, ≈16,600 km² is false exclusion (regions ≤5% impervious; 9,982 km² at
  ≤0.1%; 3,538 regions / 3,289 km² flipped by a **single** pixel; 89% of flipped
  area sits in ≥1 km² waterbodies).
- Only 3,405 regions / 222 km² are >25% impervious (legitimately impervious-laced).

## Intent and chosen approach

The impervious exclusion exists to **avoid double-counting** the same cells as
both impervious and depression storage. The pipeline treats `imperv` / `dprst` /
`perv` as a **disjoint cell-level partition**
(`perv = land ∧ ¬imperv ∧ ¬dprst`, [perv.py:19-32](../../../src/gfv2_params/depstor_builders/perv.py)).

A region-level **fraction threshold** was considered (exclude a clump only if it
is >X% impervious) but rejected: for sub-threshold regions it either keeps the
impervious cells inside dprst — reintroducing the very double-count the rule
exists to prevent — or it still needs per-cell subtraction, making the threshold
redundant.

**Chosen: Approach A — per-cell carve-out.** Impervious stops being a
region-level exclusion and becomes a cell-level removal. A waterbody region is
depression storage **unless it is on-stream (connectivity)**; impervious cells
inside it are simply excised. This satisfies the double-counting intent exactly,
keeps the imperv/dprst/perv partition disjoint, recovers ~16,800 km² of false
exclusions, and needs no calibrated threshold.

This is a restoration, not a new behavior: the ArcPy reference already carved
impervious per-cell — `getDprst` defines depressions as "areas outside of
impervious zones" and `getImpervBin` thresholds at `VALUE > 50`
([docs/0b_TB_depr_stor.py](../../0b_TB_depr_stor.py)). The region-level any-touch
exclusion was a regression introduced in the open-source port.

### Impervious threshold (unchanged, kept at 50%)

The `imperv` step thresholds NLCD fractional-impervious at **≥50%** to mark a
cell impervious ([imperv.py](../../../src/gfv2_params/depstor_builders/imperv.py),
[depstor_rasters.yml](../../../configs/depstor/depstor_rasters.yml) `threshold: 50`),
matching the ArcPy `VALUE > 50`. This threshold is a **land-classification**
lever (which NLCD cells count as impervious), not a dprst-exclusion control.
Under the broken region-level any-touch rule the high threshold *incidentally*
limited the blast radius (fewer impervious cells → fewer whole waterbodies
nuked); the per-cell carve **decouples** the two, so the threshold reverts to its
proper narrow role: it determines which cells are carved from `dprst` and counted
in `hru_percent_imperv`, scaling dprst area smoothly and proportionally rather
than all-or-nothing. Because `imperv_binary` is binary at 50%, there is no
fractional double-count (a 40% cell is fully dprst/pervious; a 60% cell is fully
impervious). **The value stays 50%** — changing it shifts `hru_percent_imperv`
CONUS-wide and is a separate concern (see Deferred).

## Detailed change — `dprst.py` `build()`

```python
# region-level exclusion is connectivity ONLY (imperv removed):
onstream_regions = regions_touching_mask(regions, connected_binary)
excluded = onstream_regions

all_ids = set(int(v) for v in np.unique(regions) if v != 0)
kept_ids = all_ids - excluded
dprst_binary = regions_to_binary(regions, kept_ids)
dprst_binary[imperv_binary == 1] = 255   # NEW: carve impervious cells out of dprst
dprst_binary[~land_valid] = 255

# onstream must not absorb the carved impervious cells:
onstream = np.where(
    (wbody_binary == 1) & (dprst_binary != 1) & (imperv_binary != 1),
    np.uint8(1), np.uint8(255))
onstream[~land_valid] = 255
```

- `regions_touching_mask(regions, imperv_binary)` is dropped from `excluded`. It
  may be retained purely as an informational log count (regions touching imperv,
  cells carved). Update the existing log lines accordingly.
- The `imperv` input is already read in `build()`; no new inputs.

### Resulting cell partition

Every land cell is at most one of `{imperv, dprst, onstream}`, with
`perv = ¬imperv ∧ ¬dprst` (so `onstream ⊆ perv`, which is existing intended
behavior — carea_map wants on-stream cells pervious). No cell is both `imperv`
and `dprst`.

## Downstream effects & rebuild cascade

`dprst` membership changes, and `perv` / `onstream` sit directly downstream, so
the cascade is fuller than the connectivity-only change:

- **perv** ← dprst: recovered cells move perv→dprst; `perv` shrinks by exactly those cells. Correct.
- **carea_map** ← onstream + perv: recovered cells leave carea_map (no longer
  pervious/on-stream). `compute_carea_map_binary` gates on `is_perv`
  (`keep = land_valid & is_perv & (above_thresh | is_onstream)`,
  [depstor.py:168](../../../src/gfv2_params/depstor.py)), so impervious cells
  never entered carea_map — no imperv leakage.
- **routing → drains_to_dprst → drains_perv / drains_imperv** ← dprst: rebuild.
- **Params re-derived:** `dprst_frac` (+~16,800 km², minus literal imperv cells),
  `sro_to_dprst_perv` / `sro_to_dprst_imperv` ratios, `carea_max` / `smidx_coef`.

Rebuild order: `dprst → {perv, carea_map, routing} → {drains_perv, drains_imperv}`
then merged ratios. Memory peak remains the dprst step (~384 G ceiling; see
HPC_REFERENCE).

## Testing

Update [test_build_depstor_dprst.py](../../../tests/test_build_depstor_dprst.py):

- Existing connected-vs-isolated test stays valid (imperv all-nodata → unchanged).
- **New case:** an isolated region touching a few impervious cells **stays
  dprst**, with only those impervious cells removed (`dprst==255` there), and
  those cells are **not** in `onstream`.
- **Invariant assertion:** `dprst_binary == 1` and `imperv == 1` never coincide
  (`dprst ∩ imperv = ∅`).

## Documentation

- [CLAUDE.md](../../../CLAUDE.md): depstor gotchas describe imperv as a region
  exclusion — update to per-cell carve-out.
- [docs/ARCHITECTURE.md](../../ARCHITECTURE.md) and depstor docs: same.
- `slurm_batch/HPC_REFERENCE.md`: note the dprst→perv/carea_map/routing rebuild cascade.

## Deferred (not in scope)

**Approach C** — in addition to the per-cell carve-out, drop a whole region from
dprst if it is >~50% impervious, to delete genuinely urban/concrete water
features as a unit. Only warranted if the "urban-feature" intent (distinct from
double-counting) becomes a goal. Recorded here for future revisit; not implemented.

**Impervious threshold value** — the 50% cut (`VALUE > 50`) is kept as-is here.
Revisiting it (e.g. 30/40/60%) is a separate land-classification decision that
shifts `hru_percent_imperv` CONUS-wide; defer to its own change with a
sensitivity scan if needed.
