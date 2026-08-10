# hru_aspect circular mean

**Issue:** [#201](https://github.com/rmcd-mscb/gfv2-params/issues/201) —
`hru_aspect` is an arithmetic mean of a circular variable
**Status:** design approved, not yet implemented
**Date:** 2026-08-10

## Problem

`configs/zonal/zonal_params.yml`'s `aspect` entry runs `script: zonal`, which is
gdptools/exactextract's fixed continuous stat set — a plain arithmetic mean. TM 6-B9
§603 requires a circular mean:

> The mean orientation (**hru_aspect**) of the predominant down slope direction of
> each HRU is expressed as (0–360) degrees clockwise from north. [...] the
> trigonometric sine and cosine of each cell's aspect are derived to create two new
> rasters of values. The average value for both of these raster values is determined
> for each HRU. The hru_aspect value is then set to the inverse tangent of these two
> values `atan2[sin(aspect), cos(aspect)]`.

Measured across all 361,471 gfv2 HRUs (`gfv2/params/merged/nhm_aspect_params.csv`):

| Statistic | Value |
| --- | --- |
| median `mean` | 179.4° (due south) |
| IQR | 151.7° – 207.5° |
| median within-HRU `std` | 91.3° (uniform-circular ≈ 104°) |

Half of CONUS reporting a mean orientation between 152° and 208° is not terrain. It
is what averaging wrapped directions produces: the column measures how symmetric each
HRU's cell distribution is about 180°, not its predominant orientation.

The circularity rule *was* applied at every raster boundary — `NEAREST` overviews in
`compute_slope_aspect.py`, `build_vrt.py` and `build_border_dem.py`, with two tests
asserting it. Only the zonal aggregation was left arithmetic, and the trade-off was
recorded (in `notebooks/_archive/check_params.ipynb`) but never measured.

`hru_aspect` feeds `PRMSSolarGeometry` and `PRMSAtmosphere`. Aspect controls
direct-beam solar radiation, so a uniform southern bias is a systematic
solar-radiation bias.

A circular mean cannot be reconstructed from an arithmetic one. The information is
gone; the zonal pass must re-run.

## Triage findings not in the issue body

Both were established by measurement before this design was written.

### RichDEM writes 270° for flat cells

RichDEM's `aspect` does not flag flats — it assigns them a real bearing. Measured on
a 3000×3000 window per VPU against the co-registered slope tile:

| VPU | frac slope == 0 | frac aspect == 270 | of aspect == 270, frac slope == 0 |
| --- | --- | --- | --- |
| 01 | 0.0279 | 0.0281 | 0.9905 |
| 07 | 0.0034 | 0.0036 | 0.9441 |
| 12 | 0.0000 | 0.0001 | 0.1781 |

No cell carries `-1` or any other flat sentinel; nodata is `-9999` and is already
excluded (the merged CSV's `min` is 0.0, never -9999). ArcGIS — the implementation TM
6-B9 §603 cites — writes `-1` for flats instead, so this is a RichDEM-specific
artifact introduced when the pipeline moved off ArcPy.

A naive sin/cos fix would propagate it faithfully: a lake or plains HRU would come out
due west. TM 6-B9 defines `hru_aspect` as the mean of the *down slope* direction, and
a flat cell has none, so flats are excluded from the circular mean.

`build_border_dem.py` uses the same `rd.TerrainAttribute(dem, attrib="aspect")` call,
so the Copernicus border fill behaves identically. No special case is needed for it.

### New CONUS sin/cos rasters are avoidable

The issue proposes emitting `sin(aspect)` and `cos(aspect)` as shared rasters. That is
~45 GB per raster (aspect's 19 tiles total 45.1 GB), plus VRTs and overview pyramids,
plus a Part 1 shared-raster re-run — and it lands the two means in *separate* merged
CSVs, which `derived_columns` cannot join without a new cross-param mechanism.

It is unnecessary. gdptools already subsets the source raster to the batch's bounding
box before exactextract sees it (`UserTiffData.prep_agg_data` →
`_get_shp_bounds_w_buffer` → `.sel()`), and batches are KD-tree spatially compact
(`batching.py`). Measured over gfv2's 66 batch gpkgs, the bounding boxes are:

| | cells | float32 |
| --- | --- | --- |
| largest (`batch_0008`) | 0.80e9 | 3.2 GB |
| median | 0.16e9 | 0.6 GB |

So sin/cos can be derived in memory per batch, at a peak of roughly four such arrays.

## Design

### 1. New runner: `src/gfv2_params/zonal_runners/aspect.py`

Registered as `script: aspect` in `BATCH_RUNNERS`. One clip, three exactextract passes
over it:

```
aspect.vrt ─clip_box(batch bounds)─→ asp        slope.vrt ─same clip─→ slp
                                      │                                 │
                                      │                            flat = (slp == 0)
                                      ├──────────── pass 1 ────→ count, mean, std, min,
                                      │            (raw, all         25%/50%/75%, max, sum
                                      │             valid cells)
                       asp.where(~flat)
                          ├─ sin(rad) ── pass 2 ──→ mean_sin, count → n_aspect_cells
                          └─ cos(rad) ── pass 3 ──→ mean_cos

flat_frac = 1 − n_aspect_cells / count
```

**Pass 1 stays unmasked.** The nine existing columns are byte-comparable to today's
product, which keeps `mean` an honest record of what the old product was and makes the
old-vs-new comparison a straight join on HRU id. Masking them would destroy the only
baseline available for validating the fix.

**`flat_frac` costs nothing extra.** It is the ratio of two coverage-weighted counts
the passes already return — no indicator raster, no fourth pass. It is the diagnostic
that makes an all-flat HRU visible instead of silently defaulting.

**The clip is buffered.** gdptools buffers the target bounds by a cell before
subsetting (`_get_shp_bounds_w_buffer`); the runner does the same, because an exact
bbox clip drops the partially-covered edge cells exactextract weights for
boundary-touching HRUs. gdptools' own subset then runs against an already-clipped
array and is a near no-op.

**Co-registration is asserted, not assumed.** Both VRTs derive from the same per-VPU
DEM, so `clip_box` on identical bounds yields identical grids. The runner checks shape
and transform equality and raises on mismatch — a silent half-cell offset would
misalign the flat mask against the aspect it masks, and nothing downstream would ever
report it.

**Writing.** gdptools writes a CSV only when `zonal_writer == "csv"`
(`zonal_gen.py:240`); `calculate_zonal` returns the frame either way. The three passes
pass `zonal_writer=None` and the runner writes the single canonical per-batch CSV
itself, named to the same `base_nhm_aspect_{fabric}_batch_{NNNN}_param.csv` pattern
`run_merge` globs for. A test asserts no stray per-pass CSVs appear in the output
directory, so if gdptools ever changes that behaviour it fails loudly rather than
littering the batch dir with files `run_merge` would then concat.

**Config keys.** The entry gains `slope_raster:`, read via the same
`{data_root}`/`{fabric}` placeholder mechanism as `source_raster`.

**Memory.** `slurm_batch/derive_zonal_params.batch` goes `--mem=32G` → `--mem=64G`.
Peak is ~13 GB of arrays on the worst batch plus exactextract's working set. There is
no per-param memory override in `submit_zonal_params.sh` and this design does not add
one — the bump is harmless for the other zonal params on ~515 GB nodes.

### 2. Two-argument derived columns

`raster_ops.atan2_deg(sin_mean, cos_mean)` returns
`degrees(arctan2(s, c)) % 360`, vectorised over Series.
`zonal_runners/merge.py:apply_derived_columns` accepts `from:` as either a plain
column name (today's shape, unchanged) or a list, dispatching to a transform of that
arity. The `_TRANSFORMS` whitelist gains `atan2_deg`.

```yaml
derived_columns:
  hru_aspect:
    from: [mean_sin, mean_cos]
    transform: atan2_deg
```

Degenerate input is deliberate, not guarded: an HRU with no non-flat cells gets NaN
means from exactextract, so `hru_aspect` is NaN until the fill pass supplies
neighbours' `mean_sin`/`mean_cos` and it is re-derived. An HRU whose orientations
genuinely cancel gets `arctan2(0, 0) = 0` (north) — arbitrary, but so is any other
answer for a surface with no predominant orientation, and `flat_frac` plus the two
means let a consumer detect the case.

### 3. Fill path: re-derive after KNN, never interpolate `hru_aspect`

`hru_aspect` is circular, so KNN-averaging it reproduces exactly the defect this
change fixes — neighbours at 350° and 10° would interpolate to 180°.

`DeclaredParam` (`params_index.py`) gains a sixth field, `derived_columns`, populated
by `_record` from the config entry. `run_fill_sweep` re-applies it after
`fill_missing_values_knn` and `apply_fabric_columns`, immediately before
`write_filled_in_place`. So:

- `mean_sin` and `mean_cos` are declared fillable — both linear, safe to interpolate;
- `hru_aspect` is **not** declared fillable — it is recomputed from whatever its
  sources ended up being, and is therefore always exactly `atan2` of the two columns
  sitting next to it in the same file;
- derived column names are excluded from `resolve_fill_plan`'s `undeclared_with_nan`
  census, since a transient NaN there is expected mid-sweep rather than a gap nobody
  declared.

Widening `DeclaredParam` follows the path its own docstring prescribes: attribute
access, so a sixth field stays invisible to consumers that do not ask for it.

**Scope expansion.** This changes `hru_slope` too. Today `hru_slope` is KNN-filled
independently of `mean`, so a filled row can carry
`hru_slope != tan(radians(mean))` — the derived column and its declared source
disagreeing inside one file. Re-deriving after the fill makes them consistent. That is
a fix, but it is beyond issue #201: it lands as its own commit and leads the PR
description as a scope-expansion callout.

### 4. Config and declaration

The `aspect` entry becomes:

```yaml
  - name: aspect
    script: aspect
    source_raster: "{data_root}/shared/conus/vrt/aspect.vrt"
    slope_raster: "{data_root}/shared/conus/vrt/slope.vrt"
    categorical: false
    merged_file: nhm_aspect_params.csv
    fill_columns: [count, mean, std, min, "25%", "50%", "75%", max, sum,
                   n_aspect_cells, flat_frac, mean_sin, mean_cos]
    derived_columns:
      hru_aspect:
        from: [mean_sin, mean_cos]
        transform: atan2_deg
    prms:
      builder: zonal_runners/aspect.py + zonal_runners/merge.py (derived_columns)
      columns:
        hru_aspect:
          prms: hru_aspect
          processes: [PRMSSolarGeometry, PRMSAtmosphere]
      provenance:
        mean: arithmetic mean of a circular variable -- retained for comparison, NOT hru_aspect
        mean_sin: coverage-weighted mean of sin(aspect) over non-flat cells
        mean_cos: coverage-weighted mean of cos(aspect) over non-flat cells
        n_aspect_cells: non-flat cell count -- the population hru_aspect is derived from
        flat_frac: fraction of the HRU's cells with slope == 0 (no down slope direction)
        count: exactextract cell count
        std: within-HRU standard deviation
        min: within-HRU minimum
        "25%": within-HRU 25th percentile
        "50%": within-HRU median
        "75%": within-HRU 75th percentile
        max: within-HRU maximum
        sum: within-HRU sum
```

The `defects:` block for `mean` is removed and `mean` moves to `provenance:` — it
remains a real statistic, just not the PRMS parameter. `params_for_process` reads
`columns:` only, so `hru_aspect` becomes reachable for the two Processes for the first
time.

`submit_zonal_params.sh`'s hardcoded `PARAMS` array needs no change: the entry keeps
the name `aspect`, so `tests/test_submit_wrapper_param_lists.py` stays satisfied.

### 5. Documentation

- `scripts/build_parameter_index.py` re-run; CI fails if `docs/parameter_index.md` is
  stale. Its "Known gaps" entry for aspect is rewritten from a defect to a resolution.
- The limitation note currently living only in
  `notebooks/_archive/check_params.ipynb` is ported into `docs/` as the historical
  record of why the column exists and what replaced it. `docs/repo_review_issues.md`
  CODE-4 proposes deleting `notebooks/_archive/` wholesale, which would otherwise
  destroy the only surviving record.
- `slurm_batch/RUNME.md` and `HPC_REFERENCE.md`: `aspect` now reads two rasters and
  runs at `--mem=64G`.
- `docs/ARCHITECTURE.md`: `aspect` is no longer a generic `script: zonal` entry.

## Testing

`tests/test_aspect_zonal.py`, built on small synthetic rasters:

- **Wrap-around.** An HRU split between 350° and 10° returns ~0°, not 180°. This is
  the defect, stated as a test.
- **Flat exclusion.** Cells at 270° with slope 0 do not enter the means; `flat_frac`
  reports their coverage-weighted share; an otherwise-uniform HRU's `hru_aspect` is
  unchanged by adding flats to it.
- **All-flat HRU.** `mean_sin`/`mean_cos`/`hru_aspect` are NaN and `flat_frac == 1.0`,
  rather than 270°.
- **Pass 1 is unmasked.** The nine legacy columns match what `run_zonal_batch` would
  produce on the same input, flats included.
- **Grid mismatch raises.** A deliberately offset slope raster raises rather than
  masking against the wrong cells.
- **No stray CSVs.** Only the one canonical per-batch file appears in the output dir.

Extensions to existing tests:

- `tests/test_merge_params.py`: two-argument `derived_columns`; a list `from:` naming a
  missing column still raises; single-argument form unchanged.
- `tests/test_merge_and_fill_params.py`: derived columns are re-derived after the KNN
  fill; a filled row's `hru_aspect` equals `atan2` of its filled sources; derived names
  do not appear in `undeclared_with_nan`.
- `tests/test_params_index.py` (Guard 1) covers the rewritten `prms:` block.

Guards 2 and 3 (`test_params_index_ondisk.py`, `test_merged_products_ondisk.py`) are
data-root-gated and SKIP in CI, so their results are recorded by SLURM job id after the
rollout, never inferred from a green badge.

## Rollout

1. **oregon** (3 batches, minutes). Inspect the distribution: the tell is the
   151.7°–207.5° IQR opening out toward roughly uniform, and `flat_frac` being small
   and spatially sensible. Compare against the retained `mean` column on the same file.
2. **gfv2** in place, after backing up
   `gfv2/params/merged/nhm_aspect_params.csv`. 66 array tasks + chained merge.
   The user chose direct-to-gfv2 over a `gfv2_dev` staging pass: the file being
   replaced is already declared defective, and oregon is a sufficient proof.
3. **tjc** (2 batches).
4. Fill sweep, then record Guard 2 / Guard 3 job ids.

The rollout runs BEFORE the PR merges — a green test suite has previously failed to
imply a correct product on this pipeline.

## Rejected alternatives

- **New CONUS `aspect_sin`/`aspect_cos` rasters** (the issue's proposal). ~90 GB of
  tiles plus overviews and a Part 1 re-run, and the two means land in separate merged
  CSVs that `derived_columns` cannot join. See "Triage findings" above.
- **A generic `source_transform:` hook on `zonal.py`.** More reusable in principle,
  but it still produces two separate merged CSVs with the same join problem, and adds
  config surface nothing else needs today.
- **Declaring `hru_aspect` fillable.** Simplest, and reintroduces the defect on
  gap-filled HRUs.
- **Keeping flat cells.** Matches the issue as filed and is less code, but bakes
  RichDEM's fabricated 270° into flat-dominated HRUs.
