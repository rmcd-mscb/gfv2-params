# Segment-driven on-stream classifier — design

**Date:** 2026-07-24
**Status:** design, awaiting review
**Issue:** file on approval

Changes the **source** of the on-stream waterbody set from NHD flowline topology to
the model's own `nsegment` network. Supersedes the "UNION of two COMID sources"
contract described in the first bullet of `CLAUDE.md`. Every guard built on top of
that contract — the endorheic subtraction (#147/spec 2026-07-12), the Playa/Ice Mass
never-on-stream guardrail, the `endorheic_wbody.tif` exemption in `dprst.py` (#161),
the on-stream routing barrier (#158/#159), and the same-HRU restriction (#160/#162) —
is retained unchanged.

Also folds in a **waterbody-layer consistency fix**: `oregon`, `gfv2_dev`, and `tjc`
move from the retired `input/nhd/conus_waterbodies.gpkg` to the current
`input/nhd/nhd_waterbodies.gpkg` that `gfv2` already uses. This was initially scoped
out, then pulled in — it turns out to change the classifier's inputs materially (see
"Waterbody layer" below), so shipping it separately would have meant validating the
new classifier twice.

## Problem

The dprst/on-stream split currently asks: **is this waterbody on the NHD network?**
It is answered by unioning two NHD-derived COMID sets in `wbody_connectivity`:
WBAREACOMI artificial-path topology (`download/nhd_flowlines`) and geometric
flow-through (`download/nhd_flowthrough`), both gated on NHDPlus Network-Flowline
membership (`download/nhd_topology`).

But the parameters this pipeline produces feed a PRMS/NHM model whose routing network
is the fabric's `nsegment` layer, not NHD's flowlines. NHD's network is far finer.
A waterbody that NHD routes through, but that has no model segment, is not routed by
the model at all — yet it is currently excluded from depression storage. The model
then has no representation of that water: not stream routing, not depression storage.

The correct question for this pipeline is therefore: **is this waterbody on the
*model's* routing network?**

### Measured magnitude (oregon fabric, `nhd_waterbodies`)

Restricted to waterbodies whose majority area lies inside the `domain` layer
(4,449 waterbodies):

| on-stream source | COMIDs |
| --- | --- |
| NHD union (WBAREACOMI ∪ flow-through) | 1,550 |
| model `nsegment` intersection, positive length | **768** |

The 654 network-WBAREACOMI waterbodies with no intersecting segment sit a **median
1,367 m from the nearest `nsegment`** (2 of 654 within 30 m; 17 within 100 m; 35%
within 1 km). They are genuinely off the model network, not casualties of line
simplification. Median area 0.045 km²; only 3 exceed 1 km²; ~85 km² total. These are
headwater ponds and small reservoirs that the model does not route — depression
storage is the right representation for them.

This near-miss conclusion is robust to the waterbody layer: on the retired
`conus_waterbodies.gpkg` it is 653 waterbodies at a median 1,369 m, the same 85 km².

Note that figures computed over the segment layer's rectangular bounding box rather
than the domain (1,511 waterbodies / 1,737 km², median distance 11.4 km) are inflated
by features outside the model domain. The domain-restricted numbers above are the
operative ones.

### Waterbody layer

`gfv2` reads `input/nhd/nhd_waterbodies.gpkg`; `oregon`, `gfv2_dev`, and `tjc` still
read the retired `input/nhd/conus_waterbodies.gpkg`. Same 448,124 COMIDs, same fields,
same CRS — but a different shoreline vintage, and the difference is not cosmetic for
this classifier:

| oregon | `conus_waterbodies` (retired) | `nhd_waterbodies` (current) |
| --- | --- | --- |
| candidate pairs | 1,629 | 1,453 |
| zero-length graze pairs | 309 (**19.0%**) | 45 (**3.1%**) |
| `intersects` COMIDs | 783 | 776 |
| positive-length COMIDs | 761 | 770 |
| graze-only COMIDs dropped | 22 | 6 |

The retired layer's 19% graze rate is largely a registration artifact of its shoreline
vintage against NHM segment geometry; on the current layer it falls to 3.1%. All three
fabrics move to `nhd_waterbodies`.

`gfv2_dev` reading a different waterbody layer from `gfv2` is a defect independent of
this change: `gfv2_dev` exists to mirror `gfv2` for unproven rebuilds, so the CONUS
shakedown in the rollout below would not have represented `gfv2`.

`gfv2_vpu01` is unaffected — it reads its own `NHM_01_draft.gpkg` layer `wbs`, not the
CONUS layer.

### Measured magnitude (gfv2 / CONUS)

From `scripts/diagnose/measure_segment_onstream.py` (job 1908953):

| quantity | value |
| --- | --- |
| candidate pairs | 73,343 |
| zero-length graze pairs | 2,300 (**3.1%**) |
| `intersects` COMIDs | 48,620 |
| **positive-length COMIDs** | **48,529** |
| graze-only dropped | 91 |
| invalid polygons repaired | 193 (0 residual failures) |
| segment-only vs WBAREACOMI (132,900) | 16,939 |
| segment-only vs flow-through (160,904) | 6,265 |

The 3.1% graze rate matches oregon's on the same layer exactly — an independent
consistency check on the geometry rule at two very different scales.

#### Reservoir FTYPE anomaly — investigated, not a defect

The CONUS result keeps only 205 of 5,224 Reservoirs (3.9%), and 138 of those 205 fall
in the oregon bounding box. That looked like regional data loss in the segment layer,
so it was chased down; it is not.

Controlling for size, Reservoirs outside the far West hit segments ~10× less often
than western ones at every size class, while LakePond shows no such gap. But **NHD's
own topology has the same gap**: it calls 4.6% of non-western Reservoirs on-network
versus 34.5% of LakePonds (7.5×), and the segment classifier reproduces that at 7.6×
(1.2% vs 9.1%) — uniformly stricter, as a coarser network should be. Of the 4,291
non-western Reservoirs the rule leaves off-segment, **NHD agrees 96.2% are
off-network** (92.5% for the >1 km² subset). Outside the West, NHD's `Reservoir` FTYPE
is dominated by off-channel impoundments — the largest examples are tailings ponds and
unnamed artificial basins 5–9 km from any flowline.

Verified also that the CONUS run lost no data: all 1,255 COMIDs found by an
independent oregon-bbox run using the gfv2 segment layer are present in the CONUS
output.

## The rule

```
onstream = { waterbody COMID : some nsegment ∩ waterbody has length > 0 }
         ∪ WBAREACOMI            # opt-in comparison only; absent by default
         ∪ flow-through          # opt-in comparison only; absent by default
         − endorheic             # unchanged strict subtraction
         − Playa / Ice Mass      # unchanged FTYPE guardrail
```

Only the first line is new. Everything below it is the existing chain, unmodified.

### Why positive length, not bare `intersects`

The handed-over crosswalk
(`{data_root}/input/staging/OR_waterbody_nseg_intersection.csv`, 1,629 rows / 783
COMIDs / 1,353 segments) was produced by a plain
`gpd.sjoin(nsegment, waterbodies, predicate="intersects")` against
`input/nhd/conus_waterbodies.gpkg`. This was verified by reproducing it **exactly** —
1,629 pairs, 783 COMIDs, zero rows differing in either direction. Its `nseg_id` is the
oregon `nsegment` layer's `segment_id` (equivalently `model_seg_idx`), **not**
`nhm_seg_id`.

Bare `intersects` promotes a waterbody a segment merely grazes. On the retired layer
the CSV was built from, **309 of 1,629 pairs (19.0%) have zero-length intersection**
and the rule drops 22 of 783 COMIDs. That is the same failure mode `CLAUDE.md` already
warns about for containment tests (Eagle Lake and Middle Alkali Lake graze closed
basins at frac = 0.000).

**On the production layer the rule is nearly a no-op: 45 of 1,453 pairs (3.1%), 6 of
776 COMIDs.** It is cheap insurance, not a major correction — and the insurance is
against exactly the vintage-registration artifact the retired layer exhibits at 19%.
That is an argument for keeping it, but the honest scale of its effect on
`nhd_waterbodies` is six waterbodies.

WBAREACOMI was considered as a counter-argument, since it is *authored* by NHD rather
than geometrically inferred — an ArtificialPath drawn through a waterbody is stamped
with that waterbody's COMID — and therefore does not degrade under geometry
simplification or vintage mismatch. If the dropped grazes were simplification artifacts
of genuine connections, WBAREACOMI would still assert them. It does not, on either
layer:

| layer | group | n | WBAREACOMI | in NHD union | in **neither** |
| --- | --- | --- | --- | --- | --- |
| `nhd_waterbodies` | graze-only (dropped) | 6 | 2 | 2 | **4** |
| `nhd_waterbodies` | positive-length (kept) | 770 | 611 | 687 | 83 |
| `conus_waterbodies` | graze-only (dropped) | 22 | 4 (18%) | 7 (32%) | **15 (68%)** |
| `conus_waterbodies` | positive-length (kept) | 761 | 610 (80%) | 685 (90%) | 76 (10%) |

On the production layer, 4 of the 6 dropped COMIDs have no NHD signal at all and 2 of
the 6 are Playa (dropped by the never-on-stream guardrail regardless). The corroboration
is directionally the same as on the retired layer, but rests on 6 cases rather than 22 —
weaker evidence for a correspondingly smaller effect. Positive length stands.

A cell-scale threshold (`> cell_size * sqrt(2)` = 42.4 m, yielding 754 COMIDs on the
retired layer) was
also rejected: it introduces a tunable knob into the dprst classifier, which this
codebase has deliberately kept knob-free.

### Edge cases, verified and deliberate

Verified against shapely; these are decisions, not accidents, and each is pinned by a
test:

| case | intersection | result |
| --- | --- | --- |
| segment crosses the interior | LineString, 10.0 | promoted |
| segment touches at a single point | Point, 0.0 | dropped |
| segment clips a corner through the vertex | Point, 0.0 | dropped |
| segment collinear with the shoreline | LineString, 6.0 | **promoted** |
| segment endpoint terminates inside | LineString, 5.0 | **promoted** |

The last two matter:

* A segment digitised **along** a shoreline is promoted. Accepted: a line the modeller
  drew along the lake edge is a routing decision, and excluding it would need
  interior-only geometry machinery for a rare case.
* A segment that **terminates inside** a waterbody is promoted, whereas the NHD
  flow-through rule deliberately required *both* inflow and outflow and left
  inflow-only terminal sinks as dprst. Under the segment rule that discrimination is
  gone, so the **endorheic subtraction is what still demotes genuinely closed
  terminal lakes** (terminus-inside-itself, reading the same FDR the router reads).
  This is a second, independent reason the endorheic guard is retained.

### Why the NHD sources stay in the code but leave the profiles

The NHD table reads remain in `wbody_connectivity` behind the existing optional
profile keys, so a fabric can union them back in for A/B comparison. The keys are
commented out of every profile, so **segments-only is the default on every fabric**.

This leaves two possible definitions of "on-stream" in one builder — a dual path that
has bitten this pipeline before via stale tables silently winning. Mitigation: when
either key is present, `wbody_connectivity` logs a **WARNING** naming it as a
non-default comparison mode, and itemises each source's contribution in its count line.

## Components

Split mirrors the `endorheic` step: pure logic in `src/gfv2_params/`, thin builder in
`src/gfv2_params/depstor_builders/`.

### `src/gfv2_params/segment_wbody.py` (new, pure logic)

```python
segment_waterbody_pairs(seg_gdf, wb_gdf) -> DataFrame[comid, seg_index, overlap_m]
segment_waterbody_comids(pairs) -> set[int]        # overlap_m > 0
write_segment_comids(comids, pairs, path) -> None  # parquet: comid, n_segments, overlap_m
load_segment_comids(path) -> set[int]
check_onstream_floor(n, *, fabric, floor, source) -> None
```

`segment_waterbody_pairs` runs `gpd.sjoin(..., predicate="intersects")`, repairs
invalid geometry with `shapely.make_valid` (guard 8), then computes
`.intersection().length` on matched pairs only, chunked with a per-row fallback.

**No segment-id column is required.** The four fabrics have mutually incompatible
`nsegment` schemas — oregon `segment_id`/`model_seg_idx`, gfv2 merged `seg_id`, tjc
`nhm_seg`/`model_seg_idx`, gfv2_vpu01 `seg_id` — so keying on the positional index
makes the builder work everywhere with no new profile key. Segment identity is needed
only for diagnostics and fixtures, never by the pipeline, whose output is a COMID set.

### `src/gfv2_params/depstor_builders/segment_wbody.py` (new builder)

Reads `segments_gpkg`/`segments_layer` and `waterbody_gpkg`/`waterbody_layer`,
reprojects both to the template CRS (asserting it is projected, since the rule is in
metres), writes `segment_waterbody_comids.parquet`, registers `segment_wbody_comids`.

DAG position: `landmask → imperv → waterbody → endorheic → segment_wbody →
wbody_connectivity → dprst → ...`. No raster inputs.

**Measured at CONUS scale** (186,709 segments × 448,124 polygons, job 1908953):
**42 s wall, 2.0 GB peak RSS.** A vector sjoin — no windowing and no special memory
sizing, unlike the full-grid raster steps. Breakdown: 3 s to read both layers, 9 s
sjoin, 6 s `make_valid`, 13 s intersection.

**Join on the polygon row index, not COMID.** The waterbody layer has 448,124 rows for
447,907 distinct COMIDs, so merging pair rows back on COMID duplicates them — it
inflated the measurement script's pair count from 73,343 to 73,723. Harmless for a
COMID set (a COMID is kept if any pair has positive length), but it would corrupt the
per-COMID `n_segments` and `overlap_m` columns, so the builder must carry
`index_right` through instead.

### `src/gfv2_params/depstor_builders/wbody_connectivity.py` (modified)

* `segment_wbody_comids` becomes the required primary source; raises if absent, with
  the same "run `--from segment_wbody`" guidance the `endorheic_comids` check gives.
* `connected_comids_table` / `flowthrough_comids_table` become fully optional; when
  present they union in and trigger the WARNING.
* Applies `min_onstream_comids` to the hydrated table (see guards).
* `select_connected_waterbodies`, `_assert_no_endorheic_repromotion`, and
  `_assert_endorheic_selection_is_comid_faithful` are unchanged — the new set is
  COMID-keyed like the old one, so those invariants hold as written.

### Config

`configs/depstor/depstor_rasters.yml`:

```yaml
  - name: segment_wbody
    output: segment_waterbody_comids.parquet
```

`configs/base_config.yml`, for `gfv2`, `gfv2_dev`, `gfv2_vpu01`, `oregon`, `tjc`:

* `connected_comids_table` / `flowthrough_comids_table` commented out, with a block
  explaining they are now opt-in comparison mode.
* `min_onstream_comids` added as an optional floor, sized ~35% below the measured
  value on each fabric: `oregon: 500` (measured 770), `gfv2: 30000` (measured 48,529).
  `tjc` omits the key, matching how it omits `min_endorheic_comids`.
* `waterbody_gpkg` for `oregon`, `gfv2_dev`, and `tjc` moves from
  `{data_root}/input/nhd/conus_waterbodies.gpkg` to
  `{data_root}/input/nhd/nhd_waterbodies.gpkg`. `gfv2` is already there;
  `gfv2_vpu01` keeps its own `NHM_01_draft.gpkg` layer `wbs`.
* The `gfv2` `segments_gpkg` comment currently states "segments no longer feed any
  depstor step (the streambuffer step was retired)". Now false — rewrite.

No profile key is added for the crosswalk itself. `segments_gpkg`/`segments_layer`
already exist on every fabric and are today plumbed into `BuildContext` but read by no
builder; this change gives them a consumer again.

## Guards

Each traces to a specific failure this pipeline has already had.

1. **`segment_wbody_comids` required** in `wbody_connectivity`. Without it the fabric
   would silently classify every waterbody as depression storage.
2. **`min_onstream_comids` enforced at both ends** — in the producing builder *and* in
   `wbody_connectivity`. This is the endorheic-floor lesson verbatim: `--from
   wbody_connectivity` (the documented cascade-rebuild recipe in `slurm_batch/RUNME.md`)
   leaves the producer out of the run list and hydrates its table off disk unvalidated,
   so a floor living only in the producer never runs on the path operators actually use.
3. **Extent guard** — raise if the segment layer's bounds do not intersect the template
   bounds. Catches a `segments_gpkg` mis-wired to another fabric, which would otherwise
   make every waterbody depression storage and exit 0.
4. **Coverage logging, no threshold** — log matched COMIDs vs. total waterbodies, plus
   the graze-drop count, so a partial segment layer is visible in the log. Deliberately
   not a magic number.
5. **Loud union warning** — see above.
6. **Playa/Ice Mass guardrail unchanged**, and still load-bearing: the raw segment
   intersection promotes 14 Playa and 10 Ice Mass in oregon on `nhd_waterbodies`
   (18 and 10 on the retired layer).
7. **Non-numeric/NaN COMID rows dropped with a logged count**, matching existing
   coercion behaviour elsewhere in the builder.
8. **Invalid input geometry repaired, and the repair counted.** Found the hard way:
   the CONUS measurement run died with
   `GEOSException: TopologyException: side location conflict at 1251436.48 1192486.72`
   after 73,343 candidate pairs. `sjoin` survives invalid polygons because prepared
   predicates are tolerant; `.intersection()` does not. Oregon never exposes this —
   it is a CONUS-only failure, in the same family as the NHD measured-3D XYZM →
   `Point()` crash fixed in `2a67d85`. The builder therefore runs
   `shapely.make_valid` on the invalid subset of both layers before intersecting,
   logs how many were repaired, and computes lengths in chunks with a per-row
   fallback so one unrepairable pair cannot abort a CONUS run. Pairs that still fail
   are scored NaN, counted, and **raise** if any remain — silently scoring an
   unmeasurable pair as zero-length would demote a waterbody to dprst on a geometry
   error.

Not guarded separately: an empty result. The existing `len(sel) == 0` raise in
`wbody_connectivity` already fails loud on that case and is retained as-is. Unlike the
endorheic table — where a domain with no closed basin (`tjc`) legitimately yields zero
rows — an empty on-stream set means every waterbody in the domain becomes depression
storage, which is a wiring failure, not a valid domain property.

## Tests

`tests/test_segment_wbody.py` — fully synthetic geometry, matching this repo's
convention that CI runs with no data root:

* the five edge cases in the table above, including collinear-shoreline and
  endpoint-inside pinned as deliberate promotions
* multi-segment lake → one COMID with `n_segments == 3`
* Playa promoted by geometry, then dropped by the FTYPE guardrail
* non-numeric / NaN COMID rows dropped, count logged
* extent guard raises on a disjoint segment layer
* `min_onstream_comids` raises in the producer
* a self-intersecting (invalid) polygon is repaired and still classified, and the
  repair count is reported
* an unrepairable pair raises rather than scoring zero-length

`tests/test_wbody_connectivity.py` — extended:

* segments-only is the default path
* both NHD keys present → union, plus the WARNING
* missing `segment_wbody_comids` → raises
* `min_onstream_comids` raises at the consuming end (the `--from wbody_connectivity`
  path)

**Fixture provenance.** The full 1,629-row CSV cannot itself drive a CI test —
reproducing it requires the oregon fabric gpkg and a CONUS waterbody layer, neither of
which is in the repo. It is committed to `tests/data/OR_waterbody_nseg_intersection.csv`
as the versioned audit trail of the handoff.

It is **not** a reproduction target under the production config, because it was built
against the now-retired `conus_waterbodies.gpkg`. Against `nhd_waterbodies.gpkg` the
same bare-`intersects` join yields 1,453 pairs: 1,451 shared with the CSV, 2 new, 178
CSV pairs absent; 776 COMIDs, all of them in the CSV, with 7 CSV COMIDs absent. That
delta is the layer swap, and it is documented rather than reconciled.

The CI fixture is therefore cut from **`nhd_waterbodies` geometry** — ~20 real oregon
features covering clean through-flow, a zero-length graze on a Playa, a multi-segment
lake, and a near-miss — with expected pairs computed against that layer. The CSV
remains the historical record of where the requirement came from.

## Docs

* `CLAUDE.md` — first bullet ("the dprst/on-stream split is driven by the UNION of two
  COMID sources") is invalidated and needs a full rewrite, including the
  `nhd_topology` ordering constraint, which now applies only to the opt-in comparison
  path.
* `docs/ARCHITECTURE.md` — depstor step list and DAG.
* `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md` — new step in the cascade;
  the `--from wbody_connectivity` recipe now also has `--from segment_wbody` upstream.
* depstor workflow docs page.
* `scripts/render_depstor_figures.py` — the DAG figure currently draws `nhd_topology`
  feeding both COMID steps; it needs `segment_wbody` as the primary path.

## Rollout

The waterbody-layer swap and the classifier change land together, so every fabric
below rebuilds against `nhd_waterbodies` at the same time. Note the swap moves the
`waterbody` builder itself, not just the classifier — so the cascade starts one step
earlier than a classifier-only change would.

1. `oregon` first — small, fast, and where the evidence in this spec was measured.
   Expect in-domain on-stream COMIDs 1,550 → 768 and a corresponding rise in
   `dprst_frac`.
2. `gfv2_dev` for the CONUS shakedown, **not** `gfv2` (standing rule: validate unproven
   rebuilds on `gfv2_dev`). Waiving it was considered and rejected on 2026-07-24: the
   change is large enough that the shakedown is cheap insurance against far costlier
   debugging on the canonical product. `gfv2_dev`'s `waterbody_gpkg` fix is a
   prerequisite for this step to mean anything — it currently reads a different
   waterbody layer from `gfv2`, so without that fix the shakedown would not represent
   `gfv2`. Expect `segment_wbody` to emit **48,529** COMIDs; a materially different
   count means an input changed, and is the signal to stop before the cascade.
3. Cascade: `--from waterbody` → `endorheic` → `segment_wbody` → `wbody_connectivity`
   → `dprst` → `routing` → `routing_hru` → `drains_perv` / `drains_imperv`. Then the
   Stage B re-derive of the dprst-derived fractions, the 6 PRMS ratios, and
   `dprst_depth`. `waterbody` and `dprst` are the ~384G full-grid steps.
4. Promote to `gfv2` once the `gfv2_dev` product is checked.

## Out of scope

* Retiring `download/nhd_flowlines`, `download/nhd_flowthrough`, or
  `download/nhd_topology`. They remain the opt-in comparison path.
* Deleting `input/nhd/conus_waterbodies.gpkg`. It stays on disk as the A/B reference
  for the layer swap, as it already did for `gfv2`.
* `gfv2_vpu01`'s waterbody layer. It reads a fabric-local `wbs` layer rather than
  either CONUS layer, so the swap does not apply to it.
