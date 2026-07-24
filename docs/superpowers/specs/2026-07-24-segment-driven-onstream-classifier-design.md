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

### Measured magnitude (oregon fabric)

Restricted to waterbodies whose majority area lies inside the `domain` layer
(4,449 waterbodies):

| on-stream source | COMIDs |
| --- | --- |
| NHD union (WBAREACOMI ∪ flow-through) | 1,550 |
| model `nsegment` intersection | 781 |

The 653 network-WBAREACOMI waterbodies with no intersecting segment sit a **median
1,369 m from the nearest `nsegment`** (1 of 653 within 30 m; 16 within 100 m; 35%
within 1 km). They are genuinely off the model network, not casualties of line
simplification. Median
area 0.045 km²; only 3 exceed 1 km²; ~85 km² total. These are headwater ponds and
small reservoirs that the model does not route — depression storage is the right
representation for them.

Note the earlier, larger figures computed over the segment layer's rectangular
bounding box (1,511 waterbodies / 1,737 km², median distance 11.4 km) are inflated by
features outside the model domain. The domain-restricted numbers above are the
operative ones.

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

Bare `intersects` promotes a waterbody a segment merely grazes. **309 of the CSV's
1,629 pairs (19.0%) have zero-length intersection**, leaving 1,320. Requiring positive
length drops 22 of the 783
COMIDs. That is the same failure mode `CLAUDE.md` already warns about for containment
tests (Eagle Lake and Middle Alkali Lake graze closed basins at frac = 0.000).

WBAREACOMI was considered as a counter-argument, since it is *authored* by NHD rather
than geometrically inferred — an ArtificialPath drawn through a waterbody is stamped
with that waterbody's COMID — and therefore does not degrade under geometry
simplification or vintage mismatch. If the 22 dropped grazes were simplification
artifacts of genuine connections, WBAREACOMI would still assert them. It does not:

| group | n | WBAREACOMI | in NHD union | in **neither** NHD signal |
| --- | --- | --- | --- | --- |
| graze-only (dropped) | 22 | 4 (18%) | 7 (32%) | **15 (68%)** |
| positive-length (kept) | 761 | 610 (80%) | 685 (90%) | 76 (10%) |

The graze-only group is ~3× enriched in "no NHD signal supports this", and **9 of the
22 are Playa** — dropped by the never-on-stream guardrail regardless. Bare `intersects`
would buy 4 genuinely-connected COMIDs at the cost of 15 that nothing corroborates.
Positive length stands.

A cell-scale threshold (`> cell_size * sqrt(2)` = 42.4 m, yielding 754 COMIDs) was
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

`segment_waterbody_pairs` runs `gpd.sjoin(..., predicate="intersects")` then computes
`.intersection().length` on matched pairs only.

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
wbody_connectivity → dprst → ...`. No raster inputs. At CONUS scale this is 186,709
segments against 448,124 polygons — a vector sjoin, minutes, no windowing, no
special memory sizing.

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
* `min_onstream_comids` added as an optional floor. `oregon: 500` (measured 781).
  **`gfv2` omits the key** until the first CONUS run measures a value — an omitted
  optional key is a valid state (`min_endorheic_comids` has the same contract), not a
  placeholder. CONUS is unguarded by this floor until then; that is explicit and
  accepted.
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
   intersection promotes 18 Playa and 10 Ice Mass in oregon.
7. **Non-numeric/NaN COMID rows dropped with a logged count**, matching existing
   coercion behaviour elsewhere in the builder.

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

`tests/test_wbody_connectivity.py` — extended:

* segments-only is the default path
* both NHD keys present → union, plus the WARNING
* missing `segment_wbody_comids` → raises
* `min_onstream_comids` raises at the consuming end (the `--from wbody_connectivity`
  path)

**Fixture provenance.** The full 1,629-row CSV cannot itself drive a CI test —
reproducing it requires the oregon fabric gpkg and `conus_waterbodies.gpkg`, neither of
which is in the repo. It is committed to `tests/data/OR_waterbody_nseg_intersection.csv`
as the versioned audit trail, and a small fixture (~20 features) is cut from real
oregon geometry covering clean through-flow, a zero-length graze on a Playa, a
multi-segment lake, and a near-miss, with expected pairs taken from that CSV. CI
exercises the real-geometry fixture; the CSV is its source.

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

1. `oregon` first — small, fast, and where the evidence in this spec was measured.
   Expect on-stream COMIDs 1,550 → 781 in-domain and a corresponding rise in
   `dprst_frac`.
2. `gfv2_dev` for the CONUS shakedown, **not** `gfv2` (standing rule: validate
   unproven rebuilds on `gfv2_dev`). Set `gfv2`'s `min_onstream_comids` from this run.
3. Cascade: `--from segment_wbody` → `wbody_connectivity` → `dprst` → `routing` →
   `routing_hru` → `drains_perv` / `drains_imperv`. Then the Stage B re-derive of the
   dprst-derived fractions, the 6 PRMS ratios, and `dprst_depth`.
4. Promote to `gfv2` once the `gfv2_dev` product is checked.

## Out of scope

* The `oregon` profile points `waterbody_gpkg` at the retired
  `input/nhd/conus_waterbodies.gpkg` while `gfv2` uses the fresher
  `input/nhd/nhd_waterbodies.gpkg`. The two give different results here (1,629 vs
  1,453 pairs; 783 vs 776 COMIDs, an ~11% difference in pairs), so the divergence is
  worth resolving — but it is a separate change and would invalidate the fixture's
  provenance if folded in here. Flagged, not fixed.
* Retiring `download/nhd_flowlines`, `download/nhd_flowthrough`, or
  `download/nhd_topology`. They remain the opt-in comparison path.
