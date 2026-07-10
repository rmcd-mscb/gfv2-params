# Design — Issue #173 Phase 0 spike: `dprst_depth_avg` from best-available topography

**Date:** 2026-07-10
**Issue:** #173 — *derive `dprst_depth_avg` from 3DEP topography over depression polygons; set `op_flow_thres = 1.0`*
**Scope of this doc:** the **Phase 0 prerequisite spike only**. No builder, no DAG
registration, no config block, no `tests/test_*` — those are Phase 1 and are
explicitly gated on this spike's go/no-go outcome.

## Why a spike, not a builder

Issue #173 hard-gates all builder work behind a focused investigation. The
open questions — *are SwampMarsh polygons hydro-flattened? is the freeboard
non-trivial? does a terrain-slope depth model give plausible magnitudes? does a
single depth–area power law span the size range?* — cannot be answered from the
desk. The spike answers them on real data and produces a **decision per FTYPE**,
which becomes the design input for Phase 1.

**Deliverable:** `docs/dprst_depth_spike.md` with a go/no-go recommendation and a
per-FTYPE method decision table. Success is a *documented decision*, not a
parameter — "no-go, fall back to constant 49 in for FTYPE X" is a valid outcome.

## Environment facts established (2026-07-10)

- **S3 `prd-tnm` is reachable from this HPC** (the DPI middlebox permits AWS over
  HTTPS, unlike the PROJ CDN and `gh`). Verified with an anonymous
  `ListObjectsV2` against `StagedProducts/Elevation/1m/Projects/`. → windowed
  `/vsis3/` COG reads work; **no bulk staging required**.
- `richdem`, `whitebox`, `rasterio`, `pyogrio` are all in the pixi env.
- dprst inputs are all present:
  `{data_root}/input/nhd/conus_waterbodies.gpkg`,
  `connected_waterbody_comids.parquet`, `flowthrough_waterbody_comids.parquet`
  (`data_root = /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`).

## Key decision: "best available topography"

Per the user, the spike uses a **resolution ladder**, not 1 m only:

```
1 m 3DEP project COG (where a project covers the polygon)
  → 10 m seamless 1/3 arc-second (full-CONUS floor)
    → documented constant (NHM median 49 in) only if both fail
```

Consequences:
- **Coverage is never zero.** The "coverage audit" reframes from *is it covered?*
  to *what resolution does each polygon get?*
- The spike does **not** confine itself to 1 m footprints; it can measure the 10 m
  path on the same ND sample and quantify the resolution sensitivity.
- Provenance in Phase 1 must record resolution **and** method per HRU; the spike
  establishes what those buckets will look like.

## Study area

Prairie Pothole Region, North Dakota (issue's recommendation; also Hay and
others 2018 PRMS depression-storage calibration site → comparison point). The
specific 3DEP 1 m project(s) are **chosen programmatically** as those with the
densest gfv2-dprst overlap, not hardcoded.

## Shared machinery

One small module: `scripts/diagnose/dprst_depth_probe.py`.

- **dprst polygon set** — reuse the *existing* classification
  (`wbody_connectivity` → `dprst`): `conus_waterbodies.gpkg` minus
  (`connected` ∪ `flowthrough`) COMIDs, Ice Mass excluded, Playa forced in.
  Import the real logic; do not re-derive — the spike must measure the shipped
  product.
- **Per-polygon windowed DEM read** — GDAL `/vsis3/prd-tnm/...` (1 m project COG)
  or the 10 m seamless source, clipped to polygon bbox + a rim buffer,
  reprojected on the fly. **Never a lattice** (1 m CONUS ≈ 1.5e13 cells).
- **Depth-to-spill** — `richdem.FillDepressions(dem_f64) − dem`, float64 (per the
  DEM-derivatives gotcha), which sidesteps the WBT LZW+`predictor=2` corruption
  entirely.

## The six investigation tasks

Each produces a numbered evidence section in `docs/dprst_depth_spike.md`.

1. **Coverage audit.** Overlay dprst polygons against the 3DEP 1 m project
   footprint index (WESM / S3 project extents). Report count-% and area-% getting
   1 m vs 10 m, per VPU. Pure vector op; head node.
2. **Flatness-detector validation.** ND sample per FTYPE: interior elevation
   range, σ, unique-value count. Confirm hydro-flattened surfaces are *exactly*
   constant (< ~1 cm), not merely low-variance; produce a separability threshold.
3. **Settle SwampMarsh (50.5% of dprst area).** Apply the detector to marsh
   polygons specifically; report the flattened fraction. Single highest-value
   number — decides whether at-risk area is ~89% or ~38.5%.
4. **Quantify freeboard.** For detected-flat polygons, the distribution of
   `filled − raw` (= spill − water surface). Non-trivial ⇒ baseline already
   carries most of the load; ≈0 ⇒ ponds are outlet-controlled and the terrain
   model must carry it.
5. **Prototype Hollister.** Reimplement `lakeMaxDepth`-style terrain-slope
   extension in Python for a LakePond sample; sanity-check magnitudes; establish
   the **max→mean conversion** empirically where any real bathymetry exists, else
   a documented literature factor (best-available, per user).
6. **Test the depth–area regression.** Fit depth–area and V–A power laws on
   playas + sub-2-acre donors; single law vs. ecoregion/VPU stratification;
   report extrapolation risk into the recipient size range.

## Execution

- Light vector work (task 1, polygon selection) on the head node.
- Per-polygon DEM sampling (tasks 2–6) on a **bounded ND sample** (a few hundred
  polygons per FTYPE) over S3 in one interactive `pixi run` session — no SLURM
  array for a spike. Sample sizes are `log`-emitted; nothing silently capped.
- No `pytest` on the head node (import-storm rule); the spike is exploratory
  analysis, not a tested builder.

## Writeup structure (`docs/dprst_depth_spike.md`)

1. Executive summary — go/no-go + one-line per-FTYPE verdict.
2. Study area & sample — project(s) chosen, per-FTYPE sample sizes, representativeness.
3. Six evidence sections (tasks 1–6): method → figure/table → finding.
4. **Decision table** (the exit-criteria artifact):

   | FTYPE | % dprst area | Best topo | Flattened? | Method chosen | Fallback |
   |---|---|---|---|---|---|
   | SwampMarsh | 50.5 | … | (empirical) | … | constant 49 in |
   | LakePond | 36.6 | … | yes | … | … |
   | Playa | 10.8 | … | n/a (dry) | freeboard = full depth | … |
   | Reservoir | 1.9 | … | yes | … | … |

5. Projected CONUS bucketing — defensible estimate of the area fraction per
   method bucket (exit criterion).
6. Go/no-go + Phase 1 recommendation — including whether the fallback share is
   large enough to revisit `op_flow_thres = 1.0`.

## Exit criteria (from the issue)

A decision, **per FTYPE**, on which of {freeboard-only, freeboard + terrain
model, depth–area regression, constant fallback} to use, plus a defensible
estimate of what fraction of CONUS dprst area lands in each bucket.

## Non-goals (this spike)

- The Phase 1 `dprst_depth` builder, its DAG registration, config, tests.
- Any change to the dprst classifier (`wbody_connectivity` → `dprst`) — consumed
  unchanged.
- `dprst_flow_coef` / `dprst_seep_rate_open` / `smidx_exp` (separate issues).
- Setting `op_flow_thres = 1.0` in the emitted parameter set (Phase 1).

## References

Issue #173; `docs/nhm_source_crosscheck_2026-07.md`;
`docs/Surface_depression_storage_Driscoll_2020.md`;
Hollister, Milstead & Urrutia 2011 (PLoS ONE 6(9):e25764);
Hollister & Milstead 2010; `lakemorpho` R package; Martinsen and others 2023;
USGS Lidar Base Specification (hydro-flattening ≥ 2 acres).
