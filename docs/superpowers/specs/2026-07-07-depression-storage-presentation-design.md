# Design: Depression-storage workflow — pyWatershed requirements + colleague presentation

**Date:** 2026-07-07
**Branch:** `worktree-depstor-presentation`
**Status:** approved design → implementation plan

## Goal

Two documentation deliverables, **no pipeline code**:

1. A **pyWatershed depression-storage requirements gap doc** that confirms our
   parameter product covers everything pyWatershed's runoff/dprst module needs, and
   documents the intended source of the few params it needs that our product does
   not yet emit.
2. A **slide deck for USGS modeling colleagues** that illustrates the
   depression-storage workflow, comparing the legacy ArcPy pipeline
   (`docs/0b_TB_depr_stor.py`) with the current open-source pipeline in this repo.
   The deck follows the pattern established by the sibling snow-depletion-curve
   deck (`docs/superpowers/specs/2026-07-07-snodas-snarea-presentation-design.md`).

**Audience:** USGS modelers familiar with PRMS/NHM but not with this pipeline's
internals. Method- and workflow-focused, not a code deep-dive. Plain-English terms.

## Ground truth this design rests on (verified 2026-07-07, not memory)

Memory going into this work was stale; these facts were re-verified against the
repo and datastore:

- **The `drains_to_dprst` hydrologic grounding is already merged to `main`.** The
  full story is landed: #163 Network-Flowline membership gate (closed #161,
  endorheic lakes); #162 same-HRU restriction on `sro_to_dprst_*`; #159 on-stream
  waterbodies as routing barriers (#158); #152 topology-aware source-lake
  promotion; #145 through-flow on-stream reclassification; #144 per-cell
  impervious carve.
- **The CONUS gfv2 product on disk reflects the grounded classifier.** Depstor
  rasters (`dprst_binary`, `onstream_binary`, `drains_to_dprst*`, `connected_wbody`)
  rebuilt **Jul 2 2026** (post-merge); final param CSVs (`nhm_dprst_frac`,
  `nhm_sro_to_dprst_{perv,imperv}`, `nhm_carea_max`, `nhm_smidx_coef`,
  `nhm_hru_percent_imperv`) rebuilt **Jul 5 2026**. Data root:
  `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`, fabric `gfv2`.
- **A pre-fix before/after snapshot exists on disk:**
  `gfv2/depstor_rasters_pre_flowthrough_2026-06-26/` vs current
  `gfv2/depstor_rasters/`.
- **Open depstor issues are refinements, not blockers:** #157 (cross-VPU seam,
  ~0.05%), #156 (~4% clump-merge reclassification), #155 (permanence gate), #154
  (Reservoir bucket), #147 (depression-respecting FDR — A/B harness merged in
  #148, investigation pending). **Per decision below, none are worked here.**
- **pyWatershed is installed** as 2.0.4 in the isolated `reference` pixi
  environment (`pixi run -e reference`; python 3.10, `no-default-feature`).

## Decisions (locked)

- **Do not touch depstor pipeline code.** The classifier is complete and the
  product reflects it. #157/#156 and the other open issues are *noted* in both
  deliverables as honest known refinements, **not fixed**.
- **pyWatershed piece is analysis + gap doc only.** Document the intended source
  of the missing params; do **not** add a builder or emit them.
- **Deck framing:** lead with the **proprietary → open-source / reproducible**
  story (ArcPy + Spatial Analyst → rasterio/GDAL/richdem/WhiteboxTools/gdptools,
  pixi + SLURM, CONUS-scale). Connectivity-based classification is presented as one
  key improvement among several, not the sole headline.
- **Showcase maps:** two — Great Basin (endorheic fix) **and** Lower Mississippi
  (over-extension fix) — to show the grounding cuts both ways.

## Deliverable A — pyWatershed dprst requirements gap doc

**File:** `docs/pywatershed_depression_storage_requirements.md`

Ground-truthed against the installed pyWatershed 2.0.4
`PRMSRunoff.get_parameters()` (18 dprst/imperv/carea-relevant input parameters).
The doc is the analysis substrate for the deck's Products slide.

### A1. Requirement table — three buckets

| Bucket | Parameters | Source in our product |
|---|---|---|
| **Spatial — we produce** ✅ | `dprst_frac`, `sro_to_dprst_perv`, `sro_to_dprst_imperv`, `carea_max`, `smidx_coef`, `hru_percent_imperv` | `gfv2/params/merged/nhm_*_params.csv` (rebuilt Jul 5, grounded classifier) |
| **Constant defaults (legacy `0b`)** | `dprst_depth_avg` (132), `dprst_et_coef` (1), `dprst_frac_init` (0.5), `dprst_frac_open` (1), `imperv_stor_max` (0.05), `op_flow_thres` (1), `va_clos_exp` (0.001), `va_open_exp` (0.001) | legacy defaults; confirm against pyWatershed metadata defaults |
| **Gaps — pyWatershed needs, legacy `0b` never emitted** ⚠️ | `dprst_flow_coef`, `dprst_seep_rate_open`, `smidx_exp` | **resolve in the doc** |

Notes to capture:
- Legacy `hru_area` comes from fabric geometry, not the `0b` script; note it as a
  fabric-provided input.
- **Naming shift:** legacy `dprst_seep_rate_close` → pyWatershed
  `dprst_seep_rate_clos`. pyWatershed also splits seep into `_open` and `_clos`.

### A2. Gap resolution

For each of `dprst_flow_coef`, `dprst_seep_rate_open`, `smidx_exp`: physical
meaning, pyWatershed's expected/default value (check pyWatershed parameter
metadata; and the NHM/PRMS paramdb `parameters.xml` if reachable), and a
recommendation (adopt the documented default vs. derive). Method: query
`pywatershed` parameter metadata under `pixi run -e reference` and cite what it
reports; do not assert defaults from memory.

### A3. Verdict

A plain statement of whether `params/merged/` **+ a documented default set** fully
satisfies pyWatershed's dprst inputs, plus the one-line action to close each gap.
No code.

## Deliverable B — the presentation deck

**File:** `docs/presentations/2026-07-depression-storage-workflow.slides.md`
(Marp, ~16–18 slides). No project-name suffix (method/workflow-focused, spans
fabrics), matching the snarea deck's naming rationale.

### B1. The comparison (legacy vs current)

- **Legacy `0b_TB_depr_stor.py` (ArcPy / Spatial Analyst):** proprietary; on-stream
  vs. depression split by a **60 m Euclidean buffer around segment lines**
  (`getSegsBuf`, `EucDistance < 60`); `drains_to_dprst` via ArcPy `Watershed()`
  with **no** on-stream barrier and **no** network gating; TWI thresholds (8.0 /
  15.6) calibrated to VPU 01 only; per-unit manual GDB workflow.
- **Current open-source pipeline:** rasterio/GDAL/richdem/WhiteboxTools/gdptools,
  pixi + SLURM, CONUS-scale, reproducible. On-stream vs. depression split by
  **hydrologic connection to the NHD flowline network** — WBAREACOMI artificial-
  path topology **∪** geometric through-flow, **both gated on Network-Flowline
  membership** (fixes endorheic over-promotion), Playa force-dprst / Ice Mass
  excluded, per-cell impervious carve. `drains_to_dprst` via an open-source **D8
  kernel** with **on-stream waterbodies as traversal barriers** and a **same-HRU**
  restriction on the ratios. Percentile TWI thresholds on open-source
  `twi_hydrodem`.

Headline = proprietary → reproducible open-source; connectivity-based
classification is the marquee *methodological* improvement within that.

### B2. Slide outline (~16–18)

1. Title
2. What depression storage is in PRMS + the 6 spatial params it needs
3. Why it matters — `dprst_frac` / `sro_to_dprst_*` → surface-runoff partition
4. The legacy ArcPy workflow (`0b_TB_depr_stor.py`) at a glance
5. Two weaknesses of the legacy method: geometric 60 m buffer ≠ connectivity;
   endorheic lakes over-promoted on-stream
6. The new principle: ground the on-stream/depression split in the stream network
7. Open-source stack + reproducibility + CONUS scale (pixi/SLURM)
8. Identifying dprst waterbodies — two-source union + Network-Flowline gate
   (Playa force-dprst, Ice Mass excluded)
9. The endorheic fix — **before/after map (Great Basin)**
10. `drains_to_dprst` — D8 kernel + on-stream barrier
11. The over-extension fix — **before/after map (Lower Mississippi)**, cite
    validated Lower Miss 70% → 8.6% land coverage
12. same-HRU restriction on `sro_to_dprst_*` (legacy `Con(rSro == hru)` reproduced
    in raster space)
13. Side-by-side legacy-vs-new decision schematic
14. Products — what pyWatershed consumes (from Deliverable A)
15. Known open refinements — #154/#155/#156/#157, #147 (honest limitations)
16. Summary + references (`0b_TB_depr_stor.py`; NHDPlus V2; the CLAUDE.md
    grounding rules)

### B3. Figures

**Script:** `scripts/render_depstor_figures.py` — headless matplotlib, reads the
on-disk CONUS rasters/CSVs, writes PNGs to `docs/figures/depstor/`. Re-runnable;
fabric paths via the profile (`require_config_key`), not hardcoded. Follows the
snarea deck's `render_snarea_figures.py` pattern.

Figures:
1. **Legacy-vs-new decision schematic** (hand-drawn matplotlib) — 60 m buffer vs.
   network-connectivity split.
2. **Pipeline DAG** — NHD + FDR + TWI + LULC → waterbody → wbody_connectivity →
   dprst → routing → drains_perv/imperv → PRMS params.
3. **Great Basin before/after** — endorheic fix, `depstor_rasters_pre_flowthrough_2026-06-26/`
   vs current `depstor_rasters/` (`dprst_binary` / `onstream_binary`).
4. **Lower Mississippi before/after** — over-extension fix (`drains_to_dprst`).
5. (If cheap) a dprst / on-stream / drains_to_dprst three-panel for one region.

Extent selection uses `vpu_id.tif` + fabric bounds; clip windows, don't load full
CONUS grids (per the CONUS-memory rule in CLAUDE.md).

## Marp tooling (ported)

Marp tooling exists **only in the snarea-presentation worktree**, not on `main`.
This worktree ports the same additive pieces from
`.claude/worktrees/snarea-presentation`:

- `[tool.pixi.feature.marp.*]` blocks + `marp` environment + `marp-setup` /
  `render-deck` tasks in `pyproject.toml` (linux-64 `libgbm`/`alsa-lib` deps +
  `LD_LIBRARY_PATH` activation env for chrome-headless-shell on bare HPC).
- `scripts/render_deck.py` — chrome-resolving wrapper (ported ~verbatim).
- `docs/presentations/README.md` — adapted for this deck.

**Coordination:** these blocks are identical and additive across the two
presentation branches. Whichever presentation PR merges **second** resolves a
trivial duplicate-block conflict (or drops its copy). Call this out in the PR
description.

## File layout (all on this worktree branch)

```
docs/pywatershed_depression_storage_requirements.md            # Deliverable A (gap doc)
docs/presentations/2026-07-depression-storage-workflow.slides.md  # Deliverable B (deck)
docs/presentations/README.md                                   # render guide (ported/adapted)
docs/figures/depstor/*.png                                     # committed figures
scripts/render_deck.py                                         # ported Marp wrapper
scripts/render_depstor_figures.py                              # new figure script
pyproject.toml                                                 # + marp feature/env/tasks
```

## Testing & docs (repo conventions)

- **Tests:** CI is the gate (CLAUDE.md forbids head-node pytest). The new code is a
  plotting/render script + a Marp wrapper — not a pipeline builder — so the
  "builder + test" rule does not strictly apply. Add a light smoke test only if
  `render_depstor_figures.py` grows non-trivial pure helpers worth unit-testing;
  otherwise no test module. Confirm against whether the snarea deck tested its
  render script.
- **Docs check (CLAUDE.md):** add the deck + gap doc to
  `docs/presentations/README.md`; add a one-line pointer from
  `docs/ARCHITECTURE.md` (depstor section) and/or `slurm_batch/HPC_REFERENCE.md`
  to the deck as the narrative overview and to the gap doc as the pyWatershed
  contract. Decide during implementation whether either belongs in the mkdocs nav
  (a raw Marp `.slides.md` renders poorly as a doc page — likely link, don't nav).
- **Atomic commits (CLAUDE.md):** separate commits for (a) Marp tooling port,
  (b) gap doc, (c) figure script + figures, (d) deck + doc pointers.

## Non-goals

- No changes to depstor pipeline code or classifier logic.
- #157/#156 (and #154/#155/#147) are **noted, not fixed**.
- No new builder; do not emit the 3 missing pyWatershed params — document their
  source only.
- No gauge-calibration discussion; params are a priori.

## Open items to resolve during implementation

- Confirm the exact `params/merged/` column names / `id_feature` join key for the
  Products slide and any per-HRU figure.
- Confirm pyWatershed's reported defaults for the 8 constant params and the 3 gap
  params via `pixi run -e reference` metadata queries (cite, don't assume).
- Pick the exact Great Basin and Lower Mississippi clip windows from `vpu_id.tif`.
- Confirm whether the snarea deck committed its rendered `.html`/`.pdf` (match that
  choice for consistency) or only the `.md` + figures.
