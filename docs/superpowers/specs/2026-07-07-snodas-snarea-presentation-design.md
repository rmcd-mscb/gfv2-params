# Design: SNODAS snow-depletion-curve workflow presentation

**Date:** 2026-07-07
**Branch:** `worktree-snarea-presentation`
**Status:** approved design → implementation plan

## Goal

A concise, jargon-light slide deck for USGS modeling colleagues that explains the
SNODAS → `snarea_curve` workflow: what a snow depletion curve is, why PRMS/NHM
needs one, how the Driscoll et al. (2017) and Sexstone et al. (2020) papers shaped
the method, and what the pipeline produces. Figures illustrate the workflow and
methods wherever possible. The deck lives in `docs/` and is built with **Marp**
(markdown → PDF/HTML), matching the workflow already established in the sibling
`nhf-spatial-targets` repo.

**Audience:** USGS modelers familiar with PRMS/NHM but not with this workflow.
Method-focused, not a code deep-dive. Define terms in plain English.

**Non-goals:** No changes to the snarea pipeline code. No gauge-calibration
discussion. Not a maintainer's internals guide.

## Deliverables

1. **The deck** — `docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md`
   (~18 slides). No project-name suffix in the filename: unlike the
   `nhf-spatial-targets` decks (whose embedded figures are one fabric's geometry),
   this deck is method-focused and spans both the Oregon and CONUS/gfv2 fabrics.

2. **Marp tooling ported into this repo** (does not exist here yet; copied/adapted
   from `nhf-spatial-targets`):
   - `[tool.pixi.feature.marp.*]` blocks + a `marp` environment + `render-deck` and
     `marp-setup` tasks in `pyproject.toml`. The source repo defines these in a
     standalone `pixi.toml` (`[feature.marp…]`); convert to this repo's
     `[tool.pixi.*]`-in-`pyproject.toml` layout. Keep the linux-64 `libgbm` +
     `alsa-lib` deps and the `LD_LIBRARY_PATH` activation env (chrome-headless-shell
     needs them on bare HPC).
   - `scripts/render_deck.py` — the chrome-resolving wrapper (resolves the puppeteer
     cache, sets `MARP_USER=root`, adds `--allow-local-files`). Ported ~verbatim.
   - `docs/presentations/README.md` — adapted: what the deck is, the naming
     convention, and the render commands.

3. **Figure-generation script** — `scripts/render_snarea_figures.py`. Headless
   matplotlib. Reads the pipeline's output CSVs/NetCDFs for a given fabric and
   writes PNGs to `docs/figures/snarea/<fabric>/`. Re-runnable: prototyped now
   against the existing Jul-6 outputs, re-run to refresh once the live pipeline
   (jobs 332599–332602) writes the final outputs. Rationale for a script over a
   marimo notebook: matches the existing `scripts/render_figures.py` pattern, runs
   headless on HPC, no browser, deterministic fixed figure set.

4. **Committed figures** — PNGs under `docs/figures/snarea/{oregon,gfv2}/`.

## Source data & the two papers (framing)

- **SNODAS** — NOAA's daily ~1 km modeled snow water equivalent (SWE) grid over
  CONUS. The observational input.
- **Driscoll, Hay & Bock (2017)** (`docs/Snow_Depletion_Curves.md` /
  `docs/Snow Depletion Curves.pdf`) — the **empirical** method: for each HRU and
  melt season, isolate the melt limb (peak SWE → snow-free), normalize, and sample
  snow-covered area (SCA) at fixed normalized-SWE levels to get a depletion curve.
  Shapes **Stage 1 + Stage 2**.
- **Sexstone, Driscoll, Hay, Hammond & Barnhart (2020)** (`docs/hyp.13735.pdf`) —
  the **lognormal / coefficient-of-variation (CV)** basis: a curve's shape is set
  by the sub-grid variability of SWE, so a small **library** of curves indexed by
  CV can represent every HRU. Shapes **Stage 3**.

## Plain-English anchor

A **snow depletion curve** tells the model, as an HRU's snowpack melts, how much of
the HRU is still snow-covered. Snow-covered area controls how much sunlight the
surface reflects vs. absorbs, which controls melt rate and therefore snowmelt-driven
streamflow timing. Two HRUs with the same average SWE can melt out very differently
depending on how *evenly* the snow is distributed — that is exactly what the CV
captures.

## Deck outline (~18 slides)

1. Title
2. **What a snow depletion curve is** — plain English + concept schematic
   (fraction snow-covered vs. fraction of peak SWE remaining)
3. **Why PRMS needs it** — SCA → albedo/energy → melt timing → streamflow
4. **The two papers** — Driscoll 2017 (empirical from SNODAS) + Sexstone 2020
   (lognormal/CV library); one line each
5. **The data** — SNODAS daily SWE, what it is
6. **Pipeline at a glance** — DAG schematic (SNODAS → Stage 1 → Stage 2 → Stage 3
   → PRMS params)
7. **Stage 1 — aggregate SNODAS to HRUs** — figure: one HRU's daily SWE +
   snow-covered fraction over a water year
8. **Stage 2 — extract melt-season curves (Driscoll)** — figure: peak→bare melt
   limb, post-peak-snowfall reversal removal
9. **Stage 2 — representative curve across years** — figure: multi-year curves +
   median; inter-annual similarity
10. **Stage 2 — when do we trust a derived curve?** — the selection criteria in
    plain English (small table)
11. **Stage 3 — the CV idea (Sexstone)** — sub-grid SWE variability sets curve
    shape; figure: curve family colored by CV
12. **Stage 3 — lognormal library + calibration** — figure: empirical vs.
    lognormal-fit overlay
13. **Stage 3 — from many curves to ~9** — the parsimony win; each HRU indexed by
    `hru_deplcrv`
14. **Results — Oregon (validation fabric)** — figure
15. **Results — CONUS / gfv2 (production)** — figure: `hru_deplcrv` map
16. **Products — what pyWatershed consumes** — see below
17. **How it runs** — one `submit_snarea_pipeline.sh` command → 4 chained SLURM
    jobs (kept light, 1 slide)
18. **Summary + outputs + references**

## Products slide (slide 16) — framed for pyWatershed

pyWatershed's snow module (`pywatershed.hydrology.prms_snow.PRMSSnow`) consumes
three parameters, all written to `nhm_snarea_curve.nc`:

- **`snarea_curve`** — the curve *library*: 2D `(ndepl × 11)`, i.e. the ~9 curves
  (8 equal-population CV bins + 1 reserved default). Written flat in ascending
  `deplcrv_id` order (`ndeplval = 11 × ndepl`).
- **`hru_deplcrv`** — per-HRU integer index (1-based) selecting which library curve
  the HRU uses.
- **`snarea_thresh`** — per-HRU SWE (inches) above which the HRU is 100%
  snow-covered (derived from representative peak SWE via
  `snarea_thresh_inches`).

**Teaching point:** the **per-HRU empirical SDC** (Stage 2 `_derived.csv`) is a
*diagnostic/intermediate* product — pyWatershed does **not** take a unique curve per
HRU. Stage 3 compresses the per-HRU empirical curves into the shared library +
index. That compression *is* the Sexstone contribution. Intermediate/diagnostic
products (`_derived.csv`, `_library.csv`, `_validation.csv`) support QA but are not
model inputs.

## Figure set (from real outputs)

Generated by `render_snarea_figures.py` for both fabrics unless noted:

1. **SWE + SCA time series** — one representative HRU, one water year (Stage 1
   signal: accumulation → peak → melt → bare). Source: `snodas/snodas_agg_<yr>.nc`.
2. **Melt-season extraction** — peak marked, melt limb highlighted, snowfall
   reversal removed (Driscoll). Source: Stage 1 NetCDF, re-running the `season.py`
   extraction for the illustrated HRU.
3. **Multi-year + median representative curve** — several years' curves + the
   elementwise median; annotate similarity. Source: same.
4. **Derived-vs-default coverage** — how many HRUs got a real derived curve vs. the
   default, by `sdc_status`. Source: `_derived.csv` / `_params.csv`.
5. **CV curve family** — the ~9 library curves colored by CV (higher CV = more
   gradual depletion). Source: `_library.csv`.
6. **Empirical-vs-lognormal validation overlay** — sample HRUs, empirical curve vs.
   fitted lognormal curve. Source: `_validation.csv` / `_library.csv`.
7. **CONUS `hru_deplcrv` map** (gfv2 only) — spatial pattern of assigned curve
   index; needs the fabric HRU geometry (gpkg) joined to `_params.csv`.

Plus **2 hand-generated schematics** (also produced by the script, matplotlib):

- **Concept diagram** (slide 2) — a labeled generic depletion curve.
- **Pipeline DAG** (slide 6) — SNODAS → Stage 1 → Stage 2 → Stage 3 → PRMS params.

**Timing:** the live re-run (jobs 332599–332602) regenerates outputs with the new
`swe_std` sidecar and the current `library.py` schema (which the Jul-6 on-disk CSVs
predate — they lack `snarea_thresh`/CV columns). Develop the figure script against
the existing outputs; refresh the committed PNGs once the final outputs land.

## File layout (all on the worktree branch)

```
docs/presentations/2026-07-snodas-snow-depletion-curves.slides.md   # the deck
docs/presentations/README.md                                        # render guide (ported)
docs/figures/snarea/oregon/*.png                                    # committed figures
docs/figures/snarea/gfv2/*.png
scripts/render_deck.py                                              # ported wrapper
scripts/render_snarea_figures.py                                   # new figure script
pyproject.toml                                                      # + marp feature/env/tasks
```

## Testing & docs (repo conventions)

- **Baseline/tests:** CI is the gate (CLAUDE.md forbids head-node pytest). The new
  code is a plotting/render script and a Marp wrapper — not a pipeline builder — so
  the "builder + test" rule does not strictly apply. Add a light smoke test only if
  `render_snarea_figures.py` grows non-trivial pure helpers worth unit-testing
  (e.g. a curve-family layout helper); otherwise no test module. Confirm during
  implementation by matching whether `nhf-spatial-targets` tests its render script.
- **Docs check (CLAUDE.md):** add the deck to `docs/presentations/README.md`; add a
  one-line pointer from `slurm_batch/RUNME.md` Step 8 and/or
  `docs/ARCHITECTURE.md`'s Part 2c section to the deck as the narrative overview.
  Decide during implementation whether the deck belongs in the mkdocs nav (a raw
  Marp `.slides.md` renders poorly as a normal doc page — likely leave it out of
  nav and only link it).
- **Atomic commits (CLAUDE.md):** separate commits for (a) Marp tooling port,
  (b) figure script + figures, (c) the deck + doc pointers.

## Open items to resolve during implementation

- Confirm the exact `_library.csv` / `_validation.csv` column names against the
  fresh Stage 3 output (the Jul-6 `_params.csv` is the old schema).
- Pick the representative HRU(s) for the illustrative figures (a clean
  seasonal-melt HRU for Oregon; something with clear inter-annual spread).
- Confirm the fabric HRU gpkg path for the CONUS `hru_deplcrv` map via the fabric
  profile (`require_config_key`), not a hardcoded path.
