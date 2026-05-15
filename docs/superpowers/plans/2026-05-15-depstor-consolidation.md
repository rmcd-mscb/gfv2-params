# Plan: Consolidate depstor generation + aggregation pipelines

## Context

The depression-storage (depstor) pipeline currently sprawls across **40+ small files**
in `configs/` and `slurm_batch/`:

- **Generation half**: 10 `configs/depstor_*_raster.yml` + 10 `build_depstor_*.batch` —
  each driving a single-purpose `scripts/build_depstor_*.py`. The configs are mostly
  output-path templates with a few unique knobs (threshold, vector layer, NLCD source).
  Build ordering (landmask → imperv/streambuffer/waterbody → dprst → perv/routing →
  drains_*/carea_map) is documented in `slurm_batch/RUNME.md` but enforced only by
  file dependencies — there is no orchestrator.

- **Aggregation half**: 9 `configs/*_frac_param.yml` + 9 `create_*_frac_params.batch`
  all driving the same `scripts/create_zonal_params.py` (gdptools exactextract per
  HRU batch), plus `configs/depstor_ratio_params.yml` driving
  `scripts/derive_depstor_ratios.py` (which is wedged into
  `slurm_batch/merge_output_params.batch` rather than having its own batch).

The result: a contributor has to know which of 20+ batch files to sbatch in what
order, each frac config is ~10 lines of mostly-identical YAML, and the ratio
derivation is hidden inside a multi-purpose merge batch. Every new fraction or
raster requires touching at least 4 files.

**Goal**: collapse the depstor pipeline to **2 configs + 2 orchestrators**, delete
the per-step configs/batches, and update `RUNME.md`. The work happens on a sibling
worktree (`../gfv2-params-depstor-consolidate`, branch `feat/depstor-consolidate`)
to avoid disturbing another active claude session in the primary checkout.

Builder script *logic* is unchanged — only their CLI entry points are retired in
favour of being imported by the new orchestrators. Tests on the existing pure
functions (`compute_perv_binary`, `compute_ratio`, …) continue to apply.

---

## Worktree setup

After plan approval, run (these are NOT part of the plan-mode allowed actions):

```bash
git worktree add ../gfv2-params-depstor-consolidate -b feat/depstor-consolidate main
cd ../gfv2-params-depstor-consolidate
bash scripts/refresh_pixi_activation.sh   # re-bake activation in the worktree
```

All edits below happen in `../gfv2-params-depstor-consolidate/`.

---

## Part A — Generation pipeline

### A1. New unified config: `configs/depstor_rasters.yml`

Single file describing all 10 outputs as a list of steps. Shared keys
(`output_dir`, `landmask_raster`, `template_raster`) sit at the top level;
per-step knobs go in each step block. Fabric-templated paths still resolve via
`load_config()` against `base_config.yml`.

```yaml
# Build the full depression-storage raster stack in dependency order.
# Replaces the 10 configs/depstor_*_raster.yml files.

output_dir: "{data_root}/{fabric}/depstor_rasters"

steps:
  - name: landmask                              # rasterize HRU fabric → land_mask.tif
    output: land_mask.tif

  - name: imperv                                # NLCD fractional imperv → uint8 binary
    nlcd_source: "{data_root}/input/mrlc_impervious/<NLCD>.tif"
    threshold: 50
    output: imperv_binary.tif

  - name: streambuffer                          # buffer nsegment lines
    buffer_distance: 60
    output: stream_buffer.tif

  - name: waterbody                             # rasterize wbody polygons + scipy label
    min_area_threshold: 30000
    outputs:
      binary: wbody_binary.tif
      regions: wbody_regions.tif

  - name: dprst                                 # combine regions + buffer + imperv
    outputs:
      dprst: dprst_binary.tif
      onstream: onstream_binary.tif

  - name: perv                                  # land ∧ ¬imperv ∧ ¬dprst
    output: perv_binary.tif

  - name: routing                               # WBT watershed from dprst on FDR
    output: drains_to_dprst.tif

  - name: drains_perv                           # drains ∩ perv
    output: drains_perv_binary.tif

  - name: drains_imperv                         # drains ∩ imperv
    output: drains_imperv_binary.tif

  - name: carea_map                             # TWI thresholds + onstream + perv
    thresholds:
      carea_max: 8.0
      smidx:     15.6
    outputs:
      carea_max: carea_map_t8_binary.tif
      smidx:     carea_map_t156_binary.tif
```

### A2. Builder modules refactor

Each existing `scripts/build_depstor_<name>.py` is restructured so its core work
is a callable function with a stable signature:

```python
def run(step_cfg: dict, ctx: BuildContext) -> None
```

where `ctx` carries the resolved `output_dir`, `landmask_raster`, `template_raster`,
and the fabric profile (`twi_raster`, `fdr_raster`, `hru_gpkg`, `segments_gpkg`,
`waterbody_gpkg`). The existing pure compute helpers in
[src/gfv2_params/depstor.py](src/gfv2_params/depstor.py) stay untouched —
they're already the right granularity for reuse.

Two options for where to put the refactored builder modules:

- **Preferred**: move them into `src/gfv2_params/depstor_builders/<name>.py` (proper
  importable package). `scripts/build_depstor_*.py` files are deleted.
- **Fallback**: keep them under `scripts/` but make them library-style (no `main()`,
  no argparse). The orchestrator imports them by path.

The Preferred path is the right one because builders become first-class library
code and `scripts/` keeps its meaning ("executable entry points").

### A3. New orchestrator: `scripts/build_depstor_rasters.py`

Single entry point that:

1. Parses CLI: `--config configs/depstor_rasters.yml --base_config configs/base_config.yml --fabric <name> [--step <name>] [--from <name>] [--force]`.
2. Loads the unified config + fabric profile via `load_config()`.
3. Builds a step DAG (hard-coded ordering — the steps are stable) and runs in
   topological order. `--step X` runs only step X; `--from X` resumes from X.
4. Each step's pre-check verifies its inputs exist and refuses to start until
   they do — fail fast with a clear "missing X.tif; run step Y first" message.
5. Calls into `gfv2_params.depstor_builders.<name>.run(step_cfg, ctx)`.

**Why a single sbatch and not array+afterok**: the build DAG is mostly serial
(landmask blocks everything; dprst blocks perv/routing; routing blocks the
drains_*). The few parallel branches (imperv/streambuffer/waterbody after
landmask) are short relative to the long pole (routing — WhiteboxTools
watershed), and the gain from parallelising them is dwarfed by the cost of
managing 10 chained slurm dependencies. One sbatch keeps it simple.

### A4. New batch file: `slurm_batch/build_depstor_rasters.batch`

Single sbatch — sources `.pixi-activate.sh`, runs
`python scripts/build_depstor_rasters.py --config configs/depstor_rasters.yml
--base_config configs/base_config.yml --fabric "$FABRIC"`. Resources sized for
the long pole (routing); roughly `--mem=64G --time=8:00:00` for CONUS gfv2 and
`--mem=16G --time=2:00:00` for `gfv2_vpu01`.

### A5. Deletions (Part A)

```
configs/depstor_landmask_raster.yml
configs/depstor_imperv_raster.yml
configs/depstor_streambuffer_raster.yml
configs/depstor_waterbody_raster.yml
configs/depstor_dprst_raster.yml
configs/depstor_perv_raster.yml
configs/depstor_routing_raster.yml
configs/depstor_drains_perv_raster.yml
configs/depstor_drains_imperv_raster.yml
configs/depstor_carea_map_raster.yml
slurm_batch/build_depstor_landmask.batch
slurm_batch/build_depstor_imperv.batch
slurm_batch/build_depstor_streambuffer.batch
slurm_batch/build_depstor_waterbody.batch
slurm_batch/build_depstor_dprst.batch
slurm_batch/build_depstor_perv.batch
slurm_batch/build_depstor_routing.batch
slurm_batch/build_depstor_drains_perv.batch
slurm_batch/build_depstor_drains_imperv.batch
slurm_batch/build_depstor_carea_map.batch
scripts/build_depstor_landmask.py
scripts/build_depstor_imperv.py
scripts/build_depstor_streambuffer.py
scripts/build_depstor_waterbody.py
scripts/build_depstor_dprst.py
scripts/build_depstor_perv.py
scripts/build_depstor_routing.py
scripts/build_depstor_intersect.py     # (drove drains_perv + drains_imperv)
scripts/build_depstor_carea_map.py
```

---

## Part B — Aggregation pipeline

### B1. New unified config: `configs/depstor_params.yml`

Single file with shared `defaults`, a `fractions` array, and a `ratios` array.
Replaces 9 `*_frac_param.yml` files + `depstor_ratio_params.yml`.

```yaml
# Drive all depstor zonal stats + ratio derivations in one config.
# Replaces 9 *_frac_param.yml + depstor_ratio_params.yml.

defaults:
  batch_dir:   "{data_root}/{fabric}/batches"
  target_layer: nhru
  id_feature:   nat_hru_id
  output_dir:   "{data_root}/{fabric}/params"
  merged_dir:   "{data_root}/{fabric}/params/merged"
  categorical:  false
  count_column: count

fractions:
  - name: perv_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/perv_binary.tif"
    merged_file:   nhm_perv_frac_params.csv
  - name: imperv_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/imperv_binary.tif"
    merged_file:   nhm_imperv_frac_params.csv
  - name: dprst_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/dprst_binary.tif"
    merged_file:   nhm_dprst_frac_params.csv
  - name: drains_perv_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/drains_perv_binary.tif"
    merged_file:   nhm_drains_perv_frac_params.csv
  - name: drains_imperv_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/drains_imperv_binary.tif"
    merged_file:   nhm_drains_imperv_frac_params.csv
  - name: onstream_storage_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/onstream_binary.tif"
    merged_file:   nhm_onstream_storage_frac_params.csv
  - name: drains_to_dprst_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/drains_to_dprst.tif"
    merged_file:   nhm_drains_to_dprst_frac_params.csv
  - name: carea_t8_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/carea_map_t8_binary.tif"
    merged_file:   nhm_carea_t8_frac_params.csv
  - name: carea_t156_frac
    source_raster: "{data_root}/{fabric}/depstor_rasters/carea_map_t156_binary.tif"
    merged_file:   nhm_carea_t156_frac_params.csv

ratios:
  - name: sro_to_dprst_perv
    numerator:   drains_perv_frac
    denominator: perv_frac
    clamp_to_one: false
    output_file: nhm_sro_to_dprst_perv_params.csv
  - name: sro_to_dprst_imperv
    numerator:   drains_imperv_frac
    denominator: imperv_frac
    clamp_to_one: false
    output_file: nhm_sro_to_dprst_imperv_params.csv
  - name: carea_max
    numerator:   carea_t8_frac
    denominator: perv_frac
    clamp_to_one: true
    output_file: nhm_carea_max_params.csv
  - name: smidx_coef
    numerator:   carea_t156_frac
    denominator: perv_frac
    clamp_to_one: true
    output_file: nhm_smidx_coef_params.csv
```

The `ratios[].numerator/denominator` reference fraction `name`s (not filenames),
so the orchestrator resolves to the corresponding `merged_file` paths. This
removes the duplication where each ratio currently hard-codes `nhm_..._params.csv`
paths that must match the upstream config's `merged_file` exactly.

### B2. New orchestrator script: `scripts/derive_depstor_params.py`

One script, three modes — every existing aggregation entry point folds in:

```
--mode zonal --fraction <name> --batch_id <N>      # array task: one fraction, one batch
--mode merge --fraction <name>                     # chained merge: combine per-batch CSVs
--mode ratios                                      # final ratio derivation from merged CSVs
```

Internals:

- **zonal mode**: imports the gdptools `UserTiffData` + `ZonalGen` flow currently
  in [scripts/create_zonal_params.py](scripts/create_zonal_params.py) (lines
  43-116). Reads the named fraction spec from the unified config. Otherwise
  identical behaviour (same `categorical: false`, same `source_var`, same
  `file_prefix` convention so downstream merges work unchanged).
- **merge mode**: thin wrapper around the existing `scripts/merge_params.py`,
  passing the right per-fraction config-equivalent kwargs.
- **ratios mode**: reuses `compute_ratio()` from
  [scripts/derive_depstor_ratios.py:31-73](scripts/derive_depstor_ratios.py#L31-L73)
  verbatim. The function moves to `src/gfv2_params/depstor_ratios.py` as a
  library helper so the existing
  [tests/test_derive_depstor_ratios.py](tests/test_derive_depstor_ratios.py)
  keeps passing with a one-line import path update.

### B3. New submit script: `slurm_batch/submit_depstor_params.sh`

Single entry point that chains everything via afterok. Functions like
`submit_jobs.sh` does today but loops over the fractions and adds a final
ratios job.

```bash
# Usage: slurm_batch/submit_depstor_params.sh <batches_dir> [fabric] [base_config] [max_concurrent]
#
# For each of the 9 fractions:
#   sbatch --array=0-N%K            create_depstor_zonal.batch   --export FRACTION=<name>
#   sbatch --dependency=afterok:..  merge_params.batch           --export MERGE_FRACTION=<name>
# Then one final ratios job depending on afterok of all 9 merges:
#   sbatch --dependency=afterok:..  derive_depstor_ratios.batch
```

Two new sbatch helpers paired with it:
- `slurm_batch/create_depstor_zonal.batch` — array task; reads `FRACTION` env var,
  calls `derive_depstor_params.py --mode zonal --fraction $FRACTION --batch_id $SLURM_ARRAY_TASK_ID`.
- `slurm_batch/derive_depstor_ratios.batch` — single task; calls
  `derive_depstor_params.py --mode ratios`. (Replaces the currently-wedged-in
  ratios call inside `merge_output_params.batch` — pull that out of
  `merge_output_params.batch` in the same PR.)

The merge step reuses the existing `slurm_batch/merge_params.batch`; the
orchestrator just passes the appropriate `MERGE_CONFIG`-equivalent env vars
that `derive_depstor_params.py --mode merge` understands.

### B4. Deletions (Part B)

```
configs/perv_frac_param.yml
configs/imperv_frac_param.yml
configs/dprst_frac_param.yml
configs/drains_perv_frac_param.yml
configs/drains_imperv_frac_param.yml
configs/onstream_storage_frac_param.yml
configs/drains_to_dprst_frac_param.yml
configs/carea_t8_frac_param.yml
configs/carea_t156_frac_param.yml
configs/depstor_ratio_params.yml
slurm_batch/create_perv_frac_params.batch
slurm_batch/create_imperv_frac_params.batch
slurm_batch/create_dprst_frac_params.batch
slurm_batch/create_drains_perv_frac_params.batch
slurm_batch/create_drains_imperv_frac_params.batch
slurm_batch/create_onstream_storage_frac_params.batch
slurm_batch/create_drains_to_dprst_frac_params.batch
slurm_batch/create_carea_t8_frac_params.batch
slurm_batch/create_carea_t156_frac_params.batch
scripts/derive_depstor_ratios.py        # logic moves to src/gfv2_params/depstor_ratios.py
```

`scripts/create_zonal_params.py` stays — it is still used by elev/slope/aspect/soils
zonal jobs. Only the depstor-flavoured driver moves.

---

## Part C — Cross-cutting updates

### C1. `slurm_batch/RUNME.md`

Replace Stage 2d (lines 267-313) and the depstor portions of Stage 4 (lines
364-379) with:

```markdown
### Stage 2d: Build depstor rasters (per fabric)

sbatch slurm_batch/build_depstor_rasters.batch

# Or for VPU01 validation:
FABRIC=gfv2_vpu01 sbatch --time=02:00:00 --mem=16G \
    slurm_batch/build_depstor_rasters.batch

### Stage 4 (depstor portion): Aggregate to PRMS params

BATCHES={data_root}/gfv2/batches
slurm_batch/submit_depstor_params.sh $BATCHES

# This single call:
#   - submits 9 zonal-stats array jobs
#   - chains 9 merge jobs (afterok)
#   - chains 1 ratio derivation job (afterok of all 9 merges)
```

Also update the "Script → Config → Entry Point" mapping table at the bottom of
RUNME.md to replace the 20+ depstor rows with the 2 new ones.

### C2. `merge_output_params.batch`

Remove the wedged `derive_depstor_ratios.py` call (line 530 mapping entry). The
ratios are now their own dedicated sbatch and are no longer triggered as a
side-effect of the "merge everything" job.

### C3. Tests

- [tests/test_build_depstor_landmask.py](tests/test_build_depstor_landmask.py) —
  update imports to pull `rasterize_land_mask` (or equivalent) from
  `gfv2_params.depstor_builders.landmask`. Behaviour unchanged.
- [tests/test_build_depstor_perv.py](tests/test_build_depstor_perv.py) — update
  import of `compute_perv_binary` to `gfv2_params.depstor_builders.perv`.
- [tests/test_derive_depstor_ratios.py](tests/test_derive_depstor_ratios.py) —
  update import of `compute_ratio` to `gfv2_params.depstor_ratios`.

No new tests required — coverage was already on the pure functions and the
function bodies don't change.

### C4. `README.md`

Update lines 100-124 (the depstor section of "project structure"): replace the
10-script listing with the 2 orchestrator scripts + reference to
`src/gfv2_params/depstor_builders/`. Verify nothing else in README.md cites
deleted config or batch filenames.

### C5. Archive this plan in-repo

Save a copy of this planning document to
`docs/superpowers/plans/2026-05-15-depstor-consolidation.md` so it lives
alongside prior architectural plans (e.g.
`docs/superpowers/plans/2026-03-23-repo-restructure.md`,
`2026-04-11-border-dem-fix.md`). Convention there is `YYYY-MM-DD-<slug>.md`.
This commit goes on `feat/depstor-consolidate` alongside the implementation
so the PR carries its own design record.

---

## Critical files

**New (created):**
- [configs/depstor_rasters.yml](configs/depstor_rasters.yml)
- [configs/depstor_params.yml](configs/depstor_params.yml)
- [scripts/build_depstor_rasters.py](scripts/build_depstor_rasters.py)
- [scripts/derive_depstor_params.py](scripts/derive_depstor_params.py)
- [slurm_batch/build_depstor_rasters.batch](slurm_batch/build_depstor_rasters.batch)
- [slurm_batch/submit_depstor_params.sh](slurm_batch/submit_depstor_params.sh)
- [slurm_batch/create_depstor_zonal.batch](slurm_batch/create_depstor_zonal.batch)
- [slurm_batch/derive_depstor_ratios.batch](slurm_batch/derive_depstor_ratios.batch)
- [src/gfv2_params/depstor_builders/](src/gfv2_params/depstor_builders/) — package
  with one module per build step (landmask, imperv, streambuffer, waterbody,
  dprst, perv, routing, intersect, carea_map)
- `src/gfv2_params/depstor_ratios.py` — extracted from old derive script

**Modified:**
- [slurm_batch/RUNME.md](slurm_batch/RUNME.md) — Stage 2d, Stage 4 depstor portion, mapping table
- [slurm_batch/merge_output_params.batch](slurm_batch/merge_output_params.batch) — remove ratio call
- [README.md](README.md) — project-structure depstor section
- [tests/test_build_depstor_landmask.py](tests/test_build_depstor_landmask.py)
- [tests/test_build_depstor_perv.py](tests/test_build_depstor_perv.py)
- [tests/test_derive_depstor_ratios.py](tests/test_derive_depstor_ratios.py)

**Archived (new):**
- [docs/superpowers/plans/2026-05-15-depstor-consolidation.md](docs/superpowers/plans/2026-05-15-depstor-consolidation.md) — copy of this plan

**Reused as-is (no edits expected):**
- [src/gfv2_params/depstor.py](src/gfv2_params/depstor.py) — shared raster helpers
- [src/gfv2_params/config.py](src/gfv2_params/config.py) — `load_config()` continues to handle templating
- [scripts/create_zonal_params.py](scripts/create_zonal_params.py) — non-depstor zonal still needs it

**Deleted**: 20 generation files + 10 aggregation files = **30 files removed**.

---

## Verification

VPU01 is the standard small-scale validation target. Procedure:

1. **Dry-run the orchestrator**: `python scripts/build_depstor_rasters.py
   --config configs/depstor_rasters.yml --base_config configs/base_config.yml
   --fabric gfv2_vpu01 --step landmask --force`. Confirm `land_mask.tif`
   produced and byte-identical to the pre-refactor output.
2. **Full generation rebuild on VPU01**:
   `FABRIC=gfv2_vpu01 sbatch --time=02:00:00 --mem=16G slurm_batch/build_depstor_rasters.batch`.
   Verify all 10 outputs present under `{data_root}/gfv2_vpu01/depstor_rasters/`
   and md5-compare against the existing pre-refactor outputs (they should match
   exactly — same logic, same inputs).
3. **Full aggregation on VPU01**:
   `slurm_batch/submit_depstor_params.sh {data_root}/gfv2_vpu01/batches gfv2_vpu01`.
   Verify the 9 merged frac CSVs + 4 ratio CSVs land in
   `{data_root}/gfv2_vpu01/params/merged/`. Diff against the pre-refactor CSVs
   — should be exact.
4. **QAQC notebook**: open
   [notebooks/qaqc_depstor_vpu01.ipynb](notebooks/qaqc_depstor_vpu01.ipynb) in
   the notebooks env and re-run; choropleths should match, and the
   `imperv_frac + dprst_frac + perv_frac ≈ 1` invariant should still hold.
5. **GitHub Actions test gate**: push the branch and let CI run pytest. Per
   project memory, local pytest is off-limits on the HPC head node.
6. **Single-step recovery**: kill a fresh build mid-routing, then
   `python scripts/build_depstor_rasters.py ... --from routing`. Confirm it
   resumes correctly without redoing landmask/imperv/etc.

If steps 2 and 3 produce byte-identical (or for ratios, value-identical within
float tolerance) outputs to the pre-refactor pipeline, the consolidation is
behaviour-preserving and the PR is ready for review.
