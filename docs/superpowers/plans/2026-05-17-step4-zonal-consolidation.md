# Plan: Step 4 — Zonal-pass parameter consolidation

## Context

Steps 1+2 (PR #74) and Step 3 (PR #75) consolidated the **fabric-independent
raster prep** (Part 1) behind a single orchestrator (`build_shared_rasters.py`)
over a unified config (`shared_rasters.yml`) and moved the on-disk store to
`shared/`. The remaining un-consolidated surface is **Part 2: per-fabric
zonal-pass parameter generation** — 6 Python scripts driven by 10 SLURM batches
via `slurm_batch/submit_jobs.sh`, each requiring a separate manual invocation
per param type. Today's flow forces operators to call `submit_jobs.sh` ten
times in sequence, manually picking the right config + batch per param.

Step 4 mirrors the **depstor-params** pattern from PR #72
(`derive_depstor_params.py` + `submit_depstor_params.sh`): one orchestrator
script with `--mode` dispatch, one unified config listing every param type,
one shell wrapper that loops the list and submits per-param array+merge jobs.
After Step 4, generating every Part 2 parameter for a fabric is **one shell
invocation**.

ssflux + `build_weights` are included via a `depends_on:` entry in the config
— the wrapper special-cases it (submits build_weights as a separate job +
chains the ssflux array afterok). Single unified surface for all Part 2 work.

Issue #82 (configs/ subdirectory reorg) stays deferred until after Step 4
lands, so the comprehensive reorg can target the settled inventory.

---

## Approach

Mirror the depstor-params shape from PR #72 (see `scripts/derive_depstor_params.py`
+ `slurm_batch/submit_depstor_params.sh` for the proven pattern).

### A. New unified config: `configs/zonal_params.yml`

Schema lifted from `configs/depstor_params.yml` structure. Top-level
`defaults:` block carries the shared per-fabric output conventions. The
`params:` list contains one entry per parameter type (10 entries today: elev,
slope, aspect, soils, soilmoistmax, lulc_nhm_v11, lulc_nalcms, lulc_nlcd,
lulc_foresce, ssflux).

```yaml
defaults:
  batch_dir: "{data_root}/{fabric}/batches"
  target_layer: nhru
  id_feature: nat_hru_id
  output_dir: "{data_root}/{fabric}/params"
  merged_subdir: merged
  weight_dir: "{data_root}/shared/conus/weights"

params:
  - name: elev
    script: zonal              # dispatch tag → create_zonal_params logic
    source_raster: "{data_root}/shared/conus/vrt/elevation.vrt"
    categorical: false
    merged_file: nhm_elev_{fabric}.csv

  - name: slope
    script: zonal
    source_raster: "{data_root}/shared/conus/vrt/slope.vrt"
    categorical: false
    merged_file: nhm_slope_{fabric}.csv

  # ... aspect, soils, soilmoistmax, 4× lulc, ssflux

  - name: ssflux
    script: ssflux
    depends_on: build_weights   # submit script special-cases this key
    source_shapefile: "{data_root}/input/soils_litho/Lithology_exp_Konly_Project.shp"
    merged_slope_file: "{data_root}/{fabric}/params/merged/nhm_slope_{fabric}.csv"
    k_perm_min: <existing value>
    flux_params: [<existing list>]
    merged_file: nhm_ssflux_{fabric}.csv
```

The existing per-source LULC configs (`configs/lulc_*_param.yml`) stay because
`configs/shared_rasters.yml` also references them as `sources:` for
`build_lulc_rasters`. Step 4 just doesn't depend on them for the zonal pass —
the relevant LULC params live inline in `zonal_params.yml` so all Part 2
state is in one file.

### B. New orchestrator: `scripts/derive_zonal_params.py`

Three-mode dispatcher, matching `scripts/derive_depstor_params.py`:

- `--mode zonal --param <name> --batch_id <N>` — single-batch run for one
  param (this is what each SLURM array task invokes)
- `--mode merge --param <name>` — concat per-batch CSVs for one param
- `--mode build_weights` — CONUS-once weight matrix for ssflux (no `--param`
  needed)

CLI mirrors depstor: `--config configs/zonal_params.yml --base_config
configs/base_config.yml --fabric <name>`.

Internally dispatches on `script:` (zonal / soils / lulc / ssflux) by calling
into the same library helpers the existing per-script CLIs use — no behaviour
change. Each existing script's `main()` body is extracted into a callable
function the orchestrator imports.

### C. New SLURM batches

| Batch | Mirrors | What |
|---|---|---|
| `slurm_batch/derive_zonal_params.batch` | `create_depstor_zonal.batch` | Generic per-batch worker. Reads `$PARAM` + `$SLURM_ARRAY_TASK_ID`. |
| `slurm_batch/merge_zonal_param.batch` | `merge_depstor_fraction.batch` | Generic single-job merge. Reads `$PARAM`. |
| `slurm_batch/build_zonal_weights.batch` | today's `build_weights.batch` resources (8h, 96G) | CONUS-once weight matrix for ssflux. |

### D. Dispatch script: `slurm_batch/submit_zonal_params.sh`

Mirrors `submit_depstor_params.sh`. For each entry in `params:`:

1. If `depends_on: build_weights`, submit `build_zonal_weights.batch` first
   (record JOBID)
2. Submit `derive_zonal_params.batch` as array job (`--array=0-N%K`,
   `--export=PARAM=<name>,FABRIC,BASE_CONFIG`); if there's a prereq from
   step 1, add `--dependency=afterok:<weights_jobid>`
3. Submit `merge_zonal_param.batch` with `--dependency=afterok:<array_jobid>`
   and the same env exports
4. Append the merge job ID to a `MERGE_JOB_IDS[]` array

No final consolidating step — Part 2 doesn't have an analog to depstor's
`derive_ratios`.

### E. Library code refactor (minimal)

Most existing scripts already call into well-factored helpers
(`gfv2_params.config`, `gfv2_params.lulc`, etc.). The minimum work for
orchestrator dispatch is:

Extract each existing `main()` body into a callable function:
- `run_zonal_batch(param_cfg, ...)` — used by `create_zonal_params.py`
- `run_soils_batch(param_cfg, ...)` — used by `create_soils_params.py`
- `run_lulc_batch(param_cfg, ...)` — used by `create_lulc_params.py`
- `run_ssflux_batch(param_cfg, ...)` — used by `create_ssflux_params.py`
- `run_build_weights(param_cfg, ...)` — used by `build_weights.py`
- `run_merge(param_cfg, ...)` — used by `merge_params.py`

The existing CLI shells thin to: parse args → load config → call the function.
The orchestrator calls the same functions for the matching `script:` tag.

Following the depstor-params precedent (which kept depstor-params logic in
`derive_depstor_params.py` itself rather than under
`src/gfv2_params/depstor_builders/`), the new functions live as importable
symbols in the existing `scripts/` modules. No new `zonal_builders/`
sub-package.

### F. Backward-compat surface (preserved)

Every existing per-param Python script (`scripts/create_zonal_params.py`,
`create_soils_params.py`, `create_lulc_params.py`, `create_ssflux_params.py`,
`build_weights.py`, `merge_params.py`) is preserved as a thin CLI shell over
the same library functions. Every existing per-param batch
(`slurm_batch/create_zonal_*.batch`, `create_soils_*.batch`,
`create_lulc_*.batch`, `create_ssflux_params.batch`, `build_weights.batch`,
`merge_*.batch`) keeps working unchanged. Matches the Step 3 backward-compat
philosophy.

### G. Documentation updates

- **README.md** — add a "Zonal-pass parameter pipeline" section paralleling
  the existing "Shared rasters pipeline" + "Depression-storage pipeline"
  sections. Mentions the new `submit_zonal_params.sh` as the canonical Part 2
  entry point.
- **slurm_batch/RUNME.md** — Stage 4 retargeted: "Recommended: run Part 2 via
  the unified zonal-params dispatcher" subsection at the top of Stage 4,
  pointing at `submit_zonal_params.sh`. The 10 individual batches remain
  documented as fallbacks for per-step debugging.
- **slurm_batch/RUNME.md script-mapping table** — list
  `derive_zonal_params.batch`, `merge_zonal_param.batch`,
  `build_zonal_weights.batch`, and `submit_zonal_params.sh` in the
  orchestrator-batches block.

### H. Outliers to address

- `slurm_batch/create_lulc_nalcms_params.batch` has a hardcoded
  `--array=0-63` instead of using `submit_jobs.sh`. Step 4 normalises this
  (the new dispatch sets `--array=0-N%K` from `manifest.yml`).
- `merge_default_output_params.batch` / `scripts/merge_default_params.py` are
  Stage 8 (final merge with NHM defaults), distinct from Part 2 param
  generation. **Out of Step 4 scope** — keep as-is.

---

## Critical files to modify

| Action | Path | Source/model |
|---|---|---|
| Add | `configs/zonal_params.yml` | mirror `configs/depstor_params.yml` |
| Add | `scripts/derive_zonal_params.py` | mirror `scripts/derive_depstor_params.py` |
| Add | `slurm_batch/derive_zonal_params.batch` | mirror `slurm_batch/create_depstor_zonal.batch` |
| Add | `slurm_batch/merge_zonal_param.batch` | mirror `slurm_batch/merge_depstor_fraction.batch` |
| Add | `slurm_batch/build_zonal_weights.batch` | mirror today's `slurm_batch/build_weights.batch` resources |
| Add | `slurm_batch/submit_zonal_params.sh` | mirror `slurm_batch/submit_depstor_params.sh` |
| Refactor (minimal) | 6 `scripts/create_*_params.py` + `build_weights.py` + `merge_params.py` | extract `main()` bodies into callable functions; CLI shells delegate |
| Modify | `README.md` | add Zonal-pass section |
| Modify | `slurm_batch/RUNME.md` | Stage 4 retargets + script-mapping table |
| Add | `tests/test_zonal_orchestrator.py` | mirror `tests/test_shared_rasters_orchestrator.py` |

## Existing functions/utilities to reuse

- `src/gfv2_params/config.py::load_config`, `load_base_config` — fabric
  profile + placeholder resolution; matches depstor pattern
- `src/gfv2_params/lulc.py::load_crosswalk`, `assign_cov_type`,
  `compute_interception`, `compute_covden`, `compute_retention`,
  `class_percentages_from_histogram` — LULC param derivation
- `src/gfv2_params/raster_ops.py::deg_to_fraction` — used by ssflux path
- `src/gfv2_params/log.py::configure_logging` — standard logger setup
- Manifest-reading + array-sizing logic from `slurm_batch/submit_jobs.sh`
  (lines that read `n_batches` from `{batches_dir}/manifest.yml`) — lift this
  verbatim into `submit_zonal_params.sh`
- The fraction-loop + afterok-chain pattern from
  `slurm_batch/submit_depstor_params.sh` — adapt for the params loop

---

## Verification

1. **VPU01 smoke test** (using the small-scale validation target):
   ```bash
   FABRIC=gfv2_vpu01 bash slurm_batch/submit_zonal_params.sh \
       /caldera/.../gfv2_param_v2/gfv2_vpu01/batches gfv2_vpu01 \
       configs/base_config.yml
   ```
   Submits every param's array+merge in one go. Verify the merged CSVs exist
   under `gfv2_vpu01/params/merged/` after completion.

2. **Byte-for-byte equivalence with legacy path** — run today's per-param
   flow against gfv2_vpu01 for one param (e.g., elev), save the merged output;
   re-run the new orchestrator path for the same param; `diff` the two CSVs
   sorted by `id_feature`. Must be identical.

3. **CI gate** — add tests under `tests/test_zonal_orchestrator.py` mirroring
   `tests/test_shared_rasters_orchestrator.py`:
   - Every registered param has an executable `script:` tag
   - Empty params list exits cleanly
   - Unknown param name fails with a clear error
   - `depends_on:` parsing produces the expected job graph (mock sbatch)

4. **Legacy CLI compat** — invoke
   `scripts/create_zonal_params.py --config configs/elev_param.yml --batch_id 0`
   (an existing flow) — must still produce the same output. Confirms the
   refactor preserved the thin-shell entrypoint.

---

## Followups (out of scope)

- **Issue #82** (configs/ reorg) — after Step 4 settles, do the
  comprehensive subdirectory reorg. The new `zonal_params.yml` becomes
  `configs/zonal/zonal_params.yml` (or similar) under the deferred restructure.
- **Per-source resource tuning** — `create_lulc_nalcms_params.batch` previously
  hardcoded a 0-63 array; the new dispatch normalises sizing but the underlying
  resource ask (`--mem`, `--time`) may need per-param adjustment based on
  Part 1 NALCMS runtimes. Tune in a follow-up if needed.
- **Stage 8 `merge_default_params.py`** — keep separate. Distinct from
  Part 2 param generation (merges NHM-default values into the generated set).
