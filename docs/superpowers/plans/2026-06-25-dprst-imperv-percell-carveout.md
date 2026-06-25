# dprst per-cell impervious carve-out — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop a single impervious pixel from removing a whole waterbody clump from depression storage; carve impervious cells out of `dprst` per-cell instead.

**Architecture:** In `dprst.py`'s `build()`, impervious is removed from the region-level exclusion set (connectivity stays the only region-level exclusion) and instead masked out of `dprst_binary` cell-by-cell. `onstream` gains an impervious guard so carved cells aren't reclassified as on-stream. The `imperv`/`dprst`/`perv` cell partition stays disjoint, satisfying the double-counting intent.

**Tech Stack:** Python, NumPy, rasterio; pixi-managed env; pytest. Builder lives in `src/gfv2_params/depstor_builders/dprst.py`.

**Spec:** `docs/superpowers/specs/2026-06-25-dprst-imperv-percell-carveout-design.md`

## Global Constraints

- Environment is **pixi**. Run tests with `pixi run -e dev pytest ...`. **Do NOT run pytest on the HPC head node** — CI (`.github/workflows/ci.yml`) is the authoritative test gate; `py_compile`/import checks are fine locally.
- **Builder + test together** — a builder change ships with its test in the same commit.
- **Atomic commits** — one logical change per commit.
- **Run `pixi run -e dev pre-commit run --all-files` before pushing.**
- Memory ceiling unchanged: `dprst` remains a full-grid step (~384 GB peak, run at `--mem=384G`). This change adds no new full-grid array — `imperv_binary` is already read by the builder.

---

### Task 1: Per-cell impervious carve-out in `dprst` (code + test)

**Files:**
- Modify: `src/gfv2_params/depstor_builders/dprst.py` (module docstring; `build()` lines ~1, 46-68)
- Test: `tests/test_build_depstor_dprst.py` (add one test; existing test unchanged)

**Interfaces:**
- Consumes (unchanged helpers from `gfv2_params.depstor`): `RasterInfo`, `read_aligned_uint8(path, info) -> np.ndarray`, `read_land_mask(path) -> np.ndarray[bool]`, `regions_to_binary(regions, keep_ids) -> np.ndarray[uint8]`, `regions_touching_mask(regions, mask) -> set[int]`, `write_uint8_binary(arr, info, path)`.
- Produces (unchanged): `build(step_cfg, ctx, logger) -> {"dprst": Path, "onstream": Path}`. Output rasters `dprst_binary.tif` and `onstream_binary.tif` are uint8 (1 = present, 255 = nodata). Behavioural change only: a region touching imperv is now kept (minus its impervious cells) rather than wholly excluded.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_build_depstor_dprst.py` (keep the existing test; reuse the module-level `_N`, `_TRANSFORM`, `_write`):

```python
def test_dprst_carves_imperv_cells_but_keeps_region(tmp_path):
    template = tmp_path / "template.tif"
    _write(template, np.full((_N, _N), 100.0), "float32", -9999.0)

    # One isolated waterbody region: rows 0-1, cols 0-3 (8 cells), not connected.
    regions = np.zeros((_N, _N), dtype=np.int32)
    regions[0:2, 0:4] = 1
    _write(tmp_path / "wbody_regions.tif", regions, "int32", 0)

    wbody_binary = np.where(regions > 0, np.uint8(1), np.uint8(255))
    _write(tmp_path / "wbody_binary.tif", wbody_binary, "uint8", 255)

    # Connected mask touches nothing -> nothing excluded for connectivity.
    _write(tmp_path / "connected_wbody.tif",
           np.full((_N, _N), 255, dtype=np.uint8), "uint8", 255)

    # Two impervious cells fall inside the region (e.g. a road across a playa).
    imperv = np.full((_N, _N), 255, dtype=np.uint8)
    imperv[0, 0] = 1
    imperv[0, 1] = 1
    _write(tmp_path / "imperv.tif", imperv, "uint8", 255)
    _write(tmp_path / "land_mask.tif", np.ones((_N, _N), dtype=np.uint8), "uint8", 255)

    ctx = BuildContext(
        fabric="t", template_path=template, output_dir=tmp_path,
        hru_gpkg=tmp_path / "x.gpkg", hru_layer="nhru",
    )
    ctx.paths.update({
        "landmask": tmp_path / "land_mask.tif",
        "wbody_binary": tmp_path / "wbody_binary.tif",
        "wbody_regions": tmp_path / "wbody_regions.tif",
        "connected_wbody": tmp_path / "connected_wbody.tif",
        "imperv": tmp_path / "imperv.tif",
    })

    produced = dprst.build(
        {"outputs": {"dprst": "dprst_binary.tif", "onstream": "onstream_binary.tif"}},
        ctx, logging.getLogger("test"),
    )

    with rasterio.open(produced["dprst"]) as src:
        dprst_arr = src.read(1)
    with rasterio.open(produced["onstream"]) as src:
        onstream_arr = src.read(1)

    # The region is kept as depression storage at its non-impervious cells ...
    assert dprst_arr[1, 0] == 1
    assert dprst_arr[1, 3] == 1
    # ... the two impervious cells are carved out of dprst ...
    assert dprst_arr[0, 0] != 1
    assert dprst_arr[0, 1] != 1
    # ... and are NOT swept into on-stream storage.
    assert onstream_arr[0, 0] != 1
    assert onstream_arr[0, 1] != 1
    # Invariant: dprst and imperv never coincide (no double-count).
    assert int(((dprst_arr == 1) & (imperv == 1)).sum()) == 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pixi run -e dev pytest tests/test_build_depstor_dprst.py::test_dprst_carves_imperv_cells_but_keeps_region -v`
Expected: **FAIL** — current code excludes the whole region on imperv touch, so `assert dprst_arr[1, 0] == 1` fails (the cell is nodata 255, not 1).

- [ ] **Step 3: Update the module docstring**

In `src/gfv2_params/depstor_builders/dprst.py`, replace line 1:

```python
"""Combine wbody regions + connected-wbody mask + imperv into dprst + onstream."""
```

with:

```python
"""Classify waterbody regions into dprst + onstream.

A waterbody region is depression storage unless it is on-stream (touches the
NHD-connected mask). Impervious is NOT a region-level exclusion: a single
impervious cell must not remove a whole clump. Impervious cells are carved out
of dprst per-cell so imperv/dprst/perv stay a disjoint partition (no cell is
counted as both impervious and depression storage).
"""
```

- [ ] **Step 4: Implement the per-cell carve-out**

In `build()`, replace the block currently at lines 46-68 (from `onstream_regions = ...` through the `onstream = np.where(...)` / `onstream[~land_valid] = 255` lines) with:

```python
    onstream_regions = regions_touching_mask(regions, connected_binary)
    excluded = onstream_regions
    # Impervious is carved per-cell (below), NOT used to exclude whole regions:
    # a single impervious pixel must not drop an entire waterbody clump from
    # depression storage. regions_touching_mask is kept only for logging.
    imperv_regions = regions_touching_mask(regions, imperv_binary)
    n_total = int(regions.max())
    logger.info(
        "  %d total wbody regions; %d touch connected wbody (excluded), "
        "%d touch imperv (kept; cells carved per-cell)",
        n_total, len(onstream_regions), len(imperv_regions),
    )

    all_ids = set(int(v) for v in np.unique(regions) if v != 0)
    kept_ids = all_ids - excluded
    dprst_binary = regions_to_binary(regions, kept_ids)
    n_carved = int(((dprst_binary == 1) & (imperv_binary == 1)).sum())
    dprst_binary[imperv_binary == 1] = 255  # carve impervious cells (no imperv/dprst double-count)
    dprst_binary[~land_valid] = 255  # drop off-land (ocean) cells
    write_uint8_binary(dprst_binary, info, dprst_path)
    n_dprst = int((dprst_binary == 1).sum())
    logger.info(
        "  %d regions kept; %d impervious cells carved; %d cells in dprst (%.4f%% of grid)",
        len(kept_ids), n_carved, n_dprst, 100 * n_dprst / dprst_binary.size,
    )

    onstream = np.where(
        (wbody_binary == 1) & (dprst_binary != 1) & (imperv_binary != 1),
        np.uint8(1), np.uint8(255),
    )
    onstream[~land_valid] = 255  # drop off-land (ocean) cells
```

(Everything below — `write_uint8_binary(onstream, ...)`, the on-stream log line, and the `return` — is unchanged.)

- [ ] **Step 5: Run both dprst tests to verify they pass**

Run: `pixi run -e dev pytest tests/test_build_depstor_dprst.py -v`
Expected: **PASS** — both `test_dprst_excludes_connected_region_keeps_isolated` (imperv all-nodata → unchanged) and the new `test_dprst_carves_imperv_cells_but_keeps_region`.

- [ ] **Step 6: Lint**

Run: `pixi run -e dev pre-commit run --files src/gfv2_params/depstor_builders/dprst.py tests/test_build_depstor_dprst.py`
Expected: PASS (ruff/format clean).

- [ ] **Step 7: Commit**

```bash
git add src/gfv2_params/depstor_builders/dprst.py tests/test_build_depstor_dprst.py
git commit -m "fix(depstor): carve impervious out of dprst per-cell, not whole-region

A single impervious pixel was excluding an entire waterbody clump from
depression storage (~16,800 km2 / 14,898 regions falsely excluded CONUS-wide).
Impervious is now removed from dprst per-cell; connectivity stays the only
region-level exclusion. Keeps imperv/dprst/perv a disjoint partition.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Documentation

**Files:**
- Modify: `CLAUDE.md` (depstor gotchas list)
- Modify: `slurm_batch/HPC_REFERENCE.md` (DAG-order / cascade note near line 305)

**Interfaces:** none (docs only).

> Note: `docs/depstor_workflow.md` already describes the original ArcPy behavior as "depressions are only areas outside of impervious area" (per-cell) — this change *restores* that intent, so that file needs no edit. `docs/ARCHITECTURE.md` does not describe the imperv-exclusion mechanism, so it needs no edit. Verify both in Step 3 below rather than assuming.

- [ ] **Step 1: Add a CLAUDE.md gotcha**

In `CLAUDE.md`, under the depstor "Non-obvious conventions & gotchas" section, add a bullet adjacent to the **Land masking** bullet:

```markdown
- **Impervious is carved from dprst per-cell, never whole-region.** A waterbody
  clump is depression storage unless it is *on-stream* (touches the NHD-connected
  mask); impervious cells are masked out of `dprst` cell-by-cell in
  `depstor_builders/dprst.py`. Do NOT restore an imperv `regions_touching_mask`
  exclusion — one impervious pixel would then drop a whole multi-km² waterbody
  (a regression that falsely excluded ~16,800 km² CONUS-wide). The
  imperv/dprst/perv cell partition must stay disjoint (no double-count).
```

- [ ] **Step 2: Note the rebuild cascade in HPC_REFERENCE.md**

In `slurm_batch/HPC_REFERENCE.md`, immediately after the **DAG order** paragraph (currently ending "... passed through to the Python script." around line 307), add:

```markdown
**dprst rebuild cascade.** Changing `dprst` membership (e.g. the per-cell
impervious carve-out) invalidates everything downstream of it in the DAG:
`perv`, `routing` → `drains_perv`/`drains_imperv`, and `carea_map` (it consumes
`onstream` + `perv`). Rebuild with `--from dprst` and `FORCE=1`, then re-run the
depstor zonal + merge for the affected fractions (`dprst_frac`, `perv_frac`,
`drains_*_frac`, `onstream_storage_frac`, `carea_*`, `sro_to_dprst_*`).
`waterbody`, `wbody_connectivity`, `vpu_id`, and `landmask` are upstream and
unaffected.
```

- [ ] **Step 3: Verify no other doc asserts whole-region imperv exclusion**

Run: `grep -rni "imperv" docs/ARCHITECTURE.md docs/depstor_workflow.md README.md | grep -i "region\|exclud\|touch\|dprst"`
Expected: no line claims impervious *excludes a whole region/clump*. (`depstor_workflow.md` saying depressions are "outside of impervious area" is per-cell and correct.) If any line does assert whole-region exclusion, fix it to per-cell carve-out wording matching the CLAUDE.md bullet.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md slurm_batch/HPC_REFERENCE.md
git commit -m "docs(depstor): document per-cell imperv carve-out + dprst rebuild cascade

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Open PR and let CI gate it

**Files:** none (process).

- [ ] **Step 1: Push the branch and open a PR**

```bash
git push -u origin dprst-imperv-percell-carveout
gh pr create --title "fix(depstor): carve impervious out of dprst per-cell" \
  --body "$(cat <<'EOF'
Replaces the impervious region-level any-touch exclusion in the dprst builder
with a per-cell carve-out. Previously one >=50%-impervious pixel removed an
entire waterbody clump from depression storage; a CONUS scan found ~16,800 km2
across 14,898 regions falsely excluded (89% of that area in >=1 km2 waterbodies;
3,538 regions flipped by a single pixel). Connectivity stays the only
region-level exclusion. imperv/dprst/perv remain a disjoint cell partition, so
the double-counting intent is preserved.

Spec: docs/superpowers/specs/2026-06-25-dprst-imperv-percell-carveout-design.md

Note: this changes dprst membership and so requires a CONUS depstor rebuild
`--from dprst` (perv, routing, drains_*, carea_map) before the params are
regenerated — see slurm_batch/HPC_REFERENCE.md.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 2: Confirm CI passes**

Wait for `.github/workflows/ci.yml` (runs `pytest tests/`) to go green on the PR. Do not run the suite on the head node.

---

### Task 4 (operational, run on HPC after merge): CONUS rebuild

**Not a code task — run by the maintainer on the cluster after the PR merges.** Documented here so the cascade isn't missed.

- [ ] **Step 1: Rebuild depstor rasters from `dprst` onward**

From a shell with `~/.pixi/bin` on `PATH`:

```bash
FABRIC=gfv2 FORCE=1 sbatch --mem=384G slurm_batch/build_depstor_rasters.batch --from dprst
```

This re-runs `dprst → perv → routing → drains_perv/drains_imperv → carea_map` (per the DAG order in HPC_REFERENCE.md).

- [ ] **Step 2: Regenerate the affected depstor fractions**

Re-run the Stage 4 depstor zonal + merge jobs (per `slurm_batch/RUNME.md` §"Depstor fractions") for the affected fractions: `dprst_frac`, `perv_frac`, `drains_perv_frac`, `drains_imperv_frac`, `onstream_storage_frac`, `carea_t8_frac`, `sro_to_dprst_perv`, `sro_to_dprst_imperv`.

- [ ] **Step 3: Spot-check the recovered features**

Confirm the probe features are now depression storage (sample `gfv2/depstor_rasters/dprst_binary.tif` over their polygons): COMID 24068219 (Playa) and COMID 24152941 (SwampMarsh) should read `dprst==1` at their non-impervious cells; COMID 24079189 (Catlow) stays dprst. Verify `dprst_binary == 1` and `imperv_binary == 1` never coincide.

---

## Self-Review

**Spec coverage:**
- Core per-cell change (spec §"Detailed change") → Task 1. ✓
- onstream impervious guard (spec §"Detailed change") → Task 1, Step 4. ✓
- Disjoint-partition invariant (spec §"Resulting cell partition") → Task 1 test assertion. ✓
- Downstream/cascade (spec §"Downstream effects") → Task 2 Step 2 + Task 4. ✓
- Tests (spec §"Testing"): keeps-region, carved cells, not-onstream, dprst∩imperv=∅ → Task 1 test. ✓
- Docs (spec §"Documentation") → Task 2 (refined: CLAUDE.md + HPC_REFERENCE; ARCHITECTURE/workflow verified rather than blindly edited). ✓
- Deferred Approach C (spec §"Deferred") → no task, by design. ✓

**Placeholder scan:** none — all steps carry exact code, commands, and expected output.

**Type consistency:** `build(step_cfg, ctx, logger) -> dict` and the `dprst`/`onstream` uint8 (1/255) semantics match the existing builder and test; helper signatures (`regions_to_binary`, `regions_touching_mask`, `read_aligned_uint8`, `write_uint8_binary`) are unchanged and used as in the current file.
