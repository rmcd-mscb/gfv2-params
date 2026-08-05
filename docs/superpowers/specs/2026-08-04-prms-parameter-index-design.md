# PRMS parameter index — design

**Date:** 2026-08-04
**Status:** design, awaiting review
**Issue:** file on approval; D0b scoped out as #201
**Companion:** `2026-08-04-snakemake-migration-design.md` (independent; this spec does not
depend on it)

A scientist asking *"which parameters feed PRMS runoff?"* cannot answer it from this repo.
The layout is organised by HOW a parameter is computed (`zonal_runners/` /
`depstor_builders/` / `snarea/` / `aggregate/`; one config per stage), which is the right
axis for code and the wrong one for that question.

This spec adds the missing metadata axis. It moves no source files.

---

## Problem

### The question, answered by hand

11 of the parameters we emit feed `PRMSRunoff` / `srunoff_smidx`. They are spread across
**3 configs**, **4 builder packages**, and **2 output directories**:

| Parameter | Config | Builder |
| --- | --- | --- |
| `soil_moist_max` | `zonal_params.yml:81` | `zonal_runners/soils.py` |
| `dprst_seep_rate_open`, `dprst_flow_coef` | `zonal_params.yml:208` | `zonal_runners/ssflux.py` |
| `sro_to_dprst_perv` | `depstor_params.yml:134` | `same_hru_drains.py` + `perv.py` |
| `sro_to_dprst_imperv` | `depstor_params.yml:141` | `same_hru_drains.py` + `imperv.py` |
| `carea_max` | `depstor_params.yml:148` | `carea_map.py` |
| `smidx_coef` | `depstor_params.yml:155` | `carea_map.py` |
| `hru_percent_imperv` | `depstor_params.yml:164` | `imperv.py` + `landmask.py` |
| `dprst_frac` | `depstor_params.yml:177` | `dprst.py` + `landmask.py` |
| `dprst_depth_avg` | `depstor_params.yml:107` (`means:`) | `dprst_depth.py` + `dprst_depth/aggregate.py` |
| `op_flow_thres` | **`depstor_rasters.yml:77`** (output name at `:83`) | `dprst_depth.py:361` |

Process membership throughout this spec is from `pywatershed.<Process>.get_parameters()`
(pywatershed 2.0.4, `reference` pixi env), not inference.

### Four defects the mapping surfaced

**D0a — `hru_slope` is degrees on disk; PRMS wants a decimal fraction.** *(Decision made:
the pipeline will emit rise/run — see Design F.)* Verified end to end:

| Step | Evidence |
| --- | --- |
| `slope.vrt` is degrees | `shared_rasters/compute_slope_aspect.py:72` — `rd.TerrainAttribute(dem, attrib="slope_degrees")` |
| so `nhm_slope_params.csv:mean` is degrees | zonal mean of that raster |
| `viz.py:506` labels it `units="degrees"` | consistent |
| PRMS `hru_slope` is decimal fraction rise/run | `docs/NHM_description_Regan_2018_TM6B9.md:536` |

For small angles the discrepancy is 180/π ≈ **57×**: a 5° hillslope is `hru_slope = 0.087`,
not `5`.

**The pipeline is not wrong** — `zonal_runners/ssflux.py:63` applies
`raster_ops.deg_to_fraction` (`np.tan(np.deg2rad(x))`, `:264-266`) before deriving
`ssr2gw_rate`/`slowcoef_lin`, so the flux params are correct. But **nothing emits
`hru_slope`**, and a consumer reading `merged/nhm_slope_params.csv` gets degrees under a
column named `mean`. This is D2 (below) with a numeric consequence rather than a naming
one, and it is the strongest single justification for this spec.

Worked example from gfv2, HRU 1: the file says `mean = 4.4252`; PRMS `hru_slope` should be
`0.0774`. Copied verbatim, that declares a 77° cliff instead of a 4.4° hillslope.

**D0b — `hru_aspect` is an arithmetic mean of a circular variable, and cannot be repaired
from what is on disk.** TM6B9:603 states the required derivation explicitly:

> "the trigonometric sine and cosine of each cell's aspect are derived to create two new
> rasters of values. The average value for both of these raster values is determined for
> each HRU. The hru_aspect value is then set to the inverse tangent of these two values
> `atan2[sin(aspect), cos(aspect)]`."

The `aspect` entry (`zonal_params.yml:64`) instead runs the generic `zonal` script — a plain
`exactextract` mean. Measured across all 361,471 gfv2 HRUs, the signature is unambiguous:

| Statistic | Value |
| --- | --- |
| median aspect `mean` | **179.4°** (due south) |
| IQR | 151.7° – 207.5° |
| median within-HRU `std` | 91.3° (uniform-circular ≈ 104°) |

Half of CONUS reporting a mean orientation between 152° and 208° is not terrain — it is what
arithmetic-averaging wrapped directions produces. The column does not measure predominant
orientation; it measures how symmetric each HRU's cell distribution is about 180°.

**This was known, and the magnitude is the only new information.** The circularity rule was
applied deliberately and testably at **every raster boundary**:

| Location | What it guards |
| --- | --- |
| `shared_rasters/compute_slope_aspect.py:68` | COG overviews → `NEAREST` |
| `shared_rasters/build_vrt.py:74` | VRT decimation → `NEAREST` |
| `shared_rasters/build_border_dem.py:224` | border overviews → `NEAREST` |
| `shared_rasters/cog.py:84` | the helper's stated contract |
| `tests/test_compute_slope_aspect.py:90`, `tests/test_build_vrt.py:185` | two tests asserting it |

And the zonal-side limitation was flagged at the time, in
`notebooks/_archive/check_params.ipynb`:

> "**Note:** Aspect is a circular variable; the arithmetic mean is a **simplification**.
> North- and south-facing slopes (0°/360° vs 180°) are climatically opposite despite being
> near-neighbours numerically."

So this is not a defect nobody noticed. It is a **documented simplification whose magnitude
was never measured** — and the measurement above is what changes the verdict. A few-percent
bias would have vindicated the original call; a median of 179.4° means the column's central
tendency is the artifact rather than the terrain.

**Do not delete `notebooks/_archive/` before porting that note.** `repo_review_issues.md`
CODE-4 proposes deleting the directory as exploratory noise; it is currently the only record
of this known limitation, and it is the reason D0b can be framed accurately rather than as an
oversight.

**Why this differs from D0a in kind, not just degree.** D0a is a unit error: `tan(radians(mean))`
recovers the correct value from data already on disk. D0b is information-destroying — a
circular mean cannot be reconstructed from an arithmetic one. The fix requires sin/cos
rasters and a second zonal pass, exactly as TM6B9 describes, plus a CONUS re-run. That is a
builder change, not an index change, so **D0b is recorded here and filed separately** (see
"Scoped out"). `hru_aspect` feeds PRMSSolarGeometry/PRMSAtmosphere, so a uniform southern
bias is a systematic solar-radiation bias.

**D1 — `op_flow_thres` is a PRMSRunoff parameter filed outside `merged/`.** Written to
`{fabric}/depstor_rasters/op_flow_thres_params.csv` (`depstor_rasters.yml:83`,
`dprst_depth.py:361`), confirmed on disk for gfv2. Because it never reaches `merged/`, it
is invisible to `iter_declared_params` (`merge_and_fill_params.py:382`), never gap-filled,
and `warn_undeclared_merged_files` (`:477`, which globs `merged_dir` at `:492`) cannot see
it either. Anyone assembling a parameter file by globbing `merged/nhm_*_params.csv`
silently drops it.

**D2 — for four files the emitted column name is not the PRMS name, and nothing records
the translation.** `nhm_elevation_params.csv` has a column named `mean`; so do slope and
aspect. `nhm_soils_params.csv` emits `soils` (`soils.py:78`). Grepping the repo for
`hru_elev` / `hru_slope` / `hru_aspect` / `soil_type` hits only
`docs/NHM_description_Regan_2018_TM6B9.md` (the imported USGS report) and
`docs/nhm_source_crosscheck_2026-07.md` — never a config, a builder, or the fill contract.

**D3 — `retention` is PRMS `rad_trncf`, but only on `lulc_nhm_v11`.** That entry's
`fill_columns` carries an alias group `[retention, rad_trncf]` (`zonal_params.yml:128`)
because `lulc_prederived.py` renamed the same computed quantity; fabrics built before the
rename (gfv2, oregon) carry `retention`, after (tjc) carry `rad_trncf`.

**This does not generalise to the other three LULC entries**, which declare a bare
`retention` (`zonal_params.yml:141-150`, `:166-175`, `:191-200`) — deliberately, because
the builders compute different things:

- `lulc_prederived.py:176-181` — `rad_trncf = rad_trncf_from_density(zonal_mean(radtrn_raster))`,
  a Beer's-law transform. Its docstring (`:20`) states "There is no `retention` column: it
  was only ever a stand-in for rad_trncf."
- `lulc.py:186-193` — `retention` = `zonal_mean(keep)/100` **or** the crosswalk's
  `evergreen_retention` column. No Beer's-law step; no `radtrn_raster` is configured for
  those entries (`zonal_params.yml:161-165` says so).

Mapping `retention → rad_trncf` for `lulc_nalcms`/`nlcd`/`foresce` would assert two
different derivations are the same PRMS parameter. Scope D3 to `lulc_nhm_v11`; the other
three get `retention` in `prms.provenance` pending verification.

---

## Non-goals

- **Moving any source file.** `src/gfv2_params/`'s 7 subpackages and `configs/`'s 5
  subdirectories stay exactly as they are. The by-process view is a metadata gap, not a
  layout gap: a file lives in one directory, a parameter feeds several processes, and
  directories cannot express a many-to-many relation. `carea_map.py` would still be wrong
  for the HRUs where `smidx_coef` is the output rather than `carea_max`.
- **Restructuring the `params:` list.** See "Hard constraint".
- **Changing any derivation.** D0's conversion is an emit-or-document decision, not a
  recomputation; the flux params that consume slope already convert correctly.

---

## Hard constraint

`submit_zonal_params.sh` does not read the YAML. It carries a hardcoded bash array
(`:68-79`) plus two name-keyed maps (`NEEDS_WEIGHTS` `:94`, `NEEDS_MERGE_OF` `:101`) and
exports `PARAM=<name>`; the driver does a flat linear scan for a matching `name:`
(`derive_zonal_params.py:64-71`). `submit_depstor_params.sh:63-74` has the same shape.

The load-bearing contract: **`params:` stays a flat list, and every `name:` value stays
stable.**

| Change | Wrapper impact |
| --- | --- |
| Add a `prms:` key **inside** an existing entry | **None.** Neither the wrapper nor `_find_param` reads unknown keys. |
| Reorder entries | None for the driver; the wrapper's array is the real order (slope must precede ssflux). Leave both alone. |
| **Add a new element to a list** (Design D) | **None for `means:`/`ratios:`/`constants:`** — no bash array indexes them. **Would break `params:`** and `fractions:`, which the two wrappers mirror. Design D touches only `means:`-family lists. |
| Nest `params:` under group keys | Breaks `_find_param` (`:67`), `iter_declared_params` (`:422`), both wrappers. Not proposed. |
| Rename any `name:` | Breaks the hardcoded arrays and dependency maps. Not proposed. |

---

## Deliverable 1 — the mapping

19 rows: **16** files currently in `gfv2/params/merged/`, **2** declared-but-unbuilt
(`lulc_nlcd`, `lulc_foresce` — their `input/lulc_veg/{nlcd,foresce}/` source directories do
not exist on disk), and **1** (`op_flow_thres`) outside `merged/` until D1 lands.

Column lists are observed on-disk headers.

| Emitted file | Column(s) | PRMS parameter | PRMS process | Config entry | Builder |
| --- | --- | --- | --- | --- | --- |
| `nhm_elevation_params.csv` | `mean` (+8 stats) | `hru_elev` ⚠️ | *no pywatershed process*; PRMS temp/precip distribution, `cov_type` reset (TM6B9:707) | `zonal_params.yml:43` | `zonal_runners/zonal.py` |
| `nhm_slope_params.csv` | **`hru_slope`** *(new, D0a)* | `hru_slope` | PRMSSolarGeometry, PRMSAtmosphere | `zonal_params.yml:56` | `zonal_runners/zonal.py` + `derived_columns` |
| ″ | `mean` (degrees) + 8 stats | *provenance* — the raw stat `hru_slope` is derived from | — | ″ | ″ |
| `nhm_aspect_params.csv` | `mean` (+8 stats) | **DEFECTIVE — not `hru_aspect`** ⚠️**D0b** | PRMSSolarGeometry, PRMSAtmosphere | `zonal_params.yml:64` | `zonal_runners/zonal.py` |
| `nhm_soils_params.csv` | `soils` | `soil_type` ⚠️ | PRMSSoilzone | `zonal_params.yml:74` | `zonal_runners/soils.py:78` |
| `nhm_soil_moist_max_params.csv` | `soil_moist_max` | same | **PRMSRunoff**, PRMSSoilzone | `zonal_params.yml:81` | `zonal_runners/soils.py` |
| `nhm_lulc_nhm_v11_params.csv` | `cov_type` | same | PRMSCanopy, PRMSSnow, PRMSSoilzone | `zonal_params.yml:96` | `lulc_prederived.py` |
| ″ | `covden_sum`, `covden_win` | same | PRMSCanopy, PRMSSnow | ″ | ″ |
| ″ | `srain_intcp`, `wrain_intcp`, `snow_intcp` | same | PRMSCanopy | ″ | ″ |
| ″ | `retention` \| `rad_trncf` | `rad_trncf` ⚠️**D3** | PRMSSnow | ″ | ″ |
| `nhm_lulc_nalcms_params.csv` | `cov_type`, 2 covden, 3 intcp | same | as above | `zonal_params.yml:133` | `lulc.py` |
| ″ | `retention` | **unverified** — not `rad_trncf` (D3) | — | ″ | ″ |
| `nhm_lulc_nlcd_params.csv` *(unbuilt)* | same 7 | same | same | `zonal_params.yml:154` | `lulc.py` |
| `nhm_lulc_foresce_params.csv` *(unbuilt)* | same 7 | same | same | `zonal_params.yml:179` | `lulc.py` |
| `nhm_ssflux_params.csv` | `soil2gw_max`, `ssr2gw_rate`, `fastcoef_lin`, `slowcoef_lin` | same | PRMSSoilzone | `zonal_params.yml:208` | `ssflux.py` |
| ″ | `gwflow_coef` | same | PRMSGroundwater | ″ | ″ |
| ″ | `dprst_seep_rate_open`, `dprst_flow_coef` | same | **PRMSRunoff** | ″ | ″ |
| ″ | `k_perm_wtd`, `mean_slope_fraction`, `hru_area` | *inputs, not params* — `hru_area` here is m², **not** PRMS `hru_area` (acres); see `zonal_params.yml:238-242` | — | ″ | ″ |
| `nhm_sro_to_dprst_perv_params.csv` | `sro_to_dprst_perv` | same | **PRMSRunoff** | `depstor_params.yml:134` | `same_hru_drains.py` + `perv.py` → `gfv2_params/depstor_ratios.py` |
| `nhm_sro_to_dprst_imperv_params.csv` | `sro_to_dprst_imperv` | same | **PRMSRunoff** | `:141` | `same_hru_drains.py` + `imperv.py` |
| `nhm_carea_max_params.csv` | `carea_max` | same | **PRMSRunoff** | `:148` | `carea_map.py` |
| `nhm_smidx_coef_params.csv` | `smidx_coef` | same | **PRMSRunoff** | `:155` | `carea_map.py` |
| `nhm_hru_percent_imperv_params.csv` | `hru_percent_imperv` | same | **PRMSRunoff**, PRMSSoilzone, PRMSEt | `:164` | `imperv.py` + `landmask.py` |
| `nhm_dprst_frac_params.csv` | `dprst_frac` | same | **PRMSRunoff**, PRMSSoilzone, PRMSEt | `:177` (`ratios:`) | `dprst.py` + `landmask.py` |
| `nhm_dprst_depth_avg_params.csv` | `dprst_depth_avg` | same | **PRMSRunoff** | `:107` (`means:`) | `dprst_depth.py` + `dprst_depth/aggregate.py` |
| ″ | `dprst_depth_provenance` | *provenance* | — | ″ | ″ |
| `nhm_snarea_curve_params.csv` | `hru_deplcrv`, `snarea_thresh`, `snarea_curve_0..10` | `hru_deplcrv`, `snarea_thresh`, `snarea_curve` | PRMSSnow | `snarea_library.yml` | `snarea/library.py` ← `snarea/build.py` ← `aggregate/` |
| ″ | 10 `cv_*`/diagnostic columns | *provenance* | — | ″ | ″ |
| **`op_flow_thres_params.csv`** | `op_flow_thres` | same | **PRMSRunoff** | `depstor_rasters.yml:83` | `dprst_depth.py:361` |

⚠️ = emitted column name is not the PRMS parameter name.

**Name collision to respect:** `dprst_frac` appears in both `fractions:` (`:50`, a count
CSV in `merged/_intermediates/`) and `ratios:` (`:177`, the PRMS param in `merged/`). Any
name-keyed index must key on the `ratios:`/`means:`/`params:` families only, as
`iter_declared_params` already does by excluding `fractions:` (`:422-435`).

---

## Design

### A. `prms:` metadata — column-level, additive

```yaml
- name: ssflux
  # ... every existing key unchanged ...
  prms:
    columns:
      soil2gw_max:          {prms: soil2gw_max,          processes: [PRMSSoilzone]}
      ssr2gw_rate:          {prms: ssr2gw_rate,          processes: [PRMSSoilzone]}
      fastcoef_lin:         {prms: fastcoef_lin,         processes: [PRMSSoilzone]}
      slowcoef_lin:         {prms: slowcoef_lin,         processes: [PRMSSoilzone]}
      gwflow_coef:          {prms: gwflow_coef,          processes: [PRMSGroundwater]}
      dprst_seep_rate_open: {prms: dprst_seep_rate_open, processes: [PRMSRunoff]}
      dprst_flow_coef:      {prms: dprst_flow_coef,      processes: [PRMSRunoff]}
    provenance:
      k_perm_wtd:          litho-weighted permeability, flux-normalisation input
      mean_slope_fraction: tan(radians(slope mean)), flux-normalisation input
      hru_area:            fabric geometry.area in m² — NOT PRMS hru_area (acres)
```

**`processes:` must be per-column, not per-entry.** `ssflux` alone spans three processes
across different columns; a flat entry-level `processes: [PRMSRunoff]` would make
`params_for_process("PRMSRunoff")` return the whole file and report 5 non-runoff parameters
as feeding runoff. The same applies to `lulc_nhm_v11` (`cov_type` → three processes,
`srain_intcp` → Canopy only, `rad_trncf` → Snow only).

**`provenance` is orthogonal to `fill_columns`, not a restatement of it.** `fill_columns`
answers *"is this KNN-interpolable?"*; `prms.provenance` answers *"is this a PRMS
parameter?"*. All four quadrants exist today:

| | filled | not filled |
| --- | --- | --- |
| **is a PRMS param** | `soil_moist_max` (`zonal_params.yml:86`) | `op_flow_thres` — D1's defect |
| **not a PRMS param** | elevation/slope/aspect's 8 stats (`:54`, `:62`, `:70`) | `dprst_depth_provenance` (`depstor_params.yml:118-123`), snarea's `cv_*` (`snarea_library.yml:14-18`), ssflux's `k_perm_wtd` (`zonal_params.yml:214-218`) |

The elevation/slope/aspect stats are the load-bearing case: `zonal_params.yml:50-54`
states explicitly that `count/std/min/25%/50%/75%/max/sum` are **not** provenance — "plain
exactextract zonal statistics with no 'not derivable by design' meaning… safe, and
desirable, to fill". They are filled *and* they are not PRMS parameters, so they belong in
`prms.provenance` while remaining in `fill_columns`.

**Alias groups.** `fill_columns` entries may be a list of alternatives, not only a string
(`zonal_params.yml:128`; handled at `merge_and_fill_params.py:101-112`). `prms.columns`
must carry **every** alternative as its own key, both mapping to the same PRMS name:

```yaml
  prms:
    columns:
      retention:  {prms: rad_trncf, processes: [PRMSSnow]}
      rad_trncf:  {prms: rad_trncf, processes: [PRMSSnow]}
```

**`prms:` is mandatory, not optional**, for every entry `iter_declared_params` returns —
otherwise Design C's guard passes vacuously, including on day one when no entry has one.
The implementing PR adds it to all declared entries in the same commit as the guard.

**Placement for `snarea_curve`:** `iter_declared_params` passes the **entire
`snarea_library.yml` document** as the entry (`merge_and_fill_params.py:437-440`), so
`prms:` is a top-level key there, alongside `library_file`/`validation_file`/`netcdf_file`
— which are outputs of the same stage but not per-HRU parameter files, and are not covered
by this spec.

### B. `gfv2_params/params_index.py`

Move `iter_declared_params` (`merge_and_fill_params.py:382-442`) into the package, widen
`DeclaredParam` with a `prms` field, and import it back.

**`DeclaredParam` is a `NamedTuple`, so equality is tuple equality.** Widening it breaks
existing assertions that compare against 4-tuples —
`tests/test_merge_and_fill_params.py:630`, `:631`, `:634`, plus positional constructions at
`:737`, `:751`, `:762`. These **must be rewritten to attribute access in the same commit**,
and `prms` must carry a default. This is not hypothetical: the `DeclaredParam` docstring
(`:359-380`) exists because the last widening broke four consumption sites while "the test
suite stayed green because its fixtures hand-built the OLD 3-tuple shape".

Promotion is import-safe: `src/gfv2_params/__init__.py` contains only `__version__`, so
`import gfv2_params.params_index` pulls in no geo libraries.

### C. Two guards, because one cannot work

The naive guard — "every `fill_columns` column is in `prms.columns` or `prms.provenance`" —
runs *declaration → declaration* and therefore **cannot catch the D2/D3 class it was
written for**. Measured against real headers it is a tautology on 12 entries, vacuous on 3,
and crashes on the alias list:

| Entry | `fill_columns` | on-disk payload cols | invisible to a declaration-only guard |
| --- | --- | --- | --- |
| `ssflux` | 7 | 10 | `k_perm_wtd`, `mean_slope_fraction`, `hru_area` |
| `snarea_curve` | 13 | 23 | 10 `cv_*`/diagnostic columns |
| `dprst_depth_avg` | 1 | 2 | `dprst_depth_provenance` |

**Guard 1 (CI, YAML-only, no data root).** `prms.columns ∪ prms.provenance` is the
**declared complete column list**. Assert it is a superset of
`flatten(fill_columns) ∪ fabric_columns.keys()`. Alias lists are flattened; every
alternative must appear. This makes the declaration self-consistent and catches a typo, but
does not see disk.

**Guard 2 (data-root-gated, skip-if-absent).** For each declared file present under
`{fabric}/params/merged/`, assert the on-disk header equals
`prms.columns ∪ prms.provenance ∪ {id_feature}`. This is the one that catches a new column
appearing with no PRMS decision recorded. It cannot run in CI — `merge_and_fill_params.py:341-346`
records the contract that "tests may parse these files but must not touch a real data root"
— so it runs where the parity tests run, not on `ubuntu-latest`.

Dropping the claim from the previous draft that this "mirrors `warn_undeclared_merged_files`":
that guard runs disk → declaration, and only Guard 2 resembles it.

### D. `op_flow_thres` → `merged/`

Add a `constants:` list to `depstor_params.yml` and a fifth loop to `iter_declared_params`:

```yaml
constants:
  - name: op_flow_thres
    source: "{data_root}/{fabric}/depstor_rasters/op_flow_thres_params.csv"
    merged_file: nhm_op_flow_thres_params.csv
    fill_columns: [op_flow_thres]
    prms:
      columns: {op_flow_thres: {prms: op_flow_thres, processes: [PRMSRunoff]}}
```

**Not a `means:` entry**, which the previous draft proposed: `run_mean_zonal` does
`raster_path = Path(spec["source_raster"])` unconditionally
(`scripts/derive_depstor_params.py:259`) and `_find_mean` advertises every `means[].name`
as a runnable `--mean` target (`:113-119`). `op_flow_thres` is a constant 1.0 built from
the fabric's id list with no raster at all (`dprst_depth.py:361-387`), so a raster-less
`means:` entry is a `KeyError` waiting for the first operator who types
`--mean op_flow_thres`.

A new `--mode copy_constants` performs the copy. Per the Hard-constraint table, adding a
list element to a non-`params:`/non-`fractions:` list has no wrapper impact.

The file is already per-HRU-complete (`dprst_depth.py:367-380` builds the id column from
`ctx.hru_gpkg`; verified on disk: `nat_hru_id,op_flow_thres` / `1,1.0`), so the fill
declaration is a no-op rather than a KNN pass.

### F. `derived_columns:` — emit `hru_slope` (D0a)

The pipeline hands modelers the PRMS quantity rather than a raw statistic plus a footnote.
Config-driven, on the existing entry:

```yaml
- name: slope
  # ... unchanged ...
  derived_columns:
    hru_slope: {from: mean, transform: deg_to_fraction}
  prms:
    columns:
      hru_slope: {prms: hru_slope, processes: [PRMSSolarGeometry, PRMSAtmosphere]}
    provenance:
      mean:  mean cell slope in DEGREES — the raw stat hru_slope is derived from
      count: {}   # …and the other 7 exactextract stats
```

`transform` names a function in `gfv2_params.raster_ops`; `deg_to_fraction`
(`np.tan(np.deg2rad(x))`, `:264-266`) already exists and is what `ssflux.py:63` uses, so
this makes the rest of the pipeline consistent with what `ssflux` already assumes rather
than introducing a new convention. Applied in `run_merge`, so **no zonal re-run is needed**
— `mean` is already on disk for every fabric.

This resolves D0a and D2-for-slope together: the PRMS parameter now carries the PRMS name,
and `mean` is explicitly demoted to provenance so the two slope-like columns cannot be
confused.

**Known approximation, recorded not fixed.** `tan(mean θ) ≠ mean(tan θ)`, and `tan` is
convex, so this systematically *under*estimates. Measured over all 361,471 gfv2 HRUs
(second-order Taylor from the on-disk `mean`/`std`): median **0.2%**, p90 2.4%, p99 5.6%.
Too small to justify building a CONUS fractional-slope VRT — none exists; only per-VPU
`Slope_pct_hydrodem_<vpu>.tif` from the opt-in hydrodem path
(`compute_dem_derivatives.py:351`). `ssflux` has carried the same approximation since it was
written.

The mechanism is not single-use: D0b's fix needs the same `derived_columns:` shape, with
`atan2` over two zonal means instead of a one-argument transform.

### E. Generated `docs/parameter_index.md`

`scripts/build_parameter_index.py` renders three views — by PRMS process, by config entry,
by builder — from `params_index.py`. Nav entry in `mkdocs.yml` and a link from
`docs/index.md` land **in the same commit as the file**, not later.

---

## Sequence

| Step | Work |
| --- | --- |
| 1 | Hand-write `docs/parameter_index.md` from Deliverable 1, with nav entry. Zero code risk; forces D0–D3 to surface as concrete decisions; becomes the generator's acceptance target. |
| 2 | `params_index.py` + `DeclaredParam` widening + the six test rewrites (B). |
| 3 | `prms:` blocks for every declared entry + Guard 1 (A, C). |
| 4 | Guard 2, wired where parity tests run. |
| 5 | `constants:` + `--mode copy_constants` + `op_flow_thres` (D). |
| 6 | `derived_columns:` + `hru_slope` (F). No zonal re-run; re-runs `--mode merge` for `slope` only. |
| 7 | `build_parameter_index.py` replaces the hand-written index (E). |

Steps 1–4 are independent of the Snakemake migration. Step 6 is where a
`snakemake prms_runoff` target group becomes possible, but nothing here requires it.

---

## Testing

- **Guard 1** — YAML-only, CI-safe, runs on `ubuntu-latest`.
- **Guard 2** — data-root-gated, `pytest.skip` when absent. **Note this means CI will
  report it green while skipping**; it is only meaningful where a data root exists, so its
  result must be recorded manually, not inferred from a green CI badge.
- **`params_index.py`** — port the existing `iter_declared_params` coverage from
  `tests/test_merge_and_fill_params.py`, extended for `prms:`, with the six tuple-equality
  assertions rewritten.

Per `CLAUDE.md`, no `pytest` on the HPC head node.

---

## Scoped out — filed separately

**D0b — `hru_aspect` circular mean.** Recorded above with its evidence, but not implemented
here. It requires two new shared rasters (`sin(aspect)`, `cos(aspect)`), a config entry, a
second zonal pass, an `atan2` combine, and a CONUS re-run — a builder change with a compute
cost, not an index change. Folding it in would block a small shippable spec behind a large
one. **Filed as issue #201**, carrying the measurement, the raster-boundary evidence, and
TM6B9:603's required method.

Until it lands, the index must mark `nhm_aspect_params.csv:mean` as **defective — not
`hru_aspect`**, not merely as an undocumented rename. That is the whole point of having an
index: it is better for a consumer to read "this column is wrong" than to read nothing and
infer it is right.

---

## Could not verify

- **`soils` → `soil_type`** — inference from the source raster (`TEXT_PRMS.tif`, soil
  texture) plus TM6B9:786. No repo artifact asserts it.
- **`mean` → `hru_elev`** — inference from `viz.py:505-507` plus TM6B9:601. Elevation is
  not a circular or transformed quantity, so the arithmetic zonal mean is the right
  statistic; only the *name* is undocumented. (`hru_slope` and `hru_aspect` are no longer
  inferences — see D0a/D0b.)
- **`retention` on `lulc_nalcms`/`nlcd`/`foresce`** — what PRMS parameter, if any, it
  corresponds to. `lulc.py`'s derivation differs from the Beer's-law path; see D3.
- **Which LULC source is the delivered one** — both `nhm_v11` and `nalcms` are on disk for
  gfv2.
- **pywatershed's process→parameter lists are pywatershed's view of PRMS**, not TM6B9's NHM
  module set. They agreed everywhere spot-checked, but `hru_elev` appears in no pywatershed
  process at all, so the two are not identical.
- No `pytest` was run (head-node prohibition).
