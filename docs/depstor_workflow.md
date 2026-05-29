# Depression Storage Overhaul

**Authors:** Andy Bock and Cory Russell

**Source:** Transcribed from `docs/DepStor_workflow.pdf` for ease of reference
in subsequent issues and PRs. The PDF is the original; this file is a working
copy. Strikethroughs from the PDF (completed checklist items) are preserved as
plain text below to keep semantic intent.

## Status

- **2/17** — Finished Level One, starting first two functions of Level Two

## Goals

1. Simplify, "efficiencify" and move depression storage calculation to open source
2. Test new datasets (DSWE) and ideas (see below)
3. Integrate NCMapper from Canada

## Needed Inputs

- HRUS
- Stream Segments
- Waterbodies
  - waterbodies
    - NHDPv2
    - NHDPHR
    - *DSWE (waiting on Anthoneh's workflow)*
      - Backup is Nate's workflow, contact if we don't have after working on
        functions below
  - Other datasets (lagos, etc.)
- **Impervious surface raster**
  - CONUS extent
- *TWI Raster (asking Mike W.)*
- **Flow Direction Raster (FDR)**
  - VPU-extent (Region 17)

## Test Areas

- 17120004 (Southeast Oregon west of Malheur NWR)
- 10130103 (Eastern Missouri River basin, Prairie Pothole Country)

## Things to do now (1/28) — all done

- ~~Write intermediate output to a directory, not geodatabase or geopackage~~
- ~~As we start to proceed and get a good feel for QAQC, we can limit the number
  of things we write out~~
- ~~Concentrate on the Tier 1 (pre-processing and data input) functions~~
- **We still need the TWI layer from Mike**
- ~~Segbuffer: Remove hard-coded buffer distances (30, 60) from segment buffer
  function, add as input variable~~
- ~~Hruimperv: Remove hard-coded impervious percentage (50%); code as input
  variable~~
- ~~In getwaterbodiesinHRUsGrid — move the reclassification of imprinting the
  HRU ID into waterbodies (lines 919-922) into the getwaterbodiesinHRUsGrid
  function, that way we can use different waterbody inputs (NHDPlusV2, HiRes,
  etc.)~~
- Additional
  - **Second raster of waterbody IDs**

## Ideas (don't implement for now)

- ~~Lower size limit for depressions (aka 120m²) (implemented)~~
- Differentiate NHD streams as on-stream storage in HRU, rather than just segments
- **For nhdpv2 waterbodies, see about getting upstream catchments from
  nhdplustools rather than delineating (Andy)**
- Vary stream buffer by stream order

## Variables

- ~~Raster cell size — use nhdplusv2 rasters as the footprint and cellsize~~
- Paramdb — where to write csv parameter files (likely doesn't matter)

## Intermediate Outputs

- ~~**nhrug**: raster version of HRUs~~
- ~~**wbodg**: raster version of waterbodies~~

---

## Workflow

The workflow is broken up into 5 levels:

1. **Level One:** derivation of datasets that are used independently as inputs
   across all levels
2. **Level Two:** Determination of depression storage features and their
   upstream surface contributing areas
3. **Level Three:** Determination of surface runoff fractions to impervious,
   non-impervious, and on-stream areas
4. **Level Four:** Derivation of different contributing area maps and `smidx`
   and `carea_max` parameters that control runoff partitioning
5. **Level Five:** Profit (i.e., generate the parameter files)

**gfv2-params land mask (PR #69, issue #68):** Before any Level One/Two builder
runs, the `landmask` step in [`scripts/build_depstor_rasters.py`](../scripts/build_depstor_rasters.py)
(implementation in [`src/gfv2_params/depstor_builders/landmask.py`](../src/gfv2_params/depstor_builders/landmask.py))
rasterises the `nhru` polygon fabric to the template grid → `land_mask.tif`
(uint8 1/255). Every other depstor raster builder masks its output against
it. This replaced an earlier DEM-nodata mask that bulged into the ocean (the
hydro-conditioned DEM carries valid elevations over coastal water; the FDR
has the same blobs). The HRU fabric is the authoritative modeling domain
and is exactly what `derive_zonal_params` aggregates to downstream.

**gfv2-params per-VPU land mask (PR #70, issue #66):** Independent of the
CONUS depstor `land_mask.tif` above, a *per-VPU* HRU mask is built by the
`build_vpu_landmask` step of the shared-rasters orchestrator
(implementation:
[`src/gfv2_params/shared_rasters/build_vpu_landmask.py`](../src/gfv2_params/shared_rasters/build_vpu_landmask.py);
configured in the `steps:` block of
[`configs/shared_rasters/shared_rasters.yml`](../configs/shared_rasters/shared_rasters.yml)).
It filters `gfv2/fabric/gfv2_nhru_merged.gpkg` by `vpu == <vpu>` (with
sub-region handling for `03N/S/W` and `10L/U` via `VPU_RASTER_MAP`) and
rasterises onto the per-VPU Hydrodem grid, writing
`shared/per_vpu/<vpu>/land_mask_<vpu>.tif`. The per-VPU TWI pipeline
(`merge_rpu_by_vpu_twi` step + `compute_dem_derivatives` opt-in step)
consumes this per-VPU mask via
[`read_land_mask_for_grid`](../src/gfv2_params/depstor.py) to clip
`Twi_merged_<vpu>.tif` and `Twi_hydrodem_<vpu>.tif` to the per-VPU HRU
boundary. Without it, per-RPU TWI bulges (coastal ocean, adjacent-VPU
drape on per-RPU Hydrodem tiles) leak into downstream zonal aggregation.
Runs as **Stage 1c1** (see `slurm_batch/HPC_REFERENCE.md`), before the TWI merge.

---

### Level One — Preprocessing and setting environments (Finished 2/17)

1. Set raster properties to `nhrug` for cell footprint and resolution
2. Convert `nhru`, `nsegments`, and waterbodies to rasters (`nhrug`, `wbg`)

3. **getHruImperv**
   - Generates impervious raster labeled with HRU IDs
   - **Inputs**
     - `nhrug`
     - Impervious surface raster
     - Impervious surface percentage (X)
   - For each cell with imperviousness > X%, assign a 1, else NULL
   - Populate each cell with 1 with the `hru_id`
   - **Outputs**
     - `hruImperv`: raster with impervious areas labeled with HRU IDs

4. **getSegBuff**
   - Buffers the stream segments, converts to grid, assigns HRU IDs within the
     buffer part of grid, NAs outside
   - **Inputs**
     - `nhrug`
     - `nsegments` vector
     - Buffer distance
   - Generate buffer around each `nsegment`, rasterize
   - Populate each cell within buffer with `hru_id`, outside buffer as NA
   - **Output**
     - `outEucDistance`

5. **getwaterbodiesInHRUsGrid**
   - Generates an HRU waterbody grid with each waterbody labeled with HRU IDs,
     then relabels with RegionGroup Function (not exactly sure what the output
     here is, may have to experiment with ArcPro functions "RegionGroup" and
     "Link"; see ~line 611)
   - **Inputs**
     - `wbodg`
   - Regions waterbodies for a minimum of 8 (8 cells surrounding the middle
     pixel in a 3×3 window); other arguments are kept as default. A "link"
     field adds a unique identifier to each "grouped" waterbody
   - This raster is derived with the "lookup" function applied to the "link"
     field
   - **Outputs**
     - `wbodsHruGrid`

---

### Level Two — Delineating depression storage features and their upslope surface contributing areas

6. **getDprst** (Cory: Start 2/17, Finished 3/31)
   - Generates depression storage raster with depressions labeled as HRU IDs
     and all other areas NULL. Depressions are only areas outside of impervious
     zones and outside the stream buffer.
   - **Inputs**
     - `hruImperv`
     - `outEucDistance`
     - `wbodsHruGrid`
   - **This is really the most confusing part of the code (lines 564–586)**
   - Populate values from `wbodsHruGrid` for areas outside non-impervious area
     only (`getHRUImperv` output); output is called `junk1` in the code
   - Exclude `junk1` from `getSegBuff` from `junk1`, this results in `junk2`
     temporary layer (waterbodies outside impervious surfaces and streambuffer
     populated with their HRU IDs)
   - Put waterbody link IDs from `junk2` into a list
   - Populate waterbodies outside the impervious areas and stream buffers with
     their HRU IDs (CON statement on line 582)
   - **Output**
     - `dprst`

7. **getHruSro_to_dprst** (Andy: start 2/17, finished 3/31)
   - Generates a raster of areas upstream of waterbodies/surface depressions
   - **Input**
     - `nhrug`
     - `dprst`
     - FDR
   - Runs watersheds upstream of depressions returned from `getDprst` function
     (`res1` in script, lines 191–197)
   - Labels areas outside of depressions in `dprst` with 0 (`res2`), lines 200–209
   - Uses a `con` (line 214) to put HRU IDs for watershed areas upstream of
     depressions (`hruSro_toDprst` grid name)
   - **Output**
     - `hruSro_to_dprst`
   - **Andy B.**
     - Identify depressions based on `featureid` negative values for catchments
     - Identify waterbodies off of segments that are incorporated into the
       network topology, delineate upstream
   - **gfv2-params**: the open-source port computes the upslope-of-depression
     mask in-process via `src/gfv2_params/d8_routing.py`
     (`drains_to_dprst_kernel`), a cycle-safe O(N) ESRI-D8 traversal. It
     replaced WhiteboxTools `Watershed`, which hung on CONUS VPU 2. The
     per-VPU tiling and `drains_to_dprst.tif` output schema are unchanged. See
     `docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md`.

8. ~~**getDprst_frac**~~
   - Possibly Deprecated / not used

---

### Level Three — Determination of surface runoff fractions to impervious, non-impervious, and on-stream areas

**Note:** FDR raster is an input at this level.

9. **GetPervAreaTotal** (Cory: Starting week of 3/30)
   - Get raster with HRU IDs in areas that are not impervious or depressions
   - **Inputs**
     - `hruImperv`
     - `dprst`
   - Basically a `con` operation to label stuff
   - **Outputs**
     - `pervAreaTotal`

10. **getOnStreamStor** (Cory: starting week of 3/30)
    - Not quite sure of the point of this one.
    - **Inputs**
      - `hruImperv`
      - `wbodsHruGrid`
      - `dprst`
    - Just two `con` statements: first identifies waterbodies outside of
      depressions, second outside of impervious areas, and returns those (I
      guess)
    - **Output**
      - `nondprstWbodies`

---

### Level Four — Derivation of different contributing area maps and `smidx` and `carea_max` parameters that control runoff partitioning

**Note:** TWI raster is an input at this level.

11. **getCarea_map** — *implemented (issue #61)*
    - Returns a raster of HRU IDs where there is `pervAreaTotal` and TWI is
      above a certain value, otherwise returns NULL. For areas less than the
      TWI threshold that are also classified as on-stream storage, the HRU ID
      is also returned.
    - **Inputs**
      - `pervAreaTotal`
      - TWI
      - `OnStreamStor`
      - Threshold (TWI threshold value)
    - CON statement: where TWI exceeds threshold, give HRU ID; else where there
      is on-stream storage, give HRU ID
    - **Output**
      - `Carea_map`
    - **gfv2-params**: produced as a uint8 binary mask (1 = cell qualifies,
      255 = nodata) by the `carea_map` step of
      [`scripts/build_depstor_rasters.py`](../scripts/build_depstor_rasters.py)
      (implementation in
      [`src/gfv2_params/depstor_builders/carea_map.py`](../src/gfv2_params/depstor_builders/carea_map.py))
      via [`compute_carea_map_binary`](../src/gfv2_params/depstor.py). The
      ArcPy HRU-ID burn is dropped — HRU identity is recovered downstream by
      `derive_depstor_params.py --mode zonal` via gdptools polygon overlay.
      Built at both PRMS thresholds (8.0 and 15.6) in one pass.

12. **getSro_to_dprst_perv** — *implemented (issue #61)*
    - Surface runoff to depression storage for pervious surfaces. Returns
      proportion of pervious area draining to depression storage.
    - **Inputs**
      - `hruSro_to_dprst`
      - `pervAreaTotal`
    - Creates raster representing pervious areas contributing to depressions
    - Rest of the code is table joins and counts. A lot of joins and such — we
      need to document this section better.
    - **Outputs**
      - `Sro_to_dprst_perv` (dataframe)
    - **gfv2-params**: per-cell intersection built by the `drains_perv` step
      of [`scripts/build_depstor_rasters.py`](../scripts/build_depstor_rasters.py)
      (implementation in
      [`src/gfv2_params/depstor_builders/intersect.py`](../src/gfv2_params/depstor_builders/intersect.py),
      configured under `steps.drains_perv` in
      [`configs/depstor/depstor_rasters.yml`](../configs/depstor/depstor_rasters.yml)).
      Per-HRU ratio computed by `derive_depstor_params.py --mode ratios` via
      [`compute_ratio`](../src/gfv2_params/depstor_ratios.py) from the
      `drains_perv_frac` and `perv_frac` count CSVs in
      `{fabric}/params/merged/_intermediates/`:
      `sro_to_dprst_perv = count(drains_perv_binary per HRU) / count(perv_binary per HRU)`.

13. **getSro_to_dprst_imperv** — *implemented (issue #61)*
    - Calculates Surface runoff to depression storage for impervious surfaces.
      Returns proportion of impervious area draining to depression storage.
    - **Inputs**
      - `hruImperv`
      - `hruSro_to_dprst`
    - Basically same as `getSro_to_dprst_perv` but impervious
    - **Outputs**
      - `Sro_to_dprst_imperv` (dataframe)
    - **gfv2-params**: same pipeline as item 12, swapping `perv_binary` for
      `imperv_binary`. We implement the *documented intent* (parallel to
      perv) rather than the ArcPy active code at `docs/0b_TB_depr_stor.py:142-149`,
      which appears to be buggy (its commented-out form on line 128 matches
      the doc-stated formula).

---

### Level Five — Generating the parameters

14. **getZonecount**
    - Get zone counts from a grid per zone ID
    - **Inputs**
      - Grid
    - **Outputs**
      - `zoneCounts` (data frame)

15. **getCarea** — *implemented (issue #61)*
    - Function: Computes proportion of pervious area that contributes to the
      stream
    - **Inputs**
      - Threshold
      - `Carea_map` (from threshold)
      - `pervAreaTotal`
    - Derives `carea_map` with a given threshold (see `get_smidx` and
      `carea_max` function)
    - Gets zone count for `perviousAreatotal` by HRU
    - Get zone count for derived `carea_map` for given threshold
    - Does some joins and easy math
    - **Outputs**
      - `Carea` (dataframe)
    - **gfv2-params**: pair-wise per-HRU ratio computed by
      `derive_depstor_params.py --mode ratios` via
      [`compute_ratio`](../src/gfv2_params/depstor_ratios.py) from the
      `carea_t<X>_frac` and `perv_frac` count CSVs in
      `{fabric}/params/merged/_intermediates/`:
      `ratio = count(carea_map_t<X>_binary per HRU) / count(perv_binary per HRU)`,
      clamped at 1.0.

16. **getSmidx** — *implemented (issue #61)*
    - Computes Soil Moisture Index Coefficient with `getCarea` function and a
      TWI threshold of **15.6**
    - Returns dataframe
    - **gfv2-params**: `nhm_smidx_coef_params.csv` written by
      `derive_depstor_params.py --mode ratios` into
      `{fabric}/params/merged/`. `smidx_exp` is *not* produced by this
      pipeline (the ArcPy source only derives `smidx_coef`); it needs a
      separate sourcing decision (typically a fixed NHM default).

17. **getCarea** *(reused; presumably `getCarea_max`)* — *implemented (issue #61)*
    - Computes Soil Moisture Index Coefficient with `getCarea` function and a
      TWI threshold of **8**
    - Returns dataframe
    - **gfv2-params**: `nhm_carea_max_params.csv` written by
      `derive_depstor_params.py --mode ratios` into
      `{fabric}/params/merged/`.

18. **getHRU_percent_imperv** — *implemented (PR #72)*
    - Compute the impervious fraction of the HRUs
    - **Inputs**
      - `hruImperv`
    - Weird stuff, maybe we can do it better
    - **Output**
      - dataframe
    - **gfv2-params**: `nhm_hru_percent_imperv_params.csv` written by
      `derive_depstor_params.py --mode ratios` into
      `{fabric}/params/merged/`. Computed as
      `count(imperv_binary per HRU) / count(land_mask per HRU)`
      from the `imperv_frac` and `hru_total` count CSVs in
      `{fabric}/params/merged/_intermediates/`. Bounded in [0, 1] by
      construction; no clamp.

19. **getDprst_frac (PRMS dprst_frac)** — *implemented (PR #72)*
    - Per-HRU fraction of land area classified as depression storage.
    - **gfv2-params**: `nhm_dprst_frac_params.csv` written by
      `derive_depstor_params.py --mode ratios` into
      `{fabric}/params/merged/`. Computed as
      `count(dprst_binary per HRU) / count(land_mask per HRU)`
      from the `dprst_frac` (count) and `hru_total` count CSVs in
      `{fabric}/params/merged/_intermediates/`. Bounded in [0, 1] by
      construction; no clamp.
    - **Note on the filename collision:** the intermediate count CSV in
      `_intermediates/` is also named `nhm_dprst_frac_params.csv` (same
      filename, different subdir; `count` column = cell count, not a
      fraction). Different consumers should pull from the right subdir:
      PRMS-ready ratio from `merged/`, intermediate count from
      `merged/_intermediates/`.

---

### Output layout (post-PR #72)

The two orchestrators
[`scripts/build_depstor_rasters.py`](../scripts/build_depstor_rasters.py)
and [`scripts/derive_depstor_params.py`](../scripts/derive_depstor_params.py)
produce, per fabric:

```
{fabric}/depstor_rasters/           # 13 generation outputs
├── land_mask.tif                   # (built first; every other raster masks against it)
├── imperv_binary.tif
├── stream_buffer.tif
├── wbody_binary.tif    wbody_regions.tif
├── dprst_binary.tif    onstream_binary.tif
├── perv_binary.tif
├── drains_to_dprst.tif
├── drains_perv_binary.tif    drains_imperv_binary.tif
└── carea_map_t8_binary.tif    carea_map_t156_binary.tif

{fabric}/params/merged/             # 6 final PRMS-ready ratio CSVs (all [0, 1])
├── nhm_sro_to_dprst_perv_params.csv
├── nhm_sro_to_dprst_imperv_params.csv
├── nhm_carea_max_params.csv
├── nhm_smidx_coef_params.csv
├── nhm_hru_percent_imperv_params.csv    # NEW in PR #72
├── nhm_dprst_frac_params.csv            # NEW in PR #72
└── _intermediates/                       # 10 per-fraction count CSVs (NOT [0, 1])
    ├── nhm_perv_frac_params.csv      nhm_imperv_frac_params.csv
    ├── nhm_dprst_frac_params.csv     nhm_onstream_storage_frac_params.csv
    ├── nhm_drains_perv_frac_params.csv   nhm_drains_imperv_frac_params.csv
    ├── nhm_drains_to_dprst_frac_params.csv
    ├── nhm_carea_t8_frac_params.csv  nhm_carea_t156_frac_params.csv
    └── nhm_hru_total_count_params.csv    # NEW in PR #72; denominator for the area-fraction ratios
```

The 13 PRMS depstor parameters not produced here (`dprst_frac_open`,
`dprst_frac_clos`, `dprst_depth_avg`, `dprst_seep_rate_clos`,
`dprst_et_coef`, `op_flow_thres`, `va_open_exp`, `va_clos_exp`,
`dprst_frac_init`, `imperv_stor_max`, `smidx_exp`) come from NHM defaults
or a separate sourcing decision — see the "Out-of-scope PRMS dprst params"
cell in `notebooks/qaqc_depstor_vpu01.ipynb`.

## Calibrating the TWI threshold (`carea_max` / `smidx_coef`)

`carea_max`/`smidx_coef` are *a priori* estimates — the fraction of each HRU's
pervious area whose TWI clears a cutoff (or sits on-stream storage). The cutoff is
data-derived (a percentile of valid-land TWI; the default is the inversion of the
legacy 8.0/15.6 through VPU 01's ArcPy TWI CDF — see #55). The original developer
*eyeballed* 8.0/15.6, so picking a final value is an iteration. The sweep tool lets
you do that without rerunning the cluster pipeline:

1. **Build the per-fabric artifact once** (per-HRU pervious-TWI histograms):
   ```bash
   sbatch -p cpu -A impd --time=01:00:00 --ntasks=1 --cpus-per-task=4 --mem=48G \
     --output=logs/job_%j.out --error=logs/job_%j.err \
     --wrap="pixi run --as-is python scripts/build_carea_twi_artifact.py --fabric <fabric>"
   # -> {data_root}/{fabric}/params/carea_twi_artifact.npz
   ```
2. **Iterate in the notebook** (`notebooks/carea_threshold_sweep.py`, marimo): set a
   candidate as an absolute TWI value **or** a percentile (live two-way readout),
   inspect the per-HRU distribution, spatial map, optional legacy/gauge diffs, and
   the sensitivity sweep curve. Evaluating a candidate is instant (no cluster run).
3. **Persist the chosen value** — paste the printed config snippet into either:
   - `configs/shared_rasters/shared_rasters.yml` → `twi_reference` `percentiles:`
     (percentile path; rerun the `twi_reference` step + depstor `carea_map`), or
   - `configs/depstor/depstor_rasters.yml` → `carea_map` `thresholds:` +
     `threshold_mode: absolute` (eyeball path; pairs with the ArcPy `twi.vrt`).

The artifact mirrors `compute_carea_map_binary`'s per-cell mask logic; a swept
threshold matches production within ~one histogram bin (~0.05 TWI) PLUS a small
per-HRU aggregation difference (the artifact uses whole-cell `all_touched`
rasterization vs production's coverage-weighted exactextract). Empirically the two
agree to <1% on oregon. Snap a threshold to a bin edge for the closest absolute match.
