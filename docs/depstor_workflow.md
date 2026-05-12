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
      255 = nodata) by [`scripts/build_depstor_carea_map.py`](../scripts/build_depstor_carea_map.py)
      via [`compute_carea_map_binary`](../src/gfv2_params/depstor.py). The
      ArcPy HRU-ID burn is dropped — HRU identity is recovered downstream by
      `create_zonal_params.py` via gdptools polygon overlay. Built at both
      PRMS thresholds (8.0 and 15.6) in one pass.

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
    - **gfv2-params**: per-cell intersection built by [`scripts/build_depstor_intersect.py`](../scripts/build_depstor_intersect.py)
      (config: `depstor_drains_perv_raster.yml`). Per-HRU ratio computed by
      [`scripts/derive_depstor_ratios.py`](../scripts/derive_depstor_ratios.py)
      from the `drains_perv_frac` and `perv_frac` merged fraction CSVs:
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
      [`scripts/derive_depstor_ratios.py`](../scripts/derive_depstor_ratios.py)
      from merged fraction CSVs:
      `ratio = count(carea_map_t<X>_binary per HRU) / count(perv_binary per HRU)`,
      clamped at 1.0.

16. **getSmidx** — *implemented (issue #61)*
    - Computes Soil Moisture Index Coefficient with `getCarea` function and a
      TWI threshold of **15.6**
    - Returns dataframe
    - **gfv2-params**: `nhm_smidx_coef_params.csv` written by
      [`scripts/derive_depstor_ratios.py`](../scripts/derive_depstor_ratios.py).
      `smidx_exp` is *not* produced by this pipeline (the ArcPy source only
      derives `smidx_coef`); it needs a separate sourcing decision (typically
      a fixed NHM default).

17. **getCarea** *(reused; presumably `getCarea_max`)* — *implemented (issue #61)*
    - Computes Soil Moisture Index Coefficient with `getCarea` function and a
      TWI threshold of **8**
    - Returns dataframe
    - **gfv2-params**: `nhm_carea_max_params.csv` written by
      [`scripts/derive_depstor_ratios.py`](../scripts/derive_depstor_ratios.py).

18. **getHRU_percent_imperv**
    - Compute the impervious fraction of the HRUs
    - **Inputs**
      - `hruImperv`
    - Weird stuff, maybe we can do it better
    - **Output**
      - dataframe
