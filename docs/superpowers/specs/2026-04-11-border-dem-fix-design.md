# Border DEM Fill Fix — Design Specification

**Date:** 2026-04-11
**Status:** Draft
**Scope:** Fix elevation, slope, and aspect for Canada/Mexico border HRUs

## Problem Statement

PR #35 added Copernicus GLO-30 border DEM fill for Canada/Mexico HRUs, but
border HRUs are still receiving KNN-interpolated values instead of actual
elevation-derived parameters. Three root causes were identified:

1. **VRT source ordering is backwards** — `build_vrt.py` lists NHDPlus files
   first and Copernicus fill files last. GDAL VRT compositing is last-source-wins
   (not first-source-wins as the code comments claim), so Copernicus data
   overwrites NHDPlus in the overlap zone (41°N–49°N for Canada, 25°N–33°N for
   Mexico) rather than filling gaps.

2. **Slope/aspect computed independently per source** — `build_border_dem.py`
   computes slope/aspect on the Copernicus elevation raster alone, and
   `compute_slope_aspect.py` computes on each VPU DEM alone. At the boundary
   between the two sources, RichDEM's 3x3 moving window hits nodata on one side,
   producing edge artifacts in the derivative rasters.

3. **Single monolithic raster approach** — The Copernicus warp produces one
   GeoTIFF covering all of Canada (41–55°N) and Mexico (25–33°N). While this ran
   successfully on HPC, it is fragile for memory and processing time.

## Issues Inventory

| # | Issue | Severity | Resolution |
|---|---|---|---|
| 1 | VRT source ordering backwards | Critical | Fix in this design |
| 2 | Single monolithic raster (fragile) | Medium | Accepted (ran on HPC) |
| 3 | Overlap zone generous (41°N into CONUS) | Low | Harmless with correct ordering |
| 4 | Lithology shapefile is CONUS-only | Future | Out of scope |
| 5 | Soils rasters are CONUS-only | Future | Out of scope |
| 6 | KNN fill is a band-aid (k=1) | Context | Motivates this fix |
| 7 | Slope/aspect seam artifacts at boundary | Critical | Fix in this design |

## Design

### Change 1: Fix VRT Source Ordering

**File:** `scripts/build_vrt.py`

Reverse the source file ordering so that fill sources are listed first and
NHDPlus primary sources are listed last. GDAL VRT renders sources in order,
with later sources overwriting earlier ones (except at nodata pixels). By
listing fill first and primary last, NHDPlus data takes priority wherever it
has valid values; Copernicus only shows through where NHDPlus has nodata.

```python
# Current (broken): primary first, fill last -> fill overwrites primary
source_files = primary_files + fill_files

# Fixed: fill first, primary last -> primary overwrites fill
source_files = fill_files + primary_files
```

### Change 2: Padded Copernicus Slope/Aspect Computation

**File:** `scripts/build_border_dem.py`

Modify the slope/aspect computation to use a composite elevation surface that
includes NHDPlus data in the overlap zone, eliminating seam artifacts.

**Current flow:**
1. Download Copernicus tiles
2. Warp to EPSG:5070 at 30m -> single elevation GeoTIFF
3. Compute slope/aspect from Copernicus-only elevation via RichDEM

**New flow:**
1. Download Copernicus tiles (unchanged)
2. Warp to EPSG:5070 at 30m -> single elevation GeoTIFF (unchanged)
3. Build a temporary composite elevation raster:
   - Use GDAL BuildVRT to composite the warped Copernicus elevation (listed
     first, lower priority) with the NHDPlus VPU `_fixed_` tiles (listed last,
     higher priority)
   - The composite is seamless: NHDPlus values in CONUS, Copernicus values in
     Canada/Mexico, continuous data at the boundary
   - `srcNodata="-9999"` for both sources ensures nodata pixels are transparent
4. Compute slope/aspect via RichDEM on the composite raster
5. Mask the slope/aspect output to only retain pixels in the fill zone — pixels
   where the original Copernicus elevation has valid data and NHDPlus does not.
   This prevents the fill raster from containing redundant values in the overlap
   zone that could conflict with per-VPU RichDEM outputs.

**Memory consideration:** The composite raster has the same extent as the
current Copernicus warp output, which already ran successfully on HPC. The
NHDPlus overlay fills values in the overlap zone but does not increase the
raster dimensions.

**Dependency:** This step must run after `compute_slope_aspect.py` (per-VPU),
because it needs the NHDPlus `_fixed_` elevation tiles. The existing pipeline
ordering already satisfies this (Stage 1 before Stage 1b).

### Change 3: Visualization Notebook

**File:** `notebooks/check_border_dem.py` (new Marimo notebook)

Interactive notebook to validate the corrected border DEM fill and its
derivatives.

**Panels:**

1. **Elevation continuity map** — Plot the merged elevation VRT zoomed to a
   border region (e.g., VPU 04 / Great Lakes touching Canada). Verify no
   discontinuity or seam at the NHDPlus/Copernicus boundary.

2. **Slope/aspect seamlessness** — Same zoom region, plot slope and aspect.
   Verify no edge artifacts at the boundary. Compare before/after if old
   outputs are available.

3. **Border HRU parameter check** — Load the fabric GeoPackage, filter to
   border HRUs that extend beyond CONUS. Plot their elevation/slope/aspect
   zonal means. Flag HRUs that still have missing values or suspiciously
   uniform KNN-filled values.

4. **Difference map** — In the overlap zone, compute the difference between
   NHDPlus and Copernicus elevation. Quantifies agreement between sources and
   helps identify areas where the seam might still be visible in derivatives.

**Interactive elements (Marimo):**
- Dropdown to select border region (Canada-East, Canada-West, Mexico)
- Dropdown to select raster layer (elevation, slope, aspect)
- Slider for zoom extent

**Best practices for large raster visualization:**
- Use `rioxarray.open_rasterio` with `overview_level` or coarsen/slice to
  reduce resolution to screen-appropriate levels (~2000x2000 pixels max)
- Use `imshow` with `rasterized=True` to prevent matplotlib from creating
  vector objects for millions of pixels
- Compute block-wise statistics for difference maps and histograms rather than
  loading entire arrays
- Use context managers for raster dataset handles
- Explicit `plt.close()` after rendering to free memory

**Dependencies:** `rioxarray`, `matplotlib`, `geopandas`, `marimo`

## Files Changed

| File | Action | Description |
|---|---|---|
| `scripts/build_vrt.py` | Modify | Reverse source ordering (fill first, primary last) |
| `scripts/build_border_dem.py` | Modify | Add NHDPlus padding before slope/aspect; mask to fill zone |
| `notebooks/check_border_dem.py` | Create | Marimo validation notebook |

## Files NOT Changed

- `src/gfv2_params/download/copernicus_dem.py` — download logic is correct
- `scripts/compute_slope_aspect.py` — per-VPU pipeline unchanged
- `scripts/merge_rpu_by_vpu.py` — unchanged
- `scripts/create_zonal_params.py` — reads from VRTs, which will now be correct
- `scripts/create_ssflux_params.py` — consumes slope params downstream

## Known Future Work

- **Issue 4:** Extend lithology coverage (Lithology_exp_Konly_Project.shp) to
  Canada/Mexico for subsurface flux parameters
- **Issue 5:** Extend soils rasters (TEXT_PRMS.tif, RootDepth.tif, AWC.tif) to
  Canada/Mexico for soils and soil_moist_max parameters

## Fallback Approaches

If the padded RichDEM computation fails (e.g., OOM on HPC), two alternatives
are available:

**Fallback A — gdaldem:** Use `gdaldem slope` and `gdaldem aspect` on the
merged elevation VRT. These tools process in scanline/block mode with no memory
limit. The tradeoff is a slight algorithm difference vs RichDEM (Horn's method
vs RichDEM's implementation), which could produce marginally different values
at the NHDPlus/Copernicus boundary compared to per-VPU outputs.

**Fallback C — tiled with overlap:** Divide the merged elevation VRT into
manageable tiles with ~10-pixel overlap, compute slope/aspect per tile via
RichDEM, crop the overlap margins, and mosaic. Most complex to implement but
preserves algorithmic consistency with per-VPU outputs.
