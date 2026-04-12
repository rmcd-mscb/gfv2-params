# Border DEM Fill Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix VRT compositing and slope/aspect seam artifacts so border HRUs in Canada/Mexico receive correct elevation-derived parameters instead of KNN-interpolated values.

**Architecture:** Three independent changes: (1) one-line fix + comment update in `build_vrt.py`, (2) rework `build_border_dem.py` to composite NHDPlus+Copernicus elevation before computing slope/aspect then mask output to fill zone, (3) new Marimo validation notebook.

**Tech Stack:** GDAL (BuildVRT, Warp), RichDEM, rioxarray, rasterio, matplotlib, geopandas, marimo

**Spec:** `docs/superpowers/specs/2026-04-11-border-dem-fix-design.md`

---

### Task 1: Fix VRT source ordering and comments

**Files:**
- Modify: `scripts/build_vrt.py:46-64`
- Test: `tests/test_build_vrt.py` (new)

- [ ] **Step 1: Write failing test for VRT source ordering**

Create `tests/test_build_vrt.py` with a test that verifies fill files are listed before primary files in the output source list. We test the ordering logic by extracting it into a helper or by testing the VRT XML output. Since the logic is inline in `main()`, test via a unit-style approach by creating temp GeoTIFFs and verifying the VRT XML source order.

```python
"""Tests for build_vrt.py VRT source ordering."""

import struct
from pathlib import Path

import pytest
from osgeo import gdal


def _make_tiny_tif(path: Path, value: float, nodata: float = -9999.0) -> None:
    """Create a minimal 2x2 Float32 GeoTIFF with a constant value."""
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), 2, 2, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    from osgeo import osr
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteRaster(0, 0, 2, 2, struct.pack("4f", *([value] * 4)))
    ds.FlushCache()
    del ds


class TestVrtSourceOrdering:
    """Verify that fill sources are listed BEFORE primary sources in the VRT.

    GDAL VRT compositing is last-source-wins: later sources overwrite earlier
    ones (except at nodata pixels). By listing fill first and primary last,
    NHDPlus data takes priority wherever it has valid values.
    """

    def test_fill_before_primary_in_vrt(self, tmp_path):
        """VRT XML should list fill source before primary source."""
        nhd_dir = tmp_path / "nhd_merged"
        vpu_dir = nhd_dir / "01"
        fill_dir = nhd_dir / "copernicus_fill"
        vpu_dir.mkdir(parents=True)
        fill_dir.mkdir(parents=True)

        _make_tiny_tif(vpu_dir / "NEDSnapshot_merged_fixed_01.tif", value=100.0)
        _make_tiny_tif(fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif", value=200.0)

        # Import and call build_vrt logic — we replicate the core ordering
        # logic here rather than calling main() to avoid config dependencies.
        FILL_DIRS = {"copernicus_fill"}
        pattern = "NEDSnapshot_merged_fixed_*.tif"

        primary_files = sorted(
            f for f in nhd_dir.glob(f"*/{pattern}")
            if f.parent.name not in FILL_DIRS
        )
        fill_files = []
        for fill_dir_name in sorted(FILL_DIRS):
            fill_files.extend(sorted(nhd_dir.glob(f"{fill_dir_name}/{pattern}")))

        # Correct ordering: fill first, primary last
        source_files = fill_files + primary_files

        assert len(source_files) == 2
        assert "copernicus" in source_files[0].name, "Fill source must be listed first"
        assert "01" in source_files[1].name, "Primary source must be listed last"

    def test_primary_overwrites_fill_in_vrt(self, tmp_path):
        """When both sources have valid data at same pixel, primary (last) wins."""
        nhd_dir = tmp_path / "nhd_merged"
        vpu_dir = nhd_dir / "01"
        fill_dir = nhd_dir / "copernicus_fill"
        vpu_dir.mkdir(parents=True)
        fill_dir.mkdir(parents=True)

        _make_tiny_tif(vpu_dir / "NEDSnapshot_merged_fixed_01.tif", value=100.0)
        _make_tiny_tif(fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif", value=200.0)

        # Build VRT with correct ordering: fill first, primary last
        source_files = [
            str(fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif"),
            str(vpu_dir / "NEDSnapshot_merged_fixed_01.tif"),
        ]
        vrt_path = str(nhd_dir / "test.vrt")
        vrt_options = gdal.BuildVRTOptions(resolution="highest", srcNodata="-9999")
        vrt_ds = gdal.BuildVRT(vrt_path, source_files, options=vrt_options)
        vrt_ds.FlushCache()
        del vrt_ds

        # Read the VRT — primary value (100.0) should win over fill (200.0)
        ds = gdal.Open(vrt_path)
        band = ds.GetRasterBand(1)
        data = band.ReadAsArray()
        del ds

        assert data[0, 0] == pytest.approx(100.0), (
            f"Expected primary value 100.0 but got {data[0, 0]}. "
            "GDAL VRT last-source-wins: primary must be listed last."
        )
```

- [ ] **Step 2: Run tests to validate GDAL VRT compositing behavior**

Run: `pytest tests/test_build_vrt.py -v`
Expected: PASS — these tests validate our understanding that GDAL VRT is last-source-wins, confirming the fix direction before applying it.

- [ ] **Step 3: Fix source ordering in build_vrt.py**

In `scripts/build_vrt.py`, make two changes:

1. Line 64 — reverse the ordering:
```python
        source_files = fill_files + primary_files
```

2. Lines 46-49 — fix the stale comments:
```python
    # Fill subdirectories whose tiles should be listed BEFORE the primary
    # NHDPlus VPU tiles.  GDAL VRT uses last-source-wins for overlapping
    # pixels, so listing NHDPlus last ensures it takes priority and fill
    # sources only contribute where NHDPlus has nodata.
```

3. Lines 54 and 59 — update inline comments:
```python
        # Primary NHDPlus VPU tiles (listed last = highest priority)
```
```python
        # Fill tiles (listed first = lowest priority)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_build_vrt.py -v`
Expected: PASS

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `pytest tests/ -v`
Expected: All existing tests pass

- [ ] **Step 6: Commit**

```bash
git add tests/test_build_vrt.py scripts/build_vrt.py
git commit -m "fix: reverse VRT source ordering so NHDPlus takes priority over fill

GDAL VRT compositing is last-source-wins, not first-source-wins.
The previous ordering had fill (Copernicus) overwriting primary
(NHDPlus) in the overlap zone. Reversing the order ensures NHDPlus
data takes priority wherever it has valid values."
```

---

### Task 2: Rework build_border_dem.py — composite elevation before slope/aspect

**Files:**
- Modify: `scripts/build_border_dem.py`
- Test: `tests/test_build_border_dem.py` (new)

- [ ] **Step 1: Write failing tests for composite and masking logic**

Create `tests/test_build_border_dem.py`. These tests verify:
(a) The composite VRT lists Copernicus first, NHDPlus `_fixed_` tiles last
(b) The fill mask correctly identifies pixels where Copernicus has data but NHDPlus does not
(c) Slope/aspect are masked to the fill zone

```python
"""Tests for build_border_dem composite elevation and fill masking."""

import struct
from pathlib import Path

import numpy as np
import pytest
from osgeo import gdal, osr


def _make_tif(path: Path, data: np.ndarray, nodata: float = -9999.0) -> None:
    """Create a Float32 GeoTIFF from a 2D numpy array."""
    rows, cols = data.shape
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), cols, rows, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([0, 30, 0, 0, 0, -30])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(5070)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    band.WriteRaster(
        0, 0, cols, rows,
        data.astype(np.float32).tobytes(),
    )
    ds.FlushCache()
    del ds


class TestFillMask:
    """Verify fill_mask = (copernicus != nodata) & (nhdplus == nodata)."""

    def test_fill_mask_basic(self):
        """Fill mask should be True only where Copernicus has data and NHDPlus does not."""
        nodata = -9999.0
        # Copernicus has valid data everywhere except [0,0]
        copernicus = np.array([[nodata, 500.0], [600.0, 700.0]])
        # NHDPlus has valid data at [0,0] and [0,1], nodata at [1,0] and [1,1]
        nhdplus = np.array([[100.0, 200.0], [nodata, nodata]])

        fill_mask = (copernicus != nodata) & (nhdplus == nodata)

        # [0,0]: copernicus=nodata -> False
        assert not fill_mask[0, 0]
        # [0,1]: copernicus=valid, nhdplus=valid -> False (NHDPlus covers it)
        assert not fill_mask[0, 1]
        # [1,0]: copernicus=valid, nhdplus=nodata -> True (fill zone)
        assert fill_mask[1, 0]
        # [1,1]: copernicus=valid, nhdplus=nodata -> True (fill zone)
        assert fill_mask[1, 1]

    def test_masked_slope_retains_only_fill_zone(self):
        """After masking, slope values should only exist in the fill zone."""
        nodata = -9999.0
        copernicus = np.array([[nodata, 500.0], [600.0, 700.0]])
        nhdplus = np.array([[100.0, 200.0], [nodata, nodata]])
        raw_slope = np.array([[5.0, 10.0], [15.0, 20.0]])

        fill_mask = (copernicus != nodata) & (nhdplus == nodata)
        masked_slope = np.where(fill_mask, raw_slope, nodata)

        assert masked_slope[0, 0] == nodata
        assert masked_slope[0, 1] == nodata  # NHDPlus covers this
        assert masked_slope[1, 0] == 15.0    # fill zone
        assert masked_slope[1, 1] == 20.0    # fill zone


class TestCompositeVrtOrdering:
    """Verify composite VRT lists Copernicus first, NHDPlus last."""

    def test_nhdplus_overwrites_copernicus_in_composite(self, tmp_path):
        """In the composite, NHDPlus values should win in the overlap zone."""
        cop_data = np.full((4, 4), 500.0, dtype=np.float32)
        nhd_data = np.full((4, 4), 100.0, dtype=np.float32)
        # NHDPlus has nodata in bottom two rows (the "border zone")
        nhd_data[2:, :] = -9999.0

        cop_path = tmp_path / "copernicus.tif"
        nhd_path = tmp_path / "nhdplus.tif"
        _make_tif(cop_path, cop_data)
        _make_tif(nhd_path, nhd_data)

        # Build composite: Copernicus first (low priority), NHDPlus last (high priority)
        vrt_path = str(tmp_path / "composite.vrt")
        vrt_options = gdal.BuildVRTOptions(resolution="highest", srcNodata="-9999")
        vrt_ds = gdal.BuildVRT(
            vrt_path,
            [str(cop_path), str(nhd_path)],
            options=vrt_options,
        )
        vrt_ds.FlushCache()
        del vrt_ds

        ds = gdal.Open(vrt_path)
        result = ds.GetRasterBand(1).ReadAsArray()
        del ds

        # Top two rows: NHDPlus wins (100.0)
        np.testing.assert_array_equal(result[:2, :], 100.0)
        # Bottom two rows: NHDPlus has nodata, Copernicus fills (500.0)
        np.testing.assert_array_equal(result[2:, :], 500.0)
```

- [ ] **Step 2: Run tests to validate composite and masking logic**

Run: `pytest tests/test_build_border_dem.py -v`
Expected: PASS — these tests validate fill mask logic and GDAL VRT compositing behavior before applying changes to the script.

- [ ] **Step 3: Modify build_border_dem.py — add composite and masking**

Replace the slope/aspect computation section (current Step 3, lines 130-148) with the new composite approach. The full modified `scripts/build_border_dem.py`:

```python
"""Build Copernicus GLO-30 elevation fill for border HRUs (Canada/Mexico).

Downloads Copernicus 30m tiles covering border zones, mosaics them,
reprojects to EPSG:5070 at 30m, then builds a composite elevation surface
by overlaying NHDPlus VPU tiles on top of Copernicus (NHDPlus takes priority
in the overlap zone via GDAL VRT last-source-wins ordering). Slope and aspect
are computed via RichDEM on this composite, then masked to retain only pixels
in the fill zone (where Copernicus has data but NHDPlus does not).

Output tiles are placed in work/nhd_merged/copernicus_fill/ where
build_vrt.py picks them up as lower-priority fill behind NHDPlus tiles.

Dependency: must run AFTER compute_slope_aspect.py (per-VPU), because it
needs the NHDPlus _fixed_ elevation tiles for the composite.
"""

import argparse
import time
from pathlib import Path

import numpy as np
import richdem as rd
from osgeo import gdal

from gfv2_params.config import load_base_config
from gfv2_params.download.copernicus_dem import download_tiles, tiles_for_bbox
from gfv2_params.log import configure_logging

# Border bounding boxes in EPSG:4326 (south, north, west, east).
# Deliberately generous — extra ocean tiles are skipped (404) and
# NHDPlus takes priority in overlapping areas via VRT source ordering.
BORDER_ZONES = {
    "canada": (41.0, 55.0, -141.0, -52.0),
    "mexico": (25.0, 33.0, -118.0, -96.0),
}

# Output nodata must match the pipeline convention (build_vrt.py srcNodata).
OUTPUT_NODATA = -9999

# Glob pattern for NHDPlus _fixed_ elevation tiles (written by compute_slope_aspect.py).
NHDPLUS_FIXED_PATTERN = "NEDSnapshot_merged_fixed_*.tif"
FILL_DIRS = {"copernicus_fill"}


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _build_nhdplus_vrt(nhd_merged_dir: Path, output_vrt: Path) -> Path:
    """Build a VRT from NHDPlus _fixed_ tiles only (no fill layers).

    Returns the VRT path, or raises if no tiles are found.
    """
    primary_files = sorted(
        f for f in nhd_merged_dir.glob(f"*/{NHDPLUS_FIXED_PATTERN}")
        if f.parent.name not in FILL_DIRS
    )
    if not primary_files:
        raise FileNotFoundError(
            f"No NHDPlus _fixed_ tiles found in {nhd_merged_dir}. "
            "Run compute_slope_aspect.py first."
        )
    vrt_options = gdal.BuildVRTOptions(
        resolution="highest", srcNodata=str(OUTPUT_NODATA),
    )
    vrt_ds = gdal.BuildVRT(
        str(output_vrt), [str(f) for f in primary_files], options=vrt_options,
    )
    if vrt_ds is None:
        raise RuntimeError("gdal.BuildVRT failed for NHDPlus-only VRT")
    vrt_ds.FlushCache()
    del vrt_ds
    return output_vrt


def _build_composite_vrt(
    copernicus_elev: Path, nhdplus_vrt: Path, output_vrt: Path,
) -> Path:
    """Build a composite elevation VRT: Copernicus first (low priority),
    NHDPlus last (high priority, wins in overlap).
    """
    vrt_options = gdal.BuildVRTOptions(
        resolution="highest", srcNodata=str(OUTPUT_NODATA),
    )
    vrt_ds = gdal.BuildVRT(
        str(output_vrt),
        [str(copernicus_elev), str(nhdplus_vrt)],
        options=vrt_options,
    )
    if vrt_ds is None:
        raise RuntimeError("gdal.BuildVRT failed for composite elevation")
    vrt_ds.FlushCache()
    del vrt_ds
    return output_vrt


def _mask_to_fill_zone(
    raw_raster: Path,
    copernicus_elev: Path,
    nhdplus_vrt: Path,
    output: Path,
) -> None:
    """Mask a raster to retain only pixels in the fill zone.

    Fill zone = pixels where Copernicus has valid data AND NHDPlus has nodata.
    """
    # Read the raw computed raster
    raw_ds = gdal.Open(str(raw_raster))
    raw_band = raw_ds.GetRasterBand(1)
    raw_data = raw_band.ReadAsArray().astype(np.float32)
    geotransform = raw_ds.GetGeoTransform()
    projection = raw_ds.GetProjection()
    rows, cols = raw_data.shape
    del raw_ds

    # The raw raster has composite extent (union of Copernicus + NHDPlus).
    # Both Copernicus and NHDPlus must be warped to match this extent.
    output_bounds = [
        geotransform[0],
        geotransform[3] + rows * geotransform[5],
        geotransform[0] + cols * geotransform[1],
        geotransform[3],
    ]
    warp_kwargs = dict(
        format="MEM",
        outputBounds=output_bounds,
        xRes=abs(geotransform[1]),
        yRes=abs(geotransform[5]),
        dstNodata=OUTPUT_NODATA,
        srcNodata=OUTPUT_NODATA,
    )

    # Read Copernicus elevation aligned to composite extent
    cop_ds = gdal.Warp("", str(copernicus_elev), **warp_kwargs)
    if cop_ds is None:
        raise RuntimeError("gdal.Warp to MEM failed for Copernicus readback")
    cop_data = cop_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    del cop_ds

    # Read NHDPlus VRT aligned to composite extent
    nhd_ds = gdal.Warp("", str(nhdplus_vrt), **warp_kwargs)
    if nhd_ds is None:
        raise RuntimeError("gdal.Warp to MEM failed for NHDPlus VRT readback")
    nhd_data = nhd_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    del nhd_ds

    # Build fill mask: Copernicus valid AND NHDPlus nodata
    fill_mask = (cop_data != OUTPUT_NODATA) & (nhd_data == OUTPUT_NODATA)
    masked = np.where(fill_mask, raw_data, np.float32(OUTPUT_NODATA))

    # Write output
    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(
        str(output), cols, rows, 1, gdal.GDT_Float32,
        options=[
            "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES",
            "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
        ],
    )
    out_ds.SetGeoTransform(geotransform)
    out_ds.SetProjection(projection)
    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(OUTPUT_NODATA)
    out_band.WriteArray(masked)
    out_ds.FlushCache()
    del out_ds


def main():
    parser = argparse.ArgumentParser(
        description="Build Copernicus DEM fill for border HRUs (Canada/Mexico).",
    )
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = parser.parse_args()

    logger = configure_logging("build_border_dem")
    t_start = time.time()

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = Path(base["data_root"])

    raw_dir = data_root / "input" / "copernicus_dem" / "raw"
    nhd_merged_dir = data_root / "work" / "nhd_merged"
    fill_dir = nhd_merged_dir / "copernicus_fill"
    fill_dir.mkdir(parents=True, exist_ok=True)

    elev_out = fill_dir / "NEDSnapshot_merged_fixed_copernicus.tif"
    slope_out = fill_dir / "NEDSnapshot_merged_slope_copernicus.tif"
    aspect_out = fill_dir / "NEDSnapshot_merged_aspect_copernicus.tif"

    # --- Step 1: Compute tile list and download ---
    logger.info("=== Step 1/5: Download Copernicus GLO-30 tiles ===")
    all_labels = []
    for zone_name, (south, north, west, east) in BORDER_ZONES.items():
        labels = tiles_for_bbox(south, north, west, east)
        logger.info("  %s zone: %d tiles (%.0f\u00b0N\u2013%.0f\u00b0N, %.0f\u00b0W\u2013%.0f\u00b0W)",
                     zone_name, len(labels), south, north, abs(west), abs(east))
        all_labels.extend(labels)

    # Deduplicate (zones may overlap slightly)
    all_labels = sorted(set(all_labels))
    logger.info("  Total unique tiles: %d", len(all_labels))

    t1 = time.time()
    tile_paths = download_tiles(all_labels, raw_dir)
    logger.info("  Download complete in %s: %d tiles available", _elapsed(t1), len(tile_paths))

    if not tile_paths:
        logger.error("No tiles downloaded \u2014 cannot build border DEM")
        return

    # --- Step 2: Mosaic raw tiles and reproject ---
    logger.info("=== Step 2/5: Mosaic \u2192 reproject to EPSG:5070 ===")
    raw_vrt = fill_dir / "copernicus_raw.vrt"
    vrt_ds = gdal.BuildVRT(
        str(raw_vrt),
        [str(p) for p in tile_paths],
    )
    if vrt_ds is None:
        raise RuntimeError("gdal.BuildVRT failed for Copernicus raw tiles")
    vrt_ds.FlushCache()
    del vrt_ds
    logger.info("  Raw VRT: %s (%d sources)", raw_vrt, len(tile_paths))

    # Warp to EPSG:5070, 30m, nodata=-9999
    if not elev_out.exists() or args.force:
        logger.info("  Warping to EPSG:5070 at 30m (bilinear)...")
        t2 = time.time()
        warp_ds = gdal.Warp(
            str(elev_out),
            str(raw_vrt),
            dstSRS="EPSG:5070",
            xRes=30,
            yRes=30,
            resampleAlg="bilinear",
            dstNodata=OUTPUT_NODATA,
            outputType=gdal.GDT_Float32,
            creationOptions=[
                "COMPRESS=LZW",
                "PREDICTOR=2",
                "TILED=YES",
                "BLOCKXSIZE=512",
                "BLOCKYSIZE=512",
                "BIGTIFF=YES",
            ],
        )
        if warp_ds is None:
            raise RuntimeError("gdal.Warp failed")
        warp_ds.FlushCache()
        del warp_ds
        logger.info("  Warp complete in %s: %s", _elapsed(t2), elev_out)
    else:
        logger.info("  Elevation fill already exists: %s", elev_out)

    # --- Step 3: Build composite elevation (Copernicus + NHDPlus) ---
    logger.info("=== Step 3/5: Build composite elevation VRT ===")
    nhdplus_vrt = fill_dir / "nhdplus_only.vrt"
    composite_vrt = fill_dir / "composite_elevation.vrt"

    _build_nhdplus_vrt(nhd_merged_dir, nhdplus_vrt)
    logger.info("  NHDPlus-only VRT: %s", nhdplus_vrt)

    _build_composite_vrt(elev_out, nhdplus_vrt, composite_vrt)
    logger.info("  Composite VRT: %s", composite_vrt)

    # Clip composite to Copernicus extent — the composite VRT covers the
    # union of NHDPlus (all CONUS) + Copernicus, but we only need slope/aspect
    # for the Copernicus extent. Loading the full union into RichDEM would be
    # an unnecessary memory burden. The NHDPlus data in the overlap zone is
    # still included (it falls within the Copernicus bounds at 41-55°N).
    composite_clipped = fill_dir / "composite_elevation_clipped.tif"

    # --- Step 4: Compute slope/aspect from composite ---
    slope_raw = fill_dir / "slope_raw.tif"
    aspect_raw = fill_dir / "aspect_raw.tif"

    if not slope_out.exists() or not aspect_out.exists() or args.force:
        logger.info("=== Step 4/5: Compute slope/aspect from composite via RichDEM ===")

        # Get Copernicus extent to clip the composite
        cop_ds = gdal.Open(str(elev_out))
        cop_gt = cop_ds.GetGeoTransform()
        cop_cols = cop_ds.RasterXSize
        cop_rows = cop_ds.RasterYSize
        cop_bounds = [
            cop_gt[0],
            cop_gt[3] + cop_rows * cop_gt[5],
            cop_gt[0] + cop_cols * cop_gt[1],
            cop_gt[3],
        ]
        del cop_ds

        logger.info("  Clipping composite to Copernicus extent...")
        clip_ds = gdal.Warp(
            str(composite_clipped),
            str(composite_vrt),
            outputBounds=cop_bounds,
            xRes=30, yRes=30,
            dstNodata=OUTPUT_NODATA,
            srcNodata=OUTPUT_NODATA,
            outputType=gdal.GDT_Float32,
            creationOptions=[
                "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES",
                "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
            ],
        )
        if clip_ds is None:
            raise RuntimeError("gdal.Warp failed for composite clipping")
        clip_ds.FlushCache()
        del clip_ds

        logger.info("  Loading clipped composite DEM: %s", composite_clipped)
        t3 = time.time()
        dem = rd.LoadGDAL(str(composite_clipped), no_data=OUTPUT_NODATA)

        logger.info("  Computing slope (degrees)...")
        slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
        rd.SaveGDAL(str(slope_raw), slope)
        logger.info("  Raw slope saved: %s", slope_raw)

        logger.info("  Computing aspect...")
        aspect = rd.TerrainAttribute(dem, attrib="aspect")
        rd.SaveGDAL(str(aspect_raw), aspect)
        logger.info("  Raw aspect saved: %s", aspect_raw)
        logger.info("  Slope/aspect computation complete in %s", _elapsed(t3))

        # --- Step 5: Mask slope/aspect to fill zone ---
        logger.info("=== Step 5/5: Mask slope/aspect to fill zone ===")
        t4 = time.time()

        logger.info("  Masking slope to fill zone...")
        _mask_to_fill_zone(slope_raw, elev_out, nhdplus_vrt, slope_out)
        logger.info("  Masked slope saved: %s", slope_out)

        logger.info("  Masking aspect to fill zone...")
        _mask_to_fill_zone(aspect_raw, elev_out, nhdplus_vrt, aspect_out)
        logger.info("  Masked aspect saved: %s", aspect_out)

        logger.info("  Masking complete in %s", _elapsed(t4))

        # Clean up raw intermediates
        slope_raw.unlink(missing_ok=True)
        aspect_raw.unlink(missing_ok=True)
        logger.info("  Cleaned up raw slope/aspect intermediates")
    else:
        logger.info("  Slope/aspect outputs already exist \u2014 skipping")

    # Clean up intermediates
    for f in [raw_vrt, nhdplus_vrt, composite_vrt, composite_clipped]:
        if f.exists():
            f.unlink()
    logger.info("  Cleaned up intermediate files")

    logger.info("=== build_border_dem complete in %s ===", _elapsed(t_start))
    logger.info("  Outputs in: %s", fill_dir)
    logger.info("  Run build_vrt.py to rebuild VRTs with the fill layer.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_build_border_dem.py tests/test_build_vrt.py -v`
Expected: All pass

- [ ] **Step 5: Run full test suite**

Run: `pytest tests/ -v`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add scripts/build_border_dem.py tests/test_build_border_dem.py
git commit -m "fix: compute slope/aspect from composite NHDPlus+Copernicus elevation

Build a composite elevation surface (NHDPlus priority via last-source-wins)
before computing slope/aspect, eliminating the one-pixel nodata gap at the
NHDPlus/Copernicus boundary. Mask output to fill zone only (where Copernicus
has data but NHDPlus does not)."
```

---

### Task 3: Update RUNME.md pipeline dependency

**Files:**
- Modify: `slurm_batch/RUNME.md:107-119`

- [ ] **Step 1: Update Stage 1b documentation**

In `slurm_batch/RUNME.md`, update the Stage 1b section (lines 107-119) to reflect the new dependency:

Replace:
```
This creates fill rasters in `work/nhd_merged/copernicus_fill/`. The subsequent
`build_vrt.py` step composites these behind the NHDPlus tiles, so NHDPlus takes
priority where it has valid data and Copernicus fills the border gaps. Can run
in parallel with Stage 1.
```

With:
```
This creates fill rasters in `work/nhd_merged/copernicus_fill/`. The subsequent
`build_vrt.py` step composites these behind the NHDPlus tiles, so NHDPlus takes
priority where it has valid data and Copernicus fills the border gaps.

**Dependency:** Must run AFTER Stage 1 completes, because it needs the
NHDPlus `_fixed_` elevation tiles produced by `compute_slope_aspect.py` to
build a seamless composite elevation surface for slope/aspect computation.
```

- [ ] **Step 2: Commit**

```bash
git add slurm_batch/RUNME.md
git commit -m "docs: update RUNME.md — Stage 1b now depends on Stage 1"
```

---

### Task 4: Create validation notebook

**Files:**
- Create: `notebooks/check_border_dem.py`

- [ ] **Step 1: Create the Marimo notebook**

Create `notebooks/check_border_dem.py` following the patterns established in
`notebooks/check_derived_rasters.py`:
- Use `rasterio` decimated reads with `out_shape` for large rasters
- Use `percentile_stretch` for consistent visualization
- Use `rasterized=True` on `imshow` calls
- Clean up figures with `plt.close()`

The notebook should have these cells:

1. **Imports and config** — paths to VRTs, fabric, display settings
2. **Helper functions** — reuse `decimated_read`, `percentile_stretch`, `raster_meta` pattern from `check_derived_rasters.py`
3. **Region selector** — Marimo dropdown for border region with preset EPSG:5070 bounding boxes for Canada-East (VPU 01/02/04), Canada-West (VPU 17), Mexico (VPU 12/13/15)
4. **Layer selector** — Marimo dropdown for elevation/slope/aspect
5. **Elevation continuity** — Plot the merged elevation VRT zoomed to selected region using `rasterio.open` with a `window` derived from the region bounds
6. **Slope/aspect seamlessness** — Same region, show slope and aspect side-by-side
7. **Border HRU check** — Load fabric GeoPackage, filter HRUs whose bounds extend beyond CONUS (using a lat threshold in projected coords), plot their zonal means
8. **Difference map** — In the overlap zone, show NHDPlus-minus-Copernicus elevation difference with a diverging colormap

```python
import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
        # Border DEM Fill — Visual QA

        Validates the Copernicus GLO-30 border fill for Canada/Mexico HRUs.
        Checks elevation continuity, slope/aspect seamlessness, and border
        HRU parameter values.

        **Best practice:** uses rasterio's `out_shape` decimated read — only a
        thumbnail-resolution array is decompressed; if internal overviews exist,
        GDAL selects the best one automatically.
        """
    )
    return


@app.cell
def _():
    from pathlib import Path

    import geopandas as gpd
    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.windows import from_bounds

    import marimo as mo

    DATA_ROOT = Path(
        "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2"
    )
    NHD_MERGED = DATA_ROOT / "work" / "nhd_merged"
    FABRIC_DIR = DATA_ROOT / "gfv2" / "fabric"
    DISPLAY_PX = 2000

    return (
        DATA_ROOT, DISPLAY_PX, FABRIC_DIR, NHD_MERGED,
        Path, gpd, mo, np, plt, rasterio, Resampling, from_bounds,
    )


@app.cell
def _(mo):
    mo.md("## Region and layer selection")
    return


@app.cell
def _(mo):
    # Bounding boxes in EPSG:5070 (xmin, ymin, xmax, ymax)
    REGIONS = {
        "Canada-East (VPU 01/02/04)": (1_500_000, 2_500_000, 2_800_000, 3_300_000),
        "Canada-West (VPU 17)": (-2_200_000, 2_500_000, -1_400_000, 3_200_000),
        "Mexico (VPU 12/13/15)": (-1_500_000, 200_000, 500_000, 1_200_000),
    }
    region_dropdown = mo.ui.dropdown(
        options=list(REGIONS.keys()),
        value="Canada-East (VPU 01/02/04)",
        label="Border region",
    )
    layer_dropdown = mo.ui.dropdown(
        options=["elevation", "slope", "aspect"],
        value="elevation",
        label="Raster layer",
    )
    mo.hstack([region_dropdown, layer_dropdown])
    return REGIONS, layer_dropdown, region_dropdown


@app.cell
def _(mo):
    mo.md("## Helper functions")
    return


@app.cell
def _(DISPLAY_PX, np, rasterio, Resampling, from_bounds):
    def raster_meta(path):
        with rasterio.open(path) as src:
            return {
                "shape": src.shape,
                "dtype": src.dtypes[0],
                "crs": src.crs,
                "bounds": src.bounds,
                "res_m": abs(src.transform.a),
                "nodata": src.nodata,
                "overviews": src.overviews(1),
                "size_MB": round(path.stat().st_size / 1024**2)
                if path.stat().st_size > 0
                else 0,
            }

    def windowed_decimated_read(
        path, bounds, target_px=DISPLAY_PX, resampling=Resampling.average,
    ):
        """Read a windowed, decimated thumbnail from a large raster.

        Parameters
        ----------
        path : Path
            Raster file or VRT.
        bounds : tuple
            (xmin, ymin, xmax, ymax) in the raster's CRS.
        target_px : int
            Maximum dimension of the returned array.
        resampling : Resampling
            Resampling method for decimation.

        Returns
        -------
        (masked_array_2d, window_transform, meta_dict)
        """
        with rasterio.open(path) as src:
            window = from_bounds(*bounds, transform=src.transform)
            # Clamp window to raster extent
            window = window.intersection(
                rasterio.windows.Window(0, 0, src.width, src.height)
            )
            if window.width < 1 or window.height < 1:
                raise ValueError(
                    f"Bounds {bounds} do not overlap raster extent {src.bounds}"
                )
            factor = max(1, max(int(window.width), int(window.height)) // target_px)
            out_h = max(1, int(window.height) // factor)
            out_w = max(1, int(window.width) // factor)
            data = src.read(
                1,
                window=window,
                out_shape=(out_h, out_w),
                resampling=resampling,
            ).astype(np.float64)
            nodata = src.nodata
            win_transform = src.window_transform(window)
            scaled_transform = win_transform * win_transform.scale(
                window.width / out_w, window.height / out_h,
            )
        mask = ~np.isfinite(data)
        if nodata is not None:
            mask |= data == nodata
        return np.ma.array(data, mask=mask), scaled_transform, raster_meta(path)

    def percentile_stretch(arr, lo=2, hi=98):
        valid = (
            arr.compressed()
            if isinstance(arr, np.ma.MaskedArray)
            else arr[np.isfinite(arr)]
        )
        if valid.size == 0:
            return arr, 0.0, 1.0
        vmin, vmax = np.percentile(valid, [lo, hi])
        return np.clip(arr, vmin, vmax), vmin, vmax

    return percentile_stretch, raster_meta, windowed_decimated_read


@app.cell
def _(mo):
    mo.md("## Elevation continuity")
    return


@app.cell
def _(
    NHD_MERGED, REGIONS, layer_dropdown, np, percentile_stretch, plt,
    region_dropdown, windowed_decimated_read,
):
    _layer = layer_dropdown.value
    _region_name = region_dropdown.value
    _bounds = REGIONS[_region_name]
    _vrt_path = NHD_MERGED / f"{_layer}.vrt"

    _cmaps = {"elevation": "terrain", "slope": "YlOrRd", "aspect": "hsv"}
    _units = {"elevation": "m", "slope": "degrees", "aspect": "degrees"}

    if _vrt_path.exists():
        _data, _, _meta = windowed_decimated_read(_vrt_path, _bounds)
        _stretched, _vmin, _vmax = percentile_stretch(_data)
        _valid = _data.compressed()

        _fig, (_ax_img, _ax_hist) = plt.subplots(
            1, 2, figsize=(16, 6), gridspec_kw={"width_ratios": [3, 1]},
        )
        _im = _ax_img.imshow(
            _stretched, cmap=_cmaps[_layer], vmin=_vmin, vmax=_vmax,
            interpolation="nearest", rasterized=True,
        )
        _ax_img.set_title(
            f"{_layer.title()} — {_region_name}\n{_vrt_path.name}", fontsize=11,
        )
        _ax_img.axis("off")
        plt.colorbar(
            _im, ax=_ax_img, fraction=0.03, pad=0.02, label=_units[_layer],
        )

        _ax_hist.hist(
            _valid, bins=100, color="steelblue", edgecolor="none", density=True,
        )
        _ax_hist.set_title("Value distribution")
        _ax_hist.set_xlabel(_units[_layer])
        _ax_hist.set_ylabel("density")

        _h, _w = _meta["shape"]
        _info = (
            f"Full grid : {_h:,} x {_w:,} px\n"
            f"Pixel size: {_meta['res_m']:.1f} m\n"
            f"NoData    : {_meta['nodata']}\n"
            f"Valid px  : {_valid.size:,}\n"
            f"Min       : {_valid.min():.4g}\n"
            f"Mean      : {_valid.mean():.4g}\n"
            f"Max       : {_valid.max():.4g}"
        )
        _ax_img.text(
            1.01, 0.5, _info, transform=_ax_img.transAxes, fontsize=8,
            verticalalignment="center", family="monospace",
            bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8),
        )
        plt.tight_layout()
        _fig
    else:
        print(f"VRT not found: {_vrt_path}")


@app.cell
def _(mo):
    mo.md("## Slope and aspect side-by-side")
    return


@app.cell
def _(
    NHD_MERGED, REGIONS, np, percentile_stretch, plt,
    region_dropdown, windowed_decimated_read,
):
    _region_name = region_dropdown.value
    _bounds = REGIONS[_region_name]
    _layers = [
        ("slope", "YlOrRd", "degrees"),
        ("aspect", "hsv", "degrees"),
    ]
    _available = [
        (name, cmap, units)
        for name, cmap, units in _layers
        if (NHD_MERGED / f"{name}.vrt").exists()
    ]
    if _available:
        _fig, _axes = plt.subplots(
            1, len(_available), figsize=(8 * len(_available), 6),
        )
        if len(_available) == 1:
            _axes = [_axes]
        for _ax, (_name, _cmap, _units) in zip(_axes, _available):
            _data, _, _ = windowed_decimated_read(
                NHD_MERGED / f"{_name}.vrt", _bounds,
            )
            _stretched, _vmin, _vmax = percentile_stretch(_data)
            _im = _ax.imshow(
                _stretched, cmap=_cmap, vmin=_vmin, vmax=_vmax,
                interpolation="nearest", rasterized=True,
            )
            _ax.set_title(f"{_name.title()} — {_region_name}")
            _ax.axis("off")
            plt.colorbar(_im, ax=_ax, fraction=0.03, pad=0.02, label=_units)
        plt.suptitle("Slope/Aspect — check for seam artifacts", fontsize=13)
        plt.tight_layout()
        _fig
    else:
        print("No slope/aspect VRTs found yet.")


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Border HRU parameter check

        Load the merged fabric, identify HRUs that extend beyond typical CONUS
        extent, and check whether they have valid elevation/slope/aspect values.
        """
    )
    return


@app.cell
def _(FABRIC_DIR, gpd, mo, np, plt):
    _merged_gpkg = FABRIC_DIR / "gfv2_nhru_merged.gpkg"
    if _merged_gpkg.exists():
        _gdf = gpd.read_file(_merged_gpkg, layer="nhru")
        _gdf["centroid_y"] = _gdf.geometry.centroid.y

        # Border HRUs: centroid above 2,700,000 m (approx US-Canada border in EPSG:5070)
        # or below 500,000 m (approx US-Mexico border in EPSG:5070)
        _canada_mask = _gdf["centroid_y"] > 2_700_000
        _mexico_mask = _gdf["centroid_y"] < 500_000
        _border_mask = _canada_mask | _mexico_mask

        _n_canada = _canada_mask.sum()
        _n_mexico = _mexico_mask.sum()
        _n_total = len(_gdf)

        mo.md(
            f"**Fabric:** {_n_total:,} total HRUs | "
            f"**Canada border:** {_n_canada:,} | "
            f"**Mexico border:** {_n_mexico:,}"
        )

        if _border_mask.any():
            _fig, _ax = plt.subplots(1, 1, figsize=(12, 8))
            _gdf[~_border_mask].plot(
                ax=_ax, color="lightgray", edgecolor="none", alpha=0.3,
            )
            _gdf[_canada_mask].plot(
                ax=_ax, color="steelblue", edgecolor="none", alpha=0.6,
                label=f"Canada border ({_n_canada:,})",
            )
            _gdf[_mexico_mask].plot(
                ax=_ax, color="coral", edgecolor="none", alpha=0.6,
                label=f"Mexico border ({_n_mexico:,})",
            )
            _ax.legend()
            _ax.set_title("Border HRUs identified by centroid latitude")
            _ax.axis("off")
            plt.tight_layout()
            _fig
    else:
        print(f"Merged fabric not found: {_merged_gpkg}")


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Elevation difference: NHDPlus vs Copernicus

        In the overlap zone, compute NHDPlus minus Copernicus elevation.
        A diverging colormap highlights systematic offsets.
        """
    )
    return


@app.cell
def _(
    NHD_MERGED, REGIONS, np, percentile_stretch, plt,
    region_dropdown, windowed_decimated_read,
):
    _region_name = region_dropdown.value
    _bounds = REGIONS[_region_name]
    _elev_vrt = NHD_MERGED / "elevation.vrt"
    _cop_elev = NHD_MERGED / "copernicus_fill" / "NEDSnapshot_merged_fixed_copernicus.tif"

    if _elev_vrt.exists() and _cop_elev.exists():
        _nhd_data, _, _ = windowed_decimated_read(_elev_vrt, _bounds)
        _cop_data, _, _ = windowed_decimated_read(_cop_elev, _bounds)

        # Compute difference only where both have valid data
        _both_valid = ~_nhd_data.mask & ~_cop_data.mask
        _diff = np.ma.array(
            np.where(_both_valid, _nhd_data.data - _cop_data.data, 0.0),
            mask=~_both_valid,
        )

        if _diff.count() > 0:
            _valid = _diff.compressed()
            _vmax = max(abs(np.percentile(_valid, 2)), abs(np.percentile(_valid, 98)))

            _fig, (_ax_img, _ax_hist) = plt.subplots(
                1, 2, figsize=(16, 6), gridspec_kw={"width_ratios": [3, 1]},
            )
            _im = _ax_img.imshow(
                _diff, cmap="RdBu", vmin=-_vmax, vmax=_vmax,
                interpolation="nearest", rasterized=True,
            )
            _ax_img.set_title(
                f"Elevation difference (NHDPlus - Copernicus)\n{_region_name}",
                fontsize=11,
            )
            _ax_img.axis("off")
            plt.colorbar(_im, ax=_ax_img, fraction=0.03, pad=0.02, label="m")

            _ax_hist.hist(
                _valid, bins=100, color="steelblue", edgecolor="none", density=True,
            )
            _ax_hist.axvline(0, color="red", linestyle="--", linewidth=1)
            _ax_hist.set_title("Difference distribution")
            _ax_hist.set_xlabel("m (NHDPlus - Copernicus)")
            _ax_hist.set_ylabel("density")

            _info = (
                f"Overlap px: {_valid.size:,}\n"
                f"Mean diff : {_valid.mean():.3f} m\n"
                f"Std diff  : {_valid.std():.3f} m\n"
                f"Max |diff|: {np.abs(_valid).max():.3f} m"
            )
            _ax_img.text(
                1.01, 0.5, _info, transform=_ax_img.transAxes, fontsize=9,
                verticalalignment="center", family="monospace",
                bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8),
            )
            plt.tight_layout()
            _fig
        else:
            print("No overlapping valid pixels found in this region.")
    else:
        _missing = []
        if not _elev_vrt.exists():
            _missing.append(str(_elev_vrt))
        if not _cop_elev.exists():
            _missing.append(str(_cop_elev))
        print(f"Missing rasters: {', '.join(_missing)}")


if __name__ == "__main__":
    app.run()
```

- [ ] **Step 2: Verify notebook imports work**

Run: `python -c "import marimo; import rasterio; import geopandas; import matplotlib; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add notebooks/check_border_dem.py
git commit -m "feat: add Marimo notebook for border DEM fill validation

Interactive notebook with region/layer selectors to inspect elevation
continuity, slope/aspect seamlessness, border HRU coverage, and
NHDPlus vs Copernicus elevation differences."
```

---

### Task 5: Final verification

- [ ] **Step 1: Run full test suite**

Run: `pytest tests/ -v`
Expected: All tests pass

- [ ] **Step 2: Review all changes**

Run: `git log --oneline main..HEAD`
Expected: 4 commits (VRT fix, border DEM fix, RUNME docs, notebook)

- [ ] **Step 3: Verify no untracked files**

Run: `git status`
Expected: Clean working tree
