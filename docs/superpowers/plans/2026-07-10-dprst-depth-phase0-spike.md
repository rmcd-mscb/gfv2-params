# dprst_depth Phase 0 Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Answer Issue #173's six Phase-0 questions on real Prairie-Pothole-Region data and ship `docs/dprst_depth_spike.md` with a per-FTYPE go/no-go decision — no builder.

**Architecture:** One reusable diagnostic module (`scripts/diagnose/dprst_depth_probe.py`) provides the shipped dprst polygon set, a windowed best-available-topo reader over `/vsis3/prd-tnm`, and a `richdem` depth-to-spill. Six analysis tasks each emit an evidence artifact (table/figure) into the scratchpad; a final task synthesizes them into the writeup. Two pure-math helpers (polygon-set reconstruction, depth-to-spill) get unit tests; the analysis tasks are exploratory and produce evidence, not assertions.

**Tech Stack:** Python, geopandas/pyogrio, rasterio + GDAL `/vsis3/`, richdem (float64), numpy/pandas, matplotlib. pixi `default` env; run via `pixi run` / `pixi run --as-is`.

## Global Constraints

- **This is a spike. No builder, no DAG registration, no config block, no `op_flow_thres` emission.** Those are Phase 1, gated on this spike's outcome. (Issue #173)
- **Reuse the shipped dprst classification** — import `load_connected_comids` / `select_connected_waterbodies` from `gfv2_params.depstor`; FTYPE rules from `gfv2_params.nhd_ftypes` (`FORCE_DPRST_FTYPES={"Playa"}`, `EXCLUDE_WATERBODY_FTYPES={"Ice Mass"}`). Do not re-derive classification.
- **Best-available-topo ladder:** 1 m 3DEP project COG → 10 m seamless 1/3 arc-second (`/vsis3/prd-tnm/StagedProducts/Elevation/13/TIFF/current/`) → documented constant (NHM median **49 in**). Record resolution + method per polygon.
- **Raw DEM only** for depth-to-spill. Never `fdr.vrt` / NHDPlus HydroDEM (`filled − raw ≡ 0`). Never a lattice — windowed per-polygon reads only (1 m CONUS ≈ 1.5e13 cells).
- **richdem fill is float64.** Do **not** route any raster through WhiteboxTools with LZW+`predictor=2` (silent corruption).
- **No `pytest` on the HPC head node** (import-storm rule). Unit tests are the CI gate; run analysis via `pixi run`. `py_compile`/import checks on the head node are fine.
- **Paths from the profile**, read via `require_config_key`; `{data_root}` = `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2`. No hardcoded data paths in committed code.
- **`log` sample sizes; never silently cap.** Report shortfalls explicitly.
- Scratchpad for intermediate artifacts: `/tmp/claude-21018/-caldera-hovenweep-projects-usgs-water-impd-nhgf-gfv2-params/a0974587-ef53-4c14-82bd-a2c889179a89/scratchpad`.

---

### Task 1: dprst polygon set reconstruction (reusable + tested)

**Files:**
- Create: `scripts/diagnose/dprst_depth_probe.py`
- Test: `tests/test_dprst_depth_probe.py`

**Interfaces:**
- Consumes: `gfv2_params.depstor.load_connected_comids(path) -> set[int]`, `gfv2_params.depstor.select_connected_waterbodies(gdf, connected) -> GeoDataFrame`; `gfv2_params.nhd_ftypes.{FORCE_DPRST_FTYPES, EXCLUDE_WATERBODY_FTYPES, NEVER_ONSTREAM_FTYPES}`.
- Produces: `dprst_polygons(wb_gdf, connected: set[int]) -> GeoDataFrame` — returns the polygons classified **dprst** (on-stream removed, Ice Mass dropped, Playa always kept), with original columns incl. `FTYPE`, `COMID`, geometry. Later tasks call this to build their samples.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dprst_depth_probe.py
import importlib.util
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

_spec = importlib.util.spec_from_file_location(
    "dprst_depth_probe",
    Path(__file__).resolve().parent.parent / "scripts" / "diagnose" / "dprst_depth_probe.py",
)
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)


def _wb(rows):
    # rows: list of (COMID, member_comid, FTYPE)
    return gpd.GeoDataFrame(
        {
            "COMID": [r[0] for r in rows],
            "member_comid": [r[1] for r in rows],
            "FTYPE": [r[2] for r in rows],
            "geometry": [Point(i, 0).buffer(1) for i in range(len(rows))],
        },
        crs="EPSG:5070",
    )


def test_dprst_polygons_classification():
    wb = _wb(
        [
            (10, 10, "LakePond"),    # on-stream (COMID in connected)  -> dropped
            (11, 11, "LakePond"),    # off-stream                       -> dprst
            (12, 12, "Playa"),       # in connected but Playa           -> dprst (forced)
            (13, 13, "Ice Mass"),    # off-stream Ice Mass              -> excluded
        ]
    )
    connected = {10, 12}
    out = probe.dprst_polygons(wb, connected)
    comids = set(out["COMID"])
    assert comids == {11, 12}          # 11 off-stream, 12 playa forced
    assert 10 not in comids            # genuine on-stream LakePond removed
    assert 13 not in comids            # Ice Mass excluded entirely
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_dprst_polygons_classification -v`
Expected: FAIL — `dprst_depth_probe.py` does not exist / `dprst_polygons` undefined.

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/diagnose/dprst_depth_probe.py
"""Diagnostic probe for Issue #173 Phase 0 spike: dprst_depth_avg from
best-available topography. Analysis-only; not a builder. See
docs/superpowers/specs/2026-07-10-dprst-depth-phase0-spike-design.md.
"""
from __future__ import annotations

import geopandas as gpd

from gfv2_params.nhd_ftypes import EXCLUDE_WATERBODY_FTYPES, FORCE_DPRST_FTYPES
from gfv2_params.depstor import select_connected_waterbodies


def dprst_polygons(wb_gdf: gpd.GeoDataFrame, connected: set[int]) -> gpd.GeoDataFrame:
    """Reconstruct the shipped `dprst` polygon set at the polygon level.

    Mirrors wbody_connectivity -> dprst: drop genuinely on-stream waterbodies,
    force Playa to dprst, exclude Ice Mass entirely.
    """
    if "FTYPE" not in wb_gdf.columns:
        raise KeyError("waterbody layer has no FTYPE column; cannot classify dprst")
    wb = wb_gdf[~wb_gdf["FTYPE"].isin(EXCLUDE_WATERBODY_FTYPES)].copy()
    onstream = select_connected_waterbodies(wb, connected)
    onstream = onstream[~onstream["FTYPE"].isin(FORCE_DPRST_FTYPES)]
    onstream_idx = set(onstream.index)
    return wb[~wb.index.isin(onstream_idx)].copy()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_dprst_polygons_classification -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/diagnose/dprst_depth_probe.py tests/test_dprst_depth_probe.py
git commit -m "feat(spike): dprst polygon-set reconstruction for #173 Phase 0"
```

---

### Task 2: Coverage audit — best-available resolution per polygon

**Files:**
- Modify: `scripts/diagnose/dprst_depth_probe.py`
- Test: `tests/test_dprst_depth_probe.py`

**Interfaces:**
- Consumes: `dprst_polygons` (Task 1). WESM footprint index at `/vsis3/prd-tnm/StagedProducts/Elevation/metadata/WESM.gpkg` (authoritative 3DEP workunit extents).
- Produces: `resolution_class(dprst_gdf, wesm_gdf) -> GeoDataFrame` — adds a `best_topo` column (`"1m"` if the polygon centroid falls inside any WESM 1 m/QL1/QL2 workunit, else `"10m"`). Later tasks read `best_topo` to pick the DEM source.

- [ ] **Step 1: Write the failing test**

```python
def test_resolution_class_assigns_1m_inside_footprint():
    import geopandas as gpd
    from shapely.geometry import box, Point

    dprst = gpd.GeoDataFrame(
        {"COMID": [1, 2], "geometry": [Point(0.5, 0.5).buffer(0.1), Point(9, 9).buffer(0.1)]},
        crs="EPSG:5070",
    )
    wesm = gpd.GeoDataFrame(
        {"workunit": ["A"], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:5070"
    )
    out = probe.resolution_class(dprst, wesm)
    assert list(out.sort_values("COMID")["best_topo"]) == ["1m", "10m"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_resolution_class_assigns_1m_inside_footprint -v`
Expected: FAIL — `resolution_class` undefined.

- [ ] **Step 3: Write minimal implementation**

```python
def resolution_class(
    dprst_gdf: gpd.GeoDataFrame, wesm_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Tag each dprst polygon with its best available topo source.

    "1m" if the polygon centroid lies inside any WESM workunit footprint,
    else "10m" (seamless 1/3 arc-second floor). Centroid test keeps it a
    single fast spatial join at CONUS scale.
    """
    out = dprst_gdf.copy()
    pts = out.set_geometry(out.geometry.centroid)
    wesm = wesm_gdf.to_crs(out.crs)[["geometry"]]
    hit = gpd.sjoin(pts, wesm, how="left", predicate="within")
    out["best_topo"] = hit.groupby(level=0)["index_right"].first().notna().map(
        {True: "1m", False: "10m"}
    )
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_resolution_class_assigns_1m_inside_footprint -v`
Expected: PASS.

- [ ] **Step 5: Run the CONUS audit and save the artifact**

Write a thin `__main__` audit block (or a scratchpad driver) that: loads `conus_waterbodies.gpkg` + the two COMID parquets via the profile paths, builds `dprst_polygons`, reads WESM over `/vsis3/`, calls `resolution_class`, and writes a per-VPU table (count-% and area-% at 1 m vs 10 m) to the scratchpad as `coverage_audit.csv`.

Run: `pixi run python scripts/diagnose/dprst_depth_probe.py --audit`
Expected: `coverage_audit.csv` written; stdout logs total dprst polygon count/area and the national 1 m vs 10 m split. Confirm the count is the same order as the issue's 285,998 / 53,159 km².

- [ ] **Step 6: Commit**

```bash
git add scripts/diagnose/dprst_depth_probe.py tests/test_dprst_depth_probe.py
git commit -m "feat(spike): #173 coverage audit — best-available topo per dprst polygon"
```

---

### Task 3: Windowed topo reader + depth-to-spill (reusable + tested)

**Files:**
- Modify: `scripts/diagnose/dprst_depth_probe.py`
- Test: `tests/test_dprst_depth_probe.py`

**Interfaces:**
- Consumes: `best_topo` column (Task 2); richdem.
- Produces:
  - `read_window(geom, best_topo, rim_buffer_m=200) -> (dem: np.ndarray float32, transform, crs)` — windowed read of the polygon bbox + rim from the appropriate `/vsis3/` source (1 m project COG or the 10 m `USGS_13_n{lat}w{lon}.tif` tile), reprojected to a local equal-area grid. Raw DEM.
  - `depth_to_spill(dem) -> np.ndarray` — `richdem.FillDepressions(dem.astype(float64)) - dem`, returned float32; nodata-safe.
  - `volume_mean_depth(dem, mask, cell_area_m2) -> (v_m3, a_m2, mean_depth_m)` — the `V/A` aggregation over in-polygon cells.

- [ ] **Step 1: Write the failing test (synthetic bowl → known V/A)**

```python
import numpy as np


def test_depth_to_spill_and_mean_depth_on_synthetic_bowl():
    # 5x5 flat plateau at z=10 with a single 3x3 pit of depth 2 (z=8).
    dem = np.full((5, 5), 10.0, dtype=np.float64)
    dem[1:4, 1:4] = 8.0
    depth = probe.depth_to_spill(dem)
    # Filled restores the pit to the rim (10); depth = 2 in the pit, 0 on the rim.
    assert np.isclose(depth[2, 2], 2.0)
    assert np.isclose(depth[0, 0], 0.0)

    mask = depth > 0            # the 3x3 pit
    v, a, mean_d = probe.volume_mean_depth(depth, mask, cell_area_m2=1.0)
    assert np.isclose(a, 9.0)          # 9 cells * 1 m^2
    assert np.isclose(v, 18.0)         # 9 cells * depth 2 * 1 m^2
    assert np.isclose(mean_d, 2.0)     # V/A
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_depth_to_spill_and_mean_depth_on_synthetic_bowl -v`
Expected: FAIL — `depth_to_spill` / `volume_mean_depth` undefined.

- [ ] **Step 3: Write minimal implementation**

```python
import numpy as np
import richdem as rd


def depth_to_spill(dem: np.ndarray, nodata: float | None = None) -> np.ndarray:
    """filled - raw over a RAW dem. float64 fill per the DEM-derivatives gotcha."""
    a = np.asarray(dem, dtype=np.float64)
    rda = rd.rdarray(a, no_data=(nodata if nodata is not None else -9999.0))
    filled = np.asarray(rd.FillDepressions(rda, in_place=False), dtype=np.float64)
    depth = filled - a
    depth[depth < 0] = 0.0
    if nodata is not None:
        depth[a == nodata] = 0.0
    return depth.astype(np.float32)


def volume_mean_depth(depth: np.ndarray, mask: np.ndarray, cell_area_m2: float):
    """V = sum(depth*area), A = sum(area) over masked cells; mean = V/A (metres)."""
    sel = depth[mask]
    a = float(mask.sum()) * cell_area_m2
    v = float(sel.sum()) * cell_area_m2
    mean_d = v / a if a > 0 else 0.0
    return v, a, mean_d
```

`read_window` (no unit test — it hits S3; smoke-tested in Step 5):

```python
import rasterio
from rasterio.warp import transform_geom


def _tile13_name(lon: float, lat: float) -> str:
    # 1x1 deg tiles named by the NW corner, e.g. n48w101.
    north = int(np.ceil(lat))
    west = int(np.ceil(-lon))
    return f"n{north:02d}w{west:03d}"


def read_window(geom, best_topo: str, wesm_row=None, rim_buffer_m: float = 200.0):
    """Windowed RAW-DEM read of geom bbox + rim from the best-available source.

    best_topo == "1m": read the covering WESM project COG (path from wesm_row).
    best_topo == "10m": read the seamless 1/3 arc-second tile from
      /vsis3/prd-tnm/StagedProducts/Elevation/13/TIFF/current/<tile>/USGS_13_<tile>.tif
    Returns (dem float32, transform, crs). Reproject-on-read to EPSG:5070 (equal
    area) so cell_area is uniform; never materialise beyond the window.
    """
    # implementation: build /vsis3/ path, open with rasterio, window to
    # geom.bounds + rim, WarpedVRT to EPSG:5070 at the source resolution.
    ...
```

> Implementer note: keep `read_window`'s `...` body faithful to the docstring — a `rasterio.open("/vsis3/…")` + `WarpedVRT(dst_crs="EPSG:5070")` + `.read(1, window=…)`. It is exercised live in Step 5, not unit-tested, because it depends on S3.

- [ ] **Step 4: Run tests to verify the math passes**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py -v`
Expected: PASS (both math tests). `read_window` is not asserted here.

- [ ] **Step 5: Live smoke test of `read_window` over S3**

Pick one ND dprst LakePond polygon (from Task 2 output) and one 10 m polygon; call `read_window` + `depth_to_spill` on each in a scratchpad snippet.

Run: `pixi run python -c "..."` (scratchpad driver)
Expected: a real DEM array with sane elevations (ND ≈ 300–900 m), non-empty; logs the source path actually used. Confirms `/vsis3/` reads work end-to-end.

- [ ] **Step 6: Commit**

```bash
git add scripts/diagnose/dprst_depth_probe.py tests/test_dprst_depth_probe.py
git commit -m "feat(spike): windowed best-available-topo reader + depth-to-spill for #173"
```

---

### Task 4: Flatness detector, validation, and SwampMarsh verdict

**Files:**
- Modify: `scripts/diagnose/dprst_depth_probe.py`
- Test: `tests/test_dprst_depth_probe.py`

**Interfaces:**
- Consumes: `read_window` (Task 3).
- Produces: `is_hydroflattened(dem_in_polygon: np.ndarray) -> dict` — returns `{"range": float, "std": float, "n_unique": int, "flat": bool}`; `flat` is `range < 0.01 m` (exactly-constant breakline surface, not merely low-variance).

- [ ] **Step 1: Write the failing test**

```python
def test_is_hydroflattened_detects_constant_surface():
    flat = np.full((20, 20), 512.30, dtype=np.float32)
    natural = flat + np.linspace(0, 1.5, 400).reshape(20, 20).astype(np.float32)
    assert probe.is_hydroflattened(flat)["flat"] is True
    r = probe.is_hydroflattened(natural)
    assert r["flat"] is False
    assert r["range"] > 1.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_is_hydroflattened_detects_constant_surface -v`
Expected: FAIL — `is_hydroflattened` undefined.

- [ ] **Step 3: Write minimal implementation**

```python
def is_hydroflattened(dem_in_polygon: np.ndarray, tol_m: float = 0.01) -> dict:
    """A hydro-flattened water surface is breakline-enforced -> exactly constant.
    Test interior range, not just variance."""
    v = np.asarray(dem_in_polygon, dtype=np.float64)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return {"range": float("nan"), "std": float("nan"), "n_unique": 0, "flat": False}
    rng = float(v.max() - v.min())
    return {
        "range": rng,
        "std": float(v.std()),
        "n_unique": int(np.unique(np.round(v, 3)).size),
        "flat": rng < tol_m,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_is_hydroflattened_detects_constant_surface -v`
Expected: PASS.

- [ ] **Step 5: Run the ND sampling analysis (issue tasks 2 + 3)**

Scratchpad driver: sample N≈300 dprst polygons per FTYPE within the chosen ND 1 m project(s); for each, `read_window` (1 m) → interior mask → `is_hydroflattened`. Tabulate flattened-fraction and the range/σ/unique distributions **per FTYPE**. This directly settles **SwampMarsh** (flattened vs bare-earth). Save `flatness_by_ftype.csv` + a separability histogram PNG to the scratchpad. `log` the exact per-FTYPE sample sizes.

Run: `pixi run python scripts/diagnose/dprst_depth_probe.py --flatness`
Expected: per-FTYPE flattened-% table; a clear bimodal separation (flat ≈ 0 range vs natural ≫ tol) confirming the detector; a definitive SwampMarsh number.

- [ ] **Step 6: Commit**

```bash
git add scripts/diagnose/dprst_depth_probe.py tests/test_dprst_depth_probe.py
git commit -m "feat(spike): flatness detector + ND validation + SwampMarsh verdict (#173)"
```

---

### Task 5: Quantify freeboard over flattened polygons (issue task 4)

**Files:**
- Modify: `scripts/diagnose/dprst_depth_probe.py`

**Interfaces:**
- Consumes: `read_window`, `depth_to_spill`, `is_hydroflattened`, `volume_mean_depth`.
- Produces: a scratchpad artifact `freeboard_dist.csv` — per sampled flattened polygon, its `filled − raw` mean/median depth (the above-water spill storage). No new tested function; this is analysis over Task 3/4 primitives.

- [ ] **Step 1: Run the freeboard analysis**

For each detected-flat polygon in the Task 4 sample: `depth_to_spill(read_window(...))`, mask to polygon, `volume_mean_depth` → mean freeboard (m and inches). Report the distribution (are flattened ponds routinely 0, i.e. outlet-controlled, or do they retain meaningful freeboard?). Save `freeboard_dist.csv` + a CDF plot.

Run: `pixi run python scripts/diagnose/dprst_depth_probe.py --freeboard`
Expected: a freeboard distribution; a one-line finding — *"baseline freeboard carries most of the storage"* vs *"≈0, terrain model must carry it."*

- [ ] **Step 2: Commit**

```bash
git add scripts/diagnose/dprst_depth_probe.py
git commit -m "feat(spike): quantify freeboard over hydro-flattened dprst polygons (#173)"
```

---

### Task 6: Hollister terrain-slope max-depth prototype + max→mean (issue task 5)

**Files:**
- Modify: `scripts/diagnose/dprst_depth_probe.py`
- Test: `tests/test_dprst_depth_probe.py`

**Interfaces:**
- Consumes: `read_window`.
- Produces: `lake_max_depth(dem, polygon_mask, transform) -> float` — `lakeMorpho`-style max depth: extend the mean slope of the shoreline-adjacent terrain across the lake's max in-polygon distance-to-shore. `max_to_mean(max_depth, shape="cone") -> float` — documented conversion (cone V/A ⇒ mean = max/3; report the factor used).

- [ ] **Step 1: Write the failing test (geometry sanity, not field accuracy)**

```python
def test_lake_max_depth_scales_with_surrounding_slope():
    import numpy as np
    from affine import Affine

    # 41x41 grid, 1 m cells; circular lake radius ~10 in the centre.
    n = 41
    yy, xx = np.mgrid[0:n, 0:n]
    r = np.hypot(xx - 20, yy - 20)
    mask = r <= 10
    # Terrain slopes 0.2 m/m toward the lake; lake cells flat (water surface).
    dem = 100 - 0.2 * np.minimum(r, 10)
    dem[mask] = dem[mask].min()
    d = probe.lake_max_depth(dem.astype(np.float64), mask, Affine.identity())
    # ~ slope(0.2) * radius(10) order of magnitude; positive, not absurd.
    assert 0.5 < d < 5.0
    assert np.isclose(probe.max_to_mean(3.0, shape="cone"), 1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_lake_max_depth_scales_with_surrounding_slope -v`
Expected: FAIL — `lake_max_depth` / `max_to_mean` undefined.

- [ ] **Step 3: Write minimal implementation**

```python
from scipy import ndimage


def lake_max_depth(dem: np.ndarray, polygon_mask: np.ndarray, transform) -> float:
    """Hollister/lakeMorpho-style: project the mean shoreline slope inward to the
    lake's point of maximum distance-to-shore. Predicts MAX depth."""
    cell = abs(transform.a) if transform.a else 1.0
    # mean terrain slope in a shoreline ring just outside the lake
    ring = ndimage.binary_dilation(polygon_mask, iterations=2) & ~polygon_mask
    gy, gx = np.gradient(np.asarray(dem, float), cell)
    slope = np.hypot(gx, gy)
    mean_slope = float(slope[ring].mean()) if ring.any() else 0.0
    # max distance from any lake cell to the shore
    dist = ndimage.distance_transform_edt(polygon_mask) * cell
    return mean_slope * float(dist.max())


def max_to_mean(max_depth: float, shape: str = "cone") -> float:
    """dprst_depth_avg is MEAN (V/A). Conical basin: mean = max/3."""
    factors = {"cone": 1.0 / 3.0, "paraboloid": 1.0 / 2.0, "cylinder": 1.0}
    return max_depth * factors[shape]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e dev pytest tests/test_dprst_depth_probe.py::test_lake_max_depth_scales_with_surrounding_slope -v`
Expected: PASS.

- [ ] **Step 5: Prototype on an ND LakePond sample + calibrate max→mean**

Run `lake_max_depth` over the Task 4 LakePond sample; report the max-depth magnitude distribution and sanity-check against plausible pothole depths (≲ a few m). Establish the max→mean factor empirically where any real bathymetry exists (best-available; else document the conical `1/3` assumption and its basis). Save `hollister_sample.csv` + a scatter of max-depth vs polygon area.

Run: `pixi run python scripts/diagnose/dprst_depth_probe.py --hollister`
Expected: plausible magnitudes; a stated max→mean factor with its justification.

- [ ] **Step 6: Commit**

```bash
git add scripts/diagnose/dprst_depth_probe.py tests/test_dprst_depth_probe.py
git commit -m "feat(spike): Hollister terrain-slope max-depth prototype + max->mean (#173)"
```

---

### Task 7: Playa-anchored depth–area regression (issue task 6)

**Files:**
- Modify: `scripts/diagnose/dprst_depth_probe.py`

**Interfaces:**
- Consumes: `read_window`, `depth_to_spill`, `volume_mean_depth`, `dprst_polygons`.
- Produces: scratchpad `depth_area_regression.csv` + fit coefficients — analysis over existing primitives; no new tested function.

- [ ] **Step 1: Run the regression analysis**

Build the donor set: Playas (dry ⇒ DEM = bed) + sub-2-acre un-flattened polygons in ND. For each, measure `V/A` mean depth via `depth_to_spill` + `volume_mean_depth`. Fit `log(depth) ~ log(area)` overall and stratified by FTYPE/ecoregion. Report: does one power law span 900 m² → 10⁹ m², or must it be stratified? Quantify extrapolation risk into the recipient size range. Save the fit + a log-log scatter with the fitted line(s).

Run: `pixi run python scripts/diagnose/dprst_depth_probe.py --regression`
Expected: fit coefficients + R²; a clear single-vs-stratified verdict; explicit extrapolation-range caveat.

- [ ] **Step 2: Commit**

```bash
git add scripts/diagnose/dprst_depth_probe.py
git commit -m "feat(spike): playa-anchored depth-area regression prototype (#173)"
```

---

### Task 8: Synthesize `docs/dprst_depth_spike.md` (the deliverable)

**Files:**
- Create: `docs/dprst_depth_spike.md`

**Interfaces:**
- Consumes: all scratchpad artifacts (Tasks 2, 4–7): `coverage_audit.csv`, `flatness_by_ftype.csv`, `freeboard_dist.csv`, `hollister_sample.csv`, `depth_area_regression.csv`, and the PNGs.

- [ ] **Step 1: Write the spike report**

Follow the spec's writeup structure exactly:
1. Executive summary — go/no-go + one-line per-FTYPE verdict.
2. Study area & sample — project(s) chosen, per-FTYPE sample sizes, representativeness.
3. Six evidence sections (Tasks 2, 4, 4, 5, 6, 7 → coverage / flatness / SwampMarsh / freeboard / Hollister / regression): method → table or figure → finding.
4. **Decision table** — FTYPE × {% dprst area, best topo, flattened?, method chosen, fallback}, all cells filled from measured evidence.
5. Projected CONUS bucketing — area fraction per method bucket.
6. Go/no-go + Phase 1 recommendation, incl. whether the fallback share warrants revisiting `op_flow_thres = 1.0`.

Embed the PNGs (copy into `docs/` alongside the report). Cite the artifact CSVs and the sample sizes so the numbers are reproducible.

- [ ] **Step 2: Docs audit (repo rule)**

Confirm no other doc needs a same-branch update for a spike (the builder-touching docs — `pywatershed_depression_storage_requirements.md`, `ARCHITECTURE.md`, `RUNME.md`, `HPC_REFERENCE.md` — are Phase 1, not this spike). Note the deferral in the report's Phase 1 section. Add a one-line pointer to the spike report from `docs/` index if one exists.

- [ ] **Step 3: Commit**

```bash
git add docs/dprst_depth_spike.md docs/*.png
git commit -m "docs: #173 Phase 0 spike findings + per-FTYPE go/no-go decision"
```

---

## Self-Review

**Spec coverage:** coverage audit → Task 2; flatness detector → Task 4; SwampMarsh → Task 4; freeboard → Task 5; Hollister + max→mean → Task 6; depth–area regression → Task 7; writeup + decision table + go/no-go → Task 8; shared machinery (polygon set, windowed reader, depth-to-spill) → Tasks 1, 2, 3; best-available ladder + provenance → Tasks 2/3 (`best_topo`). All spec sections mapped.

**Placeholder scan:** the only `...` is `read_window`'s S3 body, deliberately left as a docstring-constrained implementer note (it needs live S3 and is smoke-tested, not unit-tested) — flagged as such, not a silent TODO. Decision-table cells are output slots filled in Task 8 from measured data. No other placeholders.

**Type consistency:** `dprst_polygons(gdf, set)→gdf`, `resolution_class(gdf,gdf)→gdf` (adds `best_topo`), `read_window(geom,str,...)→(array,transform,crs)`, `depth_to_spill(array)→array`, `volume_mean_depth(array,mask,float)→(v,a,mean)`, `is_hydroflattened(array)→dict`, `lake_max_depth(array,mask,transform)→float`, `max_to_mean(float,shape)→float` — names/signatures consistent across tasks.
