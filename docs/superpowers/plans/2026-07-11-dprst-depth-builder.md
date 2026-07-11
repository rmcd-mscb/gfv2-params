# dprst_depth Builder (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `dprst_depth` builder that derives per-HRU `dprst_depth_avg` (inches) from best-available 3DEP topography and emits `op_flow_thres = 1.0`, running CONUS in ≤ 5 hr wall-clock.

**Architecture:** Promote the spike's validated per-polygon functions into an importable library, add a tile-grouped compute that reads each 1 m tile once (SLURM-array-friendly), fill the hydro-flattened minority per-ecoregion (median null + calibrated-Hollister candidate), burn per-polygon V/A depth to a 30 m raster, and reuse the existing gdptools area-weighted zonal for per-HRU aggregation. Prove on Oregon, then CONUS.

**Tech Stack:** Python, geopandas/pyogrio, rasterio + GDAL `/vsicurl/`, richdem, numpy/pandas/scipy, gdptools (existing zonal), pixi. SLURM array for CONUS.

## Global Constraints

- **Reuse the spike's validated logic** — port the tested functions from `scripts/diagnose/dprst_depth_probe.py` verbatim into an importable module; do not re-derive. Source line numbers: `depth_to_spill`:126, `volume_mean_depth`:173, `is_hydroflattened`:157, `_normalize_nodata`:107, `lake_max_depth`:182, `max_to_mean`:217, `_tile13_name`:223, `read_window`:349, `dprst_polygons`:35, `resolution_class`:50.
- **RAW DEM only** for depth-to-spill; NEVER `fdr.vrt`/NHDPlus HydroDEM. richdem fill float64. No WBT with `predictor=2`.
- **Never materialise a 1 m lattice** — windowed per-polygon reads only; CONUS 1 m ≈ 1.5e13 cells. Follow the CONUS streaming rules in `CLAUDE.md`.
- **`/vsicurl/https://prd-tnm.s3.amazonaws.com/...`** for 3DEP (NOT `/vsis3/` — hangs on missing keys). 3DEP source nodata is `-999999`, normalized to `-9999`.
- **Paths from the fabric profile** via `require_config_key`; `{data_root}`/`{fabric}`/`{vpu}` placeholders; no hardcoded data paths, no new required CLI args.
- **Builder convention** (CLAUDE.md): builder module + DAG registration in `BUILDERS` + config block + `tests/test_dprst_depth.py`. Not a standalone script.
- **Reuse the shipped dprst classification** (`wbody_connectivity` → `dprst`) unchanged; do not modify the on-stream classifier.
- Ecoregion is a **shared, reusable input** (not fabric-specific): staged under `{data_root}/input/ecoregions/`, path in the shared/base config.
- **No `pytest` on the HPC head node** beyond a single test file; CI is the gate. `py_compile`/imports fine.
- **Regional-fill floor = NHM constant 49 in.** `op_flow_thres = 1.0`. Fallback order for sparse donors: FTYPE×ecoregion median → ecoregion median → FTYPE median → 49 in constant — `log` each fallback, never silent.

---

### Task 1: Promote spike topo functions to an importable library

**Files:**
- Create: `src/gfv2_params/dprst_depth/__init__.py`, `src/gfv2_params/dprst_depth/topo.py`
- Modify: `scripts/diagnose/dprst_depth_probe.py` (import from the new module instead of defining locally)
- Test: `tests/test_dprst_depth_topo.py`

**Interfaces:**
- Produces (all moved verbatim, signatures unchanged): `depth_to_spill(dem, nodata=None) -> np.ndarray`, `volume_mean_depth(depth, mask, cell_area_m2) -> (v, a, mean)`, `is_hydroflattened(dem_in_polygon, tol_m=0.01) -> dict`, `_normalize_nodata(arr, src_nodata, sentinel=-9999.0) -> np.ndarray`, `lake_max_depth(dem, polygon_mask, transform) -> float`, `max_to_mean(max_depth, shape="cone") -> float`, `_tile13_name(lon, lat) -> str`, `read_window(geom, best_topo, wesm_row=None, rim_buffer_m=200.0)`, `dprst_polygons(wb_gdf, connected) -> gdf`, `resolution_class(dprst_gdf, wesm_gdf) -> gdf`. Later tasks import these.

- [ ] **Step 1: Move the functions verbatim**

Create `src/gfv2_params/dprst_depth/topo.py`. Copy the 10 functions listed in Global Constraints from `scripts/diagnose/dprst_depth_probe.py` **verbatim** (including the `lake_max_depth` nodata-ring guard already committed in 9cf0fe3), plus their imports (`numpy`, `rasterio`, `richdem as rd`, `scipy.ndimage`, `geopandas`, `from ..nhd_ftypes import ...`, `from ..depstor import select_connected_waterbodies`, GDAL, the `TILE13_S3_TEMPLATE` / `_native_resolution` / `_existing_paths` / `_resolve_1m_paths` helpers `read_window` depends on). Add `from __future__ import annotations` and a module docstring. `__init__.py` re-exports them.

- [ ] **Step 2: Re-point the diagnostic**

In `scripts/diagnose/dprst_depth_probe.py`, delete the moved function bodies and add `from gfv2_params.dprst_depth.topo import (depth_to_spill, volume_mean_depth, is_hydroflattened, lake_max_depth, max_to_mean, read_window, dprst_polygons, resolution_class, _normalize_nodata, _tile13_name)`. Leave the CLI/analysis modes intact.

- [ ] **Step 3: Move the unit tests**

Copy the topo-function unit tests from `tests/test_dprst_depth_probe.py` into `tests/test_dprst_depth_topo.py`, importing from `gfv2_params.dprst_depth.topo` directly (a real package now — no importlib shim). Keep the 8 assertions (bowl V/A, nodata void, flatness, `_normalize_nodata`, `lake_max_depth` geometry + nodata-ring). Leave the probe's own tests importing the probe (which now re-exports).

- [ ] **Step 4: Run tests**

Run: `pixi run -e dev pytest tests/test_dprst_depth_topo.py tests/test_dprst_depth_probe.py -v`
Expected: all pass (8 in topo + the probe's existing ones), pristine apart from the known richdem deprecation warning.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/ tests/test_dprst_depth_topo.py scripts/diagnose/dprst_depth_probe.py
git commit -m "refactor(dprst_depth): promote spike topo functions to importable library"
```

---

### Task 2: Stage EPA ecoregions as a shared input

**Files:**
- Create: `src/gfv2_params/download/epa_ecoregions.py`
- Modify: `configs/shared_rasters/shared_rasters.yml` or `configs/base_config.yml` (record the shared ecoregion path)
- Test: `tests/test_epa_ecoregions.py`

**Interfaces:**
- Produces: `ecoregion_of(points_gdf, eco_gdf) -> pd.Series` (centroid spatial join → ecoregion id per polygon, a pure function, unit-tested offline); `stage_ecoregions(dest_dir, logger) -> Path` (resolves a reachable source, downloads, writes `us_eco_l3l4.gpkg` in EPSG:5070; network, not unit-tested).

- [ ] **Step 1: Write the failing test for the pure join**

```python
# tests/test_epa_ecoregions.py
import geopandas as gpd
from shapely.geometry import Point, box
from gfv2_params.download.epa_ecoregions import ecoregion_of

def test_ecoregion_of_assigns_by_centroid():
    polys = gpd.GeoDataFrame({"COMID":[1,2]},
        geometry=[Point(0.5,0.5).buffer(0.2), Point(9,9).buffer(0.2)], crs="EPSG:5070")
    eco = gpd.GeoDataFrame({"US_L3CODE":["17","80"]},
        geometry=[box(0,0,1,1), box(8,8,10,10)], crs="EPSG:5070")
    out = ecoregion_of(polys, eco)
    assert list(out) == ["17", "80"]
```

- [ ] **Step 2: Run — verify it fails** — `pixi run -e dev pytest tests/test_epa_ecoregions.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement**

```python
# src/gfv2_params/download/epa_ecoregions.py
"""Stage EPA Level III/IV Ecoregions of the conterminous US as a shared input.
Reusable across parameterizations; not fabric-specific."""
from __future__ import annotations
from pathlib import Path
import geopandas as gpd
import pandas as pd

ECO_ID_FIELD = "US_L3CODE"  # Level III; L4 available via US_L4CODE

def ecoregion_of(points_gdf: gpd.GeoDataFrame, eco_gdf: gpd.GeoDataFrame,
                 id_field: str = ECO_ID_FIELD) -> pd.Series:
    """Assign each polygon its ecoregion by centroid-in-polygon join."""
    pts = points_gdf.set_geometry(points_gdf.geometry.centroid)
    eco = eco_gdf.to_crs(points_gdf.crs)[[id_field, "geometry"]]
    hit = gpd.sjoin(pts, eco, how="left", predicate="within")
    return hit.groupby(level=0)[id_field].first()

def stage_ecoregions(dest_dir: Path, logger) -> Path:
    """Download EPA L3/L4 ecoregions to dest_dir/us_eco_l3l4.gpkg (EPSG:5070).
    Fails loud if no source is reachable — this HPC only reaches AWS S3 over
    HTTPS; the EPA host must be probed and a reachable mirror used if blocked."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / "us_eco_l3l4.gpkg"
    if out.exists():
        logger.info("Ecoregion layer already staged: %s", out)
        return out
    # implementation: probe reachable source (EPA gaftp / ScienceBase / S3 mirror),
    # download the L4 shapefile/zip, reproject to EPSG:5070, write gpkg. Raise a
    # clear error if none reachable (do NOT silently skip).
    raise NotImplementedError("resolve reachable ecoregion source in Step 5")
```

- [ ] **Step 4: Run test to verify the join passes** — `pixi run -e dev pytest tests/test_epa_ecoregions.py -v` → PASS.

- [ ] **Step 5: Resolve the download source + stage live**

Probe reachability (mirror the S3 test): try the EPA host, ScienceBase, and any S3 mirror for the L3/L4 ecoregion layer; implement `stage_ecoregions`' body against the first reachable one; reproject to EPSG:5070; write the gpkg. Run `pixi run python -m gfv2_params.download.epa_ecoregions --dest {data_root}/input/ecoregions` and confirm a valid gpkg (CONUS extent, non-empty L3/L4 codes). Record the path in the shared/base config. If NO source is reachable from the HPC, STOP and report — the controller escalates (manual stage).

- [ ] **Step 6: Commit**

```bash
git add src/gfv2_params/download/epa_ecoregions.py tests/test_epa_ecoregions.py configs/
git commit -m "feat(dprst_depth): stage EPA ecoregions as a shared reusable input (#173)"
```

---

### Task 3: Tile-grouped work-list (polygon → 1 m tile, no 404 probes)

**Files:**
- Create: `src/gfv2_params/dprst_depth/tiling.py`
- Test: `tests/test_dprst_depth_tiling.py`

**Interfaces:**
- Consumes: `dprst_polygons`, `resolution_class`, WESM index.
- Produces: `group_by_tile(dprst_gdf, wesm_gdf) -> dict[str, list[int]]` — maps each covering 1 m tile key (or the 10 m tile for `best_topo=="10m"`) to the polygon indices whose window intersects it, resolved from the WESM/tile index up front (NO per-polygon `/vsicurl` existence probe). `tile_batches(groups, n_batches) -> list[list[str]]` — partition tile keys into ~`n_batches` roughly-equal-work SLURM array batches.

- [ ] **Step 1: Failing test**

```python
# tests/test_dprst_depth_tiling.py
import geopandas as gpd
from shapely.geometry import box
from gfv2_params.dprst_depth.tiling import group_by_tile, tile_batches

def test_group_by_tile_and_batching():
    # two polygons in a 10m fallback area (no 1m footprint) → grouped by 10m tile
    dprst = gpd.GeoDataFrame({"COMID":[1,2], "best_topo":["10m","10m"]},
        geometry=[box(0,0,10,10), box(20,20,30,30)], crs="EPSG:5070")
    wesm = gpd.GeoDataFrame({"workunit":[]}, geometry=[], crs="EPSG:5070")
    groups = group_by_tile(dprst, wesm)
    assert sum(len(v) for v in groups.values()) == 2      # every polygon placed
    batches = tile_batches(groups, n_batches=2)
    assert sum(len(b) for b in batches) == len(groups)     # every tile in one batch
```

- [ ] **Step 2: Run — verify fail.**

- [ ] **Step 3: Implement** `group_by_tile` (reproject polygon window bbox to the tile grid CRS; enumerate covering tile keys from the WESM footprint / the 10 m tile name via `_tile13_name`; no existence probe) and `tile_batches` (greedy bin-pack by polygon count). Use `resolution_class`'s `best_topo` to pick 1 m project tiles vs the 10 m tile.

- [ ] **Step 4: Run test → PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/tiling.py tests/test_dprst_depth_tiling.py
git commit -m "feat(dprst_depth): tile-grouped work-list for one-read-per-tile fan-out (#173)"
```

---

### Task 4: Per-tile compute → per-batch parquet

**Files:**
- Create: `src/gfv2_params/dprst_depth/compute.py`
- Test: `tests/test_dprst_depth_compute.py`

**Interfaces:**
- Consumes: `read_window`, `depth_to_spill`, `volume_mean_depth`, `is_hydroflattened`, the interior-mask helper (port `_interior_mask` from the probe into `topo.py` in Task 1 if not already), `lake_max_depth`.
- Produces: `compute_polygon(geom, best_topo, wesm_row) -> dict` — returns `{dprst_depth_m, measured_max_m, flat, resolution, method}` for one polygon (method ∈ {"measured","flat_pending"}); flat polygons return `dprst_depth_m=nan, flat=True, method="flat_pending"` (filled in Task 5). `run_batch(dprst_gdf, tile_keys, wesm_gdf, out_parquet, logger)` — reads each tile once, runs `compute_polygon` for its polygons, writes the batch parquet.

- [ ] **Step 1: Failing test (synthetic tile, no S3)**

```python
# tests/test_dprst_depth_compute.py
import numpy as np
from affine import Affine
from gfv2_params.dprst_depth.compute import _polygon_depth_from_dem

def test_polygon_depth_from_dem_bowl_and_flat():
    # 20x20, 1 m cells; 8x8 pit depth 2 in the centre; rest flat rim
    dem = np.full((20,20), 10.0, np.float64); dem[6:14,6:14] = 8.0
    mask = np.zeros((20,20), bool); mask[6:14,6:14] = True   # interior = the pit
    r = _polygon_depth_from_dem(dem, mask, Affine.scale(1,-1), nodata=-9999.0)
    assert not r["flat"]
    assert np.isclose(r["dprst_depth_m"], 2.0)     # V/A over the pit
    assert np.isclose(r["measured_max_m"], 2.0)
    flat = np.full((20,20), 5.0, np.float64)
    rf = _polygon_depth_from_dem(flat, mask, Affine.scale(1,-1), nodata=-9999.0)
    assert rf["flat"] and np.isnan(rf["dprst_depth_m"])
```

- [ ] **Step 2: Run — verify fail.**

- [ ] **Step 3: Implement** `_polygon_depth_from_dem` (is_hydroflattened on the interior → flat: return nan/flat=True; non-flat: `depth_to_spill` → `volume_mean_depth` over the interior mask → dprst_depth_m + measured_max_m + lake_max_depth for the Task-5 calibration), then `compute_polygon` (wraps `read_window` + provenance) and `run_batch` (read each tile once via a small tile→WarpedVRT cache, iterate its polygons, write parquet).

- [ ] **Step 4: Run test → PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/compute.py tests/test_dprst_depth_compute.py
git commit -m "feat(dprst_depth): per-tile compute of per-polygon depth + provenance (#173)"
```

---

### Task 5: Per-ecoregion fill — median null + calibrated-Hollister candidate

**Files:**
- Create: `src/gfv2_params/dprst_depth/fill.py`
- Test: `tests/test_dprst_depth_fill.py`

**Interfaces:**
- Consumes: the concatenated per-batch parquet (COMID, dprst_depth_m, measured_max_m, flat, ftype, ecoregion, method), `max_to_mean`, `lake_max_depth` outputs (a `hollister_max_m` column added in Task 4's compute for non-flat polys used as calibration fit data).
- Produces: `fit_ecoregion_models(non_flat_df) -> dict[(eco,ftype), Model]` — per group fits the median null and a calibrated Hollister (`k`, shape factor) and picks by **cross-validated** RMSE, recording which won; `fill_flat(df, models, floor_in=49.0) -> df` — fills flat/degenerate rows via the chosen per-group model, applying the fallback ladder (FTYPE×eco → eco → FTYPE → 49 in floor), setting `method` and `dprst_depth_m`; converts everything to inches downstream.

- [ ] **Step 1: Failing test**

```python
# tests/test_dprst_depth_fill.py
import numpy as np, pandas as pd
from gfv2_params.dprst_depth.fill import fill_flat, fit_ecoregion_models

def test_fill_flat_uses_group_median_and_floor():
    df = pd.DataFrame({
        "COMID":[1,2,3,4], "ftype":["LakePond"]*4, "ecoregion":["17"]*4,
        "dprst_depth_m":[1.0, 2.0, np.nan, np.nan], "measured_max_m":[1.5,3.0,np.nan,np.nan],
        "hollister_max_m":[1.2,2.8,4.0,0.1], "flat":[False,False,True,True]})
    models = fit_ecoregion_models(df[~df.flat])
    out = fill_flat(df, models, floor_in=49.0)
    filled = out[out.flat]
    assert filled["dprst_depth_m"].notna().all()          # no NaN left
    assert (filled["dprst_depth_m"] > 0).all()
    # group median of measured non-flat = median(1.0,2.0)=1.5 m unless Hollister won
    assert filled["method"].isin({"regional_fill","calibrated_hollister","constant_floor"}).all()

def test_fill_flat_floor_when_no_donors():
    df = pd.DataFrame({"COMID":[1],"ftype":["Playa"],"ecoregion":["80"],
        "dprst_depth_m":[np.nan],"measured_max_m":[np.nan],"hollister_max_m":[np.nan],"flat":[True]})
    out = fill_flat(df, fit_ecoregion_models(df[~df.flat]), floor_in=49.0)
    assert out.loc[0,"method"] == "constant_floor"
    assert np.isclose(out.loc[0,"dprst_depth_m"]*39.3701, 49.0)  # 49 in floor
```

- [ ] **Step 2: Run — verify fail.**

- [ ] **Step 3: Implement** `fit_ecoregion_models` (per (eco,ftype): null = median measured `dprst_depth_m`; candidate = calibrated Hollister `mean = shape * k * hollister_max` with `k`, shape fit on measured non-flat via least squares; K-fold CV RMSE picks winner; require ≥ N_MIN donors else the group is "median-only"/fallback) and `fill_flat` (apply chosen model to flat rows; fallback ladder FTYPE×eco → eco → FTYPE → 49 in floor; set `method`; guarantee no NaN, all > 0). `log` the per-group winner + fallback counts.

- [ ] **Step 4: Run tests → PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/fill.py tests/test_dprst_depth_fill.py
git commit -m "feat(dprst_depth): per-ecoregion fill (median null + calibrated-Hollister candidate) (#173)"
```

---

### Task 6: Burn per-polygon depth → 30 m `dprst_depth.tif`

**Files:**
- Create: `src/gfv2_params/dprst_depth/burn.py`
- Test: `tests/test_dprst_depth_burn.py`

**Interfaces:**
- Consumes: the filled per-polygon depth frame (COMID → dprst_depth_m, all finite > 0) joined back to `dprst_polygons` geometries; the fabric template raster (`template_raster` from the profile) + `land_mask.tif`.
- Produces: `burn_depth(dprst_gdf_with_depth, template_path, land_mask_path, out_tif, logger)` — rasterizes each polygon's `dprst_depth_m` (metres) onto the 30 m template grid, masked to dprst cells / land, streaming by windowed strips (CONUS-safe per `carea_map`'s `STRIP_ROWS`), nodata elsewhere.

- [ ] **Step 1: Failing test (small synthetic grid)**

```python
# tests/test_dprst_depth_burn.py
import numpy as np, geopandas as gpd, rasterio
from shapely.geometry import box
from gfv2_params.dprst_depth.burn import burn_depth

def test_burn_depth_writes_polygon_values(tmp_path):
    # 10x10 template, 1 m cells; one polygon depth 2.0 over a 4x4 block
    tmpl = tmp_path/"tmpl.tif"
    tr = rasterio.transform.from_origin(0,10,1,1)
    with rasterio.open(tmpl,"w",driver="GTiff",height=10,width=10,count=1,
                       dtype="float32",crs="EPSG:5070",transform=tr,nodata=-9999) as d:
        d.write(np.ones((10,10),np.float32),1)
    lm = tmp_path/"lm.tif"
    with rasterio.open(lm,"w",driver="GTiff",height=10,width=10,count=1,
                       dtype="uint8",crs="EPSG:5070",transform=tr,nodata=0) as d:
        d.write(np.ones((10,10),np.uint8),1)
    g = gpd.GeoDataFrame({"dprst_depth_m":[2.0]}, geometry=[box(2,2,6,6)], crs="EPSG:5070")
    out = tmp_path/"dprst_depth.tif"
    burn_depth(g, str(tmpl), str(lm), str(out), logger=_L())
    with rasterio.open(out) as d:
        a = d.read(1)
    assert np.isclose(a[a!=d.nodata].max(), 2.0)     # burned depth present
```
(Provide a trivial `_L()` returning a logging.getLogger.)

- [ ] **Step 2: Run — verify fail.**

- [ ] **Step 3: Implement** `burn_depth` using `rasterio.features.rasterize` per windowed strip against the template transform, masked to `land_mask`, dtype float32, nodata -9999. Never hold the full CONUS grid — strip rows like `carea_map`.

- [ ] **Step 4: Run test → PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/burn.py tests/test_dprst_depth_burn.py
git commit -m "feat(dprst_depth): burn per-polygon depth to 30 m dprst_depth.tif (streamed) (#173)"
```

---

### Task 7: Builder module + DAG registration + config block + op_flow_thres

**Files:**
- Create: `src/gfv2_params/depstor_builders/dprst_depth.py`
- Modify: `src/gfv2_params/depstor_builders/__init__.py` (`BUILDERS` + `STEP_ORDER`), `src/gfv2_params/depstor_builders/context.py` (new ctx keys), `configs/depstor/depstor_rasters.yml`, `configs/depstor/depstor_params.yml`
- Test: `tests/test_dprst_depth.py`

**Interfaces:**
- Consumes: Tasks 2–6 modules; `BuildContext`.
- Produces: `build(step_cfg, ctx, logger) -> dict` following the depstor builder contract (see `waterbody.py`): loads the dprst polygon set, tags `best_topo` + ecoregion, runs the tile-grouped compute (in-process for small fabrics; consumes the SLURM per-batch parquet for CONUS — Task 9), fills, burns `dprst_depth.tif`, returns `{"dprst_depth": out_path}`. Also emits `op_flow_thres = 1.0` into the constant-param output.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dprst_depth.py
from gfv2_params.depstor_builders import BUILDERS
def test_dprst_depth_registered():
    assert "dprst_depth" in BUILDERS
    assert callable(BUILDERS["dprst_depth"].build)
```
Add a second test exercising `build` on a tiny synthetic fabric (2–3 polygons over a synthetic local DEM written to tmp, monkeypatching `read_window` to read the local file) → asserts a `dprst_depth.tif` is written and a HRU CSV row has `dprst_depth_avg > 0`, and that `op_flow_thres == 1.0` is emitted.

- [ ] **Step 2: Run — verify fail** (builder unregistered).

- [ ] **Step 3: Implement** `dprst_depth.py` `build(...)` orchestrating Tasks 2–6; register in `BUILDERS`/`STEP_ORDER` after `dprst`/`landmask` (needs `land_mask.tif` + the dprst classification); add ctx keys (3DEP templates, WESM path, ecoregion path, rim buffer, flatness tol, floor, calibrated-Hollister controls) reading via `require_config_key`; add the `depstor_rasters.yml` block and a `depstor_params.yml` mean-aggregation entry for `dprst_depth.tif`. Emit `op_flow_thres=1.0` in the constant-param writer.

- [ ] **Step 4: Run tests → PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/depstor_builders/ configs/depstor/ tests/test_dprst_depth.py
git commit -m "feat(dprst_depth): builder + DAG registration + config + op_flow_thres=1.0 (#173)"
```

---

### Task 8: Per-HRU aggregation wiring (reuse gdptools zonal)

**Files:**
- Modify: `configs/depstor/depstor_params.yml` (finalize the `dprst_depth` mean-aggregation entry + provenance), docs as needed
- Test: extend `tests/test_dprst_depth.py`

**Interfaces:**
- Consumes: `dprst_depth.tif` (Task 6/7) + the existing depstor zonal runner.
- Produces: `{fabric}/params/merged/nhm_dprst_depth_avg_params.csv` — `{id_feature}, dprst_depth_avg` (inches) + provenance column; area-weighted mean over dprst cells = per-HRU `ΣV/ΣA`; HRUs with `dprst_frac==0` get the floor, never NaN.

- [ ] **Step 1: Write the failing test** — extend `tests/test_dprst_depth.py` with a small end-to-end: synthetic `dprst_depth.tif` (two HRUs, known burned depths) → run the mean-zonal path → assert the CSV `dprst_depth_avg` equals the expected area-weighted mean (in inches) and no NaN.

- [ ] **Step 2: Run — verify fail.**

- [ ] **Step 3: Implement** the `depstor_params.yml` entry (mean aggregation of `dprst_depth.tif`, m→in conversion, provenance passthrough) and any zonal-runner tweak needed to support a mean (vs the existing binary-count) aggregation. Fill `dprst_frac==0` HRUs with the floor in the writer.

- [ ] **Step 4: Run test → PASS.**

- [ ] **Step 5: Commit**

```bash
git add configs/depstor/depstor_params.yml src/gfv2_params/ tests/test_dprst_depth.py
git commit -m "feat(dprst_depth): per-HRU mean aggregation -> nhm_dprst_depth_avg_params.csv (#173)"
```

---

### Task 9: SLURM per-tile array + docs

**Files:**
- Create: `slurm_batch/submit_dprst_depth.sh` (+ any array driver)
- Modify: `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`, `docs/ARCHITECTURE.md`, `docs/pywatershed_depression_storage_requirements.md`

**Interfaces:**
- Consumes: Task 3 `tile_batches` (the array index → tile-batch mapping) and Task 4 `run_batch`.
- Produces: a SLURM array that runs `run_batch` per array task (each reads its tiles once), writing per-batch parquet; a chained fill+burn+aggregate job (afterok). Sized so CONUS lands in ≤ 5 hr (~100–200 tasks; document the sizing arithmetic).

- [ ] **Step 1: Write the array submit script** — `submit_dprst_depth.sh` invoking `pixi run --as-is` per array task (per the SLURM/pixi rule in CLAUDE.md), array size from a `--n-batches` arg; a dependent fill/burn/aggregate job.

- [ ] **Step 2: Dry-run the batching** — `pixi run python -m gfv2_params.dprst_depth.tiling --plan --n-batches 150` (add a small `__main__`) prints the per-batch tile/polygon counts for a fabric; confirm balanced batches and the projected core-hours / ≤ 5 hr sizing. No live S3.

- [ ] **Step 3: Docs** — document the step in `RUNME.md` (happy path) + `HPC_REFERENCE.md` (detail/recovery) + a paragraph in `ARCHITECTURE.md` (per the "add a new pipeline step" section); update `pywatershed_depression_storage_requirements.md` (dprst_depth_avg is now derived + op_flow_thres=1.0). Per the repo docs-audit rule, surface findings.

- [ ] **Step 4: Commit**

```bash
git add slurm_batch/ docs/
git commit -m "feat(dprst_depth): SLURM per-tile array + runbook/architecture docs (#173)"
```

---

### Task 10: Oregon end-to-end validation + calibrated-Hollister evaluation

**Files:**
- Create: `docs/dprst_depth_oregon_validation.md`

- [ ] **Step 1: Run on Oregon** — `submit_dprst_depth.sh oregon` (or the in-process path for a small fabric). Produce `oregon/params/merged/nhm_dprst_depth_avg_params.csv` + provenance + `dprst_depth.tif`.

- [ ] **Step 2: Evaluate** — distribution of `dprst_depth_avg` vs the prior on-disk Oregon param set and the NHM calibrated distribution (median ~49 in, order-of-magnitude). Per-ecoregion: did calibrated-Hollister beat the median null anywhere (report the winners + CV skill)? Report provenance shares (measured / regional_fill / calibrated_hollister / constant_floor) and the 1 m/10 m split. Humid vs arid ecoregion contrast.

- [ ] **Step 3: Write up** `docs/dprst_depth_oregon_validation.md` with a go/no-go for the CONUS run and any parameter/threshold adjustments.

- [ ] **Step 4: Commit**

```bash
git add docs/dprst_depth_oregon_validation.md
git commit -m "docs: Oregon validation of the dprst_depth builder + calibrated-Hollister eval (#173)"
```

---

## Self-Review

**Spec coverage:** ecoregion staging (§0)→Task 2; config block (§1)→Tasks 2/7; per-tile SLURM compute (§2)→Tasks 3,4,9; regional-fill + calibrated-Hollister (§3)→Task 5; burn (§3)→Task 6; per-HRU aggregation (§4)→Task 8; op_flow_thres (§5)→Task 7; convention deliverables (§6: builder+DAG+config+tests)→Task 7; function reuse→Task 1; Oregon sequencing→Task 10; CONUS run→Task 9 sizing + Task 10 go/no-go. All spec sections mapped.

**Placeholder scan:** `read_window`'s ported S3 body and `stage_ecoregions`'s download body are the only `NotImplementedError`/`...` bodies — both flagged as network-dependent and closed in their task's live step (Task 1 Step 1 ports the already-working body; Task 2 Step 5 resolves the source). No silent TODOs.

**Type consistency:** `dprst_depth_m` (metres, float) is the per-polygon field throughout Tasks 4–6; inches conversion (×39.3701) happens once at aggregation (Task 8). `flat` (bool), `method` (str enum), `ecoregion`/`ftype` (str) are consistent across compute→fill→burn. `group_by_tile`→`tile_batches`→`run_batch` share the tile-key/str contract.
