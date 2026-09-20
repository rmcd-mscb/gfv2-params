# dprst_depth: Real 3DEP Tile Inventory, Correct Reads, and a Fast Re-run — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix issue #223 (clipped DEM windows with an unclipped transform: empty reads and silent misregistration) and remove its root cause (tile assignment from convex-hull WESM footprints). Make the tiled `dprst_depth` array fast enough that a fabric re-run is a few hours rather than >24 h, then re-run gfv2r2.

**Architecture:** A one-time staging step records every published 3DEP 1 m tile with its **real** header extent, plus per-project WESM quality/date attributes. Planning assigns each dprst polygon a ranked list of candidate **tile sets** (one project, one UTM zone, whose real tiles intersect the polygon's window). The primary set is chosen by `covers-window → QL → newest collection → project name`, and the 10 m seamless tile is always the last resort. The SLURM array groups polygons by primary tile set, opens each set once, and processes the sets on a thread pool. Every windowed read goes through one clip-and-pad helper, so the returned array always matches its transform.

**Tech Stack:** Python 3.12, rasterio 1.5 / GDAL 3.12 (`/vsicurl/`), geopandas/shapely, pyogrio, numpy, `concurrent.futures.ThreadPoolExecutor`, pytest, SLURM, pixi.

**Spec:** Issue #223 and its comment 5743299538: https://github.com/rmcd-mscb/gfv2-params/issues/223. The evidence behind every decision here is recorded there and in `logs/diag_gradient/` on the HPC data root.

## Global Constraints

- **Base branch:** `main` **after PR #222 (`fix/dprst-depth-order-221`) is merged.** #222 rewrites `depstor_builders/dprst_depth.py` (plan-ownership guard `_verify_batches_match_plan`, in-process ceiling) and `tiling.py` (`_clear_stale_batches`). Do not start Task 1 on a base without it.
- **Two PRs:** PR A = Tasks 1–2 (read correctness + HTTP resilience). PR B = Tasks 3–8 (inventory, sources, planner, compute, wiring, docs). PR B branches from PR A's merge. The re-run (Tasks 9–11) happens after PR B merges.
- **Output identity:** for a polygon whose window lies wholly inside the tile it was read from, the computed depth must be **bit-identical** to the pre-change code. Changes are only allowed for overhanging windows (Task 1) or a changed source tile set (Tasks 3–6).
- **Never run pytest on the HPC login node.** CI is the gate. To run tests yourself: `srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G pixi run -e dev --as-is pytest tests/<file> -q`.
- **Worktrees have no pixi env.** Use `PYTHONPATH=<worktree>/src` with the main checkout's `.pixi/envs/dev/bin/python`. Never symlink the main env into a worktree.
- **All `srun`/`sbatch` pixi invocations use `--as-is`.**
- **Paths come from the fabric profile** via `require_config_key(...)` / `{data_root}` placeholders, never hardcoded.
- **Atomic commits.** One logical change per commit. Every code change carries a docs check (`docs/`, `README.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`), and `.amazonq/rules/workflow.md` is synced for completed issues and new pixi tasks.
- **Don't touch the running gfv2r2 chain's checkout.** Jobs 4464492 → 4439813 execute from the main checkout on `feat/gfv2r2-fabric`. Do all development in a worktree.
- The 3DEP HTTPS base is `https://prd-tnm.s3.amazonaws.com`. 1 m projects live under `StagedProducts/Elevation/1m/Projects/<project>/TIFF/USGS_1M_<zz>_x<X>y<Y>_<project>.tif`. WESM attributes are read geometry-free from `/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg` (slow: ~10+ min, run it in a batch job): fields `workunit`, `project`, `ql` (values `QL 0`/`QL 1`/`QL 2` among qualifying rows), `collect_end` (`datetime64[D]`), `sourcedem_link`, `lpc_link`.

## Decisions locked before planning

| Decision | Choice | Why |
|---|---|---|
| Source of truth for which tiles exist and their extents | S3 listing + each tile's own header (not WESM geometry, not the project VRT) | WESM hulls invent overlaps (61% of "overlapping" polygons have one real tile, 15% none). The project VRT puts other-zone tiles in one zone's grid (WI_12County_B22: 300 zone-15 + 167 zone-16 tiles in a zone-15 VRT). A cropped tile's header shows its real extent (`x27y511`: 10000×4289). |
| Project preference where overlaps are real | 1) primary set's real tiles cover the whole window, 2) best QL (lowest number), 3) newest `collect_end`, 4) project name | Rich, 2026-09-19. Denser points define the shoreline rim better, and the rim drives depth. Newest breaks QL ties; name makes it deterministic. |
| `best_topo` tag | `"1m"` iff the polygon centroid lies inside some real tile's EPSG:5070 bounds | Replaces the hull-based `resolution_class`. |
| Multi-zone projects | A tile set holds one zone only: the zone whose tiles overlap the window most | `gdal.BuildVRT` cannot mosaic sources in different CRSs. |
| Recovery | If a set's read yields an interior with 0 valid cells (or a read error), try the next candidate; the 10 m tile is always last | Folds in #223 fix step 2. |
| Batching unit | Primary tile set (each polygon in exactly one set) | Removes the union-find components whose chaining produced the >4k-tile component and the batch 0/1 long pole. |
| Concurrency | Threads over tile sets inside each array task; `--cpus-per-task=8`, `--threads` from `$SLURM_CPUS_PER_TASK` | 73% of fallback time is `vrt.read` I/O; GDAL releases the GIL. |
| HTTP resilience | `GDAL_HTTP_TIMEOUT=60`, `GDAL_HTTP_MAX_RETRY=5`, `GDAL_HTTP_RETRY_DELAY=2` everywhere | Seven tasks on node cn132 hung for ~6.9 h on a stalled socket. |
| Joining S3 project directories to WESM | `workunit == dir`, else `project == dir`, else a `sourcedem_link`/`lpc_link` containing `/Projects/<dir>/`; QL = best, date = newest over the matched rows; no `onemeter_category` filter; staging raises if <90% of directories match | Measured 2026-09-19: exact `project` matches only 366 of 967 S3 dirs, `workunit` 572, all three together 932 (96.4%). A dir under `1m/Projects/` is itself proof the data qualifies. The 35 unmatched rank last. |
| Legacy WESM hull index | Profile key `wesm_index` and `download/wesm.py` are retired; `wesm_io.py` stays for the Phase-0 probe | Nothing else reads the hull index after this change. |

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `src/gfv2_params/dprst_depth/topo.py` | modify | `GDAL_HTTP_ENV`, `read_padded` (clip-and-pad), `lake_max_depth` degenerate guard; `read_window` uses `read_padded`; `resolution_class` kept but documented as Phase-0-audit-only (the probe imports it) |
| `src/gfv2_params/dprst_depth/inventory.py` | **create** | S3 listing, tile-header reads, inventory build/load, WESM project attributes, `ql_rank` |
| `src/gfv2_params/download/dem_1m_inventory.py` | **create** | Staging CLI → `input/3dep/dem_1m_tile_inventory.parquet` + `input/wesm/wesm_project_attrs.parquet` |
| `slurm_batch/stage_dem_1m_inventory.batch` | **create** | SLURM wrapper for the staging CLI |
| `src/gfv2_params/dprst_depth/sources.py` | **create** | `TileSet`, encode/decode, `tag_best_topo`, `rank_candidates`, `assign_sources`, `tag_and_assign` (shared by builder and planner) |
| `src/gfv2_params/dprst_depth/tiling.py` | modify | Planner uses `tag_and_assign` + tile-set batching; delete `group_by_tile`, `_tile_components`, `component_tile_batches` |
| `src/gfv2_params/dprst_depth/compute.py` | modify | `open_tile_set`, tile-set `run_batch` with threads + candidate recovery; delete `_project_lookup` and the hull fallback |
| `scripts/run_dprst_depth_batch.py`, `slurm_batch/run_dprst_depth_batch.batch` | modify | Read `tile_sets` from the manifest, `--threads`, 8 CPUs |
| `src/gfv2_params/depstor_builders/dprst_depth.py`, `context.py`, `scripts/build_depstor_rasters.py` | modify | Use `tag_and_assign`; new ctx fields; carry the `source` column into provenance |
| `configs/base_config.yml` | modify | Replace `wesm_index` with `dem_1m_inventory` + `wesm_project_attrs` in all five profiles |
| `scripts/diagnose/compare_dprst_depth_runs.py` | **create** | Baseline-vs-new validation for the re-run |
| `tests/test_dprst_depth_topo.py`, `tests/test_dprst_depth_inventory.py` (new), `tests/test_dprst_depth_sources.py` (new), `tests/test_dprst_depth_tiling.py`, `tests/test_dprst_depth_compute.py`, `tests/test_dprst_depth.py`, `tests/test_compare_dprst_depth_runs.py` (new) | modify/create | See each task |
| `CLAUDE.md`, `.amazonq/rules/workflow.md`, `docs/ARCHITECTURE.md`, `docs/dprst_depth_avg_reference.md`, `README.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md` | modify | Task 8 |

---

## PR A — read correctness and resilience (closes the silent-misregistration half of #223)

### Task 1: Clip-and-pad windowed reads, degenerate-window guard, HTTP timeouts

**Files:**
- Modify: `src/gfv2_params/dprst_depth/topo.py` (add `GDAL_HTTP_ENV`, `read_padded`; guard in `lake_max_depth`; `read_window` calls `read_padded` and uses `GDAL_HTTP_ENV`)
- Modify: `src/gfv2_params/dprst_depth/compute.py` (`_ENV_OPTS = GDAL_HTTP_ENV`; `_read_tile_window` calls `read_padded`)
- Test: `tests/test_dprst_depth_topo.py`, `tests/test_dprst_depth_compute.py`

**Interfaces:**
- Produces: `topo.GDAL_HTTP_ENV: dict[str, str]`; `topo.read_padded(src, bounds: tuple[float, float, float, float], sentinel: float = -9999.0) -> tuple[np.ndarray, Affine]`. `src` is any open rasterio dataset or `WarpedVRT`. The array is float32, voids are `sentinel`, and the array always matches the returned transform.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_dprst_depth_topo.py`)

```python
import numpy as np
import pytest
import rasterio
from affine import Affine
from rasterio.io import MemoryFile

from gfv2_params.dprst_depth import topo


def _ramp_dataset(memfile, width=40, height=30, nodata=-999999.0):
    """A 1 m EPSG:5070 raster whose value encodes its own (row, col): v = 1000*row + col.
    Any misregistration then shows up as a wrong value at a known geographic point."""
    data = (np.arange(height)[:, None] * 1000 + np.arange(width)[None, :]).astype("float32")
    transform = Affine(1.0, 0, 5000.0, 0, -1.0, 8000.0)  # origin (5000, 8000)
    ds = memfile.open(driver="GTiff", width=width, height=height, count=1, dtype="float32",
                      crs="EPSG:5070", transform=transform, nodata=nodata)
    ds.write(data, 1)
    return ds


@pytest.mark.parametrize("bounds", [
    (4990.0, 7980.0, 5010.0, 7995.0),   # overhangs LEFT  by 10 cells
    (5010.0, 7990.0, 5020.0, 8012.0),   # overhangs TOP   by 12 cells
    (5030.0, 7980.0, 5050.0, 7990.0),   # overhangs RIGHT by 10 cells
    (5010.0, 7960.0, 5020.0, 7975.0),   # overhangs BOTTOM by 5 cells
])
def test_read_padded_keeps_array_aligned_with_transform_on_every_edge(bounds):
    with MemoryFile() as mf, _ramp_dataset(mf) as ds:
        dem, tr = topo.read_padded(ds, bounds)
        # every valid cell's value must match the value at the same geographic point
        rows, cols = np.nonzero(dem != -9999.0)
        assert rows.size > 0
        for r, c in zip(rows[::7], cols[::7]):
            x, y = tr * (c + 0.5, r + 0.5)
            src_r, src_c = ds.index(x, y)
            assert dem[r, c] == 1000 * src_r + src_c
        # the requested window's full size is returned, voids padded with the sentinel
        assert dem.shape == (round((bounds[3] - bounds[1])), round((bounds[2] - bounds[0])))
        assert (dem == -9999.0).any()


def test_read_padded_fully_outside_returns_all_sentinel_not_empty():
    with MemoryFile() as mf, _ramp_dataset(mf) as ds:
        dem, _ = topo.read_padded(ds, (6000.0, 9000.0, 6020.0, 9010.0))
        assert dem.shape == (10, 20)
        assert (dem == -9999.0).all()


def test_read_padded_inside_is_identical_to_plain_windowed_read():
    """Output-identity guarantee: an in-bounds window takes the unchanged code path."""
    from rasterio.windows import from_bounds
    b = (5003.3, 7981.7, 5017.9, 7996.2)  # fractional, fully inside
    with MemoryFile() as mf, _ramp_dataset(mf) as ds:
        dem, tr = topo.read_padded(ds, b)
        w = from_bounds(*b, transform=ds.transform)
        ref = topo._normalize_nodata(ds.read(1, window=w).astype(np.float32), ds.nodata)
        assert np.array_equal(dem, ref)
        assert tr == ds.window_transform(w)


@pytest.mark.parametrize("shape", [(1, 40), (40, 1), (0, 12), (12, 0)])
def test_lake_max_depth_degenerate_window_returns_zero_not_raises(shape):
    dem = np.full(shape, 10.0)
    mask = np.zeros(shape, bool)
    assert topo.lake_max_depth(dem, mask, Affine(1, 0, 0, 0, -1, 0)) == 0.0


def test_gdal_http_env_sets_timeouts():
    assert topo.GDAL_HTTP_ENV["GDAL_HTTP_TIMEOUT"] == "60"
    assert topo.GDAL_HTTP_ENV["GDAL_HTTP_MAX_RETRY"] == "5"
    assert topo.GDAL_HTTP_ENV["GDAL_HTTP_RETRY_DELAY"] == "2"
    assert topo.GDAL_HTTP_ENV["AWS_NO_SIGN_REQUEST"] == "YES"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `srun ... pixi run -e dev --as-is pytest tests/test_dprst_depth_topo.py -q -k "read_padded or degenerate or http_env"`
Expected: FAIL. `AttributeError: module ... has no attribute 'read_padded'` / `'GDAL_HTTP_ENV'`; the degenerate test raises `ValueError: Shape of array too small`.

- [ ] **Step 3: Implement in `topo.py`**

Add near the tile templates:

```python
from rasterio.errors import WindowError
from rasterio.windows import Window

# One GDAL/rasterio env for every anonymous 3DEP read. The HTTP timeout/retry
# settings exist because a stalled socket otherwise blocks forever: on
# 2026-09-18 seven array tasks on node cn132 sat ~6.9 h on one (#223).
GDAL_HTTP_ENV = {
    "AWS_NO_SIGN_REQUEST": "YES",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "GDAL_HTTP_TIMEOUT": "60",
    "GDAL_HTTP_MAX_RETRY": "5",
    "GDAL_HTTP_RETRY_DELAY": "2",
}


def read_padded(src, bounds, sentinel: float = -9999.0):
    """Windowed read of `bounds` whose array ALWAYS matches the returned transform.

    rasterio silently clips a non-boundless read to the dataset, while
    `window_transform(window)` describes the UNCLIPPED window, so a window
    overhanging the left/top edge came back shifted by the overhang (#223),
    and one wholly outside came back empty. `WarpedVRT` forbids
    `boundless=True`, so pad by hand: read the intersection and place it in
    a sentinel-filled array of the requested size.

    A window wholly inside the dataset takes the original float-window path
    unchanged, so in-bounds reads stay bit-identical to the pre-#223 code.
    """
    window = from_bounds(*bounds, transform=src.transform)
    inside = (
        window.col_off >= 0 and window.row_off >= 0
        and window.col_off + window.width <= src.width
        and window.row_off + window.height <= src.height
    )
    if inside:
        dem = src.read(1, window=window).astype(np.float32)
        return _normalize_nodata(dem, src.nodata, sentinel), src.window_transform(window)

    snapped = window.round_offsets().round_lengths()
    out = np.full((int(snapped.height), int(snapped.width)), sentinel, dtype=np.float32)
    transform = src.window_transform(snapped)
    try:
        inter = snapped.intersection(Window(0, 0, src.width, src.height))
    except WindowError:
        return out, transform
    part = _normalize_nodata(src.read(1, window=inter).astype(np.float32), src.nodata, sentinel)
    r0 = int(inter.row_off - snapped.row_off)
    c0 = int(inter.col_off - snapped.col_off)
    out[r0:r0 + part.shape[0], c0:c0 + part.shape[1]] = part
    return out, transform
```

In `lake_max_depth`, immediately after `dem_arr = np.asarray(dem, float)`:

```python
    if min(dem_arr.shape) < 2:
        # np.gradient needs >=2 cells per axis. A degenerate window is a
        # geometry fact, not a code error -- report no slope (#223).
        return 0.0
```

In `read_window`: replace `env_opts = {...}` with `env_opts = GDAL_HTTP_ENV`. Replace the three lines

```python
                    window = from_bounds(minx, miny, maxx, maxy, transform=vrt.transform)
                    dem = vrt.read(1, window=window).astype(np.float32)
                    transform = vrt.window_transform(window)
```

with

```python
                    dem, transform = read_padded(vrt, (minx, miny, maxx, maxy))
```

and delete the later `dem = _normalize_nodata(dem, nodata)` line, because `read_padded` already normalizes.

In `compute.py`: `from .topo import GDAL_HTTP_ENV, read_padded` (add to the existing import), set `_ENV_OPTS = GDAL_HTTP_ENV`, and make `_read_tile_window`'s body

```python
    minx, miny, maxx, maxy = geom.bounds
    return read_padded(
        vrt, (minx - rim_buffer_m, miny - rim_buffer_m, maxx + rim_buffer_m, maxy + rim_buffer_m)
    )
```

- [ ] **Step 4: Run the new tests plus the whole dprst_depth suite**

Run: `srun ... pixi run -e dev --as-is pytest tests/test_dprst_depth_topo.py tests/test_dprst_depth_compute.py tests/test_dprst_depth.py -q`
Expected: all PASS. If `test_read_padded_*` fails on the shape assertion, check the rounding: `round_offsets()` floors and `round_lengths()` rounds, both verified on rasterio 1.5 (`Window(-1.4,2.6,5.2,3.1)` → `Window(-2,2,5,3)`).

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/topo.py src/gfv2_params/dprst_depth/compute.py tests/test_dprst_depth_topo.py
git commit -m "fix(dprst_depth): clip-and-pad windowed reads so the array matches its transform (#223)"
```

### Task 2: PR A docs, then open PR A

**Files:**
- Modify: `docs/dprst_depth_avg_reference.md` (add a short "Windowed reads" note), `CLAUDE.md` (new gotcha bullet), `.amazonq/rules/workflow.md`

- [ ] **Step 1: Add the CLAUDE.md gotcha** (in "Non-obvious conventions & gotchas", after the WhiteboxTools bullet)

```markdown
- **Every dprst_depth DEM window goes through `topo.read_padded`, never a bare
  `vrt.read(window=...)`.** rasterio clips a non-boundless read to the dataset
  but `window_transform(window)` describes the UNCLIPPED window, so a window
  overhanging a tile's left/top edge came back misregistered by the overhang
  (mean 788 m on gfv2r2) with no error, and one wholly outside came back empty
  and crashed `np.gradient` (942 "compute errors", #223). `WarpedVRT` forbids
  `boundless=True`; `read_padded` pads with the -9999 sentinel instead. All
  3DEP reads also use `topo.GDAL_HTTP_ENV`: without an HTTP timeout, one
  stalled socket held seven array tasks for ~6.9 h.
```

- [ ] **Step 2: Add the reference-doc note.** In `docs/dprst_depth_avg_reference.md`'s section on reading the DEM, add a paragraph with the same content as the bullet, written for a scientist. Commit:

```bash
git add CLAUDE.md docs/dprst_depth_avg_reference.md .amazonq/rules/workflow.md
git commit -m "docs(dprst_depth): record the clip-and-pad read rule and HTTP timeouts (#223)"
```

- [ ] **Step 3: Lint, push, open PR A.** Run `pixi run -e dev pre-commit run --files <changed files>` (login node is fine for targeted runs). Push via git-over-SSH. Create the PR via curl + REST (the `gh` CLI's TLS is dropped on this network): build the payload with `python3 json.dumps` and POST with `curl --data-binary @payload.json` to `https://api.github.com/repos/rmcd-mscb/gfv2-params/pulls`. Title: `fix(dprst_depth): clip-and-pad reads + HTTP timeouts (#223 part 1)`. The body says this is part 1 of #223, does not close it, and states the output-identity guarantee for in-bounds windows. End with the attribution line. Wait for CI green before merging.

---

## PR B — real tile inventory, tile-set planning, threaded compute (closes #223)

### Task 3: Tile inventory + WESM project attributes (library + staging CLI + batch)

**Files:**
- Create: `src/gfv2_params/dprst_depth/inventory.py`
- Create: `src/gfv2_params/download/dem_1m_inventory.py`
- Create: `slurm_batch/stage_dem_1m_inventory.batch`
- Test: `tests/test_dprst_depth_inventory.py`

**Interfaces:**
- Produces:
  - `inventory.S3_BASE = "https://prd-tnm.s3.amazonaws.com"`, `inventory.PROJECTS_PREFIX = "StagedProducts/Elevation/1m/Projects/"`
  - `inventory.TILE_NAME_RE` with groups `zone`, `x`, `y`, `project`
  - `inventory.parse_list_response(xml: str) -> tuple[list[str], list[str], str | None]` = (keys, common_prefixes, next_continuation_token)
  - `inventory.list_s3(prefix: str, *, delimiter: str | None = None, fetch=http_get) -> tuple[list[str], list[str]]` (all pages)
  - `inventory.list_projects(fetch=http_get) -> list[str]`
  - `inventory.list_project_tiles(project: str, fetch=http_get) -> list[str]` (full `/vsicurl/https://...` keys, `.tif` matching `TILE_NAME_RE` only)
  - `inventory.read_tile_header(key: str) -> dict` with keys `crs` (str), `width`, `height`, `bounds` (native `(left, bottom, right, top)`)
  - `inventory.tile_record(key: str, header: dict) -> dict` with `INVENTORY_COLUMNS`
  - `inventory.INVENTORY_COLUMNS = ["project", "key", "zone", "crs", "width", "height", "minx", "miny", "maxx", "maxy"]` (min/max in EPSG:5070)
  - `inventory.build_inventory(projects, *, n_threads=32, lister=list_project_tiles, header_reader=read_tile_header, logger, max_fail_frac=0.01) -> pd.DataFrame`
  - `inventory.load_inventory(path) -> gpd.GeoDataFrame` (EPSG:5070 `box` geometry per tile)
  - `inventory.ql_rank(value) -> int` (`"QL 0"`→0 … `"QL 3"`→3; anything else → 9)
  - `inventory.project_attrs(wesm: pd.DataFrame, dirs: list[str]) -> pd.DataFrame` indexed by `project` (= the S3 directory name), with columns `ql_rank` (int, best over matched rows), `collect_end` (datetime64, newest), `matched_by` (`workunit`/`project`/`link`). Directories with no match are absent.
  - `inventory.load_project_attrs(path) -> pd.DataFrame`

- [ ] **Step 1: Write the failing tests** (`tests/test_dprst_depth_inventory.py`)

```python
import logging

import numpy as np
import pandas as pd
import pytest

from gfv2_params.dprst_depth import inventory as inv

PAGE1 = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_15_x50y505_P1.tif</Key></Contents>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/readme.txt</Key></Contents>
<IsTruncated>true</IsTruncated><NextContinuationToken>tok/2+=</NextContinuationToken></ListBucketResult>"""
PAGE2 = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<Contents><Key>StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.tif</Key></Contents>
<IsTruncated>false</IsTruncated></ListBucketResult>"""
PREFIXES = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>
<CommonPrefixes><Prefix>StagedProducts/Elevation/1m/Projects/P1/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>StagedProducts/Elevation/1m/Projects/P2_B22/</Prefix></CommonPrefixes>
<IsTruncated>false</IsTruncated></ListBucketResult>"""


def test_parse_list_response_reads_keys_prefixes_and_token():
    keys, prefixes, token = inv.parse_list_response(PAGE1)
    assert len(keys) == 2 and prefixes == [] and token == "tok/2+="
    assert inv.parse_list_response(PAGE2)[2] is None


def test_list_project_tiles_paginates_and_keeps_only_tile_tifs():
    calls = []

    def fetch(url):
        calls.append(url)
        return PAGE2 if "continuation-token=tok%2F2%2B%3D" in url else PAGE1

    keys = inv.list_project_tiles("P1", fetch=fetch)
    assert keys == [
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_15_x50y505_P1.tif",
        "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.tif",
    ]
    assert len(calls) == 2


def test_list_projects_returns_directory_names():
    assert inv.list_projects(fetch=lambda url: PREFIXES) == ["P1", "P2_B22"]


def test_tile_record_projects_real_bounds_to_5070():
    key = "/vsicurl/https://x/Projects/P1/TIFF/USGS_1M_16_x27y511_P1.tif"
    # the real, CROPPED header measured 2026-09-19 for WI_12County_B22 x27y511
    header = {"crs": "EPSG:26916", "width": 10000, "height": 4289,
              "bounds": (269999.79, 5105711.0, 279999.79, 5110000.0)}
    rec = inv.tile_record(key, header)
    assert rec["project"] == "P1" and rec["zone"] == 16
    assert rec["height"] == 4289
    assert rec["maxx"] > rec["minx"] and rec["maxy"] > rec["miny"]
    # ~10 km x ~4.3 km survives the reprojection (densified bounds, so >=)
    assert 9_900 < rec["maxx"] - rec["minx"] < 10_800
    assert 4_200 < rec["maxy"] - rec["miny"] < 5_000


def test_build_inventory_counts_failures_and_raises_above_threshold():
    keys = [f"/vsicurl/https://x/Projects/P1/TIFF/USGS_1M_15_x{i}y500_P1.tif" for i in range(10)]
    ok = {"crs": "EPSG:26915", "width": 10012, "height": 10012,
          "bounds": (499994.0, 4990006.0, 510006.0, 5000018.0)}

    def reader(key):
        if key.endswith("x3y500_P1.tif"):
            raise OSError("synthetic 503")
        return ok

    with pytest.raises(RuntimeError, match="1 of 10"):
        inv.build_inventory(["P1"], lister=lambda p: keys, header_reader=reader,
                            logger=logging.getLogger("t"), n_threads=4, max_fail_frac=0.05)
    df = inv.build_inventory(["P1"], lister=lambda p: keys, header_reader=reader,
                             logger=logging.getLogger("t"), n_threads=4, max_fail_frac=0.2)
    assert len(df) == 9 and list(df.columns) == inv.INVENTORY_COLUMNS
    assert df["key"].is_unique and df["key"].is_monotonic_increasing  # deterministic order


@pytest.mark.parametrize("value,rank", [("QL 0", 0), ("QL 1", 1), ("QL1", 1), ("QL 2", 2),
                                        ("QL 3", 3), ("Other", 9), (None, 9), (np.nan, 9)])
def test_ql_rank(value, rank):
    assert inv.ql_rank(value) == rank


def test_project_attrs_joins_s3_dirs_by_workunit_then_project_then_link():
    wesm = pd.DataFrame({
        "workunit": ["A_WU1", "A_WU2", "AL_25Co_B1_2017", "X_WU"],
        "project": ["A", "A", "AL_25Co_2017", "X_PROJ"],
        "ql": ["QL 2", "QL 1", "QL 2", "QL 1"],
        "collect_end": pd.to_datetime(["2019-01-01", "2018-06-01", "2017-05-01", "2020-01-01"]),
        "sourcedem_link": ["", "", "", "https://x/StagedProducts/Elevation/OPR/Projects/X_DIR/"],
        "lpc_link": ["", "", "", ""],
    })
    out = inv.project_attrs(wesm, ["A", "AL_25Co_B1_2017", "X_DIR", "NOPE"])
    assert out.loc["A", "ql_rank"] == 1 and out.loc["A", "matched_by"] == "project"
    assert out.loc["A", "collect_end"] == pd.Timestamp("2019-01-01")
    assert out.loc["AL_25Co_B1_2017", "matched_by"] == "workunit"  # the dominant real-world case
    assert out.loc["X_DIR", "matched_by"] == "link" and out.loc["X_DIR", "ql_rank"] == 1
    assert "NOPE" not in out.index  # unmatched -> ranks last in sources.rank_candidates
```

- [ ] **Step 2: Run to verify failure**

Run: `srun ... pytest tests/test_dprst_depth_inventory.py -q`
Expected: FAIL with `ModuleNotFoundError: gfv2_params.dprst_depth.inventory`.

- [ ] **Step 3: Implement `src/gfv2_params/dprst_depth/inventory.py`**

```python
"""Real 3DEP 1 m tile inventory + WESM project attributes (issue #223).

Replaces the convex-hull WESM footprint index as the answer to "which 1 m
tiles exist here, and what do they really cover?". A hull claims ground a
project never flew; on gfv2r2, 61% of polygons that the hulls said were
covered by 2+ projects had exactly one real tile and 15% had none. The
inventory is built from the S3 listing (what exists) and each tile's own
COG header (its real, possibly cropped, extent), then reprojected to
EPSG:5070 so planning is pure geometry.

WESM is still the source of project QUALITY and DATE, read geometry-free
(the geometry is what made the old staging OOM). See `project_attrs` for
how S3 directory names are joined to WESM rows.
"""
from __future__ import annotations

import re
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import transform_bounds
from shapely.geometry import box

from .topo import GDAL_HTTP_ENV

S3_BASE = "https://prd-tnm.s3.amazonaws.com"
PROJECTS_PREFIX = "StagedProducts/Elevation/1m/Projects/"
TILE_NAME_RE = re.compile(r"USGS_1M_(?P<zone>\d{2})_x(?P<x>\d+)y(?P<y>\d+)_(?P<project>.+)\.tif$")
INVENTORY_COLUMNS = ["project", "key", "zone", "crs", "width", "height", "minx", "miny", "maxx", "maxy"]
_KEY_RE = re.compile(r"<Key>([^<]*)</Key>")
_PREFIX_RE = re.compile(r"<Prefix>([^<]*)</Prefix>")
_TOKEN_RE = re.compile(r"<NextContinuationToken>([^<]*)</NextContinuationToken>")


def http_get(url: str, timeout: float = 60.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def parse_list_response(xml: str) -> tuple[list[str], list[str], str | None]:
    prefixes = [p for p in _PREFIX_RE.findall(xml.split("<CommonPrefixes>", 1)[-1])] if "<CommonPrefixes>" in xml else []
    m = _TOKEN_RE.search(xml)
    return _KEY_RE.findall(xml), prefixes, (m.group(1) if m else None)


def list_s3(prefix: str, *, delimiter: str | None = None, fetch=http_get) -> tuple[list[str], list[str]]:
    keys, prefixes, token = [], [], None
    while True:
        q = {"list-type": "2", "prefix": prefix}
        if delimiter:
            q["delimiter"] = delimiter
        if token:
            q["continuation-token"] = token
        k, p, token = parse_list_response(fetch(f"{S3_BASE}/?{urllib.parse.urlencode(q)}"))
        keys += k
        prefixes += p
        if token is None:
            return keys, prefixes


def list_projects(fetch=http_get) -> list[str]:
    _, prefixes = list_s3(PROJECTS_PREFIX, delimiter="/", fetch=fetch)
    return sorted(p[len(PROJECTS_PREFIX):].strip("/") for p in prefixes)


def list_project_tiles(project: str, fetch=http_get) -> list[str]:
    keys, _ = list_s3(f"{PROJECTS_PREFIX}{project}/TIFF/", fetch=fetch)
    return sorted(f"/vsicurl/{S3_BASE}/{k}" for k in keys if TILE_NAME_RE.search(k))


def read_tile_header(key: str) -> dict:
    with rasterio.Env(**GDAL_HTTP_ENV), rasterio.open(key) as src:
        return {"crs": src.crs.to_string(), "width": src.width, "height": src.height,
                "bounds": tuple(src.bounds)}


def tile_record(key: str, header: dict) -> dict:
    m = TILE_NAME_RE.search(key)
    minx, miny, maxx, maxy = transform_bounds(header["crs"], "EPSG:5070", *header["bounds"], densify_pts=21)
    return {"project": m.group("project"), "key": key, "zone": int(m.group("zone")),
            "crs": header["crs"], "width": int(header["width"]), "height": int(header["height"]),
            "minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}


def build_inventory(projects, *, n_threads: int = 32, lister=list_project_tiles,
                    header_reader=read_tile_header, logger, max_fail_frac: float = 0.01) -> pd.DataFrame:
    with ThreadPoolExecutor(n_threads) as ex:
        keys = [k for ks in ex.map(lister, projects) for k in ks]
    logger.info("  listed %d tile(s) across %d project(s)", len(keys), len(projects))

    def _one(key):
        try:
            return tile_record(key, header_reader(key)), None
        except Exception as exc:  # noqa: BLE001 - counted and gated below
            return None, f"{key}: {type(exc).__name__}: {exc}"

    with ThreadPoolExecutor(n_threads) as ex:
        results = list(ex.map(_one, keys))
    failures = [e for _, e in results if e]
    for e in failures[:20]:
        logger.warning("  header read failed: %s", e)
    if keys and len(failures) / len(keys) > max_fail_frac:
        raise RuntimeError(
            f"{len(failures)} of {len(keys)} tile header reads failed "
            f"(> {max_fail_frac:.1%}); refusing to stage a partial inventory"
        )
    df = pd.DataFrame([r for r, _ in results if r], columns=INVENTORY_COLUMNS)
    return df.sort_values("key").reset_index(drop=True)


def load_inventory(path) -> gpd.GeoDataFrame:
    df = pd.read_parquet(path)
    geom = [box(a, b, c, d) for a, b, c, d in df[["minx", "miny", "maxx", "maxy"]].itertuples(index=False)]
    return gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:5070")


def ql_rank(value) -> int:
    if not isinstance(value, str):
        return 9
    m = re.fullmatch(r"\s*QL\s*(\d)\s*", value)
    return int(m.group(1)) if m else 9


def project_attrs(wesm: pd.DataFrame, dirs) -> pd.DataFrame:
    """QL/date per S3 1 m project DIRECTORY. S3 directory names are mostly WESM
    WORKUNIT names, not project names: on 2026-09-19 exact `project` matched only
    366 of 967 dirs, `workunit` 572, and the link fields the rest of 932. No
    `onemeter_category` filter: a directory under 1m/Projects/ is itself proof
    the data qualifies for the 1 m product."""
    w = wesm.copy()
    w["ql_rank"] = w["ql"].map(ql_rank)
    links = w["sourcedem_link"].fillna("") + " " + w["lpc_link"].fillna("")
    rows = []
    for d in dirs:
        for how, hit in (("workunit", w["workunit"] == d), ("project", w["project"] == d),
                         ("link", links.str.contains(f"/Projects/{d}/", regex=False))):
            if hit.any():
                m = w[hit]
                rows.append({"project": d, "ql_rank": int(m["ql_rank"].min()),
                             "collect_end": m["collect_end"].max(), "matched_by": how})
                break
    return pd.DataFrame(rows, columns=["project", "ql_rank", "collect_end", "matched_by"]).set_index("project")


def load_project_attrs(path) -> pd.DataFrame:
    return pd.read_parquet(path)
```

- [ ] **Step 4: Run the tests to verify they pass.** Same command. Expected: PASS.

- [ ] **Step 5: Implement the staging CLI `src/gfv2_params/download/dem_1m_inventory.py`**

```python
"""Stage the real 3DEP 1 m tile inventory + WESM project attributes (issue #223).

Shared, fabric-independent input. Writes:
  {data_root}/input/3dep/dem_1m_tile_inventory.parquet
  {data_root}/input/wesm/wesm_project_attrs.parquet
Both are a SNAPSHOT of what USGS publishes on the day they are staged;
re-stage deliberately (--force). Every fabric's dprst_depth must then be
re-run, the same obligation a shared_rasters rebuild carries.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pyogrio

from gfv2_params.config import load_base_config
from gfv2_params.dprst_depth import inventory as inv
from gfv2_params.log import configure_logging

WESM_VSICURL = "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg"
INVENTORY_NAME = "dem_1m_tile_inventory.parquet"
ATTRS_NAME = "wesm_project_attrs.parquet"
MIN_WESM_MATCH_FRAC = 0.90


def stage(data_root: Path, *, n_threads: int, force: bool, logger) -> tuple[Path, Path]:
    inv_path = data_root / "input" / "3dep" / INVENTORY_NAME
    attrs_path = data_root / "input" / "wesm" / ATTRS_NAME
    if inv_path.exists() and attrs_path.exists() and not force:
        logger.info("already staged (use --force to refresh): %s, %s", inv_path, attrs_path)
        return inv_path, attrs_path

    wesm = pyogrio.read_dataframe(
        WESM_VSICURL, columns=["workunit", "project", "ql", "collect_end", "sourcedem_link", "lpc_link"],
        read_geometry=False,
    )
    projects = inv.list_projects()
    attrs = inv.project_attrs(wesm, projects)
    frac = len(attrs) / len(projects) if projects else 0.0
    logger.info("  %d S3 1m project dir(s); %d matched to WESM (%.1f%%; by %s); unmatched rank last",
                len(projects), len(attrs), 100 * frac, attrs["matched_by"].value_counts().to_dict())
    if frac < MIN_WESM_MATCH_FRAC:
        raise RuntimeError(
            f"only {frac:.1%} of S3 1m project dirs matched a WESM row (floor {MIN_WESM_MATCH_FRAC:.0%}; "
            f"96.4% measured 2026-09-19) -- the WESM schema or S3 naming has changed; investigate"
        )
    df = inv.build_inventory(projects, n_threads=n_threads, logger=logger)

    for path, frame in ((inv_path, df), (attrs_path, attrs)):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"_staging_{path.name}")
        frame.to_parquet(tmp)
        tmp.rename(path)
        logger.info("  wrote %s (%d rows)", path, len(frame))
    return inv_path, attrs_path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--base_config", default="configs/base_config.yml")
    p.add_argument("--threads", type=int, default=32)
    p.add_argument("--force", action="store_true")
    a = p.parse_args()
    data_root = Path(load_base_config(Path(a.base_config))["data_root"])
    stage(data_root, n_threads=a.threads, force=a.force, logger=configure_logging("stage_dem_1m_inventory"))


if __name__ == "__main__":
    main()
```

Check `load_base_config`'s real signature and return shape in `src/gfv2_params/config.py` and match the exact call that `download/wesm.py`'s `main()` uses today. If it differs, copy that call.

- [ ] **Step 6: Create `slurm_batch/stage_dem_1m_inventory.batch`**

```bash
#!/bin/bash
#SBATCH -p cpu
#SBATCH -A impd
#SBATCH --job-name=stage_dem_1m_inventory
#SBATCH --output=logs/job_%j.out
#SBATCH --error=logs/job_%j.err
#SBATCH --time=03:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
# Shared input (issue #223): real 3DEP 1 m tile extents + WESM project QL/dates.
# Re-staging obliges a dprst_depth re-run of EVERY fabric. Pass FORCE=1 to refresh.
set -euo pipefail
pixi run --as-is python -m gfv2_params.download.dem_1m_inventory \
    --base_config "${BASE_CONFIG:-configs/base_config.yml}" --threads 32 ${FORCE:+--force}
```

- [ ] **Step 7: Commit**

```bash
git add src/gfv2_params/dprst_depth/inventory.py src/gfv2_params/download/dem_1m_inventory.py \
        slurm_batch/stage_dem_1m_inventory.batch tests/test_dprst_depth_inventory.py
git commit -m "feat(dprst_depth): stage the real 3DEP 1m tile inventory + WESM project attrs (#223)"
```

- [ ] **Step 8: Stage it for real (unblocks Tasks 9–11; can run while Tasks 4–8 are developed).** From the worktree, via `srun`/`sbatch` with the worktree's `PYTHONPATH`, submit the batch. Then check: row count (expect well above the 93,640 keys the current gfv2r2 plan references), `project` count (the listing reported in the log), no duplicate keys, and every `maxx > minx`. Record the job id in the PR description.

### Task 4: Source assignment (`sources.py`) — tag, rank, assign, shared entry point

**Files:**
- Create: `src/gfv2_params/dprst_depth/sources.py`
- Test: `tests/test_dprst_depth_sources.py`

**Interfaces:**
- Consumes: `inventory.load_inventory`, `inventory.load_project_attrs`, `tiling.guard_oversized_windows`, `tiling._tile13_key`, `topo.TILE13_HTTPS_TEMPLATE`.
- Produces:
  - `@dataclass(frozen=True) class TileSet: project: str; keys: tuple[str, ...]; covers: bool`. `project == "10m"` means the seamless tile.
  - `encode(ts: TileSet) -> str` = `"project|covers|key1|key2..."` with `covers` as `"1"`/`"0"`; `decode(s: str) -> TileSet`
  - `tag_best_topo(dprst: GeoDataFrame, inventory: GeoDataFrame) -> GeoDataFrame` (adds `best_topo`)
  - `rank_candidates(window: shapely Polygon, hits: GeoDataFrame, attrs: DataFrame) -> list[TileSet]` (1 m sets only, ranked)
  - `assign_sources(dprst, inventory, attrs, rim_m=200.0) -> GeoDataFrame` (adds `source_tiles: str` (encoded primary) and `candidates: list[str]` (encoded, ranked, always ending with the 10 m set))
  - `tag_and_assign(dprst, inventory_path, attrs_path, logger) -> GeoDataFrame`: THE shared entry point for builder and planner (tag → `guard_oversized_windows` → assign)

- [ ] **Step 1: Write the failing tests** (`tests/test_dprst_depth_sources.py`)

```python
import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from gfv2_params.dprst_depth import sources as S


def _inv(rows):
    df = pd.DataFrame(rows, columns=["project", "key", "zone", "minx", "miny", "maxx", "maxy"])
    return gpd.GeoDataFrame(df, geometry=[box(*r[3:]) for r in rows], crs="EPSG:5070")


ATTRS = pd.DataFrame(
    {"ql_rank": [2, 1, 1, 1], "collect_end": pd.to_datetime(["2022-01-01", "2015-01-01", "2020-01-01", "2020-01-01"])},
    index=pd.Index(["NEW_QL2", "OLD_QL1", "MID_QL1", "AAA_QL1"], name="project"),
)


def test_encode_decode_roundtrip():
    ts = S.TileSet("P", ("/vsicurl/a.tif", "/vsicurl/b.tif"), True)
    assert S.decode(S.encode(ts)) == ts


def test_rank_prefers_full_cover_then_ql_then_newest_then_name():
    window = box(0, 0, 100, 100)
    hits = _inv([
        ("NEW_QL2", "k_new", 15, -10, -10, 200, 200),   # covers, QL2
        ("OLD_QL1", "k_old", 15, -10, -10, 200, 200),   # covers, QL1, 2015
        ("MID_QL1", "k_mid", 15, -10, -10, 200, 200),   # covers, QL1, 2020
        ("AAA_QL1", "k_aaa", 15, -10, -10, 200, 200),   # covers, QL1, 2020 -> wins on name
        ("PART", "k_part", 15, 50, -10, 200, 200),       # partial cover, no attrs
    ])
    ranked = S.rank_candidates(window, hits, ATTRS)
    assert [t.project for t in ranked] == ["AAA_QL1", "MID_QL1", "OLD_QL1", "NEW_QL2", "PART"]
    assert ranked[-1].covers is False


def test_rank_unions_a_projects_tiles_into_one_set_and_picks_one_zone():
    window = box(0, 0, 100, 100)
    hits = _inv([
        ("P", "k_left", 15, -10, -10, 50, 200),
        ("P", "k_right", 15, 50, -10, 200, 200),
        ("P", "k_other_zone", 16, 90, -10, 200, 200),   # smaller overlap, other zone -> dropped
    ])
    (ts,) = S.rank_candidates(window, hits, ATTRS)
    assert ts.keys == ("k_left", "k_right") and ts.covers is True


def test_tag_best_topo_uses_real_tile_bounds_not_hulls():
    inv = _inv([("P", "k", 15, 0, 0, 1000, 1000)])
    dprst = gpd.GeoDataFrame({"COMID": [1, 2]}, geometry=[box(10, 10, 20, 20), box(5000, 5000, 5010, 5010)],
                             crs="EPSG:5070")
    out = S.tag_best_topo(dprst, inv)
    assert out["best_topo"].tolist() == ["1m", "10m"]


def test_assign_sources_always_ends_with_the_10m_last_resort(monkeypatch):
    monkeypatch.setattr(S, "_tile13_key", lambda geom, crs: "/vsicurl/10m.tif")
    inv = _inv([("P", "k", 15, -1000, -1000, 1000, 1000)])
    dprst = gpd.GeoDataFrame({"COMID": [1, 2], "best_topo": ["1m", "10m"]},
                             geometry=[box(0, 0, 10, 10), box(0, 0, 10, 10)], crs="EPSG:5070")
    out = S.assign_sources(dprst, inv, ATTRS)
    one, ten = out.loc[0, "candidates"], out.loc[1, "candidates"]
    assert S.decode(one[0]).project == "P" and S.decode(one[-1]).project == "10m"
    assert len(ten) == 1 and S.decode(ten[0]).project == "10m"
    assert out.loc[0, "source_tiles"] == one[0]
```

- [ ] **Step 2: Run to verify failure.** Run: `srun ... pytest tests/test_dprst_depth_sources.py -q`. Expected: FAIL, `ModuleNotFoundError`.

- [ ] **Step 3: Implement `src/gfv2_params/dprst_depth/sources.py`**

```python
"""Per-polygon DEM source assignment from the real 3DEP tile inventory (issue #223).

A polygon's candidates are TILE SETS: all real tiles of ONE project, in ONE
UTM zone, that intersect the polygon's rim-buffered window. Ranked by
(covers the whole window, QL, newest collection, project name). The 10 m
seamless tile is always appended as the last resort. The primary set
(`source_tiles`) is what the array reads; the rest are for recovery when the
primary's interior turns out to hold no valid cells.

`tag_and_assign` is the ONE entry point shared by the in-process builder and
the SLURM planner, so the two paths cannot diverge (same doctrine as
`topo.load_fabric_dprst_polygons`).
"""
from __future__ import annotations

from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from shapely.ops import unary_union

from .tiling import _tile13_key, guard_oversized_windows

TEN_M = "10m"
_NO_DATE = pd.Timestamp("1900-01-01")


@dataclass(frozen=True)
class TileSet:
    project: str
    keys: tuple[str, ...]
    covers: bool


def encode(ts: TileSet) -> str:
    return "|".join([ts.project, "1" if ts.covers else "0", *ts.keys])


def decode(s: str) -> TileSet:
    project, covers, *keys = s.split("|")
    return TileSet(project, tuple(keys), covers == "1")


def tag_best_topo(dprst: gpd.GeoDataFrame, inventory: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = dprst.copy()
    pts = out.set_geometry(out.geometry.centroid)
    hit = gpd.sjoin(pts, inventory[["geometry"]], how="left", predicate="within")
    has = hit.groupby(level=0)["index_right"].first().notna()
    out["best_topo"] = has.map({True: "1m", False: TEN_M})
    return out


def rank_candidates(window, hits: gpd.GeoDataFrame, attrs: pd.DataFrame) -> list[TileSet]:
    sets = []
    for project, grp in hits.groupby("project", sort=True):
        by_zone = grp.groupby("zone")
        zone = max(by_zone.groups, key=lambda z: (by_zone.get_group(z).geometry.intersection(window).area.sum(), -z))
        tiles = by_zone.get_group(zone).sort_values("key")
        covers = bool(unary_union(list(tiles.geometry)).covers(window))
        a = attrs.loc[project] if project in attrs.index else None
        ql = int(a["ql_rank"]) if a is not None else 9
        date = a["collect_end"] if a is not None and pd.notna(a["collect_end"]) else _NO_DATE
        sets.append(((not covers, ql, -date.value, project), TileSet(project, tuple(tiles["key"]), covers)))
    return [ts for _, ts in sorted(sets, key=lambda kv: kv[0])]


def assign_sources(dprst, inventory, attrs, rim_m: float = 200.0) -> gpd.GeoDataFrame:
    out = dprst.copy()
    b = out.geometry.bounds
    windows = gpd.GeoDataFrame(
        {"idx": out.index},
        geometry=[box(r.minx - rim_m, r.miny - rim_m, r.maxx + rim_m, r.maxy + rim_m) for r in b.itertuples()],
        crs=out.crs,
    )
    is_1m = (out["best_topo"] == "1m").values
    hits = gpd.sjoin(windows[is_1m], inventory, how="inner", predicate="intersects")
    hit_groups = {idx: grp for idx, grp in hits.groupby("idx")}
    inv_cols = [c for c in inventory.columns if c != "geometry"]

    cands, primary = [], []
    for idx, geom, win in zip(out.index, out.geometry, windows.geometry):
        ten = encode(TileSet(TEN_M, (_tile13_key(geom, out.crs),), True))
        ranked = []
        if idx in hit_groups:
            g = hit_groups[idx]
            tiles = gpd.GeoDataFrame(g[inv_cols], geometry=inventory.geometry.loc[g["index_right"]].values, crs=out.crs)
            ranked = [encode(t) for t in rank_candidates(win, tiles, attrs)]
        lst = ranked + [ten]
        cands.append(lst)
        primary.append(lst[0])
    out["candidates"] = cands
    out["source_tiles"] = primary
    return out


def tag_and_assign(dprst, inventory_path, attrs_path, logger) -> gpd.GeoDataFrame:
    from .inventory import load_inventory, load_project_attrs

    inventory = load_inventory(inventory_path)
    attrs = load_project_attrs(attrs_path)
    out = tag_best_topo(dprst, inventory)
    logger.info("  best_topo: %d/%d polygons inside a real 1m tile", int((out["best_topo"] == "1m").sum()), len(out))
    out = guard_oversized_windows(out, logger=logger)
    out = assign_sources(out, inventory, attrs)
    n_multi = sum(len(decode(s).keys) > 1 for s in out["source_tiles"])
    n_alt = sum(len(c) > 2 for c in out["candidates"])
    logger.info("  sources: %d primary sets span >1 tile; %d polygons have a real alternative project",
                n_multi, n_alt)
    return out
```

`_tile13_key` is imported from `tiling` and monkeypatched on the `sources` module in the test, which is why the test patches `S._tile13_key`. `guard_oversized_windows` stays in `tiling.py`. Watch for a circular import: `tiling.py` will import `sources` in Task 5, so keep the `from .tiling import ...` here at module top and make Task 5's import in `tiling._load_and_tag_for_plan` **function-local**, matching that function's existing local-import style.

- [ ] **Step 4: Run to verify the tests pass.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/sources.py tests/test_dprst_depth_sources.py
git commit -m "feat(dprst_depth): rank real tile sets per polygon — cover, QL, newest, name (#223)"
```

### Task 5: Planner — tile-set batching replaces hull `group_by_tile` + components

**Files:**
- Modify: `src/gfv2_params/dprst_depth/tiling.py` (`_load_and_tag_for_plan`, `_plan`; delete `group_by_tile`, `_1m_tile_keys`, `_tile_components`, `component_tile_batches`; add `tile_set_groups`)
- Test: `tests/test_dprst_depth_tiling.py`

**Interfaces:**
- Consumes: `sources.tag_and_assign`.
- Produces: `tiling.tile_set_groups(dprst: GeoDataFrame) -> dict[str, list]` (encoded primary set → polygon index labels). The manifest becomes `{"n_batches", "n_polygons", "n_tile_sets", "tile_sets": [[encoded, ...], ...]}`. The tagged parquet gains `source_tiles` and `candidates`.

- [ ] **Step 1: Replace the obsolete tests.** In `tests/test_dprst_depth_tiling.py`, delete `test_group_by_tile_and_batching`, `test_group_by_tile_requires_best_topo_column`, `test_group_by_tile_empty_dprst_gdf_returns_empty`, `test_group_by_tile_1m_resolves_from_wesm_project_no_probe`, `test_group_by_tile_1m_falls_back_to_10m_without_wesm_hit`, `test_component_tile_batches_keeps_multi_tile_polygon_co_batched`, `test_component_tile_batches_matches_tile_batches_when_no_sharing` and `test_component_tile_batches_balances_by_cost`. Keep the `tile_batches`, `guard_oversized_windows`, `polygon_window_cost` and `_clear_stale_batches` tests. Add:

```python
from gfv2_params.dprst_depth.sources import TileSet, encode
from gfv2_params.dprst_depth.tiling import tile_batches, tile_set_groups


def test_tile_set_groups_puts_each_polygon_in_exactly_one_group():
    a = encode(TileSet("P", ("k1",), True))
    b = encode(TileSet("P", ("k1", "k2"), True))
    dprst = gpd.GeoDataFrame({"source_tiles": [a, b, a]}, geometry=[box(0, 0, 1, 1)] * 3, crs="EPSG:5070")
    groups = tile_set_groups(dprst)
    assert groups == {a: [0, 2], b: [1]}
    assert sorted(i for v in groups.values() for i in v) == [0, 1, 2]


def test_tile_set_batches_never_split_a_set():
    groups = {f"s{i}": [i] for i in range(10)}
    batches = tile_batches(groups, n_batches=3)
    flat = [k for b in batches for k in b]
    assert sorted(flat) == sorted(groups) and len(flat) == len(set(flat))
```

Update `test_load_and_tag_for_plan_resolves_segment_and_endorheic_from_output_dir`: write a tiny inventory parquet (`inventory.INVENTORY_COLUMNS`, one tile covering the fixture polygons) and an attrs parquet; replace `wesm_index` in the config with `dem_1m_inventory` + `wesm_project_attrs`; assert the returned frame has `source_tiles` and `candidates`. Update `test_load_and_tag_for_plan_required_keys_exclude_nhd_tables` to expect `dem_1m_inventory` and `wesm_project_attrs` in the required list and **not** `wesm_index`.

- [ ] **Step 2: Run to verify failure.** Expected: `ImportError: cannot import name 'tile_set_groups'` and a `KeyError` for the new required keys.

- [ ] **Step 3: Implement.** In `tiling.py`:

```python
def tile_set_groups(dprst: gpd.GeoDataFrame) -> dict[str, list]:
    """Encoded primary tile set -> polygon index labels. Each polygon is in
    exactly one set (its `source_tiles`), so no union-find is needed: the
    chaining that produced >4k-tile components and the batch 0/1 long pole
    on gfv2r2 cannot happen."""
    groups: dict[str, list] = defaultdict(list)
    for idx, s in dprst["source_tiles"].items():
        groups[s].append(idx)
    return dict(groups)
```

In `_load_and_tag_for_plan`: in `required`, replace `"wesm_index"` with `"dem_1m_inventory", "wesm_project_attrs"`. Replace the `wesm_index` existence check with the two new paths. Replace the block from `wesm_gdf = gpd.read_file(wesm_index)` through `dprst = guard_oversized_windows(dprst, logger=logger)` with:

```python
    from .sources import tag_and_assign
    dprst = tag_and_assign(dprst, Path(config["dem_1m_inventory"]), Path(config["wesm_project_attrs"]), logger)
```

Return `dprst` only (the signature becomes `-> gpd.GeoDataFrame`).

In `_plan`: replace `groups = group_by_tile(...)` … `batches = component_tile_batches(...)` and the component logging with:

```python
    dprst = _load_and_tag_for_plan(raw, logger)
    groups = tile_set_groups(dprst)
    costs = polygon_window_cost(dprst)
    batches = tile_batches(groups, args.n_batches, costs=costs)
```

Keep the per-batch count/cost balance logging, but delete the count-vs-cost A/B block (it existed to compare component packings). Write `tagged_cols = ["COMID", "FTYPE", "best_topo", "ecoregion", "oversized_1m", "source_tiles", "candidates", "geometry"]` and the manifest keys `n_tile_sets` / `tile_sets` (keep `n_batches`, which #222's `_verify_batches_match_plan` reads). Delete `group_by_tile`, `_1m_tile_keys`, `_tile_components`, `component_tile_batches`, update `__all__`, and remove the now-unused `_1m_candidate_tiles`/`_utm_zone_epsg`/`transform_bounds` imports. Rewrite the module docstring's "Pure index math only … existence is a Task 4 read-time concern" paragraph: existence and extent now come from the staged inventory.

- [ ] **Step 4: Run** `pytest tests/test_dprst_depth_tiling.py -q`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gfv2_params/dprst_depth/tiling.py tests/test_dprst_depth_tiling.py
git commit -m "refactor(dprst_depth): plan by real tile sets instead of hull tile keys + components (#223)"
```

### Task 6: Compute — open each tile set once, threads, candidate recovery

**Files:**
- Modify: `src/gfv2_params/dprst_depth/compute.py`
- Modify: `scripts/run_dprst_depth_batch.py`, `slurm_batch/run_dprst_depth_batch.batch`
- Modify: `src/gfv2_params/dprst_depth/topo.py` (`resolution_class` docstring only: it stays, because `scripts/diagnose/dprst_depth_probe.py` imports it for the Phase-0 coverage audit, but production tagging is `sources.tag_best_topo`)
- Test: `tests/test_dprst_depth_compute.py`

**Interfaces:**
- Consumes: `sources.decode`, `topo.read_padded`, `topo.GDAL_HTTP_ENV`, `topo._native_resolution`.
- Produces:
  - `compute.open_tile_set(ts: TileSet)`: context manager yielding an EPSG:5070 nearest-resampled `WarpedVRT` over the single key, or over an in-memory `gdal.BuildVRT` mosaic of several keys
  - `compute.run_batch(dprst_gdf, tile_sets: list[str], out_parquet, logger, n_threads: int = 1) -> pd.DataFrame`. `dprst_gdf` needs `COMID`, `source_tiles`, `candidates`. Output columns = `_OUTPUT_COLUMNS + ["source"]`, rows sorted by `COMID`.
  - `_OUTPUT_COLUMNS` gains `"source"` (the winning set's `project`, or `"10m"`).

- [ ] **Step 1: Replace the obsolete tests.** Delete `test_run_batch_skips_failed_tile_without_aborting_batch`, `test_run_batch_counts_compute_error_separately` and `test_run_batch_dedupes_multi_tile_polygon` (they patch the deleted `group_by_tile`/`compute_polygon` seams). Keep both `_polygon_depth_from_dem` tests. Add:

```python
from gfv2_params.dprst_depth.sources import TileSet, encode

P1 = encode(TileSet("P1", ("k1",), True))
P2 = encode(TileSet("P2", ("k2",), True))
TEN = encode(TileSet("10m", ("k10",), True))


def _gdf(rows):
    return gpd.GeoDataFrame(
        {"COMID": [r[0] for r in rows], "source_tiles": [r[1] for r in rows], "candidates": [r[2] for r in rows]},
        geometry=[box(0, 0, 1, 1)] * len(rows), crs="EPSG:5070",
    )


def _fake_open(void_projects=frozenset(), bad_projects=frozenset()):
    @contextmanager
    def _open(ts):
        if ts.project in bad_projects:
            raise RasterioIOError(f"synthetic 404 {ts.project}")
        yield ts.project  # the "vrt" is just the project name
    return _open


def _fake_compute(void_projects):
    def _one(vrt, geom):
        if vrt in void_projects:
            return None  # interior had 0 valid cells
        return {"dprst_depth_m": 1.0, "measured_max_m": 2.0, "hollister_max_m": 3.0, "flat": False,
                "resolution": "10m" if vrt == "10m" else "1m"}
    return _one


def test_run_batch_recovers_void_primary_from_next_candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute({"P1"}))
    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert df.loc[0, "source"] == "P2" and df.loc[0, "method"] == "measured"


def test_run_batch_falls_to_10m_when_every_1m_set_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open(bad_projects={"P1", "P2"}))
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    df = run_batch(_gdf([(7, P1, [P1, P2, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert df.loc[0, "source"] == "10m" and df.loc[0, "resolution"] == "10m"


def test_run_batch_skips_polygon_with_no_usable_source(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute({"P1", "10m"}))
    caplog.set_level(logging.INFO)
    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert len(df) == 0
    assert "n_no_source=1" in caplog.text


def test_run_batch_threads_give_identical_output_to_serial(tmp_path, monkeypatch):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())
    monkeypatch.setattr(compute_mod, "_compute_one", _fake_compute(set()))
    rows = [(i, P1 if i % 2 else P2, [P1 if i % 2 else P2, TEN]) for i in range(40)]
    serial = run_batch(_gdf(rows), [P1, P2], tmp_path / "s.parquet", _L(), n_threads=1)
    threaded = run_batch(_gdf(rows), [P1, P2], tmp_path / "t.parquet", _L(), n_threads=8)
    pd.testing.assert_frame_equal(serial, threaded)
    assert serial["COMID"].is_monotonic_increasing


def test_run_batch_counts_unexpected_error_as_compute_error(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(compute_mod, "open_tile_set", _fake_open())

    def _boom(vrt, geom):
        raise TypeError("synthetic bug")

    monkeypatch.setattr(compute_mod, "_compute_one", _boom)
    caplog.set_level(logging.INFO)
    df = run_batch(_gdf([(7, P1, [P1, TEN])]), [P1], tmp_path / "b.parquet", _L())
    assert len(df) == 0 and "n_compute_error=2" in caplog.text  # primary + 10m both raised
```

- [ ] **Step 2: Run to verify failure.** Expected: `AttributeError: ... has no attribute 'open_tile_set'`.

- [ ] **Step 3: Implement in `compute.py`.** Add the imports `import threading`, `import uuid`, `from concurrent.futures import ThreadPoolExecutor`, `from osgeo import gdal`, `from .sources import decode`. Remove `from .tiling import group_by_tile` and the `read_window` import if nothing else in the module uses it (`compute_polygon` still does, so keep it). Delete `_project_lookup` and `_open_tile_vrt` and replace them with:

```python
@contextmanager
def open_tile_set(ts):
    """ONE open per tile set: a single tile, or an in-memory mosaic of one
    project's same-zone tiles (BuildVRT cannot mix CRSs, which is why a set
    is single-zone -- see sources.rank_candidates). Warped to EPSG:5070 at
    native GSD with nearest resampling, exactly as the per-tile path always
    did, so in-bounds single-tile reads stay bit-identical."""
    vsimem = None
    path = ts.keys[0]
    if len(ts.keys) > 1:
        vsimem = f"/vsimem/dprst_depth_set_{uuid.uuid4().hex}.vrt"
        gdal.BuildVRT(vsimem, list(ts.keys))
        path = vsimem
    try:
        with rasterio.open(path) as src:
            resolution = _native_resolution(src, "EPSG:5070")
            with WarpedVRT(src, crs="EPSG:5070", resampling=Resampling.nearest, resolution=resolution) as vrt:
                yield vrt
    finally:
        if vsimem is not None:
            gdal.Unlink(vsimem)


def _compute_one(vrt, geom) -> dict | None:
    """Depth stats for one polygon against an open set, or None if its interior
    has no valid cells there (so the caller tries the next candidate)."""
    dem, transform = _read_tile_window(vrt, geom)
    interior = _interior_mask(dem, transform, geom)
    if not interior.any():
        return None
    result = _polygon_depth_from_dem(dem, interior, transform)
    result["resolution"] = "1m" if abs(transform.a) < 5.0 else "10m"
    return result
```

Replace `run_batch` entirely:

```python
def run_batch(dprst_gdf, tile_sets, out_parquet, logger, n_threads: int = 1) -> pd.DataFrame:
    """Compute every polygon whose PRIMARY tile set is in `tile_sets`.

    Each set is opened once (`open_tile_set`) and all its member polygons are
    windowed against it; sets run concurrently on `n_threads` threads (the work
    is remote-read bound: 73% of time in `vrt.read` on gfv2r2). A polygon whose
    primary yields no valid interior (or fails to read) walks its remaining
    ranked `candidates`, ending at the 10 m seamless tile. Counters stay split
    (#173 PR#177 FIX 2): read failures (expected, WARNING) vs compute errors
    (bugs, ERROR) vs polygons with no usable source at all.
    """
    for col in ("source_tiles", "candidates", "COMID"):
        if col not in dprst_gdf.columns:
            raise KeyError(f"run_batch needs '{col}' (plan with sources.tag_and_assign first)")
    wanted = set(tile_sets)
    members = {s: list(g.index) for s, g in dprst_gdf.groupby("source_tiles") if s in wanted}
    counts = {"n_read_failure": 0, "n_compute_error": 0, "n_recovered": 0, "n_no_source": 0}
    lock = threading.Lock()

    def _bump(key):
        with lock:
            counts[key] += 1

    def _attempt(ts_str, idxs):
        """Run `idxs` against one set. Returns ({idx: result}, [unresolved idx])."""
        ts = decode(ts_str)
        done, pending = {}, []
        try:
            with rasterio.Env(**_ENV_OPTS), open_tile_set(ts) as vrt:
                for idx in idxs:
                    try:
                        r = _compute_one(vrt, dprst_gdf.geometry.loc[idx])
                    except RasterioIOError as exc:
                        _bump("n_read_failure")
                        logger.warning("  set=%s idx=%s: read failure (%s)", ts.project, idx, exc)
                        r = None
                    except Exception as exc:  # noqa: BLE001 - loud, isolated, never aborts the batch
                        _bump("n_compute_error")
                        logger.error("  set=%s idx=%s: UNEXPECTED compute error (%s: %s)",
                                     ts.project, idx, type(exc).__name__, exc)
                        r = None
                    if r is None:
                        pending.append(idx)
                    else:
                        r["source"] = ts.project
                        done[idx] = r
        except RasterioIOError as exc:
            _bump("n_read_failure")
            logger.warning("  set=%s: open failed (%s) — %d polygon(s) to recovery", ts.project, exc, len(idxs))
            pending = list(idxs)
        return done, pending

    results, to_recover = {}, []
    with ThreadPoolExecutor(max(1, n_threads)) as ex:
        for i, (done, pending) in enumerate(ex.map(lambda kv: _attempt(*kv), sorted(members.items())), 1):
            results.update(done)
            to_recover += pending
            if i % 25 == 0:
                logger.info("  [%d/%d tile sets] %d polygons done", i, len(members), len(results))

    def _recover(idx):
        for ts_str in dprst_gdf.at[idx, "candidates"][1:]:
            done, _ = _attempt(ts_str, [idx])
            if idx in done:
                return idx, done[idx]
        return idx, None

    with ThreadPoolExecutor(max(1, n_threads)) as ex:
        for idx, r in ex.map(_recover, sorted(to_recover)):
            if r is None:
                counts["n_no_source"] += 1
            else:
                counts["n_recovered"] += 1
                results[idx] = r

    rows = []
    for idx, r in results.items():
        r["COMID"] = dprst_gdf.at[idx, "COMID"]
        r["method"] = "flat_pending" if r["flat"] else "measured"
        rows.append(r)
    out = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
    out = out.sort_values("COMID").reset_index(drop=True) if len(out) else _empty_batch_frame()
    out.to_parquet(out_parquet, index=False)
    logger.info("run_batch: %d/%d polygons written (%d tile sets, %s) -> %s",
                len(out), sum(len(v) for v in members.values()), len(members),
                ", ".join(f"{k}={v}" for k, v in counts.items()), out_parquet)
    return out
```

Set `_OUTPUT_COLUMNS = ["COMID", "dprst_depth_m", "measured_max_m", "hollister_max_m", "flat", "resolution", "method", "source"]`. The worker-thread counters must not race: `+=` on a dict int isn't atomic under free-threading, so they go through a `threading.Lock()`, which the code above already does via `_bump`. The recovery-phase counters are updated only on the main thread, as `ex.map` results are consumed.

`_compute_one` calls `_read_tile_window(vrt, geom)`, which is now `read_padded` (Task 1).

In `topo.py`, **keep** `resolution_class` (the Phase-0 probe imports it) and prepend to its docstring: `Phase-0 coverage AUDIT only -- it tests WESM convex hulls, which invent coverage (#223). Production tagging is sources.tag_best_topo against the real tile inventory.`

`scripts/run_dprst_depth_batch.py`: read `tile_sets = manifest["tile_sets"][batch_id]` (rename every `tile_batches`/`tile_keys` use). Drop the WESM read. Add

```python
    parser.add_argument("--threads", type=int,
                        default=int(os.environ.get("SLURM_CPUS_PER_TASK", "1")),
                        help="concurrent tile sets (default: $SLURM_CPUS_PER_TASK)")
```

and call `run_batch(dprst_gdf, tile_sets, out_parquet, logger, n_threads=args.threads)`. Update the module docstring's reference to `group_by_tile`/`component_tile_batches`.

`slurm_batch/run_dprst_depth_batch.batch`: `#SBATCH --cpus-per-task=8` (was 2). Keep `--mem=64G` and `--time=12:00:00`. Add a comment: 8 concurrent windows fit 64G because `guard_oversized_windows` caps a 1 m window at 200M cells (~4.8 GB working set) and nearly all windows are orders of magnitude smaller.

- [ ] **Step 4: Run** `pytest tests/test_dprst_depth_compute.py tests/test_dprst_depth_topo.py -q`. Expected: PASS.

- [ ] **Step 5: Commit** (two atomic commits):

```bash
git add src/gfv2_params/dprst_depth/compute.py tests/test_dprst_depth_compute.py
git commit -m "perf(dprst_depth): open each tile set once, threaded, with candidate recovery (#223)"
git add src/gfv2_params/dprst_depth/topo.py scripts/run_dprst_depth_batch.py slurm_batch/run_dprst_depth_batch.batch
git commit -m "refactor(dprst_depth): array tasks read tile_sets and run 8 threads (#223)"
```

### Task 7: Builder + profile wiring (in-process path, provenance `source` column, config keys)

**Files:**
- Modify: `src/gfv2_params/depstor_builders/dprst_depth.py` (`_tag_polygons`, `_compute_depths` in-process branch, `_DEPTH_COLUMNS`, `_PROVENANCE_DIAGNOSTIC_COLUMNS`)
- Modify: `src/gfv2_params/depstor_builders/context.py` (replace `wesm_index` with `dem_1m_inventory: Path | None = None`, `wesm_project_attrs: Path | None = None`)
- Modify: `scripts/build_depstor_rasters.py:116` (wire the two keys the same way `wesm_index` was wired)
- Modify: `configs/base_config.yml` (all five profiles: `gfv2`, `gfv2_dev`, `oregon`, `tjc`, `gfv2r2`)
- Modify: `configs/depstor/depstor_rasters.yml:22-23` (comment naming the fabric-profile keys)
- Delete: `src/gfv2_params/download/wesm.py`, plus its test if one exists (`git grep -l "download.wesm\|download import wesm" tests/`)
- Test: `tests/test_dprst_depth.py`

**Interfaces:**
- Consumes: `sources.tag_and_assign`, `tiling.tile_set_groups`, `compute.run_batch(..., n_threads=1)`.

- [ ] **Step 1: Update the tests first.** In `tests/test_dprst_depth.py`: replace `_write_wesm_gpkg(...)` with a helper `_write_inventory(tmp_path)` that writes `dem_1m_tile_inventory.parquet` (one tile covering the fixtures, `inventory.INVENTORY_COLUMNS`) and `wesm_project_attrs.parquet` (one row), returning both paths. In `test_builder_and_plan_paths_resolve_the_same_onstream_set` and `test_dprst_depth_build_end_to_end`, pass `dem_1m_inventory=`/`wesm_project_attrs=` instead of `wesm_index=`. The end-to-end test must monkeypatch `compute_mod.open_tile_set` and `compute_mod._compute_one` (no S3 in CI), as the Task 6 tests do. In `test_inprocess_fallback_refuses_more_polygons_than_the_ceiling`/`..._runs_at_or_under_the_ceiling`, replace the patched `group_by_tile` with `tile_set_groups`. Add:

```python
def test_provenance_carries_the_winning_source(tmp_path, monkeypatch):
    """The re-run validation (Task 11) needs to know WHICH project each depth came from."""
    from gfv2_params.depstor_builders import dprst_depth as b
    assert "source" in b._DEPTH_COLUMNS
    assert "source" in b._PROVENANCE_DIAGNOSTIC_COLUMNS


def test_tag_polygons_requires_the_inventory_keys(tmp_path):
    from gfv2_params.depstor_builders import dprst_depth as b
    ctx = BuildContext(
        fabric="t", template_path=Path("unused"), output_dir=tmp_path,
        hru_gpkg=tmp_path / "hru.gpkg", hru_layer="nhru",
        dem_1m_inventory=None, wesm_project_attrs=tmp_path / "attrs.parquet",
    )
    dprst = _make_dprst_gdf([1, 2])  # existing helper in this file
    with pytest.raises(KeyError, match="dem_1m_inventory"):
        b._tag_polygons(dprst, ctx, _L())
```

`_make_dprst_gdf` and `_L` already exist in this file (lines ~648 and ~66 on #222's branch). Replace `_write_wesm_gpkg` (line ~190) with `_write_inventory` rather than adding it alongside.

- [ ] **Step 2: Run to verify failure.** `pytest tests/test_dprst_depth.py -q`. Expected: `TypeError: unexpected keyword 'dem_1m_inventory'` and the new tests fail.

- [ ] **Step 3: Implement.**
  - `_tag_polygons`: replace the `wesm_index` checks and the `resolution_class`/`guard_oversized_windows` lines with

    ```python
    for key in ("dem_1m_inventory", "wesm_project_attrs"):
        path = getattr(ctx, key)
        if path is None:
            raise KeyError(f"dprst_depth step needs `{key}` in the fabric profile. Stage it: "
                           "`sbatch slurm_batch/stage_dem_1m_inventory.batch`.")
        if not path.exists():
            raise FileNotFoundError(f"{key} not found: {path}. Stage it: "
                                    "`sbatch slurm_batch/stage_dem_1m_inventory.batch`.")
    dprst = tag_and_assign(dprst, ctx.dem_1m_inventory, ctx.wesm_project_attrs, logger)
    ```

    Return `dprst` alone and update the caller and signature (`-> gpd.GeoDataFrame`). Remove every `wesm_gdf` pass-through from `_compute_depths`/`build`.
  - In-process branch of `_compute_depths`: `groups = tile_set_groups(dprst)`; `depth_df = run_batch(dprst, list(groups), tmp_parquet, logger, n_threads=1)`.
  - `_DEPTH_COLUMNS` and `_PROVENANCE_DIAGNOSTIC_COLUMNS` gain `"source"`.
  - Imports: `from ..dprst_depth.sources import tag_and_assign`, `from ..dprst_depth.tiling import tile_set_groups`. Drop `group_by_tile`, `guard_oversized_windows` and `resolution_class`.
  - `base_config.yml`: in each of the five profiles, replace
    `wesm_index: "{data_root}/input/wesm/wesm_1m_footprints.gpkg"` with

    ```yaml
    dem_1m_inventory: "{data_root}/input/3dep/dem_1m_tile_inventory.parquet"
    wesm_project_attrs: "{data_root}/input/wesm/wesm_project_attrs.parquet"
    ```
  - Delete `src/gfv2_params/download/wesm.py`. Leave `dprst_depth/wesm_io.py` in place: `scripts/diagnose/dprst_depth_probe.py` still imports it.

- [ ] **Step 4: Run** `pytest tests/test_dprst_depth*.py tests/test_config.py tests/test_params_index.py -q`. Expected: PASS. Then `git grep -n "wesm_index\|resolution_class\|group_by_tile\|component_tile_batches\|_project_lookup\|download.wesm"` must return only docs lines that Task 8 rewrites (no `src/`, `scripts/`, `configs/` or `tests/` hits).

- [ ] **Step 5: Commit** (atomic):

```bash
git add src/gfv2_params/depstor_builders/ scripts/build_depstor_rasters.py tests/test_dprst_depth.py
git commit -m "feat(depstor): dprst_depth builder tags + assigns from the real tile inventory (#223)"
git add configs/base_config.yml configs/depstor/depstor_rasters.yml
git commit -m "config: replace wesm_index with dem_1m_inventory + wesm_project_attrs in every profile (#223)"
git rm src/gfv2_params/download/wesm.py && git commit -m "chore: retire the convex-hull WESM footprint staging (#223)"
```

### Task 8: Validation script, docs, and PR B

**Files:**
- Create: `scripts/diagnose/compare_dprst_depth_runs.py`, `tests/test_compare_dprst_depth_runs.py`
- Modify: `docs/ARCHITECTURE.md` (required-field table: `wesm_index` → the two new keys; data-root layout gains `input/3dep/`), `docs/dprst_depth_avg_reference.md`, `README.md`, `slurm_batch/RUNME.md` (staging step for the inventory, in the input-staging section, **outside** the generated `fabric_rerun` markers), `slurm_batch/HPC_REFERENCE.md` (new batch file; 8 CPUs; recovery), `CLAUDE.md` (inventory gotcha), `.amazonq/rules/workflow.md`

- [ ] **Step 1: Failing test for the comparison script** (`tests/test_compare_dprst_depth_runs.py`)

```python
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

# Same loading idiom as tests/test_dprst_depth_probe.py: scripts/ is not a package.
_spec = importlib.util.spec_from_file_location(
    "compare_dprst_depth_runs",
    Path(__file__).resolve().parent.parent / "scripts" / "diagnose" / "compare_dprst_depth_runs.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
compare = _mod.compare


def test_compare_classifies_rows():
    old = pd.DataFrame({"COMID": [1, 2, 3, 4], "dprst_depth_m": [1.0, 2.0, np.nan, 4.0],
                        "method": ["measured", "measured", "calibrated_hollister", "measured"]})
    new = pd.DataFrame({"COMID": [1, 2, 3, 4], "dprst_depth_m": [1.0, 2.5, 3.0, 4.0],
                        "method": ["measured", "measured", "measured", "measured"],
                        "source": ["A", "B", "A", "A"]})
    r = compare(old, new, expect_identical={1, 2})
    assert r["identical"] == 2          # COMIDs 1 and 4
    assert r["changed"] == 1            # COMID 2: 2.0 -> 2.5
    assert r["newly_measured"] == 1     # COMID 3: filled -> measured
    assert r["violations"] == [2]       # COMID 2 was expected identical but changed
```

- [ ] **Step 2: Implement `scripts/diagnose/compare_dprst_depth_runs.py`**

```python
"""Compare a baseline and a re-run `dprst_depth_polygons.parquet` (issue #223 re-run gate).

usage: compare_dprst_depth_runs.py BASELINE.parquet NEW.parquet [--expect-identical COMIDS.txt]

`--expect-identical` lists COMIDs whose source did not change and whose window
sat wholly inside its tile in the baseline: their depth must match bit for bit
(the output-identity guarantee). Any mismatch there is a violation and exits 1.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd


def compare(old: pd.DataFrame, new: pd.DataFrame, expect_identical: set[int]) -> dict:
    m = old.merge(new, on="COMID", suffixes=("_old", "_new"), how="outer")
    a, b = m["dprst_depth_m_old"], m["dprst_depth_m_new"]
    same = (a == b) | (a.isna() & b.isna())
    newly = a.isna() & b.notna()
    changed = ~same & ~newly
    violations = sorted(int(c) for c in m.loc[~same & m["COMID"].isin(expect_identical), "COMID"])
    return {"n": len(m), "identical": int(same.sum()), "changed": int(changed.sum()),
            "newly_measured": int(newly.sum()), "violations": violations,
            "abs_change_p50_m": float(np.nanmedian((b - a)[changed].abs())) if changed.any() else 0.0}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("baseline")
    p.add_argument("new")
    p.add_argument("--expect-identical")
    a = p.parse_args()
    ids = set(int(x) for x in open(a.expect_identical).read().split()) if a.expect_identical else set()
    r = compare(pd.read_parquet(a.baseline), pd.read_parquet(a.new), ids)
    for k, v in r.items():
        print(f"{k}: {v if k != 'violations' else len(v)}")
    if r["violations"]:
        print("first violations:", r["violations"][:20])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Run the test; it passes. Commit:

```bash
git add scripts/diagnose/compare_dprst_depth_runs.py tests/test_compare_dprst_depth_runs.py
git commit -m "feat(diagnose): baseline-vs-rerun dprst_depth comparison with an identity gate (#223)"
```

- [ ] **Step 3: Docs.** Add a CLAUDE.md bullet next to Task 2's:

```markdown
- **dprst_depth sources come from the staged real 3DEP tile inventory, never
  WESM footprints.** WESM workunit footprints were convex hulls (to fit login-
  node memory) and hulls invent coverage: on gfv2r2, 67.8% of "multi-tile"
  polygons were one tile cell under 2+ project hulls, and a HEAD probe found
  61% of those had ONE real tile and 15% NONE. `input/3dep/dem_1m_tile_
  inventory.parquet` holds every published tile with its header extent;
  `sources.tag_and_assign` (shared by builder and planner) ranks per-polygon
  tile sets by (covers window, QL, newest collect_end, project). WESM is read
  geometry-free, for QL/dates only. The inventory is a SNAPSHOT: re-staging
  obliges a dprst_depth re-run of every fabric. A tile set is one project in
  ONE UTM zone, because BuildVRT cannot mosaic mixed CRSs.
```

Update `docs/ARCHITECTURE.md`, `docs/dprst_depth_avg_reference.md`, `README.md`, `slurm_batch/RUNME.md` and `slurm_batch/HPC_REFERENCE.md` to match. `git grep -n "wesm_index\|wesm_1m_footprints\|group_by_tile\|component\b.*tile\|download.wesm"` across `docs/ README.md slurm_batch/` must come back empty, or only in dated historical specs/plans (leave those alone). Run `python scripts/build_workflow_doc.py` and confirm it reports no drift. The manifest's dprst_depth `consumes:` says `WESM`; change it to `3DEP 1m tile inventory` in `configs/workflow/fabric_rerun.yml`, then regenerate. Commit docs separately.

- [ ] **Step 4: Full-suite + lint gate:** `srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G pixi run -e dev --as-is pytest tests/ -q`, then `srun ... --mem=64G pixi run -e dev --as-is pre-commit run --all-files`. Both must pass.

- [ ] **Step 5: Open PR B** via curl + REST, as in Task 2. Title: `feat(dprst_depth): plan from the real 3DEP tile inventory; threaded tile-set compute (closes #223)`. The body leads with a **scope** section (inventory staging, planner rewrite, compute rewrite, profile keys, retired WESM staging), the inventory staging job id from Task 3 Step 8, the output-change statement, and the re-run plan (Tasks 9–11) in brief. Don't merge before CI is green **and** Task 10's tjc smoke run passes, because green tests don't prove a correct product.

---

## Re-run — gfv2r2 (after PR B's tjc smoke run; before or after merge per CI)

The only products that change are `{fabric}/depstor_rasters/dprst_depth.tif`, `dprst_depth_polygons.parquet`, `dprst_depth_batches/`, and `params/merged/nhm_dprst_depth_avg_params.csv` (+ `_unfilled/`). `op_flow_thres` is a constant 1.0, and no other depstor step or param reads `dprst_depth.tif`. **So a re-run is `submit_dprst_depth.sh` then `fill`, and nothing else.** Don't use `submit_fabric_rerun.sh --from dprst_depth`: it would also re-run `depstor_rasters_post`, `zonal_params`, `depstor_params` and `snarea` (snarea isn't reproducible run-to-run, so it would add unrelated diffs).

### Task 9: Preconditions and baseline backup

- [ ] **Step 1:** Confirm the pre-fix gfv2r2 chain finished: `sacct -j 4464492,4439737,4439739,4439740,4439812,4439813 -X --format=JobID,JobName,State,Elapsed`. All must be COMPLETED. If any failed, stop and report. That chain is the baseline.
- [ ] **Step 2:** Confirm the inventory is staged (Task 3 Step 8): both parquet files exist under `$(pixi run data-root)/input/`.
- [ ] **Step 3: Back up the baseline** (a small copy; it's what Task 11 diffs against):

```bash
DR=$(pixi run data-root); B=$DR/gfv2r2/_baseline_pre223; mkdir -p $B
cp -a $DR/gfv2r2/depstor_rasters/dprst_depth.tif $DR/gfv2r2/depstor_rasters/dprst_depth_polygons.parquet $B/
cp -a $DR/gfv2r2/depstor_rasters/dprst_depth_batches/_plan $B/old_plan
cp -a $DR/gfv2r2/params/merged/nhm_dprst_depth_avg_params.csv $B/
cp -a $DR/gfv2r2/params/merged/_unfilled/nhm_dprst_depth_avg_params.csv $B/unfilled_nhm_dprst_depth_avg_params.csv
cp -a /caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params/logs/diag_gradient/groups.pkl $B/old_groups.pkl
```

- [ ] **Step 4: Build the expect-identical COMID list** from the OLD plan. A COMID qualifies if (a) its old assignment was a single tile key, (b) that key is a primary single-tile set in the NEW plan for the same COMID, and (c) its old buffered window lay wholly inside that tile's real bounds (from the new inventory). Write it as a short one-off `srun` script into `$B/expect_identical.txt`, one COMID per line. It isn't committed: it depends on one-time artifacts (`old_groups.pkl`).

### Task 10: tjc smoke run (tiled path, small), then the gfv2r2 run

- [ ] **Step 1: tjc tiled smoke** (6,113 polygons; exercises plan → array → build → mean → fill end to end). First back up tjc the same way (`$DR/tjc/_baseline_pre223`). Then, from a shell with `~/.pixi/bin` on `PATH`:

```bash
./slurm_batch/submit_dprst_depth.sh $DR/tjc/batches tjc configs/base_config.yml 8
# note TERMINAL_JOB_ID=<T>
sbatch --dependency=afterok:<T> --export=ALL,BASE_CONFIG=configs/base_config.yml,FABRIC=tjc slurm_batch/merge_and_fill_params.batch
```

Pass criteria: every array log's `run_batch:` line shows `n_compute_error=0`; the build logs measured fraction ≥ the baseline's; `compare_dprst_depth_runs.py` shows 0 violations; `n_no_source` is small and each one is explainable (no 1 m and no 10 m data).

- [ ] **Step 2: gfv2r2 run:**

```bash
./slurm_batch/submit_dprst_depth.sh $DR/gfv2r2/batches gfv2r2 configs/base_config.yml
# TERMINAL_JOB_ID=<T>
sbatch --dependency=afterok:<T> --export=ALL,BASE_CONFIG=configs/base_config.yml,FABRIC=gfv2r2 slurm_batch/merge_and_fill_params.batch
```

Expected wall-clock: plan ≲ 20 min; array ≈ 1–2 h (an estimate; the measured per-batch maximum is the number to report); build ≤ 2 h; mean_zonal (64 tasks, max 4 concurrent) and finalize; fill in minutes. **If any array task exceeds 4 h, stop and profile it with py-spy** (`srun --overlap --jobid=<raw id> pixi exec -s py-spy py-spy dump --pid <pid>`) before waiting it out: that would mean the estimate is wrong.

- [ ] **Step 3:** Update the gfv2r2 memory note with the new job ids.

### Task 11: Validate and report

- [ ] **Step 1: Gates.** Run each and paste the output into the PR (or a follow-up comment on #223):
  - `grep -h "run_batch:" logs/job_<array>_*.err`: `n_compute_error=0` on every task; also sum `n_recovered` and `n_no_source`.
  - Per-task elapsed: `sacct -j <array> -X --format=JobID,Elapsed | sort -k2 | tail`. Report the max and the median.
  - `pixi run --as-is python scripts/diagnose/compare_dprst_depth_runs.py $B/dprst_depth_polygons.parquet $DR/gfv2r2/depstor_rasters/dprst_depth_polygons.parquet --expect-identical $B/expect_identical.txt` must exit 0.
  - Measured fraction (build log `N/M polygons have a computed depth`) ≥ baseline; the 624 formerly-errored COMIDs should now be measured or have `source` = an alternative project / `10m`.
  - Guard 3 on disk: `srun ... pytest tests/test_merged_products_ondisk.py -q` for gfv2r2. Record its SLURM job id.
- [ ] **Step 2: Report** to Rich with a compact before/after table: wall-clock, measured fraction, compute errors, changed-depth count and median |Δ|, and the violation count (must be 0).
- [ ] **Step 3: Follow-on (not in this plan):** canonical gfv2 gets the same two commands, but on **`gfv2_dev` first**. Never validate on canonical gfv2.
