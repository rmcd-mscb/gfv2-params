"""Tile-grouped work-list for the dprst_depth_avg builder (issue #173).

The CONUS run reads a windowed DEM per dprst polygon (~286k polygons). To
hit the ~5 hr wall-clock budget, the SLURM array's fan-out unit must be the
elevation TILE, not the polygon: read each ~10 km tile ONCE and process
every polygon whose window falls in it, rather than opening a raster once
per polygon. This module builds that tile -> polygons work-list (Task 3);
the SLURM array batching (Task 9) and the per-tile compute (Task 4) consume
its output.

Tile *existence* and *extent* now come from the staged 3DEP inventory
(`inventory.load_inventory`, issue #223), not from geometry-only WESM hull
enumeration — a hull claims ground a project never flew (see
`sources.py`'s module docstring for the measured 51.6%/67.8% fallout this
caused). `sources.tag_and_assign` does the real tile-set assignment per
polygon (candidates ranked by coverage/QL/date, primary = `source_tiles`);
this module only groups the already-assigned `source_tiles` into batches
(`tile_set_groups`) and bin-packs those groups for the SLURM array
(`tile_batches`) — no raster reads or `/vsicurl` probes here either way.
`_tile13_key` (the 10 m seamless-tile path builder) is kept because
`sources.py` imports it to build the last-resort `TileSet`.

See docs/superpowers/specs/2026-07-10-dprst-depth-phase0-spike-design.md for
the compute-budget rationale this design responds to.

Run as ``python -m gfv2_params.dprst_depth.tiling --plan ...`` (Task 9) to
build + persist the CONUS SLURM array work-list -- see `_plan`'s docstring
below and slurm_batch/submit_dprst_depth.sh for the full array + finalize
DAG this feeds.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rasterio.warp import transform_geom

from .topo import TILE13_HTTPS_TEMPLATE, _tile13_name

__all__ = [
    "tile_set_groups",
    "tile_batches",
    "guard_oversized_windows",
    "polygon_window_cost",
    "MAX_1M_WINDOW_CELLS",
    "BASE_POLYGON_OVERHEAD_CELLS",
]


def _tile13_key(geom, src_crs) -> str:
    """10 m tile key (full `/vsicurl/` read path) for `geom`'s centroid.

    Mirrors `topo.read_window`'s 10 m branch exactly: reproject the centroid
    to EPSG:4326 and name the 1x1 deg tile via `_tile13_name`. The seamless
    1/3 arc-second product has no footprint gaps, so a single centroid-based
    tile key is always correct — no candidate enumeration or probe needed.
    """
    centroid = {"type": "Point", "coordinates": (geom.centroid.x, geom.centroid.y)}
    lon, lat = transform_geom(src_crs, "EPSG:4326", centroid)["coordinates"]
    return TILE13_HTTPS_TEMPLATE.format(tile=_tile13_name(lon, lat))


def tile_set_groups(dprst: gpd.GeoDataFrame) -> dict[str, list]:
    """Encoded primary tile set -> polygon index labels, from `dprst["source_tiles"]`.

    `dprst` must already carry `source_tiles` (`sources.tag_and_assign`'s
    output) -- one encoded `TileSet` per polygon, chosen by rank among real
    candidates. Each polygon is in EXACTLY ONE set, so no union-find is
    needed: the transitive tile-key chaining `_tile_components` used to do
    (now deleted) produced components spanning >4,000 tiles on the CONUS
    plan, and because a component can never be split across SLURM array
    tasks, two array tasks inherited ~16,000 and ~12,000 fallback polygons
    each and ran over 24 h against a median task time of 34 minutes (issue
    #223 part 2, measured on the gfv2r2 CONUS run). That chaining came from
    hull-overlap false positives (67.8% of the slow-fallback polygons); real
    tile sets don't produce it, because ranking always resolves to ONE
    primary set per polygon, never a shared multi-tile membership across
    unrelated polygons.
    """
    groups: dict[str, list] = defaultdict(list)
    for idx, s in dprst["source_tiles"].items():
        groups[s].append(idx)
    return dict(groups)


def tile_batches(
    groups: dict[str, list[int]],
    n_batches: int,
    costs: dict[int, float] | None = None,
) -> list[list[str]]:
    """Greedy bin-pack tile keys into `n_batches` roughly-equal-work SLURM batches.

    A tile key's "load" is the SUM of its member polygons' weight. By
    default (`costs=None`) that weight is 1 per polygon, i.e. plain polygon
    COUNT — the original behavior. Pass `costs` (`{polygon_idx: cost}`,
    typically `polygon_window_cost`'s output) to weight by estimated DEM
    window read volume instead: count-based balancing lets a handful of
    giant-lake polygons (huge windows) dominate one batch's wall-clock and
    memory footprint even though every batch carries a similar polygon
    COUNT (issue #173 CONUS run: batches 0-2 lagged badly and OOM'd at 24G).
    Either way, tile keys are visited in descending load order and each is
    assigned to whichever batch currently carries the least summed load
    (greedy longest-processing-time-first bin-packing) — keeps the
    `n_batches` SLURM array tasks finishing around the same time. Every tile
    key lands in exactly one batch. Always returns exactly `n_batches` lists
    (some may be empty if there are fewer tile keys than batches), matching
    a fixed-size SLURM array where an empty batch is a no-op task.
    """
    if n_batches <= 0:
        raise ValueError(f"n_batches must be positive, got {n_batches}")

    def _load(members: list[int]) -> float:
        if costs is None:
            return len(members)
        return sum(costs.get(idx, 1.0) for idx in members)

    batches: list[list[str]] = [[] for _ in range(n_batches)]
    loads = [0.0] * n_batches
    for tile_key, members in sorted(groups.items(), key=lambda kv: _load(kv[1]), reverse=True):
        i = min(range(n_batches), key=lambda b: loads[b])
        batches[i].append(tile_key)
        loads[i] += _load(members)
    return batches


# --- Giant-window guard + cost model (issue #173 CONUS load-balance/OOM fix) -

# A polygon's rim-buffered bbox read at 1 m GSD: cell count == bbox area in
# m^2 (1 m x 1 m cells), i.e. width_m * height_m. `topo.read_window`
# materializes several same-shape buffers per window -- the float32 DEM
# (`vrt.read`), `_normalize_nodata`'s float32 copy, and `depth_to_spill`'s
# float64 richdem working copy + its float64 `filled` result (via
# `rd.FillDepressions`) -- roughly 4 + 4 + 8 + 8 = 24 bytes/cell measured
# generously; budgeting 20 bytes/cell as a slightly-conservative working
# estimate against a 4 GiB per-window target gives:
#
#     4 GiB / 20 bytes/cell = 4 * 2**30 / 20 ~= 2.15e8 cells
#
# rounded down to a clean 200_000_000 (~200M) cells, i.e. a square window of
# sqrt(200e6) ~= 14,142 m (~14 km) per side at 1 m GSD. That is generous for
# any dprst polygon that legitimately warrants 1 m detail (a project's
# actual water-surface footprint is typically far smaller than its bbox),
# while reliably catching a truly giant lake, whose window at that point is
# well past what a single SLURM array task's 24 GB budget can hold (richdem
# alone needs a float64 copy at 8 bytes/cell -- 200M cells is already 1.6
# GB just for that one buffer). A giant lake's MEAN depth (`dprst_depth_avg`
# is a volume-weighted mean, not a per-cell product) does not need 1 m
# resolution to compute correctly -- only its spatial DETAIL would benefit,
# which this builder discards anyway (see `topo.volume_mean_depth`) -- so
# downgrading to 10 m is a safe, cheap escape hatch: the SAME bbox+rim
# window at 10 m GSD is `cells / 100`, i.e. 100x smaller, comfortably inside
# budget.
MAX_1M_WINDOW_CELLS = 200_000_000

# Fixed per-polygon overhead folded into `polygon_window_cost`'s estimate so
# a swarm of tiny polygons isn't modeled as free next to one huge one --
# every polygon pays a raster-open + mask-rasterize + richdem-setup cost
# roughly independent of its window size. The value is a deliberately
# nonzero, order-of-magnitude floor (not calibrated against a wall-clock
# profile); see `--plan`'s logged per-batch cost balance for the empirical
# effect on the real CONUS polygon set.
BASE_POLYGON_OVERHEAD_CELLS = 50_000


def _window_bounds(dprst_gdf: gpd.GeoDataFrame, rim_m: float) -> tuple[pd.Series, pd.Series]:
    """Rim-buffered window (width_m, height_m) per polygon, vectorized over `dprst_gdf`."""
    bounds = dprst_gdf.geometry.bounds
    width = (bounds["maxx"] - bounds["minx"]) + 2.0 * rim_m
    height = (bounds["maxy"] - bounds["miny"]) + 2.0 * rim_m
    return width, height


def guard_oversized_windows(
    dprst_gdf: gpd.GeoDataFrame,
    max_1m_cells: int = MAX_1M_WINDOW_CELLS,
    rim_m: float = 200.0,
    logger=None,
) -> gpd.GeoDataFrame:
    """Retag a `best_topo=="1m"` polygon to `"10m"` if its 1 m window would be enormous.

    `dprst_gdf` must already carry `best_topo` (`topo.resolution_class`
    output). Estimates each polygon's 1 m-GSD rim-buffered window cell count
    as `(bbox_width_m + 2*rim_m) * (bbox_height_m + 2*rim_m)` (1 m^2 per
    cell, matching `topo.read_window`'s window == geometry bbox padded by
    `rim_m`) and retags any `"1m"` polygon exceeding `max_1m_cells` to
    `"10m"` -- see the `MAX_1M_WINDOW_CELLS` module constant for the memory-
    budget arithmetic behind the default threshold. `"10m"`-tagged polygons
    are left alone (their window is already 100x smaller at the same bbox).

    Pure geometry -- no raster I/O. Must run AFTER `best_topo` is tagged
    (`resolution_class`, or `sources.tag_best_topo`) and BEFORE tile
    grouping/batching or real tile-set assignment, so every downstream
    consumer (`tile_set_groups`, the cost model below, `sources.
    assign_sources`, and the actual `topo.read_window`/`open_tile_set` call
    at compute time) agrees on the FINAL resolution. Called from both the
    `--plan` SLURM-array hook (via `sources.tag_and_assign`, issue #223) and
    the in-process builder's tag step (`depstor_builders/dprst_depth.py::
    _tag_polygons`) so the two paths can't diverge on which polygons get
    downgraded.

    Returns a copy of `dprst_gdf` with `best_topo` adjusted and a new
    `oversized_1m` bool column (True for every polygon this guard retagged,
    for provenance/diagnostics -- always False for polygons that were never
    `"1m"` or whose window was within budget).
    """
    if "best_topo" not in dprst_gdf.columns:
        raise KeyError(
            "guard_oversized_windows requires dprst_gdf to already be tagged "
            "by resolution_class() (missing 'best_topo')"
        )

    out = dprst_gdf.copy()
    width, height = _window_bounds(out, rim_m)
    est_1m_cells = width * height

    is_1m = out["best_topo"] == "1m"
    oversized = is_1m & (est_1m_cells > max_1m_cells)
    out["oversized_1m"] = oversized

    n_retag = int(oversized.sum())
    if n_retag:
        area_retagged_km2 = float(out.loc[oversized, "geometry"].area.sum()) / 1.0e6
        out.loc[oversized, "best_topo"] = "10m"
        if logger is not None:
            logger.info(
                "  guard_oversized_windows: retagged %d/%d 1m polygon(s) -> 10m "
                "(estimated 1m window > %d cells at rim=%.0fm); %.1f km^2 total area retagged",
                n_retag, int(is_1m.sum()), max_1m_cells, rim_m, area_retagged_km2,
            )
    elif logger is not None:
        logger.info(
            "  guard_oversized_windows: 0 polygon(s) retagged (all 1m windows within the "
            "%d-cell budget)", max_1m_cells,
        )

    return out


def polygon_window_cost(
    dprst_gdf: gpd.GeoDataFrame,
    rim_m: float = 200.0,
    base_overhead_cells: float = BASE_POLYGON_OVERHEAD_CELLS,
) -> dict[int, float]:
    """Estimated per-polygon DEM-window read cost, in cell-count-equivalent units.

    `dprst_gdf` must already carry its FINAL `best_topo` (i.e. after
    `guard_oversized_windows`, not just raw `resolution_class` output) so
    the cost reflects the resolution `topo.read_window` will actually read
    at. Cost model, matching `topo.read_window`'s window geometry:

      - `best_topo=="1m"`: the rim-buffered bbox read at 1 m GSD -- cost ==
        `(bbox_width_m + 2*rim_m) * (bbox_height_m + 2*rim_m)` cells (same
        arithmetic as `guard_oversized_windows`).
      - `best_topo=="10m"`: the SAME bbox+rim window, but read from the 10 m
        seamless tile -- cost == that same area / 100 (10 m x 10 m cells).

    `+base_overhead_cells` is added to every polygon so tiny polygons aren't
    modeled as free next to a huge one (see `BASE_POLYGON_OVERHEAD_CELLS`).

    Pure geometry -- no raster I/O. Returns `{dprst_gdf index label: cost}`,
    the shape `tile_batches`'s `costs` argument
    expects (a tile's load is the sum of its member polygons' cost).
    """
    if "best_topo" not in dprst_gdf.columns:
        raise KeyError(
            "polygon_window_cost requires dprst_gdf to already be tagged by "
            "resolution_class() (missing 'best_topo')"
        )

    width, height = _window_bounds(dprst_gdf, rim_m)
    est_1m_cells = width * height
    is_1m = dprst_gdf["best_topo"] == "1m"
    cells = est_1m_cells.where(is_1m, est_1m_cells / 100.0)
    cost = cells + base_overhead_cells
    return cost.to_dict()


# --- SLURM array plan/dry-run hook (Task 9, issue #173; tile-set batching #223) --
#
# Everything below is only imported/executed when this module is run as a
# script (`python -m gfv2_params.dprst_depth.tiling --plan ...`) -- the extra
# config/vector-I/O imports, including `sources.tag_and_assign`, are
# deliberately local to `_load_and_tag_for_plan` so importing `tile_batches`/
# `tile_set_groups` elsewhere (e.g. `compute.py`, on every one of the ~150
# CONUS array tasks) never pays for them. The `tag_and_assign` import is ALSO
# a circular-import guard, not just a startup-cost one: `sources.py` imports
# `_tile13_key`/`guard_oversized_windows` from THIS module at its own top
# level, so a top-level `from .sources import tag_and_assign` here would
# import-cycle. `_load_and_tag_for_plan` calls `tag_and_assign` -- the SAME
# entry point the in-process builder calls -- so the two paths cannot diverge
# on which candidate tile sets a polygon gets. See
# slurm_batch/submit_dprst_depth.sh for the full array + finalize DAG this
# feeds and the <=5 hr sizing arithmetic.


def _load_and_tag_for_plan(config: dict, logger) -> gpd.GeoDataFrame:
    """Reconstruct + fabric-clip + tag the dprst polygon set for `--plan`.

    Reuses `topo.load_fabric_dprst_polygons` — the SAME shared helper the
    builder's `_load_dprst_polygons` calls — so the SLURM plan/array path and
    the in-process builder path can't diverge on the reconstruction OR the
    fabric clip (without the clip a regional fabric like `oregon` would plan
    the entire CONUS dprst set, not its own). The on-stream COMID set fed to
    that helper is the segment classifier's on-stream set
    (`segment_waterbody_comids.parquet`) MINUS the endorheic set
    (`endorheic_waterbody_comids.parquet`), resolved from `output_dir` --
    the SAME two tables `ctx.paths` tracks for the in-process builder, just
    reached by a different route (this hook runs standalone ahead of any
    other depstor_rasters step, so it has no `BuildContext`/orchestrator to
    read `ctx.paths` from). Then hands off to `sources.tag_and_assign` --
    the ONE entry point both the in-process builder and this plan hook call
    (issue #223) -- to tag `best_topo`, guard oversized windows, and assign
    each polygon its ranked real tile-set candidates (`source_tiles`/
    `candidates`) from the staged 3DEP inventory + WESM project attrs.
    Finally applies the same EPA L3 `ecoregion` tag the builder's
    `_tag_polygons` does, reading directly off the resolved config dict
    instead of a `BuildContext`. Every input is a local, pre-staged file --
    no live S3/`/vsicurl` at all in `--plan` (tile existence and extent are
    already resolved in the staged inventory; `tag_and_assign` never probes
    a raster).

    Applies the SAME `min_onstream_comids`/`min_endorheic_comids` floors
    `wbody_connectivity` enforces at its consuming end
    (`check_onstream_floor`/`check_endorheic_floor`), because this hook has the
    identical doctrine gap: it reads both parquets straight off disk with only
    an existence check, and `submit_dprst_depth.sh` is a documented operator
    route that bypasses the orchestrator entirely. A collapsed or stale table
    would otherwise silently reconstruct the wrong dprst polygon set for the
    whole SLURM array.
    """
    from ..download.epa_ecoregions import ECO_ID_FIELD, ecoregion_of
    from ..endorheic import check_endorheic_floor, load_endorheic_comids, read_signal_counts
    from ..segment_wbody import check_onstream_floor, load_segment_comids
    from .sources import tag_and_assign
    from .topo import load_fabric_dprst_polygons

    required = [
        "waterbody_gpkg", "waterbody_layer", "output_dir",
        "dem_1m_inventory", "wesm_project_attrs", "ecoregions_gpkg", "hru_gpkg", "hru_layer",
    ]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise KeyError(
            f"--plan needs {missing} in the fabric profile (configs/base_config.yml)."
        )

    waterbody_gpkg = Path(config["waterbody_gpkg"])
    dem_1m_inventory = Path(config["dem_1m_inventory"])
    wesm_project_attrs = Path(config["wesm_project_attrs"])
    ecoregions_gpkg = Path(config["ecoregions_gpkg"])
    hru_gpkg = Path(config["hru_gpkg"])
    output_dir = Path(config["output_dir"])
    segment_table = output_dir / "segment_waterbody_comids.parquet"
    endorheic_table = output_dir / "endorheic_waterbody_comids.parquet"
    checks = [
        ("waterbody_gpkg", waterbody_gpkg),
        ("dem_1m_inventory", dem_1m_inventory),
        ("wesm_project_attrs", wesm_project_attrs),
        ("ecoregions_gpkg", ecoregions_gpkg),
        ("hru_gpkg", hru_gpkg),
        ("segment_waterbody_comids.parquet", segment_table),
        ("endorheic_waterbody_comids.parquet", endorheic_table),
    ]
    for label, p in checks:
        if not p.exists():
            raise FileNotFoundError(f"--plan: {label} not found on disk: {p}")

    # Apply the same `min_onstream_comids`/`min_endorheic_comids` floors
    # `wbody_connectivity` enforces at its consuming end. `--plan` reads both
    # parquets straight off disk (no `BuildContext`/orchestrator), which is the
    # exact same doctrine gap `wbody_connectivity`'s consuming-end check exists
    # to close for `--from wbody_connectivity` — a collapsed or stale table
    # would otherwise silently reconstruct the wrong dprst polygon set here too.
    segment_comids = load_segment_comids(segment_table)
    check_onstream_floor(
        len(segment_comids), fabric=config.get("fabric", "<unknown>"),
        floor=config.get("min_onstream_comids"), source=segment_table,
    )
    check_endorheic_floor(
        read_signal_counts(endorheic_table),
        fabric=config.get("fabric", "<unknown>"),
        floor=config.get("min_endorheic_comids"),
        signal_b_active=config.get("wbd_huc12_table") is not None,
        source=endorheic_table,
    )
    onstream_comids = segment_comids - load_endorheic_comids(endorheic_table)

    dprst = load_fabric_dprst_polygons(
        waterbody_gpkg=waterbody_gpkg,
        waterbody_layer=config["waterbody_layer"],
        onstream_comids=onstream_comids,
        hru_gpkg=hru_gpkg,
        hru_layer=config["hru_layer"],
        logger=logger,
    )

    dprst = tag_and_assign(dprst, dem_1m_inventory, wesm_project_attrs, logger)

    eco_gdf = gpd.read_file(ecoregions_gpkg)
    dprst["ecoregion"] = ecoregion_of(dprst, eco_gdf, id_field=ECO_ID_FIELD)
    dprst["ecoregion"] = dprst["ecoregion"].fillna("unassigned")

    return dprst


def _clear_stale_batches(batches_dir: Path, logger) -> int:
    """Delete the previous plan's `batch_*.parquet` before a new plan is written (#221).

    Workers write `batch_{id:04d}.parquet`, so a new plan with FEWER batches used to
    overwrite the low indices and leave the old high ones behind, where the builder's
    glob swept them in with the fresh ones. The builder now refuses a batch set that
    does not match its plan; clearing here keeps the normal path from ever tripping
    that. Only the batch files go -- `_plan/` is rewritten by the caller, and nothing
    else in the directory is touched.
    """
    stale = sorted(batches_dir.glob("batch_*.parquet")) if batches_dir.exists() else []
    for p in stale:
        p.unlink()
    if stale:
        logger.info("  cleared %d batch file(s) from the previous plan in %s", len(stale), batches_dir)
    return len(stale)


def _plan(args) -> None:
    """Build + persist the CONUS SLURM array work-list; print the sizing projection.

    Writes, under `{output_dir}/dprst_depth_batches/_plan/` (a subdirectory
    -- NOT the top level of `dprst_depth_batches/`, so it is never swept up
    by `depstor_builders/dprst_depth.py::_compute_depths`'s flat
    `batch_dir.glob("*.parquet")` scan for the array's own per-batch output):

      - `dprst_polygons_tagged.parquet` -- the full tagged dprst polygon set
        (`COMID`, `FTYPE`, `best_topo`, `ecoregion`, `oversized_1m`,
        `source_tiles`, `candidates`, geometry -- `best_topo`/`oversized_1m`
        reflect `guard_oversized_windows`'s post-guard FINAL resolution, and
        `source_tiles`/`candidates` are `sources.tag_and_assign`'s ranked
        real tile-set assignment, not a raw tag) so every array task reads it
        once instead of re-deriving it from waterbody_gpkg/inventory/WESM
        attrs/ecoregions independently (n_batches redundant reconstructions).
      - `batch_manifest.json` -- `{"n_batches", "n_polygons", "n_tile_sets",
        "tile_sets": [[encoded_tile_set, ...], ...]}`, one entry per SLURM
        array index, from `tile_batches` on `tile_set_groups`'s per-polygon
        primary tile sets, COST-weighted via `polygon_window_cost`. Each
        polygon is in exactly ONE tile set (its `source_tiles`), so no
        union-find/component step is needed here any more -- see
        `tile_set_groups`'s docstring for the measured fallout
        (>4k-tile components, two array tasks running 24+ h) the old
        hull-based `group_by_tile`/`component_tile_batches` produced.

    Pure geometry + local vector reads only -- no live S3/vsicurl (see
    `_load_and_tag_for_plan`).
    """
    import json
    import time

    from gfv2_params.config import load_config
    from gfv2_params.log import configure_logging

    logger = configure_logging("dprst_depth.tiling:plan")
    t0 = time.time()

    raw = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    output_dir = Path(raw["output_dir"])
    batches_dir = Path(args.batches_dir) if args.batches_dir else output_dir / "dprst_depth_batches"
    plan_dir = batches_dir / "_plan"

    logger.info("=== dprst_depth.tiling --plan ===")
    logger.info("  fabric        : %s", raw["fabric"])
    logger.info("  batches_dir   : %s", batches_dir)
    logger.info("  n_batches     : %d", args.n_batches)

    dprst = _load_and_tag_for_plan(raw, logger)
    groups = tile_set_groups(dprst)
    costs = polygon_window_cost(dprst)
    batches = tile_batches(groups, args.n_batches, costs=costs)

    n_polygons = len(dprst)
    n_tile_sets = len(groups)

    loads = [sum(len(groups[tk]) for tk in b) for b in batches]
    cost_loads = [sum(costs.get(idx, 0.0) for tk in b for idx in groups[tk]) for b in batches]
    nonempty = sum(1 for load in loads if load)

    logger.info(
        "  %d dprst polygons -> %d tile set(s) -> %d batch(es) (%d non-empty)",
        n_polygons, n_tile_sets, len(batches), nonempty,
    )
    if loads:
        logger.info(
            "  per-batch polygon-COUNT load: min=%d max=%d mean=%.1f (balance ratio max/mean=%.2f)",
            min(loads), max(loads), sum(loads) / len(loads),
            (max(loads) / (sum(loads) / len(loads))) if sum(loads) else 0.0,
        )
    if cost_loads:
        mean_cost = sum(cost_loads) / len(cost_loads)
        logger.info(
            "  per-batch estimated-COST load (window cells post giant-window guard, "
            "+%d/polygon overhead): min=%.0f max=%.0f mean=%.1f (balance ratio max/mean=%.2f)",
            BASE_POLYGON_OVERHEAD_CELLS, min(cost_loads), max(cost_loads), mean_cost,
            (max(cost_loads) / mean_cost) if mean_cost else 0.0,
        )

    # The 250-500 core-hour figure is the CONUS-scale estimate (~286k
    # polygons); scale it by THIS fabric's actual (fabric-clipped) polygon
    # count so the projection is meaningful for a regional fabric too (Oregon
    # ~3k polygons should not inherit the CONUS wall-clock and falsely read
    # "OVER 5 hr"). Per-polygon cost is assumed roughly constant (one windowed
    # DEM read each), so core-hours scale linearly with polygon count.
    scale = n_polygons / args.conus_ref_polygons if args.conus_ref_polygons else 1.0
    lo, hi = args.core_hours_low * scale, args.core_hours_high * scale
    wc_lo, wc_hi = lo / args.n_batches, hi / args.n_batches
    verdict = "OK" if wc_hi <= 5.0 else "OVER 5 hr TARGET -- increase --n-batches"
    logger.info(
        "  projected: %.1f-%.1f core-hours (%d polygons, scaled from %.0f-%.0f "
        "CONUS-ref for %d) / %d batches -> %.2f-%.2f hr wall-clock (target <=5 hr: %s)",
        lo, hi, n_polygons, args.core_hours_low, args.core_hours_high,
        args.conus_ref_polygons, args.n_batches, wc_lo, wc_hi, verdict,
    )

    _clear_stale_batches(batches_dir, logger)
    plan_dir.mkdir(parents=True, exist_ok=True)
    tagged_path = plan_dir / "dprst_polygons_tagged.parquet"
    tagged_cols = [
        "COMID", "FTYPE", "best_topo", "ecoregion", "oversized_1m",
        "source_tiles", "candidates", "geometry",
    ]
    dprst[tagged_cols].to_parquet(tagged_path)
    logger.info("  wrote tagged polygon set -> %s", tagged_path)

    manifest_path = plan_dir / "batch_manifest.json"
    manifest = {
        "n_batches": args.n_batches,
        "n_polygons": n_polygons,
        "n_tile_sets": n_tile_sets,
        "tile_sets": batches,
    }
    manifest_path.write_text(json.dumps(manifest))
    logger.info(
        "  wrote batch manifest -> %s (%d tile set(s) total)",
        manifest_path, sum(len(b) for b in batches),
    )
    logger.info("=== plan complete in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser(
        description=(
            "dprst_depth tile-batch work-list. Library use is tile_set_groups/"
            "tile_batches; --plan builds + persists the CONUS SLURM array "
            "work-list (Task 9, issue #173; real tile-set batching, issue "
            "#223) -- see slurm_batch/submit_dprst_depth.sh."
        )
    )
    _parser.add_argument("--plan", action="store_true", help="Build the SLURM array work-list (currently the only mode).")
    _parser.add_argument("--config", default="configs/depstor/depstor_rasters.yml", help="Path to depstor_rasters.yml")
    _parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    _parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    _parser.add_argument("--n-batches", type=int, default=150, help="SLURM array size (default 150; see sizing note)")
    _parser.add_argument("--batches-dir", default=None, help="Override {output_dir}/dprst_depth_batches")
    _parser.add_argument("--core-hours-low", type=float, default=250.0, help="CONUS-ref core-hour estimate, low end (scaled by polygon count)")
    _parser.add_argument("--core-hours-high", type=float, default=500.0, help="CONUS-ref core-hour estimate, high end (scaled by polygon count)")
    _parser.add_argument("--conus-ref-polygons", type=int, default=286000, help="Polygon count the core-hour estimate is calibrated at (for scaling)")
    _args = _parser.parse_args()

    if not _args.plan:
        _parser.error("--plan is required (the only currently supported mode)")
    _plan(_args)
