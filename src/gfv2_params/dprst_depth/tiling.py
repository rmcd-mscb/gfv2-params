"""Tile-grouped work-list for the dprst_depth_avg builder (issue #173).

The CONUS run reads a windowed DEM per dprst polygon (~286k polygons). To
hit the ~5 hr wall-clock budget, the SLURM array's fan-out unit must be the
elevation TILE, not the polygon: read each ~10 km tile ONCE and process
every polygon whose window falls in it, rather than opening a raster once
per polygon. This module builds that tile -> polygons work-list (Task 3);
the SLURM array batching (Task 9) and the per-tile compute (Task 4) consume
its output.

Pure index math only: no raster reads, no `/vsicurl` existence probes. Tile
*existence* is a Task 4 read-time concern (an absent 1 m tile at the edge of
a project's non-rectangular footprint simply yields no data for the handful
of polygons uniquely assigned to it — see `topo.read_window`'s runtime
1m -> 10m fallback); this module only resolves which tile keys COULD cover a
polygon's window from geometry + the WESM footprint index, reusing
`topo.py`'s own tile-naming helpers so a tile key doubles as the exact
`/vsicurl/` path `topo.read_window` would read (Task 4 can `rasterio.open`
it directly, no re-derivation).

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
from rasterio.warp import transform_bounds, transform_geom

from .topo import (
    TILE13_HTTPS_TEMPLATE,
    _1m_candidate_tiles,
    _tile13_name,
    _utm_zone_epsg,
)

__all__ = ["group_by_tile", "tile_batches", "component_tile_batches"]


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


def _1m_tile_keys(
    geom, project: str, bounds_buffered: tuple[float, float, float, float], src_crs
) -> list[str]:
    """Candidate 3DEP 1 m tile keys (full `/vsicurl/` read paths) for a rim-buffered bbox.

    Reuses `_utm_zone_epsg`/`_1m_candidate_tiles` (Task 1) to enumerate the
    10 km UTM grid candidates a polygon's buffered window overlaps —
    deliberately WITHOUT `topo._existing_paths`'s per-candidate probe (see
    module docstring): existence is resolved at read time in Task 4, not
    here. `bounds_buffered` is the already rim-buffered window (in
    `src_crs`); `geom`'s centroid (unbuffered) picks the UTM zone, matching
    `topo.read_window`/`topo._resolve_1m_paths`.
    """
    centroid = {"type": "Point", "coordinates": (geom.centroid.x, geom.centroid.y)}
    lon, lat = transform_geom(src_crs, "EPSG:4326", centroid)["coordinates"]
    zone, utm_crs = _utm_zone_epsg(lon)
    bounds_utm = transform_bounds(src_crs, utm_crs, *bounds_buffered, densify_pts=21)
    return _1m_candidate_tiles(project, bounds_utm, zone)


def group_by_tile(
    dprst_gdf: gpd.GeoDataFrame,
    wesm_gdf: gpd.GeoDataFrame,
    rim_buffer_m: float = 200.0,
) -> dict[str, list[int]]:
    """Map each covering elevation tile key to the dprst polygon indices in its window.

    `dprst_gdf` must already carry a `best_topo` column (`topo.resolution_class`
    output):

    - `"1m"` polygons resolve their covering 3DEP 1 m project tile(s) from
      every WESM footprint whose geometry intersects the polygon's
      rim-buffered bbox (`rim_buffer_m`, matching `topo.read_window`'s
      window padding). A polygon straddling a tile or project boundary can
      resolve to 2-4 tile keys — the same polygon index becomes a member of
      every one of them, which is expected and fine (Task 4 dedups
      per-polygon results across tiles).
    - `"10m"` polygons resolve the single seamless 1/3 arc-second tile via
      `_tile13_name` on the centroid.
    - If a `"1m"`-tagged polygon's buffered window does not intersect any
      WESM footprint (a `resolution_class` centroid-inside-footprint hit
      near a footprint edge, with the rim buffer pushing the window back
      out — or no usable WESM index at all), it falls back to its 10 m tile
      key, mirroring `topo.read_window`'s own runtime 1m -> 10m fallback.
      This guarantees every polygon lands in at least one group.

    No raster reads and no `/vsicurl` existence probes (see module
    docstring) — pure geometry against `dprst_gdf`/`wesm_gdf` in memory.

    Returns `{tile_key: [dprst_gdf index labels]}`; `tile_key` is the exact
    `/vsicurl/` path `topo.read_window` would open for that tile.
    """
    if "best_topo" not in dprst_gdf.columns:
        raise KeyError(
            "dprst_gdf must be tagged by topo.resolution_class() first (missing 'best_topo')"
        )

    groups: dict[str, set] = defaultdict(set)
    if len(dprst_gdf) == 0:
        return {}

    src_crs = dprst_gdf.crs

    is_1m = dprst_gdf["best_topo"] == "1m"
    for idx, geom in dprst_gdf.loc[~is_1m, "geometry"].items():
        groups[_tile13_key(geom, src_crs)].add(idx)

    df_1m = dprst_gdf.loc[is_1m]
    if len(df_1m) == 0:
        return {k: sorted(v) for k, v in groups.items()}

    has_wesm = wesm_gdf is not None and len(wesm_gdf) > 0 and "project" in wesm_gdf.columns
    resolved: set = set()
    if has_wesm:
        wesm = wesm_gdf.to_crs(src_crs)[["project", "geometry"]]
        windows = gpd.GeoDataFrame(
            {"dprst_idx": df_1m.index},
            geometry=df_1m.geometry.buffer(rim_buffer_m).envelope.values,
            crs=src_crs,
        )
        hits = gpd.sjoin(windows, wesm, how="left", predicate="intersects")
        for _, hit in hits.iterrows():
            project = hit["project"]
            if pd.isna(project):
                continue
            idx = hit["dprst_idx"]
            geom = df_1m.geometry.loc[idx]
            for key in _1m_tile_keys(geom, str(project), hit.geometry.bounds, src_crs):
                groups[key].add(idx)
            resolved.add(idx)

    # 1m-tagged polygons that never resolved a covering WESM project fall
    # back to their 10m tile so every polygon is placed in >=1 group.
    for idx, geom in df_1m.geometry.items():
        if idx not in resolved:
            groups[_tile13_key(geom, src_crs)].add(idx)

    return {k: sorted(v) for k, v in groups.items()}


def tile_batches(groups: dict[str, list[int]], n_batches: int) -> list[list[str]]:
    """Greedy bin-pack tile keys into `n_batches` roughly-equal-work SLURM batches.

    Work is dominated by tile count x polygons-per-tile, not batch
    cardinality, so tile keys are visited in descending polygon-count order
    and each is assigned to whichever batch currently carries the least
    summed polygon count (greedy longest-processing-time-first bin-packing)
    — keeps the `n_batches` SLURM array tasks finishing around the same
    time. Every tile key lands in exactly one batch. Always returns exactly
    `n_batches` lists (some may be empty if there are fewer tile keys than
    batches), matching a fixed-size SLURM array where an empty batch is a
    no-op task.
    """
    if n_batches <= 0:
        raise ValueError(f"n_batches must be positive, got {n_batches}")

    batches: list[list[str]] = [[] for _ in range(n_batches)]
    loads = [0] * n_batches
    for tile_key, members in sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True):
        i = min(range(n_batches), key=lambda b: loads[b])
        batches[i].append(tile_key)
        loads[i] += len(members)
    return batches


def _tile_components(groups: dict[str, list[int]]) -> list[list[str]]:
    """Union-find: merge tile keys sharing >=1 polygon into connected components.

    A polygon whose buffered window spans more than one tile is a member of
    EVERY one of those tile keys' groups (`group_by_tile`'s documented
    behavior). If plain `tile_batches` happened to bin-pack those tile keys
    into DIFFERENT SLURM batches, `compute.run_batch` would independently
    treat the polygon as single-tile within EACH batch's restricted view (it
    only ever sees its own batch's tile keys) and compute it TWICE, each
    time from only ONE of its covering tiles -- an incomplete, wrong DEM
    window for a polygon straddling a tile boundary, not a harmless
    duplicate (`compute.run_batch`'s own docstring flags cross-batch
    dedup/splitting as explicitly out of its scope -- see Task 4's report).
    Grouping tile keys into connected components before bin-packing
    (`component_tile_batches`, below) makes this structurally impossible:
    every polygon's full covering-tile set is always inside one component,
    and `tile_batches` never splits a dict key (here, a whole component)
    across batches.
    """
    parent: dict[str, str] = {tile_key: tile_key for tile_key in groups}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    poly_to_tiles: dict[int, list[str]] = defaultdict(list)
    for tile_key, idxs in groups.items():
        for idx in idxs:
            poly_to_tiles[idx].append(tile_key)
    for tile_keys in poly_to_tiles.values():
        for a, b in zip(tile_keys, tile_keys[1:]):
            union(a, b)

    components: dict[str, list[str]] = defaultdict(list)
    for tile_key in groups:
        components[find(tile_key)].append(tile_key)
    return list(components.values())


def component_tile_batches(groups: dict[str, list[int]], n_batches: int) -> list[list[str]]:
    """`tile_batches`, but a multi-tile polygon's covering tiles never split across batches.

    Bin-packs connected COMPONENTS of tile keys (`_tile_components`), not
    individual tile keys, by their combined polygon count -- same greedy LPT
    heuristic `tile_batches` itself uses, just applied one level up.

    Measured on the real full-CONUS dprst polygon set (`--plan`, 2026-07-11,
    the ~321k-polygon `gfv2`-scale set): componentisation is NOT a rare edge
    case -- of ~84k tile keys, a large fraction end up in a component with >1
    tile, and transitive chaining (tile A shares a polygon with B, B shares a
    different polygon with C -> A/B/C all merge even though A and C share
    nothing directly) produces a handful of large components (the single
    largest CONUS-wide spans >4k tiles, almost certainly the prairie-pothole
    belt's dense depression clusters); a fabric-clipped regional run has
    correspondingly smaller components (oregon: 506 components, largest 83
    tiles). LPT bin-packing still keeps batches reasonably balanced despite
    this (max/mean load ratio -- see `--plan`'s logged output -- comparable to
    plain `tile_batches`' own balance), but a single oversized component could
    in principle dominate one batch; `--plan`'s summary line surfaces the
    largest component size so this is visible before submitting, not silently
    absorbed. This is what `--plan` (below) uses to build the real SLURM array
    work-list; `tile_batches` itself is kept as the simpler, directly-tested
    primitive (Task 3).
    """
    components = _tile_components(groups)
    component_members: dict[str, list[int]] = {}
    component_tiles: dict[str, list[str]] = {}
    for i, comp in enumerate(components):
        comp_id = f"_component_{i}"
        members: set = set()
        for tile_key in comp:
            members.update(groups[tile_key])
        component_members[comp_id] = sorted(members)
        component_tiles[comp_id] = comp

    component_batches = tile_batches(component_members, n_batches)
    return [
        [tile_key for comp_id in batch for tile_key in component_tiles[comp_id]]
        for batch in component_batches
    ]


# --- SLURM array plan/dry-run hook (Task 9, issue #173) --------------------
#
# Everything below is only imported/executed when this module is run as a
# script (`python -m gfv2_params.dprst_depth.tiling --plan ...`) -- the
# extra config/vector-I/O imports are deliberately local to `_load_and_tag_
# for_plan`/`_plan` so importing `group_by_tile`/`tile_batches` elsewhere
# (e.g. `compute.py`, on every one of the ~150 CONUS array tasks) never pays
# for them. `_load_and_tag_for_plan` reimplements
# `depstor_builders/dprst_depth.py`'s `_load_dprst_polygons`/`_tag_polygons`
# rather than importing them: `depstor_builders.dprst_depth` imports THIS
# module (`from ..dprst_depth.tiling import group_by_tile`), so importing it
# back here would be a circular import. See slurm_batch/submit_dprst_depth.sh
# for the full array + finalize DAG this feeds and the <=5 hr sizing
# arithmetic.


def _load_and_tag_for_plan(config: dict, logger) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Reconstruct + fabric-clip + tag the dprst polygon set for `--plan`.

    Reuses `topo.load_fabric_dprst_polygons` — the SAME shared helper the
    builder's `_load_dprst_polygons` calls — so the SLURM plan/array path and
    the in-process builder path can't diverge on the reconstruction OR the
    fabric clip (without the clip a regional fabric like `oregon` would plan
    the entire CONUS dprst set, not its own). Then applies the same WESM
    `best_topo` and EPA L3 `ecoregion` tags the builder's `_tag_polygons`
    does, reading directly off the resolved config dict instead of a
    `BuildContext` since this runs standalone ahead of any other
    depstor_rasters step. Every input is a local, pre-staged vector file --
    no network I/O at all in `--plan` (tile *existence* is a Task 4
    compute-time concern; `group_by_tile` never probes it).
    """
    from ..download.epa_ecoregions import ECO_ID_FIELD, ecoregion_of
    from .topo import load_fabric_dprst_polygons, resolution_class

    required = [
        "waterbody_gpkg", "waterbody_layer", "connected_comids_table",
        "wesm_index", "ecoregions_gpkg", "hru_gpkg", "hru_layer",
    ]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise KeyError(
            f"--plan needs {missing} in the fabric profile (configs/base_config.yml)."
        )

    waterbody_gpkg = Path(config["waterbody_gpkg"])
    connected_comids_table = Path(config["connected_comids_table"])
    wesm_index = Path(config["wesm_index"])
    ecoregions_gpkg = Path(config["ecoregions_gpkg"])
    hru_gpkg = Path(config["hru_gpkg"])
    flowthrough_comids_table = (
        Path(config["flowthrough_comids_table"]) if config.get("flowthrough_comids_table") else None
    )
    checks = [
        ("waterbody_gpkg", waterbody_gpkg),
        ("connected_comids_table", connected_comids_table),
        ("wesm_index", wesm_index),
        ("ecoregions_gpkg", ecoregions_gpkg),
        ("hru_gpkg", hru_gpkg),
    ]
    if flowthrough_comids_table is not None:
        checks.append(("flowthrough_comids_table", flowthrough_comids_table))
    for label, p in checks:
        if not p.exists():
            raise FileNotFoundError(f"--plan: {label} not found on disk: {p}")

    dprst = load_fabric_dprst_polygons(
        waterbody_gpkg=waterbody_gpkg,
        waterbody_layer=config["waterbody_layer"],
        connected_comids_table=connected_comids_table,
        flowthrough_comids_table=flowthrough_comids_table,
        hru_gpkg=hru_gpkg,
        hru_layer=config["hru_layer"],
        logger=logger,
    )

    wesm_gdf = gpd.read_file(wesm_index)
    dprst = resolution_class(dprst, wesm_gdf)
    n_1m = int((dprst["best_topo"] == "1m").sum())
    logger.info("  best_topo: %d/%d polygons tagged 1m (rest 10m)", n_1m, len(dprst))

    eco_gdf = gpd.read_file(ecoregions_gpkg)
    dprst["ecoregion"] = ecoregion_of(dprst, eco_gdf, id_field=ECO_ID_FIELD)
    dprst["ecoregion"] = dprst["ecoregion"].fillna("unassigned")

    return dprst, wesm_gdf


def _plan(args) -> None:
    """Build + persist the CONUS SLURM array work-list; print the sizing projection.

    Writes, under `{output_dir}/dprst_depth_batches/_plan/` (a subdirectory
    -- NOT the top level of `dprst_depth_batches/`, so it is never swept up
    by `depstor_builders/dprst_depth.py::_compute_depths`'s flat
    `batch_dir.glob("*.parquet")` scan for the array's own per-batch output):

      - `dprst_polygons_tagged.parquet` -- the full tagged dprst polygon set
        (`COMID`, `FTYPE`, `best_topo`, `ecoregion`, geometry) so every array
        task reads it once instead of re-deriving it from
        waterbody_gpkg/WESM/ecoregions independently (n_batches redundant
        reconstructions).
      - `batch_manifest.json` -- `{"n_batches", "n_polygons", "n_tiles",
        "tile_batches": [[tile_key, ...], ...]}`, one entry per SLURM array
        index, from `component_tile_batches` (never splits a multi-tile
        polygon's covering tiles across batches -- see `_tile_components`).

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

    dprst, wesm_gdf = _load_and_tag_for_plan(raw, logger)

    groups = group_by_tile(dprst, wesm_gdf)
    batches = component_tile_batches(groups, args.n_batches)

    n_polygons = len(dprst)
    n_tiles = len(groups)
    components = _tile_components(groups)
    n_multi_tile_components = sum(1 for c in components if len(c) > 1)
    max_component_size = max((len(c) for c in components), default=0)

    loads = [sum(len(groups[tk]) for tk in b) for b in batches]
    nonempty = sum(1 for load in loads if load)

    logger.info(
        "  %d dprst polygons -> %d covering tile(s) -> %d batch(es) (%d non-empty)",
        n_polygons, n_tiles, len(batches), nonempty,
    )
    logger.info(
        "  tile components: %d total, %d span >1 tile (multi-tile polygons kept "
        "co-batched), largest component = %d tile(s)",
        len(components), n_multi_tile_components, max_component_size,
    )
    if loads:
        logger.info(
            "  per-batch polygon load: min=%d max=%d mean=%.1f (balance ratio max/mean=%.2f)",
            min(loads), max(loads), sum(loads) / len(loads),
            (max(loads) / (sum(loads) / len(loads))) if sum(loads) else 0.0,
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

    plan_dir.mkdir(parents=True, exist_ok=True)
    tagged_path = plan_dir / "dprst_polygons_tagged.parquet"
    dprst[["COMID", "FTYPE", "best_topo", "ecoregion", "geometry"]].to_parquet(tagged_path)
    logger.info("  wrote tagged polygon set -> %s", tagged_path)

    manifest_path = plan_dir / "batch_manifest.json"
    manifest = {
        "n_batches": args.n_batches,
        "n_polygons": n_polygons,
        "n_tiles": n_tiles,
        "tile_batches": batches,
    }
    manifest_path.write_text(json.dumps(manifest))
    logger.info(
        "  wrote batch manifest -> %s (%d tile keys total)",
        manifest_path, sum(len(b) for b in batches),
    )
    logger.info("=== plan complete in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser(
        description=(
            "dprst_depth tile-batch work-list. Library use is group_by_tile/"
            "tile_batches/component_tile_batches; --plan builds + persists the "
            "CONUS SLURM array work-list (Task 9, issue #173) -- see "
            "slurm_batch/submit_dprst_depth.sh."
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
