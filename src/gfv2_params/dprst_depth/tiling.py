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
"""
from __future__ import annotations

from collections import defaultdict

import geopandas as gpd
import pandas as pd
from rasterio.warp import transform_bounds, transform_geom

from .topo import (
    TILE13_HTTPS_TEMPLATE,
    _1m_candidate_tiles,
    _tile13_name,
    _utm_zone_epsg,
)

__all__ = ["group_by_tile", "tile_batches"]


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
