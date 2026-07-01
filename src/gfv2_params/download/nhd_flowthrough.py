"""Distil flow-through (on-stream) waterbody COMIDs from NHD topology.

WBAREACOMI (see nhd_flowlines) only flags waterbodies NHD drew an artificial
path through. Many through-flow swamps/marshes carry no WBAREACOMI and are
wrongly left in depression storage, so their whole upstream watershed counts as
draining to dprst. This module adds a second, geometry-based on-stream signal:
a waterbody that a stream demonstrably flows THROUGH (channel inflow AND
outflow) is on-stream/lake, not a depression. Playa (an endorheic terminal
sink) is force-kept as dprst; Ice Mass is not depression storage at all and is
excluded from the waterbody classification entirely (at the `waterbody`
builder), so it is neither dprst nor on-stream.

The COMID set written here is unioned with connected_waterbody_comids.parquet by
the depstor wbody_connectivity builder.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
import shapely
from shapely.geometry import Point

from gfv2_params.config import load_base_config
from gfv2_params.download.nhd_flowlines import (
    download_snapshot,
    vpu_index,
    write_connected_comids,
)
from gfv2_params.log import configure_logging

logger = configure_logging("download_nhd_flowthrough")

# A conveyance NHDFlowline carries channelised flow (vs. Pipeline, Coastline...).
CONVEYANCE_FTYPES = {"StreamRiver", "ArtificialPath", "Connector", "CanalDitch"}
# NHDArea polygons that ARE the 2-D channel (wide/braided rivers).
CONVEYANCE_AREA_FTYPES = {"StreamRiver"}
# Waterbodies that are always depression storage — never promoted on-stream.
FORCE_DPRST_FTYPES = {"Playa"}
# Waterbodies excluded from the depstor waterbody classification entirely:
# neither dprst nor on-stream. A glacier/permanent ice mass is not depression
# storage; its cells fall back to land (perv/imperv via LULC).
EXCLUDE_WATERBODY_FTYPES = {"Ice Mass"}
# Union: FTYPEs that must never appear in the on-stream set (dropped up front in
# flow-through and at the wbody_connectivity union guardrail).
NEVER_ONSTREAM_FTYPES = FORCE_DPRST_FTYPES | EXCLUDE_WATERBODY_FTYPES


def _endpoints(geom):
    """(upstream_point, downstream_point) for a (Multi)LineString: first & last coord.

    Coordinates are sliced to X/Y: NHD geometry is measured 3D (XYZM), so a raw
    `coords[i]` can carry 4 ordinates, which `Point()` rejects (it accepts only 2
    or 3). Only planar position matters for the inflow/outflow test.
    """
    if geom.geom_type == "MultiLineString":
        parts = list(geom.geoms)
        first = parts[0].coords[0]
        last = parts[-1].coords[-1]
    else:
        first = geom.coords[0]
        last = geom.coords[-1]
    return Point(first[:2]), Point(last[:2])


def flowthrough_comids(
    waterbodies: gpd.GeoDataFrame,
    flowlines: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame | None = None,
    routed_comids: set[int] | None = None,
) -> set[int]:
    """COMIDs of waterbodies classified on-stream by flow-through topology.

    `waterbodies`: polygons with COMID, FTYPE. `flowlines`: lines with COMID,
    FTYPE (direction now comes from `routed_comids`/topology, so FLOWDIR is no
    longer read). `areas`: optional NHDArea polygons with FTYPE. All share one
    CRS.

    A waterbody is on-stream if ANY of:
      T1  a single conveyance flowline flows through it: crosses the boundary
          >= 2 times, OR has non-zero interior length with both endpoints
          outside the polygon (so it must enter and exit),
      D1  a routed-network conveyance flowline (in flowline_topology with
          DnHydroseq != 0) has its UPSTREAM end inside the waterbody -> it
          discharges to the network (source lake or split-pass-through outflow),
      T3  it overlaps a conveyance NHDArea polygon.
    Playa (force-dprst) and Ice Mass (excluded from the waterbody classification
    entirely) are both dropped first and never returned.
    """
    wb = waterbodies[~waterbodies["FTYPE"].isin(NEVER_ONSTREAM_FTYPES)].copy()
    if wb.empty:
        return set()
    wb = wb.reset_index(drop=True)
    wb["_wbidx"] = wb.index

    conv = flowlines[flowlines["FTYPE"].isin(CONVEYANCE_FTYPES)].copy()
    onstream: set[int] = set()

    if not conv.empty:
        conv = conv.reset_index(drop=True)
        # Candidate (waterbody, flowline) pairs that intersect at all.
        pairs = gpd.sjoin(
            conv[["FTYPE", "geometry"]],
            wb[["_wbidx", "geometry"]],
            how="inner", predicate="intersects",
        )

        # --- T1: a single conveyance line flows through the waterbody ---
        # Two complementary signals (either suffices), both direction-free:
        #   (a) the line crosses the boundary >= 2 times, OR
        #   (b) the line has non-zero interior length with BOTH endpoints outside
        #       the polygon (it must therefore enter and exit -> flows through).
        # (a) alone is fragile for sinuous lines that run ALONG the shoreline:
        # those make `intersection(boundary)` a GeometryCollection (mixed Point +
        # LineString), which is not a `Multi*` type, so the geom-count collapsed
        # to 1 and real through-flow was missed (e.g. VPU 15 LakePond COMID
        # 21744935, flowline 21745077). (b) catches that case topologically. (a)
        # is retained because it also catches lines that cross >= 2 times but
        # terminate INSIDE the polygon (endpoint covered), which (b) skips.
        t1_idx: set[int] = set()
        for line_pos, wbidx in zip(pairs.index, pairs["_wbidx"]):
            line = conv.geometry.iloc[line_pos]
            poly = wb.geometry.iloc[wbidx]
            crossing = line.intersection(poly.boundary)
            # `geoms` exists on every multi-part/collection geometry (MultiPoint,
            # MultiLineString, GeometryCollection); a lone Point/LineString has no
            # `.geoms` and counts as a single crossing.
            n = 0 if crossing.is_empty else (
                len(crossing.geoms) if hasattr(crossing, "geoms") else 1
            )
            if n >= 2:
                t1_idx.add(int(wbidx))
                continue
            up, down = _endpoints(line)
            if (not poly.covers(up)) and (not poly.covers(down)) \
                    and line.intersection(poly).length > 0:
                t1_idx.add(int(wbidx))

        # --- D1: routed network outflow (authoritative direction via topology) ---
        # A routed conveyance flowline whose UPSTREAM end is inside W discharges
        # out of W: a source/headwater lake, or the outflow half of a stream NHD
        # split at the shore. NHDPlus network flowlines are digitized downstream,
        # so the first vertex (_endpoints[0]) is the upstream end. Direction is
        # taken from topology membership, never the unreliable FLOWDIR field.
        d1_idx: set[int] = set()
        routed = routed_comids or set()
        if routed:
            for line_pos, wbidx in zip(pairs.index, pairs["_wbidx"]):
                if int(conv["COMID"].iloc[line_pos]) not in routed:
                    continue
                up, _ = _endpoints(conv.geometry.iloc[line_pos])
                if wb.geometry.iloc[wbidx].covers(up):
                    d1_idx.add(int(wbidx))

        for wbidx in t1_idx | d1_idx:
            onstream.add(int(wb.loc[wbidx, "COMID"]))

    # --- T3: overlap a conveyance NHDArea polygon ---
    if areas is not None and not areas.empty:
        ca = areas[areas["FTYPE"].isin(CONVEYANCE_AREA_FTYPES)]
        if not ca.empty:
            hit = gpd.sjoin(
                wb[["COMID", "geometry"]], ca[["geometry"]],
                how="inner", predicate="intersects",
            )
            onstream |= {int(c) for c in hit["COMID"].unique()}

    return onstream


def locate_layer(flowline_shp: Path, layer: str) -> Path | None:
    """Find a sibling NHDSnapshot layer (e.g. NHDWaterbody) next to NHDFlowline."""
    candidate = flowline_shp.with_name(f"{layer}.shp")
    return candidate if candidate.exists() else None


def read_layer(path: Path, columns: list[str]) -> gpd.GeoDataFrame:
    """Read `columns` (case-insensitive) + geometry, normalised to upper-case.

    NHD field casing is inconsistent across VPU snapshots; requesting exact
    upper-case names would make pyogrio silently drop a mismatched-case column.
    """
    available = list(pyogrio.read_info(path)["fields"])
    by_upper = {name.upper(): name for name in available}
    rename = {}
    for canon in columns:
        actual = by_upper.get(canon)
        if actual is None:
            raise KeyError(
                f"{path}: layer has no '{canon}' field (case-insensitive). "
                f"Available: {available}"
            )
        rename[actual] = canon
    gdf = gpd.read_file(path, columns=list(rename), use_arrow=True)
    gdf = gdf.rename(columns=rename)[[*columns, "geometry"]]
    # NHD ships measured 3D (XYZM) geometry; force planar 2D so downstream
    # shapely ops (Point construction, sjoin, boundary intersection) never see
    # >2 ordinates. force_2d drops both Z and M.
    gdf["geometry"] = shapely.force_2d(gdf.geometry.to_numpy())
    return gdf


def main() -> None:
    base = load_base_config()
    data_root = Path(base["data_root"])
    download_dir = data_root / "input/nhd_downloads"
    extract_dir = data_root / "shared/source"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    topo_path = data_root / "input/nhd/flowline_topology.parquet"
    if not topo_path.exists():
        raise FileNotFoundError(
            f"flowline_topology.parquet not found: {topo_path}. Run "
            f"`python -m gfv2_params.download.nhd_topology` first."
        )
    topo = pd.read_parquet(topo_path, columns=["comid", "dnhydroseq"])
    routed_comids = {int(c) for c in topo[topo["dnhydroseq"] != 0]["comid"]}
    logger.info(f"Loaded {len(routed_comids)} routed-network COMIDs")

    onstream: set[int] = set()
    failures = []
    for vpu, dd in vpu_index.items():
        flowline = download_snapshot(dd, vpu, download_dir, extract_dir)
        if flowline is None:
            failures.append(vpu)
            continue
        wb_path = locate_layer(flowline, "NHDWaterbody")
        if wb_path is None:
            failures.append(vpu)
            continue
        waterbodies = read_layer(wb_path, ["COMID", "FTYPE"])
        flowlines = read_layer(flowline, ["COMID", "FTYPE"])
        area_path = locate_layer(flowline, "NHDArea")
        areas = read_layer(area_path, ["FTYPE"]) if area_path else None
        vpu_set = flowthrough_comids(waterbodies, flowlines, areas, routed_comids)
        if not vpu_set:
            logger.warning(
                "VPU %s: 0 flow-through COMIDs — check FTYPE/FLOWDIR domain values "
                "in this snapshot (Playa/Ice Mass waterbodies are excluded by design).",
                vpu,
            )
        logger.info(f"VPU {vpu}: {len(vpu_set)} flow-through waterbody COMIDs")
        onstream |= vpu_set

    if failures:
        raise RuntimeError(
            f"NHDSnapshot flow-through staging failed for VPU(s): {failures}"
        )

    if not onstream:
        raise ValueError(
            "distilled 0 on-stream COMIDs across all VPUs → every swamp/marsh "
            "would stay in depression storage; likely an NHD FTYPE/FLOWDIR "
            "value-domain change or CRS/geometry problem"
        )

    out_path = data_root / "input/nhd/flowthrough_waterbody_comids.parquet"
    write_connected_comids(onstream, out_path)
    logger.info(f"Wrote {len(onstream)} flow-through COMIDs -> {out_path}")


if __name__ == "__main__":
    main()
