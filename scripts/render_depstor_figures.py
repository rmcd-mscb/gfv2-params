"""Render the depression-storage workflow figures for the presentation deck.

Emits the 14 PNGs under ``docs/figures/depstor/`` that back
``docs/presentations/2026-07-depression-storage-workflow.slides.md``.

The deck is rule-first: each classification rule gets a real map tile at a named
waterbody, with the evidence the rule actually reads drawn on top. The workhorse
is ``tile()``, which composites four layers for any COMID:

1. the land / dprst / on-stream classification raster,
2. the waterbody outline (from the profile's ``waterbody_gpkg`` -- see below),
3. NHD flowlines colored by Network vs. Non-Network membership,
4. FDR code-0 (terminal) cells, which is what Signal A actually reads.

Three data gotchas this module exists to respect
------------------------------------------------
**Waterbody geometry comes from the profile's ``waterbody_gpkg``**
(``conus_waterbodies.gpkg``), not ``nhd_waterbodies.parquet``. The rasters were
built from the former; the latter is staged-from-source but not yet wired into
the profile. Their shorelines differ (Great Salt Lake: 4,368.9 vs. 4,309.7 km2),
so drawing from the parquet would misalign outlines with pixels.

**NHDFlowline field casing varies by VPU** -- VPU 16 ships ``ComID`` /
``WBAreaComI``; VPUs 01 and 08 ship ``COMID`` / ``WBAREACOMI``. Everything is
upper-cased on read (the same gotcha ``download/nhd_flowlines.py`` handles).
Flowlines are EPSG:4269 and are reprojected to the raster CRS, EPSG:5070.

**Never load a full-grid array.** The CONUS template is 153,830 x 109,901 ~ 16.9
billion cells (CLAUDE.md's CONUS-memory rule). Every read here is windowed to a
bounding box AND decimated via ``out_shape``, so GDAL streams a small array
rather than materializing the window.

Run (under SLURM -- never on the login node):

    srun --account=impd --time=60 --mem=64G \\
        pixi run --as-is python scripts/render_depstor_figures.py

    # or a single figure while iterating:
    srun ... python scripts/render_depstor_figures.py --only rule_terminus_gsl
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import argparse  # noqa: E402
import glob  # noqa: E402
import sys  # noqa: E402
from pathlib import Path  # noqa: E402

import geopandas as gpd  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import rasterio  # noqa: E402
from matplotlib.colors import ListedColormap  # noqa: E402
from rasterio.windows import from_bounds  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT = REPO_ROOT / "docs" / "figures" / "depstor"

RASTER_CRS = "EPSG:5070"

# The "before" snapshot: the CONUS product as it stood before the endorheic
# classifier (PR #178) landed. Kept alongside the live product on disk.
BEFORE_DIRNAME = "depstor_rasters_pre_endorheic_2026-07-13"

# Target max array side for every decimated read. A VPU window is tens of
# thousands of native 30 m cells per side; this keeps the read -- and the
# in-memory array -- small regardless of window size.
_MAX_SIDE = 900

_FALLBACK_DATA_ROOT = "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2"


# --------------------------------------------------------------------------
# Pure helpers (no I/O -- gated by tests/test_render_depstor_figures.py)
# --------------------------------------------------------------------------


def normalize_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Upper-case every non-geometry column name.

    NHDPlus ships different field casing per VPU: VPU 16's NHDFlowline has
    ``ComID`` / ``WBAreaComI`` / ``FCode``, while VPUs 01 and 08 have ``COMID``
    / ``WBAREACOMI`` / ``FCODE``. Callers index by the upper-case name.
    """
    renames = {c: c.upper() for c in gdf.columns if c != gdf.geometry.name}
    return gdf.rename(columns=renames)


def classification_array(dprst, dprst_nodata, onstream, onstream_nodata) -> np.ndarray:
    """Composite a 0/1/2 categorical array: land / dprst / on-stream.

    dprst is written LAST and therefore wins a tie. That mirrors the product:
    the clump-veto exemption recovers an endorheic waterbody's own cells as
    dprst even where its 8-connected region touches the on-stream mask.
    """
    cat = np.zeros(dprst.shape, dtype=np.uint8)
    cat[onstream != onstream_nodata] = 2
    cat[dprst != dprst_nodata] = 1
    return cat


def frac_own_stats(df: pd.DataFrame) -> dict:
    """Summarise the Signal-A distribution behind the deck's bimodality claim.

    ``candidates`` counts waterbodies with a computed ``frac_own`` (> 0) -- not
    every row in the table, most of which are Signal-B-only (flagged by a closed
    HUC12, never evaluated for a terminus).

    ``swing`` is how much the answer moves across a 0.3 -> 0.7 threshold sweep,
    relative to the count at 0.5. A small swing is the evidence that 0.5 is not
    a tuned knob.
    """
    candidates = df[df["frac_own"] > 0]
    sweep = {t: int((df["frac_own"] > t).sum()) for t in (0.3, 0.5, 0.7)}
    swing = (sweep[0.3] - sweep[0.7]) / max(sweep[0.5], 1)
    return {
        "candidates": int(len(candidates)),
        "at_or_above_95": int((df["frac_own"] >= 0.95).sum()),
        "in_band_45_55": int(df["frac_own"].between(0.45, 0.55).sum()),
        "sweep": sweep,
        "swing": swing,
    }


# --------------------------------------------------------------------------
# Config-driven paths
# --------------------------------------------------------------------------


def paths(fabric: str = "gfv2") -> dict:
    """Resolve every input path from the active fabric profile.

    Paths live in the profile (CLAUDE.md: never hardcode a data path). Falls
    back to the known data root ONLY for a genuinely config-less checkout --
    ``gfv2_params.config`` unimportable, or ``configs/base_config.yml``
    missing outright -- and emits a stderr warning when it does. Any other
    failure (a bad/renamed profile key, a malformed YAML value, etc.)
    propagates: a wrong path must be loud, not silently swallowed into the
    fallback, since every figure downstream is derived from whatever `paths()`
    resolves.
    """
    try:
        from gfv2_params.config import load_config, require_config_key
    except ImportError as exc:
        print(
            f"paths(): gfv2_params.config not importable ({exc}); "
            f"falling back to {_FALLBACK_DATA_ROOT}",
            file=sys.stderr,
        )
        return _fallback_paths()

    base_config_path = REPO_ROOT / "configs" / "base_config.yml"
    if not base_config_path.exists():
        print(
            f"paths(): {base_config_path} not found; "
            f"falling back to {_FALLBACK_DATA_ROOT}",
            file=sys.stderr,
        )
        return _fallback_paths()

    cfg = load_config(
        REPO_ROOT / "configs" / "depstor" / "depstor_rasters.yml", fabric=fabric
    )
    data_root = Path(cfg["data_root"])
    after = Path(cfg["output_dir"])
    waterbody_gpkg = Path(require_config_key(cfg, "waterbody_gpkg", "render_depstor_figures"))
    waterbody_layer = require_config_key(cfg, "waterbody_layer", "render_depstor_figures")
    fdr = Path(require_config_key(cfg, "fdr_raster", "render_depstor_figures"))

    return _build_paths(data_root, after, waterbody_gpkg, waterbody_layer, fdr)


def _fallback_paths() -> dict:
    """The hardcoded path set used only for a genuinely config-less checkout.

    Values are unchanged from the pre-fix fallback -- only when it fires
    changed (see `paths()`).
    """
    data_root = Path(_FALLBACK_DATA_ROOT)
    after = data_root / "gfv2" / "depstor_rasters"
    waterbody_gpkg = data_root / "input" / "nhd" / "conus_waterbodies.gpkg"
    waterbody_layer = "waterbodies"
    fdr = data_root / "gfv2" / "shared" / "gfv2_fdr.vrt"
    return _build_paths(data_root, after, waterbody_gpkg, waterbody_layer, fdr)


def _build_paths(data_root: Path, after: Path, waterbody_gpkg: Path, waterbody_layer: str, fdr: Path) -> dict:
    return {
        "data_root": data_root,
        "after": after,
        "before": after.parent / BEFORE_DIRNAME,
        "waterbody_gpkg": waterbody_gpkg,
        "waterbody_layer": waterbody_layer,
        "fdr": fdr,
        "endorheic": after / "endorheic_waterbody_comids.parquet",
        "topology": data_root / "input" / "nhd" / "flowline_topology.parquet",
        "burn_add": data_root / "input" / "nhd" / "burn_add_waterbodies.parquet",
        "huc12": data_root / "input" / "wbd" / "wbd_huc12.parquet",
        "source_root": data_root / "shared" / "source",
    }


# --------------------------------------------------------------------------
# Readers (all windowed / bbox-filtered -- never a full read)
# --------------------------------------------------------------------------


def read_window(path: Path, bbox, max_side: int = _MAX_SIDE):
    """Read *path* windowed to *bbox*, decimated so neither side exceeds *max_side*.

    Uses ``out_shape`` so GDAL decimates while reading and never materializes
    the full-resolution window in memory.
    """
    minx, miny, maxx, maxy = bbox
    with rasterio.open(path) as ds:
        win = from_bounds(minx, miny, maxx, maxy, ds.transform)
        win_h, win_w = int(round(win.height)), int(round(win.width))
        scale = max(1, win_h // max_side, win_w // max_side)
        out_h, out_w = max(1, win_h // scale), max(1, win_w // scale)
        arr = ds.read(1, window=win, out_shape=(1, out_h, out_w))
        return arr, ds.nodata


def read_classification(depstor_dir: Path, bbox) -> np.ndarray:
    """land / dprst / on-stream categorical array, windowed to *bbox*."""
    dprst, dprst_nodata = read_window(depstor_dir / "dprst_binary.tif", bbox)
    onstream, onstream_nodata = read_window(depstor_dir / "onstream_binary.tif", bbox)
    return classification_array(dprst, dprst_nodata, onstream, onstream_nodata)


def read_waterbodies(comids: list[int] | None = None, bbox=None) -> gpd.GeoDataFrame:
    """Read the profile's waterbody layer, filtered by COMID or bbox.

    Never reads all 448,124 rows: pyogrio pushes both the ``where`` clause and
    the ``bbox`` down to OGR.
    """
    p = paths()
    where = None
    if comids:
        where = "COMID IN (" + ",".join(str(int(c)) for c in comids) + ")"
    gdf = gpd.read_file(
        p["waterbody_gpkg"], layer=p["waterbody_layer"], where=where, bbox=bbox
    )
    return normalize_fields(gdf)


def read_flowlines(vpu: str, bbox) -> gpd.GeoDataFrame:
    """Read NHDFlowline for *vpu*, bbox-filtered, with a ``network`` bool column.

    ``network`` is membership in ``flowline_topology.parquet`` (NHDPlus
    PlusFlowlineVAA). Non-Network flowlines are the cartographic artificial
    paths NHD draws through essentially every closed-basin lake -- the ones the
    #161 gate exists to ignore. Reprojected from EPSG:4269 to the raster CRS.
    """
    p = paths()
    pattern = str(p["source_root"] / vpu / "NHDSnapshot" / "**" / "Hydrography" / "NHDFlowline.shp")
    hits = glob.glob(pattern, recursive=True)
    if not hits:
        raise FileNotFoundError(f"No NHDFlowline.shp for VPU {vpu} under {p['source_root']}")

    # bbox is in EPSG:5070; the shapefile is EPSG:4269. Convert the box, don't
    # reproject the layer (that would read all of it).
    box_4269 = (
        gpd.GeoSeries.from_wkt([f"POLYGON(({bbox[0]} {bbox[1]},{bbox[2]} {bbox[1]},"
                                f"{bbox[2]} {bbox[3]},{bbox[0]} {bbox[3]},{bbox[0]} {bbox[1]}))"],
                               crs=RASTER_CRS)
        .to_crs("EPSG:4269")
        .total_bounds
    )
    gdf = normalize_fields(gpd.read_file(hits[0], bbox=tuple(box_4269)))
    topo = pd.read_parquet(p["topology"], columns=["comid"])
    network = set(topo["comid"].astype("int64"))
    gdf["network"] = gdf["COMID"].astype("int64").isin(network)
    return gdf.to_crs(RASTER_CRS)


def split_terminal_cells_by_polygon(
    xs: np.ndarray, ys: np.ndarray, geom
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Partition terminal-cell points into inside/outside *geom*.

    Returns ``(inside_xs, inside_ys, outside_xs, outside_ys)``. This is the
    "inside itself" test the rule actually depends on: a terminal cell that
    falls inside the waterbody's own polygon is the evidence Signal A reads;
    one that falls outside is landscape context (the Great Basin is riddled
    with closed-basin sinks -- that's *why* the rule has to be "inside itself"
    and not merely "terminates at a sink").
    """
    xs = np.asarray(xs)
    ys = np.asarray(ys)
    if len(xs) == 0:
        empty = np.asarray([])
        return empty, empty, empty, empty
    import shapely

    inside = shapely.contains(geom, shapely.points(xs, ys))
    return xs[inside], ys[inside], xs[~inside], ys[~inside]


def read_terminal_cells(bbox) -> tuple[np.ndarray, np.ndarray]:
    """Return (x, y) coords of FDR code-0 (terminal) cells inside *bbox*.

    Code 0 is what makes Signal A possible: the NHDPlus FdrFac is depression-
    filled EVERYWHERE EXCEPT at NHDPlus's own sinks, which it leaves unfilled by
    design. Those 15,262 code-0 cells ARE the sink set, and ``d8_routing``
    already treats code 0 as a terminus -- so the classifier and the router read
    the same grid.
    """
    p = paths()
    minx, miny, maxx, maxy = bbox
    with rasterio.open(p["fdr"]) as ds:
        win = from_bounds(minx, miny, maxx, maxy, ds.transform)
        # Terminal cells are sparse and single-pixel -- decimating would drop
        # them, so read this window at FULL resolution. Safe because tiles are
        # single-waterbody windows, not CONUS. Callers must not pass a CONUS bbox.
        arr = ds.read(1, window=win)
        transform = ds.window_transform(win)
    rows, cols = np.nonzero(arr == 0)
    xs, ys = rasterio.transform.xy(transform, rows, cols)
    return np.asarray(xs), np.asarray(ys)


# --------------------------------------------------------------------------
# Styling
# --------------------------------------------------------------------------

CLASS_CMAP = ListedColormap(["#f0f0f0", "#3182bd", "#e6550d"])  # land, dprst, on-stream
CLASS_LABELS = ["land", "depression storage (dprst)", "on-stream waterbody"]

NETWORK_COLOR = "#08519c"      # Network Flowline -- counts as connectivity
NONNETWORK_COLOR = "#cc44aa"   # Non-Network cartographic path -- does NOT
TERMINUS_COLOR = "#111111"     # FDR code-0 terminal cell
TERMINUS_OUTSIDE_COLOR = "#bbbbbb"  # code-0 cell outside the waterbody -- context, not evidence


# --------------------------------------------------------------------------
# The tile compositor
# --------------------------------------------------------------------------


def waterbody_bbox(gdf: gpd.GeoDataFrame, pad_frac: float = 0.35) -> tuple:
    """Padded EPSG:5070 bounds around *gdf*, so context is visible around it."""
    minx, miny, maxx, maxy = gdf.total_bounds
    pad = max(maxx - minx, maxy - miny) * pad_frac
    return (minx - pad, miny - pad, maxx + pad, maxy + pad)


def draw_tile(
    ax,
    bbox,
    depstor_dir: Path,
    *,
    outlines: gpd.GeoDataFrame | None = None,
    vpu: str | None = None,
    show_terminals: bool = False,
    title: str | None = None,
) -> None:
    """Composite the classification raster + outlines + flowlines + terminals."""
    cat = read_classification(depstor_dir, bbox)
    ax.imshow(
        cat,
        cmap=CLASS_CMAP,
        vmin=0,
        vmax=2,
        interpolation="nearest",
        extent=(bbox[0], bbox[2], bbox[1], bbox[3]),
        origin="upper",
    )

    if vpu is not None:
        fl = read_flowlines(vpu, bbox)
        net = fl[fl["network"]]
        non = fl[~fl["network"]]
        if len(non):
            non.plot(ax=ax, color=NONNETWORK_COLOR, linewidth=1.4, linestyle="--", zorder=3)
        if len(net):
            net.plot(ax=ax, color=NETWORK_COLOR, linewidth=1.0, zorder=4)

    if outlines is not None and len(outlines):
        outlines.boundary.plot(ax=ax, color="black", linewidth=0.9, zorder=5)

    if show_terminals:
        xs, ys = read_terminal_cells(bbox)
        if len(xs):
            ax.scatter(xs, ys, s=14, c=TERMINUS_COLOR, marker="x", linewidths=1.1, zorder=6)

    ax.set_xlim(bbox[0], bbox[2])
    ax.set_ylim(bbox[1], bbox[3])
    ax.set_xticks([])
    ax.set_yticks([])
    if title:
        ax.set_title(title, fontsize=11)


def _legend_handles(
    *, flowlines: bool = False, terminals: bool = False, terminals_split: bool = False
) -> list:
    import matplotlib.lines as mlines
    import matplotlib.patches as mpatches

    handles = [
        mpatches.Patch(color=CLASS_CMAP.colors[i], label=CLASS_LABELS[i]) for i in range(3)
    ]
    if flowlines:
        handles += [
            mlines.Line2D([], [], color=NETWORK_COLOR, lw=1.6, label="Network Flowline"),
            mlines.Line2D(
                [], [], color=NONNETWORK_COLOR, lw=1.6, ls="--",
                label="Non-Network path (cartographic)",
            ),
        ]
    if terminals_split:
        # Two marker styles: the bold in-polygon terminus is the evidence the
        # rule reads, the faint elsewhere-in-tile terminus is context showing
        # why "terminates at a sink" alone would over-demote.
        handles += [
            mlines.Line2D(
                [], [], color=TERMINUS_COLOR, marker="x", ls="none",
                markersize=9, markeredgewidth=2.2,
                label="FDR code-0 terminal cell (inside this waterbody)",
            ),
            mlines.Line2D(
                [], [], color=TERMINUS_OUTSIDE_COLOR, marker="x", ls="none",
                markersize=5, markeredgewidth=0.8, alpha=0.6,
                label="FDR code-0 terminal cell (elsewhere in tile)",
            ),
        ]
    elif terminals:
        handles += [
            mlines.Line2D(
                [], [], color=TERMINUS_COLOR, marker="x", ls="none",
                label="FDR code-0 terminal cell",
            )
        ]
    return handles


# --------------------------------------------------------------------------
# Figures
# --------------------------------------------------------------------------

GREAT_SALT_LAKE = 946020001
LEWIS_AND_CLARK = 11758154
WALKER_LAKE = 10734232
MONO_LAKE = 120053921
LAKE_MICHIGAN = 904140248
LAKE_CHAMPLAIN = 15447630
EVERGLADES = 120055431
LARGEST_PLAYA = 120050227
LARGEST_ICE_MASS = 120050242
GSL_VETOING_MARSH = 10273192
# NOT in the brief's constant list -- added after the brief's Mono Lake choice
# for fig_network_gate turned out to have zero flowlines of any kind (Network
# or Non-Network) touching its polygon anywhere in VPU 16 (verified: a whole-
# VPU attribute search on WBAreaComI for both Mono's merged COMID and its raw
# MEMBER_COMID returns 0 rows, and a geometric intersection against its
# padded bbox also returns 0). Pyramid Lake is an equally-valid MUST_BE_DPRST
# fixture (scripts/diagnose/endorheic_fixtures.py) that genuinely has 7
# Non-Network ArtificialPath segments threading it (of 46 flowlines
# intersecting its polygon) -- verified empirically before this substitution.
PYRAMID_LAKE = 11310757


def fig_terminus_gsl() -> Path:
    """Signal A: the terminus-inside-itself rule, and its negative control.

    Great Salt Lake's water ends IN Great Salt Lake (frac_own = 1.000): its FDR
    code-0 terminal cells sit inside its own polygon. Lewis and Clark Lake -- a
    Missouri mainstem reservoir with one stray terminal cell -- ends in the Gulf
    of Mexico (frac_own = 0.007), so it stays on-stream. The rule is
    "terminus INSIDE ITSELF", not merely "terminates at a sink": the latter
    would demote every on-stream reservoir in the Great Basin.

    Both lakes contain an in-polygon terminal cell (a narrow-neck FDR artifact
    puts 2 code-0 cells inside Lewis and Clark's boundary, same raw count as
    Great Salt Lake) -- so raw containment does NOT discriminate them, and the
    figure must not imply that it does. What discriminates them is
    ``frac_own``: the SHARE of the waterbody's own cells whose D8 path actually
    reaches that terminus. That share, not containment, is what the panels and
    the takeaway line below the suptitle annotate.
    """
    p = paths()
    end = pd.read_parquet(p["endorheic"])

    fig, axes = plt.subplots(1, 2, figsize=(12, 5.6))
    # Fixed axes rectangle, set up front -- everything above the axes (name,
    # frac_own, share-of-water) is then placed at fixed FIGURE-fraction
    # y-coordinates via fig.text, not ax.text/set_title, so the stacked lines
    # never collide with each other regardless of how tight_layout would
    # otherwise resize the axes. Lines are spaced evenly (no single oversized
    # gap) and the header is kept short so the map -- not whitespace --
    # dominates the figure at slide size.
    fig.subplots_adjust(top=0.64, bottom=0.15, left=0.05, right=0.98, wspace=0.10)

    for ax, comid, vpu, name, downstream in (
        (axes[0], GREAT_SALT_LAKE, "16", "Great Salt Lake", None),
        (
            axes[1], LEWIS_AND_CLARK, "10U", "Lewis and Clark Lake",
            "Missouri → Gulf of Mexico",
        ),
    ):
        wb = read_waterbodies(comids=[comid])
        row = end[end["comid"] == comid]
        frac = float(row["frac_own"].iloc[0]) if len(row) else 0.0
        verdict = "dprst" if frac > 0.5 else "on-stream"
        bbox = waterbody_bbox(wb)
        # show_terminals=False here: the undifferentiated whole-window marker
        # set is exactly what destroys this figure's argument (every code-0
        # cell in the Great Basin gets drawn, not just GSL's own). Draw the
        # inside/outside split ourselves below instead.
        draw_tile(ax, bbox, p["after"], outlines=wb, vpu=vpu, show_terminals=False)

        xs, ys = read_terminal_cells(bbox)
        geom = wb.geometry.union_all()
        in_xs, in_ys, out_xs, out_ys = split_terminal_cells_by_polygon(xs, ys, geom)
        # Faint context first (low zorder, low alpha): terminal cells are
        # common in this landscape -- that's precisely why the rule has to be
        # "inside ITSELF", not merely "terminates at a sink".
        if len(out_xs):
            ax.scatter(
                out_xs, out_ys, s=6, c=TERMINUS_OUTSIDE_COLOR, marker="x",
                linewidths=0.6, alpha=0.5, zorder=2,
            )
        # Bold evidence on top: the terminal cells actually inside this
        # waterbody's own polygon.
        if len(in_xs):
            ax.scatter(
                in_xs, in_ys, s=90, c=TERMINUS_COLOR, marker="x",
                linewidths=2.4, zorder=7,
            )

        # frac_own is the rule's actual quantitative output -- color it to
        # match the classification fill (dprst blue / on-stream orange) so
        # the verdict is unmistakable even where, as at Lewis and Clark's
        # narrow neck, a couple of locally-noisy code-0 pixels happen to
        # fall geometrically inside the polygon without being where most of
        # the waterbody's own water actually ends up (frac_own stays tiny).
        verdict_color = CLASS_CMAP.colors[1] if verdict == "dprst" else CLASS_CMAP.colors[2]
        # Share-of-water framing, derived from frac_own (never hardcoded) --
        # this replaces the old raw in-polygon COUNT line, which was identical
        # (2 vs 2) for both lakes and therefore argued AGAINST the figure's
        # own point. frac_own is what actually discriminates them: 100% of
        # GSL's own cells terminate inside itself; only 0.7% of Lewis and
        # Clark's do, the rest pass through downstream.
        pct = frac * 100
        if verdict == "dprst":
            share_text = f"{pct:.0f}% of its cells drain to a terminus inside itself"
        elif downstream:
            share_text = f"{pct:.1f}% do — the rest flow through to the {downstream}"
        else:
            share_text = f"only {pct:.1f}% of its cells do"

        cx = (ax.get_position().x0 + ax.get_position().x1) / 2
        fig.text(cx, 0.85, name, ha="center", va="bottom", fontsize=12)
        fig.text(
            cx, 0.795, f"frac_own = {frac:.3f}  →  {verdict}",
            ha="center", va="bottom", fontsize=11, fontweight="bold", color=verdict_color,
        )
        fig.text(
            cx, 0.745, share_text,
            ha="center", va="bottom", fontsize=9.5, color="#555555",
        )

    fig.legend(
        handles=_legend_handles(flowlines=True, terminals_split=True),
        loc="lower center",
        ncol=2,
        frameon=False,
        fontsize=9,
    )
    fig.suptitle(
        "Signal A — a waterbody is depression storage iff its water's terminus lies inside itself",
        y=0.975,
        fontsize=13,
    )
    # The takeaway: both lakes contain an in-polygon terminal cell, so raw
    # containment can't be (and isn't) the test -- forecloses the most
    # obvious objection before a skeptical reader can raise it.
    fig.text(
        0.515, 0.91,
        "Both lakes contain a terminal cell inside their polygon — containment isn't the "
        "test; how much of the lake's own water actually ends there is.",
        ha="center", va="bottom", fontsize=10, style="italic", color="#333333",
    )
    out_path = OUT / "rule_terminus_gsl.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_network_gate() -> Path:
    """The Network-Flowline gate (#161).

    NHD draws a Non-Network cartographic artificial path through essentially
    every closed-basin lake. Only Network membership (PlusFlowlineVAA) counts
    as connectivity; the dashed path threading a correctly-blue Pyramid Lake
    in the `after` snapshot IS the figure -- it shows the trap the gate
    exists to ignore.

    Uses Pyramid Lake, not the brief's originally-specified Mono Lake: a
    whole-VPU attribute search (``WBAreaComI`` for both Mono's merged COMID
    120053921 and its raw MEMBER_COMID 20286504) and a geometric intersection
    against its polygon both return ZERO flowlines -- Mono Lake has no
    ArtificialPath of any kind threading it in this data, Network or
    Non-Network, so the rule cannot be shown firing there. Pyramid Lake is an
    equally-valid MUST_BE_DPRST fixture and genuinely has 7 Non-Network
    ArtificialPath segments threading it (of 46 flowlines intersecting its
    polygon) -- verified before this substitution, not assumed.
    """
    p = paths()
    wb = read_waterbodies(comids=[PYRAMID_LAKE])
    bbox = waterbody_bbox(wb)

    fig, ax = plt.subplots(figsize=(8, 6.5))
    draw_tile(ax, bbox, p["after"], outlines=wb, vpu="16", show_terminals=False)
    ax.legend(handles=_legend_handles(flowlines=True), loc="lower left", fontsize=8, frameon=True)
    ax.set_title("Pyramid Lake -- dprst despite Non-Network paths threading it", fontsize=11)
    fig.suptitle(
        "The Network-Flowline gate (#161) — NHD draws Non-Network artificial paths "
        "through essentially every closed-basin lake. Only Network membership counts "
        "as connectivity.",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    out_path = OUT / "rule_network_gate.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_flowthrough() -> Path:
    """On-stream evidence B -- the geometric flow-through topology test.

    A Network flowline must demonstrably enter AND exit a waterbody for the
    flow-through test to promote it on-stream. Lewis and Clark Lake (a
    Missouri mainstem reservoir) has both -- verified: 57 of the 144 Network
    flowlines intersecting its polygon actually cross its boundary (enter one
    side, exit the other), not merely touch it. Terminal sinks (inflow only)
    and locally-spilling potholes (outflow only) both stay dprst under this
    rule -- only demonstrated in-AND-out flow counts.

    Mono Lake is the negative control, but not "inflow only" as the brief
    first framed it: verified zero Network flowlines intersect its polygon at
    all in this VPU 16 window -- the nearby streams visibly approach from the
    north but stop short of the shoreline. That's an even starker illustration
    of the same point (no demonstrated connectivity at all, let alone in AND
    out), so the panel is labeled accordingly rather than claiming an inflow
    that isn't there.
    """
    p = paths()
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.6))
    for ax, comid, vpu, name in (
        (axes[0], LEWIS_AND_CLARK, "10U", "Lewis and Clark Lake — in AND out"),
        (axes[1], MONO_LAKE, "16", "Mono Lake — no Network flowline touches it"),
    ):
        wb = read_waterbodies(comids=[comid])
        bbox = waterbody_bbox(wb)
        draw_tile(ax, bbox, p["after"], outlines=wb, vpu=vpu, show_terminals=False, title=name)

    fig.legend(
        handles=_legend_handles(flowlines=True), loc="lower center", ncol=2,
        frameon=False, fontsize=9,
    )
    fig.suptitle(
        "On-stream evidence B — a Network flowline must demonstrably enter AND exit. "
        "Terminal sinks (inflow only) and locally-spilling potholes (outflow only) "
        "stay dprst.",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0.07, 1, 0.90))
    out_path = OUT / "rule_flowthrough.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_wbareacomi() -> Path:
    """On-stream evidence A: the WBAREACOMI artificial-path join.

    NHD tags a flowline with the COMID of the waterbody it threads
    (``WBAREACOMI``). If any Network flowline carries this waterbody's COMID,
    the waterbody is on-stream. The gate on Network membership is what stops
    this from promoting closed-basin lakes (#161).
    """
    p = paths()
    wb = read_waterbodies(comids=[LEWIS_AND_CLARK])
    bbox = waterbody_bbox(wb)
    fl = read_flowlines("10U", bbox)
    threading = fl[
        (fl["WBAREACOMI"].astype("int64") == LEWIS_AND_CLARK) & fl["network"]
    ]

    fig, ax = plt.subplots(figsize=(8, 6.5))
    draw_tile(ax, bbox, p["after"], outlines=wb, title=None)
    fl.plot(ax=ax, color="#999999", linewidth=0.6, zorder=3)
    if len(threading):
        threading.plot(ax=ax, color=NETWORK_COLOR, linewidth=2.6, zorder=5)
    ax.set_title(
        f"Lewis and Clark Lake — {len(threading)} Network flowline(s) carry "
        f"WBAREACOMI = {LEWIS_AND_CLARK}\n→ on-stream",
        fontsize=11,
    )
    fig.suptitle("On-stream evidence A — the WBAREACOMI artificial-path join")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    out_path = OUT / "rule_wbareacomi.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_closed_huc12_walker() -> Path:
    """Signal B -- majority-inside a closed (type-C) HUC12.

    Walker Lake's own D8 terminus never lands inside its own polygon
    (frac_own = 0.000) -- Signal A misses it entirely. Signal B catches it
    because the lake lies majority-inside a closed HUC12. Containment must be
    tested by MAJORITY AREA, never ``intersects`` (a zero-interior-overlap
    boundary touch returns True -- Eagle Lake and Middle Alkali Lake graze a
    closed basin at frac = 0.000) and never ``within`` (it drops Great Salt
    Lake, which spills 1.1% into a neighbouring HUC12 at frac = 0.989).

    Verified against the FDR raster (not assumed): Walker Lake's bbox has 5
    code-0 terminal cells, and one genuinely sits inside its polygon --
    exactly the same "raw containment isn't the test" situation
    ``fig_terminus_gsl`` found for Lewis and Clark (2 in-polygon cells,
    frac_own 0.007). Walker's single in-polygon cell isn't reached by the
    lake's own D8 drainage either (frac_own = 0.000, even lower), so it's
    shown split -- bold if inside, faint if outside -- exactly like the
    marquee figure, rather than claiming no in-polygon marker exists at all.
    """
    import matplotlib.lines as mlines

    p = paths()
    wb = read_waterbodies(comids=[WALKER_LAKE])
    bbox = waterbody_bbox(wb)
    # `wbd_huc12.parquet` has no bbox-covering column, so geopandas can't push
    # the bbox down into the Parquet read (unlike the waterbody GPKG/FDR
    # reads). It's a small CONUS-wide table of only the ~2,000 type-C closed
    # HUC12s though -- not a full-grid raster -- so a full read + client-side
    # `.cx[]` bbox filter is cheap and keeps the "never load a full-grid
    # array" rule intact.
    minx, miny, maxx, maxy = bbox
    huc12 = gpd.read_parquet(p["huc12"]).cx[minx:maxx, miny:maxy]

    fig, ax = plt.subplots(figsize=(8, 6.5))
    draw_tile(ax, bbox, p["after"], outlines=wb, show_terminals=False)
    if len(huc12):
        huc12.boundary.plot(ax=ax, color="black", linewidth=1.4, linestyle="--", zorder=6)

    xs, ys = read_terminal_cells(bbox)
    geom = wb.geometry.union_all()
    in_xs, in_ys, out_xs, out_ys = split_terminal_cells_by_polygon(xs, ys, geom)
    if len(out_xs):
        ax.scatter(
            out_xs, out_ys, s=6, c=TERMINUS_OUTSIDE_COLOR, marker="x",
            linewidths=0.6, alpha=0.5, zorder=2,
        )
    if len(in_xs):
        ax.scatter(
            in_xs, in_ys, s=90, c=TERMINUS_COLOR, marker="x", linewidths=2.4, zorder=7,
        )

    handles = _legend_handles(terminals_split=True) + [
        mlines.Line2D(
            [], [], color="black", lw=1.4, ls="--", label="closed (type-C) HUC12"
        ),
    ]
    ax.legend(handles=handles, loc="lower left", fontsize=7.5, frameon=True)
    ax.set_title(
        f"Walker Lake — {len(in_xs)} in-polygon terminal cell, but frac_own = 0.000 "
        "(none of the lake's own water reaches it)\n"
        "majority-inside a closed HUC12 → dprst (Signal B)",
        fontsize=10,
    )
    fig.suptitle(
        "Signal B — majority-AREA containment, never `intersects` (Eagle Lake grazes "
        "a closed basin at frac = 0.000) and never `within` (drops Great Salt Lake, "
        "which spills 1.1% out at frac = 0.989)",
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    out_path = OUT / "rule_closed_huc12_walker.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_domain_exits() -> Path:
    """Guardrail -- domain exits stay on-stream regardless of the classifier.

    Lake Michigan, Lake Champlain, and the Everglades SwampMarsh are terminal
    only because the CONUS model ends there, not because their basin is
    closed. All three are in the 20 named MUST_STAY_ONSTREAM fixtures the
    endorheic classifier is graded against; demoting Lake Michigan to a
    pothole would be catastrophic. The point of this figure is what did NOT
    move -- no flowlines needed, since it's a negative control on the
    classification raster itself.

    Verified per-pixel (not assumed): Champlain and the Everglades render
    ~orange fill as expected (99.7% / 99.9% of in-polygon cells). Lake
    Michigan does NOT -- only 0.1% of its polygon is orange; 99.9% is nodata
    (white, indistinguishable from "land" in this colormap), because the
    HRU fabric that the depstor rasters are built from doesn't extend into
    the Great Lakes' deep open water -- no HRU exists there to classify.
    Critically, 0 of Lake Michigan's in-polygon cells are dprst (blue) either
    -- the guardrail holds everywhere the fabric has an opinion; it simply
    has no opinion over most of the open lake. Each panel is annotated with
    the real in-polygon pixel breakdown so this isn't glossed over.
    """
    import shapely

    p = paths()
    fig, axes = plt.subplots(1, 3, figsize=(15, 5.6))
    for ax, comid, name in (
        (axes[0], LAKE_MICHIGAN, "Lake Michigan"),
        (axes[1], LAKE_CHAMPLAIN, "Lake Champlain"),
        (axes[2], EVERGLADES, "Everglades SwampMarsh"),
    ):
        wb = read_waterbodies(comids=[comid])
        bbox = waterbody_bbox(wb)
        draw_tile(ax, bbox, p["after"], outlines=wb, vpu=None, show_terminals=False, title=name)

        # Ground the "stayed on-stream" claim in the actual in-polygon pixel
        # counts rather than relying on the fill color alone -- Lake Michigan's
        # deep water is nodata (outside HRU domain), not literally orange.
        cat = read_classification(p["after"], bbox)
        h, w = cat.shape
        minx, miny, maxx, maxy = bbox
        xs = minx + (np.arange(w) + 0.5) * (maxx - minx) / w
        ys = maxy - (np.arange(h) + 0.5) * (maxy - miny) / h
        xx, yy = np.meshgrid(xs, ys)
        geom = wb.geometry.union_all()
        inside = shapely.contains(geom, shapely.points(xx.ravel(), yy.ravel())).reshape(cat.shape)
        n_dprst = int(((cat == 1) & inside).sum())
        n_onstream = int(((cat == 2) & inside).sum())
        n_outside_domain = int(((cat == 0) & inside).sum())
        ax.set_xlabel(
            f"{n_onstream:,} on-stream / {n_outside_domain:,} outside HRU domain / "
            f"{n_dprst:,} dprst (own cells)",
            fontsize=8,
        )

    fig.legend(handles=_legend_handles(), loc="lower center", ncol=3, frameon=False, fontsize=9)
    fig.suptitle(
        "Guardrail — domain exits stay on-stream. These are terminal only because the "
        "CONUS model ends there. Demoting Lake Michigan to a pothole would be "
        "catastrophic; all three are in the 20 named fixtures.",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.88))
    out_path = OUT / "rule_domain_exits.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_playa_guardrail() -> Path:
    """Two hard guardrails -- and they are NOT equivalent.

    Playa IS depression storage: FORCE_DPRST_FTYPES makes it dprst
    unconditionally, never promoted on-stream regardless of WBAREACOMI or
    flow-through evidence. Ice Mass is NOT depression storage: it is excluded
    from the waterbody classification entirely (EXCLUDE_WATERBODY_FTYPES) --
    its cells fall back to land, classified perv/imperv via LULC upstream of
    this raster.
    """
    p = paths()
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.6))
    for ax, comid, name in (
        (axes[0], LARGEST_PLAYA, "Largest Playa — force-dprst"),
        (axes[1], LARGEST_ICE_MASS, "Largest Ice Mass — excluded, falls back to land"),
    ):
        wb = read_waterbodies(comids=[comid])
        bbox = waterbody_bbox(wb)
        draw_tile(ax, bbox, p["after"], outlines=wb, vpu=None, show_terminals=False, title=name)

    fig.legend(handles=_legend_handles(), loc="lower center", ncol=3, frameon=False, fontsize=9)
    fig.suptitle(
        "Two hard guardrails, and they are NOT equivalent: Playa IS depression storage "
        "(force-dprst, never promoted on-stream). Ice Mass is NOT depression storage — "
        "it is excluded from the classification and falls back to land.",
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.88))
    out_path = OUT / "rule_playa_guardrail.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", help="render just this figure (stem, no .png)")
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    # CONTRACT: each key is exactly the PNG stem the function writes, so
    # `--only <stem>` always matches the filename the deck references.
    figures = {
        "rule_terminus_gsl": fig_terminus_gsl,
        "rule_network_gate": fig_network_gate,
        "rule_flowthrough": fig_flowthrough,
        "rule_wbareacomi": fig_wbareacomi,
        "rule_closed_huc12_walker": fig_closed_huc12_walker,
        "rule_domain_exits": fig_domain_exits,
        "rule_playa_guardrail": fig_playa_guardrail,
    }
    if args.only:
        if args.only not in figures:
            raise SystemExit(f"Unknown figure {args.only!r}. Known: {sorted(figures)}")
        figures = {args.only: figures[args.only]}
    for fn in figures.values():
        print(fn())


if __name__ == "__main__":
    main()
