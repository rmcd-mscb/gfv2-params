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
import textwrap  # noqa: E402
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


def read_dprst_presence(depstor_dir: Path, bbox, *, max_side: int = _MAX_SIDE) -> np.ndarray:
    """land / dprst binary array (no on-stream distinction), windowed to *bbox*.

    Used by the CONUS before/after figure, which cares only about the dprst
    footprint's total area -- ``read_classification``'s land/dprst/on-stream
    split would need a second full-CONUS-scale read of ``onstream_binary.tif``
    for information the area comparison doesn't use.
    """
    dprst, nodata = read_window(depstor_dir / "dprst_binary.tif", bbox, max_side=max_side)
    cat = np.zeros(dprst.shape, dtype=np.uint8)
    cat[dprst != nodata] = 1
    return cat


def read_drains_presence(depstor_dir: Path, bbox) -> np.ndarray:
    """land / drains-to-dprst binary array, windowed to *bbox*.

    ``drains_to_dprst.tif`` is a plain binary uint8 presence raster and is
    HRU-agnostic (0 = the cell does not drain to any depression; 1 = it does;
    ``nodata`` = off-fabric) -- the separate ``drains_to_dprst_hru.tif`` is the
    HRU-valued raster ``same_hru_drains`` builds on. So "drains to a
    depression" here is any valid, nonzero cell.
    """
    drains, nodata = read_window(depstor_dir / "drains_to_dprst.tif", bbox)
    cat = np.zeros(drains.shape, dtype=np.uint8)
    cat[(drains != nodata) & (drains != 0)] = 1
    return cat


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

# Shared land/#3182bd-blue binary presence styling for the two before/after
# figures that don't need the on-stream (orange) category: the drains-to-
# dprst footprint (land-only vs. draining) and the CONUS dprst-only area
# comparison. Same colors as CLASS_CMAP's land/dprst pair, so blue always
# means "dprst" across every figure in this module.
BINARY_CMAP = ListedColormap(["#f0f0f0", "#3182bd"])
DRAINS_LABELS = ["land", "land draining to depression storage"]
DPRST_ONLY_LABELS = ["land", "depression storage (dprst)"]

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


def wrap_to_width(text: str, width_in: float, *, fontsize: int = 12) -> str:
    """Wrap *text* with `textwrap.fill` so it never overflows *width_in* inches
    at *fontsize*, calibrated the same way for any caller (a figure-wide
    `fig.suptitle` or a single `ax.set_title`).

    ~9.3 chars/inch is calibrated against this file's actual title strings at
    fontsize 12 (`rule_network_gate`'s 8in-wide, ~170-char suptitle wraps to 3
    lines; `rule_domain_exits`' 15in-wide, ~210-char suptitle wraps to 2) --
    comfortably inside the canvas at every figsize/fontsize used here.
    """
    chars_per_line = max(20, int(width_in * 9.3 * 12 / fontsize))
    return textwrap.fill(text, width=chars_per_line)


def assert_ftype_coverage(ftype_counts: pd.Series, ftypes: list[str], kept_total: int) -> None:
    """Raise unless every kept row's FTYPE is one of the plotted *ftypes*.

    ``fig_burnadd_purpcode`` exists to show exactly what
    ``nhd_burn_components.py`` kept -- a chart that quietly drops an unplotted
    FTYPE from its bar would misrepresent the very thing it warns about. The
    upstream guard in ``nhd_burn_components.py`` only rejects *conveyance*
    FTYPEs (StreamRiver/CanalDitch/ArtificialPath), so a future data refresh
    could introduce e.g. Reservoir or Estuary and sail straight through this
    chart unnoticed. Fail loud instead, matching that file's own
    unrecognised-code guards (``_ftype_for_fcode``, the PurpCode check) --
    do not silently widen the plotted list, an unexpected FTYPE is a real
    signal that wants human eyes.
    """
    plotted_total = sum(int(ftype_counts.get(f, 0)) for f in ftypes)
    if plotted_total == kept_total:
        return
    unexpected = sorted(set(ftype_counts.index) - set(ftypes))
    unexpected_counts = {f: int(ftype_counts[f]) for f in unexpected}
    raise ValueError(
        f"fig_burnadd_purpcode: {kept_total - plotted_total} kept BurnAdd row(s) "
        f"carry FTYPE(s) not in the plotted set {ftypes}: {unexpected_counts}. "
        "This figure exists to show what nhd_burn_components.py kept, so an "
        "unplotted FTYPE must not be silently omitted. Add it deliberately "
        "(with its own color) after checking why it showed up -- do not "
        "widen the list reflexively."
    )


def finish_figure(
    fig,
    out_path: Path,
    *,
    suptitle: str,
    legend_handles: list | None = None,
    legend_ncol: int = 3,
    suptitle_fontsize: int = 12,
    legend_fontsize: int = 9,
    extra_bottom_in: float = 0.0,
    ax_title_extra_in: float = 0.0,
    dpi: int = 150,
) -> Path:
    """Wrap the suptitle to the canvas width, put the legend below the axes,
    reserve room for both (plus any known-tall `ax.set_title`), then save.

    Every rule figure ends by calling this instead of hand-rolling its own
    `tight_layout(rect=...)` + `fig.suptitle`/`ax.legend` combination. Two
    bugs came from that per-figure guessing: `fig.suptitle` does not wrap or
    shrink to fit -- a long string is simply drawn past the canvas edge and
    cut off in the saved PNG (every rule figure's suptitle did this) -- and
    `ax.legend(loc="lower left")` places the legend in AXES coordinates, so it
    floats on top of the map instead of below it (`rule_network_gate`,
    `rule_closed_huc12_walker`). Fix: wrap the title with `wrap_to_width` to
    the figure's actual width in inches, then reserve vertical space for the
    now-known line count (title) and handle count (legend) via
    `subplots_adjust`, computed in inches rather than left to
    `tight_layout`'s content-fitting guess -- `tight_layout` has no way to
    know how tall wrapped suptitle text will be until after it's drawn.
    `ax_title_extra_in` reserves additional room for a caller's own
    multi-line `ax.set_title` sitting just above the axes, which otherwise
    collides with a multi-line wrapped suptitle above it
    (`rule_closed_huc12_walker` did this before the parameter existed).
    """
    fig_w_in, fig_h_in = fig.get_size_inches()

    wrapped = wrap_to_width(suptitle, fig_w_in, fontsize=suptitle_fontsize)
    n_lines = wrapped.count("\n") + 1

    line_h_in = suptitle_fontsize * 1.35 / 72.0
    title_in = n_lines * line_h_in + 0.18 + ax_title_extra_in
    top = max(0.5, 1.0 - title_in / fig_h_in)

    bottom = 0.06 + extra_bottom_in / fig_h_in
    if legend_handles:
        n_rows = -(-len(legend_handles) // legend_ncol)  # ceil
        legend_in = n_rows * 0.20 + 0.14
        bottom = max(bottom, legend_in / fig_h_in + extra_bottom_in / fig_h_in)

    fig.subplots_adjust(top=top, bottom=bottom)
    fig.suptitle(wrapped, fontsize=suptitle_fontsize, y=0.99)
    if legend_handles:
        fig.legend(
            handles=legend_handles, loc="lower center", ncol=legend_ncol,
            frameon=False, fontsize=legend_fontsize,
        )
    fig.savefig(out_path, dpi=dpi)
    plt.close(fig)
    return out_path


# --------------------------------------------------------------------------
# Figures
# --------------------------------------------------------------------------

GREAT_SALT_LAKE = 946020001
LEWIS_AND_CLARK = 11758154
WALKER_LAKE = 10734232
MONO_LAKE = 120053921
# Kept per the brief's explicit constant list even though fig_domain_exits no
# longer uses it as a panel (see LAKE_OF_THE_WOODS below) -- its interior is
# outside the HRU fabric's domain, which made it render as an empty outline
# rather than a convincing "stayed on-stream" panel. Still a true
# MUST_STAY_ONSTREAM fixture, just a weak figure choice.
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
# padded bbox also returns 0). Pyramid Lake was tried next as an
# equally-valid MUST_BE_DPRST fixture, but turned out to demonstrate a
# DIFFERENT rule -- see SHEEPY_LAKE below, which replaced it.
PYRAMID_LAKE = 11310757
# fig_network_gate's exemplar (replaces PYRAMID_LAKE -- see that figure's
# docstring). Found by querying, not assumed: of the 10 MUST_BE_DPRST named
# fixtures, NONE is threaded by a Non-Network path with zero Network
# flowlines also touching it -- every one that has any Non-Network path at
# all (Pyramid, Salton Sea) also has real Network flowlines through it.
# Broadened the search to the full endorheic-demoted COMID set in VPUs
# 13/15/16/18 (22,942 COMIDs), spatially joined against each VPU's full
# NHDFlowline set: VPU 16 alone has 107 Non-Network-only candidates, VPU 18
# has 103. Sheepy Lake (Lower Klamath NWR, VPU 18) was chosen from that list:
# FTYPE=LakePond (not Playa/Ice Mass -- those are guardrail-forced regardless
# of this gate, which would undercut the story), 4.89 km2, 8 Non-Network
# paths intersect its polygon and ZERO Network flowlines do, 3 of the 8
# Non-Network paths carry WBAREACOMI == 2554835 (Sheepy Lake's own COMID) --
# the concrete "would have promoted it on-stream" case the WBAREACOMI test
# (`fig_wbareacomi`) is gated against. Raster-verified dprst: 5,284/5,444
# (97.1%) of in-polygon cells, 0 on-stream.
SHEEPY_LAKE = 2554835
# NOT in the brief's constant list -- fig_domain_exits' third panel. Lake
# Michigan's deep open water sits outside the HRU fabric's domain (verified:
# only 77 of 62,836 in-polygon cells are on-stream, 62,759 are nodata/outside
# the fabric), which makes a weak "domain exits stay on-stream" panel -- it
# renders as an empty outline, not orange. Lake of the Woods (also a named
# MUST_STAY_ONSTREAM fixture, drains north to Hudson Bay) is fully in-fabric
# and renders convincingly on-stream: verified 154,948 of 155,007 in-polygon
# cells (100.0%) are on-stream, only 59 outside the HRU domain. (Lake Borgne,
# the other candidate the brief suggested, is worse than Lake Michigan here --
# verified only 595 of 99,943 in-polygon cells, 0.6%, are on-stream -- so it
# was not used.)
LAKE_OF_THE_WOODS = 120052195

# Great Basin (VPU 16) bounding box, EPSG:5070 -- recovered unchanged from the
# pre-rewrite renderer (`git show a40f96b:scripts/render_depstor_figures.py`,
# GREAT_BASIN), where it was resolved from a decimated read of vpu_id.tif.
#
# This is the endorheic classifier's downstream cascade into `drains_to_dprst`
# showcase, NOT the on-stream-barrier-fix (#158/#159) showcase that pairing
# used to illustrate: that fix predates BOTH snapshots this module reads
# (`paths()["before"]` = `depstor_rasters_pre_endorheic_2026-07-13`, which
# already postdates #158/#159), so no before/after pair on disk isolates it
# any more -- the isolating snapshot (`pre_flowthrough_2026-06-26`) is gone.
# What the two snapshots on disk DO isolate is the endorheic classifier
# (PR #178): demoting closed-basin lakes to depression storage flips them
# from on-stream routing barriers to pour-points, which changes the on-stream
# mask `drains_to_dprst`'s barrier subtraction reads -- and that cascade is
# concentrated in the Great Basin (VPU 16, home of Great Salt Lake). Verified
# non-degenerate at native resolution within this bbox (no decimation,
# streamed in 4096-row strips): drains_to_dprst grows 96,776.7 -> 148,438.6
# km2 (+51,661.9 km2, +53.4%) -- a strict subtraction can only remove false
# barrier-gated coverage, and indeed only 20.4 km2 (0.04% of the gain) is
# lost, consistent with rounding/boundary noise rather than a real regression.
GREAT_BASIN = (-2059337.07, 1533691.97, -1195168.54, 2362453.61)


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
    as connectivity; the dashed magenta paths threading a correctly-blue
    Sheepy Lake in the `after` snapshot, with NO solid-blue Network flowline
    anywhere in the polygon, IS the figure -- it shows the trap the gate
    exists to ignore.

    Two prior fixtures were tried and rejected before this one, in order:

    1. The brief's originally-specified Mono Lake has ZERO flowlines of any
       kind (Network or Non-Network) touching its polygon in this data --
       verified by both a whole-VPU WBAreaComI attribute search and a
       geometric intersection. The rule cannot be shown firing there.
    2. Pyramid Lake (an equally-valid MUST_BE_DPRST fixture) DOES have
       Non-Network paths threading it (7 of them) -- but it ALSO has 39
       genuine Network flowlines through it (the Truckee River, its real
       inflow). A figure built on Pyramid Lake shows Signal A overriding
       on-stream evidence (already the marquee `rule_terminus_gsl` story),
       not the Network gate -- solid blue Network lines are visibly threading
       the lake, which is the opposite of what this figure claims.

    Sheepy Lake (VPU 18, Lower Klamath NWR) is the genuine exemplar, found by
    querying rather than assuming: of ALL 10 MUST_BE_DPRST named fixtures,
    none is threaded by a Non-Network path with zero Network flowlines also
    present -- every one with any Non-Network path also has real Network
    flowlines through it, same failure mode as Pyramid Lake. Broadening the
    search to the full endorheic-demoted COMID set (22,942 COMIDs) in VPUs
    13/15/16/18, spatially joined against each VPU's full NHDFlowline set,
    found genuine Non-Network-only cases in every VPU searched (VPU 16: 107,
    VPU 18: 103, VPU 15: 12, VPU 13: 30). Sheepy Lake was selected from that
    list: FTYPE=LakePond (not Playa/Ice Mass -- those are guardrail-forced
    regardless of this gate, which would undercut the story that Signal A/B
    or the connectivity gate is doing the work), 8 Non-Network paths
    intersect its polygon and ZERO Network flowlines do, and 3 of the 8
    Non-Network paths carry ``WBAREACOMI == 2554835`` -- Sheepy Lake's own
    COMID -- the concrete case the WBAREACOMI test (`fig_wbareacomi`) is
    gated against: ungated, those 3 paths would have promoted it on-stream.
    """
    p = paths()
    wb = read_waterbodies(comids=[SHEEPY_LAKE])
    bbox = waterbody_bbox(wb)

    fig, ax = plt.subplots(figsize=(8, 6.5))
    draw_tile(ax, bbox, p["after"], outlines=wb, vpu="18", show_terminals=False)
    ax.set_title(
        wrap_to_width(
            "Sheepy Lake — dprst despite 8 Non-Network paths threading it "
            "(3 carry its own WBAREACOMI); 0 Network flowlines touch it",
            width_in=7.4, fontsize=10.5,
        ),
        fontsize=10.5,
    )
    out_path = OUT / "rule_network_gate.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "The Network-Flowline gate (#161) — NHD draws Non-Network artificial paths "
            "through essentially every closed-basin lake. Only Network membership counts "
            "as connectivity."
        ),
        legend_handles=_legend_handles(flowlines=True),
        legend_ncol=2,
        ax_title_extra_in=0.30,
    )


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

    out_path = OUT / "rule_flowthrough.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "On-stream evidence B — a Network flowline must demonstrably enter AND exit. "
            "Terminal sinks (inflow only) and locally-spilling potholes (outflow only) "
            "stay dprst."
        ),
        legend_handles=_legend_handles(flowlines=True),
        legend_ncol=2,
    )


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
    out_path = OUT / "rule_wbareacomi.png"
    return finish_figure(
        fig,
        out_path,
        suptitle="On-stream evidence A — the WBAREACOMI artificial-path join",
    )


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
    ax.set_title(
        f"Walker Lake — {len(in_xs)} in-polygon terminal cell, but frac_own = 0.000 "
        "(none of the lake's own water reaches it)\n"
        "majority-inside a closed HUC12 → dprst (Signal B)",
        fontsize=10,
    )
    out_path = OUT / "rule_closed_huc12_walker.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "Signal B — majority-AREA containment, never `intersects` (Eagle Lake grazes "
            "a closed basin at frac = 0.000) and never `within` (drops Great Salt Lake, "
            "which spills 1.1% out at frac = 0.989)"
        ),
        legend_handles=handles,
        legend_ncol=3,
        legend_fontsize=8,
        ax_title_extra_in=0.45,
    )


def fig_domain_exits() -> Path:
    """Guardrail -- domain exits stay on-stream regardless of the classifier.

    Lake of the Woods, Lake Champlain, and the Everglades SwampMarsh are
    terminal only because the CONUS model ends there, not because their basin
    is closed. All three are in the 20 named MUST_STAY_ONSTREAM fixtures the
    endorheic classifier is graded against. The point of this figure is what
    did NOT move -- no flowlines needed, since it's a negative control on the
    classification raster itself.

    Verified per-pixel (not assumed): all three render ~orange fill as
    expected -- Lake of the Woods 100.0% (154,948 / 155,007 in-polygon cells),
    Champlain 99.7%, the Everglades 99.9%. Every panel is annotated with the
    real in-polygon pixel breakdown so this isn't asserted on fill color alone.

    Lake Michigan -- the brief's original third panel -- was tried and
    dropped: verified only 77 of 62,836 in-polygon cells (0.1%) are on-stream;
    99.9% is nodata, because the HRU fabric this raster is built from doesn't
    extend into the Great Lakes' deep open water (no HRU exists there to
    classify). It renders as an empty outline, not orange, which is honest
    but a poor "stayed on-stream" panel -- a reader sees "nothing happened,"
    not "the guardrail held." Lake Borgne (the other brief-suggested
    alternate) is worse still: verified only 595 of 99,943 in-polygon cells
    (0.6%) on-stream. Lake of the Woods is a fully in-fabric,
    equally-valid MUST_STAY_ONSTREAM fixture that actually shows the fill.
    Critically, 0 in-polygon cells are dprst (blue) for any of the four
    candidates checked -- the guardrail never fails; only the pixel coverage
    to *see* it varies by fixture.
    """
    import shapely

    p = paths()
    fig, axes = plt.subplots(1, 3, figsize=(15, 5.6))
    for ax, comid, name in (
        (axes[0], LAKE_OF_THE_WOODS, "Lake of the Woods"),
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

    out_path = OUT / "rule_domain_exits.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "Guardrail — domain exits stay on-stream. These are terminal only because "
            "the CONUS model ends there, not because their basin is closed. All three "
            "are in the 20 named MUST_STAY_ONSTREAM fixtures."
        ),
        legend_handles=_legend_handles(),
        legend_ncol=3,
        extra_bottom_in=0.28,
        ax_title_extra_in=0.15,
    )


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

    out_path = OUT / "rule_playa_guardrail.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "Two hard guardrails, and they are NOT equivalent: Playa IS depression storage "
            "(force-dprst, never promoted on-stream). Ice Mass is NOT depression storage — "
            "it is excluded from the classification and falls back to land."
        ),
        legend_handles=_legend_handles(),
        legend_ncol=3,
    )


def fig_rule_ladder() -> Path:
    """The five-stage dprst/on-stream decision ladder.

    Replaces the old two-panel ``fig_decision_schematic`` (legacy 60 m
    segment buffer vs. NHD network connectivity), which predates the
    endorheic classifier (#178) and no longer describes the pipeline. Same
    style as that figure -- a pure schematic, no data I/O -- because every
    count drawn here is a static, verified figure (CLAUDE.md / PR #178), not
    something re-derived per render.

    Landscape layout, boxes sized to content: each stage's title and body are
    wrapped to the box's actual width in inches via ``wrap_to_width`` first,
    then the box height is computed from the resulting line count -- so a box
    can never run text past its own border no matter how the stage text is
    edited later (the old fixed ``box_h``/hand-placed ``\\n`` broke exactly
    this way once titles/bodies got long: stages 2/3/5 overflowed their
    boxes). The axes are added via ``fig.add_axes([0, 0, 1, 1])`` instead of
    ``plt.subplots`` so data coordinates equal figure-fraction coordinates
    1:1 -- the inches-to-data-units conversion is then exact rather than
    guessed against ``plt.subplots``' default margins.

    Stage 4 (endorheic demotion) is a STRICT SUBTRACTION: the nested-set icon
    to its right shows why -- carving a subset OUT of the on-stream union can
    only shrink it, never grow it. That is the load-bearing visual claim this
    figure exists to make (CLAUDE.md: "the subtraction can only ever remove
    COMIDs, never add one").
    """
    import matplotlib.patches as mpatches

    fig_w_in, fig_h_in = 14.0, 7.3  # landscape, slide-friendly (~1.9:1)
    fig = plt.figure(figsize=(fig_w_in, fig_h_in))
    ax = fig.add_axes([0, 0, 1, 1])  # data coords == figure fraction, 1:1
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    stages = [
        (
            "1 — Every NHD waterbody",
            "448,124 waterbodies — Ice Mass (1,220 → land, excluded entirely) "
            "+ BurnAdd sink-purpose rows (1,658)",
            "#deebf7",
        ),
        (
            "2 — Default: dprst",
            "A waterbody is depression storage UNLESS proven on-stream.",
            "#9ecae1",
        ),
        (
            "3 — On-stream evidence (UNION), both Network-gated",
            "WBAREACOMI join ∪ geometric flow-through (in AND out). "
            "Non-Network cartographic paths do not count (#161)",
            "#e6550d",
        ),
        (
            "4 — Endorheic demotion (STRICT SUBTRACTION)",
            "Signal A (terminus-inside-itself) ∪ Signal B (majority-inside a "
            "closed HUC12) → 725 demotions",
            "#fee6ce",
        ),
        (
            "5 — Guardrails",
            "Playa force-dprst · Ice Mass excluded · domain exits stay on-stream "
            "→ dprst_binary.tif, onstream_binary.tif, endorheic_wbody.tif",
            "#c7e9c0",
        ),
    ]

    suptitle = (
        "The dprst / on-stream decision ladder — five stages, top to bottom. Stage 4 "
        "is a strict subtraction: it can only remove a COMID from the on-stream set, "
        "never add one."
    )

    # Reserve the same top margin `finish_figure` will reserve below for the
    # (wrapped) suptitle -- computed identically here so the ladder never
    # gets laid out under where the title will land.
    suptitle_fontsize = 12
    wrapped_sup = wrap_to_width(suptitle, fig_w_in, fontsize=suptitle_fontsize)
    sup_lines = wrapped_sup.count("\n") + 1
    sup_line_in = suptitle_fontsize * 1.35 / 72.0
    top_frac = 1.0 - (sup_lines * sup_line_in + 0.18) / fig_h_in
    bottom_frac = 0.05

    x_left = 0.03
    box_w_frac = 0.60  # leaves room at right for the subtraction icon
    box_w_in = box_w_frac * fig_w_in
    x_pad_in = 0.15
    text_w_in = box_w_in - 2 * x_pad_in

    title_fs, body_fs = 13, 11
    title_line_in = title_fs * 1.35 / 72.0
    body_line_in = body_fs * 1.35 / 72.0
    pad_in = 0.14  # top/bottom padding inside each box
    title_body_gap_in = 0.06
    gap_in = 0.22  # vertical gap between boxes, for the connecting arrow

    pad_frac = pad_in / fig_h_in
    title_body_gap_frac = title_body_gap_in / fig_h_in
    title_line_frac = title_line_in / fig_h_in
    body_line_frac = body_line_in / fig_h_in
    gap_frac = gap_in / fig_h_in
    x_pad_frac = x_pad_in / fig_w_in

    laid_out = []  # (wrapped_title, wrapped_body, color, n_title, height_frac)
    for title, body, color in stages:
        wrapped_title = wrap_to_width(title, text_w_in, fontsize=title_fs)
        wrapped_body = wrap_to_width(body, text_w_in, fontsize=body_fs)
        n_title = wrapped_title.count("\n") + 1
        n_body = wrapped_body.count("\n") + 1
        height_frac = (
            2 * pad_frac
            + n_title * title_line_frac
            + title_body_gap_frac
            + n_body * body_line_frac
        )
        laid_out.append((wrapped_title, wrapped_body, color, n_title, height_frac))

    total_h_frac = sum(h for *_, h in laid_out) + gap_frac * (len(laid_out) - 1)
    avail_frac = top_frac - bottom_frac
    y_cursor = top_frac - max(0.0, (avail_frac - total_h_frac) / 2)

    box_ys = []  # (y_top, y_bottom) per stage, top to bottom
    for wrapped_title, wrapped_body, color, n_title, h_frac in laid_out:
        y_top = y_cursor
        y_bottom = y_top - h_frac
        box = mpatches.FancyBboxPatch(
            (x_left, y_bottom), box_w_frac, h_frac,
            boxstyle="round,pad=0.008", facecolor=color, edgecolor="black",
        )
        ax.add_patch(box)
        title_y = y_top - pad_frac
        ax.text(
            x_left + x_pad_frac, title_y, wrapped_title,
            ha="left", va="top", fontsize=title_fs, fontweight="bold",
            linespacing=1.35,
        )
        body_y = title_y - n_title * title_line_frac - title_body_gap_frac
        ax.text(
            x_left + x_pad_frac, body_y, wrapped_body,
            ha="left", va="top", fontsize=body_fs, linespacing=1.35,
        )
        box_ys.append((y_top, y_bottom))
        y_cursor = y_bottom - gap_frac

    x_arrow = x_left + 0.04
    for i in range(len(box_ys) - 1):
        y_bottom_prev = box_ys[i][1]
        y_top_next = box_ys[i + 1][0]
        is_subtraction = i == 2  # arrow FROM stage 3 INTO stage 4
        color_arrow = "#cc2222" if is_subtraction else "#555555"
        ax.annotate(
            "", xy=(x_arrow, y_top_next), xytext=(x_arrow, y_bottom_prev),
            arrowprops=dict(
                arrowstyle="-|>", color=color_arrow,
                lw=2.0 if is_subtraction else 1.4,
            ),
        )

    # Stage 4 is a SUBTRACTION, not another additive stage: a nested-set icon
    # (endorheic carved OUT of the on-stream union) makes "can only shrink,
    # never grow" visible rather than merely asserted in text. Anchored to
    # stage 4's vertical center, in the column to the right of the ladder.
    subtract_y = (box_ys[3][0] + box_ys[3][1]) / 2
    icon_x0 = x_left + box_w_frac + 0.05
    icon_w = 1.0 - icon_x0 - 0.02
    outer_h = 0.20
    outer = mpatches.FancyBboxPatch(
        (icon_x0, subtract_y - outer_h / 2), icon_w, outer_h,
        boxstyle="round,pad=0.01", facecolor="none", edgecolor="#08519c", lw=1.6,
    )
    ax.add_patch(outer)
    ax.text(
        icon_x0 + icon_w / 2, subtract_y + outer_h / 2 + 0.015, "on-stream (union)",
        ha="center", va="bottom", fontsize=9, color="#08519c",
    )
    inner_w, inner_h = icon_w * 0.5, 0.09
    inner_x0 = icon_x0 + (icon_w - inner_w) / 2
    inner = mpatches.FancyBboxPatch(
        (inner_x0, subtract_y - inner_h / 2), inner_w, inner_h,
        boxstyle="round,pad=0.008", facecolor="#cc2222", alpha=0.35,
        edgecolor="#cc2222", lw=1.2, linestyle="--",
    )
    ax.add_patch(inner)
    ax.text(
        inner_x0 + inner_w / 2, subtract_y, "− endorheic",
        ha="center", va="center", fontsize=9, color="#cc2222", fontweight="bold",
    )
    ax.text(
        icon_x0 + icon_w / 2, subtract_y - outer_h / 2 - 0.03,
        "can only REMOVE a COMID —\nnever add one",
        ha="center", va="top", fontsize=8.5, color="#cc2222", style="italic",
    )

    out_path = OUT / "rule_ladder.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=suptitle,
        suptitle_fontsize=suptitle_fontsize,
    )


def fig_pipeline_dag() -> Path:
    """The depstor builder DAG: inputs through PRMS params.

    Recovered from the pre-rewrite renderer (``git show
    f73e74a:scripts/render_depstor_figures.py``, ``fig_pipeline_dag``) and
    extended with the steps PR #178 introduced: ``nhd_topology`` (highlighted
    orange -- it MUST precede both ``nhd_flowlines`` and ``nhd_flowthrough``,
    which fail loud without it), ``endorheic``, and its clump-veto exemption
    edge straight into ``dprst`` (dashed red, distinct from the solid
    ``endorheic -> wbody_connectivity`` subtraction edge feeding the union).
    """
    import matplotlib.patches as mpatches

    # (label, x, y, half_width)
    nodes = {
        # inputs
        "nhd": ("NHD\n(waterbodies, flowlines)", 0.06, 0.92, 0.075),
        "wbd": ("WBD\n(closed HUC12s)", 0.06, 0.74, 0.075),
        "fdr": ("FDR\n(fdr.vrt, code 0=sink)", 0.06, 0.56, 0.085),
        "twi": ("TWI", 0.06, 0.38, 0.05),
        "lulc": ("LULC\n(NLCD)", 0.06, 0.20, 0.065),
        # staging -- nhd_topology MUST precede both COMID steps
        "topology": ("nhd_topology", 0.25, 0.92, 0.07),
        "flowlines": ("nhd_flowlines\n(WBAREACOMI)", 0.29, 0.68, 0.085),
        "flowthrough": ("nhd_flowthrough\n(geometric)", 0.21, 0.44, 0.085),
        # classification
        "waterbody": ("waterbody", 0.44, 0.92, 0.065),
        "endorheic": ("endorheic\n(Signal A + B)", 0.46, 0.30, 0.08),
        "wbody_conn": ("wbody_connectivity\n(union − endorheic)", 0.64, 0.56, 0.095),
        "dprst": ("dprst\n(+ clump-veto exemption)", 0.68, 0.30, 0.095),
        # routing -> params
        "routing": ("routing\n(D8 + on-stream barrier)", 0.86, 0.56, 0.095),
        "same_hru": ("same_hru_drains", 0.82, 0.38, 0.08),
        "depth": ("dprst_depth", 0.90, 0.20, 0.065),
        "params": ("PRMS params\n(6 spatial)", 1.04, 0.38, 0.07),
    }

    inputs = {"nhd", "wbd", "fdr", "twi", "lulc"}
    edges = [
        ("nhd", "topology"),
        ("nhd", "waterbody"),
        ("fdr", "waterbody"),
        ("topology", "flowlines"),
        ("topology", "flowthrough"),
        ("nhd", "flowlines"),
        ("nhd", "flowthrough"),
        ("waterbody", "flowthrough"),
        ("fdr", "endorheic"),
        ("wbd", "endorheic"),
        ("waterbody", "endorheic"),
        ("flowlines", "wbody_conn"),
        ("flowthrough", "wbody_conn"),
        ("endorheic", "wbody_conn"),
        ("wbody_conn", "dprst"),
        ("endorheic", "dprst"),
        ("lulc", "dprst"),
        ("fdr", "routing"),
        ("twi", "routing"),
        ("dprst", "routing"),
        ("routing", "same_hru"),
        ("lulc", "same_hru"),
        ("same_hru", "params"),
        ("dprst", "depth"),
        ("depth", "params"),
    ]
    # Edges needing a style distinct from the default -- the hard ordering
    # constraint (topology before either COMID step) and the clump-veto
    # exemption path (endorheic straight into dprst, NOT via wbody_conn).
    edge_style = {
        ("topology", "flowlines"): dict(color="#e6550d", lw=2.2),
        ("topology", "flowthrough"): dict(color="#e6550d", lw=2.2),
        ("endorheic", "dprst"): dict(
            color="#cc2222", lw=2.0, linestyle="--", connectionstyle="arc3,rad=0.35"
        ),
    }

    fig, ax = plt.subplots(figsize=(15, 7))
    for key, (label, x, y, hw) in nodes.items():
        if key in inputs:
            color = "#deebf7"
        elif key == "topology":
            color = "#fdae6b"  # highlighted -- the hard ordering constraint
        elif key == "params":
            color = "#31a354"
        else:
            color = "#9ecae1"
        box = mpatches.FancyBboxPatch(
            (x - hw, y - 0.07), 2 * hw, 0.14,
            boxstyle="round,pad=0.01", facecolor=color, edgecolor="black",
        )
        ax.add_patch(box)
        ax.text(x, y, label, ha="center", va="center", fontsize=7.5)

    for src, dst in edges:
        x0, y0, hw0 = nodes[src][1], nodes[src][2], nodes[src][3]
        x1, y1, hw1 = nodes[dst][1], nodes[dst][2], nodes[dst][3]
        # Clip the arrow to just outside each node's box, capped so the two
        # ends can never cross (which would silently invert the arrowhead).
        gap = min(hw0 + 0.015, hw1 + 0.015, max(abs(x1 - x0) * 0.4, 0.001))
        dx = 0.0 if abs(x1 - x0) < 1e-9 else (gap if x1 >= x0 else -gap)
        style = dict(color="#555555", lw=1.2, connectionstyle="arc3,rad=0.08")
        style.update(edge_style.get((src, dst), {}))
        connectionstyle = style.pop("connectionstyle")
        ax.annotate(
            "",
            xy=(x1 - dx, y1),
            xytext=(x0 + dx, y0),
            arrowprops=dict(arrowstyle="->", connectionstyle=connectionstyle, **style),
        )

    ax.annotate(
        "must precede\nboth COMID steps", xy=(0.25, 0.92), xytext=(0.25, 1.04),
        ha="center", fontsize=7.5, color="#e6550d", fontweight="bold",
        arrowprops=dict(arrowstyle="-", color="#e6550d", lw=0.8),
    )
    ax.text(
        0.535, 0.14, "clump-veto exemption\n(endorheic_wbody.tif)",
        ha="center", fontsize=7.5, color="#cc2222", style="italic",
    )

    ax.set_xlim(-0.05, 1.20)
    ax.set_ylim(0.05, 1.10)
    ax.axis("off")

    out_path = OUT / "pipeline_dag.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "Depression-storage builder DAG — nhd_topology (orange) must precede BOTH "
            "nhd_flowlines and nhd_flowthrough (they fail loud without it); the dashed red "
            "edge is the endorheic_wbody.tif clump-veto exemption straight into dprst, "
            "distinct from the endorheic − wbody_connectivity subtraction feeding the "
            "on-stream union."
        ),
        suptitle_fontsize=11,
    )


def fig_frac_own_bimodal() -> Path:
    """frac_own is bimodal, so the 0.5 threshold is inert -- not a tuned knob.

    Reads the classifier table directly rather than transcribing the PR body,
    so the deck's numbers cannot drift from the product.
    """
    p = paths()
    df = pd.read_parquet(p["endorheic"])
    stats = frac_own_stats(df)
    candidates = df[df["frac_own"] > 0]["frac_own"]

    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.hist(candidates, bins=np.linspace(0, 1, 51), color="#3182bd", edgecolor="white")
    ax.axvline(0.5, color="#cc2222", lw=2, ls="--", label="threshold = 0.5")
    ax.axvspan(0.45, 0.55, color="#cc2222", alpha=0.10)
    ax.set_yscale("log")
    ax.set_xlabel("frac_own  (share of the waterbody's cells whose D8 path ends inside itself)")
    ax.set_ylabel("waterbodies (log scale)")
    ax.legend(loc="upper center")
    ax.set_title(
        f"{stats['candidates']:,} Signal-A candidates · "
        f"{stats['at_or_above_95']:,} at frac_own ≥ 0.95 · "
        f"only {stats['in_band_45_55']:,} in the 0.45–0.55 band\n"
        f"threshold sweep 0.3→0.7: {stats['sweep'][0.3]:,} → {stats['sweep'][0.7]:,} "
        f"({stats['swing']:.1%} swing) — the threshold is inert",
        fontsize=11,
    )
    fig.tight_layout()
    out_path = OUT / "frac_own_bimodal.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  frac_own stats (put these in the deck): {stats}")
    return out_path


def fig_burnadd_purpcode() -> Path:
    """``BurnAddWaterbody`` is NOT a sink layer -- only sink-purpose rows are kept.

    It is every waterbody NHDPlus added to the DEM burn; only rows with a
    sink ``PurpCode`` (4 Playa, 5/8 closed lake) are sinks.
    ``download/nhd_burn_components.py`` keeps only those rows and takes
    ``FTYPE`` from ``FCODE``, not ``PurpCode`` (``PurpCode`` 5 spans both
    Playa and SwampMarsh -- a Playa mislabelled LakePond loses force-dprst).

    The VPU 01 counts (702 NULL-PurpCode rows / 503 on-network / 0 sinks in
    VPU 01's own ``Sink.shp``) are static, verified numbers from PR #178,
    hardcoded here per the task-4 brief -- VPU 01's raw, pre-filter
    ``BurnAddWaterbody`` rows and its ``Sink.shp`` are not staged in this
    checkout, so they cannot be re-derived. The 1,658 kept rows and their
    FTYPE mix (LakePond 1,550 / Playa 103 / SwampMarsh 5) ARE derived from
    ``paths()["burn_add"]`` below, so those numbers cannot drift from the
    product.
    """
    import matplotlib.patches as mpatches

    p = paths()
    burn_add = pd.read_parquet(p["burn_add"])
    ftype_counts = burn_add["FTYPE"].value_counts()
    kept_total = int(len(burn_add))

    # Static, verified counts from PR #178 (VPU 01) -- see docstring.
    vpu01_null_purpcode = 702
    vpu01_null_purpcode_on_network = 503
    vpu01_sinks_in_sink_shp = 0

    ftype_colors = {"LakePond": "#3182bd", "Playa": "#e6550d", "SwampMarsh": "#31a354"}
    ftypes = ["LakePond", "Playa", "SwampMarsh"]

    # Fail loud rather than silently omitting a bar segment -- see docstring.
    assert_ftype_coverage(ftype_counts, ftypes, kept_total)

    fig, ax = plt.subplots(figsize=(9, 6.5))

    # Bar 1: all BurnAdd rows in VPU 01 -- mostly NULL PurpCode, most of
    # those on-network (StreamRiver, CanalDitch), against zero sinks in
    # VPU 01's own Sink.shp.
    ax.bar(0, vpu01_null_purpcode, width=0.5, color="#bbbbbb", edgecolor="black")
    ax.bar(0, vpu01_null_purpcode_on_network, width=0.5, color="#888888",
           edgecolor="black", hatch="//")
    ax.annotate(
        f"{vpu01_sinks_in_sink_shp} sinks in VPU 01's own Sink.shp",
        xy=(0, 2), xytext=(0, 90), textcoords="offset points",
        ha="center", fontsize=9, color="#cc2222", fontweight="bold",
        arrowprops=dict(arrowstyle="-", color="#cc2222", lw=1.2),
    )

    # Bar 2: what's actually kept CONUS-wide -- only the sink-purpose rows,
    # stacked by FTYPE (derived from the parquet, not hardcoded).
    bottom = 0
    for ftype in ftypes:
        n = int(ftype_counts.get(ftype, 0))
        ax.bar(1, n, width=0.5, bottom=bottom, color=ftype_colors[ftype], edgecolor="black")
        bottom += n

    ax.set_xticks([0, 1])
    ax.set_xticklabels(
        [
            f"all BurnAdd rows\n(VPU 01, n={vpu01_null_purpcode:,} NULL-PurpCode)",
            f"kept sink-purpose rows\n(CONUS-wide, n={kept_total:,})",
        ]
    )
    ax.set_ylabel("rows")
    ax.set_title(
        "Merging BurnAddWaterbody wholesale would turn canals into depression storage",
        fontsize=10.5,
    )

    handles = [
        mpatches.Patch(color="#bbbbbb", label=f"VPU 01 NULL PurpCode ({vpu01_null_purpcode:,})"),
        mpatches.Patch(
            facecolor="#888888", hatch="//", edgecolor="black",
            label=f"...of which on-network, e.g. StreamRiver/CanalDitch ({vpu01_null_purpcode_on_network:,})",
        ),
    ] + [
        mpatches.Patch(color=ftype_colors[f], label=f"kept {f} ({int(ftype_counts.get(f, 0)):,})")
        for f in ftypes
    ]

    out_path = OUT / "rule_burnadd_purpcode.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "BurnAddWaterbody is every waterbody NHDPlus added to the DEM burn — only "
            "sink-purpose rows (PurpCode 4 Playa, 5/8 closed lake) are sinks. VPU 01 ships "
            "702 NULL-PurpCode rows against 0 sinks in its own Sink.shp (503 on-network); "
            "FTYPE comes from FCODE, not PurpCode."
        ),
        legend_handles=handles,
        legend_ncol=2,
        legend_fontsize=8,
        # The two-line xticklabels ("all BurnAdd rows\n(VPU 01, n=...)") sit
        # right below the axes, in the same reserved strip finish_figure
        # otherwise gives only to the legend -- every prior caller has
        # `ax.set_xticks([])`, so this is the first real collision. Reserve
        # room for both.
        extra_bottom_in=0.55,
    )


# --------------------------------------------------------------------------
# Before/after figures -- the only readers of the `before` snapshot
# --------------------------------------------------------------------------


def fig_clump_veto_gsl() -> Path:
    """The clump veto, and why endorheic evidence overrides it.

    clump_regions 8-connects Great Salt Lake to a 49.1 km2 SwampMarsh
    (COMID 10273192) whose water drains INTO the lake -- so the marsh's terminus
    is GSL, not itself, and it is CORRECTLY left on-stream. But
    regions_touching_mask excludes a WHOLE region if any one cell touches the
    on-stream mask, so that one marsh vetoed all 4,854,156 GSL cells: GSL came
    out 0% dprst even though connected_wbody.tif no longer contained it.

    Fixed by exempting an endorheic waterbody's own not-on-stream cells from the
    region-level exclusion. The clump rule is a heuristic PROXY for connectivity;
    the terminus rule is direct hydrologic EVIDENCE. Evidence overrides proxy --
    but only where we have evidence, and only for the waterbody's own cells. A
    cell that is itself on-stream (the marsh) always stays excluded.

    This is the headline figure: both GSL and the marsh are outlined so the
    reader can see the 49 km2 feature that vetoed the 4,369 km2 lake.
    """
    p = paths()
    wb = read_waterbodies(comids=[GREAT_SALT_LAKE, GSL_VETOING_MARSH])
    bbox = waterbody_bbox(wb, pad_frac=0.10)

    fig, axes = plt.subplots(1, 2, figsize=(12, 6.5))
    for ax, d, title in (
        (axes[0], p["before"], "before — the marsh vetoes the whole clump\nGSL: 0% dprst"),
        (axes[1], p["after"], "after — endorheic cells exempted\nGSL: 100% dprst"),
    ):
        draw_tile(ax, bbox, d, outlines=wb, title=title)

    out_path = OUT / "clump_veto_gsl.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "A 49.1 km² SwampMarsh was vetoing a 4,368.9 km² lake — evidence overrides "
            "proxy, but only where we have evidence"
        ),
        legend_handles=_legend_handles(),
        legend_ncol=3,
        ax_title_extra_in=0.30,
    )


def drains_great_basin_deltas() -> dict:
    """Re-derive the Great Basin `drains_to_dprst` delta from the two snapshots.

    Derived, never transcribed -- mirrors `conus_area_deltas()`'s pattern so
    the figure's panel titles and this function cannot disagree. Counts come
    from `read_drains_presence`'s decimated read (bounded by `read_window`'s
    `_MAX_SIDE` guard), so they are approximate, scaled by the decimation
    factor -- this function exists to CONFIRM the direction/magnitude are in
    the right ballpark, not to be the deck's cited number.

    Cross-checked against a native-resolution (unscaled, 4096-row-streamed)
    measurement of this exact bbox: drains_to_dprst grows 96,776.7 ->
    148,438.6 km2 (+51,661.9 km2, +53.4%), with only 20.4 km2 (0.04% of the
    gain) lost -- consistent with the strict-subtraction invariant a demoted
    endorheic lake can only ever REMOVE itself as an on-stream barrier, never
    add a new one.
    """
    p = paths()
    out = {}
    for label, d in (("before", p["before"]), ("after", p["after"])):
        with rasterio.open(d / "drains_to_dprst.tif") as ds:
            win = from_bounds(*GREAT_BASIN, transform=ds.transform)
            native_h = int(round(win.height))
            cell_km2 = abs(ds.transform.a * ds.transform.e) / 1e6
        cat = read_drains_presence(d, GREAT_BASIN)
        scale = native_h / cat.shape[0]
        out[label] = float(cat.sum() * scale * scale * cell_km2)
    out["delta_pct"] = (out["after"] - out["before"]) / out["before"] * 100
    return out


def fig_drains_great_basin_before_after() -> Path:
    """The endorheic classifier's cascade into `drains_to_dprst` (Great Basin).

    NOT the on-stream-barrier-fix (#158/#159) showcase this bbox used to draw
    in the pre-rewrite renderer -- that fix predates both snapshots available
    on disk (`paths()["before"]` = `depstor_rasters_pre_endorheic_2026-07-13`
    already postdates #158/#159, merged 2026-07-01), and the snapshot that
    could isolate it (`pre_flowthrough_2026-06-26`) is gone. What the two
    snapshots on disk DO isolate is the endorheic classifier (PR #178): a
    closed-basin lake demoted from on-stream to depression storage stops being
    a `routing` traversal barrier and becomes a pour-point instead, so the
    land draining to depression storage changes accordingly. That cascade is
    concentrated in the Great Basin (VPU 16, home of Great Salt Lake) because
    that is where the endorheic classifier actually changed on-stream status
    (`endorheic_waterbody_comids.parquet` is a VPU 13/15/16/18-only set).

    Verified non-degenerate before captioning (see `drains_great_basin_deltas`
    and its native-resolution cross-check): this grows, it does not shrink --
    the opposite direction from the retired Lower Mississippi figure, because
    this is a different mechanism (barrier removal adds pour-point-reachable
    land) than that figure's claim (over-extension removal subtracts land).
    """
    p = paths()
    deltas = drains_great_basin_deltas()

    fig, axes = plt.subplots(1, 2, figsize=(11, 6))
    for ax, d, label in (
        (axes[0], p["before"], "before"),
        (axes[1], p["after"], "after"),
    ):
        cat = read_drains_presence(d, GREAT_BASIN)
        ax.imshow(
            cat,
            cmap=BINARY_CMAP,
            vmin=0,
            vmax=1,
            interpolation="nearest",
            extent=(GREAT_BASIN[0], GREAT_BASIN[2], GREAT_BASIN[1], GREAT_BASIN[3]),
            origin="upper",
        )
        ax.set_xlim(GREAT_BASIN[0], GREAT_BASIN[2])
        ax.set_ylim(GREAT_BASIN[1], GREAT_BASIN[3])
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(
            f"{label} — {deltas[label]:,.0f} km² drains to dprst (derived, decimated)",
            fontsize=11,
        )

    import matplotlib.patches as mpatches

    handles = [
        mpatches.Patch(color=BINARY_CMAP.colors[i], label=DRAINS_LABELS[i]) for i in range(2)
    ]
    out_path = OUT / "drains_great_basin_before_after.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            "Great Basin (VPU 16) — demoting endorheic lakes to depression storage flips "
            "them from on-stream routing barriers to pour-points: land draining to "
            f"depression storage grows {deltas['before']:,.0f} → {deltas['after']:,.0f} km² "
            f"({deltas['delta_pct']:+.1f}%, derived from these snapshots) — cross-check "
            "against a native-resolution measurement of this same bbox: "
            "96,776.7 → 148,438.6 km² (+53.4%), only 20.4 km² (0.04% of the gain) lost"
        ),
        legend_handles=handles,
        legend_ncol=2,
    )


def conus_area_deltas() -> dict:
    """Re-derive the deck's headline km² from the two snapshots.

    Derived, never transcribed: the results table and the maps cannot disagree.
    Counts are taken from a decimated read, so they are approximate -- scaled by
    the decimation factor. The deck quotes the exact figures from the PR's own
    A/B gate (scripts/diagnose/ab_endorheic_rebuild.py); this function exists to
    CONFIRM they are in the right ballpark, and to fail loud if they are not.
    """
    p = paths()
    with rasterio.open(p["after"] / "dprst_binary.tif") as ds:
        bounds = ds.bounds
        cell_km2 = abs(ds.transform.a * ds.transform.e) / 1e6

    out = {}
    for label, d in (("before", p["before"]), ("after", p["after"])):
        arr, nodata = read_window(d / "dprst_binary.tif", tuple(bounds), max_side=_MAX_SIDE)
        # Decimation samples 1 cell per (scale x scale) block, so scale the count.
        with rasterio.open(d / "dprst_binary.tif") as ds:
            scale = ds.height / arr.shape[0]
        out[label] = float((arr != nodata).sum() * scale * scale * cell_km2)
    out["delta_pct"] = (out["after"] - out["before"]) / out["before"] * 100
    return out


def fig_conus_dprst_before_after() -> Path:
    """CONUS-wide dprst footprint, before vs. after the endorheic classifier.

    The `_MAX_SIDE` guard in `read_window` is what makes this safe: a full-
    resolution CONUS read is 16.9 billion cells (CLAUDE.md's CONUS-memory
    rule). Both panels are decimated reads of the same CONUS bounds, so they
    stay pixel-comparable.

    Titles carry the areas `conus_area_deltas()` re-derives from these same
    two snapshots (decimated, so approximate); the suptitle carries the exact
    figures from PR #178's own A/B gate for cross-check. Growth should be most
    visible in the Great Basin (VPU 16), the source of the marquee example.
    """
    p = paths()
    with rasterio.open(p["after"] / "dprst_binary.tif") as ds:
        bounds = tuple(ds.bounds)
    deltas = conus_area_deltas()

    fig, axes = plt.subplots(1, 2, figsize=(14, 7))
    for ax, label, d in (
        (axes[0], "before", p["before"]),
        (axes[1], "after", p["after"]),
    ):
        cat = read_dprst_presence(d, bounds, max_side=_MAX_SIDE)
        ax.imshow(cat, cmap=BINARY_CMAP, vmin=0, vmax=1, interpolation="nearest")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(f"{label} — {deltas[label]:,.0f} km² dprst (derived, decimated)", fontsize=11)

    import matplotlib.patches as mpatches

    handles = [
        mpatches.Patch(color=BINARY_CMAP.colors[i], label=DPRST_ONLY_LABELS[i]) for i in range(2)
    ]
    out_path = OUT / "conus_dprst_before_after.png"
    return finish_figure(
        fig,
        out_path,
        suptitle=(
            f"CONUS depression storage grows {deltas['before']:,.0f} → {deltas['after']:,.0f} km² "
            f"({deltas['delta_pct']:+.1f}%, derived from these snapshots) — cross-check against "
            "the PR #178 A/B gate's exact 42,535 → 51,930 km² (+22.1%)"
        ),
        legend_handles=handles,
        legend_ncol=2,
    )


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
        "rule_ladder": fig_rule_ladder,
        "pipeline_dag": fig_pipeline_dag,
        "frac_own_bimodal": fig_frac_own_bimodal,
        "rule_burnadd_purpcode": fig_burnadd_purpcode,
        "clump_veto_gsl": fig_clump_veto_gsl,
        "drains_great_basin_before_after": fig_drains_great_basin_before_after,
        "conus_dprst_before_after": fig_conus_dprst_before_after,
    }
    if args.only:
        if args.only not in figures:
            raise SystemExit(f"Unknown figure {args.only!r}. Known: {sorted(figures)}")
        figures = {args.only: figures[args.only]}
    for fn in figures.values():
        print(fn())


if __name__ == "__main__":
    main()
