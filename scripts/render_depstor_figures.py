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


def fig_terminus_gsl() -> Path:
    """Signal A: the terminus-inside-itself rule, and its negative control.

    Great Salt Lake's water ends IN Great Salt Lake (frac_own = 1.000): its FDR
    code-0 terminal cells sit inside its own polygon. Lewis and Clark Lake -- a
    Missouri mainstem reservoir with one stray terminal cell -- ends in the Gulf
    of Mexico (frac_own = 0.007), so it stays on-stream. The rule is
    "terminus INSIDE ITSELF", not merely "terminates at a sink": the latter
    would demote every on-stream reservoir in the Great Basin.
    """
    p = paths()
    end = pd.read_parquet(p["endorheic"])

    fig, axes = plt.subplots(1, 2, figsize=(12, 6.5))
    # Fixed axes rectangle, set up front -- everything above the axes (name,
    # frac_own, count) is then placed at fixed FIGURE-fraction y-coordinates
    # via fig.text, not ax.text/set_title, so the three stacked lines never
    # collide with each other regardless of how tight_layout would otherwise
    # resize the axes.
    fig.subplots_adjust(top=0.68, bottom=0.16, left=0.05, right=0.98, wspace=0.12)

    for ax, comid, vpu, name in (
        (axes[0], GREAT_SALT_LAKE, "16", "Great Salt Lake"),
        (axes[1], LEWIS_AND_CLARK, "10U", "Lewis and Clark Lake"),
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
        cx = (ax.get_position().x0 + ax.get_position().x1) / 2
        fig.text(cx, 0.92, name, ha="center", va="bottom", fontsize=13)
        fig.text(
            cx, 0.86, f"frac_own = {frac:.3f}  →  {verdict}",
            ha="center", va="bottom", fontsize=12, fontweight="bold", color=verdict_color,
        )
        fig.text(
            cx, 0.71, f"{len(in_xs)} terminal cell(s) inside its own polygon",
            ha="center", va="bottom", fontsize=9, color="#555555",
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
        y=0.985,
    )
    out_path = OUT / "rule_terminus_gsl.png"
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
    }
    if args.only:
        if args.only not in figures:
            raise SystemExit(f"Unknown figure {args.only!r}. Known: {sorted(figures)}")
        figures = {args.only: figures[args.only]}
    for fn in figures.values():
        print(fn())


if __name__ == "__main__":
    main()
