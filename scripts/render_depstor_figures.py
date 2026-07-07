"""Render the depression-storage workflow figures for the presentation deck.

Makes four PNGs under ``docs/figures/depstor/``:

- ``decision_schematic.png`` — legacy 60 m segment-buffer split vs. the
  NHD-network-connectivity split now used to decide dprst vs. on-stream.
- ``pipeline_dag.png`` — the depstor builder DAG (inputs through PRMS params).
- ``great_basin_before_after.png`` — VPU 16 (Great Basin) dprst/on-stream
  classification before vs. after the geometric flow-through fix (#145):
  endorheic waterbodies correctly retained as depression storage.
- ``lower_miss_before_after.png`` — VPU 08 (Lower Mississippi) before vs.
  after: ``drains_to_dprst`` over-extension into humid open-drainage terrain
  removed.

The two before/after maps read CONUS-scale rasters (``dprst_binary.tif``,
``onstream_binary.tif``) from both the current
``depstor_rasters/`` directory and the ``depstor_rasters_pre_flowthrough_
2026-06-26/`` snapshot. The CONUS grid is ~16.9 billion cells (see
CLAUDE.md's CONUS-memory-rule), so this script NEVER loads a full-grid
array: it windows every read to a region bounding box via
``rasterio.windows.from_bounds`` and, for the wide VPU-scale windows here,
also decimates the read with ``out_shape`` so GDAL streams a downsampled
array directly rather than materializing the full-resolution window.

Run (default pixi env has matplotlib + rasterio):

    pixi run python scripts/render_depstor_figures.py
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

from pathlib import Path  # noqa: E402

import matplotlib.patches as mpatches  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import rasterio  # noqa: E402
from matplotlib.colors import ListedColormap  # noqa: E402
from rasterio.windows import from_bounds  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT = REPO_ROOT / "docs" / "figures" / "depstor"

_FALLBACK_DATA_ROOT = "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2"

try:
    from gfv2_params.config import load_base_config

    DATA_ROOT = Path(load_base_config()["data_root"])
except Exception:  # pragma: no cover - fallback for a config-less checkout
    DATA_ROOT = Path(_FALLBACK_DATA_ROOT)

CURRENT_DIR = DATA_ROOT / "gfv2" / "depstor_rasters"
PRE_FIX_DIR = DATA_ROOT / "gfv2" / "depstor_rasters_pre_flowthrough_2026-06-26"

# Region bounding boxes in the raster CRS (EPSG:5070, CONUS Albers), resolved
# in Step 3 from a decimated read of vpu_id.tif (VPU 16 = Great Basin, VPU 08
# = Lower Mississippi). (minx, miny, maxx, maxy).
GREAT_BASIN = (-2059337.07, 1533691.97, -1195168.54, 2362453.61)
LOWER_MISS = (161095.96, 680908.25, 689198.95, 1659807.87)

# Target max array side for the decimated windowed reads below (a VPU window
# is tens of thousands of native 30 m cells per side; this keeps the read --
# and the in-memory array -- small regardless of window size).
_MAX_SIDE = 900


def read_window(path: Path, bbox: tuple[float, float, float, float], max_side: int = _MAX_SIDE):
    """Read *path* windowed to *bbox*, decimated so neither side exceeds *max_side*.

    Uses ``out_shape`` so GDAL performs the decimation while reading (never
    materializes the full-resolution window in memory).
    """
    minx, miny, maxx, maxy = bbox
    with rasterio.open(path) as ds:
        win = from_bounds(minx, miny, maxx, maxy, ds.transform)
        win_h, win_w = int(round(win.height)), int(round(win.width))
        scale = max(1, win_h // max_side, win_w // max_side)
        out_h, out_w = max(1, win_h // scale), max(1, win_w // scale)
        arr = ds.read(1, window=win, out_shape=(1, out_h, out_w))
        return arr, ds.nodata


def _dprst_onstream_category(dprst_path: Path, onstream_path: Path, bbox):
    """Return a 0/1/2 categorical array: land / dprst / on-stream, windowed to *bbox*."""
    dprst, dprst_nodata = read_window(dprst_path, bbox)
    onstream, _ = read_window(onstream_path, bbox)
    cat = np.zeros(dprst.shape, dtype=np.uint8)
    cat[onstream != 255] = 2
    cat[dprst != dprst_nodata] = 1
    return cat


_CATEGORY_CMAP = ListedColormap(["#f0f0f0", "#3182bd", "#e6550d"])  # land, dprst, on-stream
_CATEGORY_LEGEND = [
    mpatches.Patch(color="#f0f0f0", label="land"),
    mpatches.Patch(color="#3182bd", label="depression storage (dprst)"),
    mpatches.Patch(color="#e6550d", label="on-stream waterbody"),
]


def fig_before_after(region_name: str, bbox, out_name: str, change_note: str) -> Path:
    """Draw a 2-panel (pre-fix | current) dprst/on-stream classification map."""
    before = _dprst_onstream_category(PRE_FIX_DIR / "dprst_binary.tif", PRE_FIX_DIR / "onstream_binary.tif", bbox)
    after = _dprst_onstream_category(CURRENT_DIR / "dprst_binary.tif", CURRENT_DIR / "onstream_binary.tif", bbox)

    fig, axes = plt.subplots(1, 2, figsize=(11, 6))
    for ax, arr, title in zip(axes, (before, after), ("before", "after")):
        ax.imshow(arr, cmap=_CATEGORY_CMAP, vmin=0, vmax=2, interpolation="nearest")
        ax.set_title(title)
        ax.set_xticks([])
        ax.set_yticks([])
    fig.legend(handles=_CATEGORY_LEGEND, loc="lower center", ncol=3, frameon=False)
    fig.suptitle(f"{region_name}: dprst / on-stream classification\n{change_note}")
    fig.tight_layout(rect=(0, 0.06, 1, 0.94))

    out_path = OUT / out_name
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_decision_schematic() -> Path:
    """Legacy 60 m segment-buffer split vs. NHD-network-connectivity split."""
    fig, axes = plt.subplots(1, 2, figsize=(11, 5.5))

    # -- Left panel: legacy 60 m stream-segment buffer --
    ax = axes[0]
    ax.set_title("Legacy: 60 m segment buffer")
    stream = mpatches.FancyArrow(0.05, 0.5, 0.9, 0.0, width=0.01, color="#3182bd", length_includes_head=True)
    ax.add_patch(stream)
    buffer_band = mpatches.Rectangle((0.05, 0.42), 0.9, 0.16, color="#9ecae1", alpha=0.6, label="60 m buffer")
    ax.add_patch(buffer_band)
    wb_in = mpatches.Circle((0.3, 0.5), 0.09, color="#e6550d", label="waterbody touching buffer\n→ on-stream")
    wb_out = mpatches.Circle((0.7, 0.75), 0.09, color="#31a354", label="waterbody outside buffer\n→ dprst")
    ax.add_patch(wb_in)
    ax.add_patch(wb_out)
    ax.text(0.3, 0.5, "on-\nstream", ha="center", va="center", fontsize=8, color="white")
    ax.text(0.7, 0.75, "dprst", ha="center", va="center", fontsize=8, color="white")
    ax.text(
        0.5,
        0.15,
        "Geometric distance only: any waterbody\nwithin 60 m of a stream segment is on-stream\n"
        "(mislabels endorheic lakes an NHD artificial\npath happens to pass through)",
        ha="center",
        va="center",
        fontsize=8,
        wrap=True,
    )
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xticks([])
    ax.set_yticks([])

    # -- Right panel: NHD network-connectivity split --
    ax = axes[1]
    ax.set_title("Current: NHD network connectivity")
    net = mpatches.FancyArrow(0.05, 0.5, 0.9, 0.0, width=0.01, color="#3182bd", length_includes_head=True)
    ax.add_patch(net)
    ax.text(0.5, 0.58, "Network-Flowline (topology-gated)", ha="center", fontsize=7)
    wb_endorheic = mpatches.Circle((0.3, 0.5), 0.09, color="#31a354")
    wb_flowthrough = mpatches.Circle((0.7, 0.5), 0.09, color="#e6550d")
    ax.add_patch(wb_endorheic)
    ax.add_patch(wb_flowthrough)
    ax.text(0.3, 0.5, "dprst", ha="center", va="center", fontsize=8, color="white")
    ax.text(0.7, 0.5, "on-\nstream", ha="center", va="center", fontsize=8, color="white")
    ax.text(0.3, 0.28, "inflow only /\nno outflow\n(endorheic sink)", ha="center", fontsize=7)
    ax.text(0.7, 0.28, "WBAREACOMI or\nflow-through:\ninflow + outflow", ha="center", fontsize=7)
    ax.text(
        0.5,
        0.08,
        "Both COMID sources gate on Network-Flowline membership;\n"
        "Playa forced dprst, Ice Mass excluded upstream (issue #161)",
        ha="center",
        va="center",
        fontsize=8,
        wrap=True,
    )
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xticks([])
    ax.set_yticks([])

    fig.suptitle("dprst vs. on-stream decision: legacy buffer vs. network-connectivity split")
    fig.tight_layout(rect=(0, 0, 1, 0.94))

    out_path = OUT / "decision_schematic.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def fig_pipeline_dag() -> Path:
    """The depstor builder DAG: inputs through PRMS params."""
    # (label, x, y, half_width)
    nodes = {
        "nhd": ("NHD\n(waterbodies, flowlines)", 0.08, 0.82, 0.075),
        "fdr": ("FDR\n(fdr.vrt)", 0.08, 0.58, 0.075),
        "twi": ("TWI", 0.08, 0.34, 0.075),
        "lulc": ("LULC\n(NLCD)", 0.08, 0.10, 0.075),
        "waterbody": ("waterbody", 0.27, 0.82, 0.075),
        "wbody_conn": ("wbody_\nconnectivity", 0.46, 0.70, 0.075),
        "dprst": ("dprst", 0.65, 0.55, 0.07),
        "routing": ("routing", 0.65, 0.85, 0.07),
        "drains": ("drains_perv /\ndrains_imperv", 0.84, 0.70, 0.08),
        "params": ("PRMS params\n(sro_to_dprst_*)", 0.965, 0.70, 0.075),
    }
    edges = [
        ("nhd", "waterbody"),
        ("fdr", "waterbody"),
        ("nhd", "wbody_conn"),
        ("waterbody", "wbody_conn"),
        ("wbody_conn", "dprst"),
        ("lulc", "dprst"),
        ("fdr", "routing"),
        ("dprst", "routing"),
        ("twi", "routing"),
        ("routing", "drains"),
        ("lulc", "drains"),
        ("drains", "params"),
    ]

    fig, ax = plt.subplots(figsize=(13, 5.5))
    for key, (label, x, y, hw) in nodes.items():
        is_input = key in ("nhd", "fdr", "twi", "lulc")
        is_output = key == "params"
        color = "#deebf7" if is_input else ("#31a354" if is_output else "#9ecae1")
        box = mpatches.FancyBboxPatch(
            (x - hw, y - 0.065),
            2 * hw,
            0.13,
            boxstyle="round,pad=0.01",
            facecolor=color,
            edgecolor="black",
        )
        ax.add_patch(box)
        ax.text(x, y, label, ha="center", va="center", fontsize=8)

    for src, dst in edges:
        x0, y0, hw0 = nodes[src][1], nodes[src][2], nodes[src][3]
        x1, y1, hw1 = nodes[dst][1], nodes[dst][2], nodes[dst][3]
        # Clip the arrow to just outside each node's box, capped so the two
        # ends can never cross (which would silently invert the arrowhead).
        gap = min(hw0 + 0.015, hw1 + 0.015, abs(x1 - x0) * 0.4)
        dx = gap if x1 >= x0 else -gap
        ax.annotate(
            "",
            xy=(x1 - dx, y1),
            xytext=(x0 + dx, y0),
            arrowprops=dict(arrowstyle="->", color="#555555", lw=1.2, connectionstyle="arc3,rad=0.08"),
        )

    ax.set_xlim(-0.03, 1.05)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("Depression-storage builder DAG")
    fig.tight_layout()

    out_path = OUT / "pipeline_dag.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    written = [
        fig_decision_schematic(),
        fig_pipeline_dag(),
        fig_before_after(
            "Great Basin (VPU 16)",
            GREAT_BASIN,
            "great_basin_before_after.png",
            "endorheic waterbodies correctly retained as depression storage (issue #161)",
        ),
        fig_before_after(
            "Lower Mississippi (VPU 08)",
            LOWER_MISS,
            "lower_miss_before_after.png",
            "drains_to_dprst over-extension into humid open-drainage terrain removed (#145)",
        ),
    ]
    for p in written:
        print(p)


if __name__ == "__main__":
    main()
