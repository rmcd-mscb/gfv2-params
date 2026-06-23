"""Shared, tested helpers for the fabric results-viewer notebooks.

The notebooks under ``notebooks/`` import this module for everything that is
worth unit-testing: grid-snapping, decimated/windowed raster reads, raster
thumbnails + parameter choropleths, config-driven raster/param inventories, and
a save-figures workflow. Keeping that logic here (rather than inline in each
notebook) lets CI exercise it on synthetic data.

Conventions mirrored from the rest of the repo:
- ``snap_bounds_to_grid`` / ``whole_cell_offset`` are the canonical grid-snap
  helpers (``scripts/clip_shared_to_fabric.py`` imports them from here).
- ``read_overview`` follows the decimated ``out_shape`` pattern originally
  prototyped in ``notebooks/_archive/check_vrts.ipynb``; ``clip_overview``
  follows the windowed ``rasterio.windows.from_bounds(..., boundless=True)``
  pattern in ``src/gfv2_params/depstor.py:read_land_mask_for_grid``.
- ``map_continuous`` / ``map_categorical`` follow the per-HRU choropleth
  pattern originally prototyped in ``notebooks/_archive/check_params.ipynb``
  but **return** the Figure so the notebook can save it.
- Inventory builders read paths from a resolved config (``cfg["data_root"]`` /
  ``cfg.get(...)`` / fabric-profile keys) — never hardcoded — and skip missing
  entries with a warning so the viewer is best-effort (intentionally lenient,
  not fail-loud).
"""

from __future__ import annotations

import math
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from matplotlib.patches import Patch
from rasterio.enums import Resampling
from rasterio.windows import from_bounds as window_from_bounds

Kind = Literal["continuous", "categorical"]

# ----------------------------------------------------------------------------- #
# Grid snapping (verbatim from scripts/clip_shared_to_fabric.py, made public)
# ----------------------------------------------------------------------------- #


def snap_bounds_to_grid(bounds, transform, buffer_cells: int = 8):
    """Expand HRU bounds by `buffer_cells` then snap OUTWARD to the source grid.

    Returns (ulx, uly, lrx, lry) on the source pixel lattice, guaranteed to
    fully contain the (buffered) input bounds. Works for a north-up raster
    (transform.a > 0, transform.e < 0).
    """
    minx, miny, maxx, maxy = bounds
    px = transform.a          # +cellsize
    py = transform.e          # -cellsize
    ox = transform.c          # left edge of column 0
    oy = transform.f          # top edge of row 0
    # The floor/ceil snapping below is only correct for an axis-aligned, north-up
    # raster. Fail loudly on a rotated/flipped source rather than emitting a
    # silently-wrong window.
    if transform.b != 0 or transform.d != 0 or px <= 0 or py >= 0:
        raise ValueError(
            f"Source raster is not axis-aligned north-up (a={px}, e={py}, "
            f"b={transform.b}, d={transform.d}); snap_bounds_to_grid assumes it."
        )
    cell_x = abs(px)
    cell_y = abs(py)

    minx -= buffer_cells * cell_x
    maxx += buffer_cells * cell_x
    miny -= buffer_cells * cell_y
    maxy += buffer_cells * cell_y

    c_left = math.floor((minx - ox) / cell_x)
    c_right = math.ceil((maxx - ox) / cell_x)
    r_top = math.floor((oy - maxy) / cell_y)
    r_bot = math.ceil((oy - miny) / cell_y)

    ulx = ox + c_left * cell_x
    lrx = ox + c_right * cell_x
    uly = oy - r_top * cell_y
    lry = oy - r_bot * cell_y
    return ulx, uly, lrx, lry


def whole_cell_offset(t_a, ref_transform):
    """Fractional-cell offset of transform origin vs a reference lattice."""
    col = (t_a.c - ref_transform.c) / ref_transform.a
    row = (t_a.f - ref_transform.f) / ref_transform.e
    return col - round(col), row - round(row)


# ----------------------------------------------------------------------------- #
# Fabric bounds
# ----------------------------------------------------------------------------- #


def fabric_bounds(hru_gpkg, hru_layer, dst_crs=None):
    """Total bounds of an HRU fabric (minx, miny, maxx, maxy).

    Drops null/empty geometries (mirrors clip_shared_to_fabric.py) and
    optionally reprojects to `dst_crs` (e.g. a raster's CRS) before computing
    the bounds.
    """
    import geopandas as gpd

    hru = gpd.read_file(hru_gpkg, layer=hru_layer)
    hru = hru[hru.geometry.notna() & ~hru.geometry.is_empty]
    if len(hru) == 0:
        raise ValueError(
            f"HRU layer '{hru_layer}' in {hru_gpkg} has no valid geometries."
        )
    if dst_crs is not None and hru.crs != dst_crs:
        hru = hru.to_crs(dst_crs)
    return tuple(hru.total_bounds)


# ----------------------------------------------------------------------------- #
# Raster reads
# ----------------------------------------------------------------------------- #


def read_overview(path, target_px: int = 800) -> np.ma.MaskedArray:
    """Return a masked 2-D float32 thumbnail of the full raster at `path`.

    Decimated read via rasterio `out_shape`, so the full raster is never loaded.
    nodata and non-finite values are masked.
    """
    with rasterio.open(path) as src:
        factor = max(1, max(src.width, src.height) // target_px)
        out_h = max(1, src.height // factor)
        out_w = max(1, src.width // factor)
        data = src.read(
            1,
            out_shape=(out_h, out_w),
            resampling=Resampling.nearest,
        ).astype(np.float32)
        nd = src.nodata
    mask = ~np.isfinite(data)
    if nd is not None:
        mask |= data == nd
    return np.ma.array(data, mask=mask)


def clip_overview(path, bounds, target_px: int = 800):
    """Windowed, decimated read of `path` over `bounds` (in the raster's CRS).

    Returns ``(masked_array, extent)`` where ``extent = (minx, maxx, miny, maxy)``
    suitable for ``ax.imshow(arr, extent=extent)``. The window is read
    `boundless=True` so a request partly outside the raster is padded with
    nodata rather than clipped. nodata / non-finite values are masked.
    """
    with rasterio.open(path) as src:
        window = window_from_bounds(*bounds, transform=src.transform)
        win_w = max(1, int(round(window.width)))
        win_h = max(1, int(round(window.height)))
        factor = max(1, max(win_w, win_h) // target_px)
        out_h = max(1, win_h // factor)
        out_w = max(1, win_w // factor)
        data = src.read(
            1,
            window=window,
            out_shape=(out_h, out_w),
            resampling=Resampling.nearest,
            boundless=True,
            fill_value=src.nodata if src.nodata is not None else 0,
        ).astype(np.float32)
        nd = src.nodata
        # extent is the window's true bounds on the raster lattice
        win_transform = src.window_transform(window)
        minx = win_transform.c
        maxy = win_transform.f
        maxx = minx + win_w * src.transform.a
        miny = maxy + win_h * src.transform.e  # transform.e is negative

    mask = ~np.isfinite(data)
    if nd is not None:
        mask |= data == nd
    arr = np.ma.array(data, mask=mask)
    extent = (minx, maxx, miny, maxy)
    return arr, extent


# ----------------------------------------------------------------------------- #
# Raster thumbnail plotting
# ----------------------------------------------------------------------------- #


def _format_category(value):
    """Render a category value as a compact label (1.0 -> '1')."""
    f = float(value)
    return str(int(f)) if f.is_integer() else f"{f:g}"


def plot_raster(ax, arr, extent=None, *, cmap="viridis", pct=(2, 98),
                categorical=False, title=None, label="", max_legend=20):
    """Render a masked raster thumbnail onto `ax`.

    Continuous: percentile-stretch (`pct`) + colorbar. Categorical: discrete
    classes with a per-class **legend** when there are <= `max_legend` distinct
    classes (so soils/LULC/FDR codes are labelled), else a discrete colorbar
    (e.g. region-label rasters with many values). Masked/nodata cells render as
    light grey so gaps (ocean, off-fabric) read as intentional. Returns the
    matplotlib image.
    """
    imshow_kwargs = {"interpolation": "nearest", "rasterized": True}
    if extent is not None:
        imshow_kwargs["extent"] = extent

    valid = np.ma.compressed(arr)
    if categorical:
        cats = np.unique(valid)
        if len(cats) == 0:
            cats = np.array([0])
        listed = mcolors.ListedColormap(
            plt.cm.tab20(np.linspace(0, 1, max(len(cats), 1)))
        )
        listed.set_bad("lightgrey")
        bounds = list(cats - 0.5) + [cats[-1] + 0.5]
        norm = mcolors.BoundaryNorm(bounds, listed.N)
        im = ax.imshow(arr, cmap=listed, norm=norm, **imshow_kwargs)
        if len(cats) <= max_legend:
            handles = [Patch(color=listed(norm(cat)), label=_format_category(cat))
                       for cat in cats]
            ax.legend(handles=handles, title=label or None, loc="center left",
                      bbox_to_anchor=(1.0, 0.5), fontsize=7, frameon=False,
                      title_fontsize=8)
        else:
            ax.figure.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label=label)
    else:
        cmap_obj = plt.get_cmap(cmap).copy()
        cmap_obj.set_bad("lightgrey")
        if len(valid):
            vmin, vmax = np.percentile(valid, pct)
        else:
            vmin, vmax = 0, 1
        im = ax.imshow(arr, cmap=cmap_obj, vmin=vmin, vmax=vmax, **imshow_kwargs)
        ax.figure.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label=label)

    if title:
        ax.set_title(title, fontsize=10)
    ax.axis("off")
    return im


# ----------------------------------------------------------------------------- #
# Param load + choropleths
# ----------------------------------------------------------------------------- #


def load_param(params_dir, csv_name, fabric_gdf, id_feature):
    """Read a merged param CSV and left-merge it onto the fabric geometry.

    Left join preserves every HRU (missing HRUs in the CSV become NaN). Returns
    a GeoDataFrame indexed like `fabric_gdf`.
    """
    df = pd.read_csv(Path(params_dir) / csv_name)
    gdf = fabric_gdf[[id_feature, "geometry"]].merge(df, on=id_feature, how="left")
    return gdf


def map_continuous(gdf, col, title, *, units="", cmap="viridis",
                   pct_lo=2, pct_hi=98):
    """Choropleth + side histogram for a continuous parameter. Returns the Figure."""
    vals = gdf[col].dropna()
    if len(vals):
        vmin, vmax = np.percentile(vals, [pct_lo, pct_hi])
    else:
        vmin, vmax = 0, 1

    fig, (ax_map, ax_hist) = plt.subplots(
        1, 2, figsize=(14, 7), gridspec_kw={"width_ratios": [3, 1]}
    )
    gdf.plot(column=col, cmap=cmap, vmin=vmin, vmax=vmax,
             linewidth=0, ax=ax_map, legend=True,
             legend_kwds={"label": units, "shrink": 0.6, "pad": 0.01},
             missing_kwds={"color": "lightgrey", "label": "No data"})
    ax_map.set_title(title, fontsize=11)
    ax_map.axis("off")

    if len(vals):
        ax_hist.hist(vals, bins=100, color="steelblue", edgecolor="none", density=True)
        p25, p50, p75 = np.percentile(vals, [25, 50, 75])
        ax_hist.axvline(p25, color="cornflowerblue", ls="--", lw=1.2, label=f"p25 = {p25:.3g}")
        ax_hist.axvline(p50, color="navy", ls="-", lw=1.8, label=f"p50 = {p50:.3g}")
        ax_hist.axvline(p75, color="cornflowerblue", ls="--", lw=1.2, label=f"p75 = {p75:.3g}")
        ax_hist.axvline(vmin, color="orange", ls=":", lw=1.2, label=f"stretch lo = {vmin:.3g}")
        ax_hist.axvline(vmax, color="red", ls=":", lw=1.2, label=f"stretch hi = {vmax:.3g}")
        ax_hist.set_title(
            f"n = {len(vals):,}  mean = {vals.mean():.4g}\nstd = {vals.std():.4g}",
            fontsize=8, loc="left", family="monospace",
        )
        ax_hist.legend(fontsize=8)
    ax_hist.set_xlabel(units or col)
    ax_hist.set_ylabel("density")
    fig.tight_layout()
    return fig


def map_categorical(gdf, col, title, *, labels=None):
    """Categorical choropleth + legend. Returns the Figure."""
    cats = sorted(gdf[col].dropna().unique())
    if not cats:
        cats = [0]
    cmap_base = plt.get_cmap("tab20", max(len(cats), 1))
    colors = [cmap_base(i) for i in range(len(cats))]
    cmap = mcolors.ListedColormap(colors)
    norm = mcolors.BoundaryNorm([c - 0.5 for c in cats] + [cats[-1] + 0.5], len(cats))

    fig, (ax_map, ax_leg) = plt.subplots(
        1, 2, figsize=(14, 7), gridspec_kw={"width_ratios": [4, 1]}
    )
    gdf.plot(column=col, cmap=cmap, norm=norm,
             linewidth=0, ax=ax_map,
             missing_kwds={"color": "lightgrey"})
    ax_map.set_title(title, fontsize=11)
    ax_map.axis("off")

    counts = gdf[col].value_counts().sort_index()
    total = counts.sum() or 1
    patches = []
    for cat, color in zip(cats, colors):
        lbl = labels.get(cat, str(cat)) if labels else str(cat)
        n = counts.get(cat, 0)
        patches.append(Patch(color=color, label=f"{lbl}\n({n:,}, {100 * n / total:.1f}%)"))
    ax_leg.legend(handles=patches, loc="center", frameon=False, fontsize=9)
    ax_leg.axis("off")
    fig.tight_layout()
    return fig


# ----------------------------------------------------------------------------- #
# Inventories
# ----------------------------------------------------------------------------- #


def _validate_kind(kind: str) -> None:
    if kind not in ("continuous", "categorical"):
        raise ValueError(
            f"kind must be 'continuous' or 'categorical', got {kind!r}"
        )


@dataclass(frozen=True)
class RasterEntry:
    name: str
    path: Path
    kind: Kind
    cmap: str = "viridis"
    units: str = ""

    def __post_init__(self) -> None:
        _validate_kind(self.kind)


@dataclass(frozen=True)
class ParamEntry:
    name: str
    csv_name: str
    column: str
    kind: Kind
    cmap: str = "viridis"
    units: str = ""

    def __post_init__(self) -> None:
        _validate_kind(self.kind)


def _existing_raster_entries(entries: list[RasterEntry]) -> list[RasterEntry]:
    """Drop entries whose path does not exist, warning for each."""
    kept = []
    for e in entries:
        if Path(e.path).exists():
            kept.append(e)
        else:
            warnings.warn(f"Skipping raster '{e.name}': path not found: {e.path}")
    return kept


def dedupe_raster_entries(entries: list[RasterEntry]) -> list[RasterEntry]:
    """Drop entries whose resolved path repeats (first occurrence wins, order kept).

    The input-raster view concatenates the shared and zonal-source inventories,
    which both list the DEM VRTs (elevation/slope/aspect) and — for single-VPU
    fabrics — the same fdr/template clip. Dedupe by path so each file is shown
    once.
    """
    seen: set[str] = set()
    out: list[RasterEntry] = []
    for e in entries:
        key = str(e.path)
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def shared_raster_inventory(cfg) -> list[RasterEntry]:
    """Fabric profile hydrology rasters + the shared DEM-derivative VRTs.

    Pulls twi_raster / fdr_raster / template_raster from the resolved fabric
    profile (when present) and adds the CONUS elevation/slope/aspect VRTs under
    ``{data_root}/shared/conus/vrt``. Missing files are skipped with a warning.
    """
    vrt_dir = Path(cfg["data_root"]) / "shared" / "conus" / "vrt"
    entries: list[RasterEntry] = []
    if cfg.get("twi_raster"):
        entries.append(RasterEntry("twi", Path(cfg["twi_raster"]), "continuous",
                                   cmap="viridis", units="unitless"))
    if cfg.get("fdr_raster"):
        entries.append(RasterEntry("fdr", Path(cfg["fdr_raster"]), "categorical",
                                   cmap="nipy_spectral", units="D8 code"))
    if cfg.get("template_raster"):
        entries.append(RasterEntry("template", Path(cfg["template_raster"]),
                                   "categorical", cmap="nipy_spectral", units="D8 code"))
    entries.append(RasterEntry("elevation", vrt_dir / "elevation.vrt", "continuous",
                               cmap="terrain", units="m"))
    entries.append(RasterEntry("slope", vrt_dir / "slope.vrt", "continuous",
                               cmap="YlOrRd", units="degrees"))
    entries.append(RasterEntry("aspect", vrt_dir / "aspect.vrt", "continuous",
                               cmap="twilight", units="degrees"))
    return _existing_raster_entries(entries)


# Heuristic: zonal sources that are class rasters render best as categorical.
_ZONAL_CATEGORICAL = {"soils", "lulc_nhm_v11", "lulc_nalcms", "lulc_nlcd", "lulc_foresce"}


def zonal_source_inventory(zonal_cfg) -> list[RasterEntry]:
    """One RasterEntry per `params:` item in zonal_params.yml with a source_raster.

    `zonal_cfg` is the result of ``load_config("configs/zonal/zonal_params.yml",
    fabric=...)``. Soils and LULC sources are categorical; the rest continuous.
    Missing files are skipped with a warning.

    Note: ``load_config`` resolves ``{data_root}``/``{fabric}`` only in top-level
    string values, not inside the nested ``params:`` list (the real orchestrator,
    ``scripts/derive_zonal_params.py``, resolves those per-param). So we resolve
    the same two placeholders here against the merged-in top-level keys.
    """
    data_root = str(zonal_cfg.get("data_root", ""))
    fabric = str(zonal_cfg.get("fabric", ""))
    entries: list[RasterEntry] = []
    for item in zonal_cfg.get("params", []):
        src = item.get("source_raster")
        if not src:
            continue
        src = src.replace("{data_root}", data_root).replace("{fabric}", fabric)
        name = item["name"]
        kind = "categorical" if name in _ZONAL_CATEGORICAL else "continuous"
        cmap = "tab20" if kind == "categorical" else "viridis"
        entries.append(RasterEntry(name, Path(src), kind, cmap=cmap))
    return _existing_raster_entries(entries)


# Fixed depstor binary raster outputs (configs/depstor/depstor_rasters.yml +
# depstor_builders/__init__.py). Binary masks / region labels -> categorical.
_DEPSTOR_RASTERS = [
    "land_mask.tif",
    "imperv_binary.tif",
    "perv_binary.tif",
    "dprst_binary.tif",
    "onstream_binary.tif",
    "connected_wbody.tif",
    "wbody_binary.tif",
    "wbody_regions.tif",
    "carea_map_t8_binary.tif",
    "carea_map_t156_binary.tif",
    "drains_imperv_binary.tif",
    "drains_perv_binary.tif",
    "drains_to_dprst.tif",
    "vpu_id.tif",
]


def depstor_raster_inventory(cfg) -> list[RasterEntry]:
    """The fixed 14 depstor binary rasters under {data_root}/{fabric}/depstor_rasters.

    All are discrete (binary masks / region or VPU labels), so kind is
    "categorical". Missing files are skipped with a warning.
    ``connected_wbody.tif`` replaced the retired ``stream_buffer.tif``.
    """
    base = Path(cfg["data_root"]) / cfg["fabric"] / "depstor_rasters"
    entries = [
        RasterEntry(fname.replace(".tif", ""), base / fname, "categorical",
                    cmap="tab20")
        for fname in _DEPSTOR_RASTERS
    ]
    return _existing_raster_entries(entries)


# Curated display metadata for the merged param CSVs (display-only; not pipeline
# logic). CSV names + columns confirmed against {fabric}/params/merged/.
# Keyword args throughout: RasterEntry/ParamEntry share a (kind, cmap, units)
# tail, so positional construction is an easy place to swap cmap/units.
_PARAM_ENTRIES = [
    ParamEntry(name="elevation", csv_name="nhm_elevation_params.csv", column="mean", kind="continuous", units="m", cmap="terrain"),
    ParamEntry(name="slope", csv_name="nhm_slope_params.csv", column="mean", kind="continuous", units="degrees", cmap="YlOrRd"),
    ParamEntry(name="aspect", csv_name="nhm_aspect_params.csv", column="mean", kind="continuous", units="degrees", cmap="twilight"),
    ParamEntry(name="soils", csv_name="nhm_soils_params.csv", column="soils", kind="categorical"),
    ParamEntry(name="soil_moist_max", csv_name="nhm_soil_moist_max_params.csv", column="soil_moist_max", kind="continuous", units="cm", cmap="Blues"),
    ParamEntry(name="cov_type", csv_name="nhm_lulc_nhm_v11_params.csv", column="cov_type", kind="categorical"),
    ParamEntry(name="covden_sum", csv_name="nhm_lulc_nhm_v11_params.csv", column="covden_sum", kind="continuous", units="fraction", cmap="Greens"),
    ParamEntry(name="covden_win", csv_name="nhm_lulc_nhm_v11_params.csv", column="covden_win", kind="continuous", units="fraction", cmap="YlGn"),
    # nhm_v11 LULC winter-canopy term: the crosswalk path (run_lulc_batch)
    # emits `retention`; the faithful lulc_prederived path (PR #135) emits
    # `rad_trncf` instead. Both are listed — only one column is present in any
    # given merged CSV, and the render loop skips whichever column is absent.
    ParamEntry(name="retention", csv_name="nhm_lulc_nhm_v11_params.csv", column="retention", kind="continuous", units="fraction", cmap="PuBuGn"),
    ParamEntry(name="rad_trncf", csv_name="nhm_lulc_nhm_v11_params.csv", column="rad_trncf", kind="continuous", units="fraction", cmap="cividis"),
    ParamEntry(name="snow_intcp", csv_name="nhm_lulc_nhm_v11_params.csv", column="snow_intcp", kind="continuous", units="in", cmap="PuBu"),
    ParamEntry(name="srain_intcp", csv_name="nhm_lulc_nhm_v11_params.csv", column="srain_intcp", kind="continuous", units="in", cmap="PuBu"),
    ParamEntry(name="wrain_intcp", csv_name="nhm_lulc_nhm_v11_params.csv", column="wrain_intcp", kind="continuous", units="in", cmap="PuBu"),
    ParamEntry(name="carea_max", csv_name="nhm_carea_max_params.csv", column="carea_max", kind="continuous", units="fraction", cmap="magma"),
    ParamEntry(name="smidx_coef", csv_name="nhm_smidx_coef_params.csv", column="smidx_coef", kind="continuous", units="fraction", cmap="magma"),
    ParamEntry(name="sro_to_dprst_perv", csv_name="nhm_sro_to_dprst_perv_params.csv", column="sro_to_dprst_perv", kind="continuous", units="fraction", cmap="cividis"),
    ParamEntry(name="sro_to_dprst_imperv", csv_name="nhm_sro_to_dprst_imperv_params.csv", column="sro_to_dprst_imperv", kind="continuous", units="fraction", cmap="cividis"),
    ParamEntry(name="hru_percent_imperv", csv_name="nhm_hru_percent_imperv_params.csv", column="hru_percent_imperv", kind="continuous", units="fraction", cmap="OrRd"),
    ParamEntry(name="dprst_frac", csv_name="nhm_dprst_frac_params.csv", column="dprst_frac", kind="continuous", units="fraction", cmap="GnBu"),
    # ssflux PRMS params (from the gap-filled Stage 7 CSV — PRMS-ready values).
    ParamEntry(name="soil2gw_max", csv_name="filled_nhm_ssflux_params.csv", column="soil2gw_max", kind="continuous", units="in/day", cmap="Blues"),
    ParamEntry(name="ssr2gw_rate", csv_name="filled_nhm_ssflux_params.csv", column="ssr2gw_rate", kind="continuous", units="1/day", cmap="BuPu"),
    ParamEntry(name="fastcoef_lin", csv_name="filled_nhm_ssflux_params.csv", column="fastcoef_lin", kind="continuous", units="1/day", cmap="YlOrRd"),
    ParamEntry(name="slowcoef_lin", csv_name="filled_nhm_ssflux_params.csv", column="slowcoef_lin", kind="continuous", units="1/day", cmap="YlGn"),
    ParamEntry(name="gwflow_coef", csv_name="filled_nhm_ssflux_params.csv", column="gwflow_coef", kind="continuous", units="1/day", cmap="Purples"),
    ParamEntry(name="dprst_seep_rate_open", csv_name="filled_nhm_ssflux_params.csv", column="dprst_seep_rate_open", kind="continuous", units="1/day", cmap="PuBu"),
    ParamEntry(name="dprst_flow_coef", csv_name="filled_nhm_ssflux_params.csv", column="dprst_flow_coef", kind="continuous", units="1/day", cmap="GnBu"),
]


def param_inventory() -> list[ParamEntry]:
    """Curated list of the merged param CSVs (display metadata, not pipeline logic).

    Returns the full list regardless of disk — existence is checked by the
    notebook/loader, not here.
    """
    return list(_PARAM_ENTRIES)


# ----------------------------------------------------------------------------- #
# Save-figures workflow (module-level state; mirrors notebooks/_helpers patterns)
# ----------------------------------------------------------------------------- #

SAVE_FIGURES: bool = False
FABRIC: str | None = None
FIGURES_DIR: Path = Path("docs/figures")


def save_figure(fig, name, *, dpi: int = 150) -> None:
    """Save `fig` to ``FIGURES_DIR[/FABRIC]/<name>.png`` when SAVE_FIGURES is set.

    No-op unless ``SAVE_FIGURES``. If saving is enabled but no ``FABRIC`` is set,
    warns (figures land in the un-namespaced base dir). A relative ``FIGURES_DIR``
    is resolved against the repo root.
    """
    if not SAVE_FIGURES:
        return
    if not FABRIC:
        warnings.warn("save_figure: SAVE_FIGURES is set but FABRIC is None; "
                      "figures will not be namespaced by fabric.")

    base = Path(FIGURES_DIR)
    if not base.is_absolute():
        repo_root = Path(__file__).resolve().parents[2]
        base = repo_root / base
    target = base / FABRIC if FABRIC else base
    target.mkdir(parents=True, exist_ok=True)
    fig.savefig(target / f"{name}.png", dpi=dpi, bbox_inches="tight")


# ----------------------------------------------------------------------------- #
# Interactive folium overlays for the depstor "output-binary" rasters
# ----------------------------------------------------------------------------- #

# Curated list of depstor binaries that directly feed a PRMS ratio CSV (i.e.
# appear as a numerator or denominator of one of the 6 ratios in
# configs/depstor/depstor_params.yml). Intentionally excludes intermediates
# (connected_wbody, wbody_binary, onstream_binary, drains_to_dprst) and the
# non-ratio rasters (land_mask, wbody_regions, vpu_id). Each entry carries a
# distinct hex color so layers stay visually distinguishable when stacked on
# an interactive map. Maintained alongside _DEPSTOR_RASTERS above.
_DEPSTOR_OUTPUT_BINARIES = [
    # (file_stem, color, what it feeds)
    ("perv_binary",          "#2ca02c", "denominator: carea_max, smidx_coef, sro_to_dprst_perv"),
    ("imperv_binary",        "#d62728", "hru_percent_imperv (num); sro_to_dprst_imperv (denom)"),
    ("dprst_binary",         "#1f77b4", "dprst_frac (numerator)"),
    ("drains_perv_binary",   "#ff7f0e", "sro_to_dprst_perv (numerator)"),
    ("drains_imperv_binary", "#9467bd", "sro_to_dprst_imperv (numerator)"),
    ("carea_map_t8_binary",  "#e377c2", "carea_max (numerator)"),
    ("carea_map_t156_binary","#17becf", "smidx_coef (numerator)"),
]


@dataclass(frozen=True)
class OverlayEntry:
    name: str
    path: Path
    color: str          # hex like "#2ca02c"; on-cells render as this color
    feeds: str          # human-readable description of the output param it feeds


def depstor_output_binary_inventory(cfg) -> list[OverlayEntry]:
    """Curated depstor binaries that directly feed a PRMS output ratio.

    Filters ``_DEPSTOR_OUTPUT_BINARIES`` against on-disk paths under
    ``{data_root}/{fabric}/depstor_rasters/``; missing files are skipped with
    a warning (mirrors the other inventory builders). Used by the interactive
    folium-overlay notebook.
    """
    base = Path(cfg["data_root"]) / cfg["fabric"] / "depstor_rasters"
    kept: list[OverlayEntry] = []
    for name, color, feeds in _DEPSTOR_OUTPUT_BINARIES:
        p = base / f"{name}.tif"
        if p.exists():
            kept.append(OverlayEntry(name=name, path=p, color=color, feeds=feeds))
        else:
            warnings.warn(f"Skipping overlay '{name}': path not found: {p}")
    return kept


def build_overlay_image(path, *, color: str, alpha: float = 0.55,
                        target_px: int = 1000, threshold: float = 0.5):
    """Reproject a binary-ish raster to EPSG:4326 and build an RGBA image.

    Returns ``(rgba_uint8, (west, south, east, north))`` where ``rgba_uint8``
    is an ``(H, W, 4)`` ``uint8`` ndarray. Cells where the raster value is
    finite, not nodata, and strictly greater than ``threshold`` render as
    ``color`` at ``alpha``; everything else is fully transparent.

    Decimated so the long side is roughly ``target_px``. No folium dep — the
    caller wraps this into ``folium.raster_layers.ImageOverlay``; see
    ``raster_to_image_overlay`` for the wrapped version.
    """
    from rasterio.vrt import WarpedVRT

    r, g, b = mcolors.to_rgb(color)
    rgba = (int(r * 255), int(g * 255), int(b * 255), int(alpha * 255))

    with rasterio.open(path) as src:
        nd = src.nodata
        with WarpedVRT(src, crs="EPSG:4326", resampling=Resampling.nearest) as vrt:
            factor = max(1, max(vrt.width, vrt.height) // target_px)
            oh = max(1, vrt.height // factor)
            ow = max(1, vrt.width // factor)
            data = vrt.read(1, out_shape=(oh, ow),
                            resampling=Resampling.nearest).astype("float32")
            bounds = vrt.bounds   # (left, bottom, right, top) in lat/lon

    on = np.isfinite(data)
    if nd is not None:
        on &= (data != nd)
    on &= (data > threshold)

    img = np.zeros((data.shape[0], data.shape[1], 4), dtype="uint8")
    img[on] = rgba
    return img, (bounds.left, bounds.bottom, bounds.right, bounds.top)


def raster_to_image_overlay(path, *, name: str, color: str,
                            alpha: float = 0.55, target_px: int = 1000,
                            threshold: float = 0.5):
    """Build a folium ImageOverlay ready to ``.add_to(map)`` + LayerControl.

    Thin wrapper around ``build_overlay_image`` that imports ``folium``
    lazily (folium is only in the ``notebooks`` pixi env). ``mercator_project``
    is enabled so the EPSG:4326 image is reprojected to Web Mercator
    client-side instead of being stretched.
    """
    import folium  # noqa: PLC0415 — optional notebook dep

    img, (w, s, e, n) = build_overlay_image(
        path, color=color, alpha=alpha, target_px=target_px, threshold=threshold,
    )
    return folium.raster_layers.ImageOverlay(
        image=img,
        bounds=[[s, w], [n, e]],
        opacity=1.0,            # alpha is baked into the RGBA
        mercator_project=True,  # client-side warp to web mercator
        name=name,
        interactive=False,
    )
