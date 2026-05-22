"""Shared, tested helpers for the fabric results-viewer notebooks.

The notebooks under ``notebooks/`` import this module for everything that is
worth unit-testing: grid-snapping, decimated/windowed raster reads, raster
thumbnails + parameter choropleths, config-driven raster/param inventories, and
a save-figures workflow. Keeping that logic here (rather than inline in each
notebook) lets CI exercise it on synthetic data.

Conventions mirrored from the rest of the repo:
- ``snap_bounds_to_grid`` / ``whole_cell_offset`` are lifted verbatim from
  ``scripts/clip_shared_to_fabric.py`` (made public here).
- ``read_overview`` follows the decimated ``out_shape`` pattern in
  ``notebooks/check_vrts.ipynb``; ``clip_overview`` follows the windowed
  ``rasterio.windows.from_bounds(..., boundless=True)`` pattern in
  ``src/gfv2_params/depstor.py:read_land_mask_for_grid``.
- ``map_continuous`` / ``map_categorical`` follow ``notebooks/check_params.ipynb``
  but **return** the Figure so the notebook can save it.
- Inventory builders read paths from a resolved config (``require_config_key`` /
  ``cfg["data_root"]``) — never hardcoded — and skip missing entries with a
  warning so the viewer is best-effort.
"""

from __future__ import annotations

import math
import warnings
from dataclasses import dataclass
from pathlib import Path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from matplotlib.patches import Patch
from rasterio.enums import Resampling
from rasterio.windows import from_bounds as window_from_bounds

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


def plot_raster(ax, arr, extent=None, *, cmap="viridis", pct=(2, 98),
                categorical=False, title=None, label=""):
    """Render a masked raster thumbnail onto `ax`.

    Continuous: percentile-stretch (`pct`) + colorbar. Categorical: discrete
    classes via a BoundaryNorm + colorbar. Returns the matplotlib image.
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
        bounds = list(cats - 0.5) + [cats[-1] + 0.5]
        norm = mcolors.BoundaryNorm(bounds, listed.N)
        im = ax.imshow(arr, cmap=listed, norm=norm, **imshow_kwargs)
    else:
        if len(valid):
            vmin, vmax = np.percentile(valid, pct)
        else:
            vmin, vmax = 0, 1
        im = ax.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax, **imshow_kwargs)

    if title:
        ax.set_title(title, fontsize=10)
    ax.axis("off")
    ax.figure.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label=label)
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
    cmap_base = plt.cm.get_cmap("tab20", max(len(cats), 1))
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


@dataclass(frozen=True)
class RasterEntry:
    name: str
    path: Path
    kind: str  # "continuous" | "categorical"
    cmap: str = "viridis"
    units: str = ""


@dataclass(frozen=True)
class ParamEntry:
    name: str
    csv_name: str
    column: str
    kind: str  # "continuous" | "categorical"
    units: str = ""
    cmap: str = "viridis"


def _existing_raster_entries(entries: list[RasterEntry]) -> list[RasterEntry]:
    """Drop entries whose path does not exist, warning for each."""
    kept = []
    for e in entries:
        if Path(e.path).exists():
            kept.append(e)
        else:
            warnings.warn(f"Skipping raster '{e.name}': path not found: {e.path}")
    return kept


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
    "stream_buffer.tif",
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
_PARAM_ENTRIES = [
    ParamEntry("elevation", "nhm_elevation_params.csv", "mean", "continuous", "m", "terrain"),
    ParamEntry("slope", "nhm_slope_params.csv", "mean", "continuous", "degrees", "YlOrRd"),
    ParamEntry("aspect", "nhm_aspect_params.csv", "mean", "continuous", "degrees", "twilight"),
    ParamEntry("soils", "nhm_soils_params.csv", "soils", "categorical"),
    ParamEntry("soil_moist_max", "nhm_soil_moist_max_params.csv", "soil_moist_max", "continuous", "cm", "Blues"),
    ParamEntry("cov_type", "nhm_lulc_nhm_v11_params.csv", "cov_type", "categorical"),
    ParamEntry("covden_sum", "nhm_lulc_nhm_v11_params.csv", "covden_sum", "continuous", "fraction", "Greens"),
    ParamEntry("covden_win", "nhm_lulc_nhm_v11_params.csv", "covden_win", "continuous", "fraction", "YlGn"),
    ParamEntry("retention", "nhm_lulc_nhm_v11_params.csv", "retention", "continuous", "fraction", "PuBuGn"),
    ParamEntry("snow_intcp", "nhm_lulc_nhm_v11_params.csv", "snow_intcp", "continuous", "in", "PuBu"),
    ParamEntry("carea_max", "nhm_carea_max_params.csv", "carea_max", "continuous", "fraction", "magma"),
    ParamEntry("smidx_coef", "nhm_smidx_coef_params.csv", "smidx_coef", "continuous", "fraction", "magma"),
    ParamEntry("sro_to_dprst_perv", "nhm_sro_to_dprst_perv_params.csv", "sro_to_dprst_perv", "continuous", "fraction", "cividis"),
    ParamEntry("sro_to_dprst_imperv", "nhm_sro_to_dprst_imperv_params.csv", "sro_to_dprst_imperv", "continuous", "fraction", "cividis"),
    ParamEntry("hru_percent_imperv", "nhm_hru_percent_imperv_params.csv", "hru_percent_imperv", "continuous", "fraction", "OrRd"),
    ParamEntry("dprst_frac", "nhm_dprst_frac_params.csv", "dprst_frac", "continuous", "fraction", "GnBu"),
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
