import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
        # Border DEM Fill — Visual QA

        Validates the Copernicus GLO-30 border fill for Canada/Mexico HRUs.
        Checks elevation continuity, slope/aspect seamlessness, and border
        HRU parameter values.

        **Best practice:** uses rasterio's `out_shape` decimated read — only a
        thumbnail-resolution array is decompressed; if internal overviews exist,
        GDAL selects the best one automatically.
        """
    )
    return


@app.cell
def _():
    from pathlib import Path

    import geopandas as gpd
    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.windows import from_bounds

    import marimo as mo

    DATA_ROOT = Path(
        "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2"
    )
    NHD_MERGED = DATA_ROOT / "work" / "nhd_merged"
    FABRIC_DIR = DATA_ROOT / "gfv2" / "fabric"
    DISPLAY_PX = 2000

    return (
        DATA_ROOT, DISPLAY_PX, FABRIC_DIR, NHD_MERGED,
        Path, gpd, mo, np, plt, rasterio, Resampling, from_bounds,
    )


@app.cell
def _(mo):
    mo.md("## Region and layer selection")
    return


@app.cell
def _(mo):
    # Bounding boxes in EPSG:5070 (xmin, ymin, xmax, ymax)
    REGIONS = {
        "Canada-East (VPU 01/02/04)": (1_500_000, 2_500_000, 2_800_000, 3_300_000),
        "Canada-West (VPU 17)": (-2_200_000, 2_500_000, -1_400_000, 3_200_000),
        "Mexico (VPU 12/13/15)": (-1_500_000, 200_000, 500_000, 1_200_000),
    }
    region_dropdown = mo.ui.dropdown(
        options=list(REGIONS.keys()),
        value="Canada-East (VPU 01/02/04)",
        label="Border region",
    )
    layer_dropdown = mo.ui.dropdown(
        options=["elevation", "slope", "aspect"],
        value="elevation",
        label="Raster layer",
    )
    mo.hstack([region_dropdown, layer_dropdown])
    return REGIONS, layer_dropdown, region_dropdown


@app.cell
def _(mo):
    mo.md("## Helper functions")
    return


@app.cell
def _(DISPLAY_PX, np, rasterio, Resampling, from_bounds):
    def raster_meta(path):
        with rasterio.open(path) as src:
            return {
                "shape": src.shape,
                "dtype": src.dtypes[0],
                "crs": src.crs,
                "bounds": src.bounds,
                "res_m": abs(src.transform.a),
                "nodata": src.nodata,
                "overviews": src.overviews(1),
                "size_MB": round(path.stat().st_size / 1024**2)
                if path.stat().st_size > 0
                else 0,
            }

    def windowed_decimated_read(
        path, bounds, target_px=DISPLAY_PX, resampling=Resampling.average,
    ):
        """Read a windowed, decimated thumbnail from a large raster.

        Parameters
        ----------
        path : Path
            Raster file or VRT.
        bounds : tuple
            (xmin, ymin, xmax, ymax) in the raster's CRS.
        target_px : int
            Maximum dimension of the returned array.
        resampling : Resampling
            Resampling method for decimation.

        Returns
        -------
        (masked_array_2d, window_transform, meta_dict)
        """
        with rasterio.open(path) as src:
            window = from_bounds(*bounds, transform=src.transform)
            # Clamp window to raster extent
            window = window.intersection(
                rasterio.windows.Window(0, 0, src.width, src.height)
            )
            if window.width < 1 or window.height < 1:
                raise ValueError(
                    f"Bounds {bounds} do not overlap raster extent {src.bounds}"
                )
            factor = max(1, max(int(window.width), int(window.height)) // target_px)
            out_h = max(1, int(window.height) // factor)
            out_w = max(1, int(window.width) // factor)
            data = src.read(
                1,
                window=window,
                out_shape=(out_h, out_w),
                resampling=resampling,
            ).astype(np.float64)
            nodata = src.nodata
            win_transform = src.window_transform(window)
            scaled_transform = win_transform * win_transform.scale(
                window.width / out_w, window.height / out_h,
            )
        mask = ~np.isfinite(data)
        if nodata is not None:
            mask |= data == nodata
        return np.ma.array(data, mask=mask), scaled_transform, raster_meta(path)

    def percentile_stretch(arr, lo=2, hi=98):
        valid = (
            arr.compressed()
            if isinstance(arr, np.ma.MaskedArray)
            else arr[np.isfinite(arr)]
        )
        if valid.size == 0:
            return arr, 0.0, 1.0
        vmin, vmax = np.percentile(valid, [lo, hi])
        return np.clip(arr, vmin, vmax), vmin, vmax

    return percentile_stretch, raster_meta, windowed_decimated_read


@app.cell
def _(mo):
    mo.md("## Elevation continuity")
    return


@app.cell
def _(
    NHD_MERGED, REGIONS, layer_dropdown, np, percentile_stretch, plt,
    region_dropdown, windowed_decimated_read,
):
    _layer = layer_dropdown.value
    _region_name = region_dropdown.value
    _bounds = REGIONS[_region_name]
    _vrt_path = NHD_MERGED / f"{_layer}.vrt"

    _cmaps = {"elevation": "terrain", "slope": "YlOrRd", "aspect": "hsv"}
    _units = {"elevation": "m", "slope": "degrees", "aspect": "degrees"}

    if _vrt_path.exists():
        _data, _, _meta = windowed_decimated_read(_vrt_path, _bounds)
        _stretched, _vmin, _vmax = percentile_stretch(_data)
        _valid = _data.compressed()

        _fig, (_ax_img, _ax_hist) = plt.subplots(
            1, 2, figsize=(16, 6), gridspec_kw={"width_ratios": [3, 1]},
        )
        _im = _ax_img.imshow(
            _stretched, cmap=_cmaps[_layer], vmin=_vmin, vmax=_vmax,
            interpolation="nearest", rasterized=True,
        )
        _ax_img.set_title(
            f"{_layer.title()} — {_region_name}\n{_vrt_path.name}", fontsize=11,
        )
        _ax_img.axis("off")
        plt.colorbar(
            _im, ax=_ax_img, fraction=0.03, pad=0.02, label=_units[_layer],
        )

        _ax_hist.hist(
            _valid, bins=100, color="steelblue", edgecolor="none", density=True,
        )
        _ax_hist.set_title("Value distribution")
        _ax_hist.set_xlabel(_units[_layer])
        _ax_hist.set_ylabel("density")

        _h, _w = _meta["shape"]
        _info = (
            f"Full grid : {_h:,} x {_w:,} px\n"
            f"Pixel size: {_meta['res_m']:.1f} m\n"
            f"NoData    : {_meta['nodata']}\n"
            f"Valid px  : {_valid.size:,}\n"
            f"Min       : {_valid.min():.4g}\n"
            f"Mean      : {_valid.mean():.4g}\n"
            f"Max       : {_valid.max():.4g}"
        )
        _ax_img.text(
            1.01, 0.5, _info, transform=_ax_img.transAxes, fontsize=8,
            verticalalignment="center", family="monospace",
            bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8),
        )
        plt.tight_layout()
        _fig
    else:
        print(f"VRT not found: {_vrt_path}")


@app.cell
def _(mo):
    mo.md("## Slope and aspect side-by-side")
    return


@app.cell
def _(
    NHD_MERGED, REGIONS, np, percentile_stretch, plt,
    region_dropdown, windowed_decimated_read,
):
    _region_name = region_dropdown.value
    _bounds = REGIONS[_region_name]
    _layers = [
        ("slope", "YlOrRd", "degrees"),
        ("aspect", "hsv", "degrees"),
    ]
    _available = [
        (name, cmap, units)
        for name, cmap, units in _layers
        if (NHD_MERGED / f"{name}.vrt").exists()
    ]
    if _available:
        _fig, _axes = plt.subplots(
            1, len(_available), figsize=(8 * len(_available), 6),
        )
        if len(_available) == 1:
            _axes = [_axes]
        for _ax, (_name, _cmap, _units) in zip(_axes, _available):
            _data, _, _ = windowed_decimated_read(
                NHD_MERGED / f"{_name}.vrt", _bounds,
            )
            _stretched, _vmin, _vmax = percentile_stretch(_data)
            _im = _ax.imshow(
                _stretched, cmap=_cmap, vmin=_vmin, vmax=_vmax,
                interpolation="nearest", rasterized=True,
            )
            _ax.set_title(f"{_name.title()} — {_region_name}")
            _ax.axis("off")
            plt.colorbar(_im, ax=_ax, fraction=0.03, pad=0.02, label=_units)
        plt.suptitle("Slope/Aspect — check for seam artifacts", fontsize=13)
        plt.tight_layout()
        _fig
    else:
        print("No slope/aspect VRTs found yet.")


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Border HRU parameter check

        Load the merged fabric, identify HRUs that extend beyond typical CONUS
        extent, and check whether they have valid elevation/slope/aspect values.
        """
    )
    return


@app.cell
def _(FABRIC_DIR, gpd, mo, np, plt):
    _merged_gpkg = FABRIC_DIR / "gfv2_nhru_merged.gpkg"
    if _merged_gpkg.exists():
        _gdf = gpd.read_file(_merged_gpkg, layer="nhru")
        _gdf["centroid_y"] = _gdf.geometry.centroid.y

        # Border HRUs: centroid above 2,700,000 m (approx US-Canada border in EPSG:5070)
        # or below 500,000 m (approx US-Mexico border in EPSG:5070)
        _canada_mask = _gdf["centroid_y"] > 2_700_000
        _mexico_mask = _gdf["centroid_y"] < 500_000
        _border_mask = _canada_mask | _mexico_mask

        _n_canada = _canada_mask.sum()
        _n_mexico = _mexico_mask.sum()
        _n_total = len(_gdf)

        mo.md(
            f"**Fabric:** {_n_total:,} total HRUs | "
            f"**Canada border:** {_n_canada:,} | "
            f"**Mexico border:** {_n_mexico:,}"
        )

        if _border_mask.any():
            _fig, _ax = plt.subplots(1, 1, figsize=(12, 8))
            _gdf[~_border_mask].plot(
                ax=_ax, color="lightgray", edgecolor="none", alpha=0.3,
            )
            _gdf[_canada_mask].plot(
                ax=_ax, color="steelblue", edgecolor="none", alpha=0.6,
                label=f"Canada border ({_n_canada:,})",
            )
            _gdf[_mexico_mask].plot(
                ax=_ax, color="coral", edgecolor="none", alpha=0.6,
                label=f"Mexico border ({_n_mexico:,})",
            )
            _ax.legend()
            _ax.set_title("Border HRUs identified by centroid latitude")
            _ax.axis("off")
            plt.tight_layout()
            _fig
    else:
        print(f"Merged fabric not found: {_merged_gpkg}")


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Elevation difference: NHDPlus vs Copernicus

        In the overlap zone, compute NHDPlus minus Copernicus elevation.
        A diverging colormap highlights systematic offsets.
        """
    )
    return


@app.cell
def _(
    NHD_MERGED, REGIONS, np, percentile_stretch, plt,
    region_dropdown, windowed_decimated_read,
):
    _region_name = region_dropdown.value
    _bounds = REGIONS[_region_name]
    _elev_vrt = NHD_MERGED / "elevation.vrt"
    _cop_elev = NHD_MERGED / "copernicus_fill" / "NEDSnapshot_merged_fixed_copernicus.tif"

    if _elev_vrt.exists() and _cop_elev.exists():
        _nhd_data, _, _ = windowed_decimated_read(_elev_vrt, _bounds)
        _cop_data, _, _ = windowed_decimated_read(_cop_elev, _bounds)

        # Compute difference only where both have valid data
        _both_valid = ~_nhd_data.mask & ~_cop_data.mask
        _diff = np.ma.array(
            np.where(_both_valid, _nhd_data.data - _cop_data.data, 0.0),
            mask=~_both_valid,
        )

        if _diff.count() > 0:
            _valid = _diff.compressed()
            _vmax = max(abs(np.percentile(_valid, 2)), abs(np.percentile(_valid, 98)))

            _fig, (_ax_img, _ax_hist) = plt.subplots(
                1, 2, figsize=(16, 6), gridspec_kw={"width_ratios": [3, 1]},
            )
            _im = _ax_img.imshow(
                _diff, cmap="RdBu", vmin=-_vmax, vmax=_vmax,
                interpolation="nearest", rasterized=True,
            )
            _ax_img.set_title(
                f"Elevation difference (NHDPlus - Copernicus)\n{_region_name}",
                fontsize=11,
            )
            _ax_img.axis("off")
            plt.colorbar(_im, ax=_ax_img, fraction=0.03, pad=0.02, label="m")

            _ax_hist.hist(
                _valid, bins=100, color="steelblue", edgecolor="none", density=True,
            )
            _ax_hist.axvline(0, color="red", linestyle="--", linewidth=1)
            _ax_hist.set_title("Difference distribution")
            _ax_hist.set_xlabel("m (NHDPlus - Copernicus)")
            _ax_hist.set_ylabel("density")

            _info = (
                f"Overlap px: {_valid.size:,}\n"
                f"Mean diff : {_valid.mean():.3f} m\n"
                f"Std diff  : {_valid.std():.3f} m\n"
                f"Max |diff|: {np.abs(_valid).max():.3f} m"
            )
            _ax_img.text(
                1.01, 0.5, _info, transform=_ax_img.transAxes, fontsize=9,
                verticalalignment="center", family="monospace",
                bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8),
            )
            plt.tight_layout()
            _fig
        else:
            print("No overlapping valid pixels found in this region.")
    else:
        _missing = []
        if not _elev_vrt.exists():
            _missing.append(str(_elev_vrt))
        if not _cop_elev.exists():
            _missing.append(str(_cop_elev))
        print(f"Missing rasters: {', '.join(_missing)}")


if __name__ == "__main__":
    app.run()
