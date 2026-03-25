import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(r"""
    # Merge VPU nhru layers into a single GeoPackage

    Reads the `nhru` layer from each `NHM_<vpu>_draft.gpkg`, fixes invalid
    geometries with `make_valid()`, simplifies, then writes a single output
    GeoPackage.
    """)
    return


@app.cell
def _():
    from pathlib import Path

    import geopandas as gpd
    import pandas as pd
    import shapely
    import marimo as mo

    return Path, gpd, mo, pd, shapely


@app.cell
def _(mo):
    mo.md(r"""
    ## Configuration
    """)
    return


@app.cell
def _(Path):
    TARGETS_DIR = Path(
        "/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param/targets"
    )
    OUTPUT_PATH = TARGETS_DIR / "gfv2_nhru_merged.gpkg"

    VPUS = [
        "01", "02", "03N", "03S", "03W",
        "04", "05", "06", "07", "08", "09",
        "10L", "10U",
        "11", "12", "13", "14", "15", "16", "17", "18",
    ]

    # Simplification tolerance in map units (EPSG:5070 → metres).
    # Set to 0 or None to skip simplification.
    SIMPLIFY_TOLERANCE = 10.0
    PRESERVE_TOPOLOGY  = True
    return (
        OUTPUT_PATH,
        PRESERVE_TOPOLOGY,
        SIMPLIFY_TOLERANCE,
        TARGETS_DIR,
        VPUS,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Load, fix, and simplify nhru
    """)
    return


@app.cell
def _(
    PRESERVE_TOPOLOGY,
    SIMPLIFY_TOLERANCE,
    TARGETS_DIR,
    VPUS,
    gpd,
    pd,
    shapely,
):
    _gdfs = []

    for _vpu in VPUS:
        _path = TARGETS_DIR / f"NHM_{_vpu}_draft.gpkg"
        _gdf = gpd.read_file(_path, layer="nhru")
        _gdf["source_vpu"] = _path.stem

        # Fix invalid geometries
        _invalid = ~_gdf.geometry.is_valid
        if _invalid.any():
            _gdf.loc[_invalid, "geometry"] = shapely.make_valid(
                _gdf.loc[_invalid, "geometry"].values
            )

        # Simplify
        if SIMPLIFY_TOLERANCE:
            _gdf = _gdf.copy()
            _gdf["geometry"] = _gdf.geometry.simplify(
                SIMPLIFY_TOLERANCE, preserve_topology=PRESERVE_TOPOLOGY
            )

        _gdfs.append(_gdf)
        print(f"VPU {_vpu:4s}: {len(_gdf):>6,} HRUs")

    nhru = gpd.GeoDataFrame(pd.concat(_gdfs, ignore_index=True), crs=_gdfs[0].crs)
    print(f"\nTotal: {len(nhru):,} HRUs  |  CRS: {nhru.crs}")
    return (nhru,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Geometry validity check
    """)
    return


@app.cell
def _(nhru):
    n_valid   = nhru.geometry.is_valid.sum()
    n_invalid = (~nhru.geometry.is_valid).sum()
    n_empty   = nhru.geometry.is_empty.sum()
    print(f"valid  : {n_valid:,}")
    print(f"invalid: {n_invalid:,}")
    print(f"empty  : {n_empty:,}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## nat_hru_id contiguity check
    """)
    return


@app.cell
def _(nhru):
    _ids = nhru["nat_hru_id"].sort_values().reset_index(drop=True)
    _expected = range(int(_ids.iloc[0]), int(_ids.iloc[-1]) + 1)
    _gaps = sorted(set(_expected) - set(_ids))
    _dupes = _ids[_ids.duplicated()].tolist()

    print(f"min : {_ids.min():,}")
    print(f"max : {_ids.max():,}")
    print(f"count     : {len(_ids):,}")
    print(f"expected  : {len(_expected):,}")
    print(f"gaps      : {len(_gaps)}" + (f"  {_gaps[:10]}{'…' if len(_gaps) > 10 else ''}" if _gaps else "  none"))
    print(f"duplicates: {len(_dupes)}" + (f"  {_dupes[:10]}{'…' if len(_dupes) > 10 else ''}" if _dupes else "  none"))
    print(f"\nContiguous and unique: {not _gaps and not _dupes}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Preview
    """)
    return


@app.cell
def _(mo, nhru):
    mo.ui.table(nhru.drop(columns="geometry").head(20))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Write output GeoPackage
    """)
    return


@app.cell
def _(OUTPUT_PATH, nhru):
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()
        print(f"Removed existing: {OUTPUT_PATH}")

    nhru.to_file(OUTPUT_PATH, layer="nhru", driver="GPKG")
    print(f"Written {len(nhru):,} features → {OUTPUT_PATH}")
    return


if __name__ == "__main__":
    app.run()
