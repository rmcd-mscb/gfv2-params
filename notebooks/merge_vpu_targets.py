import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(r"""
    # Merge VPU nhru layers into a single GeoPackage

    Reads the `nhru` layer from each `NHM_<vpu>_draft.gpkg`, fixes invalid
    geometries with `make_valid()`, simplifies, then validates geometry and
    `nat_hru_id` contiguity before writing a single output GeoPackage.
    """)
    return


@app.cell
def _():
    from pathlib import Path

    import geopandas as gpd
    import pandas as pd
    import shapely
    import marimo as mo

    from gfv2_params.config import VPUS_DETAILED, load_base_config

    return Path, VPUS_DETAILED, gpd, load_base_config, mo, pd, shapely


@app.cell
def _(mo):
    mo.md(r"""
    ## Configuration
    """)
    return


@app.cell
def _(Path, VPUS_DETAILED, load_base_config):
    _base = load_base_config()
    TARGETS_DIR = Path(_base["data_root"]) / "targets"
    OUTPUT_PATH = TARGETS_DIR / "gfv2_nhru_merged.gpkg"

    VPUS = VPUS_DETAILED

    # Simplification tolerance in map units (EPSG:5070 → metres).
    # 10 m preserves more detail than the legacy 100 m used by
    # merge_and_fill_params.py --force_rebuild.
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
    if not TARGETS_DIR.exists():
        raise FileNotFoundError(
            f"Targets directory not found: {TARGETS_DIR}\n"
            "Verify that data_root in configs/base_config.yml is correct "
            "and the filesystem is mounted."
        )

    _gdfs = []
    _skipped = []

    for _vpu in VPUS:
        _path = TARGETS_DIR / f"NHM_{_vpu}_draft.gpkg"
        if not _path.exists():
            print(f"VPU {_vpu:4s}: MISSING — {_path}")
            _skipped.append(_vpu)
            continue

        _gdf = gpd.read_file(_path, layer="nhru")
        _gdf["source_vpu"] = _path.stem

        # Fix invalid geometries
        _invalid = ~_gdf.geometry.is_valid
        if _invalid.any():
            _gdf.loc[_invalid, "geometry"] = shapely.make_valid(
                _gdf.loc[_invalid, "geometry"].values
            )
            # make_valid can produce GeometryCollections; extract polygons only
            _non_poly = ~_gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
            if _non_poly.any():
                print(f"  VPU {_vpu}: {_non_poly.sum()} non-polygon geometries after make_valid, extracting polygons")
                _gdf.loc[_non_poly, "geometry"] = _gdf.loc[_non_poly, "geometry"].apply(
                    lambda g: shapely.ops.unary_union([p for p in getattr(g, "geoms", [g]) if p.geom_type in ("Polygon", "MultiPolygon")]) if hasattr(g, "geoms") else g
                )

        # Simplify
        if SIMPLIFY_TOLERANCE:
            _gdf = _gdf.copy()
            _gdf["geometry"] = _gdf.geometry.simplify(
                SIMPLIFY_TOLERANCE, preserve_topology=PRESERVE_TOPOLOGY
            )

        _gdfs.append(_gdf)
        print(f"VPU {_vpu:4s}: {len(_gdf):>6,} HRUs")

    if not _gdfs:
        raise RuntimeError(f"No VPU GeoPackages were successfully loaded from {TARGETS_DIR}")

    if _skipped:
        print(f"\nWARNING: {len(_skipped)} VPU(s) skipped (missing): {_skipped}")

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

    if n_invalid > 0:
        raise ValueError(f"{n_invalid} invalid geometries remain after make_valid — inspect before writing output")
    if n_empty > 0:
        print(f"WARNING: {n_empty} empty geometries detected — these HRUs will have no spatial extent")
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

    # Write to a temp file first to avoid losing existing data if the write fails
    _tmp = OUTPUT_PATH.with_suffix(".gpkg.tmp")
    nhru.to_file(_tmp, layer="nhru", driver="GPKG")

    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()
    _tmp.rename(OUTPUT_PATH)
    print(f"Written {len(nhru):,} features → {OUTPUT_PATH}")
    return


if __name__ == "__main__":
    app.run()
