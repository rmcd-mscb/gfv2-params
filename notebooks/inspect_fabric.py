import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(r"""
    # Inspect a fabric GeoPackage

    Surveys the layers, fields, CRS, HRU-id columns, geometry validity, and
    spatial extent of a fabric `.gpkg` so you can fill in its
    `configs/base_config.yml` profile. Reusable for any fabric — set `FABRIC`
    (and, if your gpkg is not named `{fabric}.gpkg`, `GPKG_PATH`) below.

    Run on a **compute node** (JupyterHub or `salloc`), never the login node —
    geo-library imports can hang on the shared FS otherwise (see CLAUDE.md).
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""## Configuration""")
    return


@app.cell
def _():
    from pathlib import Path

    import geopandas as gpd
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    from pyogrio import list_layers, read_info
    from pyproj import CRS

    import marimo as mo

    from gfv2_params.config import load_base_config

    return (
        CRS,
        Path,
        gpd,
        list_layers,
        load_base_config,
        mo,
        np,
        pd,
        plt,
        read_info,
    )


@app.cell
def _(Path, load_base_config):
    # Which fabric to inspect. Only `data_root` is read from the profile — the
    # stub's `hru_gpkg` is a TODO placeholder pointing at the wrong filename,
    # so we build the gpkg path directly from the on-disk layout instead.
    FABRIC = "tjc"

    _base = load_base_config(fabric=FABRIC)
    DATA_ROOT = Path(_base["data_root"])

    # Override if your staged gpkg has a different name/location.
    GPKG_PATH = DATA_ROOT / FABRIC / "fabric" / f"{FABRIC}.gpkg"

    # Layer-name guesses used by the id/validity/map cells (auto-fallback below).
    HRU_LAYER = "nhru"
    SEG_LAYER = "nsegment"

    if not GPKG_PATH.exists():
        raise FileNotFoundError(
            f"Fabric gpkg not found: {GPKG_PATH}\n"
            f"Set GPKG_PATH to the file you staged under {DATA_ROOT / FABRIC / 'fabric'}/."
        )
    print(f"FABRIC    : {FABRIC}")
    print(f"DATA_ROOT : {DATA_ROOT}")
    print(f"GPKG_PATH : {GPKG_PATH}  ({GPKG_PATH.stat().st_size / 1e6:.1f} MB)")
    return FABRIC, GPKG_PATH, HRU_LAYER, SEG_LAYER


@app.cell
def _(mo):
    mo.md(r"""## Layer overview""")
    return


@app.cell
def _(GPKG_PATH, list_layers, mo, pd, read_info):
    _rows = []
    LAYER_INFO = {}
    for _name, _geom in list_layers(GPKG_PATH):
        _info = read_info(GPKG_PATH, layer=_name)
        LAYER_INFO[_name] = _info
        _rows.append(
            {
                "layer": _name,
                "geom_type": _geom,
                "features": _info["features"],
                "n_fields": len(_info["fields"]),
                "fields": ", ".join(_info["fields"]),
            }
        )
    overview = pd.DataFrame(_rows)
    print(overview.to_string(index=False))
    mo.ui.table(overview, selection=None)
    return LAYER_INFO, overview


@app.cell
def _(mo):
    mo.md(r"""## Fields & dtypes per layer""")
    return


@app.cell
def _(LAYER_INFO):
    for _name, _info in LAYER_INFO.items():
        print(f"=== {_name} ===")
        for _fld, _dt in zip(_info["fields"], _info["dtypes"]):
            print(f"    {_fld:24s} {_dt}")
        print()
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## CRS check

    The shared rasters and the depstor template are **EPSG:5070** (NAD83 /
    Conus Albers). The fabric must reproject cleanly onto that grid, so flag
    any datum/parameter mismatch here before running depstor.
    """)
    return


@app.cell
def _(CRS, LAYER_INFO):
    _target = CRS.from_epsg(5070)
    for _name, _info in LAYER_INFO.items():
        _crs = CRS.from_user_input(_info["crs"]) if _info["crs"] else None
        if _crs is None:
            print(f"{_name}: NO CRS DEFINED  ⚠️")
            continue
        _exact = _crs == _target
        _equiv = _crs.equals(_target) if hasattr(_crs, "equals") else _exact
        _epsg = _crs.to_epsg()
        print(
            f"{_name}: {_crs.name}  (EPSG={_epsg})  "
            f"{'== EPSG:5070' if _exact else ('≈ EPSG:5070 (equivalent)' if _equiv else '≠ EPSG:5070 — REPROJECT/CHECK ⚠️')}"
        )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## HRU id-column analysis

    For each integer column on the HRU layer, report min/max/count, whether it
    is **unique** (a usable `id_feature`) and **contiguous** (no gaps). The max
    of the chosen id column is your `expected_max_hru_id`.
    """)
    return


@app.cell
def _(GPKG_PATH, HRU_LAYER, LAYER_INFO, gpd, np, pd):
    _hru_layer = HRU_LAYER if HRU_LAYER in LAYER_INFO else max(
        LAYER_INFO, key=lambda n: LAYER_INFO[n]["features"]
    )
    hru_attrs = gpd.read_file(GPKG_PATH, layer=_hru_layer, ignore_geometry=True)

    _rows = []
    for _col in hru_attrs.columns:
        _s = hru_attrs[_col]
        if not pd.api.types.is_integer_dtype(_s):
            continue
        _vals = _s.dropna()
        _lo, _hi = int(_vals.min()), int(_vals.max())
        _unique = _vals.is_unique
        _gaps = (_hi - _lo + 1) - _vals.nunique()
        _rows.append(
            {
                "column": _col,
                "min": _lo,
                "max": _hi,
                "count": len(_vals),
                "n_unique": _vals.nunique(),
                "unique": _unique,
                "contiguous": _gaps == 0,
                "n_gaps": int(_gaps),
            }
        )
    id_candidates = pd.DataFrame(_rows)
    print(f"HRU layer: {_hru_layer}  ({len(hru_attrs):,} features)\n")
    print(id_candidates.to_string(index=False))
    print(
        "\nA good id_feature is unique (and ideally contiguous). "
        "expected_max_hru_id = the chosen column's max."
    )
    return hru_attrs, id_candidates


@app.cell
def _(mo):
    mo.md(r"""## Geometry validity""")
    return


@app.cell
def _(GPKG_PATH, HRU_LAYER, LAYER_INFO, gpd):
    _hru_layer = HRU_LAYER if HRU_LAYER in LAYER_INFO else max(
        LAYER_INFO, key=lambda n: LAYER_INFO[n]["features"]
    )
    hru = gpd.read_file(GPKG_PATH, layer=_hru_layer)
    _valid = hru.geometry.is_valid.sum()
    _invalid = int((~hru.geometry.is_valid).sum())
    _empty = int(hru.geometry.is_empty.sum())
    print(f"layer  : {_hru_layer}")
    print(f"valid  : {_valid:,}")
    print(f"invalid: {_invalid:,}" + ("  ⚠️ make_valid() before depstor" if _invalid else ""))
    print(f"empty  : {_empty:,}" + ("  ⚠️" if _empty else ""))
    print(f"\ngeom types: {hru.geometry.geom_type.value_counts().to_dict()}")
    return (hru,)


@app.cell
def _(mo):
    mo.md(r"""## Map preview""")
    return


@app.cell
def _(GPKG_PATH, LAYER_INFO, SEG_LAYER, gpd, hru, plt):
    _fig, _ax = plt.subplots(figsize=(8, 8))
    hru.plot(ax=_ax, facecolor="#cfe8ef", edgecolor="#3b8ea5", linewidth=0.2)
    if SEG_LAYER in LAYER_INFO:
        _seg = gpd.read_file(GPKG_PATH, layer=SEG_LAYER)
        _seg.plot(ax=_ax, color="#08519c", linewidth=0.5)
    if "domain" in LAYER_INFO:
        _dom = gpd.read_file(GPKG_PATH, layer="domain")
        _dom.boundary.plot(ax=_ax, color="black", linewidth=1.0)
    _ax.set_title(f"{GPKG_PATH.stem}: HRUs (fill), segments (blue), domain (black)")
    _ax.set_aspect("equal")
    _ax.ticklabel_format(style="plain")
    _ax
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Suggested `base_config.yml` profile

    Copy the active keys below into the `tjc:` profile, replacing the TODO
    stub. Pick the `id_feature` from the id-analysis table above (it must be
    **unique**); `expected_max_hru_id` is auto-filled from that column's max.
    Uncomment the depstor block once you stage the FDR clip.
    """)
    return


@app.cell
def _(FABRIC, GPKG_PATH, HRU_LAYER, LAYER_INFO, SEG_LAYER, id_candidates):
    # Best id column = unique, prefer contiguous, then smallest max.
    _usable = id_candidates[id_candidates["unique"]].sort_values(
        ["contiguous", "max"], ascending=[False, True]
    )
    _id_col = _usable.iloc[0]["column"] if len(_usable) else "<no-unique-int-column>"
    _max_id = int(_usable.iloc[0]["max"]) if len(_usable) else 0
    _hru_layer = HRU_LAYER if HRU_LAYER in LAYER_INFO else "<hru-layer>"
    _seg = SEG_LAYER if SEG_LAYER in LAYER_INFO else "nsegment"
    _rel = GPKG_PATH.name

    print(f"""  {FABRIC}:
    expected_max_hru_id: {_max_id}
    batch_size: 10000
    id_feature: {_id_col}
    hru_gpkg: "{{data_root}}/{{fabric}}/fabric/{_rel}"
    hru_layer: {_hru_layer}
    # --- depstor (uncomment after staging the FDR clip) ---
    # template_raster: "{{data_root}}/{{fabric}}/shared/{{fabric}}_fdr.vrt"
    # fdr_raster:      "{{data_root}}/{{fabric}}/shared/{{fabric}}_fdr.vrt"
    # twi_raster:      "{{data_root}}/shared/conus/vrt/twi_hydrodem.vrt"
    # vpu: "<NN>"   # set only if the fabric lies within one VPU
    # segments_gpkg:   "{{data_root}}/{{fabric}}/fabric/{_rel}"
    # segments_layer:  {_seg}
    # waterbody_gpkg:  "{{data_root}}/input/nhd/conus_waterbodies.gpkg"
    # waterbody_layer: waterbodies""")
    return


if __name__ == "__main__":
    app.run()
