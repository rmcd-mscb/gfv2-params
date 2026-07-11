"""Build `dprst_depth.tif` (per-cell `dprst_depth_avg` source) + the
`op_flow_thres` constant param (issue #173 Task 7 — integration).

Orchestrates Tasks 2-6 exactly as validated by
`scripts/diagnose/dprst_depth_probe.py`'s `load_conus_dprst` (the reference
reconstruction of the shipped dprst polygon set) and the Phase 0/1 spike
design doc:

  1. Reconstruct the dprst polygon set directly from `waterbody_gpkg` +
     the connected(WBAREACOMI) union flow-through COMID tables
     (`dprst_depth.topo.dprst_polygons`) — the SAME polygon-level
     reconstruction the diagnostic uses. This does NOT read
     `dprst_binary.tif`; STEP_ORDER still places this step after `dprst`
     (for classification-consistency and convention) and after `landmask`/
     `hru_id`, but the runtime data dependency is `waterbody_gpkg` +
     `connected_comids_table` (+ optional `flowthrough_comids_table`), not
     the raster.
  2. Tag `best_topo` (`topo.resolution_class`, needs `wesm_index`),
     `ecoregion` (`download.epa_ecoregions.ecoregion_of`, needs
     `ecoregions_gpkg`), and `ftype` (the `FTYPE` column, aliased lowercase
     to match `fill.py`'s column convention).
  3. Compute per-polygon depth stats — TWO paths, chosen by whether a
     per-batch parquet dir exists and is non-empty:
       - CONUS: load + concat the SLURM array's per-tile-batch parquets
         (Task 9, not yet built — this path activates automatically once
         that array populates `batch_dir`).
       - small/test fabrics: run `tiling.group_by_tile` +
         `compute.run_batch` in-process (one "batch" covering every tile).
  4. Fill every flat/degenerate row (`fill.fit_ecoregion_models` +
     `fill.fill_flat`) so every polygon has a finite, positive
     `dprst_depth_m`.
  5. Burn per-polygon depth onto the template grid, masked to
     `land_mask.tif` (`burn.burn_depth`).
  6. Emit the PRMS `op_flow_thres` constant (always 1.0 — the ArcPy
     convention, `docs/0b_TB_depr_stor.py:994`) as a per-HRU CSV. No
     generic constant-scalar-param writer exists elsewhere in this repo
     (every other depstor param is a raster zonal-stats aggregation driven
     by `derive_depstor_params.py`); `_write_op_flow_thres` below is the
     smallest correct one, using the same `{id_feature}` column convention
     as every merged param CSV so a future generic mechanism (or Task 8's
     params.yml assembly) can consume it identically.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..depstor import load_connected_comids
from ..download.epa_ecoregions import ECO_ID_FIELD, ecoregion_of
from ..dprst_depth.burn import burn_depth
from ..dprst_depth.compute import run_batch
from ..dprst_depth.fill import fill_flat, fit_ecoregion_models
from ..dprst_depth.tiling import group_by_tile
from ..dprst_depth.topo import dprst_polygons, resolution_class
from .context import BuildContext

# Columns computed by compute.run_batch/compute_polygon that survive the
# join back onto the dprst polygon set. Fixed list so an empty batch (all
# columns present, 0 rows — compute._empty_batch_frame's convention) still
# merges cleanly.
_DEPTH_COLUMNS = [
    "COMID", "dprst_depth_m", "measured_max_m", "hollister_max_m", "flat", "resolution", "method",
]

# PRMS op_flow_thres is a fixed constant in the legacy ArcPy source
# (docs/0b_TB_depr_stor.py:994: `op_flow_thres = [1] * nhru`), not a
# raster-derived parameter.
OP_FLOW_THRES_VALUE = 1.0


def _load_vector(path, layer, columns, logger):
    """geopandas.read_file with the pyarrow-backed pyogrio engine, falling
    back to fiona if pyarrow is unavailable (mirrors wbody_connectivity.py /
    dprst_depth_probe.py's `_read_vector`)."""
    try:
        return gpd.read_file(path, layer=layer, columns=columns, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer, columns=columns)


def _load_dprst_polygons(ctx: BuildContext, logger) -> gpd.GeoDataFrame:
    """Reconstruct the shipped dprst polygon set (waterbody_gpkg + the
    connected(WBAREACOMI) UNION flow-through COMID union -> topo.dprst_polygons).

    Mirrors `scripts/diagnose/dprst_depth_probe.py`'s `load_conus_dprst`.
    """
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "dprst_depth step needs `waterbody_gpkg`/`waterbody_layer` in the "
            "fabric profile."
        )
    if ctx.connected_comids_table is None:
        raise KeyError(
            "dprst_depth step needs `connected_comids_table` in the fabric "
            "profile. Stage it first: "
            "`python -m gfv2_params.download.nhd_flowlines`."
        )
    if not ctx.connected_comids_table.exists():
        raise FileNotFoundError(
            f"Connected-COMID table not found: {ctx.connected_comids_table}. "
            f"Run `python -m gfv2_params.download.nhd_flowlines` first."
        )
    if not ctx.waterbody_gpkg.exists():
        raise FileNotFoundError(f"Waterbody gpkg not found: {ctx.waterbody_gpkg}")

    connected = load_connected_comids(ctx.connected_comids_table)
    n_wbareacomi = len(connected)
    n_flowthrough = 0
    if ctx.flowthrough_comids_table is not None:
        if not ctx.flowthrough_comids_table.exists():
            raise FileNotFoundError(
                f"Flow-through COMID table not found: {ctx.flowthrough_comids_table}. "
                f"Run `python -m gfv2_params.download.nhd_flowthrough` first, or "
                f"remove `flowthrough_comids_table` from the profile."
            )
        flowthrough = load_connected_comids(ctx.flowthrough_comids_table)
        n_flowthrough = len(flowthrough - connected)
        connected = connected | flowthrough
    logger.info(
        "  connected COMIDs: %d WBAREACOMI + %d new flow-through = %d total",
        n_wbareacomi, n_flowthrough, len(connected),
    )

    wb_gdf = _load_vector(
        ctx.waterbody_gpkg, ctx.waterbody_layer, ["COMID", "FTYPE", "member_comid"], logger,
    )
    logger.info("  %d waterbody polygons loaded", len(wb_gdf))

    dprst = dprst_polygons(wb_gdf, connected)
    logger.info("  reconstructed dprst polygon set: %d polygons", len(dprst))
    return dprst


def _tag_polygons(dprst: gpd.GeoDataFrame, ctx: BuildContext, logger) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Tag `best_topo` (WESM), `ecoregion` (EPA L3), and `ftype` (FTYPE alias)."""
    if ctx.wesm_index is None:
        raise KeyError(
            "dprst_depth step needs `wesm_index` in the fabric profile: a "
            "pre-staged, 1m/QL1/QL2-filtered WESM workunit footprint index "
            "(a 'project' column + geometry) — see "
            "scripts/diagnose/dprst_depth_probe.py's ensure_wesm_local / "
            "load_wesm_1m_footprints for the download + filtering this file "
            "is expected to already reflect."
        )
    if not ctx.wesm_index.exists():
        raise FileNotFoundError(f"WESM index not found: {ctx.wesm_index}")
    wesm_gdf = gpd.read_file(ctx.wesm_index)

    dprst = resolution_class(dprst, wesm_gdf)
    n_1m = int((dprst["best_topo"] == "1m").sum())
    logger.info("  best_topo: %d/%d polygons tagged 1m (rest 10m)", n_1m, len(dprst))

    if ctx.ecoregions_gpkg is None:
        raise KeyError(
            "dprst_depth step needs `ecoregions_gpkg` in the fabric profile. "
            "Stage it first: `python -m gfv2_params.download.epa_ecoregions`."
        )
    if not ctx.ecoregions_gpkg.exists():
        raise FileNotFoundError(f"Ecoregions gpkg not found: {ctx.ecoregions_gpkg}")
    eco_gdf = gpd.read_file(ctx.ecoregions_gpkg)
    dprst["ecoregion"] = ecoregion_of(dprst, eco_gdf, id_field=ECO_ID_FIELD)
    n_missing = int(dprst["ecoregion"].isna().sum())
    if n_missing:
        logger.warning(
            "  %d/%d dprst polygon centroids fell outside every ecoregion "
            "polygon — tagged 'unassigned'", n_missing, len(dprst),
        )
        dprst["ecoregion"] = dprst["ecoregion"].fillna("unassigned")

    dprst["ftype"] = dprst["FTYPE"]
    return dprst, wesm_gdf


def _compute_depths(
    dprst: gpd.GeoDataFrame, wesm_gdf: gpd.GeoDataFrame, ctx: BuildContext, step_cfg: dict, logger,
) -> pd.DataFrame:
    """Per-polygon depth stats — SLURM per-batch parquet if present, else in-process.

    `batch_dir` (Task 9's SLURM array output) is a `step_cfg` key so
    per-fabric orchestration (CONUS vs a small/test fabric) doesn't require
    a code change — only a config value. Absent/empty -> in-process
    `tiling.group_by_tile` + `compute.run_batch` (correct, just not the
    CONUS-scale fan-out).
    """
    batch_dir = Path(step_cfg.get("batch_dir", ctx.output_dir / "dprst_depth_batches"))
    parquet_files = sorted(batch_dir.glob("*.parquet")) if batch_dir.exists() else []

    if parquet_files:
        logger.info(
            "  found %d per-batch parquet(s) in %s — loading SLURM array output",
            len(parquet_files), batch_dir,
        )
        depth_df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    else:
        logger.info(
            "  no per-batch parquet dir found (%s) — running compute in-process",
            batch_dir,
        )
        groups = group_by_tile(dprst, wesm_gdf, rim_buffer_m=ctx.dprst_rim_buffer_m)
        tile_keys = list(groups.keys())
        logger.info("  %d elevation tile(s) to read for %d polygons", len(tile_keys), len(dprst))
        tmp_parquet = ctx.output_dir / "_dprst_depth_inprocess.parquet"
        depth_df = run_batch(dprst, tile_keys, wesm_gdf, tmp_parquet, logger)

    if "COMID" in depth_df.columns:
        n_before = len(depth_df)
        depth_df = depth_df.drop_duplicates(subset="COMID", keep="first")
        if len(depth_df) < n_before:
            logger.warning(
                "  dropped %d duplicate-COMID depth row(s) before the join",
                n_before - len(depth_df),
            )
    return depth_df


def _fill_and_join(dprst: gpd.GeoDataFrame, depth_df: pd.DataFrame, ctx: BuildContext, logger) -> gpd.GeoDataFrame:
    """Join computed depths onto the polygon set and fill every flat/missing row."""
    keep_cols = [c for c in _DEPTH_COLUMNS if c in depth_df.columns]
    merged = dprst.merge(depth_df[keep_cols], on="COMID", how="left")
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=dprst.crs)

    n_computed = int(merged["dprst_depth_m"].notna().sum()) if "dprst_depth_m" in merged.columns else 0
    logger.info(
        "  %d/%d polygons have a computed depth (rest go through the fallback ladder)",
        n_computed, len(merged),
    )

    non_flat = merged[(merged["flat"] == False) & merged["dprst_depth_m"].notna()]  # noqa: E712
    models = fit_ecoregion_models(non_flat, n_min=ctx.dprst_hollister_n_min)
    filled = fill_flat(merged, models, floor_in=ctx.dprst_depth_floor_in)
    return filled


def _write_op_flow_thres(ctx: BuildContext, out_path: Path, logger) -> Path:
    """Write the constant PRMS `op_flow_thres` (1.0) as a per-HRU CSV.

    See the module docstring for why this ad hoc writer exists instead of a
    generic constant-scalar-param mechanism.
    """
    try:
        ids_gdf = gpd.read_file(
            ctx.hru_gpkg, layer=ctx.hru_layer, columns=[ctx.id_feature],
            use_arrow=True, ignore_geometry=True,
        )
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        ids_gdf = gpd.read_file(
            ctx.hru_gpkg, layer=ctx.hru_layer, columns=[ctx.id_feature], ignore_geometry=True,
        )
    out_df = pd.DataFrame({
        ctx.id_feature: ids_gdf[ctx.id_feature].to_numpy(),
        "op_flow_thres": OP_FLOW_THRES_VALUE,
    })
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    logger.info(
        "  op_flow_thres: wrote constant %.1f for %d HRUs -> %s",
        OP_FLOW_THRES_VALUE, len(out_df), out_path,
    )
    return out_path


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    outputs = step_cfg["outputs"]
    depth_path = ctx.resolve_output(outputs["dprst_depth"])
    op_flow_path = ctx.resolve_output(outputs["op_flow_thres"])
    landmask_path = ctx.require("landmask")

    logger.info("--- dprst_depth ---")
    logger.info("  Depth out        : %s", depth_path)
    logger.info("  op_flow_thres out: %s", op_flow_path)

    if depth_path.exists() and op_flow_path.exists() and not ctx.force:
        logger.info("  Both outputs exist — skipping (pass --force to rebuild)")
        return {"dprst_depth": depth_path, "op_flow_thres": op_flow_path}

    dprst = _load_dprst_polygons(ctx, logger)
    dprst, wesm_gdf = _tag_polygons(dprst, ctx, logger)
    depth_df = _compute_depths(dprst, wesm_gdf, ctx, step_cfg, logger)
    filled = _fill_and_join(dprst, depth_df, ctx, logger)

    burn_depth(filled, ctx.template_path, landmask_path, depth_path, logger)
    _write_op_flow_thres(ctx, op_flow_path, logger)

    return {"dprst_depth": depth_path, "op_flow_thres": op_flow_path}
