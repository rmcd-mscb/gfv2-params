"""Build `dprst_depth.tif` (per-cell `dprst_depth_avg` source) + the
`op_flow_thres` constant param (issue #173 Task 7 — integration).

Orchestrates Tasks 2-6 exactly as validated by
`scripts/diagnose/dprst_depth_probe.py`'s `load_conus_dprst` (the reference
reconstruction of the shipped dprst polygon set) and the Phase 0/1 spike
design doc:

  1. Reconstruct the dprst polygon set directly from `waterbody_gpkg` + the
     resolved on-stream COMID set, CLIPPED to the fabric's HRU extent
     (`dprst_depth.topo.load_fabric_dprst_polygons` — the same reconstruction
     as `dprst_depth.topo.dprst_polygons` plus the fabric-bounds clip,
     `topo._clip_dprst_to_fabric`, so a regional fabric doesn't reprocess the
     whole CONUS dprst set). The on-stream set is the segment classifier's
     on-stream COMIDs (`segment_wbody_comids`, from the `segment_wbody`
     step) MINUS the endorheic set (`endorheic_comids`, from the
     `endorheic` step) — the SAME set `wbody_connectivity` uses to build
     `dprst_binary.tif` ONLY while NHD comparison mode is off (no fabric
     profile sets `connected_comids_table`/`flowthrough_comids_table` — the
     default). If a fabric re-enables either key, `wbody_connectivity`
     unions NHD's COMIDs in but this reconstruction does not, silently
     reintroducing the exact on-stream divergence this branch exists to
     remove, with no local signal in `dprst_depth`. This reconstruction step alone does
     NOT read `dprst_binary.tif` — its data dependency is `waterbody_gpkg` +
     `segment_wbody_comids` + `endorheic_comids` + `hru_gpkg`, not the
     raster. STEP_ORDER
     places the overall `dprst_depth` step after `dprst` (for
     classification-consistency and convention) and after `landmask`/
     `hru_id`; since the e596de0 correctness fix, that ordering is also a
     real runtime data dependency for the whole step, not just convention —
     step 5's burn reads `dprst_binary.tif` directly (`ctx.require("dprst")`)
     as a mask.
  2. Tag `best_topo` + rank real tile-set candidates (`sources.tag_and_assign`,
     issue #223, needs `dem_1m_inventory`/`wesm_project_attrs`) — replaces the
     retired convex-hull `topo.resolution_class`/`wesm_index` path. Internally
     also retags any oversized 1m polygon to 10m (`tiling.guard_oversized_windows`
     — a polygon whose 1m rim-buffered window would be enormous, e.g. a
     giant lake's bbox, is downgraded before anything downstream ever reads
     its window; the CONUS load-balance/OOM fix) BETWEEN tagging and
     assignment. Then tag `ecoregion`
     (`download.epa_ecoregions.ecoregion_of`, needs `ecoregions_gpkg`), and
     `ftype` (the `FTYPE` column, aliased lowercase to match `fill.py`'s
     column convention).
  3. Compute per-polygon depth stats — TWO paths, chosen by whether a
     per-batch parquet dir exists and is non-empty:
       - CONUS: load + concat the SLURM array's per-tile-batch parquets
         (the tiled `submit_dprst_depth.sh` array — this path activates
         automatically once that array populates `batch_dir`).
       - small/test fabrics: run `tiling.tile_set_groups` +
         `compute.run_batch` in-process (one call per real tile SET, as
         assigned by `sources.tag_and_assign`).
  4. Fill every flat/degenerate row (`fill.fit_ecoregion_models` +
     `fill.fill_flat`) so every polygon has a finite, positive
     `dprst_depth_m`.
  5. Burn per-polygon depth onto the template grid, gated on BOTH
     `land_mask.tif` AND the shipped `dprst_binary.tif` (`ctx.require
     ("dprst")`) (`burn.burn_depth`) — the intersection of both masks makes
     `dprst_depth.tif` a cell subset of `dprst_binary.tif`'s dprst cells BY
     CONSTRUCTION, so `dprst_depth_avg` stays consistent with `dprst_frac`
     even though this module's per-polygon COMID reconstruction doesn't
     reproduce `dprst.py`'s raster CLUMP semantics.
  6. Emit the PRMS `op_flow_thres` constant (always 1.0 — the ArcPy
     convention, `docs/0b_TB_depr_stor.py:994`) as a per-HRU CSV. No
     generic constant-scalar-param writer exists elsewhere in this repo
     (every other depstor param is a raster zonal-stats aggregation driven
     by `derive_depstor_params.py`); `_write_op_flow_thres` below is the
     smallest correct one, using the same `{id_feature}` column convention
     as every merged param CSV so a future generic mechanism (or Task 8's
     params.yml assembly) can consume it identically.
  7. Persist the final per-polygon provenance table — `COMID`, `method`,
     `dprst_depth_m`, plus the diagnostic columns `resolution`, `ftype`,
     `ecoregion`, `measured_max_m`, `hollister_max_m` (#173 Oregon
     validation Risk 3: the original 3-column parquet couldn't support a
     1 m/10 m split, FTYPE, or ecoregion breakdown at CONUS scale), and
     `geometry` — as a companion GeoParquet (`dprst_depth_polygons.parquet`,
     next to `dprst_depth.tif`). `burn_depth` only burns the numeric depth
     onto the raster, discarding every other per-polygon column. Task 8's
     per-HRU aggregation (`dprst_depth.aggregate.area_weighted_provenance`)
     reads this companion file back to derive a per-HRU dominant-method
     `dprst_depth_provenance` column without recomputing the polygon set —
     it only needs `method`/`geometry`, so the extra diagnostic columns are
     additive and don't change that reader's behavior.
     A fixed filename (not config-driven, not registered in `ctx.paths` /
     the DAG's `_expected_outputs`) — it's a byproduct for the separate
     `derive_depstor_params.py` param driver, not a DAG dependency any other
     depstor_rasters step consumes.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..download.epa_ecoregions import ECO_ID_FIELD, ecoregion_of
from ..dprst_depth.burn import burn_depth
from ..dprst_depth.compute import run_batch
from ..dprst_depth.fill import fill_flat, fit_ecoregion_models
from ..dprst_depth.sources import tag_and_assign
from ..dprst_depth.tiling import tile_set_groups
from ..dprst_depth.topo import load_fabric_dprst_polygons
from ..endorheic import load_endorheic_comids
from ..segment_wbody import load_segment_comids
from .context import BuildContext

# Columns computed by compute.run_batch/compute_polygon that survive the
# join back onto the dprst polygon set. Fixed list so an empty batch (all
# columns present, 0 rows — compute._empty_batch_frame's convention) still
# merges cleanly. `source` (the winning tile set's project, or "10m" on the
# seamless fallback) and `interior_coverage` (the fraction of the polygon's
# true interior footprint that was real, not read_padded sentinel padding)
# are issue #223's per-polygon provenance additions — see compute.py's
# `_OUTPUT_COLUMNS` docstring. Both must survive this join so the re-run
# validation (which project a depth came from) and a future donor-pool
# coverage filter (fill.py, out of scope for this plan) can read them back.
_DEPTH_COLUMNS = [
    "COMID", "dprst_depth_m", "measured_max_m", "hollister_max_m", "flat", "resolution", "method",
    "source", "interior_coverage",
]

# PRMS op_flow_thres is a fixed constant in the legacy ArcPy source
# (docs/0b_TB_depr_stor.py:994: `op_flow_thres = [1] * nhru`), not a
# raster-derived parameter.
OP_FLOW_THRES_VALUE = 1.0

# Fixed filename for the per-polygon provenance companion (see module
# docstring point 7) — always written next to dprst_depth.tif.
POLYGON_PROVENANCE_FILENAME = "dprst_depth_polygons.parquet"

# Ceiling on the serial in-process fallback (#221). Above it, `_compute_depths` refuses
# and points at the tiled `submit_dprst_depth.sh` instead. Measured 2026-09-18 from the
# tiled planners' own manifests: oregon 3,717 and tjc 6,113 polygons sit well under it;
# gfv2 (279,391) and gfv2_dev (392,673) sit well over. gfv2r2's first run achieved about
# 3,300 polygons/h in-process, so the ceiling finishes in ~7.5 h -- inside the 18 h
# build_depstor_rasters.batch limit with room to spare. Override per step with
# `max_inprocess_polygons`; 0 disables the guard.
DEFAULT_MAX_INPROCESS_POLYGONS = 25_000


def _load_dprst_polygons(ctx: BuildContext, logger) -> gpd.GeoDataFrame:
    """Reconstruct the fabric-clipped dprst polygon set.

    Validates the fabric-profile paths, then delegates the reconstruction +
    fabric clip to `topo.load_fabric_dprst_polygons` — the SAME shared helper
    the SLURM plan hook (`tiling.py::_load_and_tag_for_plan`) calls, so the
    in-process builder path and the array/plan path can't diverge (both
    reconstruct from the profile's `waterbody_gpkg` layer + the resolved
    on-stream COMID set and clip to the fabric's HRU extent — without that
    clip a regional fabric would process the whole CONUS dprst set). The
    on-stream set is the segment classifier's on-stream COMIDs
    (`segment_wbody_comids`) MINUS the endorheic set (`endorheic_comids`) —
    the SAME set `wbody_connectivity` unions/subtracts to build
    `dprst_binary.tif` ONLY while NHD comparison mode is off (no fabric
    profile sets `connected_comids_table`/`flowthrough_comids_table` — the
    default). If a fabric re-enables either key, `wbody_connectivity` unions
    NHD's COMIDs into ITS on-stream set but this reconstruction does not —
    silently reintroducing the exact on-stream divergence this branch exists
    to remove, with no local signal here. See the WARNING logged below when
    either table is configured.
    """
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "dprst_depth step needs `waterbody_gpkg`/`waterbody_layer` in the "
            "fabric profile."
        )
    if not ctx.waterbody_gpkg.exists():
        raise FileNotFoundError(f"Waterbody gpkg not found: {ctx.waterbody_gpkg}")
    if "segment_wbody_comids" not in ctx.paths:
        raise KeyError(
            "dprst_depth step needs `segment_wbody_comids` in the build context, but the "
            "`segment_wbody` step has not run for this fabric. That table is the on-stream "
            "source the dprst polygon set is reconstructed against — without it "
            "dprst_depth would compute depths for a different polygon set than "
            "dprst_binary.tif. Run the full DAG, or `--from segment_wbody`."
        )
    if "endorheic_comids" not in ctx.paths:
        raise KeyError(
            "dprst_depth step needs `endorheic_comids` in the build context, but the "
            "`endorheic` step has not run for this fabric. Without the endorheic "
            "subtraction the reconstructed dprst polygon set would exclude terminal "
            "lakes that `dprst_binary.tif` includes — the Great Salt Lake among them."
        )
    if ctx.connected_comids_table is not None or ctx.flowthrough_comids_table is not None:
        logger.warning(
            "  COMPARISON MODE: an NHD COMID table is configured (connected=%s, "
            "flowthrough=%s). `wbody_connectivity` unions NHD's COMIDs into its "
            "on-stream set, but this dprst polygon reconstruction does NOT — it "
            "will diverge from `dprst_binary.tif`'s classification. This is NOT "
            "the production configuration — comment those keys out of the fabric "
            "profile for a production run.",
            ctx.connected_comids_table, ctx.flowthrough_comids_table,
        )
    onstream = load_segment_comids(ctx.require("segment_wbody_comids")) - \
        load_endorheic_comids(ctx.require("endorheic_comids"))

    return load_fabric_dprst_polygons(
        waterbody_gpkg=ctx.waterbody_gpkg,
        waterbody_layer=ctx.waterbody_layer,
        onstream_comids=onstream,
        hru_gpkg=ctx.hru_gpkg,
        hru_layer=ctx.hru_layer,
        logger=logger,
    )


def _tag_polygons(dprst: gpd.GeoDataFrame, ctx: BuildContext, logger) -> gpd.GeoDataFrame:
    """Tag `best_topo` + ranked real tile-set candidates (issue #223), `ecoregion`
    (EPA L3), and `ftype` (FTYPE alias).

    `sources.tag_and_assign` is the ONE entry point shared by this in-process
    builder path and the SLURM `--plan` hook
    (`tiling.py::_load_and_tag_for_plan`), so the two paths cannot diverge on
    which real 3DEP tile set(s) a polygon is assigned — including the
    oversized-window 1m->10m guard it runs internally between tagging and
    assignment. Replaces the retired convex-hull `topo.resolution_class` +
    `tiling.guard_oversized_windows` two-step (a hull claims ground a project
    never flew; see `dprst_depth/sources.py`'s module docstring).
    """
    for key in ("dem_1m_inventory", "wesm_project_attrs"):
        path = getattr(ctx, key)
        if path is None:
            raise KeyError(
                f"dprst_depth step needs `{key}` in the fabric profile. Stage it: "
                "`sbatch slurm_batch/stage_dem_1m_inventory.batch`."
            )
        if not path.exists():
            raise FileNotFoundError(
                f"{key} not found: {path}. Stage it: "
                "`sbatch slurm_batch/stage_dem_1m_inventory.batch`."
            )
    dprst = tag_and_assign(
        dprst, ctx.dem_1m_inventory, ctx.wesm_project_attrs, logger,
        min_inventory_tiles=ctx.min_dem_1m_tiles,
        min_inventory_projects=ctx.min_dem_1m_projects,
        min_attrs_rows=ctx.min_wesm_project_attrs_rows,
    )

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
    return dprst


def _first_few(names: list[str], n: int = 5) -> str:
    return ", ".join(names[:n]) + (f" (+{len(names) - n} more)" if len(names) > n else "")


def _verify_batches_match_plan(batch_dir: Path, parquet_files: list[Path], dprst) -> None:
    """Refuse per-batch parquets that do not belong to the CURRENT polygon set (#221).

    Nothing ever cleared `dprst_depth_batches/`, and this builder used to load whatever
    it found. The documented cascade rebuild after a classifier change (`--from
    wbody_connectivity --force`) runs through this step, so it picked up parquets
    planned for the OLD polygon set -- consistent with their own plan, wrong for the
    current one. The final product survived only by coincidence (the left join in
    `_fill_and_join` drops polygons that are gone; new ones fell to the regional fill).

    The tiled planner records exactly what a check needs under `_plan/`: the polygon set
    it planned (`dprst_polygons_tagged.parquet`) and how many batch files the array
    writes (`batch_manifest.json`). The planner and this builder both reconstruct that
    set through `topo.load_fabric_dprst_polygons`, and tagging never drops a polygon, so
    the two COMID sets are equal by construction -- any difference is a stale plan.
    """
    import json

    plan_dir = batch_dir / "_plan"
    manifest_path = plan_dir / "batch_manifest.json"
    tagged_path = plan_dir / "dprst_polygons_tagged.parquet"
    if not (manifest_path.exists() and tagged_path.exists()):
        raise RuntimeError(
            f"dprst_depth: {len(parquet_files)} per-batch parquet(s) in {batch_dir} but no "
            f"_plan/ ({manifest_path.name} + {tagged_path.name}) recording which polygon "
            f"set they were computed for, so they cannot be trusted. Re-run "
            f"slurm_batch/submit_dprst_depth.sh, which plans and computes them together."
        )

    n_batches = int(json.loads(manifest_path.read_text())["n_batches"])
    expected = {f"batch_{i:04d}.parquet" for i in range(n_batches)}
    found = {p.name for p in parquet_files}
    missing, extra = sorted(expected - found), sorted(found - expected)
    if missing or extra:
        raise RuntimeError(
            f"dprst_depth: {batch_dir} does not match its plan of {n_batches} batch(es). "
            + (f"Missing: {_first_few(missing)}. " if missing else "")
            + (f"Not in the plan: {_first_few(extra)}. " if extra else "")
            + "A missing file is a batch the array did not finish; an extra one is left "
            "over from an older plan. Re-run slurm_batch/submit_dprst_depth.sh."
        )

    planned = set(pd.read_parquet(tagged_path, columns=["COMID"])["COMID"])
    current = set(dprst["COMID"])
    if planned != current:
        raise RuntimeError(
            f"dprst_depth: the per-batch parquets in {batch_dir} were planned for a "
            f"different dprst polygon set ({len(planned):,} planned vs {len(current):,} now: "
            f"{len(current - planned):,} new, {len(planned - current):,} no longer dprst). "
            f"That is what a classifier change followed by a `--from` cascade rebuild looks "
            f"like. Re-run slurm_batch/submit_dprst_depth.sh to replan and recompute."
        )


def _compute_depths(
    dprst: gpd.GeoDataFrame, ctx: BuildContext, step_cfg: dict, logger,
) -> pd.DataFrame:
    """Per-polygon depth stats — SLURM per-batch parquet if present, else in-process.

    `batch_dir` (the tiled SLURM array's output) is a `step_cfg` key so
    per-fabric orchestration (CONUS vs a small/test fabric) doesn't require
    a code change — only a config value. Absent/empty -> in-process
    `tiling.tile_set_groups` + `compute.run_batch` (correct, just not the
    CONUS-scale fan-out) — `dprst` must already carry `source_tiles`/
    `candidates` (`sources.tag_and_assign`, via `_tag_polygons`).
    """
    batch_dir = Path(step_cfg.get("batch_dir", ctx.output_dir / "dprst_depth_batches"))
    parquet_files = sorted(batch_dir.glob("*.parquet")) if batch_dir.exists() else []

    if parquet_files:
        _verify_batches_match_plan(batch_dir, parquet_files, dprst)
        logger.info(
            "  found %d per-batch parquet(s) in %s — loading SLURM array output "
            "(verified against its plan)",
            len(parquet_files), batch_dir,
        )
        # Drop EMPTY batches before the concat. `compute._empty_batch_frame` writes
        # every column as `object`, and pandas 3 no longer ignores empty frames when
        # resolving dtypes -- so on a small fabric (flaming_gorge: 26 of 150 batches
        # empty) one empty batch turned `dprst_depth_m` into `object` and
        # `burn_depth`'s `np.isfinite` raised. Filtering here, not only at the
        # writer, is what repairs batch dirs already on disk. Keep one frame if all
        # are empty so the schema check below still sees the columns.
        frames = [pd.read_parquet(f) for f in parquet_files]
        depth_df = pd.concat([f for f in frames if len(f)] or frames[:1], ignore_index=True)
        # `_fill_and_join`'s `keep_cols = [c for c in _DEPTH_COLUMNS if c in
        # depth_df.columns]` silently drops whatever's missing -- fine for a
        # legitimately EMPTY batch (`compute._empty_batch_frame` guarantees every
        # column, just 0 rows), but a NON-empty concatenated frame missing one is
        # exactly what a `--from`/`--force` re-run against PRE-#223 batch parquets
        # looks like (written before `source`/`interior_coverage` existed).
        # `_verify_batches_match_plan` only checks `n_batches` and the COMID set,
        # not the schema, so a stale batch dir passes that check silently and this
        # join would too -- the provenance parquet is exactly what the re-run
        # validation (scripts/diagnose/compare_dprst_depth_runs.py) diffs. Scoped to
        # THIS branch only (loaded-from-disk parquets), not the in-process
        # `run_batch` call below, whose real output always carries every
        # `_OUTPUT_COLUMNS` member by construction (compute.py) -- so a test double
        # standing in for it doesn't need to replicate that schema too.
        if len(depth_df) > 0:
            missing = [c for c in _DEPTH_COLUMNS if c not in depth_df.columns]
            if missing:
                raise RuntimeError(
                    f"dprst_depth: {len(depth_df):,} non-empty per-polygon depth row(s) "
                    f"loaded from {batch_dir} are missing column(s) {missing} that "
                    f"_DEPTH_COLUMNS declares. This is what loading PRE-#223 "
                    f"batch_*.parquet files (written before source/interior_coverage "
                    f"existed) looks like. Re-run the tiled stage "
                    f"(slurm_batch/submit_dprst_depth.sh) to regenerate them with the "
                    f"current schema before building this step."
                )
    else:
        # Refuse CONUS-scale work BEFORE starting it (#221). This branch used to accept any
        # size and say so only at INFO: gfv2r2's first run reached it with 392,672
        # polygons and managed 9.4% in 11 h against an 18 h job limit, so the job could
        # only time out and cancel the whole re-run chain behind it.
        ceiling = int(step_cfg.get("max_inprocess_polygons", DEFAULT_MAX_INPROCESS_POLYGONS))
        if ceiling > 0 and len(dprst) > ceiling:
            raise RuntimeError(
                f"dprst_depth: {len(dprst):,} polygons but no per-batch parquets in "
                f"{batch_dir}, and the serial in-process fallback is capped at {ceiling:,} "
                f"(`max_inprocess_polygons`). At this size it would run for days. Run the "
                f"tiled stage first -- slurm_batch/submit_dprst_depth.sh -- which writes "
                f"those parquets; submit_fabric_rerun.sh does this for you. Set "
                f"`max_inprocess_polygons: 0` in this step's config only if you really "
                f"want the long serial run."
            )
        logger.info(
            "  no per-batch parquet dir found (%s) — running compute in-process "
            "(%d polygons, ceiling %d)",
            batch_dir, len(dprst), ceiling,
        )
        # Rim buffer (200 m) and flatness tol (0.01 m, used inside
        # compute.run_batch's is_hydroflattened call) are the validated spike
        # defaults (Phase 0/1) and are currently fixed at their function
        # defaults; expose as config only when a task threads them through
        # compute.run_batch/topo.read_window/topo.is_hydroflattened.
        groups = tile_set_groups(dprst)
        logger.info("  %d real tile set(s) to read for %d polygons", len(groups), len(dprst))
        tmp_parquet = ctx.output_dir / "_dprst_depth_inprocess.parquet"
        depth_df = run_batch(dprst, list(groups), tmp_parquet, logger, n_threads=1)

    if "COMID" in depth_df.columns:
        n_before = len(depth_df)
        depth_df = depth_df.drop_duplicates(subset="COMID", keep="first")
        if len(depth_df) < n_before:
            logger.warning(
                "  dropped %d duplicate-COMID depth row(s) before the join",
                n_before - len(depth_df),
            )
    return depth_df


def _log_achieved_resolution(dprst: gpd.GeoDataFrame, merged: gpd.GeoDataFrame, logger) -> None:
    """(#173 FIX 4a) Log the ACHIEVED `resolution` breakdown alongside the
    already-logged INTENDED `best_topo` tag count, and WARN if they diverge
    materially — catches a silent all-10m regression (e.g. every 1 m tile
    read failing and falling through to the 10 m source without anyone
    noticing, since `resolution` is set per-row by whatever source actually
    got read, not by the `best_topo` tag alone).
    """
    total = len(merged)
    if total == 0 or "resolution" not in merged.columns:
        return
    counts = merged["resolution"].value_counts(dropna=False)
    logger.info("  achieved resolution breakdown: %s", counts.to_dict())

    n_tagged_1m = int((dprst["best_topo"] == "1m").sum()) if "best_topo" in dprst.columns else 0
    n_achieved_1m = int(counts.get("1m", 0))
    pct_tagged = 100 * n_tagged_1m / total
    pct_achieved = 100 * n_achieved_1m / total
    if abs(pct_tagged - pct_achieved) > 20:
        logger.warning(
            "  achieved 1m coverage (%.1f%%, %d/%d) diverges from tagged-1m "
            "(%.1f%%, %d/%d) by more than 20 percentage points — possible "
            "silent 1m->10m regression, investigate",
            pct_achieved, n_achieved_1m, total, pct_tagged, n_tagged_1m, total,
        )


def _fill_and_join(dprst: gpd.GeoDataFrame, depth_df: pd.DataFrame, ctx: BuildContext, logger) -> gpd.GeoDataFrame:
    """Join computed depths onto the polygon set and fill every flat/missing row."""
    keep_cols = [c for c in _DEPTH_COLUMNS if c in depth_df.columns]
    merged = dprst.merge(depth_df[keep_cols], on="COMID", how="left")
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=dprst.crs)

    n_computed = int(merged["dprst_depth_m"].notna().sum()) if "dprst_depth_m" in merged.columns else 0
    n_total = len(merged)
    logger.info(
        "  %d/%d polygons have a computed depth (rest go through the fallback ladder)",
        n_computed, n_total,
    )

    # (#173 FIX 3, hardened by the robustness guards) Completeness gate: a
    # mass read-failure (S3 outage / HPC firewall regression — this project
    # has hit this class before, see proj_network_firewall_inf) would
    # otherwise ship a mostly-floored product with only INFO-level
    # breadcrumbs. Originally "flag loudly, don't abort" (a WARNING); now
    # FAILS HARD by default — at CONUS scale genuine hydro-flattening only
    # takes out ~11-22%, so a legitimate run sits near ~0.8 measured, and
    # `< 0.5` means a systemic read failure, not flattening. Configurable via
    # `ctx.dprst_depth_min_measured_frac` (`dprst_depth_min_measured_frac` in
    # depstor_rasters.yml, default 0.5); set it to `0` (or negative) to
    # disable the guard as an escape hatch for a legitimately
    # high-flattening small fabric.
    measured_fraction = n_computed / n_total if n_total else 1.0
    threshold = ctx.dprst_depth_min_measured_frac
    if threshold > 0 and measured_fraction < threshold:
        raise RuntimeError(
            f"only {100 * measured_fraction:.1f}% ({n_computed}/{n_total}) of "
            f"dprst polygons have a computed depth — below the "
            f"dprst_depth_min_measured_frac threshold ({threshold:.2f}). This "
            f"almost certainly indicates a systemic read failure (S3 outage, "
            f"HPC network/firewall regression — this project has hit this "
            f"class before, see proj_network_firewall_inf) rather than "
            f"genuine hydro-flattening, which fails only ~11-22% of polygons "
            f"at CONUS scale. Investigate the 3DEP /vsicurl/ reads before "
            f"re-running. Set dprst_depth_min_measured_frac to 0 (or "
            f"negative) in depstor_rasters.yml to disable this guard, only "
            f"if this fabric is legitimately high-flattening."
        )

    _log_achieved_resolution(dprst, merged, logger)

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


# Diagnostic columns added on top of the core (COMID, method, dprst_depth_m,
# geometry) provenance schema (#173 Oregon validation Risk 3) — resolution/
# ftype/ecoregion/measured_max_m/hollister_max_m all survive on `filled`
# after `_tag_polygons` (ftype/ecoregion) + `_compute_depths`/`_fill_and_join`
# (resolution/measured_max_m/hollister_max_m via `_DEPTH_COLUMNS`), so no
# extra computation is needed here — just don't drop them on the way out.
# `source`/`interior_coverage` (issue #223) are provenance, not PRMS
# parameters (this repo's `prms.provenance` convention, CLAUDE.md) — `source`
# is what the re-run validation diffs to know which project each depth came
# from, and `interior_coverage` is the prerequisite a future fill.py donor
# filter needs (see `_DEPTH_COLUMNS`'s comment); adding that filter itself is
# out of scope here.
_PROVENANCE_DIAGNOSTIC_COLUMNS = [
    "resolution", "ftype", "ecoregion", "measured_max_m", "hollister_max_m",
    "source", "interior_coverage",
]


def _write_polygon_provenance(filled: gpd.GeoDataFrame, depth_path: Path, logger) -> Path:
    """Persist per-polygon provenance + diagnostics next to `dprst_depth.tif`.

    Core columns are `COMID`, `method`, `dprst_depth_m`, `geometry` — see
    the module docstring (point 7) for why this file exists at all
    (`burn_depth` only burns `dprst_depth_m` onto the raster, so the
    per-polygon fill `method` label would otherwise be lost). On top of
    that, also persist `resolution`/`ftype`/`ecoregion`/`measured_max_m`/
    `hollister_max_m` (#173 Oregon validation Risk 3) so a CONUS-scale
    provenance analysis (1 m vs 10 m split, FTYPE/ecoregion breakdown,
    measured-vs-Hollister comparison) doesn't require recomputing the
    polygon set. `area_weighted_provenance` only reads `method`/`geometry`
    back, so these extra columns are additive and don't change that
    reader's behavior.
    """
    out_path = depth_path.parent / POLYGON_PROVENANCE_FILENAME
    core_cols = ["COMID", "method", "dprst_depth_m"]
    keep_cols = [
        c for c in core_cols + _PROVENANCE_DIAGNOSTIC_COLUMNS + ["geometry"] if c in filled.columns
    ]
    missing_diagnostics = [c for c in _PROVENANCE_DIAGNOSTIC_COLUMNS if c not in filled.columns]
    if missing_diagnostics:
        logger.warning(
            "  polygon provenance: expected diagnostic column(s) %s missing from the "
            "per-polygon frame — writing without them",
            missing_diagnostics,
        )
    gdf = gpd.GeoDataFrame(filled[keep_cols], geometry="geometry", crs=filled.crs)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(out_path)
    logger.info("  polygon provenance: wrote %d rows (columns: %s) -> %s", len(gdf), keep_cols, out_path)
    return out_path


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    outputs = step_cfg["outputs"]
    depth_path = ctx.resolve_output(outputs["dprst_depth"])
    op_flow_path = ctx.resolve_output(outputs["op_flow_thres"])
    landmask_path = ctx.require("landmask")
    dprst_mask_path = ctx.require("dprst")

    logger.info("--- dprst_depth ---")
    logger.info("  Depth out        : %s", depth_path)
    logger.info("  op_flow_thres out: %s", op_flow_path)

    if depth_path.exists() and op_flow_path.exists() and not ctx.force:
        logger.info("  Both outputs exist — skipping (pass --force to rebuild)")
        return {"dprst_depth": depth_path, "op_flow_thres": op_flow_path}

    dprst = _load_dprst_polygons(ctx, logger)
    dprst = _tag_polygons(dprst, ctx, logger)
    depth_df = _compute_depths(dprst, ctx, step_cfg, logger)
    filled = _fill_and_join(dprst, depth_df, ctx, logger)

    burn_depth(filled, ctx.template_path, landmask_path, dprst_mask_path, depth_path, logger)
    _write_op_flow_thres(ctx, op_flow_path, logger)
    _write_polygon_provenance(filled, depth_path, logger)

    return {"dprst_depth": depth_path, "op_flow_thres": op_flow_path}
