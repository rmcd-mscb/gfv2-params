"""Depression-respecting (breached) FDR — an additional derived raster (#147).

Opt-in shared-raster step. For each VPU it breaches the already-staged
``Hydrodem_merged_fixed_<vpu>.tif`` with WhiteboxTools
``BreachDepressionsLeastCost`` (least-cost outlet carving that PRESERVES real
closed depressions, unlike a full fill) and runs ``D8Pointer`` to produce
``Fdr_breached_<vpu>.tif``. This is strictly ADDITIONAL: it never touches the
production ``fdr.vrt`` (NHDPlus FdrFac), ``Fdr_hydrodem_*`` (richdem fill-all),
or ``Fdr_merged_*``. It exists to A/B ``drains_to_dprst`` contributing area
against the fully-filled flow field — see
docs/superpowers/specs/2026-06-29-depression-respecting-fdr-design.md and #147.

Single-purpose (FDR only): no FAC/slope/aspect/TWI — those are TWI-side
derivatives produced by compute_dem_derivatives, irrelevant to depression
routing. Reuses that module's nodata-fix and WBT-runner helpers.

Registered in BUILDERS/STEP_ORDER but NOT in the default ``steps:`` list of
configs/shared_rasters/shared_rasters.yml — users opt in explicitly.

Outputs (per VPU, in {data_root}/shared/per_vpu/<vpu>/):
- Hydrodem_breached_<vpu>.tif  (WBT BreachDepressionsLeastCost, LZW no predictor)
- Fdr_breached_<vpu>.tif       (WBT D8 pointer, Esri encoding)
"""

from __future__ import annotations

from pathlib import Path

from gfv2_params.wbt import find_whitebox_tools_binary

# DEM_NODATA is re-exported here so callers/tests can import it from this module
# without reaching into compute_dem_derivatives directly.
from .compute_dem_derivatives import DEM_NODATA, _fix_dem_nodata, _run_wbt  # noqa: F401
from .context import SharedRastersContext

# BreachDepressionsLeastCost search radius (cells). Too small -> pits that can't
# be breached within --dist fall back to fill (re-introducing the #145
# over-connection); too large -> over-carves real depressions. Start at 100 and
# tune on VPU 09 (see #147), then pin the chosen value here with rationale.
BREACH_DIST = 100
# --fill: fill any pit not breachable within --dist. NOTE the consequence: the
# resulting FDR has NO interior code-0 cells at all. The original rationale here
# was that the routing kernel "cannot leave" an interior sink, which is wrong for
# this pipeline -- `d8_routing` treats code 0 as a TERMINUS by design, and that is
# the entire basis of the endorheic classifier's Signal A (terminus-inside-itself)
# and of the classifier/router agreement CLAUDE.md relies on. Interior sinks are
# the signal here, not a hazard.
#
# Keep True anyway, for the narrow purpose this raster serves: it exists only for
# the #147 A/B on contributing area, where a breach-or-fill hybrid is the right
# comparator against a global fill. But do NOT reach for `Fdr_breached` as an
# endorheic-classifier input -- Signal A would find nothing to read and would go
# silently dark. A depression-PRESERVING variant (fill=False plus a depth/area
# threshold) would be a different raster with a different purpose.
BREACH_FILL = True


def _breach_and_d8(dem_fixed: Path, dem_breached: Path, fdr_out: Path,
                   runner: str, logger, *, dist: int = BREACH_DIST,
                   fill: bool = BREACH_FILL) -> None:
    """WBT BreachDepressionsLeastCost on the fixed DEM, then D8Pointer."""
    breach_args = [
        f"--dem={dem_fixed}",
        f"--output={dem_breached}",
        f"--dist={dist}",
    ]
    if fill:
        breach_args.append("--fill")
    _run_wbt(runner, "BreachDepressionsLeastCost", breach_args, logger)
    _run_wbt(
        runner, "D8Pointer",
        [f"--dem={dem_breached}", f"--output={fdr_out}", "--esri_pntr"],
        logger,
    )


def _process_vpu(vpu: str, input_dir: Path, output_dir: Path, runner: str,
                 force: bool, logger, *, dist: int = BREACH_DIST,
                 fill: bool = BREACH_FILL) -> None:
    vpu_dir = output_dir / vpu
    vpu_dir.mkdir(parents=True, exist_ok=True)

    dem_src = input_dir / vpu / f"Hydrodem_merged_{vpu}.tif"
    dem_fixed = vpu_dir / f"Hydrodem_merged_fixed_{vpu}.tif"
    dem_breached = vpu_dir / f"Hydrodem_breached_{vpu}.tif"
    fdr_out = vpu_dir / f"Fdr_breached_{vpu}.tif"

    if not force and fdr_out.exists():
        logger.info("[VPU %s] breached FDR exists (use --force to rebuild): %s",
                    vpu, fdr_out)
        return

    # Reuse the fixed DEM if compute_dem_derivatives already staged it; else
    # re-encode nodata from the source Hydrodem (shared helper).
    if force or not dem_fixed.exists():
        if not dem_src.exists():
            raise FileNotFoundError(
                f"Neither fixed DEM nor source Hydrodem found for VPU {vpu}: "
                f"{dem_fixed} / {dem_src}"
            )
        logger.info("[VPU %s] re-encoding Hydrodem nodata -> fixed DEM", vpu)
        _fix_dem_nodata(dem_src, dem_fixed, logger)
    else:
        logger.info("[VPU %s] reusing staged fixed DEM: %s", vpu, dem_fixed)

    logger.info("[VPU %s] --- WBT BreachDepressionsLeastCost (dist=%d, fill=%s) ---",
                vpu, dist, fill)
    _breach_and_d8(dem_fixed, dem_breached, fdr_out, runner, logger, dist=dist, fill=fill)
    logger.info("[VPU %s] wrote breached FDR: %s", vpu, fdr_out)


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Breach + D8 every VPU in ``ctx.vpus``. Opt-in; returns {} (per-VPU)."""
    input_dir = Path(step_cfg.get("input_dir", ctx.per_vpu_dir))
    output_dir = Path(step_cfg.get("output_dir", ctx.per_vpu_dir))

    if not ctx.vpus:
        logger.warning("compute_breached_fdr: ctx.vpus is empty, nothing to do")
        return {}

    dist = int(step_cfg.get("breach_dist", BREACH_DIST))
    fill = bool(step_cfg.get("breach_fill", BREACH_FILL))

    runner = find_whitebox_tools_binary()
    logger.info("WhiteboxTools binary: %s", runner)
    for vpu in ctx.vpus:
        _process_vpu(vpu, input_dir, output_dir, runner, ctx.force, logger,
                     dist=dist, fill=fill)
    return {}
