"""Pre-compute LULC derived rasters (resampled CNPY/keep + radiation transmission).

Library entrypoint for the shared-raster orchestrator. The thin CLI shell at
scripts/build_lulc_rasters.py delegates here so existing sbatch jobs keep
working unchanged.

Resamples canopy and keep rasters to match each LULC source's grid, then
computes the radiation transmission coefficient raster:
    radtrn = (cnpy * keep / 100) where lulc >= tree_threshold, else 0.

The orchestrator invocation supports a ``sources:`` list so a single step
block can process multiple LULC sources (NLCD, NALCMS, NHM v1.1, FORE-SCE)
in one walk. The legacy CLI processes one source per invocation via the
``--config`` flag, matching the existing per-source sbatch pattern.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

import rasterio
import yaml

from gfv2_params.raster_ops import compute_radtrn, resample

from .context import SharedRastersContext


def _is_valid_raster(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with rasterio.open(path):
            return True
    except Exception:
        return False


def _raster_info(path: Path) -> str:
    with rasterio.open(path) as src:
        h, w = src.height, src.width
        crs = src.crs.to_epsg() if src.crs else "unknown CRS"
        res_x, res_y = abs(src.transform.a), abs(src.transform.e)
    size_mb = path.stat().st_size / 1024 ** 2
    return (
        f"{h:,} rows x {w:,} cols | {res_x:.1f} m pixels | "
        f"EPSG:{crs} | {size_mb:.0f} MB"
    )


def _elapsed(t0: float) -> str:
    secs = time.time() - t0
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def _resolve_lulc_config(yaml_path: Path, data_root: Path) -> dict:
    """Load a LULC step config and resolve only the keys this builder uses.

    Deliberately bypasses ``gfv2_params.config.load_config`` to keep this
    builder fabric-independent — the zonal-pass keys (batch_dir, output_dir)
    that depend on ``{fabric}`` are irrelevant here and we don't want to
    require a fabric resolution at orchestrator-walk time.
    """
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise TypeError(f"LULC config must be a YAML mapping: {yaml_path}")

    # Substitute {data_root} + recursive self-referential scalars (e.g.,
    # `lulc_source: nhm_v11` lets `{lulc_source}` resolve in other values).
    replacements = {"data_root": str(data_root)}
    for k, v in cfg.items():
        if isinstance(v, (str, int, float)) and k not in replacements:
            s = str(v)
            if "{" not in s:
                replacements[k] = s

    def _sub(s: str) -> str:
        for ph, rep in replacements.items():
            s = s.replace(f"{{{ph}}}", rep)
        return s

    resolved = {}
    for k, v in cfg.items():
        if isinstance(v, str):
            resolved[k] = _sub(v)
        else:
            resolved[k] = v

    # Strip any keys we won't use that still contain {fabric} or other
    # unresolved placeholders — they belong to the zonal-pass step.
    UNUSED_KEYS = {"batch_dir", "output_dir", "target_layer", "id_feature",
                   "merged_file", "categorical", "crosswalk_file"}
    for k in list(resolved):
        if k in UNUSED_KEYS:
            continue
        if isinstance(resolved[k], str) and re.search(r"\{(\w+)\}", resolved[k]):
            raise ValueError(
                f"Unresolved placeholder in build_lulc_rasters config key "
                f"'{k}' from {yaml_path}: {resolved[k]!r}. Only {{data_root}} "
                f"and self-referential scalar keys are supported here."
            )
    return resolved


def _build_one_source(source_yaml: Path, ctx: SharedRastersContext, logger) -> dict:
    """Build the resampled CNPY / keep / radtrn rasters for one LULC source."""
    config = _resolve_lulc_config(source_yaml, ctx.data_root)
    lulc_source = config.get("lulc_source") or config.get("source_type") or "lulc"
    logger.info("=== build_lulc_rasters source=%s (config=%s) ===", lulc_source, source_yaml)

    lulc_raster = Path(config["source_raster"])
    cnpy_raster = Path(config["canopy_raster"])
    # Missing inputs are warn-and-skip, not fatal. The orchestrator processes
    # every LULC source in the `sources:` list; one source's data not being
    # staged shouldn't take down the whole step (and the other sources). The
    # warning is loud enough to surface in CI/log output, and the source is
    # cleanly omitted from `produced` so downstream consumers see the gap.
    if not lulc_raster.exists():
        logger.warning(
            "LULC raster not found for source=%s — skipping this source. "
            "Missing: %s", lulc_source, lulc_raster,
        )
        return {}
    if not cnpy_raster.exists():
        logger.warning(
            "Canopy raster not found for source=%s — skipping this source. "
            "Missing: %s", lulc_source, cnpy_raster,
        )
        return {}

    radtrn_raster_str = config.get("radtrn_raster")
    radtrn_raster = Path(radtrn_raster_str) if radtrn_raster_str else None
    keep_raster_str = config.get("keep_raster")
    keep_raster = Path(keep_raster_str) if keep_raster_str else None

    derived_dir = radtrn_raster.parent if radtrn_raster else ctx.data_root / "work" / "derived_rasters"
    derived_dir.mkdir(parents=True, exist_ok=True)

    logger.info("LULC raster  : %s", lulc_raster)
    logger.info("             : %s", _raster_info(lulc_raster))
    logger.info("Canopy raster: %s", cnpy_raster)
    logger.info("             : %s", _raster_info(cnpy_raster))
    if keep_raster:
        logger.info("Keep raster  : %s", keep_raster)
        if keep_raster.exists():
            logger.info("             : %s", _raster_info(keep_raster))
    logger.info("Output dir   : %s", derived_dir)

    produced = {}

    # Step 1: Resample CNPY to LULC grid.
    cnpy_resampled = derived_dir / f"cnpy_resampled_{lulc_source}.tif"
    if not _is_valid_raster(cnpy_resampled) or ctx.force:
        intermediate = derived_dir / "cnpy_resample_intermediate.tif"
        logger.info("--- Step 1/3: Resample canopy raster to LULC grid ---")
        logger.info("  (CONUS-scale GeoTIFF; may take 30-60 min)")
        t1 = time.time()
        # mask_values=(128,) — do NOT mask 0; value 0 = no canopy, a valid measurement
        resample(str(cnpy_raster), str(lulc_raster), str(intermediate), str(cnpy_resampled),
                 mask_values=(128,))
        logger.info("  Done in %s — written: %s", _elapsed(t1), cnpy_resampled)
        logger.info("  Result: %s", _raster_info(cnpy_resampled))
    else:
        logger.info("--- Step 1/3: Canopy resample already exists — skipping ---")
        logger.info("  %s | %s", cnpy_resampled, _raster_info(cnpy_resampled))
    produced[f"cnpy_resampled_{lulc_source}"] = cnpy_resampled

    if keep_raster is None:
        logger.info("--- Steps 2-3: No keep raster configured — skipping ---")
        return produced

    if not keep_raster.exists():
        # Same warn-and-skip philosophy as source/canopy missing, but the
        # cnpy resample already succeeded — return what we have so downstream
        # consumers can still use the cnpy_resampled output.
        logger.warning(
            "Keep raster not found for source=%s — skipping Steps 2-3 "
            "(keep resample + radtrn). Cnpy resample retained. Missing: %s",
            lulc_source, keep_raster,
        )
        return produced

    # Step 2: Resample keep to LULC grid.
    keep_resampled = derived_dir / f"keep_resampled_{lulc_source}.tif"
    if not _is_valid_raster(keep_resampled) or ctx.force:
        intermediate = derived_dir / "keep_resample_intermediate.tif"
        logger.info("--- Step 2/3: Resample keep raster to LULC grid ---")
        logger.info("  (CONUS-scale GeoTIFF; may take 30-60 min)")
        t2 = time.time()
        # mask_values=(128,) — do NOT mask 0; value 0 = fully deciduous, a valid measurement
        resample(str(keep_raster), str(lulc_raster), str(intermediate), str(keep_resampled),
                 mask_values=(128,))
        logger.info("  Done in %s — written: %s", _elapsed(t2), keep_resampled)
        logger.info("  Result: %s", _raster_info(keep_resampled))
    else:
        logger.info("--- Step 2/3: Keep resample already exists — skipping ---")
        logger.info("  %s | %s", keep_resampled, _raster_info(keep_resampled))
    produced[f"keep_resampled_{lulc_source}"] = keep_resampled

    # Step 3: Compute radiation transmission.
    if radtrn_raster is None:
        logger.warning(
            "keep_raster is configured but radtrn_raster path is missing from "
            "config; skipping radtrn"
        )
        return produced

    if not _is_valid_raster(radtrn_raster) or ctx.force:
        logger.info("--- Step 3/3: Compute radiation transmission raster ---")
        logger.info("  Output: %s (~5-20 min, block-wise CONUS)", radtrn_raster)
        t3 = time.time()
        compute_radtrn(
            str(lulc_raster), str(cnpy_resampled), str(keep_resampled), str(radtrn_raster),
        )
        logger.info("  Done in %s — written: %s", _elapsed(t3), radtrn_raster)
        logger.info("  Result: %s", _raster_info(radtrn_raster))
    else:
        logger.info("--- Step 3/3: Radiation transmission raster already exists — skipping ---")
        logger.info("  %s | %s", radtrn_raster, _raster_info(radtrn_raster))
    produced[f"radtrn_{lulc_source}"] = radtrn_raster

    return produced


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Build resampled CNPY/keep + radtrn rasters for each configured LULC source.

    step_cfg keys:
      sources — list of LULC step-config YAML paths (relative to repo root or
                absolute). Each is processed in order. Required.

    Returns a dict of produced raster paths keyed by ``{type}_{lulc_source}``
    (e.g., ``cnpy_resampled_nhm_v11``, ``radtrn_nhm_v11``).
    """
    sources = step_cfg.get("sources")
    if not sources:
        raise KeyError("build_lulc_rasters step requires `sources:` list of LULC config paths")

    repo_root = Path(__file__).resolve().parents[3]
    t_start = time.time()
    produced: dict = {}

    for src in sources:
        src_path = Path(src)
        if not src_path.is_absolute():
            src_path = repo_root / src_path
        if not src_path.exists():
            raise FileNotFoundError(f"LULC config not found: {src_path}")
        produced.update(_build_one_source(src_path, ctx, logger))

    logger.info("=== build_lulc_rasters complete in %s (%d sources) ===",
                _elapsed(t_start), len(sources))
    return produced
