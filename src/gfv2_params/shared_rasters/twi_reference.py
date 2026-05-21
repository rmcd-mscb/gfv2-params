"""Valid-land TWI percentile cutoffs for distribution-invariant carea_max /
smidx_coef (issues #94 + #55 Stage 1).

The cutoff that classifies a cell as "wet" used to be a hardcoded TWI value
(8.0 / 15.6) calibrated to the ArcPy TWI distribution. Here we derive it from
the data instead: the P-th percentile of valid-land TWI over a reference
population (per-VPU or CONUS). Because it is recomputed from whatever TWI
source is in play, swapping the source preserves each cell's rank, so the
parameters become invariant to the source.

This module holds the pure math (percentile / CDF-inversion) plus the
`build_twi_reference` shared-raster builder that samples the staged TWI tiles.
"""

from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import rasterio

from gfv2_params.config import VPU_RASTER_MAP


def _valid(values: np.ndarray, nodata: float | None) -> np.ndarray:
    """Return the finite, non-nodata subset as a 1-D float64 array."""
    v = np.asarray(values, dtype="float64").ravel()
    mask = np.isfinite(v)
    if nodata is not None:
        mask &= v != nodata
    return v[mask]


def percentile_of_values(values: np.ndarray, ps, nodata: float | None = None):
    """The P-th percentile(s) of the valid values. `ps` is a list of [0,100]."""
    valid = _valid(values, nodata)
    if valid.size == 0:
        raise ValueError("percentile_of_values: no valid (finite, non-nodata) values")
    return [float(x) for x in np.percentile(valid, ps)]


def rank_of_value(values: np.ndarray, value: float, nodata: float | None = None) -> float:
    """Percentile rank (0-100) of `value` in the valid distribution: the
    fraction of valid values <= `value`, x100. Inverse of percentile_of_values;
    used to find what percentile the legacy 8.0 / 15.6 occupy."""
    valid = _valid(values, nodata)
    if valid.size == 0:
        raise ValueError("rank_of_value: no valid values")
    return float(100.0 * np.count_nonzero(valid <= value) / valid.size)


# Legacy ArcPy thresholds (docs/0b_TB_depr_stor.py); used to derive default
# percentiles by inversion so percentile-mode reproduces VPU 01 by construction.
LEGACY_CAREA_THRESHOLD = 8.0
LEGACY_SMIDX_THRESHOLD = 15.6

_TABLE_FIELDS = ["source", "scope", "vpu", "p_carea", "p_smidx", "t_carea", "t_smidx"]


def raster_vpus(vpus):
    """Dedup detailed VPU labels to ordered-unique raster VPUs.

    03N/03S/03W -> 03, 10L/10U -> 10 (via VPU_RASTER_MAP); pass-through otherwise.
    The per-VPU TWI tiles and land masks live in the raster-VPU namespace, so the
    reference table is keyed by raster VPU to match.
    """
    out = []
    for v in vpus:
        rv = VPU_RASTER_MAP.get(str(v), str(v))
        if rv not in out:
            out.append(rv)
    return out


def assemble_reference_table(
    source: str,
    vpus: list[str],
    sampler,
    *,
    arcpy_vpu01_sample=None,
    legacy_carea: float = LEGACY_CAREA_THRESHOLD,
    legacy_smidx: float = LEGACY_SMIDX_THRESHOLD,
    p_carea: float | None = None,
    p_smidx: float | None = None,
    nodata: float | None = -9999.0,
) -> list[dict]:
    """Build the reference-percentile rows for one TWI source.

    `sampler(vpu) -> 1-D array` supplies valid-land TWI samples per VPU. If
    `p_carea`/`p_smidx` are not given, they are derived by inverting
    legacy_carea/legacy_smidx through `arcpy_vpu01_sample` (the CDF-inversion
    default). One `vpu`-scope row per VPU plus one pooled `conus` row.
    """
    if p_carea is None or p_smidx is None:
        if arcpy_vpu01_sample is None:
            raise ValueError(
                "assemble_reference_table: provide p_carea/p_smidx or "
                "arcpy_vpu01_sample to derive them by inversion"
            )
        p_carea = rank_of_value(arcpy_vpu01_sample, legacy_carea, nodata=nodata)
        p_smidx = rank_of_value(arcpy_vpu01_sample, legacy_smidx, nodata=nodata)

    rows: list[dict] = []
    pooled = []
    for vpu in vpus:
        s = sampler(vpu)
        valid = _valid(s, nodata)
        if valid.size == 0:
            continue
        pooled.append(valid)
        tc, ts = percentile_of_values(valid, [p_carea, p_smidx])
        rows.append({
            "source": source, "scope": "vpu", "vpu": vpu,
            "p_carea": round(p_carea, 4), "p_smidx": round(p_smidx, 4),
            "t_carea": tc, "t_smidx": ts,
        })
    if pooled:
        allv = np.concatenate(pooled)
        tc, ts = percentile_of_values(allv, [p_carea, p_smidx])
        rows.append({
            "source": source, "scope": "conus", "vpu": "CONUS",
            "p_carea": round(p_carea, 4), "p_smidx": round(p_smidx, 4),
            "t_carea": tc, "t_smidx": ts,
        })
    return rows


def write_reference_table(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_TABLE_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _sample_land_masked_twi(twi_path: Path, mask_path: Path, decimate: int, nodata):
    """Decimated valid-land TWI sample (1-D). Reads both rasters at a coarse
    overview to keep CONUS-scale sampling cheap; mask==1 marks land."""
    with rasterio.open(twi_path) as t:
        oh, ow = max(1, t.height // decimate), max(1, t.width // decimate)
        twi = t.read(1, out_shape=(1, oh, ow))
        tnod = t.nodata if nodata is None else nodata
        t_hw = (t.height, t.width)
    with rasterio.open(mask_path) as m:
        if (m.height, m.width) != t_hw:
            raise ValueError(
                f"TWI/mask grid mismatch for percentile sampling: "
                f"TWI {twi_path.name}={t_hw}, mask {mask_path.name}="
                f"({m.height},{m.width}); they must share a grid."
            )
        land = m.read(1, out_shape=(1, oh, ow)) == 1
    sample = twi[land]
    return _valid(sample, tnod)


# Maps the depstor "twi family" to the per-VPU tile filename prefix.
_SOURCE_PREFIX = {"arcpy": "Twi_merged", "hydrodem": "Twi_hydrodem"}


def build(step_cfg: dict, ctx, logger) -> dict:
    """shared-raster builder: write reference-percentile CSVs for each source.

    step_cfg keys:
      sources    list[str] subset of {"arcpy","hydrodem"} (default both)
      percentiles {carea_max, smidx}  optional explicit percentiles; if absent,
                  derived by inverting 8.0/15.6 on the ArcPy VPU 01 sample.
      decimate   int overview factor for sampling (default 20)
    """
    sources = step_cfg.get("sources", ["arcpy", "hydrodem"])
    pcfg = step_cfg.get("percentiles", {})
    p_carea = pcfg.get("carea_max")
    p_smidx = pcfg.get("smidx")
    decimate = int(step_cfg.get("decimate", 20))
    nodata = -9999.0
    out_dir = ctx.conus_dir
    produced = {}

    def mask_path(vpu):
        return ctx.per_vpu_dir / vpu / f"land_mask_{vpu}.tif"

    # ArcPy VPU 01 sample drives the inverted default percentiles.
    arcpy01 = None
    if p_carea is None or p_smidx is None:
        twi01 = ctx.per_vpu_dir / "01" / "Twi_merged_01.tif"
        arcpy01 = _sample_land_masked_twi(twi01, mask_path("01"), decimate, nodata)
        logger.info("build_twi_reference: derived default percentiles by "
                    "inverting 8.0/15.6 on ArcPy VPU 01 (%d samples)", arcpy01.size)

    for source in sources:
        prefix = _SOURCE_PREFIX[source]

        def sampler(vpu, _prefix=prefix):
            twi = ctx.per_vpu_dir / vpu / f"{_prefix}_{vpu}.tif"
            return _sample_land_masked_twi(twi, mask_path(vpu), decimate, nodata)

        rows = assemble_reference_table(
            source=source, vpus=raster_vpus(ctx.vpus), sampler=sampler,
            arcpy_vpu01_sample=arcpy01, p_carea=p_carea, p_smidx=p_smidx,
            nodata=nodata,
        )
        out_path = out_dir / f"twi_reference_percentiles.{source}.csv"
        write_reference_table(rows, out_path)
        logger.info("build_twi_reference: wrote %d rows -> %s", len(rows), out_path)
        produced[f"twi_reference_{source}"] = out_path

    return produced
