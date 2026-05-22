"""Fast threshold-iteration for carea_max / smidx_coef (issue #55).

carea_max and smidx_coef are the SAME per-HRU function evaluated at two TWI
thresholds:

    f_hru(t) = clip( (n_perv_onstream + #(perv non-onstream cells, TWI > t)) / n_perv , 0, 1 )

So we extract, once per fabric, each HRU's pervious-cell TWI distribution (as a
histogram) plus the two scalar counts, then evaluate any threshold in-memory.
This module holds the artifact, the pure sweep math, and the extraction driver
(`build_artifact`) that mirrors `compute_carea_map_binary`.
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from .depstor import RasterInfo


@dataclass
class CareaTwiArtifact:
    ids: np.ndarray              # (n_hru,) id_feature values
    vpu: np.ndarray              # (n_hru,) object  per-HRU VPU label
    n_perv: np.ndarray           # (n_hru,) pervious cell count (denominator)
    n_perv_onstream: np.ndarray  # (n_hru,) pervious AND on-stream (threshold-independent)
    hist: np.ndarray             # (n_hru, n_bins) TWI histogram of perv non-onstream cells
    bin_edges: np.ndarray        # (n_bins + 1,) histogram bin edges
    ref_pctl: np.ndarray         # (n_grid,) reference percentile grid (0..100)
    ref_value: np.ndarray        # (n_grid,) valid-land TWI value at each percentile
    fabric: str
    twi_source: str

    def save(self, path) -> None:
        np.savez_compressed(
            path, ids=self.ids, vpu=self.vpu.astype("U8"),
            n_perv=self.n_perv, n_perv_onstream=self.n_perv_onstream,
            hist=self.hist, bin_edges=self.bin_edges,
            ref_pctl=self.ref_pctl, ref_value=self.ref_value,
            fabric=np.array(self.fabric), twi_source=np.array(self.twi_source),
        )

    @classmethod
    def load(cls, path) -> "CareaTwiArtifact":
        z = np.load(path, allow_pickle=False)
        return cls(
            ids=z["ids"], vpu=z["vpu"].astype(object),
            n_perv=z["n_perv"], n_perv_onstream=z["n_perv_onstream"],
            hist=z["hist"], bin_edges=z["bin_edges"],
            ref_pctl=z["ref_pctl"], ref_value=z["ref_value"],
            fabric=str(z["fabric"]), twi_source=str(z["twi_source"]),
        )


def _bin_centers(bin_edges: np.ndarray) -> np.ndarray:
    return 0.5 * (bin_edges[:-1] + bin_edges[1:])


def evaluate_threshold(artifact: CareaTwiArtifact, t: float) -> np.ndarray:
    """Per-HRU parameter f_hru(t) in [0, 1] for a single TWI threshold."""
    centers = _bin_centers(artifact.bin_edges)
    above = artifact.hist[:, centers > t].sum(axis=1)
    num = artifact.n_perv_onstream + above
    denom = artifact.n_perv
    with np.errstate(divide="ignore", invalid="ignore"):
        param = np.where(denom > 0, num / denom, 0.0)
    return np.clip(param, 0.0, 1.0)


def value_to_percentile(artifact: CareaTwiArtifact, t: float) -> float:
    """Percentile (0..100) of a TWI value in the valid-land reference grid."""
    return float(np.interp(t, artifact.ref_value, artifact.ref_pctl))


def percentile_to_value(artifact: CareaTwiArtifact, p: float) -> float:
    """TWI value at a percentile (0..100) of the valid-land reference grid."""
    return float(np.interp(p, artifact.ref_pctl, artifact.ref_value))


def sweep(artifact: CareaTwiArtifact, t_grid: np.ndarray) -> pd.DataFrame:
    """Per-threshold summary stats of the per-HRU parameter (sensitivity curve)."""
    rows = []
    for t in np.asarray(t_grid, dtype="float64"):
        p = evaluate_threshold(artifact, float(t))
        rows.append({
            "threshold": float(t),
            "mean": float(p.mean()),
            "median": float(np.median(p)),
            "frac_zero": float((p == 0.0).mean()),
            "frac_one": float((p >= 1.0).mean()),
        })
    return pd.DataFrame(rows)


def accumulate_strip(
    hru_idx: np.ndarray, perv: np.ndarray, onstream: np.ndarray, twi: np.ndarray,
    land_valid: np.ndarray, twi_nodata, bin_edges: np.ndarray,
    n_perv: np.ndarray, n_perv_onstream: np.ndarray, hist: np.ndarray,
    land_twi_hist: np.ndarray,
) -> None:
    """Accumulate one aligned strip into the per-HRU and global accumulators.

    `hru_idx` is the per-cell HRU row-index (>=0 valid; <0 = no HRU). Mirrors
    `compute_carea_map_binary`: a cell contributes to a parameter when it is land
    & pervious & (TWI > t OR on-stream). Here we bin pervious non-onstream cells
    by TWI so any threshold can be evaluated later; on-stream pervious cells are
    counted separately (threshold-independent). `land_twi_hist` accumulates ALL
    land valid-TWI cells for the percentile reference grid.
    """
    if twi_nodata is not None and isinstance(twi_nodata, float) and np.isnan(twi_nodata):
        twi_valid = ~np.isnan(twi)
    elif twi_nodata is not None:
        twi_valid = (twi != twi_nodata) & ~np.isnan(twi)
    else:
        twi_valid = ~np.isnan(twi)

    has_hru = hru_idx >= 0
    land_hru = land_valid & has_hru

    # Global valid-land TWI histogram (reference distribution).
    gl = land_valid & twi_valid
    if gl.any():
        gbins = np.clip(np.digitize(twi[gl], bin_edges) - 1, 0, len(bin_edges) - 2)
        np.add.at(land_twi_hist, gbins, 1)

    is_perv = land_hru & (perv == 1)
    np.add.at(n_perv, hru_idx[is_perv], 1)

    is_os = is_perv & (onstream == 1)
    np.add.at(n_perv_onstream, hru_idx[is_os], 1)

    is_hist = is_perv & (onstream != 1) & twi_valid
    if is_hist.any():
        bins = np.clip(np.digitize(twi[is_hist], bin_edges) - 1, 0, len(bin_edges) - 2)
        np.add.at(hist, (hru_idx[is_hist], bins), 1)


def reference_grid(land_twi_hist: np.ndarray, bin_edges: np.ndarray, pctls: np.ndarray):
    """Percentile->TWI-value grid from a valid-land TWI histogram.

    Returns (pctls, values) where values[i] is the TWI at percentile pctls[i].
    Uses bin centers and the cumulative fraction (CDF) of the histogram.
    """
    centers = _bin_centers(bin_edges)
    counts = np.asarray(land_twi_hist, dtype="float64")
    total = counts.sum()
    if total <= 0:
        raise ValueError("reference_grid: empty valid-land TWI histogram")
    cdf_pct = 100.0 * np.cumsum(counts) / total   # percentile at each bin's upper extent
    values = np.interp(pctls, cdf_pct, centers)
    return np.asarray(pctls, dtype="float64"), values


_STRIP_ROWS = 1024


def build_artifact(
    *, fabric: str, twi_raster: Path, template_raster: Path, hru_gpkg: Path,
    hru_layer: str, id_feature: str, perv_path: Path, onstream_path: Path,
    landmask_path: Path, vpu_column: str | None, twi_source: str,
    bin_min: float = 0.0, bin_max: float = 30.0, bin_width: float = 0.05,
    n_ref_grid: int = 1001, logger: logging.Logger | None = None,
) -> CareaTwiArtifact:
    """Single pass over a fabric's depstor stack -> CareaTwiArtifact.

    Mirrors compute_carea_map_binary (land & perv & (twi>t | onstream)) so a swept
    threshold reproduces a production carea_map run within bin resolution.
    """
    log = logger or logging.getLogger("build_carea_twi_artifact")
    info = RasterInfo.from_path(template_raster)
    bin_edges = np.arange(bin_min, bin_max + bin_width, bin_width)
    n_bins = len(bin_edges) - 1

    hru = gpd.read_file(hru_gpkg, layer=hru_layer)
    hru = hru[hru.geometry.notna() & ~hru.geometry.is_empty]
    if hru.crs != info.crs:
        hru = hru.to_crs(info.crs)
    ids = hru[id_feature].to_numpy()
    order = np.argsort(ids)
    ids = ids[order]
    n_hru = len(ids)
    id_to_idx = {int(v): i for i, v in enumerate(ids)}
    if vpu_column and vpu_column in hru.columns:
        vpu_by_id = dict(zip(hru[id_feature].to_numpy(), hru[vpu_column].astype(str)))
        vpu = np.array([vpu_by_id[int(v)] for v in ids], dtype=object)
    else:
        vpu = np.array(["" for _ in ids], dtype=object)
    log.info("build_artifact: fabric=%s n_hru=%d grid=%dx%d bins=%d",
             fabric, n_hru, info.width, info.height, n_bins)

    # Per-cell HRU row-index raster (full template; oregon-scale OK). int32 -1=none.
    geoms = hru.geometry.values
    idxvals = np.array([id_to_idx[int(v)] for v in hru[id_feature].to_numpy()], dtype="int32")
    hru_idx_full = rasterize(
        ((g, int(i)) for g, i in zip(geoms, idxvals)),
        out_shape=(info.height, info.width), transform=info.transform,
        fill=-1, dtype="int32", all_touched=True,
    )

    n_perv = np.zeros(n_hru, "int64")
    n_perv_onstream = np.zeros(n_hru, "int64")
    hist = np.zeros((n_hru, n_bins), "int64")
    land_twi_hist = np.zeros(n_bins, "int64")

    with ExitStack() as stack:
        land_src = stack.enter_context(rasterio.open(landmask_path))
        perv_src = stack.enter_context(rasterio.open(perv_path))
        onstream_src = stack.enter_context(rasterio.open(onstream_path))
        twi_src = stack.enter_context(rasterio.open(twi_raster))
        if twi_src.crs != info.crs:
            raise ValueError(f"TWI CRS {twi_src.crs} != template CRS {info.crs}")
        twi_vrt = stack.enter_context(WarpedVRT(
            twi_src, crs=info.crs, transform=info.transform,
            width=info.width, height=info.height,
            resampling=Resampling.nearest, nodata=twi_src.nodata,
        ))
        twi_nodata = twi_src.nodata
        for row_off in range(0, info.height, _STRIP_ROWS):
            h = min(_STRIP_ROWS, info.height - row_off)
            win = Window(0, row_off, info.width, h)
            accumulate_strip(
                hru_idx_full[row_off:row_off + h, :],
                perv_src.read(1, window=win),
                onstream_src.read(1, window=win),
                twi_vrt.read(1, window=win),
                land_src.read(1, window=win) == 1,
                twi_nodata, bin_edges,
                n_perv, n_perv_onstream, hist, land_twi_hist,
            )

    ref_pctl = np.linspace(0.0, 100.0, n_ref_grid)
    ref_pctl, ref_value = reference_grid(land_twi_hist, bin_edges, ref_pctl)
    log.info("build_artifact: %d pervious cells total; %d valid-land TWI cells",
             int(n_perv.sum()), int(land_twi_hist.sum()))
    return CareaTwiArtifact(
        ids=ids, vpu=vpu, n_perv=n_perv, n_perv_onstream=n_perv_onstream,
        hist=hist, bin_edges=bin_edges, ref_pctl=ref_pctl, ref_value=ref_value,
        fabric=fabric, twi_source=twi_source,
    )
