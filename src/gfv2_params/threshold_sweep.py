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

from dataclasses import dataclass

import numpy as np
import pandas as pd


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
