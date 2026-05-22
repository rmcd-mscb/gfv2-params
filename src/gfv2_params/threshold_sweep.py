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
