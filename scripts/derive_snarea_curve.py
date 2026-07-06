"""Stage 2 driver: per-HRU snarea_curve from aggregated daily SWE/SCA.

Reads the per-year aggregated NCs written by Stage 1 (`derive_aggregate.py`,
`{data_root}/{fabric}/snodas/*_agg_*.nc`, dims `time`/`<id_feature>`, vars
`swe`/`scov`), builds per-HRU SNODAS cell counts from the gdptools weight CSV,
and writes the merged snarea_curve param CSV via `build_snarea_curve`.

Water fraction (selection criterion 4) is optional: if a per-HRU water-fraction
source is wired via --water_csv it is used; otherwise it defaults to 0 (a no-op
for the fabric, acceptable for Oregon per spec §8.3).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.snarea import DEFAULT_SNAREA_CURVE, build_snarea_curve
from gfv2_params.snarea.selection import SelectionParams

__all__ = [
    "read_daily_by_hru",
    "cells_from_weights",
    "validate_default_curve",
    "main",
]


def validate_default_curve(arr: np.ndarray) -> None:
    """Validate a `default_curve` override: shape, value range, non-increasing.

    Raises ValueError (not `assert`, which is stripped under `python -O`)
    naming which check failed.
    """
    if arr.shape != (11,):
        raise ValueError(f"default_curve must have shape (11,), got {arr.shape}")
    if not np.all((arr >= 0.0) & (arr <= 1.0)):
        raise ValueError(f"default_curve values must all be within [0.0, 1.0], got {arr}")
    if not np.all(np.diff(arr) <= 1e-9):
        raise ValueError(f"default_curve must be non-increasing, got {arr}")


def read_daily_by_hru(nc_dir: Path, id_dim: str) -> dict[int, pd.DataFrame]:
    """Concatenate per-year NCs into one daily DataFrame per HRU (index=date).

    The aggregated NC's HRU dimension is named ``id_dim`` (Stage 1 aggregates
    with `target_id = id_feature`, so the NC id-dim equals the fabric's
    `id_feature` directly).
    """
    files = sorted(Path(nc_dir).glob("*_agg_*.nc"))
    if not files:
        raise FileNotFoundError(f"No aggregated NCs in {nc_dir}")
    ds = xr.open_mfdataset(files, combine="by_coords", data_vars="minimal")
    df = ds[["swe", "scov"]].to_dataframe().reset_index()
    df = df.rename(columns={"scov": "sca"})
    out: dict[int, pd.DataFrame] = {}
    for hru_id, grp in df.groupby(id_dim):
        s = grp.set_index("time")[["swe", "sca"]].sort_index()
        out[int(hru_id)] = s
    return out


def cells_from_weights(weight_file: Path, id_col: str) -> dict[int, int]:
    """Per-HRU contributing SNODAS cell count from the gdptools weight table."""
    w = pd.read_csv(weight_file)
    return w.groupby(id_col).size().astype(int).to_dict()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--config", default="configs/snarea/snarea_curve.yml")
    ap.add_argument("--base_config", default="configs/base_config.yml")
    ap.add_argument("--water_csv", default=None,
                     help="Optional CSV with columns <id_feature>,water_frac")
    args = ap.parse_args()
    logger = configure_logging("derive_snarea_curve")

    cfg = load_config(Path(args.config), base_config_path=Path(args.base_config),
                       fabric=args.fabric)

    id_feature = require_config_key(cfg, "id_feature", "derive_snarea_curve")
    nc_dir = Path(cfg["snodas_agg_dir"])
    weight_file = Path(cfg["weight_file"])
    out_dir = Path(cfg["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    sel = SelectionParams(**cfg["selection"])
    default_curve = np.asarray(cfg.get("default_curve", DEFAULT_SNAREA_CURVE), dtype=float)
    validate_default_curve(default_curve)

    logger.info("Reading aggregated daily SWE/SCA from %s ...", nc_dir)
    daily = read_daily_by_hru(nc_dir, id_feature)
    logger.info("Loaded daily series for %d HRUs; reading SNODAS cell counts ...", len(daily))
    cells = cells_from_weights(weight_file, id_feature)
    water: dict[int, float] = {}
    if args.water_csv:
        wdf = pd.read_csv(args.water_csv)
        water = dict(zip(wdf[id_feature], wdf["water_frac"]))
        logger.info("Loaded water fraction for %d HRUs from %s", len(water), args.water_csv)

    logger.info("Deriving representative snarea_curve for %d HRUs ...", len(daily))
    table = build_snarea_curve(daily, cells, water, id_feature, sel, default_curve)
    out = out_dir / cfg["merged_file"]
    table.to_csv(out, index=False)
    logger.info("Wrote %d HRU curves -> %s", len(table), out)
    logger.info("Status counts:\n%s", table["sdc_status"].value_counts().to_string())


if __name__ == "__main__":
    main()
