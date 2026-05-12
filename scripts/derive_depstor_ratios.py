"""Derive the four PRMS depstor Level-5 ratio params from merged fraction CSVs.

Reads pairs of merged gdptools-exactextract CSVs from `{fabric}/params/merged/`
and divides `count_numerator / count_denominator` per HRU, applying optional
clamp-at-1 and NaN/inf -> 0. Writes one output CSV per PRMS param with the
schema `nat_hru_id, <param_name>` so downstream consumers (KNN gap-fill,
parameter database merge) can pick the column up by name.

Mirrors the ArcPy reference at docs/0b_TB_depr_stor.py:
- getSro_to_dprst_perv  (lines 36-99)
- getSro_to_dprst_imperv (lines 101-156; uses the documented intent rather
  than the active code, see depstor_drains_imperv_raster.yml comment)
- getCarea / getSmidx_coef / getCarea_max (lines 220-312)

The fraction-CSV `count` column carries weighted cell counts (sum of partial-
pixel coverage fractions × value). Since both sides of every ratio share the
same per-HRU coverage convention, the partial-pixel weighting cancels and the
result matches the integer cell-count ratio used in ArcPy.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging


def compute_ratio(
    numerator_df: pd.DataFrame,
    denominator_df: pd.DataFrame,
    id_feature: str,
    count_column: str,
    param_name: str,
    clamp_to_one: bool,
) -> tuple[pd.DataFrame, dict]:
    """Per-HRU ratio with clamp + div-by-zero/NaN -> 0.

    Returns (df, stats) where `df` has two columns (`id_feature`, `param_name`)
    and `stats` is a dict with QA counts: `n_total`, `n_zero_denom` (HRUs whose
    denominator was 0 or NaN -> collapsed to 0), and `n_clamped` (HRUs whose
    raw ratio exceeded 1.0; 0 when `clamp_to_one=False`).
    """
    # Use long, collision-proof names so we never shadow an input column that
    # happens to be called something short.
    cols = [id_feature, count_column]
    num = numerator_df[cols].rename(columns={count_column: "__numerator__"})
    den = denominator_df[cols].rename(columns={count_column: "__denominator__"})
    merged = num.merge(den, on=id_feature, how="outer").sort_values(id_feature).reset_index(drop=True)

    # Treat missing-row joins (one CSV has an HRU the other does not) as 0.
    merged["__numerator__"] = merged["__numerator__"].fillna(0.0)
    merged["__denominator__"] = merged["__denominator__"].fillna(0.0)

    den_values = merged["__denominator__"].to_numpy()
    num_values = merged["__numerator__"].to_numpy()
    n_zero_denom = int((den_values == 0).sum())

    # numpy float division: 0/0 -> nan, x/0 -> inf. Mask both to 0.
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = num_values / den_values
    ratio = np.where(np.isfinite(ratio), ratio, 0.0)

    n_clamped = 0
    if clamp_to_one:
        n_clamped = int((ratio > 1.0).sum())
        ratio = np.minimum(ratio, 1.0)

    stats = {"n_total": len(merged), "n_zero_denom": n_zero_denom, "n_clamped": n_clamped}
    df = pd.DataFrame({id_feature: merged[id_feature].to_numpy(), param_name: ratio})
    return df, stats


def main():
    parser = argparse.ArgumentParser(
        description="Derive PRMS depstor ratio params from merged fraction CSVs."
    )
    parser.add_argument("--config", required=True, help="Path to depstor_ratio_params.yml")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    args = parser.parse_args()

    logger = configure_logging("derive_depstor_ratios")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )

    input_dir = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])
    id_feature = config["id_feature"]
    count_column = config["count_column"]
    ratios = config["ratios"]

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("=== derive_depstor_ratios ===")
    logger.info("Input  dir: %s", input_dir)
    logger.info("Output dir: %s", output_dir)
    logger.info("Ratios    : %d", len(ratios))

    for spec in ratios:
        name = spec["name"]
        num_path = input_dir / spec["numerator_file"]
        den_path = input_dir / spec["denominator_file"]
        out_path = output_dir / spec["output_file"]
        clamp = bool(spec.get("clamp_to_one", False))

        for p in (num_path, den_path):
            if not p.exists():
                raise FileNotFoundError(
                    f"Required input CSV not found: {p}\n"
                    f"Run scripts/merge_params.py for the corresponding fraction first."
                )

        logger.info("--- %s ---", name)
        logger.info("  Numerator   : %s", num_path)
        logger.info("  Denominator : %s", den_path)
        num_df = pd.read_csv(num_path)
        den_df = pd.read_csv(den_path)
        for needed in (id_feature, count_column):
            if needed not in num_df.columns:
                raise ValueError(f"Column '{needed}' missing in {num_path}")
            if needed not in den_df.columns:
                raise ValueError(f"Column '{needed}' missing in {den_path}")

        out_df, stats = compute_ratio(num_df, den_df, id_feature, count_column, name, clamp)
        out_df.to_csv(out_path, index=False)
        n_nonzero = int((out_df[name] > 0).sum())
        logger.info(
            "  Wrote %d rows -> %s  (%d HRUs with %s > 0)",
            len(out_df), out_path, n_nonzero, name,
        )
        if stats["n_zero_denom"]:
            logger.info(
                "  %d/%d HRUs had zero/missing denominator (collapsed to 0)",
                stats["n_zero_denom"], stats["n_total"],
            )
        if clamp and stats["n_clamped"]:
            logger.info(
                "  %d/%d HRUs had raw ratio > 1.0 (clamped to 1.0)",
                stats["n_clamped"], stats["n_total"],
            )

    logger.info("=== derive_depstor_ratios complete ===")


if __name__ == "__main__":
    main()
