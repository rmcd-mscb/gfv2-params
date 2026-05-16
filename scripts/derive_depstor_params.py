"""Drive every depstor aggregation step from configs/depstor_params.yml.

Three modes:
  --mode zonal --fraction <name> --batch_id <N>
        Array task: run gdptools exactextract for one fraction over one HRU
        batch. Mirrors what scripts/create_zonal_params.py does for the
        non-depstor zonal params.
  --mode merge --fraction <name>
        Combine per-batch CSVs for one fraction into the merged CSV.
        Same logic as scripts/merge_params.py.
  --mode ratios
        Read every merged fraction CSV and produce the four PRMS Level-5
        ratio CSVs (sro_to_dprst_perv, sro_to_dprst_imperv, carea_max,
        smidx_coef) via gfv2_params.depstor_ratios.compute_ratio().

The slurm wrapper slurm_batch/submit_depstor_params.sh chains all three modes
into a single afterok DAG.
"""

import argparse
import os
import re
import sys
import time
from pathlib import Path

# Pre-import heartbeats so a future hang in the geo-library import chain
# (rasterio/GDAL/PROJ/pyogrio init under shared-FS metadata contention) can be
# localised. Mirrors create_zonal_params.py.
print(
    f"[startup pid={os.getpid()} host={os.uname().nodename} "
    f"task={os.environ.get('SLURM_ARRAY_TASK_ID', '-')}] "
    f"python {sys.version.split()[0]} interpreter up, importing libs...",
    flush=True,
)
_t_imports = time.time()

import pandas as pd

from gfv2_params.config import load_config
from gfv2_params.depstor_ratios import compute_ratio
from gfv2_params.log import configure_logging

print(f"[startup] base imports complete in {time.time() - _t_imports:.1f}s", flush=True)


def _resolve_nested(value, replacements: dict):
    if isinstance(value, str):
        for ph, rep in replacements.items():
            value = value.replace(f"{{{ph}}}", str(rep))
        remaining = re.findall(r"\{(\w+)\}", value)
        if remaining:
            raise ValueError(
                f"Unresolved placeholder(s) {remaining} in value '{value}'. "
                f"Available: {sorted(replacements)}"
            )
        return value
    if isinstance(value, list):
        return [_resolve_nested(v, replacements) for v in value]
    if isinstance(value, dict):
        return {k: _resolve_nested(v, replacements) for k, v in value.items()}
    return value


def _load_resolved_config(args) -> dict:
    raw = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    replacements = {"data_root": raw["data_root"], "fabric": raw["fabric"]}
    return {k: _resolve_nested(v, replacements) for k, v in raw.items()}


def _find_fraction(config: dict, name: str) -> dict:
    fractions = config["fractions"]
    for spec in fractions:
        if spec["name"] == name:
            return spec
    available = [s["name"] for s in fractions]
    raise ValueError(f"Fraction '{name}' not in config; available: {available}")


def _merge_paths(config: dict) -> tuple[Path, Path]:
    """Return (intermediates_dir, ratios_dir).

    Intermediate per-fraction count CSVs (one per fraction) land in
    `merged/_intermediates/`. Final PRMS-ready ratio CSVs land in `merged/`.
    Splitting them out keeps PRMS-readers (which only want the ratios) from
    accidentally consuming the count CSVs as if they were [0, 1] fractions —
    a bug the per-fraction filename convention (`nhm_<x>_frac_params.csv`)
    invited because gdptools writes a `count` column, not a normalised
    fraction.
    """
    defaults = config["defaults"]
    output_dir = Path(defaults["output_dir"])
    intermediates_dir = output_dir / defaults["merged_intermediates_subdir"]
    ratios_dir = output_dir / defaults["merged_subdir"]
    return intermediates_dir, ratios_dir


def run_zonal(args, logger) -> None:
    """One fraction, one HRU batch. Reads the gdptools UserTiffData + ZonalGen
    flow inline so we don't pay the geopandas / rioxarray import cost in the
    other two modes."""
    import geopandas as gpd
    import rioxarray
    from gdptools import UserTiffData, ZonalGen
    from osgeo import gdal, osr

    gdal.UseExceptions()
    osr.UseExceptions()

    config = _load_resolved_config(args)
    defaults = config["defaults"]
    spec = _find_fraction(config, args.fraction)

    fabric = config["fabric"]
    source_type = spec["name"]
    raster_path = Path(spec["source_raster"])
    batch_dir = Path(defaults["batch_dir"])
    batch_gpkg = batch_dir / f"batch_{args.batch_id:04d}.gpkg"
    output_dir = Path(defaults["output_dir"]) / source_type
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raster_path.exists():
        raise FileNotFoundError(f"Input raster not found: {raster_path}")
    if not batch_gpkg.exists():
        raise FileNotFoundError(f"Batch GPKG not found: {batch_gpkg}")

    logger.info("=== zonal: %s (batch %d) ===", source_type, args.batch_id)
    logger.info("Raster: %s", raster_path)
    logger.info("Batch GPKG: %s", batch_gpkg)

    nhru_gdf = gpd.read_file(batch_gpkg, layer=defaults["target_layer"])
    logger.info("Loaded %s features", len(nhru_gdf))

    ned_da = rioxarray.open_rasterio(raster_path, masked=True)
    logger.info("Raster shape=%s crs=%s", ned_da.shape, ned_da.rio.crs)

    file_prefix = f"base_nhm_{source_type}_{fabric}_batch_{args.batch_id:04d}_param"
    data = UserTiffData(
        source_var=source_type,
        source_ds=ned_da,
        source_crs=ned_da.rio.crs,
        source_x_coord="x",
        source_y_coord="y",
        band=1,
        bname="band",
        target_gdf=nhru_gdf,
        target_id=defaults["id_feature"],
    )

    zonal_gen = ZonalGen(
        user_data=data,
        zonal_engine="exactextract",
        zonal_writer="csv",
        out_path=output_dir,
        file_prefix=file_prefix,
        jobs=4,
    )
    stats = zonal_gen.calculate_zonal(categorical=bool(defaults.get("categorical", False)))
    logger.info("Zonal complete. Shape: %s", stats.shape)


def run_merge(args, logger) -> None:
    config = _load_resolved_config(args)
    defaults = config["defaults"]
    spec = _find_fraction(config, args.fraction)

    fabric = config["fabric"]
    source_type = spec["name"]
    id_feature = defaults["id_feature"]
    merged_file = spec["merged_file"]
    expected_max = config.get("expected_max_hru_id")

    input_dir = Path(defaults["output_dir"]) / source_type
    intermediates_dir, _ = _merge_paths(config)
    intermediates_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"Per-batch dir not found: {input_dir}")

    pattern = f"base_nhm_{source_type}_{fabric}_batch_*_param.csv"
    files = sorted(input_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No batch CSVs matched: {input_dir / pattern}")

    logger.info("=== merge: %s (%d batches) ===", source_type, len(files))

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        if id_feature not in df.columns:
            raise ValueError(f"'{id_feature}' column missing in {f}")
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True).sort_values(id_feature).reset_index(drop=True)

    dupes = merged[merged[id_feature].duplicated(keep=False)]
    if len(dupes) > 0:
        dupe_ids = sorted(dupes[id_feature].unique())
        raise ValueError(
            f"Duplicate {id_feature} values found ({len(dupe_ids)} IDs). "
            f"First 10: {dupe_ids[:10]}. Indicates overlapping batches."
        )

    if expected_max is not None:
        existing = set(merged[id_feature])
        gaps = sorted(set(range(1, int(expected_max) + 1)) - existing)
        if gaps:
            logger.warning(
                "%d missing %s values (expected 1-%d, got %d). First 10: %s. "
                "Run merge_and_fill_params.py to fill via KNN if needed.",
                len(gaps), id_feature, expected_max, len(existing), gaps[:10],
            )

    out_path = intermediates_dir / merged_file
    merged.to_csv(out_path, index=False)
    logger.info("Merged %d rows -> %s", len(merged), out_path)


def run_ratios(args, logger) -> None:
    config = _load_resolved_config(args)
    defaults = config["defaults"]
    ratios = config["ratios"]
    id_feature = defaults["id_feature"]
    count_column = defaults["count_column"]

    intermediates_dir, ratios_dir = _merge_paths(config)
    if not intermediates_dir.exists():
        raise FileNotFoundError(f"Intermediates dir not found: {intermediates_dir}")
    ratios_dir.mkdir(parents=True, exist_ok=True)

    fraction_files = {spec["name"]: intermediates_dir / spec["merged_file"] for spec in config["fractions"]}

    logger.info("=== ratios (%d) ===", len(ratios))
    logger.info("Intermediates dir: %s", intermediates_dir)
    logger.info("Ratios dir       : %s", ratios_dir)

    for spec in ratios:
        name = spec["name"]
        num_name = spec["numerator"]
        den_name = spec["denominator"]
        if num_name not in fraction_files:
            raise KeyError(f"Ratio '{name}' references unknown numerator fraction '{num_name}'")
        if den_name not in fraction_files:
            raise KeyError(f"Ratio '{name}' references unknown denominator fraction '{den_name}'")

        num_path = fraction_files[num_name]
        den_path = fraction_files[den_name]
        out_path = ratios_dir / spec["output_file"]
        clamp = bool(spec.get("clamp_to_one", False))

        for p in (num_path, den_path):
            if not p.exists():
                raise FileNotFoundError(
                    f"Required merged CSV not found: {p}\n"
                    f"Run --mode merge --fraction <name> first."
                )

        logger.info("--- %s ---", name)
        logger.info("  numerator   = %s -> %s", num_name, num_path)
        logger.info("  denominator = %s -> %s", den_name, den_path)

        num_df = pd.read_csv(num_path)
        den_df = pd.read_csv(den_path)
        for col in (id_feature, count_column):
            if col not in num_df.columns:
                raise ValueError(f"Column '{col}' missing in {num_path}")
            if col not in den_df.columns:
                raise ValueError(f"Column '{col}' missing in {den_path}")

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

    logger.info("=== ratios complete ===")


def main():
    parser = argparse.ArgumentParser(description="Drive depstor zonal stats + ratio derivation.")
    parser.add_argument("--config", required=True, help="Path to configs/depstor_params.yml")
    parser.add_argument("--base_config", default=None, help="Path to configs/base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--mode", required=True, choices=["zonal", "merge", "ratios"])
    parser.add_argument("--fraction", default=None, help="Fraction name (required for zonal/merge)")
    parser.add_argument("--batch_id", type=int, default=None, help="Batch ID (zonal mode only)")
    args = parser.parse_args()

    if args.mode in {"zonal", "merge"} and not args.fraction:
        parser.error(f"--fraction is required for --mode {args.mode}")
    if args.mode == "zonal" and args.batch_id is None:
        parser.error("--batch_id is required for --mode zonal")

    logger = configure_logging(f"derive_depstor_params:{args.mode}")
    if args.mode == "zonal":
        run_zonal(args, logger)
    elif args.mode == "merge":
        run_merge(args, logger)
    else:
        run_ratios(args, logger)


if __name__ == "__main__":
    main()
