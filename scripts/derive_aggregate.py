"""Stage 1 driver: aggregate a gridded source to the active fabric (per year).

Resolves the fabric geopackage + id_feature from the base_config.yml profile and
writes one per-HRU per-day NetCDF per calendar year. Fabric-agnostic.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd

from gfv2_params.aggregate import aggregate_source
from gfv2_params.aggregate.snodas import SNODAS_ADAPTER
from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging

ADAPTERS = {"snodas": SNODAS_ADAPTER}


def _resolve(value, repl: dict):
    """Recursively resolve {placeholder} substrings in nested str/list/dict.

    configs/aggregate/aggregate_sources.yml's `sources:` list is nested, so
    load_config (which only resolves top-level string values) leaves its
    {data_root}/{fabric} placeholders untouched; this fills that gap.
    """
    if isinstance(value, str):
        for ph, rep in repl.items():
            value = value.replace(f"{{{ph}}}", str(rep))
        remaining = re.findall(r"\{(\w+)\}", value)
        if remaining:
            raise ValueError(
                f"Unresolved placeholder(s) {remaining} in value '{value}'. "
                f"Available: {sorted(repl)}"
            )
        return value
    if isinstance(value, list):
        return [_resolve(v, repl) for v in value]
    if isinstance(value, dict):
        return {k: _resolve(v, repl) for k, v in value.items()}
    return value


def run_merge(output_dir: Path, output_prefix: str, id_feature: str, logger) -> list[Path]:
    """Concatenate per-batch per-year NetCDFs into the final per-year files.

    Per-batch Stage 1 outputs live in ``{output_dir}/_batches/`` (see
    ``mode == "aggregate"`` in ``main()``), named
    ``{output_prefix}_batch{NNNN}_agg_{YYYY}.nc``. This merges all batches for
    each year into ``{output_dir}/{output_prefix}_agg_{YYYY}.nc`` — the file
    Stage 2 (``derive_snarea_curve.py``) globs for. Keeping per-batch parts in
    the `_batches/` subdir means Stage 2's top-level `*_agg_*.nc` glob only
    ever sees these final merged files.
    """
    import xarray as xr

    output_dir = Path(output_dir)
    batches_dir = output_dir / "_batches"
    parts = sorted(batches_dir.glob(f"{output_prefix}_batch*_agg_*.nc"))
    if not parts:
        raise FileNotFoundError(f"No per-batch NetCDFs in {batches_dir}")

    def _year(p: Path) -> int:
        # The filename is "{prefix}_batch{NNNN}_agg_{YYYY}.nc" — the batch
        # index is also 4 digits, so a generic first-4-digit parse (as
        # driver._year_of does) would grab the batch index instead. Match the
        # year explicitly after "_agg_".
        m = re.search(r"_agg_(\d{4})\.nc$", p.name)
        if not m:
            raise ValueError(f"cannot parse year from {p.name}")
        return int(m.group(1))

    written: list[Path] = []
    for year in sorted({_year(p) for p in parts}):
        yparts = sorted(batches_dir.glob(f"{output_prefix}_batch*_agg_{year}.nc"))
        dss = [xr.open_dataset(p) for p in yparts]
        try:
            merged = xr.concat(dss, dim=id_feature).sortby(id_feature)
            out = output_dir / f"{output_prefix}_agg_{year}.nc"
            merged.to_netcdf(out)
        finally:
            for d in dss:
                d.close()
        written.append(out)
        logger.info("Merged %d batches -> %s", len(yparts), out.name)
    return written


def consolidate_weights(weight_dir: Path, source: str, fabric: str, logger) -> Path:
    """Concat per-batch weight CSVs into the single canonical weight file.

    Batched aggregation (``--batch_id``) caches gdptools weights per batch as
    ``{source}_weights_{fabric}_batch{NNNN}.csv``, but Stage 2's
    ``cells_from_weights`` reads a single ``{source}_weights_{fabric}.csv``.
    Batches cover disjoint HRUs, so a plain row-concat of the per-batch tables
    reproduces the whole-fabric weight table (identical per-HRU cell counts).
    Run as part of ``--mode merge`` so the canonical file exists before Stage 2.
    """
    weight_dir = Path(weight_dir)
    canonical = weight_dir / f"{source}_weights_{fabric}.csv"
    parts = sorted(weight_dir.glob(f"{source}_weights_{fabric}_batch*.csv"))
    if not parts:
        raise FileNotFoundError(
            f"No per-batch weight CSVs in {weight_dir} matching "
            f"{source}_weights_{fabric}_batch*.csv"
        )
    combined = pd.concat([pd.read_csv(p) for p in parts], ignore_index=True)
    combined.to_csv(canonical, index=False)
    logger.info("Consolidated %d per-batch weight files -> %s", len(parts), canonical.name)
    return canonical


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, choices=sorted(ADAPTERS))
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--years", nargs="*", type=int, default=None)
    ap.add_argument("--config", default="configs/aggregate/aggregate_sources.yml")
    ap.add_argument("--base_config", default="configs/base_config.yml")
    ap.add_argument("--mode", choices=["aggregate", "merge"], default="aggregate")
    ap.add_argument("--batch_id", type=int, default=None,
                     help="Spatial batch index (aggregate mode only); omit to run whole-fabric.")
    args = ap.parse_args()
    logger = configure_logging("derive_aggregate")

    cfg = load_config(Path(args.config), base_config_path=Path(args.base_config),
                       fabric=args.fabric)
    repl = {"data_root": cfg["data_root"], "fabric": cfg["fabric"]}
    cfg = {k: _resolve(v, repl) for k, v in cfg.items()}

    src = next(s for s in cfg["sources"] if s["name"] == args.source)
    id_feature = require_config_key(cfg, "id_feature", "derive_aggregate")

    batch_note = f" batch={args.batch_id}" if args.batch_id is not None else ""
    logger.info("derive_aggregate: source=%s fabric=%s mode=%s%s",
                args.source, args.fabric, args.mode, batch_note)

    if args.mode == "merge":
        logger.info("Merging per-batch NetCDFs + consolidating weights ...")
        out = run_merge(Path(cfg["output_dir"]), src["output_prefix"], id_feature, logger)
        logger.info("Wrote %d merged per-year files to %s", len(out), cfg["output_dir"])
        consolidate_weights(Path(cfg["weight_dir"]), args.source, args.fabric, logger)
        return

    # snodas_dir may be overridden in the profile; fall back to the source entry.
    snodas_dir = _resolve(cfg.get("snodas_dir", src["snodas_dir"]), repl)
    hru_layer = cfg.get("hru_layer", "nhru")

    if args.batch_id is not None:
        batch_gpkg = Path(cfg["batch_dir"]) / f"batch_{args.batch_id:04d}.gpkg"
        fabric_gdf = gpd.read_file(batch_gpkg, layer=hru_layer)
        out_dir = Path(cfg["output_dir"]) / "_batches"
        prefix = f"{src['output_prefix']}_batch{args.batch_id:04d}"
        wfile = Path(cfg["weight_dir"]) / f"{args.source}_weights_{args.fabric}_batch{args.batch_id:04d}.csv"
        logger.info("Fabric %s batch %04d: %d HRUs (id=%s)",
                    args.fabric, args.batch_id, len(fabric_gdf), id_feature)
    else:
        hru_gpkg = require_config_key(cfg, "hru_gpkg", "derive_aggregate")
        fabric_gdf = gpd.read_file(hru_gpkg, layer=hru_layer)
        out_dir = Path(cfg["output_dir"])
        prefix = src["output_prefix"]
        wfile = Path(cfg["weight_dir"]) / f"{args.source}_weights_{args.fabric}.csv"
        logger.info("Fabric %s: %d HRUs (id=%s)", args.fabric, len(fabric_gdf), id_feature)

    out = aggregate_source(
        ADAPTERS[args.source], fabric_gdf, id_feature,
        input_dir=Path(snodas_dir),
        output_dir=out_dir,
        weight_file=wfile,
        output_prefix=prefix,
        years=args.years,
    )
    logger.info("Wrote %d per-year files to %s", len(out), out_dir)


if __name__ == "__main__":
    main()
