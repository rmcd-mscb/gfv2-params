"""Stage 1 driver: aggregate a gridded source to the active fabric (per year).

Resolves the fabric geopackage + id_feature from the base_config.yml profile and
writes one per-HRU per-day NetCDF per calendar year. Fabric-agnostic.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import geopandas as gpd

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, choices=sorted(ADAPTERS))
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--years", nargs="*", type=int, default=None)
    ap.add_argument("--config", default="configs/aggregate/aggregate_sources.yml")
    ap.add_argument("--base_config", default="configs/base_config.yml")
    args = ap.parse_args()
    logger = configure_logging("derive_aggregate")

    cfg = load_config(Path(args.config), base_config_path=Path(args.base_config),
                       fabric=args.fabric)
    repl = {"data_root": cfg["data_root"], "fabric": cfg["fabric"]}
    cfg = {k: _resolve(v, repl) for k, v in cfg.items()}

    src = next(s for s in cfg["sources"] if s["name"] == args.source)
    # snodas_dir may be overridden in the profile; fall back to the source entry.
    snodas_dir = _resolve(cfg.get("snodas_dir", src["snodas_dir"]), repl)

    hru_gpkg = require_config_key(cfg, "hru_gpkg", "derive_aggregate")
    hru_layer = cfg.get("hru_layer", "nhru")
    id_feature = require_config_key(cfg, "id_feature", "derive_aggregate")

    fabric_gdf = gpd.read_file(hru_gpkg, layer=hru_layer)
    logger.info("Fabric %s: %d HRUs (id=%s)", args.fabric, len(fabric_gdf), id_feature)

    out = aggregate_source(
        ADAPTERS[args.source], fabric_gdf, id_feature,
        input_dir=Path(snodas_dir),
        output_dir=Path(cfg["output_dir"]),
        weight_file=Path(cfg["weight_dir"]) / f"{args.source}_weights_{args.fabric}.csv",
        output_prefix=src["output_prefix"],
        years=args.years,
    )
    logger.info("Wrote %d per-year files to %s", len(out), cfg["output_dir"])


if __name__ == "__main__":
    main()
