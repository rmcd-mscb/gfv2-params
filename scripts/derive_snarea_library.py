"""Stage 3 driver: build the CV/lognormal snarea_curve library from the Stage 2
derived CSV. Pure tabular — cheap, re-runnable at any ndepl_cv without reloading
the daily SWE. Fabric-agnostic (paths from the profile via require_config_key)."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from gfv2_params.config import load_config, require_config_key
from gfv2_params.log import configure_logging
from gfv2_params.snarea import DEFAULT_SNAREA_CURVE, validate_default_curve
from gfv2_params.snarea.library import (
    build_from_derived,
    write_library_csv,
    write_params_csv,
    write_prms_netcdf,
    write_validation_csv,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fabric", required=True)
    ap.add_argument("--config", default="configs/snarea/snarea_library.yml")
    ap.add_argument("--base_config", default="configs/base_config.yml")
    args = ap.parse_args()
    logger = configure_logging("derive_snarea_library")

    cfg = load_config(Path(args.config), base_config_path=Path(args.base_config), fabric=args.fabric)
    id_feature = require_config_key(cfg, "id_feature", "derive_snarea_library")
    derived_csv = Path(cfg["derived_csv"])
    out_dir = Path(cfg["output_dir"])
    ndepl_cv = int(cfg.get("ndepl_cv", 8))
    calibrate = cfg.get("calibrate", "auto")
    bias_tol = float(cfg.get("calibrate_bias_tol", 0.1))
    default_curve = np.asarray(cfg.get("default_curve", DEFAULT_SNAREA_CURVE), dtype=float)
    validate_default_curve(default_curve)

    logger.info("Reading Stage 2 derived table: %s", derived_csv)
    derived = pd.read_csv(derived_csv)
    logger.info(
        "Building CV/lognormal library (ndepl_cv=%d, calibrate=%s) for %d HRUs ...",
        ndepl_cv, calibrate, len(derived),
    )
    library, params, report = build_from_derived(derived, id_feature, ndepl_cv, default_curve, calibrate, bias_tol)

    write_library_csv(library, out_dir / cfg["library_file"])
    write_params_csv(params, out_dir / cfg["params_file"])
    write_validation_csv(report, out_dir / cfg["validation_file"])
    write_prms_netcdf(library, params, id_feature, out_dir / cfg["netcdf_file"])
    logger.info(
        "ndepl=%d | estimable %d/%d | calibrated=%s | recon mean %.3f",
        len(library), report["n_estimable"], report["n_hru"],
        report["calibrated"], report.get("recon_mean_after", float("nan")),
    )


if __name__ == "__main__":
    main()
