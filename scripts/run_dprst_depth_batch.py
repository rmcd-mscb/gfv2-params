"""SLURM array driver for the dprst_depth compute step (issue #173 Task 9).

The fan-out unit is the 1 m elevation TILE, not the dprst polygon (Task 3's
`tiling.group_by_tile` + `tiling.component_tile_batches`): reading each tile
ONCE and computing every polygon whose window falls in it is what turns
CONUS's ~286k-polygon, ~250-500 core-hour serial cost into a <=5 hr
wall-clock array job. See slurm_batch/submit_dprst_depth.sh's header for the
full sizing arithmetic.

One array task = one batch of tile keys, i.e. one entry of the plan step's
persisted `tile_batches` list (`python -m gfv2_params.dprst_depth.tiling
--plan`, Task 9's dry-run/work-list hook). This script does NOT re-derive
the dprst polygon set or re-run `group_by_tile`/`component_tile_batches`
itself -- it reads the plan's already-tagged polygon parquet + batch
manifest (both written once, by the plan job, not by every array task) and
calls Task 4's `compute.run_batch` for this task's batch only, writing one
per-batch parquet directly into `{output_dir}/dprst_depth_batches/`
(`depstor_builders/dprst_depth.py::_compute_depths` glob-loads every
`*.parquet` there on the next `build_depstor_rasters.py --step dprst_depth`
run).

Usage (normally invoked once per SLURM array task by
slurm_batch/run_dprst_depth_batch.batch, which exports FABRIC/BASE_CONFIG
and passes $SLURM_ARRAY_TASK_ID as --batch_id):

    pixi run --as-is python scripts/run_dprst_depth_batch.py \\
        --config configs/depstor/depstor_rasters.yml \\
        --base_config configs/base_config.yml \\
        --fabric gfv2 \\
        --batch_id $SLURM_ARRAY_TASK_ID
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

# Pre-import heartbeat so a hang in the geo-library import chain
# (rasterio/GDAL/PROJ/pyogrio under shared-FS metadata contention) is
# localisable from the SLURM log alone. Mirrors derive_depstor_params.py.
print(
    f"[startup pid={os.getpid()} host={os.uname().nodename} "
    f"task={os.environ.get('SLURM_ARRAY_TASK_ID', '-')}] "
    f"python {sys.version.split()[0]} interpreter up, importing libs...",
    flush=True,
)
_t_imports = time.time()

import geopandas as gpd  # noqa: E402

from gfv2_params.config import load_config  # noqa: E402
from gfv2_params.dprst_depth.compute import run_batch  # noqa: E402
from gfv2_params.log import configure_logging  # noqa: E402

print(f"[startup] base imports complete in {time.time() - _t_imports:.1f}s", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Compute dprst_depth for ONE tile-batch (a single SLURM array task)."
    )
    parser.add_argument("--config", default="configs/depstor/depstor_rasters.yml")
    parser.add_argument("--base_config", default=None)
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument(
        "--batch_id", type=int, default=None,
        help="Batch index into the plan's tile_batches list (default: $SLURM_ARRAY_TASK_ID)",
    )
    args = parser.parse_args()

    batch_id = args.batch_id
    if batch_id is None:
        env_id = os.environ.get("SLURM_ARRAY_TASK_ID")
        if env_id is None:
            parser.error("--batch_id is required when $SLURM_ARRAY_TASK_ID is not set")
        batch_id = int(env_id)

    logger = configure_logging(f"run_dprst_depth_batch:{batch_id}")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    output_dir = Path(config["output_dir"])
    batches_dir = output_dir / "dprst_depth_batches"
    plan_dir = batches_dir / "_plan"
    manifest_path = plan_dir / "batch_manifest.json"
    tagged_path = plan_dir / "dprst_polygons_tagged.parquet"

    if not manifest_path.exists() or not tagged_path.exists():
        raise FileNotFoundError(
            f"Plan artifacts not found under {plan_dir}. Run the plan step first: "
            f"`pixi run python -m gfv2_params.dprst_depth.tiling --plan --config {args.config} "
            f"--fabric {config['fabric']} --n-batches N` (see slurm_batch/submit_dprst_depth.sh)."
        )

    manifest = json.loads(manifest_path.read_text())
    all_batches = manifest["tile_batches"]
    if not (0 <= batch_id < len(all_batches)):
        raise IndexError(
            f"--batch_id {batch_id} out of range for {len(all_batches)} batches in {manifest_path}"
        )
    tile_keys = all_batches[batch_id]

    dprst_gdf = gpd.read_parquet(tagged_path)
    wesm_index = Path(config["wesm_index"])
    wesm_gdf = gpd.read_file(wesm_index) if wesm_index.exists() else gpd.GeoDataFrame()

    batches_dir.mkdir(parents=True, exist_ok=True)
    out_parquet = batches_dir / f"batch_{batch_id:04d}.parquet"

    logger.info(
        "=== run_dprst_depth_batch: batch %d/%d (%d tile keys, %d dprst polygons total) ===",
        batch_id, len(all_batches), len(tile_keys), len(dprst_gdf),
    )
    run_batch(dprst_gdf, tile_keys, wesm_gdf, out_parquet, logger)


if __name__ == "__main__":
    main()
