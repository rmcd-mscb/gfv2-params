"""Time the geo-library import chain and record who/where/when. I2's instrument.

Mirrors the startup heartbeat in scripts/derive_depstor_params.py:42-48 -- the same
imports, in the same order, in the same frozen default env -- so the wall-times are
comparable to a real array task's.

I2 asks whether packing rule instances into one SLURM allocation via `group:`
re-creates the import storm the `%4` array cap exists to prevent
(HPC_REFERENCE.md:40-47: "concurrent geo-library imports (rasterio / GDAL / PROJ /
pyogrio) can deadlock under shared-FS metadata contention when many tasks start
simultaneously"). The signal is import wall-time, plus the start timestamps: if
instances in a group all start within a second of each other, the group is running
them concurrently and the cap has been lost.

Deliberately writes a machine-readable record rather than logging, so the two arms
of the experiment can be diffed rather than eyeballed.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import time
from pathlib import Path


def main() -> int:
    out_path, instance = sys.argv[1], sys.argv[2]
    # Create our own parent. Doing it in the shell prefix meant 16 concurrent
    # `mkdir -p` calls against one directory on a shared FS, which is how arm A's
    # first attempt failed. exist_ok makes this safe under any concurrency.
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    t_start = time.time()
    # The real chain, in the production order. Not a stand-in: the whole question
    # is whether THESE imports contend.
    import numpy  # noqa: F401
    import pandas  # noqa: F401
    t_after_light = time.time()

    import geopandas  # noqa: F401
    import pyogrio  # noqa: F401
    import rasterio  # noqa: F401
    from osgeo import gdal  # noqa: F401
    t_end = time.time()

    record = {
        "instance": instance,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID"),
        # Absolute start, so concurrency within a group is visible: instances that
        # begin within ~1s of each other were started together, not serialised.
        "t_start_epoch": t_start,
        "import_light_s": round(t_after_light - t_start, 3),
        "import_geo_s": round(t_end - t_after_light, 3),
        "import_total_s": round(t_end - t_start, 3),
    }

    with open(out_path, "w") as fh:
        json.dump(record, fh, indent=2)
    print(f"[import_timer] {instance} total={record['import_total_s']}s "
          f"geo={record['import_geo_s']}s host={record['host']} "
          f"job={record['slurm_job_id']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
