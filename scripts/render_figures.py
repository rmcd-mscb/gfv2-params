"""Headlessly (re)generate the fabric_results figures for a fabric.

Executes each ``notebooks/fabric_results/*.ipynb`` via ``jupyter nbconvert
--execute`` with ``FABRIC`` and ``SAVE_FIGURES=1`` set in the environment, so
``gfv2_params.viz.save_figure`` writes PNGs into ``docs/figures/{fabric}/``.
The executed notebook copies land in the gitignored
``docs/figures/.cache/{fabric}/`` (only the PNGs are committed).

``MPLBACKEND=Agg`` is forced here because this path is non-interactive — the
viewer library deliberately does NOT pin a backend so that the same notebooks
display inline under JupyterHub's ``%matplotlib inline``.

Run where ``jupyter`` is available (a JupyterHub session or the ``notebooks``
pixi env) on a compute node with enough ``--mem`` (CONUS ``gfv2`` is large):

  pixi run -e notebooks python scripts/render_figures.py --fabric oregon
"""

import argparse
import os
import subprocess
from pathlib import Path

from gfv2_params.log import configure_logging

REPO_ROOT = Path(__file__).resolve().parents[1]
NB_DIR = REPO_ROOT / "notebooks" / "fabric_results"
DEFAULT_NOTEBOOKS = [
    "01_input_rasters.ipynb",
    "02_depstor_rasters.ipynb",
    "03_param_results.ipynb",
]


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--fabric", default="oregon", help="Fabric to render (default: oregon).")
    ap.add_argument("--notebooks", nargs="*", default=DEFAULT_NOTEBOOKS,
                    help="Subset of notebook filenames to run (default: all three).")
    ap.add_argument("--kernel", default="python3",
                    help="Jupyter kernel name to execute with (default: python3).")
    ap.add_argument("--timeout", type=int, default=1800,
                    help="Per-cell execution timeout in seconds (default: 1800).")
    args = ap.parse_args()

    logger = configure_logging("render_figures")
    cache = REPO_ROOT / "docs" / "figures" / ".cache" / args.fabric
    cache.mkdir(parents=True, exist_ok=True)
    figdir = REPO_ROOT / "docs" / "figures" / args.fabric

    env = dict(os.environ, FABRIC=args.fabric, SAVE_FIGURES="1", MPLBACKEND="Agg")

    for nb in args.notebooks:
        src = NB_DIR / nb
        if not src.exists():
            logger.warning("skip missing notebook: %s", src)
            continue
        logger.info("executing %s (FABRIC=%s)", nb, args.fabric)
        cmd = [
            "jupyter", "nbconvert", "--to", "notebook", "--execute",
            f"--ExecutePreprocessor.timeout={args.timeout}",
            f"--ExecutePreprocessor.kernel_name={args.kernel}",
            "--output-dir", str(cache), "--output", nb, str(src),
        ]
        result = subprocess.run(cmd, env=env)
        if result.returncode != 0:
            logger.error("nbconvert failed for %s (exit %d)", nb, result.returncode)
            return result.returncode

    pngs = sorted(figdir.glob("*.png")) if figdir.exists() else []
    logger.info("done: %d figure(s) in %s", len(pngs), figdir)
    for p in pngs:
        logger.info("  %s", p.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
