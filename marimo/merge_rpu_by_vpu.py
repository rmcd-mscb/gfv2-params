import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import yaml
    import rioxarray as rxr
    from pathlib import Path
    from rioxarray.merge import merge_arrays
    import matplotlib.pyplot as plt
    import sys

    # Add the src directory to the Python path
    src_path = Path(__file__).resolve().parent.parent / "src"
    sys.path.append(str(src_path))

    # Now you can import helpers
    from helpers import load_config

    return Path, load_config


@app.cell
def _(Path, load_config):
    # cell: load a config file using your helper
    config = load_config(Path("configs/config_merge_rpu_by_vpu.yml"))
    config
    return


if __name__ == "__main__":
    app.run()
