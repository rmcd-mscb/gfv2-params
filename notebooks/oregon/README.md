# oregon — results viewer

This directory is a thin per-fabric launcher stub. The actual notebooks live once
in [`../fabric_results/`](../fabric_results/) and are parameterized by the `FABRIC`
env var — there is no per-fabric copy to maintain.

## View the results

Spawn a **JupyterHub** session on a compute node with enough memory (`oregon` is
small; CONUS `gfv2` needs much more `--mem`), then open the three notebooks with
`FABRIC=oregon`:

```bash
# in the JupyterHub session environment
export FABRIC=oregon
# then open, top-to-bottom:
#   ../fabric_results/01_input_rasters.ipynb   (source rasters, fabric-clipped)
#   ../fabric_results/02_depstor_rasters.ipynb (depstor binary masks)
#   ../fabric_results/03_param_results.ipynb   (per-HRU param maps + summary)
```

If you can't set the env var in your Hub spawner, just edit the first code cell
(`FABRIC = os.environ.get("FABRIC", "oregon")`) — `oregon` is already the default.

## Save figures for a report

Set `SAVE_FIGURES=1` (or `viz.SAVE_FIGURES = True` in the first cell) to write each
plot to `docs/figures/oregon/` as `input_raster_*`, `depstor_*`, `param_*` PNGs.
To regenerate the whole set headlessly:

```bash
pixi run -e notebooks python scripts/render_figures.py --fabric oregon
```

Any other fabric reuses the same three notebooks + render script — just change
`FABRIC` (and add a `notebooks/<fabric>/` stub like this one if you want a
fabric-specific landing page).
