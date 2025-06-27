# gfv2-params

Geospatial analysis project using GDAL, rasterio, geopandas, and interactive scripting with [Marimo](https://marimo.io).

This project uses a hybrid environment setup:

- **Conda**: for binary dependencies (GDAL, PROJ, rasterio, etc.)
- **uv**: for Python-only packages from `pyproject.toml`
- **Marimo**: I jupyter replacement, mostly used in this context for interactive experimenting and for generating plots as a check on the processing.
---

## 📦 Environment Setup

### 1. Create and activate the Conda environment

```bash
conda env create -f environment.yml
conda activate geoenv
```
### 2. Install project dependencies with uv and install pre-commit

```bash
uv pip install -e .[dev]
pre-commit install
```

This will install all project.dependencies defined in pyproject.toml.

## Updating the Environment

If you add/remove Python packages in pyproject.toml:

```bash
uv pip install --upgrade
```

if you update the binary stack in environment.yml:

```bash
conda env update -f environment.yml --prune
```

## 🚀 Using Marimo

Marimo notebooks are stored in the marimo/ directory.  The are used for experimenting with workflow processing.

To run a notebook:

```bash
marimo run marimo/your_notebook.py
```

This will start a Jupyter server and open the notebook in your browser.
You can also run the notebook in a terminal:

```bash
marimo run marimo/your_notebook.py --terminal
```

This will run the notebook in a terminal and print the output to the console.

To launch the interactive GUI for development:

```bash
marimo edit

# or edit a specific file

merimo edit marimo/your_notebook.py
```

This will start a Jupyter server and open the Marimo GUI in your browser.
You can then create new notebooks, run existing notebooks, and manage your environment.

## Project Structure

```bash
gfv2-params/
├── environment.yml                  # Conda environment for geospatial dependencies
├── pyproject.toml           # Python dependencies managed by uv
├── .pre-commit-config.yml
├── marimo/                  # Marimo-based workflows
│   ├── 01_preprocess.marimo.py
│   └── 02_analysis.marimo.py
├── slurm_batch
|   ├── 01_create_elev_params.batch
|   └── a_process_NHD_by_vpu.batch
├── scripts
|   ├── 01_create_elev_params.py
|   └── process_NHD_by_vpu.py
├── src/
│   └── gfv2_params/         # Installable Python package
│       ├── __init__.py
│       └── core.py
├── README.md
└── .gitignore
```

## 🧠 Tips

- Use conda list to inspect installed packages and verify version compatibility.
- Avoid mixing the same package between conda and uv (e.g., don't install gdal via pip).
- Pin versions in pyproject.toml only when needed (e.g., "marimo>=0.4").
- Use uv to manage Python dependencies and conda for binary dependencies.
- Use Marimo for interactive scripting and reproducible workflows.

## 🧰 Troubleshooting

If you encounter issues with uv, try the following:

- if a package fails to install via uv, check if it has binary dependencies. If so, prefer installing it via conda.
