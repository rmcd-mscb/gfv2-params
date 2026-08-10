"""Per-HRU source-coverage diagnostic.

gdptools' ``masked_mean`` returns **0.0, not NaN**, for a polygon whose source
cells are every one of them fill. That makes a polygon sitting entirely outside
the source's valid domain indistinguishable from one that is genuinely
zero-valued — for SNODAS, an HRU off the edge of the domain reads exactly like
Florida (issue #166: 1,087 CONUS HRUs, 0.30%, land in ``default_no_snow`` this
way, and a further 35,315 aggregate over only part of their area).

Coverage is the weight-weighted fraction of a polygon's source cells that carry
valid data::

    coverage = Σ(wght · valid) / Σ(wght)

It is a **diagnostic**, never a gate: it rides alongside the derived parameter so
a thin record is visible, and deliberately does not demote any HRU. Gating on it
was considered and rejected — at a 0.999 threshold it would flip ~13.3k CONUS
HRUs off their empirical curve onto the default one.

The valid mask comes from the adapter's own ``pre_aggregate_hook`` (which is what
converts that source's fill sentinel to NaN), so this module has no per-source
constants; an adapter with no hook falls back to plain NaN.
"""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

from .adapter import SourceAdapter
from .driver import subset_to_gdf_bounds

logger = logging.getLogger(__name__)


def coverage_from_weights(
    adapter: SourceAdapter,
    source_ds: xr.Dataset,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    weights: pd.DataFrame,
    time_index: int = 0,
) -> dict:
    """Weight-weighted valid-data fraction per polygon, as ``{id: coverage}``.

    ``weights`` must be the table computed for THIS ``fabric_gdf`` — its
    ``(i, j)`` index the bounds-subset that :func:`subset_to_gdf_bounds` derives
    from these polygons, not the full source grid. Batched runs therefore have
    one index space per batch, and the consolidated weight file (a plain
    row-concat of them) is **not** usable here: its rows mix 64 different origins.
    Mismatched indices produce plausible-looking numbers rather than an error, so
    the bounds check below raises instead of clipping or wrapping.

    ``time_index`` selects the day the valid mask is read from. The mask is a
    property of the source's domain, not of the weather, so any day in the file
    works — but the domain can change between files (SNODAS's footprint expands
    over the record), which is why callers sample one day per year and average.
    """
    # Narrow to the single day BEFORE the hook. The hook rewrites every cell of
    # every variable it touches, so running it on the full year would do ~365x
    # the work for a mask that is a property of the domain, not of any one day.
    # The time dim is kept (not squeezed) so a hook written against the
    # aggregation's Dataset shape still applies unchanged.
    if adapter.time_coord in source_ds.dims:
        source_ds = source_ds.isel({adapter.time_coord: [time_index]})

    sub = subset_to_gdf_bounds(
        source_ds, fabric_gdf, adapter.source_crs, adapter.x_coord, adapter.y_coord
    )
    if adapter.pre_aggregate_hook is not None:
        sub = adapter.pre_aggregate_hook(sub)

    grid = sub[adapter.grid_variable]
    if adapter.time_coord in grid.dims:
        grid = grid.isel({adapter.time_coord: 0})
    valid = grid.notnull().values.astype("float64")

    ny, nx = valid.shape
    i = weights["i"].to_numpy()
    j = weights["j"].to_numpy()
    if i.size and (i.min() < 0 or j.min() < 0 or i.max() >= ny or j.max() >= nx):
        raise ValueError(
            f"weight index out of range for this polygon set's bounds-subset "
            f"(got i in [{i.min()}, {i.max()}], j in [{j.min()}, {j.max()}]; "
            f"subset is {ny}x{nx}). The weights must be the ones computed for "
            f"these polygons — a consolidated multi-batch weight file mixes "
            f"per-batch index spaces and cannot be used here."
        )

    w = weights["wght"].to_numpy(dtype="float64")
    frame = pd.DataFrame({id_col: weights[id_col].to_numpy(),
                          "w": w, "wv": w * valid[i, j]})
    g = frame.groupby(id_col)[["w", "wv"]].sum()
    # A polygon whose weights sum to 0 has no source cells at all; report NaN
    # ("not measurable") rather than a 0/0 warning or a misleading 0.0.
    cov = np.where(g["w"].to_numpy() > 0,
                   g["wv"].to_numpy() / np.where(g["w"].to_numpy() > 0, g["w"], 1.0),
                   np.nan)
    return dict(zip(g.index.tolist(), cov.tolist()))


def coverage_over_years(
    adapter: SourceAdapter,
    fabric_gdf: gpd.GeoDataFrame,
    id_col: str,
    weights: pd.DataFrame,
    files: list[Path],
) -> pd.DataFrame:
    """Mean per-polygon coverage across one sampled day per per-year file.

    Averaging matters because the source domain is not fixed: SNODAS's valid
    footprint shifts within a year and expands markedly across the record — 31
    of the 32 CONUS HRUs that read zero-coverage in 2004 carry real snow by 2015
    (#166). A single-year probe would therefore call a late-covered HRU
    permanently absent. The mean gives 0.0 only when the polygon is uncovered in
    every sampled year, and lands between 0 and 1 for one covered only part of
    the record — which is exactly the distinction the diagnostic is for.

    Returns a frame of ``[id_col, coverage, n_years_sampled]``.
    """
    if not files:
        raise ValueError("coverage_over_years: no source files given")
    per_year: list[dict] = []
    for k, f in enumerate(files, start=1):
        with xr.open_dataset(f) as ds:
            per_year.append(
                coverage_from_weights(adapter, ds, fabric_gdf, id_col, weights)
            )
        logger.info("  coverage [%d/%d] %s", k, len(files), Path(f).name)

    ids = sorted({i for d in per_year for i in d})
    stacked = np.array([[d.get(i, np.nan) for i in ids] for d in per_year])
    with np.errstate(invalid="ignore"):
        mean = np.nanmean(stacked, axis=0)
    return pd.DataFrame({
        id_col: ids,
        "coverage": mean,
        "n_years_sampled": np.sum(~np.isnan(stacked), axis=0),
    })
