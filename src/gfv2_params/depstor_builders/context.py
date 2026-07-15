"""Shared build context for the depstor raster builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BuildContext:
    """Resolved paths + per-fabric inputs shared across every step builder.

    `paths` accumulates the outputs each builder produces (keyed by short name —
    e.g. `landmask`, `imperv`, `perv`, `dprst`, `onstream`, `drains_to_dprst`)
    so downstream steps can look them up without re-templating paths off the
    config.
    """

    fabric: str
    template_path: Path
    output_dir: Path
    hru_gpkg: Path
    hru_layer: str
    id_feature: str = "nat_hru_id"
    segments_gpkg: Path | None = None
    segments_layer: str = "nsegment"
    waterbody_gpkg: Path | None = None
    waterbody_layer: str | None = None
    connected_comids_table: Path | None = None
    flowthrough_comids_table: Path | None = None
    # --- endorheic classifier inputs ---------------------------------------
    # Full WBD HUC12 layer (type-C rows), staged by gfv2_params.download.wbd_huc12.
    # Optional: absent -> Signal B off, Signal A (FDR terminus) still runs.
    wbd_huc12_table: Path | None = None
    # BurnAddWaterbody polygons (gfv2_params.download.nhd_burn_components), unioned
    # into the waterbody layer by the `waterbody` builder. Optional.
    burn_add_waterbody_table: Path | None = None
    # NHDPlus Sink.shp — provenance + the BurnAdd linkage only, NOT a classifier
    # signal (the classifier reads the FDR grid). INTENTIONALLY UNREAD: no builder
    # consumes it. It is threaded through the profile + this context so the sink
    # layer that BurnAddWaterbody is linked to (SOURCEFC/FEATUREID) is staged and
    # discoverable alongside the polygons it explains, and so a future step can pick
    # it up without a config change. Do not "wire it up" to a classifier — Signal A
    # deliberately reads the FDR grid the router reads, not this lossy point shadow
    # of it (see gfv2_params.endorheic).
    sink_points_table: Path | None = None
    # Optional per-fabric floor on the number of endorheic COMIDs the `endorheic`
    # builder must produce. Absent (the default) means "this domain may legitimately
    # have none" — e.g. `tjc`, Texas-Gulf, has zero closed basins. `gfv2` declares a
    # floor so a collapsed/silently-empty CONUS classifier result fails loud instead
    # of quietly leaving the Great Salt Lake on-stream.
    min_endorheic_comids: int | None = None
    fdr_raster: Path | None = None
    twi_raster: Path | None = None
    vpu: str | None = None  # single-VPU fabric's VPU label (e.g. "17"); None = use fabric `vpu` attr
    imperv_source: Path | None = None
    # --- dprst_depth (#173) inputs -----------------------------------------
    # Pre-staged, already 1m/QL1/QL2-filtered WESM workunit footprint index
    # (columns: at least "project" + geometry) — see
    # gfv2_params.dprst_depth.wesm_io's `ensure_wesm_local` /
    # `load_wesm_1m_footprints` for the download + filtering this path is
    # expected to already reflect. `topo.resolution_class` reads it directly.
    wesm_index: Path | None = None
    # EPA Level III Ecoregions (see gfv2_params.download.epa_ecoregions) —
    # already staged in every fabric profile in base_config.yml.
    ecoregions_gpkg: Path | None = None
    # Constant-floor fallback for a flat/degenerate dprst polygon with no
    # trustworthy donor group (fill.fill_flat's floor_in, inches — 49 in is
    # the NHM calibrated dprst_depth_avg median).
    dprst_depth_floor_in: float = 49.0
    # Minimum donor count per (ecoregion, FTYPE) group before fill.
    # fit_ecoregion_models attempts a CV-compared calibrated-Hollister fit
    # (fill.N_MIN_DEFAULT).
    dprst_hollister_n_min: int = 5
    # Completeness gate on `_fill_and_join`'s measured_fraction (n_computed /
    # n_total polygons with a real computed depth, before the fallback
    # ladder). Below this, RAISE — a systemic read failure (S3 outage, HPC
    # firewall regression), not genuine hydro-flattening. `0` (or negative)
    # disables the guard (escape hatch for a legitimately high-flattening
    # small fabric).
    dprst_depth_min_measured_frac: float = 0.5
    paths: dict[str, Path] = field(default_factory=dict)
    force: bool = False

    def resolve_output(self, filename: str) -> Path:
        return self.output_dir / filename

    def require(self, key: str) -> Path:
        if key not in self.paths:
            raise FileNotFoundError(
                f"Builder requires upstream output '{key}', but it has not been "
                f"produced yet. Run earlier steps first, or invoke the "
                f"orchestrator without --step / --from to honour the DAG."
            )
        path = self.paths[key]
        if not path.exists():
            raise FileNotFoundError(
                f"Upstream output '{key}' tracked in build context but file is "
                f"missing on disk: {path}"
            )
        return path
