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
    fdr_raster: Path | None = None
    twi_raster: Path | None = None
    vpu: str | None = None  # single-VPU fabric's VPU label (e.g. "17"); None = use fabric `vpu` attr
    imperv_source: Path | None = None
    # --- dprst_depth (#173) inputs -----------------------------------------
    # Pre-staged, already 1m/QL1/QL2-filtered WESM workunit footprint index
    # (columns: at least "project" + geometry) — see
    # scripts/diagnose/dprst_depth_probe.py's `ensure_wesm_local` /
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
    # DEM window padding beyond each polygon's bbox (topo.read_window /
    # tiling.group_by_tile's rim_buffer_m).
    dprst_rim_buffer_m: float = 200.0
    # Interior elevation-range tolerance for the hydro-flattening detector
    # (topo.is_hydroflattened's tol_m). NOTE: compute._polygon_depth_from_dem
    # calls is_hydroflattened with its own 0.01 default and does not yet
    # accept an override, so this ctx field is currently inert (matches the
    # hardcoded default) — a forward-compatible knob, not yet threaded
    # through Tasks 1-6's compute core.
    dprst_flatness_tol_m: float = 0.01
    # Minimum donor count per (ecoregion, FTYPE) group before fill.
    # fit_ecoregion_models attempts a CV-compared calibrated-Hollister fit
    # (fill.N_MIN_DEFAULT).
    dprst_hollister_n_min: int = 5
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
