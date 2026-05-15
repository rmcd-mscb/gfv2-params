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
    segments_gpkg: Path | None = None
    segments_layer: str = "nsegment"
    waterbody_gpkg: Path | None = None
    waterbody_layer: str | None = None
    fdr_raster: Path | None = None
    twi_raster: Path | None = None
    imperv_source: Path | None = None
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
