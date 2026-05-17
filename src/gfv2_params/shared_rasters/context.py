"""Shared build context for the CONUS shared-raster builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SharedRastersContext:
    """Resolved paths shared across every shared-raster step builder.

    Unlike the depstor BuildContext there is no fabric concept here — these
    rasters are CONUS-wide and reused across every fabric. Per-VPU steps
    iterate ``vpus`` internally rather than being launched once per VPU.

    ``paths`` accumulates outputs each builder produces (keyed by short name)
    so downstream steps can look them up without re-templating off the config.
    """

    data_root: Path
    vpus: list[str]
    output_dir: Path
    paths: dict[str, Path] = field(default_factory=dict)
    force: bool = False

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
