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

    The on-disk layout under ``data_root/shared/`` is::

        shared/
        ├── per_vpu/{vpu}/     merged NHDPlus rasters + derived per-VPU products
        ├── conus/
        │   ├── vrt/           CONUS VRT files assembled from per_vpu/*
        │   ├── derived/       CONUS-scale derived (soil_moist_max, radtrn, ...)
        │   ├── borders/       Copernicus border-DEM fill
        │   └── weights/       P2P weight matrices
        └── source/            raw unzipped NHDPlus extract cache

    Convenience properties below return resolved paths for each layout slot.
    Use them in builders instead of re-templating ``data_root / "shared" / ...``.

    ``paths`` accumulates outputs each builder produces (keyed by short name)
    so downstream steps can look them up without re-templating off the config.
    """

    data_root: Path
    vpus: list[str]
    output_dir: Path
    paths: dict[str, Path] = field(default_factory=dict)
    force: bool = False

    @property
    def shared_dir(self) -> Path:
        """Root of the fabric-independent shared raster store."""
        return self.data_root / "shared"

    @property
    def per_vpu_dir(self) -> Path:
        """Per-VPU merged + derived raster directory (one subdir per VPU)."""
        return self.shared_dir / "per_vpu"

    @property
    def conus_dir(self) -> Path:
        """Parent of every CONUS-scale output (vrt, derived, borders, weights)."""
        return self.shared_dir / "conus"

    @property
    def vrt_dir(self) -> Path:
        """CONUS VRT files (elevation, slope, aspect, fdr, twi)."""
        return self.conus_dir / "vrt"

    @property
    def derived_dir(self) -> Path:
        """CONUS-scale derived rasters (soil_moist_max, radtrn, resampled CNPY/keep)."""
        return self.conus_dir / "derived"

    @property
    def borders_dir(self) -> Path:
        """Copernicus border-DEM fill (Canada/Mexico)."""
        return self.conus_dir / "borders"

    @property
    def weights_dir(self) -> Path:
        """Polygon-to-polygon weight matrices."""
        return self.conus_dir / "weights"

    @property
    def source_dir(self) -> Path:
        """Raw NHDPlus extract cache (downstream of nhd_downloads/)."""
        return self.shared_dir / "source"

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
