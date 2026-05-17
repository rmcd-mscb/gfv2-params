import os
import re
from pathlib import Path

import yaml

# Canonical VPU definitions
VPUS_DETAILED = [
    "01", "02", "03N", "03S", "03W", "04", "05", "06", "07", "08",
    "09", "10L", "10U", "11", "12", "13", "14", "15", "16", "17", "18",
]

VPUS_SIMPLE = [f"{i:02d}" for i in range(1, 19)]

VPU_RASTER_MAP = {
    "03N": "03", "03S": "03", "03W": "03",
    "10U": "10", "10L": "10",
    "OR": "17",
}

# Default base config location (relative to this file -> repo root)
_DEFAULT_BASE_CONFIG = Path(__file__).resolve().parent.parent.parent / "configs" / "base_config.yml"


def resolve_vpu(vpu: str) -> tuple[str, str]:
    """Return (raster_vpu, gpkg_vpu) for a given VPU code.

    For VPUs with sub-regions (03N/S/W, 10L/U), the raster VPU is the
    parent region while the geopackage VPU retains the sub-region suffix.
    """
    raster_vpu = VPU_RASTER_MAP.get(vpu, vpu)
    gpkg_vpu = vpu
    return raster_vpu, gpkg_vpu


def load_base_config(
    base_config_path: Path | None = None,
    fabric: str | None = None,
) -> dict:
    """Load the base config (data_root, fabric profile, etc.).

    The active fabric profile is flattened onto the top-level dict (profile
    keys win), then `{data_root}` and `{fabric}` placeholders in any value
    are resolved. Fabric resolution order: explicit kwarg -> FABRIC env var
    -> default_fabric in base config.

    Use this when a script needs base paths but does not use a per-step
    YAML config (e.g., merge_and_fill_params, find_missing_hru_ids).
    """
    if base_config_path is None:
        base_config_path = _DEFAULT_BASE_CONFIG
    base = _load_yaml(base_config_path)
    base = _resolve_fabric_profile(base, fabric)
    replacements = {"data_root": base["data_root"], "fabric": base["fabric"]}
    return _resolve_placeholders(base, replacements)


def _resolve_fabric_profile(base: dict, fabric: str | None) -> dict:
    """Flatten the active fabric profile onto the base config."""
    profiles = base.get("fabrics") or {}
    if not profiles:
        raise ValueError(
            "base_config.yml has no `fabrics:` mapping. Define at least one "
            "fabric profile (see configs/base_config.yml)."
        )

    fabric = fabric or os.environ.get("FABRIC") or base.get("default_fabric")
    if fabric is None:
        raise ValueError(
            "No fabric resolved: pass fabric=, set FABRIC env, or add "
            "default_fabric to base_config.yml."
        )
    if fabric not in profiles:
        raise ValueError(
            f"Fabric '{fabric}' not in base_config.yml fabrics: "
            f"{sorted(profiles)}"
        )

    profile = profiles[fabric]
    flat = {k: v for k, v in base.items() if k != "fabrics"}
    flat.update(profile)
    flat["fabric"] = fabric
    return flat


def require_config_key(config: dict, key: str, script_name: str) -> object:
    """Read a required config key, raising a clear error if absent.

    Used by scripts for keys that are expected to come from the active
    fabric profile in base_config.yml. The error message points the user
    there, since that's where these keys live by convention.
    """
    if key not in config:
        fabric = config.get("fabric", "<unknown>")
        raise KeyError(
            f"Required key '{key}' missing from merged config for "
            f"{script_name}. Expected from fabric profile '{fabric}' "
            f"in configs/base_config.yml."
        )
    return config[key]


def _load_yaml(path: Path) -> dict:
    """Load a YAML file and return its contents as a dict."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if data is None:
        raise ValueError(f"Config file is empty or contains no YAML data: {path}")
    if not isinstance(data, dict):
        raise TypeError(f"Config file must contain a YAML mapping, got {type(data).__name__}: {path}")
    return data


def _resolve_placeholders(config: dict, replacements: dict) -> dict:
    """Resolve {placeholder} strings in config values."""
    resolved = {}
    for key, value in config.items():
        if isinstance(value, str):
            for placeholder, replacement in replacements.items():
                value = value.replace(f"{{{placeholder}}}", replacement)
            remaining = re.findall(r'\{(\w+)\}', value)
            if remaining:
                raise ValueError(
                    f"Unresolved placeholder(s) {remaining} in config key '{key}'. "
                    f"Value: '{value}'. Check --vpu or fabric in base_config."
                )
        resolved[key] = value
    return resolved


def load_config(
    step_config_path: Path,
    vpu: str | None = None,
    base_config_path: Path | None = None,
    fabric: str | None = None,
) -> dict:
    """Load base config + step config, resolve placeholders.

    Parameters
    ----------
    step_config_path : Path
        Path to the per-step YAML config file.
    vpu : str, optional
        VPU code (e.g., "03N", "14"). When provided, resolves {data_root},
        {vpu}, and {raster_vpu} placeholders. When None, only {data_root}
        is resolved and all paths must be explicit in the config.
    base_config_path : Path, optional
        Path to base_config.yml. Defaults to configs/base_config.yml
        relative to the package installation.
    fabric : str, optional
        Active fabric name (e.g., "gfv2", "gfv2_vpu01"). Used when the
        base config has a `fabrics:` mapping. Resolution order:
        explicit kwarg -> FABRIC env var -> default_fabric in base config.

    Returns
    -------
    dict
        Merged and resolved configuration dictionary. Contains all keys
        from both base and step configs, with base config values available
        as top-level keys (data_root, targets_dir, output_dir, etc.).
    """
    if base_config_path is None:
        base_config_path = _DEFAULT_BASE_CONFIG

    base = _load_yaml(base_config_path)
    base = _resolve_fabric_profile(base, fabric)
    step = _load_yaml(step_config_path)

    data_root = base["data_root"]

    # Build replacement map
    replacements = {"data_root": data_root}
    fabric = base.get("fabric")
    if fabric is not None:
        replacements["fabric"] = fabric
    if vpu is not None:
        raster_vpu, gpkg_vpu = resolve_vpu(vpu)
        replacements["vpu"] = gpkg_vpu
        replacements["raster_vpu"] = raster_vpu

    # Allow step config scalar values to serve as placeholders.
    # This enables patterns like: source_raster: ".../{lulc_source}/{scenario}_{year}.tif"
    for key, value in step.items():
        if isinstance(value, (str, int, float)) and key not in replacements:
            str_val = str(value)
            if "{" not in str_val:
                replacements[key] = str_val

    # Resolve placeholders in fabric-profile values flattened onto base
    # (e.g., template_raster: "{data_root}/shared/per_vpu/01/Hydrodem_merged_01.tif").
    # Step config values can override these and are resolved next.
    resolved_base = _resolve_placeholders(base, replacements)
    resolved_step = _resolve_placeholders(step, replacements)

    # Merge: base config provides defaults, step config overrides
    merged = {**resolved_base, **resolved_step}

    # Add vpu to config if provided
    if vpu is not None:
        merged["vpu"] = vpu

    return merged
