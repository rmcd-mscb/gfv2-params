import os
import yaml
from pathlib import Path

def load_config(path: Path) -> dict:
    with path.open() as f:
        config_raw = f.read()
    config_expanded = os.path.expandvars(config_raw)
    return yaml.safe_load(config_expanded)
