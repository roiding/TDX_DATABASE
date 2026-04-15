import yaml
import os
from pathlib import Path

_config = None


def load_config(config_path: str = None) -> dict:
    global _config
    if _config is not None and config_path is None:
        return _config

    if config_path is None:
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config.yaml"

    with open(config_path, "r", encoding="utf-8") as f:
        _config = yaml.safe_load(f)
    return _config


def get_config() -> dict:
    if _config is None:
        return load_config()
    return _config
