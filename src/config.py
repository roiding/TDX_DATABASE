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

    # 读取 yaml 配置文件（可能不存在，给空字典兜底）
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            _config = yaml.safe_load(f) or {}
    except FileNotFoundError:
        _config = {}

    # 环境变量覆盖 mysql 配置
    _config.setdefault("mysql", {})
    env_map = {
        "MYSQL_HOST": ("host", str),
        "MYSQL_PORT": ("port", int),
        "MYSQL_USER": ("user", str),
        "MYSQL_PASSWORD": ("password", str),
        "MYSQL_DATABASE": ("database", str),
    }
    for env_key, (cfg_key, cast) in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            _config["mysql"][cfg_key] = cast(val)

    return _config


def get_config() -> dict:
    if _config is None:
        return load_config()
    return _config
