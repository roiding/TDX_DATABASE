import sys
from loguru import logger as _logger
from src.config import get_config


def setup_logger():
    cfg = get_config().get("logging", {})
    level = cfg.get("level", "INFO")
    log_file = cfg.get("file", "logs/tdx_sync.log")
    rotation = cfg.get("rotation", "10 MB")
    retention = cfg.get("retention", "30 days")

    _logger.remove()
    _logger.add(sys.stderr, level=level, format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}")
    _logger.add(
        log_file,
        level=level,
        rotation=rotation,
        retention=retention,
        encoding="utf-8",
        mode="w",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {module}:{function}:{line} | {message}",
    )
    return _logger


logger = _logger
