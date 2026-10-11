"""Shared I/O helpers: config loading and logger factory (TRPA data engineering conventions)."""
import logging
from datetime import datetime
from pathlib import Path

import yaml


def project_root() -> Path:
    """The Currents project folder (parent of src/)."""
    return Path(__file__).resolve().parent.parent


def load_config(path: str = "config.yaml") -> dict:
    with open(project_root() / path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Logger writing to console and logs/<name>_<timestamp>.log."""
    logs_dir = project_root() / "logs"
    logs_dir.mkdir(exist_ok=True)
    log_file = logs_dir / f"{name}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    for h in (logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")):
        h.setFormatter(fmt)
        logger.addHandler(h)
    logger.info(f"Log file: {log_file}")
    return logger
