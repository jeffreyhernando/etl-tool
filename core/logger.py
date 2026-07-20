"""
core/logger.py — Structured JSON/text logger
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path


class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "ts":      datetime.utcnow().isoformat(),
            "level":   record.levelname,
            "logger":  record.name,
            "message": record.getMessage(),
        })


def get_logger(name: str = "ETL") -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    ))
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    # File handler (JSON)
    fh = logging.FileHandler(log_dir / "etl.log")
    fh.setFormatter(JSONFormatter())
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    return logger
