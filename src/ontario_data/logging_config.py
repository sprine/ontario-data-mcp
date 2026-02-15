from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging() -> logging.Logger:
    """Configure structured logging to a rotating file."""
    level_name = os.environ.get("LOG_LEVEL", "WARNING").upper()
    level = getattr(logging, level_name, logging.WARNING)

    logger = logging.getLogger("ontario_data")
    logger.setLevel(level)

    if not logger.handlers:
        cache_dir = os.path.expanduser(
            os.environ.get("ONTARIO_DATA_CACHE_DIR", "~/.cache/ontario-data")
        )
        os.makedirs(cache_dir, exist_ok=True)
        log_path = os.path.join(cache_dir, "server.log")

        handler = RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=3,
        )
        handler.setLevel(level)
        formatter = logging.Formatter(
            '{"time":"%(asctime)s","level":"%(levelname)s","module":"%(name)s","message":"%(message)s"}',
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
