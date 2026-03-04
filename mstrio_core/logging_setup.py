"""
Loguru logging configuration for MicroStrategy scripts.

MstrConfig calls setup_logging() automatically in __post_init__, so most
scripts need only `config = MstrConfig()` to have logging fully configured.

Call setup_logging() directly only when fine-tuning rotation, retention,
or console settings beyond the defaults.

Log files rotate daily and are retained for 30 days.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Union

from loguru import logger


def setup_logging(
    log_dir: Union[str, Path] = "logs",
    level: str = "INFO",
    *,
    rotation: str = "1 day",
    retention: str = "30 days",
    console: bool = True,
) -> None:
    """
    Configure loguru for MicroStrategy scripts.

    Sets up:
        - Console sink (stderr) at the given level.
        - Rotating daily log file in log_dir.

    Args:
        log_dir:   Directory for log files. Default "logs".
        level:     Minimum log level for both sinks (DEBUG/INFO/WARNING/ERROR).
        rotation:  When to rotate log files. Default "1 day".
        retention: How long to keep old log files. Default "30 days".
        console:   Enable console output. Default True.

    Example:
        # Most scripts — logging is auto-configured by MstrConfig:
        from mstrio_core import MstrConfig
        config = MstrConfig()   # setup_logging() called automatically

        # Override defaults (e.g. custom retention):
        from mstrio_core import setup_logging, MstrConfig
        config = MstrConfig()
        setup_logging(log_dir=config.log_dir, level=config.log_level, retention="90 days")
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Remove the default loguru handler so we control the format
    logger.remove()

    fmt = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "{message}"
    )

    if console:
        logger.add(sys.stderr, level=level, format=fmt, colorize=True)

    logger.add(
        log_dir / "{time:YYYY-MM-DD}.log",
        level=level,
        format=fmt,
        rotation=rotation,
        retention=retention,
        encoding="utf-8",
    )

    logger.debug(
        "Logging configured: level={level} log_dir={dir}",
        level=level,
        dir=log_dir,
    )
