"""Centralized logging configuration for the ELT pipeline."""

import logging
import sys


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Return a configured logger with a consistent format.

    Args:
        name: Logger name (typically __name__ of the calling module).
        level: Log level (default INFO).

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
