import logging
import os


def configure_logging(name: str) -> logging.Logger:
    """Configure and return a logger.

    Reads LOG_LEVEL from environment (default: INFO).
    Format includes timestamp, level, and logger name for SLURM log files.

    Uses explicit handler setup rather than basicConfig to avoid conflicts
    when multiple modules configure logging in the same process.
    """
    level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, level_str, None)
    if numeric_level is None:
        raise ValueError(f"Invalid LOG_LEVEL: '{level_str}'. Use DEBUG, INFO, WARNING, ERROR, or CRITICAL.")
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(numeric_level)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False

    return logger
