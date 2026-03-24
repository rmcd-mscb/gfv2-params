import logging
import os


def configure_logging(name: str) -> logging.Logger:
    """Configure and return a logger.

    Reads LOG_LEVEL from environment (default: INFO).
    Format includes timestamp, level, and logger name for SLURM log files.

    Uses explicit handler setup rather than basicConfig to avoid conflicts
    when multiple modules configure logging in the same process.
    """
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(getattr(logging, level))
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False

    return logger
