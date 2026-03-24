import logging
import os

from gfv2_params.log import configure_logging


def test_configure_logging_returns_logger():
    logger = configure_logging("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    assert logger.propagate is False


def test_configure_logging_respects_env(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    logger = configure_logging("test_debug_logger")
    assert logger.level == logging.DEBUG


def test_configure_logging_no_duplicate_handlers():
    logger = configure_logging("test_dup_logger")
    configure_logging("test_dup_logger")
    assert len(logger.handlers) == 1
