import logging

from medicare_rebuild.logger import setup_logger


def _teardown(logger):
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def test_setup_logger_creates_log_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger("test_logger_creates_file", level="debug")
    try:
        assert isinstance(logger, logging.Logger)
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) == 2
        log_file = tmp_path / "logs" / "test_logger_creates_file_logfile.log"
        assert log_file.exists()
    finally:
        _teardown(logger)


def test_setup_logger_reuses_existing_handlers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    name = "test_logger_reuse"
    first = setup_logger(name, level="info")
    try:
        second = setup_logger(name, level="debug")
        assert second is first
        assert len(second.handlers) == 2
        assert second.level == logging.INFO
    finally:
        _teardown(first)


def test_setup_logger_default_level_is_warning(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger("test_logger_default_level")
    try:
        assert logger.level == logging.WARNING
    finally:
        _teardown(logger)
