import logging
import warnings
import re
from src.adapters.logging import get_logger

ANSI_ESCAPE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


def strip_ansi(s: str) -> str:
    return ANSI_ESCAPE.sub("", s)


def test_get_logger_returns_bound_logger():
    log = get_logger()
    assert hasattr(log, "bind")
    assert hasattr(log, "info")


def test_root_logger_and_handler_configured():
    get_logger()
    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]
    assert handler.level == logging.INFO
    assert root.level == logging.INFO
    from structlog.stdlib import ProcessorFormatter

    assert isinstance(handler.formatter, ProcessorFormatter)


def test_info_event_is_rendered(capsys):
    log = get_logger()
    log.info("hello_world", foo=123)
    out = strip_ansi(capsys.readouterr().out)
    assert "hello_world" in out
    assert "foo=123" in out


def test_warnings_are_redirected_to_logging(capsys):
    get_logger()
    warnings.warn("going away soon", DeprecationWarning)
    out = strip_ansi(capsys.readouterr().out)
    assert "DeprecationWarning: going away soon" in out
    assert "py.warnings" in out
