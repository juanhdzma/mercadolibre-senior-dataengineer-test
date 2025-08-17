import sys
import logging
import warnings
import structlog
from structlog.stdlib import ProcessorFormatter


def get_logger():
    level = logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(colors=True),
            foreign_pre_chain=[
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(
                    fmt="%Y-%m-%d %H:%M:%S", utc=False, key="ts"
                ),
            ],
        )
    )

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)

    logging.captureWarnings(True)
    logging.getLogger("py.warnings").propagate = True

    def warning_to_log(
        message, category, filename, lineno, file=None, line=None
    ):
        logging.getLogger("py.warnings").warning(
            f"{category.__name__}: {message}",
            extra={
                "event": "python_warning",
                "warn_category": category.__name__,
                "warn_message": str(message),
                "warn_file": filename,
                "warn_line": lineno,
            },
        )

    warnings.showwarning = warning_to_log

    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.CallsiteParameterAdder(
                parameters=[
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.LINENO,
                    structlog.processors.CallsiteParameter.MODULE,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.THREAD_NAME,
                    structlog.processors.CallsiteParameter.PROCESS,
                ]
            ),
            structlog.processors.TimeStamper(
                fmt="%Y-%m-%d %H:%M:%S", utc=False, key="ts"
            ),
            ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()
