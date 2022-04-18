import logging
import logging.handlers

from ..config import RAVSOCK_LOG_FILE


def get_logger():
    # Set up a specific logger with our desired output level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Add the log message handler to the logger
    handler = logging.handlers.RotatingFileHandler(RAVSOCK_LOG_FILE)

    logger.addHandler(handler)

    return logger
