"""
This module contains function for logging.
"""

import logging
import os

handler = logging.StreamHandler()

logging.basicConfig(format='%(asctime)s-%(levelname)s-%(name)s: %(message)s')

logging_levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARN,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL}


def get_default_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name and log level. The log level can be set with a environment
    variable the name of the logger is modified to remove the "src." prefix.
    :param name: name of the logger use __name__ to get the module name
    :return: configured logger
    """

    name = name.replace("src.", "")

    logger = logging.getLogger(name)
    log_level = os.environ.get("SERVER_LOG_LEVEL")

    if log_level is None:
        logger.setLevel(logging.WARN)
    else:
        logger.setLevel(logging_levels[log_level.upper()])
    return logger
