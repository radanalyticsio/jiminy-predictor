"""logger module

This module contains common logging functionality used by the predictor.
"""
import logging


def get_logger(level=logging.DEBUG):
    """Get a logger instance for the predictor.

    Default logging level is `DEBUG` and default outpot is the console.

    :param level: Logging level, default is `DEBUG`
    :return: A logger instance
    """
    logger = logging.getLogger("jiminy-predictor")
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(level)

    return logger
