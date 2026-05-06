import logging
import sys


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = (
        "%(asctime)s %(filename)s:%(lineno)d " + "%(levelname)s %(message)s"
    )

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        datefmt = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(log_fmt, datefmt)
        return formatter.format(record)


def init_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(CustomFormatter())
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
        force=True,
    )
